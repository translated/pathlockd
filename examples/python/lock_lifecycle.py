"""Lock lifecycle: a high-level Lock object managing a pathlockd lease.

Demonstrates:
  - A Lock class wrapping acquire / renew / release with background threads.
  - Adding and removing paths during the lease lifetime.
  - Fencing tokens: assert_fencing() before each backing-store write.
  - Renewals: a background thread renews at ttl/3.
  - Preemption: the SSE listener reacts to KILLED (force-release) and stops I/O.
  - Deadlock: two owners wait on each other, DetectCycle finds the cycle,
    one victim is force-released, the other is granted.

The old anti-starvation claim subsystem was removed in v0.9.0; the wait queue
(QUEUED + GRANT) replaces it. This Lock handles QUEUED transparently: on a
contended acquire it sets a wait edge (for deadlock detection) and waits for
a GRANT event on the owner's SSE stream.

Run: python3 lock_lifecycle.py
Prereq: pathlockd with web_listen enabled (default https://localhost:8443).
"""

import threading
import time

from pathlockd_client import PathlockdClient, PathlockdError

TTL_MS = 30_000
QUEUE_TTL_MS = 60_000


class PreemptedError(Exception):
    pass


class Lock:
    """A pathlockd lease handle for one owner.

    Wraps acquire / renew / release and monitors the owner's SSE stream for
    preemption (KILLED) and cooperative revoke (REVOKE). Paths can be added
    and removed during the lease lifetime via add_paths() / remove_paths().
    Fencing tokens are exposed for backing-store write guards (assert_fencing).

    On a contended acquire the Lock is enqueued (ACQUIRE_STATUS_QUEUED), sets a
    wait edge for deadlock detection, and waits for a GRANT event before
    proceeding. The wait edge is cleared on clean release or preemption.
    """

    def __init__(self, client, owner, paths, *, ttl_ms=TTL_MS,
                 mode="MODE_WRITE", queue_ttl_ms=QUEUE_TTL_MS):
        self._client = client
        self._owner = owner
        self._ttl_ms = ttl_ms
        self._mode = mode
        self._fences = {}
        self._namespace = ""
        self._held_paths = set()
        self._killed = threading.Event()
        self._revoked = threading.Event()
        self._closed = False
        self._guards = []

        self._acquire(paths, queue_ttl_ms)

        self._renewer = threading.Thread(
            target=self._renew_loop, daemon=True, name=f"renew-{owner}")
        self._listener = threading.Thread(
            target=self._event_loop, daemon=True, name=f"sse-{owner}")
        self._renewer.start()
        self._listener.start()

    def _acquire(self, paths, queue_ttl_ms):
        requests = [{"path": p, "mode": self._mode} for p in paths]
        resp = self._client.acquire(
            self._owner, requests, self._ttl_ms,
            queue_ttl_ms=queue_ttl_ms)

        if resp["status"] == "ACQUIRE_STATUS_QUEUED":
            conflict_path = resp.get("path", "")
            conflict_owner = resp.get("owner", "")
            reason = resp.get("reason", "REASON_CODE_UNSPECIFIED")
            print(f"[{self._owner}] QUEUED behind {conflict_owner} "
                  f"({reason}); waiting for GRANT ...")
            if conflict_owner:
                self._client.set_wait_edge(
                    self._owner, conflict_owner, self._ttl_ms,
                    conflict_path=conflict_path, reason=reason)
            self._wait_for_grant()
            resp = self._client.acquire(
                self._owner, requests, self._ttl_ms)

        if resp["status"] != "ACQUIRE_STATUS_OK":
            raise RuntimeError(f"[{self._owner}] acquire failed: {resp}")

        fence = resp.get("fencingToken", 0)
        self._namespace = resp.get("namespace", "")
        for p in paths:
            self._fences[p] = fence
        self._held_paths.update(paths)
        self._client.clear_wait_edge(self._owner)
        print(f"[{self._owner}] acquired {paths} "
              f"(fence={fence}, ns={self._namespace})")

    def _wait_for_grant(self):
        for ev in self._client.stream_events(self._owner):
            if ev["type"] == "grant":
                return
            if ev["type"] == "killed":
                self._killed.set()
                raise PreemptedError(
                    f"[{self._owner}] preempted while waiting in queue")

    def add_paths(self, paths):
        """Acquire additional paths and fold them into this lease."""
        self._check_alive()
        with threading.Lock():
            new = [p for p in paths if p not in self._held_paths]
        if not new:
            return
        print(f"[{self._owner}] adding paths {new} ...")
        self._acquire(new, QUEUE_TTL_MS)

    def remove_paths(self, paths):
        """Release specific paths while keeping the rest of the lease alive."""
        with threading.Lock():
            to_release = [p for p in paths if p in self._held_paths]
        if not to_release:
            return
        print(f"[{self._owner}] removing paths {to_release} ...")
        requests = [{"path": p, "mode": self._mode} for p in to_release]
        self._client.release(self._owner, requests)
        with threading.Lock():
            self._held_paths.difference_update(to_release)

    def assert_fencing(self, paths=None):
        """Verify fencing tokens before a backing-store write.

        Fencing tokens are per-path and must exactly match the persisted
        fence. Returns True if every path's fence matches; False if any
        is stale — the caller MUST abort the write.
        """
        self._check_alive()
        with threading.Lock():
            check = paths or list(self._held_paths)
        if not check:
            return True
        for p in check:
            fence = self._fences.get(p, 0)
            if fence == 0:
                continue
            resp = self._client.assert_fencing(
                self._owner, fence, [p])
            if resp["status"] != "ASSERT_STATUS_OK":
                print(f"[{self._owner}] FENCING FAILED on "
                      f"{resp.get('path')} ({resp.get('reason')}) "
                      f"— aborting write")
                return False
        return True

    def detect_deadlock(self, max_depth=20):
        """Walk the wait-for graph from this owner."""
        resp = self._client.detect_cycle(self._owner, max_depth)
        return resp

    @property
    def fencing_token(self):
        with threading.Lock():
            return dict(self._fences)

    @property
    def held_paths(self):
        with threading.Lock():
            return set(self._held_paths)

    @property
    def killed(self):
        return self._killed.is_set()

    @property
    def revoked(self):
        return self._revoked.is_set()

    def _check_alive(self):
        if self._killed.is_set():
            raise PreemptedError(
                f"[{self._owner}] lock was preempted — all I/O must stop")

    def close(self):
        """Release all paths and stop background threads."""
        if self._closed:
            return
        self._closed = True
        try:
            self._client.clear_wait_edge(self._owner)
            self._client.release_all(self._owner)
            print(f"[{self._owner}] released all paths")
        except PathlockdError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def _renew_loop(self):
        interval = self._ttl_ms / 1000 / 3
        while not self._closed and not self._killed.is_set():
            time.sleep(interval)
            if self._closed or self._killed.is_set():
                break
            try:
                resp = self._client.renew(
                    self._owner, self._ttl_ms,
                    domains=[self._namespace] if self._namespace else None)
                if resp["status"] != "RENEW_STATUS_OK":
                    print(f"[{self._owner}] renew LOST: {resp}")
                    self._killed.set()
                    break
            except PathlockdError as e:
                print(f"[{self._owner}] renew error: {e}")

    def _event_loop(self):
        try:
            for ev in self._client.stream_events(self._owner):
                if ev["type"] == "killed":
                    print(f"[{self._owner}] KILLED — force-released; "
                          f"stop all backing-store I/O immediately")
                    self._killed.set()
                    return
                elif ev["type"] == "revoke":
                    print(f"[{self._owner}] REVOKE — cooperative yield request")
                    self._revoked.set()
        except (PathlockdError, OSError):
            if not self._closed:
                print(f"[{self._owner}] SSE stream lost")


def demo_basic_lifecycle(client):
    print("\n=== Phase 1: Basic lifecycle — acquire, grow, shrink, fence ===")

    with Lock(client, "owner-1", ["vfs:/docs/a", "vfs:/docs/b"]) as lock:
        time.sleep(0.5)

        lock.add_paths(["vfs:/docs/c"])
        print(f"[owner-1] now holding: {sorted(lock.held_paths)}")

        lock.remove_paths(["vfs:/docs/a"])
        print(f"[owner-1] now holding: {sorted(lock.held_paths)}")

        print("[owner-1] asserting fencing before a backing-store write ...")
        if lock.assert_fencing():
            print("[owner-1] fence OK — proceeding with write")
        else:
            raise RuntimeError("fencing failed — should not happen here")

        time.sleep(1)
    time.sleep(0.5)


def demo_preemption(client):
    print("\n=== Phase 2: Preemption — force-release and KILLED handling ===")

    lock = Lock(client, "owner-2", ["vfs:/work/critical"])
    time.sleep(0.5)
    print(f"[owner-2] holding: {sorted(lock.held_paths)}")

    print("[main] force-releasing owner-2 ...")
    client.force_release("owner-2")

    deadline = time.time() + 10
    while not lock.killed and time.time() < deadline:
        time.sleep(0.1)
    if not lock.killed:
        raise RuntimeError("owner-2 did not detect KILLED within 10s")
    print("[main] owner-2 detected preemption — I/O would stop here")

    try:
        lock.assert_fencing()
    except PreemptedError as e:
        print(f"[main] correctly blocked post-preemption: {e}")
    lock.close()
    time.sleep(0.5)


def demo_deadlock(client):
    print("\n=== Phase 3: Deadlock — cycle detection and victim release ===")

    client.release_all("deadlock-A")
    client.release_all("deadlock-B")
    time.sleep(0.3)

    lock_a = Lock(client, "deadlock-A", ["dl:/resource-X"])
    lock_b = Lock(client, "deadlock-B", ["dl:/resource-Y"])
    time.sleep(0.5)

    a_blocked = threading.Event()
    a_error = [None]

    def a_wants_y():
        try:
            lock_a.add_paths(["dl:/resource-Y"])
        except Exception as e:
            a_error[0] = e
        a_blocked.set()

    def b_wants_x():
        try:
            lock_b.add_paths(["dl:/resource-X"])
        except PreemptedError as e:
            print(f"[deadlock-B] preempted while waiting: {e}")
        except Exception as e:
            print(f"[deadlock-B] error while waiting: {e}")

    t_a = threading.Thread(target=a_wants_y, daemon=True)
    t_b = threading.Thread(target=b_wants_x, daemon=True)
    t_a.start()
    time.sleep(1)
    t_b.start()
    time.sleep(2)

    print("[main] detecting deadlock from deadlock-A ...")
    cycle = lock_a.detect_deadlock(max_depth=10)
    kind = cycle["kind"]
    print(f"[main] DetectCycle -> kind={kind}, chain={cycle['chain']}")

    if kind in (1, "CYCLE_KIND_FOUND"):
        victim = cycle["chain"][-1]
        print(f"[main] force-releasing victim: {victim}")
        client.force_release(victim)

        deadline = time.time() + 10
        while not a_blocked.is_set() and time.time() < deadline:
            time.sleep(0.1)

        if a_error[0]:
            print(f"[main] deadlock-A was also preempted: {a_error[0]}")
        elif a_blocked.is_set():
            print(f"[main] deadlock-A now holds: "
                  f"{sorted(lock_a.held_paths)}")
    else:
        print("[main] no cycle detected — check timing")

    lock_a.close()
    lock_b.close()
    time.sleep(0.5)


def main():
    client = PathlockdClient()
    client.health()

    for owner in ("owner-1", "owner-2", "deadlock-A", "deadlock-B"):
        client.release_all(owner)

    demo_basic_lifecycle(client)
    demo_preemption(client)
    demo_deadlock(client)

    print("\nlock lifecycle demo complete.")


if __name__ == "__main__":
    main()
