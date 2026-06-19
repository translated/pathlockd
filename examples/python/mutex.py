"""Mutual exclusion: two workers contend on one path via the FIFO wait queue.

Worker A holds the lock; Worker B is enqueued (ACQUIRE_STATUS_QUEUED) and
waits for a GRANT event on its SSE stream. When A releases, the daemon
grants B in place.

Run: python3 mutex.py
Prereq: pathlockd with web_listen enabled (default https://localhost:8443).
"""

import threading
import time

from pathlockd_client import PathlockdClient

PATH = "mutex:/critical-section"
TTL_MS = 30_000


def wait_for_grant(client, owner_id):
    for ev in client.stream_events(owner_id):
        if ev["type"] == "grant":
            return ev
    raise RuntimeError("SSE stream closed without a GRANT")


def worker(client, owner):
    print(f"[{owner}] acquiring {PATH} ...")
    resp = client.acquire(
        owner,
        [{"path": PATH, "mode": "MODE_WRITE"}],
        TTL_MS,
        queue_ttl_ms=60_000,
    )
    if resp["status"] == "ACQUIRE_STATUS_QUEUED":
        blocker = resp.get("owner", "?")
        reason = resp.get("reason", "?")
        print(f"[{owner}] QUEUED behind {blocker} ({reason}); "
              f"waiting for GRANT on SSE ...")
        wait_for_grant(client, owner)
        print(f"[{owner}] GRANT received — re-issuing acquire ...")
        resp = client.acquire(
            owner, [{"path": PATH, "mode": "MODE_WRITE"}], TTL_MS)
    if resp["status"] != "ACQUIRE_STATUS_OK":
        raise RuntimeError(f"[{owner}] acquire failed: {resp}")

    fence = resp.get("fencingToken", 0)
    print(f"[{owner}] holding lock (fence={fence}); doing work ...")
    time.sleep(3)
    client.assert_fencing(owner, fence, [PATH])
    print(f"[{owner}] releasing ...")
    client.release(owner, [{"path": PATH, "mode": "MODE_WRITE"}])
    print(f"[{owner}] done.")


def main():
    client = PathlockdClient()
    client.health()
    client.release_all("worker-A")
    client.release_all("worker-B")

    t_a = threading.Thread(target=worker, args=(client, "worker-A"))
    t_b = threading.Thread(target=worker, args=(client, "worker-B"))
    t_a.start()
    time.sleep(1)
    t_b.start()
    t_a.join()
    t_b.join()
    print("mutex demo complete.")


if __name__ == "__main__":
    main()
