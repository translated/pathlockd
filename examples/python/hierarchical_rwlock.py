"""Hierarchical RWLock: subtree writes vs. point reads.

A write on vfs:/docs/team owns the whole subtree, so a read on
vfs:/docs/team/file.txt is QUEUED (REASON_CODE_ANCESTOR_LOCKED). A read on
the sibling vfs:/docs/other succeeds immediately — the subtree rule does not
cross into sibling branches. When the writer releases, the queued reader is
granted in place via a GRANT event.

Note: a successful acquire by an owner dequeues that owner's wait-queue
entries, so the sibling read is done by a *different* owner to avoid
clearing the queued entry.

Run: python3 hierarchical_rwlock.py
Prereq: pathlockd with web_listen enabled (default https://localhost:8443).
"""

import threading
import time

from pathlockd_client import PathlockdClient

WRITER_PATH = "vfs:/docs/team"
QUEUED_READ = "vfs:/docs/team/file.txt"
SIBLING_READ = "vfs:/docs/other"
TTL_MS = 30_000


def main():
    client = PathlockdClient()
    client.health()
    client.release_all("writer-1")
    client.release_all("reader-1")
    client.release_all("reader-2")
    time.sleep(0.5)

    resp = client.acquire(
        "writer-1", [{"path": WRITER_PATH, "mode": "MODE_WRITE"}], TTL_MS)
    assert resp["status"] == "ACQUIRE_STATUS_OK", resp
    print(f"[writer-1] write lock on {WRITER_PATH} "
          f"(fence={resp['fencingToken']})")

    resp = client.acquire(
        "reader-1",
        [{"path": QUEUED_READ, "mode": "MODE_READ"}],
        TTL_MS,
        queue_ttl_ms=60_000,
    )
    print(f"[reader-1] acquire {QUEUED_READ} -> {resp['status']} "
          f"(reason={resp.get('reason')})")
    assert resp["status"] == "ACQUIRE_STATUS_QUEUED"

    resp = client.acquire(
        "reader-2", [{"path": SIBLING_READ, "mode": "MODE_READ"}], TTL_MS)
    print(f"[reader-2] acquire {SIBLING_READ} -> {resp['status']} "
          f"(sibling succeeds — subtree rule does not cross branches)")
    assert resp["status"] == "ACQUIRE_STATUS_OK"

    grant = threading.Event()

    def listen():
        for ev in client.stream_events("reader-1"):
            print(f"[reader-1] SSE event: {ev}")
            if ev["type"] == "grant":
                grant.set()
                return

    t = threading.Thread(target=listen, daemon=True)
    t.start()

    time.sleep(3)
    print("[writer-1] releasing subtree ...")
    client.release("writer-1", [{"path": WRITER_PATH, "mode": "MODE_WRITE"}])

    if not grant.wait(timeout=10):
        raise RuntimeError("no GRANT received within 10s")

    resp = client.acquire(
        "reader-1", [{"path": QUEUED_READ, "mode": "MODE_READ"}], TTL_MS)
    print(f"[reader-1] re-acquire {QUEUED_READ} -> {resp['status']}")
    assert resp["status"] == "ACQUIRE_STATUS_OK"

    client.release_all("reader-1")
    client.release_all("reader-2")
    print("hierarchical rwlock demo complete.")


if __name__ == "__main__":
    main()
