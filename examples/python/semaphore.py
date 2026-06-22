"""Counting semaphore: cap concurrent holders at N.

Sets the namespace policy to LOCK_ALGORITHM_SEMAPHORE, then acquires with
permits=3. Three workers hold simultaneously; a fourth is QUEUED
(REASON_CODE_SEMAPHORE_FULL) and waits for a GRANT on its SSE stream when
one holder releases.

Run: python3 semaphore.py
Prereq: pathlockd with web_listen enabled (default https://localhost:8443).
"""

import threading
import time

from pathlockd_client import PathlockdClient

PATH = "pool:/db-connections"
NAMESPACE = "pool:/"
CAPACITY = 3
TTL_MS = 30_000
OWNERS = ["sem-1", "sem-2", "sem-3", "sem-4"]


def holder(client, owner):
    resp = client.acquire(
        owner,
        [{"path": PATH, "mode": "MODE_WRITE", "permits": CAPACITY}],
        TTL_MS,
        queue_ttl_ms=60_000,
    )
    if resp["status"] == "ACQUIRE_STATUS_QUEUED":
        print(f"[{owner}] QUEUED (semaphore full); waiting for GRANT ...")
        for ev in client.stream_events(owner):
            if ev["type"] == "grant":
                break
        print(f"[{owner}] GRANT received — re-issuing acquire ...")
        resp = client.acquire(
            owner,
            [{"path": PATH, "mode": "MODE_WRITE", "permits": CAPACITY}],
            TTL_MS,
        )
    if resp["status"] != "ACQUIRE_STATUS_OK":
        raise RuntimeError(f"[{owner}] acquire failed: {resp}")

    held = client.inspect_path(PATH)
    in_use = len(held.get("semaphoreOwners", []))
    print(f"[{owner}] holding permit ({in_use}/{CAPACITY} in use)")
    time.sleep(2)
    client.release(owner, [{"path": PATH, "mode": "MODE_WRITE"}])
    print(f"[{owner}] released permit")


def main():
    client = PathlockdClient()
    client.health()

    client.set_namespace_policy(NAMESPACE, "LOCK_ALGORITHM_SEMAPHORE")
    policy = client.get_namespace_policy(NAMESPACE)
    print(f"namespace {NAMESPACE} policy: {policy['algorithm']} "
          f"(explicit={policy['explicit']})")

    for o in OWNERS:
        client.release_all(o)

    threads = []
    for owner in OWNERS:
        t = threading.Thread(target=holder, args=(client, owner))
        threads.append(t)
        t.start()
        time.sleep(0.5)

    for t in threads:
        t.join()

    print("semaphore demo complete.")


if __name__ == "__main__":
    main()
