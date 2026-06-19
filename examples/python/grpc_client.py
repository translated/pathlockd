"""gRPC client example — async, with Subscribe stream for GRANT events.

Uses the native gRPC wire (not the HTTP/JSON facade). Generates Python stubs
from the proto, then drives a mutex-style contention scenario: two workers
acquire the same path; the second is enqueued (QUEUED) and waits for a GRANT
event on its Subscribe stream.

Setup (one-time):
    pip install grpcio grpcio-tools
    python -m grpc_tools.protoc -I proto \\
        --python_out=examples/python/generated \\
        --grpc_python_out=examples/python/generated \\
        proto/pathlockd.proto

Run: PYTHONPATH=examples/python/generated python3 examples/python/grpc_client.py
Prereq: pathlockd running (default grpc://localhost:50051).
"""

import asyncio
import os
import sys

generated = os.path.join(os.path.dirname(__file__), "generated")
if generated not in sys.path:
    sys.path.insert(0, generated)

import grpc.aio
from pathlockd_pb2 import (
    AcquireRequest,
    LockRequest,
    ReleaseRequest,
    ReleaseLocksRequest,
    ReleaseAllRequest,
    RenewRequest,
    AssertFencingRequest,
    SubscribeRequest,
)
from pathlockd_pb2_grpc import PathLockStub

GRPC_ADDR = os.getenv("PATHLOCKD_GRPC_ADDR", "localhost:50051")
PATH = "mutex:/grpc-critical"
TTL_MS = 30_000


async def wait_for_grant(stub, owner_id):
    sub = stub.Subscribe(SubscribeRequest(owner_id=owner_id))
    async for event in sub:
        if event.type == 3:
            return
        if event.type == 1:
            raise RuntimeError(f"{owner_id} was KILLED while waiting")
    raise RuntimeError("Subscribe stream closed without GRANT")


async def worker(stub, owner, start_delay):
    await asyncio.sleep(start_delay)

    print(f"[{owner}] acquiring {PATH} ...")
    resp = await stub.Acquire(AcquireRequest(
        owner_id=owner,
        ttl_ms=TTL_MS,
        requests=[LockRequest(path=PATH, mode=0)],
        queue_ttl_ms=60_000,
    ))

    if resp.status == 3:
        blocker = resp.owner
        reason = resp.reason
        print(f"[{owner}] QUEUED behind {blocker} (reason={reason}); "
              f"waiting for GRANT on Subscribe stream ...")
        await wait_for_grant(stub, owner)
        print(f"[{owner}] GRANT received — re-issuing acquire ...")
        resp = await stub.Acquire(AcquireRequest(
            owner_id=owner,
            ttl_ms=TTL_MS,
            requests=[LockRequest(path=PATH, mode=0)],
        ))

    if resp.status != 0:
        raise RuntimeError(f"[{owner}] acquire failed: {resp}")

    fence = resp.fencing_token
    ns = resp.namespace
    print(f"[{owner}] holding lock (fence={fence}, ns={ns})")

    renew_task = asyncio.create_task(renew_loop(stub, owner, ns))

    await asyncio.sleep(2)

    print(f"[{owner}] asserting fencing before backing-store write ...")
    assert_resp = await stub.AssertFencing(AssertFencingRequest(
        owner_id=owner,
        fencing_token=fence,
        paths=[PATH],
    ))
    if assert_resp.status != 0:
        raise RuntimeError(f"[{owner}] fencing failed: {assert_resp}")
    print(f"[{owner}] fence OK — proceeding with write")

    renew_task.cancel()
    try:
        await renew_task
    except asyncio.CancelledError:
        pass

    print(f"[{owner}] releasing ...")
    await stub.Release(ReleaseLocksRequest(
        owner_id=owner,
        requests=[ReleaseRequest(path=PATH, mode=0)],
    ))
    print(f"[{owner}] done.")


async def renew_loop(stub, owner, namespace):
    interval = TTL_MS / 1000 / 3
    while True:
        await asyncio.sleep(interval)
        domains = [namespace] if namespace else []
        resp = await stub.Renew(RenewRequest(
            owner_id=owner,
            ttl_ms=TTL_MS,
            domains=domains,
        ))
        if resp.status != 0:
            print(f"[{owner}] renew LOST: {resp}")
            return
        print(f"[{owner}] renewed")


async def main():
    async with grpc.aio.insecure_channel(GRPC_ADDR) as channel:
        stub = PathLockStub(channel)
        await asyncio.sleep(0.2)

        await stub.ReleaseAll(ReleaseAllRequest(
            owner_id="grpc-worker-A", del_wait_key=True))
        await stub.ReleaseAll(ReleaseAllRequest(
            owner_id="grpc-worker-B", del_wait_key=True))

        await asyncio.gather(
            worker(stub, "grpc-worker-A", 0),
            worker(stub, "grpc-worker-B", 1),
        )
        print("gRPC demo complete.")


if __name__ == "__main__":
    asyncio.run(main())
