"""HTTP/3 + 0-RTT example using aioquic.

Demonstrates the server's 0-RTT safety gate (src/web/h3.rs):
  - Read-only RPCs (GET /v1/health, POST /v1/inspectPath) dispatched during
    QUIC early data (before the TLS handshake confirms) succeed.
  - Mutating RPCs (POST /v1/incrFencingToken) sent in early data get
    425 Too Early and must be retried on the established 1-RTT connection.

Flow:
  1. First connection: complete the handshake, save the session ticket.
  2. Second connection (0-RTT): send a read-only request in early data
     → succeeds.
  3. Same connection, still in early data: send a mutating request
     → 425 Too Early.
  4. After the handshake confirms (1-RTT): retry the mutating request
     → succeeds.

Setup:
    pip install aioquic

Run: python3 examples/python/http3_zero_rtt.py
Prereq: pathlockd with h3_listen enabled (default UDP :8443) and
        web_zero_rtt = true.
"""

import asyncio
import json
import os
import socket
import time

from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.connection import QuicConnection
from aioquic.h3.connection import H3Connection
from aioquic.h3.events import DataReceived, HeadersReceived

H3_HOST = os.getenv("PATHLOCKD_H3_HOST", "localhost")
H3_PORT = int(os.getenv("PATHLOCKD_H3_PORT", "8443"))


def build_configuration(session_ticket=None):
    config = QuicConfiguration(alpn_protocols=["h3"], is_client=True)
    config.verify_mode = False
    if session_ticket is not None:
        config.session_ticket = session_ticket
    return config


class H3Response:
    def __init__(self):
        self.status = 0
        self.body = b""
        self.done = False


def pump_quic(quic, sock):
    now = time.monotonic()
    for datagram, addr in quic.datagrams_to_send(now):
        sock.sendto(datagram, addr)
    try:
        sock.setblocking(False)
        while True:
            data, addr = sock.recvfrom(65535)
            quic.receive_datagram(data, addr, now=time.monotonic())
    except (BlockingIOError, InterruptedError):
        pass


def drain_events(quic, h3, responses):
    while True:
        event = quic.next_event()
        if event is None:
            break
        for h3_event in h3.handle_event(event):
            if isinstance(h3_event, HeadersReceived):
                sid = h3_event.stream_id
                if sid not in responses:
                    responses[sid] = H3Response()
                for name, value in h3_event.headers:
                    name = name.decode() if isinstance(name, bytes) else name
                    value = value.decode() if isinstance(value, bytes) else value
                    if name == ":status":
                        responses[sid].status = int(value)
            elif isinstance(h3_event, DataReceived):
                sid = h3_event.stream_id
                if sid not in responses:
                    responses[sid] = H3Response()
                responses[sid].body += h3_event.data
                if h3_event.stream_ended:
                    responses[sid].done = True


def send_request(h3, stream_id, method, path, body=None):
    headers = [
        (b":method", method.encode()),
        (b":scheme", b"https"),
        (b":authority", f"{H3_HOST}:{H3_PORT}".encode()),
        (b":path", path.encode()),
    ]
    if body is not None:
        headers.append((b"content-type", b"application/json"))
    h3.send_headers(stream_id=stream_id, headers=headers, end_stream=False)
    h3.send_data(
        stream_id=stream_id,
        data=body if body else b"",
        end_stream=True,
    )


async def connect_and_save_ticket():
    print("--- Phase 1: first connection (full handshake, save session ticket) ---")

    ticket_holder = {}

    def on_ticket(ticket):
        ticket_holder["ticket"] = ticket

    config = build_configuration()
    quic = QuicConnection(
        configuration=config,
        session_ticket_handler=on_ticket,
    )
    quic.connect(addr=(H3_HOST, H3_PORT), now=time.monotonic())

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.connect((H3_HOST, H3_PORT))
    h3 = H3Connection(quic)

    for _ in range(200):
        pump_quic(quic, sock)
        await asyncio.sleep(0.02)
        if "ticket" in ticket_holder:
            break

    sock.close()
    if "ticket" in ticket_holder:
        print("    session ticket saved")
    else:
        print("    warning: no session ticket received — 0-RTT will not be possible")
    return ticket_holder.get("ticket")


async def zero_rtt_round(saved_ticket):
    print("\n--- Phase 2: 0-RTT reconnect (early data) ---")

    if saved_ticket is None:
        print("    no saved ticket — skipping 0-RTT round")
        return

    config = build_configuration(session_ticket=saved_ticket)
    quic = QuicConnection(configuration=config)
    quic.connect(addr=(H3_HOST, H3_PORT), now=time.monotonic())

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.connect((H3_HOST, H3_PORT))
    h3 = H3Connection(quic)

    responses = {}

    ro_stream = quic.get_next_available_stream_id()
    print(f"    [early data] GET /v1/health on stream {ro_stream} ...")
    send_request(h3, ro_stream, "GET", "/v1/health")

    mut_stream = quic.get_next_available_stream_id()
    print(f"    [early data] POST /v1/incrFencingToken on stream {mut_stream} ...")
    send_request(h3, mut_stream, "POST", "/v1/incrFencingToken", b"{}")

    print("    driving QUIC until both responses arrive ...")
    for _ in range(400):
        pump_quic(quic, sock)
        drain_events(quic, h3, responses)
        if all(
            responses.get(s) and responses[s].done
            for s in [ro_stream, mut_stream]
        ):
            break
        await asyncio.sleep(0.02)

    ro = responses.get(ro_stream)
    if ro:
        print(f"    GET /v1/health -> {ro.status} {ro.body.decode()}")
    else:
        print("    GET /v1/health -> no response")

    mut = responses.get(mut_stream)
    if mut:
        print(f"    POST /v1/incrFencingToken -> {mut.status} {mut.body.decode()}")
        if mut.status == 425:
            print("    -> 425 Too Early (expected for mutation in 0-RTT)")
        elif mut.status == 200:
            print("    -> 200 (handshake completed before dispatch)")
    else:
        print("    POST /v1/incrFencingToken -> no response")

    print("\n--- Phase 3: retry mutation on established 1-RTT connection ---")
    retry_stream = quic.get_next_available_stream_id()
    print(f"    [1-RTT] POST /v1/incrFencingToken on stream {retry_stream} ...")
    send_request(h3, retry_stream, "POST", "/v1/incrFencingToken", b"{}")

    for _ in range(300):
        pump_quic(quic, sock)
        drain_events(quic, h3, responses)
        if responses.get(retry_stream) and responses[retry_stream].done:
            break
        await asyncio.sleep(0.02)

    retry = responses.get(retry_stream)
    if retry:
        print(f"    POST /v1/incrFencingToken -> {retry.status} {retry.body.decode()}")
        if retry.status == 200:
            print("    -> 200 (mutation accepted on 1-RTT connection)")
    else:
        print("    POST /v1/incrFencingToken -> no response")

    sock.close()


async def main():
    print(f"HTTP/3 + 0-RTT demo against https://{H3_HOST}:{H3_PORT}")
    print()

    ticket = await connect_and_save_ticket()
    await zero_rtt_round(ticket)

    print("\nHTTP/3 0-RTT demo complete.")


if __name__ == "__main__":
    asyncio.run(main())
