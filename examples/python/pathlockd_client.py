"""Minimal pathlockd HTTP client (Python stdlib only).

Talks to the JSON HTTP facade (web_listen) over HTTP/1.1 with TLS.
Use verify_tls=False for the daemon's self-signed dev cert.

Enum fields are accepted as readable string names (e.g. "MODE_WRITE") and
translated to the integer values the prost-serde wire format expects.
Response enums are translated back to string names.
"""

import http.client
import json
import ssl
import urllib.parse

MODE = {"MODE_WRITE": 0, "MODE_READ": 1}
LOCK_STATE = {"LOCK_STATE_NEW": 0, "LOCK_STATE_HELD": 1}
LOCK_ALGORITHM = {
    "LOCK_ALGORITHM_RECURSIVE_RW": 0,
    "LOCK_ALGORITHM_POINT_RW": 1,
    "LOCK_ALGORITHM_RECURSIVE_WRITE": 2,
    "LOCK_ALGORITHM_POINT_WRITE": 3,
    "LOCK_ALGORITHM_SEMAPHORE": 4,
}
REASON = {
    "REASON_CODE_UNSPECIFIED": 0,
    "REASON_CODE_ANCESTOR_LOCKED": 1,
    "REASON_CODE_WRITE_LOCKED": 2,
    "REASON_CODE_READ_LOCKED": 3,
    "REASON_CODE_DESCENDANT_WRITE_LOCKED": 4,
    "REASON_CODE_DESCENDANT_READ_LOCKED": 5,
    "REASON_CODE_READ_LOCKS_DISABLED": 6,
    "REASON_CODE_STALE_FENCING_TOKEN": 7,
    "REASON_CODE_INVALID_PERMITS": 8,
    "REASON_CODE_SEMAPHORE_FULL": 9,
    "REASON_CODE_MISSING_SEMAPHORE": 10,
    "REASON_CODE_MISSING_WRITE": 11,
    "REASON_CODE_MISSING_READ": 12,
    "REASON_CODE_MISSING_FENCE": 13,
    "REASON_CODE_MISSING_ALIVE": 14,
    "REASON_CODE_MISSING_OWNER_SET": 15,
    "REASON_CODE_EMPTY_OWNER_SET": 16,
    "REASON_CODE_QUEUED": 17,
    "REASON_CODE_STALE_OWNER": 18,
}
ACQUIRE_STATUS = {
    0: "ACQUIRE_STATUS_OK",
    1: "ACQUIRE_STATUS_CONFLICT",
    2: "ACQUIRE_STATUS_LOST",
    3: "ACQUIRE_STATUS_QUEUED",
}
RENEW_STATUS = {0: "RENEW_STATUS_OK", 1: "RENEW_STATUS_LOST"}
ASSERT_STATUS = {0: "ASSERT_STATUS_OK", 1: "ASSERT_STATUS_FAIL"}
REASON_INT = {v: k for k, v in REASON.items()}


def _enum(value, mapping):
    if isinstance(value, int):
        return value
    if value in mapping:
        return mapping[value]
    raise ValueError(f"unknown enum name {value!r}; expected one of {list(mapping)}")


def _translate_lock_request(req):
    out = dict(req)
    if "mode" in out:
        out["mode"] = _enum(out["mode"], MODE)
    if "state" in out:
        out["state"] = _enum(out["state"], LOCK_STATE)
    return out


def _translate_release_request(req):
    out = dict(req)
    if "mode" in out:
        out["mode"] = _enum(out["mode"], MODE)
    return out


def _decode_acquire(resp):
    if "status" in resp and isinstance(resp["status"], int):
        resp["status"] = ACQUIRE_STATUS.get(resp["status"], str(resp["status"]))
    if "reason" in resp and isinstance(resp["reason"], int):
        resp["reason"] = REASON_INT.get(resp["reason"], str(resp["reason"]))
    return resp


def _decode_renew(resp):
    if "status" in resp and isinstance(resp["status"], int):
        resp["status"] = RENEW_STATUS.get(resp["status"], str(resp["status"]))
    if "reason" in resp and isinstance(resp["reason"], int):
        resp["reason"] = REASON_INT.get(resp["reason"], str(resp["reason"]))
    return resp


def _decode_assert(resp):
    if "status" in resp and isinstance(resp["status"], int):
        resp["status"] = ASSERT_STATUS.get(resp["status"], str(resp["status"]))
    if "reason" in resp and isinstance(resp["reason"], int):
        resp["reason"] = REASON_INT.get(resp["reason"], str(resp["reason"]))
    return resp


class PathlockdError(Exception):
    def __init__(self, code, message, http_status):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.http_status = http_status


class PathlockdClient:
    def __init__(self, base_url="https://localhost:8443", verify_tls=False,
                 timeout=30):
        parsed = urllib.parse.urlparse(base_url)
        self._host = parsed.hostname
        self._port = parsed.port or (443 if parsed.scheme == "https" else 80)
        self._scheme = parsed.scheme
        self._timeout = timeout
        if self._scheme == "https":
            ctx = ssl.create_default_context()
            if not verify_tls:
                ctx.check_hostname = False
                ctx.verify_mode = ssl.CERT_NONE
            self._conn_factory = lambda: http.client.HTTPSConnection(
                self._host, self._port, context=ctx, timeout=self._timeout)
            self._stream_conn_factory = lambda: http.client.HTTPSConnection(
                self._host, self._port, context=ctx, timeout=None)
        else:
            self._conn_factory = lambda: http.client.HTTPConnection(
                self._host, self._port, timeout=self._timeout)
            self._stream_conn_factory = lambda: http.client.HTTPConnection(
                self._host, self._port, timeout=None)

    def _post(self, path, body):
        conn = self._conn_factory()
        try:
            conn.request("POST", path,
                         body=json.dumps(body).encode(),
                         headers={"Content-Type": "application/json"})
            resp = conn.getresponse()
            data = resp.read()
            return self._handle(resp.status, data)
        finally:
            conn.close()

    def _get(self, path, headers=None):
        conn = self._conn_factory()
        try:
            conn.request("GET", path, headers=headers or {})
            resp = conn.getresponse()
            data = resp.read()
            return self._handle(resp.status, data)
        finally:
            conn.close()

    @staticmethod
    def _handle(status, data):
        if status == 200:
            return json.loads(data) if data else {}
        body = json.loads(data) if data else {}
        err = body.get("error", {})
        raise PathlockdError(
            err.get("code", "UNKNOWN"),
            err.get("message", ""),
            status,
        )

    def health(self):
        return self._get("/v1/health")

    def acquire(self, owner_id, requests, ttl_ms, *, fencing_token=0,
                release_requests=None, idempotency_key="", queue_ttl_ms=0):
        body = {"ownerId": owner_id, "ttlMs": ttl_ms,
                "requests": [_translate_lock_request(r) for r in requests]}
        if fencing_token:
            body["fencingToken"] = fencing_token
        if release_requests:
            body["releaseRequests"] = [
                _translate_release_request(r) for r in release_requests]
        if idempotency_key:
            body["idempotencyKey"] = idempotency_key
        if queue_ttl_ms:
            body["queueTtlMs"] = queue_ttl_ms
        return _decode_acquire(self._post("/v1/acquire", body))

    def release(self, owner_id, requests=None, *, del_wait_key=False,
                idempotency_key=""):
        body = {"ownerId": owner_id, "delWaitKey": del_wait_key}
        if requests:
            body["requests"] = [
                _translate_release_request(r) for r in requests]
        if idempotency_key:
            body["idempotencyKey"] = idempotency_key
        return self._post("/v1/release", body)

    def release_all(self, owner_id, *, del_wait_key=False,
                    idempotency_key=""):
        body = {"ownerId": owner_id, "delWaitKey": del_wait_key}
        if idempotency_key:
            body["idempotencyKey"] = idempotency_key
        return self._post("/v1/releaseAll", body)

    def renew(self, owner_id, ttl_ms, *, domains=None,
              idempotency_key=""):
        body = {"ownerId": owner_id, "ttlMs": ttl_ms}
        if domains:
            body["domains"] = domains
        if idempotency_key:
            body["idempotencyKey"] = idempotency_key
        return _decode_renew(self._post("/v1/renew", body))

    def assert_fencing(self, owner_id, fencing_token, paths):
        return _decode_assert(self._post("/v1/assertFencing", {
            "ownerId": owner_id,
            "fencingToken": fencing_token,
            "paths": paths,
        }))

    def set_namespace_policy(self, namespace, algorithm, *,
                             idempotency_key=""):
        body = {"namespace": namespace,
                "algorithm": _enum(algorithm, LOCK_ALGORITHM)}
        if idempotency_key:
            body["idempotencyKey"] = idempotency_key
        return self._post("/v1/setNamespacePolicy", body)

    def get_namespace_policy(self, namespace):
        return self._post("/v1/getNamespacePolicy", {"namespace": namespace})

    def delete_namespace_policy(self, namespace, *, idempotency_key=""):
        body = {"namespace": namespace}
        if idempotency_key:
            body["idempotencyKey"] = idempotency_key
        return self._post("/v1/deleteNamespacePolicy", body)

    def is_owner_alive(self, owner_id):
        return self._post("/v1/isOwnerAlive", {"ownerId": owner_id})

    def list_owner_locks(self, owner_id):
        return self._post("/v1/listOwnerLocks", {"ownerId": owner_id})

    def inspect_path(self, path):
        return self._post("/v1/inspectPath", {"path": path})

    def set_wait_edge(self, owner_id, conflict_owner, ttl_ms, *,
                      conflict_path="", reason="REASON_CODE_UNSPECIFIED",
                      idempotency_key=""):
        body = {
            "ownerId": owner_id,
            "conflictOwner": conflict_owner,
            "ttlMs": ttl_ms,
        }
        if conflict_path:
            body["conflictPath"] = conflict_path
        if reason:
            body["reason"] = _enum(reason, REASON)
        if idempotency_key:
            body["idempotencyKey"] = idempotency_key
        return self._post("/v1/setWaitEdge", body)

    def clear_wait_edge(self, owner_id, *, idempotency_key=""):
        body = {"ownerId": owner_id}
        if idempotency_key:
            body["idempotencyKey"] = idempotency_key
        return self._post("/v1/clearWaitEdge", body)

    def detect_cycle(self, start_owner_id, max_depth=0):
        return self._post("/v1/detectCycle", {
            "startOwnerId": start_owner_id,
            "maxDepth": max_depth,
        })

    def force_release(self, victim_id, *, idempotency_key=""):
        body = {"victimId": victim_id}
        if idempotency_key:
            body["idempotencyKey"] = idempotency_key
        return self._post("/v1/forceRelease", body)

    def request_revoke(self, owner_id):
        return self._post("/v1/requestRevoke", {"ownerId": owner_id})

    def stream_events(self, owner_id, *, after=None, last_event_id=None):
        """Yield event dicts from /v1/events/sse.

        Each event: {"id": int, "type": "grant"|"revoke"|"killed",
        "ownerId": str}. Blocks until events arrive; run in a thread.

        By default `after=0` so the stream replays from the start of the
        retained event ring — this ensures a GRANT that fires between the
        QUEUED response and the SSE connection opening is not missed.
        Pass `after=<last_id>` to stream only future events.
        """
        if after is None:
            after = 0
        path = f"/v1/events/sse?owner_id={urllib.parse.quote(owner_id)}"
        if after is not None:
            path += f"&after={after}"
        headers = {}
        if last_event_id is not None:
            headers["Last-Event-ID"] = str(last_event_id)

        conn = self._stream_conn_factory()
        try:
            conn.request("GET", path, headers=headers)
            resp = conn.getresponse()
            if resp.status != 200:
                raise PathlockdError(
                    "HTTP", resp.read().decode(), resp.status)
            event_data = None
            while True:
                raw = resp.readline()
                if not raw:
                    break
                line = raw.decode("utf-8").rstrip("\r\n")
                if line == "":
                    if event_data is not None:
                        yield json.loads(event_data)
                    event_data = None
                    continue
                if line.startswith("id:"):
                    pass
                elif line.startswith("data:"):
                    event_data = line[5:].strip()
                elif line.startswith(":"):
                    pass
        finally:
            conn.close()
