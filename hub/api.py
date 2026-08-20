"""tandem hub HTTP API — the /v1 surface spokes talk to.

Stdlib ThreadingHTTPServer (deliberately no framework — matches the
things-gateway convention this deploys next to). All routes except
`GET /v1/health` (unauthenticated liveness) require
`Authorization: Bearer <device-token>`; auth resolves to a Principal
(tenant, member, device, caps) and the sync core never sees tokens.

Surface (PROTOCOL.md has the formal spec):
  POST /v1/transfers                    push a snapshot (idempotent)
  GET  /v1/deliveries?limit=&wait=      poll + lease (long-poll)
  POST /v1/deliveries/{id}/ack          create: {dst_uuid}; terminal: {}
  POST /v1/deliveries/{id}/nack         {error}
  GET  /v1/watch                        open transfers this member observes
  POST /v1/observations                 {transfer_id, state}
  POST /v1/transfers/{id}/retagged      sender-side D2 auto-complete, hub-durable
  GET  /v1/health                       liveness + queue depth
  POST /v1/admin/tenants|members|devices, /v1/admin/devices/{id}/revoke
"""

from __future__ import annotations

import json
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from .ledger import Ledger, LedgerError, AuthError, Forbidden, Principal

DEFAULT_LEASE_SECONDS = 300  # design: lease TTL 5m


class ApiHandler(BaseHTTPRequestHandler):
    ledger: Ledger = None  # set on class before serving
    lease_seconds: float = DEFAULT_LEASE_SECONDS

    def log_message(self, fmt, *args):
        print(f"[tandem-hub] {self.address_string()} - {fmt % args}",
              file=sys.stderr)

    # -- plumbing -----------------------------------------------------------
    def _send(self, code: int, payload: dict) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _principal(self) -> Principal:
        auth = self.headers.get("Authorization", "")
        if not auth.startswith("Bearer "):
            raise AuthError("missing bearer token")
        return self.ledger.authenticate(auth[len("Bearer "):].strip())

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length) if length else b""
        if not raw:
            return {}
        return json.loads(raw)

    def _handle(self, fn) -> None:
        try:
            fn()
        except AuthError as exc:
            self._send(401, {"error": str(exc)})
        except Forbidden as exc:
            self._send(403, {"error": str(exc)})
        except LedgerError as exc:
            self._send(exc.status, {"error": str(exc)})
        except json.JSONDecodeError:
            self._send(400, {"error": "invalid JSON body"})
        except Exception as exc:  # noqa: BLE001 — last-resort 500, never hang
            print(f"[tandem-hub] 500: {exc!r}", file=sys.stderr)
            self._send(500, {"error": "internal error"})

    # -- routes ----------------------------------------------------------------
    def do_GET(self):
        self._handle(self._get)

    def do_POST(self):
        self._handle(self._post)

    def _get(self):
        parsed = urlparse(self.path)
        if parsed.path == "/v1/health":
            # Unauthenticated by design: liveness/readiness probes (k8s) and
            # uptime checks can't carry a device token. Exposes only ok +
            # aggregate queue counts — no tenant/member/payload data.
            return self._send(200, self.ledger.health())
        p = self._principal()
        if parsed.path == "/v1/deliveries":
            qs = parse_qs(parsed.query)
            limit = min(int(qs.get("limit", ["10"])[0]), 50)
            wait = min(float(qs.get("wait", ["0"])[0]), 30.0)
            deadline = time.time() + wait
            deliveries = self.ledger.lease_deliveries(p, limit, self.lease_seconds)
            # Held long-poll: park on the ledger's delivery CV and re-check on
            # wake (a delivery committed) or timeout — no busy sleep, no per-
            # connection sqlite scan every 0.5s. Near-push once a row lands.
            while not deliveries and time.time() < deadline:
                self.ledger.wait_for_delivery(deadline - time.time())
                deliveries = self.ledger.lease_deliveries(p, limit, self.lease_seconds)
            return self._send(200, {"deliveries": deliveries})
        if parsed.path == "/v1/watch":
            return self._send(200, {"watch": self.ledger.watchlist(p)})
        return self._send(404, {"error": "not found"})

    def _post(self):
        p = self._principal()
        parsed = urlparse(self.path)
        body = self._body()

        if parsed.path == "/v1/transfers":
            rec = self.ledger.push_transfer(
                p,
                to_handle=body.get("to", ""),
                src_uuid=body.get("src_uuid", ""),
                payload=body.get("payload"),
                rev=int(body.get("rev", 1)),
            )
            return self._send(200 if rec.get("deduped") else 201, rec)

        if parsed.path.startswith("/v1/transfers/"):
            rest = parsed.path[len("/v1/transfers/"):]
            if rest.endswith("/retagged"):
                tid = rest[: -len("/retagged")]
                return self._send(200, self.ledger.mark_retagged(p, tid))
            return self._send(404, {"error": "not found"})

        if parsed.path.startswith("/v1/deliveries/"):
            rest = parsed.path[len("/v1/deliveries/"):]
            if rest.endswith("/ack"):
                did = rest[: -len("/ack")]
                return self._send(200, self.ledger.ack_delivery(
                    p, did, dst_uuid=body.get("dst_uuid")))
            if rest.endswith("/nack"):
                did = rest[: -len("/nack")]
                return self._send(200, self.ledger.nack_delivery(
                    p, did, error=body.get("error", "")))
            return self._send(404, {"error": "not found"})

        if parsed.path == "/v1/observations":
            return self._send(200, self.ledger.observe(
                p, body.get("transfer_id", ""), body.get("state", "")))

        if parsed.path.startswith("/v1/admin/"):
            if not p.can_admin:
                raise Forbidden("member lacks can_admin")
            rest = parsed.path[len("/v1/admin/"):]
            if rest == "tenants":
                return self._send(201, self.ledger.create_tenant(body.get("name", "")))
            if rest == "members":
                # admin creates members in their OWN tenant only — no
                # tenant parameter is accepted from any client, ever.
                return self._send(201, self.ledger.create_member(
                    p.tenant_id, body.get("handle", ""),
                    body.get("display_name", body.get("handle", "")),
                    can_send=bool(body.get("can_send", True)),
                    can_receive=bool(body.get("can_receive", True)),
                    can_admin=bool(body.get("can_admin", False))))
            if rest == "devices":
                with self.ledger.lock:
                    member = self.ledger.conn.execute(
                        "SELECT * FROM members WHERE id=? AND tenant_id=?",
                        (body.get("member_id", ""), p.tenant_id)).fetchone()
                if member is None:
                    raise LedgerError("member_id not found in your tenant")
                return self._send(201, self.ledger.create_device(
                    member["id"], body.get("name", "device")))
            if rest.startswith("devices/") and rest.endswith("/revoke"):
                did = rest[len("devices/"): -len("/revoke")]
                with self.ledger.lock:
                    row = self.ledger.conn.execute(
                        "SELECT d.id FROM devices d JOIN members m ON m.id=d.member_id"
                        " WHERE d.id=? AND m.tenant_id=?", (did, p.tenant_id)).fetchone()
                if row is None:
                    raise LedgerError("device not found in your tenant")
                self.ledger.revoke_device(did)
                return self._send(200, {"ok": True})

        return self._send(404, {"error": "not found"})


def make_server(ledger: Ledger, bind: str, port: int,
                lease_seconds: float = DEFAULT_LEASE_SECONDS) -> ThreadingHTTPServer:
    handler = type("BoundApiHandler", (ApiHandler,), {
        "ledger": ledger, "lease_seconds": lease_seconds})
    return ThreadingHTTPServer((bind, port), handler)
