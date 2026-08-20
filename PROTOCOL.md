# Tandem — wire protocol (v1)

All endpoints under `/v1`, JSON bodies, `Authorization: Bearer <device-token>`
(exception: `GET /v1/health` is unauthenticated — liveness for probes/monitors).
Auth resolves to a Principal (tenant, member, device, capabilities); no
endpoint ever accepts a tenant or member identifier *for* the caller — who
you are always comes from auth.

## Envelope — `tandem.todo/1`

```json
{
  "schema":      "tandem.todo/1",
  "title":       "string, required",
  "notes":       "string, byte-identical, ≤ 10000",
  "checklist":   ["item title", "…"],
  "when":        "yyyy-mm-dd | someday | null",
  "deadline":    "yyyy-mm-dd | null",
  "context_url": null
}
```

`context_url` is a reserved seam (null in v1). No tags in the payload —
control tags never cross the wire, payload tags are dropped in v1, and the
provenance tag is computed by the hub (`from-<sender-handle>`, plus a
per-member emoji suffix where one's configured — `hub/ledger.py`'s
`provenance_tag()` / `_PROVENANCE_EMOJI`), not carried.

## Transfer state machine

```
created ──(create delivery acked with dst_uuid)──▶ applied
   │                                                  │
   │ (sender revokes before apply:                    │ (either side observes
   │  create skipped, resolved)                       ▼  terminal on their copy)
   └──────────────────────────────▶ terminal ∈ {completed, canceled}
                                        │ (echo delivery to the other
                                        ▼  party acked)
                                    resolved
```

Terminal is **set-once**, with one sanctioned upgrade: `canceled` →
`completed` (work done wins), permitted only while the cancel echo hasn't
been delivered. A `canceled` arriving after `completed` is ignored.

## Delivery state machine

```
queued ──(GET /v1/deliveries)──▶ leased ──(ack)──▶ done
  ▲                                 │
  └──(nack, or lease expiry)────────┘
  │
  └──(attempts reaches MAX_ATTEMPTS)──▶ dead_letter
```

Ordering guards enforced by the hub: a terminal delivery is never handed to
the recipient before the transfer has a `dst_uuid`; a terminal echo replaces
a still-queued create when the sender revoked first (the create is marked
done/skipped and the transfer resolves).

**Retry policy (2026-08-20).** Each grant of a lease (`GET /v1/deliveries`)
sets `next_attempt_at = now + backoff(attempts)` — an exponential backoff
floor (`BACKOFF_BASE_SECONDS * 2**(attempts-1)`, capped at
`BACKOFF_CAP_SECONDS`) — so a nacked or lease-expired delivery isn't
re-leasable until that floor elapses. Once `attempts` reaches `MAX_ATTEMPTS`
the delivery moves to the terminal `dead_letter` state — never leased
again, whether the caller explicitly nacks (`nack_delivery`) or simply
vanishes mid-lease (caught on the next `lease_deliveries` call, since
nothing else would notice a silent crash). `dead_letter` is intentionally
not `done`: it's a distinct, visible "gave up" state, counted separately in
`/v1/health` (below), not folded into ordinary completed work.

## Endpoints

### POST /v1/transfers — requires `can_send`
```json
{"to": "jill", "src_uuid": "THINGS-UUID", "rev": 1, "payload": {…envelope…}}
```
`201` with the transfer record, or `200` with `"deduped": true` on replay
(idempotent on `(from_member, src_uuid, rev)`). Errors: `403` (capability),
`404` (unknown recipient handle *in the caller's tenant*), `400`
(self-delegation, missing fields).

### GET /v1/deliveries?limit=N&wait=S — requires `can_receive`
Leases up to N deliveries for the calling member (long-polls up to S≤30s).
Lease TTL 300s; expiry re-queues. Each entry:
```json
{"id": "…", "transfer_id": "…", "kind": "create", "attempts": 1,
 "payload": {…envelope…}, "from": "bradley", "provenance_tag": "from-bradley 👨"}
```
or, for terminal kinds:
```json
{"id": "…", "transfer_id": "…", "kind": "complete|cancel", "attempts": 1,
 "uuid": "the caller's OWN copy's Things uuid", "to_role": "sender|recipient"}
```
`to_role` says which side of the transfer this echo addresses — the two
sides apply an echo differently: the sender's own copy is forced to
`completed` unconditionally regardless of `kind` (D2, "delegating IS the
action" — see DESIGN.md), but the recipient must apply the literal `kind`
(a `cancel` echo — e.g. the sender canceling their own copy after
apply — lands as `canceled`, never force-completed).

### POST /v1/deliveries/{id}/ack
Create kind: `{"dst_uuid": "…"}` (required — this is what closes the uuid
mapping). Terminal kinds: `{}`. Idempotent; re-acking a done delivery
returns `{"ok": true, "already_done": true}`.

### POST /v1/deliveries/{id}/nack
`{"error": "…"}` — re-queues immediately (client paces retries by tick).

### GET /v1/watch
Open (non-terminal, non-resolved) transfers the caller is party to:
```json
{"watch": [{"transfer_id": "…", "uuid": "their own copy's uuid",
            "role": "sender|recipient", "state": "created|applied",
            "retagged": false}]}
```
Senders use `state: "applied"` as the retag (delivery-receipt) signal.
`retagged: true` means the SENDER's own copy already went through D2
auto-complete (see `POST /v1/transfers/{id}/retagged` below) — recorded on
the hub, not locally, so it holds regardless of which spoke instance last
observed this transfer.

### POST /v1/observations
`{"transfer_id": "…", "state": "completed|canceled"}` — report a terminal
state seen on a watched copy (trashed reports as `canceled`). Set-once
semantics above; always returns the winning terminal.

### POST /v1/transfers/{id}/retagged
No body. Records that the CALLER's own copy of this transfer (caller must
be the sender) was auto-completed via D2 ("delegating IS the action" —
2026-07-11). Idempotent, set-once, hub-durable — any observer of this
transfer sees `retagged: true` in `GET /v1/watch` from its very first
tick, so a spoke reinstall or a second observer coming up with empty local
state can never re-report that local completion as a fresh terminal
observation (which would wrongly echo a completion to the recipient — see
things-agent-interaction-model.md §2.6 in the operator's dotfiles).
Deliberately does NOT set `terminal` — this is not a real completion of
the transfer, just a note about the sender's own copy. `404` if the caller
isn't this transfer's sender.

### GET /v1/health
`{"ok": true, "pending_deliveries": n, "dead_letter_deliveries": n, "open_transfers": n}`
— `pending_deliveries` excludes `dead_letter` rows (they're retired, not
still-actionable); `dead_letter_deliveries` is the loud signal a stuck
delivery gave up instead of retrying silently forever (2026-08-20).

### Admin (requires `can_admin`; scoped to the caller's tenant)
- `POST /v1/admin/tenants {"name"}` — bootstrap-only in practice
- `POST /v1/admin/members {"handle", "display_name", "can_send", "can_receive", "can_admin"}`
- `POST /v1/admin/devices {"member_id", "name"}` → `{"token"}` **shown once**
- `POST /v1/admin/devices/{id}/revoke`

Deployed provisioning is declarative (hub bootstrap spec + token files); the
admin API is the runtime escape hatch.

## Delivery semantics

At-least-once end to end. Every retry path is absorbed by an idempotency
anchor: transfer replays by the natural key, delivery queueing by
`(transfer, kind, member)`, applies by the spoke journal (re-correlate,
don't re-fire) plus a pre-flight correlate probe against the hub's durable
`delivery.created_at` when the local journal has no entry at all, acks by
done-state no-ops, observations by set-once terminal. The one residual
(spoke journal lost, the pre-flight probe also misses because the
correlate title genuinely never matches, AND the ack is lost — the same
window) degrades to a single visible duplicate on the recipient side. That
window is now also bounded: `MAX_ATTEMPTS` + backoff retire a stuck
delivery to `dead_letter` instead of retrying forever (retry-policy note,
above).
