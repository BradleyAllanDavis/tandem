# Tandem — design (v1)

*Derived from the 2026-07-03 design checkpoint; deployment topology amended
2026-07-11 after review against live infrastructure. This document is the
behavioral contract; PROTOCOL.md is the wire spec.*

## 1. Product framing

Cultured Code offers no way to sync tasks between separate Things accounts.
Tandem fills that gap: delegate a todo to another person by tagging it;
it appears natively in their Things; when they finish it, yours resolves.
Long-term vision (NOT v1): "Tandem Family / Team" — multi-tenant,
cloud-hosted, inviteable. v1 is two members over a home LAN.

## 2. The physics (verified constraints)

Verified against the official URL-scheme docs
(culturedcode.com/things/support/articles/2803573/):

- **Writes**: `things:///add`, `/update`, `/add-project`, and `/json` (batch)
  only. Updates require the per-account auth token (Things → Settings →
  Enable Things URLs).
- **Write targeting**: URL scheme + AppleScript hit only the *current GUI
  user's* Things on *that Mac*. Separate iCloud/Things accounts on separate
  Macs ⇒ **the writer must run Mac-locally, per member. No way around it.**
- **Reads**: local SQLite, read-only. Direct SQLite *writes* are a
  non-starter (out-of-band mutations fork Things Cloud state).
- `add` does not return the new item's ID headless ⇒ ID discovery is
  read-after-write against the local DB.
- Envelope-shaping limits: notes ≤ 10,000 chars; checklist ≤ 100 items (no
  per-item checked flag on create); **unknown tags are silently ignored,
  never created**; unresolved `list` falls back to Inbox; ≤ 250 items per
  10s; **repeating todos can't be completed/updated via the URL scheme**.

## 3. Architecture

Hub-and-spoke (p2p rejected — an id-mapping ledger needs one durable home).

**Hub** = the only stateful coordinator: membership, the id-mapping ledger,
a durable delivery queue. It never touches Things. Relocatable by
construction: stdlib HTTP + SQLite in a configurable state dir, all client
contact through the HTTP API, zero host-specific paths in code.

**Spoke** = thin, near-stateless agent per member: reads its own Things DB
(RO, via snapshot mirror), pushes outbound-tagged snapshots, polls for
inbound deliveries, applies them via the URL scheme, echoes terminal states.
Stdlib-only Python so Apple's `/usr/bin/python3` suffices on a
non-developer's Mac, and macOS disk-access grants stay on stable binaries.

### 3.1 Deployment topology (amended 2026-07-11)

The original design gave every member a spoke process. In the deployed v1,
one member's infrastructure already runs an always-on **write gateway** for
Things (a durable op queue on the hub host + an applier LaunchAgent on an
always-on Mac + a Syncthing-synced read mirror). For that member the hub
runs the spoke core **in-process**:

- outbound detection: read the synced Things DB mirror on the hub host
- inbound writes: enqueue ops into the write gateway; its applier is the Mac hand
- completion echoes: same two paths

Only members without such a gateway get a real LaunchAgent spoke. Same
`SpokeCore`, different reader/writer/transport backends — the sync semantics
cannot diverge because the code is shared.

### 3.2 The sync flow (A delegates to B)

1. A tags a todo with B's trigger tag. **Trigger tags match exact,
   configured titles (case-insensitive), never substrings** — a
   pre-existing decorative tag containing a member's name must not trigger
   delegation.
2. A's spoke sees it, snapshots full fidelity, pushes the transfer
   (idempotent). The tag stays on; A's copy is untouched. Hub commits the
   transfer + queues a `create` delivery for B.
3. B's spoke leases the delivery, journals intent locally, applies the
   create (title, notes, checklist, when-if-set, deadline, provenance tag
   `from-a`, no list ⇒ **real Inbox**), correlates the new row in its DB
   (title + creation window + provenance tag), acks with the discovered
   UUID. The ledger now holds src_uuid ↔ dst_uuid.
4. A's spoke sees the transfer reach `applied` via its watchlist and retags
   A's copy — trigger tag → `👉 delegated` — **and marks A's own copy
   completed** (D2, amended 2026-07-11: delegating IS the action; A's part
   is done the moment the handoff is confirmed, not whenever B eventually
   finishes the task). Symmetric on both directions ("either side").
5. B completes it (whenever they get to it — could be minutes or days
   later). B's spoke observes the terminal status on a watched UUID,
   reports it; the hub records the terminal state (set-once) and queues a
   `complete`/`cancel` echo delivery for A.
6. A's spoke applies the echo. This is a race-safety-net path, not the
   normal path — by the time B acts, A's copy is already completed from
   step 4. It only does real work if B resolves faster than A's own next
   tick (the transfer's terminal gets set before A ever retags, so step 4
   never fires for it); even then the echo unconditionally lands on
   **completed**, never `canceled` — D2 has no downgrade path.

Reverse direction is symmetric. There is no "sender revocation" anymore —
the old behavior (canceling your own still-open delegated copy to pull it
back before the recipient acts) has no window under D2, since the sender's
copy is completed within one tick of confirmed delivery, well before a human
could act on it. A stray cancel on an already-completed sender copy is
inert: the retagged watch entry is permanently skipped (never re-observed),
so it can't cascade to the recipient's copy.

### 3.3 Spoke tick (serialized, on a configurable interval — 5s as deployed
2026-07-11, was 60s)

```
tick():
  refresh local mirror
  outbound:  for each todo carrying a trigger tag, not known-sent
             (local cache; miss ⇒ re-push, hub dedupes):
               refuse loudly (repeating / >100 checklist / non-todo)
               or push snapshot
  inbound:   for each leased delivery, ONE AT A TIME:
               create  → journal intent → apply → correlate → ack{dst_uuid}
               terminal→ apply completed/canceled → verify → ack
  observe:   watched uuids at terminal state locally → report
             (completed / canceled / trashed→canceled) — skipped for a
             sender-role uuid once already retagged (that "completed" is
             our own doing, not a fresh signal to relay)
  retag:     sender-side transfers that reached `applied` (once) →
             trigger tag → 👉 delegated AND mark completed (D2)
```

Crash safety: the journal (a tiny local SQLite) records create-apply intent
*before* firing the write. On restart with an open journal entry, the spoke
re-runs correlation and re-acks instead of re-firing. The residual
double-failure window (journal loss + lost ack) degrades to one visible
duplicate in the recipient's Inbox — recoverable by a human, never silent.

**Pre-flight correlate (2026-08-20).** The journal above is local-machine
state — lost on a reinstall/data wipe, or simply never written if the spoke
died between the hub granting a lease and `journal_intent()` committing.
Closing that gap needs a check independent of local state: before firing a
create with no open journal entry, the spoke does one unblocking
`correlate()` probe against the *hub's* `delivery.created_at` (durable,
not local) as the window floor. A hit means a prior attempt already landed
this exact create — adopt its uuid and ack, never re-fire. A miss proceeds
to fire normally. See `spoke/core.py::_apply_create`.

**Retry policy (2026-08-20).** A create can still time out correlating
against a title that genuinely never matches (see PROTOCOL.md's retry-policy
note) — without a ceiling this retried unboundedly (the incident this was
written for: 1300+ attempts over a month on one stuck delivery). Each
delivery now gets at most `MAX_ATTEMPTS` leases, backing off exponentially
between them; past the ceiling it's retired to `dead_letter` — bounded,
and visible via `/v1/health` instead of silent.

Hub unreachable: **no spoke-side outbound queue.** Things itself is the
outbound queue (the tag stays until the hub acked the transfer), and the hub
is the inbound queue. The spoke backs off and retries.

## 4. Ledger schema

See `hub/ledger.py` — tenants / members (capability tiers: can_send,
can_receive, can_admin) / devices (sha256 token hashes, plaintext shown once)
/ transfers (src_uuid ↔ dst_uuid, rev seam, terminal, payload envelope) /
deliveries (create|complete|cancel, queued→leased→done, lease expiry) /
events (append-only audit).

Idempotency anchors: `UNIQUE(tenant_id, from_member, src_uuid, rev)` on
transfers; `UNIQUE(transfer_id, kind, to_member)` on deliveries.

## 5. Envelope (fidelity contract)

Schema-versioned (`tandem.todo/1`). Title and notes arrive
byte-identical — **no sync markers are ever injected into content** (the
ledger's uuid mapping makes items self-identifying). Checklist items arrive
unchecked (platform limit). `when` carries only *explicit* scheduling — a
scheduled date, or `someday`; the default resting state (Anytime) maps to
nothing, so undated delegations land in the recipient's real Inbox. Deadline
is date-only. Tags: control tags (trigger tags, `👉 delegated`) never cross
the wire; payload tags are dropped in v1 (the recipient's vocabulary is
theirs; a per-tenant tag-mapping table is a v2 seam); the recipient side
gets exactly one provenance tag, `from-<sender-handle>`, **pre-created
during member bootstrap** (tags must exist to apply).

Refused loudly at snapshot (spoke log + item left tagged and untouched, so
it's visible): repeating todos, projects/areas/headings, >100 checklist
items.

## 6. Auth

`Authorization: Bearer <device-token>` resolves to
`Principal(tenant_id, member_id, device_id, caps)`; the sync core never sees
tokens. v1: hashed static device tokens, provisioned declaratively (the
deploy materializes token files from a secrets manager; the hub stores only
hashes; rotation = new file + restart). Tenant isolation is structural:
every table roots at tenant_id, every query passes through the principal's
tenant, and no API accepts a tenant parameter from a client.

## 6b. Push transport (delivery latency)

The system is asymmetric: of a spoke's three tick duties, only **inbound**
("the hub has a delivery for me") is a genuine server→client push problem —
outbound and observe read the sender's own local mirror and are gated by local
freshness, not a hub round trip. So there is exactly one async hub→client
event class, and long-poll expresses it completely; no WebSocket/SSE (they'd
add framing + proxy-idle-timeout keepalive for zero capability long-poll lacks
here, and long-poll survives Cloudflare Tunnel/Tailscale/proxying untouched at
`wait≤30s`). Full rationale: the push-transport design doc.

- **The delivery condition variable.** `Ledger` holds a `threading.Condition`
  with its own lock (never the data `RLock`) and a monotonic generation
  counter. Every code path that commits a *leasable* (`queued`) delivery —
  `push_transfer`, `ack_delivery`'s mid-flight-revoke echo, `observe`/
  `_queue_echo` — calls `_signal_delivery()` **after** the transaction commits.
  A waiter parked in `wait_for_delivery(timeout)` wakes on the generation
  change (or times out). Signal-after-commit is load-bearing: a waiter woken
  while the write is still uncommitted could re-query, miss the row, re-park,
  and reintroduce a latency floor.
- **One primitive, two consumers.** The HTTP `/v1/deliveries?wait=` handler
  parks on the CV between `lease_deliveries` re-checks (no 0.5s busy-poll);
  the in-process gateway worker's inbound loop calls `wait_for_delivery`
  directly. Same push for Jill (HTTP) and Bradley (in-process).
- **Phase decoupling.** A held inbound long-poll would starve outbound/observe
  in one serial tick, so `SpokeCore.tick()` is split into `tick_local()`
  (refresh + outbound + observe/retag) and `tick_inbound()` (lease + apply).
  LAN spokes run Option 1 (one loop: local, then a short held poll woken early
  by the CV — keeps the single-threaded, invariant-tested spoke). The gateway
  runs Option 2 (two loops: inbound parks on the CV; local is driven by the
  Syncthing-fed mirror's mtime with a `tick_seconds` floor) — its shared
  `SpokeState` is made thread-safe by a private `RLock` around every accessor.
- **Measured:** push-commit → held long-poll return is ~2ms over the real
  `ThreadingHTTPServer` (was a 0–500ms busy-poll floor plus the spoke's 60s
  tick). Per-phase sync logic is byte-identical, so the invariant suite still
  governs correctness — the concurrency is confined to the driver/transport
  layer and the isolated CV.

## 7. Quality bar

- Ledger tests: push idempotency under replay, lease expiry/re-queue,
  ack/nack, terminal set-once + completed-beats-canceled, revocation edge
  cases (before-apply skip, mid-flight), tenant isolation, auth.
- API tests: the real HTTP server exercised through the same client class a
  deployed spoke uses.
- Spoke tests: fakes behind the reader/writer/hub interfaces —
  exact-tag-match discipline, refusals, crash-then-rejournal,
  lost-ack-redelivery, full round trips both directions.
- Invariant simulation: real hub + two spokes + fake Things accounts under
  randomized chaos (writer crashes, lost responses, expired leases),
  asserting no-loss / no-dup / round-trip at quiescence across seeds.

All stdlib `unittest` — the zero-dependency rule covers the tests too.

## 8. Rejected alternatives

- **Git-repo-as-transport with LWW state merge** (predecessor system): the
  merge/reset/push-retry machinery was a third of the code and its likeliest
  failure surface. The HTTP hub + transactional ledger dissolves it.
- **Sync markers in notes**: identity lives in the ledger's uuid mapping,
  not inside items; notes stay byte-identical.
- **Full bidirectional field merge with conflict resolution**: v1 is
  delegation — single writer until terminal state — which dissolves the
  conflict problem instead of solving it. Edit propagation returns as
  `rev > 1` with the sender as sole editor.
- **Direct SQLite writes / patching the app binary**: fork cloud state /
  fight code signing, respectively. All the leverage is in the protocol.
- **Auto-created "Sync Inbox" project**: a permanent landing project is a
  queue graveyard. The real Inbox is GTD's native landing zone; provenance
  tags make arrivals filterable; the hub tracks by uuid so filing anywhere
  never breaks the completion echo.
