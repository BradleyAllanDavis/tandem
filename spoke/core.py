"""tandem spoke core — the sync tick, backend-agnostic.

ONE core, TWO deployments (DESIGN.md amendment, 2026-07-11):

- The hub's in-process gateway worker (Bradley's side): reader = the
  Syncthing-synced Things DB mirror on the hub host, writer = the
  things-gateway queue (HTTP), hub client = direct ledger calls.
- Jill's MacBook Air LaunchAgent: reader = her local things-mirror
  snapshot, writer = local `things:///json` opens, hub client = HTTP.

The tick (DESIGN.md §3.2), serialized, at-least-once end to end:

  1. outbound  — todos carrying a configured trigger tag, not known-sent
                 (local cache; miss ⇒ re-push, the hub dedupes) → push.
                 Refusals (repeating todo, >100 checklist items) are
                 logged loudly and left tagged — visible, never silent.
  2. inbound   — leased deliveries, applied ONE AT A TIME (makes
                 read-after-write correlation unambiguous):
                 create → journal intent → write → correlate uuid →
                 ack{dst_uuid}; terminal → write completed/canceled →
                 verify → ack.
  3. observe   — watched uuids whose local copy hit a terminal state →
                 report (completed / canceled / trashed→canceled).
  4. retag     — sender-side: transfers that reached `applied` swap the
                 trigger tag for the delegated tag AND mark the sender's own
                 copy completed (2026-07-11 D2 amendment: delegating IS the
                 action; the sender's copy is done the moment the handoff is
                 confirmed, not when the recipient eventually finishes it).
                 Symmetric on both directions ("either side").

Crash safety: a tiny local SQLite journal records create-apply intent
BEFORE firing the write. On restart with an open journal entry the spoke
re-runs correlation first and re-acks instead of re-firing — the residual
double-failure window (journal loss + lost ack) degrades to one visible
duplicate in the recipient's Inbox, recoverable by a human, never silent.

Python 3.9-compatible (Jill's Air runs Apple's /usr/bin/python3), stdlib only.
"""

from __future__ import annotations

import json
import os
import sqlite3
import sys
import threading
import time

DELEGATED_TAG = "👉 delegated"
ENVELOPE_SCHEMA = "tandem.todo/1"
MAX_CHECKLIST = 100
MAX_NOTES = 10000
# small clock-skew allowance for correlate windows: mirror timestamps come
# from another machine.
CLOCK_SKEW_MARGIN = 120

_STATE_SCHEMA = """
CREATE TABLE IF NOT EXISTS sent_cache (
  src_uuid   TEXT NOT NULL,
  rev        INTEGER NOT NULL,
  transfer_id TEXT,
  pushed_at  REAL NOT NULL,
  PRIMARY KEY (src_uuid, rev)
);
CREATE TABLE IF NOT EXISTS journal (
  delivery_id TEXT PRIMARY KEY,
  transfer_id TEXT,
  title       TEXT,
  provenance_tag TEXT,
  fired_at    REAL NOT NULL,
  dst_uuid    TEXT
);
CREATE TABLE IF NOT EXISTS observed (
  transfer_id TEXT PRIMARY KEY,
  state       TEXT NOT NULL,
  observed_at REAL NOT NULL
);
"""


def _log(msg: str) -> None:
    print(f"[tandem-spoke] {msg}", file=sys.stderr, flush=True)


class SpokeState:
    """Machine-local runtime state only — everything durable lives in the
    hub ledger or in Things itself.

    One sqlite connection, opened check_same_thread=False. The gateway's
    two-loop driver (inbound + local, §6) touches this from two threads, so
    every accessor is guarded by a private RLock — the ops are tiny and the
    tables near-disjoint, so contention is nil; single-loop spokes (Jill)
    take an uncontended lock and pay nothing. This is the ONLY concurrency
    added to the state layer; the sync-core methods themselves are unchanged."""

    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.conn.executescript(_STATE_SCHEMA)
        self.conn.commit()

    def known_sent(self, src_uuid: str, rev: int = 1) -> bool:
        with self._lock:
            return self.conn.execute(
                "SELECT 1 FROM sent_cache WHERE src_uuid=? AND rev=?",
                (src_uuid, rev)).fetchone() is not None

    def mark_sent(self, src_uuid: str, rev: int, transfer_id: str) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO sent_cache (src_uuid, rev, transfer_id,"
                " pushed_at) VALUES (?,?,?,?)",
                (src_uuid, rev, transfer_id, time.time()))

    def journal_open(self, delivery_id: str):
        with self._lock:
            return self.conn.execute(
                "SELECT * FROM journal WHERE delivery_id=?", (delivery_id,)).fetchone()

    def journal_intent(self, delivery_id: str, transfer_id: str, title: str,
                       provenance_tag: str) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO journal (delivery_id, transfer_id, title,"
                " provenance_tag, fired_at) VALUES (?,?,?,?,?)",
                (delivery_id, transfer_id, title, provenance_tag, time.time()))

    def journal_close(self, delivery_id: str, dst_uuid: str) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE journal SET dst_uuid=? WHERE delivery_id=?",
                (dst_uuid, delivery_id))

    def journaled_dst_uuids(self) -> set:
        """Every dst_uuid this spoke has already correlated — the exclusion
        set for read-after-write correlation. A method (not a raw conn read
        in core) so the shared connection is only ever touched under lock."""
        with self._lock:
            return set(row[0] for row in self.conn.execute(
                "SELECT dst_uuid FROM journal WHERE dst_uuid IS NOT NULL"))

    def last_observed(self, transfer_id: str):
        with self._lock:
            row = self.conn.execute(
                "SELECT state FROM observed WHERE transfer_id=?", (transfer_id,)).fetchone()
            return row[0] if row else None

    def mark_observed(self, transfer_id: str, state: str) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "INSERT OR REPLACE INTO observed (transfer_id, state, observed_at)"
                " VALUES (?,?,?)", (transfer_id, state, time.time()))


class SpokeCore:
    """The tick. All Things access via `reader`/`writer`, all hub access
    via `hub` — interfaces documented in things_db.py / writer_*.py /
    hub client modules; tests substitute in-memory fakes.

    reader:
      outbound_candidates(trigger_tags) -> [snapshot dict]
      status(uuid) -> {"status": open|completed|canceled, "trashed": bool} | None
      correlate(title, created_after, provenance_tag, exclude_uuids) -> uuid|None
      tags_of(uuid) -> [tag titles]
      refresh() -> None (freshen the mirror if the backend can)
    writer:
      create(envelope, provenance_tag, extra) -> None (fire-and-verify inside)
      set_terminal(uuid, state) -> bool          (True = verified/applied)
      set_tags(uuid, tags) -> bool
      set_tags_and_terminal(uuid, tags, state) -> bool
        (single combined write+verify -- HALF the round-trip latency of
        calling set_tags then set_terminal separately, which is what the
        sender-completes-at-send path does every time; see
        _retag_sender_copy, 2026-07-11)
    hub:
      push_transfer(to, src_uuid, payload, rev) -> {id, deduped, terminal, ...}
      deliveries(limit) -> [delivery]
      ack(delivery_id, dst_uuid=None) / nack(delivery_id, error)
      watch() -> [{transfer_id, uuid, role, state, retagged}]
      observe(transfer_id, state) -> dict
      mark_retagged(transfer_id) -> dict  (hub-durable D2 sender-retag record, N2)
    """

    def __init__(self, reader, writer, hub, state: SpokeState,
                 trigger_tags: dict, correlate_timeout: float = 240.0,
                 correlate_interval: float = 5.0):
        self.reader = reader
        self.writer = writer
        self.hub = hub
        self.state = state
        # {recipient_handle: [exact tag titles]} — EXACT titles, matched
        # case-insensitively but never by substring (a pre-existing
        # `Jillian 👩🏻‍🦰` tag must not trigger delegation — 2026-07-11
        # review amendment).
        self.trigger_tags = {
            handle: [t.lower() for t in tags]
            for handle, tags in trigger_tags.items()
        }
        self.correlate_timeout = correlate_timeout
        self.correlate_interval = correlate_interval

    # -- tick ------------------------------------------------------------
    def tick(self) -> None:
        """Serialized full tick — one pass over all four phases in order.
        Kept intact as the composition the invariant suite drives; the
        deployed drivers now call the two split phases below instead, so a
        held inbound long-poll can't starve outbound/observe (§5)."""
        self.reader.refresh()
        self._outbound()
        self._inbound()
        self._observe_and_retag()

    def tick_local(self) -> None:
        """Local-mirror-driven phases: detect newly-tagged outbound todos and
        observe/retag watched copies. Gated by local freshness, never by a hub
        round trip — driven off a timer or the mirror's mtime, not the inbound
        poll. Byte-identical phase logic to tick()."""
        self.reader.refresh()
        self._outbound()
        self._observe_and_retag()

    def tick_inbound(self) -> None:
        """Hub-driven phase: lease + apply deliveries. This is the leg that
        blocks on the held long-poll (HTTP spoke) or the delivery CV (gateway),
        decoupled from tick_local so a long wait never starves the local work."""
        self._inbound()

    # -- 1. outbound -------------------------------------------------------
    def _outbound(self) -> None:
        all_triggers = {t: h for h, tags in self.trigger_tags.items() for t in tags}
        if not all_triggers:
            return
        for cand in self.reader.outbound_candidates(sorted(all_triggers)):
            src_uuid = cand["uuid"]
            if self.state.known_sent(src_uuid):
                continue
            matched = [t for t in cand.get("tags", []) if t.lower() in all_triggers]
            if not matched:
                continue
            to_handle = all_triggers[matched[0].lower()]
            refusal = self._refusal(cand)
            if refusal:
                _log(f"REFUSED outbound {src_uuid} ({cand.get('title', '')!r}): "
                     f"{refusal} — item left tagged and untouched")
                continue
            payload = self._envelope(cand, strip_tags=matched)
            try:
                rec = self.hub.push_transfer(to_handle, src_uuid, payload, rev=1)
            except Exception as exc:  # hub down: Things holds outbound intent
                _log(f"push failed for {src_uuid}: {exc}; will retry next tick")
                continue
            if rec.get("deduped") and rec.get("terminal"):
                _log(f"{src_uuid} was already delegated once and resolved "
                     f"({rec['terminal']}); re-delegation of the same todo is a "
                     f"v2 seam (rev>1) — ignoring. Duplicate the todo to re-send.")
            self.state.mark_sent(src_uuid, 1, rec.get("id"))

    @staticmethod
    def _refusal(cand: dict) -> str:
        if cand.get("is_repeating"):
            return "repeating todos can't be completed remotely (URL-scheme limit)"
        if len(cand.get("checklist", [])) > MAX_CHECKLIST:
            return f"checklist exceeds {MAX_CHECKLIST} items (URL-scheme limit)"
        if cand.get("type", 0) != 0:
            return "projects/headings are not syncable in v1 (todos only)"
        return ""

    def _envelope(self, cand: dict, strip_tags) -> dict:
        # ALL tags are dropped in v1 — payload tags because the recipient's
        # vocabulary is theirs (the URL scheme would silently ignore unknown
        # tags anyway), control tags (strip_tags, the delegated tag) because
        # they must never cross the wire even as metadata.
        del strip_tags
        notes = cand.get("notes") or ""
        return {
            "schema": ENVELOPE_SCHEMA,
            "title": cand.get("title") or "",
            "notes": notes[:MAX_NOTES],
            "checklist": [c for c in cand.get("checklist", [])][:MAX_CHECKLIST],
            "when": cand.get("when"),          # None → recipient's real Inbox (D3)
            "deadline": cand.get("deadline"),  # date-only or None
            "context_url": None,               # reserved seam, null in v1
        }

    # -- 2. inbound ---------------------------------------------------------
    def _inbound(self) -> None:
        try:
            deliveries = self.hub.deliveries(limit=10)
        except Exception as exc:
            _log(f"delivery poll failed: {exc}")
            return
        for d in deliveries:
            try:
                if d["kind"] == "create":
                    self._apply_create(d)
                else:
                    self._apply_terminal(d)
            except Exception as exc:  # noqa: BLE001 — nack and move on
                _log(f"apply failed for delivery {d.get('id')}: {exc}")
                try:
                    self.hub.nack(d["id"], str(exc))
                except Exception:
                    pass  # lease expiry will requeue

    def _apply_create(self, d: dict) -> None:
        payload = d["payload"]
        provenance = d["provenance_tag"]
        journal = self.state.journal_open(d["id"])
        t0 = time.time()
        if journal is None:
            # Pre-flight: the LOCAL journal is our primary no-refire guard,
            # but it's local-machine state — lost on a reinstall/data wipe,
            # or simply never written if a prior process died between the
            # hub granting this lease and journal_intent() committing. The
            # hub's delivery.created_at is a hub-durable lower bound on when
            # this could first have fired, independent of any local state:
            # a one-shot correlate against it catches "this actually landed
            # already" before we'd otherwise re-fire (2026-08-20, closing
            # the residual dup-create gap the crash journal alone doesn't
            # cover — see DESIGN.md §3.3 and the correlation-timeout
            # incident this was written for).
            preflight_after = d.get("created_at", t0)
            found = self._correlate_once(payload["title"], preflight_after, provenance)
            if found is not None:
                self.state.journal_intent(d["id"], d["transfer_id"],
                                          payload["title"], provenance)
                self.state.journal_close(d["id"], found)
                self.hub.ack(d["id"], dst_uuid=found)
                _log(f"pre-flight correlate found an existing copy for delivery "
                     f"{d['id']} -> {found}; not re-firing")
                return
            # journal intent BEFORE firing — crash-restart re-correlates
            # instead of re-firing (no-dup).
            self.state.journal_intent(d["id"], d["transfer_id"],
                                      payload["title"], provenance)
            # idem_key = delivery id: if the journal is lost and the create
            # re-fires, an idempotency-aware writer (the queue) still
            # collapses it to one todo.
            self.writer.create(payload, provenance, idem_key=d["id"])
        else:
            t0 = journal["fired_at"]
            if journal["dst_uuid"]:  # lost-ack redelivery: just re-ack
                self.hub.ack(d["id"], dst_uuid=journal["dst_uuid"])
                return
            _log(f"open journal entry for delivery {d['id']} — re-correlating,"
                 " not re-firing")
        dst_uuid = self._correlate(payload["title"], t0, provenance)
        if dst_uuid is None:
            raise RuntimeError(
                f"correlation timeout: created todo {payload['title']!r} not"
                f" found in local DB within {self.correlate_timeout}s")
        self.state.journal_close(d["id"], dst_uuid)
        self.hub.ack(d["id"], dst_uuid=dst_uuid)
        _log(f"applied create {d['transfer_id']} -> {dst_uuid}")

    def _correlate_once(self, title: str, created_after: float, provenance_tag: str):
        """Single unblocking correlate probe — no polling loop. Excludes
        uuids this spoke has already journaled elsewhere, same as
        `_correlate`: this delivery has no journal entry of its own yet
        (that's the only time this is called), but a title collision with a
        DIFFERENT, already-fully-applied delivery is otherwise possible —
        without this exclusion a pre-flight hit could wrongly adopt a uuid
        some other delivery already owns. Used only for the pre-flight
        already-applied check."""
        self.reader.refresh()
        return self.reader.correlate(
            title, created_after - CLOCK_SKEW_MARGIN, provenance_tag,
            exclude_uuids=self.state.journaled_dst_uuids())

    def _correlate(self, title: str, created_after: float, provenance_tag: str):
        """Read-after-write uuid discovery, bounded. Excludes uuids the hub
        already knows (the reader gets a fresh exclusion set per call via
        its own ledger/journal knowledge is NOT assumed — exclusion here is
        only what this spoke journaled, the natural-key window does the rest)."""
        exclude = self.state.journaled_dst_uuids()
        deadline = time.time() + self.correlate_timeout
        window_start = created_after - CLOCK_SKEW_MARGIN
        while True:
            self.reader.refresh()
            found = self.reader.correlate(title, window_start, provenance_tag, exclude)
            if found:
                return found
            if time.time() >= deadline:
                return None
            time.sleep(self.correlate_interval)

    def _apply_terminal(self, d: dict) -> None:
        """Terminal echo deliveries address EITHER side now (`d["to_role"]`,
        added alongside N2's pending_retag window -- see
        _observe_and_retag): most commonly the sender (D2's own
        auto-complete already handles the fast path — see
        _retag_sender_copy; this is the race-safety-net path for when the
        recipient resolves their copy before the sender's own next tick
        gets to retag). For the sender, D2 is unconditional — the sender's
        copy is ALWAYS "completed", never "canceled", regardless of what
        the recipient did with theirs or which delivery kind carried the
        echo. Idempotent if _retag_sender_copy already got there first.

        But a sender can ALSO cancel/trash their own copy for real in the
        window between apply and retag confirming (_observe_and_retag's
        pending_retag case) — that genuine cancel queues an echo to the
        RECIPIENT, and unlike the sender-side case, D2 does NOT apply
        there: the recipient's copy must reflect the LITERAL kind (a
        `cancel` echo lands as canceled, not force-completed), or the
        recipient's real task silently gets marked done for no reason.
        `to_role` missing (older hub/spoke version mismatch) falls back to
        the pre-existing sender-side behavior, which was always safe."""
        if d.get("to_role") == "recipient":
            state = "completed" if d["kind"] == "complete" else "canceled"
        else:
            state = "completed"  # sender-side (or unknown/legacy): D2 is unconditional
        ok = self.writer.set_terminal(d["uuid"], state)
        if not ok:
            raise RuntimeError(f"terminal apply not verified for {d['uuid']}")
        self.hub.ack(d["id"])
        _log(f"applied {state} on {d.get('to_role', 'sender')} copy {d['uuid']}"
             f" (delivery kind={d['kind']})")

    # -- 3+4. observations + sender retag -----------------------------------
    def _observe_and_retag(self) -> None:
        try:
            watch = self.hub.watch()
        except Exception as exc:
            _log(f"watch poll failed: {exc}")
            return
        for w in watch:
            if w["role"] == "sender" and w.get("retagged"):
                # Hub-durable (Ledger.mark_retagged(), N2): our own D2
                # auto-complete of this transfer's sender copy already
                # happened — on THIS spoke instance or any other, which is
                # the whole point (a spoke-local flag couldn't survive a
                # reinstall or a second observer coming up with empty
                # state; see things-agent-interaction-model.md §2.6). That
                # "completed" status is our doing, not a fresh signal —
                # reporting it via observe() would wrongly echo a
                # completion to the recipient before they've actually done
                # anything.
                continue
            # Applied but not yet hub-confirmed retagged: either the first
            # D2 attempt, or a prior attempt's local write succeeded while
            # its hub.mark_retagged() call failed (network blip, hub
            # restart) and is due a retry.
            pending_retag = w["role"] == "sender" and w["state"] == "applied"
            status = self.reader.status(w["uuid"])
            if status is None:
                continue  # not visible (mirror lag) — try next tick
            terminal = None
            if status.get("trashed"):
                terminal = "canceled"
            elif status.get("status") == "completed":
                terminal = "completed"
            elif status.get("status") == "canceled":
                terminal = "canceled"
            # In the pending_retag window, a LOCAL "completed" status is
            # ambiguous: it may be OUR OWN D2 write whose hub confirmation
            # is still outstanding (see _retag_sender_copy) rather than a
            # fresh signal — reporting it would reopen the exact
            # two-observer defect this whole change exists to close,
            # self-inflicted by a transient hub outage instead of a
            # topology change. "canceled" is NOT ambiguous even in this
            # window: D2 never produces it, so it's always a genuine user
            # action (canceling/trashing their own copy before we got to
            # it) and must be reported normally, exactly as it always was
            # before this window existed — a stray cancel only became inert
            # AFTER the retag confirmed (DESIGN.md §3.2), never before.
            if pending_retag and terminal == "completed":
                terminal = None
            if terminal and self.state.last_observed(w["transfer_id"]) != terminal:
                try:
                    self.hub.observe(w["transfer_id"], terminal)
                    self.state.mark_observed(w["transfer_id"], terminal)
                    _log(f"observed {terminal} on {w['role']} copy {w['uuid']}")
                except Exception as exc:
                    _log(f"observation failed for {w['transfer_id']}: {exc}")
                continue
            if pending_retag:
                self._retag_sender_copy(w)

    def _retag_sender_copy(self, w: dict) -> None:
        """Swap the trigger tag for the delegated tag AND mark the sender's
        own copy completed, once delivery is CONFIRMED (never before — the
        tag is the outbound retry queue). D2 (2026-07-11): delegating IS the
        action from the sender's side — their copy is done as soon as the
        handoff is confirmed, symmetric on both directions ("either side"),
        regardless of what the recipient later does with theirs.

        Idempotent and safe to call repeatedly: the local write reapplies
        the same tags/terminal state, and Ledger.mark_retagged() is a
        set-once hub call — this is exactly what happens on retry, whether
        the local write or the hub mark was what failed last time."""
        current = self.reader.tags_of(w["uuid"])
        if current is None:
            return
        all_triggers = set(t for tags in self.trigger_tags.values() for t in tags)
        new_tags = [t for t in current if t.lower() not in all_triggers]
        if DELEGATED_TAG not in new_tags:
            new_tags.append(DELEGATED_TAG)
        # One combined write+verify, not two sequential ones -- each
        # set_*() round-trips through the full write-then-verify path
        # (queue+applier for the gateway worker, URL-scheme+mirror-poll for
        # a real Mac spoke), so doing tags then terminal separately doubled
        # this step's latency (measured live: ~25s instead of ~13s).
        if not self.writer.set_tags_and_terminal(w["uuid"], new_tags, "completed"):
            _log(f"retag+auto-complete not yet verified for {w['uuid']}; will retry next tick")
            return
        try:
            self.hub.mark_retagged(w["transfer_id"])
        except Exception as exc:
            _log(f"mark_retagged failed for {w['transfer_id']}: {exc}; local write "
                 f"already done, will retry marking the hub next tick")
            return
        _log(f"retagged + auto-completed sender copy {w['uuid']} -> {DELEGATED_TAG} (D2)")
