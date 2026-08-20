"""SpokeCore tests over in-memory fakes: outbound scan discipline (exact
trigger match, refusals, sent-cache), crash-then-rejournal, lost-ack
redelivery, terminal apply, retag-only-after-applied."""

import os
import tempfile
import time
import unittest

from hub.direct import DirectHubClient
from hub.ledger import Ledger
from spoke.core import DELEGATED_TAG, SpokeCore, SpokeState
from tests.fakes import CrashAfterFire, FakeReader, FakeThings, FakeWriter, FlakyHub


class SpokeCoreTestBase(unittest.TestCase):
    """A real ledger + hub (Direct clients) with fake Things on both ends —
    bradley's spoke and jill's spoke share the hub, like production."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ledger = Ledger(os.path.join(self.tmp.name, "ledger.sqlite"),
                            backoff_base_seconds=0.001, backoff_cap_seconds=0.01)
        tenant = self.ledger.create_tenant("davis")["id"]
        b = self.ledger.create_member(tenant, "bradley", "B")
        j = self.ledger.create_member(tenant, "jill", "J")
        pb = self.ledger.principal_for_member(
            b["id"], self.ledger.ensure_gateway_device(b["id"]))
        pj = self.ledger.principal_for_member(
            j["id"], self.ledger.ensure_gateway_device(j["id"], "air"))

        self.b_things = FakeThings("b")
        self.j_things = FakeThings("j")
        for things in (self.b_things, self.j_things):
            things.tags.update({"jill", "bradley", DELEGATED_TAG,
                                "from-bradley 👨", "from-jill 👩🏻‍🦰"})

        self.b_hub = FlakyHub(DirectHubClient(self.ledger, pb, lease_seconds=0.2))
        self.j_hub = FlakyHub(DirectHubClient(self.ledger, pj, lease_seconds=0.2))
        self.b_writer = FakeWriter(self.b_things)
        self.j_writer = FakeWriter(self.j_things)
        self.b_spoke = self._spoke(self.b_things, self.b_writer, self.b_hub,
                                   {"jill": ["jill"]}, "b-state")
        self.j_spoke = self._spoke(self.j_things, self.j_writer, self.j_hub,
                                   {"bradley": ["bradley"]}, "j-state")

    def _spoke(self, things, writer, hub, triggers, name):
        return SpokeCore(
            reader=FakeReader(things), writer=writer, hub=hub,
            state=SpokeState(os.path.join(self.tmp.name, f"{name}.sqlite")),
            trigger_tags=triggers,
            correlate_timeout=1.0, correlate_interval=0.01)

    def tearDown(self):
        self.ledger.close()
        self.tmp.cleanup()

    def settle(self, rounds=4):
        import time
        for _ in range(rounds):
            time.sleep(0.25)  # let expired leases requeue between rounds
            self.b_spoke.tick()
            self.j_spoke.tick()


class TestOutbound(SpokeCoreTestBase):
    def test_exact_tag_match_only(self):
        # `Jillian 👩🏻‍🦰` must NOT trigger — the 2026-07-11 review amendment
        self.b_things.add("about jill", tags=["Jillian 👩🏻‍🦰"])
        tagged = self.b_things.add("for jill", tags=["jill"])
        self.settle(1)
        self.assertEqual(self.j_things.count_titled("for jill"), 1)
        self.assertEqual(self.j_things.count_titled("about jill"), 0)
        self.assertTrue(self.b_spoke.state.known_sent(tagged))

    def test_case_insensitive_exact_match(self):
        self.b_things.add("shout", tags=["JILL"])
        self.settle(1)
        self.assertEqual(self.j_things.count_titled("shout"), 1)

    def test_repeating_todo_refused_loudly_and_left_tagged(self):
        u = self.b_things.add("water plants", tags=["jill"], is_repeating=True)
        self.settle(1)
        self.assertEqual(self.j_things.count_titled("water plants"), 0)
        self.assertFalse(self.b_spoke.state.known_sent(u))
        self.assertIn("jill", self.b_things.todos[u]["tags"])  # visible, not silent

    def test_oversized_checklist_refused(self):
        self.b_things.add("mega", tags=["jill"], checklist=[str(i) for i in range(101)])
        self.settle(1)
        self.assertEqual(self.j_things.count_titled("mega"), 0)

    def test_sent_cache_suppresses_repush_but_hub_dedupes_anyway(self):
        self.b_things.add("once", tags=["jill"])
        self.settle(2)
        self.assertEqual(self.j_things.count_titled("once"), 1)

    def test_no_control_tags_cross_the_wire(self):
        self.b_things.add("clean", tags=["jill", "home 🏠"])
        self.settle(1)
        [todo] = [t for t in self.j_things.todos.values() if t["title"] == "clean"]
        # only the provenance tag arrives; trigger + payload tags dropped (v1)
        self.assertEqual(todo["tags"], ["from-bradley 👨"])


class TestInboundCrashSafety(SpokeCoreTestBase):
    def test_create_lands_in_inbox_with_provenance(self):
        self.b_things.add("errand", tags=["jill"], notes="details",
                          checklist=["x", "y"])
        self.settle(1)
        [todo] = [t for t in self.j_things.todos.values() if t["title"] == "errand"]
        self.assertEqual(todo["notes"], "details")
        self.assertEqual(todo["checklist"], ["x", "y"])
        self.assertIsNone(todo["when"])  # D3: real Inbox, no forced 'today'

    def test_crash_after_fire_rejournals_no_duplicate(self):
        self.b_things.add("fragile", tags=["jill"])
        self.b_spoke.tick()          # push
        self.j_writer.crash_next_create = True
        self.j_spoke.tick()          # fires, "crashes" before ack
        self.settle(3)               # restart-equivalent: re-correlate, re-ack
        self.assertEqual(self.j_things.count_titled("fragile"), 1)  # NOT 2
        self.assertEqual(self.j_writer.creates, 1)  # never re-fired

    def test_lost_ack_redelivery_reacks_same_uuid(self):
        self.b_things.add("flaky", tags=["jill"])
        self.b_spoke.tick()
        self.j_hub.drop_next.add("ack")  # ack succeeds hub-side, response lost
        self.j_spoke.tick()
        self.settle(3)
        self.assertEqual(self.j_things.count_titled("flaky"), 1)
        self.assertEqual(self.j_writer.creates, 1)

    def test_forced_correlation_timeout_retries_produce_exactly_one_todo(self):
        """Defect 1 regression (2026-08-08 incident): correlate() genuinely
        can't find the freshly-created todo for several full poll-cycles in
        a row (title-transformation mismatch / mirror lag, not a crash) —
        every retry must re-correlate against the SAME journal entry, never
        re-fire the create. Forces 4 full correlate_timeout cycles (the
        incident saw 4 failed attempts over 24 minutes) before correlation
        starts succeeding, then asserts exactly one todo exists throughout
        and after."""
        self.b_things.add("flaky title", tags=["jill"])
        self.b_spoke.tick()  # push the transfer
        self.j_spoke.reader.fail_correlate_times = 10**6  # "stuck" until cleared
        for _ in range(4):
            self.j_spoke.tick()   # attempt 0 fires; every attempt times out -> nack
            time.sleep(0.02)      # past the test's injected tiny backoff floor
            self.assertEqual(self.j_writer.creates, 1,
                             "the create must fire exactly once, ever")
            self.assertEqual(self.j_things.count_titled("flaky title"), 1,
                             "exactly one copy exists even while stuck")
        # correlation starts working (mirror caught up / mismatch resolved)
        self.j_spoke.reader.fail_correlate_times = 0
        self.settle(3)
        self.assertEqual(self.j_writer.creates, 1)
        self.assertEqual(self.j_things.count_titled("flaky title"), 1)
        [dst] = [u for u, t in self.j_things.todos.items()
                if t["title"] == "flaky title"]
        self.assertIn("from-bradley 👨", self.j_things.todos[dst]["tags"])
        # the transfer actually resolved (delivery acked, not stuck forever)
        self.assertEqual(self.ledger.watchlist(self.b_hub.inner.principal)[0]["state"],
                         "applied")

    def test_preflight_correlate_prevents_refire_after_journal_loss(self):
        """Defect 1's residual gap: the crash journal is LOCAL-machine
        state — lost on a reinstall/data wipe (the documented "residual
        double-failure window" in DESIGN.md/PROTOCOL.md). Simulates that:
        the create already fired and landed in Things on a prior attempt,
        but the delivery was never acked AND the journal that would have
        remembered firing it is gone (a brand-new SpokeState, as after a
        fresh install). The pre-flight correlate check must find the
        already-applied copy and adopt it instead of re-firing."""
        self.b_things.add("already there", tags=["jill"])
        self.b_spoke.tick()  # push transfer, create delivery queued
        # Simulate "a prior attempt already fired the create" directly —
        # the delivery is still queued/unacked at the hub the whole time.
        self.j_writer.create({"title": "already there", "notes": "",
                              "checklist": []}, "from-bradley 👨")
        self.assertEqual(self.j_writer.creates, 1)
        self.assertEqual(self.j_things.count_titled("already there"), 1)
        # Fresh journal (empty) standing in for a reinstalled spoke.
        fresh_state = SpokeState(os.path.join(self.tmp.name, "j-state-fresh.sqlite"))
        self.j_spoke.state = fresh_state
        self.j_spoke.tick()  # inbound: journal is empty -> pre-flight correlate
        self.assertEqual(self.j_writer.creates, 1,  # NOT re-fired
                         "pre-flight correlate must find the existing copy, not re-fire")
        self.assertEqual(self.j_things.count_titled("already there"), 1)
        self.assertEqual(self.ledger.watchlist(self.b_hub.inner.principal)[0]["state"],
                         "applied")


class TestCompletionEchoLongDelay(SpokeCoreTestBase):
    def test_echo_fires_when_recipient_completes_days_after_delivery(self):
        """Defect investigation (2026-08-20): six 2026-08-08 deliveries have
        never echoed a completion back. Hypothesis (a) is "the recipient
        never actually completed them"; hypothesis (b) is "terminal
        detection is broken/bounded for old transfers". GET /v1/watch
        (ledger.watchlist) has no time bound at all — it's a live query for
        resolved_at IS NULL AND terminal IS NULL, so an open transfer stays
        on the watchlist and gets re-checked every tick regardless of age.
        This proves (b) false: backdating the transfer 8 days and THEN
        completing the recipient's copy must still echo normally."""
        src = self.b_things.add("old delegation", tags=["jill"])
        self.settle(2)
        [dst] = [u for u, t in self.j_things.todos.items()
                if t["title"] == "old delegation"]
        # Backdate the transfer as if delivery happened over a week ago —
        # nothing in the watch/observe path should care.
        eight_days_ago = time.time() - 8 * 86400
        with self.ledger.lock, self.ledger.conn:
            self.ledger.conn.execute(
                "UPDATE transfers SET created_at=?, applied_at=? WHERE dst_uuid=?",
                (eight_days_ago, eight_days_ago, dst))
        # spoke process "restarts" between delivery and completion — fresh
        # local state, exactly like a real days-later gap; watch() is
        # server-side so this must not matter either.
        self.j_spoke.state = SpokeState(
            os.path.join(self.tmp.name, "j-state-restarted.sqlite"))
        self.j_things.complete(dst)
        self.settle(3)
        self.assertEqual(self.b_things.todos[src]["status"], "completed")  # D2, unchanged
        self.assertEqual(self.ledger.watchlist(self.b_hub.inner.principal), [])
        row = self.ledger.get_transfer(
            [t["id"] for t in self.ledger.conn.execute(
                "SELECT id FROM transfers WHERE dst_uuid=?", (dst,)).fetchall()][0])
        self.assertEqual(row["terminal"], "completed")
        self.assertIsNotNone(row["resolved_at"])


class TestRoundTrip(SpokeCoreTestBase):
    def test_sender_completes_at_send_not_at_recipient_completion(self):
        # D2 (2026-07-11): delegating IS the action — sender's copy is done
        # the moment delivery is confirmed, not when the recipient finishes.
        src = self.b_things.add("deliver", tags=["jill"])
        self.settle(2)
        self.assertIn(DELEGATED_TAG, self.b_things.todos[src]["tags"])
        self.assertNotIn("jill", self.b_things.todos[src]["tags"])
        self.assertEqual(self.b_things.todos[src]["status"], "completed")
        # jill's copy is still open — completing sender's copy does NOT
        # cascade to the recipient
        [dst] = [u for u, t in self.j_things.todos.items() if t["title"] == "deliver"]
        self.assertEqual(self.j_things.todos[dst]["status"], "open")
        # jill completes her copy for real; transfer resolves, watchlist clears
        self.j_things.complete(dst)
        self.settle(3)
        self.assertEqual(self.b_things.todos[src]["status"], "completed")  # unchanged
        self.assertEqual(self.ledger.watchlist(
            self.b_hub.inner.principal), [])

    def test_recipient_trash_does_not_uncomplete_sender_copy(self):
        src = self.b_things.add("unwanted", tags=["jill"])
        self.settle(2)
        self.assertEqual(self.b_things.todos[src]["status"], "completed")  # D2
        [dst] = [u for u, t in self.j_things.todos.items() if t["title"] == "unwanted"]
        self.j_things.trash(dst)
        self.settle(3)
        self.assertTrue(self.j_things.todos[dst]["trashed"])  # recipient's own action stands
        self.assertEqual(self.b_things.todos[src]["status"], "completed")  # sender NOT downgraded

    def test_sender_cancel_after_auto_complete_does_not_cascade(self):
        # Old "sender revocation" behavior (cancel your still-open delegated
        # copy to pull it back) has no window anymore under D2 — the sender's
        # copy is already completed by the time this could happen. Manually
        # canceling it afterward is a no-op from the sync system's view: the
        # retagged sender-role watch entry is permanently skipped, so it must
        # never propagate to the recipient's copy.
        src = self.b_things.add("nvm", tags=["jill"])
        self.settle(2)
        self.assertEqual(self.b_things.todos[src]["status"], "completed")
        self.b_things.cancel(src)
        self.settle(3)
        [dst] = [u for u, t in self.j_things.todos.items() if t["title"] == "nvm"]
        self.assertEqual(self.j_things.todos[dst]["status"], "open")

    def test_reverse_direction_jill_to_bradley(self):
        src = self.j_things.add("pick up kids", tags=["bradley"], when="2026-07-12")
        self.settle(2)
        [dst] = [u for u, t in self.b_things.todos.items()
                 if t["title"] == "pick up kids"]
        self.assertEqual(self.b_things.todos[dst]["tags"], ["from-jill 👩🏻‍🦰"])
        self.assertEqual(self.b_things.todos[dst]["when"], "2026-07-12")  # honored
        # D2 symmetric ("either side"): jill's sender copy already completed
        self.assertEqual(self.j_things.todos[src]["status"], "completed")
        self.b_things.complete(dst)
        self.settle(3)
        self.assertEqual(self.j_things.todos[src]["status"], "completed")  # unchanged

    def test_fresh_spoke_reinstall_does_not_reecho_prior_retag(self):
        """N2 regression -- the exact 2026-08-20 two-observer defect
        (things-agent-interaction-model.md §2.6): a transfer already
        D2-retagged by b_spoke (hub-durable) must NOT be re-reported as a
        fresh completion by a SECOND bradley-side spoke instance that
        never personally did the retag and has empty local state --
        standing in for a reinstall, or a second observer coming up
        alongside an existing one during a topology cutover. Before the
        fix (spoke-local `is_retagged`), this spoke would see the sender's
        own D2-completed status, treat it as a fresh signal, and wrongly
        complete jill's real copy."""
        src = self.b_things.add("deliver", tags=["jill"])
        self.settle(2)
        self.assertIn(DELEGATED_TAG, self.b_things.todos[src]["tags"])
        self.assertEqual(self.b_things.todos[src]["status"], "completed")  # D2

        # Same Things account, same hub principal (bradley), but a
        # completely fresh (empty) local SpokeState -- what a
        # reinstalled/new spoke instance looks like.
        fresh = self._spoke(self.b_things, self.b_writer, self.b_hub,
                            {"jill": ["jill"]}, "b-state-fresh")
        fresh.tick()
        [dst] = [u for u, t in self.j_things.todos.items() if t["title"] == "deliver"]
        self.assertEqual(self.j_things.todos[dst]["status"], "open")

        # Drive both real spokes forward too -- if fresh.tick() above
        # wrongly queued a completion echo to jill, this is where it would
        # actually land on her copy.
        self.settle(3)
        self.assertEqual(self.j_things.todos[dst]["status"], "open")

    def test_sender_manual_cancel_during_pending_retag_window_is_honored(self):
        """N2 fix regression, found in review: the hub-durable retag check
        made the "applied but not yet retagged" window potentially long
        (it now survives a hub outage, retried every tick with no
        give-up), and an earlier version of the fix unconditionally forced
        the sender's copy back to completed in that window -- silently
        discarding a genuine cancel/trash the user made themselves, with
        no error and no signal to the recipient. A cancel in this window
        must be honored exactly as it always was before this change:
        reported as a real cancel, propagated to the recipient, and never
        overwritten back to completed."""
        src = self.b_things.add("deliver", tags=["jill"])
        self.settle(1)  # jill's spoke applies the create; bradley hasn't retagged yet
        self.assertEqual(self.b_things.todos[src]["status"], "open")

        self.b_things.cancel(src)  # bradley cancels his own copy before retag fires
        self.b_spoke.tick()

        self.assertEqual(self.b_things.todos[src]["status"], "canceled")  # honored
        self.assertNotIn(DELEGATED_TAG, self.b_things.todos[src]["tags"])  # never retagged

        # Propagates to jill as a genuine cancel, same as the documented
        # pre-apply revocation path.
        self.settle(2)
        [dst] = [u for u, t in self.j_things.todos.items() if t["title"] == "deliver"]
        self.assertEqual(self.j_things.todos[dst]["status"], "canceled")

    def test_retag_survives_a_lost_mark_retagged_response(self):
        """N2's own resilience story: hub.mark_retagged() can succeed
        hub-side but the spoke never hears back (FlakyHub -- the
        lost-response class, not a real failure). _retag_sender_copy's
        except branch must not crash the tick, and the NEXT tick must see
        the hub already has it marked (the response loss didn't mean the
        write didn't happen) and settle cleanly -- no error, and no
        re-observation of the ambiguous "completed" status that would
        wrongly complete jill's real copy."""
        src = self.b_things.add("deliver", tags=["jill"])
        self.settle(1)  # jill applies; bradley hasn't retagged yet
        self.b_hub.drop_next.add("mark_retagged")
        self.b_spoke.tick()  # local write succeeds; the mark_retagged response is lost

        self.assertEqual(self.b_things.todos[src]["status"], "completed")
        self.assertIn(DELEGATED_TAG, self.b_things.todos[src]["tags"])

        self.b_spoke.tick()  # must not error, must not re-observe
        [dst] = [u for u, t in self.j_things.todos.items() if t["title"] == "deliver"]
        self.assertEqual(self.j_things.todos[dst]["status"], "open")  # never wrongly completed


if __name__ == "__main__":
    unittest.main()
