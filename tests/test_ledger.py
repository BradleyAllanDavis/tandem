"""Ledger unit tests: push idempotency, lease expiry/re-queue, ack/nack,
terminal set-once + completed-beats-canceled, tenant isolation, auth."""

import os
import tempfile
import time
import unittest

from hub.ledger import AuthError, Forbidden, Ledger, LedgerError, NotFound

PAYLOAD = {"schema": "tandem.todo/1", "title": "buy milk", "notes": "",
           "checklist": [], "when": None, "deadline": None, "context_url": None}


class LedgerTestBase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ledger = Ledger(os.path.join(self.tmp.name, "ledger.sqlite"),
                            backoff_base_seconds=0.001, backoff_cap_seconds=0.01)
        t = self.ledger.create_tenant("davis")
        self.tenant = t["id"]
        self.bradley = self.ledger.create_member(self.tenant, "bradley", "Bradley",
                                                 can_admin=True)
        self.jill = self.ledger.create_member(self.tenant, "jill", "Jill")
        self.b_dev = self.ledger.create_device(self.bradley["id"], "gateway")
        self.j_dev = self.ledger.create_device(self.jill["id"], "air")
        self.b = self.ledger.authenticate(self.b_dev["token"])
        self.j = self.ledger.authenticate(self.j_dev["token"])

    def tearDown(self):
        self.ledger.close()
        self.tmp.cleanup()


class TestAuth(LedgerTestBase):
    def test_bad_token(self):
        with self.assertRaises(AuthError):
            self.ledger.authenticate("nope")

    def test_revoked_token(self):
        self.ledger.revoke_device(self.j_dev["id"])
        with self.assertRaises(AuthError):
            self.ledger.authenticate(self.j_dev["token"])

    def test_token_shown_once_only_hash_stored(self):
        with self.ledger.lock:
            row = self.ledger.conn.execute(
                "SELECT token_hash FROM devices WHERE id=?",
                (self.j_dev["id"],)).fetchone()
        self.assertNotEqual(row["token_hash"], self.j_dev["token"])
        self.assertEqual(len(row["token_hash"]), 64)  # sha256 hex


class TestPush(LedgerTestBase):
    def test_push_creates_transfer_and_delivery(self):
        rec = self.ledger.push_transfer(self.b, "jill", "SRC-1", PAYLOAD)
        self.assertFalse(rec["deduped"])
        deliveries = self.ledger.lease_deliveries(self.j, 10, 300)
        self.assertEqual(len(deliveries), 1)
        self.assertEqual(deliveries[0]["kind"], "create")
        self.assertEqual(deliveries[0]["payload"]["title"], "buy milk")
        self.assertEqual(deliveries[0]["provenance_tag"], "from-bradley 👨")

    def test_push_idempotent_under_replay(self):
        r1 = self.ledger.push_transfer(self.b, "jill", "SRC-1", PAYLOAD)
        r2 = self.ledger.push_transfer(self.b, "jill", "SRC-1", PAYLOAD)
        self.assertEqual(r1["id"], r2["id"])
        self.assertTrue(r2["deduped"])
        # still exactly one delivery
        deliveries = self.ledger.lease_deliveries(self.j, 10, 300)
        self.assertEqual(len(deliveries), 1)

    def test_push_requires_can_send(self):
        kid = self.ledger.create_member(self.tenant, "kid", "Kid", can_send=False)
        dev = self.ledger.create_device(kid["id"], "ipad")
        p = self.ledger.authenticate(dev["token"])
        with self.assertRaises(Forbidden):
            self.ledger.push_transfer(p, "jill", "SRC-K", PAYLOAD)

    def test_push_to_unknown_member(self):
        with self.assertRaises(NotFound):
            self.ledger.push_transfer(self.b, "aaron", "SRC-1", PAYLOAD)

    def test_push_to_self_rejected(self):
        with self.assertRaises(LedgerError):
            self.ledger.push_transfer(self.b, "bradley", "SRC-1", PAYLOAD)


class TestDeliveries(LedgerTestBase):
    def test_lease_prevents_double_claim(self):
        self.ledger.push_transfer(self.b, "jill", "SRC-1", PAYLOAD)
        first = self.ledger.lease_deliveries(self.j, 10, 300)
        second = self.ledger.lease_deliveries(self.j, 10, 300)
        self.assertEqual(len(first), 1)
        self.assertEqual(second, [])

    def test_expired_lease_requeues(self):
        self.ledger.push_transfer(self.b, "jill", "SRC-1", PAYLOAD)
        first = self.ledger.lease_deliveries(self.j, 10, lease_seconds=0.01)
        time.sleep(0.05)
        second = self.ledger.lease_deliveries(self.j, 10, 300)
        self.assertEqual(first[0]["id"], second[0]["id"])
        self.assertEqual(second[0]["attempts"], 2)

    def test_ack_create_sets_dst_uuid(self):
        t = self.ledger.push_transfer(self.b, "jill", "SRC-1", PAYLOAD)
        d = self.ledger.lease_deliveries(self.j, 10, 300)[0]
        self.ledger.ack_delivery(self.j, d["id"], dst_uuid="DST-1")
        row = self.ledger.get_transfer(t["id"])
        self.assertEqual(row["dst_uuid"], "DST-1")
        self.assertIsNotNone(row["applied_at"])

    def test_ack_create_requires_dst_uuid(self):
        self.ledger.push_transfer(self.b, "jill", "SRC-1", PAYLOAD)
        d = self.ledger.lease_deliveries(self.j, 10, 300)[0]
        with self.assertRaises(LedgerError):
            self.ledger.ack_delivery(self.j, d["id"])

    def test_ack_is_idempotent(self):
        self.ledger.push_transfer(self.b, "jill", "SRC-1", PAYLOAD)
        d = self.ledger.lease_deliveries(self.j, 10, 300)[0]
        self.ledger.ack_delivery(self.j, d["id"], dst_uuid="DST-1")
        again = self.ledger.ack_delivery(self.j, d["id"], dst_uuid="DST-1")
        self.assertTrue(again.get("already_done"))

    def test_nack_requeues(self):
        # Nacked deliveries wait out their backoff floor before becoming
        # leasable again (2026-08-20 retry policy) — not available in the
        # same instant, but available once the (test-injected, tiny) backoff
        # elapses.
        self.ledger.push_transfer(self.b, "jill", "SRC-1", PAYLOAD)
        d = self.ledger.lease_deliveries(self.j, 10, 300)[0]
        self.ledger.nack_delivery(self.j, d["id"], "boom")
        self.assertEqual(self.ledger.lease_deliveries(self.j, 10, 300), [])
        time.sleep(0.02)  # past the test's injected 0.001s backoff floor
        again = self.ledger.lease_deliveries(self.j, 10, 300)
        self.assertEqual(len(again), 1)

    def test_cannot_touch_another_members_delivery(self):
        self.ledger.push_transfer(self.b, "jill", "SRC-1", PAYLOAD)
        d = self.ledger.lease_deliveries(self.j, 10, 300)[0]
        with self.assertRaises(NotFound):
            self.ledger.ack_delivery(self.b, d["id"], dst_uuid="X")


class TestTerminal(LedgerTestBase):
    def _applied_transfer(self):
        t = self.ledger.push_transfer(self.b, "jill", "SRC-1", PAYLOAD)
        d = self.ledger.lease_deliveries(self.j, 10, 300)[0]
        self.ledger.ack_delivery(self.j, d["id"], dst_uuid="DST-1")
        return t

    def test_completion_round_trip(self):
        t = self._applied_transfer()
        # both sides watch it
        self.assertEqual(len(self.ledger.watchlist(self.b)), 1)
        self.assertEqual(len(self.ledger.watchlist(self.j)), 1)
        # jill completes; echo delivery reaches bradley with src uuid
        self.ledger.observe(self.j, t["id"], "completed")
        echo = self.ledger.lease_deliveries(self.b, 10, 300)
        self.assertEqual(len(echo), 1)
        self.assertEqual(echo[0]["kind"], "complete")
        self.assertEqual(echo[0]["uuid"], "SRC-1")
        self.ledger.ack_delivery(self.b, echo[0]["id"])
        row = self.ledger.get_transfer(t["id"])
        self.assertIsNotNone(row["resolved_at"])
        # resolved transfers leave both watchlists
        self.assertEqual(self.ledger.watchlist(self.b), [])
        self.assertEqual(self.ledger.watchlist(self.j), [])

    def test_terminal_set_once(self):
        t = self._applied_transfer()
        self.ledger.observe(self.j, t["id"], "completed")
        out = self.ledger.observe(self.b, t["id"], "canceled")
        self.assertEqual(out["terminal"], "completed")  # completed sticks

    def test_completed_beats_canceled_upgrade(self):
        t = self._applied_transfer()
        self.ledger.observe(self.b, t["id"], "canceled")   # sender revokes
        out = self.ledger.observe(self.j, t["id"], "completed")  # jill finished it
        self.assertEqual(out["terminal"], "completed")
        # jill's pending cancel echo was replaced by a complete echo to bradley
        echoes = self.ledger.lease_deliveries(self.j, 10, 300)
        kinds_j = [e["kind"] for e in echoes]
        self.assertNotIn("cancel", kinds_j)
        echoes_b = self.ledger.lease_deliveries(self.b, 10, 300)
        self.assertEqual([e["kind"] for e in echoes_b], ["complete"])

    def test_no_upgrade_after_cancel_echo_done(self):
        t = self._applied_transfer()
        self.ledger.observe(self.b, t["id"], "canceled")
        echo = self.ledger.lease_deliveries(self.j, 10, 300)[0]
        self.assertEqual(echo["kind"], "cancel")
        self.ledger.ack_delivery(self.j, echo["id"])  # cancel already applied
        out = self.ledger.observe(self.j, t["id"], "completed")
        self.assertEqual(out["terminal"], "canceled")  # too late to upgrade

    def test_sender_revokes_before_apply_skips_create(self):
        t = self.ledger.push_transfer(self.b, "jill", "SRC-1", PAYLOAD)
        self.ledger.observe(self.b, t["id"], "canceled")
        # jill must never receive the create OR any echo
        self.assertEqual(self.ledger.lease_deliveries(self.j, 10, 300), [])
        row = self.ledger.get_transfer(t["id"])
        self.assertIsNotNone(row["resolved_at"])

    def test_sender_revokes_while_create_leased(self):
        t = self.ledger.push_transfer(self.b, "jill", "SRC-1", PAYLOAD)
        d = self.ledger.lease_deliveries(self.j, 10, 300)[0]  # in flight
        self.ledger.observe(self.b, t["id"], "canceled")
        # apply finishes; echo must be queued at ack time
        self.ledger.ack_delivery(self.j, d["id"], dst_uuid="DST-1")
        echo = self.ledger.lease_deliveries(self.j, 10, 300)
        self.assertEqual([e["kind"] for e in echo], ["cancel"])
        self.assertEqual(echo[0]["uuid"], "DST-1")

    def test_echo_to_recipient_gated_on_dst_uuid(self):
        t = self.ledger.push_transfer(self.b, "jill", "SRC-1", PAYLOAD)
        d = self.ledger.lease_deliveries(self.j, 10, lease_seconds=0.01)[0]
        del d
        self.ledger.observe(self.b, t["id"], "canceled")
        time.sleep(0.05)
        # create lease expired; the requeued CREATE may be re-leased but no
        # terminal echo may appear before dst_uuid exists
        leased = self.ledger.lease_deliveries(self.j, 10, 300)
        self.assertTrue(all(e["kind"] == "create" for e in leased))


class TestRetryPolicy(unittest.TestCase):
    """Defect 2 regression (2026-08-20 incident: a delivery retried 1300+
    times over a month with no backoff or ceiling). Its own Ledger with a
    tiny max_attempts/backoff so the policy's edges (growth, cap, the
    dead-letter transition, both the explicit-nack path and the silent-
    crash/lease-expiry path) are exercised fast and deterministically —
    not the module defaults, which are sized for production, not tests."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ledger = Ledger(os.path.join(self.tmp.name, "ledger.sqlite"),
                             max_attempts=3, backoff_base_seconds=0.01,
                             backoff_cap_seconds=0.02)
        t = self.ledger.create_tenant("davis")
        self.tenant = t["id"]
        self.bradley = self.ledger.create_member(self.tenant, "bradley", "Bradley",
                                                 can_admin=True)
        self.jill = self.ledger.create_member(self.tenant, "jill", "Jill")
        self.b = self.ledger.authenticate(
            self.ledger.create_device(self.bradley["id"], "gateway")["token"])
        self.j = self.ledger.authenticate(
            self.ledger.create_device(self.jill["id"], "air")["token"])

    def tearDown(self):
        self.ledger.close()
        self.tmp.cleanup()

    def test_nack_backs_off_before_next_lease(self):
        self.ledger.push_transfer(self.b, "jill", "SRC-1", PAYLOAD)
        d = self.ledger.lease_deliveries(self.j, 10, 300)[0]
        self.ledger.nack_delivery(self.j, d["id"], "boom")
        self.assertEqual(self.ledger.lease_deliveries(self.j, 10, 300), [],
                         "not eligible again until the backoff floor elapses")
        time.sleep(0.03)
        again = self.ledger.lease_deliveries(self.j, 10, 300)
        self.assertEqual(len(again), 1)
        self.assertEqual(again[0]["attempts"], 2)

    def test_exhausting_max_attempts_dead_letters_via_explicit_nack(self):
        self.ledger.push_transfer(self.b, "jill", "SRC-1", PAYLOAD)
        last = None
        for attempt in range(1, 4):  # max_attempts=3
            d = self.ledger.lease_deliveries(self.j, 10, 300)
            self.assertEqual(len(d), 1, f"attempt {attempt} should still be leasable")
            self.assertEqual(d[0]["attempts"], attempt)
            last = self.ledger.nack_delivery(self.j, d[0]["id"], "still stuck")
            time.sleep(0.03)
        self.assertTrue(last.get("dead_letter"),
                        "the 3rd (== max_attempts) nack must dead-letter, not requeue")
        # never leased again, no matter how long we wait
        time.sleep(0.03)
        self.assertEqual(self.ledger.lease_deliveries(self.j, 10, 300), [])
        health = self.ledger.health()
        self.assertEqual(health["dead_letter_deliveries"], 1)
        self.assertEqual(health["pending_deliveries"], 0)
        # a further nack on an already-dead-lettered delivery is an inert no-op
        d_id = self.ledger.conn.execute(
            "SELECT id FROM deliveries").fetchone()["id"]
        out = self.ledger.nack_delivery(self.j, d_id, "still stuck")
        self.assertTrue(out.get("already_done"))

    def test_exhausting_max_attempts_dead_letters_via_lease_expiry(self):
        """The spoke process vanishes mid-lease without ever calling nack
        (a genuine crash, not a reported failure) — lease_deliveries itself
        must catch attempts >= max_attempts on the next poll and retire the
        delivery, since nothing else will."""
        self.ledger.push_transfer(self.b, "jill", "SRC-1", PAYLOAD)
        for attempt in range(1, 4):  # max_attempts=3, lease TTL so short it
            d = self.ledger.lease_deliveries(self.j, 10, lease_seconds=0.01)
            self.assertEqual(len(d), 1, f"attempt {attempt} should still be leasable")
            time.sleep(0.03)  # let the lease (and any backoff) expire, no ack/nack
        self.assertEqual(self.ledger.lease_deliveries(self.j, 10, 300), [])
        self.assertEqual(self.ledger.health()["dead_letter_deliveries"], 1)

    def test_dead_letter_delivery_never_blocks_the_transfer_forever_silently(self):
        """A dead-lettered delivery should be loud (visible in health), not
        a silent black hole — this is the actual complaint behind the
        incident (1300+ attempts, nobody noticed for a month)."""
        self.assertEqual(self.ledger.health()["dead_letter_deliveries"], 0)
        self.ledger.push_transfer(self.b, "jill", "SRC-1", PAYLOAD)
        for _ in range(3):
            d = self.ledger.lease_deliveries(self.j, 10, 300)[0]
            self.ledger.nack_delivery(self.j, d["id"], "boom")
            time.sleep(0.03)
        self.assertEqual(self.ledger.health()["dead_letter_deliveries"], 1)


class TestTenantIsolation(unittest.TestCase):
    """Two tenants seeded; every surface asserted cross-tenant-blind."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ledger = Ledger(os.path.join(self.tmp.name, "ledger.sqlite"),
                            backoff_base_seconds=0.001, backoff_cap_seconds=0.01)
        ta = self.ledger.create_tenant("davis")["id"]
        tb = self.ledger.create_tenant("smith")["id"]
        a1 = self.ledger.create_member(ta, "bradley", "Bradley", can_admin=True)
        self.ledger.create_member(ta, "jill", "Jill")
        b1 = self.ledger.create_member(tb, "alice", "Alice")
        b2 = self.ledger.create_member(tb, "bob", "Bob")
        self.pa = self.ledger.authenticate(
            self.ledger.create_device(a1["id"], "d")["token"])
        self.pb1 = self.ledger.authenticate(
            self.ledger.create_device(b1["id"], "d")["token"])
        self.pb2 = self.ledger.authenticate(
            self.ledger.create_device(b2["id"], "d")["token"])

    def tearDown(self):
        self.ledger.close()
        self.tmp.cleanup()

    def test_cannot_push_to_other_tenants_member(self):
        with self.assertRaises(NotFound):
            self.ledger.push_transfer(self.pa, "alice", "SRC-1", PAYLOAD)

    def test_same_handle_different_tenants_dont_collide(self):
        # a 'jill' in davis is invisible to smith even by handle
        with self.assertRaises(NotFound):
            self.ledger.push_transfer(self.pb1, "jill", "SRC-1", PAYLOAD)

    def test_deliveries_watch_and_observe_are_scoped(self):
        t = self.ledger.push_transfer(self.pb1, "bob", "SRC-B", PAYLOAD)
        self.assertEqual(self.ledger.lease_deliveries(self.pa, 10, 300), [])
        self.assertEqual(self.ledger.watchlist(self.pa), [])
        with self.assertRaises(NotFound):
            self.ledger.observe(self.pa, t["id"], "completed")


if __name__ == "__main__":
    unittest.main()
