"""Push-transport tests (Tier 2): the hub delivery condition variable and the
decoupled spoke phases.

The invariant suite (test_invariants) still owns correctness — nothing here
weakens it. These tests only prove the NEW push machinery:
  - the ledger CV wakes a parked waiter the instant a leasable delivery commits
    (at each of the three delivery-queuing sites), and times out cleanly when
    idle — no busy-poll floor;
  - a real HTTP long-poll returns near-instantly on a push instead of waiting
    out its `wait` window (genuine server push over real sockets);
  - tick_local()/tick_inbound() compose to the same round-trip tick() does;
  - SpokeState tolerates the gateway's two-thread (inbound + local) access.
"""

import os
import tempfile
import threading
import time
import unittest

from hub.api import make_server
from hub.direct import DirectHubClient
from hub.ledger import Ledger
from spoke.core import DELEGATED_TAG, SpokeCore, SpokeState
from spoke.hub_http import HttpHubClient
from tests.fakes import FakeReader, FakeThings, FakeWriter, FlakyHub

PAYLOAD = {"schema": "tandem.todo/1", "title": "task", "notes": "",
           "checklist": [], "when": None, "deadline": None, "context_url": None}


class LedgerCVTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.ledger = Ledger(os.path.join(self.tmp.name, "ledger.sqlite"),
                            backoff_base_seconds=0.001, backoff_cap_seconds=0.01)
        self.addCleanup(self.ledger.close)
        tenant = self.ledger.create_tenant("davis")["id"]
        self.b = self.ledger.create_member(tenant, "bradley", "B")
        self.j = self.ledger.create_member(tenant, "jill", "J")
        self.pb = self.ledger.principal_for_member(
            self.b["id"], self.ledger.ensure_gateway_device(self.b["id"], "b"))
        self.pj = self.ledger.principal_for_member(
            self.j["id"], self.ledger.ensure_gateway_device(self.j["id"], "j"))

    def _wake_latency(self, trigger, park_timeout=10.0) -> float:
        """Park a waiter, run `trigger` after it's parked, return how long the
        waiter took to wake. A latency << park_timeout proves it woke on the
        signal, not the timeout."""
        woke = []
        start = time.monotonic()

        def waiter():
            self.ledger.wait_for_delivery(park_timeout)
            woke.append(time.monotonic())

        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.2)          # let the waiter reach the CV
        trigger()
        t.join(3.0)
        self.assertTrue(woke, "waiter never woke")
        return woke[0] - start

    def test_push_transfer_wakes_waiter(self):
        latency = self._wake_latency(
            lambda: self.ledger.push_transfer(self.pb, "jill", "SRC-1", PAYLOAD))
        self.assertLess(latency, 1.0)  # woke on signal, not the 10s park

    def test_observe_echo_wakes_waiter(self):
        # create + apply so a later observe queues a (leasable) terminal echo
        self.ledger.push_transfer(self.pb, "jill", "SRC-2", PAYLOAD)
        d = self.ledger.lease_deliveries(self.pj, 10, 60)[0]
        rec = self.ledger.get_transfer(d["transfer_id"])
        self.ledger.ack_delivery(self.pj, d["id"], dst_uuid="DST-2")
        latency = self._wake_latency(
            lambda: self.ledger.observe(self.pj, rec["id"], "completed"))
        self.assertLess(latency, 1.0)

    def test_ack_midflight_revoke_wakes_waiter(self):
        # sender revokes while the create is LEASED → the echo is queued at
        # ack time (not observe time); that ack must signal the CV.
        rec = self.ledger.push_transfer(self.pb, "jill", "SRC-3", PAYLOAD)
        d = self.ledger.lease_deliveries(self.pj, 10, 60)[0]     # create leased
        self.ledger.observe(self.pb, rec["id"], "canceled")      # queues nothing yet
        latency = self._wake_latency(
            lambda: self.ledger.ack_delivery(self.pj, d["id"], dst_uuid="DST-3"))
        self.assertLess(latency, 1.0)

    def test_idle_wait_times_out_without_signal(self):
        start = time.monotonic()
        self.ledger.wait_for_delivery(0.3)
        elapsed = time.monotonic() - start
        self.assertGreaterEqual(elapsed, 0.25)   # actually blocked ~the timeout
        self.assertLess(elapsed, 2.0)

    def test_nonpositive_timeout_returns_immediately(self):
        start = time.monotonic()
        self.ledger.wait_for_delivery(0)
        self.ledger.wait_for_delivery(-5)
        self.assertLess(time.monotonic() - start, 0.2)


class LongPollHttpTest(unittest.TestCase):
    """A real in-process HTTP server exercised through the deployed
    HttpHubClient — the genuine push path a remote spoke uses."""

    @classmethod
    def setUpClass(cls):
        cls.tmp = tempfile.TemporaryDirectory()
        cls.ledger = Ledger(os.path.join(cls.tmp.name, "ledger.sqlite"),
                            backoff_base_seconds=0.001, backoff_cap_seconds=0.01)
        tenant = cls.ledger.create_tenant("davis")["id"]
        b = cls.ledger.create_member(tenant, "bradley", "B", can_admin=True)
        j = cls.ledger.create_member(tenant, "jill", "J")
        cls.b_token = cls.ledger.create_device(b["id"], "gw")["token"]
        cls.j_token = cls.ledger.create_device(j["id"], "air")["token"]
        cls.server = make_server(cls.ledger, "127.0.0.1", 0)
        cls.port = cls.server.server_address[1]
        threading.Thread(target=cls.server.serve_forever, daemon=True).start()
        cls.url = f"http://127.0.0.1:{cls.port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.ledger.close()
        cls.tmp.cleanup()

    def test_long_poll_returns_on_push_not_timeout(self):
        b = HttpHubClient(self.url, lambda: self.b_token)
        j = HttpHubClient(self.url, lambda: self.j_token, poll_wait=25.0)
        got = {}

        def hold_poll():
            start = time.monotonic()
            got["deliveries"] = j.deliveries()   # GET /v1/deliveries?wait=25
            got["latency"] = time.monotonic() - start

        t = threading.Thread(target=hold_poll)
        t.start()
        time.sleep(0.3)                          # spoke is now parked on the hub
        b.push_transfer("jill", "SRC-HTTP-PUSH", PAYLOAD)
        t.join(5.0)
        self.assertFalse(t.is_alive(), "long-poll never returned")
        self.assertEqual(len(got["deliveries"]), 1)
        self.assertEqual(got["deliveries"][0]["payload"]["title"], "task")
        # returned on the push (~0.3s), nowhere near the 25s wait window
        self.assertLess(got["latency"], 3.0)

    def test_long_poll_returns_empty_after_timeout_when_idle(self):
        j = HttpHubClient(self.url, lambda: self.j_token, poll_wait=1.0)
        start = time.monotonic()
        deliveries = j.deliveries()
        elapsed = time.monotonic() - start
        self.assertEqual(deliveries, [])
        self.assertGreaterEqual(elapsed, 0.8)    # honored the wait window
        self.assertLess(elapsed, 4.0)


class SplitPhaseTest(unittest.TestCase):
    """tick_local()/tick_inbound() must compose to the same behavior tick()
    has — the decoupling is composition-only, per-phase logic unchanged."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.ledger = Ledger(os.path.join(self.tmp.name, "ledger.sqlite"),
                            backoff_base_seconds=0.001, backoff_cap_seconds=0.01)
        self.addCleanup(self.ledger.close)
        tenant = self.ledger.create_tenant("davis")["id"]
        b = self.ledger.create_member(tenant, "bradley", "B")
        j = self.ledger.create_member(tenant, "jill", "J")
        pb = self.ledger.principal_for_member(
            b["id"], self.ledger.ensure_gateway_device(b["id"]))
        pj = self.ledger.principal_for_member(
            j["id"], self.ledger.ensure_gateway_device(j["id"], "air"))
        self.b_things, self.j_things = FakeThings("b"), FakeThings("j")
        for things in (self.b_things, self.j_things):
            things.tags.update({"jill", "bradley", DELEGATED_TAG,
                                "from-bradley 👨", "from-jill 👩🏻‍🦰"})
        self.b_hub = FlakyHub(DirectHubClient(self.ledger, pb, lease_seconds=0.2))
        self.j_hub = FlakyHub(DirectHubClient(self.ledger, pj, lease_seconds=0.2))
        self.b_spoke = self._spoke(self.b_things, self.b_hub, {"jill": ["jill"]}, "b")
        self.j_spoke = self._spoke(self.j_things, self.j_hub, {"bradley": ["bradley"]}, "j")

    def _spoke(self, things, hub, triggers, name):
        return SpokeCore(
            reader=FakeReader(things), writer=FakeWriter(things), hub=hub,
            state=SpokeState(os.path.join(self.tmp.name, f"{name}.sqlite")),
            trigger_tags=triggers, correlate_timeout=1.0, correlate_interval=0.01)

    def _settle_split(self, rounds=4):
        for _ in range(rounds):
            time.sleep(0.25)
            for s in (self.b_spoke, self.j_spoke):
                s.tick_local()
                s.tick_inbound()

    def test_round_trip_over_split_phases(self):
        src = self.b_things.add("deliver", tags=["jill"])
        self._settle_split(2)
        # created on jill's side, sender copy retagged after confirmed apply
        self.assertEqual(self.j_things.count_titled("deliver"), 1)
        self.assertIn(DELEGATED_TAG, self.b_things.todos[src]["tags"])
        [dst] = [u for u, t in self.j_things.todos.items() if t["title"] == "deliver"]
        self.j_things.complete(dst)
        self._settle_split(3)
        self.assertEqual(self.b_things.todos[src]["status"], "completed")
        self.assertEqual(self.ledger.watchlist(self.b_hub.inner.principal), [])


class SpokeStateConcurrencyTest(unittest.TestCase):
    """The gateway's two loops (inbound touches journal, local touches
    sent_cache/observed) share one SpokeState connection across threads —
    the added RLock must keep that from erroring/corrupting."""

    def test_two_threads_hammer_state(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        state = SpokeState(os.path.join(tmp.name, "state.sqlite"))
        errors = []

        def local_writer():
            try:
                for i in range(400):
                    state.mark_sent(f"src-{i}", 1, f"tr-{i}")
                    state.mark_observed(f"tr-{i}", "completed")
                    state.known_sent(f"src-{i}")
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        def inbound_writer():
            try:
                for i in range(400):
                    state.journal_intent(f"d-{i}", f"tr-{i}", "t", "from-bradley")
                    state.journal_close(f"d-{i}", f"dst-{i}")
                    state.journaled_dst_uuids()
            except Exception as exc:  # noqa: BLE001
                errors.append(exc)

        threads = [threading.Thread(target=local_writer),
                   threading.Thread(target=inbound_writer)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(10.0)
        self.assertEqual(errors, [])
        self.assertTrue(state.known_sent("src-399"))
        self.assertEqual(len(state.journaled_dst_uuids()), 400)


if __name__ == "__main__":
    unittest.main()
