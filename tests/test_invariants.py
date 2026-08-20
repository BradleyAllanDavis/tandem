"""Invariant simulation: a real hub (in-process ledger) + two spokes over
fake Things accounts, driven through randomized chaos — writer crashes
after firing, lost hub responses (push/ack/observe), delayed ticks with
expired leases — asserting the three system invariants at quiescence:

  NO-LOSS   every delegated todo eventually materializes on the recipient
            (or is loudly refused), and every terminal state echoes back.
  NO-DUP    at most one live copy per side per transfer, ever.
  ROUND-TRIP every transfer ends resolved with both sides consistent.

Deterministic per seed; run several seeds. This is the test the ledger's
UNIQUE constraints, the delivery leases, and the spoke journal exist for.
"""

import os
import random
import tempfile
import time
import unittest

from hub.direct import DirectHubClient
from hub.ledger import Ledger
from spoke.core import DELEGATED_TAG, SpokeCore, SpokeState
from tests.fakes import CrashAfterFire, FakeReader, FakeThings, FakeWriter, FlakyHub

N_TODOS = 12
CHAOS_TICKS = 60


class InvariantSim(unittest.TestCase):
    def run_sim(self, seed: int):
        rng = random.Random(seed)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        ledger = Ledger(os.path.join(tmp.name, "ledger.sqlite"),
                        backoff_base_seconds=0.001, backoff_cap_seconds=0.01)
        self.addCleanup(ledger.close)
        tenant = ledger.create_tenant("davis")["id"]
        members = {
            "bradley": ledger.create_member(tenant, "bradley", "B"),
            "jill": ledger.create_member(tenant, "jill", "J"),
        }
        things = {"bradley": FakeThings("b"), "jill": FakeThings("j")}
        for t in things.values():
            t.tags.update({"jill", "bradley", DELEGATED_TAG,
                           "from-bradley 👨", "from-jill 👩🏻‍🦰"})
        writers, hubs, spokes = {}, {}, {}
        for handle, m in members.items():
            p = ledger.principal_for_member(
                m["id"], ledger.ensure_gateway_device(m["id"], f"{handle}-dev"))
            writers[handle] = FakeWriter(things[handle])
            hubs[handle] = FlakyHub(DirectHubClient(ledger, p, lease_seconds=0.05))
            other = "jill" if handle == "bradley" else "bradley"
            spokes[handle] = SpokeCore(
                reader=FakeReader(things[handle]), writer=writers[handle],
                hub=hubs[handle],
                state=SpokeState(os.path.join(tmp.name, f"{handle}.sqlite")),
                trigger_tags={other: [other]},
                correlate_timeout=0.5, correlate_interval=0.01)

        # -- seed work: delegations both directions, some with baggage ------
        expectations = []  # (sender, title, terminal_plan)
        for i in range(N_TODOS):
            sender = rng.choice(["bradley", "jill"])
            recipient = "jill" if sender == "bradley" else "bradley"
            title = f"todo-{seed}-{i}"
            things[sender].add(
                title, tags=[recipient],
                notes=f"notes {i}" * rng.randint(0, 3),
                checklist=[f"c{j}" for j in range(rng.randint(0, 4))],
                when=rng.choice([None, None, "2026-08-01", "someday"]))
            # what the human on each side will eventually do. "sender_revokes"
            # (canceling your own still-open delegated copy) has no window
            # anymore under D2 (2026-07-11): the sender's copy auto-completes
            # the moment delivery is confirmed, well before a human could act
            # on it, so that scenario is retired — see
            # test_spoke_core.test_sender_cancel_after_auto_complete_does_not_cascade
            # for the (now-inert) manual-cancel-after-auto-complete case.
            plan = rng.choice(["recipient_completes", "recipient_completes",
                               "recipient_cancels"])
            expectations.append((sender, recipient, title, plan))

        acted = set()

        # -- chaos loop -------------------------------------------------------
        for tick in range(CHAOS_TICKS):
            handle = rng.choice(["bradley", "jill"])
            # inject failures
            if rng.random() < 0.15:
                writers[handle].crash_next_create = True
            if rng.random() < 0.20:
                hubs[handle].drop_next.add(
                    rng.choice(["push_transfer", "ack", "observe"]))
            try:
                spokes[handle].tick()
            except CrashAfterFire:
                pass  # the spoke process "died"; next tick is the restart
            time.sleep(0.06)  # let leases expire across rounds

            # humans act once their copy exists
            for sender, recipient, title, plan in expectations:
                key = (sender, title)
                if key in acted:
                    continue
                # prefix match, not exact -- provenance tags may carry a
                # per-member emoji suffix (hub/ledger.py's provenance_tag())
                dst = [u for u, t in things[recipient].todos.items()
                       if t["title"] == title
                       and any(tag.startswith("from-" + sender) for tag in t["tags"])]
                if dst:
                    if plan == "recipient_completes":
                        things[recipient].complete(dst[0])
                    else:
                        things[recipient].cancel(dst[0])
                    acted.add(key)

        # -- drain to quiescence (no injected failures) ----------------------
        for _ in range(30):
            time.sleep(0.06)
            spokes["bradley"].tick()
            spokes["jill"].tick()

        # -- assert invariants ------------------------------------------------
        with ledger.lock:
            transfers = ledger.conn.execute("SELECT * FROM transfers").fetchall()
            undone = ledger.conn.execute(
                "SELECT COUNT(*) FROM deliveries WHERE state != 'done'").fetchone()[0]

        self.assertEqual(len(transfers), N_TODOS,
                         "every delegation became exactly one transfer")
        self.assertEqual(undone, 0, "no delivery left unfinished at quiescence")

        by_title = {}
        for sender, recipient, title, plan in expectations:
            by_title[title] = (sender, recipient, plan)

        for t in transfers:
            payload_title = None
            for title, (sender, recipient, plan) in by_title.items():
                if t["src_uuid"] in things[sender].todos and \
                        things[sender].todos[t["src_uuid"]]["title"] == title:
                    payload_title = title
                    break
            self.assertIsNotNone(payload_title, "transfer maps to a seeded todo")
            sender, recipient, plan = by_title[payload_title]

            # ROUND-TRIP: resolved, with a terminal state
            self.assertIsNotNone(t["resolved_at"],
                                 f"{payload_title} ({plan}) resolved")
            self.assertIn(t["terminal"], ("completed", "canceled"))

            # NO-DUP: at most one live recipient copy
            copies = things[recipient].count_titled(payload_title)
            self.assertEqual(copies, 1,
                             f"exactly one copy of {payload_title!r} on "
                             f"{recipient} (got {copies})")

            # NO-LOSS + D2: sender's copy always completed at send,
            # regardless of the recipient's eventual action; recipient's own
            # copy reflects their real action.
            src_status = things[sender].todos[t["src_uuid"]]["status"]
            self.assertEqual(src_status, "completed",
                             f"sender copy of {payload_title} completed at send (D2)")
            if t["dst_uuid"]:
                dst_status = things[recipient].todos[t["dst_uuid"]]["status"]
                expect = t["terminal"]
                self.assertEqual(dst_status, expect,
                                 f"recipient copy of {payload_title} is {expect}")

    def test_seed_1(self):
        self.run_sim(1)

    def test_seed_7(self):
        self.run_sim(7)

    def test_seed_42(self):
        self.run_sim(42)


if __name__ == "__main__":
    unittest.main()
