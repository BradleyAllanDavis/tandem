"""LocalWriter must never let a things:// auth-token leak into an
exception message — CalledProcessError/TimeoutExpired from `open -g`
embed the full argv (including the url) by default, and that string
ends up in spoke logs and in the hub nack payload (core.py)."""

import subprocess
import time
import unittest
from unittest import mock

from spoke.writer_local import LocalWriter

SECRET_TOKEN = "super-secret-things-token"


class FakeReader:
    def refresh(self):
        pass

    def status(self, uuid):
        return {"status": "open"}

    def tags_of(self, uuid):
        return []


class TokenRedactionTests(unittest.TestCase):
    def setUp(self):
        self.writer = LocalWriter(lambda: SECRET_TOKEN, FakeReader())

    def test_called_process_error_does_not_leak_token(self):
        with mock.patch(
            "subprocess.run",
            side_effect=subprocess.CalledProcessError(
                1, ["open", "-g", f"things:///json?auth-token={SECRET_TOKEN}&data=x"]),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                self.writer.create({"title": "t"}, "from-bradley")
        self.assertNotIn(SECRET_TOKEN, str(ctx.exception))
        self.assertNotIn("auth-token", str(ctx.exception))

    def test_timeout_expired_does_not_leak_token(self):
        with mock.patch(
            "subprocess.run",
            side_effect=subprocess.TimeoutExpired(
                ["open", "-g", f"things:///json?auth-token={SECRET_TOKEN}&data=x"], 15),
        ):
            with self.assertRaises(RuntimeError) as ctx:
                self.writer.create({"title": "t"}, "from-bradley")
        self.assertNotIn(SECRET_TOKEN, str(ctx.exception))
        self.assertNotIn("auth-token", str(ctx.exception))


class VerifyDoesNotRefreshTests(unittest.TestCase):
    """F7 regression (things-agent-interaction-model.md N1): `_verify()`
    used to call `reader.refresh()` on every poll iteration. refresh() is a
    synchronous on-demand mirror kickstart (`launchctl kickstart -k`),
    measured 4-13s per call on a real Mac and sometimes exceeding its own
    10s timeout — calling it every iteration is what serialized
    recipient-side deliveries ~10-11s apart. The mirror's own
    `StartInterval=5` LaunchAgent keeps it fresh independent of anything
    this loop does, so `_verify()` must never call refresh() at all."""

    def test_verify_never_calls_refresh_even_across_multiple_polls(self):
        class SettlesAfterAFewPollsReader:
            def __init__(self):
                self.refresh_calls = 0
                self._checks = 0

            def refresh(self):
                self.refresh_calls += 1

            def status(self, uuid):
                self._checks += 1
                # Doesn't report "completed" until the 3rd poll -- proves
                # the multi-iteration path, not just the first check.
                if self._checks >= 3:
                    return {"status": "completed"}
                return {"status": "open"}

        reader = SettlesAfterAFewPollsReader()
        writer = LocalWriter(lambda: SECRET_TOKEN, reader)
        # SETTLE_SECONDS patched to 0: with time.sleep mocked but time.time()
        # real, the (unrelated, intentional) post-write focus-settle window
        # would otherwise busy-spin for the full real 3s instead of sleeping.
        with mock.patch("subprocess.run") as run, mock.patch("time.sleep"), \
                mock.patch("spoke.writer_local.SETTLE_SECONDS", 0.0):
            run.return_value = mock.Mock(returncode=0)
            ok = writer.set_terminal("uuid-1", "completed")

        self.assertTrue(ok)
        self.assertGreaterEqual(reader._checks, 3)  # actually polled multiple times
        self.assertEqual(reader.refresh_calls, 0)


class RecipientSideLatencyTests(unittest.TestCase):
    """Times several consecutive terminal-delivery applies -- the same
    one-at-a-time loop SpokeCore._inbound() drives (spoke/core.py) -- with a
    reader whose refresh() is deliberately slow, standing in for the real
    4-13s `launchctl kickstart -k` cost measured on Jill's Air. Proves the
    inter-arrival gap collapses because refresh() is never invoked from
    this path. Fake reader/member only -- never jill-air, per N1.

    SETTLE_SECONDS (LocalWriter's real, intentional post-write focus-watch
    window -- unrelated to the F7 bug) is patched down so it doesn't swamp
    the measurement; real time.sleep() is NOT mocked, so the reader's
    injected kickstart cost is a genuine wall-clock trap: if _verify() ever
    regresses back to calling refresh() per poll iteration, this test goes
    from sub-second to N x SIMULATED_KICKSTART_COST and fails on the bound
    below, not just on the call-count assertion."""

    SIMULATED_KICKSTART_COST = 0.2  # stand-in for the real 4-13s

    def test_consecutive_applies_do_not_serialize_on_refresh(self):
        cost = self.SIMULATED_KICKSTART_COST

        class SlowKickstartReader:
            def __init__(self):
                self.refresh_calls = 0

            def refresh(self):
                # If this regresses back into _verify()'s poll loop, every
                # one of the N applies below pays this cost at least once.
                self.refresh_calls += 1
                time.sleep(cost)

            def status(self, uuid):
                return {"status": "completed"}  # already settled by the time we check

            def tags_of(self, uuid):
                return []

        reader = SlowKickstartReader()
        writer = LocalWriter(lambda: SECRET_TOKEN, reader)
        n_deliveries = 5
        uuids = [f"uuid-{i}" for i in range(n_deliveries)]

        arrivals = []
        with mock.patch("subprocess.run") as run, \
                mock.patch("spoke.writer_local.SETTLE_SECONDS", 0.0):
            run.return_value = mock.Mock(returncode=0)
            t0 = time.monotonic()
            for u in uuids:
                self.assertTrue(writer.set_terminal(u, "completed"))
                arrivals.append(time.monotonic() - t0)

        total = arrivals[-1]
        gaps = [b - a for a, b in zip([0.0] + arrivals[:-1], arrivals)]

        self.assertEqual(reader.refresh_calls, 0)
        self.assertLess(total, cost)  # N deliveries, not N x kickstart-cost
        for gap in gaps:
            self.assertLess(gap, cost)  # inter-arrival gap collapsed


if __name__ == "__main__":
    unittest.main()
