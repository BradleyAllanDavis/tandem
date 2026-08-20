"""Schema migration test (2026-08-20): deliveries gained `next_attempt_at`
and the 'dead_letter' state as part of the retry-policy fix. SQLite can't
ALTER a CHECK constraint in place, so Ledger._migrate_deliveries_dead_letter
rebuilds the table — this must be idempotent and must not lose rows against
a fixture built on the OLD pre-migration schema (simulating the live
production ledger, which predates this change)."""

import os
import sqlite3
import tempfile
import time
import unittest
import unittest.mock
import uuid

from hub.ledger import Ledger

# The schema exactly as it shipped before the 2026-08-20 retry-policy change
# (hub/ledger.py's original _SCHEMA, deliveries table only needs to be
# pre-migration shape — the other tables are unaffected by this migration).
_OLD_SCHEMA = """
CREATE TABLE tenants (
  id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE, created_at REAL NOT NULL
);
CREATE TABLE members (
  id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL REFERENCES tenants(id),
  handle TEXT NOT NULL, display_name TEXT NOT NULL,
  can_send INTEGER NOT NULL DEFAULT 1, can_receive INTEGER NOT NULL DEFAULT 1,
  can_admin INTEGER NOT NULL DEFAULT 0, created_at REAL NOT NULL,
  UNIQUE(tenant_id, handle)
);
CREATE TABLE devices (
  id TEXT PRIMARY KEY, member_id TEXT NOT NULL REFERENCES members(id),
  name TEXT NOT NULL, token_hash TEXT UNIQUE, created_at REAL NOT NULL,
  last_seen_at REAL, revoked_at REAL
);
CREATE TABLE transfers (
  id TEXT PRIMARY KEY, tenant_id TEXT NOT NULL REFERENCES tenants(id),
  from_member TEXT NOT NULL REFERENCES members(id),
  to_member TEXT NOT NULL REFERENCES members(id),
  src_uuid TEXT NOT NULL, dst_uuid TEXT, rev INTEGER NOT NULL DEFAULT 1,
  payload TEXT NOT NULL,
  terminal TEXT CHECK (terminal IN ('completed','canceled')),
  terminal_by TEXT REFERENCES members(id),
  created_at REAL NOT NULL, applied_at REAL, resolved_at REAL,
  UNIQUE(tenant_id, from_member, src_uuid, rev)
);
CREATE TABLE deliveries (
  id TEXT PRIMARY KEY, transfer_id TEXT NOT NULL REFERENCES transfers(id),
  kind TEXT NOT NULL CHECK (kind IN ('create','complete','cancel')),
  to_member TEXT NOT NULL REFERENCES members(id),
  state TEXT NOT NULL CHECK (state IN ('queued','leased','done')),
  leased_by_device TEXT, lease_expires_at REAL,
  attempts INTEGER NOT NULL DEFAULT 0, last_error TEXT,
  created_at REAL NOT NULL, done_at REAL,
  UNIQUE(transfer_id, kind, to_member)
);
CREATE TABLE events (
  id TEXT PRIMARY KEY, transfer_id TEXT, device_id TEXT, kind TEXT NOT NULL,
  detail TEXT, created_at REAL NOT NULL
);
CREATE INDEX idx_deliveries_member_state ON deliveries (to_member, state);
CREATE INDEX idx_transfers_open ON transfers (tenant_id, resolved_at);
"""


class TestDeadLetterMigration(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "old-ledger.sqlite")

    def _seed_pre_migration_db(self):
        """Build a fixture on the OLD schema with real pre-existing rows —
        tenant, both members, a transfer, and 3 delivery rows in varied
        states (queued/leased/done, one with attempts already > 0) — the
        shape a live production ledger predating this change would have."""
        conn = sqlite3.connect(self.path)
        conn.executescript(_OLD_SCHEMA)
        now = time.time()
        tid, mb, mj = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
        conn.execute("INSERT INTO tenants VALUES (?,?,?)", (tid, "davis", now))
        conn.execute("INSERT INTO members VALUES (?,?,?,?,?,?,?,?)",
                     (mb, tid, "bradley", "B", 1, 1, 1, now))
        conn.execute("INSERT INTO members VALUES (?,?,?,?,?,?,?,?)",
                     (mj, tid, "jill", "J", 1, 1, 0, now))
        xfer = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO transfers (id, tenant_id, from_member, to_member,"
            " src_uuid, rev, payload, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (xfer, tid, mb, mj, "SRC-1", 1, "{}", now))
        rows = [
            (str(uuid.uuid4()), xfer, "create", mj, "queued", 3, now),
            (str(uuid.uuid4()), xfer, "complete", mb, "leased", 1, now),
            (str(uuid.uuid4()), xfer, "cancel", mj, "done", 5, now),
        ]
        for rid, t, kind, to, state, attempts, created in rows:
            conn.execute(
                "INSERT INTO deliveries (id, transfer_id, kind, to_member,"
                " state, attempts, created_at) VALUES (?,?,?,?,?,?,?)",
                (rid, t, kind, to, state, attempts, created))
        conn.commit()
        conn.close()
        return [r[0] for r in rows]

    def test_migration_preserves_all_rows_and_adds_new_columns(self):
        seeded_ids = self._seed_pre_migration_db()

        ledger = Ledger(self.path)  # migration runs inside __init__
        self.addCleanup(ledger.close)

        cols = {r["name"] for r in
                ledger.conn.execute("PRAGMA table_info(deliveries)")}
        self.assertIn("next_attempt_at", cols)

        rows = {r["id"]: dict(r) for r in
                ledger.conn.execute("SELECT * FROM deliveries")}
        self.assertEqual(set(rows), set(seeded_ids), "no rows lost or added")
        for rid in seeded_ids:
            self.assertIsNone(rows[rid]["next_attempt_at"],
                              "pre-existing rows are eligible immediately")
        # attempts/state/kind/to_member carried forward unchanged
        by_kind = {r["kind"]: r for r in rows.values()}
        self.assertEqual(by_kind["create"]["attempts"], 3)
        self.assertEqual(by_kind["create"]["state"], "queued")
        self.assertEqual(by_kind["complete"]["state"], "leased")
        self.assertEqual(by_kind["cancel"]["state"], "done")

    def test_dead_letter_state_accepted_after_migration(self):
        seeded_ids = self._seed_pre_migration_db()
        ledger = Ledger(self.path)
        self.addCleanup(ledger.close)
        with ledger.lock, ledger.conn:
            ledger.conn.execute(
                "UPDATE deliveries SET state='dead_letter' WHERE id=?",
                (seeded_ids[0],))
        row = ledger.conn.execute(
            "SELECT state FROM deliveries WHERE id=?", (seeded_ids[0],)).fetchone()
        self.assertEqual(row["state"], "dead_letter")

    def test_migration_is_idempotent_across_reopens(self):
        seeded_ids = self._seed_pre_migration_db()
        ledger1 = Ledger(self.path)
        ledger1.close()
        ledger2 = Ledger(self.path)  # re-open: must be a no-op, not re-migrate
        self.addCleanup(ledger2.close)
        rows = ledger2.conn.execute("SELECT id FROM deliveries").fetchall()
        self.assertEqual({r["id"] for r in rows}, set(seeded_ids))

    def test_fresh_db_never_touches_migration_path(self):
        # A brand-new DB's _SCHEMA already has the final shape — the
        # migration must detect that and no-op without error.
        path = os.path.join(self.tmp.name, "fresh.sqlite")
        ledger = Ledger(path)
        self.addCleanup(ledger.close)
        cols = {r["name"] for r in
                ledger.conn.execute("PRAGMA table_info(deliveries)")}
        self.assertIn("next_attempt_at", cols)

    def test_interrupted_migration_rolls_back_and_leaves_original_table_intact(self):
        """Proves the migration is genuinely atomic (2026-08-20 review
        finding): a fault injected mid-rebuild (between the RENAME/CREATE
        and the data copy landing) must roll back the WHOLE thing — the
        pre-migration table restored exactly as it was, not a half-rebuilt
        schema with data stuck in a renamed-aside table. A clean retry
        afterward must then succeed normally."""
        seeded_ids = self._seed_pre_migration_db()
        real_connect = sqlite3.connect

        # sqlite3.Connection is a C-extension type — its methods can't be
        # monkeypatched on the class (immutable type). Inject the fault via
        # a real Python subclass passed as the connection `factory` instead
        # (sqlite3.connect's own kwarg for exactly this), transparently, by
        # patching the module-level sqlite3.connect ledger.py calls.
        class FlakyConnection(sqlite3.Connection):
            def execute(self, sql, *args, **kwargs):
                if isinstance(sql, str) and "INSERT INTO deliveries" in sql:
                    raise sqlite3.OperationalError("simulated crash mid-migration")
                return super().execute(sql, *args, **kwargs)

        def patched_connect(*args, **kwargs):
            kwargs.setdefault("factory", FlakyConnection)
            return real_connect(*args, **kwargs)

        with unittest.mock.patch("sqlite3.connect", patched_connect):
            with self.assertRaises(sqlite3.OperationalError):
                Ledger(self.path)

        # reconnect plain (no mock) and verify the rollback actually undid
        # the RENAME too, not just the failed INSERT.
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        self.assertIn("deliveries", tables)
        self.assertNotIn("deliveries_pre_dead_letter", tables,
                         "rollback must undo the RENAME, not leave a stray table")
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(deliveries)")}
        self.assertNotIn("next_attempt_at", cols,
                         "a rolled-back migration must leave the OLD schema in place")
        rows = conn.execute("SELECT id FROM deliveries").fetchall()
        self.assertEqual({r["id"] for r in rows}, set(seeded_ids),
                         "no rows lost even though the migration crashed mid-way")
        conn.close()

        # a clean retry (fault no longer injected) must now succeed.
        ledger = Ledger(self.path)
        self.addCleanup(ledger.close)
        cols2 = {r["name"] for r in
                ledger.conn.execute("PRAGMA table_info(deliveries)")}
        self.assertIn("next_attempt_at", cols2)
        rows2 = ledger.conn.execute("SELECT id FROM deliveries").fetchall()
        self.assertEqual({r["id"] for r in rows2}, set(seeded_ids))


class TestRetaggedMigration(unittest.TestCase):
    """N2 (2026-08-20): transfers gained `retagged_at`. Unlike deliveries'
    dead_letter migration, this is a plain nullable `ADD COLUMN` (no CHECK/
    UNIQUE/DEFAULT), so no table rebuild and no fault-injection/rollback
    test is needed -- just row preservation and idempotency."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = os.path.join(self.tmp.name, "old-ledger.sqlite")

    def _seed_pre_migration_db(self):
        """Fixture on the schema exactly as it shipped before N2 (transfers
        table with no retagged_at column) -- the shape a live production
        ledger predating this change would have."""
        conn = sqlite3.connect(self.path)
        conn.executescript(_OLD_SCHEMA)
        now = time.time()
        tid, mb, mj = str(uuid.uuid4()), str(uuid.uuid4()), str(uuid.uuid4())
        conn.execute("INSERT INTO tenants VALUES (?,?,?)", (tid, "davis", now))
        conn.execute("INSERT INTO members VALUES (?,?,?,?,?,?,?,?)",
                     (mb, tid, "bradley", "B", 1, 1, 1, now))
        conn.execute("INSERT INTO members VALUES (?,?,?,?,?,?,?,?)",
                     (mj, tid, "jill", "J", 1, 1, 0, now))
        xfers = [str(uuid.uuid4()), str(uuid.uuid4())]
        for i, xfer in enumerate(xfers):
            conn.execute(
                "INSERT INTO transfers (id, tenant_id, from_member, to_member,"
                " src_uuid, rev, payload, created_at) VALUES (?,?,?,?,?,?,?,?)",
                (xfer, tid, mb, mj, f"SRC-{i}", 1, "{}", now))
        conn.commit()
        conn.close()
        return xfers

    def test_migration_preserves_all_rows_and_adds_retagged_column(self):
        seeded_ids = self._seed_pre_migration_db()

        ledger = Ledger(self.path)  # migration runs inside __init__
        self.addCleanup(ledger.close)

        cols = {r["name"] for r in
                ledger.conn.execute("PRAGMA table_info(transfers)")}
        self.assertIn("retagged_at", cols)

        rows = {r["id"]: dict(r) for r in
                ledger.conn.execute("SELECT * FROM transfers")}
        self.assertEqual(set(rows), set(seeded_ids), "no rows lost or added")
        for tid in seeded_ids:
            self.assertIsNone(rows[tid]["retagged_at"],
                              "pre-existing rows report not-yet-retagged")

    def test_migration_is_idempotent_across_reopens(self):
        seeded_ids = self._seed_pre_migration_db()
        ledger1 = Ledger(self.path)
        ledger1.close()
        ledger2 = Ledger(self.path)  # re-open: must be a no-op, not re-migrate
        self.addCleanup(ledger2.close)
        rows = ledger2.conn.execute("SELECT id FROM transfers").fetchall()
        self.assertEqual({r["id"] for r in rows}, set(seeded_ids))

    def test_fresh_db_never_touches_migration_path(self):
        path = os.path.join(self.tmp.name, "fresh.sqlite")
        ledger = Ledger(path)
        self.addCleanup(ledger.close)
        cols = {r["name"] for r in
                ledger.conn.execute("PRAGMA table_info(transfers)")}
        self.assertIn("retagged_at", cols)


if __name__ == "__main__":
    unittest.main()
