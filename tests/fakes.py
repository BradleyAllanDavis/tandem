"""In-memory fakes for spoke tests and the invariant simulation:
a FakeThings 'account' plus ThingsReader/ThingsWriter implementations
over it, with failure-injection hooks (crash-after-fire, verify failure).
"""

from __future__ import annotations

import itertools
import time

_counter = itertools.count(1)


class FakeClock:
    """Deterministic, test-controlled clock — injected as Ledger's `now_fn`
    for any test asserting timing (retry backoff / lease eligibility)
    instead of sleeping past a real threshold. A real-time sleep racing a
    small backoff floor is inherently flaky under CI-runner scheduling
    variance (confirmed 2026-08-20: PR #2's ubuntu-latest leg failed
    test_nack_requeues this exact way, un-reproducible locally since it's
    a genuine race, not a version or platform difference) — advancing a
    fake clock makes "not yet eligible" vs "eligible" assertions exact,
    with zero real elapsed time and zero flake risk."""

    def __init__(self, start: float = None):
        self.t = time.time() if start is None else start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class CrashAfterFire(Exception):
    """Injected: the write reached Things, the spoke died before ack."""


class FakeThings:
    """One member's Things account: uuid -> todo dict."""

    def __init__(self, name: str):
        self.name = name
        self.todos = {}
        self.tags = set()  # pre-created tag vocabulary

    def add(self, title, tags=(), notes="", checklist=(), when=None,
            deadline=None, is_repeating=False, type_=0):
        uuid = f"{self.name}-{next(_counter)}"
        self.todos[uuid] = {
            "uuid": uuid, "title": title, "notes": notes,
            "checklist": list(checklist), "tags": list(tags),
            "when": when, "deadline": deadline, "status": "open",
            "trashed": False, "created": time.time(),
            "is_repeating": is_repeating, "type": type_,
        }
        return uuid

    def complete(self, uuid):
        self.todos[uuid]["status"] = "completed"

    def cancel(self, uuid):
        self.todos[uuid]["status"] = "canceled"

    def trash(self, uuid):
        self.todos[uuid]["trashed"] = True

    def count_titled(self, title):
        return sum(1 for t in self.todos.values()
                   if t["title"] == title and not t["trashed"])


class FakeReader:
    def __init__(self, things: FakeThings):
        self.things = things
        self.refresh_calls = 0
        # Injection: force the next N correlate() calls to report "not
        # found" even when a matching row exists — simulates a genuinely
        # stuck correlation (title-transformation mismatch, mirror lag)
        # rather than a crash. Decrements on every call while > 0.
        self.fail_correlate_times = 0

    def refresh(self):
        self.refresh_calls += 1

    def outbound_candidates(self, trigger_titles):
        triggers = set(t.lower() for t in trigger_titles)
        out = []
        for t in self.things.todos.values():
            if t["trashed"] or t["status"] != "open":
                continue
            if any(tag.lower() in triggers for tag in t["tags"]):
                out.append(dict(t))
        return out

    def status(self, uuid):
        t = self.things.todos.get(uuid)
        if t is None:
            return None
        return {"status": t["status"], "trashed": t["trashed"]}

    def tags_of(self, uuid):
        t = self.things.todos.get(uuid)
        return None if t is None else list(t["tags"])

    def correlate(self, title, created_after, provenance_tag, exclude_uuids):
        if self.fail_correlate_times > 0:
            self.fail_correlate_times -= 1
            return None
        matches = sorted(
            (t for t in self.things.todos.values()
             if t["title"] == title and t["created"] >= created_after
             and provenance_tag in t["tags"] and not t["trashed"]
             and t["uuid"] not in exclude_uuids),
            key=lambda t: t["created"])
        return matches[0]["uuid"] if matches else None


class FakeWriter:
    """Applies to a FakeThings account the way the real writers apply to
    real Things — honoring the pre-created-tags-only rule (unknown tags
    are silently dropped, exactly like the URL scheme)."""

    def __init__(self, things: FakeThings):
        self.things = things
        self.creates = 0
        self.crash_next_create = False   # inject: fire then die before ack
        self.fail_next_terminal = False  # inject: verify failure

    def create(self, envelope, provenance_tag, idem_key=None):
        tags = [provenance_tag] if provenance_tag in self.things.tags else []
        self.things.add(
            envelope["title"], tags=tags, notes=envelope.get("notes", ""),
            checklist=envelope.get("checklist", ()),
            when=envelope.get("when"), deadline=envelope.get("deadline"))
        self.creates += 1
        if self.crash_next_create:
            self.crash_next_create = False
            raise CrashAfterFire("spoke died after firing the create")

    def set_terminal(self, uuid, state):
        if self.fail_next_terminal:
            self.fail_next_terminal = False
            return False
        t = self.things.todos.get(uuid)
        if t is None:
            return False
        t["status"] = "completed" if state == "completed" else "canceled"
        return True

    def set_tags(self, uuid, tags):
        t = self.things.todos.get(uuid)
        if t is None:
            return False
        t["tags"] = [tag for tag in tags if tag in self.things.tags]
        # real Things drops unknown tags silently; a retag to a tag that
        # doesn't exist "succeeds" at the URL level but verification would
        # catch the difference — mimic verified-success only when all landed
        return set(t["tags"]) == set(tags)

    def set_tags_and_terminal(self, uuid, tags, state):
        if self.fail_next_terminal:
            self.fail_next_terminal = False
            return False
        t = self.things.todos.get(uuid)
        if t is None:
            return False
        t["tags"] = [tag for tag in tags if tag in self.things.tags]
        t["status"] = "completed" if state == "completed" else "canceled"
        return set(t["tags"]) == set(tags)


class FlakyHub:
    """Wraps a HubClient, dropping the response of selected calls (the
    call SUCCEEDS hub-side, the spoke never hears back — the lost-ack /
    lost-response failure class)."""

    def __init__(self, inner):
        self.inner = inner
        self.drop_next = set()  # method names whose next response vanishes

    def _call(self, name, *args, **kwargs):
        result = getattr(self.inner, name)(*args, **kwargs)
        if name in self.drop_next:
            self.drop_next.discard(name)
            raise ConnectionError(f"response to {name} lost in transit")
        return result

    def push_transfer(self, *a, **k):
        return self._call("push_transfer", *a, **k)

    def deliveries(self, *a, **k):
        return self._call("deliveries", *a, **k)

    def ack(self, *a, **k):
        return self._call("ack", *a, **k)

    def nack(self, *a, **k):
        return self._call("nack", *a, **k)

    def watch(self, *a, **k):
        return self._call("watch", *a, **k)

    def observe(self, *a, **k):
        return self._call("observe", *a, **k)

    def mark_retagged(self, *a, **k):
        return self._call("mark_retagged", *a, **k)
