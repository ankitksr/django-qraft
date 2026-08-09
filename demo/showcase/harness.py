"""
Scenario harness.

A scenario is a function that drives qraft and then asserts what qraft did.
Every assertion goes through `Ctx.check`, so a scenario that prints activity
but proves nothing reports zero checks and cannot pass.

Timing is always a poll with a deadline, never a sleep-and-hope.
"""

import time
from dataclasses import dataclass, field
from typing import Callable

from showcase.models import Event


class ScenarioFailed(Exception):
    """Raised by `require` when a precondition for the rest of the scenario fails."""


class ScenarioSkipped(Exception):
    """Raised when a scenario cannot run here, and says why."""


@dataclass
class Check:
    name: str
    ok: bool
    detail: str = ""

    def as_dict(self) -> dict:
        return {"name": self.name, "ok": self.ok, "detail": self.detail}


@dataclass
class Scenario:
    key: str
    group: str
    title: str
    proves: str
    clusters: tuple[str, ...]
    func: Callable
    # Clusters this scenario starts and stops itself, so the runner must leave
    # them down beforehand (the priority scenario needs a full queue and an
    # idle cluster at the same time).
    manual_clusters: tuple[str, ...] = ()


REGISTRY: dict[str, Scenario] = {}
GROUP_ORDER = ("core", "workflows", "durability", "ai", "django-tasks")


def scenario(
    key: str,
    *,
    group: str,
    title: str,
    proves: str,
    clusters: tuple[str, ...] = ("default",),
    manual_clusters: tuple[str, ...] = (),
):
    """Register a scenario under `key`."""

    def decorate(func):
        REGISTRY[key] = Scenario(
            key=key,
            group=group,
            title=title,
            proves=proves,
            clusters=clusters,
            func=func,
            manual_clusters=manual_clusters,
        )
        return func

    return decorate


def ordered_scenarios() -> list[Scenario]:
    """Every scenario, grouped in reading order."""
    return sorted(
        REGISTRY.values(),
        key=lambda s: (
            GROUP_ORDER.index(s.group) if s.group in GROUP_ORDER else 99,
            s.key,
        ),
    )


@dataclass
class Ctx:
    """What a scenario is handed: an id to tag its evidence with, and a scorer."""

    run: str
    clusters: object = None
    checks: list[Check] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    on_event: Callable | None = None

    # --- scoring ---

    def check(self, name: str, ok: bool, detail: str = "") -> bool:
        """Record a pass/fail. Returns `ok` so a caller can branch on it."""
        self.checks.append(Check(name, bool(ok), detail))
        if self.on_event:
            self.on_event(self)
        return bool(ok)

    def equals(self, name: str, actual, expected) -> bool:
        return self.check(
            name, actual == expected, f"expected {expected!r}, got {actual!r}"
        )

    def between(self, name: str, actual: float, low: float, high: float) -> bool:
        return self.check(
            name,
            low <= actual <= high,
            f"expected {low:g}..{high:g}, got {actual:.2f}",
        )

    def require(self, name: str, ok: bool, detail: str = "") -> None:
        """A check the rest of the scenario depends on. Stops the run if it fails."""
        if not self.check(name, ok, detail):
            raise ScenarioFailed(f"{name}: {detail}")

    def note(self, message: str) -> None:
        """Context for a reader. Never counts as evidence."""
        self.notes.append(message)
        if self.on_event:
            self.on_event(self)

    def skip(self, reason: str) -> None:
        raise ScenarioSkipped(reason)

    # --- waiting ---

    def poll(self, predicate: Callable, timeout: float, interval: float = 0.25):
        """
        Call `predicate` until it returns something truthy or `timeout` passes.

        Returns the truthy value, or None on timeout. Never sleeps blind.
        """
        deadline = time.monotonic() + timeout
        while True:
            value = predicate()
            if value:
                return value
            if time.monotonic() >= deadline:
                return None
            time.sleep(interval)

    def wait(
        self,
        name: str,
        predicate: Callable,
        timeout: float,
        detail: str = "",
        interval: float = 0.25,
    ):
        """Poll for a condition and record whether it arrived in time."""
        started = time.monotonic()
        value = self.poll(predicate, timeout, interval)
        elapsed = time.monotonic() - started
        suffix = f"after {elapsed:.1f}s" if value else f"timed out after {timeout:.0f}s"
        self.check(name, value is not None, f"{detail} ({suffix})".strip())
        return value

    def settle(self, seconds: float) -> None:
        """
        Deliberate quiet period.

        Only used to prove a *negative* ("nothing more happened"), which is the
        one thing a poll cannot establish.
        """
        time.sleep(seconds)

    # --- evidence ---

    def events(self, **filters):
        """Events this scenario's own tasks and hooks recorded."""
        return Event.objects.filter(run=self.run, **filters)

    def names(self, kind: str | None = None) -> list[str]:
        """Recorded event names in the order they happened."""
        qs = self.events() if kind is None else self.events(kind=kind)
        return list(qs.values_list("name", flat=True))

    def count(self, **filters) -> int:
        return self.events(**filters).count()
