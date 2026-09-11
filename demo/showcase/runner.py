"""
Scenario runner.

Boots the clusters the selected scenarios need, runs each one, records the
result, and tears the clusters down again. The same entry point serves the
command line and the dashboard's "run" button.
"""

import importlib
import time
import traceback
import uuid

from django.utils import timezone

from showcase.clusters import ClusterManager
from showcase.harness import (
    REGISTRY,
    Ctx,
    Scenario,
    ScenarioFailed,
    ScenarioSkipped,
    ordered_scenarios,
)
from showcase.models import ScenarioRun

_MODULES = (
    "showcase.scenarios.core",
    "showcase.scenarios.workflows",
    "showcase.scenarios.graphs",
    "showcase.scenarios.durability",
    "showcase.scenarios.ai",
    "showcase.scenarios.djangotasks",
    "showcase.scenarios.bench",
)


def load() -> dict[str, Scenario]:
    """Import every scenario module so the registry is populated."""
    for module in _MODULES:
        importlib.import_module(module)
    return REGISTRY


def select(keys: list[str] | None, group: str | None) -> list[Scenario]:
    """Resolve a selection to scenarios, in reading order."""
    load()
    scenarios = ordered_scenarios()
    if group:
        scenarios = [item for item in scenarios if item.group == group]
    if keys:
        wanted = set(keys)
        unknown = wanted - set(REGISTRY)
        if unknown:
            raise KeyError(f"Unknown scenario(s): {', '.join(sorted(unknown))}")
        scenarios = [item for item in scenarios if item.key in wanted]
    return scenarios


def required_clusters(scenarios: list[Scenario]) -> list[str]:
    """Clusters to boot up front, in a stable order."""
    needed: list[str] = []
    for item in scenarios:
        for name in item.clusters:
            if name not in needed:
                needed.append(name)
    return needed


def run_one(item: Scenario, clusters: ClusterManager, log=print) -> ScenarioRun:
    """Execute one scenario and persist its result."""
    run_id = uuid.uuid4().hex[:12]
    record = ScenarioRun.objects.create(
        id=uuid.uuid4(), key=item.key, group=item.group, title=item.title, run=run_id
    )

    def flush(ctx: Ctx) -> None:
        record.checks = [check.as_dict() for check in ctx.checks]
        record.notes = list(ctx.notes)
        record.save(update_fields=["checks", "notes"])

    ctx = Ctx(run=run_id, clusters=clusters, on_event=flush)
    log(f"\n▶ {item.key} — {item.title}")
    started = time.monotonic()
    status, error = ScenarioRun.PASSED, ""

    try:
        item.func(ctx)
    except ScenarioSkipped as skipped:
        status, error = ScenarioRun.SKIPPED, str(skipped)
    except ScenarioFailed as failed:
        status, error = ScenarioRun.FAILED, str(failed)
    except Exception:
        status, error = ScenarioRun.ERROR, traceback.format_exc()

    if status == ScenarioRun.PASSED:
        if not ctx.checks:
            status = ScenarioRun.ERROR
            error = "scenario recorded no checks, so it proved nothing"
        elif any(not check.ok for check in ctx.checks):
            status = ScenarioRun.FAILED

    record.checks = [check.as_dict() for check in ctx.checks]
    record.notes = list(ctx.notes)
    record.status = status
    record.error = error
    record.duration_ms = int((time.monotonic() - started) * 1000)
    record.finished_at = timezone.now()
    record.save()

    for check in ctx.checks:
        mark = "✓" if check.ok else "✗"
        detail = f"  — {check.detail}" if check.detail and not check.ok else ""
        log(f"   {mark} {check.name}{detail}")
    for note in ctx.notes:
        log(f"   · {note}")
    if error and status != ScenarioRun.SKIPPED:
        log(f"   ! {error.strip().splitlines()[-1]}")

    return record


def run_all(
    scenarios: list[Scenario],
    log=print,
    keep_clusters: bool = False,
    clusters: ClusterManager | None = None,
) -> list[ScenarioRun]:
    """Boot what is needed, run the scenarios, then stop the clusters."""
    owned = clusters is None
    manager = clusters or ClusterManager(log=lambda message: log(f"   {message}"))

    # A scenario that manages a cluster itself needs it down beforehand.
    manual = {name for item in scenarios for name in item.manual_clusters}
    boot = [name for name in required_clusters(scenarios) if name not in manual]

    results: list[ScenarioRun] = []
    try:
        if boot:
            log(f"Starting clusters: {', '.join(boot)}")
            manager.ensure(boot)
        for item in scenarios:
            # Manual profiles must be idle each time, including when the
            # supplied manager came from an already-running demo server.
            for name in item.manual_clusters:
                manager.stop(name)
            results.append(run_one(item, manager, log=log))
    finally:
        for name in manual:
            manager.stop(name)
        if owned and not keep_clusters:
            manager.stop_all()
    return results


def matrix(results: list[ScenarioRun]) -> str:
    """The PASS/FAIL table printed at the end of a run."""
    mark = {
        ScenarioRun.PASSED: "PASS",
        ScenarioRun.FAILED: "FAIL",
        ScenarioRun.ERROR: "ERROR",
        ScenarioRun.SKIPPED: "SKIP",
        ScenarioRun.RUNNING: "?",
    }
    width = max([len(item.key) for item in results] + [8])
    lines = [
        f"{'SCENARIO'.ljust(width)}  RESULT  CHECKS   TIME",
        f"{'-' * width}  ------  ------  -----",
    ]
    group = None
    for item in results:
        if item.group != group:
            group = item.group
            lines.insert(len(lines), f"[{group}]")
        checks = f"{item.passed_checks}/{len(item.checks)}"
        seconds = (item.duration_ms or 0) / 1000
        lines.append(
            f"{item.key.ljust(width)}  {mark[item.status]:<6}  "
            f"{checks:>6}  {seconds:5.1f}s"
        )

    failed = [
        item
        for item in results
        if item.status in (ScenarioRun.FAILED, ScenarioRun.ERROR)
    ]
    total_checks = sum(len(item.checks) for item in results)
    passed_checks = sum(item.passed_checks for item in results)
    lines.append("")
    passed = sum(item.status == ScenarioRun.PASSED for item in results)
    skipped = sum(item.status == ScenarioRun.SKIPPED for item in results)
    lines.append(
        f"{passed}/{len(results)} scenarios passed ({skipped} skipped), "
        f"{passed_checks}/{total_checks} checks passed"
    )
    if failed:
        lines.append("Failed: " + ", ".join(item.key for item in failed))
    return "\n".join(lines)


def exit_code(results: list[ScenarioRun]) -> int:
    return (
        1
        if any(
            item.status in (ScenarioRun.FAILED, ScenarioRun.ERROR) for item in results
        )
        else 0
    )
