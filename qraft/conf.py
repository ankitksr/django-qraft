"""
Settings management for Django-Qraft.

Configuration is loaded from the QRAFT_CLUSTER dict in Django settings.
Supports ALT_CLUSTERS for running mixed worker pools (process + threaded).
"""

import os
from enum import Enum
from functools import lru_cache
from typing import Any

from django.conf import settings as django_settings
from pydantic import BaseModel, ConfigDict, Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource


class RetryBackoff(str, Enum):
    """Retry backoff strategies."""

    EXPONENTIAL = "exponential"
    LINEAR = "linear"
    FIXED = "fixed"


class RetryDefaultsSettings(BaseModel):
    """Default retry configuration settings (can be overridden per-task)."""

    max_attempts: int = Field(
        3, ge=1, le=10, description="Total executions including the first, not retries"
    )
    delay: float = Field(30.0, ge=0, description="Initial retry delay in seconds")
    backoff: RetryBackoff = Field(
        RetryBackoff.EXPONENTIAL, description="Retry backoff strategy"
    )
    jitter: bool = Field(True, description="Add jitter to retry delays")
    jitter_max: float = Field(
        0.2, ge=0, le=1, description="Maximum jitter as fraction of delay"
    )


# Settings a cluster owns rather than inherits. `metrics_gauges` names the one
# process that reports the fleet-wide backlog; inheriting it from the base
# entry would make every ALT_CLUSTERS cluster a second reporter of the same
# numbers, which is what a consumer would sum.
NOT_INHERITED_BY_ALT_CLUSTERS = {"metrics_gauges": False}


def _merge_alt_cluster(config: dict[str, Any], cluster_name: str | None) -> dict:
    """
    Overlay the ALT_CLUSTERS entry for `cluster_name` onto a settings dict.

    Keys in `NOT_INHERITED_BY_ALT_CLUSTERS` fall back to their stated default
    for an alt cluster that does not set them itself.
    """
    merged = config.copy()
    alt_clusters = merged.pop("ALT_CLUSTERS", None)
    if cluster_name and isinstance(alt_clusters, dict):
        alt_conf = alt_clusters.get(cluster_name)
        if isinstance(alt_conf, dict):
            declared = {key.lower() for key in alt_conf}
            for key, default in NOT_INHERITED_BY_ALT_CLUSTERS.items():
                if key not in declared:
                    merged = {
                        name: value
                        for name, value in merged.items()
                        if name.lower() != key
                    }
                    merged[key] = default
            merged.update(alt_conf)
    return merged


def _inherited_save_limit(cluster_name: str | None) -> int | None:
    """
    Count-based bound implied by an explicit Q_CLUSTER["save_limit"].

    Reading the raw dict rather than Conf.SAVE_LIMIT is what separates a
    stated intent from Django-Q2's own default of 250; mirroring the default
    would silently destroy the history of every user who never asked for
    pruning.
    """
    q_cluster = getattr(django_settings, "Q_CLUSTER", {})
    if not isinstance(q_cluster, dict):
        return None

    q_cluster = _merge_alt_cluster(q_cluster, cluster_name)
    save_limit = q_cluster.get("save_limit")
    if not isinstance(save_limit, int) or isinstance(save_limit, bool):
        return None

    # 0 is Django-Q2's "keep everything"; a negative value means it saves no
    # successful results at all, which carries no count to mirror.
    return save_limit if save_limit > 0 else None


class DjangoSettingsSource(PydanticBaseSettingsSource):
    """Custom settings source that loads from QRAFT_CLUSTER in Django settings."""

    def get_field_value(self, field, field_name: str) -> tuple[Any, str, bool]:
        """
        Get value for a specific field from Django settings.

        Required by PydanticBaseSettingsSource abstract interface.
        Returns (value, field_name, is_complex).
        """
        # We handle all fields in __call__, so return None here
        return None, field_name, False

    def __call__(self) -> dict[str, Any]:
        """Load all settings from Django configuration."""
        data: dict[str, Any] = {}

        # Load from QRAFT_CLUSTER settings
        qraft_settings = getattr(django_settings, "QRAFT_CLUSTER", {})
        if not isinstance(qraft_settings, dict):
            qraft_settings = {}

        # Check for ALT_CLUSTERS selection via environment
        cluster_name = os.getenv("Q_CLUSTER_NAME")
        qraft_settings = _merge_alt_cluster(qraft_settings, cluster_name)

        data.update({k.lower(): v for k, v in qraft_settings.items()})

        # A user who set save_limit has already said task history should be
        # bounded; Qraft's tables should not grow forever beside a Q2 table
        # that is being trimmed. Any retention key written in QRAFT_CLUSTER is
        # a Qraft-specific answer to the same question and always wins.
        if "retention_days" not in data and "retention_max_tasks" not in data:
            inherited = _inherited_save_limit(cluster_name)
            if inherited is not None:
                data["retention_max_tasks"] = inherited
                data["retention_inherited_from_save_limit"] = True

        return data


class QraftSettings(BaseSettings):
    """Settings for Django-Qraft, loaded from QRAFT_CLUSTER."""

    model_config = ConfigDict(extra="ignore")

    # Nested retry default settings
    retry_defaults: RetryDefaultsSettings = Field(default_factory=RetryDefaultsSettings)

    # Threading settings for multithreaded workers
    threads: int = Field(
        1,
        ge=1,
        description="Threads per worker process (1=disabled, uses standard worker)",
    )
    max_inflight: int | None = Field(
        None,
        ge=1,
        description="Max concurrent tasks per worker process (default: threads * 2)",
    )
    grace_period: float = Field(
        30.0, ge=0, description="Seconds to wait for in-flight threads on shutdown"
    )

    # Hook execution settings
    sync_hooks: bool = Field(
        False, description="Run hooks synchronously in monitor (legacy behavior)"
    )

    # Orphan reaper settings
    reap_interval: float = Field(
        60.0, gt=0, description="Seconds between orphan reaper sweeps in the sentinel"
    )
    reap_stale_after: float = Field(
        3600.0,
        gt=0,
        description="Seconds an attempt that never started may sit unresolved "
        "before it is treated as orphaned",
    )
    heartbeat_interval: float = Field(
        30.0,
        gt=0,
        description="Seconds between execution-lease heartbeats from a running "
        "worker; the reaper reaps on a heartbeat older than "
        "max(3 * heartbeat_interval, min_heartbeat_grace)",
    )
    min_heartbeat_grace: float = Field(
        90.0,
        gt=0,
        description="Floor on the heartbeat grace period, so a short "
        "heartbeat_interval cannot make the reaper trigger-happy on a "
        "briefly-paused worker. Lower it deliberately when the tasks are "
        "short enough that 90s of lost work costs more than a rare "
        "false reap",
    )
    max_executions_per_attempt: int = Field(
        1,
        ge=1,
        description="How many times one attempt may be handed to a worker. "
        "The default 1 means a broker redelivery of an attempt that already "
        "started is refused and resolved as RedeliveredAttempt, so the retry "
        "policy decides the next attempt instead of the delivery loop",
    )

    # Scheduler dispatcher settings
    dispatch_interval: float = Field(
        0.5,
        gt=0,
        description="Ceiling on how long the scheduler dispatcher sleeps "
        "between passes; it wakes earlier when an attempt comes due sooner, "
        "so this bounds idle poll load rather than retry latency",
    )
    dispatch_batch: int = Field(
        50,
        ge=1,
        description="Maximum due attempts one dispatcher pass enqueues, so a "
        "large backlog cannot hold the loop in a single pass",
    )

    # Retention sweep settings
    retention_days: float | None = Field(
        None,
        gt=0,
        description="Days to keep terminal Qraft rows before pruning them "
        "(None disables the sweep and keeps history forever)",
    )
    retention_max_tasks: int | None = Field(
        None,
        ge=1,
        description="Keep only the newest N settled QraftTasks (count-based "
        "bound; inherited from an explicit Q_CLUSTER['save_limit'] when no "
        "Qraft retention setting is given)",
    )
    retention_inherited_from_save_limit: bool = Field(
        False,
        description="Whether retention_max_tasks came from Q_CLUSTER rather "
        "than from QRAFT_CLUSTER",
    )
    retention_interval: float = Field(
        3600.0, gt=0, description="Seconds between retention sweeps"
    )
    retention_batch_size: int = Field(
        500,
        ge=1,
        description="Rows deleted per transaction, so a first sweep over a "
        "large table does not hold one long lock",
    )

    # Observability
    metrics_sink: str = Field(
        "qraft.metrics.NullSink",
        description="Dotted path to the metrics Sink class; resolved once per process",
    )
    metrics_gauges: bool = Field(
        False,
        description="Emit queue/backlog gauges from this cluster's dispatcher loop. "
        "Set on exactly one cluster, or every replica reports the same backlog",
    )
    progress_min_interval: float = Field(
        0.0,
        ge=0,
        description="Seconds between progress writes from one attempt; a call "
        "inside the interval is skipped unless current/total changed or "
        "force=True. 0 writes every call",
    )

    def retention_enabled(self) -> bool:
        """Whether any retention bound is in force, age-based or count-based."""
        return bool(self.retention_days or self.retention_max_tasks)

    def get_max_inflight(self) -> int:
        """Return max_inflight, defaulting to threads * 2 if not set."""
        if self.max_inflight is not None:
            return self.max_inflight
        return self.threads * 2

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Use only Django settings as the source."""
        return (DjangoSettingsSource(settings_cls),)


@lru_cache(maxsize=8)
def _cached_conf(cluster_name: str | None) -> QraftSettings:
    """Cache settings instances per cluster name."""
    return QraftSettings()


def get_conf() -> QraftSettings:
    """
    Get Qraft settings, respecting current Q_CLUSTER_NAME environment variable.

    Uses LRU cache keyed on cluster name to avoid re-creating settings objects
    on every call while still supporting ALT_CLUSTERS.
    """
    return _cached_conf(os.getenv("Q_CLUSTER_NAME"))


def executing_cluster() -> str:
    """
    Name of the cluster this process belongs to.

    Follow-up work a cluster queues for itself - hooks from the monitor,
    retry Schedules - must land back on that same cluster, or it strands
    whenever the default cluster is not running. Django-Q2 resolves an
    omitted `cluster=` against this same value, so naming it explicitly is
    what keeps the routing intact once a broker override is also in play.
    """
    from django_q.conf import Conf

    return Conf.CLUSTER_NAME


# Global settings instance (loaded at import time)
# Note: For ALT_CLUSTERS support, use get_conf() after setting Q_CLUSTER_NAME
conf = QraftSettings()
