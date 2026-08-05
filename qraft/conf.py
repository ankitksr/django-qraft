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

    max_attempts: int = Field(3, ge=1, le=10, description="Maximum retry attempts")
    delay: float = Field(30.0, ge=0, description="Initial retry delay in seconds")
    backoff: RetryBackoff = Field(
        RetryBackoff.EXPONENTIAL, description="Retry backoff strategy"
    )
    jitter: bool = Field(True, description="Add jitter to retry delays")
    jitter_max: float = Field(
        0.2, ge=0, le=1, description="Maximum jitter as fraction of delay"
    )


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
        qraft_settings = getattr(django_settings, "QRAFT_CLUSTER", {}).copy()

        # Check for ALT_CLUSTERS selection via environment
        cluster_name = os.getenv("Q_CLUSTER_NAME")
        if cluster_name:
            alt_clusters = qraft_settings.pop("ALT_CLUSTERS", {})
            if isinstance(alt_clusters, dict) and cluster_name in alt_clusters:
                alt_conf = alt_clusters[cluster_name]
                if isinstance(alt_conf, dict):
                    qraft_settings.update(alt_conf)

        data.update({k.lower(): v for k, v in qraft_settings.items()})

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
        description="Seconds a RUNNING attempt may go without a Q2 result "
        "before it is treated as orphaned",
    )

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


# Global settings instance (loaded at import time)
# Note: For ALT_CLUSTERS support, use get_conf() after setting Q_CLUSTER_NAME
conf = QraftSettings()
