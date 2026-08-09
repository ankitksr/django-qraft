"""Tests for qraft.conf module."""

import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from qraft.conf import (
    QraftSettings,
    RetryBackoff,
    RetryDefaultsSettings,
    _cached_conf,
    get_conf,
)


class TestRetryDefaultsSettings:
    """Tests for RetryDefaultsSettings model."""

    def test_default_values(self):
        """Test retry default settings with default values."""
        retry_defaults = RetryDefaultsSettings()

        assert retry_defaults.max_attempts == 3
        assert retry_defaults.delay == 30.0
        assert retry_defaults.backoff == RetryBackoff.EXPONENTIAL
        assert retry_defaults.jitter is True
        assert retry_defaults.jitter_max == 0.2

    def test_custom_values(self):
        """Test retry default settings with custom values."""
        retry_defaults = RetryDefaultsSettings(
            max_attempts=5,
            delay=60.0,
            backoff=RetryBackoff.LINEAR,
            jitter=False,
            jitter_max=0.5,
        )

        assert retry_defaults.max_attempts == 5
        assert retry_defaults.delay == 60.0
        assert retry_defaults.backoff == RetryBackoff.LINEAR
        assert retry_defaults.jitter is False
        assert retry_defaults.jitter_max == 0.5

    def test_validation_max_attempts(self):
        """Test validation of max_attempts."""
        with pytest.raises(ValidationError):
            RetryDefaultsSettings(max_attempts=0)

        with pytest.raises(ValidationError):
            RetryDefaultsSettings(max_attempts=11)

    def test_validation_delay(self):
        """Test validation of delay."""
        with pytest.raises(ValidationError):
            RetryDefaultsSettings(delay=-1.0)

    def test_validation_jitter_max(self):
        """Test validation of jitter_max."""
        with pytest.raises(ValidationError):
            RetryDefaultsSettings(jitter_max=-0.1)

        with pytest.raises(ValidationError):
            RetryDefaultsSettings(jitter_max=1.5)


class TestQraftSettings:
    """Tests for QraftSettings model."""

    def test_default_values(self):
        """Test settings with default values from test configuration."""
        settings = QraftSettings()

        assert settings.threads == 1
        # In tests, max_inflight is set to 2 in test settings
        assert settings.max_inflight in (None, 2)
        assert settings.grace_period in (10.0, 30.0)  # Test config uses 10.0
        assert settings.sync_hooks is False

    @patch("qraft.conf.django_settings")
    def test_get_max_inflight_default(self, mock_django_settings):
        """Test get_max_inflight returns threads * 2 when not set."""
        mock_django_settings.QRAFT_CLUSTER = {"threads": 4}
        settings = QraftSettings()
        assert settings.get_max_inflight() == 8

    @patch("qraft.conf.django_settings")
    def test_get_max_inflight_custom(self, mock_django_settings):
        """Test get_max_inflight returns custom value when set."""
        mock_django_settings.QRAFT_CLUSTER = {"threads": 4, "max_inflight": 16}
        settings = QraftSettings()
        assert settings.get_max_inflight() == 16

    def test_nested_retry_defaults_settings(self):
        """Test nested retry_defaults settings."""
        settings = QraftSettings()

        assert isinstance(settings.retry_defaults, RetryDefaultsSettings)
        assert settings.retry_defaults.max_attempts == 3
        assert settings.retry_defaults.delay == 30.0

    @patch("qraft.conf.django_settings")
    def test_validation_threads(self, mock_django_settings):
        """Test validation of threads."""
        mock_django_settings.QRAFT_CLUSTER = {"threads": 0}
        with pytest.raises(ValidationError):
            QraftSettings()

    @patch("qraft.conf.django_settings")
    def test_validation_max_inflight(self, mock_django_settings):
        """Test validation of max_inflight."""
        mock_django_settings.QRAFT_CLUSTER = {"max_inflight": 0}
        with pytest.raises(ValidationError):
            QraftSettings()

    @patch("qraft.conf.django_settings")
    def test_validation_grace_period(self, mock_django_settings):
        """Test validation of grace_period."""
        mock_django_settings.QRAFT_CLUSTER = {"grace_period": -1.0}
        with pytest.raises(ValidationError):
            QraftSettings()


class TestDjangoSettingsIntegration:
    """Tests for Django settings integration."""

    @patch("qraft.conf.django_settings")
    def test_loads_from_django_settings(self, mock_django_settings):
        """Test loading settings from Django QRAFT_CLUSTER."""
        mock_django_settings.QRAFT_CLUSTER = {
            "threads": 8,
            "max_inflight": 16,
            "sync_hooks": True,
            "retry_defaults": {
                "max_attempts": 5,
                "delay": 60.0,
            },
        }

        settings = QraftSettings()

        assert settings.threads == 8
        assert settings.max_inflight == 16
        assert settings.sync_hooks is True
        assert settings.retry_defaults.max_attempts == 5
        assert settings.retry_defaults.delay == 60.0

    @patch("qraft.conf.django_settings")
    def test_alt_clusters_via_environment(self, mock_django_settings):
        """Test ALT_CLUSTERS selection via Q_CLUSTER_NAME."""
        mock_django_settings.QRAFT_CLUSTER = {
            "threads": 1,
            "max_inflight": 2,
            "ALT_CLUSTERS": {
                "io-workers": {
                    "threads": 8,
                    "max_inflight": 16,
                }
            },
        }

        # Test default cluster
        with patch.dict(os.environ, {}, clear=True):
            settings = QraftSettings()
            assert settings.threads == 1
            assert settings.max_inflight == 2

        # Test io-workers cluster
        with patch.dict(os.environ, {"Q_CLUSTER_NAME": "io-workers"}):
            settings = QraftSettings()
            assert settings.threads == 8
            assert settings.max_inflight == 16

    @patch("qraft.conf.django_settings")
    def test_alt_clusters_missing_cluster_name(self, mock_django_settings):
        """Test ALT_CLUSTERS with non-existent cluster name."""
        mock_django_settings.QRAFT_CLUSTER = {
            "threads": 1,
            "ALT_CLUSTERS": {
                "io-workers": {
                    "threads": 8,
                }
            },
        }

        # Non-existent cluster should use default
        with patch.dict(os.environ, {"Q_CLUSTER_NAME": "non-existent"}):
            settings = QraftSettings()
            assert settings.threads == 1

    @patch("qraft.conf.django_settings")
    def test_alt_clusters_override_defaults(self, mock_django_settings):
        """Test that ALT_CLUSTERS override default settings."""
        mock_django_settings.QRAFT_CLUSTER = {
            "threads": 1,
            "max_inflight": 2,
            "sync_hooks": False,
            "ALT_CLUSTERS": {
                "io-workers": {
                    "threads": 8,
                    # max_inflight not overridden, should use default
                    "sync_hooks": True,
                }
            },
        }

        with patch.dict(os.environ, {"Q_CLUSTER_NAME": "io-workers"}):
            settings = QraftSettings()
            assert settings.threads == 8
            assert settings.max_inflight == 2  # From default
            assert settings.sync_hooks is True  # From alt cluster

    @patch("qraft.conf.django_settings")
    def test_extra_fields_ignored(self, mock_django_settings):
        """Test that extra fields in Django settings are ignored."""
        mock_django_settings.QRAFT_CLUSTER = {
            "threads": 4,
            "unknown_field": "should be ignored",
            "another_unknown": 123,
        }

        # Should not raise validation error
        settings = QraftSettings()
        assert settings.threads == 4
        assert not hasattr(settings, "unknown_field")

    @patch("qraft.conf.django_settings")
    def test_case_insensitive_field_names(self, mock_django_settings):
        """Test that field names are case-insensitive."""
        mock_django_settings.QRAFT_CLUSTER = {
            "THREADS": 4,
            "MAX_INFLIGHT": 8,
            "SYNC_HOOKS": True,
        }

        settings = QraftSettings()
        assert settings.threads == 4
        assert settings.max_inflight == 8
        assert settings.sync_hooks is True


class TestGetConf:
    """Tests for get_conf function."""

    @patch("qraft.conf.django_settings")
    def test_get_conf_respects_environment(self, mock_django_settings):
        """Test that get_conf respects current Q_CLUSTER_NAME."""
        mock_django_settings.QRAFT_CLUSTER = {
            "threads": 1,
            "ALT_CLUSTERS": {
                "io-workers": {
                    "threads": 8,
                }
            },
        }

        # Change environment and get new config
        with patch.dict(os.environ, {"Q_CLUSTER_NAME": "io-workers"}):
            conf = get_conf()
            assert conf.threads == 8

        # Without environment variable
        with patch.dict(os.environ, {}, clear=True):
            conf = get_conf()
            assert conf.threads == 1

    @patch("qraft.conf.django_settings")
    def test_get_conf_returns_cached_instance(self, mock_django_settings):
        """Test that get_conf returns the same cached instance for same cluster."""
        mock_django_settings.QRAFT_CLUSTER = {"threads": 2}
        _cached_conf.cache_clear()

        conf1 = get_conf()
        conf2 = get_conf()

        # LRU cache returns same instance for same cluster name
        assert conf1 is conf2
        assert conf1.threads == 2


class TestRetryBackoff:
    """Tests for RetryBackoff enum."""

    def test_enum_values(self):
        """Test retry backoff enum values."""
        assert RetryBackoff.EXPONENTIAL.value == "exponential"
        assert RetryBackoff.LINEAR.value == "linear"
        assert RetryBackoff.FIXED.value == "fixed"

    def test_enum_membership(self):
        """Test checking enum membership."""
        assert "exponential" in {strategy.value for strategy in RetryBackoff}
        assert "linear" in {strategy.value for strategy in RetryBackoff}
        assert "fixed" in {strategy.value for strategy in RetryBackoff}
        assert "invalid" not in {strategy.value for strategy in RetryBackoff}


class TestRetentionInheritance:
    """Retention resolution against an explicit Q_CLUSTER['save_limit']."""

    @pytest.fixture(autouse=True)
    def _fresh_conf(self, settings):
        """get_conf is cached per cluster name, so each case needs a clean slate."""
        settings.QRAFT_CLUSTER = {}
        settings.Q_CLUSTER = {"name": "test", "orm": "default"}
        _cached_conf.cache_clear()
        yield
        _cached_conf.cache_clear()

    def test_silent_by_default(self, settings):
        """Django-Q2's own default of 250 must never be mirrored."""
        conf = get_conf()

        assert conf.retention_max_tasks is None
        assert conf.retention_days is None
        assert conf.retention_enabled() is False

    def test_explicit_save_limit_is_inherited_as_a_count_bound(self, settings):
        settings.Q_CLUSTER = {**settings.Q_CLUSTER, "save_limit": 1000}

        conf = get_conf()

        assert conf.retention_max_tasks == 1000
        assert conf.retention_inherited_from_save_limit is True
        assert conf.retention_days is None
        assert conf.retention_enabled() is True

    @pytest.mark.parametrize("save_limit", [0, -1])
    def test_unlimited_and_never_saved_do_not_bound_qraft(self, settings, save_limit):
        """0 is Django-Q2's unlimited; a negative saves no results at all."""
        settings.Q_CLUSTER = {**settings.Q_CLUSTER, "save_limit": save_limit}

        conf = get_conf()

        assert conf.retention_max_tasks is None
        assert conf.retention_enabled() is False

    def test_explicit_retention_days_wins(self, settings):
        settings.Q_CLUSTER = {**settings.Q_CLUSTER, "save_limit": 1000}
        settings.QRAFT_CLUSTER = {"retention_days": 30}

        conf = get_conf()

        assert conf.retention_days == 30
        assert conf.retention_max_tasks is None
        assert conf.retention_inherited_from_save_limit is False

    def test_explicit_retention_max_tasks_wins(self, settings):
        settings.Q_CLUSTER = {**settings.Q_CLUSTER, "save_limit": 1000}
        settings.QRAFT_CLUSTER = {"retention_max_tasks": 25}

        conf = get_conf()

        assert conf.retention_max_tasks == 25
        assert conf.retention_inherited_from_save_limit is False

    def test_alt_cluster_save_limit_is_inherited(self, settings):
        settings.Q_CLUSTER = {
            **settings.Q_CLUSTER,
            "save_limit": 1000,
            "ALT_CLUSTERS": {"io-workers": {"save_limit": 50}},
        }

        with patch.dict(os.environ, {"Q_CLUSTER_NAME": "io-workers"}):
            _cached_conf.cache_clear()
            conf = get_conf()

        assert conf.retention_max_tasks == 50

    def test_alt_cluster_retention_days_still_wins(self, settings):
        settings.Q_CLUSTER = {**settings.Q_CLUSTER, "save_limit": 1000}
        settings.QRAFT_CLUSTER = {
            "ALT_CLUSTERS": {"io-workers": {"retention_days": 7}},
        }

        with patch.dict(os.environ, {"Q_CLUSTER_NAME": "io-workers"}):
            _cached_conf.cache_clear()
            conf = get_conf()

        assert conf.retention_days == 7
        assert conf.retention_max_tasks is None
