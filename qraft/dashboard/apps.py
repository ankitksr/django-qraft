from django.apps import AppConfig


class QraftDashboardConfig(AppConfig):
    """Bundled monitoring dashboard; no models, templates only."""

    name = "qraft.dashboard"
    label = "qraft_dashboard"
    verbose_name = "Qraft Dashboard"
