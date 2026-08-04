from django.apps import AppConfig


class QraftConfig(AppConfig):
    """Qraft app config"""

    default_auto_field = "django.db.models.BigAutoField"
    name = "qraft"
    verbose_name = "Django Qraft"

    def ready(self):
        from django_q.signals import pre_execute

        from qraft.context import _on_pre_execute

        pre_execute.connect(_on_pre_execute, dispatch_uid="qraft_pre_execute_context")
