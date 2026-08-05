from django.apps import AppConfig


class QraftConfig(AppConfig):
    """Qraft app config"""

    default_auto_field = "django.db.models.BigAutoField"
    name = "qraft"
    verbose_name = "Django Qraft"

    def ready(self):
        from django_q.signals import post_execute, pre_execute

        from qraft.context import _on_pre_execute
        from qraft.lease import _on_post_execute, _on_pre_execute_lease

        pre_execute.connect(_on_pre_execute, dispatch_uid="qraft_pre_execute_context")
        pre_execute.connect(
            _on_pre_execute_lease, dispatch_uid="qraft_pre_execute_lease"
        )
        post_execute.connect(_on_post_execute, dispatch_uid="qraft_post_execute_lease")
