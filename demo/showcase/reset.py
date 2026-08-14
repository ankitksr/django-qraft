"""
Full wipe of demo state, for a fresh start.

Deliberately not Django's `flush`: that truncates every table on the
connection, including auth/admin/session, which would sign out anyone using
the "qraft admin" link. This clears only the rows the dashboard panels read -
qraft's own tables, django-q2's broker/task tables, and showcase's own
bookkeeping - and leaves auth, admin, and migration history untouched.

Shared by `manage.py demo` (which reset the database before every suite run)
and the dashboard's reset button, so there is exactly one place this list
can go stale.
"""

from django.db import transaction
from django_q.models import OrmQ, Schedule, Task

from qraft.models import (
    HookDispatch,
    QraftBatchModel,
    QraftChainModel,
    QraftChainStep,
    QraftIterModel,
    QraftTask,
    QraftTaskAttempt,
    RateBucket,
    WorkflowHookDispatch,
)
from showcase.models import Control, Event, ScenarioRun


def reset_state() -> None:
    """Delete every row a dashboard panel can show."""
    with transaction.atomic():
        QraftChainStep.objects.all().delete()
        QraftChainModel.objects.all().delete()
        QraftIterModel.objects.all().delete()
        QraftBatchModel.objects.all().delete()
        HookDispatch.objects.all().delete()
        WorkflowHookDispatch.objects.all().delete()
        QraftTaskAttempt.objects.all().delete()
        QraftTask.objects.all().delete()
        RateBucket.objects.all().delete()
        OrmQ.objects.all().delete()
        Schedule.objects.all().delete()
        Task.objects.all().delete()  # covers the Success/Failure proxy models too
        Event.objects.all().delete()
        Control.objects.all().delete()
        ScenarioRun.objects.all().delete()
