"""
Scheduling state on the attempt row.

Safe to apply while the previous release is still running: every added column
is nullable, and `state` carries a real database default ("queued") rather
than only a Python one, so inserts from code that predates the column still
satisfy the NOT NULL constraint. "queued" is the correct reading for them -
before this migration an attempt row was only ever created at enqueue time.
Dropping NOT NULL from `q2_task_id` is a catalog-only change on PostgreSQL.
The one operation that takes a write lock is the new index; on a large live
table, create it with CONCURRENTLY by hand and `--fake` that step.
"""

import django.core.serializers.json
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('qraft', '0005_execution_lease'),
    ]

    operations = [
        migrations.AddField(
            model_name='qrafttaskattempt',
            name='claimed_at',
            field=models.DateTimeField(blank=True, help_text='When a dispatcher claimed this attempt and enqueued it', null=True),
        ),
        migrations.AddField(
            model_name='qrafttaskattempt',
            name='cluster',
            field=models.CharField(blank=True, help_text='Cluster this attempt is routed to; null means whichever dispatcher claims it', max_length=150, null=True),
        ),
        migrations.AddField(
            model_name='qrafttaskattempt',
            name='dispatch_args',
            field=models.JSONField(blank=True, encoder=django.core.serializers.json.DjangoJSONEncoder, help_text='Positional arguments for dispatch_func', null=True),
        ),
        migrations.AddField(
            model_name='qrafttaskattempt',
            name='dispatch_func',
            field=models.CharField(blank=True, help_text='Dotted path to enqueue instead of qraft.runner.run_task', max_length=256, null=True),
        ),
        migrations.AddField(
            model_name='qrafttaskattempt',
            name='not_before',
            field=models.DateTimeField(blank=True, help_text='Earliest time a dispatcher may enqueue this attempt', null=True),
        ),
        migrations.AddField(
            model_name='qrafttaskattempt',
            name='state',
            field=models.CharField(choices=[('scheduled', 'Scheduled'), ('queued', 'Queued')], db_default='queued', default='queued', help_text='Whether this attempt is still waiting for the dispatcher', max_length=16),
        ),
        migrations.AlterField(
            model_name='qrafttaskattempt',
            name='q2_task_id',
            field=models.CharField(blank=True, db_index=True, help_text='Django-Q2 task ID for this attempt; null until a dispatcher enqueues a SCHEDULED attempt', max_length=32, null=True, unique=True),
        ),
        migrations.AddIndex(
            model_name='qrafttaskattempt',
            index=models.Index(fields=['state', 'not_before'], name='qraft_attempt_due_idx'),
        ),
    ]
