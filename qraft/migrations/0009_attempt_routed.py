from django.db import migrations, models


def backfill_routed(apps, schema_editor):
    """
    Existing resolved attempts count as routed: their post-commit work either
    ran long ago or is unknowable, and replaying hooks for historical rows
    (whose HookDispatch records retention may have pruned) would re-fire them.
    """
    QraftTaskAttempt = apps.get_model("qraft", "QraftTaskAttempt")
    QraftTaskAttempt.objects.filter(success__isnull=False).update(routed=True)


class Migration(migrations.Migration):

    dependencies = [
        ("qraft", "0008_drop_redundant_db_index"),
    ]

    operations = [
        migrations.AddField(
            model_name="qrafttaskattempt",
            name="routed",
            field=models.BooleanField(
                default=False,
                db_default=False,
                help_text="Whether post-resolution routing and hook dispatch completed",
            ),
        ),
        migrations.RunPython(backfill_routed, migrations.RunPython.noop),
    ]
