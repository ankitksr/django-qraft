"""
One entry point for the whole verification suite.

    manage.py demo all          run everything, print the matrix, fail loudly
    manage.py demo list         show what exists and what each scenario proves
    manage.py demo run KEY ...  run named scenarios only
    manage.py demo serve        boot the clusters and serve the dashboard
"""

import sys

from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError

from showcase import runner
from showcase.clusters import PROFILES, ClusterManager
from showcase.reset import reset_state


class Command(BaseCommand):
    help = "Run the django-qraft verification suite, or serve its dashboard."

    def add_arguments(self, parser):
        sub = parser.add_subparsers(dest="action")

        for name in ("all", "run"):
            action = sub.add_parser(name)
            if name == "run":
                action.add_argument("keys", nargs="+", help="Scenario keys to run")
            action.add_argument(
                "--group",
                choices=[
                    "core",
                    "workflows",
                    "durability",
                    "ai",
                    "django-tasks",
                    "bench",
                ],
                help="Limit to one group",
            )
            action.add_argument(
                "--keep-data",
                action="store_true",
                help="Do not reset the database first (results become cumulative)",
            )
            action.add_argument(
                "--keep-clusters",
                action="store_true",
                help="Leave the worker clusters running after the suite",
            )

        sub.add_parser("list")

        serve = sub.add_parser("serve")
        serve.add_argument("--port", default="8000")
        serve.add_argument("--no-reset", action="store_true", help="Keep existing data")

    def handle(self, *args, **options):
        action = options.get("action") or "list"
        handler = getattr(self, f"_{action.replace('-', '_')}")
        return handler(options)

    # --- actions ---

    def _list(self, options):
        runner.load()
        group = None
        for item in runner.select(None, None):
            if item.group != group:
                group = item.group
                self.stdout.write(self.style.HTTP_INFO(f"\n[{group}]"))
            self.stdout.write(f"  {item.key:<26} {item.title}")
            self.stdout.write(f"  {'':<26} {self.style.MIGRATE_HEADING(item.proves)}")
        self.stdout.write(
            "\nCluster profiles:\n"
            + "\n".join(f"  {name:<12} {why}" for name, why in PROFILES.items())
        )
        self.stdout.write("\nRun everything with: manage.py demo all")

    def _all(self, options):
        return self._execute(options, keys=None)

    def _run(self, options):
        return self._execute(options, keys=options["keys"])

    def _execute(self, options, keys):
        try:
            scenarios = runner.select(keys, options.get("group"))
        except KeyError as error:
            raise CommandError(str(error)) from error
        if not scenarios:
            raise CommandError("Nothing selected.")

        if not options.get("keep_data"):
            self.stdout.write("Resetting demo data...")
            reset_state()

        self.stdout.write(
            self.style.HTTP_INFO(f"Running {len(scenarios)} scenario(s)\n")
        )
        results = runner.run_all(
            scenarios,
            log=lambda message: self.stdout.write(str(message)),
            keep_clusters=options.get("keep_clusters", False),
        )

        self.stdout.write("\n" + runner.matrix(results))
        code = runner.exit_code(results)
        if code:
            self.stdout.write(self.style.ERROR("\nSuite FAILED"))
        else:
            self.stdout.write(self.style.SUCCESS("\nSuite PASSED"))
        sys.exit(code)

    def _serve(self, options):
        if not options.get("no_reset"):
            reset_state()

        runner.load()
        manager = ClusterManager(log=lambda message: self.stdout.write(f"  {message}"))
        self.stdout.write("Starting worker clusters for the dashboard...")
        # Other profiles boot on demand when a scenario or operator needs
        # them; serving an idle page should not start every worker process.
        manager.ensure(["default"])

        from showcase import views

        views.CLUSTERS = manager
        port = options["port"]
        self.stdout.write(
            self.style.SUCCESS(f"\nDashboard: http://127.0.0.1:{port}/\n")
        )
        try:
            call_command("runserver", port, use_reloader=False)
        finally:
            manager.stop_all()
