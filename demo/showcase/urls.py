from django.urls import path

from showcase import views

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("api/state/", views.state, name="state"),
    path("scenarios/<str:key>/run/", views.run_scenario, name="run-scenario"),
    path("dlq/<str:task_id>/requeue/", views.requeue_task, name="requeue"),
]
