from django.urls import path

from showcase import views

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("api/state/", views.state, name="state"),
    path("scenarios/<str:key>/run/", views.run_scenario, name="run-scenario"),
    path("dlq/<str:task_id>/requeue/", views.requeue_task, name="requeue"),
    path("clusters/<str:name>/start/", views.start_cluster, name="start-cluster"),
    path("clusters/<str:name>/stop/", views.stop_cluster, name="stop-cluster"),
    path("soak/start/", views.start_soak, name="start-soak"),
    path("reset/", views.reset, name="reset"),
]
