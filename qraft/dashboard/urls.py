from django.urls import path

from . import views

app_name = "qraft_dashboard"

urlpatterns = [
    path("", views.dashboard, name="dashboard"),
    path("api/state/", views.state, name="state"),
    path("api/metrics/", views.metrics_view, name="metrics"),
    path("dlq/<uuid:task_id>/requeue/", views.requeue_task, name="requeue"),
    path("chains/<uuid:chain_id>/approve/", views.approve_chain, name="approve"),
    path("chains/<uuid:chain_id>/reject/", views.reject_chain, name="reject"),
    path("graphs/<uuid:graph_id>/cancel/", views.cancel_graph, name="cancel_graph"),
    path("graphs/<uuid:graph_id>/resume/", views.resume_graph, name="resume_graph"),
    path(
        "graphs/<uuid:graph_id>/nodes/<str:node_key>/skip/",
        views.skip_graph_node,
        name="skip_node",
    ),
    path(
        "workflows/<str:kind>/<uuid:workflow_id>/cancel/",
        views.cancel_workflow,
        name="cancel",
    ),
]
