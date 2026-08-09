from django.urls import include, path

urlpatterns = [
    path("qraft/", include("qraft.dashboard.urls")),
]
