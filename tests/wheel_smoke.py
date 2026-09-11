"""Run with an installed wheel, outside the checkout's import path."""
from pathlib import Path

import django
from django.conf import settings

settings.configure(
    SECRET_KEY='wheel-smoke',
    INSTALLED_APPS=['django.contrib.contenttypes', 'django.contrib.auth',
                    'django_q', 'qraft', 'qraft.dashboard'],
    DATABASES={'default': {'ENGINE': 'django.db.backends.sqlite3', 'NAME': ':memory:'}},
    TEMPLATES=[{'BACKEND': 'django.template.backends.django.DjangoTemplates', 'APP_DIRS': True}],
    Q_CLUSTER={'orm': 'default', 'timeout': 30, 'retry': 60},
    ROOT_URLCONF='qraft.dashboard.urls',
)
django.setup()
from django.template.loader import get_template
from django.db.migrations.loader import MigrationLoader
import qraft

assert 'site-packages' in str(Path(qraft.__file__)), qraft.__file__
template = get_template('qraft_dashboard/dashboard.html')
assert 'Dead letters' in template.template.source
assert ('qraft', '0020_qraftgraphsettlement_and_more') in MigrationLoader(None).disk_migrations
print('Installed wheel: imports, dashboard template, and migrations OK')
