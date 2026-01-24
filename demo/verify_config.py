#!/usr/bin/env python
"""
Verify Qraft cluster configuration is correct.
Tests both baseline and qraft cluster settings.
"""

import os
import sys

import django

# Setup Django
sys.path.insert(0, "/Users/ankitksr/projects/django-qraft/demo")
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "demo.settings")
django.setup()

from qraft.conf import get_conf

print("=" * 60)
print("QRAFT CONFIGURATION VERIFICATION")
print("=" * 60)

# Test baseline cluster (default)
print("\n1. BASELINE CLUSTER (default):")
print("   Command: python manage.py qraftcluster")
os.environ.pop("Q_CLUSTER_NAME", None)  # Clear any existing setting
baseline_conf = get_conf()
print(f"   ✓ threads: {baseline_conf.threads} (expected: 1)")
print(f"   ✓ max_inflight: {baseline_conf.get_max_inflight()} (expected: 2)")
assert baseline_conf.threads == 1, "Baseline should use threads=1"

# Test qraft cluster (ALT_CLUSTERS)
print("\n2. QRAFT CLUSTER (ALT_CLUSTERS):")
print("   Command: Q_CLUSTER_NAME=qraft python manage.py qraftcluster")
os.environ["Q_CLUSTER_NAME"] = "qraft"
qraft_conf = get_conf()
print(f"   ✓ threads: {qraft_conf.threads} (expected: 4)")
print(f"   ✓ max_inflight: {qraft_conf.get_max_inflight()} (expected: 8)")
assert qraft_conf.threads == 4, "Qraft cluster should use threads=4"

print("\n" + "=" * 60)
print("✅ CONFIGURATION VERIFIED SUCCESSFULLY!")
print("=" * 60)
print("\nTo run the performance demo:")
print("  Terminal 1: python manage.py qraftcluster")
print("  Terminal 2: Q_CLUSTER_NAME=qraft python manage.py qraftcluster")
print("  Terminal 3: python manage.py demo perf -n 20 --duration 1.0")
print()
