# Django-Qraft Documentation

## Quick Navigation

### Getting Started
- [Getting Started Guide](getting-started.md) - Installation, setup, and basic usage
- [Configuration Reference](configuration.md) - Complete settings documentation

### Core Features
- [Dual-Phase Hooks](hooks.md) - Success and failure hook system
- [Retry Policies](retry.md) - Backoff strategies and retry configuration
- [Multithreaded Workers](threading.md) - Concurrency for I/O-bound tasks
- [Workflow Primitives](workflows.md) - Chain, Iter, Batch, approval steps, and execution graphs
- [AI Workloads](ai-workloads.md) - Rate limits, throttling, usage accounting, idempotency, reaper, priority lanes
- [Monitoring Dashboard](dashboard.md) - Bundled staff dashboard with live metrics and JSON endpoints
- [Dead Letter Queue](dlq.md) - Listing and requeuing exhausted attempts

### Advanced Topics
- [django.tasks Backend](django-tasks-backend.md) - Qraft as an engine for Django 6.0's Tasks API
- [Roadmap](roadmap.md) - Positioning and planned features
- [Architecture](architecture.md) - System design and extension patterns
- [Development Guide](development.md) - Contributing and local development
- [Test Drive](test-drive.md) - A guided walkthrough of the demo app
- [Lifecycle Map](lifecycle-map.html) - Visual map of task and workflow state transitions

### Additional Resources
- [Testing Guide](../tests/README.md) - Running and writing tests
- [Demo Application](../demo/README.md) - Interactive feature demonstrations
- [Future Features](future/) - Planned enhancements: [asyncio worker](future/asyncio-worker.md),
  [Django-Q2 absorption plan](future/q2-absorption.md)

## Getting Help

### Resources
- [Main README](../README.md) - Project overview
- [CONTRIBUTING](../CONTRIBUTING.md) - How to contribute
- [CHANGELOG](../CHANGELOG.md) - Version history

### Support
- **Issues**: [GitHub Issues](https://github.com/ankitksr/django-qraft/issues)
- **Discussions**: [GitHub Discussions](https://github.com/ankitksr/django-qraft/discussions)

## Next Steps

1. **New to Django-Qraft?** → Start with [Getting Started](getting-started.md)
2. **Migrating from Django-Q2?** → Check [Configuration](configuration.md) for compatibility
3. **Need retries?** → Read [Retry Policies](retry.md)
4. **Want better performance?** → See [Multithreaded Workers](threading.md)
5. **Contributing?** → Review [Development Guide](development.md)
