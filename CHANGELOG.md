# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Comprehensive documentation structure in `docs/`
- LICENSE file (MIT)
- CONTRIBUTING.md with development guidelines
- CHANGELOG.md for version tracking

## [1.0.0] - 2025-01-24

### Added
- Dual-phase hooks with separate success and failure handlers
- Rich retry policies with exponential, linear, and fixed backoff strategies
- Jitter support for retry delays to prevent thundering herd
- Optional multithreaded workers for I/O-bound workloads
- Async hook execution to prevent monitor bottlenecks
- `QraftTask` model for logical task tracking
- `QraftTaskAttempt` model for execution history
- `HookDispatch` model for idempotent hook execution
- `async_task()` function with Qraft-specific options
- `qraftcluster` management command
- Pydantic-based settings with Django integration
- ALT_CLUSTERS support for mixed workload pools
- Comprehensive unit test suite (108 tests, >90% coverage)
- Demo application with hooks, retry, and performance benchmarks

### Features

#### Dual-Phase Hooks
- Separate `success_hook` and `failure_hook` configuration
- Custom arguments and kwargs for each hook
- Async execution via worker pool (default) or sync in monitor
- Idempotent dispatch with `HookDispatch` tracking

#### Retry Policies
- Maximum attempts with exhaustion handling
- Backoff strategies: exponential (default), linear, fixed
- Jitter randomization (0-30% by default)
- Exception filtering with `retry_exceptions` and `skip_exceptions`
- Automatic scheduling via Django-Q2 Schedule

#### Multithreaded Workers
- Configurable threads per worker process
- Semaphore-based backpressure control (`max_inflight`)
- Graceful shutdown with configurable grace period
- Database connection management per thread
- Process-level timeout enforcement

#### Configuration
- `QRAFT_CLUSTER` settings (fallback to `Q_CLUSTER`)
- Pydantic validation with sensible defaults
- Environment-based cluster selection (`Q_CLUSTER_NAME`)
- Backward compatibility with Django-Q2

### Compatibility
- Python 3.9+
- Django 4.2+
- Django-Q2 1.8+
- Drop-in enhancement of Django-Q2

## [0.1.0] - 2024-XX-XX (Initial Development)

### Added
- Initial project structure
- Basic task queue functionality
- Integration with Django-Q2

---

## Release Notes Guidelines

### Version Format
- **Major (X.0.0)**: Breaking changes
- **Minor (0.X.0)**: New features, backward compatible
- **Patch (0.0.X)**: Bug fixes, backward compatible

### Categories
- **Added**: New features
- **Changed**: Changes to existing functionality
- **Deprecated**: Soon-to-be removed features
- **Removed**: Removed features
- **Fixed**: Bug fixes
- **Security**: Security fixes

[Unreleased]: https://github.com/yourusername/django-qraft/compare/v1.0.0...HEAD
[1.0.0]: https://github.com/yourusername/django-qraft/releases/tag/v1.0.0
[0.1.0]: https://github.com/yourusername/django-qraft/releases/tag/v0.1.0
