# Security Policy

## Supported versions

| Version | Supported |
| --- | --- |
| 1.3.x | Yes |
| 1.2.x and earlier | No |

Security fixes go to the latest minor release only. Upgrade to 1.3.x before you
report a problem in an older version.

## How to report a vulnerability

Do not open a public issue for a vulnerability.

1. Open a private report at
   [Security > Report a vulnerability](https://github.com/ankitksr/django-qraft/security/advisories/new).
2. If GitHub is not available to you, send an email to `ankitksrdev@gmail.com`.
   Put `django-qraft security` in the subject line.
3. Include the affected version, the Django and Django-Q2 versions, and the
   steps that show the problem.

## What to expect

- A reply within 7 days that confirms receipt.
- An assessment within 14 days that tells you if the report is accepted.
- A fix in a new patch release, and a GitHub Security Advisory with credit to
  you. Tell us if you prefer no credit.

This project has one maintainer and no commercial support contract. The times
above are targets, not a guarantee.

## Security model

Read these points before you deploy Qraft.

**`SECRET_KEY` is a code-execution credential.** Django-Q2 sends task payloads
as pickled data with a signature from `SECRET_KEY`. A person who knows the key
can enqueue a payload that runs arbitrary code in a worker. Protect the key as
you protect a database password. Use a different key for each environment.

**Broker access is equal to worker access.** A person with write access to the
broker (Redis, or the `OrmQ` table) can put work into the queue. Do not expose
the broker to an untrusted network.

**The dashboard needs a staff account.** Every view in `qraft.dashboard`
requires `request.user.is_active and request.user.is_staff`. All actions use
POST and Django's CSRF protection. The dashboard shows task arguments and
exception text, so a staff account can see application data. Mount the
dashboard behind your own authentication if `is_staff` is too wide a group for
your site.

**Task paths come from your code, not from user input.** Qraft imports a task,
hook, or pricing resolver from a dotted path in your settings or your
`async_task()` call. Never build one of these paths from data that a user
supplies.

**SQL is parameterized.** The module `qraft/context.py` uses raw SQL for
atomic JSONB updates. Every value goes in as a query parameter. The only
interpolated text is a table name from Django's model metadata and a fixed SQL
fragment.

**Qraft does not call `eval`, `exec`, or `yaml.load`.** The pickle path above
belongs to Django-Q2, not to Qraft.
