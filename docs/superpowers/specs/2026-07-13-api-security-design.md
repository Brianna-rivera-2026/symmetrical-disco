# API Security Hardening — Design

**Date:** 2026-07-13
**Source requirement:** `docs/requirements/10-api-security.md`

## Goal

Harden the job-processing API against abuse and blast-radius risks: per-user rate
limiting, webhook egress restrictions, request size caps, stricter input
validation with domain allowlists, and least-privilege Postgres roles.

## 1. Rate limiting

**Library:** `fastapi-limiter` (Redis-backed), added with `uv add fastapi-limiter`.

- Initialize `FastAPILimiter` in the `create_app` lifespan using the existing
  Redis client.
- **Identifier:** custom callable keyed by the authenticated user resolved from
  the API key header; falls back to client IP when no valid key is present.
  All API replicas share counters via Redis.
- **Limits** (route dependencies, settings-driven):

  | Route group | Default limit |
  |---|---|
  | `POST /jobs` | 20/min per user |
  | `POST /jobs/{id}/retry`, `POST /jobs/{id}/cancel` | 30/min per user |
  | `GET /jobs`, `GET /jobs/{id}` | 120/min per user |
  | `GET /stats` | 30/min per user |
  | `/health`, `/ready` | exempt (probes) |

- Over-limit responses are **429** with a `Retry-After` header (library default).
- New `Settings` fields: `rate_limit_enabled: bool = True` and one
  `<group>_rate_limit_per_min: int` per group above. When disabled (tests,
  local debugging) the dependencies become no-ops.

## 2. Webhook payload tightening + egress allowlist

- `WebhookPayload.url` becomes `HttpUrl` restricted to `http`/`https` schemes,
  keeping `max_length=2048`.
- `WebhookPayload.method` becomes `Literal["GET", "POST"]` (default `"POST"`).
- New setting `webhook_allowed_hosts: list[str]` — host **suffix** match, so
  `hooks.example.com` also matches `a.hooks.example.com`. **Empty list = deny
  all webhook jobs** (secure default). docker-compose dev sets a permissive
  value inline.
- Enforcement at two points:
  1. **Submission:** the allowlist check runs during payload validation on
     `POST /jobs`; a non-allowlisted host is rejected with 422 and a clear
     message.
  2. **Worker:** `handle_webhook` re-checks the host before "sending". A
     non-allowlisted host raises a **non-retryable** error so jobs enqueued
     before the list was tightened (or injected via DB) cannot bypass it.

## 3. Request size limits

- New ASGI middleware in `app/api/middleware.py`:
  - Rejects requests whose `Content-Length` exceeds
    `max_request_body_bytes` (default **262144** / 256 KB) with **413**.
  - For chunked/streamed bodies (no `Content-Length`), wraps `receive` and
    aborts with 413 once the cap is crossed.
- Registered in `create_app` so it applies identically in dev (uvicorn) and
  production.
- Pydantic field caps remain the fine-grained layer (see §4).
- `GET /jobs` `cursor` query param gets `max_length=512`.

## 4. Stricter pydantic types + email domain allowlist

- `EmailPayload.to` becomes `EmailStr` (adds the `email-validator` dependency).
- New setting `email_allowed_domains: list[str]` checked during payload
  validation. **Empty list = deny all email jobs**, matching the webhook
  default. Matching is on the exact domain part of the address,
  case-insensitive. docker-compose dev sets a permissive value inline.
- `EmailPayload.subject`: add `min_length=1`.
- `ReportPayload.report_type` becomes a closed enum: new `ReportType(str, Enum)`
  in `app/schemas/enums.py` with the values already used by the codebase —
  `sales`, `ops`, `weekly_summary`. Unknown report types are rejected with 422.
- `ReportPayload.params`: keep the 50-key intent but enforce it with a real
  validator (pydantic `max_length` on `dict` is a no-op), plus a serialized
  size cap of 8 KB.
- `JobSubmission.idempotency_key`: `min_length=1, max_length=255`.
- `JobSubmission.scheduled_at`: reject values more than **365 days** in the
  future (new validator).
- All payload models and `JobSubmission` get
  `model_config = ConfigDict(extra="forbid")` so unknown keys are rejected
  instead of silently stored.
- Because allowlist checks need `Settings`, `validate_payload` gains a
  `settings` parameter; the API route passes `request.app.state.settings`, the
  worker passes its own settings instance.

## 5. Postgres role split

- New init script `deploy/db-init/01-roles.sql`, mounted via
  `docker-entrypoint-initdb.d` in **both** docker-compose and the Helm
  Postgres StatefulSet, creating:
  - **`jobs_migrator`** — owns the application database/schema; Alembic
    (`migrate-job.yaml`) connects as this role and may run DDL.
  - **`jobs_app`** — runtime role: `SELECT/INSERT/UPDATE/DELETE` on all tables
    in the app schema plus `USAGE` on sequences; no DDL.
  - `ALTER DEFAULT PRIVILEGES FOR ROLE jobs_migrator IN SCHEMA public GRANT
    SELECT, INSERT, UPDATE, DELETE ON TABLES TO jobs_app` (and sequence
    equivalent) so tables created by future migrations are granted
    automatically.
- **Wiring:**
  - Helm: `migrate-job.yaml` gets the migrator DSN; `api`, `worker`, `ticker`
    deployments and `users-sync-job` (DML only) get the app DSN. Both DSNs and
    passwords live in the existing `credentials-secret.yaml` /
    `init-secrets.sh` flow.
  - docker-compose: two `DATABASE_URL` values with dev passwords inline, per
    the repo's current dev convention.

## Error handling summary

| Condition | Response |
|---|---|
| Rate limit exceeded | 429 + `Retry-After` |
| Body over size cap | 413 |
| Non-allowlisted webhook host / email domain (submit) | 422 |
| Non-allowlisted webhook host (worker) | job fails, non-retryable |
| Unknown payload keys / invalid types | 422 |
| Runtime role attempts DDL | Postgres permission error (deploy misconfig signal) |

## Testing

- **Unit:** middleware 413 paths (Content-Length and streamed), payload
  validators (both allowlists, stricter types, `extra="forbid"`), rate-limit
  identifier keying (user vs IP fallback).
- **Integration:** rate limit against test Redis — user hits 429 at the
  threshold, a different user is unaffected; disabled flag bypasses limits.
- **Role split:** verified manually (compose up, attempt DDL as `jobs_app`,
  expect permission denied; run Alembic as `jobs_migrator`, expect success) —
  no pytest for infra config, per project convention.

## Out of scope

- Real outbound HTTP for webhooks (handler remains simulated).
- Router/ingress-level size or rate limits (may complement later, not relied on).
- Per-endpoint quotas beyond rate limits (e.g. max jobs per user per day).
