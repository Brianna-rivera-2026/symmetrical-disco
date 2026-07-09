# Authentication & Authorization — Design

**Date:** 2026-07-10
**Requirement:** [docs/requirements/06-auth.md](../../requirements/06-auth.md) — add authentication and authorization; protect job fetching and control (cancel, retry) scoped to the user who created them.

## Summary

API-key authentication backed by a `users` table, with job ownership enforced in SQL. Callers present a per-user key in the `X-API-Key` header; all job routes require it, and every read/control operation is filtered to the caller's own jobs. Users and keys are provisioned by a CLI command, not an HTTP endpoint.

## Decisions made during brainstorming

| Decision | Choice | Rationale |
|---|---|---|
| Auth mechanism | Static per-user API keys | Fits a service-to-service job API; no login flow, token expiry, or IdP to run |
| Protected surface | All job routes; `/health` and `/stats` stay open | Anonymous submission would leave jobs ownerless; health/stats are infra probes |
| Provisioning | CLI command | No admin auth surface to build; keys minted out-of-band |
| Implementation shape | Users table + FastAPI dependency | Matches existing DI patterns (`get_db`, `get_redis`); keys revocable; ownership enforced in SQL |

## Data model

### New table: `users`

| Column | Type | Notes |
|---|---|---|
| `id` | UUID | PK |
| `name` | text | unique, human-readable (CLI/logs) |
| `key_hash` | text | unique, indexed; SHA-256 hex of the raw key |
| `created_at` | timestamptz | server default `now()` |

Raw keys are `secrets.token_urlsafe(32)`, shown exactly once by the CLI; only the hash is stored. SHA-256 rather than bcrypt is deliberate: these are high-entropy random keys, not passwords — security comes from key entropy, and a fast hash keeps per-request auth to one indexed equality query.

### Changes to `jobs`

- `user_id` — UUID, FK → `users.id`, **nullable**. Existing rows have no owner and cannot be backfilled; the application always sets it for new jobs. Legacy NULL-owner jobs become invisible through the API (acceptable: pre-auth data).
- New index `(user_id, created_at, id)` supporting the scoped list with cursor pagination.
- **Idempotency scoping:** the global unique index on `idempotency_key` becomes a partial unique index on `(user_id, idempotency_key)`. Without this, user B reusing user A's key string would collide with or replay A's job — a cross-tenant leak. The idempotency lookup adds a `user_id` filter.

All of the above ships in a single Alembic migration.

## Authentication flow

New dependency `get_current_user` in `app/api/deps.py`, built on FastAPI's `APIKeyHeader` security scheme (header name: `X-API-Key`) so OpenAPI documents the requirement and Swagger UI gets an Authorize button.

1. Header missing → **401**.
2. SHA-256 the presented key; look up `users.key_hash` → no match → **401** (covers revoked/deleted users too).
3. Match → return the `User` row; bind `user_id` into the structlog context so all request logs carry it.

## Authorization (ownership)

Enforced in SQL via `user_id` filters — no post-fetch checks, no read-then-verify races.

| Route | Behavior |
|---|---|
| `POST /jobs` | Auth required; stamps `user_id`; idempotency lookup scoped to caller |
| `GET /jobs/{id}` | Fetch adds `WHERE user_id = caller`; another user's job → **404** |
| `POST /jobs/{id}/retry` | Ownership resolved via scoped fetch first; transition logic unchanged |
| `POST /jobs/{id}/cancel` | Same as retry |
| `GET /jobs` | Always filtered to caller's jobs; existing status/type/priority filters and cursor unchanged |
| `GET /health` | Open (orchestration probes) |
| `GET /stats` | Open; remains global counts (ops endpoint, not per-user) |

Cross-user access returns **404**, not 403, so the API is not an existence oracle for other users' job IDs.

### Repo layer

`get_job`, `list_jobs`, and `get_by_idempotency_key` gain a required `user_id` parameter. State-transition functions (`cancel_pending_or_scheduled`, `request_cancel`, `reset_failed_to_pending`, `mark_synced`) stay keyed by job id — ownership is already resolved before they run. Workers, ticker, and reaper are untouched; they operate on job ids internally, not through the API.

## Provisioning CLI

New module `app/users/`, runnable as:

```
uv run python -m app.users create <name>
```

- Generates the key, stores the hash, prints the raw key once to stdout with a "save this now — it will not be shown again" notice.
- Exits non-zero if the name already exists.
- Revocation = delete the user's row (documented; no dedicated command — YAGNI).
- Uses structlog like the rest of the codebase; the raw key goes to stdout because it is the command's output, not a log line.

## Error handling

| Condition | Response |
|---|---|
| Missing `X-API-Key` | 401 |
| Unknown/revoked key | 401 |
| Job exists but owned by another user | 404 (same as nonexistent) |
| Idempotency key reused by a different user | Treated as a fresh submission (no collision) |
| Same user, same idempotency key, same payload | 200 replay (unchanged behavior) |

## Testing

- **API tests (existing dependency-override pattern):**
  - 401 on missing and on invalid key for every protected route.
  - 404 on cross-user `GET /jobs/{id}`, retry, cancel.
  - `GET /jobs` returns only the caller's jobs.
  - Same idempotency key across two users → two separate jobs; same user + same key still replays.
- **Fixture update:** a seeded test user + key header applied to existing route tests so they pass with auth enabled.
- **Integration (testcontainers):** one end-to-end test — create user via the CLI entry point, submit a job, fetch it with the right key (200) and a different user's key (404).

## Out of scope

Key expiry/rotation, roles or admin tier, per-user stats, rate limiting, and any backfill of legacy jobs' ownership. All deferred until a concrete need exists.
