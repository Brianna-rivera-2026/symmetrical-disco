# Runbook: Total Redis Loss Recovery

**When to use this:** Redis has lost all data (e.g., the `redisdata` volume was
deleted, or the AOF file was corrupted and Redis started with an empty
dataset) while Postgres still holds job history. This is a manual procedure —
there is no automated self-heal for this case (see
`docs/superpowers/specs/2026-07-01-production-hardening-design.md`, §3.3, for
why).

## Symptoms

- Redis has no streams, no `jobs:delayed` ZSET, and no consumer group.
- Postgres still has `pending`/`scheduled`/`processing` rows.
- `pending`/`scheduled` rows are flagged `is_synced_to_redis = TRUE` even
  though Redis has nothing for them — `reconcile_orphans` only acts on rows
  flagged `FALSE`, so it will not pick these up on its own.
- `processing` rows have lost their PEL entry, so the reaper (which reads the
  PEL via `XAUTOCLAIM`) is blind to them too.

## Steps

1. **Confirm scope.** Connect to Redis and Postgres; verify Redis has no
   streams/ZSET/consumer group while Postgres still holds
   `pending`/`scheduled`/`processing` rows.

2. **Recreate the consumer group.** Bring the services up (or restart them) —
   `ensure_group` runs on `api`, `worker`, and `ticker` startup and recreates
   the consumer group on each stream. No manual action needed unless you want
   it immediately, in which case:
   ```
   redis-cli XGROUP CREATE jobs:stream:high workers $ MKSTREAM
   redis-cli XGROUP CREATE jobs:stream:normal workers $ MKSTREAM
   redis-cli XGROUP CREATE jobs:stream:low workers $ MKSTREAM
   ```

3. **Re-arm reconcile.** Run once against Postgres:
   ```sql
   UPDATE jobs SET is_synced_to_redis = FALSE
   WHERE status IN ('pending', 'scheduled');
   ```
   The ticker's `reconcile_orphans` loop (runs every `reconcile_interval_s`,
   default 60s) will then re-derive each row: `pending` jobs go to their
   priority stream, `scheduled` jobs go into `jobs:delayed` at
   `scheduled_at`. Any duplicate delivery is absorbed by the worker's
   idempotent claim-guard — this step is safe to run even if some rows
   already have a live message.

4. **Recover stuck `processing` jobs.** For `processing` rows older than
   `visibility_timeout_s` (default 60s) whose worker process is confirmed
   dead, re-enqueue them:
   ```sql
   UPDATE jobs SET status = 'pending', is_synced_to_redis = FALSE
   WHERE id = '<job-id>';
   ```
   **Do not** touch a `processing` row if its worker might still be alive —
   that worker will finish and commit normally; resetting it risks the job
   running twice.

5. **Verify.** Watch the ticker's logs for `ticker.reconciled count=…` and
   confirm affected jobs transition to `completed`/`failed` via
   `GET /jobs/{id}` or `GET /jobs?status=...`.
