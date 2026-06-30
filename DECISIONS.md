# Design Decisions

## 1. Job Pickup Strategy

**Approach chosen:** Redis Streams + one consumer group; each worker process is a competing consumer pulling one job at a time (`XREADGROUP COUNT 1`).

**Why:** 
- **Built-in load balancing:** Redis automatically distributes unacked messages across consumers, so all replicas stay busy.
- **Pending List (PEL):** Each consumer maintains its own PEL of unacked messages, making it possible to detect and recover from crashes without polling (Phase 2 reaper).
- **Simple, battle-tested:** Consumer groups are the standard pattern for distributed job queues on Redis.

**Trade-offs:**
- **Concurrency = replica count:** We pull one job per process. Throughput is bounded by the number of worker replicas, not by async concurrency within a process. For I/O-heavy workloads (email, webhooks), this is fine; for CPU-bound work, you'd scale replicas. Phase 2 may explore async workers (Approach C in the brainstorm) for higher throughput.
- **Consumer naming:** Consumer names must be unique per process (not reused across restarts), otherwise Redis merges their PLEs. We use `<hostname>_<uuid>` to guarantee uniqueness and enable targeted recovery.

---

## 2. Worker Crash Recovery

**Approach chosen:** Describe-only (not implemented in Phase 1); recovery mechanism documented for Phase 2.

**Why:** Crash recovery adds complexity (reaper loop, graceful shutdown, stale-claim detection) that isn't critical for Phase 1's basic flow. We've built the foundation (claim-guard, ack-after-commit, unique consumer names) so recovery is safe and straightforward when needed.

**What happens if a worker crashes mid-job:**

1. **In-flight message stays unacked** in the consumer group's PEL under the crashed worker's consumer name.
2. **Job status in Postgres remains `processing`.** The claim-guard makes redelivery safe: a future attempt to process the same job will see it's already `processing`, not `pending`, and skip it (no-op).
3. **Future recovery (Phase 2):** A "reaper" background task periodically:
   - Scans the PEL for messages older than a timeout (e.g., 30 minutes).
   - Calls `XAUTOCLAIM` to reassign stale messages to a recovery consumer.
   - Resets the corresponding job status from `processing` → `pending` in Postgres.
   - Calls `XGROUP DELCONSUMER` to clean up dead consumers from the group.

**The orphan gap (commit-then-XADD):** If a process crashes *after* committing the job to Postgres but *before* calling `XADD`, the job remains in `pending` status with no corresponding message in the stream. A Phase 2 "pending-sweeper" would scan for `pending` jobs older than a threshold and re-enqueue them or escalate an alert.

---

## 3. Priority Queue Implementation

**Approach chosen:** Deferred to Phase 2.

**Why:** Phase 1 is strictly FIFO. Adding priorities would require either:
- Multiple streams (one per priority level), adding coordination complexity.
- Redis Sorted Sets for priority ordering, requiring custom dequeue logic outside consumer groups.

Both can be added in Phase 2 without breaking the current architecture.

---

## 4. Retry Backoff Strategy

**Approach chosen:** Deferred to Phase 2 (Phase 1 marks failures terminal).

**Why:** Phase 1 treats job failure as permanent (`failed` status, no retry). The claim-guard and ack-after-commit give us safe re-delivery, so backoff-on-redelivery can be added later. A simple approach would be:
- Store `attempt_count` and `next_retry_at` in the job record.
- On handler failure, increment attempt count and set `next_retry_at` instead of marking `failed`.
- A time-based reaper re-enqueues jobs when `next_retry_at` passes.

---

## 5. One Thing I Would Do Differently With More Time

**Transactional outbox + dedicated recovery service:** The biggest risk in the current design is the gap between committing the job and enqueuing it (commit-then-XADD). A production system would use an outbox pattern:

1. On job submission, write *both* the job record *and* an outbox record (same transaction) to Postgres.
2. Commit atomically.
3. A separate outbox sweeper task reads unprocessed outbox entries and enqueues them to Redis.

This eliminates the orphan gap entirely. The sweeper is idempotent (checking if a job is already in Redis before re-enqueuing), so even if it processes the same outbox row twice, the outcome is correct.

**Why not now:** It adds a worker process and monitoring logic (is the sweeper running? how far behind is it?). For Phase 1, commit-then-XADD with eventual manual recovery is acceptable. Phase 2's reaper makes it explicit and automatable.
