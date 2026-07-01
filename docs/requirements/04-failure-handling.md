Focus: Hardening the infrastructure against errors and worker death.

## Job

* Attempt tracking (current attempts, max attempts)

Turn on the Reaper Loop: Every Infrastructure Visibility Timeout, it inspects the Stream's Pending Entries List (PEL) to find jobs that were popped by a worker but never acknowledged (XACK).

Failed jobs should retry with exponential backoff:

Attempt 1: immediate (or nearly immediate) should be done in the worker
Attempt 2: after delay (e.g., 30 seconds)
Attempt 3: after longer delay (e.g., 2 minutes)
After max attempts: mark as FAILED permanently

# API Endpoints
Retry a failed job (after all retries)

# Worker
Worker should have an application-level timeout Maximum Job Handler Timeout from configuration for handling job timeout.

Maximum Job Handler Timeout < Infrastructure Visibility Timeout

Layer 1: Proactive Defense via Strict Timeout Invariants
The simplest way to protect a slow worker from being penalized by the ticker’s XAUTOCLAIM is to ensure it cuts itself off before the visibility timeout expires.

The Invariant: Max Job Runtime Timeout < Visibility Timeout

Implementation: Enforce an explicit timeout context within the worker’s execution loop around the handler execution using asyncio.wait_for() (or a signal/alarm timer for synchronous code).

Example Sizing: If your visibility timeout is 60s, set your worker's internal job execution timeout to 45s.

Behavior: If a job runs slow, the worker throws a TimeoutError internally. It enters its own try/except block, safely registers the failure, allows the ticker to transition the job state cleanly, and calls XACK before XAUTOCLAIM ever sees the message as stale.

Layer 2: Reactive Defense via Optimistic Locking (Postgres as Source of Truth)
If a worker undergoes a total stop-the-world garbage collection pause or a temporary network isolation that prevents it from enforcing its internal timeout, it might wake up after the ticker has already executed XAUTOCLAIM and re-queued the job. To prevent this slow worker from corrupting the state machine, you must use Optimistic Concurrency Control.

1. The Worker Update Guard
When a worker finishes executing a job, it must never execute an un-guarded UPDATE statement. It must verify it still owns the job execution lease:

SQL
UPDATE jobs 
SET status = 'completed', result = :result, completed_at = now() 
WHERE id = :job_id AND status = 'processing';
If rows_affected == 1: The worker safely completed within its window. It can proceed to call XACK on the Redis stream message.

If rows_affected == 0: The ticker reaper has already reclaimed this job due to a visibility timeout expiration and changed its status. The worker must abort immediately, skip the XACK, and log a critical warning.

2. The Ticker Reaper Guard
When the ticker pulls an expired message via XAUTOCLAIM, it must also guard its database transition before orchestrating a re-queue:

SQL
UPDATE jobs 
SET status = 'pending', started_at = NULL 
WHERE id = :job_id AND status = 'processing';
If rows_affected == 0: The worker finished the job right as the ticker claimed it. The ticker should simply XACK the message to clear it from the stream and do nothing else.

2. Network Edge Case Guard on the Ticker
Because the worker does not handle the long-term retry state logic, the Ticker Reaper must be defensive when XAUTOCLAIM pulls a job.
Before the Ticker increments attempts or marks a job as PENDING/FAILED, it must read the Postgres row. If it sees the row is already completed or failed (meaning the worker finished the job and updated the DB, but dropped its network connection right before it could send the XACK), the Ticker should only call XACK to clear the ghost message from the stream, and do nothing else