# AI Tool Usage

## Tools Used

- **Claude Code** — primary coding agent for the project: generated implementation code, wrote and ran tests, and did interactive debugging. Most features were built via **Subagent-Driven Development**.
- **Superpowers plugin** — supplied the process skills that structured the work end-to-end, not just SDD:
  - `brainstorming` + `writing-plans` turned each feature request into a design doc  (`docs/superpowers/specs/*-design.md`) and a task-by-task implementation plan (`docs/superpowers/plans/*.md`) before any code was written.
  - `subagent-driven-development` / `executing-plans` executed those plans task-by-task.
  - `test-driven-development` enforced the RED-then-GREEN commit pairing visible throughout the git history.
  - `systematic-debugging` and `receiving-code-review` were used to root-cause the concurrency and ordering issues below, rather than patching symptoms.


## What Helped Most

**1. Test scaffolding and fixtures:**
AI generated the test structure, including pytest fixtures, SQLAlchemy session factories, and the overall test layout. This reduced the repetitive setup work, letting human review focus on test logic instead of wiring.

**2. Interactive design-space exploration:**
For non-trivial architecture decisions, walking through 2-3 concrete options with the AI — and having it argue the trade-offs of each instead of just proposing one — surfaced consequences I would have missed by committing to the first reasonable-sounding approach.

## What I Had to Fix

**Critical concurrency issues caught in review:**

1. **Reconcile correctness came from `is_synced_to_redis`, not from batching (ticker reconcile + submission handoff):**
   - AI's initial design queried "scheduled job with no Redis presence" directly and re-`ZADD`ed the *entire* result back into the delayed set on every tick, with no way to tell an already-handed-off job from a genuine orphan — and it only looked at the scheduled path, so a job that committed to Postgres on the *immediate* path but then failed its enqueue (a Redis blip) was never revisited at all.
   - **Problem:** With tens of thousands of jobs scheduled ahead, re-querying and re-broadcasting the full backlog every tick forces Redis to repeatedly rebalance its skip-list for the same entries, pinning its single thread near 100% CPU and starving normal dispatch — a self-inflicted denial of service against our own queue. My first pushback landed on just adding a batch size and a grace period, but that only bounds *how much* gets re-sent per tick — it doesn't stop the same already-synced rows from qualifying again next tick, and it still leaves the immediate-path gap untouched.
   - **Fix:** The real fix was a single `is_synced_to_redis` flag on the job row, flipped to `TRUE` only once the Redis handoff actually succeeds — immediate enqueue or scheduled handoff alike. Reconcile then filters on `is_synced_to_redis = FALSE`: a row drops out of that set permanently the moment its handoff succeeds, so the result is always just the true orphans, not the whole backlog — batch size and grace period end up bounding an already-small set instead of doing the correctness work themselves. Because the flag isn't scoped to either path, it closes the immediate-path gap for free too.

2. **Reaper reclaim racing a slow-but-alive worker into double execution (ticker reaper):**
   - AI's initial `XAUTOCLAIM`-based reaper design reclaimed any job idle past `visibility_timeout_s` and handed it straight back to the stream for another consumer, treating "past timeout" as equivalent to "worker crashed."
   - **Problem:** A worker that's merely slow, not dead, is still holding and will still finish that job. If the reaper hands the same job to a second consumer, both can run the handler concurrently — a real double-execution risk (e.g. sending the same email twice), not just a theoretical one.
   - **Fix:** Every terminal transition (`complete_job`, `fail_job`, `retry_to_pending`, `retry_to_scheduled`) is a guarded `UPDATE ... WHERE status = 'processing'` that only one caller can win (`rowcount == 1`) — the same optimistic-concurrency pattern as a claim-guard, now applied symmetrically on exit. Whichever of {original worker, reaper-triggered retry} finishes first wins; the loser's write is a no-op. Retry/attempt-count bookkeeping was also centralized into one guarded helper (`schedule_retry_or_fail` in `app/retry.py`), shared by both the worker's own failure path and the reaper, instead of duplicated ad hoc inside the worker.

## What AI Struggled With

**Over-engineering robustness instead of the assumptions this system is allowed to make:** AI's default instinct was to make a new mechanism correct under arbitrary concurrency and unbounded scale, even when the system's own deployment model already ruled that scenario out:
- **Automated rebuild-from-Postgres after a total Redis loss.** The design doc itself describes the ticker as "the existing singleton that already owns reconcile/reap" — and then goes on to add a sentinel key with an `NX` lock and a written ordering proof specifically to defend against *"two tickers racing."* There is only ever one ticker; that race cannot happen in this deployment. AI designed for "what if this ran as N instances" as a default, instead of accepting the single-instance assumption the architecture already makes everywhere else. The whole subsystem — sentinel, idempotency argument, dedicated unit tests — existed to survive a race that isn't real here, and collapsed to one manual `UPDATE` plus the reconcile loop that already existed for the ordinary orphan case.
- **Batch items as parent/child jobs.** For making batch items "real" jobs, AI's proposal was a fan-out pattern: submitting a batch creates a parent `Job` row plus a real child `Job` row per item, each pushed through the same Redis priority streams so every item inherits the full retry/backoff/claim-guard machinery built for top-level jobs. That's solving for a requirement that is way more complex. The shipped design dispatches each item inline inside the batch's own handler loop and, on failure, simply records it in the summary's `errors` list and moves on to the next item — no retry, no child job rows, no parent/child aggregation to reconcile. A batch that's genuinely too big for the handler timeout already has a safety net (the whole batch retries as one unit via the existing `HandlerTimeout` path);

**The underlying pattern:** in both cases AI reached for the robustness bar it would apply to a system with no constraints — arbitrary horizontal scale, full per-item infrastructure resilience — rather than accepting the bounds this system's own architecture already commits to (a singleton ticker; batch items that only need to be attempted once and reported, not independently retried). It's not that AI didn't know how to build the simpler version — it needed to be told the assumption was allowed before it would.