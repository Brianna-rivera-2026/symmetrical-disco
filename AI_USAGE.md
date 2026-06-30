# AI Tool Usage

## Tools Used

- **Claude (Anthropic)** — Code generation, schema design, and boilerplate via Subagent-Driven Development.
- Specific capabilities: schema scaffolding (Pydantic models, SQLAlchemy ORM), test structure, and API route templates.

---

## What Helped Most

**1. Schema and boilerplate generation:** 
AI generated the discriminated-union payload schemas (`EmailPayload`, `WebhookPayload`, `ReportPayload`) with Pydantic's `Annotated[Union[...], Field(discriminator="type")]` pattern. This is boilerplate-heavy and error-prone to hand-code, especially ensuring consistency across API and worker validation. AI got it right on the first pass.

**2. Test scaffolding and fixtures:**
AI generated the test structure, including pytest fixtures for fake Redis (via `fakeredis`), SQLAlchemy session factories, and the overall test layout. This reduced the repetitive setup work, letting human review focus on test logic instead of wiring.

**3. API route templates:**
The FastAPI routes for `POST /jobs`, `GET /jobs/{id}`, and `GET /jobs` (with cursor pagination) were generated from the spec. The cursor encoding/decoding logic and the row-value SQL comparison were correct, avoiding a common pitfall (offset-based pagination).

---

## What I Had to Fix

**Critical concurrency issues caught in review:**

1. **Claim-guard correctness (Task 11, worker dispatch):**
   - AI initially generated a check for `job.status == "pending"` but placed it *after* message dequeue, not atomically with the update.
   - **Problem:** A redelivered message could pass the check, then lose a race with another worker updating the job, resulting in lost updates or duplicate processing.
   - **Fix:** Replaced the check with a conditional `UPDATE jobs SET status='processing', started_at=NOW() WHERE id=:id AND status='pending'`, which returns `rowcount == 1` if and only if the claim succeeded. This is an atomic optimistic update — no lock needed, no race possible.

2. **Ack-after-commit ordering (Task 11, worker dispatch):**
   - AI generated the order as: run handler → `XACK` → commit (to Redis first, then DB).
   - **Problem:** If the process crashed between `XACK` and commit, the message would be marked done in Redis but the job would remain `processing` in Postgres, creating an orphan.
   - **Fix:** Reordered to: run handler → commit (Postgres) → `XACK` (Redis). Now the Postgres commit is the atomic boundary; redelivery is safe because the claim-guard re-checks `pending` status.

3. **Unique consumer names per process (Task 8, Redis setup; Task 11, worker initialization):**
   - AI generated a fixed consumer name (e.g., `"worker"`) for all processes.
   - **Problem:** When a crashed worker restarted under the same name, its PEL merged with a new consumer's PEL, causing message confusion and making recovery impossible.
   - **Fix:** Changed to `<hostname>_<uuid>` per process, ensuring every consumer is unique. This is essential for Phase 2 reaper (`XGROUP DELCONSUMER` to clean up dead consumers).

4. **Module-level app validation (Task 10, API):**
   - AI generated `app = create_app()` at module level, which runs `app.dependency_overrides.update(...)` and validation on import.
   - **Problem:** In test files, importing `app` from `app.main` before setting up test fixtures caused validation errors (Redis, DB) at test discovery time.
   - **Fix:** Wrapped the app creation in a try/except catching `ValidationError`, so missing dependencies don't fail import. Actual routes validate on first request (when fixtures are ready).

---

## What AI Struggled With

**Distributed systems reasoning:** AI's initial drafts didn't always consider crash scenarios holistically. For example:
- It knew *what* the claim-guard was (from the spec) but not *why* the exact placement and isolation level mattered.
- It generated correct SQL but didn't reason about the order of operations across process boundaries (Redis vs. Postgres commits).

**Trade-off explanations:** AI generated placeholder trade-off text in decisions without depth. Human review replaced these with concrete reasoning (e.g., "concurrency = replica count, not async workers within a process").

**Testing concurrency edge cases:** AI's initial test suite didn't cover crash scenarios (orphaned messages, stale claims, redelivery). These were added in review to ensure the claim-guard and ack-after-commit actually prevent data loss under faults.

---

## Summary

AI excelled at boilerplate, scaffolding, and patterns it had seen in training (discriminated unions, async routes, fixtures). But it required human review and rework on every concurrency detail — especially the invisible dependencies (ordering of commits, atomic read-and-update, unique naming). The fixes were small in code size but critical in correctness. Phase 1 is solid because human review caught these; Phase 2's reaper will depend on the same care.