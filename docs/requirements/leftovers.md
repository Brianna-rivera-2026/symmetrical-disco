## Job

* Priority level (higher = more urgent)
* Attempt tracking (current attempts, max attempts)
* Scheduling (optional future execution time)
* Progress percentage (for batch jobs)
* Idempotency key (for duplicate prevention)


# Job Types to Implement

4. Batch Job
Processes multiple items. Process each item with small delay, track progress percentage, return summary.

# API Endpoints
Cancel a pending/scheduled job
Retry a failed job
Health check with queue statistics