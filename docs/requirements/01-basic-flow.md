Focus: Get the basic flow working first (Submit -> Queue -> Process -> Complete). Treat every job as immediate.

# API
API Endpoints
* Submit a new job
* Get job status, result, or error
* List jobs with filters (status, type)

# Data Model
Design your data model to support:

## Job
* Unique identifier
* Job type and payload (JSON)
* Status (scheduled, pending, processing, completed, failed, cancelled)
* Error information for failed jobs
* Timestamps (created, started, completed)
* Result storage (JSON)

# Job Types to Implement
Implement these mock job types to demonstrate your system:
1. Email Job
Simulates sending an email. Sleep 1-3 seconds, return success with mock message ID.
2. Webhook Job
Simulates calling an external webhook. Sleep 1-2 seconds. 80% success rate, 20% simulated failure (for testing
retry logic).
3. Report Job
Simulates generating a report. Sleep 3-5 seconds, return mock file URL.


