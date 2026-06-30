Focus: Integrate the ZSET to handle delayed work.

Modify the API: If a user submits a job with a future execution parameter, the API writes the Postgres status as SCHEDULED. Instead of pushing to the Stream, it pushes to a Redis ZSET: ZADD jobs:delayed <unix_timestamp> <job_id>.

Build the Ticker Process: Write a small, independent Python background loop (or a separate thread in your API) that runs every 1 second.

The Handshake: The Ticker runs ZRANGEBYSCORE jobs:delayed 0 <current_timestamp>. If it finds mature job_ids, it removes them from the ZSET via ZREM and pushes them onto the main active stream. It then updates the Postgres status from SCHEDULED to PENDING (this can fail if the worker picked it up before, its ok). When the worker claims the job, its very first step is to open a transaction in PostgreSQL to move the job to PROCESSING. To make this fault-tolerant, write your worker's state update query to accept either preceding state: 'PENDING', 'SCHEDULED'.

Make sure to pay attention to edge cases and network failures.

Add this to the job model and update the api 
* Scheduling (optional future execution time)