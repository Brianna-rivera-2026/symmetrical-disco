# Design Decisions


## 1. Job Pickup Strategy

**Approach chosen:** One job per process, scaled horizontally via container replicas. Workers compete as a unified consumer group using Redis Streams (`XREADGROUP`).

* **Mechanics:** Each worker process executes a blocking loop: `XREADGROUP` (requesting exactly 1 message) $\rightarrow$ claim and load job data from PostgreSQL $\rightarrow$ execute the job handler $\rightarrow$ update state in PostgreSQL $\rightarrow$ issue `XACK` to Redis. Concurrency is managed at the infrastructure layer via Docker Compose scaling (`docker compose up --scale worker=N`).

**Why:**

* **Reliable Delivery Tracking:** Redis Streams with Consumer Groups natively track message delivery state. When a worker reads a message via `XREADGROUP`, Redis assigns ownership to that specific worker and moves the message into a Pending Entries List (PEL). This guarantees that the message is explicitly assigned to exactly one worker at a time, preventing competing workers from processing the same stream item simultaneously.
* **True Process Isolation:** A fatal crash (e.g., an unhandled C-extension exception or a catastrophic memory leak) only drops a single job container, completely isolating failures.

**Trade-offs & Mitigations:**

* **Resource Overhead:** Running multiple OS processes is heavier than green threads or async event loops.
* **Idempotency vs. Race Conditions:** While Redis Streams guarantees exclusive delivery within the active consumer group, network disruptions or unexpected worker crashes before an `XACK` can leave messages in the PEL to be claimed later by a reaper/reconciliation process. To ensure this does not result in duplicate execution, the worker's `claim_job` phase utilizes an atomic PostgreSQL `UPDATE ... WHERE status IN ('pending', 'scheduled')` statement. Redundant claims from stream reprocessing are gracefully discarded by the worker, trading minor Redis stream bloat for absolute transactional integrity in the database.


## 2. Worker Crash Recovery

**Approach chosen:**  A dedicated single-instance
`ticker` service runs a reaper loop on top of the same Redis consumer group the workers read
from, reclaiming stale PEL entries with `XAUTOCLAIM` and routing them
through the same retry/backoff path used for an ordinary handler failure.

**Why:** The claim-guard, ack-after-commit ordering, and unique consumer
names built for the pickup path (Decision #1) made a timeout-based reaper
safe to add without touching that path. Folding recovery into the existing
retry/backoff machinery — instead of a bespoke "reset to pending" — means a
reaped job gets the same attempt-counting and terminal-failure semantics as
any other failure, rather than a second code path to keep in sync.

**Two layers, for two kinds of "stuck":**
- **Hung handler, process still alive:** each handler runs in a single-use
  thread (`run_with_timeout`, `app/worker/timeout.py`) bounded by
  `job_handler_timeout_s` (45s). Python can't forcibly kill a thread, so on
  timeout the worker abandons it, immediately routes the job through
  retry/backoff (`HandlerTimeout` → `schedule_retry_or_fail`), and recycles
  its own process (`max_handler_timeouts_before_recycle`, default 1) so the
  abandoned thread can't leak resources indefinitely.
- **Actually-dead process** (crash, OOM-kill, host failure): nothing
  in-process can react, so recovery falls entirely to the ticker's reaper.
  `job_handler_timeout_s` is validated to always be `< visibility_timeout_s`
  (60s), so the handler-layer timeout always resolves a hung job first — the
  reaper only ever sees jobs from processes that are truly gone.

**What happens if a worker crashes mid-job (high-level):**

1. The in-flight message stays unacked in the consumer group's PEL, and the
   job stays `processing` in Postgres — safe to leave alone, since the
   atomic claim-guard means no one else can double-claim it.
2. Once that PEL entry has been idle past `visibility_timeout_s`, the
   ticker's reaper reclaims it via `XAUTOCLAIM` and feeds it through the same
   retry/backoff path as any other failure — re-enqueue with backoff,
   immediate retry, or terminal `failed` at `max_attempts`.
3. If the crash happened even earlier — after the Postgres commit but before
   the job's own `XADD` — there's no stream message to reclaim at all.
   `reconcile_orphans()` and the reaper's own inline-recovery path both catch
   this by re-enqueuing any job still marked `is_synced_to_redis = False`
   past a short grace period.

**Trade-offs:**
- **Single ticker instance, no distributed lock.** `docker-compose.yml` runs
  exactly one `ticker` replica; scaling it to N>1 without adding
  leader-election would cause concurrent reapers to double-process. Accepted
  since only `worker` needs to scale horizontally — the ticker's own workload
  (promote/reconcile/reap ticks) doesn't.
- **Recovery is still timeout-based, not heartbeat-based**
- **The "Drain-Until-Not-Full" Loop:** To prevent a 100-second artificial lag when 10,000 jobs are scheduled at the exact same instant, the ticker implements a drain loop.
- **Pipelining:** To bypass network round-trip bottlenecks, the loop pipelines Redis XADD commands, executes a multi-member ZREM, and performs a bulk PostgreSQL update.

## 3. Priority Queue Implementation

**Approach chosen:** Three parallel streams — `jobs:stream:high`,
`jobs:stream:normal`, `jobs:stream:low`, one per `JobPriority` level — with
workers reading them in strict high → normal → low order.

**Why:** Redis Streams have no native priority primitive, so per-priority
streams sidestep that entirely: priority becomes "which stream," decided
once via `stream_for_priority(priority)` and reused at every enqueue site.
This is the "multiple streams" option from the original Phase 2 sketch —
chosen over Sorted Sets because it stays inside the existing consumer-group
machinery instead of needing custom dequeue logic outside it.

**Why not a Sorted Set:** A ZSET (score = priority/enqueue-time,
`ZPOPMIN`/`BZPOPMIN` to dequeue) would give ordering without extra streams,
but a pop is just a pop — no ownership tracking. There's no PEL, no
per-consumer claim, no `XACK`/`XAUTOCLAIM`. We specifically wanted consumer
groups (Decision #1's delivery guarantees, Decision #2's crash recovery),
and a ZSET doesn't have them: we'd have to hand-roll a "popped but not yet
acked" side-table plus our own staleness sweep to get back to where three
streams already start. Three streams cost more setup (N groups to
create/consume/reap instead of one) but reuse all of that machinery as-is.

**Mechanics:**
- Priority lives on the `Job` row (`priority` column, indexed, `JobPriority`
  enum, defaults to `normal`) and is client-settable at submission via the
  API's `JobSubmission.priority` field.
- `read_priority()` probes high → normal → low, non-blocking, returning as
  soon as one stream has a message — a **full high-priority backlog is
  drained** before normal/low are even checked. Only when all three are empty
  does it fall back to a single blocking `XREADGROUP` across all three.

**Trade-offs:**
- **Starvation, by design.** Priority is strict, not weighted/fair: a
  continuous high-priority backlog means normal/low are never read
  Acceptable without a fairness requirement, but a sustained flood of high-priority jobs would indefinitely delay everything else. If that ever becomes a real problem, the fix
  doesn't need to touch the priority model — every worker currently reads
  `settings.ordered_streams` (all three), so dedicating one or more workers
  to only the low-priority stream would guarantee it always makes progress,
  at the cost of a small config knob to let a worker's stream list be
  restricted.
- **N streams instead of 1** means N `XGROUP CREATE`/`XREADGROUP`/
  `XAUTOCLAIM` targets to keep in sync everywhere a stream is touched
  (pickup, reaper, promote) — more moving parts than a single stream, though
  `settings.ordered_streams` centralizes the list so nothing iterates them
  ad hoc.

---

## 4. Retry Backoff Strategy

**Approach chosen:** All retries go through one
shared path, no matter how the failure was discovered — a worker catching
its own handler's exception or timeout, or the ticker's reaper finding a
job whose worker died before it could react (Decision #2).

It lives in both places because of *when* each one is able to act. The
requirement is for a failed job to retry immediately, not on the next
periodic tick — so the worker itself has to trigger the retry synchronously,
the moment its own handler fails. But when the worker is the thing that
died, there's no "itself" left to do that — the ticker's reaper is what
discovers the abandoned job later and triggers the retry on the worker's
behalf instead. Since the worker-side retry function already had the
correct attempt-counting and backoff logic, having the ticker reuse it was
the natural choice over building a second implementation.

Either way the job ends up in the same place: back to `pending`/`scheduled`
with an incremented attempt count, or `failed` once it's out of attempts.

**Why:** One shared path means "handler failed" and "worker crashed" are
treated identically instead of two behaviors that could drift apart over
time. This was safe to add because redelivery was already safe (Decision
#1) — backoff only had to decide *when* to redeliver, not whether it was
safe to.

**Mechanics:**
- Each job carries an `attempts` counter and a per-row `max_attempts`
  (default 4).
- `backoff_delay(attempts, schedule)` looks up
  `retry_backoff_schedule = [0, 30, 120]` seconds: the 1st retry is
  immediate, the 2nd waits 30s, the 3rd+ waits 120s.
- A 0s delay → job goes straight back to `pending` and is re-enqueued
  immediately. A nonzero delay → job goes to `scheduled` and is inserted
  into the Redis delayed ZSET (`jobs:delayed`); the ticker's `promote_due()`
  moves it back to `pending` and enqueues it once due.
- At `attempts >= max_attempts`, the job is marked `failed` permanently
  instead of retried.

**Why not exponential backoff + jitter:** the more common pattern (delay =
`base * 2^attempts + random jitter`, capped) spreads retries out and avoids
many jobs retrying in lockstep after a shared outage. With only
`max_attempts = 4`, a fixed 3-entry table (`[0, 30, 120]`) gets the same
practical shape — near-immediate, then a short wait, then a longer one —
without a formula to tune. It doesn't scale as well if `max_attempts` grows
much beyond that, and see the thundering-herd trade-off below.

**Trade-offs:**
- **Cooperative cancellation only works for batch jobs.** Cancellation
  (`POST /jobs/{id}/cancel`) is purely cooperative — a handler has to
  explicitly poll for it. Only `handle_batch` does. To handle individual item failures, the system adopts a Permissive Batch pattern that isolates item-level errors using internal try/except blocks, aggregates the findings into a final result JSON summary, and allows the overall parent job to transition to COMPLETED.
- **Retry policy is global, not per job type or per submission.**
  `max_attempts` and the backoff schedule both come from `Settings`, the
  same for every job — there's no way to give a cheap, idempotent `webhook`
  job a more aggressive retry policy than an expensive `report` job, and no
  `max_attempts` field on `JobSubmission` for a caller to override it.


## 5. One Thing I Would Do Differently With More Time

**Worker heartbeats instead of a fixed job timeout.**

As designed (see Decision #2), stale-job detection is timeout-based: the
Phase 2 reaper scans the PEL for messages idle longer than a fixed threshold
(e.g., 30 minutes) and reclaims them via `XAUTOCLAIM`. That works, but one
fixed timeout is a poor fit across job types — a `report` job with a 45s
handler timeout and a hypothetical long-running export job would need the
same generous window, so a genuinely stuck fast job sits undetected for the
full timeout while the ceiling still has to be sized for the slowest handler.

With more time, I'd have each in-flight worker periodically write a
heartbeat (e.g., a Redis key with TTL, or `last_seen_at` in Postgres, keyed
by consumer name), refreshed on an interval well under the job's handler
timeout. The reaper would then key off "no heartbeat in N seconds" instead
of "message age > fixed threshold":

- Detection latency scales with actual liveness, not the slowest job type's
  worst case.
- A worker that's alive but legitimately slow keeps refreshing its heartbeat
  and is correctly left alone — a pure XADD-age timeout can't distinguish
  "still working" from "crashed" for long jobs.
- The same heartbeat signal can double as a liveness/readiness check for
  orchestration, instead of maintaining that separately.

**Trade-off:** adds a periodic write per in-flight job (extra Redis/Postgres
traffic) and a little worker-side bookkeeping, in exchange for tighter,
per-job-accurate crash detection than a one-size-fits-all timeout.

---

## 6. OpenShift (CRC) Live Verification — Task 15

Ran the full chart against a local CRC cluster (KEDA + Red Hat OpenTelemetry
operators installed via OLM Subscription since OperatorHub's UI wasn't
scripted for this pass; equivalent to the manual admin step in the chart
README). All checks in the plan's Task 15 passed. Three real OpenShift
incompatibilities surfaced that the plan's design couldn't have caught
without a live cluster, all fixed in the chart:

**1. `edoburu/pgbouncer`'s entrypoint hardcodes `user = postgres` in the
generated `pgbouncer.ini`**, telling pgbouncer to `setuid` after starting.
OpenShift's restricted SCC runs every container as an arbitrary non-root UID
with no setuid capability, so pgbouncer crash-looped with `failed to assume
identity of user 'postgres': Operation not permitted`. Fix: ship a
hand-written `pgbouncer.ini` via ConfigMap (`pgbouncer-ini-configmap.yaml`),
mounted by `subPath` onto the writable `emptyDir` at `/etc/pgbouncer` so the
entrypoint's separate (and still-needed) `userlist.txt`-writing logic keeps
working, while its ini-autogeneration path is skipped entirely because the
file already exists.

**2. CRI-O bind-mounts host RHEL entitlement secrets (`/run/secrets/rhsm`,
`/run/secrets/redhat.repo`) into every container**, shadowing any pod
volume a chart author mounts at that exact path. The `users-sync` hook Job
originally mounted the API-user-keys Secret at `/run/secrets` (matching
`Settings.api_user_keys_file`'s default), so the app's `open()` call saw the
host's entitlement directory instead of the mounted key and failed with
`FileNotFoundError`. Fix: mount the secret at `/etc/jobprocessor-secrets`
instead and point `Settings` at it via `API_USER_KEYS_FILE` env var on the
Job, rather than changing the app's default (keeps the default valid for
non-OpenShift deployments).

**3. KEDA's `ScaledObject` never went `Ready`** (`connection to redis failed:
dial tcp ... i/o timeout`, then a DNS `no such host` before that). The
`keda-operator` pod that actually polls Redis for stream lag lives in a
different namespace (`openshift-keda`), and the `redis-ingress`
NetworkPolicy only admitted same-namespace pods matching `component in
{api,worker,ticker}` — cross-namespace traffic was silently dropped by the
default-deny posture. Fix: added a `keda.enabled`-gated ingress rule to
`redis-ingress` allowing `namespaceSelector: kubernetes.io/metadata.name=
openshift-keda` + `podSelector: app=keda-operator` on port 6379.

**4. (Anticipated, not a bug) No `podLabels`/`additionalLabels` field
exists on the installed `OpenTelemetryCollector` CRD** (checked live via `oc
explain opentelemetrycollector.spec --recursive`), confirming the plan's
Task 13 fallback: NetworkPolicies target the operator's own default pod
label (`app.kubernetes.io/component: opentelemetry-collector`) instead of a
custom label from the CR.

**Verified live and working as designed, no further changes needed:**
TLS end-to-end (Postgres `pg_stat_ssl.ssl = t` for all PgBouncer
connections, Redis rejects plaintext and accepts TLS+auth, PgBouncer
presents/verifies serving certs both sides); default-deny NetworkPolicy
(an unrelated scratch pod cannot reach Postgres or Redis; the api pod
cannot reach the public internet); a real job submitted through the
edge-TLS Route reaches `completed` via the full API → Redis Streams →
worker path; the memory recycler (`worker.maxRssMb=30`) logs
`worker.recycling`, flips readiness, and exits 0 for a clean pod
replacement; KEDA scales the worker Deployment from 1 to 6 (the configured
`maxReplicas` ceiling — never past it) under a 200-job burst and scales
back down to 1 once the backlog drains; the OTel collector's debug exporter
shows live traces/metrics/logs from the app pods.

**Unrelated environment note:** the CRC VM's disk hit the kubelet
`DiskPressure` eviction threshold (~85% of its 32GB image) partway through
this pass, from image pulls plus exited-container layers accumulated across
several failed/retried installs — not caused by the chart. Freed by
`crictl rmi --prune` (safe: only removed the OLM catalog index images,
already-installed operators are unaffected) over the VM's SSH port
(`crc podman-env` reveals the key/port; `oc debug node` itself couldn't
schedule while disk pressure was active — a chicken-and-egg worth knowing
about if this recurs).

---

## 7. SSO Migration: API Keys → Kubernetes TokenReview — Task 9 (2026-07-14)

**Approach chosen:** Authentication delegated entirely to the cluster.
Requests carry a bearer token (`Authorization: Bearer <token>`) that the API
validates by calling the Kubernetes `TokenReview` API (the cluster's
apiserver in production, the `fake-tokenreview` sidecar in local dev) —
there is no local password/key store or `users` table in the application
database anymore.

**Ownership:** Jobs are scoped by `user_id`, populated from the OpenShift
User's UID (`status.user.uid` in the `TokenReview` response) rather than a
locally-issued identifier. This ties job ownership directly to the cluster's
notion of identity, so a user's job history survives credential rotation and
matches whatever the cluster's identity provider (htpasswd IdP via
`deploy/openshift/setup-idp.sh`) considers that user to be.

**Authorization gate:** A successfully-authenticated token is only accepted
if its `status.user.groups` includes `jobprocessor-users` (configurable via
`auth_required_group`); this is the single authorization check, applied
before any route logic runs. The group is provisioned cluster-wide by
`setup-idp.sh`, not by the application.

**What was removed:** the `X-API-Key` header scheme, the `users` DB table
and its migration, `app/users/sync.py`, the `api_user_keys_file` setting,
and `deploy/openshift/init-secrets.sh`. `docker-compose.yml`'s dev-only
`users-sync` service and inline `configs: api_user_keys` block were replaced
by the `fake-tokenreview` sidecar with two baked-in dev tokens (`dev-alice`,
`dev-bob`; see `tests/support/fake_tokenreview.py`).
