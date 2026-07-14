# SSO via Kubernetes TokenReview (htpasswd IdP) — Design

**Date:** 2026-07-14
**Replaces:** API-key authentication (`X-API-Key`), the mounted key secret,
`users-sync` hook job, and `deploy/openshift/init-secrets.sh`
(spec `2026-07-13-api-security-design.md` §identity remains otherwise valid).

## Goal

Delegate API authentication to the cluster. Users log in to OpenShift through
an **htpasswd identity provider** (`oc login`, then `oc whoami -t`) and call
the job API with `Authorization: Bearer <token>`. The API validates each token
by POSTing a **TokenReview** to the Kubernetes apiserver. This removes all
app-managed credentials: no key generation script, no secret mounts, no
`users` table, no sync job.

**Decisions made during brainstorming:**

- **TokenReview only** — the API-key path is deleted, not kept as a dev mode.
  Tests and docker-compose use a fake TokenReview HTTP endpoint.
- **Group-gated, no users table** — a token is authorized iff its TokenReview
  groups include the required group (default `jobprocessor-users`).
  No JIT rows, no allowlist table.
- **Ownership by UID** — `jobs.user_id` (UUID) is stamped from TokenReview's
  `status.user.uid` (the OpenShift `User` object's `metadata.uid`). Known
  trade-off: deleting and recreating a cluster user yields a new UID, orphaning
  their old jobs — acceptable (user deletion revokes access).
- **IdP setup owned by this repo** — a cluster-admin script configures the
  htpasswd IdP and Group (CRC/demo-cluster friendly).
- **TokenReview client is plain httpx** (approach A) — one stable v1 endpoint;
  the official `kubernetes` client is overkill and hostile to the HTTP-level
  test fake.

## 1. Authentication flow (API service)

New module `app/auth/tokenreview.py` replaces `app/users/` (both `keys.py`
and `sync.py` are deleted).

Per request, `get_current_user` in `app/api/deps.py`:

1. Extract the bearer token from `Authorization: Bearer …`
   (FastAPI `HTTPBearer(auto_error=False)`). Missing/malformed → **401**.
2. Look up SHA-256(token) in the TTL cache (the existing `KeyCache`
   implementation moves to `app/auth/cache.py` unchanged: bounded, TTL from
   the existing `auth_cache_ttl_s` setting, only successful validations are
   cached so revocation propagates within one TTL and the raw token is never
   stored).
3. On miss, POST to the apiserver:

   ```
   POST {auth_tokenreview_url}
   Authorization: Bearer <pod ServiceAccount token, read from auth_sa_token_file>
   {"apiVersion": "authentication.k8s.io/v1", "kind": "TokenReview",
    "spec": {"token": "<user's token>"}}
   ```

   Two tokens per call: the pod's own SA token authenticates *the API
   service* to the apiserver; the user's token is payload being reviewed.
   The SA token file is re-read on each TokenReview call (cheap at
   cache-miss frequency) so kubelet rotation of the projected token is
   picked up without a restart. TLS verifies against `auth_ca_file`.
4. `status.authenticated == false` → **401**.
5. Group gate: `auth_required_group` ∉ `status.user.groups` → **403**.
   (This also excludes ServiceAccount tokens unless deliberately added to
   the group.)
6. Success → `AuthedUser(id=UUID(status.user.uid), name=status.user.username)`
   — same frozen dataclass, cached, bound to log context, `enduser.id` span
   attribute, `request.state.authed_user_id` for the rate limiter. Nothing
   downstream of `AuthedUser` changes.

**Error handling:**

| Condition | Response |
|---|---|
| No/malformed Authorization header | 401 |
| `authenticated: false` | 401 |
| Authenticated but not in required group | 403 |
| Apiserver unreachable / 5xx / timeout | **503** (auth infra down, not client error) |
| `uid` missing or not a UUID | 401, logged at warning (unexpected IdP) |

A short httpx timeout (~2 s) bounds the worst case; the cache absorbs brief
apiserver blips for already-seen tokens. Metrics: the existing
`auth_validations` counter keeps `result` (`ok`, `missing_token`,
`invalid_token`, `forbidden_group`, `apiserver_error`) and `source`
(`cache`/`tokenreview`) labels. Log events renamed accordingly
(`auth.invalid_token`, etc.); tokens never logged.

## 2. Schema migration (users table dropped)

One Alembic migration:

- Drop FK `jobs.user_id → users.id` and drop the `users` table.
- `jobs.user_id` stays `UUID NULL` (existing rows keep their values; their
  UUIDs simply no longer resolve to rows — old jobs become effectively
  unowned, same as today's `SET NULL` semantics).
- Add `jobs.user_name TEXT NULL` — display/log convenience stamped at
  submission time; **never** used for authorization.
- Delete `app/models/user.py`; remove `upsert_user` /
  `get_user_by_key_hash` from `app/repository.py`.

Downgrade recreates an empty `users` table + FK (data is not restorable —
noted in the migration docstring).

There is **no data migration**: existing UIDs in old jobs were app-generated
and won't match cluster UIDs. Fresh clusters are unaffected; on the demo
cluster, old jobs simply belong to no one. Accepted.

## 3. Kubernetes / Helm changes

**New: dedicated ServiceAccount for the API only.** The chart currently
defines no SA (pods run as `default`). Granting TokenReview rights to
`default` would leak them to every pod in the namespace (workers, hooks,
postgres), so:

1. `api-serviceaccount.yaml` — SA `<fullname>-api`.
2. `api-deployment.yaml` — `serviceAccountName` set to it. Kubelet's
   auto-projected token (`/var/run/secrets/kubernetes.io/serviceaccount/`)
   is the only credential the pod needs — no secret mounts.
3. `api-tokenreview-rbac.yaml` — ClusterRoleBinding of the built-in
   **`system:auth-delegator`** ClusterRole to that SA, gated by
   `auth.rbac.create` (default `true`; cluster-scoped, so operators may
   create it out-of-band instead).

Workers/ticker/hooks need none of this — they never see user tokens.

**Removed:** `users-sync-job.yaml`, the `api-user-keys` secret mount, and
`secrets.apiUserKeysSecret` + related values.

**New values** (`auth.*`): `requiredGroup` (default `jobprocessor-users`),
`rbac.create`. TokenReview URL/token/CA paths default to in-cluster values
in app settings; the chart doesn't set them.

## 4. Cluster IdP setup (script)

`deploy/openshift/setup-idp.sh` **replaces** `init-secrets.sh`. Run once by
cluster-admin: `setup-idp.sh user1:pass1 [user2:pass2 …]`. Idempotent:

1. Build an htpasswd file (`htpasswd -B`) and create-or-update secret
   `jobprocessor-htpasswd` in `openshift-config`.
2. Patch `OAuth/cluster` to add an `HTPasswd` identity provider named
   `jobprocessor-htpasswd` — merge into `spec.identityProviders`, preserving
   existing IdPs; skip if already present.
3. Create Group `jobprocessor-users` (if absent) and `oc adm groups add-users`
   for each given user.

Passwords are taken from arguments (demo cluster); the script prints a
reminder that OAuth pod rollout takes ~1 min before logins work.
`bootstrap-cluster.sh` docs updated to reference it as the day-0 identity
step.

## 5. Settings

| Setting | Default | Notes |
|---|---|---|
| `auth_tokenreview_url` | `https://kubernetes.default.svc/apis/authentication.k8s.io/v1/tokenreviews` | tests/compose override |
| `auth_sa_token_file` | `/var/run/secrets/kubernetes.io/serviceaccount/token` | re-read per call |
| `auth_ca_file` | `/var/run/secrets/kubernetes.io/serviceaccount/ca.crt` | httpx `verify=` |
| `auth_required_group` | `jobprocessor-users` | group gate |
| `auth_cache_ttl_s` | `60.0` (existing) | token-validation cache |
| `auth_timeout_s` | `2.0` | httpx timeout per TokenReview |

`api_user_keys_file` is removed.

## 6. Tests and docker-compose

**Fake TokenReview** — one small module `tests/support/fake_tokenreview.py`:
an ASGI app that parses a TokenReview request and answers from a configured
`token -> (username, uid, groups)` map (unknown token →
`authenticated: false`; optional failure mode returning 500 for the 503
path).

- **Unit tests:** mount it in httpx via `ASGITransport` — no sockets. Cover:
  valid+in-group (200), wrong group (403), unknown token (401), missing
  header (401), apiserver error (503), cache hit skips second TokenReview
  call, uid→`AuthedUser` mapping.
- **Integration tests:** run the fake on a local socket (uvicorn task in a
  fixture), point `auth_tokenreview_url` at it over plain HTTP (an
  `auth_ca_file` of `""`/unset disables custom verify for http URLs).
  Ownership scoping tests re-use two fake users; rate-limit tests key off
  uid as before.
- **docker-compose:** the fake runs as a small sidecar service (same image,
  `python -m tests.support.fake_tokenreview` or a dedicated
  `deploy/dev/fake_tokenreview.py`) with two baked-in dev tokens
  (`dev-alice`, `dev-bob`); the api service points
  `AUTH_TOKENREVIEW_URL` at it. Local dev:
  `curl -H "Authorization: Bearer dev-alice" …`.

Per project convention, Helm/RBAC/IdP script changes are verified manually
on the cluster (no pytest for infra config).

## Out of scope

- OIDC or any non-htpasswd IdP (the app is IdP-agnostic anyway — it only
  sees TokenReview results, so swapping IdPs later needs no app change).
- Kubernetes RBAC (SubjectAccessReview) for per-endpoint authorization —
  group gate + ownership scoping is the whole authz model.
- Token issuance/refresh UX (`oc login` is the client story).
- Migrating existing job ownership to cluster UIDs.
