# GitHub Actions CI Design

## Purpose
Add continuous integration to catch test failures, lint issues, and broken Helm
chart changes on every pull request and push to `main`.

## Scope
- Run the full pytest suite (`tests/unit` + `tests/integration`).
- Run `ruff check` and `ruff format --check`.
- Lint the Helm chart at `deploy/chart/jobprocessor`.
- Out of scope: deployment, publishing images, Helm chart packaging/release,
  coverage reporting.

## Triggers
`pull_request` (any branch) and `push` to `main`.

## Jobs

### `test` (ubuntu-latest)
1. Checkout repo.
2. Install `uv` via `astral-sh/setup-uv`, with dependency caching enabled.
3. `uv sync` (installs the default `dev` dependency group: pytest, ruff,
   testcontainers, httpx).
4. `uv run ruff check .`
5. `uv run ruff format --check .`
6. `uv run pytest tests/unit tests/integration`

`tests/integration` uses `testcontainers` (Postgres 16, Redis 7), which talks
to the Docker daemon already present on GitHub-hosted `ubuntu-latest` runners.
No extra `services:` block or manual container setup is required — the
containers are started and torn down by the test fixtures themselves
(`tests/integration/conftest.py`).

### `helm-lint` (ubuntu-latest)
1. Checkout repo.
2. Set up `helm` via `azure/setup-helm`.
3. `helm lint deploy/chart/jobprocessor`.

Runs independently of `test` (no shared dependency), so both jobs execute in
parallel.

## Failure handling
Either job failing fails the workflow run and blocks the PR check; no retry
or continue-on-error logic — a red CI run should be visible and actionable.

## File
Single workflow file: `.github/workflows/ci.yml`.
