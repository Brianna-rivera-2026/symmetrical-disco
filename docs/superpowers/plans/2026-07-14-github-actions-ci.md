# GitHub Actions CI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a GitHub Actions workflow that runs the pytest suite (unit + integration), ruff lint/format checks, and Helm chart linting on every pull request and push to `main`.

**Architecture:** Single workflow file `.github/workflows/ci.yml` with two independent jobs (`test`, `helm-lint`) that run in parallel. `test` uses `astral-sh/setup-uv` to install the project via `uv sync`, then runs ruff and pytest; `tests/integration` spins up its own Postgres/Redis via `testcontainers` against the Docker daemon already on the runner, so no `services:` block is needed. `helm-lint` uses `azure/setup-helm` and runs `helm lint` against the chart.

**Tech Stack:** GitHub Actions, `astral-sh/setup-uv`, `azure/setup-helm`, uv, pytest, ruff, testcontainers, Helm.

## Global Constraints

- Spec: `docs/superpowers/specs/2026-07-14-github-actions-ci-design.md`
- Triggers: `pull_request` (any branch) and `push` to `main` — from spec "Triggers" section.
- No `services:` block for Postgres/Redis — testcontainers manages its own containers (spec "test" job, step 6 note).
- Two independent jobs (`test`, `helm-lint`) run in parallel, no `needs:` dependency between them (spec "helm-lint" section).
- Either job failing must fail the whole workflow run — no `continue-on-error` (spec "Failure handling").
- This is a config change (CI YAML), not application code — per project convention, verify by running the equivalent commands locally and by confirming the workflow file is well-formed, not by writing pytest tests for the YAML itself.

---

### Task 1: Create the CI workflow file with the `test` job

**Files:**
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Produces: a GitHub Actions workflow named `CI` with job `test`, which Task 2 will extend by adding a sibling job `helm-lint` in the same file.

- [ ] **Step 1: Verify the commands the job will run all succeed locally**

Run each of these from the repo root and confirm they exit 0:

```bash
uv sync
uv run ruff check .
uv run ruff format --check .
uv run pytest tests/unit tests/integration
```

Expected: all four commands succeed (exit code 0). If `ruff format --check` fails because files aren't formatted yet, run `uv run ruff format .` first, review the diff, and commit the formatting fix separately before continuing — the workflow must not be built around a red baseline.

- [ ] **Step 2: Write `.github/workflows/ci.yml` with the `test` job**

```yaml
name: CI

on:
  pull_request:
  push:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v3
        with:
          enable-cache: true

      - name: Install dependencies
        run: uv sync

      - name: Ruff check
        run: uv run ruff check .

      - name: Ruff format check
        run: uv run ruff format --check .

      - name: Run tests
        run: uv run pytest tests/unit tests/integration
```

- [ ] **Step 3: Validate the YAML is well-formed**

Run:

```bash
uv run python -c "import yaml, sys; yaml.safe_load(open('.github/workflows/ci.yml'))" && echo OK
```

Expected: prints `OK`. (`pyyaml` is pulled in transitively via other deps; if this import fails with `ModuleNotFoundError: yaml`, run `uv run --with pyyaml python -c "..."` instead as a one-off check — do not add `pyyaml` as a project dependency for this.)

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add GitHub Actions workflow for pytest and ruff"
```

---

### Task 2: Add the `helm-lint` job to the same workflow

**Files:**
- Modify: `.github/workflows/ci.yml` (add a second top-level job under `jobs:`)

**Interfaces:**
- Consumes: the `jobs:` mapping produced in Task 1 — `helm-lint` is inserted as a sibling of `test`, not nested under it, and has no `needs:` on `test`.

- [ ] **Step 1: Verify `helm lint` passes locally against the chart**

```bash
helm lint deploy/chart/jobprocessor
```

Expected: output ending in `1 chart(s) linted, 0 chart(s) failed`. If it fails, fix the chart issue first — do not wire up CI around a chart that doesn't lint cleanly.

- [ ] **Step 2: Add the `helm-lint` job to `.github/workflows/ci.yml`**

Append this job to the existing `jobs:` mapping (as a sibling of `test`, same indentation level):

```yaml
  helm-lint:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Install Helm
        uses: azure/setup-helm@v4

      - name: Lint chart
        run: helm lint deploy/chart/jobprocessor
```

The full file after this step should read:

```yaml
name: CI

on:
  pull_request:
  push:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Install uv
        uses: astral-sh/setup-uv@v3
        with:
          enable-cache: true

      - name: Install dependencies
        run: uv sync

      - name: Ruff check
        run: uv run ruff check .

      - name: Ruff format check
        run: uv run ruff format --check .

      - name: Run tests
        run: uv run pytest tests/unit tests/integration

  helm-lint:
    runs-on: ubuntu-latest
    steps:
      - name: Checkout
        uses: actions/checkout@v4

      - name: Install Helm
        uses: azure/setup-helm@v4

      - name: Lint chart
        run: helm lint deploy/chart/jobprocessor
```

- [ ] **Step 3: Validate the YAML is still well-formed with two jobs**

```bash
uv run python -c "import yaml, sys; d = yaml.safe_load(open('.github/workflows/ci.yml')); assert set(d['jobs']) == {'test', 'helm-lint'}, d['jobs'].keys(); print('OK', list(d['jobs']))"
```

Expected: prints `OK ['test', 'helm-lint']`.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: add helm lint job"
```

---

### Task 3: Push and confirm the workflow runs green on GitHub

**Files:**
- None (verification only, no file changes)

**Interfaces:**
- Consumes: the completed `.github/workflows/ci.yml` from Tasks 1–2.

- [ ] **Step 1: Push the branch**

```bash
git push
```

Expected: push succeeds. (If the branch has no upstream yet: `git push -u origin HEAD`.)

- [ ] **Step 2: Confirm the workflow triggers and both jobs succeed**

```bash
gh run list --branch "$(git branch --show-current)" --limit 1
```

Then watch it:

```bash
gh run watch --exit-status
```

Expected: the run referencing the `ci.yml` workflow completes with conclusion `success` for both the `test` and `helm-lint` jobs. If `gh` reports a failure, open the run in the browser (`gh run view --web`) to read the failing step's log before making any fix.
