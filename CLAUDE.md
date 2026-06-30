# Python Rules (uv)

This project is distributed background job processing system that uses **uv** for environment and package management. Do not use raw `pip`, `venv`, or `poetry`.

## Commands for Claude

- **Run Python/Tools:** Always prefix with `uv run` (e.g., `uv run python main.py`, `uv run pytest`).
- **Add Dependency:** `uv add <package>` (use `--dev` for linting/testing tools).
- **Remove Dependency:** `uv remove <package>`.
- **Sync Environment:** `uv sync` if `pyproject.toml` changes.

## Tooling Quick-Reference
- **Test:** `uv run pytest`
- **Lint/Format:** `uv run ruff check --fix` and `uv run ruff format`

## Strict Guidelines
1. Never suggest `python -m venv` or `pip install`.
2. Always run tests via `uv run pytest` before declaring a task complete.
3. Don't use print statements, only structured logging with job context

## Tech stack
* FastAPI for the API
* PostgreSQL as the source of truth
* Redis for the queue (Redis Streams + Consumer Group for dispatch), scheduled jobs
* SQLAlchemy 2.0 + Alembic
* Pytest for testing
* Docker Compose for orchestration
* structlog for structured logging

## Architecture
1. API Service - Accepts job submissions and status queries
2. Queue - Holds pending jobs (use any queue/cache technology)
3. Multiple concurrent Worker Processes - Pull jobs and execute them
4. Database - Stores job state and results
