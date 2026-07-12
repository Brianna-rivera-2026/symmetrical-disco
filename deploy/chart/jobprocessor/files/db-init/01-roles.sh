#!/usr/bin/env bash
# Creates the least-privilege role pair (spec §5). Idempotent — safe to run
# on every start. Runs under two images:
#   - postgres:16 (compose):   /docker-entrypoint-initdb.d/, fresh init only
#   - sclorg postgresql (Helm): /opt/app-root/src/postgresql-start/, EVERY
#     start — which is what migrates existing volumes automatically.
set -euo pipefail

DB="${POSTGRES_DB:-${POSTGRESQL_DATABASE:?no database name in env}}"
SUPERUSER="${POSTGRES_USER:-postgres}"
: "${APP_DB_PASSWORD:?}" "${MIGRATOR_DB_PASSWORD:?}"

psql -v ON_ERROR_STOP=1 --username "$SUPERUSER" --dbname "$DB" \
     -v db="$DB" -v app_pw="$APP_DB_PASSWORD" -v mig_pw="$MIGRATOR_DB_PASSWORD" <<'SQL'
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'jobs_migrator') THEN
    CREATE ROLE jobs_migrator LOGIN;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'jobs_app') THEN
    CREATE ROLE jobs_app LOGIN;
  END IF;
END
$$;

-- psql var interpolation does not reach inside dollar-quoted DO bodies,
-- so passwords are set here instead.
ALTER ROLE jobs_migrator PASSWORD :'mig_pw';
ALTER ROLE jobs_app PASSWORD :'app_pw';

GRANT CONNECT ON DATABASE :"db" TO jobs_migrator, jobs_app;
ALTER SCHEMA public OWNER TO jobs_migrator;
GRANT USAGE, CREATE ON SCHEMA public TO jobs_migrator;
GRANT USAGE ON SCHEMA public TO jobs_app;

-- Adopt tables/sequences created before the role split (no-op on fresh DBs).
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT c.relname, c.relkind
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'public'
      AND c.relkind IN ('r', 'p', 'S')
      AND pg_get_userbyid(c.relowner) <> 'jobs_migrator'
  LOOP
    IF r.relkind = 'S' THEN
      EXECUTE format('ALTER SEQUENCE public.%I OWNER TO jobs_migrator', r.relname);
    ELSE
      EXECUTE format('ALTER TABLE public.%I OWNER TO jobs_migrator', r.relname);
    END IF;
  END LOOP;
END
$$;

-- Explicit grants: zero rows on fresh init; they are what makes re-runs
-- against existing databases pick up pre-existing tables.
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO jobs_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO jobs_app;

-- Future objects created by migrations get granted automatically.
ALTER DEFAULT PRIVILEGES FOR ROLE jobs_migrator IN SCHEMA public
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO jobs_app;
ALTER DEFAULT PRIVILEGES FOR ROLE jobs_migrator IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO jobs_app;
SQL
