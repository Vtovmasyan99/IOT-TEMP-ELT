# IoT Temperature ELT

Lightweight ELT pipeline that ingests CSV files from a landing directory into PostgreSQL, applies in-database transformations, and maintains run metadata, rejects, and final curated tables.

## Features

- Strict CSV header validation and normalization
- Fast bulk load to staging via PostgreSQL COPY
- In-database transformation and validation with clear metrics (valid/rejected)
- Run metadata tracking (`elt_runs`) and failure logging to `logs/failures.log`
- Idempotency based on file checksum (skips already successful files)
- Prefect 2 flows for orchestration (local run or agent mode)

## Project layout

- `elt/`: pipeline code
  - `flow.py`: entrypoint with `process_directory` and CLI (`init_db`/run)
  - `db.py`: engine creation, DDL init, COPY, transform execution
  - `io.py`: file discovery, hashing, safe moving utilities
  - `dq.py`: CSV schema validation
  - `sql/ddl.sql`: schema creation
  - `sql/transform.sql`: transformation/upsert + rejects
- `landing_zone/`: drop CSV files here for ingestion
- `archive/`: successfully processed files are moved here
- `error/`: files that fail validation/processing are moved here
- `logs/failures.log`: prepend-only log of failures

## Requirements

- Python 3.11+
- PostgreSQL 13+ reachable from the runtime
- Packages from `requirements.txt`

## Environment variables

You can provide a `DATABASE_URL` or the usual libpq variables. Precedence:

1) `DATABASE_URL`
2) `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`
3) `POSTGRES_HOST`/`POSTGRES_PORT`/`POSTGRES_DB`/`POSTGRES_USER`/`POSTGRES_PASSWORD` or `DB_HOST`/`DB_PORT`/`DB_NAME`/`DB_USER`/`DB_PASSWORD`

Optional runtime variables:

- `LANDING_DIR` (default: `./landing_zone`)
- `ARCHIVE_DIR` (default: `./archive`)
- `ERROR_DIR` (default: `./error`)
- `LOG_DIR` (default: `./logs`)
- `FAILURE_LOG_FILE` (default: `./logs/failures.log`)
- `VERBOSE_LOGS` = `1|true|yes|on` to show full tracebacks
- `RUN_AS_AGENT` = `1|true|yes|on` to serve Prefect agent instead of immediate run
- `PREFECT_API_SERVE` defaulted to `false` locally to avoid auto-starting a server

Example `.env` :

```
# Either provide DATABASE_URL or PG* variables
# DATABASE_URL=postgresql+psycopg2://postgres:password@localhost:5432/iot
PGHOST=localhost
PGPORT=5432
PGDATABASE=iot
PGUSER=postgres
PGPASSWORD=your_password

# Optional paths (container defaults to /app/*)
LANDING_DIR=${PWD}/landing_zone
ARCHIVE_DIR=${PWD}/archive
ERROR_DIR=${PWD}/error
LOG_DIR=${PWD}/logs
VERBOSE_LOGS=1
```

## Install and run locally

```
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

# load env vars
set -a && source .env 2>/dev/null || true && set +a

# initialize database schema
python -m elt.flow init_db

# run the pipeline once over LANDING_DIR
python -m elt.flow
```

Place CSV files (with exact header `id,room_id/id,noted_date,temp,out/in`) into `landing_zone/`. On success, files move to `archive/`; on failure, to `error/`. Failures are also recorded in `logs/failures.log`.

## Expected CSV schema

- Exact headers (names and order): `[id, room_id/id, noted_date, temp, out/in]`
- `noted_date` format: `DD-MM-YYYY HH24:MI` (e.g., `01-02-2020 13:45`)
- `temp` numeric range enforced: -50 to 80 (Celsius)
- `out/in` accepted values: `In` or `Out` (case-insensitive; normalized)

## Database objects created

- `staging_temperature_raw`
- `temperature_readings` (final curated table)
- `temperature_rejects` (invalid rows with reason)
- `elt_runs` (run metadata and metrics)

## Run with Docker

Build the image:

```
docker build -t iot-elt:latest .
```

Connectivity note: If your PostgreSQL is on the host machine, add `-e PGHOST=host.docker.internal` on macOS/Windows to let the container reach it. On Linux, you can use `--network=host` or expose the DB port and use the host IP.

Initialize the database schema (override the default CMD to run only init):

```
docker run --rm \
  --env-file .env \
  -e PGHOST=host.docker.internal \
  -v "$(pwd)/landing_zone:/app/landing_zone" \
  -v "$(pwd)/archive:/app/archive" \
  -v "$(pwd)/error:/app/error" \
  -v "$(pwd)/logs:/app/logs" \
  iot-elt:latest python -m elt.flow init_db
```

Run the pipeline (default CMD initializes DB then runs the flow):

```
docker run --rm \
  --env-file .env \
  -e PGHOST=host.docker.internal \
  -v "$(pwd)/landing_zone:/app/landing_zone" \
  -v "$(pwd)/archive:/app/archive" \
  -v "$(pwd)/error:/app/error" \
  -v "$(pwd)/logs:/app/logs" \
  iot-elt:latest
```

Run the pipeline without the init step (override CMD):


```
docker run --rm \
  --env-file .env \
  -e PGHOST=host.docker.internal \
  -v "$(pwd)/landing_zone:/app/landing_zone" \
  -v "$(pwd)/archive:/app/archive" \
  -v "$(pwd)/error:/app/error" \
  -v "$(pwd)/logs:/app/logs" \
  iot-elt:latest python -m elt.flow
```

Check PostgreSQL connectivity from your host:


```
# Requires psql installed and env loaded
set -a && source .env 2>/dev/null || true && set +a

# Using pg_isready (if available)
pg_isready -h "${PGHOST:-localhost}" -p "${PGPORT:-5432}" -d "${PGDATABASE:-iot}" -U "${PGUSER:-postgres}"

# Or via psql (returns 1 row)
PGPASSWORD="${PGPASSWORD}" psql -h "${PGHOST:-localhost}" -p "${PGPORT:-5432}" -U "${PGUSER:-postgres}" -d "${PGDATABASE:-iot}" -c "select 1;"
```



## Notes



- If you change CSV schema, update `elt/dq.py`, `elt/db.py`, and `elt/sql/*` accordingly.
- For troubleshooting, set `VERBOSE_LOGS=1` to see full tracebacks in logs.

## Schedule with cron + Docker (every 15 minutes)

Run the container on a fixed cadence using your host crontab. This triggers a one-shot run that processes any CSVs in `landing_zone` and moves them to `archive`/`error`.

Example crontab entry (macOS/Linux):

```
*/15 * * * * cd /path/to/iot-temp-elt && \
docker run --rm \
  --env-file .env \
  -e PGHOST=host.docker.internal \
  -v "/path/to/iot-temp-elt/landing_zone:/app/landing_zone" \
  -v "/path/to/iot-temp-elt/archive:/app/archive" \
  -v "/path/to/iot-temp-elt/error:/app/error" \
  -v "/path/to/iot-temp-elt/logs:/app/logs" \
  iot-elt:latest
```

Notes:

- Replace `/path/to/iot-temp-elt` with your absolute project path.
- On macOS/Windows Docker Desktop, `-e PGHOST=host.docker.internal` lets the container reach your local Postgres.
- On Linux, use your host IP or `--network=host` if appropriate, and omit `host.docker.internal`.
- The image’s default CMD initializes the schema then runs the flow. To skip init on each run, override the command:

```
docker run --rm ... iot-elt:latest python -m elt.flow
```


