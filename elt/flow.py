import os
import sys
import uuid
import csv
from datetime import datetime, timezone
from typing import Dict, Optional

from prefect import flow, task, get_run_logger
from sqlalchemy import text

from . import db
from . import io as io_utils
from . import dq
import logging
import traceback


def _project_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), os.pardir))


DEFAULT_LANDING_DIR = os.path.join(_project_root(), "landing_zone")
ARCHIVE_DIR = os.getenv("ARCHIVE_DIR", os.path.join(_project_root(), "archive"))
ERROR_DIR = os.getenv("ERROR_DIR", os.path.join(_project_root(), "error"))

# Failure logging
LOG_DIR = os.getenv("LOG_DIR", os.path.join(_project_root(), "logs"))
FAILURE_LOG_FILE = os.getenv("FAILURE_LOG_FILE", os.path.join(LOG_DIR, "failures.log"))
VERBOSE_LOGS = os.getenv("VERBOSE_LOGS", "1").lower() in {"1", "true", "yes", "on"}


def _classify_error(exc: Exception) -> str:
    text_msg = str(exc).lower()
    if isinstance(exc, io_utils.IOErrorWithContext):
        return "file_io_error"
    if "on conflict do update" in text_msg:
        return "duplicate_id_conflict"
    if "copy" in text_msg and "staging_temperature_raw" in text_msg:
        return "staging_copy_error"
    if "csv" in text_msg or "header" in text_msg:
        return "csv_format_error"
    if "psycopg2" in text_msg or "sqlalchemy" in text_msg:
        return "database_error"
    return "processing_error"


def _append_failure_log(source_file: str, run_id: str, reason: str, details: str) -> None:
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        line = f"[{ts}] file={source_file} run_id={run_id} reason={reason} details={details}\n"
        # Prepend new entries to keep latest on top
        try:
            with open(FAILURE_LOG_FILE, "r", encoding="utf-8") as f:
                prev = f.read()
        except FileNotFoundError:
            prev = ""
        with open(FAILURE_LOG_FILE, "w", encoding="utf-8") as f:
            f.write(line)
            if prev:
                f.write(prev)
    except Exception:
        # Never let logging failure break the flow
        pass

# Reduce noisy tracebacks from common noisy libraries when not verbose
if not VERBOSE_LOGS:
    _NOISY_LOGGERS = (
        "uvicorn",
        "uvicorn.error",
        "uvicorn.access",
        "starlette",
        "fastapi",
        "prefect.server",
        "sqlalchemy.pool",
        "aiosqlite",
    )
    for _name in _NOISY_LOGGERS:
        try:
            logging.getLogger(_name).setLevel(logging.CRITICAL)
        except Exception:
            pass

# Do not start Prefect's temporary API server when running locally unless explicitly enabled
os.environ.setdefault("PREFECT_API_SERVE", "false")


def _count_csv_rows(path: str) -> int:
    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            # Count lines excluding header
            total_lines = sum(1 for _ in f)
            return max(total_lines - 1, 0)
    except Exception:
        # Non-fatal; return 0 if counting fails
        return 0


@task
def start_run(source_file: str, checksum: str, rows_in_file: Optional[int] = None) -> str:
    """Insert a new row into elt_runs with status 'running' and return run_id."""
    run_id = str(uuid.uuid4())
    engine = db.get_engine()
    started_at = datetime.now(timezone.utc)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                insert into elt_runs (
                  run_id, source_file, file_checksum_sha256, started_at, status, rows_in_file
                ) values (
                  :run_id, :source_file, :checksum, :started_at, 'running', :rows_in_file
                )
                """
            ),
            {
                "run_id": run_id,
                "source_file": source_file,
                "checksum": checksum,
                "started_at": started_at,
                "rows_in_file": rows_in_file,
            },
        )
    return run_id


@task
def load_to_staging(run_id: str, source_file: str, path: str) -> Dict[str, int]:
    try:
        n = db.copy_csv_to_staging(run_id, source_file, path)
        return {"rows_loaded_staging": int(n)}
    except Exception as exc:
        if VERBOSE_LOGS:
            # Re-raise to allow full Prefect traceback in terminal when verbose
            raise
        return {
            "__failed__": True,
            "reason": _classify_error(exc),
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }


@task
def transform_and_load(run_id: str, source_file: str) -> Dict[str, int]:
    try:
        return db.run_transform(run_id, source_file)
    except Exception as exc:
        if VERBOSE_LOGS:
            # Re-raise to show full traceback when verbose
            raise
        reason = _classify_error(exc)
        return {
            "__failed__": True,
            "reason": reason,
            "error": str(exc),
            "traceback": traceback.format_exc(),
            "rows_valid": 0,
            "rows_rejected": 0,
        }


@task
def finalize_run(
    run_id: str,
    status: str,
    counts: Optional[Dict[str, int]] = None,
    message: Optional[str] = None,
) -> None:
    counts = counts or {}
    rows_loaded_staging = int(counts.get("rows_loaded_staging", 0))
    rows_valid = int(counts.get("rows_valid", 0))
    rows_rejected = int(counts.get("rows_rejected", 0))

    engine = db.get_engine()
    ended_at = datetime.now(timezone.utc)
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                update elt_runs
                set ended_at = :ended_at,
                    status = :status,
                    rows_loaded_staging = :rows_loaded_staging,
                    rows_valid = :rows_valid,
                    rows_rejected = :rows_rejected,
                    message = :message
                where run_id = :run_id
                """
            ),
            {
                "ended_at": ended_at,
                "status": status,
                "rows_loaded_staging": rows_loaded_staging,
                "rows_valid": rows_valid,
                "rows_rejected": rows_rejected,
                "message": message,
                "run_id": run_id,
            },
        )


def _already_processed_success(checksum: str) -> bool:
    engine = db.get_engine()
    with engine.begin() as conn:
        res = conn.execute(
            text(
                "select 1 from elt_runs where file_checksum_sha256 = :checksum and status = 'success' limit 1"
            ),
            {"checksum": checksum},
        )
        return res.first() is not None


@flow(name="process_file")
def process_file(path: str) -> Optional[str]:
    logger = get_run_logger()
    if not path or not os.path.isfile(path):
        logger.error("Path is not a file: %s", path)
        return None

    source_file = os.path.basename(path)
    checksum = io_utils.sha256_of_file(path)

    if _already_processed_success(checksum):
        logger.info("Skipping %s: checksum already ingested.", source_file)
        return None

    run_id: Optional[str] = None
    rows_loaded_staging = 0
    counts: Dict[str, int] = {"rows_valid": 0, "rows_rejected": 0}

    try:
        # Preflight: strictly validate CSV headers before any DB work
        try:
            with open(path, "r", encoding="utf-8-sig", newline="") as f:
                reader = csv.reader(f)
                try:
                    header = next(reader)
                except StopIteration:
                    raise dq.HeaderValidationError("CSV header row is missing")
            dq.validate_headers(header)
        except Exception as header_exc:
            reason = _classify_error(header_exc)
            details = str(header_exc)
            _append_failure_log(source_file, "-", reason, details)
            moved = io_utils.safe_move(path, ERROR_DIR)
            if VERBOSE_LOGS:
                logger.error("Rejected %s due to header validation: %s. Moved to %s", source_file, details, moved)
            else:
                logger.error("Rejected file due to header validation. Moved to error folder.")
            return None

        rows_in_file = _count_csv_rows(path)
        run_id = start_run(source_file, checksum, rows_in_file)

        load_result = load_to_staging(run_id, source_file, path)

        if isinstance(load_result, dict) and load_result.get("__failed__"):
            reason = str(load_result.get("reason", "processing_error"))
            details = str(load_result.get("traceback") or load_result.get("error", ""))
            counts.update({"rows_valid": 0, "rows_rejected": 0, "rows_loaded_staging": 0})
            finalize_run(run_id, "failed", counts, details)
            _append_failure_log(source_file, run_id, reason, details)
            moved = io_utils.safe_move(path, ERROR_DIR)
            if VERBOSE_LOGS:
                logger.error("Failed processing %s: %s. Moved to %s", source_file, reason, moved)
            else:
                logger.error("Failed processing %s. Moved to error folder.", source_file)
            return None

        rows_loaded_staging = int(load_result.get("rows_loaded_staging", 0))

        xform_counts = transform_and_load(run_id, source_file)

        # If transform task returned failure payload, treat as failure without raising stack trace
        if isinstance(xform_counts, dict) and xform_counts.get("__failed__"):
            reason = str(xform_counts.get("reason", "processing_error"))
            details = str(xform_counts.get("traceback") or xform_counts.get("error", ""))
            counts.update({"rows_valid": 0, "rows_rejected": 0, "rows_loaded_staging": rows_loaded_staging})
            finalize_run(run_id, "failed", counts, details)
            _append_failure_log(source_file, run_id, reason, details)
            moved = io_utils.safe_move(path, ERROR_DIR)
            if VERBOSE_LOGS:
                logger.error("Failed processing %s: %s. Moved to %s", source_file, reason, moved)
            else:
                logger.error("Failed processing %s. Moved to error folder.", source_file)
            return None

        counts.update(xform_counts)
        counts["rows_loaded_staging"] = rows_loaded_staging

        finalize_run(run_id, "success", counts, None)

        moved = io_utils.safe_move(path, ARCHIVE_DIR)
        logger.info("Archived file to %s", moved)
        return run_id
    except Exception as exc:
        reason = _classify_error(exc)
        details = str(exc)
        if VERBOSE_LOGS:
            logger.exception("Failed processing %s", source_file)
        else:
            logger.error("Failed processing %s. Moved to error folder.", source_file)
        if run_id:
            try:
                # Ensure counts at least contain staging rows if any
                if "rows_loaded_staging" not in counts:
                    counts["rows_loaded_staging"] = rows_loaded_staging
                finalize_run(run_id, "failed", counts, details)
            except Exception:
                pass
            _append_failure_log(source_file, run_id, reason, details)

        try:
            moved = io_utils.safe_move(path, ERROR_DIR)
            # Keep this info minimal (no exception details)
            logger.info("Moved failed file to error folder")
        except Exception as move_exc:
            logger.error("Failed to move errored file to error directory")
        return None


@flow(name="process_directory")
def process_directory(directory: Optional[str] = None) -> None:
    logger = get_run_logger()
    directory = directory or os.getenv("LANDING_DIR", DEFAULT_LANDING_DIR)
    logger.info("Processing directory: %s", directory)

    for csv_path in io_utils.discover_csvs(directory):
        process_file(csv_path)


def _run_cli() -> None:
    if len(sys.argv) > 1 and sys.argv[1].lower() == "init_db":
        db.init_db()
        print("Initialized database schema.")
        return

    run_as_agent = os.getenv("RUN_AS_AGENT", "").lower() in {"1", "true", "yes", "on"}
    if run_as_agent:
        process_directory.serve(name="process_directory")
    else:
        process_directory()


if __name__ == "__main__":
    _run_cli()


