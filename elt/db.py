import csv
import os
from io import StringIO
from typing import Any, Dict, Iterable, List, Mapping, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Result
from . import dq


def _project_path(*parts: str) -> str:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, *parts)


def get_engine() -> Engine:
    """Create a SQLAlchemy engine using environment variables.

    Precedence:
    1) DATABASE_URL
    2) libpq-style envs: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
    3) POSTGRES_*/DB_* fallbacks
    """
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        return create_engine(database_url, pool_pre_ping=True)

    host = (
        os.getenv("PGHOST")
        or os.getenv("POSTGRES_HOST")
        or os.getenv("DB_HOST")
        or "localhost"
    )
    port = (
        os.getenv("PGPORT")
        or os.getenv("POSTGRES_PORT")
        or os.getenv("DB_PORT")
        or "5432"
    )
    dbname = (
        os.getenv("PGDATABASE")
        or os.getenv("POSTGRES_DB")
        or os.getenv("DB_NAME")
        or "postgres"
    )
    user = (
        os.getenv("PGUSER")
        or os.getenv("POSTGRES_USER")
        or os.getenv("DB_USER")
        or "postgres"
    )
    password = (
        os.getenv("PGPASSWORD")
        or os.getenv("POSTGRES_PASSWORD")
        or os.getenv("DB_PASSWORD")
        or ""
    )

    safe_password = password.replace("@", "%40") if "@" in password else password
    url = f"postgresql+psycopg2://{user}:{safe_password}@{host}:{port}/{dbname}"
    return create_engine(url, pool_pre_ping=True)


def init_db(engine: Optional[Engine] = None) -> None:
    """Execute schema DDL from elt/sql/ddl.sql."""
    engine = engine or get_engine()
    ddl_path = _project_path("sql", "ddl.sql")
    with open(ddl_path, "r", encoding="utf-8") as f:
        ddl_sql = f.read()

    # Use exec_driver_sql for multi-statement DDL
    with engine.begin() as conn:
        conn.exec_driver_sql(ddl_sql)


def execute_sql(path: str, params: Optional[Mapping[str, Any]] = None, engine: Optional[Engine] = None) -> Result:
    """Execute a SQL file with bound parameters using SQLAlchemy text binds.

    - path: absolute or relative path to a .sql file. If relative, resolved relative to this module file.
    - params: mapping passed to SQLAlchemy for :param binds.
    Returns a SQLAlchemy Result.
    """
    engine = engine or get_engine()
    sql_path = path
    if not os.path.isabs(sql_path):
        sql_path = _project_path(sql_path)
    with open(sql_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with engine.begin() as conn:
        return conn.execute(text(sql), params or {})


def _normalize_header(name: str) -> str:
    # Remove BOM and normalize whitespace/case
    cleaned = name.replace("\ufeff", "").strip().lower()
    cleaned = cleaned.replace(" ", "_")
    return cleaned


def _build_sanitized_csv(
    run_id: str,
    source_file: str,
    input_rows: Iterable[Mapping[str, Any]],
    fieldnames: List[str],
) -> StringIO:
    # Map incoming headers to our staging column names
    normalized_to_staging: Dict[str, str] = {
        "id": "id",
        "room_id": "room_id_raw",
        "room_id/id": "room_id_raw",
        "room_id_id": "room_id_raw",
        "noted_date": "noted_date_raw",
        "date": "noted_date_raw",
        "temp": "temp_raw",
        "temperature": "temp_raw",
        "out/in": "location_raw",
        "in/out": "location_raw",
        "location": "location_raw",
    }

    normalized_headers = [_normalize_header(h) for h in fieldnames]
    header_map: Dict[str, str] = {}
    for raw, norm in zip(fieldnames, normalized_headers):
        if norm in normalized_to_staging:
            header_map[raw] = normalized_to_staging[norm]

    # Ensure required headers are present in some form
    required_staging = {"id", "room_id_raw", "noted_date_raw", "temp_raw", "location_raw"}
    mapped_staging = set(header_map.values())
    missing = sorted(required_staging - mapped_staging)
    if missing:
        missing_list = ", ".join(missing)
        raise ValueError(
            f"CSV format error: missing required headers mapped to [{missing_list}]"
        )

    # Final CSV should strictly match staging order
    output_headers = [
        "run_id",
        "source_file",
        "id",
        "room_id_raw",
        "noted_date_raw",
        "temp_raw",
        "location_raw",
    ]

    sio = StringIO()
    writer = csv.writer(sio, lineterminator="\n")
    writer.writerow(output_headers)

    for row in input_rows:
        # Extract using the first occurrence mapped to each staging column
        id_val = _first_value(row, header_map, "id")
        room_id_val = _first_value(row, header_map, "room_id_raw")
        noted_date_val = _first_value(row, header_map, "noted_date_raw")
        temp_val = _first_value(row, header_map, "temp_raw")
        location_val = _first_value(row, header_map, "location_raw")

        writer.writerow([
            str(run_id),
            str(source_file),
            _sanitize_cell(id_val),
            _sanitize_cell(room_id_val),
            _sanitize_cell(noted_date_val),
            _sanitize_cell(temp_val),
            _sanitize_cell(location_val),
        ])

    sio.seek(0)
    return sio


def _first_value(row: Mapping[str, Any], header_map: Mapping[str, str], staging_key: str) -> str:
    for source_header, mapped in header_map.items():
        if mapped == staging_key and source_header in row:
            return row.get(source_header)  # type: ignore[return-value]
    return ""


def _sanitize_cell(value: Any) -> str:
    if value is None:
        return ""
    text_value = str(value)
    # Remove NULs and normalize whitespace
    text_value = text_value.replace("\x00", "").strip()
    return text_value


def copy_csv_to_staging(run_id: str, source_file: str, csv_path: str, engine: Optional[Engine] = None) -> int:
    """Copy a CSV file into staging using psycopg2 COPY with a sanitized in-memory CSV.

    The input CSV is expected to contain headers: id, room_id/id, noted_date, temp, out/in.
    They are mapped to staging columns: id, room_id_raw, noted_date_raw, temp_raw, location_raw.
    We generate an in-memory CSV with header strictly matching staging order and include
    run_id and source_file as constant columns. Uses CSV HEADER mode.

    Returns the number of data rows copied.
    """
    engine = engine or get_engine()

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError("Input CSV is missing a header row")
        # Strictly validate headers before proceeding
        dq.validate_headers(reader.fieldnames)
        sanitized_csv = _build_sanitized_csv(run_id, source_file, reader, reader.fieldnames)

    copy_sql = (
        "COPY staging_temperature_raw (run_id, source_file, id, room_id_raw, noted_date_raw, temp_raw, location_raw) "
        "FROM STDIN WITH (FORMAT CSV, HEADER TRUE)"
    )

    raw_conn = engine.raw_connection()
    try:
        with raw_conn.cursor() as cur:
            cur.copy_expert(copy_sql, sanitized_csv)
        raw_conn.commit()
    finally:
        raw_conn.close()

    # Row count equals total lines minus header
    total_lines = sanitized_csv.getvalue().count("\n")
    return max(total_lines - 1, 0)


def run_transform(run_id: str, source_file: str, engine: Optional[Engine] = None) -> Dict[str, int]:
    """Execute transform.sql and return counts as a dict with rows_valid and rows_rejected."""
    engine = engine or get_engine()
    transform_path = _project_path("sql", "transform.sql")
    with open(transform_path, "r", encoding="utf-8") as f:
        sql = f.read()
    with engine.begin() as conn:
        res = conn.execute(text(sql), {"run_id": run_id, "source_file": source_file})
        row = res.mappings().first()
        if row is None:
            return {"rows_valid": 0, "rows_rejected": 0}
        return {
            "rows_valid": int(row.get("rows_valid", 0)),
            "rows_rejected": int(row.get("rows_rejected", 0)),
        }


