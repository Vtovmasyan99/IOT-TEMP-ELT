import csv
import hashlib
import io
import os
import shutil
import time
from typing import Generator, Iterable, List


class IOErrorWithContext(Exception):
    """Raised when file IO operations fail with additional context."""


def discover_csvs(directory: str) -> Generator[str, None, None]:
    """Yield absolute paths to .csv files under the given directory.

    - Traverses only the top-level of the directory (not recursive) to keep
      behavior explicit and predictable for landing-zone style folders.
    - Skips hidden files and non-regular files.
    """
    if not directory:
        raise ValueError("directory must be a non-empty string")
    if not os.path.isdir(directory):
        raise IOErrorWithContext(f"Not a directory: {directory}")

    try:
        for entry in os.scandir(directory):
            if not entry.is_file():
                continue
            if entry.name.startswith("."):
                continue
            if entry.name.lower().endswith(".csv"):
                yield os.path.abspath(entry.path)
    except Exception as exc:
        raise IOErrorWithContext(f"Failed to list CSVs in {directory}: {exc}") from exc


def sha256_of_file(path: str, chunk_size: int = 1024 * 1024) -> str:
    """Return the SHA-256 hex digest of the file at path.

    Reads the file in chunks to avoid high memory usage for large files.
    """
    if not path:
        raise ValueError("path must be a non-empty string")
    if not os.path.isfile(path):
        raise IOErrorWithContext(f"Not a file: {path}")

    hasher = hashlib.sha256()
    try:
        with open(path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                hasher.update(chunk)
    except Exception as exc:
        raise IOErrorWithContext(f"Failed to compute sha256 for {path}: {exc}") from exc
    return hasher.hexdigest()


def _unique_destination_path(dst_dir: str, filename: str) -> str:
    base, ext = os.path.splitext(filename)
    candidate = os.path.join(dst_dir, filename)
    if not os.path.exists(candidate):
        return candidate

    # Append timestamp-based suffix to ensure uniqueness
    ts = int(time.time())
    counter = 1
    while True:
        candidate = os.path.join(dst_dir, f"{base}.{ts}.{counter}{ext}")
        if not os.path.exists(candidate):
            return candidate
        counter += 1


def safe_move(src: str, dst_dir: str) -> str:
    """Move src file into dst_dir, creating it if necessary.

    - If a file with the same name exists, appends a unique suffix.
    - Returns the absolute path to the moved file.
    """
    if not src or not dst_dir:
        raise ValueError("src and dst_dir must be non-empty strings")
    if not os.path.isfile(src):
        raise IOErrorWithContext(f"Source is not a file: {src}")

    try:
        os.makedirs(dst_dir, exist_ok=True)
        filename = os.path.basename(src)
        dst_path = _unique_destination_path(dst_dir, filename)
        shutil.move(src, dst_path)
        return os.path.abspath(dst_path)
    except Exception as exc:
        raise IOErrorWithContext(
            f"Failed to move {src} to {dst_dir}: {exc}"
        ) from exc


def split_csv(input_path: str, chunk_size: int, out_dir: str) -> List[str]:
    """Split a CSV file into numbered chunk files in out_dir.

    - Preserves the header row in each chunk.
    - chunk_size is the number of data rows per chunk (must be > 0).
    - Returns a list of created chunk file paths in order.
    """
    if not input_path:
        raise ValueError("input_path must be provided")
    if chunk_size <= 0:
        raise ValueError("chunk_size must be > 0")
    if not os.path.isfile(input_path):
        raise IOErrorWithContext(f"Not a file: {input_path}")

    try:
        os.makedirs(out_dir, exist_ok=True)
    except Exception as exc:
        raise IOErrorWithContext(f"Failed to create out_dir {out_dir}: {exc}") from exc

    created: List[str] = []
    try:
        with open(input_path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f)
            try:
                header = next(reader)
            except StopIteration:
                raise IOErrorWithContext("Input CSV has no rows (missing header)")

            chunk_index = 1
            data_rows_in_chunk = 0
            writer: csv.writer
            out_file: io.TextIOBase
            out_path = os.path.join(out_dir, f"chunk_{chunk_index:04d}.csv")
            out_file = open(out_path, "w", encoding="utf-8", newline="")
            created.append(os.path.abspath(out_path))
            writer = csv.writer(out_file, lineterminator="\n")
            writer.writerow(header)

            for row in reader:
                writer.writerow(row)
                data_rows_in_chunk += 1
                if data_rows_in_chunk >= chunk_size:
                    out_file.close()
                    chunk_index += 1
                    data_rows_in_chunk = 0
                    out_path = os.path.join(out_dir, f"chunk_{chunk_index:04d}.csv")
                    out_file = open(out_path, "w", encoding="utf-8", newline="")
                    created.append(os.path.abspath(out_path))
                    writer = csv.writer(out_file, lineterminator="\n")
                    writer.writerow(header)

            out_file.close()
    except Exception as exc:
        # Attempt cleanup of partially created files if there was a failure
        for p in created:
            try:
                if os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass
        raise IOErrorWithContext(f"Failed to split CSV {input_path}: {exc}") from exc

    return created


def _normalize_header(name: str) -> str:
    cleaned = name.replace("\ufeff", "").strip().lower()
    cleaned = cleaned.replace(" ", "_")
    return cleaned


def read_csv_and_normalize_headers(path: str) -> io.StringIO:
    """Read a CSV and return a StringIO with headers remapped to staging order.

    The output header order must be exactly what copy_csv_to_staging expects:
    [run_id, source_file, id, room_id_raw, noted_date_raw, temp_raw, location_raw]

    This function only remaps headers and preserves row data as-is. The caller
    is responsible for prepending run_id and source_file when using COPY, but we
    include them as empty placeholders to match the expected columns order.
    """
    if not os.path.isfile(path):
        raise IOErrorWithContext(f"Not a file: {path}")

    normalized_to_staging = {
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

    output_headers = [
        "run_id",
        "source_file",
        "id",
        "room_id_raw",
        "noted_date_raw",
        "temp_raw",
        "location_raw",
    ]

    try:
        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                raise IOErrorWithContext("CSV is missing a header row")

            normalized_headers = [_normalize_header(h) for h in reader.fieldnames]
            header_map = {}
            for raw, norm in zip(reader.fieldnames, normalized_headers):
                if norm in normalized_to_staging:
                    header_map[raw] = normalized_to_staging[norm]

            out = io.StringIO()
            writer = csv.writer(out, lineterminator="\n")
            writer.writerow(output_headers)

            for row in reader:
                # Extract mapped values
                id_val = _first_value(row, header_map, "id")
                room_id_val = _first_value(row, header_map, "room_id_raw")
                noted_date_val = _first_value(row, header_map, "noted_date_raw")
                temp_val = _first_value(row, header_map, "temp_raw")
                location_val = _first_value(row, header_map, "location_raw")

                writer.writerow([
                    "",  # run_id placeholder (filled by db.copy_csv_to_staging)
                    "",  # source_file placeholder
                    _sanitize_cell(id_val),
                    _sanitize_cell(room_id_val),
                    _sanitize_cell(noted_date_val),
                    _sanitize_cell(temp_val),
                    _sanitize_cell(location_val),
                ])

            out.seek(0)
            return out
    except Exception as exc:
        raise IOErrorWithContext(f"Failed to read and normalize {path}: {exc}") from exc


def _first_value(row: dict, header_map: dict, staging_key: str) -> str:
    for source_header, mapped in header_map.items():
        if mapped == staging_key and source_header in row:
            return row.get(source_header, "")
    return ""


def _sanitize_cell(value: object) -> str:
    if value is None:
        return ""
    text_value = str(value)
    text_value = text_value.replace("\x00", "").strip()
    return text_value


