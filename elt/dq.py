from typing import Iterable, List


class HeaderValidationError(ValueError):
    """Raised when CSV headers don't match the expected Kaggle schema."""


EXPECTED_HEADERS: List[str] = [
    "id",
    "room_id/id",
    "noted_date",
    "temp",
    "out/in",
]


def _sanitize_header(name: str) -> str:
    """Remove BOM and surrounding whitespace without changing case/content."""
    return name.replace("\ufeff", "").strip()


def validate_headers(header_list: Iterable[str]) -> None:
    """Validate that headers exactly match the Kaggle schema.

    - Uses a minimal sanitization: strips BOM and surrounding whitespace only.
    - Enforces exact names and order: [id, room_id/id, noted_date, temp, out/in].
    - Raises HeaderValidationError with a clear message if mismatch.
    """
    if header_list is None:
        raise HeaderValidationError("CSV header row is missing")

    headers = [_sanitize_header(h) for h in list(header_list)]
    expected = EXPECTED_HEADERS

    if headers == expected:
        return

    missing = [h for h in expected if h not in headers]
    extra = [h for h in headers if h not in expected]
    order_issue = not missing and not extra and headers != expected

    details: List[str] = [
        f"expected={expected}",
        f"got={headers}",
    ]
    if missing:
        details.append(f"missing={missing}")
    if extra:
        details.append(f"extra={extra}")
    if order_issue:
        details.append("order_mismatch=true")

    raise HeaderValidationError("CSV header mismatch: " + "; ".join(details))


