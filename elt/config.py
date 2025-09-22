import os
from typing import Optional
from zoneinfo import ZoneInfo

try:
    # Prefer pydantic-settings (Pydantic v2 way)
    from pydantic_settings import BaseSettings, SettingsConfigDict  # type: ignore
    _USE_SETTINGS = True
except Exception:  # pragma: no cover - optional dependency fallback
    # Fallback to a light shim using pydantic.BaseModel + python-dotenv
    from pydantic import BaseModel  # type: ignore
    from dotenv import load_dotenv  # type: ignore

    _USE_SETTINGS = False

    class BaseSettings(BaseModel):  # type: ignore
        """Minimal shim to populate fields from process env after loading .env.

        This is not a full replacement for pydantic-settings, but is sufficient
        for this project's simple use-case.
        """

        def __init__(self, **data):  # type: ignore[override]
            # Load .env into process environment, then populate from os.environ
            load_dotenv()

            def _get(name: str) -> Optional[str]:
                return os.getenv(name)

            def _get_int(name: str) -> Optional[int]:
                value = os.getenv(name)
                if value is None:
                    return None
                try:
                    return int(value)
                except Exception:
                    return None

            env_data = {
                "DATABASE_URL": _get("DATABASE_URL"),
                "PGHOST": _get("PGHOST"),
                "PGPORT": _get_int("PGPORT"),
                "PGDATABASE": _get("PGDATABASE"),
                "PGUSER": _get("PGUSER"),
                "PGPASSWORD": _get("PGPASSWORD"),
                "POSTGRES_HOST": _get("POSTGRES_HOST"),
                "POSTGRES_PORT": _get_int("POSTGRES_PORT"),
                "POSTGRES_DB": _get("POSTGRES_DB"),
                "POSTGRES_USER": _get("POSTGRES_USER"),
                "POSTGRES_PASSWORD": _get("POSTGRES_PASSWORD"),
                "DB_HOST": _get("DB_HOST"),
                "DB_PORT": _get_int("DB_PORT"),
                "DB_NAME": _get("DB_NAME"),
                "DB_USER": _get("DB_USER"),
                "DB_PASSWORD": _get("DB_PASSWORD"),
                "TIMEZONE": _get("TIMEZONE"),
                "TZ": _get("TZ"),
            }

            env_data.update(data)
            super().__init__(**env_data)


class Settings(BaseSettings):
    """Application configuration loaded from environment and .env file.

    Fields mirror the environment variable keys used throughout the project.
    """

    if _USE_SETTINGS:
        # Only used when pydantic-settings is available
        model_config = SettingsConfigDict(  # type: ignore[name-defined]
            env_file=".env",
            case_sensitive=True,
            extra="ignore",
        )

    # Primary consolidated URL
    DATABASE_URL: Optional[str] = None

    # libpq-style envs
    PGHOST: Optional[str] = None
    PGPORT: Optional[int] = None
    PGDATABASE: Optional[str] = None
    PGUSER: Optional[str] = None
    PGPASSWORD: Optional[str] = None

    # POSTGRES_* fallbacks
    POSTGRES_HOST: Optional[str] = None
    POSTGRES_PORT: Optional[int] = None
    POSTGRES_DB: Optional[str] = None
    POSTGRES_USER: Optional[str] = None
    POSTGRES_PASSWORD: Optional[str] = None

    # DB_* fallbacks
    DB_HOST: Optional[str] = None
    DB_PORT: Optional[int] = None
    DB_NAME: Optional[str] = None
    DB_USER: Optional[str] = None
    DB_PASSWORD: Optional[str] = None

    # Optional time zone names
    TIMEZONE: Optional[str] = None
    TZ: Optional[str] = None

    def as_sqlalchemy_url(self) -> str:
        """Return a SQLAlchemy URL derived from available settings.

clea        Precedence matches `elt.db.get_engine`:
        1) DATABASE_URL
        2) libpq-style envs (PG*)
        3) POSTGRES_*/DB_* fallbacks
        """

        if self.DATABASE_URL:
            return self.DATABASE_URL

        host = self.PGHOST or self.POSTGRES_HOST or self.DB_HOST or "localhost"
        port = (
            self.PGPORT
            or self.POSTGRES_PORT
            or self.DB_PORT
            or 5432
        )
        dbname = (
            self.PGDATABASE
            or self.POSTGRES_DB
            or self.DB_NAME
            or "postgres"
        )
        user = self.PGUSER or self.POSTGRES_USER or self.DB_USER or "postgres"
        password = (
            self.PGPASSWORD or self.POSTGRES_PASSWORD or self.DB_PASSWORD or ""
        )

        safe_password = (
            password.replace("@", "%40") if isinstance(password, str) and "@" in password else password
        )
        return f"postgresql+psycopg2://{user}:{safe_password}@{host}:{port}/{dbname}"

    @property
    def timezone(self) -> ZoneInfo:
        """Return the configured IANA time zone, defaulting to UTC.

        Respects `TZ` or `TIMEZONE` if present in .env or environment.
        """

        name = self.TZ or self.TIMEZONE or "UTC"
        try:
            return ZoneInfo(str(name))
        except Exception:
            return ZoneInfo("UTC")


# Convenient singleton instance
settings = Settings()


