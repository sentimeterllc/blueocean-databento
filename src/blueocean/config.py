"""Configuration, loaded from the environment (optionally via a .env file).

Nothing in this package hard-codes a credential or a machine-specific path: every
value below is an environment variable with a documented default. See
``.env.example`` for the full list.
"""

from __future__ import annotations

import os
from pathlib import Path

# --------------------------------------------------------------------------- #
# .env loading
# --------------------------------------------------------------------------- #
# Look for a .env next to the repository root (two levels above this file:
# src/blueocean/config.py -> src -> <repo root>), unless BLUEOCEAN_ENV points
# somewhere else. Absent .env is fine — real deployments export the variables.
_REPO_ROOT = Path(__file__).resolve().parents[2]


def _load_dotenv() -> None:
    try:
        from dotenv import load_dotenv
    except ImportError:  # python-dotenv is optional at runtime
        return
    env_file = Path(os.getenv("BLUEOCEAN_ENV", _REPO_ROOT / ".env"))
    if env_file.is_file():
        load_dotenv(env_file)


_load_dotenv()


def _require(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(
            f"{name} is not set. Copy .env.example to .env and fill it in, "
            f"or export {name} in the environment."
        )
    return value


# --------------------------------------------------------------------------- #
# Databento
# --------------------------------------------------------------------------- #
def databento_api_key() -> str:
    """Databento API key (`db-...`). Required for every fetch."""
    return _require("DATABENTO_API_KEY")


def dataset() -> str:
    """Databento dataset code. Blue Ocean ATS publishes as ``OCEA.MEMOIR``."""
    return os.getenv("BLUEOCEAN_DATASET", "OCEA.MEMOIR")


def job_poll_seconds() -> int:
    """Seconds between batch-job status polls."""
    return int(os.getenv("BLUEOCEAN_JOB_POLL_SECONDS", "5"))


def job_timeout_seconds() -> int:
    """Give up waiting for a batch job after this many seconds (default 2 h)."""
    return int(os.getenv("BLUEOCEAN_JOB_TIMEOUT_SECONDS", "7200"))


def mbo_retention_days() -> int:
    """How far back MBO (level 2) stays fetchable on the subscription.

    Beyond this the vendor no longer serves the book for that date, so the
    updater silently degrades to a trades-only (level 1) fetch unless the caller
    passes ``--force-mbo``.
    """
    return int(os.getenv("BLUEOCEAN_MBO_RETENTION_DAYS", "27"))


def session_end_utc() -> str:
    """UTC clock time at which the overnight session ends, as ``HH:MM:SS``.

    The Blue Ocean session runs ~20:00 ET (previous day) to 04:00 ET, i.e. it
    finishes at 08:00 UTC. The batch request is bounded at 09:00 UTC by default:
    far enough past the close to capture the whole session, early enough to avoid
    asking for a slice of the day the vendor has not published yet (HTTP 422).
    """
    return os.getenv("BLUEOCEAN_SESSION_END_UTC", "09:00:00")


# --------------------------------------------------------------------------- #
# Paths
# --------------------------------------------------------------------------- #
def lake_root() -> Path:
    """Root of the Hive-partitioned Parquet lake this pipeline writes."""
    return Path(os.getenv("BLUEOCEAN_ROOT", "C:/data/blueocean"))


def download_dir() -> Path:
    """Scratch directory for raw ``.dbn.zst`` shards (deleted after conversion)."""
    return Path(os.getenv("BLUEOCEAN_DOWNLOAD_DIR", "C:/data/blueocean_downloads"))


def log_dir() -> Path:
    """Directory for the rolling daily log file."""
    return Path(os.getenv("BLUEOCEAN_LOG_DIR", "C:/logs/blueocean"))


# --------------------------------------------------------------------------- #
# Postgres (one reference lookup: the trading calendar)
# --------------------------------------------------------------------------- #
# The pipeline stores every row the vendor returns, so there is no symbol
# universe to resolve and no screen to read. The only thing it still asks the
# database is which dates are trading days — and that is used to choose which
# sessions to request, never to filter what comes back.
def main_conn() -> str:
    """libpq connection string for the database holding ``bizcal``."""
    conn = os.getenv("DB_TRADINGDATA_CONN")
    if not conn:
        raise RuntimeError("DB_TRADINGDATA_CONN is not set.")
    return conn
