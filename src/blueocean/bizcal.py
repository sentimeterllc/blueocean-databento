"""Business-calendar lookups.

The trading calendar is authoritative reference data held in Postgres (``bizcal``),
not something the pipeline derives from weekday arithmetic: half-days, exchange
holidays and ad-hoc closures all have to come from one place or the lake silently
grows empty partitions.
"""

from __future__ import annotations

from datetime import date

import psycopg2


def prior_bizdate(conn_str: str) -> date | None:
    """Most recent business date strictly before today, or ``None`` if unavailable."""
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(bizdate)::date FROM bizcal WHERE bizdate < current_date")
            row = cur.fetchone()
            return row[0] if row and row[0] is not None else None


def is_bizdate(conn_str: str, day: date) -> bool:
    """True when ``day`` is itself a trading day."""
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT bizdate FROM bizcal WHERE todaysdate = %s", (day,))
            row = cur.fetchone()
            return bool(row and row[0] == day)


def recent_bizdates(conn_str: str, limit: int) -> list[date]:
    """The ``limit`` most recent business dates before today, newest first."""
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT bizdate::date FROM bizcal WHERE bizdate < current_date "
                "ORDER BY bizdate DESC LIMIT %s",
                (limit,),
            )
            return [r[0] for r in cur.fetchall()]


def bizdates_between(conn_str: str, start: str, end: str) -> list[date]:
    """Business dates in ``[start, end]`` (ISO strings), newest first."""
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT bizdate::date FROM bizcal WHERE bizdate >= %s AND bizdate <= %s "
                "ORDER BY bizdate DESC",
                (start, end),
            )
            return [r[0] for r in cur.fetchall()]
