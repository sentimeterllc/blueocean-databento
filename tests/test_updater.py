"""Tests for the parts of the updater that do not need the vendor API.

The merge/sort step is the one that shapes what lands in the lake, so it is
exercised against real Parquet files built in a temporary directory. The first
test is the load-bearing one: nothing the vendor sends may be dropped.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import duckdb
import pytest

from blueocean.updater import _field, merge_and_sort


def _write_parquet(path, rows):
    """Write ``rows`` (symbol, ts_recv, sequence, price) as a Parquet file."""
    conn = duckdb.connect()
    conn.execute("CREATE TABLE t (symbol VARCHAR, ts_recv TIMESTAMPTZ, sequence UBIGINT, price DOUBLE)")
    conn.executemany("INSERT INTO t VALUES (?, ?, ?, ?)", rows)
    conn.execute(f"COPY t TO '{str(path).replace(chr(39), chr(39) * 2)}' (FORMAT PARQUET)")
    conn.close()


@pytest.fixture
def base_ts():
    return datetime(2026, 9, 17, 20, 0, tzinfo=timezone.utc)


def test_every_row_received_is_kept(tmp_path, base_ts):
    """No row is dropped on the way in — the lake is the whole venue."""
    src = tmp_path / "raw.parquet"
    _write_parquet(
        src,
        [
            ("AAAA", base_ts, 2, 1.10),
            ("ZZZZ", base_ts, 1, 2.20),          # illiquid, unscreened, kept anyway
            ("AAAA", base_ts + timedelta(seconds=1), 3, 1.15),
            ("QQQQ", base_ts + timedelta(seconds=2), 4, 0.02),  # sub-penny, kept
        ],
    )
    out = tmp_path / "ocea_l1.parquet"

    rows = merge_and_sort([src], out, "test")

    assert rows == 4
    symbols = duckdb.connect().execute(
        f"SELECT DISTINCT symbol FROM read_parquet('{out.as_posix()}') ORDER BY 1"
    ).fetchall()
    assert symbols == [("AAAA",), ("QQQQ",), ("ZZZZ",)]


def test_output_is_sorted_by_symbol_then_time_then_sequence(tmp_path, base_ts):
    src = tmp_path / "raw.parquet"
    _write_parquet(
        src,
        [
            ("BBBB", base_ts + timedelta(seconds=5), 9, 3.0),
            ("AAAA", base_ts + timedelta(seconds=2), 7, 1.0),
            ("AAAA", base_ts + timedelta(seconds=2), 6, 1.0),  # same ts, earlier sequence
            ("AAAA", base_ts, 1, 1.0),
        ],
    )
    out = tmp_path / "ocea_l1.parquet"
    merge_and_sort([src], out, "test")

    rows = duckdb.connect().execute(
        f"SELECT symbol, sequence FROM read_parquet('{out.as_posix()}')"
    ).fetchall()
    assert rows == [("AAAA", 1), ("AAAA", 6), ("AAAA", 7), ("BBBB", 9)]


def test_multiple_shards_merge_into_one_ordered_file(tmp_path, base_ts):
    """A job the vendor splits must still land as a single ordered partition."""
    first, second = tmp_path / "p0.parquet", tmp_path / "p1.parquet"
    _write_parquet(first, [("AAAA", base_ts + timedelta(seconds=3), 4, 1.0)])
    _write_parquet(second, [("AAAA", base_ts, 1, 1.0), ("CCCC", base_ts, 2, 1.0)])
    out = tmp_path / "ocea_l1.parquet"

    rows_written = merge_and_sort([first, second], out, "test")

    assert rows_written == 3
    rows = duckdb.connect().execute(
        f"SELECT symbol, sequence FROM read_parquet('{out.as_posix()}')"
    ).fetchall()
    assert rows == [("AAAA", 1), ("AAAA", 4), ("CCCC", 2)]


def test_existing_output_is_replaced_not_appended(tmp_path, base_ts):
    src = tmp_path / "raw.parquet"
    _write_parquet(src, [("AAAA", base_ts, 1, 1.0)])
    out = tmp_path / "ocea_l1.parquet"
    _write_parquet(out, [("AAAA", base_ts, 99, 9.0)])  # a stale partial from a previous run

    merge_and_sort([src], out, "test")

    rows = duckdb.connect().execute(
        f"SELECT sequence FROM read_parquet('{out.as_posix()}')"
    ).fetchall()
    assert rows == [(1,)]


@pytest.mark.parametrize(
    "job, expected",
    [
        ({"id": "abc", "state": "done"}, "abc"),
        (type("Job", (), {"id": "xyz", "state": "queued"})(), "xyz"),
    ],
)
def test_field_reads_dict_and_object_responses(job, expected):
    """The vendor client returns dicts on some versions and objects on others."""
    assert _field(job, "id") == expected
    assert _field(job, "missing", "fallback") == "fallback"
