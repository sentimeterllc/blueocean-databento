"""The updater: Databento batch job -> DBN shards -> merged, sorted Parquet.

One call to :func:`process_date` produces (at most) two files for one overnight
session::

    <lake_root>/date=<YYYY-MM-DD>/ocea_l1.parquet     level 1 trades (tape)
    <lake_root>/date=<YYYY-MM-DD>/ocea_mbo.parquet    level 2 market-by-order (book)

Design notes that matter in production:

* **Everything received is kept.** The batch job is submitted for the whole
  venue (``ALL_SYMBOLS``) and every row that comes back is written to the lake.
  Nothing is dropped on the way in. That is deliberate: the raw shards are
  deleted once converted, so a row discarded here is a row that can only be
  recovered by re-fetching — and re-paying for — the session. Narrowing to a
  symbol list is a *read-time* decision the lake leaves open; see
  :func:`merge_and_sort` for the one-line change that would move it upstream.
* **Idempotent.** A date whose Parquet already exists is skipped, so the whole
  pipeline is safe to re-run, retry or schedule twice.
* **Job reuse, not resubmission.** Before submitting, the updater scans recent
  batch jobs for an equivalent request that is queued/processing/done and
  attaches to it. A crashed run therefore costs nothing on restart.
* **Graceful no-op.** Holidays and not-yet-published sessions come back as HTTP
  422; that is a normal outcome, logged and returned, not an exception. Callers
  that need a hard success signal should test for the artefact on disk rather
  than trusting the exit code.
* **Windows file locks.** Databento's reader holds the DBN file open through a
  native handle; the store is dropped and a GC forced before the scratch
  directory is removed, otherwise the cleanup fails on Windows.
"""

from __future__ import annotations

import gc
import logging
import shutil
import time
from collections.abc import Iterable, Sequence
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import databento as db
import duckdb
import requests

from . import config

log = logging.getLogger("blueocean.updater")

L1_FILENAME = "ocea_l1.parquet"
MBO_FILENAME = "ocea_mbo.parquet"

#: Databento schema -> output filename for the two feeds this pipeline lands.
SCHEMAS = (("trades", L1_FILENAME), ("mbo", MBO_FILENAME))

#: Jobs older than this are irrelevant when looking for a reusable submission.
_JOB_LOOKBACK_DAYS = 30


# --------------------------------------------------------------------------- #
# Databento response helpers — the client returns dicts or objects by version
# --------------------------------------------------------------------------- #
def _field(obj, name: str, default=None):
    if isinstance(obj, dict):
        return obj.get(name, default)
    return getattr(obj, name, default)


# --------------------------------------------------------------------------- #
# Batch jobs
# --------------------------------------------------------------------------- #
def _recent_jobs(client) -> Sequence:
    since = (datetime.now(timezone.utc) - timedelta(days=_JOB_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    return client.batch.list_jobs(since=since)


def submit_or_find_job(client, target_date_str: str, end_str: str, schema: str) -> str:
    """Return the id of a usable batch job for this (dataset, schema, date).

    Reuses an existing queued/processing/done job when one matches, so a rerun
    after a crash does not pay for the same data twice.
    """
    dataset = config.dataset()
    for job in _recent_jobs(client):
        if (
            _field(job, "dataset") == dataset
            and _field(job, "schema") == schema
            and target_date_str in str(_field(job, "start", ""))
            and _field(job, "state") in ("queued", "processing", "done")
        ):
            job_id = _field(job, "id")
            log.info("[%s] reusing existing %s job %s", target_date_str, schema, job_id)
            return job_id

    job = client.batch.submit_job(
        dataset=dataset,
        schema=schema,
        start=target_date_str,
        end=end_str,
        symbols="ALL_SYMBOLS",
        encoding="dbn",
        split_duration="day",
    )
    job_id = _field(job, "id")
    log.info("[%s] submitted new %s job %s", target_date_str, schema, job_id)
    time.sleep(1)  # be polite to the batch API between submissions
    return job_id


def wait_for_jobs(
    api_key: str,
    job_ids: Iterable[str],
    poll_seconds: int | None = None,
    timeout_seconds: int | None = None,
) -> tuple[set[str], set[str]]:
    """Poll until every job is done, or the timeout elapses.

    Returns ``(completed, still_pending)``. A job the vendor marks ``error`` or
    ``expired`` raises — that is a real failure, unlike a slow queue.
    """
    pending: set[str] = {j for j in job_ids if j}
    completed: set[str] = set()
    if not pending:
        return completed, pending

    poll_seconds = poll_seconds if poll_seconds is not None else config.job_poll_seconds()
    timeout_seconds = timeout_seconds if timeout_seconds is not None else config.job_timeout_seconds()

    client = db.Historical(api_key)
    started = time.time()

    while pending:
        if (time.time() - started) >= timeout_seconds:
            log.warning("timeout after %ss with %d job(s) still pending", timeout_seconds, len(pending))
            break
        try:
            jobs = {_field(j, "id"): j for j in _recent_jobs(client)}
            for job_id in list(pending):
                job = jobs.get(job_id)
                if job is None:
                    continue
                state = _field(job, "state")
                if state == "done":
                    log.info("job %s complete", job_id)
                    pending.discard(job_id)
                    completed.add(job_id)
                elif state in ("error", "expired"):
                    raise RuntimeError(f"job {job_id} failed with state {state!r}")
        except RuntimeError:
            raise
        except Exception as exc:  # transient API/network error — keep polling
            log.warning("error polling batch jobs: %s", exc)
        if pending:
            time.sleep(poll_seconds)

    return completed, pending


# --------------------------------------------------------------------------- #
# Download + convert
# --------------------------------------------------------------------------- #
def _download_shards(client, api_key: str, job_id: str, dest: Path, tag: str) -> list[Path]:
    """Stream every DBN shard of a completed job to ``dest``. Skips what exists."""
    paths: list[Path] = []
    for entry in client.batch.list_files(job_id):
        filename = _field(entry, "filename", "") or ""
        if not (filename.endswith(".dbn.zst") or filename.endswith(".dbn")):
            continue
        urls = _field(entry, "urls", {}) or {}
        url = urls.get("https") if hasattr(urls, "get") else _field(entry, "url")
        if not url:
            continue

        out_path = dest / filename
        if not out_path.exists():
            log.info("[%s] streaming %s", tag, filename)
            with requests.get(url, auth=(api_key, ""), stream=True, timeout=300) as resp:
                resp.raise_for_status()
                with open(out_path, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            fh.write(chunk)
        paths.append(out_path)
    return paths


def _dbn_to_parquet(dbn_path: Path, parquet_path: Path) -> None:
    """Convert one DBN shard to Parquet and release the native file handle."""
    store = db.DBNStore.from_file(str(dbn_path))
    store.to_parquet(str(parquet_path))
    del store
    gc.collect()  # Windows: the Rust reader keeps the .dbn locked until collected


def _sql_literal(path: Path) -> str:
    return str(path).replace("'", "''")


def merge_and_sort(parts: Sequence[Path], out_path: Path, tag: str) -> int:
    """Merge the converted shards into one sorted ``out_path``. Nothing is dropped.

    Every row the vendor returned is written. The sort (``symbol, ts_recv,
    sequence``) is applied once here so that every consumer downstream can rely
    on ordered input and skip its own sort — the single most valuable thing this
    step does for query cost. Merging happens in the same pass, so a job the
    vendor splits across several shards still lands as one ordered file.

    Returns the row count written.

    Narrowing to symbols you care about
    -----------------------------------
    This is the place to do it, and it is a semi-join away. Build a one-column
    table of the symbols you want and restrict the SELECT below::

        # 1. the symbols of interest, from anywhere: a literal list, a file, a
        #    watchlist table, a screen over your own reference database
        symbols = ["AAPL", "TSLA", ...]

        # 2. register them, then semi-join the scan against them
        conn.execute(
            "CREATE OR REPLACE TEMP TABLE universe AS "
            "SELECT unnest(?::VARCHAR[]) AS symbol",
            [list(symbols)],
        )

        COPY (
            SELECT t.*
            FROM read_parquet([...]) t
            SEMI JOIN universe u ON u.symbol = t.symbol   # <-- the filter
            ORDER BY t.symbol, t.ts_recv, t.sequence
        ) TO '<out>' (FORMAT PARQUET)

    A screen of ~2,500 symbols typically keeps ~24% of trade rows and ~7% of
    book rows, so the saving on disk is real. It is off by default anyway,
    because it is not free: the download and the decode are whole-venue
    regardless (the batch job is ``ALL_SYMBOLS``), so filtering here buys
    storage and scan time, never bandwidth or vendor cost — and it is
    irreversible. The raw shards are deleted at the end of the run, so a symbol
    excluded tonight cannot be recovered from this partition later; widening the
    list does not widen history. Keeping everything and filtering at read time
    (``WHERE symbol IN (...)`` against the Parquet, which prunes on the row-group
    statistics anyway) leaves that door open.
    """
    tmp_path = out_path.with_name(out_path.name + ".tmp")
    sources = [f"'{_sql_literal(p)}'" for p in parts]
    src_list = "[" + ", ".join(sources) + "]"

    conn = duckdb.connect()
    try:
        conn.execute(
            f"""
            COPY (
                SELECT t.*
                FROM read_parquet({src_list}) t
                ORDER BY t.symbol, t.ts_recv, t.sequence
            ) TO '{_sql_literal(tmp_path)}' (FORMAT PARQUET)
            """
        )
        rows = conn.execute(
            f"SELECT count(*) FROM read_parquet('{_sql_literal(tmp_path)}')"
        ).fetchone()[0]
    finally:
        conn.close()

    out_path.unlink(missing_ok=True)
    shutil.move(str(tmp_path), str(out_path))
    log.info("[%s] %s merged + sorted: %s rows", tag, out_path.name, f"{rows:,}")
    return rows


# --------------------------------------------------------------------------- #
# One session
# --------------------------------------------------------------------------- #
def process_date(target_date: date, api_key: str, force_mbo: bool = False) -> bool:
    """Fetch and land one overnight session. Returns True when data was written.

    ``target_date`` is the *partition* date: the morning the overnight session
    leads into. The session ``date=2026-09-18`` covers roughly 20:00 ET on
    2026-09-17 through 04:00 ET on 2026-09-18.

    Everything the vendor returns for that session is written. No database is
    consulted here: the only reference lookup left in the pipeline is the
    trading calendar, used by the CLI to decide *which* sessions to ask for.
    """
    tag = target_date.strftime("%Y-%m-%d")
    end_str = f"{tag}T{config.session_end_utc()}Z"
    log.info("[%s] starting Blue Ocean session fetch (dataset=%s)", tag, config.dataset())

    partition = config.lake_root() / f"date={tag}"
    partition.mkdir(parents=True, exist_ok=True)
    l1_path = partition / L1_FILENAME
    mbo_path = partition / MBO_FILENAME

    # Level 2 ages out of the subscription; level 1 does not.
    skip_mbo = (date.today() - target_date).days > config.mbo_retention_days() and not force_mbo
    if skip_mbo:
        if l1_path.exists():
            log.info("[%s] level 1 already present (level 2 out of retention) — nothing to do", tag)
            return False
        log.warning(
            "[%s] older than %d days: level 2 is no longer fetchable, taking level 1 only",
            tag,
            config.mbo_retention_days(),
        )
    elif l1_path.exists() and mbo_path.exists():
        log.info("[%s] level 1 and level 2 already present — nothing to do", tag)
        return False

    client = db.Historical(api_key)

    # 1. Submit (or attach to) one batch job per missing feed.
    wanted: list[tuple[str, Path]] = []
    if not l1_path.exists():
        wanted.append(("trades", l1_path))
    if not skip_mbo and not mbo_path.exists():
        wanted.append(("mbo", mbo_path))

    jobs: list[tuple[str, str, Path]] = []
    try:
        for schema, out_path in wanted:
            jobs.append((schema, submit_or_find_job(client, tag, end_str, schema), out_path))
    except Exception as exc:
        # 422 = the vendor has no data for this window: holiday, or not published yet.
        if getattr(exc, "http_status", None) == 422 or "422" in str(exc):
            log.warning(
                "[%s] dataset unavailable for this session (holiday or not yet published): %s",
                tag, exc,
            )
            return False
        raise

    if not jobs:
        return False

    # 2. Wait for the vendor to build them.
    completed, pending = wait_for_jobs(api_key, [job_id for _, job_id, _ in jobs])
    if pending or not completed:
        log.error("[%s] jobs did not complete in time: %s", tag, sorted(pending))
        return False

    # 3. Download, convert, merge. Every row that arrives is kept.
    scratch = config.download_dir() / tag
    scratch.mkdir(parents=True, exist_ok=True)
    wrote_any = False

    for schema, job_id, out_path in jobs:
        log.info("[%s] downloading %s shards for job %s", tag, schema, job_id)
        try:
            shards = _download_shards(client, api_key, job_id, scratch, tag)
            if not shards:
                log.warning("[%s] job %s produced no %s shards", tag, job_id, schema)
                continue
            parts: list[Path] = []
            for index, shard in enumerate(shards):
                part = scratch / f"{out_path.stem}.part{index}.parquet"
                log.info("[%s] converting %s -> %s", tag, shard.name, part.name)
                _dbn_to_parquet(shard, part)
                parts.append(part)
            merge_and_sort(parts, out_path, tag)
            wrote_any = True
        except Exception as exc:
            log.error("[%s] failed to produce %s: %s", tag, out_path.name, exc)

    # 4. Reclaim the scratch space.
    gc.collect()
    time.sleep(1)  # let Windows release the last native handle
    try:
        shutil.rmtree(scratch)
    except OSError as exc:
        log.warning("[%s] could not fully remove %s: %s", tag, scratch, exc)

    log.info("[%s] session complete (data written: %s)", tag, wrote_any)
    return wrote_any
