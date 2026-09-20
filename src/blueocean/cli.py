"""Command-line entry point: ``python -m blueocean`` / ``blueocean-update``."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date, datetime

from . import bizcal, config
from .updater import process_date


def _setup_logging() -> None:
    root = logging.getLogger("blueocean")
    if root.handlers:
        return
    root.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    stream = logging.StreamHandler(sys.stdout)
    stream.setFormatter(fmt)
    root.addHandler(stream)

    log_dir = config.log_dir()
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(
            log_dir / f"blueocean_{datetime.now():%Y-%m-%d}.log", encoding="utf-8"
        )
        handler.setFormatter(fmt)
        root.addHandler(handler)
    except OSError as exc:  # console-only is an acceptable degradation
        root.warning("file logging disabled (%s): %s", log_dir, exc)


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="blueocean-update",
        description="Fetch Blue Ocean overnight level 1 + level 2 data into the Parquet lake.",
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--date", help="single session partition date, YYYY-MM-DD")
    group.add_argument("--backfill", type=int, metavar="N", help="the N most recent business days")
    parser.add_argument("--start", help="backfill range start, YYYY-MM-DD (with --end)")
    parser.add_argument("--end", help="backfill range end, YYYY-MM-DD (with --start)")
    parser.add_argument(
        "--force-mbo",
        action="store_true",
        help="request level 2 even for sessions past the vendor retention window",
    )
    args = parser.parse_args(argv)
    if bool(args.start) != bool(args.end):
        parser.error("--start and --end must be given together")
    return args


def _target_dates(args: argparse.Namespace, main_conn: str) -> list[date]:
    if args.date:
        return [datetime.strptime(args.date, "%Y-%m-%d").date()]
    if args.backfill:
        return bizcal.recent_bizdates(main_conn, args.backfill)
    if args.start and args.end:
        return bizcal.bizdates_between(main_conn, args.start, args.end)

    # Default: today's partition, i.e. last night's session — but only on a
    # trading day, so a weekend or holiday run is a clean no-op. This is the
    # only thing the pipeline asks the database: which dates to request. What
    # comes back for those dates is never filtered.
    today = date.today()
    try:
        if not bizcal.is_bizdate(main_conn, today):
            logging.getLogger("blueocean").info(
                "[%s] not a trading day — nothing to fetch", today.isoformat()
            )
            return []
    except Exception as exc:
        logging.getLogger("blueocean").warning(
            "business-calendar check failed (%s); proceeding with %s", exc, today
        )
    return [today]


def main(argv: list[str] | None = None) -> int:
    _setup_logging()
    log = logging.getLogger("blueocean")
    args = _parse_args(sys.argv[1:] if argv is None else argv)

    api_key = config.databento_api_key()
    main_conn = config.main_conn()

    dates = _target_dates(args, main_conn)
    if not dates:
        return 0

    log.info("processing %d session(s): %s", len(dates), ", ".join(d.isoformat() for d in dates))
    failures = 0
    for target in dates:
        try:
            process_date(target, api_key, force_mbo=args.force_mbo)
        except Exception as exc:
            failures += 1
            log.exception("[%s] unhandled failure: %s", target.isoformat(), exc)
    return 1 if failures else 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
