"""Command line interface for SEC insider ingestion and queries."""
from __future__ import annotations

import argparse
import json
from datetime import date, timedelta, datetime

from .models import init_db
from .ingestion import ingest_form4_filings
from .queries import get_top_trades, get_sector_activity, get_person_activity


def _parse_date(text: str) -> date:
    return datetime.strptime(text, "%Y-%m-%d").date()


def _resolve_date_range(args) -> tuple[date, date]:
    if args.days is not None:
        end = date.today()
        start = end - timedelta(days=int(args.days))
    else:
        if not args.start or not args.end:
            raise SystemExit("Provide --days or both --start and --end")
        start = _parse_date(args.start)
        end = _parse_date(args.end)
    return start, end


def cmd_init_db(_args):
    init_db()
    print("Database initialized")


def cmd_ingest(args):
    start, end = _resolve_date_range(args)
    ingest_form4_filings(start, end)
    print(f"Ingestion complete for {start} to {end}")


def cmd_top_trades(args):
    start, end = _resolve_date_range(args)
    limit = args.limit or 10
    results = get_top_trades(start, end, limit)
    print(json.dumps(results, default=str, indent=2))


def cmd_sector_activity(args):
    start, end = _resolve_date_range(args)
    results = get_sector_activity(start, end, args.sector)
    print(json.dumps(results, default=str, indent=2))


def cmd_person(args):
    results = get_person_activity(args.name, args.days)
    print(json.dumps(results, default=str, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="SEC insider Form 4 toolkit")
    subparsers = parser.add_subparsers(dest="command", required=True)

    init_parser = subparsers.add_parser("init-db", help="Initialize database tables")
    init_parser.set_defaults(func=cmd_init_db)

    ingest_parser = subparsers.add_parser("ingest", help="Ingest Form 4 filings")
    ingest_group = ingest_parser.add_mutually_exclusive_group(required=False)
    ingest_group.add_argument("--days", type=int, default=7, help="Number of trailing days to ingest")
    ingest_group.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    ingest_group.add_argument("--end", type=str, help="End date YYYY-MM-DD")
    ingest_parser.set_defaults(func=cmd_ingest)

    top_parser = subparsers.add_parser("top-trades", help="Top trades by value")
    top_parser.add_argument("--days", type=int, help="Trailing days window")
    top_parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    top_parser.add_argument("--end", type=str, help="End date YYYY-MM-DD")
    top_parser.add_argument("--limit", type=int, default=10)
    top_parser.set_defaults(func=cmd_top_trades)

    sector_parser = subparsers.add_parser("sector-activity", help="Activity aggregated by sector")
    sector_parser.add_argument("--days", type=int, help="Trailing days window")
    sector_parser.add_argument("--start", type=str, help="Start date YYYY-MM-DD")
    sector_parser.add_argument("--end", type=str, help="End date YYYY-MM-DD")
    sector_parser.add_argument("--sector", type=str, help="Optional sector filter")
    sector_parser.set_defaults(func=cmd_sector_activity)

    person_parser = subparsers.add_parser("person", help="Lookup activity for an individual")
    person_parser.add_argument("--name", required=True, type=str)
    person_parser.add_argument("--days", type=int, default=90)
    person_parser.set_defaults(func=cmd_person)

    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()