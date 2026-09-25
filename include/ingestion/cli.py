"""Command-line entry point so the same ingestion code runs outside Airflow (CI, local dev).

    python -m include.ingestion.cli extract
    python -m include.ingestion.cli load-reference
    python -m include.ingestion.cli load-orders --start 2016-09-01 --end 2018-11-01 [--monthly]
"""
import argparse
import logging
from datetime import datetime

from include.ingestion.extract import extract_all
from include.ingestion.load import load_order_batch, load_reference_tables


def month_starts(start: datetime, end: datetime):
    cur = start.replace(day=1)
    while cur < end:
        nxt = cur.replace(year=cur.year + (cur.month == 12), month=cur.month % 12 + 1)
        yield cur, min(nxt, end)
        cur = nxt


def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("extract")
    sub.add_parser("load-reference")
    lo = sub.add_parser("load-orders")
    lo.add_argument("--start", required=True, type=datetime.fromisoformat)
    lo.add_argument("--end", required=True, type=datetime.fromisoformat)
    lo.add_argument("--monthly", action="store_true", help="split into monthly batches")
    args = p.parse_args()

    if args.cmd == "extract":
        extract_all()
    elif args.cmd == "load-reference":
        load_reference_tables()
    else:
        windows = month_starts(args.start, args.end) if args.monthly else [(args.start, args.end)]
        for s, e in windows:
            load_order_batch(s, e)


if __name__ == "__main__":
    main()
