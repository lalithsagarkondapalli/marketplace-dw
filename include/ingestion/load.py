"""Load step: idempotent batch loads from the landing zone into the ``raw`` schema.

Design notes
------------
* Raw tables keep every source column as TEXT. Typing and cleaning happen in dbt
  staging models, so a bad value never blocks ingestion and the raw layer stays a
  faithful copy of the source.
* Every raw row carries ``_batch_id`` and ``_loaded_at``. Re-running a batch first
  deletes that batch's rows, so retries and backfills are idempotent.
* Orders are loaded by purchase-date interval (one Airflow data interval per batch).
  Child rows (items, payments, reviews) and customers are loaded for exactly the
  orders in that batch, so each batch is referentially complete.
* Reference tables are full-refreshed only when the file checksum changes.
"""
import csv
import io
import logging
import os
from datetime import datetime

import psycopg2

from include.ingestion.extract import landing_dir, sha256
from include.ingestion.sources import SOURCES, SOURCES_BY_NAME

log = logging.getLogger(__name__)

ORDER_TS_FORMAT = "%Y-%m-%d %H:%M:%S"


def connect():
    return psycopg2.connect(
        os.environ.get(
            "WAREHOUSE_DSN",
            "host=localhost port=5432 dbname=warehouse user=warehouse password=warehouse",
        )
    )


def ensure_raw_schema(cur) -> None:
    cur.execute("create schema if not exists raw")
    for s in SOURCES:
        cols = ", ".join(f"{c} text" for c in s.columns)
        cur.execute(
            f"create table if not exists raw.{s.name} ({cols}, "
            f"_batch_id text not null, _loaded_at timestamptz not null default now())"
        )
        cur.execute(f"create index if not exists {s.name}_batch_idx on raw.{s.name} (_batch_id)")
    cur.execute(
        """create table if not exists raw._load_audit (
               batch_id text, table_name text, rows_loaded bigint,
               source_sha256 text, loaded_at timestamptz default now())"""
    )


def _rows(source):
    with open(landing_dir() / source.file, newline="", encoding="utf-8-sig") as fh:
        reader = csv.reader(fh)
        next(reader)  # header already validated by the extract step
        for row in reader:
            yield row


def _copy(cur, source, rows, batch_id: str) -> int:
    buf = io.StringIO()
    writer = csv.writer(buf)
    count = 0
    for row in rows:
        writer.writerow([*row, batch_id])
        count += 1
    buf.seek(0)
    cols = ", ".join([*source.columns, "_batch_id"])
    cur.copy_expert(f"copy raw.{source.name} ({cols}) from stdin with (format csv)", buf)
    return count


def _audit(cur, batch_id, table, rows, checksum):
    cur.execute(
        "insert into raw._load_audit (batch_id, table_name, rows_loaded, source_sha256) "
        "values (%s, %s, %s, %s)",
        (batch_id, table, rows, checksum),
    )


def load_reference_tables() -> dict:
    """Full refresh of reference tables whose source checksum changed since the last load."""
    loaded = {}
    with connect() as conn, conn.cursor() as cur:
        ensure_raw_schema(cur)
        for s in (x for x in SOURCES if x.load_type == "reference"):
            checksum = sha256(landing_dir() / s.file)
            cur.execute(
                "select source_sha256 from raw._load_audit where table_name = %s "
                "order by loaded_at desc limit 1",
                (s.name,),
            )
            last = cur.fetchone()
            if last and last[0] == checksum:
                log.info("%s unchanged, skipping", s.name)
                continue
            cur.execute(f"truncate raw.{s.name}")
            n = _copy(cur, s, _rows(s), batch_id=f"ref-{checksum[:12]}")
            _audit(cur, f"ref-{checksum[:12]}", s.name, n, checksum)
            loaded[s.name] = n
            log.info("%s: %s rows", s.name, n)
    return loaded


def load_order_batch(interval_start: datetime, interval_end: datetime) -> dict:
    """Load orders purchased in [interval_start, interval_end) plus their child rows."""
    batch_id = interval_start.strftime("%Y-%m-%d")
    start = interval_start.replace(tzinfo=None)
    end = interval_end.replace(tzinfo=None)
    counts = {}

    orders_src = SOURCES_BY_NAME["orders"]
    ts_idx = orders_src.columns.index("order_purchase_timestamp")
    cust_idx = orders_src.columns.index("customer_id")
    batch_orders, order_ids, customer_ids = [], set(), set()
    for row in _rows(orders_src):
        ts = datetime.strptime(row[ts_idx], ORDER_TS_FORMAT)
        if start <= ts < end:
            batch_orders.append(row)
            order_ids.add(row[0])
            customer_ids.add(row[cust_idx])

    with connect() as conn, conn.cursor() as cur:
        ensure_raw_schema(cur)
        for s in (x for x in SOURCES if x.load_type != "reference"):
            cur.execute(f"delete from raw.{s.name} where _batch_id = %s", (batch_id,))
            if s.load_type == "orders":
                rows = batch_orders
            elif s.load_type == "order_child":
                key = s.columns.index("order_id")
                rows = (r for r in _rows(s) if r[key] in order_ids)
            else:  # order_customer
                rows = (r for r in _rows(s) if r[0] in customer_ids)
            counts[s.name] = _copy(cur, s, rows, batch_id)
            _audit(cur, batch_id, s.name, counts[s.name], sha256(landing_dir() / s.file))
    log.info("batch %s: %s", batch_id, counts)
    return counts
