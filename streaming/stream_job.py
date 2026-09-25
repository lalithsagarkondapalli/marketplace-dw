"""Spark Structured Streaming job for the order-event stream.

Two queries share one source (Kafka topic or a directory of JSON-lines files):

1. order_events  - validates every message, quarantines bad ones with a reason,
                   appends valid events (deduplicated on event_id) and upserts the
                   current state of each order. Every write is idempotent, so a
                   micro-batch replayed after a failure changes nothing.
2. daily_purchases - event-time aggregation of purchase events per day with a
                   3-day watermark: duplicates are dropped with
                   dropDuplicatesWithinWatermark, windows are emitted once final
                   (append mode), and events that arrive later than the watermark
                   are excluded from the finalized windows.

Both queries checkpoint their progress, so a restart resumes where it stopped.

    python -m streaming.stream_job --source files --input data/stream/input
    python -m streaming.stream_job --source kafka --bootstrap localhost:9092 --topic order_events
"""
import argparse
import json
import logging
import os

import psycopg2
from psycopg2.extras import execute_values
from pyspark.sql import SparkSession, functions as F, types as T

from streaming.events import EVENT_TYPES

log = logging.getLogger("stream_job")

KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
STAGE_RANK = {t: i + 1 for i, t in enumerate(EVENT_TYPES)}
MILESTONE_COLUMN = {
    "order_purchased": "purchased_at",
    "order_approved": "approved_at",
    "order_shipped": "shipped_at",
    "order_delivered": "delivered_at",
}

EVENT_SCHEMA = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("order_id", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("event_time", T.TimestampType()),
    T.StructField("item_count", T.IntegerType()),
    T.StructField("items_value", T.DecimalType(12, 2)),
    T.StructField("freight_value", T.DecimalType(12, 2)),
])

DDL = """
create schema if not exists streaming;
create table if not exists streaming.order_events (
    event_id      text primary key,
    order_id      text not null,
    event_type    text not null,
    event_time    timestamp not null,
    item_count    int,
    items_value   numeric(12,2),
    freight_value numeric(12,2),
    batch_id      bigint not null,
    ingested_at   timestamptz not null default now()
);
create table if not exists streaming.order_state (
    order_id        text primary key,
    current_stage   text not null,
    stage_rank      int not null,
    purchased_at    timestamp,
    approved_at     timestamp,
    shipped_at      timestamp,
    delivered_at    timestamp,
    last_batch_id   bigint not null
);
create table if not exists streaming.quarantine (
    payload_md5  text primary key,
    reason       text not null,
    payload      text not null,
    batch_id     bigint not null,
    received_at  timestamptz not null default now()
);
create table if not exists streaming.daily_purchases (
    purchase_date  date primary key,
    orders         bigint not null,
    items_value    numeric(14,2) not null,
    freight_value  numeric(14,2) not null,
    batch_id       bigint not null
);
create table if not exists streaming.batch_log (
    query        text not null,
    batch_id     bigint not null,
    rows_in      bigint not null,
    rows_valid   bigint,
    rows_quarantined bigint,
    rows_new_events  bigint,
    processed_at timestamptz not null default now(),
    primary key (query, batch_id)
);
"""


def connect():
    return psycopg2.connect(os.environ.get(
        "WAREHOUSE_DSN", "host=localhost port=5432 dbname=warehouse user=warehouse password=warehouse"))


def parse(raw):
    """Parse raw message strings and attach a quarantine reason (null when valid)."""
    parsed = raw.withColumn("e", F.from_json("value", EVENT_SCHEMA)).select("value", "e.*")
    valid_json = F.get_json_object("value", "$.event_id").isNotNull() | \
        F.get_json_object("value", "$.order_id").isNotNull()
    reason = (
        F.when(~valid_json, "malformed_json")
        .when(F.col("event_id").isNull() | F.col("order_id").isNull() | F.col("event_time").isNull(),
              "missing_required_field")
        .when(~F.col("event_type").isin(*EVENT_TYPES), "unknown_event_type")
        .when(F.coalesce(F.col("items_value"), F.lit(0)) < 0, "negative_amount")
    )
    return parsed.withColumn("reject_reason", reason)


def write_events_batch(batch_df, batch_id):
    rows = batch_df.collect()
    bad = [(r.value, r.reject_reason) for r in rows if r.reject_reason]
    good = {}
    for r in rows:
        if not r.reject_reason:
            good.setdefault(r.event_id, r)  # in-batch dedup; cross-batch dedup via primary key

    with connect() as conn, conn.cursor() as cur:
        if bad:
            execute_values(cur, """
                insert into streaming.quarantine (payload_md5, reason, payload, batch_id)
                select md5(v.payload), v.reason, v.payload, v.batch_id
                from (values %s) as v(payload, reason, batch_id)
                on conflict (payload_md5) do nothing""",
                [(p, why, batch_id) for p, why in bad])
        new_events = 0
        if good:
            execute_values(cur, """
                insert into streaming.order_events
                    (event_id, order_id, event_type, event_time, item_count, items_value, freight_value, batch_id)
                values %s on conflict (event_id) do nothing""",
                [(r.event_id, r.order_id, r.event_type, r.event_time, r.item_count,
                  r.items_value, r.freight_value, batch_id) for r in good.values()], page_size=100000)
            new_events = cur.rowcount
            # Order state: milestone timestamps merge regardless of arrival order; the
            # current stage only moves forward, so a late "approved" never overwrites
            # an earlier-arrived "delivered".
            per_order = {}
            for r in good.values():
                s = per_order.setdefault(r.order_id, {"rank": 0, "stage": None, **{c: None for c in MILESTONE_COLUMN.values()}})
                s[MILESTONE_COLUMN[r.event_type]] = r.event_time
                if STAGE_RANK[r.event_type] > s["rank"]:
                    s["rank"], s["stage"] = STAGE_RANK[r.event_type], r.event_type
            execute_values(cur, """
                insert into streaming.order_state as t
                    (order_id, current_stage, stage_rank, purchased_at, approved_at, shipped_at, delivered_at, last_batch_id)
                values %s
                on conflict (order_id) do update set
                    purchased_at  = coalesce(t.purchased_at, excluded.purchased_at),
                    approved_at   = coalesce(t.approved_at,  excluded.approved_at),
                    shipped_at    = coalesce(t.shipped_at,   excluded.shipped_at),
                    delivered_at  = coalesce(t.delivered_at, excluded.delivered_at),
                    current_stage = case when excluded.stage_rank > t.stage_rank
                                         then excluded.current_stage else t.current_stage end,
                    stage_rank    = greatest(t.stage_rank, excluded.stage_rank),
                    last_batch_id = excluded.last_batch_id""",
                [(oid, s["stage"], s["rank"], s["purchased_at"], s["approved_at"], s["shipped_at"],
                  s["delivered_at"], batch_id) for oid, s in per_order.items()])
        cur.execute("""
            insert into streaming.batch_log (query, batch_id, rows_in, rows_valid, rows_quarantined, rows_new_events)
            values ('order_events', %s, %s, %s, %s, %s) on conflict do nothing""",
            (batch_id, len(rows), len(good), len(bad), new_events))
    log.info("order_events batch %s: in=%s valid=%s quarantined=%s new=%s",
             batch_id, len(rows), len(good), len(bad), new_events)


def write_daily_batch(batch_df, batch_id):
    rows = batch_df.collect()
    with connect() as conn, conn.cursor() as cur:
        if rows:
            execute_values(cur, """
                insert into streaming.daily_purchases (purchase_date, orders, items_value, freight_value, batch_id)
                values %s on conflict (purchase_date) do update set
                    orders = excluded.orders, items_value = excluded.items_value,
                    freight_value = excluded.freight_value, batch_id = excluded.batch_id""",
                [(r.purchase_date, r.orders, r.items_value, r.freight_value, batch_id) for r in rows])
        cur.execute("""insert into streaming.batch_log (query, batch_id, rows_in)
                       values ('daily_purchases', %s, %s) on conflict do nothing""", (batch_id, len(rows)))


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--source", choices=["files", "kafka"], required=True)
    p.add_argument("--input", default="data/stream/input")
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="order_events")
    p.add_argument("--checkpoints", default="data/stream/checkpoints")
    p.add_argument("--max-per-trigger", type=int, default=5000)
    p.add_argument("--watermark", default="3 days")
    p.add_argument("--metrics-out", default="data/stream/metrics.json")
    args = p.parse_args()

    builder = (SparkSession.builder.appName("order-event-stream").master("local[2]")
               .config("spark.sql.shuffle.partitions", "4")
               .config("spark.sql.session.timeZone", "UTC"))
    if args.source == "kafka":
        builder = builder.config("spark.jars.packages", KAFKA_PACKAGE)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    with connect() as conn, conn.cursor() as cur:
        cur.execute(DDL)

    if args.source == "kafka":
        raw = (spark.readStream.format("kafka")
               .option("kafka.bootstrap.servers", args.bootstrap)
               .option("subscribe", args.topic)
               .option("startingOffsets", "earliest")
               .option("maxOffsetsPerTrigger", args.max_per_trigger)
               .load()
               .selectExpr("CAST(value AS STRING) AS value"))
    else:
        raw = (spark.readStream.format("text")
               .option("maxFilesPerTrigger", 1)
               .load(args.input))

    events = parse(raw)

    q_events = (events.writeStream.queryName("order_events")
                .foreachBatch(write_events_batch)
                .option("checkpointLocation", f"{args.checkpoints}/order_events")
                .trigger(availableNow=True)
                .start())

    daily = (events.where(F.col("reject_reason").isNull() & (F.col("event_type") == "order_purchased"))
             .withWatermark("event_time", args.watermark)
             .dropDuplicatesWithinWatermark(["event_id"])
             .groupBy(F.window("event_time", "1 day"))
             .agg(F.count("*").alias("orders"),
                  F.sum("items_value").alias("items_value"),
                  F.sum("freight_value").alias("freight_value"))
             .select(F.to_date(F.col("window.start")).alias("purchase_date"),
                     "orders", "items_value", "freight_value"))
    q_daily = (daily.writeStream.queryName("daily_purchases")
               .outputMode("append")
               .foreachBatch(write_daily_batch)
               .option("checkpointLocation", f"{args.checkpoints}/daily_purchases")
               .trigger(availableNow=True)
               .start())

    q_events.awaitTermination()
    q_daily.awaitTermination()

    dropped = sum(op.get("numRowsDroppedByWatermark", 0)
                  for prog in q_daily.recentProgress for op in prog.get("stateOperators", []))
    metrics = {
        "order_events_batches": len(q_events.recentProgress),
        "daily_batches": len(q_daily.recentProgress),
        "last_watermark": (q_daily.recentProgress[-1]["eventTime"].get("watermark")
                           if q_daily.recentProgress else None),
    }
    os.makedirs(os.path.dirname(args.metrics_out), exist_ok=True)
    if metrics["last_watermark"] is None and os.path.exists(args.metrics_out):
        # no new data this run: keep the watermark reached by the previous run
        metrics["last_watermark"] = json.load(open(args.metrics_out)).get("last_watermark")
    with open(args.metrics_out, "w") as fh:
        json.dump(metrics, fh, indent=1)
    log.info("metrics: %s", metrics)
    spark.stop()


if __name__ == "__main__":
    main()
