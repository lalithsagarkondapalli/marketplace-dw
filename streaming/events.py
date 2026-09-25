"""Turn Olist orders into an order-lifecycle event stream.

Each order produces up to four events, one per milestone timestamp it has:
order_purchased, order_approved, order_shipped, order_delivered.
The purchase event carries the order's item and freight totals.
"""
import csv
import hashlib
import os
from collections import defaultdict
from pathlib import Path

LANDING = Path(os.environ.get("LANDING_DIR", "data/landing"))

MILESTONES = [
    ("order_purchased", "order_purchase_timestamp"),
    ("order_approved", "order_approved_at"),
    ("order_shipped", "order_delivered_carrier_date"),
    ("order_delivered", "order_delivered_customer_date"),
]
EVENT_TYPES = [m[0] for m in MILESTONES]


def _read(name):
    with open(LANDING / name, newline="", encoding="utf-8-sig") as fh:
        yield from csv.DictReader(fh)


def event_id(order_id: str, event_type: str) -> str:
    return hashlib.md5(f"{order_id}|{event_type}".encode()).hexdigest()


def build_events() -> list[dict]:
    totals = defaultdict(lambda: [0, 0.0, 0.0])  # item_count, items_value, freight
    for row in _read("olist_order_items_dataset.csv"):
        t = totals[row["order_id"]]
        t[0] += 1
        t[1] += float(row["price"])
        t[2] += float(row["freight_value"])

    events = []
    for order in _read("olist_orders_dataset.csv"):
        oid = order["order_id"]
        for event_type, column in MILESTONES:
            ts = order[column].strip()
            if not ts:
                continue
            ev = {
                "event_id": event_id(oid, event_type),
                "order_id": oid,
                "event_type": event_type,
                "event_time": ts.replace(" ", "T"),
            }
            if event_type == "order_purchased":
                count, value, freight = totals.get(oid, (0, 0.0, 0.0))
                ev["item_count"] = count
                ev["items_value"] = round(value, 2)
                ev["freight_value"] = round(freight, 2)
            events.append(ev)
    events.sort(key=lambda e: (e["event_time"], e["event_id"]))
    return events
