"""Check the stream job's output against the producer's ground truth.

Exits non-zero on any mismatch, so it can gate CI.

    python -m streaming.verify
"""
import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta

from streaming.events import EVENT_TYPES, build_events
from streaming.producer import plan_messages
from streaming.stream_job import connect

RANK = {t: i + 1 for i, t in enumerate(EVENT_TYPES)}
WATERMARK_DELAY = timedelta(days=3)


def main(metrics_path="data/stream/metrics.json"):
    events = build_events()
    arrivals, manifest = plan_messages()
    very_late = set(manifest["very_late_event_ids"])
    metrics = json.load(open(metrics_path))
    failures = []

    def check(name, actual, expected):
        ok = actual == expected
        print(f"{'PASS' if ok else 'FAIL'} {name}: actual={actual} expected={expected}")
        if not ok:
            failures.append(name)

    with connect() as conn, conn.cursor() as cur:
        cur.execute("select count(*), count(distinct event_id) from streaming.order_events")
        n_events, n_distinct = cur.fetchone()
        check("every valid event landed exactly once", n_events, manifest["unique_events"])
        check("no duplicate event_ids", n_distinct, n_events)

        cur.execute("select count(*) from streaming.quarantine")
        check("malformed messages quarantined", cur.fetchone()[0], manifest["malformed_injected"])

        cur.execute("select reason, count(*) from streaming.quarantine group by 1 order by 1")
        print("     quarantine reasons:", dict(cur.fetchall()))

        # Order state must equal the state derived from the full, ordered history.
        truth = {}
        for e in events:
            s = truth.setdefault(e["order_id"], {"rank": 0, "stage": None})
            if RANK[e["event_type"]] > s["rank"]:
                s["rank"], s["stage"] = RANK[e["event_type"]], e["event_type"]
        cur.execute("select order_id, current_stage from streaming.order_state")
        state = dict(cur.fetchall())
        check("orders tracked", len(state), len(truth))
        mismatched = sum(1 for oid, s in truth.items() if state.get(oid) != s["stage"])
        check("order stages correct despite out-of-order arrival", mismatched, 0)

        # Daily purchase windows. Every on-time purchase must be counted exactly once;
        # injected very-late events may be excluded (that is the watermark doing its
        # job) but can never be double counted. So for each finalized day:
        #   unique on-time events <= counted <= all unique events
        final_wm = datetime.fromisoformat(metrics["last_watermark"][:19])
        cutoff = (final_wm - timedelta(days=1)).date()
        on_time, everything = Counter(), Counter()
        for e in events:
            if e["event_type"] != "order_purchased":
                continue
            d = datetime.fromisoformat(e["event_time"]).date()
            everything[d] += 1
            if e["event_id"] not in very_late:
                on_time[d] += 1
        cur.execute("select purchase_date, orders from streaming.daily_purchases where purchase_date < %s", (cutoff,))
        actual = dict(cur.fetchall())
        days = [d for d in everything if d < cutoff]
        check("finalized daily windows present", len(actual), len(days))
        undercount = sum(1 for d in days if actual.get(d, 0) < on_time[d])
        overcount = sum(1 for d in days if actual.get(d, 0) > everything[d])
        check("days missing on-time purchases", undercount, 0)
        check("days double counting (dedup failure)", overcount, 0)
        excluded = sum(everything[d] for d in days) - sum(actual.values())
        late_total = sum(everything[d] - on_time[d] for d in days)
        print(f"     very-late purchase events in finalized days: {late_total}; excluded by watermark: {excluded}")
        dups = sum(1 for a in arrivals if a[3] == "duplicate" and a[4]["event_type"] == "order_purchased")
        print(f"     duplicate purchase messages delivered: {dups} (none double counted)")

    print(f"\n{len(failures)} failure(s)")
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main(*sys.argv[1:])
