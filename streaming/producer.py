"""Replay the order-event stream with realistic defects, to files or Kafka.

The replay is deterministic (seeded) and injects:
* jitter      - events arrive slightly out of order (within a few hours)
* duplicates  - some events are delivered twice (at-least-once delivery)
* malformed   - some messages are broken JSON or miss required fields
* very late   - some events arrive long after their event time

A manifest with the ground truth is written next to the output so the
stream job's results can be verified exactly.

    python -m streaming.producer --sink files --out data/stream/input
    python -m streaming.producer --sink kafka --bootstrap localhost:9092 --topic order_events
"""
import argparse
import json
import random
from datetime import datetime, timedelta
from pathlib import Path

from streaming.events import build_events

SEED = 7
DUP_RATE = 0.01
MALFORMED_RATE = 0.002
VERY_LATE_RATE = 0.003
VERY_LATE_DELAY = timedelta(days=20)
JITTER = timedelta(hours=6)
MESSAGES_PER_BATCH = 5000  # one file per micro-batch; matches maxOffsetsPerTrigger for Kafka


def plan_messages():
    rng = random.Random(SEED)
    events = build_events()
    arrivals = []  # (arrival_time, seq, payload, kind, event)
    seq = 0
    very_late_ids = []
    for ev in events:
        et = datetime.fromisoformat(ev["event_time"])
        if rng.random() < VERY_LATE_RATE:
            arrival = et + VERY_LATE_DELAY
            very_late_ids.append(ev["event_id"])
        else:
            arrival = et + timedelta(seconds=rng.uniform(0, JITTER.total_seconds()))
        arrivals.append((arrival, seq, json.dumps(ev), "event", ev)); seq += 1
        if rng.random() < DUP_RATE:
            dup_arrival = arrival + timedelta(seconds=rng.uniform(0, 3600))
            arrivals.append((dup_arrival, seq, json.dumps(ev), "duplicate", ev)); seq += 1
        if rng.random() < MALFORMED_RATE:
            bad = rng.choice([
                json.dumps(ev)[:-7],                                            # truncated JSON
                json.dumps({k: v for k, v in ev.items() if k != "order_id"}),  # missing order_id
                json.dumps({**ev, "event_type": "order_teleported"}),          # unknown type
            ])
            arrivals.append((arrival, seq, bad, "malformed", None)); seq += 1
    arrivals.sort(key=lambda a: (a[0], a[1]))

    manifest = {
        "seed": SEED,
        "unique_events": len(events),
        "messages": len(arrivals),
        "duplicates_injected": sum(1 for a in arrivals if a[3] == "duplicate"),
        "malformed_injected": sum(1 for a in arrivals if a[3] == "malformed"),
        "very_late_injected": len(very_late_ids),
        "very_late_event_ids": very_late_ids,
    }
    return arrivals, manifest


def to_files(arrivals, out: Path, per_file: int, start: int, stop: int | None):
    out.mkdir(parents=True, exist_ok=True)
    chunks = [arrivals[i:i + per_file] for i in range(0, len(arrivals), per_file)]
    stop = len(chunks) if stop is None else min(stop, len(chunks))
    for n in range(start, stop):
        tmp = out / f".part-{n:05d}.json"
        tmp.write_text("\n".join(a[2] for a in chunks[n]) + "\n")
        tmp.rename(out / f"part-{n:05d}.json")  # atomic publish for the file source
    return len(chunks), stop


def to_kafka(arrivals, bootstrap: str, topic: str, broker_wait_s: int = 90):
    import time
    from confluent_kafka import KafkaException, Producer
    from confluent_kafka.admin import AdminClient, NewTopic

    # Wait for the broker to accept metadata requests, then make sure the topic exists
    # with a single partition so message order is preserved end to end.
    admin = AdminClient({"bootstrap.servers": bootstrap})
    deadline = time.time() + broker_wait_s
    while True:
        try:
            topics = admin.list_topics(timeout=5).topics
            break
        except KafkaException:
            if time.time() > deadline:
                raise
            time.sleep(2)
    if topic not in topics:
        for fut in admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)]).values():
            fut.result()

    failed = []
    def on_delivery(err, _msg):
        if err is not None:
            failed.append(err)

    producer = Producer({"bootstrap.servers": bootstrap, "linger.ms": 20, "acks": "all",
                         "enable.idempotence": True})
    for a in arrivals:
        while True:
            try:
                producer.produce(topic, a[2].encode(), on_delivery=on_delivery)
                break
            except BufferError:
                producer.poll(0.5)  # local queue full: let deliveries drain (backpressure)
        producer.poll(0)
    remaining = producer.flush(120)
    if remaining or failed:
        raise RuntimeError(f"{remaining} undelivered, {len(failed)} failed deliveries: {failed[:3]}")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--sink", choices=["files", "kafka"], required=True)
    p.add_argument("--out", default="data/stream/input")
    p.add_argument("--per-file", type=int, default=MESSAGES_PER_BATCH)
    p.add_argument("--from-chunk", type=int, default=0)
    p.add_argument("--to-chunk", type=int, default=None)
    p.add_argument("--bootstrap", default="localhost:9092")
    p.add_argument("--topic", default="order_events")
    p.add_argument("--manifest", default="data/stream/manifest.json")
    args = p.parse_args()

    arrivals, manifest = plan_messages()
    Path(args.manifest).parent.mkdir(parents=True, exist_ok=True)
    Path(args.manifest).write_text(json.dumps(manifest, indent=1))
    if args.sink == "files":
        total, written_to = to_files(arrivals, Path(args.out), args.per_file, args.from_chunk, args.to_chunk)
        print(f"wrote chunks {args.from_chunk}..{written_to - 1} of {total}")
    else:
        to_kafka(arrivals, args.bootstrap, args.topic)
        print(f"sent {len(arrivals)} messages to {args.topic}")
    print({k: v for k, v in manifest.items() if k != "very_late_event_ids"})


if __name__ == "__main__":
    main()
