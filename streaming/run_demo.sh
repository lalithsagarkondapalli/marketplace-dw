#!/usr/bin/env bash
# End-to-end streaming check on the file source:
#   1. stream the first half of the events
#   2. stream the second half (resumes from the checkpoint)
#   3. simulate a crash after the last micro-batch wrote its output but before
#      Spark committed it, then restart: the batch is replayed and must change nothing
#   4. verify every table against the producer's ground truth
set -euo pipefail
PY=${PY:-python}
rm -rf data/stream
psql_reset="drop schema if exists streaming cascade"
$PY - <<PYEOF
from streaming.stream_job import connect
with connect() as c, c.cursor() as cur: cur.execute("$psql_reset")
PYEOF

$PY -m streaming.producer --sink files --to-chunk 40
$PY -m streaming.stream_job --source files
$PY -m streaming.producer --sink files --from-chunk 40
$PY -m streaming.stream_job --source files

last=$(ls data/stream/checkpoints/order_events/commits | sort -n | tail -1)
echo "simulating crash: removing commit for batch $last"
rm -f "data/stream/checkpoints/order_events/commits/$last" "data/stream/checkpoints/order_events/commits/.$last.crc"
$PY -m streaming.stream_job --source files

$PY -m streaming.verify
