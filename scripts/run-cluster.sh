#!/usr/bin/env bash
# ============================================================================
#  run-cluster.sh  -  submit the sanitizer to YARN
#
#  Usage:
#    ./scripts/run-cluster.sh <local-input.csv> [split-bytes]
#
#  Examples:
#    ./scripts/run-cluster.sh ~/hr_raw.csv                # default 128 MiB splits
#    ./scripts/run-cluster.sh ~/hr_raw.csv 67108864       # force 64 MiB splits
#
#  The second argument is the split-size experiment described in
#  docs/RESULTS.md. It changes how many map tasks the job gets WITHOUT
#  changing the data, the jar or the rules - which is what makes the
#  comparison honest.
# ============================================================================
set -euo pipefail

IN_LOCAL="${1:?usage: run-cluster.sh <local-input.csv> [split-bytes]}"
SPLIT="${2:-}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

: "${HADOOP_HOME:=/opt/hadoop}"
export PATH="$HADOOP_HOME/bin:$PATH"

HDFS_BASE=/data-sanitizer/hr
HDFS_IN="$HDFS_BASE/input"
HDFS_OUT="$HDFS_BASE/output"
REDUCERS=2

[[ -f data-sanitizer.jar ]] || { echo "no jar - run ./scripts/build.sh first" >&2; exit 1; }
[[ -f "$IN_LOCAL" ]]        || { echo "no such file: $IN_LOCAL" >&2; exit 1; }

echo "==> 1/5  cluster reachable?"
hdfs dfsadmin -report | head -8
yarn node -list 2>/dev/null | head -4
echo

echo "==> 2/5  staging input into HDFS"
hdfs dfs -mkdir -p "$HDFS_IN"
# -put -f overwrites. A 244 MiB file over a mesh VPN takes a few minutes;
# skip the upload if the size already matches.
LOCAL_SZ=$(stat -c%s "$IN_LOCAL")
HDFS_SZ=$(hdfs dfs -du -s "$HDFS_IN/$(basename "$IN_LOCAL")" 2>/dev/null | awk '{print $1}' || echo 0)
if [[ "$LOCAL_SZ" == "$HDFS_SZ" ]]; then
  echo "    already present at the right size ($LOCAL_SZ bytes) - skipping upload"
else
  hdfs dfs -put -f "$IN_LOCAL" "$HDFS_IN/"
fi
hdfs dfs -ls -h "$HDFS_IN"
echo

echo "==> 3/5  clearing the output directory"
# MapReduce refuses to start if the output path exists. This is a feature -
# it stops a re-run silently half-overwriting a previous result.
hdfs dfs -rm -r -f -skipTrash "$HDFS_OUT" || true
echo

echo "==> 4/5  submitting"
EXTRA=()
if [[ -n "$SPLIT" ]]; then
  # split.maxsize caps a split BELOW the block size, so one 128 MiB block can
  # yield several map tasks. It cannot make splits larger than a block.
  EXTRA+=(-D "mapreduce.input.fileinputformat.split.maxsize=$SPLIT")
  echo "    split.maxsize = $SPLIT bytes"
fi

set -x
hadoop jar data-sanitizer.jar com.bda.sanitizer.DataSanitizerDriver \
    "${EXTRA[@]}" \
    -D mapreduce.job.reduces=$REDUCERS \
    "$HDFS_IN" "$HDFS_OUT"
set +x
echo

echo "==> 5/5  verifying"
exec "$ROOT/scripts/verify-output.sh" "$HDFS_OUT"
