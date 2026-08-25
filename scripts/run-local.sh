#!/usr/bin/env bash
# ============================================================================
#  run-local.sh  -  run the identical jar without HDFS or YARN
#
#  Usage:
#    ./scripts/run-local.sh <input.csv> <output-dir>
#
#  Two overrides turn a cluster job into a single-JVM job:
#    fs.defaultFS=file:///                 read and write the local filesystem
#    mapreduce.framework.name=local        run the task in-process, no YARN
#
#  Same mapper, same reducer, same rules, same counters. This is how you can
#  develop the logic while the cluster is down, and how the artefacts in
#  output/ in this package were produced - see docs/RESULTS.md.
#
#  It is NOT a substitute for the cluster run: one JVM, one map task, no
#  shuffle across a network, and no YARN metrics. Use it to prove the rules
#  are right, not to make performance claims.
# ============================================================================
set -euo pipefail

IN="${1:?usage: run-local.sh <input.csv> <output-dir>}"
OUT="${2:?usage: run-local.sh <input.csv> <output-dir>}"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

: "${HADOOP_HOME:=/opt/hadoop}"
export PATH="$HADOOP_HOME/bin:$PATH"

[[ -f data-sanitizer.jar ]] || { echo "no jar - run ./scripts/build.sh first" >&2; exit 1; }
[[ -f "$IN" ]]              || { echo "no such file: $IN" >&2; exit 1; }

# The driver will refuse to overwrite, same as on the cluster.
rm -rf "$OUT"
mkdir -p "$(dirname "$OUT")"

hadoop jar data-sanitizer.jar com.bda.sanitizer.DataSanitizerDriver \
    -D fs.defaultFS=file:/// \
    -D mapreduce.framework.name=local \
    "$IN" "$OUT"

echo
echo "==> output"
ls -l "$OUT"
echo
exec "$ROOT/scripts/verify-output.sh" "$OUT" --local
