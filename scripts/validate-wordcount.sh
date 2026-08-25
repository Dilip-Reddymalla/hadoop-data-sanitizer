#!/usr/bin/env bash
# ============================================================================
#  validate-wordcount.sh  -  the gate you pass BEFORE running your own job
#
#  Runs Hadoop's own bundled WordCount on a handful of lines. It is not
#  interesting as a computation. It is interesting because it exercises every
#  moving part of the cluster in the right order:
#
#     HDFS write -> RM submit -> container allocation on some node
#     -> map -> shuffle over the network -> reduce -> HDFS write -> commit
#
#  If WordCount cannot finish, your own job cannot either, and every minute
#  spent debugging your Java is wasted. If WordCount finishes and your job
#  does not, the fault is in your code or your client config - which is a much
#  smaller place to look.
#
#  Run this from the node you intend to SUBMIT from, not necessarily the master.
#  A job that works from the master and fails from a worker is a client-config
#  problem: see docs/TROUBLESHOOTING.md issues #2 and #3.
# ============================================================================
set -euo pipefail

: "${HADOOP_HOME:=/opt/hadoop}"
export PATH="$HADOOP_HOME/bin:$PATH"

IN=/validation/input
OUT=/validation/output

EXAMPLES=$(find "$HADOOP_HOME/share/hadoop/mapreduce" -name 'hadoop-mapreduce-examples-*.jar' | head -1)
[[ -n "$EXAMPLES" ]] || { echo "cannot find hadoop-mapreduce-examples jar under $HADOOP_HOME" >&2; exit 1; }
echo "examples jar: $EXAMPLES"

TMP=$(mktemp)
cat > "$TMP" <<'EOF'
hadoop distributed file system
yarn resource manager node manager container
mapreduce map shuffle reduce
hadoop yarn container
hadoop yarn container
hadoop container
EOF

echo "==> staging input"
hdfs dfs -mkdir -p "$IN"
hdfs dfs -put -f "$TMP" "$IN/wc_input.txt"
rm -f "$TMP"
hdfs dfs -rm -r -f -skipTrash "$OUT" || true

echo
echo "==> submitting word count"
echo "    Watch the console. What you want to see is 'map 0% reduce 0%'"
echo "    becoming 'map 100% reduce 100%'. If it prints the application id and"
echo "    then nothing at all for minutes, the app is stuck in ACCEPTED and no"
echo "    container was ever allocated - stop and read TROUBLESHOOTING #1."
time hadoop jar "$EXAMPLES" wordcount "$IN" "$OUT"

echo
echo "==> output"
hdfs dfs -ls "$OUT"
hdfs dfs -cat "$OUT/part-r-00000"

echo
echo "==> which node actually ran it"
APP=$(yarn application -list -appStates FINISHED 2>/dev/null \
      | awk '/word count/ {print $1}' | head -1)
if [[ -n "${APP:-}" ]]; then
  yarn application -status "$APP" 2>/dev/null \
    | grep -Ei 'application-id|state|final-state|started|finished|am host|node|queue'
  echo
  echo "If 'AM Host' is a WORKER and not the master, you have just proved that"
  echo "work is genuinely being distributed - not quietly running on one box."
else
  echo "(could not find the application in the FINISHED list)"
fi

echo
echo "PASS means: State=FINISHED, FinalState=SUCCEEDED, and the part file has"
echo "counts in it. Only now is it worth running your own jar."
