#!/usr/bin/env bash
# ============================================================================
#  verify-output.sh  -  independent checks on the job output
#
#  Usage:
#    ./scripts/verify-output.sh /data-sanitizer/hr/output     # HDFS path
#    ./scripts/verify-output.sh ./out --local                 # local path
#
#  The driver already prints three integrity checks of its own, computed from
#  the job counters. This script re-derives the same facts from the OUTPUT
#  BYTES instead, with tools that know nothing about the job. If the counters
#  lied, these would disagree.
#
#  What the output looks like: 15 columns, the same schema as the input, with
#  values normalised (dates to yyyy-MM-dd, categories to canonical case,
#  salaries with the trailing .0 stripped). There is no extra status column -
#  invalid records are dropped in the mapper, so presence in the file IS the
#  flag.
# ============================================================================
set -euo pipefail

OUT="${1:?usage: verify-output.sh <output-path> [--local]}"
MODE="${2:-}"

: "${HADOOP_HOME:=/opt/hadoop}"
export PATH="$HADOOP_HOME/bin:$PATH"

if [[ "$MODE" == "--local" ]]; then
  CAT() { cat "$OUT"/part-r-*; }
  LS()  { ls -l "$OUT"; }
else
  CAT() { hdfs dfs -cat "$OUT/part-r-*"; }
  LS()  { hdfs dfs -ls -h "$OUT"; }
fi

echo "=================================================================="
echo " output listing"
echo "=================================================================="
LS
echo
echo "A _SUCCESS marker means the job committed. Its absence means the output"
echo "is partial even if part files exist. The number of part-r-NNNNN files"
echo "equals the number of reducers, not the number of workers."
echo

echo "=================================================================="
echo " check 1  -  every row has exactly 15 fields"
echo "=================================================================="
echo "Parsed quote-aware. Do NOT check this with awk -F',' - a name such as"
echo "\"Nuwenhuysen, van der\" legitimately contains a comma inside quotes,"
echo "and naive splitting reports 16 fields for a perfectly valid row. That"
echo "false alarm is the same mistake the mapper had to avoid."
CAT | python3 -c '
import sys, csv, collections
c = collections.Counter(len(r) for r in csv.reader(sys.stdin) if r)
for n, k in sorted(c.items()):
    print(f"  {k:>12,}  rows x {n} fields")
print("  RESULT:", "PASS" if set(c) == {15} else "FAIL")
'
echo

echo "=================================================================="
echo " check 2  -  Employee_ID is unique"
echo "=================================================================="
echo "The reducer groups on Employee_ID and keeps the first record per key, so"
echo "a duplicate here would mean the shuffle did not group correctly. Safe to"
echo "cut on a comma: the id is field 1 and is never quoted."
DUPES=$(CAT | cut -d, -f1 | sort | uniq -d | wc -l)
echo "  duplicate ids: $DUPES"
echo "  RESULT: $([[ "$DUPES" -eq 0 ]] && echo PASS || echo FAIL)"
echo

echo "=================================================================="
echo " check 3  -  re-apply the rules to the output"
echo "=================================================================="
echo "Independently re-tests the four rules that are cheap to restate outside"
echo "Java. Any hit means a record escaped the mapper's ladder."
CAT | python3 -c '
import sys, csv, re

PLACEHOLDERS = {"", "n/a", "na", "null", "none", "unknown", "-", "--", "?",
                "nan", "<na>"}
ID_RE   = re.compile(r"^EMP\d{7}$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

rows = blanks = bad_id = bad_date = bad_salary = bad_logic = 0
for r in csv.reader(sys.stdin):
    if not r:
        continue
    rows += 1
    if any(v.strip().lower() in PLACEHOLDERS for v in r):        blanks     += 1
    if not ID_RE.match(r[0]):                                     bad_id     += 1
    if not DATE_RE.match(r[4]):                                   bad_date   += 1
    try:
        if float(r[9]) <= 0:                                      bad_salary += 1
    except ValueError:                                            bad_salary += 1
    try:
        if float(r[13]) < float(r[6]) + 21:                       bad_logic  += 1
    except ValueError:                                            bad_logic  += 1

print(f"  rows examined                        : {rows:,}")
print(f"  rows containing a blank/placeholder  : {blanks:,}")
print(f"  ids not matching EMP + 7 digits      : {bad_id:,}")
print(f"  hire dates not yyyy-MM-dd            : {bad_date:,}")
print(f"  salaries <= 0 or unparseable         : {bad_salary:,}")
print(f"  rows where Age < Experience + 21     : {bad_logic:,}")
bad = blanks + bad_id + bad_date + bad_salary + bad_logic
print("  RESULT:", "PASS" if bad == 0 else f"FAIL - {bad:,} rows should not be here")
'
echo

echo "=================================================================="
echo " row count"
echo "=================================================================="
echo "Compare with 'FINAL unique clean records' in the driver report and with"
echo "REDUCE_OUTPUT_RECORDS in the job counters. All three must agree."
CAT | wc -l
