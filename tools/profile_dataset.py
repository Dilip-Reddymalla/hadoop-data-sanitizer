#!/usr/bin/env python3
"""
profile_dataset.py  -  find out what is actually wrong with a CSV before you
write a single line of MapReduce.

    python3 tools/profile_dataset.py hr_raw.csv
    python3 tools/profile_dataset.py hr_raw.csv --limit 200000

Why this exists
---------------
Every validation rule in the Java mapper was chosen because this script found
the defect first. Writing rules by guesswork gives you a job that rejects
nothing (so it looks like it did nothing) or rejects everything (so it looks
broken). Profile first, then encode.

It also produced the numbers quoted in docs/RESULTS.md, which is why it ships
with the code rather than being deleted after use. Anyone re-running the project
on a different HR export can re-derive their own numbers instead of trusting
ours.

Pure standard library. No pandas: a 244 MiB file is streamed row by row, so peak
memory is a few megabytes plus the duplicate-id set, and it runs anywhere python3
runs - including inside a bare WSL install.
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from collections import Counter, defaultdict

# --------------------------------------------------------------------------- #
# The same schema constants as RecordSchema.java. Kept in sync by hand, which is
# a real cost, but the alternative is a config file that has to be parsed twice.
# --------------------------------------------------------------------------- #
COLUMNS = [
    "Employee_ID", "Full_Name", "Department", "Job_Title", "Hire_Date",
    "Performance_Rating", "Experience_Years", "Status", "Work_Mode",
    "Salary", "Year", "Country", "City", "Age", "Job_Level",
]
IDX = {name: i for i, name in enumerate(COLUMNS)}

PLACEHOLDERS = {
    "", "n/a", "na", "null", "none", "unknown", "-", "--", "?", "nan", "<na>",
}

ID_RE = re.compile(r"^EMP\d{7}$")

DATE_RES = [
    re.compile(r"^\d{4}-\d{2}-\d{2}$"),   # yyyy-MM-dd
    re.compile(r"^\d{2}-\d{2}-\d{4}$"),   # dd-MM-yyyy
    re.compile(r"^\d{2}/\d{2}/\d{4}$"),   # MM/dd/yyyy
    re.compile(r"^\d{4}/\d{2}/\d{2}$"),   # yyyy/MM/dd
]

CATEGORIES = {
    "Department":         {"sales", "it", "operations", "finance", "hr"},
    "Job_Level":          {"junior", "mid", "senior", "director"},
    "Status":             {"active", "resigned", "terminated", "retired"},
    "Work_Mode":          {"on-site", "remote", "hybrid"},
    "Performance_Rating": {"excellent", "good", "satisfactory",
                           "needs improvement"},
}

# (column, low, high, must_be_integer)
NUMERIC = [
    ("Salary",           0.01, 1.0e7, False),
    ("Experience_Years", 0.0,  60.0,  True),
    ("Age",              16.0, 75.0,  True),
    ("Year",             1990, 2026,  True),
]

MIN_ENTRY_AGE = 21


def is_blank(v: str) -> bool:
    return v.strip().lower() in PLACEHOLDERS


def profile(path: str, limit: int | None):
    rows = 0
    header: list[str] | None = None

    wrong_width = Counter()          # field count -> rows
    blank_by_col = Counter()
    bad_id = 0
    bad_date = 0
    bad_category = defaultdict(Counter)   # column -> offending value -> count
    numeric_bad = Counter()               # column -> rows
    numeric_range = {}                    # column -> (min, max)
    quoted_rows = 0
    seen_ids: set[str] = set()
    dup_ids = 0

    # The logical rule is the interesting one, so profile the whole margin
    # distribution rather than a single pass/fail. This is how we learned the
    # dataset's exact floor is 21 and not, say, 18.
    margin_hist = Counter()
    min_margin = None

    # csv.reader is RFC-4180 aware: it keeps "Smith, John" as one field. That is
    # the whole point - a naive line.split(",") is what produces phantom
    # "malformed" rows and sends you chasing a bug that is in your parser.
    with open(path, newline="", encoding="utf-8", errors="replace") as fh:
        reader = csv.reader(fh)
        for raw in reader:
            if header is None:
                header = raw
                continue
            if limit is not None and rows >= limit:
                break
            rows += 1

            if len(raw) != len(COLUMNS):
                wrong_width[len(raw)] += 1
                continue

            # A row is "quote-rescued" if any field contains a comma. Split
            # naively it would have looked like an extra column.
            if any("," in v for v in raw):
                quoted_rows += 1

            for name, i in IDX.items():
                if is_blank(raw[i]):
                    blank_by_col[name] += 1

            emp = raw[IDX["Employee_ID"]].strip()
            if not ID_RE.match(emp):
                bad_id += 1
            if emp in seen_ids:
                dup_ids += 1
            else:
                seen_ids.add(emp)

            hire = raw[IDX["Hire_Date"]].strip()
            if not any(r.match(hire) for r in DATE_RES):
                bad_date += 1

            for col, allowed in CATEGORIES.items():
                v = raw[IDX[col]].strip()
                if v.lower() not in allowed and not is_blank(v):
                    bad_category[col][v] += 1

            for col, lo, hi, must_int in NUMERIC:
                v = raw[IDX[col]].strip()
                try:
                    f = float(v)
                except ValueError:
                    numeric_bad[col] += 1
                    continue
                if must_int and f != int(f):
                    numeric_bad[col] += 1
                    continue
                if not (lo <= f <= hi):
                    numeric_bad[col] += 1
                    continue
                cur = numeric_range.get(col)
                numeric_range[col] = (min(f, cur[0]), max(f, cur[1])) if cur \
                    else (f, f)

            try:
                age = float(raw[IDX["Age"]])
                exp = float(raw[IDX["Experience_Years"]])
            except ValueError:
                pass
            else:
                m = age - exp
                margin_hist[int(m)] += 1
                min_margin = m if min_margin is None else min(min_margin, m)

    return locals()


def report(r) -> None:
    rows = r["rows"]
    w = lambda n: f"{n:,}"
    pct = lambda n: f"{100.0 * n / rows:6.3f}%" if rows else "     -"

    def h(title):
        print()
        print("=" * 74)
        print(f" {title}")
        print("=" * 74)

    h("FILE")
    print(f"  path                : {r['path']}")
    print(f"  header              : {len(r['header'] or [])} columns")
    print(f"  data rows examined  : {w(rows)}")
    print(f"  rows with a comma inside a quoted field : {w(r['quoted_rows'])}"
          f"  ({pct(r['quoted_rows'])})")
    print("      ^ these are the rows a naive split would corrupt. If this is")
    print("        non-zero you MUST parse quote-aware.")
    print()
    print("  NOTE ON COMPARING THIS WITH THE JOB COUNTERS")
    print("  Every section below counts each defect INDEPENDENTLY, so one bad")
    print("  row can appear under several headings. The mapper does not: it")
    print("  applies the rules as a ladder and stops at the first failure, so")
    print("  each rejected row is charged to exactly one counter and the seven")
    print("  reason counters sum to the invalid total. Expect this report's")
    print("  per-rule numbers to be greater than or equal to the job's, and")
    print("  read the difference as overlap, not as a discrepancy.")

    if r["wrong_width"]:
        h("STRUCTURAL - wrong number of fields")
        for n, k in sorted(r["wrong_width"].items()):
            print(f"  {w(k):>12} rows have {n} fields (expected {len(COLUMNS)})")
    else:
        h("STRUCTURAL - wrong number of fields")
        print("  none. Every row has the expected field count.")

    h("BLANKS AND PLACEHOLDER TOKENS, BY COLUMN")
    print("  Counting empty strings plus " + ", ".join(sorted(
        t for t in PLACEHOLDERS if t)) + ".")
    if r["blank_by_col"]:
        for col, k in r["blank_by_col"].most_common():
            print(f"  {col:<20} {w(k):>12}   {pct(k)}")
    else:
        print("  none")

    h("IDENTIFIERS")
    print(f"  not matching EMP + 7 digits : {w(r['bad_id'])}   {pct(r['bad_id'])}")
    print(f"  distinct ids                : {w(len(r['seen_ids']))}")
    print(f"  duplicate ids               : {w(r['dup_ids'])}")
    print("      ^ if this is 0 the reducer's dedup will be a no-op on this")
    print("        file. Say so rather than implying it saved you.")

    h("DATES")
    print(f"  Hire_Date matching none of the {len(DATE_RES)} accepted formats: "
          f"{w(r['bad_date'])}   {pct(r['bad_date'])}")

    h("CATEGORICAL - values outside the allowed set")
    any_cat = False
    for col in CATEGORIES:
        c = r["bad_category"][col]
        if c:
            any_cat = True
            print(f"  {col}: {w(sum(c.values()))} rows, "
                  f"{len(c)} distinct offending values")
            for v, k in c.most_common(8):
                print(f"      {w(k):>10}  {v!r}")
    if not any_cat:
        print("  none. Every categorical value is in the allowed set")
        print("  (case aside - the mapper normalises case rather than rejecting).")

    h("NUMERIC - unparseable, non-integer where integer required, or out of range")
    for col, lo, hi, must_int in NUMERIC:
        k = r["numeric_bad"][col]
        rng = r["numeric_range"].get(col)
        rngs = f"observed {rng[0]:g} .. {rng[1]:g}" if rng else "no valid values"
        kind = "int" if must_int else "any"
        print(f"  {col:<20} bad {w(k):>10}  {pct(k)}   "
              f"allowed [{lo:g}, {hi:g}] {kind};  {rngs}")

    h("CROSS-FIELD LOGIC - Age vs Experience_Years")
    print("  A career cannot start before you can legally have one. The rule is")
    print("  Age >= Experience_Years + MIN_ENTRY_AGE.")
    mm = r["min_margin"]
    print(f"  observed minimum of (Age - Experience_Years) : "
          f"{mm if mm is None else f'{mm:g}'}")
    print()
    print("  distribution of the margin, lowest 12 buckets:")
    for m in sorted(r["margin_hist"])[:12]:
        k = r["margin_hist"][m]
        bar = "#" * min(46, int(46 * k / max(r["margin_hist"].values())))
        print(f"    Age-Exp = {m:>4}  {w(k):>12}  {bar}")
    print()
    for thr in (MIN_ENTRY_AGE, MIN_ENTRY_AGE + 1, MIN_ENTRY_AGE + 2):
        n = sum(k for m, k in r["margin_hist"].items() if m < thr)
        print(f"  rows failing Age >= Exp + {thr:<3} : {w(n):>12}   {pct(n)}")
    print()
    print("  Pick the threshold from THIS table, not from intuition. One step")
    print("  either side of the real floor changes the rejection count by an")
    print("  order of magnitude, and a rule that rejects a third of a clean")
    print("  dataset is a bug in the rule, not a finding about the data.")
    print()


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Profile an HR CSV for the defects the sanitizer filters.")
    ap.add_argument("csv_path")
    ap.add_argument("--limit", type=int, default=None,
                    help="stop after N data rows (quick pass over a huge file)")
    a = ap.parse_args()
    try:
        report(profile(a.csv_path, a.limit))
    except FileNotFoundError:
        print(f"no such file: {a.csv_path}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        return 130
    return 0


if __name__ == "__main__":
    sys.exit(main())
