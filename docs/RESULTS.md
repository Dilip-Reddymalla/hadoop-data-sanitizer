# Results — every measured figure, and what it does and does not prove

Nothing here is estimated, rounded for effect, or reconstructed from memory.
Every number comes from a MapReduce counter, a `yarn application -status`
output, or a `stat` on a real file. Where a figure was not recoverable it is
marked absent rather than filled in.

Application IDs are as they appeared. Node names in the container-placement
table are replaced with the placeholder names used throughout this package —
see [ANONYMIZATION.md](ANONYMIZATION.md).

---

## 1. The input

| | |
|---|---|
| Source | public HR dataset, 2M rows, clean + raw variants (Kaggle) |
| File used | `hr_raw.csv` |
| Size | 256,161,831 bytes = 244.3 MiB |
| Rows | 2,000,000 data rows + 1 header |
| Columns | 15 |
| HDFS block size | 134,217,728 (128 MiB) |
| Blocks | 2 |

```
Employee_ID,Full_Name,Department,Job_Title,Hire_Date,Performance_Rating,
Experience_Years,Status,Work_Mode,Salary,Year,Country,City,Age,Job_Level
```

Two blocks is the fact that drives §5. It means the default split count is 2,
therefore 2 map tasks, therefore three of the five NodeManagers have no map work
to do.

### What was actually wrong with it

From `tools/profile_dataset.py` over the full file, counted independently per
defect:

| Defect | Rows | Note |
|---|---|---|
| Names containing a comma inside quotes | 436 | naive `split(",")` corrupts these |
| Blank / placeholder `Performance_Rating` | 3,333 | |
| `Salary` ≤ 0 | 3,333 | |
| `Experience_Years` < 0 | 2,421 | |
| `Age` < `Experience_Years` + 21 | 48,356 | |
| Malformed field count | 0 | |
| `Employee_ID` not matching `EMP` + 7 digits | 0 | |
| Unparseable `Hire_Date` | 0 | |
| Duplicate `Employee_ID` | 0 | |

Three of those deserve comment rather than just a row in a table.

**Zero duplicates.** The reducer's de-duplication is a genuine no-op on this
file. It is exercised only by `data/hr_corrupt_test.csv`. Saying the job
"removed duplicates" here would be false; what it did was confirm there were
none, which is a different and less impressive claim.

**Zero malformed rows and zero bad dates.** Two of the seven rules never fire on
this input. They still belong in the ladder — a pipeline that only works on
clean input is not a sanitizer — but they were validated on the corrupt test file,
not here.

**The logical rule's threshold came from the data.** The exact minimum of
`Age − Experience_Years` in the *clean* variant of the dataset is 21.0, with
116,597 rows sitting exactly at 21. That is what makes `Age >= Exp + 21` the
right rule. Moving it one step to `Exp + 22` would reject 164,794 rows instead of
48,356 — more than three times as many — and would be rejecting valid records.
This is why `profile_dataset.py` prints the whole margin histogram: the threshold
is a measurement, not a preference.

---

## 2. The cluster, at job submission

| | |
|---|---|
| Nodes | 5 |
| NodeManagers | 5 of 5 `RUNNING` |
| Cluster memory | 10,240 MB (5 × 2,048) |
| Cluster vcores | 10 (5 × 2) |
| HDFS configured capacity | 4.92 TB |
| HDFS available | 4.64 TB |
| Scheduler | Capacity Scheduler |
| Replication | 3 |

Labelled *at job submission* deliberately. A cluster of laptops does not hold
5/5 — machines get closed and Wi-Fi drops. A later check showed 4 active and 2
shutdown entries, one of which was a stale registration from before the hostname
fix in [TROUBLESHOOTING.md](TROUBLESHOOTING.md) #1. That is drift, not a
contradiction, and it is why the figure is timestamped rather than stated as a
standing property.

---

## 3. Cluster validation — WordCount

Run before the sanitizer, from a worker node rather than the master.

| | |
|---|---|
| Application | `application_1787582423680_0001` |
| Job name | `word count` |
| State | `FINISHED` / `SUCCEEDED` |
| Elapsed | 32 s |
| AM Host | a **worker** node |
| Input | `/validation/input/wc_input.txt` |
| Output | `/validation/output/{_SUCCESS, part-r-00000}` |
| Distinct words | 14 |

Sample of the output: `container 5`, `hadoop 4`, `yarn 4`.

The word counts are not the point. Two things are:

**The full path executed.** HDFS write → RM submit → container allocation →
map → shuffle across the mesh → reduce → HDFS write → commit. Every moving part,
in order.

**The ApplicationMaster ran on a worker.** That is the moment the cluster is
demonstrably distributed rather than quietly single-node. It is also the
milestone that proved [TROUBLESHOOTING.md](TROUBLESHOOTING.md) #1 was actually
fixed — before the fix, no container was ever allocated anywhere.

This is one job with 14 distinct words. It is a gate, not a result.

---

## 4. The full run — 2,000,000 rows, default splits

`application_1787582423680_0006` · `SUCCEEDED`

Evidence: `output/screenshots/report_full.png` and
`output/screenshots/yarn_status_0006.png`.

### Records

| | Count | Share |
|---|---|---|
| Read | 2,000,000 | 100% |
| **Valid** | **1,942,704** | **97.14%** |
| Invalid | 57,296 | 2.86% |

### Rejections by rule, first failure wins

| Rule | Rejected |
|---|---|
| 7 · logical (`Age < Exp + 21`) | 48,209 |
| 6 · numeric (range / integer-ness) | 5,754 |
| 2 · blank or placeholder | 3,333 |
| 1 · structural (field count) | 0 |
| 3 · identifier format | 0 |
| 4 · date format | 0 |
| 5 · categorical value | 0 |
| **Total** | **57,296** |

That total equals the invalid count exactly. It only can because the rules stop
at the first failure — each rejected row is charged to exactly one counter.

**The 48,356 vs 48,209 gap is the proof that the ladder works as described.**
Profiling found 48,356 rows violating the logical rule. The job charged 48,209 to
it. The difference of 147 rows failed an *earlier* rule — a blank or a bad number
— and were already gone before rule 7 ran. Two numbers that disagree by exactly
the amount the design predicts.

### Other counters

| | |
|---|---|
| Quote-rescued rows | 436 |
| Duplicate `Employee_ID` dropped | 0 |
| Final unique records | 1,942,704 |
| Output parts | 2 (= reducer count) |
| Integrity checks | 3 / 3 PASS |

### Job metrics

| | |
|---|---|
| Splits / map tasks | 2 |
| Reduce tasks | 2 |
| Launched map tasks | 3 (1 killed — speculative execution) |
| Launched reduce tasks | 2 |
| Rack-local map tasks | 3 |
| HDFS bytes read | 256,166,175 |
| HDFS bytes written | 243,036,804 |
| Total map time | 798,151 ms |
| Total reduce time | 820,435 ms |
| **Total CPU time** | **53,660 ms** |
| Container occupancy | 3,457,336 MB-s · 2,500 vcore-s |
| Peak map RSS | 494,813,184 bytes |
| Peak reduce RSS | 326,369,280 bytes |
| Driver wall clock | 875.5 s |
| YARN elapsed | 866.5 s |

**On "launched 3, killed 1" with 2 splits.** That is speculative execution, not a
failure. YARN noticed one attempt running slowly, started a duplicate, and killed
whichever lost the race. On borrowed laptops with uneven load this is exactly the
mechanism working as intended, and it is worth naming — otherwise a reader sees a
killed task and assumes something broke.

### Where the containers actually ran

| Container | Node | Role |
|---|---|---|
| `..._01_000001` | worker4 | ApplicationMaster |
| `..._01_000002` | worker1 | map / reduce |
| `..._01_000003` | worker2 | map / reduce |

Three containers on three different machines, and the ApplicationMaster on a
worker rather than the master. Distribution, demonstrated rather than asserted.

Also: **two of the five nodes ran no container in this job.** Two splits cannot
occupy five nodes. That is arithmetic, not a misconfiguration, and §5 is what to
do about it.

---

## 5. The 4-split run — same data, same jar, one property changed

`application_1787582423680_0007` · `SUCCEEDED`

Evidence: `output/screenshots/yarn_status_0007.png`.

Only change:

```
-D mapreduce.input.fileinputformat.split.maxsize=67108864
```

`split.maxsize` caps a split below the block size, so one 128 MiB block yields
more than one map task. It cannot make splits larger than a block.

### Identical where it must be

| | Run 0006 | Run 0007 |
|---|---|---|
| Records read | 2,000,000 | 2,000,000 |
| Valid | 1,942,704 | 1,942,704 |
| Invalid | 57,296 | 57,296 |
| Per-rule breakdown | 48,209 / 5,754 / 3,333 | 48,209 / 5,754 / 3,333 |
| Quote-rescued | 436 | 436 |
| **HDFS bytes written** | **243,036,804** | **243,036,804** |

Byte-for-byte identical output. That is the strongest available evidence that the
two runs are genuinely comparable and that split count does not affect
correctness.

### Different where you would expect

| | Run 0006 (2 splits) | Run 0007 (4 splits) |
|---|---|---|
| Splits / map tasks | 2 | 4 |
| Launched maps (killed) | 3 (1) | 5 (2) |
| Total map time | 798,151 ms | 90,410 ms |
| Total reduce time | 820,435 ms | 149,841 ms |
| **Total CPU time** | **53,660 ms** | **57,570 ms** |
| Container occupancy | 3,457,336 MB-s · 2,500 vcore-s | 492,104 MB-s · 363 vcore-s |
| Driver wall clock | 875.5 s | 112.9 s |
| YARN elapsed | 866.5 s | 103.7 s |
| AM host | worker4 | worker2 |

### Reading this honestly

**CPU time barely moved: 53,660 ms → 57,570 ms, about 7% apart.** This is the
result that matters, and it is the correct one. Splitting a job into more pieces
does not reduce the work; it redistributes it. The small increase is
coordination overhead — more tasks, more setup, more speculative attempts.

**The wall clock ratio is 7.8×, and it is not a speedup.** These are shared
laptops. During run 0006 they were doing other things; during 0007 they were
quieter. Container occupancy dropped from 3,457,336 MB-s to 492,104 MB-s, which
is the honest efficiency signal — but even that is entangled with how long tasks
sat waiting on a contended host.

Claiming a 7.8× performance improvement from a configuration change would be the
easy version of this section and it would be wrong. The defensible claims are
smaller and actually supported:

1. Split count controls map-task count and therefore how many nodes participate.
2. Total CPU time is invariant to split count, within measurement noise, so the
   same work was done.
3. Byte-identical output proves correctness is unaffected.
4. Wall clock on shared hardware measures the hardware's other commitments as
   much as the job's.

A benchmark would need dedicated machines, repeated trials, and reported
variance. This was not that, and it is better to say so than to publish a number
that will not survive a question.

---

## 6. The correctness run — 39 hand-built rows

`application_1787582423680_0005` · `SUCCEEDED`

Evidence: `output/screenshots/report_corrupt.png`,
`output/screenshots/job_phases_0005.png`.
Full output: `output/corrupt-run-output-full.csv`.

`data/hr_corrupt_test.csv` exists because the real dataset never triggers four of
the seven rules. A rule you cannot demonstrate is a rule you cannot claim.

| | Count | Share |
|---|---|---|
| Read | 39 | 100% |
| Valid | 9 | 23.08% |
| Invalid | 30 | 76.92% |

### Every rule fires

| Rule | Rejected |
|---|---|
| 1 · structural | 4 |
| 2 · blank / placeholder | 5 |
| 3 · identifier format | 3 |
| 4 · date format | 5 |
| 5 · categorical | 5 |
| 6 · numeric | 6 |
| 7 · logical | 2 |
| **Total** | **30** |

| | |
|---|---|
| Quote-rescued | 1 |
| Duplicates dropped | 3 |
| Final unique records | 6 |
| Wall clock | 30.2 s |
| YARN elapsed | 23.5 s |

Nine valid rows became six unique — the only run in this package where the
reducer's de-duplication does anything at all.

What the rows were aimed at: short and 3-field records; `N/A` and `null` blanks;
`EMP99` and `XYZ0000110` identifiers; `2019-13-01`; a `Telepathic` work mode; a
negative salary; and rows on both sides of the `Age >= Exp + 21` boundary,
including one exactly on it that must pass.

`job_phases_0005.png` shows `map 0 → 100%` completing before `reduce 0 → 100%`
begins, and exit code 0. The ordering is not incidental — the shuffle cannot
start until every mapper's output exists.

---

## 7. Local-mode corroboration

`output/local-run-50k-console.log`

HDFS was down when this package was assembled and a reboot had cleared `/tmp`,
taking the original cluster console logs with it. Rather than paste figures into
a fabricated log, the same jar was run in local mode over a real 50,000-row slice
of the same input:

```bash
hadoop jar data-sanitizer.jar com.bda.sanitizer.DataSanitizerDriver -D fs.defaultFS=file:/// -D mapreduce.framework.name=local <in> <out>
```

| | Count | Share |
|---|---|---|
| Read | 50,000 | 100% |
| Valid | 48,590 | **97.18%** |
| Invalid | 1,410 | 2.82% |

| Rule | Rejected |
|---|---|
| 7 · logical | 1,171 |
| 6 · numeric | 142 |
| 2 · blank | 97 |
| **Total** | **1,410** |

| | |
|---|---|
| Quote-rescued | 8 |
| Duplicates | 0 |
| Bytes read / written | 6,400,816 / 6,123,360 |
| Wall clock | 5.3 s |
| Integrity checks | 3 / 3 PASS |

**97.18% here against 97.14% on the full 2,000,000 rows** — a 0.04 point
difference across a 40× change in sample size. That is what you want from a
sample: it corroborates the cluster figure using an entirely separate execution
path.

It is corroboration, not a substitute. One JVM, one map task, no network shuffle,
no YARN metrics. It says the rules are right. It says nothing at all about
cluster performance, and no timing from it appears anywhere in this document's
performance discussion.

---

## 8. What this project demonstrates, and what it does not

### Does

- Five heterogeneous laptops, on five networks, behind five routers, running one
  functioning Hadoop cluster over a mesh VPN.
- A real distributed failure diagnosed to its root cause — a container token that
  could not be minted because a NodeManager advertised a name only it could
  resolve — and fixed with one property rather than a reinstall.
- 2,000,000 records processed end to end, `SUCCEEDED`, with three integrity
  checks reconciling and containers demonstrably on three different machines.
- Validation rules derived from measured defects rather than guessed, with the
  logical rule's threshold read off a margin histogram.
- Quote-aware CSV parsing, verified against 436 rows that a naive split would
  have destroyed.
- The same conclusion reached twice by different means: 97.14% valid on the
  cluster, 97.18% in local mode.

### Does not

- **Not a benchmark.** No dedicated hardware, no repeated trials, no variance.
  The wall-clock difference between runs 0006 and 0007 is host contention.
- **Not full cluster utilisation.** A 244 MiB input at a 128 MiB block size gives
  2 splits. Two nodes ran no container in the default run. Forcing 4 splits
  helps; it does not make the input bigger.
- **Not a test of de-duplication at scale.** Zero duplicates in 2,000,000 rows.
  That path is exercised only by the 39-row test file.
- **Not a test of every rule at scale.** Four of seven rules never fired on the
  real input. They were validated on the corrupt file.
- **Not high availability.** Single NameNode, single ResourceManager. Either one
  going down takes the cluster with it.
- **Not secure.** No Kerberos. Access control is HDFS POSIX permissions on a
  private mesh, which is appropriate for this and would not be for anything real.

---

## 9. Provenance of every figure here

| Figure group | Source |
|---|---|
| Input size, row count, columns | `stat`, `wc -l`, header read |
| Per-defect profile of the raw file | `tools/profile_dataset.py` |
| Clean-variant margin minimum, 116,597 at exactly 21 | `tools/profile_dataset.py` on the clean variant |
| Runs 0005 / 0006 / 0007 record and rule counts | MapReduce counters, via the driver report — `output/screenshots/report_*.png` |
| Job metrics, timings, container occupancy | `yarn application -status` + job counters — `output/screenshots/yarn_status_*.png` |
| Container placement and AM hosts | `yarn application -status` and RM application list |
| Cluster capacity and node states | `hdfs dfsadmin -report`, `yarn node -list`, RM REST `/ws/v1/cluster/metrics` |
| WordCount validation | `yarn application -status`, `hdfs dfs -cat` on the part file |
| Local-mode run | `output/local-run-50k-console.log`, produced by `scripts/run-local.sh` |

Not included because it was not recoverable: the original cluster console logs
for runs 0005–0007. `/tmp` was cleared by a reboot. The figures survive in the
screenshots, which is why the screenshots are unedited — see
[ANONYMIZATION.md](ANONYMIZATION.md).
