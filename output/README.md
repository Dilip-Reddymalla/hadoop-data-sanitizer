# output/ — what each artefact is, and how it was produced

Nothing here was hand-written or edited. Each file is the direct output of a run
you can reproduce with the scripts in this package.

Read [../docs/RESULTS.md](../docs/RESULTS.md) for the analysis. This file is just
provenance.

---

## The console logs and CSVs

### `local-run-50k-console.log`

The complete console output of this exact jar over a real 50,000-row slice of the
2,000,000-row input, in local mode:

```bash
./scripts/run-local.sh <50k-slice.csv> /tmp/out
```

48,590 valid (97.18%), 1,410 invalid (2.82%), all three integrity checks PASS.
The full cluster run over all 2,000,000 rows gave 97.14%.

**Why a slice and not the full cluster output.** HDFS was down when this package
was assembled, and a reboot had cleared `/tmp` along with the original cluster
console logs. The choice was between shipping a genuine local-mode run of the
same code, or pasting remembered numbers into a fabricated log. This is the first
option.

It is corroboration, not a substitute. One JVM, one map task, no network shuffle,
no YARN metrics. It demonstrates the rules are correct. No timing from it is used
in any performance claim.

### `local-run-corrupt-console.log`

The same, over `../data/hr_corrupt_test.csv` — 39 hand-built rows aimed at each
of the seven rules in turn:

```bash
./scripts/run-local.sh data/hr_corrupt_test.csv /tmp/out-corrupt
```

39 read, 9 valid, 30 invalid, per-rule 4/5/3/5/5/6/2, 1 quote-rescued, 3
duplicates dropped, 6 unique. Every rule fires. This matches the cluster run
`application_1787582423680_0005` exactly — same jar, same rules, different
execution engine.

### `corrupt-run-output-full.csv`

The complete output of that run. Six rows, so you can read every one and check
the survivors by eye against the input. Note `"Nuwenhuysen, van der"` on row 2 —
that is the quote-aware parser working; a naive split would have rejected it as
malformed.

### `sample-clean-output-head-200.csv`

The first 200 rows of a real `part-r-00000`. Shows the output shape: 15 columns,
the same schema as the input, values normalised — dates to `yyyy-MM-dd`,
categories to canonical case, the trailing `.0` stripped from salaries. There is
no extra status column; invalid records are dropped in the mapper, so presence in
the file is the flag.

Truncated to 200 rows because the full part file is not the interesting part and
the archive should not be 243 MB.

### `_SUCCESS`

The zero-byte commit marker MapReduce writes when a job's output has been
committed. Included because its **absence** is meaningful: part files without a
`_SUCCESS` marker mean the output is partial.

---

## `screenshots/` — the cluster evidence

Five unedited captures from the actual cluster runs. These are the primary
evidence for every cluster figure in [../docs/RESULTS.md](../docs/RESULTS.md).

| File | Run | Shows |
|---|---|---|
| `report_full.png` | `_0006` | driver report, 2,000,000 rows — counts, per-rule breakdown, 3/3 checks PASS |
| `yarn_status_0006.png` | `_0006` | `yarn application -status` plus job counters |
| `yarn_status_0007.png` | `_0007` | the 4-split run — same jar, same input, only `split.maxsize` changed |
| `report_corrupt.png` | `_0005` | driver report, 39-row corrupt test — every rule firing |
| `job_phases_0005.png` | `_0005` | `map 0→100%` then `reduce 0→100%`, exit code 0 |

`job_phases_0005.png` is worth a second look for the ordering: the map phase
completes before the reduce phase starts. That is not incidental — the shuffle
cannot begin until every mapper's output exists.

> **These five images are not anonymised.** They still show the real mesh IPs,
> hostnames and container addresses, while every text file in this package uses
> placeholders. That is a deliberate trade — a redacted terminal capture is a
> claim about evidence rather than evidence. See
> [../docs/ANONYMIZATION.md](../docs/ANONYMIZATION.md) for exactly what is
> exposed, why, and three ways to clean it if you would rather.

---

## Reproducing any of this

```bash
./scripts/build.sh
```

```bash
./scripts/run-local.sh data/hr_corrupt_test.csv /tmp/out-corrupt
```

```bash
./scripts/verify-output.sh /tmp/out-corrupt --local
```

The corrupt-file run takes a few seconds and needs no cluster. It reproduces
`local-run-corrupt-console.log` and `corrupt-run-output-full.csv` exactly.

For the cluster runs, see §15 and §17 of [../README.md](../README.md).
