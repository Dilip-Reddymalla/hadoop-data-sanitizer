# Anonymisation — what was replaced, and what was not

Everything written as text in this package uses placeholder addresses, hostnames
and usernames. The five PNG screenshots do not. This document says exactly where
the line is, so you can decide whether to redraw it before sharing the archive.

---

## The substitution table

| Placeholder used here | What it stands for |
|---|---|
| `100.64.0.10` | the master's mesh IP |
| `100.64.0.11` – `100.64.0.14` | workers 1 – 4 |
| `master`, `worker1` … `worker4` | machine hostnames |
| `your-tailnet.ts.net` | the tailnet DNS suffix |
| `<node-user>`, `<node1-user>`, `<other-user>` | Linux usernames — different on every machine |
| `MACHINE.localdomain` | the unresolvable `hostname -f` value in TROUBLESHOOTING #1 |
| `application_XXXXXXXXXXXXX_XXXX` | an application ID, where the specific run does not matter |

`100.64.0.0/10` is the Carrier-Grade NAT range, which is the range mesh VPNs
allocate from. The placeholders therefore look like plausible mesh addresses
while belonging to no actual cluster.

## What was left real, and why

**Application IDs in [RESULTS.md](RESULTS.md)** — `application_1787582423680_0005`
through `_0007`, and `_0001` for WordCount. These are internal YARN identifiers.
They contain a ResourceManager start timestamp and a counter, and no address,
hostname or user. They are kept because they are how a figure in RESULTS.md is
tied to a specific screenshot, and severing that link would make the numbers
unverifiable.

**Paths** — `/opt/hadoop`, `/data/hadoop/...`, `/data-sanitizer/hr/...`. Genuinely
these on every node, and not sensitive.

**Software versions** — Ubuntu 24.04.4, OpenJDK 17.0.19, Hadoop 3.4.2,
Python 3.12.3, Tailscale 1.102.2. Needed for reproducibility.

**Measured numbers** — all of them. Record counts, counters, timings, byte counts.
Anonymising a result would defeat the point of publishing it.

---

## The exception: `output/screenshots/`

These five files are **unedited captures of real terminal sessions**:

| File | Shows |
|---|---|
| `report_full.png` | driver report, 2,000,000-row run |
| `yarn_status_0006.png` | `yarn application -status` + counters, run 0006 |
| `yarn_status_0007.png` | `yarn application -status`, the 4-split run |
| `report_corrupt.png` | driver report, 39-row corrupt test |
| `job_phases_0005.png` | `map 0→100%` then `reduce 0→100%`, exit code 0 |

They contain, depending on the image:

- real mesh IP addresses in the `100.x.y.z` range
- real machine hostnames, including one Linux username visible as a hostname
- real tailnet DNS names of the form `HOST.TAILNET.ts.net`
- real container IDs and node addresses with their RPC ports

### Why they were not cropped or blurred

An unedited terminal capture is evidence. A redacted one is a claim about
evidence. The whole point of including them is that a reader can check the
numbers in [RESULTS.md](RESULTS.md) against something that was not typed by the
person making the claim — and blurred regions invite exactly the question the
screenshots exist to close.

There is also a practical limit: the container IDs, node addresses and tailnet
names are woven through the counter output. Redacting them thoroughly enough to
be worth doing would leave very little readable.

### What the exposure actually is

A mesh IP is only reachable from inside that tailnet. Someone outside it cannot
route to `100.x.y.z` at all — these are not internet-routable addresses. The
practical disclosure is: five machine names, one username visible as a hostname,
and a tailnet name.

That is a real disclosure, and it is small. But it is a judgement call, not a
non-issue, and it is yours rather than ours.

### If you would rather they were clean

Three options, in increasing order of effort and decreasing order of evidential
value:

1. **Crop tightly.** In most of the five, the identifying strings sit in the
   header lines and the container table. The record counts and rule breakdowns
   can often be cropped away from them.
2. **Re-run and re-capture** on a cluster whose nodes you have named
   generically. The scripts in this package reproduce every run.
3. **Delete `output/screenshots/` entirely** and rely on
   `output/local-run-*.log`, which are genuine, complete, and contain no
   identifiers at all — they were produced in local mode and were checked. The
   cost is that the cluster figures in RESULTS.md then have no attached primary
   evidence.

Option 3 is the only one that is genuinely zero-effort, and it is the one that
costs the most.

---

## How the text was checked

Every text file in this archive — Java, XML, shell, Python, Markdown, CSV, logs —
was scanned for the real IP octets, the real tailnet name, the real hostnames and
the five real usernames. The check is worth re-running after any edit:

```bash
grep -rInE '100\.(6[0-9]|7[0-9]|8[0-9]|12[0-9])\.[0-9]{1,3}\.[0-9]{1,3}' . --include='*.md' --include='*.xml' --include='*.sh' --include='*.py' --include='*.java' --include='*.log' --include='*.csv' --include='*.snippet' --include='workers'
```

Every hit should be a `100.64.0.1x` placeholder. Anything else is a leak.

Note the deliberate exception in [RESULTS.md](RESULTS.md): the application IDs.
They match no address pattern, and §"What was left real" above explains why they
stayed.
