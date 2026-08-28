# Data Sanitizer — a five-node Hadoop cluster built from laptops

A MapReduce job that filters and normalises a 2-million-row HR employee export,
plus everything needed to rebuild the cluster it ran on from a blank Windows
machine.

This README is the long version. It starts at "I have a Windows laptop and
nothing else installed" and ends at "the job finished and here is how I proved
the output is correct". Every configuration value is explained rather than just
listed, because on a hand-built cluster the values that matter are the ones you
would never guess.

---

## Table of contents

| # | Section |
|---|---|
| 0 | [What this actually does](#0-what-this-actually-does) |
| 1 | [What is in this package](#1-what-is-in-this-package) |
| 2 | [The cluster](#2-the-cluster) |
| 3 | [A note on the addresses in this document](#3-a-note-on-the-addresses-in-this-document) |
| 4 | [Install Ubuntu on WSL2](#4-install-ubuntu-on-wsl2) |
| 5 | [Install Java](#5-install-java) |
| 6 | [Install Python](#6-install-python) |
| 7 | [Install Tailscale — the network layer](#7-install-tailscale--the-network-layer) |
| 8 | [Set up SSH — the administration layer](#8-set-up-ssh--the-administration-layer) |
| 9 | [Install Hadoop](#9-install-hadoop) |
| 10 | [Configure Hadoop — every file, explained](#10-configure-hadoop--every-file-explained) |
| 11 | [Format the NameNode and start the cluster](#11-format-the-namenode-and-start-the-cluster) |
| 12 | [Validate with WordCount before anything else](#12-validate-with-wordcount-before-anything-else) |
| 13 | [Get the dataset and profile it](#13-get-the-dataset-and-profile-it) |
| 14 | [Build the jar](#14-build-the-jar) |
| 15 | [Run the sanitizer](#15-run-the-sanitizer) |
| 16 | [Verify the output](#16-verify-the-output) |
| 17 | [The split-size experiment](#17-the-split-size-experiment) |
| 18 | [Web UIs, and which port is which](#18-web-uis-and-which-port-is-which) |
| 19 | [Shutting down and starting up again](#19-shutting-down-and-starting-up-again) |
| 20 | [Things never to do](#20-things-never-to-do) |
| 21 | [Where the numbers are](#21-where-the-numbers-are) |

---

## 0. What this actually does

The input is an HR employee export: 2,000,000 rows, 15 columns, 244.3 MiB of
CSV. It is a realistic export in the sense that it is mostly fine and
occasionally not — blanks written as `N/A`, salaries of zero, negative years of
experience, employees whose careers are longer than their lives, and names like
`"Nuwenhuysen, van der"` that contain a comma inside a quoted field.

The job applies seven validation rules in a fixed order, drops any record that
fails one, normalises the survivors, and de-duplicates on `Employee_ID`.

```
                    ┌──────────────────────── the seven rules, in order ────┐
input CSV ─► map ─► │ 1 structural  field count after quote-aware parsing   │
                    │ 2 blanks      empty or a placeholder token            │
                    │ 3 identifier  EMP + exactly 7 digits                  │
                    │ 4 date        one of four accepted formats,           │
                    │               and the year must agree with Year       │
                    │ 5 category    5 columns against closed value sets     │
                    │ 6 numeric     4 columns, range + integer-ness         │
                    │ 7 logic       Age >= Experience_Years + 21            │
                    └──────────────────────────────────────────────────────┘
                             │ survivors, keyed by Employee_ID
                             ▼
                          shuffle
                             │
                             ▼
                       reduce ─► keep the first record per id ─► output CSV
```

Two design decisions are worth stating up front because they are the ones a
reviewer will question.

**The rules stop at the first failure.** A row with a blank department *and* a
negative salary is charged to the blank counter only. That is what makes the
seven reason counters sum exactly to the invalid total, which in turn is what
makes the driver's integrity check meaningful. Counting every defect on every
row would be more informative but would no longer reconcile, and a number that
does not reconcile is a number nobody has to believe.

**There is no combiner.** A combiner looks free here and is not. The reducer
keeps the first record per key and counts the rest as dropped duplicates; run it
as a combiner on map output and then again on the reduce side and the duplicate
counter double-counts. The operation is not associative, so it does not get a
combiner.

---

## 1. What is in this package

```
hadoop-data-sanitizer/
├── README.md                      this file
├── src/com/bda/sanitizer/         the MapReduce job, 717 lines of Java
│   ├── RecordSchema.java          columns, allowed values, ranges, formats
│   ├── DataSanitizerMapper.java   the rule ladder, the CSV parser, 12 counters
│   ├── DataSanitizerReducer.java  de-duplication on Employee_ID
│   └── DataSanitizerDriver.java   job wiring, the report, 3 integrity checks
├── data-sanitizer.jar             prebuilt, so you can run before you compile
├── conf/                          anonymised configuration templates
│   ├── core-site.xml              NameNode address
│   ├── hdfs-site.xml              storage dirs, replication, block size
│   ├── yarn-site.xml              THE ONLY FILE THAT DIFFERS PER NODE
│   ├── mapred-site.xml            framework + the three env properties
│   ├── workers                    master only
│   ├── hadoop-env.sh.snippet      JAVA_HOME for non-interactive shells
│   ├── bashrc.snippet             the HADOOP_* environment
│   ├── hosts.snippet              static name resolution
│   └── wsl.conf                   stop WSL rewriting /etc/hosts on boot
├── scripts/
│   ├── build.sh                   javac + jar, no Maven
│   ├── ssh-setup.sh               master → every node, passwordless
│   ├── cluster-health.sh          "is the cluster actually up?"
│   ├── validate-wordcount.sh      the gate you pass before your own job
│   ├── run-cluster.sh             stage input, submit, verify
│   ├── run-local.sh               same jar, no HDFS, no YARN
│   └── verify-output.sh           re-derive the checks from the output bytes
├── tools/
│   └── profile_dataset.py         find the defects before writing the rules
├── data/
│   └── hr_corrupt_test.csv        39 hand-built rows, one per rule
├── output/                        real artefacts from real runs
│   ├── local-run-50k-console.log
│   ├── local-run-corrupt-console.log
│   ├── corrupt-run-output-full.csv
│   ├── sample-clean-output-head-200.csv
│   ├── _SUCCESS
│   └── screenshots/               five captures from the cluster runs
└── docs/
    ├── RESULTS.md                 every measured number, and what it means
    ├── TROUBLESHOOTING.md         the three real bugs, root cause and fix
    └── ANONYMIZATION.md           what was replaced, and what was not
```

**Not included, deliberately:**

- **The 244 MiB input dataset.** Downloadable; see §13. Shipping it would make
  this archive 130 MB of somebody else's data.
- **The full 243 MB cluster output.** See the honesty note at the end of §16.
- **The presentation build code.** Excluded on request.
- **`build/classes/`.** Regenerated by `scripts/build.sh` in seconds.

---

## 2. The cluster

Five Windows laptops, each running Ubuntu under WSL2, joined into one private
network by a mesh VPN. No cloud, no switch, no static IPs, no two machines
necessarily in the same building.

```
                    ┌───────────────────────────────────────────┐
                    │            mesh VPN overlay               │
                    │   every node gets one stable 100.x IP     │
                    │   traffic is WireGuard-encrypted,         │
                    │   direct peer-to-peer where possible      │
                    └───────────────────────────────────────────┘
                                      │
     ┌───────────────┬────────────────┼────────────────┬───────────────┐
     │               │                │                │               │
┌────▼─────┐   ┌─────▼────┐    ┌──────▼───┐     ┌──────▼───┐   ┌───────▼──┐
│  master  │   │ worker1  │    │ worker2  │     │ worker3  │   │ worker4  │
│ .0.10    │   │ .0.11    │    │ .0.12    │     │ .0.13    │   │ .0.14    │
├──────────┤   ├──────────┤    ├──────────┤     ├──────────┤   ├──────────┤
│ NameNode │   │ DataNode │    │ DataNode │     │ DataNode │   │ DataNode │
│ Resource │   │ Node     │    │ Node     │     │ Node     │   │ Node     │
│ Manager  │   │ Manager  │    │ Manager  │     │ Manager  │   │ Manager  │
│ DataNode │   └──────────┘    └──────────┘     └──────────┘   └──────────┘
│ NodeMgr  │
└──────────┘
```

The master runs worker daemons too. It has storage and cores like everything
else and there is no reason to waste them on a five-node cluster.

**Two layers, and they are not the same layer.** This distinction matters enough
to be worth its own diagram, because getting it backwards is the single most
common error in write-ups of clusters like this one.

```
  mesh VPN  ── the DATA layer ────────────────────────────────────────
              carries HDFS block transfer, NameNode and DataNode RPC,
              ResourceManager and NodeManager RPC, and the MapReduce
              shuffle. Every byte the cluster moves, moves here.
              Ports 9000, 9866, 8030-8033, the shuffle port.

  SSH       ── the ADMIN layer ───────────────────────────────────────
              start-dfs.sh and start-yarn.sh log into each host in
              conf/workers and launch a daemon. You log in to read a
              log or restart something. That is all.
              Port 22.
```

Stop `sshd` after the daemons are running and a job in flight will not notice.
SSH is not Hadoop's data transport, and any diagram that shows MapReduce data
flowing over SSH is wrong.

**Software versions this was built and measured on:**

| | |
|---|---|
| Windows host | Windows 10/11 with WSL2 |
| Linux | Ubuntu 24.04.4 LTS |
| WSL2 kernel | 6.18.x-microsoft-standard-WSL2 |
| Java | OpenJDK 17.0.19 |
| Python | 3.12.3 |
| Hadoop | 3.4.2 |
| Mesh VPN | Tailscale 1.102.2 |

Hadoop 3.4.x needs Java 8 or 11 or 17. Java 21 is **not** supported by 3.4.2 and
will fail in ways that look like your code's fault. Use 17.

---

## 3. A note on the addresses in this document

Every IP, hostname, tailnet name and username in this package is a placeholder.

| Placeholder | Meaning |
|---|---|
| `100.64.0.10` | the master's mesh IP |
| `100.64.0.11` … `100.64.0.14` | workers 1–4 |
| `master`, `worker1` … `worker4` | hostnames |
| `your-tailnet.ts.net` | your tailnet's DNS suffix |
| `<node-user>` | the Linux username on that node — different on each machine |
| `/opt/hadoop` | `HADOOP_HOME`, genuinely this on every node |
| `/data/hadoop/...` | HDFS storage root, genuinely this on every node |

`100.64.0.0/10` is the Carrier-Grade NAT range, which is what mesh VPNs
allocate from, so these look like real mesh addresses and are not any specific
cluster's. Substitute your own throughout.

**One exception you must know about.** The five PNGs in `output/screenshots/`
are unedited captures of real terminal sessions. They still show the real
hostnames, mesh IPs and application IDs. Everything written in text has been
anonymised; the evidence images have not, because cropping them would weaken
them as evidence. See [docs/ANONYMIZATION.md](docs/ANONYMIZATION.md) and decide
for yourself whether that matters for your audience.

---

## 4. Install Ubuntu on WSL2

Do this on all five machines. Nothing about this step is cluster-specific.

### 4.1 From Windows

Open **PowerShell as Administrator** and run:

```powershell
wsl --install -d Ubuntu-24.04
```

On a current Windows build that single command enables the Virtual Machine
Platform and WSL features, downloads the WSL2 kernel, installs Ubuntu 24.04, and
prompts you to reboot. After the reboot, Ubuntu launches and asks for a UNIX
username and password. That password is your `sudo` password inside WSL; it has
nothing to do with your Windows password.

If `wsl --install` is not recognised, the machine is on an older build. Enable
the two features by hand, reboot, then install the kernel update package from
Microsoft and set the default version:

```powershell
dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart
dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart
```

```powershell
wsl --set-default-version 2
```

Confirm you are on version 2, not 1. Version 1 does not have a real kernel and
Hadoop's networking will behave strangely:

```powershell
wsl -l -v
```

You want `VERSION 2` next to `Ubuntu-24.04`.

### 4.2 Give WSL enough memory

WSL2 defaults to claiming up to half the host RAM, and it does not release it
back readily. Create `C:\Users\<you>\.wslconfig`:

```ini
[wsl2]
memory=6GB
processors=4
swap=2GB
localhostForwarding=true
```

Six gigabytes is comfortable for a NodeManager configured with 2 GB of container
memory plus a DataNode plus the desktop the machine still has to run. Apply it
with a full shutdown from PowerShell — a restart of the Ubuntu window is not
enough:

```powershell
wsl --shutdown
```

### 4.3 Inside Ubuntu

```bash
sudo apt update && sudo apt upgrade -y
```

```bash
sudo apt install -y curl wget vim net-tools iputils-ping dnsutils unzip zip htop
```

`net-tools` and `iputils-ping` are not installed by default on 24.04 and you
will want `netstat` and `ping` within the first ten minutes of debugging.

### 4.4 Stop WSL rewriting your network files

This step will save you a genuinely confusing hour. WSL regenerates
`/etc/hosts` and `/etc/resolv.conf` on **every boot**, discarding anything you
put there. Copy `conf/wsl.conf` to `/etc/wsl.conf`:

```bash
sudo cp conf/wsl.conf /etc/wsl.conf
```

It sets `generateHosts=false` and `generateResolvConf=false` so your static
cluster entries survive, and `systemd=true` so you can actually run services.
Then, from PowerShell:

```powershell
wsl --shutdown
```

### 4.5 Where to put files

Work inside the Linux filesystem — `/home/<node-user>` and `/opt` and `/data`.
Do **not** put HDFS storage, the dataset, or `HADOOP_HOME` under `/mnt/c/`. That
path is a 9p filesystem bridge to Windows: it is roughly an order of magnitude
slower and does not give HDFS the file semantics it expects. `/mnt/c` is for
copying a finished artefact out to the Windows Downloads folder, and nothing
else.

---

## 5. Install Java

On every node:

```bash
sudo apt install -y openjdk-17-jdk
```

`openjdk-17-jre` is not enough. `scripts/build.sh` runs `javac`, which only the
JDK provides.

```bash
java -version && javac -version
```

Find the real `JAVA_HOME`. Do not guess it and do not use the `java` symlink,
which points into `/etc/alternatives`:

```bash
readlink -f "$(command -v java)" | sed 's|/bin/java||'
```

On Ubuntu 24.04 that prints `/usr/lib/jvm/java-17-openjdk-amd64`. If yours
differs, use your value everywhere `JAVA_HOME` appears below.

Now set the environment. Append `conf/bashrc.snippet` to `~/.bashrc`:

```bash
cat conf/bashrc.snippet >> ~/.bashrc && source ~/.bashrc
```

That exports `JAVA_HOME`, `HADOOP_HOME`, `HADOOP_CONF_DIR`, the four
`HADOOP_*_HOME` variables and the `PATH` additions. Hadoop 3 split those
`*_HOME` variables apart and different tools read different ones; a missing
`HADOOP_MAPRED_HOME` is a classic cause of a job that submits cleanly and then
dies.

**And set `JAVA_HOME` a second time, in `hadoop-env.sh`.** This is not
redundancy for its own sake. `~/.bashrc` is not sourced by a non-interactive SSH
session, which is exactly how `start-dfs.sh` launches daemons on other nodes.
Set it only in `.bashrc` and Hadoop works perfectly when you type commands and
fails mysteriously when the startup script does:

```bash
cat conf/hadoop-env.sh.snippet >> /opt/hadoop/etc/hadoop/hadoop-env.sh
```

(Run that after §9, when `/opt/hadoop` exists.)

Verify the non-interactive case explicitly, because this is the failure mode:

```bash
ssh localhost 'echo $JAVA_HOME'
```

Empty output means you have the bug. Non-empty means you do not.

---

## 6. Install Python

Ubuntu 24.04 ships Python 3.12.3, which is what `tools/profile_dataset.py`
needs. Add the pieces that are not preinstalled:

```bash
sudo apt install -y python3 python3-pip python3-venv
```

```bash
python3 --version
```

The profiler is deliberately standard-library only — `csv`, `re`, `collections`,
`argparse`. No pandas, no numpy, no `pip install` at all. Two reasons: a 244 MiB
file streams row by row so peak memory stays at a few megabytes plus the
duplicate-id set, and it runs on a bare WSL install without a dependency step
that can fail.

Ubuntu 24.04 enforces PEP 668, so system-wide `pip install` is blocked. If you
ever do need a package, use a virtual environment rather than
`--break-system-packages`:

```bash
python3 -m venv ~/venv && source ~/venv/bin/activate
```

Nothing in this package requires it.

---

## 7. Install Tailscale — the network layer

Five laptops on five different networks, behind five different routers, with
DHCP addresses that change. Hadoop needs every node to have one stable address
that every other node can reach. That is the entire problem the mesh solves.

A mesh VPN gives each node a permanent `100.x.y.z` address, connects peers
directly where NAT allows and relays where it does not, and encrypts everything
with WireGuard. From Hadoop's point of view it is simply a flat, private, stable
LAN.

### 7.1 Install, on every node

```bash
curl -fsSL https://tailscale.com/install.sh | sh
```

### 7.2 Join the tailnet

```bash
sudo tailscale up
```

It prints a URL. Open it in a browser, sign in with the **same account or
tailnet** on all five machines. A node on a different tailnet is on a different
network and will not be reachable, which produces a very puzzling set of
symptoms.

### 7.3 Record each node's address

```bash
tailscale ip -4
```

Do this on all five and write the results down. These five addresses go into
`core-site.xml`, `yarn-site.xml`, `workers`, and `/etc/hosts`. Getting one wrong
is the source of most of §10's failure modes.

### 7.4 Make the addresses permanent

By default a mesh address is stable but not guaranteed. In the admin console,
find each machine and **disable key expiry** — otherwise a node silently drops
off the network after a few months and you get a cluster that "used to work".

### 7.5 Verify the mesh before touching Hadoop

From the master:

```bash
tailscale status
```

Every node should be listed. `direct` means a peer-to-peer connection;
`relay` means traffic is going through a relay, which works but adds latency —
and on a shuffle-heavy job you will feel it.

```bash
ping -c 3 100.64.0.11
```

Do this for all four workers, then from one worker ping another. Full mesh
reachability, not just star-to-master: the shuffle phase has workers fetching
map output directly from each other.

### 7.6 The DNS caveat

Mesh DNS gives you names like `worker1.your-tailnet.ts.net`. Convenient, and on
at least one of our five machines it did not work at all from inside WSL2 —
`/etc/resolv.conf` held only the WSL NAT resolver, so mesh names did not
resolve.

Two consequences, and both are deliberate choices in `conf/`:

1. **Hadoop config uses raw mesh IPs, never hostnames.** An IP cannot fail to
   resolve. Given a choice between a pretty config and a cluster that starts,
   take the cluster.
2. **`/etc/hosts` gets a static block anyway**, for the times you want to type a
   name by hand:

```bash
sudo tee -a /etc/hosts < conf/hosts.snippet
```

This is also why `conf/wsl.conf` disables `generateHosts` — without that, §4.4
undoes this on the next boot.

---

## 8. Set up SSH — the administration layer

Read §2 again if you skipped it: SSH starts daemons and lets you troubleshoot.
It does not carry cluster data.

You need **master → every node** passwordless, including master → master. You do
not need worker → worker.

```bash
./scripts/ssh-setup.sh
```

Edit the `NODES` array at the top first. The script installs and starts `sshd`,
generates an ed25519 key if you do not already have one, copies it to each node,
and then proves the result by running `hostname` over SSH on each. Three details
in it are worth knowing about:

**`sshd` is not running by default in WSL2**, and without `systemd=true` in
`/etc/wsl.conf` it does not start on boot either. A worker that does not answer
on port 22 is a worker `start-dfs.sh` will hang on.

**The key has no passphrase.** `start-dfs.sh` is non-interactive and cannot type
one. The key's only capability is starting daemons inside a private mesh; that is
a defensible trade, but make it knowingly.

**Include the master itself.** The startup scripts SSH to every host in
`conf/workers`, and the master is in that list because it runs a DataNode and a
NodeManager. Skip it and you get the odd symptom of all four workers starting
while the master's own worker daemons do not.

Verify:

```bash
for ip in 100.64.0.10 100.64.0.11 100.64.0.12 100.64.0.13 100.64.0.14; do printf '%s -> ' "$ip"; ssh -o BatchMode=yes -o ConnectTimeout=5 "$ip" hostname; done
```

`BatchMode=yes` fails instead of prompting, which is what you want in a test —
a password prompt here is a hang there.

If a node still prompts: the remote `~/.ssh` must be `700` and
`~/.ssh/authorized_keys` must be `600`. `sshd` ignores them silently otherwise,
with no error message that explains why.

Since each laptop belongs to a different person, the usernames differ. Put them
in `~/.ssh/config` on the master once and stop remembering them:

```
Host worker1
    HostName 100.64.0.11
    User <node1-user>
    IdentityFile ~/.ssh/id_ed25519
```

---

## 9. Install Hadoop

On every node, identically. Same version, same path — a version skew between
nodes produces protocol errors that read like corruption.

```bash
cd /tmp && wget https://dlcdn.apache.org/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz
```

Verify the download. A truncated tarball extracts far enough to look fine:

```bash
wget -q https://dlcdn.apache.org/hadoop/common/hadoop-3.4.2/hadoop-3.4.2.tar.gz.sha512 && sha512sum -c hadoop-3.4.2.tar.gz.sha512
```

Extract to `/opt/hadoop`:

```bash
sudo tar -xzf /tmp/hadoop-3.4.2.tar.gz -C /opt && sudo mv /opt/hadoop-3.4.2 /opt/hadoop
```

Own it as your user. Running daemons as root is unnecessary here and makes every
later permission question harder to reason about:

```bash
sudo chown -R "$USER":"$USER" /opt/hadoop
```

Create the storage directories:

```bash
sudo mkdir -p /data/hadoop/hdfs/namenode /data/hadoop/hdfs/datanode /data/hadoop/tmp && sudo chown -R "$USER":"$USER" /data/hadoop
```

Only the master really needs `namenode`, but creating both everywhere keeps the
five machines identical, and identical machines are easier to debug than
almost-identical ones.

Confirm Hadoop can find Java:

```bash
hadoop version
```

If that reports `JAVA_HOME is not set`, §5 is incomplete.

---

## 10. Configure Hadoop — every file, explained

All five files live in `/opt/hadoop/etc/hadoop/`. Templates are in `conf/`.

**Back up before you edit. Every time.**

```bash
cd /opt/hadoop/etc/hadoop && for f in core-site.xml hdfs-site.xml yarn-site.xml mapred-site.xml workers; do cp -n "$f" "$f.bak-$(date +%Y%m%d-%H%M%S)"; done
```

This is not ceremony. One of the three bugs in
[docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) was diagnosed by diffing a
config against its backup, which turned "something changed" into "these three
properties were missing" in about ten seconds.

Copy the templates in, then edit the addresses:

```bash
cp conf/core-site.xml conf/hdfs-site.xml conf/yarn-site.xml conf/mapred-site.xml /opt/hadoop/etc/hadoop/
```

### 10.1 What goes where

| File | Same on every node? | The one thing to get right |
|---|---|---|
| `core-site.xml` | yes | `fs.defaultFS` = the **master's** mesh IP |
| `hdfs-site.xml` | yes | storage paths on the Linux filesystem |
| `mapred-site.xml` | yes | all three `*.env` properties, on clients too |
| `yarn-site.xml` | **no** | `yarn.nodemanager.hostname` = **this node's own** IP |
| `workers` | master only | mesh IPs, and include the master |

### 10.2 `core-site.xml`

```xml
<property>
  <name>fs.defaultFS</name>
  <value>hdfs://100.64.0.10:9000</value>
</property>
<property>
  <name>hadoop.tmp.dir</name>
  <value>/data/hadoop/tmp</value>
</property>
```

One address, used by every client, DataNode and NodeManager to find the
NameNode. It must resolve from every node, which is exactly why it is a mesh IP
and not a hostname.

**Never put `localhost` or `127.0.0.1` here.** A worker resolving `localhost`
looks for a NameNode on itself, does not find one, and reports a connection
error that names its own address — which sends you looking at the wrong machine.

`hadoop.tmp.dir` is off `/tmp` on purpose. `/tmp` is cleared on reboot, and
under it Hadoop keeps job staging state and, if you leave the defaults, HDFS
metadata. A reboot should not be able to destroy your filesystem.

Port 9000 is the NameNode's **RPC** port. 9870 is its web UI. They are not
interchangeable and a config pointing at 9870 fails in a way that looks like a
protocol mismatch.

### 10.3 `hdfs-site.xml`

```xml
<property><name>dfs.namenode.name.dir</name><value>file:///data/hadoop/hdfs/namenode</value></property>
<property><name>dfs.datanode.data.dir</name><value>file:///data/hadoop/hdfs/datanode</value></property>
<property><name>dfs.replication</name><value>3</value></property>
<property><name>dfs.blocksize</name><value>134217728</value></property>
<property><name>dfs.namenode.datanode.registration.ip-hostname-check</name><value>false</value></property>
```

`dfs.replication` = 3 across five nodes. These are laptops that get closed, go
to sleep and lose Wi-Fi; 3 is the lowest value that tolerates two simultaneous
absences.

`dfs.blocksize` = 134,217,728 (128 MiB) is the default, and it is listed
explicitly because **this property decides how many map tasks you get.** A
244.3 MiB input becomes 2 blocks, therefore 2 splits, therefore 2 map tasks —
on a five-node cluster. That is not a misconfiguration, it is arithmetic, and
§17 is about what to do when you would rather have four.

`ip-hostname-check` off: DataNodes register by IP, and on a mesh the reverse
lookup of an address does not necessarily match the forward one. With the check
on, a perfectly healthy DataNode gets rejected over that mismatch.

This IP registration is also why HDFS came up cleanly on our cluster while YARN
did not — see §10.4.

### 10.4 `yarn-site.xml` — the one that differs per node

```xml
<!-- cluster-wide -->
<property><name>yarn.resourcemanager.hostname</name><value>100.64.0.10</value></property>
<property><name>yarn.nodemanager.aux-services</name><value>mapreduce_shuffle</value></property>

<!-- THIS NODE ONLY - change on every machine -->
<property><name>yarn.nodemanager.hostname</name><value>100.64.0.14</value></property>

<!-- per-node resource budget -->
<property><name>yarn.nodemanager.resource.memory-mb</name><value>2048</value></property>
<property><name>yarn.nodemanager.resource.cpu-vcores</name><value>2</value></property>
<property><name>yarn.scheduler.maximum-allocation-mb</name><value>2048</value></property>
<property><name>yarn.scheduler.minimum-allocation-mb</name><value>512</value></property>

<!-- WSL2 sanity -->
<property><name>yarn.nodemanager.vmem-check-enabled</name><value>false</value></property>
<property><name>yarn.nodemanager.pmem-check-enabled</name><value>true</value></property>
```

Set `yarn.nodemanager.hostname` to **that machine's own** mesh IP:

| Node | Value |
|---|---|
| master | `100.64.0.10` |
| worker1 | `100.64.0.11` |
| worker2 | `100.64.0.12` |
| worker3 | `100.64.0.13` |
| worker4 | `100.64.0.14` |

**Why this property is set at all.** Leave it out and a NodeManager advertises
whatever `hostname -f` returns. Under WSL2 that is typically
`MACHINE.localdomain` — a name resolvable only on the machine that owns it. The
ResourceManager must resolve a NodeManager's address in order to mint a
container token for it; the resolution throws `UnknownHostException`, the token
is never minted, no container is ever issued, and every application sits in
`ACCEPTED` forever with no error in the client output.

That is the worst class of bug: silent, and it looks like a scheduler capacity
problem. It cost us real hours, and it is issue #1 in
[docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) with the full call chain.

Note this also explains why HDFS was fine while YARN was not — DataNodes
register by IP and never involve a hostname lookup.

**Copying this file between nodes without changing that value** makes every
NodeManager advertise the same address, and the ResourceManager treats them as
one node that keeps re-registering. Symptom: a five-node cluster reporting one
node.

`aux-services` = `mapreduce_shuffle` is required on **every** NodeManager. It is
what serves map output to reducers. A node missing it will happily run map tasks
and then stall the entire job at "reduce 0%".

2048 MB and 2 vcores per node is deliberately modest — these are laptops that
also run a desktop and a browser. Five nodes gives 10,240 MB and 10 vcores of
cluster capacity. Keep `maximum-allocation-mb` at or below the per-node memory:
set it higher and the scheduler accepts container requests no node can satisfy,
and the application waits forever. Same visible symptom as the bug above, and a
completely different cause, which is a good reason to fix one thing at a time.

`vmem-check-enabled=false` is a WSL2 accommodation. The virtual-memory check
compares a JVM's reserved address space against a multiple of its physical
allocation; under WSL2 that reservation is large and the check kills healthy
containers. `pmem-check-enabled` stays **true** — that is the one actually
protecting the host, and turning both off to make an error go away is how you
get a laptop that locks up under load.

### 10.5 `mapred-site.xml`

```xml
<property><name>mapreduce.framework.name</name><value>yarn</value></property>
<property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop</value></property>
<property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop</value></property>
<property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop</value></property>
```

Those three `*.env` properties are the ones people omit.

At submit time the **client** serialises its configuration into `job.xml` and
ships it to the ApplicationMaster container. If the machine you typed
`hadoop jar` on lacks these properties, the AM launches with no MapReduce
classpath and dies with:

```
Error: Could not find or load main class
org.apache.hadoop.mapreduce.v2.app.MRAppMaster
```

The cluster is perfectly healthy. The daemons all have the right config. The job
still fails, purely because of which node submitted it. We hit exactly this: the
same jar ran from the master and failed from a worker.

The corollary is genuinely useful — **fixing it needs no daemon restart.** Edit
the submitting client's `mapred-site.xml` and resubmit. This is issue #3 in
[docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md).

Put all three on all five nodes, whether or not you think a node will ever
submit a job.

### 10.6 `workers` — master only

```
100.64.0.10
100.64.0.11
100.64.0.12
100.64.0.13
100.64.0.14
```

Read by `start-dfs.sh` and `start-yarn.sh` to decide which hosts to SSH into.
Mesh IPs, not hostnames — the scripts SSH to exactly what is written here, and a
name that resolves only on one machine fails from the master. On a worker this
file is irrelevant; leave it as `localhost`.

### 10.7 Sanity check before starting anything

```bash
./scripts/cluster-health.sh
```

Read-only. It starts nothing, stops nothing, deletes nothing, and is safe to run
mid-job. Before the first start it will report no daemons, which is correct; what
you are checking now is that the mesh is reachable and Java is found.

---

## 11. Format the NameNode and start the cluster

### 11.1 Format — on the master, exactly once, ever

```bash
hdfs namenode -format -clusterId CID-$(uuidgen)
```

**Read this before running it.** Formatting creates an empty filesystem
directory. Run it on a cluster that already has data and you have discarded
every block reference in it — the DataNodes still hold the blocks, the NameNode
no longer knows any file they belong to, and there is no undo. This command is
for a brand-new cluster.

If you think you need to format an existing cluster, you almost certainly need
something else. Ask why first.

Note the cluster ID it prints. It appears in `dfsadmin -report` and is how you
confirm a DataNode belongs to this filesystem rather than a previous one.

### 11.2 Start HDFS — on the master

```bash
start-dfs.sh
```

This SSHes to every host in `workers` and starts a DataNode there, plus the
NameNode locally. Confirm on each node:

```bash
jps
```

Master: `NameNode`, `DataNode`. Workers: `DataNode`.

```bash
hdfs dfsadmin -report | head -20
```

`Live datanodes (5)`. If a node is missing, read its log — the filename tells
you the node and the daemon:

```bash
tail -40 /opt/hadoop/logs/hadoop-*-datanode-*.log
```

### 11.3 Start YARN — on the master

```bash
start-yarn.sh
```

```bash
yarn node -list -all
```

Five nodes, all `RUNNING`. Look at the addresses in that list carefully: each
should be a **mesh IP** with a port. A node listed as
`something.localdomain:PORT` means §10.4 was not applied on that machine, and
you should fix it now rather than after a job hangs.

### 11.4 Create the HDFS directories the job needs

```bash
hdfs dfs -mkdir -p /data-sanitizer/hr/input /validation/input
```

And the shared staging directory — this one has a subtlety:

```bash
hdfs dfs -mkdir -p /tmp && hdfs dfs -chmod 1777 /tmp
```

YARN writes per-job staging state under `/tmp/hadoop-yarn/staging`. Created
implicitly by the first job, it is owned by whoever submitted it, mode
`drwxr-xr-x`. When a **different** user on a different node submits the next
job, they cannot write there:

```
org.apache.hadoop.security.AccessControlException: Permission denied:
user=<other-user>, access=WRITE, inode="/tmp/hadoop-yarn/staging"
```

Since every one of our five laptops has a different Linux username, this bites
immediately. Mode `1777` is the standard fix and the reason is the leading `1`:
the sticky bit lets everyone write while still preventing anyone from deleting
another user's files. This is issue #2 in
[docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md).

### 11.5 Optional: the JobHistory Server

```bash
mapred --daemon start historyserver
```

Serves finished-job counters and logs at `:19888` after the ApplicationMaster
exits. Worth starting if you intend to read counters later; without it, the
tracking URLs in the ResourceManager's finished-application list are dead links.
Nothing in this package requires it — the driver prints its own report, and
`yarn application -status` works regardless.

---

## 12. Validate with WordCount before anything else

```bash
./scripts/validate-wordcount.sh
```

WordCount is uninteresting as a computation. It is extremely interesting as a
test, because it exercises every moving part in the right order:

```
HDFS write ─► RM submit ─► container allocated on some node ─► map
   ─► shuffle over the mesh ─► reduce ─► HDFS write ─► commit
```

If WordCount cannot finish, your own job cannot either, and every minute spent
reading your Java is wasted. If WordCount finishes and your job does not, the
fault is in your code or your client config — a much smaller place to look.

**Run it from the node you intend to submit from.** A job that works from the
master and fails from a worker is a client-config problem (§10.5), and this is
where you find that out cheaply.

What a pass looks like: `map 0% reduce 0%` progressing to `map 100% reduce 100%`,
`State: FINISHED`, `Final-State: SUCCEEDED`, and word counts in the part file.

What a specific failure looks like: the application ID prints, and then nothing
at all for minutes. That is `ACCEPTED` with no container ever allocated — stop
and read TROUBLESHOOTING #1 rather than waiting.

One more thing worth checking in the output — the `AM Host` line:

```bash
yarn application -status <application_id> | grep -i 'am host'
```

On our cluster the WordCount ApplicationMaster ran on a **worker**, not the
master. That is the moment you know work is genuinely distributed and not quietly
running on one machine, and it is worth capturing.

Only now build your own jar.

---

## 13. Get the dataset and profile it

The input is the public *HR Dataset (clean and raw, 2M rows)* from Kaggle. It is
not bundled here — 130 MB compressed, and not ours to redistribute. Download
`hr_raw.csv` and put it somewhere on the Linux filesystem, **not** under
`/mnt/c/`.

What it looks like: 2,000,000 data rows plus a header, 15 columns, 256,161,831
bytes (244.3 MiB).

```
Employee_ID,Full_Name,Department,Job_Title,Hire_Date,Performance_Rating,
Experience_Years,Status,Work_Mode,Salary,Year,Country,City,Age,Job_Level
```

### Profile it before writing a single rule

```bash
python3 tools/profile_dataset.py ~/hr_raw.csv
```

Every rule in the mapper exists because this script found the defect first.
Writing rules by guesswork gives you a job that rejects nothing — so it looks
like it did nothing — or rejects everything, so it looks broken.

The report covers structural width, blanks and placeholder tokens per column,
identifier format, date formats, categorical values outside their allowed sets,
numeric range and integer-ness, duplicate ids, and the `Age − Experience_Years`
margin distribution.

Two things to actually read it for:

**The quoted-comma count.** If it is non-zero — ours was 436 — you must parse
quote-aware. `line.split(",")` will report those rows as malformed, and you will
spend an afternoon debugging a parser bug you introduced yourself.

**The margin histogram.** It is why the logical rule is `Age >= Exp + 21` and not
some rounder-looking number. Read down the buckets and pick the threshold from
the data. One step either side of the real floor changes the rejection count by
an order of magnitude, and a rule that rejects a third of a clean dataset is a
bug in the rule, not a finding about the data.

The report counts every defect independently, so one bad row appears under
several headings. The mapper does not — it stops at the first failure. Expect the
profiler's per-rule numbers to be greater than or equal to the job's and read
the difference as overlap, not as a discrepancy.

### The deliberately-broken test file

`data/hr_corrupt_test.csv` is 39 hand-built rows aimed at each rule in turn:
short and 3-field records, `N/A` and `null` blanks, `EMP99` and `XYZ0000110`
identifiers, `2019-13-01`, a `Telepathic` work mode, a negative salary, and rows
on both sides of the `Age >= Exp + 21` boundary — plus repeated ids to exercise
the reducer's de-duplication.

It runs in seconds, so it is the file to use while developing. A rule you cannot
demonstrate on 39 rows you cannot claim on 2,000,000.

---

## 14. Build the jar

```bash
./scripts/build.sh
```

No Maven, no Gradle. The only external dependency is Hadoop, and
`hadoop classpath` already knows where every jar is. A build tool would add a
wrapper script, a lockfile and a download step to solve a problem this project
does not have.

What it runs, in essence:

```bash
javac -cp "$(hadoop classpath)" -d build/classes $(find src -name '*.java')
```

```bash
jar cf data-sanitizer.jar -C build/classes .
```

Quote `$(hadoop classpath)` — it expands to a long colon-separated list and
individual paths can contain spaces.

A prebuilt `data-sanitizer.jar` ships in this package, so §15 works before you
have compiled anything.

---

## 15. Run the sanitizer

### On the cluster

```bash
./scripts/run-cluster.sh ~/hr_raw.csv
```

The script checks the cluster is reachable, uploads the input to HDFS (skipping
the upload if a file of the right size is already there — 244 MiB over a mesh VPN
is not instant), clears the output directory, submits, and then verifies.

By hand, if you prefer:

```bash
hdfs dfs -put -f ~/hr_raw.csv /data-sanitizer/hr/input/
```

```bash
hadoop jar data-sanitizer.jar com.bda.sanitizer.DataSanitizerDriver -D mapreduce.job.reduces=2 /data-sanitizer/hr/input /data-sanitizer/hr/output
```

MapReduce refuses to start if the output path exists. That is a feature — it
stops a re-run from silently half-overwriting a previous result. Remove it
explicitly:

```bash
hdfs dfs -rm -r -f -skipTrash /data-sanitizer/hr/output
```

### Without a cluster

The identical jar runs in a single JVM with two overrides:

```bash
./scripts/run-local.sh data/hr_corrupt_test.csv /tmp/out-corrupt
```

which is:

```bash
hadoop jar data-sanitizer.jar com.bda.sanitizer.DataSanitizerDriver -D fs.defaultFS=file:/// -D mapreduce.framework.name=local <in> <out>
```

Same mapper, same reducer, same rules, same counters, no HDFS and no YARN. This
is how you develop the logic while the cluster is down, and it is how the
artefacts in `output/` were produced.

It is **not** a substitute for the cluster run. One JVM, one map task, no
network shuffle, no YARN metrics. Use it to prove the rules are right, never to
make a performance claim.

### What the driver prints

Counters read straight from the completed job — reads, valid, invalid, a
breakdown by each of the seven rules, quote-rescued rows, duplicates dropped,
final unique records, driver-measured wall clock, and three integrity checks:

```
CHECK valid + invalid == read
CHECK reasons == invalid
CHECK unique + dupes == valid
```

The middle one is the one that earns its keep. It only passes because the rules
stop at the first failure, so each rejected row is charged to exactly one
counter. If that check ever fails, the per-rule breakdown is not trustworthy and
neither is anything derived from it.

---

## 16. Verify the output

```bash
./scripts/verify-output.sh /data-sanitizer/hr/output
```

```bash
./scripts/verify-output.sh /tmp/out-corrupt --local
```

The driver's checks come from the job's own counters. These come from the
**output bytes**, using tools that know nothing about the job. If the counters
lied, these disagree.

1. **Every row has exactly 15 fields** — parsed quote-aware, deliberately not
   with `awk -F','`, because `"Nuwenhuysen, van der"` contains a comma inside
   quotes and naive splitting would report 16 fields for a valid row. That false
   alarm is precisely the mistake the mapper had to avoid, so the verifier had
   better not make it.
2. **`Employee_ID` is unique** — a duplicate would mean the shuffle did not
   group correctly.
3. **The rules re-applied from outside** — blanks, id format, date format,
   salary, and the `Age >= Exp + 21` logic, re-tested in Python. Any hit means a
   record escaped the ladder.

Also check the `_SUCCESS` marker exists. Its absence means the output is partial
even if part files are present. And note the number of `part-r-NNNNN` files
equals the number of **reducers**, not the number of workers.

### Honest note about what is in `output/`

The 243 MB output of the full cluster run is **not** in this package, and not
because of size alone. HDFS was down when this archive was assembled and `/tmp`
had been cleared by a reboot, taking the original console logs with it.

Rather than paste numbers into a fake log, `output/` contains:

- **`local-run-50k-console.log`** — a genuine local-mode run of this exact jar
  over a 50,000-row slice of the real input. 48,590 valid (97.18%), 1,410
  invalid (2.82%), all three integrity checks PASS. The full cluster run over
  2,000,000 rows gave 97.14%. Independent corroboration, not a substitute.
- **`local-run-corrupt-console.log`** and **`corrupt-run-output-full.csv`** — the
  39-row test file, start to finish, with the complete output.
- **`sample-clean-output-head-200.csv`** — the first 200 rows of a real
  `part-r-00000`.
- **`screenshots/`** — five unedited captures from the actual cluster runs:
  `yarn application -status` with job counters, the driver reports, and the
  map/reduce progress with exit code 0.

Every measured figure quoted anywhere in this package comes from those
screenshots or that local run. Nothing is invented, and where a number was not
recoverable it is absent rather than approximated. Full detail in
[docs/RESULTS.md](docs/RESULTS.md).

---

## 17. The split-size experiment

A 244.3 MiB input at a 128 MiB block size gives 2 blocks, 2 splits, 2 map tasks.
On a five-node cluster that means **three NodeManagers have nothing to do**.

Say that plainly rather than implying the cluster was saturated. It is a property
of the input size, not a fault.

You can force more parallelism without touching the data, the jar or the rules:

```bash
./scripts/run-cluster.sh ~/hr_raw.csv 67108864
```

which passes:

```bash
-D mapreduce.input.fileinputformat.split.maxsize=67108864
```

`split.maxsize` caps a split **below** the block size, so one 128 MiB block
yields multiple map tasks. It cannot make splits larger than a block. 64 MiB
gives 4 splits, 4 map tasks, and containers on more nodes.

**What the comparison actually showed, and it is not what we expected.** Total
CPU time was 53,660 ms with 2 splits and 57,570 ms with 4 — within about 7%. The
same work was done either way, which is the correct result: splitting a job into
more pieces does not reduce the work, it redistributes it.

Wall clock differed enormously — 875.5 s versus 112.9 s — but the honest reading
of that is **contention on shared laptops**, not an engineered speedup. Those
machines were doing other things during the first run. Container occupancy tells
the real efficiency story: 1,618,586 ms versus 240,251 ms of allocated container
time. HDFS bytes written were byte-for-byte identical, which is the proof that
both runs produced the same output.

Reporting the wall-clock ratio as a 7.8× improvement would be the easy claim and
it would be wrong. The defensible claim is the smaller one: the split count
controls parallelism, the CPU time confirms the work is invariant, and the wall
clock on borrowed hardware is not a benchmark.

---

## 18. Web UIs, and which port is which

| UI | URL | Shows |
|---|---|---|
| NameNode | `http://100.64.0.10:9870` | filesystem, blocks, DataNodes, capacity |
| ResourceManager | `http://100.64.0.10:8088` | applications, scheduler, nodes |
| NodeManager | `http://100.64.0.11:8042` | one node's containers and logs |
| JobHistory | `http://100.64.0.10:19888` | finished jobs — needs the §11.5 daemon |

The distinction people get wrong: **8042 is the NodeManager's web port. It is
not the port the ResourceManager talks to.** The RPC port is ephemeral and shows
up in `yarn node -list` beside the node's address. Assuming 8042 is the RPC port
sends you hunting a firewall problem that does not exist — and the actual bug in
that neighbourhood was §10.4's hostname resolution, which no amount of port
checking would have found.

Useful from the command line:

```bash
curl -s http://100.64.0.10:8088/ws/v1/cluster/metrics | python3 -m json.tool
```

```bash
yarn application -list -appStates ALL
```

```bash
yarn application -status <application_id>
```

```bash
yarn logs -applicationId <application_id> | less
```

`yarn logs` needs log aggregation, or the JobHistory Server, or both. Without
them the container logs live on whichever node ran the container, under
`/opt/hadoop/logs/userlogs/`.

---

## 19. Shutting down and starting up again

### Stop cleanly, in this order, from the master

```bash
stop-yarn.sh && stop-dfs.sh
```

YARN first. Stopping HDFS underneath a running job leaves the job writing into a
filesystem that is no longer there.

WSL2 does not stop daemons gracefully when the Windows terminal closes. Closing
the window on a running NameNode is an unclean shutdown; do this instead.

### Start again, in this order

```bash
start-dfs.sh && start-yarn.sh
```

Then always:

```bash
./scripts/cluster-health.sh
```

### After a reboot

Three things typically need attention, in this order:

1. **The mesh.** `tailscale status` — the client usually reconnects on its own,
   but a node that has not been on for a while may need `sudo tailscale up`.
2. **`sshd`.** `sudo service ssh start` if you did not enable `systemd`.
3. **A worker that did not come back.** Start its daemons on that node:

```bash
/opt/hadoop/bin/hdfs --daemon start datanode
```

```bash
/opt/hadoop/bin/yarn --daemon start nodemanager
```

Then confirm from anywhere:

```bash
curl -s http://100.64.0.10:8088/ws/v1/cluster/metrics | python3 -c "import sys,json;m=json.load(sys.stdin)['clusterMetrics'];print(m['activeNodes'],'active /',m['shutdownNodes'],'shutdown')"
```

**Expect drift.** A cluster of laptops does not stay at 5/5 — machines get
closed, Wi-Fi drops, people go home. Node counts reported in
[docs/RESULTS.md](docs/RESULTS.md) are labelled *at job submission* for exactly
this reason. If you check the cluster a week later and see 4 active and 2
shutdown entries, nothing is broken; a node is asleep and a stale registration
from before a hostname fix is still listed. Stale shutdown entries are harmless
and clear when the ResourceManager restarts.

---

## 20. Things never to do

Learned the boring way. Every one of these is something that looks like a fix and
is actually how you lose a day or a filesystem.

**Never `hdfs namenode -format` on a cluster with data in it.** It discards
every block reference. The DataNodes still hold the blocks; nothing knows what
file they belong to. There is no undo.

**Never delete the NameNode metadata directory** (`dfs.namenode.name.dir`).
`fsimage` and the edit log are the filesystem. The blocks without them are
anonymous bytes.

**Never `rm -rf /opt/hadoop` to "start clean."** Almost every problem that
tempts you to is a configuration problem, and reinstalling preserves it exactly
while destroying your logs and your backups.

**Never reinstall Hadoop, or change its version, without a specific reason.** A
version skew between nodes causes protocol errors that read like corruption.

**Never disable a firewall globally as a diagnostic step.** Find out which port
is actually blocked first. `ss -tlnp | grep <port>` and
`nc -zv <ip> <port>` will tell you in seconds, without leaving a machine
exposed and without you forgetting to turn it back on.

**Never use `localhost` or `127.0.0.1` for inter-node communication.** Every
node resolves it to itself. The error message names an address that looks right
and points at the wrong machine.

**Never assume the NodeManager web port is its RPC port.** 8042 is the web UI.
See §18.

**Never change a dozen configuration values at once.** Change one, restart the
affected daemon, test. Otherwise you cannot tell which change helped and you
will keep all twelve, including the harmful ones.

**Never reach for Docker to solve a normal YARN container problem.** Containers
here are JVM processes under a NodeManager, not Docker containers. Adding a
container runtime to fix a scheduling or resolution problem adds a whole new
layer of things that can break. Our container problem was a hostname that would
not resolve; Docker would not have touched it.

**Always back up a configuration file before editing it.** §10 has the one-liner.
One of our three bugs was diagnosed in ten seconds by diffing against a backup.

---

## 21. Where the numbers are

| Document | Contents |
|---|---|
| [docs/RESULTS.md](docs/RESULTS.md) | Every measured figure from the three cluster runs and the local corroboration — record counts, per-rule rejections, counters, timings, container placement — with what each does and does not prove. |
| [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | The three real bugs: the container-token `UnknownHostException`, the `/tmp/hadoop-yarn/staging` `AccessControlException`, and the `MRAppMaster ClassNotFoundException`. Symptom, root cause, fix, and how to confirm the fix. |
| [docs/ANONYMIZATION.md](docs/ANONYMIZATION.md) | What was replaced with placeholders, and what was deliberately left real. |

Nothing in either document is estimated. Where a figure was not recoverable it is
marked as such rather than filled in.

---

# Author
R. Dilip
# Contributors
D. Sohan <br>
P. Akshith Kumar
