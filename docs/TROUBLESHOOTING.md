# Troubleshooting — the three bugs that actually happened

Not a generic FAQ. These are the three failures that cost real time on this
cluster, written up with the symptom you see first, the root cause, the fix, and
how to confirm the fix worked.

All three share a shape worth noticing: **the cluster looked healthy in every
case.** Daemons up, nodes registered, capacity available. Nothing in a dashboard
would have told you. That is why the diagnostic step in each is "read a specific
log or resolve a specific name", not "check the UI".

| # | Symptom you see | Real cause | Layer |
|---|---|---|---|
| 1 | Every application sits in `ACCEPTED` forever | NodeManager advertised an unresolvable hostname, so no container token could be minted | cluster |
| 2 | `AccessControlException` on `/tmp/hadoop-yarn/staging` | Staging dir owned by whoever submitted first, mode `755` | cluster |
| 3 | `ClassNotFoundException: MRAppMaster` | The **submitting client's** `mapred-site.xml` lacked three properties | client |

---

## 1. Applications hang in ACCEPTED — no container is ever allocated

### What you see

You submit a job. The client prints an application ID. Then nothing. No progress
line, no error, no timeout. `map 0% reduce 0%` never appears.

```bash
yarn application -status application_XXXXXXXXXXXXX_XXXX
```

```
State : ACCEPTED
Final-State : UNDEFINED
```

Meanwhile everything looks correct:

- `yarn node -list` shows all five NodeManagers `RUNNING`
- the ResourceManager reports free memory and free vcores
- HDFS is completely healthy — `put`, `get`, `ls` all work perfectly
- no errors in the client output at all

The natural conclusion is a scheduler capacity problem. It is not. Do not go
tuning the Capacity Scheduler; you will change a dozen values and fix nothing.

### The diagnostic that finds it

Look at the **addresses** in the node list, not the states:

```bash
yarn node -list -all
```

If a node appears as `MACHINE.localdomain:PORT` rather than a mesh IP with a
port, you have this bug. Prove it by trying to resolve that name from the master:

```bash
getent hosts MACHINE.localdomain
```

Nothing comes back. The name resolves only on the machine that owns it.

Then confirm in the ResourceManager log — this is where the actual exception is,
and it is the only place it appears:

```bash
grep -i 'UnknownHostException\|createContainerToken\|buildTokenService' /opt/hadoop/logs/hadoop-*-resourcemanager-*.log | tail -20
```

### Root cause

Left unset, `yarn.nodemanager.hostname` defaults to whatever `hostname -f`
returns. Under WSL2 that is typically `MACHINE.localdomain` — a name with no
entry in any DNS the other nodes can query, and no entry in their `/etc/hosts`.

The NodeManager registers under that name. Registration succeeds, because
registration does not require the ResourceManager to resolve anything. The node
shows up `RUNNING`. Everything looks fine.

The failure happens one step later, when the scheduler tries to hand out a
container:

```
RMContainerTokenSecretManager.createContainerToken(...)
    └─► SecurityUtil.buildTokenService(nmAddress)
            └─► InetSocketAddress resolution of "MACHINE.localdomain"
                    └─► UnknownHostException
```

A container token is cryptographically bound to the NodeManager's address — it is
what stops a container being redeemed on the wrong node. The address cannot be
resolved, so the token cannot be built, so the container is never issued. The
application waits for a container that will never come.

```
  ┌─────────────────────────────────────────────────────────────┐
  │  NM registers as MACHINE.localdomain   ──► SUCCEEDS         │
  │  RM lists it as RUNNING                ──► LOOKS FINE       │
  │  Scheduler picks it for a container    ──► SUCCEEDS         │
  │  RM mints the container token          ──► UnknownHostEx    │
  │  Container issued to the app           ──► NEVER HAPPENS    │
  │  App state                             ──► ACCEPTED, ever   │
  └─────────────────────────────────────────────────────────────┘
```

**Why HDFS was completely unaffected**, which is the clue that should have led us
here faster: DataNodes register with the NameNode **by IP address**. There is no
hostname in that path, so no resolution to fail. A cluster where HDFS is flawless
and YARN allocates nothing is pointing straight at name resolution in the YARN
control path.

### Fix

On **every node**, pin the NodeManager to that node's own mesh IP in
`/opt/hadoop/etc/hadoop/yarn-site.xml`:

```xml
<property>
  <name>yarn.nodemanager.hostname</name>
  <value>100.64.0.14</value>   <!-- THIS node's mesh IP -->
</property>
```

| Node | Value |
|---|---|
| master | `100.64.0.10` |
| worker1 | `100.64.0.11` |
| worker2 | `100.64.0.12` |
| worker3 | `100.64.0.13` |
| worker4 | `100.64.0.14` |

Back the file up first, then restart the NodeManager **on that node only**:

```bash
cp /opt/hadoop/etc/hadoop/yarn-site.xml /opt/hadoop/etc/hadoop/yarn-site.xml.bak-$(date +%Y%m%d-%H%M%S)
```

```bash
/opt/hadoop/bin/yarn --daemon stop nodemanager && /opt/hadoop/bin/yarn --daemon start nodemanager
```

No ResourceManager restart and no reformat is needed. The NodeManager
re-registers under the new address within a heartbeat.

**Do not copy one node's `yarn-site.xml` to the others.** If every NodeManager
advertises the same address, the ResourceManager treats them as one node that
keeps re-registering — a five-node cluster reporting one node.

### Confirm the fix

```bash
yarn node -list -all
```

Every node listed as a mesh IP with a port, all `RUNNING`. Old `SHUTDOWN` entries
under the previous addresses may linger; they are harmless and clear when the
ResourceManager restarts.

Then run the real test:

```bash
./scripts/validate-wordcount.sh
```

`ACCEPTED` should become `RUNNING` within seconds rather than never.

### If it is not this

Two other causes produce the identical `ACCEPTED`-forever symptom. Rule them out
before assuming resolution:

- **`yarn.scheduler.maximum-allocation-mb` exceeds
  `yarn.nodemanager.resource.memory-mb`.** The scheduler accepts a container
  request no node can satisfy. Compare the two values; the maximum must be at or
  below the per-node figure.
- **No NodeManager has enough free memory** because an earlier application never
  released its containers. `yarn application -list` will show it still running;
  kill it with `yarn application -kill <id>`.

---

## 2. AccessControlException on the YARN staging directory

### What you see

Submitting from one node works. Submitting the same jar from a different node
fails immediately, before any container starts:

```
org.apache.hadoop.security.AccessControlException: Permission denied:
user=<other-user>, access=WRITE,
inode="/tmp/hadoop-yarn/staging":<first-user>:supergroup:drwxr-xr-x
```

The message is unusually good — it names the user, the access, the inode, its
owner and its mode. Read it rather than searching for it.

### Root cause

YARN keeps per-job staging state — the job jar, `job.xml`, the split metadata —
under `/tmp/hadoop-yarn/staging` in HDFS. That directory is created implicitly by
the first job that runs, and it is owned by whoever submitted that job, with
mode `drwxr-xr-x`.

`755` means only the owner can write. So the second user to submit a job cannot,
and the job dies at submit time.

On this cluster every laptop belongs to a different person and therefore has a
different Linux username, so this appeared the moment a second person tried to
run something. On a cluster where everyone shares one account it never appears at
all — which is why it is easy to be surprised by.

### Fix

```bash
hdfs dfs -chmod 1777 /tmp
```

The leading `1` is the point. `1777` is world-writable **with the sticky bit**:
anyone can create files, and only a file's owner can delete it. Plain `777`
would also fix the symptom while letting any user delete another user's
in-flight job state — a much worse problem, arriving later, and much harder to
diagnose.

This is the same convention as `/tmp` on a Linux filesystem, for the same reason.

Include it in cluster setup so it is never the thing you discover mid-demo:

```bash
hdfs dfs -mkdir -p /tmp && hdfs dfs -chmod 1777 /tmp
```

### Confirm the fix

```bash
hdfs dfs -ls -d /tmp
```

Look for `drwxrwxrwt` — the trailing `t` is the sticky bit. Then resubmit from
the node that failed.

---

## 3. ClassNotFoundException: MRAppMaster

### What you see

The job submits successfully. A container starts. It dies instantly. YARN retries
twice and gives up:

```
Application application_XXXXXXXXXXXXX_XXXX failed 2 times due to AM Container
for appattempt_... exited with exitCode: 1
```

```
Error: Could not find or load main class
org.apache.hadoop.mapreduce.v2.app.MRAppMaster
```

And the part that makes it confusing: **the same jar runs perfectly from the
master.** Only submitting from one particular node fails. Nothing about the
cluster is different between the two attempts.

### Root cause

At submit time the **client** — the machine you typed `hadoop jar` on —
serialises its own configuration into `job.xml` and ships it to the
ApplicationMaster container. Whatever the client does not have, the AM does not
get.

Three properties supply the AM and the task JVMs with a MapReduce classpath:

```xml
<property><name>yarn.app.mapreduce.am.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop</value></property>
<property><name>mapreduce.map.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop</value></property>
<property><name>mapreduce.reduce.env</name><value>HADOOP_MAPRED_HOME=/opt/hadoop</value></property>
```

On the node that failed, `mapred-site.xml` was still the stock file — it had
`mapreduce.framework.name` and nothing else. The AM launched with no MapReduce
classpath and could not find its own main class.

This is worth stating as a principle because it generalises well beyond this
error: **on a Hadoop cluster, the submitting client's configuration is part of
the job.** A cluster can be perfectly configured and a job still fail because of
which machine typed the command. When a job fails from one node and works from
another, look at the client config before you look at the cluster.

```
  ┌──────────────┐  serialises config   ┌──────────┐  launches  ┌────────┐
  │  the client  │ ───── into ────────► │ job.xml  │ ─────────► │   AM   │
  │  you typed   │                      │          │            │container│
  │  the command │  ← missing here      └──────────┘            └────────┘
  │  on          │     means missing there                       dies: no
  └──────────────┘                                               classpath
```

### Fix

On the submitting node, back up and add the three properties:

```bash
cp /opt/hadoop/etc/hadoop/mapred-site.xml /opt/hadoop/etc/hadoop/mapred-site.xml.bak-$(date +%Y%m%d-%H%M%S)
```

```bash
cp conf/mapred-site.xml /opt/hadoop/etc/hadoop/mapred-site.xml
```

**No daemon restart is required.** This is the useful corollary of the cause: the
config is read by the client at submit time, not by a running daemon. Edit the
file and resubmit. That is the whole fix.

Do this on all five nodes whether or not you expect them to submit jobs. It costs
nothing and removes an entire failure mode.

### Confirm the fix

Diff against the backup to see exactly what changed — this is the reason for the
backup discipline in the first place:

```bash
diff /opt/hadoop/etc/hadoop/mapred-site.xml.bak-* /opt/hadoop/etc/hadoop/mapred-site.xml
```

Then resubmit from the node that failed. Or check what the running config
actually resolves to, which is more reliable than reading the file:

```bash
hadoop org.apache.hadoop.util.FindClass -c org.apache.hadoop.mapreduce.v2.app.MRAppMaster
```

```bash
mapred job -list all 2>/dev/null | head
```

---

## A short list of other things that go wrong

Not bugs we hit on this cluster, but the near neighbours of the three above —
worth knowing so you can tell them apart.

**Job hangs at `reduce 0%` while maps all completed.** A NodeManager is missing
`yarn.nodemanager.aux-services=mapreduce_shuffle`. Its map output cannot be
fetched, so the reduce phase waits. It must be set on **every** NodeManager, and
the node that is missing it will not report an error — it will run maps
successfully and stall the whole job.

**`JAVA_HOME is not set` from the startup scripts only.** `~/.bashrc` is not
sourced by a non-interactive SSH session. Set `JAVA_HOME` in `hadoop-env.sh` as
well. Test with `ssh localhost 'echo $JAVA_HOME'` — empty output means you have
this.

**Containers killed for exceeding virtual memory.** The virtual-memory check
compares a JVM's reserved address space against a multiple of its physical
allocation; under WSL2 the reservation is large and healthy containers get
killed. Set `yarn.nodemanager.vmem-check-enabled=false`. Leave
`pmem-check-enabled=true` — that is the one protecting the host, and disabling
both to make an error message disappear is how you get a laptop that locks up.

**A DataNode will not join, complaining about a cluster ID.** It holds storage
from a previous, differently-formatted filesystem. Do not reformat the NameNode
to fix this — that solves one node's problem by destroying every node's data.
Stop that DataNode and clear only **its** data directory.

**Output directory already exists.** MapReduce refuses to start rather than
half-overwrite a previous result. Remove it explicitly:
`hdfs dfs -rm -r -f -skipTrash <output>`.

**Static `/etc/hosts` entries vanish after a reboot.** WSL regenerates
`/etc/hosts` on every boot unless `generateHosts=false` is set in
`/etc/wsl.conf`. See `conf/wsl.conf`.

**A node silently drops off the mesh weeks later.** Mesh key expiry. Disable key
expiry per machine in the admin console.

---

## The diagnostic order that works

When something fails, work outward from the network. Each step is cheap and rules
out everything below it.

```
1  mesh          ping the master's mesh IP from the failing node
                 tailscale status  — every node present, prefer "direct"

2  resolution    getent hosts <whatever the node list shows>
                 yarn node -list -all  — mesh IPs, not *.localdomain

3  HDFS          hdfs dfs -ls /            NameNode answering?
                 hdfs dfsadmin -report     all DataNodes live?

4  YARN          yarn node -list           all NodeManagers RUNNING?
                 free memory and vcores available?

5  WordCount     ./scripts/validate-wordcount.sh
                 from the node you intend to submit from

6  your job      only now. If 1-5 pass and this fails, it is your code
                 or your client config, which is a small place to look.
```

And one habit that is worth more than any of them: **change one thing, restart
the affected daemon, test.** Changing a dozen configuration values at once means
you cannot tell which one helped, so you keep all twelve — including the ones
that will hurt you later.
