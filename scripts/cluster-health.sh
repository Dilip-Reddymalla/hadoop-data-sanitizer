#!/usr/bin/env bash
# ============================================================================
#  cluster-health.sh  -  one command that answers "is the cluster actually up?"
#
#  Run from ANY node. Read-only: it starts nothing, stops nothing, deletes
#  nothing. Safe to run mid-job.
#
#  Set MASTER_IP to the master's mesh address, or export it once in ~/.bashrc.
# ============================================================================
set -uo pipefail

: "${MASTER_IP:=100.64.0.10}"
: "${HADOOP_HOME:=/opt/hadoop}"
export PATH="$HADOOP_HOME/bin:$PATH"

hr() { printf '\n%s\n' "------------------------------------------------------------"; }

hr; echo " 0  this node"
echo "host : $(hostname)"
echo "java : $(java -version 2>&1 | head -1)"
echo "mesh : $(tailscale ip -4 2>/dev/null || echo '(tailscale not on PATH)')"
echo "daemons running here:"
jps -l 2>/dev/null | grep -Ei 'namenode|datanode|resourcemanager|nodemanager|historyserver' \
  || echo "  (none)"

hr; echo " 1  mesh reachability"
# Ping the master over the mesh. If this fails nothing else will work, and the
# problem is the network, not Hadoop.
if ping -c1 -W2 "$MASTER_IP" >/dev/null 2>&1; then
  echo "  master $MASTER_IP  reachable"
else
  echo "  master $MASTER_IP  UNREACHABLE  <-- fix the mesh first"
fi
tailscale status 2>/dev/null | head -8 || true

hr; echo " 2  NameNode RPC (port 9000)"
# -ls on the root is the cheapest proof that the NameNode is up AND that this
# node's fs.defaultFS points at it. A hang here means a firewall or a wrong IP.
timeout 20 hdfs dfs -ls / && echo "  RESULT: NameNode answering" \
  || echo "  RESULT: no answer within 20 s  <-- NameNode down, or wrong fs.defaultFS"

hr; echo " 3  HDFS capacity and live DataNodes"
timeout 25 hdfs dfsadmin -report 2>/dev/null \
  | grep -Ei 'configured capacity|dfs remaining|live datanodes|dead datanodes|under.replicated' \
  | head -12

hr; echo " 4  YARN nodes"
# Every NodeManager should be RUNNING and should be listed under its MESH
# address. A node listed as something.localdomain is issue #1 in
# docs/TROUBLESHOOTING.md and will make applications hang in ACCEPTED.
timeout 25 yarn node -list -all 2>/dev/null | tail -15

hr; echo " 5  cluster metrics, straight from the ResourceManager REST API"
curl -s --max-time 10 "http://$MASTER_IP:8088/ws/v1/cluster/metrics" \
  | python3 -c '
import sys, json
try:
    m = json.load(sys.stdin)["clusterMetrics"]
except Exception:
    print("  (no answer from the ResourceManager web app on port 8088)"); raise SystemExit
print(f"  active NodeManagers : {m[\"activeNodes\"]}")
print(f"  lost / shutdown     : {m[\"lostNodes\"]} / {m[\"shutdownNodes\"]}")
print(f"  memory              : {m[\"totalMB\"]:,} MB  ({m[\"availableMB\"]:,} free)")
print(f"  vcores              : {m[\"totalVirtualCores\"]}")
print(f"  apps running/pending: {m[\"appsRunning\"]} / {m[\"appsPending\"]}")
' 2>/dev/null || echo "  (curl or python3 unavailable)"

hr; echo " 6  recent applications"
timeout 20 yarn application -list -appStates ALL 2>/dev/null | tail -10

hr
cat <<'EOF'
 Web UIs, and which port is which
   NameNode          http://<MASTER_IP>:9870    filesystem, blocks, DataNodes
   ResourceManager   http://<MASTER_IP>:8088    applications, scheduler, nodes
   NodeManager       http://<NODE_IP>:8042      one node's containers and logs
   JobHistory        http://<MASTER_IP>:19888   finished jobs (needs the daemon)

 8042 is the NodeManager's WEB port. It is NOT the port the ResourceManager
 talks to. The RPC port is ephemeral and appears in `yarn node -list` next to
 the node name. Confusing the two sends you looking for a firewall problem
 that is not there.
EOF
