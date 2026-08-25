#!/usr/bin/env bash
# ============================================================================
#  ssh-setup.sh  -  passwordless SSH from the master to every node
#
#  Run ON THE MASTER, once.
#
#  What SSH is for here, and what it is NOT for:
#
#    IS   - start-dfs.sh and start-yarn.sh log into each host in conf/workers
#           and launch a daemon there. That is the only thing Hadoop uses SSH
#           for. It is also how you administer and troubleshoot the cluster by
#           hand.
#
#    NOT  - carrying HDFS blocks or MapReduce shuffle traffic. Those move over
#           Hadoop's own RPC and HTTP ports (9000, 9866, the shuffle port),
#           inside the mesh. You could stop sshd after the daemons are up and a
#           running job would not notice. Describing SSH as Hadoop's data
#           transport is simply wrong, and it is a common thing to get wrong.
#
#  You do not need SSH between workers. Master -> each worker is enough.
# ============================================================================
set -euo pipefail

# Edit to match your cluster. Mesh IPs, not hostnames - the same addresses that
# go in conf/workers.
NODES=(
  100.64.0.11
  100.64.0.12
  100.64.0.13
  100.64.0.14
)

echo "==> 1  make sure sshd is installed and running on THIS node"
# WSL2 does not ship a running sshd. It also does not start it on boot unless
# systemd is enabled in /etc/wsl.conf - see conf/wsl.conf.
sudo apt-get install -y openssh-server
sudo systemctl enable --now ssh 2>/dev/null || sudo service ssh start

echo
echo "==> 2  key pair"
if [[ -f ~/.ssh/id_ed25519 ]]; then
  echo "    ~/.ssh/id_ed25519 already exists - keeping it."
else
  # No passphrase: the startup scripts are non-interactive and cannot type one.
  # This key's only power is starting daemons inside a private mesh.
  ssh-keygen -t ed25519 -N '' -C "hadoop-master" -f ~/.ssh/id_ed25519
fi

echo
echo "==> 3  authorise the master to itself"
# The startup scripts SSH to the master too, because the master runs a DataNode
# and a NodeManager. Forgetting this step produces the odd symptom of every
# worker starting and the master's own daemons not.
ssh-copy-id -i ~/.ssh/id_ed25519.pub -o StrictHostKeyChecking=accept-new \
    "$(whoami)@localhost" || true

echo
echo "==> 4  copy the key to each worker"
echo "    You will be asked for each worker's password once. After this, never"
echo "    again. Note the username can differ per node - each machine belongs"
echo "    to a different person - so we ask rather than assume."
for ip in "${NODES[@]}"; do
  read -rp "    login user for $ip : " user
  ssh-copy-id -i ~/.ssh/id_ed25519.pub -o StrictHostKeyChecking=accept-new "$user@$ip"
done

echo
echo "==> 5  prove it"
# `hostname` over SSH with no password prompt is the whole test. If any node
# asks for a password here, start-dfs.sh will hang on that node.
for ip in "${NODES[@]}"; do
  printf '    %-14s -> ' "$ip"
  ssh -o BatchMode=yes -o ConnectTimeout=5 "$ip" hostname 2>&1 | head -1
done

cat <<'EOF'

If a node still prompts for a password, the usual causes are:
  * wrong permissions - the remote ~/.ssh must be 700 and
    ~/.ssh/authorized_keys must be 600, or sshd ignores them silently
  * the username differs from the one you tried
  * sshd is not running on the remote node (WSL2 does not start it by default)

Handy: put a per-node block in ~/.ssh/config on the master so you can type
`ssh worker1` instead of remembering five usernames.

    Host worker1
        HostName 100.64.0.11
        User <that-node-user>
        IdentityFile ~/.ssh/id_ed25519
EOF
