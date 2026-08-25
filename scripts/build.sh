#!/usr/bin/env bash
# ============================================================================
#  build.sh  -  compile the four Java sources into data-sanitizer.jar
#
#  Run from the project root:   ./scripts/build.sh
#
#  There is no Maven and no Gradle here on purpose. The only external
#  dependency is Hadoop itself, and `hadoop classpath` already knows where
#  every jar lives. Adding a build tool would add a lockfile, a wrapper
#  script and a download step to solve a problem we do not have.
# ============================================================================
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

: "${HADOOP_HOME:=/opt/hadoop}"
export HADOOP_HOME
export PATH="$HADOOP_HOME/bin:$PATH"

command -v hadoop >/dev/null 2>&1 || {
  echo "ERROR: 'hadoop' is not on PATH. Set HADOOP_HOME (currently '$HADOOP_HOME')" >&2
  exit 1
}
command -v javac >/dev/null 2>&1 || {
  echo "ERROR: 'javac' is not on PATH. Install a JDK, not just a JRE." >&2
  exit 1
}

echo "javac : $(javac -version 2>&1)"
echo "hadoop: $(hadoop version | head -1)"
echo

rm -rf build/classes
mkdir -p build/classes

# `hadoop classpath` expands to every Hadoop jar plus its transitive deps.
# Quote it: it contains colons but the individual paths can contain spaces.
echo "==> compiling"
javac -Xlint:-options \
      -cp "$(hadoop classpath)" \
      -d build/classes \
      $(find src -name '*.java' | sort)

echo "==> packaging"
jar cf data-sanitizer.jar -C build/classes .

echo
echo "built: $ROOT/data-sanitizer.jar"
jar tf data-sanitizer.jar | grep -c '\.class$' | xargs echo "classes:"
ls -l data-sanitizer.jar
