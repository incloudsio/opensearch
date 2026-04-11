#!/usr/bin/env bash
# Build/test OpenSearch side-car with Elassandra :server sources (Cassandra on compile classpath).
# Requires the Ant-built jar from the Elassandra repo: server/cassandra/build/elassandra-cassandra-*.jar
#
# Usage:
#   ./scripts/gradlew-elassandra-sidecar.sh :server:compileJava
#   ELASSANDRA_HOME=/path/to/elassandra ./scripts/gradlew-elassandra-sidecar.sh :server:test ...
#
# Or set the jar explicitly:
#   ELASSANDRA_CASSANDRA_JAR=/path/to/elassandra-cassandra-x.y.z.jar ./scripts/gradlew-elassandra-sidecar.sh …
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
INIT="$ROOT/gradle/opensearch-sidecar-elassandra.init.gradle"

if [[ ! -f "$INIT" ]]; then
  echo "Missing $INIT" >&2
  exit 1
fi

if [[ -z "${JAVA11_HOME:-}" ]] && [[ -n "${JAVA_HOME:-}" ]] && [[ -d "${JAVA_HOME}" ]]; then
  export JAVA11_HOME="$JAVA_HOME"
fi
if [[ -z "${JAVA11_HOME:-}" ]] && [[ -x /usr/libexec/java_home ]]; then
  JAVA11_HOME="$(/usr/libexec/java_home -v 11 2>/dev/null || true)"
  export JAVA11_HOME
fi
if [[ -z "${JAVA11_HOME:-}" ]]; then
  echo "JAVA11_HOME is not set (OpenSearch global-build-info + Elassandra BuildPlugin need it)." >&2
  echo "Example: export JAVA11_HOME=\"\$(/usr/libexec/java_home -v 11)\"  # or export JAVA11_HOME=\"\$JAVA_HOME\"" >&2
  exit 1
fi
export JAVA_HOME="${JAVA_HOME:-$JAVA11_HOME}"

JAR="${ELASSANDRA_CASSANDRA_JAR:-}"
if [[ -z "$JAR" || ! -f "$JAR" ]]; then
  for dir in \
    "${ELASSANDRA_HOME:+$ELASSANDRA_HOME/server/cassandra/build}" \
    "$ROOT/../elassandra/server/cassandra/build"; do
    if [[ -d "$dir" ]]; then
      # shellcheck disable=SC2012
      cand="$(ls -t "$dir"/elassandra-cassandra-*.jar 2>/dev/null | head -1)"
      if [[ -n "${cand:-}" && -f "$cand" ]]; then
        JAR="$cand"
        break
      fi
    fi
  done
fi

if [[ -z "$JAR" || ! -f "$JAR" ]]; then
  echo "Could not find elassandra-cassandra-*.jar." >&2
  echo "Build the Elassandra cassandra jar first (from the elassandra repo):" >&2
  echo "  ./scripts/build-elassandra-cassandra-jar.sh" >&2
  echo "  # or: export JAVA11_HOME=\"\$(/usr/libexec/java_home -v 11)\" && ./gradlew --stop && ./gradlew :cassandra-jar" >&2
  echo "Then set ELASSANDRA_HOME or ELASSANDRA_CASSANDRA_JAR, or keep this repo next to ../elassandra." >&2
  exit 1
fi

echo "[gradlew-elassandra-sidecar] JAVA11_HOME=$JAVA11_HOME" >&2
echo "[gradlew-elassandra-sidecar] elassandra.cassandra.jar=$JAR" >&2

export ELASSANDRA_CASSANDRA_JAR="$JAR"
# -P is applied on every build (reliable); -D/env can be invisible to a long-lived Gradle daemon.
exec ./gradlew --init-script "$INIT" -Pelassandra.cassandra.jar="$JAR" -Delassandra.cassandra.jar="$JAR" "$@"
