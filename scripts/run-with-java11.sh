#!/usr/bin/env bash
# OpenSearch Gradle applies 'opensearch.global-build-info', which requires JAVA11_HOME.
# Usage: ./scripts/run-with-java11.sh :server:test ...
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

if [[ -z "${JAVA11_HOME:-}" ]]; then
  if [[ -x /usr/libexec/java_home ]]; then
    JAVA11_HOME="$(/usr/libexec/java_home -v 11 2>/dev/null || true)"
    export JAVA11_HOME
  fi
fi

if [[ -z "${JAVA11_HOME:-}" ]]; then
  echo "JAVA11_HOME is not set and could not be resolved (e.g. install Temurin 11 and retry)." >&2
  echo "Example: export JAVA11_HOME=\"\$(/usr/libexec/java_home -v 11)\"" >&2
  echo "Then: ./gradlew --stop   # so the daemon picks up the new env" >&2
  exit 1
fi

exec ./gradlew "$@"
