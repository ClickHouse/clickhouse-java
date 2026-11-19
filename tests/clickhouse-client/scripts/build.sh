#!/usr/bin/env bash
# Builds the shim against the clickhouse-java source of this repository.
#
# Usage: scripts/build.sh [<extra Maven arguments for the shim build>...]
#
# 1. Installs the modules that the shim uses (clickhouse-jdbc and the modules
#    that it needs: jdbc-v2, client-v2, clickhouse-data, ...) from the current
#    source into the local Maven repository, without their tests. Set
#    SKIP_CLIENT_INSTALL=1 to use the modules that are already installed.
# 2. Packages the shim (fat jar target/clickhouse-client-cli-1.0.0.jar) against
#    exactly that version: the <revision> of the root pom.xml. The unit tests of
#    the shim run in this step (add -DskipTests to skip them).
#
# Maven needs JDK 17 with a toolchain for JDK 8 and 17, as for the rest of the
# repository (see CONTRIBUTING.md).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUNNER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
REPO_DIR="$(cd "${RUNNER_DIR}/../.." && pwd)"

revision="$(sed -n 's:.*<revision>\(.*\)</revision>.*:\1:p' "${REPO_DIR}/pom.xml" | head -n 1)"
if [[ -z "${revision}" ]]; then
  echo "Error: no <revision> in ${REPO_DIR}/pom.xml." >&2
  exit 1
fi

# Two build threads: enough for the few modules, and the memory use stays small.
mvn_args=(--batch-mode --no-transfer-progress --quiet -T 2)
if [[ "${SKIP_CLIENT_INSTALL:-0}" != "1" ]]; then
  echo "Installing clickhouse-jdbc ${revision} and its modules from ${REPO_DIR}" >&2
  (cd "${REPO_DIR}" && mvn "${mvn_args[@]}" -DskipTests -Dmaven.javadoc.skip=true -pl clickhouse-jdbc -am install)
fi
echo "Building the shim against clickhouse-java ${revision}" >&2
mvn "${mvn_args[@]}" -f "${RUNNER_DIR}/pom.xml" -Dclickhouse-java.version="${revision}" "$@" package
