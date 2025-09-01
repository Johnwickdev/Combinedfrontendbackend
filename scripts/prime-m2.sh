#!/usr/bin/env bash
set -euo pipefail

# Prime the Maven local repository with all dependencies for offline builds.
SCRIPT_DIR=$(cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P)
PROJECT_ROOT="$SCRIPT_DIR/../apps/api"

cd "$PROJECT_ROOT"
echo "Priming Maven cache..."
./mvnw -B -DskipTests dependency:go-offline
echo "Resolved dependencies:"
./mvnw -B -DskipTests -Poffline-ready help:evaluate -Dexpression=project.dependencies | tail -n 20
