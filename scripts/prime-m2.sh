#!/usr/bin/env bash
set -euo pipefail

# Prime the Maven local repository with all dependencies for offline builds.
SCRIPT_DIR=$(cd -- "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P)
PROJECT_ROOT="$SCRIPT_DIR/.."

cd "$PROJECT_ROOT"
echo "Priming Maven cache..."
./mvnw -q -f apps/api/pom.xml help:effective-settings | sed -n '/<localRepository>/,/<\/localRepository>/p'
./mvnw -B -DskipTests -Poffline-ready -f apps/api/pom.xml -pl :backend -am package
echo "Verifying required artifacts..."
ls -la "$HOME/.m2/repository/io/netty/netty-common/4.1.109.Final/"
ls -la "$HOME/.m2/repository/io/netty/netty-transport/4.1.109.Final/"
ls -la "$HOME/.m2/repository/io/netty/netty-codec-http/4.1.109.Final/"
ls -la "$HOME/.m2/repository/com/squareup/okhttp3/okhttp/4.12.0/"
ls -la "$HOME/.m2/repository/io/reactivex/rxjava3/rxjava/3.1.8/"
ls -la "$HOME/.m2/repository/org/jetbrains/kotlin/kotlin-stdlib-jdk8/1.9.23/"
ls -la "$HOME/.m2/repository/net/bytebuddy/byte-buddy/1.14.13/"
