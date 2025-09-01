# Offline Build Guide

This project uses the Maven wrapper and a local `settings.xml` that defines a mirror for Maven Central and an `offline-ready` profile. Once the dependency cache is warmed, the project can be built without internet access.

## 1. Prime the Maven cache (online)

```bash
scripts/prime-m2.sh       # downloads all dependencies and verifies cache
```

In CI, the `prime-cache` job runs the same commands, then uploads the Maven
repository as an artifact named `m2-cache`. A cross‑run cache keyed by
`m2-${OS}-${hashFiles('**/pom.xml')}` avoids re‑priming when dependencies
haven't changed.

## 2. Restore and build offline

```bash
# Extract the m2-cache artifact into ~/.m2 if not already present
./mvnw -q -o -Poffline-ready -f apps/api/pom.xml -pl :backend -am package
```

The fat JAR is generated under `apps/api/target/`.

## 3. Run and verify an endpoint

```bash
java -jar apps/api/target/*.jar &
sleep 10
curl -i http://localhost:8080/ops/upstox-balance
```

The unauthenticated request returns HTTP 401 with body:

```json
{"error":"unauthorized"}
```

## 4. Toggling online/offline

- **Online build:** `./mvnw -f apps/api/pom.xml -DskipTests -Poffline-ready -pl :backend -am package`
- **Offline build:** `./mvnw -q -o -Poffline-ready -f apps/api/pom.xml -pl :backend -am package`

## 5. Re-priming the cache

When dependencies change, rerun `scripts/prime-m2.sh`. The cache key uses the
hash of all `pom.xml` files, so CI will automatically re-prime when those
files change.
