# Offline Build Guide

This project uses the Maven wrapper and a local `settings.xml` that defines a mirror for Maven Central and an `offline-ready` profile. Once the dependency cache is warmed, the project can be built without internet access.

## 1. Prime the Maven cache (online)

```bash
scripts/prime-m2.sh       # downloads all dependencies
# archive the cache for reuse
tar -C ~/.m2 -c repository | zstd -T0 > m2-cache-$(git rev-parse HEAD).tar.zst
```

In CI, the `prime-cache` job performs the same warm‑up and uploads the Maven
repository as an artifact named `m2-cache`. A cross‑run cache keyed by
`m2-${OS}-${hashFiles('**/pom.xml')}` avoids re‑priming when dependencies
haven't changed.

## 2. Restore and build offline

```bash
# Extract previously archived cache or download the m2-cache artifact
mkdir -p ~/.m2 && tar --use-compress-program=unzstd -xf m2-cache-<commit>.tar.zst -C ~/.m2
cd apps/api
./mvnw -q -o -Poffline-ready -DskipTests package
```

The fat JAR is generated under `apps/api/target/`.

## 3. Run and verify an endpoint

```bash
java -jar target/*.jar &
sleep 10
curl -i http://localhost:8080/ops/upstox-balance
```

The unauthenticated request returns HTTP 401 with body:

```json
{"error":"unauthorized"}
```

## 4. Toggling online/offline

- **Online build:** `./mvnw -DskipTests package`
- **Offline build:** `./mvnw -q -o -Poffline-ready -DskipTests package`

## 5. Re-priming the cache

When dependencies change, rerun `scripts/prime-m2.sh` and archive the new `~/.m2/repository`.
