# Offline Build Guide

This project ships with a Maven wrapper and a local `settings.xml` that defines a mirror for Maven Central and an `offline-ready` profile.  Once the dependency cache is warmed, the project can be built without internet access.

## 1. Prime the Maven cache (online)

```bash
cd apps/api
./mvnw -B -DskipTests dependency:go-offline
# Optional: archive ~/.m2 as m2-cache-<commit>
```

## 2. Restore and build offline

```bash
# Restore the archived ~/.m2 directory if needed
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

