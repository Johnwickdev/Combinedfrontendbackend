# Monorepo

## Building on Railway

Run the backend build with the Maven wrapper using the hardened settings:

```
./mvnw -B -s .mvn/settings.xml -f apps/api/pom.xml clean package
```

If tests are flaky you can skip them:

```
./mvnw -B -s .mvn/settings.xml -f apps/api/pom.xml -DskipTests package
```

## DBI5 STANDARD FLOW
Backend implements a self-sufficient market data flow. See `apps/api/README.md` for details on startup sequence and stable endpoints.
