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
