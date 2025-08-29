# syntax=docker/dockerfile:1
FROM maven:3.9.7-eclipse-temurin-17 AS build
WORKDIR /app
# Copy just pom + wrapper first to leverage Docker cache
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
RUN chmod +x mvnw

# Pre-fetch deps (faster, fewer network surprises)
RUN ./mvnw -B -V -e -DskipFrontend=true -Dmaven.test.skip=true dependency:go-offline

# Now copy sources and build
COPY src ./src
RUN ./mvnw -B -V -e clean package -DskipFrontend=true -Dmaven.test.skip=true -Pci

FROM eclipse-temurin:17-jre
WORKDIR /app
COPY --from=build /app/target/backend-0.0.1-SNAPSHOT.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java","-jar","/app/app.jar"]
