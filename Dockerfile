FROM eclipse-temurin:17-jdk

ENV JAVA_HOME=/opt/java/openjdk
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /app
COPY . .

RUN chmod +x mvnw
RUN echo "JAVA_HOME is: $JAVA_HOME" && java -version
# Skip building the Angular frontend so Maven doesn't try to download Node
RUN ./mvnw clean package -DskipTests -DskipFrontend=true

CMD ["java", "-jar", "target/backend-0.0.1-SNAPSHOT.jar"]
