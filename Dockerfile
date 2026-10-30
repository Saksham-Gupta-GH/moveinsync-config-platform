# Multi-stage Docker build for MoveInSync Configuration Platform
# Stage 1: Build
FROM eclipse-temurin:17-jdk-jammy AS build
WORKDIR /app

# Copy Maven Wrapper & POMs
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
COPY config-core/pom.xml config-core/
COPY config-client-sdk/pom.xml config-client-sdk/
COPY config-server/pom.xml config-server/

# Download dependencies
RUN ./mvnw dependency:go-offline -B || true

# Copy Source Code & Build
COPY config-core/src config-core/src
COPY config-client-sdk/src config-client-sdk/src
COPY config-server/src config-server/src

RUN ./mvnw clean package -DskipTests

# Stage 2: Production JRE Runtime
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app

# Install curl for healthcheck
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copy executable jar from build stage
COPY --from=build /app/config-server/target/config-server-1.0.0-SNAPSHOT.jar app.jar

# Configuration Data Volume
VOLUME /app/data
ENV SPRING_DATASOURCE_URL="jdbc:sqlite:/app/data/config_server.db"
ENV SERVER_PORT=8080

EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --retries=5 --start-period=10s \
  CMD curl -f http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["java", "-XX:+UseG1GC", "-XX:MaxRAMPercentage=75.0", "-jar", "app.jar"]
