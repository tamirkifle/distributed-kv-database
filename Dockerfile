# syntax=docker/dockerfile:1

# ---- build stage: compile + shade the runnable jar ----
FROM maven:3.9-eclipse-temurin-11 AS build
WORKDIR /build
# Prime the dependency cache from the POM (best-effort; the package step re-fetches if needed).
COPY pom.xml .
RUN mvn -B -q dependency:go-offline || true
COPY src ./src
RUN mvn -B -q -DskipTests clean package

# ---- runtime stage: slim JRE with the shaded jar ----
FROM eclipse-temurin:11-jre
WORKDIR /app
# curl backs the container healthcheck against /health.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /build/target/ledgerkv.jar /app/ledgerkv.jar
# gRPC (9090) + health (8080); overridable via env.
EXPOSE 9090 8080
ENTRYPOINT ["java", "-jar", "/app/ledgerkv.jar"]
