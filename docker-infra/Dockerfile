# ---------------------------------------------------------------------------
# Multi-stage image for both Spring Boot modules.
#
# MODULE selects the Gradle module to build, e.g. producer-service.
# Build from the repository root:
#   docker build -f docker-infra/Dockerfile --build-arg MODULE=producer-service .
# ---------------------------------------------------------------------------

# --- Stage 1: build the fat jar -------------------------------------------------
FROM eclipse-temurin:21-jdk AS build
WORKDIR /workspace

# Wrap the Gradle invocation so the demo should never fail on a too small metaspace.
ENV GRADLE_OPTS="-Dorg.gradle.daemon=false"

# Copy the build descriptors first: this layer is cached until the build itself changes.
COPY gradlew gradlew
COPY gradle gradle
COPY settings.gradle.kts build.gradle.kts gradle.properties ./
COPY order-contract/build.gradle.kts order-contract/
COPY producer-service/build.gradle.kts producer-service/
COPY consumer-service/build.gradle.kts consumer-service/

# Warm up dependencies (best effort: a missing network must not break the build below).
RUN ./gradlew --no-daemon --console=plain dependencies > /dev/null 2>&1 || true

# Copy the sources and build the requested module (tests run in CI, not in the image).
COPY order-contract/src order-contract/src
COPY producer-service/src producer-service/src
COPY consumer-service/src consumer-service/src

ARG MODULE
RUN test -n "$MODULE" || (echo "MODULE build-arg is required" && exit 1)
RUN ./gradlew --no-daemon --console=plain :${MODULE}:bootJar -x test

# --- Stage 2: runtime ----------------------------------------------------------
FROM eclipse-temurin:21-jre

# curl is needed by the compose healthcheck of the services.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# Never run the application as root.
RUN groupadd --system --gid 1001 spring \
    && useradd --system --uid 1001 --gid spring --shell /bin/bash spring

WORKDIR /app
ARG MODULE
COPY --from=build /workspace/${MODULE}/build/libs/ /app/libs/
RUN cp "$(find /app/libs -maxdepth 1 -name '*.jar' ! -name '*-plain.jar' | head -n 1)" /app/app.jar \
    && chown -R spring:spring /app

USER spring
EXPOSE 8080 8081

# Container-aware JVM settings: respect the cgroup limits instead of the host's RAM.
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/app.jar"]
