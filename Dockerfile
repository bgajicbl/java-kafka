# ── Build stage ────────────────────────────────────────────────────
FROM eclipse-temurin:21-jdk-alpine AS build
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN apk add --no-cache maven \
 && mvn -q package -DskipTests

# ── Runtime stage ─────────────────────────────────────────────────
FROM eclipse-temurin:21-jre-alpine

RUN addgroup -S appgroup && adduser -S appuser -G appgroup
WORKDIR /app
COPY --from=build /app/target/*.jar app.jar

# RocksDB state directory — mount a PVC here in K8s
RUN mkdir -p /var/kafka-streams && chown appuser:appgroup /var/kafka-streams

USER appuser
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=40s --retries=3 \
  CMD wget -qO- http://localhost:8080/actuator/health || exit 1

ENTRYPOINT ["java", \
  "--enable-native-access=ALL-UNNAMED", \
  "-Dspring.profiles.active=k8s", \
  "-Dspring.kafka.streams.properties.state.dir=/var/kafka-streams", \
  "-jar", "app.jar"]

