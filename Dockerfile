# Stage 1: Build the Uberjar (amd64 and arm64)
FROM clojure:temurin-21-tools-deps AS build
WORKDIR /app
COPY deps.edn .
COPY build.clj .
COPY ./src src
RUN clj -T:build uber

# Stage 2: Create a minimal runtime image (amd64 and arm64). Java 21, the
# same major version as the desktop runtime (desktop/JDK_VERSION). Jetty 12
# needs 17 or later; the old openjdk:11 Debian buster image is end of life.
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=build /app/target/pine-standalone.jar /app/pine.jar
CMD ["java", "-jar", "pine.jar"]
