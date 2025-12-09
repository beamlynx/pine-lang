# Stage 1: Build the Uberjar (amd64 and arm64)
FROM clojure:tools-deps-1.10.3.1020 AS build
WORKDIR /app
COPY deps.edn .
COPY build.clj .
COPY ./src src
RUN clj -T:build uber

# Stage 2: Create a minimal runtime image (amd64 and arm64)
FROM openjdk:11.0.16-jre-slim-buster
WORKDIR /app
COPY --from=build /app/target/pine-standalone.jar /app/pine.jar
COPY src/pine/pine.bnf /app/src/pine/pine.bnf
CMD ["java", "-jar", "pine.jar"]
