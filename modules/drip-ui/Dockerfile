FROM clojure:temurin-21-tools-deps AS builder
WORKDIR /app

COPY src /app/src
COPY resources /app/resources
COPY deps.edn /app/deps.edn
COPY build.clj /app/build.clj

RUN clojure -T:build uber

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=builder /app/target/drip-ui.jar drip-ui.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "drip-ui.jar"]
