FROM clojure:temurin-21-tools-deps AS builder
WORKDIR /app

COPY modules/drip-ui/src /app/src
COPY modules/drip-ui/resources /app/resources
COPY modules/drip-ui/deps.edn /app/deps.edn
COPY modules/drip-ui/build.clj /app/build.clj

RUN clojure -T:build uber

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=builder /app/target/drip-ui.jar drip-ui.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "drip-ui.jar"]
