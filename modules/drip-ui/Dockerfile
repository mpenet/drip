FROM clojure:temurin-21-tools-deps AS builder
WORKDIR /app
# Copy local deps first so Docker can cache the layer
COPY modules/drip /drip
COPY hirundo /hirundo
COPY modules/drip-ui /app
RUN clojure -T:build uber

FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=builder /app/target/drip-ui.jar drip-ui.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "drip-ui.jar"]
