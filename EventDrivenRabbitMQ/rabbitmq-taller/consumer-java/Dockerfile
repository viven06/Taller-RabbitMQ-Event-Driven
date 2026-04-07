FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /app

COPY pom.xml .
RUN mvn dependency:go-offline -q

COPY src ./src
RUN mvn package -q -DskipTests

# ---- runtime ----
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app
COPY --from=builder /app/target/consumer-java-1.0.0.jar app.jar

CMD ["java", "-jar", "app.jar"]
