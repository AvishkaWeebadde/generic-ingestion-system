# stage 1 - build
FROM maven:3.9-eclipse-temurin-17 AS build

WORKDIR /build

# optimization - docker caches downloaded dependencies that changes only when pom.xml changes
COPY pom.xml .
RUN mvn dependency:go-offline

COPY src ./src
RUN mvn package -DskipTests

# stage 2 - run
FROM eclipse-temurin:17-jre-alpine AS run

WORKDIR /app

RUN addgroup -S app && adduser -S app -G app

 # for running the tests
COPY src/main/resources/webhooks.txt .
COPY --from=build /build/target/app.jar app.jar

USER app

ENTRYPOINT ["java", "-jar", "app.jar"]