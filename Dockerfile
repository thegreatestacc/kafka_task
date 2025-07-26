## Stage 1: Build layered jar
#FROM maven:3.9.6-eclipse-temurin-22 AS builder
#WORKDIR /build
#COPY pom.xml .
#COPY src ./src
#RUN mvn clean package -DskipTests spring-boot:repackage -Dspring-boot.repackage.layers.enabled=true
#
## Stage 2: Extract layers using layertools
#FROM eclipse-temurin:22-jre-jammy AS layertools
#WORKDIR /layers
#COPY --from=builder /build/target/*.jar app.jar
#RUN java -Djarmode=layertools -jar app.jar extract \
# && echo "--- [layertools] JarLauncher found:" \
# && find /layers -name "JarLauncher.class"
#
## Stage 3: Final runtime image
#FROM eclipse-temurin:22-jre-jammy AS runtime
#WORKDIR /app
#COPY --from=layertools /layers/dependencies/ ./
#COPY --from=layertools /layers/snapshot-dependencies/ ./
#COPY --from=layertools /layers/spring-boot-loader/ ./
#COPY --from=layertools /layers/application/ ./
#
## Проверка на наличие JarLauncher (debug)
#RUN echo "--- [runtime] in /app" && find . -name "JarLauncher.class"
#
#EXPOSE 8081
#ENTRYPOINT ["java", "org.springframework.boot.loader.JarLauncher"]

#получаю ошибку в контейнере приложения при docker-compose up -d
#2025-07-26 13:13:22 Error: Could not find or load main class org.springframework.boot.loader.JarLauncher
#2025-07-26 13:13:22 Caused by: java.lang.ClassNotFoundException: org.springframework.boot.loader.JarLauncher


# Stage 1: Build
FROM maven:3.9.6-eclipse-temurin-22 as builder
WORKDIR /app
COPY pom.xml .
COPY src ./src
RUN mvn clean package -DskipTests

# Stage 2: Run
FROM eclipse-temurin:22-jre-jammy
WORKDIR /app
COPY --from=builder /app/target/KafkaTask-0.0.1-SNAPSHOT.jar ./app.jar
EXPOSE 8081
ENTRYPOINT ["java", "-jar", "app.jar"]