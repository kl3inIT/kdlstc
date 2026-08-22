FROM docker.io/library/eclipse-temurin:25-jdk-noble AS build

WORKDIR /workspace
COPY gradlew settings.gradle build.gradle ./
COPY gradle ./gradle
RUN chmod +x gradlew && ./gradlew --no-daemon dependencies

COPY src ./src
RUN ./gradlew --no-daemon bootJar

FROM docker.io/library/eclipse-temurin:25-jre-noble
RUN groupadd --gid 10001 application \
    && useradd --uid 10001 --gid application --create-home --shell /usr/sbin/nologin application

WORKDIR /app
COPY --from=build --chown=application:application /workspace/build/libs/application.jar ./application.jar

USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/application.jar"]
