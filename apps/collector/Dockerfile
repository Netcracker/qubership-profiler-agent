FROM openjdk:21-jdk AS build

WORKDIR /app

ENV CP_UI="https://github.com/pd.sandbox.npm.staging/%40netcracker/cloud-profiler-ui/-/%40netcracker/cloud-profiler-ui"

ARG CP_UI_VERSION
ENV CP_UI_VERSION=$CP_UI_VERSION

COPY mvnw pom.xml ./
COPY .mvn .mvn
RUN chmod +x ./mvnw

# Cache some dependencies on an earlier layer
RUN ./mvnw --batch-mode clean package -Dmaven.main.skip -Dmaven.test.skip -B && rm -r target

COPY src src

RUN curl -o /tmp/cloud-profiler-ui.tgz "${CP_UI}-${CP_UI_VERSION}.tgz" \
    && tar -xvzf /tmp/cloud-profiler-ui.tgz -C /tmp \
    && cp -fR /tmp/package/build/* src/main/resources/META-INF/resources/

RUN ./mvnw --batch-mode clean package -Dmaven.test.skip=true -Dmaven.javadoc.skip=true -V -B

FROM alpine/openjdk21:21.0.8.9.01 AS runtime
ENV APP_UID=10001 \
    JVM_KEYSTORE="/etc/ssl/certs/java/cacerts"

WORKDIR /app

USER root

# Copy the build artifacts from the build stage
COPY --from=build /app/target/app/lib/ /app/lib/
COPY --from=build /app/target/app/*.jar /app/
COPY --from=build /app/target/app/app/ /app/app/
COPY --from=build /app/target/app/quarkus/ /app/quarkus/
RUN chown -R ${APP_UID} /app
USER ${APP_UID}

EXPOSE 1710 1715 5005 8080 8180

RUN mkdir /app/bin
COPY docker/start.sh /app/bin/start.sh
CMD ["/bin/bash", "/app/bin/start.sh"]
