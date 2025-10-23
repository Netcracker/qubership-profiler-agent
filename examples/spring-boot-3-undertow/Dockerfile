ARG IMAGE
FROM ${IMAGE:-alpine/openjdk17:17.0.7.7.05}

### Star update agent inside the image
ENV APP_UID=10001

USER root

COPY target/agent/*.zip /tmp/installer-cloud.zip

# Replace in-built archive with profiler agent to latest version
RUN rm -rf /app/volumes/ncdiag/config/* \
    && unzip -oq /tmp/installer-cloud.zip -d /app/volumes/ncdiag \
    && rm -rf /tmp/installer-cloud.zip \
    && chmod a+rwx -R /app/volumes/ncdiag \
    && chown -R ${APP_UID} /app/volumes/ncdiag

USER ${APP_UID}
### End

COPY target/*.jar /app/app-example-1.0.0.jar

CMD ["/usr/bin/java", "-Xmx512m", "-Djava.security.egd=file:/dev/./urandom", "-XX:-OmitStackTraceInFastThrow", "-jar", "/app/app-example-1.0.0.jar"]
