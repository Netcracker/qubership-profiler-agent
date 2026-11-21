ARG IMAGE=alpine/openjdk21:21.0.3.9.04
FROM ${IMAGE}

COPY target/*.jar /app/app-example-1.0.0.jar

USER 10001

CMD ["/usr/bin/java", "-Xmx512m", "-Djava.security.egd=file:/dev/./urandom", "-XX:-OmitStackTraceInFastThrow", "-jar", "/app/app-example-1.0.0.jar"]
