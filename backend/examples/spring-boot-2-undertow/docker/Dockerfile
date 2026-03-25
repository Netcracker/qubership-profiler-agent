ARG IMAGE=alpine/openjdk17:17.0.7.7.05
FROM ${IMAGE}

COPY target/*.jar /app/app-example-1.0.0.jar

USER 10001

CMD ["/usr/bin/java", "-Xmx512m", "-Djava.security.egd=file:/dev/./urandom", "-XX:-OmitStackTraceInFastThrow", "-jar", "/app/app-example-1.0.0.jar"]
