ARG IMAGE
FROM ${IMAGE:-alpine/openjdk17:17.0.11.9.04}

COPY target/*.jar /app/app-example-1.0.0.jar

CMD ["/usr/bin/java", "-Xmx512m", "-Djava.security.egd=file:/dev/./urandom", "-XX:-OmitStackTraceInFastThrow", "-jar", "/app/app-example-1.0.0.jar"]
