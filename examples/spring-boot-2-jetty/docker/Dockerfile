ARG IMAGE
FROM ${IMAGE:-alpine/openjdk21:21.0.3.9.04}

COPY target/*.jar /app/app-example-1.0.0.jar

CMD ["/usr/bin/java", "-Xmx512m", "-Djava.security.egd=file:/dev/./urandom", "-XX:-OmitStackTraceInFastThrow", "-jar", "/app/app-example-1.0.0.jar"]
