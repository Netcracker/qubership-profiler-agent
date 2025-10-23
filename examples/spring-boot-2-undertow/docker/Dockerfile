ARG IMAGE
FROM ${IMAGE:-centos7/openjdk11-x86:11.0.23.01}

COPY target/*.jar /app/app-example-1.0.0.jar

CMD ["/usr/bin/java", "-Xmx512m", "-Djava.security.egd=file:/dev/./urandom", "-XX:-OmitStackTraceInFastThrow", "-jar", "/app/app-example-1.0.0.jar"]
