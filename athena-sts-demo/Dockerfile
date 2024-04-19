FROM openjdk:17
MAINTAINER gourabp
COPY target/athena-sts-demo.jar athena-sts-demo.jar

ENTRYPOINT [ \
    "java", \
    ##"-Dspring.profiles.active=default", \
    ##"-Dspring.config.location=/app/config/,optional:/app/shared-config/", \
    "-Djava.net.preferIPv4Stack=true", \
    "-jar", \
    "athena-sts-demo.jar", \
    "-Xms512m -Xmx1G" \
]
