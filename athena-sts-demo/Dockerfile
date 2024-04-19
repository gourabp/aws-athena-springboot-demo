FROM openjdk:17
MAINTAINER gourabp
COPY target/athena-sts-demo.jar athena-sts-demo.jar

ENTRYPOINT [ \
    "java", \
    "-Djava.net.preferIPv4Stack=true", \
    "-jar", \
    "athena-sts-demo.jar", \
    "-Xms512m -Xmx1G" \
]
