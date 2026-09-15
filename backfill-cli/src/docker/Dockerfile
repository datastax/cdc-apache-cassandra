ARG JRE_IMAGE_TAG
FROM eclipse-temurin:${JRE_IMAGE_TAG}
ARG BUILD_VERSION
COPY backfill-cli-${BUILD_VERSION}-all.jar /backfill-cli.jar
ENTRYPOINT ["sleep", "infinity"]
