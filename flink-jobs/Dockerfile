# Build the job jar, then layer it onto the official Flink image.
FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY schemas /src/schemas
COPY flink-jobs/pom.xml /src/flink-jobs/pom.xml
RUN cd flink-jobs && mvn -q -B dependency:go-offline
COPY flink-jobs/src /src/flink-jobs/src
RUN cd flink-jobs && mvn -q -B package -DskipTests \
 && mvn -q -B dependency:copy-dependencies -DincludeArtifactIds=hadoop-client-api,hadoop-client-runtime,commons-logging -DincludeScope=provided \
      -DoutputDirectory=target/flink-lib

FROM flink:1.20.1-scala_2.12-java17
COPY --from=build /src/flink-jobs/target/paystream-flink-0.1.0.jar /opt/paystream/paystream-flink.jar
# Hadoop client for Iceberg's catalog; it must be on Flink's own classpath (see pom.xml)
COPY --from=build /src/flink-jobs/target/flink-lib/ /opt/flink/lib/
