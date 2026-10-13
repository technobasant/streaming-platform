# syntax=docker/dockerfile:1.7
# Flink 1.20 with the Kafka SQL connector, Iceberg, an S3 filesystem for checkpoints
# and the Prometheus reporter. Every JAR is pinned by version and SHA-256.
FROM flink:1.20.2-scala_2.12-java17

ARG MVN=https://repo1.maven.org/maven2
ADD --checksum=sha256:b860724fd97d7da0901bfc3859680442f4c1aa2b35ab944407558b393a1cf93e \
    ${MVN}/org/apache/flink/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar /opt/flink/lib/
ADD --checksum=sha256:f06b3f2fbd004feeb10adc8957f27d43203a0dc526a9ae2e0a42219fbcdbcfe7 \
    ${MVN}/org/apache/iceberg/iceberg-flink-runtime-1.20/1.10.1/iceberg-flink-runtime-1.20-1.10.1.jar /opt/flink/lib/
ADD --checksum=sha256:86bf20892ea5b4c17688f19b075399885f6aa5303f6b2dc9f491e76ceef9633b \
    ${MVN}/org/apache/iceberg/iceberg-aws-bundle/1.10.1/iceberg-aws-bundle-1.10.1.jar /opt/flink/lib/
# Iceberg's Flink sink needs Hadoop's Configuration class even when nothing touches HDFS.
ADD --checksum=sha256:a964d4daa054c9615bbafb4553efbb140fa7fb9ac6f358a24393f183a5703438 \
    ${MVN}/org/apache/hadoop/hadoop-client-api/3.4.1/hadoop-client-api-3.4.1.jar /opt/flink/lib/
ADD --checksum=sha256:f6a800a159f918670db533606d33560d6c13b7e13f14eda493280ae33b9eeb2f \
    ${MVN}/org/apache/hadoop/hadoop-client-runtime/3.4.1/hadoop-client-runtime-3.4.1.jar /opt/flink/lib/
ADD --checksum=sha256:042aba3347f52c332a7f57fae0249d003cd3deac6f5cb8022d7891e7b06ca6f9 \
    ${MVN}/org/apache/flink/flink-metrics-prometheus/1.20.2/flink-metrics-prometheus-1.20.2.jar /opt/flink/lib/

# Checkpoints go to S3 (SeaweedFS) through the Presto S3 filesystem, loaded as a plugin.
RUN mkdir -p /opt/flink/plugins/s3-fs-presto \
 && cp /opt/flink/opt/flink-s3-fs-presto-1.20.2.jar /opt/flink/plugins/s3-fs-presto/ \
 && chmod 644 /opt/flink/lib/*.jar

COPY --chown=flink:flink flink/sql /opt/flink/sql
