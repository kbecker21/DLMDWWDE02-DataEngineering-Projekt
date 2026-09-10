FROM spark:4.1.2-scala2.13-java21-python3-ubuntu

# Kafka connector JARs are baked in at build time instead of a --packages
# download on every start. Versions must match Spark (4.1.2 / Scala 2.13);
# kafka-clients and commons-pool2 are the versions the connector POM pins.
USER root
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; \
    cd /opt/spark/jars; \
    curl -fSLO $MAVEN/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.2/spark-sql-kafka-0-10_2.13-4.1.2.jar; \
    curl -fSLO $MAVEN/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.2/spark-token-provider-kafka-0-10_2.13-4.1.2.jar; \
    curl -fSLO $MAVEN/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar; \
    curl -fSLO $MAVEN/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar

# checkpoint dir is owned by the unprivileged spark user; the named volume
# mounted there inherits this ownership on first creation
RUN mkdir -p /checkpoints && chown spark:spark /checkpoints
COPY log4j2.properties /opt/spark/conf/
USER spark

WORKDIR /app
COPY streaming_job.py .

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "local[2]", \
     "--conf", "spark.driver.memory=2g", \
     "--conf", "spark.sql.shuffle.partitions=12", \
     "--conf", "spark.sql.session.timeZone=UTC", \
     "--conf", "spark.ui.showConsoleProgress=false", \
     "/app/streaming_job.py"]
