FROM spark:4.1.3-scala2.13-java21-python3-ubuntu

# Kafka connector JARs are baked in at build time instead of a --packages
# download on every start. Versions must match Spark (4.1.3 / Scala 2.13;
# 4.1.2 restarts into a NullPointerException in the Kafka source metrics
# whenever a batch is replayed after a crash, SPARK-55271);
# kafka-clients and commons-pool2 are the versions the connector POM pins.
USER root
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; \
    cd /opt/spark/jars; \
    curl -fSLO $MAVEN/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.3/spark-sql-kafka-0-10_2.13-4.1.3.jar; \
    curl -fSLO $MAVEN/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.3/spark-token-provider-kafka-0-10_2.13-4.1.3.jar; \
    curl -fSLO $MAVEN/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar; \
    curl -fSLO $MAVEN/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar

# checkpoint dir is owned by the unprivileged spark user; the named volume
# mounted there inherits this ownership on first creation
RUN mkdir -p /checkpoints && chown spark:spark /checkpoints

# Postgres driver for the foreachBatch upsert (plain JDBC has no upsert)
RUN pip install --no-cache-dir "psycopg[binary]==3.3.5"
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
