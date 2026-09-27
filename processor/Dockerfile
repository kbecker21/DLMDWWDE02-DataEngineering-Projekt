FROM spark:4.1.3-scala2.13-java21-python3-ubuntu

# Kafka connector baked in instead of --packages on every start, versions as
# pinned by the connector POM. 4.1.3 instead of 4.1.2 because of SPARK-55271.
USER root
ARG MAVEN=https://repo1.maven.org/maven2
RUN set -eux; \
    cd /opt/spark/jars; \
    curl -fSLO $MAVEN/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.3/spark-sql-kafka-0-10_2.13-4.1.3.jar; \
    curl -fSLO $MAVEN/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.3/spark-token-provider-kafka-0-10_2.13-4.1.3.jar; \
    curl -fSLO $MAVEN/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar; \
    curl -fSLO $MAVEN/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar

# the named volume takes over this ownership when it is created
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
