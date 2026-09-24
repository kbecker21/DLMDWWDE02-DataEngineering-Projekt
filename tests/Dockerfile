# same base as the processor, so the Spark tests run on the same version
FROM spark:4.1.3-scala2.13-java21-python3-ubuntu

USER root
RUN pip install --no-cache-dir pytest==9.1.1 pyarrow==22.0.0 confluent-kafka==2.15.0 "psycopg[binary]==3.3.5" \
    && ln -s /opt/spark/python/lib/py4j-*-src.zip /opt/spark/python/lib/py4j-src.zip
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-src.zip \
    PYTHONDONTWRITEBYTECODE=1
USER spark

# the repo is mounted read-only at /app
WORKDIR /app
CMD ["python3", "-m", "pytest"]
