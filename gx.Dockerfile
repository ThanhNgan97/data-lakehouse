FROM python:3.10-slim-bookworm

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    openjdk-17-jre-headless \
    procps \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/gx /opt/spark/extra-jars /home/gx/.ivy2 \
    && useradd --create-home --home-dir /home/gx --uid 10001 gx \
    && chown -R gx:gx /opt/gx /opt/spark /home/gx

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV SPARK_EXTRA_JARS_DIR=/opt/spark/extra-jars
ENV PYTHONPATH=/opt/gx
ENV GX_ANALYTICS_ENABLED=false
ENV PATH=/home/gx/.local/bin:${PATH}

COPY --chown=gx:gx lakehouse/gx_runtime/requirements.txt /tmp/requirements.txt

USER gx

RUN pip install --no-cache-dir --user -r /tmp/requirements.txt

COPY --chown=gx:gx scripts/resolve_spark_dependencies.py /tmp/resolve_spark_dependencies.py

RUN pyspark --version >/dev/null \
    && spark-submit \
       --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,org.apache.hadoop:hadoop-aws:3.3.4 \
       /tmp/resolve_spark_dependencies.py \
    && cp /home/gx/.ivy2/jars/*.jar /opt/spark/extra-jars/ \
    && rm -rf /home/gx/.ivy2 /tmp/resolve_spark_dependencies.py

COPY --chown=gx:gx lakehouse/gx_runtime /opt/gx/gx_runtime
COPY --chown=gx:gx lakehouse/spark/gx_validation_request.py /opt/gx/gx_validation_request.py
COPY --chown=gx:gx lakehouse/gx /opt/gx/config

WORKDIR /opt/gx

HEALTHCHECK --interval=10s --timeout=5s --start-period=10s --retries=12 \
    CMD ["python", "-m", "gx_runtime.healthcheck", "ready"]

ENTRYPOINT ["python", "-m", "gx_runtime.smoke"]
CMD ["imports"]
