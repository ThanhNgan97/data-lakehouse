FROM apache/airflow:2.9.2-python3.10

USER root

# Cài Java (cần cho PySpark)
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /home/airflow/.ivy2/cache /home/airflow/.ivy2/jars /opt/spark/extra-jars \
    && chown -R airflow:root /home/airflow/.ivy2 /opt/spark/extra-jars

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SPARK_EXTRA_JARS_DIR=/opt/spark/extra-jars

USER airflow

# Cài tất cả packages một lần khi build image
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    minio \
    python-docx \
    pdfplumber \
    boto3 \
    psycopg2-binary \
    openpyxl \
    xlrd \
    google-genai

# Resolve JVM dependencies once while building the immutable image. Runtime
# DAGs load these local JARs and do not need Maven or a machine-specific Ivy
# cache. spark-submit also resolves transitive dependencies (for example the
# AWS SDK required by hadoop-aws).
COPY --chown=airflow:root scripts/resolve_spark_dependencies.py /tmp/resolve_spark_dependencies.py

RUN spark-submit \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,org.apache.hadoop:hadoop-aws:3.3.4 \
    /tmp/resolve_spark_dependencies.py \
    && cp /home/airflow/.ivy2/jars/*.jar /opt/spark/extra-jars/ \
    && rm -rf /home/airflow/.ivy2 /tmp/resolve_spark_dependencies.py
