FROM apache/airflow:2.9.2-python3.10

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    curl \
    ca-certificates \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

RUN mkdir -p /opt/airflow/jars \
    && curl -fsSL \
       https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar \
       -o /opt/airflow/jars/mysql-connector-j-8.2.0.jar \
    && chown -R airflow:root /opt/airflow/jars

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    minio \
    python-docx \
    pdfplumber \
    boto3 \
    psycopg2-binary \
    google-genai
