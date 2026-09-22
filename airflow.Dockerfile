FROM apache/airflow:2.9.2-python3.10

USER root

# Cài Java (cần cho PySpark)
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

# Cài tất cả packages một lần khi build image
RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    minio \
    python-docx \
    pdfplumber \
    boto3 \
    psycopg2-binary \
    google-genai
