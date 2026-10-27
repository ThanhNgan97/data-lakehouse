ARG JAR_SOURCE_IMAGE=custom-airflow:2.9.2
FROM ${JAR_SOURCE_IMAGE} AS spark-jars

FROM apache/spark:3.5.0

USER root

COPY spark-runtime-requirements.txt /tmp/spark-runtime-requirements.txt
COPY wheelhouse/ /tmp/wheelhouse/

RUN python3 -m pip install --no-cache-dir --no-index \
      --find-links=/tmp/wheelhouse \
      -r /tmp/spark-runtime-requirements.txt \
    && rm -rf /tmp/wheelhouse /tmp/spark-runtime-requirements.txt

COPY --from=spark-jars /opt/spark/extra-jars/*.jar /opt/spark/jars/
COPY --chown=185:185 . /opt/lakehouse/spark/

ENV PYTHONPATH=/opt/lakehouse/spark

WORKDIR /opt/lakehouse/spark
USER 185
