from pyspark.sql import SparkSession
import os

os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'
os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-java-options "-Djava.net.preferIPv4Stack=true" --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,org.apache.hadoop:hadoop-aws:3.3.4 pyspark-shell'

spark = SparkSession.builder \
    .config('spark.driver.host', '127.0.0.1') \
    .config('spark.driver.bindAddress', '127.0.0.1') \
    .config('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions') \
    .config('spark.sql.catalog.lakehouse', 'org.apache.iceberg.spark.SparkCatalog') \
    .config('spark.sql.catalog.lakehouse.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog') \
    .config('spark.sql.catalog.lakehouse.uri', 'http://localhost:19120/api/v1') \
    .config('spark.sql.catalog.lakehouse.warehouse', 's3a://university-lakehouse/iceberg-warehouse') \
    .config('spark.sql.catalog.lakehouse.s3.endpoint', 'http://127.0.0.1:9000') \
    .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem') \
    .config('spark.hadoop.fs.s3a.endpoint', 'http://127.0.0.1:9000') \
    .config('spark.hadoop.fs.s3a.access.key', 'minioadmin') \
    .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin') \
    .config('spark.hadoop.fs.s3a.path.style.access', 'true') \
    .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false') \
    .getOrCreate()

spark.sql('USE REFERENCE main IN lakehouse')
spark.sql('DELETE FROM lakehouse.silver.kpi_cusc_master WHERE quy_danh_gia = "UNKNOWN_KY"')
print('Deleted UNKNOWN_KY from main!')
