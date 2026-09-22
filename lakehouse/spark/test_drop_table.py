import os
from pyspark.sql import SparkSession
from env_config import MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT, NESSIE_API_URL, HADOOP_HOME, SPARK_LOCAL_IP

os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PATH"] = os.path.join(HADOOP_HOME, "bin") + ";" + os.environ.get("PATH", "")
os.environ["AWS_ACCESS_KEY_ID"] = MINIO_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
os.environ["SPARK_LOCAL_IP"] = SPARK_LOCAL_IP
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4 "
    "pyspark-shell"
)

spark = (
    SparkSession.builder
    .appName("TestDropTable")
    .config("spark.driver.host", SPARK_LOCAL_IP)
    .config("spark.driver.bindAddress", SPARK_LOCAL_IP)
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
            "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
    .config("spark.sql.catalog.lakehouse", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lakehouse.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
    .config("spark.sql.catalog.lakehouse.uri", NESSIE_API_URL)
    .config("spark.sql.catalog.lakehouse.warehouse", "s3a://university-lakehouse/iceberg-warehouse")
    .config("spark.sql.catalog.lakehouse.s3.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

try:
    spark.sql("DROP TABLE IF EXISTS lakehouse.gold.kpi_tong_hop_don_vi")
    print("Dropped kpi_tong_hop_don_vi successfully")
except Exception as e:
    print(f"Failed to drop kpi_tong_hop_don_vi: {e}")

try:
    spark.sql("DROP TABLE IF EXISTS lakehouse.gold.kpi_chi_tiet_dashboard")
    print("Dropped kpi_chi_tiet_dashboard successfully")
except Exception as e:
    print(f"Failed to drop kpi_chi_tiet_dashboard: {e}")

try:
    spark.sql("DROP TABLE IF EXISTS lakehouse.gold.kpi_so_sanh_ky")
    print("Dropped kpi_so_sanh_ky successfully")
except Exception as e:
    print(f"Failed to drop kpi_so_sanh_ky: {e}")

try:
    spark.sql("DROP TABLE IF EXISTS lakehouse.gold.dm_chi_tieu")
    print("Dropped dm_chi_tieu successfully")
except Exception as e:
    print(f"Failed to drop dm_chi_tieu: {e}")

spark.stop()
