# -*- coding: utf-8 -*-
import os
import sys

from env_config import (
    MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT,
    MINIO_BUCKET_NAME, NESSIE_API_URL, HADOOP_HOME, SPARK_LOCAL_IP,
)

os.environ["HADOOP_HOME"]           = HADOOP_HOME
os.environ["PATH"]                  = os.path.join(HADOOP_HOME, "bin") + ";" + os.environ.get("PATH", "")
os.environ["AWS_ACCESS_KEY_ID"]     = MINIO_ACCESS_KEY
os.environ["AWS_SECRET_ACCESS_KEY"] = MINIO_SECRET_KEY
os.environ["SPARK_LOCAL_IP"]        = SPARK_LOCAL_IP
os.environ["PYSPARK_SUBMIT_ARGS"]   = (
    "--driver-java-options \"-Djava.net.preferIPv4Stack=true\" "
    "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,"
    "org.apache.hadoop:hadoop-aws:3.3.4 "
    "pyspark-shell"
)

from spark_ingest_bronze import extract_structured_data, get_s3_client, process_single_file
from pyspark.sql import SparkSession
from nessie_catalog_utils import use_main
from spark_silver_to_gold import run_silver_to_gold

csv_data = """ma_chi_tieu,noi_dung_muc_tieu,dinh_ky_thu_thap,muc_dang_ky,muc_dat,ket_qua_he_thong,nguyen_nhan,hanh_dong_khac_phuc,quy_danh_gia
HT-MT01,Đảm bảo an toàn hệ thống,Quý,100%,100%,DAT,Tăng cường giám sát,Tiếp tục duy trì,Q1/2018
HT-MT02,Duy trì tỷ lệ sẵn sàng dịch vụ,Tháng,99%,99%,DAT,Theo dõi thường xuyên,Tiếp tục duy trì,Q1/2018
HT-MT03,Tỷ lệ xử lý sự cố đúng SLA,Tháng,95%,92%,KHONG_DAT,Thiếu nhân sự,Bổ sung nhân sự,Q1/2018
HT-MT04,Hoàn thành sao lưu dữ liệu,Quý,100%,100%,DAT,Tự động hóa quy trình,Đã hoàn thành,Q1/2018
PM-MT01,Tỷ lệ hoàn thành dự án phần mềm,Quý,90%,92%,DAT,Kiểm soát tiến độ tốt,Tiếp tục duy trì,Q1/2018
PM-MT02,Tỷ lệ yêu cầu được xử lý đúng hạn,Tháng,95%,96%,DAT,Phối hợp tốt,Đã hoàn thành,Q1/2018
PM-MT03,Hoàn thành kiểm thử trước phát hành,Quý,95%,90%,KHONG_DAT,Khối lượng công việc cao,Tăng cường kiểm thử,Q1/2018
PM-MT04,Tỷ lệ chức năng đạt nghiệm thu,Quý,90%,93%,DAT,Chuẩn hóa quy trình,Tiếp tục duy trì,Q1/2018
"""

def main():
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")

    print("1. Testing extract_structured_data on CSV content...")
    res = extract_structured_data(csv_data.encode("utf-8"), ".csv")
    if not res:
        print("FAIL: extract_structured_data returned None")
        return
    rows, quy, candidates = res
    print(f"Extracted {len(rows)} rows successfully! Sample row 0: {rows[0]}")

    print("\n2. Simulating Bronze Parquet creation...")
    s3 = get_s3_client()
    key = "staging/45e005a7b7054594b592471948098d72_danh_gia_muc_tieu_kpi.csv"
    s3.put_object(Bucket=MINIO_BUCKET_NAME, Key=key, Body=csv_data.encode("utf-8"))

    fkey, file_extracted, is_success = process_single_file(s3, key)
    print(f"process_single_file result: success={is_success}, rows={len(file_extracted)}")
    if file_extracted:
        print(f"file_nguon in extracted data: {file_extracted[0]['file_nguon']}")

    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    import io
    df_pd = pd.DataFrame(file_extracted)
    table = pa.Table.from_pandas(df_pd)
    buf = io.BytesIO()
    pq.write_table(table, buf)
    s3.put_object(Bucket=MINIO_BUCKET_NAME, Key="bronze/danh_gia_muc_tieu_kpi.parquet", Body=buf.getvalue())
    print("Uploaded test bronze parquet file to MinIO.")

    print("\n3. Running Bronze -> Silver -> Gold pipeline...")
    # Import and run silver pipeline
    from spark_bronze_to_silver import get_spark_session as get_silver_spark, main as run_silver
    # Call script directly
    os.system("python spark_bronze_to_silver.py")
    os.system("python spark_silver_to_gold.py")
    print("Done pipeline test!")

if __name__ == "__main__":
    main()
