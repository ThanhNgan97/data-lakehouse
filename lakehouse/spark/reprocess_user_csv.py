# -*- coding: utf-8 -*-
import boto3
import os
import sys

csv_content = """ma_chi_tieu,noi_dung_muc_tieu,dinh_ky_thu_thap,muc_dang_ky,muc_dat,ket_qua_he_thong,nguyen_nhan,hanh_dong_khac_phuc,quy_danh_gia
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
    s3 = boto3.client(
        "s3",
        endpoint_url="http://minio:9000" if os.path.exists("/.dockerenv") else "http://127.0.0.1:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin"
    )
    
    file_key = "staging/45e005a7b7054594b592471948098d72_danh_gia_muc_tieu_kpi.csv"
    s3.put_object(Bucket="university-lakehouse", Key=file_key, Body=csv_content.encode("utf-8"))
    print(f"✅ Uploaded test file to S3: {file_key}")
    
    os.system(f"python /opt/airflow/spark/spark_ingest_bronze.py --file_key {file_key}")
    os.system("python /opt/airflow/spark/spark_bronze_to_silver.py")
    os.system("python /opt/airflow/spark/spark_silver_to_gold.py")
    print("🎉 Ingestion & Pipeline run complete for danh_gia_muc_tieu_kpi.csv!")

if __name__ == "__main__":
    main()
