import { useEffect, useState } from "react";
import {
  ArrowRight,
  BarChart3,
  ChevronRight,
  Database,
  Server,
  Upload,
} from "lucide-react";
import { useNavigate } from "react-router-dom";

const Landing = () => {
  const navigate = useNavigate();
  const [isLoggedIn, setIsLoggedIn] = useState(!!localStorage.getItem("token"));

  // Đồng bộ khi localStorage thay đổi (logout từ tab khác)
  useEffect(() => {
    const sync = () => setIsLoggedIn(!!localStorage.getItem("token"));
    window.addEventListener("storage", sync);
    return () => window.removeEventListener("storage", sync);
  }, []);

  return (
    <div className="min-h-screen bg-slate-50 font-sans flex flex-col">
      {/* Navbar */}
      <nav className="bg-white border-b border-slate-200 sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex justify-between h-16 items-center">
            {/* Logo */}
            <div className="flex items-center gap-3">
              <img
                src="/CUSC Logo Series.png"
                alt="CUSC Logo"
                className="h-10 w-auto object-contain"
                onError={(e) => {
                  e.target.style.display = "none";
                }}
              />
              <div className="hidden sm:block border-l border-slate-300 pl-3">
                <span className="text-sm font-bold text-slate-800 tracking-tight block leading-tight">
                  CUSC ANALYSIS PLATFORM
                </span>
                <span className="text-[10px] text-slate-500 font-medium">
                  Trung tâm Công nghệ Thông tin - CTU
                </span>
              </div>
            </div>

            {/* Actions */}
            <div className="flex items-center gap-3">
              {isLoggedIn ? (
                <button
                  onClick={() => navigate("/user")}
                  className="inline-flex items-center justify-center px-5 py-2.5 text-sm font-semibold text-white bg-blue-800 rounded-full hover:bg-blue-700 transition-colors shadow-sm gap-2"
                >
                  <Upload className="w-4 h-4" />
                  Tải lên dữ liệu
                </button>
              ) : (
                <button
                  onClick={() => navigate("/login")}
                  className="inline-flex items-center justify-center px-5 py-2.5 text-sm font-semibold text-white bg-blue-800 rounded-full hover:bg-blue-700 transition-colors shadow-sm gap-2"
                >
                  Đăng nhập hệ thống
                  <ArrowRight className="w-4 h-4" />
                </button>
              )}
            </div>
          </div>
        </div>
      </nav>

      {/* Hero Section */}
      <main className="flex-1 flex flex-col">
        <section className="bg-white border-b border-slate-200">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-20 lg:py-28 text-center">
            <h1 className="text-4xl md:text-5xl font-extrabold text-slate-900 tracking-tight mb-6">
              Nền tảng Phân tích Dữ liệu <br className="hidden sm:block" />
              <span className="text-blue-600">Giáo dục &amp; Hành chính Công</span>
            </h1>
            <p className="mt-4 max-w-2xl text-lg text-slate-600 mx-auto mb-10 leading-relaxed">
              Hệ thống Data Lakehouse tích hợp toàn diện quy trình Thu thập, Xử
              lý và Phân tích Dữ liệu quy mô lớn. Sử dụng công nghệ Big Data
              hiện đại phục vụ công tác ra quyết định chiến lược.
            </p>
            <div className="flex justify-center gap-4">
              {isLoggedIn ? (
                <button
                  onClick={() => navigate("/user")}
                  className="px-8 py-5 text-base font-bold text-white bg-blue-800 rounded-full hover:bg-blue-700 transition-colors shadow-md flex items-center gap-2"
                >
                  Tải lên dữ liệu
                  <Upload className="w-5 h-5" />
                </button>
              ) : (
                <button
                  onClick={() => navigate("/login")}
                  className="px-8 py-5 text-base font-bold text-white bg-blue-800 rounded-full hover:bg-blue-700 transition-colors shadow-md flex items-center gap-2"
                >
                  Bắt đầu sử dụng
                  <ChevronRight className="w-5 h-5" />
                </button>
              )}
            </div>
          </div>
        </section>

        {/* Workflow Overview */}
        <section className="py-16 bg-white border-b border-slate-200">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
            <h2 className="text-3xl font-extrabold text-slate-900 mb-10">
              Quy trình Vận hành
            </h2>
            <div className="flex flex-col md:flex-row items-center justify-center gap-4 md:gap-8">
              <div className="p-6 bg-slate-50 border border-slate-200 rounded-xl flex-1 w-full shadow-sm hover:shadow-md transition-shadow text-left md:text-center">
                <div className="w-12 h-12 bg-indigo-100 text-indigo-600 rounded-full flex items-center justify-center mx-auto mb-4 font-bold text-xl">
                  1
                </div>
                <h4 className="font-bold text-slate-800 text-lg mb-2">
                  Data Ingestion
                </h4>
                <p className="text-sm text-slate-600">
                  Tiếp nhận tài liệu hành chính (Word, PDF, Excel, Images), chuẩn hóa
                  định dạng và đưa vào hệ thống lưu trữ phân tán MinIO S3 an
                  toàn.
                </p>
              </div>
              <ArrowRight className="w-8 h-8 text-slate-300 hidden md:block shrink-0" />
              <div className="p-6 bg-slate-50 border border-slate-200 rounded-xl flex-1 w-full shadow-sm hover:shadow-md transition-shadow text-left md:text-center">
                <div className="w-12 h-12 bg-blue-100 text-blue-600 rounded-full flex items-center justify-center mx-auto mb-4 font-bold text-xl">
                  2
                </div>
                <h4 className="font-bold text-slate-800 text-lg mb-2">
                  ETL Processing
                </h4>
                <p className="text-sm text-slate-600">
                  Tự động trích xuất nội dung văn bản, làm sạch dữ liệu nhiễu và
                  biến đổi cấu trúc sử dụng sức mạnh tính toán của Apache Spark.
                </p>
              </div>
              <ArrowRight className="w-8 h-8 text-slate-300 hidden md:block shrink-0" />
              <div className="p-6 bg-slate-50 border border-slate-200 rounded-xl flex-1 w-full shadow-sm hover:shadow-md transition-shadow text-left md:text-center">
                <div className="w-12 h-12 bg-teal-100 text-teal-600 rounded-full flex items-center justify-center mx-auto mb-4 font-bold text-xl">
                  3
                </div>
                <h4 className="font-bold text-slate-800 text-lg mb-2">
                  Analytics
                </h4>
                <p className="text-sm text-slate-600">
                  Hệ thống hóa dữ liệu sạch vào Data Warehouse, trực quan hóa
                  biểu đồ trên Apache Superset phục vụ công tác báo cáo và ra
                  quyết định chiến lược.
                </p>
              </div>
            </div>
          </div>
        </section>

        {/* Features / Architecture Section */}
        <section className="py-20 bg-slate-50">
          <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
            <div className="text-center mb-16">
              <h2 className="text-3xl font-bold text-slate-900">
                Kiến trúc Phân tầng Dữ liệu
              </h2>
              <p className="mt-5 text-slate-600 max-w-3xl mx-auto text-lg leading-relaxed">
                Hệ thống áp dụng chuẩn thiết kế Medallion tiên tiến nhất, tổ
                chức dữ liệu thành 3 tầng logic liên tiếp. Quy trình chuẩn hóa
                tự động giúp đảm bảo chất lượng dữ liệu (Data Quality) từ lúc
                còn nguyên sơ (Raw) cho đến khi sẵn sàng khai thác nghiệp vụ
                (Business-ready).
              </p>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-8">
              {/* Bronze */}
              <div className="bg-white p-8 rounded-2xl shadow-sm border border-slate-200 flex flex-col items-center text-center hover:shadow-md transition-shadow">
                <div className="w-14 h-14 bg-amber-50 rounded-2xl flex items-center justify-center text-amber-600 mb-6 border border-amber-100">
                  <Database className="w-7 h-7" />
                </div>
                <h3 className="text-xl font-bold text-slate-900 mb-3">
                  Bronze Layer
                </h3>
                <p className="text-slate-600 text-sm leading-relaxed mb-4">
                  Lưu trữ dữ liệu nguyên bản (Raw Data) từ nhiều nguồn khác nhau
                  một cách an toàn. Duy trì lịch sử dữ liệu phục vụ audit.
                </p>
                <div className="mt-auto">
                  <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-slate-100 text-slate-800">
                    MinIO Object Storage
                  </span>
                </div>
              </div>

              {/* Silver */}
              <div className="bg-white p-8 rounded-2xl shadow-sm border border-slate-200 flex flex-col items-center text-center hover:shadow-md transition-shadow">
                <div className="w-14 h-14 bg-slate-100 rounded-2xl flex items-center justify-center text-slate-700 mb-6 border border-slate-200">
                  <Server className="w-7 h-7" />
                </div>
                <h3 className="text-xl font-bold text-slate-900 mb-3">
                  Silver Layer
                </h3>
                <p className="text-slate-600 text-sm leading-relaxed mb-4">
                  Làm sạch, lọc, và chuẩn hóa cấu trúc dữ liệu. Kết hợp các
                  luồng dữ liệu (ETL/ELT) tạo ra bộ dữ liệu đáng tin cậy.
                </p>
                <div className="mt-auto flex flex-wrap justify-center gap-2">
                  <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-slate-100 text-slate-800">
                    Apache Spark
                  </span>
                  <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-slate-100 text-slate-800">
                    Kafka
                  </span>
                </div>
              </div>

              {/* Gold */}
              <div className="bg-white p-8 rounded-2xl shadow-sm border border-slate-200 flex flex-col items-center text-center hover:shadow-md transition-shadow">
                <div className="w-14 h-14 bg-blue-50 rounded-2xl flex items-center justify-center text-blue-600 mb-6 border border-blue-100">
                  <BarChart3 className="w-7 h-7" />
                </div>
                <h3 className="text-xl font-bold text-slate-900 mb-3">
                  Gold Layer
                </h3>
                <p className="text-slate-600 text-sm leading-relaxed mb-4">
                  Tập hợp dữ liệu chất lượng cao đã được tối ưu hóa theo mô hình
                  nghiệp vụ , sẵn sàng cho báo cáo và BI.
                </p>
                <div className="mt-auto">
                  <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-slate-100 text-slate-800">
                    Apache Superset
                  </span>
                </div>
              </div>
            </div>
          </div>
        </section>
      </main>

      {/* Footer */}
      <footer className="bg-white border-t border-slate-200 py-8">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 flex flex-col md:flex-row justify-between items-center gap-4">
          <div className="flex items-center gap-3">
            <img
              src="/CUSC Logo Series.png"
              alt="CUSC Logo"
              className="h-6 w-auto grayscale opacity-70"
            />
            <span className="text-sm text-slate-500 font-medium">
              © 2026 CUSC. All rights reserved.
            </span>
          </div>
          <div className="text-sm text-slate-500">
            Trung tâm Công nghệ Thông tin - Đại học Cần Thơ
          </div>
        </div>
      </footer>
    </div>
  );
};

export default Landing;
