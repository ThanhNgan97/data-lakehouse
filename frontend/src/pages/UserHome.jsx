import { Link } from "react-router-dom";
import { ArrowDown, ArrowRight, BarChart3, Check, Code2, Database, UploadCloud, Zap } from "lucide-react";
import UserTopNavigation from "../components/UserTopNavigation";
import "./UserHome.css";

const layers = [
  { name: "Bronze Layer", suffix: "Raw Data", tone: "bronze", icon: Database, detail: "Tiếp nhận và lưu trữ tài liệu đầu vào ở dạng thô." },
  { name: "Silver Layer", suffix: "Cleaned & Processed", tone: "silver", icon: Zap, detail: "Xử lý và chuyển đổi dữ liệu cho bước phân tích." },
  { name: "Gold Layer", suffix: "Curated Analytics", tone: "gold", icon: BarChart3, detail: "Dữ liệu phục vụ báo cáo và phân tích." },
];

const sources = [
  { title: "Tải file", icon: UploadCloud, tone: "file", to: "/user/file", description: "Tải file từ máy tính hoặc nhập từ URL và theo dõi tiến trình xử lý.", metadata: "8 định dạng · Tối đa 100 MB", action: "Tải file ngay" },
  { title: "Kết nối API", icon: Code2, tone: "api", to: "/user/api", description: "Kết nối, kiểm tra và đồng bộ dữ liệu từ các REST API bên ngoài.", metadata: "REST API · Bearer Token", action: "Kết nối API" },
  { title: "Kết nối Database", icon: Database, tone: "database", to: "/user/database", description: "Định hướng kết nối nguồn dữ liệu từ cơ sở dữ liệu nghiệp vụ.", metadata: "Dự kiến · Chưa khả dụng", action: "Kết nối Database" },
  { title: "Kết nối API", icon: Code2, tone: "api", to: "/user/api", description: "Định hướng tiếp nhận dữ liệu từ các API bên ngoài.", metadata: "Dự kiến · Chưa khả dụng", action: "Kết nối API" },
  { title: "Kết nối Database", icon: Database, tone: "database", to: "/user/database", description: "Kết nối PostgreSQL, MySQL hoặc SQL Server và theo dõi pipeline dữ liệu.", metadata: "Airbyte · CDC · Superset", action: "Kết nối Database" },
  { title: "Kết nối IoT", icon: Zap, tone: "iot", to: "/user/iot", description: "Định hướng tiếp nhận dữ liệu từ thiết bị và cảm biến.", metadata: "Dự kiến · Chưa khả dụng", action: "Kết nối IoT" },
];

const steps = [
  { number: "01", title: "Data Ingestion", tone: "file", copy: "Tiếp nhận tài liệu đầu vào và đưa vào tầng Bronze. Các nguồn API, Database và IoT thuộc định hướng mở rộng.", note: "Lưu trữ dạng thô (Raw Format)" },
  { number: "02", title: "ETL Processing", tone: "database", copy: "Xử lý và chuyển đổi dữ liệu qua các tầng Bronze, Silver và Gold.", note: "Xử lý dữ liệu theo pipeline" },
  { number: "03", title: "Analytics", tone: "iot", copy: "Tổng hợp dữ liệu tại Gold Layer và khai thác thông qua các công cụ báo cáo phân tích.", note: "Trực quan hóa phục vụ chỉ đạo" },
];

export default function UserHome() {
  return (
    <div className="portal-home">
      <UserTopNavigation />
      <main>
        <section className="portal-hero" aria-labelledby="portal-title">
          <div className="portal-shell portal-hero-grid">
            <div className="portal-hero-copy">
              <p className="portal-eyebrow">CUSC Analysis Platform</p>
              <h1 id="portal-title" className="portal-heading">
                <span>Nền tảng Phân tích Dữ liệu</span>
                <span>Giáo dục &amp; Hành chính Công</span>
              </h1>
              <p className="portal-hero-description">
                Nền tảng Data Lakehouse hỗ trợ thu thập, xử lý và phân tích dữ liệu
                trong một quy trình thống nhất, phục vụ giáo dục và hành chính công.
              </p>
              <a href="#nguon-du-lieu" className="portal-primary-action">
                Kết nối nguồn dữ liệu <ArrowRight size={19} aria-hidden="true" />
              </a>
            </div>

            <div className="portal-architecture-wrap">
              <figure className="portal-architecture">
                <figcaption><span aria-hidden="true" />Kiến trúc luồng dữ liệu Lakehouse</figcaption>
                <ol className="portal-layers">
                  {layers.map((layer, index) => {
                    const LayerIcon = layer.icon;
                    return (
                      <li key={layer.tone}>
                        <div className={`portal-layer portal-layer-${layer.tone}`}>
                          <span className="portal-layer-icon"><LayerIcon size={21} aria-hidden="true" /></span>
                          <div>
                            <h2>{layer.name} <span aria-hidden="true">·</span> {layer.suffix}</h2>
                            <p>{layer.detail}</p>
                          </div>
                        </div>
                        {index < layers.length - 1 && <div className="portal-layer-arrow" aria-hidden="true"><ArrowDown size={18} /></div>}
                      </li>
                    );
                  })}
                </ol>
              </figure>
            </div>
          </div>
        </section>

        <section id="nguon-du-lieu" className="portal-sources" aria-labelledby="portal-sources-title">
          <div className="portal-shell">
            <div className="portal-section-heading">
              <h2 id="portal-sources-title">Phương thức Thu nạp Dữ liệu</h2>
              <p>Lựa chọn nguồn dữ liệu để bắt đầu quy trình thu thập và xử lý.</p>
            </div>
            <div className="portal-source-grid">
              {sources.map((source) => {
                const SourceIcon = source.icon;
                return (
                  <article key={source.to} className={`portal-source-card portal-tone-${source.tone}`}>
                    <div className="portal-source-icon"><SourceIcon size={24} aria-hidden="true" /></div>
                    <h3>{source.title}</h3>
                    <p className="portal-source-description">{source.description}</p>
                    <p className="portal-source-metadata">{source.metadata}</p>
                    <div className="portal-source-action">
                      <Link to={source.to}>{source.action}<ArrowRight size={16} aria-hidden="true" /></Link>
                    </div>
                  </article>
                );
              })}
            </div>
          </div>
        </section>

        <section className="portal-workflow" aria-labelledby="portal-workflow-title">
          <div className="portal-shell">
            <div className="portal-section-heading">
              <h2 id="portal-workflow-title">Quy trình Vận hành</h2>
              <p>Từ dữ liệu đầu vào đến thông tin sẵn sàng cho phân tích qua ba giai đoạn.</p>
            </div>
            <ol className="portal-step-grid">
              {steps.map((step) => (
                <li key={step.number} className={`portal-step portal-tone-${step.tone}`}>
                  <span className="portal-step-number">{step.number}</span>
                  <h3>{step.title}</h3>
                  <p>{step.copy}</p>
                  <span className="portal-step-note"><Check size={15} aria-hidden="true" />{step.note}</span>
                </li>
              ))}
            </ol>
          </div>
        </section>
      </main>

      <footer className="portal-footer">
        <div className="portal-shell">
          <span>© 2026 CUSC. Bảo lưu mọi quyền.</span>
          <span>Trung tâm Công nghệ Phần mềm - Đại học Cần Thơ</span>
        </div>
      </footer>
    </div>
  );
}
