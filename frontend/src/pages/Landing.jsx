import { useEffect, useState } from "react";
import {
  ArrowDown,
  BarChart3,
  Check,
  Database,
  Upload,
  Zap,
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

  const goToUpload = () => navigate(isLoggedIn ? "/user" : "/login");

  const medallionLayers = [
    {
      name: "BRONZE LAYER",
      suffix: "RAW DATA",
      detail: "Thu thập tài liệu hành chính, tệp bảng tính & dữ liệu số hóa thô",
      icon: Database,
      shell: "border-bronze-300 bg-bronze-100",
      iconShell: "bg-orange-100 text-bronze-500",
      title: "text-bronze-600",
    },
    {
      name: "SILVER LAYER",
      suffix: "CLEANED & PROCESSED",
      detail: "Làm sạch, chuẩn hóa, khử trùng lặp & chuyển đổi cấu trúc",
      icon: Zap,
      shell: "border-silver-200 bg-silver-100",
      iconShell: "bg-blue-50 text-silver-500",
      title: "text-silver-600",
    },
    {
      name: "GOLD LAYER",
      suffix: "CURATED ANALYTICS",
      detail: "Chỉ số điều hành, biểu đồ phân tích & hỗ trợ ra quyết định chiến lược",
      icon: BarChart3,
      shell: "border-gold-200 bg-gold-100",
      iconShell: "bg-amber-50 text-gold-500",
      title: "text-gold-600",
    },
  ];

  const processSteps = [
    {
      number: "01",
      title: "Data Ingestion",
      copy: "Tiếp nhận tập trung dữ liệu từ nhiều nguồn vào nền tảng lưu trữ.",
      status: "Lưu trữ dạng thô (Raw Format)",
      numberClass: "border-blue-600 text-blue-700 ring-blue-50",
      connectorClass: "bg-blue-200",
      statusClass: "bg-blue-50/80 text-blue-700",
    },
    {
      number: "02",
      title: "ETL Processing",
      copy: "Làm sạch, chuẩn hóa và biến đổi dữ liệu qua pipeline ETL.",
      status: "Đồng bộ và làm sạch lược đồ",
      numberClass: "border-indigo-600 text-indigo-700 ring-indigo-50",
      connectorClass: "bg-indigo-200",
      statusClass: "bg-indigo-50/80 text-indigo-700",
    },
    {
      number: "03",
      title: "Analytics",
      copy: "Khai thác dữ liệu phục vụ báo cáo, phân tích và ra quyết định.",
      status: "Trực quan hóa phục vụ chỉ đạo",
      numberClass: "border-emerald-600 text-emerald-700 ring-emerald-50",
      connectorClass: "bg-emerald-200",
      statusClass: "bg-emerald-50 text-emerald-700",
    },
  ];

  return (
    <div className="flex min-h-screen flex-col bg-canvas font-sans text-navy-950">
      <header className="sticky top-0 z-50 border-b border-line bg-white/95 backdrop-blur-sm">
        <div className="mx-auto flex h-[72px] w-full max-w-7xl items-center justify-between gap-4 px-4 sm:px-6 lg:px-8">
          <div className="flex min-w-0 items-center gap-3">
            <img
              src="/CUSC Logo Series.png"
              alt="CUSC Logo"
              className="h-11 w-auto shrink-0 object-contain"
              onError={(e) => {
                e.target.style.display = "none";
              }}
            />
            <div className="hidden min-w-0 border-l border-line pl-3 sm:block">
              <span className="block truncate font-display text-sm font-extrabold leading-tight tracking-tight text-navy-950">
                CUSC ANALYSIS PLATFORM
              </span>
              <span className="mt-0.5 block truncate text-[11px] font-medium text-ink-500">
                Trung tâm Công nghệ Thông tin - CTU
              </span>
            </div>
          </div>

          <button
            onClick={() => navigate("/login")}
            className="group inline-flex items-center justify-center gap-2 rounded-xl border border-cobalt-200 bg-white px-5 py-2.5 text-sm font-semibold text-navy-900 shadow-[0_6px_18px_-12px_rgba(37,99,235,0.35)] transition-all duration-200 hover:border-cobalt-300 hover:bg-cobalt-50 hover:text-cobalt-700 hover:shadow-[0_8px_22px_-12px_rgba(37,99,235,0.45)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-cobalt-500 focus-visible:ring-offset-2"
          >
            <span>Đăng nhập</span>
            <span
              aria-hidden="true"
              className="text-base leading-none text-cobalt-500 transition-transform duration-200 group-hover:translate-x-0.5 group-hover:text-cobalt-700"
            >
              →
            </span>
          </button>
        </div>
      </header>

      <main className="flex-1">
        <section className="relative overflow-hidden border-b border-line bg-[#F8F9FC]">
          <div
            className="pointer-events-none absolute inset-0 opacity-60 [background-image:radial-gradient(circle,rgba(148,163,184,0.22)_1px,transparent_1px)] [background-size:26px_26px]"
            aria-hidden="true"
          />
          <div
            className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_66%_38%,rgba(219,234,254,0.72)_0%,rgba(248,250,252,0)_56%)]"
            aria-hidden="true"
          />

          {/* UI_PHASE_1B_HERO_WAVE_START */}
          <style>{`
            @keyframes cuscHeroWaveSweep {
              0%, 56% {
                transform: translate3d(-10%, 0, 0);
                opacity: 0;
              }

              62% {
                opacity: 0;
              }

              69% {
                opacity: 0.82;
              }

              88% {
                opacity: 0.34;
              }

              100% {
                transform: translate3d(260%, 0, 0);
                opacity: 0;
              }
            }

            .cusc-hero-wave {
              animation:
                cuscHeroWaveSweep
                8s
                cubic-bezier(0.4, 0, 0.2, 1)
                infinite;
              will-change: transform, opacity;
            }

            @media (prefers-reduced-motion: reduce) {
              .cusc-hero-wave {
                display: none;
                animation: none;
              }
            }
          `}</style>

          <svg
            viewBox="0 0 900 260"
            preserveAspectRatio="none"
            aria-hidden="true"
            className="cusc-hero-wave pointer-events-none absolute -left-[55%] top-[8%] h-[78%] w-[78%] opacity-0 blur-[3px]"
          >
            <defs>
              <linearGradient
                id="cuscHeroWaveGradient"
                x1="0%"
                y1="0%"
                x2="100%"
                y2="0%"
              >
                <stop
                  offset="0%"
                  stopColor="#60A5FA"
                  stopOpacity="0"
                />
                <stop
                  offset="28%"
                  stopColor="#93C5FD"
                  stopOpacity="0.18"
                />
                <stop
                  offset="50%"
                  stopColor="#FFFFFF"
                  stopOpacity="0.96"
                />
                <stop
                  offset="72%"
                  stopColor="#38BDF8"
                  stopOpacity="0.20"
                />
                <stop
                  offset="100%"
                  stopColor="#60A5FA"
                  stopOpacity="0"
                />
              </linearGradient>
            </defs>

            <path
              d="
                M -40 165
                C 150 20, 330 245, 535 105
                S 815 35, 960 115
              "
              fill="none"
              stroke="url(#cuscHeroWaveGradient)"
              strokeWidth="104"
              strokeLinecap="round"
            />

            <path
              d="
                M -40 165
                C 150 20, 330 245, 535 105
                S 815 35, 960 115
              "
              fill="none"
              stroke="#93C5FD"
              strokeOpacity="0.32"
              strokeWidth="2"
              strokeLinecap="round"
            />
          </svg>
          {/* UI_PHASE_1B_HERO_WAVE_END */}

          <div className="relative mx-auto grid w-full max-w-7xl items-center gap-12 px-4 py-16 sm:px-6 sm:py-20 lg:grid-cols-12 lg:gap-14 lg:px-8 lg:py-24">
            <div className="lg:col-span-7">
              <div className="max-w-[690px]">
                <p className="mb-4 font-data text-[11px] font-semibold uppercase tracking-[0.12em] text-cobalt-700">
                  CUSC ANALYSIS PLATFORM
                </p>
                <h1 className="font-display text-[2.35rem] font-extrabold leading-[1.08] tracking-[-0.035em] text-navy-950 sm:text-5xl lg:text-[3.35rem]">
                  Nền tảng Phân tích Dữ liệu
                  <span className="mt-1 block text-cobalt-700">
                    Giáo dục &amp; Hành chính Công
                  </span>
                </h1>

                <p className="mt-6 max-w-2xl text-base leading-7 text-ink-600 sm:text-lg sm:leading-8">
                  Hệ thống Data Lakehouse tích hợp toàn diện quy trình Thu thập, Xử
                  lý và Phân tích Dữ liệu quy mô lớn. Sử dụng công nghệ Big Data
                  hiện đại phục vụ công tác ra quyết định chiến lược.
                </p>

                <div className="mt-8">
                  <button
                    onClick={goToUpload}
                    className="inline-flex items-center justify-center gap-2.5 rounded-panel bg-cobalt-700 px-7 py-3.5 text-sm font-semibold text-white shadow-elevated transition hover:bg-cobalt-800 focus-visible:outline-none focus-visible:ring-4 focus-visible:ring-cobalt-600/20 sm:text-base"
                  >
                    Kết nối nguồn dữ liệu
                    <Upload className="h-5 w-5" />
                  </button>
                </div>
              </div>
            </div>

            <div className="lg:col-span-5">
              <div className="relative mx-auto w-full max-w-lg">
                <div
                  className="pointer-events-none absolute -inset-4 rounded-3xl bg-blue-100/50 blur-2xl"
                  aria-hidden="true"
                />
                <div className="relative rounded-panel border border-line bg-white p-5 shadow-elevated sm:p-6">
                  <div className="mb-4 flex items-center gap-2 border-b border-ink-100 pb-4">
                    <span className="h-2.5 w-2.5 rounded-full bg-cobalt-600" />
                    <p className="font-data text-[11px] font-semibold uppercase tracking-[0.08em] text-ink-700">
                      Kiến trúc Luồng Dữ liệu Lakehouse
                    </p>
                  </div>

                  <div className="space-y-2">
                    {medallionLayers.map((layer, index) => {
                      const LayerIcon = layer.icon;
                      return (
                        <div key={layer.name}>
                          <div className={`flex items-start gap-3.5 rounded-panel border p-3.5 ${layer.shell}`}>
                            <div className={`flex h-10 w-10 shrink-0 items-center justify-center rounded-control shadow-sm ${layer.iconShell}`}>
                              <LayerIcon className="h-5 w-5" />
                            </div>
                            <div className="min-w-0 pt-0.5">
                              <p className={`font-data text-[11px] font-semibold uppercase tracking-[0.02em] ${layer.title}`}>
                                {layer.name} • {layer.suffix}
                              </p>
                              <p className="mt-1 text-xs leading-5 text-ink-600">
                                {layer.detail}
                              </p>
                            </div>
                          </div>
                          {index < medallionLayers.length - 1 && (
                            <div className="flex h-7 items-center justify-center text-ink-400" aria-hidden="true">
                              <ArrowDown className="h-4 w-4" />
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                </div>
              </div>
            </div>
          </div>
        </section>

        <section className="border-b border-slate-200/80 bg-white py-20 lg:py-24">
          <div className="mx-auto w-full max-w-7xl px-4 sm:px-6 lg:px-8">
            <div className="mx-auto mb-16 max-w-2xl text-center">
              <h2 className="font-display text-3xl font-extrabold tracking-tight text-navy-950 sm:text-4xl">
                Quy trình Vận hành
              </h2>
              <p className="mt-3 text-base font-normal text-ink-600">
                Từ dữ liệu đầu vào đến thông tin sẵn sàng cho phân tích qua ba giai đoạn.
              </p>
            </div>

            <div className="relative mx-auto max-w-5xl pb-4">
              <div
                className="absolute left-12 right-12 top-7 z-0 hidden h-0.5 bg-gradient-to-r from-blue-200 via-indigo-200 to-teal-200 md:block"
                aria-hidden="true"
              />

              <div className="relative z-10 grid grid-cols-1 gap-10 md:grid-cols-3 md:gap-8">
                {processSteps.map((step, index) => (
                  <article
                    key={step.number}
                    className="group flex flex-col items-center text-center md:items-start md:text-left"
                  >
                    <div className="mb-5 flex items-center gap-3">
                      <div
                        className={`flex h-14 w-14 shrink-0 items-center justify-center rounded-2xl border-2 bg-white font-data text-base font-bold shadow-sm ring-4 transition-transform duration-300 group-hover:scale-105 ${step.numberClass}`}
                      >
                        {step.number}
                      </div>
                      {index < processSteps.length - 1 && (
                        <div
                          className={`h-0.5 w-12 md:hidden ${step.connectorClass}`}
                          aria-hidden="true"
                        />
                      )}
                    </div>

                    <h3 className="mb-2 font-display text-base font-bold tracking-tight text-navy-950">
                      {step.title}
                    </h3>
                    <p className="mb-3 max-w-sm text-sm font-normal leading-relaxed text-ink-600">
                      {step.copy}
                    </p>
                    <div
                      className={`inline-flex items-center gap-1.5 rounded-md px-2.5 py-1 text-xs font-medium ${step.statusClass}`}
                    >
                      <Check className="h-3.5 w-3.5" strokeWidth={2.25} />
                      <span>{step.status}</span>
                    </div>
                  </article>
                ))}
              </div>
            </div>
          </div>
        </section>
      </main>

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
