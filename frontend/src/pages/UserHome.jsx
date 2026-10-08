import { Link } from "react-router-dom";
import UserTopNavigation from "../components/UserTopNavigation";
import "./UserHome.css";

export default function UserHome() {
 return <div className="portal-home min-h-screen bg-slate-50 text-slate-800 font-sans"><UserTopNavigation /><main className="flex-1">

<section className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pt-12 pb-16 lg:pt-16 lg:pb-20 micro-dot-bg" data-purpose="hero-overview">
<div className="grid grid-cols-1 lg:grid-cols-12 gap-12 lg:gap-8 items-center">

<div className="lg:col-span-6 space-y-6">
<div className="space-y-4">
<h1 className="tracking-tight"><span className="block portal-title-line font-bold text-slate-900 leading-tight pb-1">Nền tảng Phân tích Dữ liệu</span><span className="block portal-title-line font-extrabold text-[#1d4ed8] leading-tight whitespace-nowrap">Giáo dục &amp; Hành chính Công</span></h1>
<p className="text-[15px] sm:text-base text-slate-600 leading-relaxed font-normal pt-2 pr-0 sm:pr-6">
  Nền tảng Data Lakehouse hỗ trợ thu thập, xử lý và phân tích dữ liệu trong một quy trình thống nhất.
</p>
</div>
<div className="pt-3">
<a className="inline-flex items-center space-x-2 px-6 py-3 rounded-lg bg-blue-600 hover:bg-blue-700 text-white font-semibold text-sm shadow-md shadow-blue-500/20 transition-all hover:shadow-lg hover:shadow-blue-500/30" href="#nguon-du-lieu">
<span className="">Khám phá nguồn dữ liệu</span>
<svg className="w-4 h-4 stroke-[2.2]" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M14 5l7 7m0 0l-7 7m7-7H3" strokeLinecap="round" strokeLinejoin="round"></path></svg>
</a>
</div>
</div>

<div className="lg:col-span-6 flex justify-center lg:justify-end">
<div className="w-full max-w-xl bg-white border border-slate-200 rounded-2xl p-6 sm:p-7 medallion-shadow relative">

<div className="flex items-center space-x-2 mb-5">
<span className="w-2.5 h-2.5 rounded-full bg-blue-600 animate-pulse"></span>
<span className="text-xs font-bold uppercase tracking-wider text-slate-600">Kiến trúc luồng dữ liệu Lakehouse</span>
</div>

<div className="space-y-3.5">

<div className="bg-amber-50/50 border border-amber-200/70 rounded-xl p-3.5 sm:p-4 transition hover:bg-amber-50" data-purpose="bronze-layer">
<div className="flex items-start space-x-3.5">
<div className="w-9 h-9 rounded-lg bg-amber-100/90 border border-amber-300 text-amber-700 flex items-center justify-center shrink-0">
<svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
<path d="M5 8h14M5 8a2 2 0 110-4h14a2 2 0 110 4M5 8v10a2 2 0 002 2h10a2 2 0 002-2V8m-9 4h4" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.8"></path>
</svg>
</div>
<div>
<h2 className="text-xs font-bold text-amber-900 uppercase tracking-wide">BRONZE LAYER • RAW DATA</h2>
<p className="text-xs text-amber-800/90 mt-0.5 leading-snug">
                      Tiếp nhận và lưu trữ tài liệu đầu vào ở dạng thô
                    </p>
</div>
</div>
</div>

<div className="flex justify-center -my-1 text-slate-400">
<svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
<path d="M19 14l-7 7m0 0l-7-7m7 7V3" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2"></path>
</svg>
</div>

<div className="bg-blue-50/50 border border-blue-200/80 rounded-xl p-3.5 sm:p-4 transition hover:bg-blue-50" data-purpose="silver-layer">
<div className="flex items-start space-x-3.5">
<div className="w-9 h-9 rounded-lg bg-blue-100/90 border border-blue-300 text-blue-700 flex items-center justify-center shrink-0">
<svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
<path d="M13 10V3L4 14h7v7l9-11h-7z" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.8"></path>
</svg>
</div>
<div>
<h2 className="text-xs font-bold text-blue-900 uppercase tracking-wide">SILVER LAYER • CLEANED &amp; PROCESSED</h2>
<p className="text-xs text-blue-800/90 mt-0.5 leading-snug">
                      Xử lý và chuyển đổi dữ liệu cho bước phân tích
                    </p>
</div>
</div>
</div>

<div className="flex justify-center -my-1 text-slate-400">
<svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
<path d="M19 14l-7 7m0 0l-7-7m7 7V3" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2"></path>
</svg>
</div>

<div className="bg-emerald-50/40 border border-emerald-200/80 rounded-xl p-3.5 sm:p-4 transition hover:bg-emerald-50" data-purpose="gold-layer">
<div className="flex items-start space-x-3.5">
<div className="w-9 h-9 rounded-lg bg-emerald-100/90 border border-emerald-300 text-emerald-700 flex items-center justify-center shrink-0">
<svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
<path d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" strokeLinecap="round" strokeLinejoin="round" strokeWidth="1.8"></path>
</svg>
</div>
<div>
<h2 className="text-xs font-bold text-emerald-900 uppercase tracking-wide">GOLD LAYER • CURATED ANALYTICS</h2>
<p className="text-xs text-emerald-800/90 mt-0.5 leading-snug">
                      Dữ liệu phục vụ báo cáo và phân tích
                    </p>
</div>
</div>
</div>
</div>

<div className="mt-4 pt-3 border-t border-slate-100 flex items-center justify-between text-[11px] text-slate-500"><span className="inline-flex items-center"><span className="w-1.5 h-1.5 rounded-full bg-emerald-500 mr-1.5"></span> Kiến trúc Bronze / Silver / Gold</span><span className="">Luồng xử lý dữ liệu</span></div>
</div>
</div>
</div>
</section>


<section className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-10" data-purpose="ingestion-methods" id="nguon-du-lieu">
<div className="text-center max-w-2xl mx-auto mb-10">
<h2 className="text-2xl sm:text-3xl font-bold text-slate-900 tracking-tight">Phương thức Thu nạp Dữ liệu</h2>
<p className="text-sm text-slate-500 mt-2">Linh hoạt kết nối và tiếp nhận dữ liệu từ nhiều loại nguồn khác nhau.</p>
</div>
<div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-6">

<div className="bg-white rounded-2xl border border-slate-200/90 border-t-4 border-t-blue-600 p-6 card-soft-shadow hover:shadow-lg hover:border-slate-300 transition-all flex flex-col justify-between group" id="tai-file">
<div className="space-y-4">
<div className="w-12 h-12 rounded-xl bg-blue-50 text-blue-600 flex items-center justify-center shadow-sm group-hover:scale-105 transition-transform">
<svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2"></path></svg>
</div>
<div>
<h3 className="text-lg font-bold text-slate-900">Tải file</h3>
<p className="text-xs text-slate-600 leading-relaxed mt-1.5">Tải file từ máy tính hoặc nhập từ URL và theo dõi tiến trình xử lý.</p>
</div>
<div className="pt-1">
<span className="inline-block text-[11px] font-semibold text-blue-700 bg-blue-50 px-2.5 py-1 rounded-md border border-blue-100">8 định dạng · Tối đa 100 MB</span>
</div>
</div>
<div className="pt-6 mt-4 border-t border-slate-100">
<Link className="w-full inline-flex items-center justify-center space-x-1.5 px-4 py-2.5 rounded-lg bg-blue-600 hover:bg-blue-700 text-white text-xs font-semibold shadow-sm transition-all shadow-blue-500/20" to="/user/file">
<span className="">Tải file ngay</span>
<svg className="w-3.5 h-3.5 stroke-[2]" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M14 5l7 7m0 0l-7 7m7-7H3" strokeLinecap="round" strokeLinejoin="round"></path></svg>
</Link>
</div>
</div>

<div className="bg-white rounded-2xl border border-slate-200/90 border-t-4 border-t-sky-500 p-6 card-soft-shadow hover:shadow-lg hover:border-slate-300 transition-all flex flex-col justify-between group" id="ket-noi-api">
<div className="space-y-4">
<div className="w-12 h-12 rounded-xl bg-sky-50 text-sky-600 flex items-center justify-center shadow-sm group-hover:scale-105 transition-transform">
<svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M10 20l4-16m4 4l4 4-4 4M6 16l-4-4 4-4" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2"></path></svg>
</div>
<div>
<h3 className="text-lg font-bold text-slate-900">Kết nối API</h3>
<p className="text-xs text-slate-600 leading-relaxed mt-1.5">Định hướng tiếp nhận dữ liệu từ các API bên ngoài.</p>
</div>
<div className="pt-1">
<span className="inline-block text-[11px] font-semibold text-sky-700 bg-sky-50 px-2.5 py-1 rounded-md border border-sky-100">Dự kiến · Chưa khả dụng</span>
</div>
</div>
<div className="pt-6 mt-4 border-t border-slate-100">
<Link className="w-full inline-flex items-center justify-center space-x-1.5 px-4 py-2.5 rounded-lg bg-sky-600 hover:bg-sky-700 text-white text-xs font-semibold shadow-sm transition-all shadow-sky-500/20" to="/user/api">
<span className="">Kết nối API</span>
<svg className="w-3.5 h-3.5 stroke-[2]" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M14 5l7 7m0 0l-7 7m7-7H3" strokeLinecap="round" strokeLinejoin="round"></path></svg>
</Link>
</div>
</div>

<div className="bg-white rounded-2xl border border-slate-200/90 border-t-4 border-t-indigo-600 p-6 card-soft-shadow hover:shadow-lg hover:border-slate-300 transition-all flex flex-col justify-between group" id="ket-noi-db">
<div className="space-y-4">
<div className="w-12 h-12 rounded-xl bg-indigo-50 text-indigo-600 flex items-center justify-center shadow-sm group-hover:scale-105 transition-transform">
<svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M4 7v10c0 2.21 3.582 4 8 4s8-1.79 8-4V7M4 7c0 2.21 3.582 4 8 4s8-1.79 8-4M4 7c0-2.21 3.582-4 8-4s8 1.79 8 4m0 5c0 2.21-3.582 4-8 4s-8-1.79-8-4" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2"></path></svg>
</div>
<div>
<h3 className="text-lg font-bold text-slate-900">Kết nối Database</h3>
<p className="text-xs text-slate-600 leading-relaxed mt-1.5">Định hướng kết nối nguồn dữ liệu từ cơ sở dữ liệu nghiệp vụ.</p>
</div>
<div className="pt-1">
<span className="inline-block text-[11px] font-semibold text-indigo-700 bg-indigo-50 px-2.5 py-1 rounded-md border border-indigo-100">Dự kiến · Chưa khả dụng</span>
</div>
</div>
<div className="pt-6 mt-4 border-t border-slate-100">
<Link className="w-full inline-flex items-center justify-center space-x-1.5 px-4 py-2.5 rounded-lg bg-indigo-600 hover:bg-indigo-700 text-white text-xs font-semibold shadow-sm transition-all shadow-indigo-500/20" to="/user/database">
<span className="">Kết nối Database</span>
<svg className="w-3.5 h-3.5 stroke-[2]" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M14 5l7 7m0 0l-7 7m7-7H3" strokeLinecap="round" strokeLinejoin="round"></path></svg>
</Link>
</div>
</div>

<div className="bg-white rounded-2xl border border-slate-200/90 border-t-4 border-t-teal-600 p-6 card-soft-shadow hover:shadow-lg hover:border-slate-300 transition-all flex flex-col justify-between group" id="ket-noi-iot">
<div className="space-y-4">
<div className="w-12 h-12 rounded-xl bg-teal-50 text-teal-600 flex items-center justify-center shadow-sm group-hover:scale-105 transition-transform">
<svg className="w-6 h-6" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M13 10V3L4 14h7v7l9-11h-7z" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2"></path></svg>
</div>
<div>
<h3 className="text-lg font-bold text-slate-900">Kết nối IoT</h3>
<p className="text-xs text-slate-600 leading-relaxed mt-1.5">Định hướng tiếp nhận dữ liệu từ thiết bị và cảm biến.</p>
</div>
<div className="pt-1">
<span className="inline-block text-[11px] font-semibold text-teal-700 bg-teal-50 px-2.5 py-1 rounded-md border border-teal-100">Dự kiến · Chưa khả dụng</span>
</div>
</div>
<div className="pt-6 mt-4 border-t border-slate-100">
<Link className="w-full inline-flex items-center justify-center space-x-1.5 px-4 py-2.5 rounded-lg bg-teal-600 hover:bg-teal-700 text-white text-xs font-semibold shadow-sm transition-all shadow-teal-500/20" to="/user/iot">
<span className="">Kết nối IoT</span>
<svg className="w-3.5 h-3.5 stroke-[2]" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M14 5l7 7m0 0l-7 7m7-7H3" strokeLinecap="round" strokeLinejoin="round"></path></svg>
</Link>
</div>
</div>
</div>
</section>


<section className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-16" data-purpose="workflow-stages" id="quy-trinh">
<div className="text-center max-w-xl mx-auto mb-14">
<h2 className="text-2xl sm:text-3xl font-extrabold text-slate-900 tracking-tight">Quy trình Vận hành</h2>
<p className="text-sm text-slate-500 mt-2 font-normal">Từ dữ liệu đầu vào đến thông tin sẵn sàng cho phân tích qua ba giai đoạn.</p>
</div>
<div className="relative">
<div className="hidden md:block absolute top-6 left-12 right-12 h-0.5 bg-gradient-to-r from-blue-300 via-indigo-300 to-teal-300 z-0"></div>
<div className="grid grid-cols-1 md:grid-cols-3 gap-8 md:gap-6 relative z-10">
<div className="flex flex-col items-center md:items-start text-center md:text-left space-y-3">
<div className="w-12 h-12 rounded-xl bg-white border-2 border-blue-600 text-blue-600 font-bold text-base flex items-center justify-center shadow-sm">01</div>
<div className="pt-1">
<div className="flex items-center justify-center md:justify-start space-x-1.5">
<span className="w-2 h-2 rounded-full bg-blue-600"></span>
<h3 className="text-base font-bold text-slate-900">Data Ingestion</h3>
</div>
<p className="text-xs text-slate-600 mt-2 leading-relaxed">Tiếp nhận tài liệu đầu vào và đưa vào tầng Bronze. Các nguồn API, Database và IoT thuộc định hướng mở rộng.</p>
<div className="inline-flex items-center mt-3 text-xs text-blue-700 font-medium bg-blue-50 px-2.5 py-1 rounded-md">
<svg className="w-3.5 h-3.5 mr-1 text-blue-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M5 13l4 4L19 7" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2.5"></path></svg>Lưu trữ dạng thô (Raw Format)
</div>
</div>
</div>
<div className="flex flex-col items-center md:items-start text-center md:text-left space-y-3">
<div className="w-12 h-12 rounded-xl bg-white border-2 border-indigo-600 text-indigo-600 font-bold text-base flex items-center justify-center shadow-sm">02</div>
<div className="pt-1">
<div className="flex items-center justify-center md:justify-start space-x-1.5">
<span className="w-2 h-2 rounded-full bg-indigo-600"></span>
<h3 className="text-base font-bold text-slate-900">ETL Processing</h3>
</div>
<p className="text-xs text-slate-600 mt-2 leading-relaxed">Xử lý và chuyển đổi dữ liệu qua các tầng Bronze, Silver và Gold.</p>
<div className="inline-flex items-center mt-3 text-xs text-indigo-700 font-medium bg-indigo-50 px-2.5 py-1 rounded-md">
<svg className="w-3.5 h-3.5 mr-1 text-indigo-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M5 13l4 4L19 7" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2.5"></path></svg>Xử lý dữ liệu theo pipeline
</div>
</div>
</div>
<div className="flex flex-col items-center md:items-start text-center md:text-left space-y-3">
<div className="w-12 h-12 rounded-xl bg-white border-2 border-emerald-600 text-emerald-600 font-bold text-base flex items-center justify-center shadow-sm">03</div>
<div className="pt-1">
<div className="flex items-center justify-center md:justify-start space-x-1.5">
<span className="w-2 h-2 rounded-full bg-emerald-600"></span>
<h3 className="text-base font-bold text-slate-900">Analytics</h3>
</div>
<p className="text-xs text-slate-600 mt-2 leading-relaxed">Tổng hợp dữ liệu tại Gold Layer và khai thác thông qua các công cụ báo cáo phân tích.</p>
<div className="inline-flex items-center mt-3 text-xs text-emerald-700 font-medium bg-emerald-50 px-2.5 py-1 rounded-md">
<svg className="w-3.5 h-3.5 mr-1 text-emerald-600" fill="none" stroke="currentColor" viewBox="0 0 24 24"><path d="M5 13l4 4L19 7" strokeLinecap="round" strokeLinejoin="round" strokeWidth="2.5"></path></svg>Trực quan hóa phục vụ chỉ đạo
</div>
</div>
</div>
</div>
</div>
</section>

</main>


<footer className="bg-white border-t border-slate-200 py-6 mt-12" data-purpose="site-footer">
<div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
<p className="text-xs text-slate-500 font-medium">
        © 2026 Trung tâm Công nghệ Thông tin - Đại học Cần Thơ (CUSC). Bảo lưu mọi quyền.
      </p>
</div>
</footer></div>;
}
