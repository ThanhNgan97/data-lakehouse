import { useEffect, useState } from "react";
import { NavLink } from "react-router-dom";
import { LogOut } from "lucide-react";

const items = [
  ["/user", "Trang chủ"],
  ["/user/file", "Tải file"],
  ["/user/api", "API"],
  ["/user/database", "Database"],
  ["/user/iot", "IoT"],
];

export default function UserTopNavigation() {
  const [scrolled, setScrolled] = useState(false);
  const username = localStorage.getItem("username") || "Người dùng";
  const initials = username.trim().split(/\s+/).slice(-2).map((word) => word[0]).join("").toUpperCase() || "ND";

  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 8);
    onScroll();
    window.addEventListener("scroll", onScroll, { passive: true });
    return () => window.removeEventListener("scroll", onScroll);
  }, []);

  function logout() {
    localStorage.removeItem("token");
    localStorage.removeItem("role");
    localStorage.removeItem("username");
    window.location.assign("/login");
  }

  return (
    <header className={`sticky top-0 z-50 border-b border-slate-200 transition-shadow ${scrolled ? "bg-white/95 backdrop-blur-md shadow-sm" : "bg-white"}`}>
      <div className="mx-auto flex min-h-[72px] max-w-[1680px] flex-wrap items-center justify-between gap-3 px-4 py-3 sm:px-6 lg:px-8">
        <NavLink to="/user" className="flex min-w-0 items-center gap-3 rounded-lg focus-visible:outline focus-visible:outline-2 focus-visible:outline-blue-600">
          <img src="/CUSC Logo Series.png" alt="CUSC" className="h-9 w-auto shrink-0 object-contain" />
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-2">
              <span className="text-[11px] font-extrabold tracking-tight text-slate-900 sm:text-sm">CUSC ANALYSIS PLATFORM</span>
              <span className="rounded-full bg-blue-50 px-2 py-0.5 text-[9px] font-semibold text-blue-700">USER PORTAL</span>
            </div>
            <p className="mt-1 hidden text-[10px] text-slate-500 sm:block">Trung tâm Công nghệ Phần mềm - Đại học Cần Thơ</p>
          </div>
        </NavLink>

        <nav aria-label="Điều hướng User Portal" className="order-3 flex w-full gap-1 overflow-x-auto xl:order-none xl:w-auto">
          {items.map(([to, label]) => (
            <NavLink key={to} to={to} end={to === "/user"} className={({ isActive }) => `relative flex shrink-0 items-center rounded-lg px-3 py-2 text-sm font-medium transition-colors focus-visible:outline focus-visible:outline-2 focus-visible:outline-blue-600 ${isActive ? "bg-blue-50 text-blue-700 after:absolute after:bottom-0 after:left-3 after:right-3 after:h-0.5 after:bg-blue-700" : "text-slate-600 hover:bg-slate-50 hover:text-blue-700"}`}>
              {label}
            </NavLink>
          ))}
        </nav>

        <div className="flex items-center gap-3">
          <div className="hidden items-center gap-2.5 sm:flex">
            <span aria-hidden="true" className="flex h-8 w-8 items-center justify-center rounded-full bg-blue-600 text-[11px] font-bold text-white">{initials}</span>
            <div className="hidden max-w-36 lg:block">
              <p title={username} className="truncate text-xs font-semibold text-slate-700">{username}</p>
              <p className="mt-0.5 text-[10px] text-slate-500">Tài khoản người dùng</p>
            </div>
          </div>
          <button type="button" onClick={logout} className="flex items-center gap-2 rounded-lg bg-slate-950 px-3 py-2 text-xs font-bold text-white shadow-sm transition hover:-translate-y-0.5 hover:bg-slate-800 hover:shadow-md focus-visible:outline focus-visible:outline-2 focus-visible:outline-blue-600">
            <LogOut size={16} aria-hidden="true" />Đăng xuất
          </button>
        </div>
      </div>
    </header>
  );
}
