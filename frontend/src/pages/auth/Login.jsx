import React, { useState } from "react";
import axios from "axios";
import { useNavigate } from "react-router-dom";
import { Eye, EyeOff, User, Lock, AlertCircle } from "lucide-react";

const API_BASE_URL =
  import.meta.env.VITE_API_URL || "http://localhost:8000/api";

const Login = ({ setToken, setRole }) => {
  const savedUser = localStorage.getItem("remembered_username") || "";
  const [username, setUsername] = useState(savedUser);
  const [password, setPassword] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [rememberMe, setRememberMe] = useState(Boolean(savedUser));
  const [error, setError] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  const navigate = useNavigate();

  const handleLogin = async (e) => {
    e.preventDefault();
    setError("");
    setIsLoading(true);

    try {
      const formData = new URLSearchParams();
      formData.append("username", username);
      formData.append("password", password);

      const res = await axios.post(`${API_BASE_URL}/login`, formData);
      const { access_token, role } = res.data;

      // Lưu thông tin phiên đăng nhập
      localStorage.setItem("token", access_token);
      localStorage.setItem("role", role);
      localStorage.setItem("username", username);

      if (rememberMe) {
        localStorage.setItem("remembered_username", username);
      } else {
        localStorage.removeItem("remembered_username");
      }

      if (typeof setToken === "function") setToken(access_token);
      if (typeof setRole === "function") setRole(role);

      if (role === "admin") {
        navigate("/admin");
      } else {
        navigate("/user");
      }
    } catch (err) {
      console.error(err);
      if (err.response && err.response.data && err.response.data.detail) {
        setError(err.response.data.detail);
      } else {
        setError("Tài khoản hoặc mật khẩu không chính xác.");
      }
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-[#F8F9FF] font-sans text-navy-950 lg:flex">
      <section className="relative flex min-h-[460px] w-full overflow-hidden bg-navy-800 px-6 py-8 text-white sm:px-10 sm:py-10 lg:min-h-screen lg:w-[48%] lg:px-12 lg:py-12 xl:px-14">
        <div
          className="pointer-events-none absolute inset-0 opacity-35 [background-image:radial-gradient(circle,rgba(56,189,248,0.72)_1px,transparent_1px)] [background-size:24px_24px]"
          aria-hidden="true"
        />
        <div
          className="pointer-events-none absolute right-[-8rem] top-[-8rem] h-96 w-96 rounded-full bg-cobalt-700/20 blur-3xl"
          aria-hidden="true"
        />
        <div
          className="pointer-events-none absolute bottom-[-7rem] left-[-6rem] h-80 w-80 rounded-full bg-lake-400/10 blur-3xl"
          aria-hidden="true"
        />



        {/* UI_PHASE_1B_LOGIN_PARTICLE_WAVE_START */}
<style>{`
  @keyframes cuscLoginParticleTravel {
    0% {
      transform: translate3d(-4%, 8%, 0);
      opacity: 0.18;
    }

    20% {
      opacity: 0.32;
    }

    50% {
      transform: translate3d(0%, 0%, 0);
      opacity: 0.56;
    }

    80% {
      opacity: 0.34;
    }

    100% {
      transform: translate3d(4%, -8%, 0);
      opacity: 0.18;
    }
  }

  @keyframes cuscLoginParticleTwist {
    0%, 100% {
      transform: rotate(-5deg) scaleY(0.88) scaleX(0.98);
    }

    25% {
      transform: rotate(-1deg) scaleY(1.02) scaleX(1);
    }

    50% {
      transform: rotate(4deg) scaleY(0.82) scaleX(1.01);
    }

    75% {
      transform: rotate(0deg) scaleY(1.05) scaleX(0.99);
    }
  }

  @keyframes cuscLoginParticleFlow {
    from {
      stroke-dashoffset: 0;
    }

    to {
      stroke-dashoffset: -150;
    }
  }

  @keyframes cuscLoginParticleGlow {
    0%, 100% {
      opacity: 0.18;
    }

    50% {
      opacity: 0.4;
    }
  }

  .cusc-login-particle-wave {
    animation:
      cuscLoginParticleTravel
      10s
      cubic-bezier(0.42, 0, 0.25, 1)
      infinite;
    will-change: transform, opacity;
  }

  .cusc-login-particle-wave-inner {
    animation:
      cuscLoginParticleTwist
      4.8s
      ease-in-out
      infinite;
    transform-origin: center;
    will-change: transform;
  }

  .cusc-login-particle-path {
    animation:
      cuscLoginParticleFlow
      6.2s
      linear
      infinite;
  }

  .cusc-login-particle-glow {
    animation:
      cuscLoginParticleFlow
      6.2s
      linear
      infinite,
      cuscLoginParticleGlow
      3.4s
      ease-in-out
      infinite;
  }

  @media (prefers-reduced-motion: reduce) {
    .cusc-login-particle-wave {
      animation-duration: 18s;
    }

    .cusc-login-particle-wave-inner {
      animation-duration: 9s;
    }

    .cusc-login-particle-path,
    .cusc-login-particle-glow {
      animation-duration: 11s;
    }
  }
`}</style>

<div
  aria-hidden="true"
  className="cusc-login-particle-wave pointer-events-none absolute inset-0 overflow-hidden"
>
  <div className="cusc-login-particle-wave-inner absolute inset-0">
    <svg
      viewBox="0 0 920 560"
      preserveAspectRatio="none"
      className="absolute bottom-[-10%] left-[-10%] h-[74%] w-[114%]"
    >
      <defs>
        <linearGradient
          id="cuscLoginParticleGradient"
          x1="0%"
          y1="100%"
          x2="100%"
          y2="0%"
        >
          <stop offset="0%" stopColor="#38BDF8" stopOpacity="0" />
          <stop offset="20%" stopColor="#7DD3FC" stopOpacity="0.48" />
          <stop offset="52%" stopColor="#93C5FD" stopOpacity="0.92" />
          <stop offset="82%" stopColor="#60A5FA" stopOpacity="0.42" />
          <stop offset="100%" stopColor="#60A5FA" stopOpacity="0" />
        </linearGradient>

        <filter
          id="cuscLoginParticleBlur"
          x="-50%"
          y="-50%"
          width="200%"
          height="200%"
        >
          <feGaussianBlur stdDeviation="4.6" />
        </filter>
      </defs>

      <path
        className="cusc-login-particle-glow"
        d="
          M 30 530
          C 120 500, 200 430, 280 360
          S 450 235, 565 185
          S 730 130, 900 75
        "
        fill="none"
        stroke="url(#cuscLoginParticleGradient)"
        strokeWidth="14"
        strokeLinecap="round"
        strokeDasharray="1 21"
        filter="url(#cuscLoginParticleBlur)"
      />

      <path
        className="cusc-login-particle-path"
        d="
          M 30 530
          C 120 500, 200 430, 280 360
          S 450 235, 565 185
          S 730 130, 900 75
        "
        fill="none"
        stroke="url(#cuscLoginParticleGradient)"
        strokeWidth="8"
        strokeLinecap="round"
        strokeDasharray="1 21"
      />
    </svg>
  </div>
</div>
{/* UI_PHASE_1B_LOGIN_PARTICLE_WAVE_END */}

        <div className="relative z-10 flex w-full flex-col justify-between gap-10">
          <div className="flex min-w-0 items-center gap-3">
            <img
              src="/CUSC Logo Series.png"
              alt="CUSC Logo"
              className="h-11 w-auto shrink-0 object-contain sm:h-12"
              onError={(e) => {
                e.target.style.display = "none";
              }}
            />
            <div className="min-w-0 border-l border-white/20 pl-3">
              <p className="truncate font-display text-base font-extrabold leading-tight tracking-tight text-white sm:text-lg">
                CUSC ANALYSIS PLATFORM
              </p>
              <p className="mt-0.5 truncate text-xs font-medium text-blue-100/80 sm:text-sm">
                Trung tâm Công nghệ Thông tin - Đại học Cần Thơ
              </p>
            </div>
          </div>

          <div className="my-auto max-w-xl py-3 lg:py-0">
            <div className="max-w-lg">
              <h1 className="font-display text-[2rem] font-bold leading-[1.2] tracking-[-0.025em] text-white sm:text-4xl">
                Nền tảng Phân tích Dữ liệu
                <span className="mt-1 block text-blue-200">
                  Giáo dục &amp; Hành chính Công
                </span>
              </h1>
              <p className="mt-4 max-w-md text-sm leading-6 text-blue-100/75">
                Hạ tầng phân tích dữ liệu tập trung phục vụ giáo dục và hành chính công.
              </p>
            </div>

            <div className="mt-8 max-w-lg rounded-panel border border-white/10 bg-white/[0.055] p-5 shadow-card backdrop-blur-sm">
              <p className="mb-3 font-data text-[11px] font-semibold uppercase tracking-[0.08em] text-blue-200/90">
                Data Lakehouse Topology
              </p>

              <div className="space-y-2">
                <div className="flex items-center justify-between gap-4 rounded-control bg-white/[0.055] px-4 py-3">
                  <div className="flex items-center gap-3">
                    <span className="h-7 w-2 rounded-full bg-bronze-300" />
                    <span className="font-display text-sm font-bold text-white">Bronze Tier</span>
                  </div>
                  <span className="rounded bg-bronze-500/20 px-2 py-0.5 font-data text-[10px] font-semibold text-orange-100">
                    INGESTION
                  </span>
                </div>
                <div className="mx-auto h-3 w-px bg-lake-300/35" aria-hidden="true" />
                <div className="flex items-center justify-between gap-4 rounded-control bg-white/[0.055] px-4 py-3">
                  <div className="flex items-center gap-3">
                    <span className="h-7 w-2 rounded-full bg-silver-300" />
                    <span className="font-display text-sm font-bold text-white">Silver Tier</span>
                  </div>
                  <span className="rounded bg-silver-500/20 px-2 py-0.5 font-data text-[10px] font-semibold text-blue-100">
                    CONFORMED
                  </span>
                </div>
                <div className="mx-auto h-3 w-px bg-lake-300/35" aria-hidden="true" />
                <div className="flex items-center justify-between gap-4 rounded-control bg-white/[0.055] px-4 py-3">
                  <div className="flex items-center gap-3">
                    <span className="h-7 w-2 rounded-full bg-gold-300" />
                    <span className="font-display text-sm font-bold text-white">Gold Tier</span>
                  </div>
                  <span className="rounded bg-gold-500/20 px-2 py-0.5 font-data text-[10px] font-semibold text-yellow-100">
                    ANALYTICS
                  </span>
                </div>
              </div>
            </div>
          </div>

          <div className="h-px w-24 bg-lake-300/70" aria-hidden="true" />
        </div>
      </section>

      <section className="flex min-h-[620px] w-full flex-col items-center justify-between bg-[#F8F9FF] px-5 py-8 sm:px-10 sm:py-10 lg:min-h-screen lg:w-[52%] lg:px-12 lg:py-12">
        <div className="w-full" />

        <div className="my-auto w-full max-w-[440px] rounded-panel border border-white bg-white p-7 shadow-popover sm:p-9">
          <p className="font-data text-[11px] font-semibold uppercase tracking-[0.08em] text-cobalt-700">
            Truy cập CUSC Analysis Platform
          </p>

          <div className="mb-8 mt-6">
            <h2 className="font-display text-3xl font-bold tracking-[-0.02em] text-navy-950">
              Đăng nhập
            </h2>
            <p className="mt-2 text-sm leading-6 text-ink-600">
              Vui lòng nhập thông tin xác thực để truy cập hệ thống
            </p>
          </div>

          {error && (
            <div className="mb-6 flex items-start gap-2 rounded-control border border-rose-200 bg-rose-50 p-3 text-sm text-rose-700">
              <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
              <span>{error}</span>
            </div>
          )}

          <form onSubmit={handleLogin} className="space-y-5">
            <div>
              <label className="mb-1.5 block text-sm font-semibold text-navy-950">
                Tài khoản
              </label>
              <div className="relative">
                <div className="pointer-events-none absolute inset-y-0 left-0 flex items-center pl-3.5 text-ink-400">
                  <User className="h-5 w-5" />
                </div>
                <input
                  type="text"
                  placeholder="Nhập tài khoản"
                  className="h-12 w-full rounded-panel border border-ink-100 bg-white pl-11 pr-4 text-sm text-navy-950 shadow-card outline-none transition placeholder:text-ink-400 focus:border-cobalt-200 focus:shadow-elevated focus:ring-2 focus:ring-cobalt-100"
                  value={username}
                  onChange={(e) => setUsername(e.target.value)}
                  required
                />
              </div>
            </div>

            <div>
              <label className="mb-1.5 block text-sm font-semibold text-navy-950">
                Mật khẩu
              </label>
              <div className="relative">
                <div className="pointer-events-none absolute inset-y-0 left-0 flex items-center pl-3.5 text-ink-400">
                  <Lock className="h-5 w-5" />
                </div>
                <input
                  type={showPassword ? "text" : "password"}
                  placeholder="Nhập mật khẩu"
                  className="h-12 w-full rounded-panel border border-ink-100 bg-white pl-11 pr-11 text-sm text-navy-950 shadow-card outline-none transition placeholder:text-ink-400 focus:border-cobalt-200 focus:shadow-elevated focus:ring-2 focus:ring-cobalt-100"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  required
                />
                <button
                  type="button"
                  onClick={() => setShowPassword(!showPassword)}
                  className="absolute inset-y-0 right-0 flex items-center pr-3.5 text-ink-400 transition hover:text-navy-800 focus:outline-none"
                  title={showPassword ? "Ẩn mật khẩu" : "Xem mật khẩu"}
                >
                  {showPassword ? (
                    <EyeOff className="h-5 w-5" />
                  ) : (
                    <Eye className="h-5 w-5" />
                  )}
                </button>
              </div>
            </div>

            <div className="flex items-center justify-between pt-1 text-sm">
              <label className="flex cursor-pointer select-none items-center gap-2.5 text-ink-600">
                <input
                  type="checkbox"
                  checked={rememberMe}
                  onChange={(e) => setRememberMe(e.target.checked)}
                  className="h-4 w-4 cursor-pointer rounded border-ink-300 text-cobalt-700 focus:ring-cobalt-500"
                />
                <span className="font-medium">Ghi nhớ tài khoản</span>
              </label>
            </div>

            <div className="pt-2">
              <button
                type="submit"
                disabled={isLoading}
                className="flex h-12 w-full items-center justify-center gap-2 rounded-panel bg-cobalt-700 px-4 text-sm font-semibold text-white shadow-elevated transition hover:bg-cobalt-800 active:scale-[0.99] focus-visible:outline-none focus-visible:ring-4 focus-visible:ring-cobalt-600/20 disabled:cursor-not-allowed disabled:opacity-70"
              >
                {isLoading ? (
                  <>
                    <div className="h-4 w-4 animate-spin rounded-full border-2 border-white border-t-transparent"></div>
                    <span>Đang đăng nhập...</span>
                  </>
                ) : (
                  <>
                    <span>Đăng nhập</span>
                    <span aria-hidden="true" className="text-lg leading-none">→</span>
                  </>
                )}
              </button>
            </div>
          </form>
        </div>

        <footer className="w-full py-3 text-center">
          <p className="text-xs text-ink-500">
            © 2026 CUSC. Hệ thống Quản trị Dữ liệu Hành chính Công.
          </p>
        </footer>
      </section>
    </div>
  );
};

export default Login;
