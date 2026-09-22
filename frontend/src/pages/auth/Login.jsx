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
    <div className="min-h-screen bg-slate-50 flex flex-col items-center justify-center p-4 font-sans">
      <div className="w-full max-w-md bg-white rounded-xl shadow-lg border border-slate-200 p-8">
        {/* Header (Logo & Tiêu đề) */}
        <div className="flex flex-col items-center mb-8 text-center">
          <img
            src="/CUSC Logo Series.png"
            alt="CUSC Logo"
            className="h-16 w-auto object-contain mb-4"
            onError={(e) => {
              e.target.style.display = "none";
            }}
          />
          <h2 className="text-xl font-bold text-slate-800">
            CUSC ANALYSIS PLATFORM
          </h2>
          <p className="text-sm text-slate-500 mt-1">
            Trung tâm Công nghệ Thông tin - Đại học Cần Thơ 
          </p>
        </div>

        {/* Thông báo lỗi */}
        {error && (
          <div className="bg-red-50 border border-red-200 text-red-600 p-3 rounded-lg mb-6 text-sm flex items-start gap-2">
            <AlertCircle className="w-4 h-4 shrink-0 mt-0.5" />
            <span>{error}</span>
          </div>
        )}

        {/* Form Đăng nhập */}
        <form onSubmit={handleLogin} className="space-y-5">
          <div>
            <label className="block text-sm font-medium text-slate-700 mb-1">
              Tài khoản
            </label>
            <div className="relative">
              <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none text-slate-400">
                <User className="w-4 h-4" />
              </div>
              <input
                type="text"
                placeholder="Nhập tài khoản"
                className="w-full pl-9 pr-3 py-2.5 bg-white border border-slate-300 rounded-lg text-slate-800 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-colors"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                required
              />
            </div>
          </div>

          <div>
            <label className="block text-sm font-medium text-slate-700 mb-1">
              Mật khẩu
            </label>
            <div className="relative">
              <div className="absolute inset-y-0 left-0 pl-3 flex items-center pointer-events-none text-slate-400">
                <Lock className="w-4 h-4" />
              </div>
              <input
                type={showPassword ? "text" : "password"}
                placeholder="Nhập mật khẩu"
                className="w-full pl-9 pr-10 py-2.5 bg-white border border-slate-300 rounded-lg text-slate-800 text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-colors"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
              />
              <button
                type="button"
                onClick={() => setShowPassword(!showPassword)}
                className="absolute inset-y-0 right-0 pr-3 flex items-center text-slate-400 hover:text-slate-600 focus:outline-none"
                title={showPassword ? "Ẩn mật khẩu" : "Xem mật khẩu"}
              >
                {showPassword ? (
                  <EyeOff className="w-4 h-4" />
                ) : (
                  <Eye className="w-4 h-4" />
                )}
              </button>
            </div>
          </div>

          <div className="flex items-center justify-between text-sm">
            <label className="flex items-center gap-2 cursor-pointer text-slate-600">
              <input
                type="checkbox"
                checked={rememberMe}
                onChange={(e) => setRememberMe(e.target.checked)}
                className="w-4 h-4 rounded border-slate-300 text-blue-600 focus:ring-blue-500 cursor-pointer"
              />
              <span>Ghi nhớ tài khoản</span>
            </label>
          </div>

          <button
            type="submit"
            disabled={isLoading}
            className="w-full bg-blue-600 hover:bg-blue-700 text-white font-semibold py-2.5 px-4 rounded-lg shadow-sm transition-colors flex items-center justify-center gap-2 disabled:opacity-70 disabled:cursor-not-allowed"
          >
            {isLoading ? (
              <>
                <div className="w-4 h-4 border-2 border-white border-t-transparent rounded-full animate-spin"></div>
                <span>Đang đăng nhập...</span>
              </>
            ) : (
              <span>Đăng nhập</span>
            )}
          </button>
        </form>
      </div>

      <div className="mt-8 text-center text-xs text-slate-400">
        <p>© 2026 CUSC. Hệ thống Quản trị Dữ liệu Hành chính Công.</p>
      </div>
    </div>
  );
};

export default Login;
