import React, { useState } from 'react';
import axios from 'axios';
import { useNavigate } from 'react-router-dom';

const Login = ({ setToken, setRole }) => {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const navigate = useNavigate();

  const handleLogin = async (e) => {
    e.preventDefault();
    setError(''); // Reset lỗi cũ trước khi bấm đăng nhập
    try {
      const formData = new URLSearchParams();
      formData.append('username', username);
      formData.append('password', password);


      const res = await axios.post(import.meta.env.VITE_API_URL + '/login', formData);
      console.log('Login response:', res.data); // Debug: In ra dữ liệu phản hồi từ server
      const { access_token, role } = res.data;

      // Lưu vào LocalStorage để duy trì phiên đăng nhập
      localStorage.setItem('token', access_token);
      localStorage.setItem('role', role);

      // Kiểm tra bảo mật nếu props setToken/setRole có tồn tại thì mới chạy
      if (typeof setToken === 'function') setToken(access_token);
      if (typeof setRole === 'function') setRole(role);

      // Điều hướng chuyển trang tự động dựa theo vai trò (Role)
      if (role === 'admin') {
        navigate('/admin');
      localStorage.setItem('username', username); // Lưu tên người dùng vào LocalStorage
      } else {
        navigate('/user');
      }

    } catch (err) {
      console.error(err);
      setError('Sai tài khoản hoặc mật khẩu! Hoặc Server Backend chưa bật.');
    }
  };

  return (
    <div className="flex h-screen bg-gray-50 font-sans">
      {/* Nửa trái: Form đăng nhập */}
      <div className="w-full lg:w-1/2 flex items-center justify-center p-8">
        <div className="w-full max-w-md bg-white p-10 rounded-2xl shadow-xl border border-gray-100">
          <div className="mb-8 text-center">
            <h1 className="text-3xl font-extrabold text-blue-600 tracking-tight">Lakehouse</h1>
            <p className="text-gray-500 mt-2 font-medium">Hệ thống Chia sẻ Dữ liệu Hành chính công</p>
          </div>

          {error && (
            <div className="bg-red-50 text-red-600 border border-red-200 p-3 rounded-lg mb-6 text-center font-medium text-sm animate-pulse">
              {error}
            </div>
          )}

          <form onSubmit={handleLogin} className="space-y-5">
            <div>
              <label className="block text-sm font-semibold text-gray-700 mb-1">Tài khoản</label>
              <input
                type="text"
                placeholder="Nhập tài khoản "
                className="mt-1 block w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition"
                value={username}
                onChange={(e) => setUsername(e.target.value)}
                required
              />
            </div>
            <div>
              <label className="block text-sm font-semibold text-gray-700 mb-1">Mật khẩu</label>
              <input
                type="password"
                placeholder="Nhập mật khẩu"
                className="mt-1 block w-full px-4 py-3 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition"
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                required
              />
            </div>
            <button
              type="submit"
              className="w-full bg-blue-600 hover:bg-blue-700 text-white font-bold py-3 px-4 rounded-lg shadow-md hover:shadow-lg transition duration-200 mt-2"
            >
              Đăng nhập hệ thống
            </button>
          </form>
        </div>
      </div>

      {/* Nửa phải: Giao diện nền đồ án */}
      <div className="hidden lg:flex w-1/2 bg-blue-900 items-center justify-center relative overflow-hidden">
        <div className="absolute inset-0 bg-gradient-to-br from-blue-800 to-indigo-900 opacity-90"></div>
        <div className="relative z-10 text-center text-white p-10">
          <h2 className="text-4xl font-extrabold mb-4 leading-tight">Mô hình Kiến trúc<br/>Bronze - Silver - Gold</h2>
          <p className="text-lg text-blue-200 font-light">Tích hợp Apache Spark, Kafka, MinIO & Superset</p>
          <div className="mt-8 flex justify-center space-x-2">
            <span className="px-3 py-1 bg-blue-700 text-xs font-semibold rounded-full text-blue-100">FastAPI</span>
            <span className="px-3 py-1 bg-blue-700 text-xs font-semibold rounded-full text-blue-100">ReactJS</span>
            <span className="px-3 py-1 bg-blue-700 text-xs font-semibold rounded-full text-blue-100">TailwindCSS</span>
          </div>
        </div>cl
      </div>
    </div>
  );
};

export default Login;
