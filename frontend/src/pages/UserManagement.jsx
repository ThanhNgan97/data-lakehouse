import React, { useEffect, useState } from 'react';
import axios from 'axios';

const BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000';

const UserManagement = () => {
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [successMsg, setSuccessMsg] = useState('');
  
  // Modal state
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [formData, setFormData] = useState({
    username: '',
    password: '',
    email: '',
    full_name: '',
    role: 'user',
  });
  const [submitting, setSubmitting] = useState(false);
  const [formError, setFormError] = useState('');

  const token = localStorage.getItem('token');
  const authHeader = { Authorization: `Bearer ${token}` };

  const fetchUsers = async () => {
    setLoading(true);
    setError('');
    try {
      // Try /users or /auth/users
      let res;
      try {
        res = await axios.get(`${BASE_URL}/users`, { headers: authHeader });
      } catch (err) {
        res = await axios.get(`${BASE_URL}/auth/users`, { headers: authHeader });
      }
      setUsers(res.data || []);
    } catch (err) {
      console.error(err);
      setError('Không thể tải danh sách người dùng. Vui lòng kiểm tra quyền Admin hoặc server backend.');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchUsers();
  }, []);

  const handleInputChange = (e) => {
    setFormData({
      ...formData,
      [e.target.name]: e.target.value,
    });
  };

  const handleCreateUser = async (e) => {
    e.preventDefault();
    setFormError('');
    setSubmitting(true);

    try {
      let res;
      try {
        res = await axios.post(`${BASE_URL}/create_user`, formData, { headers: authHeader });
      } catch (err) {
        res = await axios.post(`${BASE_URL}/auth/create_user`, formData, { headers: authHeader });
      }

      setSuccessMsg(`Tạo tài khoản "${formData.username}" thành công!`);
      setIsModalOpen(false);
      setFormData({
        username: '',
        password: '',
        email: '',
        full_name: '',
        role: 'user',
      });
      fetchUsers();
      setTimeout(() => setSuccessMsg(''), 4000);
    } catch (err) {
      console.error(err);
      const detail = err.response?.data?.detail || 'Lỗi khi tạo người dùng. Vui lòng kiểm tra lại.';
      setFormError(typeof detail === 'string' ? detail : JSON.stringify(detail));
    } finally {
      setSubmitting(false);
    }
  };

  const handleChangeRole = async (userId, currentRole) => {
    const newRole = currentRole === 'admin' ? 'user' : 'admin';
    if (!window.confirm(`Bạn có chắc muốn đổi vai trò của người dùng này thành '${newRole}'?`)) return;

    try {
      try {
        await axios.put(`${BASE_URL}/users/${userId}/role`, { role: newRole }, { headers: authHeader });
      } catch (err) {
        await axios.put(`${BASE_URL}/auth/users/${userId}/role`, { role: newRole }, { headers: authHeader });
      }
      setSuccessMsg('Cập nhật quyền thành công!');
      fetchUsers();
      setTimeout(() => setSuccessMsg(''), 3000);
    } catch (err) {
      alert('Lỗi cập nhật quyền: ' + (err.response?.data?.detail || err.message));
    }
  };

  return (
    <div className="p-6 bg-gray-50 min-h-full flex flex-col">
      {/* Header & Button */}
      <div className="flex flex-col md:flex-row justify-between items-start md:items-center mb-6 gap-4">
        <div>
          <h2 className="text-2xl font-bold text-gray-800">Quản lý Người dùng</h2>
          <p className="text-sm text-gray-500 mt-1">Danh sách tài khoản và phân quyền truy cập hệ thống Data Lakehouse</p>
        </div>

        <button
          onClick={() => setIsModalOpen(true)}
          className="bg-blue-600 hover:bg-blue-700 text-white font-semibold py-2.5 px-5 rounded-lg shadow-md hover:shadow-lg transition flex items-center gap-2"
        >
          <svg className="w-5 h-5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M12 4v16m8-8H4" />
          </svg>
          Tạo người dùng mới
        </button>
      </div>

      {/* Alert Messages */}
      {successMsg && (
        <div className="mb-4 bg-emerald-50 border border-emerald-200 text-emerald-700 px-4 py-3 rounded-lg flex items-center justify-between">
          <span>{successMsg}</span>
          <button onClick={() => setSuccessMsg('')} className="font-bold">✕</button>
        </div>
      )}

      {error && (
        <div className="mb-4 bg-rose-50 border border-rose-200 text-rose-700 px-4 py-3 rounded-lg flex items-center justify-between">
          <span>{error}</span>
          <button onClick={fetchUsers} className="underline text-sm font-semibold ml-2">Thử lại</button>
        </div>
      )}

      {/* User Table */}
      <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden flex-1">
        {loading ? (
          <div className="flex items-center justify-center p-12 text-gray-500">
            <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-blue-600 mr-3"></div>
            Đang tải danh sách tài khoản...
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-left border-collapse">
              <thead>
                <tr className="bg-gray-100/75 border-b border-gray-200 text-xs font-semibold text-gray-600 uppercase tracking-wider">
                  <th className="py-3.5 px-6">ID</th>
                  <th className="py-3.5 px-6">Tài khoản</th>
                  <th className="py-3.5 px-6">Họ & Tên</th>
                  <th className="py-3.5 px-6">Email</th>
                  <th className="py-3.5 px-6">Vai trò (Role)</th>
                  <th className="py-3.5 px-6">Trạng thái</th>
                  <th className="py-3.5 px-6 text-right">Thao tác</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200 text-sm text-gray-700">
                {users.length === 0 ? (
                  <tr>
                    <td colSpan="7" className="py-8 text-center text-gray-400">
                      Chưa có dữ liệu người dùng
                    </td>
                  </tr>
                ) : (
                  users.map((user) => (
                    <tr key={user.id || user.username} className="hover:bg-gray-50 transition">
                      <td className="py-4 px-6 font-mono text-xs text-gray-400">{user.id || '—'}</td>
                      <td className="py-4 px-6 font-semibold text-gray-900 flex items-center gap-2">
                        <div className="w-8 h-8 rounded-full bg-blue-100 text-blue-600 font-bold flex items-center justify-center text-xs">
                          {user.username ? user.username.charAt(0).toUpperCase() : 'U'}
                        </div>
                        {user.username}
                      </td>
                      <td className="py-4 px-6">{user.full_name || '—'}</td>
                      <td className="py-4 px-6 text-gray-500">{user.email || '—'}</td>
                      <td className="py-4 px-6">
                        <span
                          className={`inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium ${
                            user.role === 'admin'
                              ? 'bg-purple-100 text-purple-700 border border-purple-200'
                              : 'bg-emerald-100 text-emerald-700 border border-emerald-200'
                          }`}
                        >
                          {user.role === 'admin' ? 'Quản trị viên' : 'Người dùng'}
                        </span>
                      </td>
                      <td className="py-4 px-6">
                        <span className="inline-flex items-center gap-1.5 text-xs text-emerald-600 font-medium">
                          <span className="w-2 h-2 rounded-full bg-emerald-500 animate-pulse"></span>
                          Đang hoạt động
                        </span>
                      </td>
                      <td className="py-4 px-6 text-right">
                        {user.id ? (
                          <button
                            onClick={() => handleChangeRole(user.id, user.role)}
                            className="text-xs bg-gray-100 hover:bg-gray-200 text-gray-700 font-medium px-3 py-1.5 rounded transition border border-gray-300"
                          >
                            Đổi vai trò
                          </button>
                        ) : (
                          <span className="text-xs text-gray-400 italic">Mặc định</span>
                        )}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Modal Tạo Người Dùng Mới */}
      {isModalOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 backdrop-blur-sm p-4 animate-fadeIn">
          <div className="bg-white rounded-2xl shadow-2xl border border-gray-100 w-full max-w-md overflow-hidden">
            {/* Modal Header */}
            <div className="px-6 py-4 bg-slate-900 text-white flex justify-between items-center">
              <h3 className="text-lg font-bold">Tạo Tài Khoản Người Dùng Mới</h3>
              <button
                onClick={() => setIsModalOpen(false)}
                className="text-slate-400 hover:text-white text-xl font-bold transition"
              >
                ✕
              </button>
            </div>

            {/* Modal Form */}
            <form onSubmit={handleCreateUser} className="p-6 space-y-4">
              {formError && (
                <div className="bg-rose-50 border border-rose-200 text-rose-600 p-3 rounded-lg text-sm font-medium">
                  {formError}
                </div>
              )}

              <div>
                <label className="block text-xs font-semibold text-gray-700 uppercase mb-1">
                  Tên tài khoản (Username) <span className="text-red-500">*</span>
                </label>
                <input
                  type="text"
                  name="username"
                  value={formData.username}
                  onChange={handleInputChange}
                  placeholder="Nhập tên tài khoản (vd: user_khach)"
                  required
                  className="w-full px-3.5 py-2.5 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition"
                />
              </div>

              <div>
                <label className="block text-xs font-semibold text-gray-700 uppercase mb-1">
                  Mật khẩu <span className="text-red-500">*</span>
                </label>
                <input
                  type="password"
                  name="password"
                  value={formData.password}
                  onChange={handleInputChange}
                  placeholder="Nhập mật khẩu (tối thiểu 6 ký tự)"
                  minLength={6}
                  required
                  className="w-full px-3.5 py-2.5 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition"
                />
              </div>

              <div>
                <label className="block text-xs font-semibold text-gray-700 uppercase mb-1">Họ và tên</label>
                <input
                  type="text"
                  name="full_name"
                  value={formData.full_name}
                  onChange={handleInputChange}
                  placeholder="Nhập họ và tên (vd: Nguyễn Văn A)"
                  className="w-full px-3.5 py-2.5 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition"
                />
              </div>

              <div>
                <label className="block text-xs font-semibold text-gray-700 uppercase mb-1">Email</label>
                <input
                  type="email"
                  name="email"
                  value={formData.email}
                  onChange={handleInputChange}
                  placeholder="Nhập địa chỉ email (vd: user@lakehouse.vn)"
                  className="w-full px-3.5 py-2.5 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition"
                />
              </div>

              <div>
                <label className="block text-xs font-semibold text-gray-700 uppercase mb-1">
                  Vai trò (Role) <span className="text-red-500">*</span>
                </label>
                <select
                  name="role"
                  value={formData.role}
                  onChange={handleInputChange}
                  className="w-full px-3.5 py-2.5 border border-gray-300 rounded-lg text-sm focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition bg-white"
                >
                  <option value="user">👤 User (Người dùng tiêu chuẩn)</option>
                  <option value="admin">🛡️ Admin (Quản trị viên toàn quyền)</option>
                </select>
              </div>

              {/* Form Buttons */}
              <div className="pt-3 flex justify-end gap-3">
                <button
                  type="button"
                  onClick={() => setIsModalOpen(false)}
                  className="px-4 py-2.5 text-sm font-medium text-gray-600 hover:bg-gray-100 rounded-lg transition"
                >
                  Hủy
                </button>
                <button
                  type="submit"
                  disabled={submitting}
                  className="px-5 py-2.5 text-sm font-bold text-white bg-blue-600 hover:bg-blue-700 rounded-lg shadow-md hover:shadow-lg transition disabled:opacity-50"
                >
                  {submitting ? 'Đang khởi tạo...' : 'Xác nhận tạo'}
                </button>
              </div>
            </form>
          </div>
        </div>
      )}
    </div>
  );
};

export default UserManagement;
