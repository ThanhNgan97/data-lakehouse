import React, { useEffect, useState } from 'react';
import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

const authHeader = () => ({
  Authorization: `Bearer ${localStorage.getItem('token')}`,
});

const ROLE_COLORS = {
  admin: 'bg-purple-100 text-purple-700 border-purple-200',
  user: 'bg-blue-100 text-blue-700 border-blue-200',
};

const UserManagement = () => {
  const [users, setUsers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [success, setSuccess] = useState('');
  const [showForm, setShowForm] = useState(false);
  const [form, setForm] = useState({ username: '', password: '', full_name: '', email: '', role: 'user' });
  const [formLoading, setFormLoading] = useState(false);
  const [formError, setFormError] = useState('');

  const fetchUsers = async () => {
    setLoading(true);
    setError('');
    try {
      const res = await axios.get(`${API_URL}/users`, { headers: authHeader() });
      setUsers(res.data);
    } catch (e) {
      setError(e.response?.data?.detail || 'Không thể tải danh sách người dùng.');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchUsers(); }, []);

  const notify = (msg, isError = false) => {
    if (isError) setError(msg); else setSuccess(msg);
    setTimeout(() => { setError(''); setSuccess(''); }, 3500);
  };

  const handleDelete = async (user) => {
    if (!window.confirm(`Xóa tài khoản "${user.username}"?`)) return;
    try {
      await axios.delete(`${API_URL}/users/${user.id}`, { headers: authHeader() });
      notify(`✅ Đã xóa tài khoản "${user.username}".`);
      fetchUsers();
    } catch (e) {
      notify(e.response?.data?.detail || 'Lỗi xóa tài khoản.', true);
    }
  };

  const handleToggleActive = async (user) => {
    try {
      const res = await axios.put(`${API_URL}/users/${user.id}/toggle-active`, {}, { headers: authHeader() });
      notify(`✅ ${res.data.message}`);
      fetchUsers();
    } catch (e) {
      notify(e.response?.data?.detail || 'Lỗi cập nhật trạng thái.', true);
    }
  };

  const handleRoleChange = async (user, newRole) => {
    try {
      await axios.put(`${API_URL}/users/${user.id}/role`, { role: newRole }, { headers: authHeader() });
      notify(`✅ Đã đổi quyền "${user.username}" thành ${newRole === 'admin' ? 'Admin' : 'Cán bộ'}.`);
      fetchUsers();
    } catch (e) {
      notify(e.response?.data?.detail || 'Lỗi cập nhật quyền.', true);
    }
  };

  const handleCreateUser = async (e) => {
    e.preventDefault();
    setFormLoading(true);
    setFormError('');
    try {
      await axios.post(`${API_URL}/users`, form, { headers: authHeader() });
      notify(`✅ Đã tạo tài khoản "${form.username}".`);
      setShowForm(false);
      setForm({ username: '', password: '', full_name: '', email: '', role: 'user' });
      fetchUsers();
    } catch (e) {
      setFormError(e.response?.data?.detail || 'Lỗi tạo tài khoản.');
    } finally {
      setFormLoading(false);
    }
  };

  return (
    <div className="p-6 h-full overflow-y-auto">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h3 className="text-lg font-semibold text-gray-800">👥 Quản lý Người dùng</h3>
          <p className="text-sm text-gray-400 mt-1">
            Thêm, xóa, phân quyền và khoá tài khoản trong hệ thống.
          </p>
        </div>
        <button
          onClick={() => setShowForm((v) => !v)}
          className="flex items-center gap-2 px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm rounded-lg transition shadow"
        >
          {showForm ? '✕ Đóng' : '+ Thêm người dùng'}
        </button>
      </div>

      {/* Thông báo */}
      {error && (
        <div className="mb-4 p-3 bg-red-50 border border-red-200 text-red-600 rounded-lg text-sm">{error}</div>
      )}
      {success && (
        <div className="mb-4 p-3 bg-green-50 border border-green-200 text-green-700 rounded-lg text-sm">{success}</div>
      )}

      {/* Form thêm user */}
      {showForm && (
        <div className="mb-6 bg-white border border-blue-100 rounded-xl p-5 shadow-sm">
          <h4 className="font-semibold text-gray-700 mb-4 text-sm">Tạo tài khoản mới</h4>
          <form onSubmit={handleCreateUser} className="grid grid-cols-2 gap-4">
            <div>
              <label className="block text-xs text-gray-500 mb-1">Tên đăng nhập *</label>
              <input
                required
                value={form.username}
                onChange={(e) => setForm({ ...form, username: e.target.value })}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
                placeholder="vd: canbo_phong_kh"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Mật khẩu *</label>
              <input
                required
                type="password"
                value={form.password}
                onChange={(e) => setForm({ ...form, password: e.target.value })}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
                placeholder="Tối thiểu 6 ký tự"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Họ và tên</label>
              <input
                value={form.full_name}
                onChange={(e) => setForm({ ...form, full_name: e.target.value })}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
                placeholder="vd: Nguyễn Văn A"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Email</label>
              <input
                type="email"
                value={form.email}
                onChange={(e) => setForm({ ...form, email: e.target.value })}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
                placeholder="vd: email@cusc.vn"
              />
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Vai trò</label>
              <select
                value={form.role}
                onChange={(e) => setForm({ ...form, role: e.target.value })}
                className="w-full border border-gray-300 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-blue-400"
              >
                <option value="user">Cán bộ (User)</option>
                <option value="admin">Quản trị (Admin)</option>
              </select>
            </div>
            <div className="flex items-end gap-3">
              {formError && <span className="text-xs text-red-500 flex-1">{formError}</span>}
              <button
                type="submit"
                disabled={formLoading}
                className="px-5 py-2 bg-blue-600 hover:bg-blue-700 text-white text-sm rounded-lg transition disabled:opacity-60 whitespace-nowrap"
              >
                {formLoading ? 'Đang tạo...' : '+ Tạo tài khoản'}
              </button>
            </div>
          </form>
        </div>
      )}

      {/* Bảng danh sách */}
      {loading ? (
        <div className="text-center text-gray-400 py-16 text-sm">Đang tải danh sách người dùng...</div>
      ) : (
        <div className="bg-white rounded-xl border border-gray-200 shadow-sm overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-gray-50 border-b border-gray-200">
              <tr>
                {['#', 'Tài khoản', 'Họ tên / Email', 'Vai trò', 'Trạng thái', 'Ngày tạo', 'Thao tác'].map((h) => (
                  <th key={h} className="text-left px-4 py-3 text-xs text-gray-500 font-semibold">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {users.map((u, idx) => (
                <tr key={u.id} className="border-b border-gray-100 hover:bg-gray-50 transition">
                  <td className="px-4 py-3 text-gray-400 text-xs">{idx + 1}</td>
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-2">
                      <div className="w-8 h-8 rounded-full bg-gradient-to-br from-blue-500 to-teal-400 flex items-center justify-center text-white font-bold text-xs shrink-0">
                        {u.username?.[0]?.toUpperCase()}
                      </div>
                      <span className="font-medium text-gray-800">{u.username}</span>
                    </div>
                  </td>
                  <td className="px-4 py-3 text-gray-500 text-xs">
                    <div>{u.full_name || <span className="italic text-gray-300">—</span>}</div>
                    <div className="text-gray-400">{u.email || ''}</div>
                  </td>
                  <td className="px-4 py-3">
                    <select
                      value={u.role}
                      onChange={(e) => handleRoleChange(u, e.target.value)}
                      className={`text-xs border rounded-full px-2 py-1 font-medium cursor-pointer ${ROLE_COLORS[u.role] || 'bg-gray-100 text-gray-600 border-gray-200'}`}
                    >
                      <option value="user">Cán bộ</option>
                      <option value="admin">Admin</option>
                    </select>
                  </td>
                  <td className="px-4 py-3">
                    <span className={`text-xs px-2 py-1 rounded-full border font-medium ${u.is_active ? 'bg-green-50 text-green-700 border-green-200' : 'bg-red-50 text-red-500 border-red-200'}`}>
                      {u.is_active ? '● Hoạt động' : '● Bị khoá'}
                    </span>
                  </td>
                  <td className="px-4 py-3 text-gray-400 text-xs">
                    {u.created_at ? new Date(u.created_at).toLocaleDateString('vi-VN') : '—'}
                  </td>
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-1">
                      <button
                        onClick={() => handleToggleActive(u)}
                        className={`px-2 py-1 text-xs rounded transition border ${u.is_active ? 'bg-yellow-50 hover:bg-yellow-100 text-yellow-700 border-yellow-200' : 'bg-green-50 hover:bg-green-100 text-green-700 border-green-200'}`}
                      >
                        {u.is_active ? '🔒 Khoá' : '🔓 Mở'}
                      </button>
                      <button
                        onClick={() => handleDelete(u)}
                        className="px-2 py-1 text-xs rounded bg-red-50 hover:bg-red-100 text-red-500 border border-red-200 transition"
                      >
                        🗑 Xóa
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
              {users.length === 0 && (
                <tr>
                  <td colSpan={7} className="text-center py-10 text-gray-400 text-sm">Chưa có người dùng nào.</td>
                </tr>
              )}
            </tbody>
          </table>
          <div className="px-4 py-2 text-xs text-gray-400 border-t bg-gray-50">Tổng {users.length} tài khoản</div>
        </div>
      )}
    </div>
  );
};

export default UserManagement;
