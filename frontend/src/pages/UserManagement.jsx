import React, { useEffect, useState } from 'react';
import axios from 'axios';
import { Users, X, UserPlus, Lock, Unlock, Trash2 } from 'lucide-react';

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
      notify(`Thành công: Đã xóa tài khoản "${user.username}".`);
      fetchUsers();
    } catch (e) {
      notify(e.response?.data?.detail || 'Lỗi xóa tài khoản.', true);
    }
  };

  const handleToggleActive = async (user) => {
    try {
      const res = await axios.put(`${API_URL}/users/${user.id}/toggle-active`, {}, { headers: authHeader() });
      notify(`Thành công: ${res.data.message}`);
      fetchUsers();
    } catch (e) {
      notify(e.response?.data?.detail || 'Lỗi cập nhật trạng thái.', true);
    }
  };

  const handleRoleChange = async (user, newRole) => {
    try {
      await axios.put(`${API_URL}/users/${user.id}/role`, { role: newRole }, { headers: authHeader() });
      notify(`Thành công: Đã đổi quyền "${user.username}" thành ${newRole === 'admin' ? 'Admin' : 'Cán bộ'}.`);
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
      notify(`Thành công: Đã tạo tài khoản "${form.username}".`);
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
    <div className="p-6 h-full overflow-y-auto bg-slate-50/50">
      {/* Header */}
      <div className="flex items-center justify-between mb-8">
        <div>
          <h3 className="text-xl font-bold text-slate-800 flex items-center gap-2 tracking-tight">
            <Users className="w-6 h-6 text-blue-600" /> Quản lý Người dùng
          </h3>
          <p className="text-sm text-slate-500 mt-1.5 font-medium">
            Thêm, xóa, phân quyền và khoá tài khoản trong hệ thống.
          </p>
        </div>
        <button
          onClick={() => setShowForm((v) => !v)}
          className="flex items-center gap-2 px-5 py-2.5 bg-blue-600 hover:bg-blue-700 text-white text-sm font-semibold rounded-xl transition-all duration-300 shadow-md shadow-blue-600/20"
        >
          {showForm ? <><X className="w-4 h-4" /> Đóng</> : <><UserPlus className="w-4 h-4" /> Thêm người dùng</>}
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
                {formLoading ? 'Đang tạo...' : <span className="flex items-center gap-1"><UserPlus className="w-4 h-4" /> Tạo tài khoản</span>}
              </button>
            </div>
          </form>
        </div>
      )}

      {/* Bảng danh sách */}
      {loading ? (
        <div className="text-center text-slate-400 py-16 text-sm font-medium">Đang tải danh sách người dùng...</div>
      ) : (
        <div className="bg-white rounded-2xl border border-slate-100 shadow-xl shadow-slate-200/40 overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-slate-50 border-b border-slate-100">
              <tr>
                {['#', 'Tài khoản', 'Họ tên / Email', 'Vai trò', 'Trạng thái', 'Ngày tạo', 'Thao tác'].map((h) => (
                  <th key={h} className="text-left px-6 py-4 text-xs text-slate-500 font-bold uppercase tracking-wider">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {users.map((u, idx) => (
                <tr key={u.id} className="border-b border-slate-100 hover:bg-slate-50/80 transition-colors">
                  <td className="px-6 py-4 text-slate-400 font-medium text-xs">{idx + 1}</td>
                  <td className="px-6 py-4">
                    <div className="flex items-center gap-3">
                      <div className="w-9 h-9 rounded-xl bg-gradient-to-br from-blue-500 to-indigo-500 flex items-center justify-center text-white font-bold text-xs shadow-md shadow-blue-500/20 shrink-0">
                        {u.username?.[0]?.toUpperCase()}
                      </div>
                      <span className="font-bold text-slate-700">{u.username}</span>
                    </div>
                  </td>
                  <td className="px-6 py-4 text-slate-500 text-xs font-medium">
                    <div className="text-slate-800 font-semibold">{u.full_name || <span className="italic text-slate-300">—</span>}</div>
                    <div className="text-slate-500 mt-0.5">{u.email || ''}</div>
                  </td>
                  <td className="px-6 py-4">
                    <select
                      value={u.role}
                      onChange={(e) => handleRoleChange(u, e.target.value)}
                      className={`text-xs border rounded-full px-3 py-1 font-bold cursor-pointer focus:outline-none focus:ring-2 focus:ring-blue-500/20 ${ROLE_COLORS[u.role] || 'bg-slate-100 text-slate-600 border-slate-200'}`}
                    >
                      <option value="user">Cán bộ</option>
                      <option value="admin">Admin</option>
                    </select>
                  </td>
                  <td className="px-6 py-4">
                    <span className={`text-xs px-3 py-1.5 rounded-full border font-bold flex items-center w-max gap-1.5 ${u.is_active ? 'bg-emerald-50 text-emerald-700 border-emerald-200 shadow-sm shadow-emerald-500/10' : 'bg-red-50 text-red-600 border-red-200 shadow-sm shadow-red-500/10'}`}>
                      <span className={`w-1.5 h-1.5 rounded-full ${u.is_active ? 'bg-emerald-500 animate-pulse' : 'bg-red-500'}`}></span>
                      {u.is_active ? 'Hoạt động' : 'Bị khoá'}
                    </span>
                  </td>
                  <td className="px-6 py-4 text-slate-400 text-xs font-medium">
                    {u.created_at ? new Date(u.created_at).toLocaleDateString('vi-VN') : '—'}
                  </td>
                  <td className="px-6 py-4">
                    <div className="flex items-center gap-1">
                      <button
                        onClick={() => handleToggleActive(u)}
                        className={`flex items-center gap-1 px-2 py-1 text-xs rounded transition border ${u.is_active ? 'bg-yellow-50 hover:bg-yellow-100 text-yellow-700 border-yellow-200' : 'bg-green-50 hover:bg-green-100 text-green-700 border-green-200'}`}
                      >
                        {u.is_active ? <><Lock className="w-3 h-3" /> Khoá</> : <><Unlock className="w-3 h-3" /> Mở</>}
                      </button>
                      <button
                        onClick={() => handleDelete(u)}
                        className="flex items-center gap-1 px-2 py-1 text-xs rounded bg-red-50 hover:bg-red-100 text-red-500 border border-red-200 transition"
                      >
                        <Trash2 className="w-3 h-3" /> Xóa
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
              {users.length === 0 && (
                <tr>
                  <td colSpan={7} className="text-center py-12 text-slate-400 text-sm font-medium">Chưa có người dùng nào.</td>
                </tr>
              )}
            </tbody>
          </table>
          <div className="px-6 py-3 text-xs font-semibold text-slate-500 border-t border-slate-100 bg-slate-50">Tổng {users.length} tài khoản</div>
        </div>
      )}
    </div>
  );
};

export default UserManagement;
