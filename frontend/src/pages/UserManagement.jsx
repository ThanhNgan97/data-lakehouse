import React, { useEffect, useState } from 'react';
import axios from 'axios';
import Icon from '../components/icons';
import { Badge, Button, EmptyState } from '../components/ui';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

const authHeader = () => ({
  Authorization: `Bearer ${localStorage.getItem('token')}`,
});

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
      notify(`Đã xóa tài khoản "${user.username}".`);
      fetchUsers();
    } catch (e) {
      notify(e.response?.data?.detail || 'Lỗi xóa tài khoản.', true);
    }
  };

  const handleToggleActive = async (user) => {
    try {
      const res = await axios.put(`${API_URL}/users/${user.id}/toggle-active`, {}, { headers: authHeader() });
      notify(res.data.message);
      fetchUsers();
    } catch (e) {
      notify(e.response?.data?.detail || 'Lỗi cập nhật trạng thái.', true);
    }
  };

  const handleRoleChange = async (user, newRole) => {
    try {
      await axios.put(`${API_URL}/users/${user.id}/role`, { role: newRole }, { headers: authHeader() });
      notify(`Đã đổi quyền "${user.username}" thành ${newRole === 'admin' ? 'Admin' : 'Cán bộ'}.`);
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
      notify(`Đã tạo tài khoản "${form.username}".`);
      setShowForm(false);
      setForm({ username: '', password: '', full_name: '', email: '', role: 'user' });
      fetchUsers();
    } catch (e) {
      setFormError(e.response?.data?.detail || 'Lỗi tạo tài khoản.');
    } finally {
      setFormLoading(false);
    }
  };

  const inputClass =
    'w-full border border-ink-100 bg-ink-50/50 rounded-lg px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-lake-300 focus:bg-white transition';

  return (
    <div className="p-6 h-full overflow-y-auto bg-[#FAFBFD]">
      {/* Header */}
      <div className="flex items-center justify-between mb-6 flex-wrap gap-3">
        <div>
          <p className="font-data text-[11px] uppercase tracking-widest text-lake-600 font-semibold mb-1">
            Access control
          </p>
          <h3 className="text-lg font-bold text-ink-900">Quản lý Người dùng</h3>
          <p className="text-sm text-ink-400 mt-1">Thêm, xóa, phân quyền và khoá tài khoản trong hệ thống.</p>
        </div>
        <Button variant={showForm ? 'secondary' : 'primary'} onClick={() => setShowForm((v) => !v)}>
          <Icon name={showForm ? 'x' : 'plus'} className="w-4 h-4" />
          {showForm ? 'Đóng' : 'Thêm người dùng'}
        </Button>
      </div>

      {error && (
        <div className="mb-4 p-3 bg-rose-50 border border-rose-200 text-rose-600 rounded-xl text-sm flex items-center gap-2">
          <Icon name="alertTriangle" className="w-4 h-4 shrink-0" /> {error}
        </div>
      )}
      {success && (
        <div className="mb-4 p-3 bg-emerald-50 border border-emerald-200 text-emerald-700 rounded-xl text-sm flex items-center gap-2">
          <Icon name="checkCircle" className="w-4 h-4 shrink-0" /> {success}
        </div>
      )}

      {/* Form thêm user */}
      {showForm && (
        <div className="mb-6 bg-white border border-ink-100 rounded-2xl p-5 shadow-card">
          <h4 className="font-bold text-ink-800 mb-4 text-sm">Tạo tài khoản mới</h4>
          <form onSubmit={handleCreateUser} className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-xs font-semibold text-ink-500 mb-1.5">Tên đăng nhập *</label>
              <input required value={form.username} onChange={(e) => setForm({ ...form, username: e.target.value })} className={inputClass} placeholder="vd: canbo_phong_kh" />
            </div>
            <div>
              <label className="block text-xs font-semibold text-ink-500 mb-1.5">Mật khẩu *</label>
              <input required type="password" value={form.password} onChange={(e) => setForm({ ...form, password: e.target.value })} className={inputClass} placeholder="Tối thiểu 6 ký tự" />
            </div>
            <div>
              <label className="block text-xs font-semibold text-ink-500 mb-1.5">Họ và tên</label>
              <input value={form.full_name} onChange={(e) => setForm({ ...form, full_name: e.target.value })} className={inputClass} placeholder="vd: Nguyễn Văn A" />
            </div>
            <div>
              <label className="block text-xs font-semibold text-ink-500 mb-1.5">Email</label>
              <input type="email" value={form.email} onChange={(e) => setForm({ ...form, email: e.target.value })} className={inputClass} placeholder="vd: email@cusc.vn" />
            </div>
            <div>
              <label className="block text-xs font-semibold text-ink-500 mb-1.5">Vai trò</label>
              <select value={form.role} onChange={(e) => setForm({ ...form, role: e.target.value })} className={inputClass}>
                <option value="user">Cán bộ (User)</option>
                <option value="admin">Quản trị (Admin)</option>
              </select>
            </div>
            <div className="flex items-end gap-3">
              {formError && <span className="text-xs text-rose-500 flex-1">{formError}</span>}
              <Button type="submit" disabled={formLoading} className="whitespace-nowrap">
                {formLoading ? 'Đang tạo...' : '+ Tạo tài khoản'}
              </Button>
            </div>
          </form>
        </div>
      )}

      {/* Bảng danh sách */}
      {loading ? (
        <div className="flex items-center justify-center gap-2 text-ink-400 py-16 text-sm">
          <Icon name="refresh" className="w-4 h-4 animate-spin" /> Đang tải danh sách người dùng...
        </div>
      ) : (
        <div className="bg-white rounded-2xl border border-ink-100 shadow-card overflow-hidden">
          <table className="w-full text-sm">
            <thead className="bg-ink-50 border-b border-ink-100">
              <tr>
                {['#', 'Tài khoản', 'Họ tên / Email', 'Vai trò', 'Trạng thái', 'Ngày tạo', 'Thao tác'].map((h) => (
                  <th key={h} className="text-left px-4 py-3 text-[11px] font-data uppercase tracking-wide text-ink-500 font-semibold">{h}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {users.map((u, idx) => (
                <tr key={u.id} className="border-b border-ink-50 hover:bg-ink-50/40 transition">
                  <td className="px-4 py-3 text-ink-300 text-xs font-data">{idx + 1}</td>
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-2.5">
                      <div className="w-8 h-8 rounded-full bg-gradient-to-br from-lake-400 to-lake-700 flex items-center justify-center text-white font-bold text-xs shrink-0">
                        {u.username?.[0]?.toUpperCase()}
                      </div>
                      <span className="font-semibold text-ink-800">{u.username}</span>
                    </div>
                  </td>
                  <td className="px-4 py-3 text-ink-500 text-xs">
                    <div>{u.full_name || <span className="italic text-ink-300">—</span>}</div>
                    <div className="text-ink-400 font-data">{u.email || ''}</div>
                  </td>
                  <td className="px-4 py-3">
                    <select
                      value={u.role}
                      onChange={(e) => handleRoleChange(u, e.target.value)}
                      className={`text-xs border rounded-full px-2.5 py-1.5 font-semibold font-data cursor-pointer ${
                        u.role === 'admin' ? 'bg-gold-100 text-gold-700 border-gold-300' : 'bg-lake-50 text-lake-700 border-lake-200'
                      }`}
                    >
                      <option value="user">Cán bộ</option>
                      <option value="admin">Admin</option>
                    </select>
                  </td>
                  <td className="px-4 py-3">
                    <Badge tone={u.is_active ? 'success' : 'danger'} dot>
                      {u.is_active ? 'Hoạt động' : 'Bị khoá'}
                    </Badge>
                  </td>
                  <td className="px-4 py-3 text-ink-400 text-xs font-data">
                    {u.created_at ? new Date(u.created_at).toLocaleDateString('vi-VN') : '—'}
                  </td>
                  <td className="px-4 py-3">
                    <div className="flex items-center gap-1.5">
                      <button
                        onClick={() => handleToggleActive(u)}
                        className={`flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-semibold rounded-lg transition border ${
                          u.is_active
                            ? 'bg-amber-50 hover:bg-amber-100 text-amber-700 border-amber-200'
                            : 'bg-emerald-50 hover:bg-emerald-100 text-emerald-700 border-emerald-200'
                        }`}
                      >
                        <Icon name={u.is_active ? 'lock' : 'unlock'} className="w-3.5 h-3.5" />
                        {u.is_active ? 'Khoá' : 'Mở'}
                      </button>
                      <button
                        onClick={() => handleDelete(u)}
                        className="flex items-center gap-1.5 px-2.5 py-1.5 text-xs font-semibold rounded-lg bg-rose-50 hover:bg-rose-100 text-rose-600 border border-rose-200 transition"
                      >
                        <Icon name="trash" className="w-3.5 h-3.5" />
                        Xóa
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
              {users.length === 0 && (
                <tr>
                  <td colSpan={7}>
                    <EmptyState icon="users" title="Chưa có người dùng nào" />
                  </td>
                </tr>
              )}
            </tbody>
          </table>
          <div className="px-4 py-2.5 text-xs text-ink-400 font-data border-t border-ink-100 bg-ink-50/40">
            Tổng {users.length} tài khoản
          </div>
        </div>
      )}
    </div>
  );
};

export default UserManagement;
