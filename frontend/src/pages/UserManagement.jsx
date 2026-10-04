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
    'h-10 w-full rounded-control border border-line bg-white px-3 text-sm text-ink-800 outline-none transition placeholder:text-ink-300 hover:border-ink-200 focus:border-cobalt-400 focus:ring-2 focus:ring-cobalt-500/15';

  return (
    <div className="h-full overflow-y-auto bg-canvas p-4 sm:p-5 lg:p-6">
      <div className="mx-auto w-full max-w-[1480px]">
        {/* Header */}
        <div className="mb-6 flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
          <div className="min-w-0">
            <p className="mb-1.5 font-data text-[10px] font-bold uppercase tracking-[0.12em] text-cobalt-700">
              Access control
            </p>
            <h3 className="font-display text-2xl font-extrabold tracking-[-0.03em] text-navy-950">
              Quản lý Người dùng
            </h3>
            <p className="mt-1.5 max-w-2xl text-sm leading-6 text-ink-500">
              Thêm, xóa, phân quyền và khoá tài khoản trong hệ thống.
            </p>
          </div>

          <Button
            variant={showForm ? 'secondary' : 'primary'}
            onClick={() => setShowForm((v) => !v)}
            className="h-10 shrink-0 px-4"
          >
            <Icon name={showForm ? 'x' : 'plus'} className="h-4 w-4" />
            {showForm ? 'Đóng' : 'Thêm người dùng'}
          </Button>
        </div>

        {error && (
          <div className="mb-4 flex items-start gap-2.5 rounded-panel border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700 shadow-sm">
            <Icon name="alertTriangle" className="mt-0.5 h-4 w-4 shrink-0" />
            <span>{error}</span>
          </div>
        )}

        {success && (
          <div className="mb-4 flex items-start gap-2.5 rounded-panel border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700 shadow-sm">
            <Icon name="checkCircle" className="mt-0.5 h-4 w-4 shrink-0" />
            <span>{success}</span>
          </div>
        )}

        {/* Form thêm user */}
        {showForm && (
          <section className="mb-6 overflow-hidden rounded-panel border border-line bg-white shadow-card">
            <div className="flex items-center gap-3 border-b border-ink-100 px-4 py-4 sm:px-5">
              <span className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl border border-cobalt-100 bg-cobalt-50 text-cobalt-700">
                <Icon name="plus" className="h-4 w-4" />
              </span>
              <div>
                <h4 className="text-sm font-bold text-ink-900">Tạo tài khoản mới</h4>
                <p className="mt-0.5 text-xs text-ink-400">
                  Nhập thông tin tài khoản theo đúng vai trò hiện có trong hệ thống.
                </p>
              </div>
            </div>

            <form
              onSubmit={handleCreateUser}
              className="grid grid-cols-1 gap-x-4 gap-y-4 p-4 sm:p-5 md:grid-cols-2 xl:grid-cols-5"
            >
              <div className="xl:col-span-1">
                <label className="mb-1.5 block text-xs font-semibold text-ink-600">
                  Tên đăng nhập *
                </label>
                <input
                  required
                  value={form.username}
                  onChange={(e) => setForm({ ...form, username: e.target.value })}
                  className={inputClass}
                  placeholder="vd: canbo_phong_kh"
                />
              </div>

              <div className="xl:col-span-1">
                <label className="mb-1.5 block text-xs font-semibold text-ink-600">
                  Mật khẩu *
                </label>
                <input
                  required
                  type="password"
                  value={form.password}
                  onChange={(e) => setForm({ ...form, password: e.target.value })}
                  className={inputClass}
                  placeholder="Tối thiểu 6 ký tự"
                />
              </div>

              <div className="xl:col-span-1">
                <label className="mb-1.5 block text-xs font-semibold text-ink-600">
                  Họ và tên
                </label>
                <input
                  value={form.full_name}
                  onChange={(e) => setForm({ ...form, full_name: e.target.value })}
                  className={inputClass}
                  placeholder="vd: Nguyễn Văn A"
                />
              </div>

              <div className="xl:col-span-1">
                <label className="mb-1.5 block text-xs font-semibold text-ink-600">
                  Email
                </label>
                <input
                  type="email"
                  value={form.email}
                  onChange={(e) => setForm({ ...form, email: e.target.value })}
                  className={inputClass}
                  placeholder="vd: email@cusc.vn"
                />
              </div>

              <div className="xl:col-span-1">
                <label className="mb-1.5 block text-xs font-semibold text-ink-600">
                  Vai trò
                </label>
                <select
                  value={form.role}
                  onChange={(e) => setForm({ ...form, role: e.target.value })}
                  className={inputClass}
                >
                  <option value="user">Cán bộ (User)</option>
                  <option value="admin">Quản trị (Admin)</option>
                </select>
              </div>

              <div className="flex flex-col gap-3 border-t border-ink-100 pt-4 md:col-span-2 sm:flex-row sm:items-center sm:justify-between xl:col-span-5">
                <div className="min-h-[20px]">
                  {formError && (
                    <span className="inline-flex items-center gap-1.5 text-xs font-medium text-rose-600">
                      <Icon name="alertTriangle" className="h-3.5 w-3.5" />
                      {formError}
                    </span>
                  )}
                </div>

                <Button type="submit" disabled={formLoading} className="h-10 whitespace-nowrap px-4">
                  {formLoading ? 'Đang tạo...' : '+ Tạo tài khoản'}
                </Button>
              </div>
            </form>
          </section>
        )}

        {/* Bảng danh sách */}
        {loading ? (
          <div className="flex min-h-[220px] items-center justify-center">
            <div className="flex items-center gap-2.5 rounded-panel border border-line bg-white px-4 py-3 text-sm text-ink-500 shadow-card">
              <Icon name="refresh" className="h-4 w-4 animate-spin text-cobalt-600" />
              Đang tải danh sách người dùng...
            </div>
          </div>
        ) : (
          <section className="overflow-hidden rounded-panel border border-line bg-white shadow-card">
            <div className="flex flex-col gap-2 border-b border-ink-100 px-4 py-4 sm:flex-row sm:items-center sm:justify-between sm:px-5">
              <div>
                <h4 className="text-sm font-bold text-ink-900">Danh sách tài khoản</h4>
                <p className="mt-0.5 text-xs text-ink-400">
                  Quản lý vai trò, trạng thái và thao tác cho từng tài khoản.
                </p>
              </div>
              <span className="font-data text-[11px] font-semibold text-ink-400">
                Tổng {users.length} tài khoản
              </span>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full min-w-[1080px] table-fixed text-sm">
                <colgroup>
                  <col className="w-[58px]" />
                  <col className="w-[190px]" />
                  <col className="w-[250px]" />
                  <col className="w-[150px]" />
                  <col className="w-[135px]" />
                  <col className="w-[130px]" />
                  <col className="w-[205px]" />
                </colgroup>

                <thead className="border-b border-line bg-ink-50/80">
                  <tr>
                    {['#', 'Tài khoản', 'Họ tên / Email', 'Vai trò', 'Trạng thái', 'Ngày tạo', 'Thao tác'].map((h) => (
                      <th
                        key={h}
                        className="whitespace-nowrap px-4 py-3 text-left font-data text-[10px] font-bold uppercase tracking-[0.08em] text-ink-500"
                      >
                        {h}
                      </th>
                    ))}
                  </tr>
                </thead>

                <tbody className="divide-y divide-ink-100">
                  {users.map((u, idx) => (
                    <tr key={u.id} className="bg-white transition-colors hover:bg-cobalt-50/30">
                      <td className="px-4 py-4 font-data text-xs text-ink-300">
                        {idx + 1}
                      </td>

                      <td className="px-4 py-4">
                        <div className="flex min-w-0 items-center gap-3">
                          <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-cobalt-700 text-xs font-bold text-white shadow-sm">
                            {u.username?.[0]?.toUpperCase()}
                          </div>
                          <span className="truncate font-semibold text-ink-900">
                            {u.username}
                          </span>
                        </div>
                      </td>

                      <td className="px-4 py-4">
                        <div className="min-w-0">
                          <div className="truncate text-xs font-medium text-ink-700">
                            {u.full_name || <span className="italic text-ink-300">—</span>}
                          </div>
                          <div className="mt-1 truncate font-data text-[11px] text-ink-400">
                            {u.email || ''}
                          </div>
                        </div>
                      </td>

                      <td className="px-4 py-4">
                        <select
                          value={u.role}
                          onChange={(e) => handleRoleChange(u, e.target.value)}
                          className={`h-9 w-full rounded-control border px-3 font-data text-xs font-semibold outline-none transition focus:ring-2 focus:ring-cobalt-500/15 ${
                            u.role === 'admin'
                              ? 'border-gold-300 bg-gold-100 text-gold-700'
                              : 'border-cobalt-100 bg-cobalt-50 text-cobalt-700'
                          }`}
                        >
                          <option value="user">Cán bộ</option>
                          <option value="admin">Admin</option>
                        </select>
                      </td>

                      <td className="px-4 py-4">
                        <Badge tone={u.is_active ? 'success' : 'danger'} dot>
                          {u.is_active ? 'Hoạt động' : 'Bị khoá'}
                        </Badge>
                      </td>

                      <td className="whitespace-nowrap px-4 py-4 font-data text-xs text-ink-500">
                        {u.created_at ? new Date(u.created_at).toLocaleDateString('vi-VN') : '—'}
                      </td>

                      <td className="px-4 py-4">
                        <div className="flex items-center gap-2 whitespace-nowrap">
                          <button
                            onClick={() => handleToggleActive(u)}
                            className={`inline-flex h-9 items-center gap-1.5 rounded-control border px-3 text-xs font-semibold transition ${
                              u.is_active
                                ? 'border-amber-200 bg-amber-50 text-amber-700 hover:bg-amber-100'
                                : 'border-emerald-200 bg-emerald-50 text-emerald-700 hover:bg-emerald-100'
                            }`}
                          >
                            <Icon name={u.is_active ? 'lock' : 'unlock'} className="h-3.5 w-3.5" />
                            {u.is_active ? 'Khoá' : 'Mở'}
                          </button>

                          <button
                            onClick={() => handleDelete(u)}
                            className="inline-flex h-9 items-center gap-1.5 rounded-control border border-rose-200 bg-white px-3 text-xs font-semibold text-rose-600 transition hover:bg-rose-50"
                          >
                            <Icon name="trash" className="h-3.5 w-3.5" />
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
            </div>
          </section>
        )}
      </div>
    </div>
  );
};

export default UserManagement;
