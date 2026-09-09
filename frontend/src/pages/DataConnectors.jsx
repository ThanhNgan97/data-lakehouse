import React, { useEffect, useMemo, useState } from 'react';
import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

const authHeader = () => ({
  Authorization: `Bearer ${localStorage.getItem('token')}`,
});

const EMPTY_FORM = {
  name: '',
  host: '',
  port: 3306,
  database_name: '',
  username: '',
  password: '',
  is_active: true,
};

const formatDateTime = (value) => {
  if (!value) return '—';

  try {
    return new Intl.DateTimeFormat('vi-VN', {
      dateStyle: 'short',
      timeStyle: 'medium',
    }).format(new Date(value));
  } catch {
    return value;
  }
};

const statusClass = (kind, value) => {
  const normalized = String(value || '').toLowerCase();

  if (kind === 'active') {
    return value
      ? 'bg-emerald-50 text-emerald-700 border-emerald-200'
      : 'bg-slate-100 text-slate-500 border-slate-200';
  }

  if (normalized === 'success') {
    return 'bg-emerald-50 text-emerald-700 border-emerald-200';
  }

  if (
    normalized === 'failed' ||
    normalized === 'trigger_failed' ||
    normalized === 'trigger_error' ||
    normalized === 'airflow_unreachable'
  ) {
    return 'bg-rose-50 text-rose-700 border-rose-200';
  }

  if (normalized === 'triggered') {
    return 'bg-blue-50 text-blue-700 border-blue-200';
  }

  return 'bg-amber-50 text-amber-700 border-amber-200';
};

const testStatusLabel = (value) => {
  const normalized = String(value || '').toLowerCase();

  if (normalized === 'success') return 'Kết nối tốt';
  if (normalized === 'failed') return 'Kết nối lỗi';
  return 'Chưa kiểm tra';
};

const syncStatusLabel = (value) => {
  const normalized = String(value || '').toLowerCase();

  if (normalized === 'triggered') return 'Đã kích hoạt';
  if (normalized === 'trigger_failed') return 'Airflow từ chối';
  if (normalized === 'airflow_unreachable') return 'Không tới được Airflow';
  if (normalized === 'trigger_error') return 'Lỗi kích hoạt';
  return 'Chưa đồng bộ';
};

const DataConnectors = () => {
  const [connectors, setConnectors] = useState([]);
  const [loading, setLoading] = useState(true);
  const [pageError, setPageError] = useState('');
  const [notice, setNotice] = useState(null);

  const [showForm, setShowForm] = useState(false);
  const [editingConnector, setEditingConnector] = useState(null);
  const [form, setForm] = useState(EMPTY_FORM);
  const [formError, setFormError] = useState('');
  const [saving, setSaving] = useState(false);

  const [operationKey, setOperationKey] = useState('');

  const activeCount = useMemo(
    () => connectors.filter((item) => item.is_active).length,
    [connectors],
  );

  const notify = (message, isError = false) => {
    setNotice({ message, isError });
    window.setTimeout(() => {
      setNotice(null);
    }, 4500);
  };

  const fetchConnectors = async () => {
    setLoading(true);
    setPageError('');

    try {
      const res = await axios.get(`${API_URL}/connectors`, {
        headers: authHeader(),
      });
      setConnectors(Array.isArray(res.data) ? res.data : []);
    } catch (error) {
      setPageError(
        error.response?.data?.detail ||
          'Không thể tải danh sách Data Connector.',
      );
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchConnectors();
  }, []);

  const openCreateForm = () => {
    setEditingConnector(null);
    setForm(EMPTY_FORM);
    setFormError('');
    setShowForm(true);
  };

  const openEditForm = (connector) => {
    setEditingConnector(connector);
    setForm({
      name: connector.name || '',
      host: connector.host || '',
      port: connector.port || 3306,
      database_name: connector.database_name || '',
      username: connector.username || '',
      password: '',
      is_active: Boolean(connector.is_active),
    });
    setFormError('');
    setShowForm(true);
  };

  const closeForm = () => {
    if (saving) return;
    setShowForm(false);
    setEditingConnector(null);
    setFormError('');
  };

  const handleFormChange = (event) => {
    const { name, value, type, checked } = event.target;

    setForm((current) => ({
      ...current,
      [name]: type === 'checkbox' ? checked : value,
    }));
  };

  const validateForm = () => {
    if (!form.name.trim()) return 'Tên connector không được để trống.';
    if (!form.host.trim()) return 'MySQL host không được để trống.';
    if (!form.database_name.trim()) return 'Tên database không được để trống.';
    if (!form.username.trim()) return 'Username không được để trống.';

    const port = Number(form.port);
    if (!Number.isInteger(port) || port < 1 || port > 65535) {
      return 'Port phải là số nguyên từ 1 đến 65535.';
    }

    if (!editingConnector && !form.password) {
      return 'Password là bắt buộc khi tạo connector mới.';
    }

    return '';
  };

  const handleSave = async (event) => {
    event.preventDefault();

    const validationError = validateForm();
    if (validationError) {
      setFormError(validationError);
      return;
    }

    setSaving(true);
    setFormError('');

    const payload = {
      name: form.name.trim(),
      host: form.host.trim(),
      port: Number(form.port),
      database_name: form.database_name.trim(),
      username: form.username.trim(),
      is_active: Boolean(form.is_active),
    };

    if (!editingConnector) {
      payload.connector_type = 'MYSQL';
    }

    if (!editingConnector || form.password) {
      payload.password = form.password;
    }

    try {
      if (editingConnector) {
        await axios.put(
          `${API_URL}/connectors/${editingConnector.id}`,
          payload,
          { headers: authHeader() },
        );
        notify(`Đã cập nhật connector "${payload.name}".`);
      } else {
        await axios.post(`${API_URL}/connectors`, payload, {
          headers: authHeader(),
        });
        notify(`Đã tạo connector "${payload.name}".`);
      }

      setShowForm(false);
      setEditingConnector(null);
      setForm(EMPTY_FORM);
      await fetchConnectors();
    } catch (error) {
      setFormError(
        error.response?.data?.detail ||
          'Không thể lưu Data Connector.',
      );
    } finally {
      setSaving(false);
    }
  };

  const handleTest = async (connector) => {
    const key = `test-${connector.id}`;
    setOperationKey(key);

    try {
      const res = await axios.post(
        `${API_URL}/connectors/${connector.id}/test`,
        {},
        { headers: authHeader() },
      );

      if (res.data?.success) {
        notify(`Test Connection "${connector.name}" thành công.`);
      } else {
        notify(
          res.data?.message ||
            `Test Connection "${connector.name}" thất bại.`,
          true,
        );
      }

      await fetchConnectors();
    } catch (error) {
      notify(
        error.response?.data?.detail ||
          `Không thể kiểm tra connector "${connector.name}".`,
        true,
      );
      await fetchConnectors();
    } finally {
      setOperationKey('');
    }
  };

  const handleSync = async (connector) => {
    const key = `sync-${connector.id}`;
    setOperationKey(key);

    try {
      const res = await axios.post(
        `${API_URL}/connectors/${connector.id}/sync`,
        {},
        { headers: authHeader() },
      );

      const dagRunId = res.data?.dag_run_id;
      notify(
        dagRunId
          ? `Đã kích hoạt Sync Now. DAG run: ${dagRunId}`
          : `Đã kích hoạt Sync Now cho "${connector.name}".`,
      );

      await fetchConnectors();
    } catch (error) {
      notify(
        error.response?.data?.detail ||
          `Không thể kích hoạt đồng bộ "${connector.name}".`,
        true,
      );
      await fetchConnectors();
    } finally {
      setOperationKey('');
    }
  };

  const handleToggleActive = async (connector) => {
    const key = `toggle-${connector.id}`;
    setOperationKey(key);

    try {
      await axios.put(
        `${API_URL}/connectors/${connector.id}`,
        { is_active: !connector.is_active },
        { headers: authHeader() },
      );

      notify(
        connector.is_active
          ? `Đã vô hiệu hóa "${connector.name}".`
          : `Đã kích hoạt "${connector.name}".`,
      );

      await fetchConnectors();
    } catch (error) {
      notify(
        error.response?.data?.detail ||
          'Không thể cập nhật trạng thái connector.',
        true,
      );
    } finally {
      setOperationKey('');
    }
  };

  const handleDelete = async (connector) => {
    if (connector.is_default) {
      notify('Default Connector được bảo vệ và không thể xóa.', true);
      return;
    }

    if (
      !window.confirm(
        `Xóa Data Connector "${connector.name}"?\n\nThao tác này không thể hoàn tác.`,
      )
    ) {
      return;
    }

    const key = `delete-${connector.id}`;
    setOperationKey(key);

    try {
      await axios.delete(`${API_URL}/connectors/${connector.id}`, {
        headers: authHeader(),
      });

      notify(`Đã xóa connector "${connector.name}".`);
      await fetchConnectors();
    } catch (error) {
      notify(
        error.response?.data?.detail ||
          `Không thể xóa connector "${connector.name}".`,
        true,
      );
    } finally {
      setOperationKey('');
    }
  };

  return (
    <div className="p-6 min-h-full bg-[#F8FAFC]">
      <div className="max-w-7xl mx-auto space-y-5">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
          <div>
            <p className="text-[11px] font-bold uppercase tracking-[0.18em] text-lake-700">
              Data Connectors
            </p>
            <h3 className="mt-1 text-xl font-bold text-slate-900">
              Quản lý Nguồn Dữ liệu
            </h3>
            <p className="mt-1 text-sm text-slate-500 max-w-3xl">
              Quản lý kết nối MySQL dùng cho pipeline Lakehouse. Credential
              được lưu mã hóa ở backend và không được hiển thị lại trên giao diện.
            </p>
          </div>

          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              onClick={fetchConnectors}
              disabled={loading}
              className="px-3.5 py-2 rounded-lg border border-slate-200 bg-white text-sm font-semibold text-slate-700 hover:bg-slate-50 disabled:opacity-50"
            >
              {loading ? 'Đang tải...' : 'Làm mới'}
            </button>

            <button
              type="button"
              onClick={openCreateForm}
              className="px-3.5 py-2 rounded-lg bg-slate-900 text-sm font-semibold text-white hover:bg-slate-800"
            >
              + Thêm MySQL Connector
            </button>
          </div>
        </div>

        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
          <div className="bg-white border border-slate-200 rounded-xl p-4">
            <p className="text-xs text-slate-500">Tổng connector</p>
            <p className="mt-1 text-2xl font-bold text-slate-900">
              {connectors.length}
            </p>
          </div>

          <div className="bg-white border border-slate-200 rounded-xl p-4">
            <p className="text-xs text-slate-500">Đang hoạt động</p>
            <p className="mt-1 text-2xl font-bold text-slate-900">
              {activeCount}
            </p>
          </div>

          <div className="bg-white border border-slate-200 rounded-xl p-4">
            <p className="text-xs text-slate-500">Loại nguồn</p>
            <p className="mt-1 text-2xl font-bold text-slate-900">
              MySQL
            </p>
          </div>
        </div>

        {notice && (
          <div
            className={`rounded-xl border px-4 py-3 text-sm font-medium ${
              notice.isError
                ? 'bg-rose-50 text-rose-700 border-rose-200'
                : 'bg-emerald-50 text-emerald-700 border-emerald-200'
            }`}
          >
            {notice.message}
          </div>
        )}

        {pageError && (
          <div className="rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
            {pageError}
          </div>
        )}

        {showForm && (
          <form
            onSubmit={handleSave}
            className="bg-white border border-slate-200 rounded-2xl shadow-sm overflow-hidden"
          >
            <div className="px-5 py-4 border-b border-slate-100 flex items-center justify-between">
              <div>
                <h4 className="font-bold text-slate-900">
                  {editingConnector
                    ? `Sửa Connector #${editingConnector.id}`
                    : 'Thêm MySQL Connector'}
                </h4>
                <p className="text-xs text-slate-500 mt-0.5">
                  {editingConnector
                    ? 'Để trống password nếu muốn giữ credential hiện tại.'
                    : 'Nhập thông tin kết nối tới MySQL nguồn.'}
                </p>
              </div>

              <button
                type="button"
                onClick={closeForm}
                className="text-sm text-slate-500 hover:text-slate-900"
              >
                Đóng
              </button>
            </div>

            <div className="p-5 grid grid-cols-1 md:grid-cols-2 gap-4">
              <label className="space-y-1.5">
                <span className="text-xs font-semibold text-slate-700">
                  Tên Connector
                </span>
                <input
                  name="name"
                  value={form.name}
                  onChange={handleFormChange}
                  className="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-lake-200 focus:border-lake-400"
                  placeholder="Ví dụ: MySQL Phòng Đào tạo"
                />
              </label>

              <label className="space-y-1.5">
                <span className="text-xs font-semibold text-slate-700">
                  Host
                </span>
                <input
                  name="host"
                  value={form.host}
                  onChange={handleFormChange}
                  className="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-lake-200 focus:border-lake-400"
                  placeholder="192.168.1.20 hoặc db.example.local"
                />
              </label>

              <label className="space-y-1.5">
                <span className="text-xs font-semibold text-slate-700">
                  Port
                </span>
                <input
                  name="port"
                  type="number"
                  min="1"
                  max="65535"
                  value={form.port}
                  onChange={handleFormChange}
                  className="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-lake-200 focus:border-lake-400"
                />
              </label>

              <label className="space-y-1.5">
                <span className="text-xs font-semibold text-slate-700">
                  Database
                </span>
                <input
                  name="database_name"
                  value={form.database_name}
                  onChange={handleFormChange}
                  className="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-lake-200 focus:border-lake-400"
                  placeholder="cusc_kpi_operational"
                />
              </label>

              <label className="space-y-1.5">
                <span className="text-xs font-semibold text-slate-700">
                  Username
                </span>
                <input
                  name="username"
                  value={form.username}
                  onChange={handleFormChange}
                  autoComplete="off"
                  className="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-lake-200 focus:border-lake-400"
                  placeholder="kpi_user"
                />
              </label>

              <label className="space-y-1.5">
                <span className="text-xs font-semibold text-slate-700">
                  Password
                </span>
                <input
                  name="password"
                  type="password"
                  value={form.password}
                  onChange={handleFormChange}
                  autoComplete="new-password"
                  className="w-full rounded-lg border border-slate-200 px-3 py-2 text-sm outline-none focus:ring-2 focus:ring-lake-200 focus:border-lake-400"
                  placeholder={
                    editingConnector
                      ? 'Để trống để giữ mật khẩu hiện tại'
                      : 'Nhập mật khẩu MySQL'
                  }
                />
              </label>

              <label className="md:col-span-2 flex items-center gap-2.5 rounded-lg border border-slate-200 px-3 py-2.5">
                <input
                  name="is_active"
                  type="checkbox"
                  checked={form.is_active}
                  onChange={handleFormChange}
                  className="h-4 w-4"
                />
                <span>
                  <span className="block text-sm font-semibold text-slate-800">
                    Connector đang hoạt động
                  </span>
                  <span className="block text-xs text-slate-500">
                    Connector bị vô hiệu hóa sẽ không được phép Sync Now.
                  </span>
                </span>
              </label>
            </div>

            {formError && (
              <div className="mx-5 mb-4 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700">
                {formError}
              </div>
            )}

            <div className="px-5 py-4 bg-slate-50 border-t border-slate-100 flex justify-end gap-2">
              <button
                type="button"
                onClick={closeForm}
                disabled={saving}
                className="px-4 py-2 rounded-lg border border-slate-200 bg-white text-sm font-semibold text-slate-700 hover:bg-slate-100 disabled:opacity-50"
              >
                Hủy
              </button>
              <button
                type="submit"
                disabled={saving}
                className="px-4 py-2 rounded-lg bg-slate-900 text-sm font-semibold text-white hover:bg-slate-800 disabled:opacity-50"
              >
                {saving
                  ? 'Đang lưu...'
                  : editingConnector
                    ? 'Lưu thay đổi'
                    : 'Tạo Connector'}
              </button>
            </div>
          </form>
        )}

        <div className="bg-white border border-slate-200 rounded-2xl shadow-sm overflow-hidden">
          <div className="px-5 py-4 border-b border-slate-100">
            <h4 className="font-bold text-slate-900">
              MySQL Data Connectors
            </h4>
            <p className="mt-0.5 text-xs text-slate-500">
              Test Connection kiểm tra MySQL trực tiếp. Sync Now kích hoạt
              Airflow để chạy MySQL → Bronze → Silver → Gold.
            </p>
          </div>

          {loading ? (
            <div className="p-10 text-center text-sm text-slate-500">
              Đang tải danh sách connector...
            </div>
          ) : connectors.length === 0 ? (
            <div className="p-10 text-center">
              <p className="font-semibold text-slate-700">
                Chưa có Data Connector
              </p>
              <p className="mt-1 text-sm text-slate-500">
                Thêm một MySQL Connector để bắt đầu.
              </p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full min-w-[1080px] text-sm">
                <thead className="bg-slate-50 text-slate-500">
                  <tr>
                    <th className="text-left px-4 py-3 font-semibold">
                      Connector
                    </th>
                    <th className="text-left px-4 py-3 font-semibold">
                      MySQL
                    </th>
                    <th className="text-left px-4 py-3 font-semibold">
                      Trạng thái
                    </th>
                    <th className="text-left px-4 py-3 font-semibold">
                      Test Connection
                    </th>
                    <th className="text-left px-4 py-3 font-semibold">
                      Sync
                    </th>
                    <th className="text-right px-4 py-3 font-semibold">
                      Thao tác
                    </th>
                  </tr>
                </thead>

                <tbody className="divide-y divide-slate-100">
                  {connectors.map((connector, index) => {
                    const busy = operationKey.endsWith(`-${connector.id}`);
                    const testing = operationKey === `test-${connector.id}`;
                    const syncing = operationKey === `sync-${connector.id}`;

                    return (
                      <tr
                        key={connector.id}
                        className="align-top hover:bg-slate-50/60"
                      >
                        <td className="px-4 py-4">
                          <div className="flex items-start gap-2">
                            <div>
                              <div className="font-bold text-slate-900">
                                {connector.name}
                              </div>
                              <div className="mt-1 flex flex-wrap gap-1.5">
                                <span className="inline-flex rounded-full border border-slate-200 bg-slate-100 px-2 py-0.5 text-[11px] font-semibold text-slate-600">
                                  {index + 1}
                                </span>

                                {connector.is_default && (
                                  <span className="inline-flex rounded-full border border-violet-200 bg-violet-50 px-2 py-0.5 text-[11px] font-semibold text-violet-700">
                                    Default
                                  </span>
                                )}

                                {connector.has_password && (
                                  <span className="inline-flex rounded-full border border-slate-200 bg-white px-2 py-0.5 text-[11px] font-semibold text-slate-500">
                                    Credential đã lưu
                                  </span>
                                )}
                              </div>
                            </div>
                          </div>
                        </td>

                        <td className="px-4 py-4 text-slate-600">
                          <div className="font-medium text-slate-800">
                            {connector.host}:{connector.port}
                          </div>
                          <div className="text-xs mt-1">
                            DB: {connector.database_name}
                          </div>
                          <div className="text-xs mt-0.5">
                            User: {connector.username}
                          </div>
                        </td>

                        <td className="px-4 py-4">
                          <button
                            type="button"
                            disabled={busy}
                            onClick={() => handleToggleActive(connector)}
                            className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-bold ${statusClass(
                              'active',
                              connector.is_active,
                            )} disabled:opacity-50`}
                          >
                            {connector.is_active
                              ? 'Đang hoạt động'
                              : 'Đã vô hiệu hóa'}
                          </button>
                        </td>

                        <td className="px-4 py-4">
                          <span
                            className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-bold ${statusClass(
                              'test',
                              connector.last_test_status,
                            )}`}
                          >
                            {testStatusLabel(connector.last_test_status)}
                          </span>
                          <div className="mt-1.5 text-[11px] text-slate-400">
                            {formatDateTime(connector.last_tested_at)}
                          </div>
                        </td>

                        <td className="px-4 py-4">
                          <span
                            className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-bold ${statusClass(
                              'sync',
                              connector.last_sync_status,
                            )}`}
                          >
                            {syncStatusLabel(connector.last_sync_status)}
                          </span>
                          <div className="mt-1.5 text-[11px] text-slate-400">
                            {formatDateTime(connector.last_sync_at)}
                          </div>
                        </td>

                        <td className="px-4 py-4">
                          <div className="flex justify-end flex-wrap gap-1.5">
                            <button
                              type="button"
                              disabled={busy}
                              onClick={() => handleTest(connector)}
                              className="px-2.5 py-1.5 rounded-lg border border-slate-200 bg-white text-xs font-semibold text-slate-700 hover:bg-slate-100 disabled:opacity-50"
                            >
                              {testing ? 'Đang test...' : 'Test'}
                            </button>

                            <button
                              type="button"
                              disabled={busy || !connector.is_active}
                              onClick={() => handleSync(connector)}
                              className="px-2.5 py-1.5 rounded-lg border border-blue-200 bg-blue-50 text-xs font-semibold text-blue-700 hover:bg-blue-100 disabled:opacity-50"
                            >
                              {syncing ? 'Đang kích hoạt...' : 'Sync Now'}
                            </button>

                            <button
                              type="button"
                              disabled={busy}
                              onClick={() => openEditForm(connector)}
                              className="px-2.5 py-1.5 rounded-lg border border-slate-200 bg-white text-xs font-semibold text-slate-700 hover:bg-slate-100 disabled:opacity-50"
                            >
                              Sửa
                            </button>

                            <button
                              type="button"
                              disabled={busy || connector.is_default}
                              onClick={() => handleDelete(connector)}
                              title={
                                connector.is_default
                                  ? 'Default Connector không thể xóa'
                                  : 'Xóa connector'
                              }
                              className="px-2.5 py-1.5 rounded-lg border border-rose-200 bg-rose-50 text-xs font-semibold text-rose-700 hover:bg-rose-100 disabled:opacity-40 disabled:cursor-not-allowed"
                            >
                              Xóa
                            </button>
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </div>

        <div className="rounded-xl border border-blue-100 bg-blue-50/70 p-4 text-xs text-blue-800">
          <span className="font-bold">Phạm vi Day 4:</span>{' '}
          giao diện quản lý connector dùng schema KPI MySQL hiện tại.
          Schema Mapping cho database có tên bảng/cột khác sẽ được bổ sung
          ở Day 5.
        </div>
      </div>
    </div>
  );
};

export default DataConnectors;
