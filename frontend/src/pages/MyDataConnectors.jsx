import React, { useEffect, useMemo, useRef, useState } from 'react';
import axios from 'axios';
import { useNavigate } from 'react-router-dom';
import Icon from '../components/icons';
import { Card } from '../components/ui';
import { SUPERSET_DASHBOARD_URL } from '../config/appConfig';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

const supersetUrl = SUPERSET_DASHBOARD_URL;

const MYSQL_PIPELINE_TASKS = [
  {
    id: 'ingest_mysql',
    label: 'Ingest',
    sub: 'Bronze',
    dot: 'bg-bronze-500',
  },
  {
    id: 'bronze_to_silver',
    label: 'Transform',
    sub: 'Silver',
    dot: 'bg-silver-500',
  },
  {
    id: 'silver_to_gold',
    label: 'Load',
    sub: 'Gold',
    dot: 'bg-gold-500',
  },
  {
    id: 'predictive_analysis',
    label: 'Analyze',
    sub: 'Predict',
    dot: 'bg-lake-500',
  },
];

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

const MyDataConnectors = () => {
  const navigate = useNavigate();
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
  const [activePipeline, setActivePipeline] = useState(null);
  const [activeConnector, setActiveConnector] = useState(null);
  const [dashboardUrl, setDashboardUrl] = useState(supersetUrl);
  const pollingRef = useRef(null);
  const lastAutoFilteredRunRef = useRef('');

  const handleLogout = () => {
    localStorage.removeItem('token');
    localStorage.removeItem('role');
    navigate('/login');
  };

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
      const res = await axios.get(`${API_URL}/my-connectors`, {
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

    return () => {
      if (pollingRef.current) {
        clearInterval(pollingRef.current);
      }
    };
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
          `${API_URL}/my-connectors/${editingConnector.id}`,
          payload,
          { headers: authHeader() },
        );
        notify(`Đã cập nhật connector "${payload.name}".`);
      } else {
        await axios.post(`${API_URL}/my-connectors`, payload, {
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
        `${API_URL}/my-connectors/${connector.id}/test`,
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

  const buildDashboardUrl = (nativeFiltersKey = '') => {
    if (!supersetUrl) return '';

    try {
      const url = new URL(supersetUrl);
      if (nativeFiltersKey) {
        url.searchParams.set('native_filters_key', nativeFiltersKey);
      } else {
        url.searchParams.delete('native_filters_key');
      }
      return url.toString();
    } catch {
      return supersetUrl;
    }
  };

  const resetDashboardToGold = () => {
    lastAutoFilteredRunRef.current = '';
    setDashboardUrl(buildDashboardUrl());
    notify('Dashboard đã trở về Gold chung.');
  };

  const applyDashboardFilterForConnector = async (connector, dagRunId) => {
    if (!connector?.id || !dagRunId) return;

    // Một DAG run success chỉ sinh filter state một lần.
    if (lastAutoFilteredRunRef.current === dagRunId) return;

    try {
      const res = await axios.post(
        `${API_URL}/my-connectors/${connector.id}/dashboard-filter`,
        {},
        { headers: authHeader() },
      );

      const nativeFiltersKey = res.data?.native_filters_key;
      if (!nativeFiltersKey) {
        throw new Error('missing_native_filters_key');
      }

      lastAutoFilteredRunRef.current = dagRunId;
      setDashboardUrl(buildDashboardUrl(nativeFiltersKey));

      notify(
        `Đồng bộ hoàn thành. Dashboard đã tự lọc theo "${connector.name}".`,
      );
    } catch (error) {
      // Auto-filter là tiện ích UI: pipeline success vẫn được giữ nguyên.
      notify(
        error.response?.data?.detail ||
          'Đồng bộ đã hoàn thành nhưng chưa thể tự áp dụng filter Superset.',
        true,
      );
    }
  };

  const pollSyncStatus = async (connector, dagRunId) => {
    if (!connector?.id || !dagRunId) return;

    if (pollingRef.current) {
      clearInterval(pollingRef.current);
      pollingRef.current = null;
    }

    setActiveConnector(connector);
    setActivePipeline({
      dag_run_id: dagRunId,
      state: 'queued',
      tasks: [],
    });

    const refreshStatus = async () => {
      try {
        const res = await axios.get(
          `${API_URL}/my-connectors/${connector.id}/runs/${encodeURIComponent(dagRunId)}`,
          { headers: authHeader() },
        );

        const data = res.data || {};
        setActivePipeline(data);

        if (['success', 'failed'].includes(data.state)) {
          if (pollingRef.current) {
            clearInterval(pollingRef.current);
            pollingRef.current = null;
          }

          if (data.state === 'success') {
            await applyDashboardFilterForConnector(connector, dagRunId);
          }

          return true;
        }

        return false;
      } catch (error) {
        if (pollingRef.current) {
          clearInterval(pollingRef.current);
          pollingRef.current = null;
        }

        setActivePipeline((current) => ({
          ...(current || {}),
          dag_run_id: dagRunId,
          state: 'unreachable',
          error_message:
            error.response?.data?.detail ||
            'Không thể lấy trạng thái đồng bộ từ Airflow.',
          tasks: current?.tasks || [],
        }));
        return true;
      }
    };

    const finished = await refreshStatus();
    if (!finished) {
      pollingRef.current = setInterval(refreshStatus, 5000);
    }
  };

  const handleSync = async (connector) => {
    const key = `sync-${connector.id}`;
    setOperationKey(key);

    try {
      const res = await axios.post(
        `${API_URL}/my-connectors/${connector.id}/sync`,
        {},
        { headers: authHeader() },
      );

      const dagRunId = res.data?.dag_run_id;
      notify(
        dagRunId
          ? `Đã kích hoạt Sync Now. DAG run: ${dagRunId}`
          : `Đã kích hoạt Sync Now cho "${connector.name}".`,
      );

      if (dagRunId) {
        await pollSyncStatus(connector, dagRunId);
      }

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
        `${API_URL}/my-connectors/${connector.id}`,
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
      await axios.delete(`${API_URL}/my-connectors/${connector.id}`, {
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
    <div className="min-h-screen bg-slate-50 font-sans flex flex-col text-ink-900">
      {/* Header đồng bộ với khu vực người dùng */}
      <header className="bg-white border-b border-slate-200 px-6 md:px-8 py-3.5 flex justify-between items-center sticky top-0 z-50 shadow-sm">
        <div className="flex items-center gap-3">
          <img
            src="/CUSC Logo Series.png"
            alt="CUSC Logo"
            className="h-10 w-auto object-contain"
          />
          <div>
            <h1 className="text-base md:text-lg font-bold text-slate-900 leading-tight tracking-tight">
              CUSC ANALYSIS PLATFORM
            </h1>
            <p className="text-[11px] md:text-xs text-slate-500 font-medium">
              Trung tâm Công nghệ Thông tin - Đại học Cần Thơ
            </p>
          </div>
        </div>

        <div className="flex items-center gap-2">
          <button
            type="button"
            onClick={() => navigate('/user')}
            className="flex items-center gap-2 px-3.5 py-2 bg-lake-50 hover:bg-lake-100 text-lake-700 border border-lake-200 rounded-lg text-sm font-semibold transition"
          >
            <Icon name="layers" className="w-4 h-4" />
            <span className="hidden sm:inline">Khu vực người dùng</span>
            <span className="sm:hidden">Quay lại</span>
          </button>

          <button
            type="button"
            onClick={handleLogout}
            className="flex items-center gap-2 px-4 py-2 bg-ink-900 hover:bg-rose-600 text-white rounded-lg text-sm font-semibold transition"
          >
            <Icon name="logOut" className="w-4 h-4" />
            <span className="hidden sm:inline">Đăng xuất</span>
          </button>
        </div>
      </header>

      <main className="flex-1 w-full max-w-[1400px] mx-auto p-4 md:p-6 lg:p-8 space-y-6">
        {/* Tiêu đề trang */}
        <section className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
          <div className="flex items-start gap-3">
            <div className="w-11 h-11 rounded-xl bg-lake-50 border border-lake-100 text-lake-600 flex items-center justify-center shrink-0">
              <Icon name="layers" className="w-5 h-5" />
            </div>

            <div>
              <p className="font-data text-[11px] uppercase tracking-widest text-lake-600 font-semibold mb-1">
                Data Connectors
              </p>
              <h2 className="text-xl md:text-2xl font-bold text-ink-900 tracking-tight">
                Nguồn dữ liệu MySQL của tôi
              </h2>
            </div>
          </div>

          <div className="flex flex-wrap gap-2">
            <button
              type="button"
              onClick={fetchConnectors}
              disabled={loading}
              className="flex items-center gap-2 px-3.5 py-2 rounded-lg border border-lake-200 bg-lake-50 text-sm font-semibold text-lake-700 hover:bg-lake-100 disabled:opacity-50 transition"
            >
              <Icon
                name="refresh"
                className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`}
              />
              {loading ? 'Đang tải...' : 'Làm mới'}
            </button>

            <button
              type="button"
              onClick={openCreateForm}
              className="flex items-center gap-2 px-4 py-2 rounded-lg bg-ink-900 text-sm font-semibold text-white hover:bg-lake-700 transition"
            >
              <span className="text-base leading-none">+</span>
              Thêm MySQL Connector
            </button>
          </div>
        </section>

        {/* Thống kê */}
        <section className="grid grid-cols-1 sm:grid-cols-3 gap-4">
          <Card className="p-5">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-xs text-ink-400 font-medium">
                  Tổng connector
                </p>
                <p className="mt-1 text-2xl font-bold text-ink-900">
                  {connectors.length}
                </p>
              </div>
              <div className="w-10 h-10 rounded-xl bg-lake-50 text-lake-600 border border-lake-100 flex items-center justify-center">
                <Icon name="layers" className="w-4.5 h-4.5" />
              </div>
            </div>
          </Card>

          <Card className="p-5">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-xs text-ink-400 font-medium">
                  Đang hoạt động
                </p>
                <p className="mt-1 text-2xl font-bold text-ink-900">
                  {activeCount}
                </p>
              </div>
              <div className="w-10 h-10 rounded-xl bg-emerald-50 text-emerald-600 border border-emerald-100 flex items-center justify-center">
                <Icon name="activity" className="w-4.5 h-4.5" />
              </div>
            </div>
          </Card>

          <Card className="p-5">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-xs text-ink-400 font-medium">
                  Loại nguồn
                </p>
                <p className="mt-1 text-2xl font-bold text-ink-900">
                  MySQL
                </p>
              </div>
              <div className="w-10 h-10 rounded-xl bg-amber-50 text-amber-600 border border-amber-100 flex items-center justify-center">
                <Icon name="grid" className="w-4.5 h-4.5" />
              </div>
            </div>
          </Card>
        </section>

        {notice && (
          <div
            className={`rounded-xl border px-4 py-3 text-sm font-medium flex items-start gap-2.5 ${
              notice.isError
                ? 'bg-rose-50 text-rose-700 border-rose-200'
                : 'bg-emerald-50 text-emerald-700 border-emerald-200'
            }`}
          >
            <Icon
              name={notice.isError ? 'alertTriangle' : 'checkCircle'}
              className="w-4 h-4 mt-0.5 shrink-0"
            />
            <span>{notice.message}</span>
          </div>
        )}

        {pageError && (
          <div className="rounded-xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700 flex items-start gap-2.5">
            <Icon name="alertTriangle" className="w-4 h-4 mt-0.5 shrink-0" />
            <span>{pageError}</span>
          </div>
        )}

        {/* Form tạo / sửa */}
        {showForm && (
          <Card className="overflow-hidden">
            <form onSubmit={handleSave}>
              <div className="px-5 md:px-6 py-4 border-b border-ink-100 bg-white flex items-center justify-between">
                <div>
                  <p className="font-data text-[10px] uppercase tracking-widest text-lake-600 font-semibold mb-1">
                    MySQL Connector
                  </p>
                  <h3 className="font-bold text-ink-900">
                    {editingConnector
                      ? `Sửa Connector #${editingConnector.id}`
                      : 'Thêm MySQL Connector'}
                  </h3>
                </div>

                <button
                  type="button"
                  onClick={closeForm}
                  className="text-sm font-semibold text-ink-400 hover:text-ink-800 transition"
                >
                  Đóng
                </button>
              </div>

              <div className="p-5 md:p-6 grid grid-cols-1 md:grid-cols-2 gap-4">
                <label className="space-y-1.5">
                  <span className="text-xs font-semibold text-ink-700">
                    Tên Connector
                  </span>
                  <input
                    name="name"
                    value={form.name}
                    onChange={handleFormChange}
                    className="w-full rounded-lg border border-ink-100 px-3 py-2.5 text-sm outline-none bg-white focus:ring-2 focus:ring-lake-100 focus:border-lake-400 transition"
                    placeholder="Ví dụ: MySQL Phòng Đào tạo"
                  />
                </label>

                <label className="space-y-1.5">
                  <span className="text-xs font-semibold text-ink-700">
                    Host
                  </span>
                  <input
                    name="host"
                    value={form.host}
                    onChange={handleFormChange}
                    className="w-full rounded-lg border border-ink-100 px-3 py-2.5 text-sm outline-none bg-white focus:ring-2 focus:ring-lake-100 focus:border-lake-400 transition"
                    placeholder="192.168.1.20 hoặc db.example.local"
                  />
                </label>

                <label className="space-y-1.5">
                  <span className="text-xs font-semibold text-ink-700">
                    Port
                  </span>
                  <input
                    name="port"
                    type="number"
                    min="1"
                    max="65535"
                    value={form.port}
                    onChange={handleFormChange}
                    className="w-full rounded-lg border border-ink-100 px-3 py-2.5 text-sm outline-none bg-white focus:ring-2 focus:ring-lake-100 focus:border-lake-400 transition"
                  />
                </label>

                <label className="space-y-1.5">
                  <span className="text-xs font-semibold text-ink-700">
                    Database
                  </span>
                  <input
                    name="database_name"
                    value={form.database_name}
                    onChange={handleFormChange}
                    className="w-full rounded-lg border border-ink-100 px-3 py-2.5 text-sm outline-none bg-white focus:ring-2 focus:ring-lake-100 focus:border-lake-400 transition"
                    placeholder="cusc_kpi_operational"
                  />
                </label>

                <label className="space-y-1.5">
                  <span className="text-xs font-semibold text-ink-700">
                    Username
                  </span>
                  <input
                    name="username"
                    value={form.username}
                    onChange={handleFormChange}
                    autoComplete="off"
                    className="w-full rounded-lg border border-ink-100 px-3 py-2.5 text-sm outline-none bg-white focus:ring-2 focus:ring-lake-100 focus:border-lake-400 transition"
                    placeholder="kpi_user"
                  />
                </label>

                <label className="space-y-1.5">
                  <span className="text-xs font-semibold text-ink-700">
                    Password
                  </span>
                  <input
                    name="password"
                    type="password"
                    value={form.password}
                    onChange={handleFormChange}
                    autoComplete="new-password"
                    className="w-full rounded-lg border border-ink-100 px-3 py-2.5 text-sm outline-none bg-white focus:ring-2 focus:ring-lake-100 focus:border-lake-400 transition"
                    placeholder={
                      editingConnector
                        ? 'Để trống để giữ mật khẩu hiện tại'
                        : 'Nhập mật khẩu MySQL'
                    }
                  />
                </label>

                <label className="md:col-span-2 flex items-center gap-3 rounded-xl border border-ink-100 bg-[#FAFBFD] px-4 py-3">
                  <input
                    name="is_active"
                    type="checkbox"
                    checked={form.is_active}
                    onChange={handleFormChange}
                    className="h-4 w-4 accent-teal-600"
                  />
                  <span className="text-sm font-semibold text-ink-800">
                    Connector đang hoạt động
                  </span>
                </label>
              </div>

              {formError && (
                <div className="mx-5 md:mx-6 mb-4 rounded-lg border border-rose-200 bg-rose-50 px-3 py-2 text-sm text-rose-700 flex items-start gap-2">
                  <Icon name="alertTriangle" className="w-4 h-4 mt-0.5 shrink-0" />
                  <span>{formError}</span>
                </div>
              )}

              <div className="px-5 md:px-6 py-4 bg-[#FAFBFD] border-t border-ink-100 flex justify-end gap-2">
                <button
                  type="button"
                  onClick={closeForm}
                  disabled={saving}
                  className="px-4 py-2 rounded-lg border border-ink-100 bg-white text-sm font-semibold text-ink-700 hover:bg-ink-50 disabled:opacity-50 transition"
                >
                  Hủy
                </button>
                <button
                  type="submit"
                  disabled={saving}
                  className="px-4 py-2 rounded-lg bg-ink-900 text-sm font-semibold text-white hover:bg-lake-700 disabled:opacity-50 transition"
                >
                  {saving
                    ? 'Đang lưu...'
                    : editingConnector
                      ? 'Lưu thay đổi'
                      : 'Tạo Connector'}
                </button>
              </div>
            </form>
          </Card>
        )}

        {/* Danh sách connector */}
        <Card className="overflow-hidden">
          <div className="px-5 md:px-6 py-4 border-b border-ink-100 bg-white flex items-center gap-3">
            <div className="w-9 h-9 rounded-lg bg-lake-50 text-lake-600 border border-lake-100 flex items-center justify-center">
              <Icon name="layers" className="w-4 h-4" />
            </div>
            <div>
              <h3 className="font-bold text-ink-900 text-base">
                MySQL Data Connectors
              </h3>
              <p className="text-[11px] text-ink-400 mt-0.5 font-data">
                MySQL → Bronze → Silver → Gold
              </p>
            </div>
          </div>

          {loading ? (
            <div className="p-12 text-center text-sm text-ink-400">
              <Icon name="refresh" className="w-6 h-6 mx-auto mb-3 animate-spin text-lake-500" />
              Đang tải danh sách connector...
            </div>
          ) : connectors.length === 0 ? (
            <div className="p-12 text-center">
              <div className="w-12 h-12 mx-auto mb-3 rounded-xl bg-lake-50 text-lake-500 border border-lake-100 flex items-center justify-center">
                <Icon name="inbox" className="w-5 h-5" />
              </div>
              <p className="font-semibold text-ink-700">
                Chưa có Data Connector
              </p>
              <p className="mt-1 text-sm text-ink-400">
                Thêm một MySQL Connector để bắt đầu.
              </p>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full min-w-[1080px] text-sm">
                <thead className="bg-[#FAFBFD] text-ink-400 border-b border-ink-100">
                  <tr>
                    <th className="text-left px-5 py-3 font-semibold">
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
                    <th className="text-right px-5 py-3 font-semibold">
                      Thao tác
                    </th>
                  </tr>
                </thead>

                <tbody className="divide-y divide-ink-100 bg-white">
                  {connectors.map((connector, index) => {
                    const busy = operationKey.endsWith(`-${connector.id}`);
                    const testing = operationKey === `test-${connector.id}`;
                    const syncing = operationKey === `sync-${connector.id}`;

                    return (
                      <tr
                        key={connector.id}
                        className="align-top hover:bg-lake-50/30 transition-colors"
                      >
                        <td className="px-5 py-4">
                          <div className="font-bold text-ink-900">
                            {connector.name}
                          </div>
                          <div className="mt-1.5 flex flex-wrap gap-1.5">
                            <span className="inline-flex rounded-full border border-ink-100 bg-ink-50 px-2 py-0.5 text-[11px] font-semibold text-ink-500 font-data">
                              {String(index + 1).padStart(2, '0')}
                            </span>

                            {connector.has_password && (
                              <span className="inline-flex rounded-full border border-lake-100 bg-lake-50 px-2 py-0.5 text-[11px] font-semibold text-lake-700">
                                Credential đã lưu
                              </span>
                            )}
                          </div>
                        </td>

                        <td className="px-4 py-4 text-ink-500">
                          <div className="font-semibold text-ink-800 font-data">
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
                            className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-bold transition ${statusClass(
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
                          <div className="mt-1.5 text-[11px] text-ink-300 font-data">
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
                          <div className="mt-1.5 text-[11px] text-ink-300 font-data">
                            {formatDateTime(connector.last_sync_at)}
                          </div>
                        </td>

                        <td className="px-5 py-4">
                          <div className="flex justify-end flex-wrap gap-1.5">
                            <button
                              type="button"
                              disabled={busy}
                              onClick={() => handleTest(connector)}
                              className="inline-flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg border border-lake-200 bg-lake-50 text-xs font-semibold text-lake-700 hover:bg-lake-100 disabled:opacity-50 transition"
                            >
                              <Icon name="activity" className="w-3.5 h-3.5" />
                              {testing ? 'Đang test...' : 'Test'}
                            </button>

                            <button
                              type="button"
                              disabled={busy || !connector.is_active}
                              onClick={() => handleSync(connector)}
                              className="inline-flex items-center gap-1.5 px-2.5 py-1.5 rounded-lg border border-sky-200 bg-sky-50 text-xs font-semibold text-sky-700 hover:bg-sky-100 disabled:opacity-50 transition"
                            >
                              <Icon
                                name="refresh"
                                className={`w-3.5 h-3.5 ${syncing ? 'animate-spin' : ''}`}
                              />
                              {syncing ? 'Đang kích hoạt...' : 'Sync Now'}
                            </button>

                            <button
                              type="button"
                              disabled={busy}
                              onClick={() => openEditForm(connector)}
                              className="px-2.5 py-1.5 rounded-lg border border-ink-100 bg-white text-xs font-semibold text-ink-700 hover:bg-ink-50 disabled:opacity-50 transition"
                            >
                              Sửa
                            </button>

                            <button
                              type="button"
                              disabled={busy}
                              onClick={() => handleDelete(connector)}
                              title="Xóa connector"
                              className="px-2.5 py-1.5 rounded-lg border border-rose-200 bg-rose-50 text-xs font-semibold text-rose-700 hover:bg-rose-100 disabled:opacity-40 disabled:cursor-not-allowed transition"
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
        </Card>

        {/* Theo dõi Sync MySQL + Superset */}
        <section className="grid grid-cols-1 lg:grid-cols-12 gap-6">
          <Card className="lg:col-span-4 p-6">
            <div className="flex justify-between items-start mb-6">
              <div>
                <p className="font-data text-[11px] uppercase tracking-widest text-lake-600 font-semibold mb-1">
                  Đồng bộ dữ liệu
                </p>
                <h3 className="text-base font-bold text-ink-900">
                  Tiến trình đồng bộ
                </h3>
                <p className="text-xs text-ink-400 mt-0.5">
                  Airflow Orchestration
                </p>
              </div>

              {activePipeline?.dag_run_id && (
                <span
                  className="text-[10px] font-data px-2 py-1 bg-ink-50 text-ink-500 rounded border border-ink-100"
                  title={activePipeline.dag_run_id}
                >
                  ID: {activePipeline.dag_run_id.slice(0, 10)}...
                </span>
              )}
            </div>

            {activePipeline ? (
              <>
                <div className="mb-5 rounded-xl border border-lake-100 bg-lake-50/50 px-3.5 py-3">
                  <p className="text-[10px] uppercase tracking-wider text-lake-600 font-data font-semibold">
                    Connector đang theo dõi
                  </p>
                  <p className="mt-1 text-sm font-bold text-ink-800">
                    {activeConnector?.name || 'MySQL Connector'}
                  </p>
                  <p className="mt-1 text-[11px] font-data text-ink-400">
                    Trạng thái DAG: {activePipeline.state || 'queued'}
                  </p>
                </div>

                <div className="relative pl-5 border-l-2 border-ink-100 space-y-5 ml-2">
                  {MYSQL_PIPELINE_TASKS.map((pt) => {
                    const task = activePipeline.tasks?.find(
                      (item) => item.task_id === pt.id,
                    );
                    const state = task?.state || 'pending';

                    let dotClass = 'bg-ink-200 border-ink-100';
                    let stateText = 'Đang đợi';
                    let icon = 'clock';
                    let textColor = 'text-ink-400';
                    let cardBorder = 'border-ink-100';
                    let cardBg = '';

                    if (state === 'success') {
                      dotClass = `${pt.dot} border-white shadow-[0_0_0_3px_rgba(16,185,129,0.15)]`;
                      stateText = 'Thành công';
                      icon = 'checkCircle';
                      textColor = 'text-emerald-700';
                      cardBorder = 'border-emerald-100';
                    } else if (state === 'running') {
                      dotClass = `${pt.dot} border-white animate-pulse shadow-[0_0_0_3px_rgba(15,151,168,0.2)]`;
                      stateText = 'Đang xử lý...';
                      icon = 'activity';
                      textColor = 'text-lake-700';
                      cardBorder = 'border-lake-200';
                      cardBg = 'bg-lake-50/50';
                    } else if (state === 'failed' || state === 'upstream_failed') {
                      dotClass =
                        'bg-rose-500 border-white shadow-[0_0_0_3px_rgba(244,63,94,0.15)]';
                      stateText = 'Lỗi';
                      icon = 'alertTriangle';
                      textColor = 'text-rose-700';
                      cardBorder = 'border-rose-200';
                      cardBg = 'bg-rose-50/40';
                    } else if (state === 'queued' || state === 'scheduled') {
                      stateText = 'Đang xếp hàng';
                      textColor = 'text-lake-600';
                    }

                    return (
                      <div key={pt.id} className="relative">
                        <div
                          className={`absolute -left-[27px] top-2 w-3.5 h-3.5 rounded-full border-2 ${dotClass} z-10 transition-colors duration-300`}
                        />

                        <div
                          className={`rounded-xl p-3 border ${cardBorder} ${cardBg} transition-all duration-300`}
                        >
                          <div className="flex items-center justify-between">
                            <p
                              className={`font-semibold text-sm ${
                                state === 'pending'
                                  ? 'text-ink-400'
                                  : 'text-ink-800'
                              }`}
                            >
                              {pt.label}{' '}
                              <span className="text-ink-300 font-data text-[11px] font-normal">
                                · {pt.sub}
                              </span>
                            </p>
                            <Icon
                              name={icon}
                              className={`w-4 h-4 ${textColor}`}
                            />
                          </div>

                          <p
                            className={`text-[11px] font-data font-medium mt-1 ${textColor}`}
                          >
                            {stateText}
                          </p>
                        </div>
                      </div>
                    );
                  })}
                </div>

                {activePipeline.state === 'success' && (
                  <div className="mt-5 p-3 bg-emerald-50 border border-emerald-200 text-emerald-700 rounded-xl text-sm flex items-start gap-2.5">
                    <Icon
                      name="checkCircle"
                      className="w-4 h-4 mt-0.5 shrink-0"
                    />
                    <span className="font-medium">
                      Đồng bộ MySQL → Bronze → Silver → Gold đã hoàn thành.
                    </span>
                  </div>
                )}

                {activePipeline.error_message && (
                  <div className="mt-5 p-3 bg-rose-50 border border-rose-200 text-rose-700 rounded-xl text-sm flex items-start gap-2.5">
                    <Icon
                      name="alertTriangle"
                      className="w-4 h-4 mt-0.5 shrink-0"
                    />
                    <span className="font-medium">
                      {activePipeline.error_message}
                    </span>
                  </div>
                )}
              </>
            ) : (
              <div className="min-h-[310px] flex flex-col items-center justify-center text-center">
                <div className="w-12 h-12 rounded-xl bg-lake-50 text-lake-500 border border-lake-100 flex items-center justify-center mb-3">
                  <Icon name="activity" className="w-5 h-5" />
                </div>
                <p className="font-semibold text-ink-700">
                  Chưa có tiến trình đồng bộ
                </p>
                <p className="mt-1 text-sm text-ink-400 max-w-[260px]">
                  Bấm Sync Now trên một connector để theo dõi tiến trình tại đây.
                </p>
              </div>
            )}
          </Card>

          <Card className="lg:col-span-8 overflow-hidden flex flex-col min-h-[520px]">
            <div className="px-5 md:px-6 py-4 border-b border-ink-100 bg-white flex justify-between items-center shrink-0">
              <div>
                <h3 className="text-base font-bold text-ink-900">
                  Báo cáo Phân tích
                </h3>
                <p className="text-[11px] text-ink-400 mt-0.5 font-data">
                  Gold Layer · Apache Superset
                </p>
              </div>

              <div className="flex items-center gap-2">
                <button
                  type="button"
                  onClick={resetDashboardToGold}
                  className="text-xs text-ink-600 hover:text-ink-900 bg-white hover:bg-ink-50 border border-ink-100 px-3 py-1.5 rounded-lg transition font-semibold"
                >
                  Xem tất cả
                </button>

                <a
                  href={dashboardUrl || supersetUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="text-xs text-lake-700 hover:text-lake-800 bg-lake-50 hover:bg-lake-100 px-3 py-1.5 rounded-lg transition font-semibold flex items-center gap-1.5"
                >
                  Mở tab mới
                  <Icon name="externalLink" className="w-3.5 h-3.5" />
                </a>
              </div>
            </div>

            <div className="flex-1 bg-[#FAFBFD] relative p-3 min-h-[450px]">
              <iframe
                key={dashboardUrl || supersetUrl}
                src={dashboardUrl || supersetUrl}
                title="Superset Dashboard"
                className="w-full h-full min-h-[430px] border border-ink-100 bg-white rounded-xl shadow-inner"
              />
            </div>
          </Card>
        </section>

      </main>
    </div>
  );
};

export default MyDataConnectors;
