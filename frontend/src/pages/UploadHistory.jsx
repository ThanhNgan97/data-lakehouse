import React, { useEffect, useState } from 'react';
import axios from 'axios';
import Icon from '../components/icons';
import { Badge, EmptyState } from '../components/ui';

const STATUS_CONFIG = {
  pending: { label: "Đang chờ", tone: "ink" },
  uploaded: { label: "Đã upload", tone: "lake" },
  triggered: { label: "Pipeline đang chạy", tone: "warn" },
  trigger_failed: { label: "Chưa kích hoạt pipeline", tone: "warn" },
  upload_failed: { label: "Upload thất bại", tone: "danger" },
  running: { label: "Pipeline đang chạy", tone: "warn" },
  success: { label: "Hoàn thành", tone: "success" },
  failed: { label: "Pipeline lỗi", tone: "danger" },
  queued: { label: "Đang xếp hàng", tone: "lake" },
  unreachable: { label: "Airflow offline", tone: "ink" },
};

const UploadHistory = () => {
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [filterStatus, setFilterStatus] = useState("all");

  const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

  const fetchHistory = async () => {
    try {
      setLoading(true);
      const token = localStorage.getItem('token');
      const res = await axios.get(`${API_URL}/upload/history`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      setHistory(res.data);
      setError(null);
    } catch (err) {
      console.error(err);
      setError('Lỗi khi tải lịch sử upload. Vui lòng thử lại.');
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchHistory();
  }, []);

  const getStatus = (item) =>
    STATUS_CONFIG[item.pipeline_status] || {
      label: item.pipeline_status || item.status || 'Uploaded',
      tone: "ink",
    };

  if (loading) {
    return (
      <div className="p-10 flex items-center justify-center gap-2 text-ink-400 text-sm">
        <Icon name="refresh" className="w-4 h-4 animate-spin" /> Đang tải lịch sử...
      </div>
    );
  }
  if (error) {
    return (
      <div className="p-6 flex items-center gap-2 text-rose-600 text-sm">
        <Icon name="alertTriangle" className="w-4 h-4" /> {error}
      </div>
    );
  }

  const filteredHistory = history.filter((item) => {
    if (filterStatus === "all") return true;
    const st = item.pipeline_status;
    if (filterStatus === "success") return st === "success";
    if (filterStatus === "failed") return ["failed", "upload_failed", "trigger_failed", "unreachable"].includes(st);
    if (filterStatus === "running") return ["pending", "uploaded", "triggered", "running", "queued"].includes(st);
    return true;
  });

  return (
    <div className="p-6 bg-[#FAFBFD] h-full overflow-y-auto flex flex-col">
      <div className="flex justify-between items-start mb-6 shrink-0">
        <div>
          <p className="font-data text-[11px] uppercase tracking-widest text-lake-600 font-semibold mb-1">
            Audit trail
          </p>
          <h2 className="text-lg font-bold text-ink-900">Lịch sử Tải lên dữ liệu</h2>
          <div className="flex items-center gap-2 mt-3">
            {["all", "running", "success", "failed"].map((f) => (
              <button
                key={f}
                onClick={() => setFilterStatus(f)}
                className={`text-[10px] font-semibold uppercase tracking-wider px-2.5 py-1 rounded-md transition ${
                  filterStatus === f
                    ? "bg-lake-100 text-lake-700"
                    : "bg-ink-50 text-ink-400 hover:bg-ink-100"
                }`}
              >
                {f === "all"
                  ? "Tất cả"
                  : f === "running"
                  ? "Đang xử lý"
                  : f === "success"
                  ? "Thành công"
                  : "Lỗi"}
              </button>
            ))}
          </div>
        </div>
        <button
          onClick={fetchHistory}
          className="flex items-center gap-1.5 px-3.5 py-2 bg-lake-50 text-lake-700 rounded-lg hover:bg-lake-100 transition text-sm font-semibold"
        >
          <Icon name="refresh" className="w-3.5 h-3.5" />
          Làm mới
        </button>
      </div>

      {filteredHistory.length === 0 ? (
        <div className="bg-white rounded-2xl border border-ink-100">
          <EmptyState icon="inbox" title="Chưa có dữ liệu phù hợp" />
        </div>
      ) : (
        <div className="bg-white rounded-2xl border border-ink-100 overflow-hidden shadow-card">
          <div className="overflow-x-auto">
            <table className="w-full text-sm text-left">
              <thead className="bg-ink-50 text-ink-500 font-data text-[11px] uppercase tracking-wide">
                <tr>
                  <th className="px-4 py-3 font-semibold">ID người dùng</th>
                  <th className="px-4 py-3 font-semibold">Người upload</th>
                  <th className="px-4 py-3 font-semibold">Tên file</th>
                  <th className="px-4 py-3 font-semibold">Loại</th>
                  <th className="px-4 py-3 font-semibold">Kích thước</th>
                  <th className="px-4 py-3 font-semibold">Thời gian</th>
                  <th className="px-4 py-3 font-semibold">Trạng thái</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-ink-50">
                {filteredHistory.map((item) => {
                  const st = getStatus(item);
                  return (
                    <tr key={item.id} className="hover:bg-ink-50/50 transition">
                      <td className="px-4 py-3 font-data text-ink-500">{item.user_id}</td>
                      <td className="px-4 py-3 text-ink-700 font-medium">{item.full_name || item.username || 'N/A'}</td>
                      <td className="px-4 py-3 text-ink-600 truncate max-w-xs" title={item.filename}>
                        <div className="flex items-center gap-2">
                          <Icon name="file" className="w-3.5 h-3.5 text-ink-300 shrink-0" />
                          <span className="truncate">{item.filename}</span>
                        </div>
                      </td>
                      <td className="px-4 py-3 text-ink-500 font-data text-xs">{item.file_type?.split('/')[1] || item.file_type || 'N/A'}</td>
                      <td className="px-4 py-3 text-ink-500 font-data text-xs">{(item.file_size_bytes / 1024).toFixed(2)} KB</td>
                      <td className="px-4 py-3 text-ink-500 font-data text-xs">{new Date(item.uploaded_at).toLocaleString('vi-VN')}</td>
                      <td className="px-4 py-3">
                        <Badge tone={st.tone}>{st.label}</Badge>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
};

export default UploadHistory;
