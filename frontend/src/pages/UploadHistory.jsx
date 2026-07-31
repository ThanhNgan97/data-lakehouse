import React, { useEffect, useState } from 'react';
import axios from 'axios';
import Icon from '../components/icons';
import { Badge, EmptyState } from '../components/ui';

const UploadHistory = () => {
  const [history, setHistory] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

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

  return (
    <div className="p-6 bg-[#FAFBFD] h-full overflow-y-auto">
      <div className="flex justify-between items-center mb-6">
        <div>
          <p className="font-data text-[11px] uppercase tracking-widest text-lake-600 font-semibold mb-1">
            Audit trail
          </p>
          <h2 className="text-lg font-bold text-ink-900">Lịch sử Tải lên dữ liệu</h2>
        </div>
        <button
          onClick={fetchHistory}
          className="flex items-center gap-1.5 px-3.5 py-2 bg-lake-50 text-lake-700 rounded-lg hover:bg-lake-100 transition text-sm font-semibold"
        >
          <Icon name="refresh" className="w-3.5 h-3.5" />
          Làm mới
        </button>
      </div>

      {history.length === 0 ? (
        <div className="bg-white rounded-2xl border border-ink-100">
          <EmptyState icon="inbox" title="Chưa có dữ liệu tải lên nào" />
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
                {history.map((item) => (
                  <tr key={item.id} className="hover:bg-ink-50/50 transition">
                    <td className="px-4 py-3 font-data text-ink-500">{item.user_id}</td>
                    <td className="px-4 py-3 text-ink-700 font-medium">{item.full_name || 'N/A'}</td>
                    <td className="px-4 py-3 text-ink-600 truncate max-w-xs flex items-center gap-2" title={item.filename}>
                      <Icon name="file" className="w-3.5 h-3.5 text-ink-300 shrink-0" />
                      {item.filename}
                    </td>
                    <td className="px-4 py-3 text-ink-500 font-data text-xs">{item.file_type?.split('/')[1] || item.file_type || 'N/A'}</td>
                    <td className="px-4 py-3 text-ink-500 font-data text-xs">{(item.file_size_bytes / 1024).toFixed(2)} KB</td>
                    <td className="px-4 py-3 text-ink-500 font-data text-xs">{new Date(item.uploaded_at).toLocaleString('vi-VN')}</td>
                    <td className="px-4 py-3">
                      <Badge tone="success">{item.status || 'Uploaded'}</Badge>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
};

export default UploadHistory;
