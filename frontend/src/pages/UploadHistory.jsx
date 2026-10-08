import React, { useEffect, useState } from 'react';
import axios from 'axios';
import Icon from '../components/icons';
import { Badge, Card, EmptyState } from '../components/ui';

const getStatusTone = (status) => {
  const normalized = String(status || '').toLowerCase();

  if (['success', 'completed', 'complete', 'uploaded', 'done'].includes(normalized)) {
    return 'success';
  }

  if (['running', 'in_progress', 'in-progress', 'processing', 'triggered', 'queued'].includes(normalized)) {
    return 'lake';
  }

  if (['warning', 'warn', 'trigger_failed'].includes(normalized)) {
    return 'warn';
  }

  if (['failed', 'error', 'upload_failed'].includes(normalized)) {
    return 'danger';
  }

  return 'ink';
};

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
      <div className="flex h-full items-center justify-center bg-canvas p-6">
        <div className="flex items-center gap-2.5 rounded-panel border border-line bg-white px-4 py-3 text-sm text-ink-500 shadow-card">
          <Icon name="refresh" className="h-4 w-4 animate-spin text-cobalt-600" />
          Đang tải lịch sử...
        </div>
      </div>
    );
  }
  if (error) {
    return (
      <div className="flex h-full items-center justify-center bg-canvas p-6">
        <div className="flex max-w-lg items-center gap-3 rounded-panel border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700 shadow-card">
          <Icon name="alertTriangle" className="h-4 w-4 shrink-0" />
          <span>{error}</span>
        </div>
      </div>
    );
  }

  return (
    <div className="h-full overflow-y-auto bg-canvas p-4 sm:p-5 lg:p-6">
      <div className="mx-auto w-full max-w-[1500px]">
        <div className="mb-5 flex flex-col gap-4 sm:flex-row sm:items-end sm:justify-between">
          <div className="min-w-0">
            <p className="mb-1.5 font-data text-[10px] font-semibold uppercase tracking-[0.14em] text-cobalt-700">
              Audit trail
            </p>
            <h2 className="font-display text-xl font-bold tracking-tight text-navy-950">
              Lịch sử Tải lên dữ liệu
            </h2>
          </div>

          <button
            onClick={fetchHistory}
            className="inline-flex h-9 shrink-0 items-center justify-center gap-1.5 self-start rounded-control border border-line bg-white px-3 text-xs font-semibold text-ink-600 shadow-sm transition hover:border-cobalt-200 hover:bg-cobalt-50 hover:text-cobalt-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-cobalt-500 focus-visible:ring-offset-2 sm:self-auto"
          >
            <Icon name="refresh" className="h-3.5 w-3.5" />
            Làm mới danh sách
          </button>
        </div>

        {history.length === 0 ? (
          <Card className="overflow-hidden">
            <EmptyState icon="inbox" title="Chưa có dữ liệu tải lên nào" />
          </Card>
        ) : (
          <Card className="overflow-hidden">
            <div className="overflow-x-auto">
              <table className="w-full min-w-[940px] table-fixed text-left text-sm">
                <thead className="border-b border-line bg-ink-50/80 font-data text-[10px] uppercase tracking-[0.08em] text-ink-500">
                  <tr>
                    <th className="w-[108px] px-4 py-3 font-semibold">ID người dùng</th>
                    <th className="w-[170px] px-4 py-3 font-semibold">Người upload</th>
                    <th className="px-4 py-3 font-semibold">Tên file</th>
                    <th className="w-[96px] px-4 py-3 font-semibold">Loại</th>
                    <th className="w-[112px] px-4 py-3 font-semibold">Kích thước</th>
                    <th className="w-[190px] px-4 py-3 font-semibold">Thời gian</th>
                    <th className="w-[138px] px-4 py-3 font-semibold">Trạng thái</th>
                  </tr>
                </thead>

                <tbody className="divide-y divide-ink-100 bg-white">
                  {history.map((item) => (
                    <tr key={item.id} className="transition-colors hover:bg-cobalt-50/35">
                      <td className="whitespace-nowrap px-4 py-4 font-data text-xs text-ink-500">
                        {item.user_id}
                      </td>

                      <td className="px-4 py-4 font-medium text-ink-700">
                        <div className="truncate" title={item.full_name || 'N/A'}>
                          {item.full_name || 'N/A'}
                        </div>
                      </td>

                      <td className="px-4 py-4 text-ink-700" title={item.filename}>
                        <div className="flex min-w-0 items-center gap-2.5">
                          <span className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg border border-cobalt-100 bg-cobalt-50 text-cobalt-600">
                            <Icon name="file" className="h-3.5 w-3.5" />
                          </span>
                          <span className="min-w-0 truncate font-medium">
                            {item.filename}
                          </span>
                        </div>
                      </td>

                      <td className="whitespace-nowrap px-4 py-4 font-data text-xs text-ink-500">
                        {item.file_type?.split('/')[1] || item.file_type || 'N/A'}
                      </td>

                      <td className="whitespace-nowrap px-4 py-4 font-data text-xs text-ink-500">
                        {(item.file_size_bytes / 1024).toFixed(2)} KB
                      </td>

                      <td className="whitespace-nowrap px-4 py-4 font-data text-xs text-ink-500">
                        {new Date(item.uploaded_at).toLocaleString('vi-VN')}
                      </td>

                      <td className="whitespace-nowrap px-4 py-4">
                        <Badge tone={getStatusTone(item.status)}>
                          {item.status || 'Uploaded'}
                        </Badge>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </Card>
        )}
      </div>
    </div>
  );
};

export default UploadHistory;
