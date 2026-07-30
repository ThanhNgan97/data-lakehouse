import React, { useEffect, useState } from 'react';
import axios from 'axios';
import { History, RefreshCw, Inbox } from 'lucide-react';

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

  if (loading) return <div className="p-5 text-gray-500">Đang tải lịch sử...</div>;
  if (error) return <div className="p-5 text-red-500">{error}</div>;

  return (
    <div className="p-6 h-full overflow-y-auto bg-slate-50/50">
      <div className="flex justify-between items-center mb-8">
        <h2 className="text-xl font-bold text-slate-800 flex items-center gap-2 tracking-tight">
          <History className="w-6 h-6 text-blue-600" /> Lịch sử Tải lên dữ liệu
        </h2>
        <button 
          onClick={fetchHistory}
          className="flex items-center gap-2 px-5 py-2.5 bg-blue-600 text-white rounded-xl hover:bg-blue-700 transition-all duration-300 text-sm font-semibold shadow-md shadow-blue-600/20"
        >
          <RefreshCw className={`w-4 h-4 ${loading ? 'animate-spin' : ''}`} /> Làm mới
        </button>
      </div>

      {history.length === 0 ? (
        <div className="flex flex-col items-center justify-center p-10 text-slate-500 border border-slate-100 rounded-2xl bg-white min-h-[300px] shadow-sm">
          <Inbox className="w-12 h-12 mb-3 text-slate-300" />
          <p className="font-medium">Chưa có dữ liệu tải lên nào.</p>
        </div>
      ) : (
        <div className="bg-white border border-slate-100 rounded-2xl shadow-xl shadow-slate-200/40 overflow-hidden">
          <table className="w-full text-sm text-left">
            <thead className="bg-slate-50 text-slate-500 uppercase tracking-wider text-xs border-b border-slate-100">
              <tr>
                <th className="px-6 py-4 font-bold">Id nguời dùng</th>
                <th className="px-6 py-4 font-bold">Tên người upload</th>
                <th className="px-6 py-4 font-bold">Tên File</th>
                <th className="px-6 py-4 font-bold">Loại</th>
                <th className="px-6 py-4 font-bold">Kích thước</th>
                <th className="px-6 py-4 font-bold">Thời gian</th>
                <th className="px-6 py-4 font-bold">Trạng thái</th>
              </tr>
            </thead>
            <tbody className="divide-y divide-slate-100">
              {history.map((item) => (
                <tr key={item.id} className="hover:bg-slate-50/80 transition-colors">
                  <td className="px-6 py-4 font-bold text-slate-400">{item.user_id}</td>
                  <td className="px-6 py-4 font-medium text-slate-700">{item.full_name || 'N/A'}</td>
                  <td className="px-6 py-4 font-semibold text-slate-800 truncate max-w-xs" title={item.filename}>{item.filename}</td>
                  <td className="px-6 py-4 font-medium text-slate-500">{item.file_type?.split('/')[1] || item.file_type || 'N/A'}</td>
                  <td className="px-6 py-4 font-medium text-slate-500">{(item.file_size_bytes / 1024).toFixed(2)} KB</td>
                  <td className="px-6 py-4 font-medium text-slate-400">{new Date(item.uploaded_at).toLocaleString('vi-VN')}</td>
                  <td className="px-6 py-4">
                    <span className="px-3 py-1.5 text-xs font-bold rounded-full bg-emerald-50 text-emerald-700 border border-emerald-200 shadow-sm shadow-emerald-500/10">
                      {item.status || 'Uploaded'}
                    </span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
};

export default UploadHistory;
