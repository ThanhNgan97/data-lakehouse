import React, { useEffect, useState } from 'react';
import axios from 'axios';

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
    <div className="p-6 bg-white h-full overflow-y-auto">
      <div className="flex justify-between items-center mb-6">
        <h2 className="text-xl font-bold text-gray-800">Lịch sử Tải lên dữ liệu</h2>
        <button 
          onClick={fetchHistory}
          className="px-4 py-2 bg-blue-50 text-blue-600 rounded hover:bg-blue-100 transition text-sm font-medium"
        >
          Làm mới
        </button>
      </div>

      {history.length === 0 ? (
        <div className="text-center p-10 text-gray-500 border rounded-lg bg-gray-50">
          Chưa có dữ liệu tải lên nào.
        </div>
      ) : (
        <div className="overflow-x-auto border rounded-lg">
          <table className="w-full text-sm text-left">
            <thead className="bg-gray-50 text-gray-700 uppercase">
              <tr>
                <th className="px-4 py-3 font-semibold">Id nguời dùng</th>
                <th className="px-4 py-3 font-semibold">Tên người upload</th>
                <th className="px-4 py-3 font-semibold">Tên File</th>
                <th className="px-4 py-3 font-semibold">Loại</th>
                <th className="px-4 py-3 font-semibold">Kích thước</th>
                <th className="px-4 py-3 font-semibold">Thời gian</th>
                <th className="px-4 py-3 font-semibold">Trạng thái</th>
              </tr>
            </thead>
            <tbody className="divide-y">
              {history.map((item) => (
                <tr key={item.id} className="hover:bg-gray-50">
                  <td className="px-4 py-3 font-medium text-gray-900">{item.user_id}</td>
                  <td className="px-4 py-3 text-gray-600">{item.full_name || 'N/A'}</td>
                  <td className="px-4 py-3 text-gray-600 truncate max-w-xs" title={item.filename}>{item.filename}</td>
                  <td className="px-4 py-3 text-gray-600">{item.file_type?.split('/')[1] || item.file_type || 'N/A'}</td>
                  <td className="px-4 py-3 text-gray-600">{(item.file_size_bytes / 1024).toFixed(2)} KB</td>
                  <td className="px-4 py-3 text-gray-600">{new Date(item.uploaded_at).toLocaleString('vi-VN')}</td>
                  <td className="px-4 py-3">
                    <span className="px-2 py-1 text-xs rounded-full bg-green-100 text-green-800">
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
