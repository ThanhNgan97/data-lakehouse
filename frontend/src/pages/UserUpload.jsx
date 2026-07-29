import React, { useState, useRef, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

const authHeader = () => ({
  Authorization: `Bearer ${localStorage.getItem('token')}`,
});

const STATUS_CONFIG = {
  pending:        { label: '⏳ Đang chờ',       cls: 'bg-gray-100 text-gray-600' },
  uploaded:       { label: '📦 Đã upload',       cls: 'bg-blue-100 text-blue-600' },
  triggered:      { label: '🚀 Pipeline đang chạy', cls: 'bg-yellow-100 text-yellow-700' },
  trigger_failed: { label: '⚠️ Chưa kích hoạt pipeline', cls: 'bg-orange-100 text-orange-600' },
  upload_failed:  { label: '❌ Upload thất bại',  cls: 'bg-red-100 text-red-600' },
  // Trạng thái từ Airflow
  running:        { label: '⚙️ Pipeline đang chạy', cls: 'bg-yellow-100 text-yellow-700' },
  success:        { label: '✅ Hoàn thành',       cls: 'bg-green-100 text-green-700' },
  failed:         { label: '❌ Pipeline lỗi',     cls: 'bg-red-100 text-red-600' },
  queued:         { label: '🕐 Đang xếp hàng',   cls: 'bg-purple-100 text-purple-600' },
  unreachable:    { label: '🔌 Airflow offline',  cls: 'bg-gray-100 text-gray-500' },
};

const PIPELINE_TASKS = [
  { id: 'ingest_bronze', label: 'Extract (Bronze)' },
  { id: 'bronze_to_silver', label: 'Transform (Silver)' },
  { id: 'silver_to_gold', label: 'Load (Gold)' },
  { id: 'predictive_analysis', label: 'Analyze (Predict)' }
];

const UserUpload = () => {
  const navigate = useNavigate();
  const fileInputRef = useRef(null);
  const [isDragging, setIsDragging] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [uploadStatus, setUploadStatus] = useState('');
  const [uploadError, setUploadError] = useState('');
  const [history, setHistory] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [activePipeline, setActivePipeline] = useState(null);
  // polling dag run
  const pollingRef = useRef(null);

  const fetchHistory = useCallback(async () => {
    setHistoryLoading(true);
    try {
      const res = await axios.get(`${API_URL}/upload/history`, { headers: authHeader() });
      const hist = res.data || [];
      setHistory(hist);
      
      // Auto poll if the most recent upload is still running
      if (hist.length > 0 && ['running', 'queued', 'pending'].includes(hist[0].pipeline_status)) {
         if (!pollingRef.current && hist[0].dag_run_id) {
            pollPipelineStatus(hist[0].dag_run_id);
         }
      }
    } catch { /* silence */ } finally {
      setHistoryLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchHistory();
    return () => { if (pollingRef.current) clearInterval(pollingRef.current); };
  }, [fetchHistory]);

  const pollPipelineStatus = (dagRunId) => {
    if (!dagRunId) return;
    if (pollingRef.current) clearInterval(pollingRef.current);
    
    // Gọi ngay lập tức lần đầu
    (async () => {
      try {
        const res = await axios.get(`${API_URL}/upload/pipeline-status/${dagRunId}`, { headers: authHeader() });
        setActivePipeline(res.data);
      } catch {}
    })();

    pollingRef.current = setInterval(async () => {
      try {
        const res = await axios.get(`${API_URL}/upload/pipeline-status/${dagRunId}`, { headers: authHeader() });
        const state = res.data.state;
        setActivePipeline(res.data);
        if (['success', 'failed', 'unreachable'].includes(state)) {
          clearInterval(pollingRef.current);
          fetchHistory();
        }
      } catch { clearInterval(pollingRef.current); }
    }, 5000);
  };

  const handleLogout = () => {
    localStorage.removeItem('token');
    localStorage.removeItem('role');
    navigate('/login');
  };

  const processFile = async (file) => {
    if (!file) return;
    setUploadError('');
    setUploadStatus('');
    setUploading(true);
    setUploadProgress(10);

    const formData = new FormData();
    formData.append('file', file);

    try {
      setUploadProgress(30);
      const res = await axios.post(`${API_URL}/upload/`, formData, {
        headers: {
          'Content-Type': 'multipart/form-data',
          ...authHeader(),
        },
        onUploadProgress: (e) => {
          const pct = Math.round((e.loaded * 60) / e.total) + 30;
          setUploadProgress(Math.min(pct, 90));
        },
      });
      setUploadProgress(100);
      setUploadStatus(`✅ ${res.data.message}`);
      if (res.data.dag_run_id) {
        pollPipelineStatus(res.data.dag_run_id);
      }
      fetchHistory();
    } catch (err) {
      setUploadError(err.response?.data?.detail || '❌ Lỗi upload. Hãy kiểm tra kết nối server.');
    } finally {
      setUploading(false);
      setTimeout(() => setUploadProgress(0), 1200);
    }
  };

  const onDragOver = (e) => { e.preventDefault(); setIsDragging(true); };
  const onDragLeave = (e) => { e.preventDefault(); setIsDragging(false); };
  const onDrop = (e) => {
    e.preventDefault();
    setIsDragging(false);
    if (e.dataTransfer.files.length > 0) processFile(e.dataTransfer.files[0]);
  };

  const getStatus = (item) => STATUS_CONFIG[item.pipeline_status] || { label: item.pipeline_status, cls: 'bg-gray-100 text-gray-500' };

  return (
    <div className="min-h-screen bg-gray-50 font-sans">
      <header className="bg-white shadow px-8 py-4 flex justify-between items-center">
        <div>
          <h1 className="text-xl font-bold text-blue-600">EduLakehouse — Cổng Nạp Dữ Liệu</h1>
          <p className="text-xs text-gray-400 mt-0.5">Upload file PDF/DOCX để kích hoạt pipeline Bronze → Silver → Gold</p>
        </div>
        <button onClick={handleLogout} className="px-4 py-2 bg-red-50 hover:bg-red-100 text-red-600 rounded-lg text-sm transition">
          Đăng xuất
        </button>
      </header>

      <main className="max-w-4xl mx-auto mt-10 p-6 space-y-6">
        {/* Upload Card */}
        <div className="bg-white rounded-2xl shadow-lg p-8 border border-gray-100">
          <h2 className="text-lg font-bold text-gray-800 mb-1">Upload Dữ liệu vào Staging Zone</h2>
          <p className="text-xs text-gray-400 mb-6">Định dạng hỗ trợ: PDF, DOCX</p>

          <input
            type="file"
            ref={fileInputRef}
            onChange={(e) => processFile(e.target.files[0])}
            accept=".pdf,.docx"
            className="hidden"
          />

          <div
            onClick={() => !uploading && fileInputRef.current.click()}
            onDragOver={onDragOver}
            onDragLeave={onDragLeave}
            onDrop={onDrop}
            className={`border-4 border-dashed rounded-xl p-14 text-center transition-all duration-300 ${
              uploading ? 'cursor-wait opacity-70' : 'cursor-pointer'
            } ${isDragging ? 'border-blue-500 bg-blue-50 scale-[1.01]' : 'border-gray-200 hover:border-blue-300 hover:bg-blue-50'}`}
          >
            <div className="text-5xl mb-3">{uploading ? '⏳' : isDragging ? '📥' : '📄'}</div>
            <p className="text-base font-medium text-gray-700">
              {uploading ? 'Đang tải lên...' : 'Kéo & Thả hoặc Click để chọn file'}
            </p>
            <p className="text-xs text-gray-400 mt-1">Định dạng: PDF, DOCX</p>
          </div>

          {/* Progress bar */}
          {uploadProgress > 0 && (
            <div className="mt-4">
              <div className="flex justify-between text-xs text-gray-500 mb-1">
                <span>Tiến trình upload</span>
                <span>{uploadProgress}%</span>
              </div>
              <div className="w-full bg-gray-100 rounded-full h-2">
                <div
                  className="bg-blue-500 h-2 rounded-full transition-all duration-300"
                  style={{ width: `${uploadProgress}%` }}
                />
              </div>
            </div>
          )}

          {/* Thông báo */}
          {uploadStatus && (
            <div className="mt-4 p-3 bg-green-50 border border-green-200 text-green-700 rounded-lg text-sm">
              {uploadStatus}
            </div>
          )}
          {uploadError && (
            <div className="mt-4 p-3 bg-red-50 border border-red-200 text-red-600 rounded-lg text-sm">
              {uploadError}
            </div>
          )}
        </div>

        {/* Pipeline Step-by-Step Progress */}
        {activePipeline && (
          <div className="bg-white rounded-2xl shadow-lg p-6 border border-gray-100">
             <div className="flex justify-between items-center mb-4">
               <h3 className="text-sm font-bold text-gray-800">Tiến trình Pipeline (Airflow)</h3>
               <span className="text-xs px-2 py-1 bg-blue-50 text-blue-600 rounded-md">ID: {activePipeline.dag_run_id?.slice(0, 15)}...</span>
             </div>
             
             <div className="relative flex items-center justify-between">
                {/* Đường line kết nối (tạo background line) */}
                <div className="absolute top-4 left-10 right-10 h-0.5 bg-gray-200 z-0"></div>
                
                {PIPELINE_TASKS.map((pt, idx) => {
                   const t = activePipeline.tasks?.find(x => x.task_id === pt.id);
                   const state = t?.state || 'pending';
                   let color = 'bg-gray-200';
                   let textColor = 'text-gray-500';
                   if (state === 'success') { color = 'bg-green-500'; textColor = 'text-green-700'; }
                   else if (state === 'running') { color = 'bg-yellow-400 animate-pulse'; textColor = 'text-yellow-700'; }
                   else if (state === 'failed') { color = 'bg-red-500'; textColor = 'text-red-700'; }
                   
                   return (
                      <div key={pt.id} className="relative z-10 flex flex-col items-center flex-1 bg-white">
                         <div className={`w-8 h-8 rounded-full ${color} text-white flex items-center justify-center mb-2 text-xs font-bold ring-4 ring-white shadow-sm`}>
                            {state === 'success' ? '✓' : (idx + 1)}
                         </div>
                         <div className={`text-xs font-semibold ${textColor}`}>{pt.label}</div>
                         <div className="text-[10px] text-gray-400 capitalize">{state}</div>
                      </div>
                   )
                })}
             </div>
          </div>
        )}

        {/* Superset Dashboard Placeholder */}
        <div className="bg-white rounded-2xl shadow-lg p-6 border border-gray-100">
           <h3 className="text-lg font-bold text-gray-800 mb-4">Sơ đồ Superset</h3>
           <div className="w-full bg-gray-50 rounded-lg overflow-hidden border flex items-center justify-center" style={{ height: '400px' }}>
              {/* NOTE: Bạn hãy thay thế URL src bằng đường dẫn embed thực tế của Superset Dashboard */}
              <iframe
                 width="100%"
                 height="100%"
                 frameBorder="0"
                 src="http://localhost:8088/superset/dashboard/1/?standalone=1&height=400"
                 title="Superset Chart"
                 className="w-full h-full border-none"
              ></iframe>
           </div>
        </div>

        {/* Upload History */}
        <div className="bg-white rounded-2xl shadow-lg border border-gray-100 overflow-hidden">
          <div className="flex items-center justify-between px-6 py-4 border-b bg-gray-50">
            <div>
              <h3 className="font-semibold text-gray-800 text-sm">📋 Lịch sử Upload</h3>
              <p className="text-xs text-gray-400 mt-0.5">Trạng thái pipeline cập nhật mỗi 5 giây khi đang chạy</p>
            </div>
            <button
              onClick={fetchHistory}
              className="text-xs text-blue-600 hover:underline"
              disabled={historyLoading}
            >
              {historyLoading ? 'Đang tải...' : '🔄 Làm mới'}
            </button>
          </div>

          {history.length === 0 ? (
            <div className="text-center py-10 text-gray-400 text-sm">Chưa có file nào được upload.</div>
          ) : (
            <table className="w-full text-sm">
              <thead className="bg-gray-50 text-xs text-gray-500">
                <tr>
                  {['#', 'Tên file', 'Người upload', 'Thời gian', 'Trạng thái Pipeline'].map((h) => (
                    <th key={h} className="text-left px-4 py-2 font-semibold">{h}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {history.map((item, i) => {
                  const st = getStatus(item);
                  return (
                    <tr key={item.id || i} className="border-t border-gray-100 hover:bg-gray-50">
                      <td className="px-4 py-3 text-gray-400 text-xs">{i + 1}</td>
                      <td className="px-4 py-3">
                        <div className="font-medium text-gray-800 text-xs">{item.filename}</div>
                        <div className="text-gray-400 text-xs">{item.minio_path}</div>
                      </td>
                      <td className="px-4 py-3 text-gray-600 text-xs">{item.username}</td>
                      <td className="px-4 py-3 text-gray-500 text-xs">
                        {item.uploaded_at
                          ? new Date(item.uploaded_at + 'Z').toLocaleString('vi-VN')
                          : '—'}
                      </td>
                      <td className="px-4 py-3">
                        <span className={`text-xs px-2 py-1 rounded-full font-medium ${st.cls}`}>
                          {st.label}
                        </span>
                        {item.dag_run_id && (
                          <div className="text-xs text-gray-300 mt-0.5 font-mono">
                            {item.dag_run_id.slice(0, 24)}...
                          </div>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>
      </main>
    </div>
  );
};

export default UserUpload;