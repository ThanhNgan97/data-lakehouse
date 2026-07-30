import React, { useState, useRef, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import axios from 'axios';

import { 
  Hourglass, 
  Package, 
  Rocket, 
  AlertTriangle, 
  XCircle, 
  Settings, 
  CheckCircle2, 
  Clock, 
  Unplug, 
  Inbox, 
  FileText, 
  RefreshCw, 
  FolderOpen,
  User 
} from 'lucide-react';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';
  const supersetUrl =
    import.meta.env.VITE_SUPERSET_DASHBOARD_URL ||
    'http://localhost:8088/superset/dashboard/1/?standalone=3';
const authHeader = () => ({
  Authorization: `Bearer ${localStorage.getItem('token')}`,
});

const STATUS_CONFIG = {
  pending:        { label: 'Đang chờ',       icon: <Hourglass className="w-3 h-3 inline mr-1" />,       cls: 'bg-gray-100 text-gray-600' },
  uploaded:       { label: 'Đã upload',       icon: <Package className="w-3 h-3 inline mr-1" />,       cls: 'bg-blue-100 text-blue-600' },
  triggered:      { label: 'Pipeline đang chạy', icon: <Rocket className="w-3 h-3 inline mr-1" />, cls: 'bg-yellow-100 text-yellow-700' },
  trigger_failed: { label: 'Chưa kích hoạt pipeline', icon: <AlertTriangle className="w-3 h-3 inline mr-1" />, cls: 'bg-orange-100 text-orange-600' },
  upload_failed:  { label: 'Upload thất bại',  icon: <XCircle className="w-3 h-3 inline mr-1" />,  cls: 'bg-red-100 text-red-600' },
  // Trạng thái từ Airflow
  running:        { label: 'Pipeline đang chạy', icon: <Settings className="w-3 h-3 inline mr-1" />, cls: 'bg-yellow-100 text-yellow-700' },
  success:        { label: 'Hoàn thành',       icon: <CheckCircle2 className="w-3 h-3 inline mr-1" />,       cls: 'bg-green-100 text-green-700' },
  failed:         { label: 'Pipeline lỗi',     icon: <XCircle className="w-3 h-3 inline mr-1" />,     cls: 'bg-red-100 text-red-600' },
  queued:         { label: 'Đang xếp hàng',   icon: <Clock className="w-3 h-3 inline mr-1" />,   cls: 'bg-purple-100 text-purple-600' },
  unreachable:    { label: 'Airflow offline',  icon: <Unplug className="w-3 h-3 inline mr-1" />,  cls: 'bg-gray-100 text-gray-500' },
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
      setUploadStatus(`Thành công: ${res.data.message}`);
      if (res.data.dag_run_id) {
        pollPipelineStatus(res.data.dag_run_id);
      }
      fetchHistory();
    } catch (err) {
      setUploadError(err.response?.data?.detail || 'Lỗi upload. Hãy kiểm tra kết nối server.');
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

  const getStatus = (item) => STATUS_CONFIG[item.pipeline_status] || { label: item.pipeline_status, icon: null, cls: 'bg-gray-100 text-gray-500' };

  return (
    <div className="min-h-screen bg-slate-50 font-sans flex flex-col">
      <header className="bg-white/80 backdrop-blur-md border-b border-slate-200/60 px-6 md:px-8 py-4 flex justify-between items-center sticky top-0 z-50">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 bg-gradient-to-br from-blue-600 to-indigo-600 rounded-xl flex items-center justify-center shadow-lg shadow-blue-600/20">
            <span className="text-white font-bold text-xl">E</span>
          </div>
          <div>
            <h1 className="text-lg md:text-xl font-bold text-slate-800 leading-tight tracking-tight">EduLakehouse Portal</h1>
            <p className="text-[11px] md:text-xs text-slate-500 font-medium">Cổng nạp & Xử lý dữ liệu trung tâm</p>
          </div>
        </div>
        <button onClick={handleLogout} className="px-4 md:px-5 py-2 bg-white border border-slate-200 hover:bg-slate-50 hover:border-slate-300 text-slate-700 rounded-lg text-sm font-semibold transition-all duration-200 shadow-sm">
          Đăng xuất
        </button>
      </header>

      <main className="flex-1 w-full max-w-[1400px] mx-auto p-4 md:p-6 lg:p-8 grid grid-cols-1 lg:grid-cols-12 gap-6 lg:gap-8">
        
        {/* CỘT TRÁI (Upload & Pipeline) */}
        <div className="lg:col-span-4 flex flex-col gap-6">
          
          {/* 1. Upload Card */}
          <div className="bg-white rounded-2xl shadow-xl shadow-slate-200/40 border border-slate-100 p-7">
            <div className="mb-6">
              <h2 className="text-lg font-bold text-slate-800 tracking-tight">Tải lên Dữ liệu</h2>
              <p className="text-xs text-slate-500 mt-1">Hệ thống sẽ tự động đưa vào Bronze Zone và kích hoạt Pipeline</p>
            </div>

            <input type="file" ref={fileInputRef} onChange={(e) => processFile(e.target.files[0])} accept=".pdf,.docx" className="hidden" />

            <div
              onClick={() => !uploading && fileInputRef.current.click()}
              onDragOver={onDragOver}
              onDragLeave={onDragLeave}
              onDrop={onDrop}
              className={`relative overflow-hidden border-2 border-dashed rounded-2xl p-10 text-center transition-all duration-300 flex flex-col items-center justify-center group ${
                uploading ? 'cursor-wait opacity-90 bg-slate-50 border-slate-300' : 'cursor-pointer border-blue-200 hover:border-blue-400 hover:bg-blue-50/50 hover:shadow-inner'
              } ${isDragging ? 'border-blue-500 bg-blue-50 scale-[1.02] shadow-lg shadow-blue-500/10' : ''}`}
            >
              <div className={`w-16 h-16 mb-4 rounded-2xl flex items-center justify-center transition-all duration-300 shadow-sm ${isDragging ? 'bg-blue-600 scale-110 text-white shadow-blue-500/30' : 'bg-blue-50 text-blue-600 group-hover:scale-110 group-hover:bg-blue-600 group-hover:text-white group-hover:shadow-blue-500/30 group-hover:-rotate-3'}`}>
                 {uploading ? <Hourglass className="w-8 h-8 animate-pulse" /> : isDragging ? <Inbox className="w-8 h-8" /> : <FileText className="w-8 h-8" />}
              </div>
              <p className="text-sm font-semibold text-slate-700">
                {uploading ? 'Đang tải lên và xử lý...' : 'Kéo thả file hoặc Click'}
              </p>
              <p className="text-[11px] font-medium text-slate-400 mt-2">Hỗ trợ định dạng: PDF, DOCX</p>
              
              {uploading && uploadProgress > 0 && (
                <div className="absolute bottom-0 left-0 h-1.5 bg-blue-500 transition-all duration-300" style={{ width: `${uploadProgress}%` }} />
              )}
            </div>

            {/* Thông báo */}
            {uploadStatus && (
              <div className="mt-4 p-3 bg-green-50 border border-green-200 text-green-700 rounded-lg text-sm flex items-start gap-2 transition-opacity">
                <CheckCircle2 className="w-5 h-5 shrink-0" /><span className="font-medium">{uploadStatus}</span>
              </div>
            )}
            {uploadError && (
              <div className="mt-4 p-3 bg-red-50 border border-red-200 text-red-600 rounded-lg text-sm flex items-start gap-2 transition-opacity">
                <AlertTriangle className="w-5 h-5 shrink-0" /><span className="font-medium">{uploadError}</span>
              </div>
            )}
          </div>

          {/* 2. Pipeline Status (Vertical Stepper) */}
          {activePipeline && (
            <div className="bg-white rounded-2xl shadow-xl shadow-slate-200/40 border border-slate-100 p-7 flex-1">
               <div className="flex justify-between items-start mb-8">
                 <div>
                   <h3 className="text-base font-bold text-slate-800 tracking-tight">Tiến trình Pipeline</h3>
                   <p className="text-[11px] font-medium text-slate-500 mt-1">Airflow Orchestration</p>
                 </div>
                 {activePipeline.dag_run_id && (
                   <span className="text-[10px] font-mono px-2 py-1 bg-gray-100 text-gray-600 rounded border border-gray-200" title={activePipeline.dag_run_id}>
                      ID: {activePipeline.dag_run_id.slice(0, 8)}...
                   </span>
                 )}
               </div>
               
               <div className="relative pl-5 border-l-2 border-gray-100 space-y-6 ml-2">
                  {PIPELINE_TASKS.map((pt, idx) => {
                     const t = activePipeline.tasks?.find(x => x.task_id === pt.id);
                     const state = t?.state || 'pending';
                     
                     let dotColor = 'bg-gray-200 border-gray-300';
                     let borderColor = 'border-gray-100';
                     let stateText = 'Đang đợi';
                     let icon = <Hourglass className="w-4 h-4" />;
                     let textColor = 'text-gray-400';
                     
                     if (state === 'success') {
                         dotColor = 'bg-green-500 border-green-200 shadow-[0_0_8px_rgba(34,197,94,0.4)]'; borderColor = 'border-green-200'; stateText = 'Thành công'; icon = <CheckCircle2 className="w-4 h-4 text-green-500" />; textColor = 'text-green-700';
                     } else if (state === 'running') {
                         dotColor = 'bg-blue-500 border-blue-200 animate-pulse shadow-[0_0_8px_rgba(59,130,246,0.6)]'; borderColor = 'border-blue-300'; stateText = 'Đang xử lý...'; icon = <Settings className="w-4 h-4 text-blue-500 animate-spin" />; textColor = 'text-blue-700';
                     } else if (state === 'failed') {
                         dotColor = 'bg-red-500 border-red-200 shadow-[0_0_8px_rgba(239,68,68,0.4)]'; borderColor = 'border-red-200'; stateText = 'Lỗi'; icon = <XCircle className="w-4 h-4 text-red-500" />; textColor = 'text-red-700';
                     }
                     
                     return (
                        <div key={pt.id} className="relative">
                           {/* Dot */}
                           <div className={`absolute -left-[27px] top-2 w-3.5 h-3.5 rounded-full border-2 bg-clip-padding ${dotColor} z-10 transition-colors duration-300`} />
                           
                           {/* Card */}
                           <div className={`bg-white rounded-xl p-3 border ${borderColor} shadow-sm transition-all duration-300 ${state === 'running' ? 'bg-blue-50/40 scale-[1.02]' : ''}`}>
                              <div className="flex items-center justify-between">
                                 <p className={`font-semibold text-sm ${state === 'pending' ? 'text-gray-500' : 'text-gray-800'}`}>{pt.label}</p>
                                 <span className="text-sm opacity-80">{icon}</span>
                              </div>
                              <p className={`text-[11px] font-medium mt-0.5 ${textColor}`}>{stateText}</p>
                           </div>
                        </div>
                     )
                  })}
               </div>
            </div>
          )}
        </div>

        {/* CỘT PHẢI (History & Superset) */}
        <div className="lg:col-span-8 flex flex-col gap-6 h-full">
          
          {/* 3. Upload History */}
          <div className="bg-white rounded-2xl shadow-xl shadow-slate-200/40 border border-slate-100 overflow-hidden flex flex-col h-[340px]">
            <div className="flex items-center justify-between px-7 py-5 border-b border-slate-100 bg-white shrink-0">
              <div>
                <h3 className="font-bold text-slate-800 text-base tracking-tight">Lịch sử tải lên</h3>
                <p className="text-[11px] font-medium text-slate-500 mt-1">Tự động đồng bộ trạng thái Pipeline mỗi 5 giây</p>
              </div>
              <button
                onClick={fetchHistory}
                className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-semibold text-blue-700 bg-blue-50 hover:bg-blue-100 rounded-lg transition"
                disabled={historyLoading}
              >
                <RefreshCw className={`w-3.5 h-3.5 ${historyLoading ? 'animate-spin' : ''}`} /> {historyLoading ? 'Đang tải...' : 'Làm mới'}
              </button>
            </div>

            <div className="flex-1 overflow-auto bg-gray-50/50 p-4">
              {history.length === 0 ? (
                <div className="h-full flex flex-col items-center justify-center text-gray-400">
                  <FolderOpen className="w-12 h-12 mb-3 opacity-50" />
                  <p className="text-sm font-medium">Chưa có dữ liệu nào được tải lên hệ thống.</p>
                </div>
              ) : (
                <div className="space-y-3">
                  {history.map((item, i) => {
                    const st = getStatus(item);
                    return (
                      <div key={item.id || i} className="bg-white border border-slate-100 rounded-2xl p-4 shadow-sm hover:shadow-md hover:border-slate-200 transition-all flex items-center justify-between gap-4">
                        <div className="flex items-center gap-4 min-w-0">
                           <div className="w-12 h-12 rounded-xl bg-blue-50 text-blue-600 flex items-center justify-center shrink-0">
                             <FileText className="w-5 h-5" />
                           </div>
                           <div className="min-w-0">
                             <h4 className="font-bold text-slate-800 text-sm truncate" title={item.filename}>{item.filename}</h4>
                             <div className="text-[11px] text-gray-500 mt-1 flex items-center gap-2 truncate">
                                <span className="font-medium text-gray-600 flex items-center gap-1"><User className="w-3 h-3" /> {item.username}</span>
                                <span className="text-gray-300">•</span>
                                <span className="font-mono text-gray-500" title={item.dag_run_id}>{item.dag_run_id?.slice(0, 12)}...</span>
                             </div>
                           </div>
                        </div>
                        
                        <div className="flex flex-col items-end shrink-0 gap-1.5">
                           <span className={`text-[10px] px-2.5 py-1 rounded-md font-semibold border border-opacity-20 flex items-center ${st.cls}`}>
                             {st.icon}{st.label}
                           </span>
                           <span className="text-[10px] text-gray-400 font-medium">
                             {item.uploaded_at ? new Date(item.uploaded_at + 'Z').toLocaleString('vi-VN') : '—'}
                           </span>
                        </div>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </div>

          {/* 4. Superset Dashboard Placeholder */}
          <div className="bg-white rounded-2xl shadow-xl shadow-slate-200/40 border border-slate-100 overflow-hidden flex flex-col flex-1 min-h-[450px]">
             <div className="px-7 py-5 border-b border-slate-100 bg-white flex justify-between items-center shrink-0">
                <div>
                  <h3 className="text-base font-bold text-slate-800 tracking-tight">Báo cáo Phân tích (Gold Layer)</h3>
                  <p className="text-[11px] font-medium text-slate-500 mt-1">Trực quan hóa dữ liệu bằng Apache Superset</p>
                </div>
                <a href={supersetUrl} target="_blank" rel="noreferrer" className="text-xs text-blue-700 hover:text-blue-800 bg-blue-50 hover:bg-blue-100 px-3 py-1.5 rounded-lg transition font-medium flex items-center gap-1.5">
                  Mở tab mới ↗
                </a>
             </div>
             <div className="flex-1 bg-gray-50 relative p-3">
                {/* Lớp overlay trong suốt (nếu cần) hoặc dùng thẳng pointer-events-none trên iframe */}
                <iframe
                   src={supersetUrl}
                   title="Superset Chart"
                   className="w-full h-full border border-gray-200 bg-white rounded-xl shadow-inner "
                ></iframe>
             </div>
          </div>

        </div>
      </main>
    </div>
  );
};

export default UserUpload;