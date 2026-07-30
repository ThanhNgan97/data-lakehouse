import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import CatalogHistoryTimeline from './CatalogHistoryTimeline';
import PipelineDataExplorer from './Pipelinedataexplorer';
import UserManagement from './UserManagement';
import UploadHistory from './UploadHistory';
import { BarChart3, GitMerge, GitBranch, History, Users, ExternalLink, LogOut } from 'lucide-react';

const AdminDashboard = () => {
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState('dashboard');

  const supersetUrl =
    import.meta.env.VITE_SUPERSET_DASHBOARD_URL ||
    'http://localhost:8088/superset/dashboard/1/?standalone=3';

  const handleLogout = () => {
    localStorage.removeItem('token');
    localStorage.removeItem('role');
    navigate('/login');
  };

  const NAV = [
    { key: 'dashboard', icon: <BarChart3 className="w-5 h-5" />, label: 'Báo cáo Tổng hợp (Gold)' },
    { key: 'pipeline',  icon: <GitMerge className="w-5 h-5" />, label: 'Dữ liệu Pipeline' },
    { key: 'catalog',   icon: <GitBranch className="w-5 h-5" />, label: 'Lịch sử Branch/Merge' },
    { key: 'history',   icon: <History className="w-5 h-5" />, label: 'Lịch sử Upload' },
    { key: 'users',     icon: <Users className="w-5 h-5" />, label: 'Quản lý Người dùng' },
  ];

  const TAB_TITLES = {
    dashboard: 'Báo cáo Thường niên chất lượng Giáo dục',
    pipeline:  'Pipeline Dữ liệu: Bronze → Silver → Gold',
    catalog:   'Lịch sử Pipeline',
    history:   'Lịch sử Tải lên dữ liệu',
    users:     'Quản lý Người dùng',
  };

  return (
    <div className="flex h-screen bg-gray-100 font-sans">
      {/* Sidebar */}
      <div className="w-64 bg-gradient-to-b from-slate-900 to-[#0f172a] text-white flex flex-col shadow-2xl shrink-0 z-20">
        <div className="h-16 flex items-center px-6 border-b border-slate-800/50">
          <div className="flex items-center gap-2">
            <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-blue-500 to-teal-400 flex items-center justify-center shadow-lg shadow-blue-500/30">
              <span className="text-white font-bold text-lg">E</span>
            </div>
            <h1 className="text-lg font-bold text-transparent bg-clip-text bg-gradient-to-r from-white to-slate-300 tracking-tight">
              EduLakehouse
            </h1>
          </div>
        </div>

        <nav className="flex-1 px-4 py-6 space-y-1.5 overflow-y-auto">
          <div className="text-[10px] text-slate-500 uppercase font-bold tracking-wider mb-3 px-3">Quản trị hệ thống</div>
          {NAV.map(({ key, icon, label }) => (
            <button
              key={key}
              onClick={() => setActiveTab(key)}
              className={`w-full text-left py-2.5 px-3 rounded-xl transition-all duration-300 text-sm flex items-center gap-3 font-medium ${
                activeTab === key
                  ? 'bg-blue-500/15 text-blue-400 shadow-sm border border-blue-500/20'
                  : 'text-slate-400 hover:bg-slate-800/50 hover:text-slate-200'
              }`}
            >
              <div className={activeTab === key ? 'text-blue-400' : 'text-slate-500'}>{icon}</div>
              <span>{label}</span>
            </button>
          ))}
        </nav>

        <div className="p-4 border-t border-slate-800/50 bg-slate-900/50 backdrop-blur-md">
          <div className="flex items-center space-x-3 mb-4 px-2">
            <div className="w-9 h-9 rounded-full bg-gradient-to-br from-indigo-500 to-purple-500 flex items-center justify-center font-bold text-sm shadow-md shrink-0 border border-white/10">
              A
            </div>
            <div className="min-w-0">
              <p className="text-sm font-semibold text-slate-200 truncate">Ban Giám Hiệu</p>
              <p className="text-[11px] text-slate-400 font-medium">Admin System</p>
            </div>
          </div>
          <button
            onClick={handleLogout}
            className="w-full flex items-center justify-center gap-2 py-2 bg-slate-800/80 hover:bg-red-500/20 text-slate-300 hover:text-red-400 rounded-lg transition-colors duration-300 text-sm font-medium border border-transparent hover:border-red-500/30"
          >
            <LogOut className="w-4 h-4" /> Đăng xuất
          </button>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col overflow-hidden bg-slate-50/50">
        <header className="h-16 bg-white/80 backdrop-blur-md border-b border-gray-200/80 flex items-center justify-between px-8 z-10 shrink-0">
          <h2 className="text-[15px] font-semibold text-slate-700 tracking-tight">{TAB_TITLES[activeTab]}</h2>
          <div className="flex items-center gap-2">
            <span className="relative flex h-2.5 w-2.5">
              <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
              <span className="relative inline-flex rounded-full h-2.5 w-2.5 bg-green-500"></span>
            </span>
            <div className="text-[11px] font-medium text-slate-500 uppercase tracking-wide">
              Hệ thống Online
            </div>
          </div>
        </header>

        <main className="flex-1 overflow-hidden p-6 relative">
          <div className="absolute top-0 left-0 w-full h-64 bg-gradient-to-b from-white to-transparent opacity-60 pointer-events-none"></div>
          <div className="bg-white/90 backdrop-blur-sm rounded-2xl shadow-sm border border-slate-200/60 h-full overflow-hidden flex flex-col relative z-10">
            {/* Tiêu đề phụ (ẩn với pipeline vì nó tự có) */}
            {activeTab !== 'pipeline' && (
              <div className="px-6 py-4 border-b border-slate-100 flex justify-between items-center shrink-0 bg-white">
                <h3 className="font-semibold text-slate-800 text-sm tracking-tight">{TAB_TITLES[activeTab]}</h3>
                {activeTab === 'dashboard' && (
                  <a
                    href={supersetUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="text-xs font-medium text-blue-600 hover:text-blue-700 bg-blue-50 hover:bg-blue-100 px-3 py-1.5 rounded-lg transition-colors flex items-center gap-1.5"
                  >
                    Mở toàn màn hình <ExternalLink className="w-3.5 h-3.5" />
                  </a>
                )}
              </div>
            )}

            <div className={`flex-1 overflow-auto ${activeTab === 'dashboard' ? 'overflow-hidden' : ''}`}>
              {activeTab === 'dashboard' && (
                <iframe
                  src={supersetUrl}
                  title="Superset Dashboard"
                  className="w-full h-full border-0"
                />
              )}
              {activeTab === 'pipeline' && <PipelineDataExplorer />}
              {activeTab === 'catalog'  && <CatalogHistoryTimeline />}
              {activeTab === 'history'  && <UploadHistory />}
              {activeTab === 'users'    && <UserManagement />}
            </div>
          </div>
        </main>
      </div>
    </div>
  );
};

export default AdminDashboard;