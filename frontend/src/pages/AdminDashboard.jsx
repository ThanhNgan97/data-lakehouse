import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import CatalogHistoryTimeline from './CatalogHistoryTimeline';
import PipelineDataExplorer from './Pipelinedataexplorer';
import UserManagement from './UserManagement';
import UploadHistory from './UploadHistory';

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
    { key: 'dashboard', icon: '', label: 'Báo cáo Tổng hợp (Gold)' },
    { key: 'pipeline',  icon: '', label: 'Dữ liệu Pipeline' },
    { key: 'catalog',   icon: '', label: 'Lịch sử Branch/Merge' },
    { key: 'history',   icon: '', label: 'Lịch sử Upload' },
    { key: 'users',     icon: '', label: 'Quản lý Người dùng' },
  ];

  const TAB_TITLES = {
    dashboard: 'Báo cáo Thường niên chất lượng Giáo dục (Apache Superset)',
    pipeline:  'Pipeline Dữ liệu: Bronze → Silver → Gold',
    catalog:   'Lịch sử Pipeline (Nessie Catalog)',
    history:   'Lịch sử Tải lên dữ liệu',
    users:     'Quản lý Người dùng',
  };

  return (
    <div className="flex h-screen bg-gray-100 font-sans">
      {/* Sidebar */}
      <div className="w-64 bg-slate-900 text-white flex flex-col shadow-2xl shrink-0">
        <div className="h-20 flex items-center justify-center border-b border-slate-800">
          <h1 className="text-2xl font-bold text-transparent bg-clip-text bg-gradient-to-r from-blue-400 to-teal-300">
            Lakehouse
          </h1>
        </div>

        <nav className="flex-1 p-4 space-y-1 mt-4 overflow-y-auto">
          <div className="text-xs text-slate-400 uppercase font-semibold mb-3">Quản trị hệ thống</div>
          {NAV.map(({ key, icon, label }) => (
            <button
              key={key}
              onClick={() => setActiveTab(key)}
              className={`w-full text-left py-3 px-4 rounded-lg transition text-sm flex items-center gap-2 ${
                activeTab === key
                  ? 'bg-blue-600 shadow text-white'
                  : 'text-slate-300 hover:bg-slate-800'
              }`}
            >
              <span>{icon}</span>
              <span>{label}</span>
            </button>
          ))}
        </nav>

        <div className="p-5 border-t border-slate-800">
          <div className="flex items-center space-x-3 mb-4">
            <div className="w-10 h-10 rounded-full bg-gradient-to-br from-teal-500 to-blue-500 flex items-center justify-center font-bold text-lg shrink-0">
              A
            </div>
            <div className="min-w-0">
              <p className="text-sm font-medium truncate">Ban Giám Hiệu</p>
              <p className="text-xs text-slate-400">Admin</p>
            </div>
          </div>
          <button
            onClick={handleLogout}
            className="w-full text-center py-2 bg-slate-800 hover:bg-red-600 text-slate-300 hover:text-white rounded-lg transition text-sm"
          >
            Đăng xuất
          </button>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        <header className="h-16 bg-white shadow-sm flex items-center justify-between px-8 z-10 shrink-0">
          <h2 className="text-base font-semibold text-gray-800">{TAB_TITLES[activeTab]}</h2>
          <div className="text-xs text-gray-500">
            Kết nối:{' '}
            <span className="text-green-600 font-semibold">Nessie · Trino · MinIO · Superset</span>
          </div>
        </header>

        <main className="flex-1 overflow-hidden p-5 bg-gray-50">
          <div className="bg-white rounded-xl shadow-sm border border-gray-200 h-full overflow-hidden flex flex-col">
            {/* Tiêu đề phụ (ẩn với pipeline vì nó tự có) */}
            {activeTab !== 'pipeline' && (
              <div className="px-5 py-3 border-b bg-gray-50 flex justify-between items-center shrink-0">
                <h3 className="font-medium text-gray-700 text-sm">{TAB_TITLES[activeTab]}</h3>
                {activeTab === 'dashboard' && (
                  <a
                    href={supersetUrl}
                    target="_blank"
                    rel="noreferrer"
                    className="text-xs text-blue-600 hover:underline"
                  >
                    Mở toàn màn hình ↗
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