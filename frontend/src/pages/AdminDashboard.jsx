import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import CatalogHistoryTimeline from './CatalogHistoryTimeline';
import PipelineDataExplorer from './Pipelinedataexplorer';
import UserManagement from './UserManagement';
import UploadHistory from './UploadHistory';
import DataConnectors from './DataConnectors';
import Icon from '../components/icons';
import { ConnChip, LakehouseMark } from '../components/ui';
import { SUPERSET_DASHBOARD_URL } from '../config/appConfig';

const AdminDashboard = () => {
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState('dashboard');

  const supersetUrl = SUPERSET_DASHBOARD_URL;

  const handleLogout = () => {
    localStorage.removeItem('token');
    localStorage.removeItem('role');
    navigate('/login');
  };

  const NAV = [
    { key: 'dashboard', icon: 'grid', label: 'Báo cáo Tổng hợp', sub: 'Gold Layer' },
    { key: 'pipeline', icon: 'layers', label: 'Dữ liệu Pipeline', sub: 'Bronze · Silver · Gold' },
    { key: 'catalog', icon: 'gitBranch', label: 'Lịch sử Branch/Merge', sub: 'Nessie Catalog' },
    { key: 'history', icon: 'history', label: 'Lịch sử Upload', sub: 'Audit trail' },
    { key: 'users', icon: 'users', label: 'Quản lý Người dùng', sub: 'Phân quyền' },
    { key: 'connectors', icon: 'layers', label: 'Quản lý Nguồn Dữ liệu', sub: 'MySQL Connectors' },
  ];

  const TAB_TITLES = {
    dashboard: 'Báo cáo Thường niên Chất lượng Giáo dục',
    pipeline: 'Pipeline Dữ liệu: Bronze → Silver → Gold',
    catalog: 'Lịch sử Pipeline (Nessie Catalog)',
    history: 'Lịch sử Tải lên Dữ liệu',
    users: 'Quản lý Người dùng',
    connectors: 'Quản lý Nguồn Dữ liệu',
  };
  const TAB_DESC = {
    dashboard: 'Trực quan hóa dữ liệu bằng Apache Superset',
    pipeline: 'Xem dữ liệu thật ở từng tầng, giống trạng thái chạy trên Airflow',
    catalog: 'Mỗi mốc thời gian tương ứng 1 commit ingest/merge/tag trên Iceberg',
    history: 'Toàn bộ nhật ký tải lên và trạng thái xử lý',
    users: 'Thêm, khoá và phân quyền tài khoản trong hệ thống',
    connectors: 'Cấu hình, kiểm tra và đồng bộ MySQL Data Connectors',
  };

  return (
    <div className="flex h-screen bg-[#F5F6FA] font-sans text-ink-900">
      {/* Sidebar */}
      <div className="w-64 bg-slate-900 text-white flex flex-col shadow-2xl shrink-0 z-20 border-r border-slate-800">
        <div className="h-20 flex items-center px-5 border-b border-slate-800 gap-3">
          <div className="p-1.5 bg-white rounded-lg shrink-0 shadow-sm">
            <img src="/CUSC Logo Series.png" alt="CUSC Logo" className="h-7 w-auto object-contain" />
          </div>
          <div>
            <h1 className="text-[13px] font-bold text-white tracking-tight leading-tight mt-1">CUSC ANALYSIS PLATFORM</h1>
            <p className="text-[9px] text-slate-400 font-medium">Trung tâm Công nghệ Thông tin - ĐHCT</p>
          </div>
        </div>

        <nav className="flex-1 px-3 py-5 space-y-1 overflow-y-auto">
          <p className="px-3 text-[10px] font-data uppercase tracking-widest text-ink-400 mb-2">
            Quản trị hệ thống
          </p>
          {NAV.map(({ key, icon, label, sub }) => {
            const active = activeTab === key;
            return (
              <button
                key={key}
                onClick={() => setActiveTab(key)}
                className={`w-full group flex items-center gap-3 py-2.5 px-3 rounded-xl text-sm transition relative ${
                  active ? 'bg-white/[0.08] text-white' : 'text-ink-300 hover:bg-white/[0.05] hover:text-white'
                }`}
              >
                {active && (
                  <span className="absolute left-0 top-1.5 bottom-1.5 w-[3px] rounded-full bg-lake-400" />
                )}
                <span
                  className={`w-8 h-8 rounded-lg flex items-center justify-center shrink-0 transition ${
                    active ? 'bg-lake-500/20 text-lake-300' : 'bg-white/[0.04] text-ink-400 group-hover:text-ink-200'
                  }`}
                >
                  <Icon name={icon} className="w-4 h-4" />
                </span>
                <span className="flex flex-col items-start min-w-0">
                  <span className="font-semibold truncate w-full text-left">{label}</span>
                  <span className="text-[10.5px] text-ink-400 font-data truncate w-full text-left">{sub}</span>
                </span>
              </button>
            );
          })}
        </nav>

        <div className="p-4 border-t border-white/10">
          <div className="flex items-center gap-3 mb-3 px-2">
            <div className="w-9 h-9 rounded-full bg-gradient-to-br from-lake-400 to-lake-700 flex items-center justify-center font-bold text-sm shrink-0">
              A
            </div>
            <div className="min-w-0">
              <p className="text-sm font-semibold truncate">Ban Giám Hiệu</p>
              <p className="text-[11px] text-ink-400 font-data">admin</p>
            </div>
          </div>
          <button
            onClick={handleLogout}
            className="w-full flex items-center justify-center gap-2 py-2.5 bg-white/[0.05] hover:bg-rose-500/90 text-ink-200 hover:text-white rounded-lg transition text-sm font-semibold"
          >
            <Icon name="logOut" className="w-4 h-4" />
            Đăng xuất
          </button>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col overflow-hidden">
        <header className="h-16 bg-white/80 backdrop-blur border-b border-ink-100 flex items-center justify-between px-8 shrink-0">
          <div>
            <h2 className="text-sm font-bold text-ink-900">{TAB_TITLES[activeTab]}</h2>
            <p className="text-[12px] text-ink-400">{TAB_DESC[activeTab]}</p>
          </div>
          <div className="flex items-center gap-2">
            <ConnChip label="Nessie" />
            <ConnChip label="Trino" />
            <ConnChip label="MinIO" />
            <ConnChip label="Superset" />
            {activeTab === 'dashboard' && (
              <a
                href={supersetUrl}
                target="_blank"
                rel="noreferrer"
                className="ml-2 inline-flex items-center gap-1.5 text-xs font-semibold text-lake-700 bg-lake-50 hover:bg-lake-100 border border-lake-200 px-3 py-1.5 rounded-lg transition"
              >
                Mở toàn màn hình
                <Icon name="externalLink" className="w-3.5 h-3.5" />
              </a>
            )}
          </div>
        </header>

        <main className="flex-1 overflow-hidden p-6 bg-[#F5F6FA]">
          <div className="bg-white rounded-2xl border border-ink-100 shadow-card h-full overflow-hidden flex flex-col">
            <div className={`flex-1 overflow-auto ${activeTab === 'dashboard' ? 'overflow-hidden' : ''}`}>
              {activeTab === 'dashboard' && (
                <iframe
                  src={supersetUrl}
                  title="Superset Dashboard"
                  className="w-full h-full border-0"
                />
              )}
              {activeTab === 'pipeline' && <PipelineDataExplorer />}
              {activeTab === 'catalog' && <CatalogHistoryTimeline />}
              {activeTab === 'history' && <UploadHistory />}
              {activeTab === 'users' && <UserManagement />}
              {activeTab === 'connectors' && <DataConnectors />}
            </div>
          </div>
        </main>
      </div>
    </div>
  );
};

export default AdminDashboard;
