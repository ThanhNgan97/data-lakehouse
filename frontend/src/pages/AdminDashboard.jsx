import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';
import { Menu, X } from 'lucide-react';
import CatalogHistoryTimeline from './CatalogHistoryTimeline';
import PipelineDataExplorer from './Pipelinedataexplorer';
import UserManagement from './UserManagement';
import UploadHistory from './UploadHistory';
import Icon from '../components/icons';
import { ConnChip } from '../components/ui';

const AdminDashboard = () => {
  const navigate = useNavigate();
  const [activeTab, setActiveTab] = useState('dashboard');
  const [sidebarOpen, setSidebarOpen] = useState(false);

  const supersetUrl =
    import.meta.env.VITE_SUPERSET_DASHBOARD_URL ||
    'http://localhost:8088/superset/dashboard/1/?standalone=3';

  const handleLogout = () => {
    localStorage.removeItem('token');
    localStorage.removeItem('role');
    navigate('/login');
  };

  const username = localStorage.getItem('username') || 'admin';
  const role = localStorage.getItem('role') || 'admin';
  const userInitial = username.trim().charAt(0).toUpperCase() || 'A';

  const NAV = [
    { key: 'dashboard', icon: 'grid', label: 'Báo cáo Tổng hợp', sub: 'Gold Layer' },
    { key: 'pipeline', icon: 'layers', label: 'Dữ liệu Pipeline', sub: 'Bronze · Silver · Gold' },
    { key: 'catalog', icon: 'gitBranch', label: 'Lịch sử Branch/Merge', sub: 'Nessie Catalog' },
    { key: 'history', icon: 'history', label: 'Lịch sử Upload', sub: 'Audit trail' },
    { key: 'users', icon: 'users', label: 'Quản lý Người dùng', sub: 'Phân quyền' },
  ];

  const TAB_TITLES = {
    dashboard: 'Báo cáo Thường niên Chất lượng Giáo dục',
    pipeline: 'Pipeline Dữ liệu: Bronze → Silver → Gold',
    catalog: 'Lịch sử Pipeline (Nessie Catalog)',
    history: 'Lịch sử Tải lên Dữ liệu',
    users: 'Quản lý Người dùng',
  };
  const TAB_DESC = {
    dashboard: 'Trực quan hóa dữ liệu bằng Apache Superset',
    pipeline: 'Xem dữ liệu thật ở từng tầng, giống trạng thái chạy trên Airflow',
    catalog: 'Mỗi mốc thời gian tương ứng 1 commit ingest/merge/tag trên Iceberg',
    history: 'Toàn bộ nhật ký tải lên và trạng thái xử lý',
    users: 'Thêm, khoá và phân quyền tài khoản trong hệ thống',
  };

  return (
    <div className="flex h-screen overflow-hidden bg-canvas font-sans text-ink-900">
      {sidebarOpen && (
        <button
          type="button"
          aria-label="Đóng menu quản trị"
          onClick={() => setSidebarOpen(false)}
          className="fixed inset-0 z-30 bg-navy-950/45 md:hidden"
        />
      )}

      {/* Shared Admin Sidebar — desktop expanded, tablet icon rail, mobile drawer */}
      <aside
        className={`fixed inset-y-0 left-0 z-40 flex w-[260px] shrink-0 flex-col overflow-hidden border-r border-white/10 bg-navy-950 text-white shadow-[0_1px_8px_rgba(0,0,0,0.08)] transition-transform duration-300 md:static md:w-[72px] md:translate-x-0 lg:w-[260px] ${
          sidebarOpen ? 'translate-x-0' : '-translate-x-full'
        }`}
      >
        <div className="flex h-[72px] shrink-0 items-center border-b border-white/10 px-3 lg:px-4">
          <div className="flex min-w-0 items-center gap-3">
            <div className="flex h-10 w-12 shrink-0 items-center justify-center overflow-hidden rounded-lg bg-white shadow-sm">
              <img
                src="/CUSC Logo Series.png"
                alt="CUSC Logo"
                style={{
                  width: '46px',
                  height: '28px',
                  maxWidth: '46px',
                  maxHeight: '28px',
                  objectFit: 'contain',
                }}
              />
            </div>

            <div className="hidden min-w-0 lg:block">
              <h1 className="truncate font-display text-[12px] font-extrabold leading-tight tracking-[-0.01em] text-white">
                CUSC ANALYSIS PLATFORM
              </h1>
              <p className="mt-1 truncate text-[9px] font-medium text-navy-300">
                Trung tâm Công nghệ Thông tin - ĐHCT
              </p>
            </div>
          </div>

          <button
            type="button"
            aria-label="Đóng thanh điều hướng"
            onClick={() => setSidebarOpen(false)}
            className="ml-auto inline-flex h-9 w-9 items-center justify-center rounded-lg text-navy-300 transition hover:bg-white/10 hover:text-white md:hidden"
          >
            <X className="h-5 w-5" />
          </button>
        </div>

        <nav className="flex-1 overflow-y-auto px-2 py-5 lg:px-3">
          <p className="mb-2 hidden px-3 font-data text-[10px] font-semibold uppercase tracking-[0.14em] text-navy-400 lg:block">
            Quản trị hệ thống
          </p>

          <div className="space-y-1.5">
            {NAV.map(({ key, icon, label, sub }) => {
              const active = activeTab === key;
              return (
                <button
                  key={key}
                  type="button"
                  onClick={() => setActiveTab(key)}
                  aria-current={active ? 'page' : undefined}
                  title={label}
                  className={`group flex w-full items-center rounded-lg text-left transition-colors ${
                    active
                      ? 'bg-cobalt-600 text-white shadow-sm'
                      : 'text-navy-200 hover:bg-white/[0.07] hover:text-white'
                  } justify-center px-2 py-2.5 lg:justify-start lg:gap-3 lg:px-3`}
                >
                  <span
                    className={`flex h-9 w-9 shrink-0 items-center justify-center rounded-lg transition-colors ${
                      active
                        ? 'bg-white/15 text-white'
                        : 'text-navy-300 group-hover:bg-white/[0.06] group-hover:text-white'
                    }`}
                  >
                    <Icon name={icon} className="h-[18px] w-[18px]" />
                  </span>

                  <span className="hidden min-w-0 flex-1 lg:block">
                    <span className="block truncate text-sm font-semibold">
                      {label}
                    </span>
                    <span
                      className={`mt-0.5 block truncate font-data text-[10px] ${
                        active ? 'text-cobalt-100' : 'text-navy-400'
                      }`}
                    >
                      {sub}
                    </span>
                  </span>
                </button>
              );
            })}
          </div>
        </nav>

        <div className="shrink-0 border-t border-white/10 p-2 lg:p-3">
          <div className="flex items-center justify-center rounded-lg border border-white/[0.06] bg-white/[0.04] p-2.5 lg:justify-start lg:gap-3">
            <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-cobalt-600 text-sm font-bold text-white">
              {userInitial}
            </div>

            <div className="hidden min-w-0 flex-1 lg:block">
              <p className="truncate text-xs font-semibold text-white">
                {username}
              </p>
              <p className="mt-0.5 truncate font-data text-[9px] uppercase tracking-wide text-navy-400">
                {role}
              </p>
            </div>
          </div>

          <button
            type="button"
            onClick={handleLogout}
            title="Đăng xuất"
            className="mt-2 flex w-full items-center justify-center rounded-lg border border-transparent px-2 py-2.5 text-navy-300 transition hover:border-rose-400/20 hover:bg-rose-500/90 hover:text-white lg:gap-2 lg:px-3"
          >
            <Icon name="logOut" className="h-4 w-4 shrink-0" />
            <span className="hidden text-sm font-semibold lg:inline">Đăng xuất</span>
          </button>
        </div>
      </aside>

      {/* Shared Admin Workspace */}
      <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
        <header className="z-20 shrink-0 border-b border-line bg-white/95 shadow-[0_1px_8px_rgba(15,23,42,0.03)] backdrop-blur">
          <div className="flex min-h-16 items-center justify-between gap-3 px-4 sm:px-5 lg:px-6">
            <div className="flex min-w-0 items-center gap-3">
              <button
                type="button"
                aria-label="Mở menu quản trị"
                onClick={() => setSidebarOpen(true)}
                className="inline-flex h-10 w-10 shrink-0 items-center justify-center rounded-lg border border-line bg-white text-ink-600 shadow-sm transition hover:border-cobalt-200 hover:bg-cobalt-50 hover:text-cobalt-700 md:hidden"
              >
                <Menu className="h-5 w-5" />
              </button>

              <div className="min-w-0">
                <h2 className="truncate font-display text-sm font-bold tracking-tight text-navy-950 sm:text-[15px]">
                  {TAB_TITLES[activeTab]}
                </h2>
                <p className="mt-0.5 hidden truncate text-[11px] text-ink-500 sm:block">
                  {TAB_DESC[activeTab]}
                </p>
              </div>
            </div>

            <div className="flex shrink-0 items-center gap-2">
              <div className="hidden items-center gap-1.5 xl:flex">
                <ConnChip label="Nessie" />
                <ConnChip label="Trino" />
                <ConnChip label="MinIO" />
                <ConnChip label="Superset" />
              </div>

              {activeTab === 'dashboard' && (
                <a
                  href={supersetUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex h-9 shrink-0 items-center gap-1.5 rounded-control border border-cobalt-200 bg-white px-3 text-xs font-semibold text-cobalt-700 shadow-sm transition hover:bg-cobalt-50"
                >
                  <span className="hidden sm:inline">Mở toàn màn hình</span>
                  <Icon name="externalLink" className="h-3.5 w-3.5" />
                </a>
              )}
            </div>
          </div>

          <div className="flex gap-1.5 overflow-x-auto border-t border-line/80 px-4 py-2 xl:hidden">
            <ConnChip label="Nessie" />
            <ConnChip label="Trino" />
            <ConnChip label="MinIO" />
            <ConnChip label="Superset" />
          </div>
        </header>

        <main className="min-h-0 flex-1 overflow-hidden bg-canvas p-3 sm:p-4 lg:p-5">
          <div className="flex h-full min-w-0 flex-col overflow-hidden rounded-panel border border-line bg-white shadow-card">
            <div className={`min-h-0 flex-1 overflow-auto ${activeTab === 'dashboard' ? 'overflow-hidden' : ''}`}>
              {activeTab === 'dashboard' && (
                <iframe
                  src={supersetUrl}
                  title="Superset Dashboard"
                  className="h-full w-full border-0"
                />
              )}
              {activeTab === 'pipeline' && <PipelineDataExplorer />}
              {activeTab === 'catalog' && <CatalogHistoryTimeline />}
              {activeTab === 'history' && <UploadHistory />}
              {activeTab === 'users' && <UserManagement />}
            </div>
          </div>
        </main>
      </div>
    </div>
  );
};

export default AdminDashboard;
