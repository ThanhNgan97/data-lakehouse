import React from "react";
import SupersetDashboard from "../components/SupersetDashboard";
import { LayoutDashboard, ArrowLeft } from "lucide-react";
import { Link } from "react-router-dom";

export default function DashboardPage() {
  // Thay thế bằng ID thật của Dashboard trên Superset
  const dashboardId = "your-dashboard-uuid-here";

  return (
    <div className="min-h-screen bg-gray-50 flex flex-col">
      <header className="bg-white border-b px-6 py-4 flex items-center justify-between">
        <div className="flex items-center gap-3">
          <LayoutDashboard className="w-6 h-6 text-blue-600" />
          <h1 className="text-xl font-semibold text-gray-800">Hệ thống Báo cáo Quản trị (BI)</h1>
        </div>
        <Link 
          to="/admin" 
          className="flex items-center gap-2 text-sm text-gray-600 hover:text-blue-600 transition-colors"
        >
          <ArrowLeft className="w-4 h-4" />
          Quay lại Admin
        </Link>
      </header>
      <main className="flex-1 p-6">
        <div className="max-w-[1600px] mx-auto h-[85vh]">
          {/* Component nhúng Superset */}
          <SupersetDashboard dashboardId={dashboardId} />
        </div>
      </main>
    </div>
  );
}
