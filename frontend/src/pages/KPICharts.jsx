import React, { useEffect, useState } from 'react';
import axios from 'axios';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  LineChart, Line, Legend, Cell,
} from 'recharts';
import { 
  Building2, 
  Target, 
  Star, 
  BarChart3, 
  TrendingUp, 
  AlertTriangle, 
  Activity,
  Inbox
} from 'lucide-react';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

const authHeader = () => ({
  Authorization: `Bearer ${localStorage.getItem('token')}`,
});

const COLOR_PALETTE = ['#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6', '#EC4899', '#06B6D4', '#84CC16'];

// Custom tooltip cho BarChart
const CustomTooltip = ({ active, payload, label }) => {
  if (!active || !payload?.length) return null;
  return (
    <div className="bg-white border border-gray-200 shadow-lg rounded-lg p-3 text-xs">
      <p className="font-semibold text-gray-700 mb-1">{label}</p>
      {payload.map((p) => (
        <p key={p.dataKey} style={{ color: p.fill || p.color }}>
          {p.name}: <strong>{typeof p.value === 'number' ? p.value.toFixed(1) : p.value}</strong>
          {p.name.includes('%') || p.name.includes('hoàn thành') ? '%' : ''}
        </p>
      ))}
    </div>
  );
};

const KPICharts = () => {
  const [summaryData, setSummaryData] = useState([]);
  const [compareData, setCompareData] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [activeChart, setActiveChart] = useState('summary'); // 'summary' | 'compare'

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true);
      setError('');
      try {
        // Bảng tổng hợp: tỷ lệ hoàn thành theo đơn vị
        const summaryRes = await axios.get(
          `${API_URL}/pipeline/gold/preview?table=kpi_tong_hop_don_vi&limit=500`,
          { headers: authHeader() },
        );
        setSummaryData(summaryRes.data.rows || []);

        // Bảng so sánh kỳ
        try {
          const compareRes = await axios.get(
            `${API_URL}/pipeline/gold/preview?table=kpi_so_sanh_ky&limit=500`,
            { headers: authHeader() },
          );
          setCompareData(compareRes.data.rows || []);
        } catch {
          // Bảng so sánh kỳ có thể chưa có dữ liệu
        }
      } catch (e) {
        setError('Không thể tải dữ liệu biểu đồ. Có thể dữ liệu Gold chưa được xử lý.');
      } finally {
        setLoading(false);
      }
    };
    fetchData();
  }, []);

  const chartTabs = [
    { key: 'summary', label: 'Tỷ lệ Hoàn thành KPI', icon: <BarChart3 className="w-4 h-4 inline mr-2" />, desc: 'Tổng hợp theo đơn vị' },
    { key: 'compare', label: 'So sánh Tăng trưởng', icon: <TrendingUp className="w-4 h-4 inline mr-2" />, desc: 'So sánh kỳ này vs kỳ trước' },
  ];

  return (
    <div className="p-6 h-full overflow-y-auto bg-slate-50/50">
      {/* Header */}
      <div className="mb-8">
        <h3 className="text-xl font-bold text-slate-800 flex items-center gap-2 tracking-tight">
          <TrendingUp className="w-6 h-6 text-blue-600" /> Phân tích & Biểu đồ KPI
        </h3>
        <p className="text-sm text-slate-500 mt-1.5 font-medium">
          Trực quan hóa dữ liệu từ tầng Gold (Iceberg) qua Trino — không phụ thuộc Superset.
        </p>
      </div>

      {/* Chart Tabs */}
      <div className="flex gap-3 mb-8">
        {chartTabs.map((tab) => (
          <button
            key={tab.key}
            onClick={() => setActiveChart(tab.key)}
            className={`px-5 py-2.5 rounded-xl text-sm font-semibold transition-all duration-300 border ${
              activeChart === tab.key
                ? 'bg-blue-600 text-white border-blue-600 shadow-lg shadow-blue-600/20'
                : 'bg-white text-slate-600 border-slate-200 hover:bg-slate-50 hover:border-slate-300 shadow-sm'
            }`}
          >
            {tab.icon}
            {tab.label}
          </button>
        ))}
      </div>

      {error && (
        <div className="mb-6 p-4 bg-amber-50 border border-amber-200 text-amber-700 rounded-xl text-sm flex items-start gap-2">
          <AlertTriangle className="w-5 h-5 shrink-0" />
          <div>
            <p>{error}</p>
            <p className="mt-1 text-xs text-amber-500">
              Chạy pipeline Bronze → Silver → Gold để có dữ liệu hiển thị.
            </p>
          </div>
        </div>
      )}

      {loading ? (
        <div className="flex items-center justify-center h-64 text-gray-400 text-sm">
          <div className="text-center">
            <div className="mb-3 flex justify-center"><Activity className="w-10 h-10 animate-pulse text-blue-500" /></div>
            Đang tải dữ liệu biểu đồ...
          </div>
        </div>
      ) : (
        <>
          {/* Biểu đồ 1: Tỷ lệ hoàn thành KPI theo đơn vị */}
          {activeChart === 'summary' && (
            <div className="space-y-6">
              {summaryData.length === 0 ? (
                <EmptyState />
              ) : (
                <>
                  {/* Stat Cards */}
                  <div className="grid grid-cols-3 gap-4 mb-2">
                    <StatCard
                      label="Số đơn vị"
                      value={summaryData.length}
                      icon={<Building2 className="w-6 h-6" />}
                      color="blue"
                    />
                    <StatCard
                      label="TB hoàn thành"
                      value={`${(
                        summaryData.reduce((s, r) => s + parseFloat(r.ty_le_hoan_thanh_phan_tram || 0), 0) /
                        summaryData.length
                      ).toFixed(1)}%`}
                      icon={<Target className="w-6 h-6" />}
                      color="green"
                    />
                    <StatCard
                      label="Đơn vị xuất sắc (≥90%)"
                      value={summaryData.filter((r) => parseFloat(r.ty_le_hoan_thanh_phan_tram || 0) >= 90).length}
                      icon={<Star className="w-6 h-6" />}
                      color="yellow"
                    />
                  </div>

                  {/* Bar Chart */}
                  <div className="bg-white rounded-2xl border border-slate-100 shadow-xl shadow-slate-200/40 p-6">
                    <h4 className="text-[15px] font-bold text-slate-800 mb-6 tracking-tight">
                      Tỷ lệ hoàn thành KPI theo đơn vị (%)
                    </h4>
                    <ResponsiveContainer width="100%" height={320}>
                      <BarChart
                        data={summaryData.map((r) => ({
                          name: r.nhom_don_vi || r.ten_phong_ban || '?',
                          'Tỷ lệ hoàn thành (%)': parseFloat(r.ty_le_hoan_thanh_phan_tram || 0),
                          'Số đạt': parseInt(r.so_chi_tieu_dat || 0),
                          'Tổng chỉ tiêu': parseInt(r.tong_chi_tieu_danh_gia || 0),
                        }))}
                        margin={{ top: 5, right: 20, left: 0, bottom: 40 }}
                      >
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" vertical={false} />
                        <XAxis
                          dataKey="name"
                          tick={{ fontSize: 11, fill: '#64748b' }}
                          angle={-30}
                          textAnchor="end"
                          interval={0}
                          axisLine={false}
                          tickLine={false}
                        />
                        <YAxis domain={[0, 100]} tick={{ fontSize: 11, fill: '#64748b' }} unit="%" axisLine={false} tickLine={false} />
                        <Tooltip content={<CustomTooltip />} />
                        <Bar dataKey="Tỷ lệ hoàn thành (%)" radius={[4, 4, 0, 0]}>
                          {summaryData.map((_, i) => (
                            <Cell
                              key={i}
                              fill={
                                parseFloat(summaryData[i]?.ty_le_hoan_thanh_phan_tram || 0) >= 90
                                  ? '#10B981'
                                  : parseFloat(summaryData[i]?.ty_le_hoan_thanh_phan_tram || 0) >= 70
                                  ? '#3B82F6'
                                  : '#EF4444'
                              }
                            />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                    <div className="flex items-center gap-4 mt-3 text-xs text-gray-500 justify-center">
                      <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-sm bg-green-500 inline-block"></span> ≥ 90% (Xuất sắc)</span>
                      <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-sm bg-blue-500 inline-block"></span> 70–89% (Đạt)</span>
                      <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-sm bg-red-500 inline-block"></span> &lt; 70% (Cần cải thiện)</span>
                    </div>
                  </div>

                  {/* Ranking Table */}
                  <div className="bg-white rounded-2xl border border-slate-100 shadow-xl shadow-slate-200/40 overflow-hidden">
                    <div className="p-6 border-b border-slate-100">
                      <h4 className="text-[15px] font-bold text-slate-800 tracking-tight">Bảng xếp hạng đơn vị</h4>
                    </div>
                    <table className="w-full text-sm">
                      <thead className="bg-slate-50 border-b border-slate-100">
                        <tr className="text-slate-500">
                          <th className="text-left py-3 px-6 font-semibold">Xếp hạng</th>
                          <th className="text-left py-3 px-6 font-semibold">Đơn vị</th>
                          <th className="text-right py-3 px-6 font-semibold">Đạt / Tổng</th>
                          <th className="text-right py-3 px-6 font-semibold">Tỷ lệ</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-100">
                        {[...summaryData]
                          .sort((a, b) => parseFloat(b.ty_le_hoan_thanh_phan_tram || 0) - parseFloat(a.ty_le_hoan_thanh_phan_tram || 0))
                          .map((r, i) => {
                            const pct = parseFloat(r.ty_le_hoan_thanh_phan_tram || 0);
                            return (
                              <tr key={i} className="hover:bg-slate-50/80 transition-colors">
                                <td className="py-3 px-6 font-bold text-slate-400">
                                  {i === 0 ? <span className="text-yellow-500 font-black">#1</span> : i === 1 ? <span className="text-gray-400 font-black">#2</span> : i === 2 ? <span className="text-amber-600 font-black">#3</span> : `#${i + 1}`}
                                </td>
                                <td className="py-3 px-6 font-medium text-slate-700">{r.nhom_don_vi || r.ten_phong_ban}</td>
                                <td className="py-3 px-6 text-right text-slate-500 font-medium">
                                  {r.so_chi_tieu_dat} / {r.tong_chi_tieu_danh_gia}
                                </td>
                                <td className="py-3 px-6 text-right">
                                  <span className={`font-bold ${pct >= 90 ? 'text-green-600' : pct >= 70 ? 'text-blue-600' : 'text-red-500'}`}>
                                    {pct.toFixed(1)}%
                                  </span>
                                  <div className="w-full bg-slate-100 rounded-full h-1.5 mt-2">
                                    <div
                                      className={`h-1.5 rounded-full ${pct >= 90 ? 'bg-green-500' : pct >= 70 ? 'bg-blue-500' : 'bg-red-400'}`}
                                      style={{ width: `${Math.min(pct, 100)}%` }}
                                    />
                                  </div>
                                </td>
                              </tr>
                            );
                          })}
                      </tbody>
                    </table>
                  </div>
                </>
              )}
            </div>
          )}

          {/* Biểu đồ 2: So sánh tăng trưởng */}
          {activeChart === 'compare' && (
            <div>
              {compareData.length === 0 ? (
                <EmptyState message="Chưa có dữ liệu so sánh kỳ. Cần có ít nhất 2 kỳ đánh giá." />
              ) : (
                <div className="bg-white rounded-2xl border border-slate-100 shadow-xl shadow-slate-200/40 p-6">
                  <h4 className="text-[15px] font-bold text-slate-800 mb-6 tracking-tight">
                    Tăng trưởng KPI so với kỳ trước (%)
                  </h4>
                  <ResponsiveContainer width="100%" height={350}>
                    <BarChart
                      data={compareData.map((r) => ({
                        name: r.nhom_don_vi || r.ten_phong_ban || '?',
                        'Kỳ này': parseFloat(r.muc_dat_numeric || 0),
                        'Kỳ trước': parseFloat(r.muc_dat_numeric_ky_truoc || 0),
                        'Tăng trưởng (%)': parseFloat(r.tang_truong_phan_tram || 0),
                      }))}
                      margin={{ top: 5, right: 20, left: 0, bottom: 40 }}
                    >
                      <CartesianGrid strokeDasharray="3 3" stroke="#F3F4F6" />
                      <XAxis dataKey="name" tick={{ fontSize: 11 }} angle={-30} textAnchor="end" interval={0} />
                      <YAxis tick={{ fontSize: 11 }} />
                      <Tooltip content={<CustomTooltip />} />
                      <Legend />
                      <Bar dataKey="Kỳ này" fill="#3B82F6" radius={[3, 3, 0, 0]} />
                      <Bar dataKey="Kỳ trước" fill="#94A3B8" radius={[3, 3, 0, 0]} />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              )}
            </div>
          )}
        </>
      )}
    </div>
  );
};

const StatCard = ({ label, value, icon, color }) => {
  const colors = {
    blue: 'bg-gradient-to-br from-blue-50 to-indigo-50 border-blue-100 text-blue-700 shadow-blue-500/10',
    green: 'bg-gradient-to-br from-emerald-50 to-green-50 border-emerald-100 text-emerald-700 shadow-emerald-500/10',
    yellow: 'bg-gradient-to-br from-amber-50 to-orange-50 border-amber-100 text-amber-700 shadow-amber-500/10',
  };
  return (
    <div className={`rounded-2xl border p-5 shadow-lg ${colors[color]}`}>
      <div className="mb-2 opacity-80">{icon}</div>
      <div className="text-3xl font-black tracking-tight">{value}</div>
      <div className="text-sm font-medium opacity-80 mt-1">{label}</div>
    </div>
  );
};

const EmptyState = ({ message }) => (
  <div className="flex flex-col items-center justify-center h-64 text-gray-400 text-sm bg-white rounded-xl border border-dashed border-gray-200">
    <Inbox className="w-12 h-12 mb-3 text-gray-300" />
    <p className="font-medium">{message || 'Chưa có dữ liệu để hiển thị.'}</p>
    <p className="text-xs mt-1 text-gray-300">Chạy pipeline đầy đủ Bronze → Silver → Gold để xem biểu đồ.</p>
  </div>
);

export default KPICharts;
