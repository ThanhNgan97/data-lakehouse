import React, { useEffect, useState, useMemo } from 'react';
import axios from 'axios';
import Icon from '../components/icons';
import { Badge, EmptyState } from '../components/ui';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

/* Mỗi tầng dữ liệu dùng đúng tông kim loại theo tên gọi Medallion */
const LAYER_INFO = {
  bronze: { label: 'Bronze', desc: 'File Parquet thô (PDF/DOCX/PPT đã parse)', tone: 'bronze', ring: 'ring-bronze-300', dot: 'bg-bronze-500', text: 'text-bronze-700', bg: 'bg-bronze-100' },
  silver: { label: 'Silver', desc: 'Bảng Iceberg đã chuẩn hóa (kpi_cusc_master)', tone: 'silver', ring: 'ring-silver-300', dot: 'bg-silver-500', text: 'text-silver-700', bg: 'bg-silver-100' },
  gold: { label: 'Gold', desc: '4 Data Mart phục vụ báo cáo & dashboard', tone: 'gold', ring: 'ring-gold-300', dot: 'bg-gold-500', text: 'text-gold-700', bg: 'bg-gold-100' },
};
const Heardertable = {
  ma_chi_tieu: 'Mã chỉ tiêu',
  nhom_don_vi: 'Nhóm đơn vị',
  ten_phong_ban: 'Tên phòng ban',
  quy_danh_gia: 'Quy trình đánh giá',
  noi_dung_muc_tieu: 'Nội dung mục tiêu',
  dinh_ky_thu_thap: 'Định kỳ thu thập',
  muc_dang_ky: 'Mức đăng ký',
  muc_dat: 'Mức đạt',
  muc_dat_numeric: 'Mức đạt (số)',
  ket_qua_he_thong: 'Kết quả hệ thống',
  nguyen_nhan: 'Nguyên nhân',
  hanh_dong_khac_phuc: 'Hành động khắc phục',
  file_nguon: 'File nguồn',
  thoi_gian_dong_goi_gold: 'Thời gian đóng gói',
  tong_chi_tieu_danh_gia: 'Tổng chỉ tiêu đánh giá',
  so_chi_tieu_dat: 'Số chỉ tiêu đạt',
  so_chi_tieu_khong_dat: 'Số chỉ tiêu không đạt',
  ty_le_hoan_thanh_phan_tram: 'Tỷ lệ hoàn thành (%)',
  quy_danh_gia_ky_truoc: 'Kỳ trước',
  muc_dat_numeric_ky_truoc: 'Mức đạt kỳ trước',
  tang_truong_phan_tram: 'Tăng trưởng (%)',
  ky_dau_tien_xuat_hien: 'Kỳ đầu tiên xuất hiện',
  ky_gan_nhat_cap_nhat: 'Kỳ gần nhất cập nhật',
  quy_hien_tai: 'Kỳ hiện tại',
  muc_dat_hien_tai: 'Mức đạt hiện tại',
  quy_du_doan: 'Kỳ dự đoán',
  muc_dat_du_doan: 'Mức đạt dự đoán',
  avg_growth_pct: 'Tăng trưởng TB (%)',
  thoi_gian_du_doan: 'Thời gian dự đoán',
};

const GOLD_TABLE_LABELS = {
  kpi_tong_hop_don_vi: 'Tổng hợp theo đơn vị',
  kpi_chi_tiet_dashboard: 'Chi tiết đầy đủ',
  kpi_so_sanh_ky: 'So sánh giữa các kỳ',
  dm_chi_tieu: 'Chú thích / Data Dictionary',
  kpi_du_doan_tuong_lai: 'Dự đoán kết quả tương lai',
};

const DRILLABLE_SOURCE_TABLE = 'kpi_tong_hop_don_vi';
const DRILL_TARGET_TABLE = 'kpi_chi_tiet_dashboard';

const PAGE_SIZE = 20;

const exportCSV = (columns, rows, filename = 'export.csv') => {
  const header = columns.join(',');
  const body = rows.map((r) =>
    columns.map((c) => {
      let val = String(r[c] ?? '');
      if (/^[=+\-@]/.test(val)) val = "'" + val;
      return `"${val.replace(/"/g, '""')}"`;
    }).join(',')
  ).join('\n');
  const blob = new Blob([`\uFEFF${header}\n${body}`], { type: 'text/csv;charset=utf-8;' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a'); a.href = url; a.download = filename; a.click();
  URL.revokeObjectURL(url);
};

const PipelineDataExplorer = () => {
  const [status, setStatus] = useState({ bronze: false, silver: false, gold: false });
  const [activeLayer, setActiveLayer] = useState(null);
  const [activeGoldTable, setActiveGoldTable] = useState('kpi_chi_tiet_dashboard');
  const [previewData, setPreviewData] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');
  const [searchQuery, setSearchQuery] = useState('');
  const [page, setPage] = useState(1);
  const [selectedUnit, setSelectedUnit] = useState(null);

  const authHeader = { Authorization: `Bearer ${localStorage.getItem('token')}` };

  useEffect(() => {
    axios
      .get(`${API_URL}/pipeline/status`, { headers: authHeader })
      .then((res) => setStatus(res.data))
      .catch(() => setError('Không thể tải trạng thái pipeline.'));
  }, []);

  const openLayer = async (layer) => {
    setActiveLayer(layer);
    setError('');
    setPreviewData(null);
    setLoading(true);
    try {
      let url;
      if (layer === 'gold') {
        const params = new URLSearchParams({ table: activeGoldTable, limit: 50 });
        if (selectedUnit) params.set('nhom_don_vi', selectedUnit.code);
        url = `${API_URL}/pipeline/gold/preview?${params.toString()}`;
      } else {
        url = `${API_URL}/pipeline/${layer}/preview?limit=50`;
      }
      const res = await axios.get(url, { headers: authHeader });
      setPreviewData(res.data);
    } catch (e) {
      setError(`Không tải được dữ liệu tầng ${LAYER_INFO[layer].label}. Có thể tầng này chưa chạy.`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    if (activeLayer === 'gold') openLayer('gold');
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [activeGoldTable, selectedUnit]);

  const closeModal = () => {
    setActiveLayer(null);
    setPreviewData(null);
    setError('');
    setSelectedUnit(null);
    setSearchQuery('');
    setPage(1);
  };

  const filteredRows = useMemo(() => {
    if (!previewData?.rows) return [];
    if (!searchQuery.trim()) return previewData.rows;
    const q = searchQuery.toLowerCase();
    return previewData.rows.filter((row) =>
      Object.values(row).some((v) => String(v ?? '').toLowerCase().includes(q))
    );
  }, [previewData, searchQuery]);

  const totalPages = Math.max(1, Math.ceil(filteredRows.length / PAGE_SIZE));
  const pagedRows = filteredRows.slice((page - 1) * PAGE_SIZE, page * PAGE_SIZE);

  const handleRowDrillDown = (row) => {
    if (activeGoldTable !== DRILLABLE_SOURCE_TABLE) return;
    const code = row['nhom_don_vi'];
    if (!code) return;
    setSelectedUnit({ code, label: row['ten_phong_ban'] || code });
    setActiveGoldTable(DRILL_TARGET_TABLE);
  };

  const clearUnitFilter = () => setSelectedUnit(null);

  return (
    <div className="p-6 h-full overflow-y-auto bg-[#FAFBFD]">
      <div className="mb-6">
        <p className="font-data text-[11px] uppercase tracking-widest text-lake-600 font-semibold mb-1">
          Medallion Architecture
        </p>
        <h3 className="text-lg font-bold text-ink-900">Pipeline Dữ Liệu: Bronze → Silver → Gold</h3>
        <p className="text-sm text-ink-400 mt-1">
          Bấm vào từng tầng để xem dữ liệu thật đang có ở tầng đó, giống trạng thái chạy trên Airflow.
        </p>
      </div>

      {/* Sơ đồ pipeline */}
      <div className="flex items-center gap-3 mb-8">
        {['bronze', 'silver', 'gold'].map((layer, idx) => {
          const info = LAYER_INFO[layer];
          const isDone = status[layer];
          return (
            <React.Fragment key={layer}>
              <button
                onClick={() => openLayer(layer)}
                className={`relative flex-1 text-left border rounded-2xl p-5 transition group ${
                  isDone
                    ? `border-ink-100 bg-white hover:ring-2 ${info.ring} hover:shadow-card`
                    : 'border-dashed border-ink-200 bg-ink-50/50 opacity-70 hover:opacity-100'
                }`}
              >
                <div className="flex items-center gap-3 mb-2">
                  <span className={`w-9 h-9 rounded-xl flex items-center justify-center ${isDone ? info.bg : 'bg-ink-100'}`}>
                    <span className={`w-2.5 h-2.5 rounded-full ${isDone ? info.dot : 'bg-ink-300'}`} />
                  </span>
                  <div>
                    <p className={`font-bold ${isDone ? info.text : 'text-ink-400'}`}>{info.label}</p>
                    <p className="text-[11px] text-ink-400 font-data">{isDone ? 'Sẵn sàng' : 'Chưa có dữ liệu'}</p>
                  </div>
                </div>
                <p className="text-xs text-ink-400 leading-snug">{info.desc}</p>
                <p className="text-[11px] text-lake-600 font-semibold mt-3 flex items-center gap-1 group-hover:gap-1.5 transition-all">
                  Xem bảng dữ liệu <Icon name="chevronRight" className="w-3.5 h-3.5" />
                </p>
              </button>
              {idx < 2 && <Icon name="chevronRight" className="w-5 h-5 text-ink-200 shrink-0" />}
            </React.Fragment>
          );
        })}
      </div>

      {error && !activeLayer && (
        <div className="bg-rose-50 text-rose-600 border border-rose-200 p-3 rounded-xl mb-4 text-sm flex items-center gap-2">
          <Icon name="alertTriangle" className="w-4 h-4 shrink-0" /> {error}
        </div>
      )}

      {/* Modal xem dữ liệu */}
      {activeLayer && (
        <div className="fixed inset-0 bg-ink-900/50 backdrop-blur-sm flex items-center justify-center z-50 p-6">
          <div className="bg-white rounded-2xl shadow-popover w-full max-w-6xl max-h-[85vh] flex flex-col overflow-hidden">
            <div className="flex items-center justify-between px-6 py-4 border-b border-ink-100 bg-white">
              <div className="flex items-center gap-3">
                <span className={`w-8 h-8 rounded-lg flex items-center justify-center ${LAYER_INFO[activeLayer].bg}`}>
                  <span className={`w-2 h-2 rounded-full ${LAYER_INFO[activeLayer].dot}`} />
                </span>
                <div>
                  <h4 className="font-bold text-ink-900 text-sm">
                    Dữ liệu tầng {LAYER_INFO[activeLayer].label}
                    {previewData?.source_file && (
                      <span className="text-xs text-ink-400 font-normal font-data ml-2">({previewData.source_file})</span>
                    )}
                    {previewData?.source_table && (
                      <span className="text-xs text-ink-400 font-normal font-data ml-2">({previewData.source_table})</span>
                    )}
                  </h4>
                  {previewData && (
                    <p className="text-xs text-ink-400 mt-0.5 font-data">
                      Tổng {previewData.total_rows} dòng
                      {searchQuery && ` — lọc ra ${filteredRows.length} dòng`}
                      {` — trang ${page}/${totalPages}`}
                    </p>
                  )}
                </div>
              </div>
              <div className="flex items-center gap-2">
                {previewData && (
                  <div className="relative">
                    <Icon name="search" className="w-3.5 h-3.5 text-ink-300 absolute left-3 top-1/2 -translate-y-1/2" />
                    <input
                      type="text"
                      value={searchQuery}
                      onChange={(e) => { setSearchQuery(e.target.value); setPage(1); }}
                      placeholder="Tìm kiếm..."
                      className="border border-ink-100 bg-ink-50/60 rounded-lg pl-8 pr-3 py-1.5 text-xs focus:outline-none focus:ring-2 focus:ring-lake-300 focus:bg-white w-48 transition"
                    />
                  </div>
                )}
                {previewData?.rows?.length > 0 && (
                  <button
                    onClick={() => exportCSV(
                      previewData.columns,
                      filteredRows,
                      `${activeLayer}_${activeGoldTable || 'data'}.csv`
                    )}
                    className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-semibold bg-emerald-50 hover:bg-emerald-100 text-emerald-700 border border-emerald-200 rounded-lg transition"
                  >
                    <Icon name="download" className="w-3.5 h-3.5" />
                    Xuất CSV
                  </button>
                )}
                <button onClick={closeModal} className="w-8 h-8 flex items-center justify-center rounded-lg text-ink-400 hover:text-ink-700 hover:bg-ink-50 transition ml-1">
                  <Icon name="x" className="w-4 h-4" />
                </button>
              </div>
            </div>

            {activeLayer === 'gold' && (
              <div className="flex items-center flex-wrap gap-2 px-6 py-3 border-b border-ink-100 bg-white">
                {Object.entries(GOLD_TABLE_LABELS).map(([key, label]) => (
                  <button
                    key={key}
                    onClick={() => setActiveGoldTable(key)}
                    className={`text-xs h-9 px-3.5 rounded-full border font-semibold transition ${
                      activeGoldTable === key
                        ? 'bg-lake-600 text-white border-lake-600 shadow-sm'
                        : 'bg-ink-50 text-ink-500 border-ink-100 hover:bg-ink-100'
                    }`}
                  >
                    {label}
                  </button>
                ))}

                {selectedUnit && (
                  <span className="flex items-center gap-2 text-xs h-9 px-3 rounded-full border border-lake-200 bg-lake-50 text-lake-700 ml-2 font-semibold">
                    <Icon name="filter" className="w-3.5 h-3.5" />
                    Đang lọc: <strong>{selectedUnit.label}</strong>
                    <button
                      onClick={clearUnitFilter}
                      className="text-lake-500 hover:text-lake-800 ml-1"
                      title="Bỏ lọc đơn vị"
                    >
                      <Icon name="x" className="w-3.5 h-3.5" />
                    </button>
                  </span>
                )}
              </div>
            )}

            {activeLayer === 'gold' && activeGoldTable === DRILLABLE_SOURCE_TABLE && !selectedUnit && !loading && (
              <div className="px-6 py-2 text-xs text-lake-700 bg-lake-50/60 border-b border-ink-100 flex items-center gap-2">
                <Icon name="target" className="w-3.5 h-3.5" />
                Bấm vào 1 dòng bên dưới để xem chi tiết đầy đủ của đơn vị đó.
              </div>
            )}

            <div className="overflow-auto p-4 min-h-[60vh]">
              {loading && (
                <div className="flex items-center justify-center py-16 text-ink-400 text-sm gap-2">
                  <Icon name="refresh" className="w-4 h-4 animate-spin" /> Đang tải dữ liệu...
                </div>
              )}
              {error && !loading && (
                <div className="flex items-center justify-center py-16 text-rose-500 text-sm gap-2">
                  <Icon name="alertTriangle" className="w-4 h-4" /> {error}
                </div>
              )}

              {!loading && !error && previewData && (
                <>
                  <table className="w-full text-xs border-collapse">
                    <thead className="sticky top-0 bg-ink-50 z-10">
                      <tr>
                        <th className="text-left px-3 py-2.5 border-b border-ink-100 text-ink-500 font-data font-semibold whitespace-nowrap">STT</th>
                        {previewData.columns.map((c) => (
                          <th key={c} className="text-left px-3 py-2.5 border-b border-ink-100 text-ink-500 font-data font-semibold whitespace-nowrap">
                            {Heardertable[c] || c}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {pagedRows.map((row, i) => {
                        const isDrillable = activeLayer === 'gold' && activeGoldTable === DRILLABLE_SOURCE_TABLE;
                        const globalIdx = (page - 1) * PAGE_SIZE + i + 1;
                        return (
                          <tr
                            key={i}
                            onClick={() => handleRowDrillDown(row)}
                            className={`odd:bg-white even:bg-ink-50/40 ${isDrillable ? 'cursor-pointer hover:bg-lake-50' : ''}`}
                            title={isDrillable ? 'Click để xem chi tiết đơn vị này' : undefined}
                          >
                            <td className="px-3 py-2 border-b border-ink-50 whitespace-nowrap text-ink-300 font-data">{globalIdx}</td>
                            {previewData.columns.map((c) => (
                              <td key={c} className="px-3 py-2 border-b border-ink-50 whitespace-nowrap text-ink-700">
                                {String(row[c] ?? '')}
                              </td>
                            ))}
                          </tr>
                        );
                      })}
                      {pagedRows.length === 0 && (
                        <tr><td colSpan={previewData.columns.length + 1} className="text-center py-10 text-ink-300">Không có kết quả phù hợp.</td></tr>
                      )}
                    </tbody>
                  </table>
                  {totalPages > 1 && (
                    <div className="flex items-center justify-between px-4 py-3 border-t border-ink-100 bg-ink-50/40 text-xs text-ink-400 font-data mt-2 rounded-b-xl">
                      <span>{filteredRows.length} dòng · trang {page}/{totalPages}</span>
                      <div className="flex gap-1">
                        <button onClick={() => setPage(1)} disabled={page === 1} className="px-2 py-1 rounded-md border border-ink-100 disabled:opacity-30 hover:bg-white">«</button>
                        <button onClick={() => setPage((p) => p - 1)} disabled={page === 1} className="px-2 py-1 rounded-md border border-ink-100 disabled:opacity-30 hover:bg-white">‹</button>
                        {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                          const p = Math.max(1, Math.min(page - 2, totalPages - 4)) + i;
                          return p <= totalPages ? (
                            <button key={p} onClick={() => setPage(p)} className={`px-2.5 py-1 rounded-md border ${p === page ? 'bg-lake-600 text-white border-lake-600' : 'border-ink-100 hover:bg-white'}`}>{p}</button>
                          ) : null;
                        })}
                        <button onClick={() => setPage((p) => p + 1)} disabled={page === totalPages} className="px-2 py-1 rounded-md border border-ink-100 disabled:opacity-30 hover:bg-white">›</button>
                        <button onClick={() => setPage(totalPages)} disabled={page === totalPages} className="px-2 py-1 rounded-md border border-ink-100 disabled:opacity-30 hover:bg-white">»</button>
                      </div>
                    </div>
                  )}
                </>
              )}

              {!loading && !error && previewData && previewData.rows.length === 0 && selectedUnit && (
                <EmptyState
                  icon="filter"
                  title={`Không có dữ liệu cho đơn vị "${selectedUnit.label}" ở bảng này`}
                  action={
                    <button onClick={clearUnitFilter} className="text-lake-600 hover:text-lake-800 text-xs font-semibold underline underline-offset-2">
                      Bỏ lọc
                    </button>
                  }
                />
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default PipelineDataExplorer;
