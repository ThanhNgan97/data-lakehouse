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
  muc_dat_numberic: 'Mức đạt (số)',
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
    columns.map((c) => `"${String(r[c] ?? '').replace(/"/g, '""')}"`).join(',')
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
    <div className="h-full overflow-y-auto bg-canvas px-4 py-5 sm:px-6 sm:py-6 lg:px-8 lg:py-8">
      <div className="mx-auto w-full max-w-[1480px]">
        <div className="mb-7">
          <p className="mb-1.5 font-data text-[10px] font-bold uppercase tracking-[0.12em] text-cobalt-700">
            Medallion Architecture
          </p>
          <h3 className="font-display text-[28px] font-extrabold tracking-[-0.035em] text-navy-950 sm:text-[30px]">
            Pipeline Dữ liệu: Bronze → Silver → Gold
          </h3>
          <p className="mt-1.5 max-w-3xl text-sm leading-6 text-ink-500">
            Bấm vào từng tầng để xem dữ liệu thật đang có ở tầng đó, giống trạng thái chạy trên Airflow.
          </p>
        </div>

        {/* Sơ đồ pipeline */}
        <section className="mb-8 rounded-2xl border border-line bg-white px-5 py-5 shadow-card sm:px-6 sm:py-6 lg:px-7 lg:py-7">
          <div className="flex flex-col gap-4 border-b border-ink-100 pb-5 sm:flex-row sm:items-center sm:justify-between">
            <div className="flex items-center gap-3">
              <span className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl border border-cobalt-100 bg-cobalt-50 text-cobalt-700">
                <Icon name="layers" className="h-4.5 w-4.5" />
              </span>
              <div>
                <h4 className="font-display text-[15px] font-bold tracking-tight text-navy-950 sm:text-base">
                  Quy trình Chuyển giao &amp; Chế tác Dữ liệu
                </h4>
                <p className="mt-0.5 text-xs text-ink-400">
                  Theo dõi trạng thái và mở dữ liệu thực tế ở từng tầng.
                </p>
              </div>
            </div>

            <div className="flex flex-wrap items-center gap-x-4 gap-y-2 font-data text-[10px] text-ink-500 sm:text-[11px]">
              <span className="inline-flex items-center gap-1.5">
                <span className="h-2 w-2 rounded-full bg-ink-200" />
                Chưa có dữ liệu
              </span>
              <span className="inline-flex items-center gap-1.5">
                <span className="h-2 w-2 rounded-full bg-emerald-500" />
                Sẵn sàng
              </span>
            </div>
          </div>

          <div className="mt-6 grid grid-cols-1 items-stretch gap-0 lg:grid-cols-3 lg:gap-6">
            {['bronze', 'silver', 'gold'].map((layer, idx) => {
              const info = LAYER_INFO[layer];
              const isDone = status[layer];

              const accentClass = {
                bronze: 'bg-bronze-500',
                silver: 'bg-silver-500',
                gold: 'bg-gold-500',
              }[layer];

              const iconShellClass = {
                bronze: 'border-bronze-300 bg-bronze-100 text-bronze-700',
                silver: 'border-silver-300 bg-silver-100 text-silver-700',
                gold: 'border-gold-300 bg-gold-100 text-gold-700',
              }[layer];

              const hoverBorderClass = {
                bronze: 'hover:border-bronze-300',
                silver: 'hover:border-silver-300',
                gold: 'hover:border-gold-300',
              }[layer];

              const iconName = {
                bronze: 'layers',
                silver: 'filter',
                gold: 'grid',
              }[layer];

              const stageNumber = String(idx + 1).padStart(2, '0');

              return (
                <React.Fragment key={layer}>
                  <button
                    onClick={() => openLayer(layer)}
                    className={`group relative flex min-h-[286px] flex-col overflow-hidden rounded-2xl border border-ink-100 bg-white text-left shadow-sm transition-all duration-200 hover:-translate-y-0.5 hover:shadow-card ${hoverBorderClass}`}
                  >
                    <span className={`absolute inset-x-0 top-0 h-[3px] ${accentClass}`} />

                    <div className="flex flex-1 flex-col px-5 pb-0 pt-6 sm:px-6">
                      <div className="mb-5 flex items-start justify-between gap-3">
                        <span className={`flex h-11 w-11 shrink-0 items-center justify-center rounded-xl border ${iconShellClass}`}>
                          <Icon name={iconName} className="h-5 w-5" />
                        </span>

                        <Badge
                          tone={isDone ? 'success' : 'ink'}
                          dot={isDone}
                          className="rounded-full px-2.5 py-0.5 text-[10px] normal-case tracking-normal"
                        >
                          {isDone ? 'Sẵn sàng' : 'Chưa có dữ liệu'}
                        </Badge>
                      </div>

                      <div>
                        <p className="mb-1.5 font-data text-[10px] font-bold uppercase tracking-[0.12em] text-ink-400">
                          Tầng {stageNumber}
                        </p>
                        <p className="font-display text-[27px] font-extrabold tracking-[-0.035em] text-navy-950">
                          {info.label}
                        </p>
                        <p className="mt-2 min-h-[44px] text-sm leading-6 text-ink-500">
                          {info.desc}
                        </p>
                      </div>

                      <div className="mt-auto border-t border-ink-100 py-4">
                        <span className="inline-flex items-center gap-1.5 text-sm font-semibold text-cobalt-700">
                          Xem bảng dữ liệu
                          <Icon
                            name="chevronRight"
                            className="h-3.5 w-3.5 transition-transform duration-200 group-hover:translate-x-0.5"
                          />
                        </span>
                      </div>
                    </div>

                    {idx < 2 && (
                      <span className="pointer-events-none absolute -right-6 top-1/2 z-20 hidden w-6 -translate-y-1/2 items-center lg:flex">
                        <span className="h-px flex-1 bg-ink-200" />
                        <Icon name="chevronRight" className="-ml-0.5 h-3.5 w-3.5 shrink-0 text-ink-300" />
                      </span>
                    )}
                  </button>

                  {idx < 2 && (
                    <div className="flex h-8 items-center justify-center lg:hidden">
                      <span className="h-5 w-px bg-ink-200" />
                      <Icon name="chevronRight" className="-ml-2 h-3.5 w-3.5 rotate-90 text-ink-300" />
                    </div>
                  )}
                </React.Fragment>
              );
            })}
          </div>
        </section>

        {error && !activeLayer && (
          <div className="mb-4 flex items-start gap-2.5 rounded-panel border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700 shadow-sm">
            <Icon name="alertTriangle" className="mt-0.5 h-4 w-4 shrink-0" />
            <span>{error}</span>
          </div>
        )}

        {/* Modal xem dữ liệu */}
        {activeLayer && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-ink-900/50 p-3 backdrop-blur-sm sm:p-6">
            <div className="flex max-h-[92vh] w-full max-w-6xl flex-col overflow-hidden rounded-panel border border-white/70 bg-white shadow-popover sm:max-h-[85vh]">
              <div className="flex flex-col gap-4 border-b border-line bg-white px-4 py-4 sm:px-6 lg:flex-row lg:items-center lg:justify-between">
                <div className="flex min-w-0 items-start gap-3">
                  <span
                    className={`flex h-9 w-9 shrink-0 items-center justify-center rounded-lg ${
                      LAYER_INFO[activeLayer].bg
                    }`}
                  >
                    <span className={`h-2 w-2 rounded-full ${LAYER_INFO[activeLayer].dot}`} />
                  </span>

                  <div className="min-w-0">
                    <h4 className="text-sm font-bold text-ink-900">
                      Dữ liệu tầng {LAYER_INFO[activeLayer].label}
                      {previewData?.source_file && (
                        <span className="ml-2 font-data text-xs font-normal text-ink-400">
                          ({previewData.source_file})
                        </span>
                      )}
                      {previewData?.source_table && (
                        <span className="ml-2 font-data text-xs font-normal text-ink-400">
                          ({previewData.source_table})
                        </span>
                      )}
                    </h4>

                    {previewData && (
                      <p className="mt-1 font-data text-[11px] text-ink-400">
                        Tổng {previewData.total_rows} dòng
                        {searchQuery && ` — lọc ra ${filteredRows.length} dòng`}
                        {` — trang ${page}/${totalPages}`}
                      </p>
                    )}
                  </div>
                </div>

                <div className="flex w-full flex-wrap items-center gap-2 lg:w-auto lg:flex-nowrap">
                  {previewData && (
                    <div className="relative min-w-[180px] flex-1 lg:w-48 lg:flex-none">
                      <Icon
                        name="search"
                        className="absolute left-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-ink-300"
                      />
                      <input
                        type="text"
                        value={searchQuery}
                        onChange={(e) => { setSearchQuery(e.target.value); setPage(1); }}
                        placeholder="Tìm kiếm..."
                        className="h-9 w-full rounded-control border border-line bg-ink-50/70 pl-8 pr-3 text-xs text-ink-700 outline-none transition focus:border-cobalt-300 focus:bg-white focus:ring-2 focus:ring-cobalt-500/15"
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
                      className="inline-flex h-9 items-center gap-1.5 rounded-control border border-emerald-200 bg-emerald-50 px-3 text-xs font-semibold text-emerald-700 transition hover:bg-emerald-100"
                    >
                      <Icon name="download" className="h-3.5 w-3.5" />
                      Xuất CSV
                    </button>
                  )}

                  <button
                    onClick={closeModal}
                    className="ml-auto flex h-9 w-9 shrink-0 items-center justify-center rounded-control border border-transparent text-ink-400 transition hover:border-line hover:bg-ink-50 hover:text-ink-700 lg:ml-0"
                  >
                    <Icon name="x" className="h-4 w-4" />
                  </button>
                </div>
              </div>

              {activeLayer === 'gold' && (
                <div className="flex gap-2 overflow-x-auto border-b border-line bg-ink-50/45 px-4 py-3 sm:px-6">
                  {Object.entries(GOLD_TABLE_LABELS).map(([key, label]) => (
                    <button
                      key={key}
                      onClick={() => setActiveGoldTable(key)}
                      className={`h-8 shrink-0 rounded-full border px-3 text-xs font-semibold transition ${
                        activeGoldTable === key
                          ? 'border-cobalt-600 bg-cobalt-600 text-white shadow-sm'
                          : 'border-line bg-white text-ink-500 hover:border-cobalt-200 hover:text-cobalt-700'
                      }`}
                    >
                      {label}
                    </button>
                  ))}

                  {selectedUnit && (
                    <span className="ml-1 flex h-8 shrink-0 items-center gap-2 rounded-full border border-cobalt-200 bg-cobalt-50 px-3 text-xs font-semibold text-cobalt-700">
                      <Icon name="filter" className="h-3.5 w-3.5" />
                      Đang lọc: <strong>{selectedUnit.label}</strong>
                      <button
                        onClick={clearUnitFilter}
                        className="ml-1 text-cobalt-500 hover:text-cobalt-800"
                        title="Bỏ lọc đơn vị"
                      >
                        <Icon name="x" className="h-3.5 w-3.5" />
                      </button>
                    </span>
                  )}
                </div>
              )}

              {activeLayer === 'gold' && activeGoldTable === DRILLABLE_SOURCE_TABLE && !selectedUnit && !loading && (
                <div className="flex items-center gap-2 border-b border-line bg-cobalt-50/55 px-4 py-2 text-xs text-cobalt-700 sm:px-6">
                  <Icon name="target" className="h-3.5 w-3.5 shrink-0" />
                  Bấm vào 1 dòng bên dưới để xem chi tiết đầy đủ của đơn vị đó.
                </div>
              )}

              <div className="min-h-[60vh] overflow-auto bg-white p-3 sm:p-4">
                {loading && (
                  <div className="flex items-center justify-center gap-2 py-16 text-sm text-ink-400">
                    <Icon name="refresh" className="h-4 w-4 animate-spin text-cobalt-600" />
                    Đang tải dữ liệu...
                  </div>
                )}

                {error && !loading && (
                  <div className="flex items-center justify-center gap-2 py-16 text-sm text-rose-600">
                    <Icon name="alertTriangle" className="h-4 w-4" />
                    {error}
                  </div>
                )}

                {!loading && !error && previewData && (
                  <>
                    <div className="overflow-x-auto rounded-panel border border-line">
                      <table className="w-full min-w-max border-collapse text-xs">
                        <thead className="sticky top-0 z-10 bg-ink-50">
                          <tr>
                            <th className="whitespace-nowrap border-b border-line px-3 py-3 text-left font-data font-semibold text-ink-500">
                              STT
                            </th>
                            {previewData.columns.map((c) => (
                              <th
                                key={c}
                                className="whitespace-nowrap border-b border-line px-3 py-3 text-left font-data font-semibold text-ink-500"
                              >
                                {Heardertable[c] || c}
                              </th>
                            ))}
                          </tr>
                        </thead>

                        <tbody className="divide-y divide-ink-100">
                          {pagedRows.map((row, i) => {
                            const isDrillable = activeLayer === 'gold' && activeGoldTable === DRILLABLE_SOURCE_TABLE;
                            const globalIdx = (page - 1) * PAGE_SIZE + i + 1;

                            return (
                              <tr
                                key={i}
                                onClick={() => handleRowDrillDown(row)}
                                className={`bg-white transition-colors ${
                                  isDrillable ? 'cursor-pointer hover:bg-cobalt-50/45' : 'hover:bg-ink-50/50'
                                }`}
                                title={isDrillable ? 'Click để xem chi tiết đơn vị này' : undefined}
                              >
                                <td className="whitespace-nowrap px-3 py-2.5 font-data text-ink-300">
                                  {globalIdx}
                                </td>
                                {previewData.columns.map((c) => (
                                  <td
                                    key={c}
                                    className="whitespace-nowrap px-3 py-2.5 text-ink-700"
                                  >
                                    {String(row[c] ?? '')}
                                  </td>
                                ))}
                              </tr>
                            );
                          })}

                          {pagedRows.length === 0 && (
                            <tr>
                              <td
                                colSpan={previewData.columns.length + 1}
                                className="py-10 text-center text-ink-300"
                              >
                                Không có kết quả phù hợp.
                              </td>
                            </tr>
                          )}
                        </tbody>
                      </table>
                    </div>

                    {totalPages > 1 && (
                      <div className="mt-3 flex flex-col gap-3 rounded-panel border border-line bg-ink-50/45 px-3 py-3 font-data text-xs text-ink-400 sm:flex-row sm:items-center sm:justify-between">
                        <span>{filteredRows.length} dòng · trang {page}/{totalPages}</span>

                        <div className="flex max-w-full gap-1 overflow-x-auto">
                          <button
                            onClick={() => setPage(1)}
                            disabled={page === 1}
                            className="rounded-md border border-line bg-white px-2 py-1 disabled:opacity-30"
                          >
                            «
                          </button>
                          <button
                            onClick={() => setPage((p) => p - 1)}
                            disabled={page === 1}
                            className="rounded-md border border-line bg-white px-2 py-1 disabled:opacity-30"
                          >
                            ‹
                          </button>

                          {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                            const p = Math.max(1, Math.min(page - 2, totalPages - 4)) + i;

                            return p <= totalPages ? (
                              <button
                                key={p}
                                onClick={() => setPage(p)}
                                className={`rounded-md border px-2.5 py-1 ${
                                  p === page
                                    ? 'border-cobalt-600 bg-cobalt-600 text-white'
                                    : 'border-line bg-white hover:border-cobalt-200 hover:text-cobalt-700'
                                }`}
                              >
                                {p}
                              </button>
                            ) : null;
                          })}

                          <button
                            onClick={() => setPage((p) => p + 1)}
                            disabled={page === totalPages}
                            className="rounded-md border border-line bg-white px-2 py-1 disabled:opacity-30"
                          >
                            ›
                          </button>
                          <button
                            onClick={() => setPage(totalPages)}
                            disabled={page === totalPages}
                            className="rounded-md border border-line bg-white px-2 py-1 disabled:opacity-30"
                          >
                            »
                          </button>
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
                      <button
                        onClick={clearUnitFilter}
                        className="text-xs font-semibold text-cobalt-600 underline underline-offset-2 hover:text-cobalt-800"
                      >
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
    </div>
  );
};

export default PipelineDataExplorer;
