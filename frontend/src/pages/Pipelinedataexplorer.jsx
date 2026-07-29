import React, { useEffect, useState, useMemo } from 'react';
import axios from 'axios';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

const LAYER_INFO = {
  bronze: { label: 'Bronze', desc: 'File Parquet thô (PDF/DOCX đã parse)', color: '#B45309' },
  silver: { label: 'Silver', desc: 'Bảng Iceberg đã chuẩn hóa (kpi_cusc_master)', color: '#94A3B8' },
  gold:   { label: 'Gold',   desc: '4 Data Mart phục vụ báo cáo & dashboard', color: '#CA8A04' },
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
    // [MỚI] cột phục vụ bảng tổng hợp / so sánh kỳ
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
}

const GOLD_TABLE_LABELS = {
  kpi_tong_hop_don_vi: 'Tổng hợp theo đơn vị',
  kpi_chi_tiet_dashboard: 'Chi tiết đầy đủ',
  kpi_so_sanh_ky: 'So sánh giữa các kỳ',
  dm_chi_tieu: 'Chú thích / Data Dictionary',
  kpi_du_doan_tuong_lai: 'Dự đoán kết quả tương lai',
};

// [MỚI] Bảng gốc dùng để click-to-drill: chỉ bảng tổng hợp mới có nghĩa để
// bấm vào 1 dòng rồi "đi xuống" chi tiết của đúng đơn vị đó.
const DRILLABLE_SOURCE_TABLE = 'kpi_tong_hop_don_vi';
// Khi drill xuống, mặc định nhảy sang bảng chi tiết đầy đủ.
const DRILL_TARGET_TABLE = 'kpi_chi_tiet_dashboard';

const PAGE_SIZE = 20;

// CSV export helper
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
  // Search + Pagination
  const [searchQuery, setSearchQuery] = useState('');
  const [page, setPage] = useState(1);

  // [MỚI] Đơn vị đang được lọc (drill-down từ tổng -> chi tiết).
  // Lưu cả mã (để gọi API) lẫn tên đầy đủ (để hiển thị badge cho dễ đọc).
  const [selectedUnit, setSelectedUnit] = useState(null); // { code, label } | null

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
        // [MỚI] Gắn thêm filter đơn vị nếu đang có, áp dụng cho cả 4 bảng Gold
        // vì bảng nào cũng có cột nhom_don_vi.
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

  // Khi đổi bảng Gold, hoặc đổi đơn vị đang lọc, trong lúc đang xem Gold -> tự tải lại
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

  // Rows sau khi filter search
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

  // [MỚI] Xử lý click vào 1 dòng của bảng tổng hợp -> drill xuống bảng chi tiết
  // đã lọc đúng đơn vị vừa click.
  const handleRowDrillDown = (row) => {
    if (activeGoldTable !== DRILLABLE_SOURCE_TABLE) return;
    const code = row['nhom_don_vi'];
    if (!code) return;
    setSelectedUnit({ code, label: row['ten_phong_ban'] || code });
    setActiveGoldTable(DRILL_TARGET_TABLE);
  };

  const clearUnitFilter = () => setSelectedUnit(null);

  const nodeClass = (layer) => {
    const isDone = status[layer];
    return `relative flex-1 border-2 rounded-xl p-5 text-center transition cursor-pointer ${
      isDone
        ? 'border-green-500 bg-green-50 hover:bg-green-100'
        : 'border-gray-300 bg-gray-50 hover:bg-gray-100 opacity-70'
    }`;
  };

  return (
    <div className="p-6 h-full overflow-y-auto">
      <div className="mb-6">
        <h3 className="text-lg font-semibold text-gray-800">Pipeline Dữ Liệu: Bronze → Silver → Gold</h3>
        <p className="text-sm text-gray-400 mt-1">
          Click vào từng tầng để xem bảng dữ liệu thật đang có ở tầng đó (giống trạng thái chạy trên Airflow).
        </p>
      </div>

      {/* Sơ đồ pipeline dạng node nối tiếp */}
      <div className="flex items-center gap-3 mb-8">
        {['bronze', 'silver', 'gold'].map((layer, idx) => (
          <React.Fragment key={layer}>
            <div className={nodeClass(layer)} onClick={() => openLayer(layer)}>
              <div className="flex items-center justify-center gap-2 mb-1">
                <span
                  className={`w-2.5 h-2.5 rounded-full ${status[layer] ? 'bg-green-500' : 'bg-gray-400'}`}
                />
                <span className="font-semibold text-gray-800">{LAYER_INFO[layer].label}</span>
              </div>
              <p className="text-xs text-gray-500">{LAYER_INFO[layer].desc}</p>
              <p className="text-[11px] text-blue-600 mt-2 underline">Xem bảng dữ liệu →</p>
            </div>
            {idx < 2 && <div className="text-gray-300 text-2xl">➜</div>}
          </React.Fragment>
        ))}
      </div>

      {error && !activeLayer && (
        <div className="bg-red-50 text-red-600 border border-red-200 p-3 rounded-lg mb-4 text-sm">{error}</div>
      )}

      {/* Modal xem dữ liệu */}
      {activeLayer && (
        <div className="fixed inset-0 bg-black/40 flex items-center justify-center z-50 p-6">
          <div className="bg-white rounded-xl shadow-2xl w-full max-w-6xl max-h-[85vh] flex flex-col overflow-hidden">
            <div className="flex items-center justify-between px-6 py-4 border-b bg-gray-50">
              <div>
                <h4 className="font-semibold text-gray-800">
                  Dữ liệu tầng {LAYER_INFO[activeLayer].label}
                  {previewData?.source_file && (
                    <span className="text-xs text-gray-400 font-normal ml-2">({previewData.source_file})</span>
                  )}
                  {previewData?.source_table && (
                    <span className="text-xs text-gray-400 font-normal ml-2">({previewData.source_table})</span>
                  )}
                </h4>
                {previewData && (
                  <p className="text-xs text-gray-400 mt-0.5">
                    Tổng {previewData.total_rows} dòng
                    {searchQuery && ` — lọc ra ${filteredRows.length} dòng`}
                    {` — trang ${page}/${totalPages}`}
                  </p>
                )}
              </div>
              <div className="flex items-center gap-2">
                {/* Search */}
                {previewData && (
                  <input
                    type="text"
                    value={searchQuery}
                    onChange={(e) => { setSearchQuery(e.target.value); setPage(1); }}
                    placeholder="🔍 Tìm kiếm..."
                    className="border border-gray-200 rounded-lg px-3 py-1.5 text-xs focus:outline-none focus:ring-2 focus:ring-blue-400 w-48"
                  />
                )}
                {/* Export CSV */}
                {previewData?.rows?.length > 0 && (
                  <button
                    onClick={() => exportCSV(
                      previewData.columns,
                      filteredRows,
                      `${activeLayer}_${activeGoldTable || 'data'}.csv`
                    )}
                    className="px-3 py-1.5 text-xs bg-green-50 hover:bg-green-100 text-green-700 border border-green-200 rounded-lg transition"
                  >
                     Xuất CSV
                  </button>
                )}
                <button onClick={closeModal} className="text-gray-400 hover:text-gray-700 text-xl leading-none ml-2">
                  ✕
                </button>
              </div>
            </div>

            {/* Chọn bảng khi đang xem Gold (Gold có 4 bảng) */}
            {activeLayer === 'gold' && (
              <div className="flex items-center flex-wrap gap-2 px-6 py-3 border-b bg-white">
                {Object.entries(GOLD_TABLE_LABELS).map(([key, label]) => (
                  <button
                    key={key}
                    onClick={() => setActiveGoldTable(key)}
                    className={`text-xs  h-[40px] px-3 py-2 rounded-full border transition ${
                      activeGoldTable === key
                        ? 'bg-blue-600 text-white border-blue-600'
                        : 'bg-gray-100 text-gray-600 border-gray-200 hover:bg-gray-200'
                    }`}
                  >
                    {label}
                  </button>
                ))}

                {/* [MỚI] Badge hiển thị đơn vị đang lọc (kết quả drill-down) + nút bỏ lọc */}
                {selectedUnit && (
                  <span className="flex items-center gap-2 text-xs h-[40px] px-3 py-2 rounded-full border border-blue-300 bg-blue-50 text-blue-700 ml-2">
                    🔎 Đang lọc theo đơn vị: <strong>{selectedUnit.label}</strong>
                    <button
                      onClick={clearUnitFilter}
                      className="text-blue-500 hover:text-blue-800 font-bold leading-none ml-1"
                      title="Bỏ lọc đơn vị"
                    >
                      ✕
                    </button>
                  </span>
                )}
              </div>
            )}

            {/* [MỚI] Gợi ý drill-down khi đang ở bảng tổng hợp và chưa lọc gì */}
            {activeLayer === 'gold' && activeGoldTable === DRILLABLE_SOURCE_TABLE && !selectedUnit && !loading && (
              <div className="px-6 py-2 text-xs text-gray-400 bg-white border-b">
                💡 Click vào 1 dòng bên dưới để xem chi tiết đầy đủ của đơn vị đó.
              </div>
            )}

            <div className="overflow-auto p-4 min-h-[60vh]" >
              {loading && <p className="text-gray-400 text-sm text-center flex items-center justify-center py-10">Đang tải dữ liệu...</p>}
              {error && !loading && <p className="text-red-500 text-sm text-center py-10">{error}</p>}

              {!loading && !error && previewData && (
                <>
                  <table className="w-full text-xs border-collapse">
                    <thead className="sticky top-0 bg-gray-100 z-10">
                      <tr className="border-b border-gray-200">
                        <th className="text-left px-3 py-2 border-b text-slate-700 font-semibold whitespace-nowrap">STT</th>
                        {previewData.columns.map((c) => (
                          <th key={c} className="text-left px-3 py-2 border-b text-slate-700 font-semibold whitespace-nowrap">
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
                            className={`odd:bg-white even:bg-gray-50 ${isDrillable ? 'cursor-pointer hover:bg-blue-50' : ''}`}
                            title={isDrillable ? 'Click để xem chi tiết đơn vị này' : undefined}
                          >
                            <td className="px-3 py-1.5 border-b border-gray-100 whitespace-nowrap text-gray-400">{globalIdx}</td>
                            {previewData.columns.map((c) => (
                              <td key={c} className="px-3 py-1.5 border-b border-gray-100 whitespace-nowrap">
                                {String(row[c] ?? '')}
                              </td>
                            ))}
                          </tr>
                        );
                      })}
                      {pagedRows.length === 0 && (
                        <tr><td colSpan={previewData.columns.length + 1} className="text-center py-8 text-gray-400">Không có kết quả phù hợp.</td></tr>
                      )}
                    </tbody>
                  </table>
                  {/* Pagination */}
                  {totalPages > 1 && (
                    <div className="flex items-center justify-between px-4 py-2 border-t bg-gray-50 text-xs text-gray-500">
                      <span>{filteredRows.length} dòng · trang {page}/{totalPages}</span>
                      <div className="flex gap-1">
                        <button onClick={() => setPage(1)} disabled={page === 1} className="px-2 py-1 rounded border disabled:opacity-40 hover:bg-gray-100">«</button>
                        <button onClick={() => setPage((p) => p - 1)} disabled={page === 1} className="px-2 py-1 rounded border disabled:opacity-40 hover:bg-gray-100">‹</button>
                        {Array.from({ length: Math.min(5, totalPages) }, (_, i) => {
                          const p = Math.max(1, Math.min(page - 2, totalPages - 4)) + i;
                          return p <= totalPages ? (
                            <button key={p} onClick={() => setPage(p)} className={`px-2 py-1 rounded border ${p === page ? 'bg-blue-600 text-white border-blue-600' : 'hover:bg-gray-100'}`}>{p}</button>
                          ) : null;
                        })}
                        <button onClick={() => setPage((p) => p + 1)} disabled={page === totalPages} className="px-2 py-1 rounded border disabled:opacity-40 hover:bg-gray-100">›</button>
                        <button onClick={() => setPage(totalPages)} disabled={page === totalPages} className="px-2 py-1 rounded border disabled:opacity-40 hover:bg-gray-100">»</button>
                      </div>
                    </div>
                  )}
                </>
              )}

              {/* [MỚI] Trường hợp lọc ra rỗng: gợi ý bỏ lọc thay vì để trắng khó hiểu */}
              {!loading && !error && previewData && previewData.rows.length === 0 && selectedUnit && (
                <div className="text-center text-sm text-gray-400 py-10">
                  Không có dữ liệu cho đơn vị "{selectedUnit.label}" ở bảng này.{' '}
                  <button onClick={clearUnitFilter} className="text-blue-600 underline">
                    Bỏ lọc
                  </button>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
};

export default PipelineDataExplorer;
