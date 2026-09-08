import React, { useState, useRef, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";
import Icon from "../components/icons";
import { Badge, Card, LakehouseMark } from "../components/ui";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000/api";
const supersetUrl =
  import.meta.env.VITE_SUPERSET_DASHBOARD_URL ||
  "http://localhost:8088/superset/dashboard/1/?standalone=3";
const authHeader = () => ({
  Authorization: `Bearer ${localStorage.getItem("token")}`,
});

/* Nhãn + màu (tone Badge) cho từng trạng thái pipeline/upload */
const STATUS_CONFIG = {
  pending: { label: "Đang chờ", tone: "ink" },
  uploaded: { label: "Đã upload", tone: "lake" },
  triggered: { label: "Pipeline đang chạy", tone: "warn" },
  trigger_failed: { label: "Chưa kích hoạt pipeline", tone: "warn" },
  upload_failed: { label: "Upload thất bại", tone: "danger" },
  running: { label: "Pipeline đang chạy", tone: "warn" },
  success: { label: "Hoàn thành", tone: "success" },
  failed: { label: "Pipeline lỗi", tone: "danger" },
  queued: { label: "Đang xếp hàng", tone: "lake" },
  unreachable: { label: "Airflow offline", tone: "ink" },
};

/* Mỗi bước pipeline gắn đúng tông màu của tầng dữ liệu tương ứng —
   Bronze / Silver / Gold theo đúng kiến trúc Medallion, bước dự đoán
   dùng màu "lake" (phân tích/insight). */
const PIPELINE_TASKS = [
  {
    id: "ingest_bronze",
    label: "Extract",
    sub: "Bronze",
    tone: "bronze",
    dot: "bg-bronze-500",
  },
  {
    id: "bronze_to_silver",
    label: "Transform",
    sub: "Silver",
    tone: "silver",
    dot: "bg-silver-500",
  },
  {
    id: "silver_to_gold",
    label: "Load",
    sub: "Gold",
    tone: "gold",
    dot: "bg-gold-500",
  },
  {
    id: "predictive_analysis",
    label: "Analyze",
    sub: "Predict",
    tone: "lake",
    dot: "bg-lake-500",
  },
];

const UserUpload = () => {
  const navigate = useNavigate();
  const fileInputRef = useRef(null);
  const [isDragging, setIsDragging] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [uploadStatus, setUploadStatus] = useState("");
  const [uploadError, setUploadError] = useState("");
  const [history, setHistory] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [activePipeline, setActivePipeline] = useState(null);
  const [filterStatus, setFilterStatus] = useState("all");
  const pollingRef = useRef(null);

  const fetchHistory = useCallback(async () => {
    setHistoryLoading(true);
    try {
      const res = await axios.get(`${API_URL}/upload/history`, {
        headers: authHeader(),
      });
      const hist = res.data || [];
      setHistory(hist);

      if (
        hist.length > 0 &&
        ["running", "queued", "pending"].includes(hist[0].pipeline_status)
      ) {
        if (!pollingRef.current && hist[0].dag_run_id) {
          pollPipelineStatus(hist[0].dag_run_id);
        }
      }
    } catch {
      /* silence */
    } finally {
      setHistoryLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchHistory();
    return () => {
      if (pollingRef.current) clearInterval(pollingRef.current);
    };
  }, [fetchHistory]);

  const pollPipelineStatus = (dagRunId) => {
    if (!dagRunId) return;
    if (pollingRef.current) clearInterval(pollingRef.current);

    (async () => {
      try {
        const res = await axios.get(
          `${API_URL}/upload/pipeline-status/${dagRunId}`,
          { headers: authHeader() },
        );
        setActivePipeline(res.data);
      } catch {}
    })();

    pollingRef.current = setInterval(async () => {
      try {
        const res = await axios.get(
          `${API_URL}/upload/pipeline-status/${dagRunId}`,
          { headers: authHeader() },
        );
        const state = res.data.state;
        setActivePipeline(res.data);
        if (["success", "failed", "unreachable"].includes(state)) {
          clearInterval(pollingRef.current);
          fetchHistory();
        }
      } catch {
        clearInterval(pollingRef.current);
      }
    }, 5000);
  };

  const handleLogout = () => {
    localStorage.removeItem("token");
    localStorage.removeItem("role");
    navigate("/login");
  };

  const processFile = async (file) => {
    if (!file) return;
    setUploadError("");
    setUploadStatus("");
    setUploading(true);
    setUploadProgress(10);

    const formData = new FormData();
    formData.append("file", file);

    try {
      setUploadProgress(30);
      const res = await axios.post(`${API_URL}/upload/`, formData, {
        headers: {
          "Content-Type": "multipart/form-data",
          ...authHeader(),
        },
        onUploadProgress: (e) => {
          const pct = Math.round((e.loaded * 60) / e.total) + 30;
          setUploadProgress(Math.min(pct, 90));
        },
      });
      setUploadProgress(100);
      setUploadStatus(res.data.message);
      if (res.data.dag_run_id) {
        pollPipelineStatus(res.data.dag_run_id);
      }
      fetchHistory();
    } catch (err) {
      setUploadError(
        err.response?.data?.detail ||
          "Lỗi upload. Hãy kiểm tra kết nối server.",
      );
    } finally {
      setUploading(false);
      setTimeout(() => setUploadProgress(0), 1200);
    }
  };

  const onDragOver = (e) => {
    e.preventDefault();
    setIsDragging(true);
  };
  const onDragLeave = (e) => {
    e.preventDefault();
    setIsDragging(false);
  };
  const onDrop = (e) => {
    e.preventDefault();
    setIsDragging(false);
    if (e.dataTransfer.files.length > 0) processFile(e.dataTransfer.files[0]);
  };

  const getStatus = (item) =>
    STATUS_CONFIG[item.pipeline_status] || {
      label: item.pipeline_status,
      tone: "ink",
    };

  return (
    <div className="min-h-screen bg-slate-50 font-sans flex flex-col">
      <header className="bg-white border-b border-slate-200 px-6 md:px-8 py-3.5 flex justify-between items-center sticky top-0 z-50 shadow-sm">
        <div className="flex items-center gap-3">
          <img
            src="/CUSC Logo Series.png"
            alt="CUSC Logo"
            className="h-10 w-auto object-contain"
          />
          <div>
            <h1 className="text-base md:text-lg font-bold text-slate-900 leading-tight tracking-tight">
              CUSC ANALYSIS PLATFORM
            </h1>
            <p className="text-[11px] md:text-xs text-slate-500 font-medium">
              Trung tâm Công nghệ Thông tin - Đại học Cần Thơ
            </p>
          </div>
        </div>
        <button
          onClick={handleLogout}
          className="flex items-center gap-2 px-4 py-2 bg-ink-900 hover:bg-rose-600 text-white rounded-lg text-sm font-semibold transition"
        >
          <Icon name="logOut" className="w-4 h-4" />
          Đăng xuất
        </button>
      </header>

      <main className="flex-1 w-full max-w-[1400px] mx-auto p-4 md:p-6 lg:p-8 grid grid-cols-1 lg:grid-cols-12 gap-6 lg:gap-8">
        {/* CỘT TRÁI (Upload & Pipeline) */}
        <div className="lg:col-span-4 flex flex-col gap-6">
          {/* 1. Upload Card */}
          <Card className="p-6">
            <div className="mb-5">
              <p className="font-data text-[11px] uppercase tracking-widest text-lake-600 font-semibold mb-1">
                Bước 1
              </p>
              <h2 className="text-lg font-bold text-ink-900">
                Tải lên Dữ liệu
              </h2>
              <p className="text-xs text-ink-400 mt-1">
                Hệ thống sẽ tự động đưa vào Bronze Zone và kích hoạt Pipeline
              </p>
            </div>

            <input
              type="file"
              ref={fileInputRef}
              onChange={(e) => processFile(e.target.files[0])}
              accept=".pdf,.docx,.ppt,.pptx,.json,.csv,.xlsx,.xls"
              className="hidden"
            />

            <div
              onClick={() => !uploading && fileInputRef.current.click()}
              onDragOver={onDragOver}
              onDragLeave={onDragLeave}
              onDrop={onDrop}
              className={`relative overflow-hidden border-2 border-dashed rounded-2xl p-8 text-center transition-all duration-300 flex flex-col items-center justify-center group ${
                uploading
                  ? "cursor-wait opacity-90 bg-ink-50 border-ink-200"
                  : "cursor-pointer border-lake-200 hover:border-lake-400 hover:bg-lake-50/50"
              } ${isDragging ? "border-lake-500 bg-lake-50 scale-[1.02]" : ""}`}
            >
              <div
                className={`w-14 h-14 mb-3 rounded-xl flex items-center justify-center transition-all duration-300 ${
                  isDragging
                    ? "bg-lake-500 text-white scale-110"
                    : "bg-lake-50 text-lake-600 group-hover:scale-110 group-hover:bg-lake-500 group-hover:text-white"
                }`}
              >
                <Icon
                  name={uploading ? "clock" : "uploadCloud"}
                  className="w-6 h-6"
                />
              </div>
              <p className="text-sm font-semibold text-ink-800">
                {uploading
                  ? "Đang tải lên và xử lý..."
                  : "Kéo thả file hoặc bấm để chọn"}
              </p>
              <p className="text-[11px] text-ink-400 mt-1.5 font-data">
                Hỗ trợ định dạng: PDF, DOCX, PPT, PPTX, JSON, CSV, EXCEL
              </p>

              {uploading && uploadProgress > 0 && (
                <div
                  className="absolute bottom-0 left-0 h-1.5 bg-lake-500 transition-all duration-300"
                  style={{ width: `${uploadProgress}%` }}
                />
              )}
            </div>

            {uploadStatus && (
              <div className="mt-4 p-3 bg-emerald-50 border border-emerald-200 text-emerald-700 rounded-xl text-sm flex items-start gap-2.5">
                <Icon name="checkCircle" className="w-4 h-4 mt-0.5 shrink-0" />
                <span className="font-medium">{uploadStatus}</span>
              </div>
            )}
            {uploadError && (
              <div className="mt-4 p-3 bg-rose-50 border border-rose-200 text-rose-600 rounded-xl text-sm flex items-start gap-2.5">
                <Icon
                  name="alertTriangle"
                  className="w-4 h-4 mt-0.5 shrink-0"
                />
                <span className="font-medium">{uploadError}</span>
              </div>
            )}
          </Card>

          {/* 2. Pipeline Status (Vertical Stepper) */}
          {activePipeline && (
            <Card className="p-6 flex-1">
              <div className="flex justify-between items-start mb-6">
                <div>
                  <p className="font-data text-[11px] uppercase tracking-widest text-lake-600 font-semibold mb-1">
                    Bước 2
                  </p>
                  <h3 className="text-base font-bold text-ink-900">
                    Tiến trình Pipeline
                  </h3>
                  <p className="text-xs text-ink-400 mt-0.5">
                    Airflow Orchestration
                  </p>
                </div>
                {activePipeline.dag_run_id && (
                  <span
                    className="text-[10px] font-data px-2 py-1 bg-ink-50 text-ink-500 rounded border border-ink-100"
                    title={activePipeline.dag_run_id}
                  >
                    ID: {activePipeline.dag_run_id.slice(0, 8)}...
                  </span>
                )}
              </div>

              <div className="relative pl-5 border-l-2 border-ink-100 space-y-6 ml-2">
                {PIPELINE_TASKS.map((pt) => {
                  const t = activePipeline.tasks?.find(
                    (x) => x.task_id === pt.id,
                  );
                  const state = t?.state || "pending";

                  let dotClass = "bg-ink-200 border-ink-100";
                  let stateText = "Đang đợi";
                  let icon = "clock";
                  let textColor = "text-ink-400";
                  let cardBorder = "border-ink-100";
                  let cardBg = "";

                  if (state === "success") {
                    dotClass = `${pt.dot} border-white shadow-[0_0_0_3px_rgba(16,185,129,0.15)]`;
                    stateText = "Thành công";
                    icon = "checkCircle";
                    textColor = "text-emerald-700";
                    cardBorder = "border-emerald-100";
                  } else if (state === "running") {
                    dotClass = `${pt.dot} border-white animate-pulse shadow-[0_0_0_3px_rgba(15,151,168,0.2)]`;
                    stateText = "Đang xử lý...";
                    icon = "activity";
                    textColor = "text-lake-700";
                    cardBorder = "border-lake-200";
                    cardBg = "bg-lake-50/50";
                  } else if (state === "failed") {
                    dotClass =
                      "bg-rose-500 border-white shadow-[0_0_0_3px_rgba(244,63,94,0.15)]";
                    stateText = "Lỗi";
                    icon = "alertTriangle";
                    textColor = "text-rose-700";
                    cardBorder = "border-rose-200";
                  }

                  return (
                    <div key={pt.id} className="relative">
                      <div
                        className={`absolute -left-[27px] top-2 w-3.5 h-3.5 rounded-full border-2 ${dotClass} z-10 transition-colors duration-300`}
                      />
                      <div
                        className={`rounded-xl p-3 border ${cardBorder} ${cardBg} transition-all duration-300`}
                      >
                        <div className="flex items-center justify-between">
                          <div>
                            <p
                              className={`font-semibold text-sm ${state === "pending" ? "text-ink-400" : "text-ink-800"}`}
                            >
                              {pt.label}{" "}
                              <span className="text-ink-300 font-data text-[11px] font-normal">
                                · {pt.sub}
                              </span>
                            </p>
                          </div>
                          <Icon
                            name={icon}
                            className={`w-4 h-4 ${textColor}`}
                          />
                        </div>
                        <p
                          className={`text-[11px] font-data font-medium mt-1 ${textColor}`}
                        >
                          {stateText}
                        </p>
                      </div>
                    </div>
                  );
                })}
              </div>
              {activePipeline.error_message && (
                <div className="mt-6 p-4 bg-rose-50 border border-rose-200 rounded-xl flex items-start gap-3 text-rose-800 text-sm">
                  <Icon name="alertTriangle" className="w-5 h-5 shrink-0 mt-0.5 text-rose-600" />
                  <div>
                    <p className="font-bold mb-1">Lỗi Pipeline:</p>
                    <p className="font-medium whitespace-pre-wrap">{activePipeline.error_message}</p>
                  </div>
                </div>
              )}
            </Card>
          )}
        </div>

        {/* CỘT PHẢI (History & Superset) */}
        <div className="lg:col-span-8 flex flex-col gap-6 h-full">
          {/* 3. Upload History */}
          <Card className="overflow-hidden flex flex-col h-[320px]">
            <div className="flex items-center justify-between px-6 py-4 border-b border-ink-100 bg-white shrink-0">
              <div>
                <h3 className="font-bold text-ink-900 text-base">
                  Lịch sử tải lên
                </h3>
                <div className="flex items-center gap-2 mt-2">
                  {["all", "running", "success", "failed"].map((f) => (
                    <button
                      key={f}
                      onClick={() => setFilterStatus(f)}
                      className={`text-[10px] font-semibold uppercase tracking-wider px-2.5 py-1 rounded-md transition ${
                        filterStatus === f
                          ? "bg-lake-100 text-lake-700"
                          : "bg-ink-50 text-ink-400 hover:bg-ink-100"
                      }`}
                    >
                      {f === "all"
                        ? "Tất cả"
                        : f === "running"
                        ? "Đang xử lý"
                        : f === "success"
                        ? "Thành công"
                        : "Lỗi"}
                    </button>
                  ))}
                </div>
              </div>
              <button
                onClick={fetchHistory}
                className="flex items-center gap-1.5 px-3 py-1.5 text-xs font-semibold text-lake-700 bg-lake-50 hover:bg-lake-100 rounded-lg transition"
                disabled={historyLoading}
              >
                <Icon
                  name="refresh"
                  className={`w-3.5 h-3.5 ${historyLoading ? "animate-spin" : ""}`}
                />
                {historyLoading ? "Đang tải..." : "Làm mới"}
              </button>
            </div>

            <div className="flex-1 overflow-auto bg-[#FAFBFD] p-4">
              {(() => {
                const filteredHistory = history.filter((item) => {
                  if (filterStatus === "all") return true;
                  const st = item.pipeline_status;
                  if (filterStatus === "success") return st === "success";
                  if (filterStatus === "failed") return ["failed", "upload_failed", "trigger_failed", "unreachable"].includes(st);
                  if (filterStatus === "running") return ["pending", "uploaded", "triggered", "running", "queued"].includes(st);
                  return true;
                });

                if (filteredHistory.length === 0) {
                  return (
                    <div className="h-full flex flex-col items-center justify-center text-ink-300">
                      <Icon name="inbox" className="w-9 h-9 mb-3 opacity-60" />
                      <p className="text-sm font-medium text-ink-400">
                        Chưa có dữ liệu phù hợp.
                      </p>
                    </div>
                  );
                }

                return (
                  <div className="space-y-3">
                    {filteredHistory.map((item, i) => {
                    const st = getStatus(item);
                    return (
                      <div
                        key={item.id || i}
                        className="bg-white border border-ink-100 rounded-xl p-3.5 hover:border-lake-200 hover:shadow-sm transition-all flex flex-col gap-3"
                      >
                        <div className="flex items-center justify-between gap-4">
                          <div className="flex items-center gap-3.5 min-w-0">
                            <div className="w-10 h-10 rounded-lg bg-lake-50 text-lake-600 border border-lake-100 flex items-center justify-center shrink-0">
                              <Icon name="file" className="w-4.5 h-4.5" />
                            </div>
                            <div className="min-w-0">
                              <h4
                                className="font-semibold text-ink-800 text-sm truncate"
                                title={item.filename}
                              >
                                {item.filename}
                              </h4>
                              <div className="text-[11px] text-ink-400 mt-1 flex items-center gap-2 truncate font-data">
                                <span className="font-medium text-ink-500">
                                  {item.username}
                                </span>
                                <span className="text-ink-200">•</span>
                                <span title={item.dag_run_id}>
                                  {item.dag_run_id?.slice(0, 12)}...
                                </span>
                              </div>
                            </div>
                          </div>

                          <div className="flex flex-col items-end shrink-0 gap-1.5">
                            <Badge tone={st.tone}>{st.label}</Badge>
                            <span className="text-[10px] text-ink-300 font-data">
                              {item.uploaded_at
                                ? new Date(item.uploaded_at + "Z").toLocaleString(
                                    "vi-VN",
                                  )
                                : "—"}
                            </span>
                          </div>
                        </div>
                        {item.metadata_info?.error_message && (
                          <div className="bg-rose-50 border border-rose-100 rounded-lg p-2.5 text-xs text-rose-700 flex items-start gap-2">
                            <Icon name="alertTriangle" className="w-4 h-4 shrink-0 mt-0.5" />
                            <span className="font-medium line-clamp-2" title={item.metadata_info.error_message}>{item.metadata_info.error_message}</span>
                          </div>
                        )}
                      </div>
                    );
                  })}
                  </div>
                );
              })()}
            </div>
          </Card>

          {/* 4. Superset Dashboard */}
          <Card className="overflow-hidden flex flex-col flex-1 min-h-[450px]">
            <div className="px-6 py-4 border-b border-ink-100 bg-white flex justify-between items-center shrink-0">
              <div>
                <h3 className="text-base font-bold text-ink-900">
                  Báo cáo Phân tích
                </h3>
                <p className="text-[11px] text-ink-400 mt-0.5 font-data">
                  Gold Layer · Apache Superset
                </p>
              </div>
              <a
                href={supersetUrl}
                target="_blank"
                rel="noreferrer"
                className="text-xs text-lake-700 hover:text-lake-800 bg-lake-50 hover:bg-lake-100 px-3 py-1.5 rounded-lg transition font-semibold flex items-center gap-1.5"
              >
                Mở tab mới
                <Icon name="externalLink" className="w-3.5 h-3.5" />
              </a>
            </div>
            <div className="flex-1 bg-[#FAFBFD] relative p-3">
              <iframe
                src={supersetUrl}
                title="Superset Chart"
                className="w-full h-full border border-ink-100 bg-white rounded-xl shadow-inner"
              ></iframe>
            </div>
          </Card>
        </div>
      </main>
    </div>
  );
};

export default UserUpload;
