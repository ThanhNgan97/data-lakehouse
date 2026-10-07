import React, { useState, useRef, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";
import Icon from "../components/icons";
import { Badge, Card, LakehouseMark } from "../components/ui";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000/api";
const MAX_UPLOAD_FILE_BYTES = 100 * 1024 * 1024;
const UPLOAD_EXTENSIONS = ["pdf", "docx", "csv", "tsv", "xlsx", "xls", "json", "parquet"];
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
    id: "profile",
    taskIds: ["ai_semantic_profiler"],
    label: "Profile",
    sub: "AI Semantic",
    tone: "bronze",
    dot: "bg-bronze-500",
  },
  {
    id: "route",
    taskIds: ["branch_router"],
    label: "Route",
    sub: "Universal",
    tone: "silver",
    dot: "bg-silver-500",
  },
  {
    id: "process",
    taskIds: [
      "kpi_flow.ingest_bronze", "kpi_flow.bronze_to_silver",
      "kpi_flow.silver_to_gold", "kpi_flow.predictive_analysis",
      "api_flow.run_registered_api", "generic_flow.process_dynamic_silver_gold",
    ],
    label: "Process",
    sub: "Selected Flow",
    tone: "gold",
    dot: "bg-gold-500",
  },
  {
    id: "validate",
    taskIds: ["join_and_smoke_test"],
    label: "Validate",
    sub: "Trino Smoke Test",
    tone: "lake",
    dot: "bg-lake-500",
  },
];

const taskForStage = (tasks = [], taskIds) => {
  const matches = tasks.filter((task) => taskIds.includes(task.task_id));
  const priority = ["failed", "running", "up_for_retry", "queued", "success"];
  return priority.map((state) => matches.find((task) => task.state === state)).find(Boolean) || matches[0];
};

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
  const [inputMode, setInputMode] = useState("upload");
  const [sourceUrl, setSourceUrl] = useState("");
  const [urlManifest, setUrlManifest] = useState(null);
  const [selectedCandidateIds, setSelectedCandidateIds] = useState([]);
  const [urlBusy, setUrlBusy] = useState(false);
  const [activeImportJob, setActiveImportJob] = useState(null);
  const pollingRef = useRef(null);
  const urlPollingRef = useRef(null);

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
      if (urlPollingRef.current) clearInterval(urlPollingRef.current);
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
          pollingRef.current = null;
          fetchHistory();
        }
      } catch {
        clearInterval(pollingRef.current);
        pollingRef.current = null;
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
    const extension = file.name.split(".").pop()?.toLowerCase();
    if (!UPLOAD_EXTENSIONS.includes(extension)) {
      setUploadStatus("");
      setUploadError("Định dạng file chưa được hỗ trợ. Hãy chọn PDF, DOCX, CSV, TSV, XLSX, XLS, JSON hoặc Parquet.");
      return;
    }
    if (file.size > MAX_UPLOAD_FILE_BYTES) {
      setUploadStatus("");
      setUploadError("File vượt giới hạn 100 MB.");
      return;
    }
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
      if (fileInputRef.current) fileInputRef.current.value = "";
      setTimeout(() => setUploadProgress(0), 1200);
    }
  };

  const onDragOver = (e) => {
    e.preventDefault();
    setIsDragging(true);
  };

  const scanSourceUrl = async () => {
    setUploadError("");
    setUploadStatus("");
    setUrlManifest(null);
    setActiveImportJob(null);
    setUrlBusy(true);
    try {
      const res = await axios.post(`${API_URL}/url-import/scan`, { url: sourceUrl, recursive: false }, { headers: authHeader() });
      setUrlManifest(res.data);
      setSelectedCandidateIds(res.data.files.filter((file) => file.supported).map((file) => file.candidate_id));
    } catch (err) {
      setUploadError(err.response?.data?.detail || "Không thể quét URL nguồn.");
    } finally {
      setUrlBusy(false);
    }
  };

  const importSourceUrl = async () => {
    setUploadError("");
    setUploadStatus("");
    setUrlBusy(true);
    try {
      const candidateIds = selectedCandidateIds;
      const res = await axios.post(`${API_URL}/url-import/jobs`, {
        scan_id: urlManifest.scan_id,
        candidate_ids: candidateIds,
        force_reprocess: true,
      }, { headers: { ...authHeader(), "Idempotency-Key": crypto.randomUUID() } });
      setUploadStatus(res.data.message || "Đã nhập file từ URL.");
      setActiveImportJob(res.data);
      setUrlManifest(null);
      fetchHistory();
      pollImportJob(res.data.job_id);
    } catch (err) {
      setUploadError(err.response?.data?.detail || "Không thể nhập file từ URL.");
    } finally {
      setUrlBusy(false);
    }
  };
  const pollImportJob = (jobId) => {
    if (urlPollingRef.current) clearInterval(urlPollingRef.current);
    urlPollingRef.current = setInterval(async () => {
      try {
        const res = await axios.get(`${API_URL}/url-import/jobs/${jobId}`, { headers: authHeader() });
        setActiveImportJob(res.data);
        fetchHistory();
        const activeFile = res.data.files?.find((file) => file.dag_run_id && file.status === "PROCESSING");
        if (activeFile && !pollingRef.current) pollPipelineStatus(activeFile.dag_run_id);
        if (["COMPLETED", "PARTIAL_SUCCESS", "FAILED", "CANCELLED"].includes(res.data.status)) {
          clearInterval(urlPollingRef.current);
          urlPollingRef.current = null;
          setUploadStatus(`Import URL: ${res.data.status}`);
          fetchHistory();
        }
      } catch {
        clearInterval(urlPollingRef.current);
        urlPollingRef.current = null;
      }
    }, 3000);
  };

  const retryImportJob = async () => {
    if (!activeImportJob?.job_id) return;
    const res = await axios.post(`${API_URL}/url-import/jobs/${activeImportJob.job_id}/retry`, {}, { headers: authHeader() });
    setActiveImportJob(res.data);
    pollImportJob(activeImportJob.job_id);
  };

  const cancelImportJob = async () => {
    if (!activeImportJob?.job_id) return;
    const res = await axios.post(`${API_URL}/url-import/jobs/${activeImportJob.job_id}/cancel`, {}, { headers: authHeader() });
    setActiveImportJob(res.data);
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
      <header className="bg-white border-b border-slate-200 px-4 sm:px-6 md:px-8 py-3.5 flex justify-between items-center sticky top-0 z-50 shadow-sm">
        <div className="flex items-center gap-3 min-w-0">
          <img
            src="/CUSC Logo Series.png"
            alt="CUSC Logo"
            className="h-10 w-auto object-contain shrink-0"
          />
          <div className="min-w-0">
            <h1 className="text-sm sm:text-base md:text-lg font-bold text-slate-900 leading-tight tracking-tight truncate">
              CUSC ANALYSIS PLATFORM
            </h1>
            <p className="text-[10px] sm:text-[11px] md:text-xs text-slate-500 font-medium truncate">
              Trung tâm Công nghệ Thông tin - Đại học Cần Thơ
            </p>
          </div>
        </div>
        <button
          onClick={handleLogout}
          className="flex shrink-0 items-center gap-2 px-3 sm:px-4 py-2 bg-ink-900 hover:bg-rose-600 text-white rounded-lg text-xs sm:text-sm font-semibold transition"
        >
          <Icon name="logOut" className="w-4 h-4" />
          Đăng xuất
        </button>
      </header>

      <main className="flex-1 w-full max-w-[1480px] mx-auto p-4 md:p-6 lg:p-8 space-y-7 lg:space-y-8">
        {/* ROW 1 — Upload + History */}
        <section className="grid grid-cols-1 xl:grid-cols-12 items-stretch gap-5 lg:gap-6 xl:h-[430px] 2xl:h-[450px]">
          {/* 1. Upload Card */}
          <Card className="xl:col-span-5 h-full p-5 sm:p-6 flex flex-col">
            <div className="mb-3 shrink-0">
              <p className="font-data text-[10px] uppercase tracking-[0.14em] text-lake-600 font-semibold mb-1">
                Bước 1
              </p>
              <h2 className="text-lg font-bold text-ink-900">
                Tải lên Dữ liệu
              </h2>
              <p className="text-xs text-ink-400 mt-1">
                Hệ thống sẽ tự động đưa vào Bronze Zone và kích hoạt Pipeline
              </p>
            </div>

            <div className="mb-4 grid grid-cols-2 rounded-xl bg-ink-50 p-1 text-xs font-semibold">
              <button type="button" onClick={() => setInputMode("upload")}
                className={`rounded-lg px-3 py-2 transition ${inputMode === "upload" ? "bg-white text-cobalt-700 shadow-sm" : "text-ink-500"}`}>
                Upload file
              </button>
              <button type="button" onClick={() => setInputMode("url")}
                className={`rounded-lg px-3 py-2 transition ${inputMode === "url" ? "bg-white text-cobalt-700 shadow-sm" : "text-ink-500"}`}>
                Dán đường dẫn URL
              </button>
            </div>

            {inputMode === "url" ? (
              <div className="flex-1 rounded-[22px] border border-ink-100 bg-ink-50/40 p-4">
                <label className="block text-xs font-semibold text-ink-700">
                  Đường dẫn file
                  <input type="url" value={sourceUrl}
                    onChange={(event) => { setSourceUrl(event.target.value); setUrlManifest(null); setSelectedCandidateIds([]); }}
                    placeholder="https://example.com/data/report.pdf"
                    className="mt-2 w-full rounded-xl border border-ink-200 bg-white px-3 py-2.5 text-sm font-normal outline-none focus:border-cobalt-400 focus:ring-2 focus:ring-cobalt-100" />
                </label>
                <p className="mt-2 text-[11px] text-ink-400">Hỗ trợ file HTTP trực tiếp và Google Drive file/folder · tối đa 100 MB/file.</p>
                {!urlManifest ? (
                  <button type="button" disabled={!sourceUrl || urlBusy} onClick={scanSourceUrl}
                    className="mt-4 w-full rounded-xl bg-cobalt-600 px-4 py-2.5 text-xs font-semibold text-white disabled:opacity-50">
                    {urlBusy ? "Đang quét..." : "Scan URL"}
                  </button>
                ) : (
                  <div className="mt-4 rounded-xl border border-ink-100 bg-white p-3">
                    <p className="text-xs font-semibold text-ink-800">
                      Tìm thấy {urlManifest.summary.discovered} file · Có thể import {urlManifest.summary.accepted}
                    </p>
                    {urlManifest.files.map((file) => (
                      <div key={file.candidate_id} className="mt-2 flex items-center justify-between gap-3 text-xs">
                        <label className="flex min-w-0 items-center gap-2 text-ink-700">
                          <input type="checkbox" disabled={!file.supported}
                            checked={selectedCandidateIds.includes(file.candidate_id)}
                            onChange={(event) => setSelectedCandidateIds((current) => event.target.checked
                              ? [...current, file.candidate_id]
                              : current.filter((id) => id !== file.candidate_id))} />
                          <span className="truncate">{file.supported ? "✓" : "✕"} {file.name}</span>
                        </label>
                        <span className="shrink-0 text-ink-400">{file.size ? `${(file.size / 1024 / 1024).toFixed(1)} MB` : "Không rõ"}</span>
                      </div>
                    ))}
                    {urlManifest.files[0]?.reason && <p className="mt-2 text-xs text-rose-600">{urlManifest.files[0].reason}</p>}
                    <button type="button" disabled={!selectedCandidateIds.length || urlBusy} onClick={importSourceUrl}
                      className="mt-3 w-full rounded-xl bg-cobalt-600 px-4 py-2.5 text-xs font-semibold text-white disabled:opacity-50">
                      {urlBusy ? "Đang import..." : "Import file"}
                    </button>
                  </div>
                )}
                {activeImportJob && (
                  <div className="mt-3 rounded-xl border border-lake-100 bg-lake-50 p-3 text-xs">
                    <div className="flex items-center justify-between gap-3">
                      <span className="font-semibold text-ink-800">Import {activeImportJob.status}</span>
                      <span className="font-data text-ink-400">{activeImportJob.files?.length || 0} file</span>
                    </div>
                    <div className="mt-2 flex flex-wrap gap-2 text-[11px] text-ink-600">
                      {Object.entries(activeImportJob.counts || {}).map(([status, count]) => (
                        <span key={status} className="rounded bg-white px-2 py-1">
                          {status === "DUPLICATE" ? "\u0110\u00e3 x\u1eed l\u00fd tr\u01b0\u1edbc \u0111\u00f3" : status}: {count}
                        </span>
                      ))}
                    </div>
                    {(activeImportJob.counts?.DUPLICATE || 0) > 0 && (
                      <p className="mt-2 rounded-lg border border-sky-100 bg-white px-3 py-2 text-[11px] text-sky-700">
                        {"File c\u00f3 n\u1ed9i dung gi\u1ed1ng h\u1ec7t file \u0111\u00e3 x\u1eed l\u00fd th\u00e0nh c\u00f4ng, n\u00ean h\u1ec7 th\u1ed1ng d\u00f9ng l\u1ea1i k\u1ebft qu\u1ea3 v\u00e0 kh\u00f4ng ch\u1ea1y pipeline l\u1eb7p l\u1ea1i."}
                      </p>
                    )}
                    <div className="mt-3 flex gap-2">
                      {["FAILED", "PARTIAL_SUCCESS"].includes(activeImportJob.status) && (
                        <button type="button" onClick={retryImportJob} className="rounded-lg bg-cobalt-600 px-3 py-2 font-semibold text-white">
                          Retry file lỗi
                        </button>
                      )}
                      {["PENDING", "PROCESSING"].includes(activeImportJob.status) && (
                        <button type="button" onClick={cancelImportJob} className="rounded-lg border border-rose-200 bg-white px-3 py-2 font-semibold text-rose-600">
                          Hủy import
                        </button>
                      )}
                    </div>
                  </div>
                )}
              </div>
            ) : (
            <>
            <input
              type="file"
              ref={fileInputRef}
              onChange={(e) => processFile(e.target.files[0])}
              accept=".pdf,.docx,.csv,.tsv,.xlsx,.xls,.json,.parquet"
              className="hidden"
            />

            <div
              onClick={() => !uploading && fileInputRef.current.click()}
              onDragOver={onDragOver}
              onDragLeave={onDragLeave}
              onDrop={onDrop}
              className={`relative min-h-[190px] flex-1 overflow-hidden rounded-[22px] border-2 border-dashed px-5 py-6 text-center transition-all duration-300 flex flex-col items-center justify-center group ${
                uploading
                  ? "cursor-wait opacity-90 bg-ink-50 border-ink-200"
                  : "cursor-pointer border-cobalt-200 bg-ink-50/40 hover:border-cobalt-300 hover:bg-cobalt-50/40"
              } ${isDragging ? "border-cobalt-500 bg-cobalt-50 scale-[1.01]" : ""}`}
            >
              <div
                className={`w-12 h-12 mb-3 rounded-2xl flex items-center justify-center transition-all duration-300 ${
                  isDragging
                    ? "bg-cobalt-600 text-white scale-110"
                    : "bg-cobalt-50 text-cobalt-600 group-hover:scale-105"
                }`}
              >
                <Icon
                  name={uploading ? "clock" : "uploadCloud"}
                  className="w-5 h-5"
                />
              </div>
              <p className="text-sm font-semibold text-ink-900">
                {uploading
                  ? "Đang tải lên và xử lý..."
                  : "Kéo thả file hoặc bấm để chọn"}
              </p>
              <p className="mt-1 text-[11px] text-ink-400 font-data">
                Hỗ trợ: PDF, DOCX, CSV, TSV, XLSX, XLS, JSON, PARQUET
              </p>
              <span className="mt-2 inline-flex items-center rounded-full border border-cobalt-200 bg-cobalt-50 px-2.5 py-1 text-[11px] font-semibold text-cobalt-700">
                Tối đa 100 MB mỗi file
              </span>
              {!uploading && (
                <span className="mt-4 inline-flex h-9 items-center justify-center rounded-control bg-cobalt-600 px-4 text-xs font-semibold text-white shadow-sm transition group-hover:bg-cobalt-700">
                  <span className="mr-1.5 text-base leading-none">+</span>
                  Chọn tệp từ máy tính
                </span>
              )}

              {uploading && uploadProgress > 0 && (
                <div
                  className="absolute bottom-0 left-0 h-1.5 bg-lake-500 transition-all duration-300"
                  style={{ width: `${uploadProgress}%` }}
                />
              )}
            </div>
            </>
            )}

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

          {/* 2. Upload History */}
          <Card className="xl:col-span-7 h-full min-h-[360px] xl:min-h-0 overflow-hidden flex flex-col">
            <div className="flex items-center justify-between gap-4 px-5 sm:px-6 py-3.5 border-b border-ink-100 bg-white shrink-0">
              <div className="min-w-0">
                <h3 className="font-bold text-ink-900 text-base">
                  Lịch sử tải lên
                </h3>
                <p className="text-[11px] text-ink-400 mt-0.5">
                  Tự động đồng bộ trạng thái Pipeline mỗi 5 giây
                </p>
              </div>
              <button
                onClick={fetchHistory}
                className="flex shrink-0 items-center gap-1.5 px-3 py-1.5 text-xs font-semibold text-lake-700 bg-lake-50 hover:bg-lake-100 rounded-lg transition"
                disabled={historyLoading}
              >
                <Icon
                  name="refresh"
                  className={`w-3.5 h-3.5 ${historyLoading ? "animate-spin" : ""}`}
                />
                {historyLoading ? "Đang tải..." : "Làm mới"}
              </button>
            </div>

            <div className="min-h-0 flex-1 overflow-y-auto bg-[#FAFBFD] p-3">
              {history.length === 0 ? (
                <div className="h-full flex flex-col items-center justify-center text-ink-300">
                  <Icon name="inbox" className="w-9 h-9 mb-3 opacity-60" />
                  <p className="text-sm font-medium text-ink-400 text-center">
                    Chưa có dữ liệu nào được tải lên hệ thống.
                  </p>
                </div>
              ) : (
                <div className="space-y-2">
                  {history.map((item, i) => {
                    const st = getStatus(item);
                    return (
                      <div
                        key={item.id || i}
                        className="bg-white border border-ink-100 rounded-xl p-2.5 hover:border-lake-200 hover:shadow-sm transition-all flex flex-col gap-2"
                      >
                        <div className="flex items-center justify-between gap-4">
                          <div className="flex items-center gap-3 min-w-0">
                            <div className="w-9 h-9 rounded-lg bg-lake-50 text-lake-600 border border-lake-100 flex items-center justify-center shrink-0">
                              <Icon name="file" className="w-4 h-4" />
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
              )}
            </div>
          </Card>
        </section>

        {/* ROW 2 — Pipeline Status */}
        {activePipeline && (
          <Card className="p-6 sm:p-7">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between mb-6">
              <div>
                <p className="font-data text-[10px] uppercase tracking-[0.14em] text-lake-600 font-semibold mb-1">
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
                  className="self-start text-[10px] font-data px-2 py-1 bg-ink-50 text-ink-500 rounded border border-ink-100"
                  title={activePipeline.dag_run_id}
                >
                  ID: {activePipeline.dag_run_id.slice(0, 8)}...
                </span>
              )}
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-4 xl:gap-5">
              {PIPELINE_TASKS.map((pt) => {
                const t = taskForStage(activePipeline.tasks, pt.taskIds);
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
                  <div
                    key={pt.id}
                    className={`relative rounded-xl p-4 xl:p-5 border ${cardBorder} ${cardBg} transition-all duration-300`}
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div className="flex min-w-0 items-start gap-3">
                        <span
                          className={`mt-1 w-3 h-3 rounded-full border-2 shrink-0 ${dotClass}`}
                        />
                        <div className="min-w-0">
                          <p
                            className={`font-semibold text-sm ${state === "pending" ? "text-ink-400" : "text-ink-800"}`}
                          >
                            {pt.label}{" "}
                            <span className="text-ink-300 font-data text-[11px] font-normal">
                              · {pt.sub}
                            </span>
                          </p>
                          <p
                            className={`text-[11px] font-data font-medium mt-1 ${textColor}`}
                          >
                            {stateText}
                          </p>
                        </div>
                      </div>
                      <Icon
                        name={icon}
                        className={`w-4 h-4 shrink-0 ${textColor}`}
                      />
                    </div>
                  </div>
                );
              })}
            </div>

            {activePipeline.error_message && (
              <div className="mt-5 p-4 bg-rose-50 border border-rose-200 rounded-xl flex items-start gap-3 text-rose-800 text-sm">
                <Icon name="alertTriangle" className="w-5 h-5 shrink-0 mt-0.5 text-rose-600" />
                <div>
                  <p className="font-bold mb-1">Lỗi Pipeline:</p>
                  <p className="font-medium whitespace-pre-wrap">{activePipeline.error_message}</p>
                </div>
              </div>
            )}
          </Card>
        )}

        {/* ROW 3 — Superset Analytics */}
        <Card className="overflow-hidden flex flex-col">
          <div className="px-5 sm:px-6 py-4 border-b border-ink-100 bg-white flex flex-col gap-3 sm:flex-row sm:justify-between sm:items-center shrink-0">
            <div>
              <p className="font-data text-[10px] uppercase tracking-[0.14em] text-gold-500 font-semibold mb-1">
                Bước 3
              </p>
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
              className="self-start sm:self-auto text-xs text-lake-700 hover:text-lake-800 bg-lake-50 hover:bg-lake-100 px-3 py-1.5 rounded-lg transition font-semibold flex items-center gap-1.5"
            >
              Mở tab mới
              <Icon name="externalLink" className="w-3.5 h-3.5" />
            </a>
          </div>
          <div className="bg-[#FAFBFD] relative p-2 sm:p-3">
            <iframe
              src={supersetUrl}
              title="Superset Chart"
              className="w-full h-[560px] sm:h-[620px] lg:h-[720px] xl:h-[820px] border border-ink-100 bg-white rounded-xl shadow-inner"
            ></iframe>
          </div>
        </Card>
      </main>
    </div>
  );
};

export default UserUpload;
