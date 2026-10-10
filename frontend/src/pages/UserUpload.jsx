import React, { useState, useRef, useEffect, useCallback } from "react";
import axios from "axios";
import UserTopNavigation from "../components/UserTopNavigation";
import Icon from "../components/icons";
import { Badge, Card } from "../components/ui";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000/api";
const MAX_UPLOAD_FILE_BYTES = 100 * 1024 * 1024;
const UPLOAD_EXTENSIONS = ["pdf", "docx", "json", "csv", "xlsx", "xls"];
const SUPERSET_ORIGIN = new URL(
  import.meta.env.VITE_SUPERSET_URL ||
    import.meta.env.VITE_SUPERSET_DASHBOARD_URL ||
    "http://localhost:8088",
).origin;
const dashboardUrlFor = (slug) =>
  slug
    ? `${SUPERSET_ORIGIN}/superset/dashboard/${encodeURIComponent(slug)}/?standalone=3`
    : "";
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
    id: "extract",
    taskIds: ["ai_semantic_profiler", "kpi_flow.ingest_bronze", "api_flow.run_registered_api"],
    label: "Extract",
    sub: "Thu thập dữ liệu",
    icon: "extract",
    iconClass: "bg-orange-50 text-orange-600",
    tone: "bronze",
    dot: "bg-bronze-500",
    number: "border-cobalt-100 bg-cobalt-50 text-cobalt-700",
  },
  {
    id: "transform",
    taskIds: ["branch_router", "kpi_flow.bronze_to_silver", "generic_flow.process_dynamic_silver_gold"],
    label: "Transform",
    sub: "Chuẩn hóa dữ liệu",
    icon: "refresh",
    iconClass: "bg-blue-50 text-blue-600",
    tone: "silver",
    dot: "bg-silver-500",
    number: "border-lake-100 bg-lake-50 text-lake-700",
  },
  {
    id: "load",
    taskIds: [
      "kpi_flow.silver_to_gold", "join_and_smoke_test",
    ],
    label: "Load",
    sub: "Nạp vào Lakehouse",
    icon: "loadData",
    iconClass: "bg-amber-50 text-amber-600",
    tone: "gold",
    dot: "bg-gold-500",
    number: "border-violet-100 bg-violet-50 text-violet-700",
  },
  {
    id: "analyze",
    taskIds: ["kpi_flow.predictive_analysis", "auto_provision_superset"],
    label: "Analyze",
    sub: "Phân tích dữ liệu",
    icon: "analyze",
    iconClass: "bg-cyan-50 text-cyan-600",
    tone: "lake",
    dot: "bg-lake-500",
    number: "border-emerald-100 bg-emerald-50 text-emerald-700",
  },
];

const taskForStage = (tasks = [], taskIds) => {
  const matches = tasks.filter((task) => taskIds.includes(task.task_id));
  const priority = ["failed", "upstream_failed", "running", "up_for_retry", "queued", "success"];
  return priority.map((state) => matches.find((task) => task.state === state)).find(Boolean) || matches[0];
};

const formatFileSize = (bytes) => {
  const value = Number(bytes);
  if (!Number.isFinite(value) || value < 0) return "—";
  if (value === 0) return "0 MB";
  const megabytes = value / 1024 ** 2;
  if (megabytes < 0.01) return "< 0.01 MB";
  return `${megabytes.toFixed(megabytes >= 10 ? 1 : 2)} MB`;
};

const IMPORT_STATUS_LABELS = {
  PENDING: "Đang chuẩn bị",
  PROCESSING: "Đang nhập dữ liệu",
  COMPLETED: "Đã hoàn thành",
  PARTIAL_SUCCESS: "Hoàn thành một phần",
  FAILED: "Import thất bại",
  CANCELLED: "Đã hủy",
};

const IMPORT_FILE_STATUS_LABELS = {
  QUEUED: "Đang chờ",
  DOWNLOADING: "Đang tải",
  PROCESSING: "Đang xử lý",
  COMPLETED: "Hoàn thành",
  DUPLICATE: "Đã có dữ liệu",
  FAILED: "Thất bại",
  UNSUPPORTED: "Không hỗ trợ",
  CANCELLED: "Đã hủy",
};

const formatDate = (value) => {
  if (!value) return "—";
  const date = new Date(value);
  return Number.isNaN(date.getTime())
    ? "—"
    : date.toLocaleDateString("vi-VN", { day: "2-digit", month: "2-digit", year: "numeric" });
};

const fileVisual = (filename = "") => {
  const extension = filename.split(".").pop()?.toLowerCase();
  if (extension === "pdf") return { icon: "filePdf", label: "PDF", className: "bg-rose-50 text-rose-600 ring-rose-100" };
  if (["xlsx", "xls"].includes(extension)) {
    return { icon: "fileExcel", label: extension.toUpperCase(), className: "bg-emerald-50 text-emerald-700 ring-emerald-100" };
  }
  if (["csv", "tsv"].includes(extension)) return { icon: "fileExcel", label: extension.toUpperCase(), className: "bg-teal-50 text-teal-700 ring-teal-100" };
  if (extension === "docx") return { icon: "fileWord", label: "DOCX", className: "bg-blue-50 text-blue-700 ring-blue-100" };
  if (extension === "json") return { icon: "fileJson", label: "JSON", className: "bg-amber-50 text-amber-700 ring-amber-100" };
  if (extension === "parquet") return { icon: "fileParquet", label: "PARQ", className: "bg-violet-50 text-violet-700 ring-violet-100" };
  return { icon: "file", label: extension?.toUpperCase() || "FILE", className: "bg-lake-50 text-lake-600 ring-lake-100" };
};

const durationInSeconds = (task, now) => {
  if (!task?.start_date) return null;
  const start = new Date(task.start_date).getTime();
  const end = task.end_date ? new Date(task.end_date).getTime() : now;
  if (!Number.isFinite(start) || !Number.isFinite(end)) return null;
  return Math.max(0, Math.round((end - start) / 1000));
};

const formatDuration = (seconds) => {
  if (seconds === null) return "Chưa chạy";
  if (seconds < 60) return `${seconds} giây`;
  const minutes = Math.floor(seconds / 60);
  const remainder = seconds % 60;
  return `${minutes} phút${remainder ? ` ${remainder} giây` : ""}`;
};

const UserUpload = () => {
  const fileInputRef = useRef(null);
  const [isDragging, setIsDragging] = useState(false);
  const [uploading, setUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [uploadStatus, setUploadStatus] = useState("");
  const [uploadError, setUploadError] = useState("");
  const [history, setHistory] = useState([]);
  const [historyLoading, setHistoryLoading] = useState(false);
  const [refreshingId, setRefreshingId] = useState(null);
  const [now, setNow] = useState(Date.now());
  const [activePipeline, setActivePipeline] = useState(null);
  const [reportFile, setReportFile] = useState("");
  const [reportYear, setReportYear] = useState("");
  const [reportTerm, setReportTerm] = useState("");
  const [inputMode, setInputMode] = useState("upload");
  const [sourceUrl, setSourceUrl] = useState("");
  const [urlManifest, setUrlManifest] = useState(null);
  const [selectedCandidateIds, setSelectedCandidateIds] = useState([]);
  const [urlBusy, setUrlBusy] = useState(false);
  const [activeImportJob, setActiveImportJob] = useState(null);
  const pollingRef = useRef(null);
  const urlPollingRef = useRef(null);
  const dashboardPollingRef = useRef(null);

  const fetchHistory = useCallback(async () => {
    setHistoryLoading(true);
    try {
      const res = await axios.get(`${API_URL}/upload/history`, {
        headers: authHeader(),
      });
      const hist = res.data || [];
      setHistory(hist);

      if (hist[0]?.dag_run_id) {
        try {
          const statusRes = await axios.get(
            `${API_URL}/upload/pipeline-status/${hist[0].dag_run_id}`,
            { headers: authHeader() },
          );
          setActivePipeline(statusRes.data);
        } catch {
          /* Keep history usable while Airflow is temporarily unavailable. */
        }
      }

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
      if (dashboardPollingRef.current) clearInterval(dashboardPollingRef.current);
    };
  }, [fetchHistory]);

  useEffect(() => {
    if (!activePipeline?.tasks?.some((task) => task.state === "running")) return undefined;
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, [activePipeline]);

  const removeHistoryItem = (id, index) => {
    setHistory((current) => current.filter((item, itemIndex) => (item.id ?? itemIndex) !== (id ?? index)));
  };

  const refreshHistoryItem = async (item) => {
    if (!item.dag_run_id) return;
    setRefreshingId(item.id ?? item.dag_run_id);
    try {
      const res = await axios.get(`${API_URL}/upload/pipeline-status/${item.dag_run_id}`, {
        headers: authHeader(),
      });
      setActivePipeline(res.data);
      setHistory((current) => current.map((entry) =>
        entry.id === item.id ? { ...entry, pipeline_status: res.data.state } : entry
      ));
    } catch {
      /* Keep the current row unchanged when Airflow is unavailable. */
    } finally {
      setRefreshingId(null);
    }
  };

  useEffect(() => {
    if (
      activePipeline?.state !== "success" ||
      activePipeline?.dashboard?.imported ||
      !activePipeline?.dag_run_id
    ) {
      if (dashboardPollingRef.current) {
        clearInterval(dashboardPollingRef.current);
        dashboardPollingRef.current = null;
      }
      return undefined;
    }

    // XCom can become visible a fraction after the DAG changes to success.
    // Retry automatically so the user never has to press "Làm mới".
    dashboardPollingRef.current = setInterval(async () => {
      try {
        const res = await axios.get(
          `${API_URL}/upload/pipeline-status/${activePipeline.dag_run_id}`,
          { headers: authHeader() },
        );
        setActivePipeline(res.data);
      } catch {
        /* Keep retrying while this completed run is selected. */
      }
    }, 2000);

    return () => {
      if (dashboardPollingRef.current) {
        clearInterval(dashboardPollingRef.current);
        dashboardPollingRef.current = null;
      }
    };
  }, [
    activePipeline?.state,
    activePipeline?.dashboard?.imported,
    activePipeline?.dag_run_id,
  ]);

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
      setUploadStatus(`Đã tải lên ${file.name}`);
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

  const completedDashboardUrl =
    activePipeline?.state === "success" && activePipeline?.dashboard?.imported
      ? dashboardUrlFor(activePipeline.dashboard.slug)
      : "";
  const reportYears = [...new Set(history.map((item) => {
    const filenameYear = item.filename?.match(/(?:19|20)\d{2}/)?.[0];
    if (filenameYear) return filenameYear;
    const date = item.uploaded_at ? new Date(item.uploaded_at) : null;
    return date && !Number.isNaN(date.getTime()) ? String(date.getFullYear()) : null;
  }).filter(Boolean))].sort((a, b) => Number(b) - Number(a));
  const filteredDashboardUrl = completedDashboardUrl
    ? (() => {
        const url = new URL(completedDashboardUrl);
        if (reportFile) url.searchParams.set("filename", reportFile);
        if (reportYear) url.searchParams.set("year", reportYear);
        if (reportTerm) url.searchParams.set("term", reportTerm);
        return url.toString();
      })()
    : "";
  const importCounts = activeImportJob?.counts || {};
  const importTotal = activeImportJob?.files?.length || 0;
  const importFinished = ["COMPLETED", "DUPLICATE", "FAILED", "UNSUPPORTED", "CANCELLED"]
    .reduce((total, status) => total + (importCounts[status] || 0), 0);
  const importInProgress = (importCounts.DOWNLOADING || 0) + (importCounts.PROCESSING || 0);
  const importProgress = importTotal
    ? Math.round(((importFinished + importInProgress * 0.5) / importTotal) * 100)
    : 0;

  return (
    <div className="flex min-h-screen flex-col bg-slate-50 font-sans">
      <UserTopNavigation />

      <main className="mx-auto w-full max-w-[1680px] flex-1 space-y-5 px-4 py-6 sm:px-6 lg:space-y-5 lg:px-8 lg:py-8">
        <div className="relative overflow-hidden rounded-2xl border border-blue-100 bg-gradient-to-br from-white via-blue-50/60 to-indigo-50 p-5 shadow-card sm:p-7 lg:p-8">
          <span className="relative mb-3 inline-flex rounded-full bg-blue-700 px-3 py-1 text-[10px] font-extrabold uppercase tracking-[0.16em] text-white">File ingestion</span>
          <h1 className="relative font-display text-2xl font-extrabold tracking-tight text-slate-950 sm:text-3xl lg:text-4xl">Tải file dữ liệu</h1>
          <p className="relative mt-3 max-w-2xl text-sm leading-6 text-slate-600 sm:text-base">Tải dữ liệu từ máy tính hoặc đường dẫn URL, theo dõi toàn bộ quá trình xử lý và khám phá kết quả trên dashboard phân tích.</p>
        </div>
        {/* ROW 1 — Upload + History */}
        <section className="grid grid-cols-1 items-stretch gap-5 xl:grid-cols-[7fr_13fr]">
          {/* 1. Upload Card */}
          <Card className="flex h-full min-w-0 flex-col rounded-2xl border border-slate-200 bg-white p-5 shadow-card sm:p-6">
            <div className="mb-3 shrink-0">
              <h2 className="font-display text-lg font-extrabold tracking-tight text-slate-950 sm:text-xl">
                Tải lên dữ liệu
              </h2>
              {/* <p className="text-xs text-ink-400 mt-1">
                Hệ thống sẽ tự động đưa vào Bronze Zone và kích hoạt Pipeline
              </p> */}
            </div>

            <div className="mb-4 grid grid-cols-2 rounded-xl bg-slate-100 p-1 text-xs font-bold sm:text-[13px]">
              <button type="button" onClick={() => setInputMode("upload")}
                className={`rounded-lg px-3 py-2.5 transition ${inputMode === "upload" ? "bg-white text-blue-700 shadow-sm ring-1 ring-slate-200" : "text-slate-500 hover:text-slate-900"}`}>
                Upload file
              </button>
              <button type="button" onClick={() => setInputMode("url")}
                className={`rounded-lg px-3 py-2.5 transition ${inputMode === "url" ? "bg-white text-blue-700 shadow-sm ring-1 ring-slate-200" : "text-slate-500 hover:text-slate-900"}`}>
                Dán đường dẫn URL
              </button>
            </div>

            {inputMode === "url" ? (
              <div className="flex-1 rounded-xl border border-slate-200 bg-slate-50/70 p-4">
                <label className="block text-xs font-semibold text-ink-700">
                  Đường dẫn file
                  <div className="relative mt-2">
                    <input type="url" value={sourceUrl}
                      onChange={(event) => { setSourceUrl(event.target.value); setUrlManifest(null); setSelectedCandidateIds([]); }}
                      placeholder="https://example.com/data/report.pdf"
                      className="w-full rounded-xl border border-slate-200 bg-white py-2.5 pl-3 pr-11 text-sm font-normal outline-none transition focus:border-blue-500 focus:ring-4 focus:ring-blue-100/70" />
                    {sourceUrl && (
                      <button
                        type="button"
                        onClick={() => {
                          setSourceUrl("");
                          setUrlManifest(null);
                          setSelectedCandidateIds([]);
                          setUploadError("");
                        }}
                        aria-label="Xóa đường dẫn URL"
                        title="Xóa đường dẫn"
                        className="absolute right-2 top-1/2 inline-flex h-7 w-7 -translate-y-1/2 items-center justify-center rounded-lg text-ink-400 transition hover:bg-ink-100 hover:text-ink-700"
                      >
                        <Icon name="x" className="h-4 w-4" />
                      </button>
                    )}
                  </div>
                </label>
                <p className="mt-2 text-[11px] text-ink-400">Hỗ trợ file HTTP trực tiếp và Google Drive file/folder · tối đa 100 MB/file.</p>
                {!urlManifest ? (
                  <button type="button" disabled={!sourceUrl || urlBusy} onClick={scanSourceUrl}
                    className="mt-4 w-full rounded-lg bg-blue-700 px-4 py-2.5 text-xs font-bold text-white transition hover:-translate-y-0.5 hover:bg-blue-800 hover:shadow-md disabled:translate-y-0 disabled:opacity-50">
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
                        <span className="shrink-0 font-data font-semibold text-ink-500">
                          {file.size === null || file.size === undefined ? "Chưa xác định" : formatFileSize(file.size)}
                        </span>
                      </div>
                    ))}
                    {urlManifest.files[0]?.reason && <p className="mt-2 text-xs text-rose-600">{urlManifest.files[0].reason}</p>}
                    <button type="button" disabled={!selectedCandidateIds.length || urlBusy} onClick={importSourceUrl}
                      className="mt-3 w-full rounded-lg bg-blue-700 px-4 py-2.5 text-xs font-bold text-white transition hover:-translate-y-0.5 hover:bg-blue-800 hover:shadow-md disabled:translate-y-0 disabled:opacity-50">
                      {urlBusy ? "Đang import..." : "Import file"}
                    </button>
                  </div>
                )}
                {false && activeImportJob && (
                  <div className="mt-4 rounded-2xl bg-white p-4 text-xs shadow-[0_10px_28px_-18px_rgba(15,23,42,0.38)] ring-1 ring-ink-100">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <div className="flex items-center gap-2">
                        <span className={`h-2.5 w-2.5 rounded-full ${["PENDING", "PROCESSING"].includes(activeImportJob.status) ? "animate-pulse bg-cobalt-500" : activeImportJob.status === "COMPLETED" ? "bg-emerald-500" : "bg-amber-500"}`} />
                        <span className="text-sm font-bold text-navy-950">
                          {IMPORT_STATUS_LABELS[activeImportJob.status] || activeImportJob.status}
                        </span>
                      </div>
                      <span className="rounded-full bg-ink-50 px-2.5 py-1 font-data font-semibold text-ink-500">{importTotal} file</span>
                    </div>

                    <div className="mt-3 h-2 overflow-hidden rounded-full bg-ink-100">
                      <div
                        className="h-full rounded-full bg-cobalt-600 transition-all duration-500"
                        style={{ width: `${["COMPLETED", "PARTIAL_SUCCESS", "FAILED", "CANCELLED"].includes(activeImportJob.status) ? 100 : importProgress}%` }}
                      />
                    </div>
                    <div className="mt-1.5 flex items-center justify-between text-[10px] font-medium text-ink-400">
                      <span>{importFinished}/{importTotal} file đã xử lý</span>
                      <span>{["COMPLETED", "PARTIAL_SUCCESS", "FAILED", "CANCELLED"].includes(activeImportJob.status) ? 100 : importProgress}%</span>
                    </div>

                    <div className="mt-3 grid grid-cols-2 gap-2 sm:grid-cols-3">
                      {Object.entries(importCounts).map(([status, count]) => (
                        <span key={status} className="rounded-lg bg-ink-50 px-2.5 py-2 text-center font-semibold text-ink-600">
                          <span className="block text-sm font-bold text-navy-900">{count}</span>
                          {IMPORT_FILE_STATUS_LABELS[status] || status}
                        </span>
                      ))}
                    </div>
                    {(activeImportJob.counts?.DUPLICATE || 0) > 0 && (
                      <p className="mt-2 rounded-lg border border-sky-100 bg-white px-3 py-2 text-[11px] text-sky-700">
                        {"File c\u00f3 n\u1ed9i dung gi\u1ed1ng h\u1ec7t file \u0111\u00e3 x\u1eed l\u00fd th\u00e0nh c\u00f4ng, n\u00ean h\u1ec7 th\u1ed1ng d\u00f9ng l\u1ea1i k\u1ebft qu\u1ea3 v\u00e0 kh\u00f4ng ch\u1ea1y pipeline l\u1eb7p l\u1ea1i."}
                      </p>
                    )}
                    <div className="mt-4 flex flex-wrap gap-2 border-t border-ink-100 pt-3">
                      {["FAILED", "PARTIAL_SUCCESS"].includes(activeImportJob.status) && (
                        <button type="button" onClick={retryImportJob} className="rounded-lg bg-cobalt-600 px-3.5 py-2 font-bold text-white shadow-sm">
                          Thử lại file lỗi
                        </button>
                      )}
                      {["PENDING", "PROCESSING"].includes(activeImportJob.status) && (
                        <button type="button" onClick={cancelImportJob} className="rounded-lg bg-rose-50 px-3.5 py-2 font-bold text-rose-600 transition hover:bg-rose-100">
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
              accept=".pdf,.docx,.json,.csv,.xlsx,.xls"
              className="hidden"
            />

            <div
              onClick={() => !uploading && fileInputRef.current.click()}
              onDragOver={onDragOver}
              onDragLeave={onDragLeave}
              onDrop={onDrop}
              className={`group relative flex min-h-[210px] flex-1 flex-col items-center justify-center overflow-hidden rounded-xl border-2 border-dashed px-3 py-5 text-center transition-all duration-300 sm:min-h-[240px] sm:px-5 sm:py-6 ${
                uploading
                  ? "cursor-wait border-slate-200 bg-slate-50 opacity-90"
                  : "cursor-pointer border-blue-200 bg-blue-50/40 hover:border-blue-400 hover:bg-blue-50/70 hover:shadow-md"
              } ${isDragging ? "scale-[1.01] border-blue-500 bg-blue-50" : ""}`}
            >
              <div
                className={`w-12 h-12 mb-3 rounded-2xl flex items-center justify-center transition-all duration-300 ${
                  isDragging
                    ? "scale-110 bg-blue-700 text-white"
                    : "border border-blue-100 bg-white text-blue-700 shadow-sm group-hover:scale-105"
                }`}
              >
                <Icon
                  name={uploading ? "clock" : "uploadCloud"}
                  className="w-5 h-5"
                />
              </div>
              <p className="text-sm font-bold text-navy-950 sm:text-base">
                {uploading
                  ? "Đang tải lên và xử lý..."
                  : "Kéo thả file hoặc bấm để chọn"}
              </p>
              <p className="mt-1 max-w-full break-words text-[10px] font-medium text-ink-500 font-data sm:text-[11px]">
                Hỗ trợ: PDF, DOCX, JSON, CSV, EXCEL
              </p>
              <span className="mt-2 inline-flex items-center rounded-full bg-blue-50 px-3 py-1 text-[11px] font-bold text-blue-700 shadow-sm">
                Tối đa 100 MB mỗi file
              </span>
              {!uploading && (
                <span className="mt-4 inline-flex h-10 items-center justify-center rounded-lg bg-blue-700 px-5 text-xs font-bold text-white shadow-md transition group-hover:-translate-y-0.5 group-hover:bg-blue-800 group-hover:shadow-lg">
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
              <div className="mt-4 flex min-w-0 items-start gap-2.5 rounded-xl border border-emerald-200 bg-emerald-50 p-3 text-sm text-emerald-700">
                <Icon name="checkCircle" className="w-4 h-4 mt-0.5 shrink-0" />
                <span className="min-w-0 break-all font-medium leading-5">{uploadStatus}</span>
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
          <Card className="flex h-full min-w-0 flex-col overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-card">
            <div className="flex shrink-0 items-center justify-between gap-3 border-b border-slate-200 bg-white px-5 py-4 sm:px-6">
              <div className="min-w-0">
                <h3 className="font-display text-lg font-extrabold tracking-tight text-slate-950 sm:text-xl">
                  Lịch sử tải lên
                </h3>
                {/* <p className="text-[11px] text-ink-400 mt-0.5">
                  Tự động đồng bộ trạng thái Pipeline mỗi 5 giây
                </p> */}
              </div>
              <button
                onClick={fetchHistory}
                className="flex shrink-0 items-center gap-1.5 rounded-lg bg-blue-50 px-3 py-2 text-xs font-bold text-blue-700 shadow-sm transition hover:-translate-y-0.5 hover:bg-blue-100"
                disabled={historyLoading}
              >
                <Icon
                  name="refresh"
                  className={`w-3.5 h-3.5 ${historyLoading ? "animate-spin" : ""}`}
                />
                {historyLoading ? "Đang tải..." : "Làm mới"}
              </button>
            </div>

            <div className="min-h-[280px] max-h-[440px] flex-1 overflow-y-auto bg-slate-50 p-2.5 sm:min-h-[340px] sm:p-3 xl:h-[382px] xl:min-h-0">
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
                    const visual = fileVisual(item.filename);
                    return (
                      <div
                        key={item.id || i}
                        className="flex flex-col gap-2 rounded-xl border border-slate-200 bg-white p-3 transition-all duration-300 hover:-translate-y-0.5 hover:border-blue-200 hover:shadow-md"
                      >
                        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
                          <div className="flex items-center gap-3 min-w-0">
                            <div
                              className={`flex h-11 w-12 shrink-0 flex-col items-center justify-center gap-0.5 rounded-lg shadow-sm ring-1 ${visual.className}`}
                              title={`File ${item.filename?.split(".").pop()?.toUpperCase() || "dữ liệu"}`}
                            >
                              <Icon name={visual.icon} className="h-5 w-5" strokeWidth={1.8} />
                              <span className="font-data text-[8px] font-extrabold leading-none tracking-tight">{visual.label}</span>
                            </div>
                            <div className="min-w-0">
                              <h4
                                className="truncate text-sm font-bold text-navy-950"
                                title={item.filename}
                              >
                                {item.filename}
                              </h4>
                              <div className="mt-1 flex flex-wrap items-center gap-x-2 gap-y-1 text-[11px] font-medium text-ink-500 font-data">
                                <span className="font-medium text-ink-500">{formatFileSize(item.file_size_bytes)}</span>
                                <span className="text-ink-200">•</span>
                                <span>{formatDate(item.uploaded_at)}</span>
                              </div>
                            </div>
                          </div>

                          <div className="flex shrink-0 items-center justify-end gap-1 border-t border-ink-50 pt-2 sm:border-0 sm:pt-0 sm:gap-2">
                            <Badge tone={st.tone}>{st.label}</Badge>
                            <button
                              type="button"
                              onClick={() => refreshHistoryItem(item)}
                              disabled={!item.dag_run_id || refreshingId === (item.id ?? item.dag_run_id)}
                              aria-label={`Tải lại trạng thái ${item.filename}`}
                              title="Tải lại trạng thái"
                              className="inline-flex h-8 w-8 items-center justify-center rounded-lg text-lake-600 transition hover:bg-lake-50 disabled:cursor-not-allowed disabled:opacity-40"
                            >
                              <Icon name="refresh" className={`h-4 w-4 ${refreshingId === (item.id ?? item.dag_run_id) ? "animate-spin" : ""}`} />
                            </button>
                            <button
                              type="button"
                              onClick={() => removeHistoryItem(item.id, i)}
                              aria-label={`Ẩn ${item.filename} khỏi giao diện`}
                              title="Chỉ xóa khỏi giao diện"
                              className="inline-flex h-8 w-8 items-center justify-center rounded-lg text-ink-400 transition hover:bg-rose-50 hover:text-rose-600"
                            >
                              <Icon name="trash" className="h-4 w-4" />
                            </button>
                          </div>
                        </div>
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
          <Card className="min-w-0 rounded-2xl border border-slate-200 bg-white p-5 shadow-card sm:p-6">
            <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between mb-6">
              <div>
                <h3 className="font-display text-lg font-extrabold tracking-tight text-navy-950 sm:text-xl">
                  Tiến trình Pipeline
                </h3>
                {/* <p className="text-xs text-ink-400 mt-0.5">
                  Airflow Orchestration
                </p> */}
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

            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 sm:gap-4 2xl:grid-cols-4 2xl:gap-5">
              {PIPELINE_TASKS.map((pt, index) => {
                const t = taskForStage(activePipeline.tasks, pt.taskIds);
                const state = t?.state === "upstream_failed" ? "failed" : (t?.state || "pending");
                const duration = durationInSeconds(t, now);

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
                    className={`relative min-w-0 overflow-hidden rounded-xl border p-4 ${cardBorder} ${cardBg} transition-all duration-300 hover:-translate-y-1 hover:shadow-md 2xl:p-5`}
                  >
                    <div className="flex items-start justify-between gap-3">
                      <div className="flex min-w-0 items-start gap-3">
                        <span className={`relative flex h-10 w-10 shrink-0 items-center justify-center rounded-xl shadow-sm ${pt.iconClass}`}>
                          <Icon
                            name={pt.icon}
                            className={`h-5 w-5 ${state === "running" ? (pt.id === "transform" ? "animate-spin" : "animate-pulse") : ""}`}
                          />
                          <span className={`absolute -right-1 -top-1 h-3 w-3 rounded-full border-2 border-white ${dotClass}`} />
                        </span>
                        <div className="min-w-0">
                          <p
                            className={`text-sm font-bold ${state === "pending" ? "text-ink-400" : "text-navy-950"}`}
                          >
                            <span className={`mr-2 inline-flex h-6 min-w-6 items-center justify-center rounded-md border px-1 font-data text-[10px] ${pt.number}`}>B{index + 1}</span>
                            {pt.label}
                          </p>
                          <p className="mt-1 truncate text-[11px] font-medium text-ink-500">{pt.sub}</p>
                          <p
                            className={`mt-1.5 text-[11px] font-data font-semibold ${textColor}`}
                          >
                            {stateText} · {formatDuration(duration)}
                          </p>
                        </div>
                      </div>
                      <Icon
                        name={icon}
                        className={`h-4 w-4 shrink-0 ${textColor} ${state === "running" ? "animate-pulse" : ""}`}
                      />
                    </div>
                    <div className="mt-4 h-1.5 overflow-hidden rounded-full bg-ink-100">
                      <div
                        className={`h-full rounded-full transition-all duration-500 ${state === "failed" ? "bg-rose-500" : state === "success" ? "bg-emerald-500" : state === "running" ? "bg-lake-500 animate-pulse" : "bg-ink-200"}`}
                        style={{ width: state === "success" || state === "failed" ? "100%" : state === "running" ? "65%" : "0%" }}
                      />
                    </div>
                  </div>
                );
              })}
            </div>
          </Card>
        )}

        {/* ROW 3 — Superset Analytics */}
        {activePipeline?.state === "success" && (
        <Card className="flex flex-col overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-card">
          <div className="flex shrink-0 flex-col gap-3 border-b border-slate-200 bg-white px-5 py-4 sm:flex-row sm:items-center sm:justify-between sm:px-6">
            <div>
              <h3 className="font-display text-lg font-extrabold tracking-tight text-slate-950 sm:text-xl">
                Báo cáo Phân tích
              </h3>
              {/* <p className="text-[11px] text-ink-400 mt-0.5 font-data">
                Gold Layer · Apache Superset
              </p> */}
            </div>
            {completedDashboardUrl && (
            <a
              href={filteredDashboardUrl}
              target="_blank"
              rel="noreferrer"
              className="self-start sm:self-auto text-xs text-lake-700 hover:text-lake-800 bg-lake-50 hover:bg-lake-100 px-3 py-1.5 rounded-lg transition font-semibold flex items-center gap-1.5"
            >
              Mở tab mới
              <Icon name="externalLink" className="w-3.5 h-3.5" />
            </a>
            )}
          </div>
          <div className="relative bg-slate-50 p-2 sm:p-3">
            {completedDashboardUrl ? (
              <div className="flex min-w-0 flex-col gap-3 lg:flex-row lg:items-start">
                <aside className="w-full shrink-0 rounded-xl bg-white p-5 shadow-card ring-1 ring-ink-100 lg:sticky lg:top-20 lg:w-72 xl:w-80">
                  <div className="mb-4 flex items-center gap-2">
                    <span className="flex h-9 w-9 items-center justify-center rounded-lg bg-cobalt-50 text-cobalt-700">
                      <Icon name="filter" className="h-[18px] w-[18px]" />
                    </span>
                    <div>
                      <p className="text-base font-bold text-navy-950">Bộ lọc báo cáo</p>
                      {/* <p className="text-[10px] text-ink-400">Lọc dữ liệu trên Superset</p> */}
                    </div>
                  </div>

                  <div className="grid grid-cols-1 gap-3 sm:grid-cols-3 lg:grid-cols-1">
                    <label className="text-sm font-bold text-ink-700">
                      Tên file
                      <select
                        value={reportFile}
                        onChange={(event) => setReportFile(event.target.value)}
                        className="mt-2 w-full rounded-lg border border-ink-200 bg-white px-3 py-3 text-sm font-medium text-ink-700 outline-none transition focus:border-cobalt-400 focus:ring-2 focus:ring-cobalt-100"
                      >
                        <option value="">Tất cả file</option>
                        {[...new Map(history.map((item) => [item.filename, item])).values()].map((item) => (
                          <option key={item.id || item.filename} value={item.filename}>{item.filename}</option>
                        ))}
                      </select>
                    </label>

                    <label className="text-sm font-bold text-ink-700">
                      Năm
                      <select
                        value={reportYear}
                        onChange={(event) => setReportYear(event.target.value)}
                        className="mt-2 w-full rounded-lg border border-ink-200 bg-white px-3 py-3 text-sm font-medium text-ink-700 outline-none transition focus:border-cobalt-400 focus:ring-2 focus:ring-cobalt-100"
                      >
                        <option value="">Tất cả năm</option>
                        {reportYears.map((year) => <option key={year} value={year}>{year}</option>)}
                      </select>
                    </label>

                    <label className="text-sm font-bold text-ink-700">
                      Kỳ
                      <select
                        value={reportTerm}
                        onChange={(event) => setReportTerm(event.target.value)}
                        className="mt-2 w-full rounded-lg border border-ink-200 bg-white px-3 py-3 text-sm font-medium text-ink-700 outline-none transition focus:border-cobalt-400 focus:ring-2 focus:ring-cobalt-100"
                      >
                        <option value="">Tất cả kỳ</option>
                        <option value="1">Kỳ 1</option>
                        <option value="2">Kỳ 2</option>
                        <option value="3">Kỳ 3</option>
                      </select>
                    </label>
                  </div>

                  {(reportFile || reportYear || reportTerm) && (
                    <button
                      type="button"
                      onClick={() => { setReportFile(""); setReportYear(""); setReportTerm(""); }}
                      className="mt-5 flex w-full items-center justify-center gap-1.5 rounded-lg bg-ink-50 px-3 py-3 text-sm font-bold text-ink-600 transition hover:bg-ink-100"
                    >
                      <Icon name="x" className="h-3.5 w-3.5" />
                      Xóa bộ lọc
                    </button>
                  )}
                </aside>

                <iframe
                  key={filteredDashboardUrl}
                  src={filteredDashboardUrl}
                  title={activePipeline.dashboard.title || "Superset Dashboard"}
                  className="h-[480px] min-w-0 flex-1 rounded-xl border border-ink-100 bg-white shadow-inner sm:h-[600px] lg:h-[720px] xl:h-[820px]"
                  allowFullScreen
                />
              </div>
            ) : (
              <div className="flex min-h-48 items-center justify-center rounded-xl border border-amber-200 bg-amber-50 px-6 text-center text-sm font-medium text-amber-800">
                Đang đồng bộ dashboard từ Superset, giao diện sẽ tự động hiển thị ngay khi sẵn sàng...
              </div>
            )}
          </div>
        </Card>
        )}
      </main>
    </div>
  );
};

export default UserUpload;
