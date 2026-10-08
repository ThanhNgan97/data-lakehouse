import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import axios from "axios";
import {
  Activity, BarChart3, Check, CheckCircle2, ChevronRight,
  Circle, CloudCog, Database, Download, ExternalLink, FileJson, HardDrive,
  Info, Loader2, LockKeyhole, Network, RefreshCw, Server, ShieldCheck,
  Sparkles, UploadCloud, X, XCircle,
} from "lucide-react";
import UserTopNavigation from "../components/UserTopNavigation";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000/api";
const BRIDGE_URL = import.meta.env.VITE_DB_PROVISIONER_BRIDGE_URL || "http://localhost:9090";
const TOOL_URL = import.meta.env.VITE_DB_PROVISIONER_TOOL_URL || "http://localhost:8085";
const ONBOARD_URL = `${BRIDGE_URL.replace(/\/$/, "")}/api/v1/airbyte/onboard-source`;
const socketUrl = (base, path) => `${base.replace(/^http/, "ws").replace(/\/$/, "")}${path}`;
const SUPERSET_ORIGIN = new URL(import.meta.env.VITE_SUPERSET_URL || "http://localhost:8088").origin;
const STORAGE_KEY = "database-onboarding-session";
const authHeader = () => ({ Authorization: `Bearer ${localStorage.getItem("token")}` });

const stages = [
  { id: "connect", label: "Kết nối nguồn", detail: "Airbyte đọc schema", icon: Network, taskIds: [] },
  { id: "ingest", label: "Đồng bộ dữ liệu", detail: "Database → Bronze", icon: CloudCog, taskIds: ["ai_semantic_profiler"] },
  { id: "process", label: "Chuẩn hóa", detail: "Silver & Gold", icon: Sparkles, taskIds: ["branch_router", "relational_flow.process_relational_context"] },
  { id: "visualize", label: "Trực quan hóa", detail: "Superset dashboard", icon: BarChart3, taskIds: ["join_and_smoke_test", "auto_provision_superset"] },
];

const readStoredSession = () => {
  try { return JSON.parse(localStorage.getItem(STORAGE_KEY) || "null"); }
  catch { return null; }
};

const statusFromTasks = (tasks, ids) => {
  const matches = tasks.filter((task) => ids.includes(task.task_id));
  if (matches.some((task) => ["failed", "upstream_failed"].includes(task.state))) return "failed";
  if (matches.some((task) => ["running", "queued", "up_for_retry"].includes(task.state))) return "running";
  if (matches.length && matches.every((task) => ["success", "skipped"].includes(task.state))) return "success";
  return "pending";
};

const StatusPill = ({ status, children }) => {
  const styles = {
    online: "border-emerald-200 bg-emerald-50 text-emerald-700",
    success: "border-emerald-200 bg-emerald-50 text-emerald-700",
    checking: "border-amber-200 bg-amber-50 text-amber-700",
    running: "border-blue-200 bg-blue-50 text-blue-700",
    outdated: "border-amber-200 bg-amber-50 text-amber-700",
    offline: "border-slate-200 bg-slate-50 text-slate-600",
    failed: "border-rose-200 bg-rose-50 text-rose-700",
  };
  return <span className={`inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-xs font-bold ${styles[status] || styles.offline}`}><span className={`h-1.5 w-1.5 rounded-full bg-current ${status === "checking" || status === "running" ? "animate-pulse" : ""}`} />{children}</span>;
};

export default function UserDatabase() {
  const inputRef = useRef(null);
  const pollRef = useRef(null);
  const [bridgeStatus, setBridgeStatus] = useState("checking");
  const [toolStatus, setToolStatus] = useState("checking");
  const [liveSource, setLiveSource] = useState(null);
  const [showManualFallback, setShowManualFallback] = useState(false);
  const [bundle, setBundle] = useState(null);
  const [bundleFile, setBundleFile] = useState("");
  const [dragging, setDragging] = useState(false);
  const [connecting, setConnecting] = useState(false);
  const [downloading, setDownloading] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [session, setSession] = useState(readStoredSession);
  const [pipeline, setPipeline] = useState(null);
  const [dashboardLoading, setDashboardLoading] = useState(true);

  const checkBridge = useCallback(async () => {
    setBridgeStatus("checking");
    try {
      const response = await fetch(`${BRIDGE_URL.replace(/\/$/, "")}/api/v1/airbyte/health`, { signal: AbortSignal.timeout(3500) });
      setBridgeStatus(response.ok ? "online" : "offline");
    } catch { setBridgeStatus("offline"); }
  }, []);

  const fetchPipeline = useCallback(async (jobId) => {
    if (!jobId) return;
    try {
      const { data } = await axios.get(`${API_URL}/upload/pipeline-status/airbyte__${jobId}`, { headers: authHeader() });
      setPipeline(data);
      if (["success", "failed"].includes(data.state)) {
        clearInterval(pollRef.current);
        pollRef.current = null;
      }
    } catch {
      // Airbyte can still be syncing before its Airflow DAG exists.
      setPipeline((current) => current || { state: "queued", tasks: [] });
    }
  }, []);

  useEffect(() => { checkBridge(); }, [checkBridge]);
  useEffect(() => {
    let disposed = false;
    let toolSocket;
    let bridgeSocket;
    let toolRetry;
    let bridgeRetry;

    const connectToolSocket = () => {
      if (disposed) return;
      setToolStatus((current) => current === "outdated" ? current : "checking");
      toolSocket = new WebSocket(socketUrl(TOOL_URL, "/api/portal-events"));
      toolSocket.onopen = () => setToolStatus("online");
      toolSocket.onmessage = (event) => {
        try {
          const state = JSON.parse(event.data);
          if (state.database_engine || state.table_count) setLiveSource(state);
          setToolStatus("online");
        } catch { /* Ignore malformed local events. */ }
      };
      toolSocket.onerror = () => toolSocket.close();
      toolSocket.onclose = () => {
        if (disposed) return;
        fetch(`${TOOL_URL.replace(/\/$/, "")}/api/portal-state`, {
          mode: "no-cors",
          signal: AbortSignal.timeout(1800),
        }).then(() => {
          if (!disposed) setToolStatus("outdated");
        }).catch(() => {
          if (!disposed) setToolStatus("offline");
        });
        toolRetry = window.setTimeout(connectToolSocket, 4000);
      };
    };

    const connectBridgeSocket = () => {
      if (disposed) return;
      bridgeSocket = new WebSocket(socketUrl(BRIDGE_URL, "/api/v1/airbyte/events"));
      bridgeSocket.onopen = () => setBridgeStatus("online");
      bridgeSocket.onmessage = (event) => {
        try {
          const state = JSON.parse(event.data);
          if (!state.airbyte_job_id) return;
          const next = { ...state, connected_at: state.updated_at || new Date().toISOString(), realtime: true };
          setSession(next);
          setPipeline((current) => current || { state: "queued", tasks: [] });
          localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
          if (state.message) setMessage(state.message);
        } catch { /* Ignore malformed bridge events. */ }
      };
      bridgeSocket.onerror = () => bridgeSocket.close();
      bridgeSocket.onclose = () => {
        if (disposed) return;
        setBridgeStatus("offline");
        bridgeRetry = window.setTimeout(connectBridgeSocket, 4000);
      };
    };

    connectToolSocket();
    connectBridgeSocket();
    return () => {
      disposed = true;
      clearTimeout(toolRetry); clearTimeout(bridgeRetry);
      toolSocket?.close(); bridgeSocket?.close();
    };
  }, []);
  useEffect(() => {
    if (!session?.airbyte_job_id) return undefined;
    fetchPipeline(session.airbyte_job_id);
    pollRef.current = window.setInterval(() => fetchPipeline(session.airbyte_job_id), 6000);
    return () => { if (pollRef.current) clearInterval(pollRef.current); };
  }, [session?.airbyte_job_id, fetchPipeline]);

  const loadBundle = async (file) => {
    setError("");
    if (!file) return;
    if (!file.name.toLowerCase().endsWith(".json")) {
      setError("Vui lòng chọn đúng file airbyte_bundle_*.json do db-provisioner tạo ra.");
      return;
    }
    try {
      const parsed = JSON.parse(await file.text());
      if (!parsed.tenant_id || !parsed.database_engine || !parsed.connection || !Array.isArray(parsed.exposed_views)) {
        throw new Error("invalid");
      }
      setBundle(parsed);
      setBundleFile(file.name);
      setMessage("Bundle hợp lệ và sẵn sàng kết nối.");
    } catch {
      setBundle(null);
      setBundleFile("");
      setError("Bundle không hợp lệ hoặc thiếu thông tin kết nối bắt buộc.");
    }
  };

  const downloadTool = async () => {
    setDownloading(true); setError("");
    try {
      const response = await axios.get(`${API_URL}/database/tool/windows`, { headers: authHeader(), responseType: "blob" });
      const url = URL.createObjectURL(response.data);
      const anchor = document.createElement("a");
      anchor.href = url; anchor.download = "db-provisioner.exe"; anchor.click();
      URL.revokeObjectURL(url);
    } catch (requestError) {
      setError(requestError.response?.data?.detail || "Không thể tải db-provisioner lúc này.");
    } finally { setDownloading(false); }
  };

  const connect = async () => {
    if (!bundle) return;
    setConnecting(true); setError(""); setMessage("Đang gửi cấu hình an toàn đến Airbyte...");
    try {
      const response = await axios.post(ONBOARD_URL, bundle, { timeout: 190000 });
      const next = { ...response.data, bundle_file: bundleFile, connected_at: new Date().toISOString() };
      setSession(next); setPipeline({ state: "queued", tasks: [] });
      localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
      setMessage(response.data.message || "Kết nối thành công. Airbyte đang đồng bộ dữ liệu.");
    } catch (requestError) {
      setError(requestError.response?.data?.error || requestError.response?.data?.detail || "Không thể kết nối. Hãy mở db-provisioner bridge và thử lại.");
      checkBridge();
    } finally { setConnecting(false); }
  };

  const resetSession = () => {
    localStorage.removeItem(STORAGE_KEY);
    setSession(null); setPipeline(null); setBundle(null); setBundleFile(""); setMessage(""); setError("");
  };

  const dashboardUrl = pipeline?.dashboard?.slug
    ? `${SUPERSET_ORIGIN}/superset/dashboard/${encodeURIComponent(pipeline.dashboard.slug)}/?standalone=3`
    : "";
  const resources = session?.discovered_resources || liveSource?.tables?.map((name) => ({ resource_name: name })) || bundle?.exposed_views || [];
  const databaseEngine = session?.database_engine || liveSource?.database_engine || bundle?.database_engine;
  const tasks = pipeline?.tasks || [];
  const stageStates = useMemo(() => stages.map((stage, index) => {
    if (index === 0) return session || liveSource ? "success" : connecting ? "running" : "pending";
    if (!session) return "pending";
    if (!pipeline || pipeline.state === "queued") return index === 1 ? "running" : "pending";
    return statusFromTasks(tasks, stage.taskIds);
  }), [session, liveSource, connecting, pipeline, tasks]);

  return (
    <div className="min-h-screen bg-slate-50">
      <UserTopNavigation /> 
      <main className="mx-auto w-full max-w-[1480px] px-4 py-6 sm:px-6 lg:px-8 lg:py-8">
        <section className="relative overflow-hidden rounded-2xl border border-blue-100 bg-gradient-to-br from-white via-blue-50/60 to-indigo-50 p-5 shadow-card sm:p-7 lg:p-8">
          <div className="absolute -right-20 -top-24 h-64 w-64 rounded-full bg-blue-200/30 blur-3xl" />
          <div className="relative flex flex-col gap-6 xl:flex-row xl:items-center xl:justify-between">
            <div className="max-w-3xl">
              <div className="mb-3 flex flex-wrap items-center gap-2">
                <span className="rounded-full bg-blue-700 px-3 py-1 text-[10px] font-extrabold uppercase tracking-[0.16em] text-white">Database ingestion</span>
                <StatusPill status={bridgeStatus}>{bridgeStatus === "online" ? "Bridge sẵn sàng" : bridgeStatus === "checking" ? "Đang kiểm tra bridge" : "Bridge chưa kết nối"}</StatusPill>
              </div>
              <h1 className="font-display text-2xl font-extrabold tracking-tight text-slate-950 sm:text-3xl lg:text-4xl">Kết nối database với Lakehouse</h1>
              <p className="mt-3 max-w-2xl text-sm leading-6 text-slate-600 sm:text-base">Thiết lập quyền đọc an toàn bằng db-provisioner, đồng bộ qua Airbyte và theo dõi toàn bộ hành trình dữ liệu đến dashboard Superset.</p>
            </div>
            <div className="grid grid-cols-3 gap-2 sm:gap-3 xl:min-w-[470px]">
              {[
                [Database, databaseEngine || "—", "Loại database"],
                [HardDrive, resources.length || "—", "Bảng dữ liệu"],
                [Activity, pipeline?.state === "success" ? "Hoàn tất" : session ? "Đang chạy" : "Chưa chạy", "Pipeline"],
              ].map(([MetricIcon, value, label]) => <div key={label} className="rounded-xl border border-white/80 bg-white/80 p-3 shadow-sm backdrop-blur transition duration-300 hover:-translate-y-1 hover:shadow-md sm:p-4"><MetricIcon className="mb-3 h-5 w-5 text-blue-700" /><p className="truncate text-base font-extrabold text-slate-900 sm:text-lg">{value}</p><p className="mt-0.5 text-[10px] font-medium text-slate-500 sm:text-xs">{label}</p></div>)}
            </div>
          </div>
        </section>

        <section className="mt-5 grid min-w-0 gap-5 xl:grid-cols-[minmax(0,1.45fr)_minmax(360px,.55fr)]">
          <div className="min-w-0 rounded-2xl border border-slate-200 bg-white p-5 shadow-card sm:p-6">
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div><p className="text-xs font-bold uppercase tracking-wider text-blue-700">Thiết lập nguồn dữ liệu</p><h2 className="mt-1 font-display text-xl font-extrabold text-slate-950">Bắt đầu trong 3 bước</h2></div>
              {session && <button onClick={resetSession} className="rounded-lg px-3 py-2 text-xs font-semibold text-slate-500 transition hover:bg-slate-100 hover:text-slate-800">Kết nối nguồn khác</button>}
            </div>

            <div className="mt-6 grid gap-4 lg:grid-cols-3">
              <article className="group flex min-h-48 flex-col rounded-xl border border-slate-200 p-5 transition duration-300 hover:-translate-y-1 hover:border-blue-200 hover:shadow-elevated">
                <div className="flex items-center justify-between"><span className="flex h-10 w-10 items-center justify-center rounded-xl bg-blue-50 text-blue-700"><Download size={20} /></span><span className="font-data text-[11px] font-bold text-slate-400">01</span></div>
                <h3 className="mt-4 font-bold text-slate-900">Tải công cụ</h3><p className="mt-2 flex-1 text-xs leading-5 text-slate-500">Chạy db-provisioner trên máy có quyền truy cập database để tạo tài khoản chỉ đọc.</p>
                <button onClick={downloadTool} disabled={downloading} className="mt-4 flex w-full items-center justify-center gap-2 rounded-lg bg-blue-700 px-3 py-2.5 text-xs font-bold text-white transition hover:bg-blue-800 hover:shadow-md disabled:opacity-60">{downloading ? <Loader2 size={15} className="animate-spin" /> : <Download size={15} />}{downloading ? "Đang chuẩn bị..." : "Tải cho Windows"}</button>
              </article>

              <article className="group flex min-h-48 flex-col rounded-xl border border-slate-200 p-5 transition duration-300 hover:-translate-y-1 hover:border-indigo-200 hover:shadow-elevated">
                <div className="flex items-center justify-between"><span className="flex h-10 w-10 items-center justify-center rounded-xl bg-indigo-50 text-indigo-700"><ShieldCheck size={20} /></span><span className="font-data text-[11px] font-bold text-slate-400">02</span></div>
                <h3 className="mt-4 font-bold text-slate-900">Cấu hình & cấp quyền</h3><p className="mt-2 flex-1 text-xs leading-5 text-slate-500">Mở tool, kiểm tra kết nối, chọn bảng cần đồng bộ và xuất bundle cấu hình.</p>
                <div className="mt-4 flex items-center gap-2 rounded-lg bg-slate-50 px-3 py-2.5 text-[11px] font-semibold text-slate-600"><LockKeyhole size={14} className="text-emerald-600" />Bundle chỉ gửi tới bridge cục bộ</div>
              </article>

              <article className="group flex min-h-48 flex-col rounded-xl border border-slate-200 p-5 transition duration-300 hover:-translate-y-1 hover:border-cyan-200 hover:shadow-elevated">
                <div className="flex items-center justify-between"><span className="flex h-10 w-10 items-center justify-center rounded-xl bg-cyan-50 text-cyan-700"><UploadCloud size={20} /></span><span className="font-data text-[11px] font-bold text-slate-400">03</span></div>
                <h3 className="mt-4 font-bold text-slate-900">Kết nối Airbyte</h3><p className="mt-2 flex-1 text-xs leading-5 text-slate-500">Portal tự nhận kết nối từ tool và cập nhật sync job theo thời gian thực.</p>
                <div className={`mt-4 flex items-center justify-center gap-2 rounded-lg border px-3 py-2.5 text-xs font-bold ${toolStatus === "online" ? "border-emerald-200 bg-emerald-50 text-emerald-700" : "border-amber-200 bg-amber-50 text-amber-700"}`}>{toolStatus === "checking" ? <Loader2 size={15} className="animate-spin" /> : toolStatus === "online" ? <CheckCircle2 size={15} /> : <Activity size={15} />}{toolStatus === "online" ? "Đang nhận realtime" : toolStatus === "checking" ? "Đang tìm db-provisioner" : toolStatus === "outdated" ? "Tool đang mở · cần cập nhật" : "Chờ mở db-provisioner"}</div>
              </article>
            </div>

            {!session && !showManualFallback && <div className={`mt-5 flex flex-col gap-4 rounded-xl border p-4 sm:flex-row sm:items-center sm:justify-between ${liveSource ? "border-emerald-200 bg-emerald-50/60" : "border-blue-200 bg-blue-50/60"}`}>
              <div className="flex min-w-0 items-center gap-3"><span className={`flex h-11 w-11 shrink-0 items-center justify-center rounded-xl ${liveSource ? "bg-emerald-100 text-emerald-700" : "bg-white text-blue-700 shadow-sm"}`}>{liveSource ? <CheckCircle2 size={21} /> : <Activity size={21} className={toolStatus === "online" ? "animate-pulse" : ""} />}</span><div className="min-w-0"><p className="text-sm font-bold text-slate-800">{liveSource ? `Đã nhận ${liveSource.database_engine} · ${liveSource.table_count} bảng` : "Đang chờ kết nối từ db-provisioner"}</p><p className="mt-1 text-xs leading-5 text-slate-500">{liveSource ? "Không cần tải bundle. Portal sẽ tự cập nhật khi Airbyte bắt đầu đồng bộ." : "Mở tool và hoàn tất kết nối database; dữ liệu sẽ tự xuất hiện tại đây."}</p></div></div>
              <button type="button" onClick={() => setShowManualFallback(true)} className="shrink-0 rounded-lg px-3 py-2 text-xs font-semibold text-slate-500 transition hover:bg-white hover:text-blue-700">Không tự nhận? Dùng bundle</button>
            </div>}

            {!session && showManualFallback && <div onDragOver={(event) => { event.preventDefault(); setDragging(true); }} onDragLeave={() => setDragging(false)} onDrop={(event) => { event.preventDefault(); setDragging(false); loadBundle(event.dataTransfer.files[0]); }} className={`mt-5 rounded-xl border-2 border-dashed p-4 transition ${dragging ? "border-blue-500 bg-blue-50" : bundle ? "border-emerald-300 bg-emerald-50/50" : "border-slate-200 bg-slate-50/70"}`}>
              <input ref={inputRef} type="file" accept="application/json,.json" className="hidden" onChange={(event) => loadBundle(event.target.files?.[0])} />
              <div className="flex flex-col gap-4 sm:flex-row sm:items-center sm:justify-between">
                <div className="flex min-w-0 items-center gap-3"><span className={`flex h-11 w-11 shrink-0 items-center justify-center rounded-xl ${bundle ? "bg-emerald-100 text-emerald-700" : "bg-white text-slate-500 shadow-sm"}`}>{bundle ? <CheckCircle2 size={21} /> : <FileJson size={21} />}</span><div className="min-w-0"><p className="truncate text-sm font-bold text-slate-800">{bundleFile || "Kéo thả airbyte_bundle_*.json vào đây"}</p><p className="mt-1 text-xs text-slate-500">{bundle ? `${bundle.database_engine} · ${bundle.exposed_views.length} bảng · Tenant ${bundle.tenant_id}` : "Tối đa 1 bundle cấu hình do db-provisioner xuất ra"}</p></div></div>
                <div className="flex shrink-0 gap-2"><button type="button" onClick={() => inputRef.current?.click()} className="rounded-lg border border-slate-200 bg-white px-3 py-3 text-xs font-bold text-slate-600 transition hover:border-blue-300 hover:text-blue-700">Chọn file</button><button onClick={connect} disabled={!bundle || connecting || bridgeStatus !== "online"} className="flex items-center justify-center gap-2 rounded-lg bg-slate-950 px-5 py-3 text-xs font-bold text-white transition hover:-translate-y-0.5 hover:bg-blue-800 hover:shadow-lg disabled:translate-y-0 disabled:cursor-not-allowed disabled:opacity-40">{connecting ? <Loader2 size={15} className="animate-spin" /> : <Network size={15} />}{connecting ? "Đang kết nối..." : "Kết nối & đồng bộ"}</button></div>
              </div>
            </div>}

            {(message || error) && <div className={`mt-4 flex items-start gap-2 rounded-lg border px-4 py-3 text-xs leading-5 ${error ? "border-rose-200 bg-rose-50 text-rose-700" : "border-emerald-200 bg-emerald-50 text-emerald-700"}`}>{error ? <XCircle size={16} className="mt-0.5 shrink-0" /> : <CheckCircle2 size={16} className="mt-0.5 shrink-0" />}<span>{error || message}</span>{error && bridgeStatus === "offline" && <button onClick={checkBridge} className="ml-auto shrink-0 font-bold underline">Kiểm tra lại</button>}</div>}
          </div>

          <aside className="rounded-2xl border border-slate-200 bg-white p-5 shadow-card sm:p-6">
            <div className="flex items-center justify-between"><div><p className="text-xs font-bold uppercase tracking-wider text-slate-400">Trạng thái hệ thống</p><h2 className="mt-1 font-display text-lg font-extrabold text-slate-950">Kết nối hiện tại</h2></div><button onClick={checkBridge} disabled={bridgeStatus === "checking"} title="Kiểm tra lại" className="rounded-lg border border-slate-200 p-2 text-slate-500 transition hover:bg-slate-50 hover:text-blue-700"><RefreshCw size={16} className={bridgeStatus === "checking" ? "animate-spin" : ""} /></button></div>
            <div className="mt-5 space-y-3">
              {[
                [Server, "DB Provisioner", toolStatus === "online" ? "WebSocket · realtime" : toolStatus === "outdated" ? "Đã mở · phiên bản chưa hỗ trợ realtime" : TOOL_URL, toolStatus],
                [Server, "Airbyte bridge", BRIDGE_URL, bridgeStatus],
                [Network, "Airbyte connection", session?.airbyte_connection_id ? `ID ${session.airbyte_connection_id}` : "Chưa khởi tạo", session?.airbyte_connection_id ? "online" : "offline"],
                [Activity, "Sync job", session?.airbyte_job_id ? `Job #${session.airbyte_job_id} · ${session.job_status || "RUNNING"}` : "Chưa có job", session?.airbyte_job_id ? (["FAILED", "CANCELLED", "INCOMPLETE"].includes(session.job_status) ? "failed" : session.job_status === "SUCCEEDED" ? "success" : "running") : "offline"],
              ].map(([RowIcon, label, value, status]) => <div key={label} className="flex items-center gap-3 rounded-xl border border-slate-100 p-3 transition hover:border-slate-200 hover:bg-slate-50"><span className="flex h-9 w-9 shrink-0 items-center justify-center rounded-lg bg-slate-100 text-slate-600"><RowIcon size={17} /></span><div className="min-w-0 flex-1"><p className="text-sm font-bold text-slate-800">{label}</p><p className="mt-1 truncate font-data text-xs leading-5 text-slate-500">{value}</p></div><StatusPill status={status}>{status === "online" || status === "success" ? "Sẵn sàng" : status === "running" ? "Đang chạy" : status === "checking" ? "Kiểm tra" : status === "outdated" ? "Cập nhật" : status === "failed" ? "Có lỗi" : "Chờ"}</StatusPill></div>)}
            </div>
            <div className="mt-5 rounded-xl bg-blue-50 p-4"><div className="flex gap-2 text-sm font-bold text-blue-900"><Info size={16} className="mt-0.5 shrink-0" />Lệnh khởi chạy bridge</div><code className="mt-3 block overflow-x-auto rounded-lg bg-slate-950 px-3 py-2.5 font-data text-xs leading-5 text-cyan-200">db-provisioner.exe bridge --port 9090</code></div>
          </aside>
        </section>

        {session && <section className="mt-5 rounded-2xl border border-slate-200 bg-white p-5 shadow-card sm:p-6">
          <div className="flex flex-wrap items-center justify-between gap-3"><div><p className="text-xs font-bold uppercase tracking-wider text-blue-700">Data pipeline</p><h2 className="mt-1 font-display text-xl font-extrabold text-slate-950">Tiến trình xử lý dữ liệu</h2></div><StatusPill status={pipeline?.state === "failed" ? "failed" : pipeline?.state === "success" ? "success" : "running"}>{pipeline?.state === "success" ? "Hoàn thành" : pipeline?.state === "failed" ? "Pipeline lỗi" : "Đang xử lý"}</StatusPill></div>
          <div className="mt-6 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            {stages.map((stage, index) => { const state = stageStates[index]; const StageIcon = stage.icon; return <div key={stage.id} className={`relative rounded-xl border p-4 transition duration-300 hover:-translate-y-1 hover:shadow-md ${state === "success" ? "border-emerald-200 bg-emerald-50/40" : state === "running" ? "border-blue-300 bg-blue-50/60" : state === "failed" ? "border-rose-200 bg-rose-50" : "border-slate-200"}`}><div className="flex items-start justify-between"><span className={`flex h-10 w-10 items-center justify-center rounded-xl ${state === "success" ? "bg-emerald-100 text-emerald-700" : state === "running" ? "bg-blue-100 text-blue-700" : state === "failed" ? "bg-rose-100 text-rose-700" : "bg-slate-100 text-slate-400"}`}><StageIcon size={19} className={state === "running" ? "animate-pulse" : ""} /></span>{state === "success" ? <Check size={17} className="text-emerald-600" /> : state === "running" ? <Loader2 size={17} className="animate-spin text-blue-600" /> : state === "failed" ? <X size={17} className="text-rose-600" /> : <Circle size={14} className="text-slate-300" />}</div><p className="mt-4 text-sm font-bold text-slate-900">{index + 1}. {stage.label}</p><p className="mt-1 text-xs text-slate-500">{stage.detail}</p>{index < stages.length - 1 && <ChevronRight className="absolute -right-3 top-1/2 z-10 hidden h-5 w-5 -translate-y-1/2 rounded-full bg-white text-slate-300 xl:block" />}</div>; })}
          </div>
          {resources.length > 0 && <div className="mt-5 overflow-hidden rounded-xl border border-slate-200"><div className="flex items-center justify-between border-b border-slate-200 bg-slate-50 px-4 py-3"><p className="text-xs font-bold text-slate-700">Tài nguyên được đồng bộ</p><span className="text-[11px] font-semibold text-slate-500">{resources.length} bảng</span></div><div className="grid gap-px bg-slate-100 sm:grid-cols-2 lg:grid-cols-3">{resources.slice(0, 6).map((item, index) => <div key={`${item.resource_name || item.source_table}-${index}`} className="flex items-center gap-3 bg-white px-4 py-3 transition hover:bg-blue-50/50"><Database size={15} className="shrink-0 text-blue-600" /><div className="min-w-0"><p className="truncate text-xs font-bold text-slate-800">{item.resource_name || item.source_table}</p><p className="mt-0.5 truncate font-data text-[10px] text-slate-400">{item.target_view || item.sync_mode || "incremental"}</p></div></div>)}</div></div>}
        </section>}

        {session && <section className="mt-5 overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-card">
          <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-200 px-5 py-4 sm:px-6"><div><p className="text-xs font-bold uppercase tracking-wider text-blue-700">Gold layer · Apache Superset</p><h2 className="mt-1 font-display text-xl font-extrabold text-slate-950">Dashboard phân tích database</h2></div>{dashboardUrl && <a href={dashboardUrl} target="_blank" rel="noreferrer" className="flex items-center gap-2 rounded-lg bg-blue-50 px-3 py-2 text-xs font-bold text-blue-700 transition hover:-translate-y-0.5 hover:bg-blue-100"><ExternalLink size={15} />Mở tab mới</a>}</div>
          <div className="bg-slate-50 p-2 sm:p-3">
            {dashboardUrl ? <div className="relative"><iframe src={dashboardUrl} title={pipeline.dashboard.title || "Database Superset Dashboard"} onLoad={() => setDashboardLoading(false)} className="h-[560px] w-full rounded-xl border border-slate-200 bg-white shadow-inner sm:h-[680px] lg:h-[820px]" allowFullScreen />{dashboardLoading && <div className="absolute inset-0 flex items-center justify-center rounded-xl bg-white"><div className="text-center"><Loader2 className="mx-auto h-7 w-7 animate-spin text-blue-700" /><p className="mt-3 text-sm font-semibold text-slate-600">Đang tải dashboard...</p></div></div>}</div> : <div className="flex min-h-64 items-center justify-center rounded-xl border border-dashed border-slate-300 bg-white px-6 text-center"><div><span className="mx-auto flex h-12 w-12 items-center justify-center rounded-xl bg-blue-50 text-blue-700"><BarChart3 size={22} /></span><p className="mt-4 font-bold text-slate-800">Dashboard đang được chuẩn bị</p><p className="mx-auto mt-2 max-w-lg text-xs leading-5 text-slate-500">Sau khi Airbyte đồng bộ và pipeline hoàn tất, dashboard Superset sẽ tự động xuất hiện tại đây.</p><div className="mx-auto mt-4 h-1.5 w-48 overflow-hidden rounded-full bg-slate-100"><div className="h-full w-2/3 animate-pulse rounded-full bg-blue-600" /></div></div></div>}
          </div>
        </section>}
      </main>
    </div>
  );
}
