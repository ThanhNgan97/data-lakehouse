import { useEffect, useMemo, useState } from "react";
import axios from "axios";
import { Link } from "react-router-dom";
import {
  ArrowLeft,
  Check,
  CheckCircle2,
  ChevronDown,
  Clock3,
  Code2,
  Database,
  Eye,
  EyeOff,
  FileJson2,
  History,
  KeyRound,
  LoaderCircle,
  Plus,
  RefreshCw,
  Save,
  Search,
  Settings2,
  ShieldCheck,
  Table2,
  TestTube2,
} from "lucide-react";
import UserTopNavigation from "../components/UserTopNavigation";

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000/api";
const AIRFLOW_ORIGIN = import.meta.env.VITE_AIRFLOW_URL || "http://localhost:8080";
const AIRFLOW_DAG_URL = `${AIRFLOW_ORIGIN}/dags/api_dataset_pipeline/grid`;
const SUPERSET_ORIGIN = import.meta.env.VITE_SUPERSET_URL || "http://localhost:8088";
const authHeader = () => ({ Authorization: `Bearer ${localStorage.getItem("token")}` });
const normalizeSource = (source) => ({
  id: source.id,
  datasetId: source.dataset_id,
  name: source.name,
  endpoint: source.url,
  description: source.description || "",
  updatedAt: source.updated_at
    ? `Cập nhật: ${new Date(source.updated_at).toLocaleString("vi-VN")}`
    : source.dataset_id,
  status: source.status === "READY" ? "Đã kết nối" : source.status,
  dashboardSlug: source.dashboard_slug,
});

function SectionCard({ children, className = "" }) {
  return (
    <section className={`overflow-hidden rounded-2xl border border-slate-200 bg-white shadow-[0_12px_30px_rgba(15,23,42,0.06)] ${className}`}>
      {children}
    </section>
  );
}

function FieldLabel({ children, required = false }) {
  return (
    <label className="mb-2 block text-sm font-semibold text-slate-700">
      {children}{required && <span className="ml-1 text-rose-500">*</span>}
    </label>
  );
}

export default function ApiConnection() {
  const [sources, setSources] = useState([]);
  const [selectedId, setSelectedId] = useState("");
  const [search, setSearch] = useState("");
  const [authType, setAuthType] = useState("bearer");
  const [showToken, setShowToken] = useState(false);
  const [paginationOpen, setPaginationOpen] = useState(false);
  const [activeTab, setActiveTab] = useState("history");
  const [pipeline, setPipeline] = useState({ runId: "", state: "idle", tasks: [], error: "" });
  const [preview, setPreview] = useState({ loading: false, data: [], metadata: null, error: "" });
  const [saveState, setSaveState] = useState({ loading: false, message: "", error: "" });
  const [form, setForm] = useState({
    name: "",
    datasetId: "",
    url: "",
    description: "",
    token: "",
  });

  const filteredSources = useMemo(
    () => sources.filter((source) => source.name.toLowerCase().includes(search.toLowerCase())),
    [search, sources],
  );

  const selectedSource = sources.find((source) => source.id === selectedId);

  useEffect(() => {
    let active = true;
    axios.get(`${API_URL}/api-sources`, { headers: authHeader() })
      .then((response) => {
        if (!active) return;
        setSources((response.data || []).map(normalizeSource));
      })
      .catch((error) => {
        if (active) setSaveState({ loading: false, message: "", error: error.response?.data?.detail || "Không tải được danh sách nguồn API." });
      });
    return () => { active = false; };
  }, []);

  useEffect(() => {
    if (!pipeline.runId || !["queued", "running"].includes(pipeline.state)) return undefined;

    const timer = window.setInterval(async () => {
      try {
        const response = await axios.get(
          `${API_URL}/api-sources/runs/${encodeURIComponent(pipeline.runId)}`,
          { headers: authHeader() },
        );
        setPipeline((current) => ({
          ...current,
          state: response.data.state || "unknown",
          tasks: response.data.tasks || [],
          error: "",
        }));
      } catch (error) {
        setPipeline((current) => ({
          ...current,
          error: error.response?.data?.detail || "Không thể đọc trạng thái pipeline.",
        }));
      }
    }, 3000);

    return () => window.clearInterval(timer);
  }, [pipeline.runId, pipeline.state]);

  function updateForm(key, value) {
    setForm((current) => ({
      ...current,
      [key]: value,
      ...(key === "url" ? { datasetId: "" } : {}),
    }));
    setSaveState({ loading: false, message: "", error: "" });
    if (key === "url") setPreview({ loading: false, data: [], metadata: null, error: "" });
  }

  function selectSource(source) {
    setSelectedId(source.id);
    setForm((current) => ({
      ...current,
      name: source.name,
      datasetId: source.datasetId,
      url: source.endpoint,
      description: source.description || "",
      token: "",
    }));
    setPreview({ loading: false, data: [], metadata: null, error: "" });
    setPipeline({ runId: "", state: "idle", tasks: [], error: "" });
  }

  async function loadPreview() {
    if (!form.url.trim()) return;
    setPreview({ loading: true, data: [], metadata: null, error: "" });
    try {
      const response = await axios.post(
        `${API_URL}/api-sources/inspect`,
        {
          url: form.url.trim(),
          bearer_token: authType === "bearer" ? form.token.trim() || null : null,
          limit: 3,
        },
        { headers: authHeader() },
      );
      setForm((current) => ({
        ...current,
        name: current.name.trim() || response.data.name || "",
        datasetId: response.data.dataset_id || "",
        description: current.description.trim() || response.data.description || "",
      }));
      setPreview({
        loading: false,
        data: response.data.data || [],
        metadata: response.data,
        error: "",
      });
    } catch (error) {
      setPreview({
        loading: false,
        data: [],
        metadata: null,
        error: error.response?.data?.detail || "Không thể tải dữ liệu từ Mock API.",
      });
    }
  }

  async function saveSource(event) {
    event.preventDefault();
    if (!form.name.trim() || !form.url.trim()) {
      setSaveState({ loading: false, message: "", error: "Vui lòng nhập tên nguồn và địa chỉ API." });
      return;
    }
    if (!form.datasetId.trim() || preview.metadata?.dataset_id !== form.datasetId.trim()) {
      setSaveState({ loading: false, message: "", error: "Vui lòng kiểm tra nguồn API trước khi lưu." });
      return;
    }

    setSaveState({ loading: true, message: "", error: "" });
    try {
      const response = await axios.post(
        `${API_URL}/api-sources`,
        {
          name: form.name.trim(),
          dataset_id: form.datasetId.trim(),
          url: form.url.trim(),
          description: form.description.trim() || null,
          auth_type: authType,
        },
        { headers: authHeader() },
      );
      const saved = normalizeSource(response.data);
      setSources((current) => [saved, ...current.filter((item) => item.id !== saved.id)]);
      setSelectedId(saved.id);
      setSaveState({ loading: false, message: "Đã lưu nguồn API vào hệ thống.", error: "" });
    } catch (error) {
      setSaveState({ loading: false, message: "", error: error.response?.data?.detail || "Không thể lưu nguồn API." });
    }
  }

  async function runPipeline() {
    if (!selectedSource?.datasetId) {
      setPipeline({ runId: "", state: "failed", tasks: [], error: "Nguồn này chưa được đăng ký với API Dataset Pipeline." });
      return;
    }
    setPipeline({ runId: "", state: "starting", tasks: [], error: "" });
    try {
      const response = await axios.post(
        `${API_URL}/api-sources/${encodeURIComponent(selectedSource.datasetId)}/runs`,
        { source_url: selectedSource.endpoint },
        { headers: authHeader() },
      );
      setPipeline({
        runId: response.data.dag_run_id,
        state: response.data.state || "queued",
        tasks: [],
        error: "",
      });
    } catch (error) {
      setPipeline({
        runId: "",
        state: "failed",
        tasks: [],
        error: error.response?.data?.detail || "Không thể khởi chạy API Dataset Pipeline.",
      });
    }
  }

  function pipelineTaskState(index) {
    const taskGroups = [
      ["api_preflight", "ingest_bronze"],
      ["validate_bronze", "process_silver", "validate_silver"],
      ["process_gold", "validate_gold"],
      ["trino_smoke_test"],
    ];
    const matches = pipeline.tasks.filter((task) => taskGroups[index].includes(task.task_id));
    if (matches.some((task) => ["failed", "upstream_failed"].includes(task.state))) return "failed";
    if (matches.some((task) => ["running", "queued", "scheduled"].includes(task.state))) return "running";
    if (matches.length && matches.every((task) => task.state === "success")) return "success";
    return "waiting";
  }

  return (
    <div className="min-h-screen bg-[#f6f8fc] text-slate-900">
      <UserTopNavigation />

      <main className="mx-auto w-full max-w-[1480px] px-3 py-5 sm:px-6 sm:py-7 lg:px-8">
        {/* <Link to="/user" className="mb-5 inline-flex items-center gap-2 rounded-lg px-2 py-1.5 text-sm font-medium text-slate-600 transition hover:bg-white hover:text-blue-700">
          <ArrowLeft size={17} /> Về trang chủ
        </Link> */}

        <div className="mb-6 border-l-4 border-blue-600 pl-4">
          <h1 className="text-2xl font-extrabold tracking-tight sm:text-3xl">Kết nối API</h1>
          {/* <p className="mt-1 text-sm text-slate-500 sm:text-base">Thêm nguồn dữ liệu từ API, kiểm tra kết nối và theo dõi các lần cập nhật.</p> */}
        </div>

        <div className="grid grid-cols-1 items-start gap-5 lg:grid-cols-[minmax(0,1.35fr)_minmax(340px,.85fr)]">
          <SectionCard className="order-1 min-w-0 border-t-4 border-t-blue-600">
            <form onSubmit={saveSource}>
              <div className="flex flex-col gap-3 border-b border-slate-100 p-4 sm:flex-row sm:items-center sm:justify-between sm:p-6">
                <div className="flex items-center gap-3">
                  <span className="grid h-10 w-10 place-items-center rounded-xl bg-blue-50 text-blue-600"><Code2 size={20} /></span>
                  <div><h2 className="font-bold">Thêm kết nối API mới</h2><p className="text-xs text-slate-500">Khai báo thông số dịch vụ dữ liệu RESTful</p></div>
                </div>
                <span className="w-fit rounded-full bg-blue-50 px-3 py-1 text-xs font-semibold text-blue-700">Thêm mới</span>
              </div>

              <div className="space-y-5 p-4 sm:p-6">
                <div>
                  <FieldLabel required>Tên nguồn dữ liệu</FieldLabel>
                  <input value={form.name} onChange={(event) => updateForm("name", event.target.value)} placeholder="Nhập tên nguồn dữ liệu" className="w-full rounded-xl border border-slate-200 bg-white px-4 py-3 text-sm outline-none transition focus:border-blue-500 focus:ring-4 focus:ring-blue-100" />
                  <p className="mt-1.5 text-xs text-slate-400">Tên dễ hiểu giúp bạn nhận diện nguồn trong hệ thống.</p>
                </div>

                <div>
                  <FieldLabel required>Dataset ID</FieldLabel>
                  <input value={form.datasetId} readOnly placeholder="Hệ thống tự nhận diện sau khi kiểm tra API" className="w-full rounded-xl border border-slate-200 bg-slate-50 px-4 py-3 font-mono text-sm text-slate-600 outline-none" />
                  <p className="mt-1.5 text-xs text-slate-400">Dataset ID được đọc từ phản hồi API và truyền vào api_dataset_pipeline.</p>
                </div>

                <div>
                  <FieldLabel required>Địa chỉ API (URL)</FieldLabel>
                  <div className="flex overflow-hidden rounded-xl border border-slate-200 bg-white focus-within:border-blue-500 focus-within:ring-4 focus-within:ring-blue-100">
                    <span className="grid min-w-16 place-items-center border-r border-slate-200 bg-slate-50 text-xs font-bold text-blue-700">GET</span>
                    <input type="url" value={form.url} onChange={(event) => updateForm("url", event.target.value)} placeholder="https://..." className="min-w-0 flex-1 px-4 py-3 text-sm outline-none" />
                  </div>
                  <p className="mt-1.5 text-xs text-slate-400">Địa chỉ API ổn định và có thể truy cập từ máy chủ.</p>
                </div>

                <div>
                  <FieldLabel>Mô tả <span className="float-right font-normal text-slate-400">Không bắt buộc</span></FieldLabel>
                  <textarea rows="3" value={form.description} onChange={(event) => updateForm("description", event.target.value)} placeholder="Nhập mô tả nguồn dữ liệu" className="w-full resize-none rounded-xl border border-slate-200 px-4 py-3 text-sm outline-none transition focus:border-blue-500 focus:ring-4 focus:ring-blue-100" />
                </div>

                <div>
                  <FieldLabel>Xác thực truy cập</FieldLabel>
                  <div className="grid gap-2 sm:grid-cols-2">
                    {[{ value: "none", label: "Không yêu cầu", icon: ShieldCheck }, { value: "bearer", label: "Bearer Token", icon: KeyRound }].map(({ value, label, icon: Icon }) => (
                      <button key={value} type="button" onClick={() => setAuthType(value)} className={`flex items-center justify-center gap-2 rounded-xl border px-4 py-3 text-sm font-semibold transition ${authType === value ? "border-blue-600 bg-blue-50 text-blue-700 ring-2 ring-blue-100" : "border-slate-200 text-slate-600 hover:bg-slate-50"}`}>
                        <Icon size={17} />{label}
                      </button>
                    ))}
                  </div>
                </div>

                {authType === "bearer" && <div>
                  <FieldLabel required>Mã xác thực Bearer Token</FieldLabel>
                  <div className="relative">
                    <input type={showToken ? "text" : "password"} value={form.token} onChange={(event) => updateForm("token", event.target.value)} className="w-full rounded-xl border border-slate-200 px-4 py-3 pr-12 text-sm outline-none transition focus:border-blue-500 focus:ring-4 focus:ring-blue-100" />
                    <button type="button" onClick={() => setShowToken((current) => !current)} aria-label={showToken ? "Ẩn token" : "Hiện token"} className="absolute inset-y-0 right-0 px-4 text-slate-400 hover:text-blue-600">{showToken ? <EyeOff size={18} /> : <Eye size={18} />}</button>
                  </div>
                  <p className="mt-1.5 text-xs text-slate-400">Hệ thống gửi kèm theo định dạng Authorization: Bearer [token].</p>
                </div>}

                <div className="overflow-hidden rounded-xl border border-slate-200">
                  <button type="button" onClick={() => setPaginationOpen((current) => !current)} className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left text-sm font-semibold hover:bg-slate-50">
                    <span className="flex items-center gap-2"><Settings2 size={17} className="text-blue-600" />Cách lấy dữ liệu &amp; Phân trang</span>
                    <ChevronDown size={17} className={`transition ${paginationOpen ? "rotate-180" : ""}`} />
                  </button>
                  {paginationOpen && <div className="border-t border-slate-200 bg-slate-50 p-4 text-sm text-slate-500">Cấu hình mặc định: đọc JSON từ trang đầu tiên, tối đa 100 bản ghi mỗi yêu cầu.</div>}
                </div>

                {saveState.error && <p role="alert" className="rounded-xl border border-rose-200 bg-rose-50 p-3 text-sm font-semibold text-rose-700">{saveState.error}</p>}
                {saveState.message && <p className="rounded-xl border border-emerald-200 bg-emerald-50 p-3 text-sm font-semibold text-emerald-700">{saveState.message}</p>}
                <div className="grid gap-3 sm:grid-cols-2">
                  <button type="button" onClick={loadPreview} disabled={!form.url.trim() || preview.loading} className="inline-flex w-full items-center justify-center gap-2 rounded-xl border border-blue-200 bg-blue-50 px-5 py-3 text-sm font-bold text-blue-700 transition hover:bg-blue-100 disabled:cursor-not-allowed disabled:opacity-50">
                    {preview.loading ? <LoaderCircle size={17} className="animate-spin" /> : <TestTube2 size={17} />}
                    {preview.loading ? "Đang kiểm tra" : "Kiểm tra nguồn API"}
                  </button>
                  <button type="submit" disabled={saveState.loading || !preview.metadata} className="inline-flex w-full items-center justify-center gap-2 rounded-xl bg-blue-600 px-5 py-3 text-sm font-bold text-white shadow-lg shadow-blue-200 transition hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-50">
                    {saveState.loading ? <LoaderCircle size={17} className="animate-spin" /> : <Save size={17} />}
                    {saveState.loading ? "Đang lưu" : "Lưu nguồn API"}
                  </button>
                </div>
              </div>
            </form>
          </SectionCard>

          <SectionCard className="order-2 min-w-0 self-start border-t-4 border-t-cyan-500 lg:sticky lg:top-24 lg:max-h-[calc(100vh-7rem)] lg:overflow-y-auto">
            <div className="flex items-center justify-between gap-3 border-b border-slate-100 p-4 sm:p-6">
              <div className="flex items-center gap-3"><Database size={20} className="text-cyan-600" /><h2 className="font-bold">Nguồn API </h2></div>
              <button type="button" className="inline-flex items-center gap-1 rounded-lg bg-cyan-50 px-3 py-2 text-xs font-bold text-cyan-700 hover:bg-cyan-100"><Plus size={14} />Thêm mới</button>
            </div>
            <div className="space-y-4 p-4 sm:p-6">
              <div className="relative"><Search size={16} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" /><input value={search} onChange={(event) => setSearch(event.target.value)} placeholder="Tìm theo tên nguồn..." className="w-full rounded-xl border border-slate-200 py-2.5 pl-10 pr-4 text-sm outline-none focus:border-cyan-500 focus:ring-4 focus:ring-cyan-100" /></div>
              <div className="space-y-3">
                {filteredSources.map((source) => (
                  <button key={source.id} type="button" onClick={() => selectSource(source)} className={`w-full rounded-xl border p-4 text-left transition ${selectedId === source.id ? "border-blue-500 bg-blue-50/60 ring-2 ring-blue-100" : "border-slate-200 hover:border-slate-300 hover:bg-slate-50"}`}>
                    <div className="flex items-start justify-between gap-3"><span className="font-bold text-slate-800">{source.name}</span><span className={`shrink-0 rounded-full px-2 py-1 text-[10px] font-bold ${source.status === "Đã kết nối" ? "bg-emerald-100 text-emerald-700" : "bg-slate-100 text-slate-500"}`}>{source.status}</span></div>
                    <p className="mt-2 truncate font-mono text-xs text-slate-500">{source.endpoint}</p>
                    <div className="mt-3 flex flex-wrap items-center justify-between gap-2 text-xs"><span className="text-slate-400">{source.updatedAt}</span><span className="font-semibold text-blue-700">Xem chi tiết</span></div>
                  </button>
                ))}
                {!filteredSources.length && <p className="rounded-xl bg-slate-50 p-6 text-center text-sm text-slate-500">Chưa có nguồn API. Hãy nhập URL và lưu nguồn ở biểu mẫu bên trái.</p>}
              </div>
              <div className="rounded-xl border border-dashed border-slate-200 bg-slate-50 p-5 text-center"><Plus className="mx-auto text-slate-400" size={22} /><p className="mt-2 text-xs leading-5 text-slate-500">Nhập thông tin bên trái để tạo thêm một nguồn API.</p></div>
            </div>
          </SectionCard>
        </div>

        <SectionCard className="mt-5 border-t-4 border-t-teal-500">
          <div className="flex flex-col gap-3 border-b border-slate-100 p-4 sm:flex-row sm:items-center sm:justify-between sm:p-6">
            <div className="flex items-center gap-3"><span className="grid h-10 w-10 place-items-center rounded-xl bg-teal-50 text-teal-600"><FileJson2 size={20} /></span><div><h2 className="font-bold">Kiểm tra nguồn &amp; Xem dữ liệu</h2><p className="text-xs text-slate-500">Xác thực phản hồi JSON trước khi tích hợp vào Data Lakehouse</p></div></div>
            <div className="flex flex-wrap items-center gap-2">
              {preview.metadata && <span className="rounded-full bg-emerald-100 px-3 py-1.5 text-xs font-semibold text-emerald-700">Schema {preview.metadata.schema_version}</span>}
              <button type="button" onClick={loadPreview} disabled={!form.url.trim() || preview.loading} className="inline-flex items-center gap-2 rounded-xl bg-teal-600 px-4 py-2.5 text-sm font-bold text-white hover:bg-teal-700 disabled:cursor-not-allowed disabled:opacity-50">
                {preview.loading ? <LoaderCircle size={16} className="animate-spin" /> : <TestTube2 size={16} />}
                {preview.loading ? "Đang kiểm tra" : "Kiểm tra nguồn API"}
              </button>
            </div>
          </div>
          {preview.error && <div className="m-4 rounded-xl border border-rose-200 bg-rose-50 p-4 text-sm text-rose-700 sm:m-6">{preview.error}</div>}
          {!preview.data.length ? (
            <div className="grid min-h-48 place-items-center p-6 text-center"><div><TestTube2 size={30} className="mx-auto text-slate-400" /><h3 className="mt-3 font-bold">Chưa có dữ liệu phản hồi từ API</h3><p className="mx-auto mt-1 max-w-xl text-sm text-slate-500">Nhập URL ở biểu mẫu và nhấn “Kiểm tra nguồn API” để nhận diện dữ liệu.</p></div></div>
          ) : (
            <div className="p-4 sm:p-6">
              <div className="mb-3 flex flex-wrap items-center justify-between gap-2"><p className="text-sm font-bold">Xem trước {preview.data.length} bản ghi</p><span className="font-mono text-xs text-slate-500">{preview.metadata?.dataset_id}</span></div>
              <div className="overflow-x-auto rounded-xl border border-slate-200">
                <table className="w-full min-w-max text-left text-sm">
                  <thead className="bg-slate-50 text-xs uppercase text-slate-500"><tr>{Object.keys(preview.data[0] || {}).map((heading) => <th key={heading} className="whitespace-nowrap px-4 py-3 font-semibold">{heading}</th>)}</tr></thead>
                  <tbody className="divide-y divide-slate-100">{preview.data.map((row, rowIndex) => <tr key={row.record_id || rowIndex}>{Object.keys(preview.data[0] || {}).map((key) => <td key={key} className="max-w-64 whitespace-nowrap px-4 py-3 text-slate-600">{String(row[key] ?? "")}</td>)}</tr>)}</tbody>
                </table>
              </div>
            </div>
          )}
        </SectionCard>

        {selectedSource && <SectionCard className="mt-5 border-t-4 border-t-blue-600">
          <div className="flex flex-col gap-3 border-b border-slate-100 p-4 sm:flex-row sm:items-center sm:justify-between sm:p-6">
            <div><h2 className="text-lg font-bold sm:text-xl">Chi tiết nguồn: {selectedSource?.name}</h2><p className="mt-1 text-xs text-slate-500">Cấu hình kỹ thuật, lịch sử xử lý và kết nối báo cáo</p></div>
            <div className="flex flex-wrap items-center gap-2">
              <a href={AIRFLOW_DAG_URL} target="_blank" rel="noreferrer" className="rounded-xl border border-slate-200 px-4 py-2.5 text-sm font-bold text-slate-600 hover:bg-slate-50">Mở Airflow</a>
              <button type="button" onClick={runPipeline} disabled={["starting", "queued", "running"].includes(pipeline.state)} className="inline-flex w-fit items-center gap-2 rounded-xl bg-blue-600 px-4 py-2.5 text-sm font-bold text-white hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-60">
                <RefreshCw size={16} className={["starting", "queued", "running"].includes(pipeline.state) ? "animate-spin" : ""} />
                {["starting", "queued", "running"].includes(pipeline.state) ? "Pipeline đang chạy" : "Cập nhật dữ liệu"}
              </button>
            </div>
          </div>
          <div className="p-4 sm:p-6">
            <div className="grid gap-3 sm:grid-cols-3">
              {[{ icon: Save, label: "Lưu cấu hình", value: "Đã lưu vào hệ thống" }, { icon: CheckCircle2, label: "Kiểm tra kết nối", value: "Đã kiểm tra" }, { icon: Settings2, label: "Dataset ID", value: selectedSource.datasetId }].map(({ icon: Icon, label, value }) => <div key={label} className="rounded-xl border border-slate-200 p-4"><p className="text-xs text-slate-500">{label}</p><p className="mt-2 flex items-center gap-2 break-all text-sm font-bold"><Icon size={16} className="shrink-0 text-blue-500" />{value}</p></div>)}
            </div>
            <div className={`mt-4 rounded-xl border p-4 text-sm ${pipeline.state === "success" ? "border-emerald-200 bg-emerald-50 text-emerald-800" : pipeline.state === "failed" ? "border-rose-200 bg-rose-50 text-rose-800" : "border-blue-100 bg-blue-50 text-blue-800"}`}>
              <strong>API Dataset Pipeline:</strong>{" "}
              {pipeline.state === "idle" && "Sẵn sàng chạy luồng api_dataset_pipeline."}
              {pipeline.state === "starting" && "Đang gửi yêu cầu khởi chạy đến Airflow..."}
              {["queued", "running"].includes(pipeline.state) && `Đang xử lý ${selectedSource?.name}.`}
              {pipeline.state === "success" && "Pipeline đã hoàn thành thành công."}
              {pipeline.state === "failed" && (pipeline.error || "Pipeline thực thi thất bại.")}
              {pipeline.runId && <span className="mt-1 block break-all font-mono text-xs opacity-75">Run ID: {pipeline.runId}</span>}
            </div>
            <h3 className="mb-3 mt-6 text-xs font-extrabold uppercase tracking-wider text-slate-500">Tiến trình thực thi chuẩn</h3>
            <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
              {["Nhận dữ liệu", "Kiểm tra & chuẩn hóa", "Tổng hợp kết quả", "Kiểm tra báo cáo"].map((step, index) => {
                const state = pipelineTaskState(index);
                const stateLabel = { waiting: "Sẵn sàng", running: "Đang chạy", success: "Hoàn thành", failed: "Thất bại" }[state];
                const stateClass = { waiting: "bg-slate-100 text-slate-500", running: "bg-blue-100 text-blue-700", success: "bg-emerald-100 text-emerald-700", failed: "bg-rose-100 text-rose-700" }[state];
                return <div key={step} className={`rounded-xl border p-4 ${state === "running" ? "border-blue-300 bg-blue-50/50" : state === "failed" ? "border-rose-200" : "border-slate-200"}`}><div className="flex items-center justify-between"><span className="font-mono text-xs text-blue-600">0{index + 1}</span><span className={`rounded-full px-2 py-1 text-[10px] font-bold ${stateClass}`}>{stateLabel}</span></div><p className="mt-4 text-sm font-bold">{step}</p></div>;
              })}
            </div>
            <div className="mt-6 border-b border-slate-200"><div className="flex gap-5 overflow-x-auto">{[{ id: "history", label: "Lịch sử cập nhật", icon: History }, { id: "report", label: "Báo cáo liên quan", icon: Table2 }].map(({ id, label, icon: Icon }) => <button key={id} type="button" onClick={() => setActiveTab(id)} className={`flex shrink-0 items-center gap-2 border-b-2 px-1 pb-3 text-sm font-semibold ${activeTab === id ? "border-blue-600 text-blue-700" : "border-transparent text-slate-500"}`}><Icon size={16} />{label}</button>)}</div></div>
            {activeTab === "history" ? <div className="mt-4 grid min-h-28 place-items-center rounded-xl border border-dashed border-slate-200 text-center"><div><Clock3 size={22} className="mx-auto text-slate-400" /><p className="mt-2 text-sm font-semibold text-slate-600">Chưa có thông tin lịch sử cập nhật</p></div></div> : <div className="mt-4 flex flex-col gap-3 rounded-xl border border-slate-200 p-4 sm:flex-row sm:items-center sm:justify-between"><div><p className="font-bold">CTU IOC – Kết quả học tập</p><p className="mt-1 text-xs text-slate-500">Báo cáo mở theo phân quyền tài khoản của bạn.</p></div><button type="button" className="rounded-lg border border-slate-200 px-4 py-2 text-sm font-bold text-blue-700">Mở báo cáo</button></div>}
          </div>
        </SectionCard>}

        {selectedSource?.dashboardSlug && <SectionCard className="mt-5 border-t-4 border-t-violet-500">
          <div className="flex flex-col gap-3 border-b border-slate-100 p-4 sm:flex-row sm:items-center sm:justify-between sm:p-6">
            <div><h2 className="text-lg font-bold sm:text-xl">Dashboard: {selectedSource.name}</h2><p className="mt-1 text-xs text-slate-500">Báo cáo phân tích tương ứng với nguồn API đã chọn</p></div>
            <a href={`${SUPERSET_ORIGIN}/superset/dashboard/${encodeURIComponent(selectedSource.dashboardSlug)}/`} target="_blank" rel="noreferrer" className="w-fit rounded-xl bg-violet-600 px-4 py-2.5 text-sm font-bold text-white hover:bg-violet-700">Mở toàn màn hình</a>
          </div>
          <div className={`mx-2 mt-3 rounded-xl border p-3 text-sm sm:mx-4 ${pipeline.state === "success" ? "border-emerald-200 bg-emerald-50 text-emerald-700" : "border-amber-200 bg-amber-50 text-amber-800"}`}>
            {pipeline.state === "success" ? "Dữ liệu Dashboard đã được đồng bộ thành công." : "Dashboard đang hiển thị dữ liệu hiện có. Nhấn “Cập nhật dữ liệu” để chạy pipeline và đồng bộ dữ liệu mới nhất."}
          </div>
          <div className="p-2 sm:p-4">
            <iframe title={`Dashboard ${selectedSource.name}`} src={`${SUPERSET_ORIGIN}/superset/dashboard/${encodeURIComponent(selectedSource.dashboardSlug)}/?standalone=3`} className="h-[560px] w-full rounded-xl border border-slate-200 bg-white sm:h-[720px]" />
          </div>
        </SectionCard>}
      </main>

      <footer className="mt-8 border-t border-slate-200 bg-white"><div className="mx-auto flex max-w-[1480px] flex-col gap-2 px-4 py-5 text-xs text-slate-500 sm:px-6 md:flex-row md:items-center md:justify-between lg:px-8"><span>© 2026 Trung tâm Công nghệ Phần mềm – Đại học Cần Thơ (CUSC).</span><span>Hỗ trợ kỹ thuật: support@cusc.ctu.edu.vn</span></div></footer>
    </div>
  );
}
