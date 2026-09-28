import React, { useEffect, useState } from 'react';
import axios from 'axios';
import Icon from '../components/icons';
import { Badge, EmptyState } from '../components/ui';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000/api';

const CatalogHistoryTimeline = () => {
  const [references, setReferences] = useState([]);
  const [selectedRef, setSelectedRef] = useState('main');
  const [commits, setCommits] = useState([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState('');

  const authHeader = { Authorization: `Bearer ${localStorage.getItem('token')}` };

  useEffect(() => {
    axios
      .get(`${API_URL}/catalog/references`, { headers: authHeader })
      .then((res) => setReferences(res.data.references || []))
      .catch(() => setError('Không thể tải danh sách branch/tag từ Nessie.'));
  }, []);

  useEffect(() => {
    setLoading(true);
    setError('');
    axios
      .get(`${API_URL}/catalog/history`, {
        headers: authHeader,
        params: { ref: selectedRef, limit: 50 },
      })
      .then((res) => setCommits(res.data.commits || []))
      .catch(() => setError(`Không thể tải lịch sử commit cho ref '${selectedRef}'.`))
      .finally(() => setLoading(false));
  }, [selectedRef]);

  const formatTime = (iso) => {
    if (!iso) return '—';
    return new Date(iso).toLocaleString('vi-VN');
  };

  return (
    <div className="h-full overflow-y-auto bg-canvas p-4 sm:p-5 lg:p-6">
      <div className="mx-auto w-full max-w-[1180px]">
        <div className="mb-6 flex flex-col gap-4 border-b border-line pb-5 sm:flex-row sm:items-end sm:justify-between">
          <div className="min-w-0">
            <p className="mb-1.5 font-data text-[10px] font-semibold uppercase tracking-[0.14em] text-cobalt-700">
              Git-for-data
            </p>
            <h3 className="font-display text-xl font-bold tracking-tight text-navy-950">
              Lịch sử Branch/Merge
            </h3>
            <p className="mt-1.5 max-w-2xl text-sm leading-6 text-ink-500">
              Lịch sử commit của reference đang được chọn trong Nessie catalog.
            </p>
          </div>

          <div className="relative w-full sm:w-auto">
            <Icon
              name="gitBranch"
              className="pointer-events-none absolute left-3 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-ink-400"
            />
            <select
              value={selectedRef}
              onChange={(e) => setSelectedRef(e.target.value)}
              className="h-9 w-full rounded-control border border-line bg-white pl-8 pr-8 font-data text-xs font-medium text-ink-700 shadow-sm outline-none transition hover:border-cobalt-200 focus:border-cobalt-400 focus:ring-2 focus:ring-cobalt-500/15 sm:min-w-[210px]"
            >
              {references.length === 0 && <option value="main">main</option>}
              {references.map((r) => (
                <option key={r.name} value={r.name}>
                  {r.type === 'TAG' ? '🏷 ' : '⑂ '}
                  {r.name}
                </option>
              ))}
            </select>
          </div>
        </div>

        {error && (
          <div className="mb-5 flex items-start gap-2.5 rounded-panel border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700 shadow-sm">
            <Icon name="alertTriangle" className="mt-0.5 h-4 w-4 shrink-0" />
            <span>{error}</span>
          </div>
        )}

        {loading ? (
          <div className="flex min-h-[180px] items-center justify-center">
            <div className="flex items-center gap-2.5 rounded-panel border border-line bg-white px-4 py-3 text-sm text-ink-500 shadow-card">
              <Icon name="refresh" className="h-4 w-4 animate-spin text-cobalt-600" />
              Đang tải lịch sử commit...
            </div>
          </div>
        ) : commits.length === 0 ? (
          <div className="overflow-hidden rounded-panel border border-line bg-white shadow-card">
            <EmptyState icon="gitBranch" title="Chưa có commit nào trên ref này" />
          </div>
        ) : (
          <ol className="relative ml-3 border-l border-cobalt-100 sm:ml-4">
            {commits.map((c, idx) => (
              <li key={c.hash || idx} className="relative mb-5 ml-6 last:mb-0 sm:ml-8">
                <span
                  className={`absolute -left-[31px] top-5 flex h-4 w-4 items-center justify-center rounded-full border-2 border-white ring-4 ring-canvas sm:-left-[39px] ${
                    idx === 0 ? 'bg-cobalt-600' : 'bg-ink-300'
                  }`}
                >
                  <span className="h-1.5 w-1.5 rounded-full bg-white" />
                </span>

                <article className="rounded-panel border border-line bg-white px-4 py-4 shadow-card transition hover:border-cobalt-200 sm:px-5">
                  <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                    <p className="min-w-0 text-sm font-semibold leading-6 text-ink-900 sm:text-[15px]">
                      {c.message || '(không có message)'}
                    </p>

                    {idx === 0 && (
                      <div className="shrink-0">
                        <Badge tone="lake">Mới nhất</Badge>
                      </div>
                    )}
                  </div>

                  <div className="mt-3 flex flex-col gap-2 border-t border-ink-100 pt-3 text-xs text-ink-500 sm:flex-row sm:flex-wrap sm:items-center sm:gap-x-4 sm:gap-y-2">
                    <span className="inline-flex w-fit max-w-full items-center rounded-md border border-ink-100 bg-ink-50 px-2 py-1 font-data text-[11px] font-medium text-ink-600">
                      {c.hash ? c.hash.slice(0, 8) : '—'}
                    </span>

                    <span className="inline-flex min-w-0 items-center gap-1.5">
                      <Icon name="users" className="h-3.5 w-3.5 shrink-0 text-ink-400" />
                      <span className="truncate">{c.author || 'unknown'}</span>
                    </span>

                    <span className="inline-flex items-center gap-1.5 whitespace-nowrap">
                      <Icon name="clock" className="h-3.5 w-3.5 shrink-0 text-ink-400" />
                      <span className="font-data text-[11px]">{formatTime(c.commit_time)}</span>
                    </span>
                  </div>
                </article>
              </li>
            ))}
          </ol>
        )}
      </div>
    </div>
  );
};

export default CatalogHistoryTimeline;
