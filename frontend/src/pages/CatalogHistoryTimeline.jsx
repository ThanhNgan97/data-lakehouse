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
    <div className="p-6 overflow-y-auto h-full bg-[#FAFBFD]">
      <div className="flex items-center justify-between mb-6 flex-wrap gap-3">
        <div>
          <p className="font-data text-[11px] uppercase tracking-widest text-lake-600 font-semibold mb-1">
            Git-for-data
          </p>
          <h3 className="text-lg font-bold text-ink-900">Lịch sử Phiên bản Catalog (Nessie)</h3>
          <p className="text-sm text-ink-400 mt-1">
            Mỗi dòng thời gian tương ứng với 1 commit (ingest/merge/tag) trên Iceberg catalog.
          </p>
        </div>
        <div className="relative">
          <Icon name="gitBranch" className="w-3.5 h-3.5 text-ink-300 absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none" />
          <select
            value={selectedRef}
            onChange={(e) => setSelectedRef(e.target.value)}
            className="border border-ink-100 rounded-lg pl-8 pr-3 py-2 text-sm bg-white shrink-0 font-data focus:outline-none focus:ring-2 focus:ring-lake-300"
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
        <div className="bg-rose-50 text-rose-600 border border-rose-200 p-3 rounded-xl mb-4 text-sm flex items-center gap-2">
          <Icon name="alertTriangle" className="w-4 h-4 shrink-0" /> {error}
        </div>
      )}

      {loading ? (
        <div className="flex items-center gap-2 text-ink-400 text-sm py-8">
          <Icon name="refresh" className="w-4 h-4 animate-spin" /> Đang tải lịch sử commit...
        </div>
      ) : commits.length === 0 ? (
        <div className="bg-white rounded-2xl border border-ink-100">
          <EmptyState icon="gitBranch" title="Chưa có commit nào trên ref này" />
        </div>
      ) : (
        <ol className="relative border-l-2 border-lake-100 ml-3">
          {commits.map((c, idx) => (
            <li key={c.hash || idx} className="mb-6 ml-6">
              <span className={`absolute flex items-center justify-center w-6 h-6 rounded-full -left-3 ring-4 ring-[#FAFBFD] ${
                idx === 0 ? 'bg-lake-500' : 'bg-ink-200'
              }`}>
                <span className={`w-2 h-2 rounded-full ${idx === 0 ? 'bg-white' : 'bg-white/70'}`} />
              </span>
              <div className="bg-white border border-ink-100 rounded-xl p-4 shadow-sm hover:border-lake-200 transition">
                <div className="flex justify-between items-start gap-4">
                  <p className="font-semibold text-ink-800 text-sm">
                    {c.message || '(không có message)'}
                  </p>
                  {idx === 0 && <Badge tone="lake">Mới nhất</Badge>}
                </div>
                <div className="mt-2.5 flex items-center gap-3 text-xs text-ink-400 font-data">
                  <span className="bg-ink-50 border border-ink-100 px-2 py-0.5 rounded">
                    {c.hash ? c.hash.slice(0, 8) : '—'}
                  </span>
                  <span className="flex items-center gap-1">
                    <Icon name="users" className="w-3 h-3" /> {c.author || 'unknown'}
                  </span>
                  <span className="flex items-center gap-1">
                    <Icon name="clock" className="w-3 h-3" /> {formatTime(c.commit_time)}
                  </span>
                </div>
              </div>
            </li>
          ))}
        </ol>
      )}
    </div>
  );
};

export default CatalogHistoryTimeline;
