import React, { useEffect, useState } from 'react';
import axios from 'axios';
import { History, Tag, GitBranch, Circle, Dot, User } from 'lucide-react';

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

  const fetchLog = async (refName) => {
    setLoading(true);
    setError('');
    try {
      const res = await axios.get(`${API_URL}/catalog/history`, {
        headers: authHeader,
        params: { ref: refName, limit: 50 },
      });
      setCommits(res.data.commits || []);
    } catch (e) {
      setError(`Không thể tải lịch sử commit cho ref '${refName}'.`);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchLog(selectedRef);
  }, [selectedRef]);

  return (
    <div className="p-6 h-full overflow-y-auto bg-slate-50/50">
      {/* Header */}
      <div className="mb-8">
        <h3 className="text-xl font-bold text-slate-800 flex items-center gap-2 tracking-tight">
          <History className="w-6 h-6 text-blue-600" /> Lịch sử Dữ liệu (Iceberg Nessie)
        </h3>
        <p className="text-sm text-slate-500 mt-1.5 font-medium">
          Xem lịch sử commit, thay đổi tag/branch trực tiếp trên Iceberg Nessie.
        </p>
      </div>

      <div className="flex items-center gap-3 mb-8">
        <select
          className="border border-slate-200 rounded-xl px-4 py-2.5 text-sm font-semibold text-slate-700 bg-white focus:outline-none focus:ring-2 focus:ring-blue-500/20 shadow-sm"
          value={selectedRef}
          onChange={(e) => {
            setSelectedRef(e.target.value);
          }}
        >
          {references.length === 0 && <option value="main">main</option>}
          {references.map((r) => (
            <option key={r.name} value={r.name}>
              {r.type === 'TAG' ? '[TAG] ' : '[BRANCH] '}
              {r.name}
            </option>
          ))}
        </select>
      </div>

      {/* Loading / Error / Empty */}
      {loading && <div className="text-slate-400 py-10 font-medium">Đang tải lịch sử...</div>}
      {error && <div className="text-red-500 py-10 font-medium">{error}</div>}
      {!loading && !error && commits.length === 0 && (
        <div className="text-slate-400 py-10 font-medium">Không có lịch sử commit nào trên nhánh này.</div>
      )}

      {/* Timeline */}
      {!loading && !error && commits.length > 0 && (
        <div className="relative border-l-2 border-slate-200 ml-4 space-y-8 pb-10">
          {commits.map((c) => (
            <div key={c.hash} className="relative">
              {/* Timeline Marker */}
              <div className="absolute -left-3.5 top-1.5 w-7 h-7 bg-white border-[3px] border-blue-500 rounded-full flex items-center justify-center shadow-sm">
                <div className="w-2.5 h-2.5 bg-blue-500 rounded-full"></div>
              </div>

              {/* Content Card */}
              <div className="bg-white rounded-2xl p-5 shadow-sm border border-slate-100 hover:shadow-lg hover:border-slate-200 transition-all duration-300 ml-6">
                <div className="flex justify-between items-start mb-2">
                  <h4 className="font-bold text-slate-800 text-[15px]">{c.message || 'No commit message'}</h4>
                  <span className="text-[11px] text-slate-400 font-medium bg-slate-50 px-2.5 py-1 rounded-full whitespace-nowrap border border-slate-100">
                    {new Date(c.commit_time).toLocaleString('vi-VN')}
                  </span>
                </div>
                <div className="flex flex-col gap-1.5 text-xs text-slate-500 mt-3 font-medium">
                  <div className="flex items-center gap-1.5">
                    <User className="w-3.5 h-3.5" /> <span className="text-slate-700">{c.author || 'Unknown'}</span>
                  </div>
                  <div className="flex items-center gap-1.5">
                    <span className="font-mono bg-slate-100 px-2 py-0.5 rounded text-slate-600 border border-slate-200">
                      hash: {c.hash ? c.hash.slice(0, 8) : '—'}
                    </span>
                  </div>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
};

export default CatalogHistoryTimeline;
