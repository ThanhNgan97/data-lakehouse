import React from 'react';
import Icon from './icons';

/* Badge trạng thái — dùng chung 1 hệ tông màu cho toàn app.
   tone: 'ink' | 'lake' | 'bronze' | 'silver' | 'gold' | 'success' | 'danger' | 'warn' */
export const Badge = ({ tone = 'ink', children, dot = false, className = '' }) => {
  const tones = {
    ink: 'bg-ink-50 text-ink-600 border-ink-200',
    lake: 'bg-lake-50 text-lake-700 border-lake-200',
    bronze: 'bg-bronze-100 text-bronze-700 border-bronze-200',
    silver: 'bg-silver-100 text-silver-700 border-silver-200',
    gold: 'bg-gold-100 text-gold-700 border-gold-200',
    success: 'bg-emerald-50 text-emerald-700 border-emerald-200',
    danger: 'bg-rose-50 text-rose-700 border-rose-200',
    warn: 'bg-amber-50 text-amber-700 border-amber-200',
  };
  return (
    <span
      className={`inline-flex items-center gap-1.5 whitespace-nowrap rounded-full border px-2.5 py-1 font-data text-[11px] font-semibold uppercase tracking-[0.04em] ${tones[tone]} ${className}`}
    >
      {dot && <span className="h-1.5 w-1.5 shrink-0 rounded-full bg-current" />}
      {children}
    </span>
  );
};

export const Card = ({ children, className = '', ...rest }) => (
  <div
    className={`rounded-panel border border-line bg-surface shadow-card ${className}`}
    {...rest}
  >
    {children}
  </div>
);

export const PageHeader = ({ eyebrow, title, description, actions }) => (
  <div className="mb-6 flex flex-wrap items-start justify-between gap-4">
    <div className="min-w-0">
      {eyebrow && (
        <p className="mb-1.5 font-data text-[11px] font-semibold uppercase tracking-widest text-cobalt-700">
          {eyebrow}
        </p>
      )}
      <h1 className="font-display text-xl font-bold tracking-tight text-navy-950 sm:text-2xl">{title}</h1>
      {description && <p className="mt-1 max-w-3xl text-sm leading-6 text-ink-500">{description}</p>}
    </div>
    {actions && <div className="flex shrink-0 items-center gap-2">{actions}</div>}
  </div>
);

export const EmptyState = ({ icon = 'inbox', title, description, action }) => (
  <div className="flex flex-col items-center justify-center px-6 py-16 text-center">
    <div className="mb-4 flex h-12 w-12 items-center justify-center rounded-xl border border-cobalt-100 bg-cobalt-50 text-cobalt-700">
      <Icon name={icon} className="h-5 w-5" />
    </div>
    <p className="font-display text-sm font-semibold text-navy-800">{title}</p>
    {description && <p className="mt-1 max-w-sm text-xs leading-5 text-ink-500">{description}</p>}
    {action && <div className="mt-4">{action}</div>}
  </div>
);

/* Chip nhỏ báo trạng thái kết nối hệ thống (Nessie · Trino · MinIO...).
   `ok` giữ nguyên là dữ liệu đầu vào trực quan; component không tự kiểm tra health. */
export const ConnChip = ({ label, ok = true }) => (
  <span className="inline-flex items-center gap-1.5 rounded-full border border-line bg-white px-2.5 py-1 font-data text-[11px] text-ink-500 shadow-sm">
    <span className={`h-1.5 w-1.5 rounded-full ${ok ? 'bg-emerald-500' : 'bg-rose-400'}`} />
    {label}
  </span>
);

export const LakehouseMark = ({ className = 'w-9 h-9' }) => (
  <svg viewBox="0 0 36 36" className={className} aria-hidden="true">
    <path d="M18 4 4 12l14 8 14-8-14-8Z" fill="#F6D365" />
    <path d="M18 15 4 23l14 8 14-8-14-8Z" fill="#B7D0ED" opacity="0.95" />
    <path d="M18 4 4 12l14 8 14-8-14-8Z" fill="none" stroke="#7F6510" strokeOpacity="0.22" />
    <path d="M4 12v6l14 8v-6L4 12Z" fill="#FDBA74" opacity="0.95" />
    <path d="M32 12v6l-14 8v-6l14-8Z" fill="#7EA6D8" opacity="0.9" />
  </svg>
);

export const IconButton = ({ children, className = '', ...rest }) => (
  <button
    className={`inline-flex h-9 w-9 items-center justify-center rounded-control border border-line bg-white text-ink-500 transition hover:border-cobalt-200 hover:bg-cobalt-50 hover:text-cobalt-700 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-cobalt-500 focus-visible:ring-offset-2 ${className}`}
    {...rest}
  >
    {children}
  </button>
);

export const Button = ({ variant = 'primary', className = '', children, ...rest }) => {
  const variants = {
    primary: 'bg-cobalt-700 text-white shadow-sm hover:bg-cobalt-800',
    secondary: 'border border-line bg-white text-navy-800 hover:border-cobalt-200 hover:bg-cobalt-50',
    ghost: 'bg-transparent text-ink-500 hover:bg-ink-50 hover:text-navy-800',
    danger: 'border border-rose-200 bg-white text-rose-600 hover:bg-rose-50',
  };
  return (
    <button
      className={`inline-flex items-center justify-center gap-1.5 rounded-control px-4 py-2 text-sm font-semibold transition focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-cobalt-500 focus-visible:ring-offset-2 disabled:cursor-not-allowed disabled:opacity-50 ${variants[variant]} ${className}`}
      {...rest}
    >
      {children}
    </button>
  );
};
