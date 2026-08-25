import React from 'react';
import Icon from './icons';

/* Badge trạng thái — dùng chung 1 hệ tông màu cho toàn app.
   tone: 'ink' | 'lake' | 'bronze' | 'silver' | 'gold' | 'success' | 'danger' | 'warn' */
export const Badge = ({ tone = 'ink', children, dot = false, className = '' }) => {
  const tones = {
    ink: 'bg-ink-50 text-ink-600 border-ink-200',
    lake: 'bg-lake-50 text-lake-700 border-lake-200',
    bronze: 'bg-bronze-100 text-bronze-700 border-bronze-300',
    silver: 'bg-silver-100 text-silver-700 border-silver-300',
    gold: 'bg-gold-100 text-gold-700 border-gold-300',
    success: 'bg-emerald-50 text-emerald-700 border-emerald-200',
    danger: 'bg-rose-50 text-rose-700 border-rose-200',
    warn: 'bg-amber-50 text-amber-700 border-amber-200',
  };
  return (
    <span
      className={`inline-flex items-center gap-1.5 text-[11px] font-data font-semibold uppercase tracking-wide px-2 py-1 rounded-md border whitespace-nowrap ${tones[tone]} ${className}`}
    >
      {dot && <span className="w-1.5 h-1.5 rounded-full bg-current shrink-0" />}
      {children}
    </span>
  );
};

export const Card = ({ children, className = '', ...rest }) => (
  <div className={`bg-white rounded-2xl border border-ink-100 shadow-card ${className}`} {...rest}>
    {children}
  </div>
);

export const PageHeader = ({ eyebrow, title, description, actions }) => (
  <div className="flex items-start justify-between gap-4 mb-6 flex-wrap">
    <div>
      {eyebrow && (
        <p className="font-data text-[11px] uppercase tracking-widest text-lake-600 font-semibold mb-1.5">
          {eyebrow}
        </p>
      )}
      <h1 className="text-xl font-bold text-ink-900">{title}</h1>
      {description && <p className="text-sm text-ink-400 mt-1">{description}</p>}
    </div>
    {actions && <div className="flex items-center gap-2 shrink-0">{actions}</div>}
  </div>
);

export const EmptyState = ({ icon = 'inbox', title, description, action }) => (
  <div className="flex flex-col items-center justify-center text-center py-16 px-6">
    <div className="w-12 h-12 rounded-xl bg-ink-50 border border-ink-100 flex items-center justify-center text-ink-300 mb-4">
      <Icon name={icon} className="w-5 h-5" />
    </div>
    <p className="text-sm font-semibold text-ink-700">{title}</p>
    {description && <p className="text-xs text-ink-400 mt-1 max-w-sm">{description}</p>}
    {action && <div className="mt-4">{action}</div>}
  </div>
);

  /* Chip nhỏ báo trạng thái kết nối hệ thống (Nessie · Trino · MinIO...) */
export const ConnChip = ({ label, ok = true }) => (
  <span className="inline-flex items-center gap-1.5 text-[11px] font-data text-ink-500 bg-white border border-ink-100 rounded-full px-2.5 py-1">
    <span className={`w-1.5 h-1.5 rounded-full ${ok ? 'bg-emerald-500' : 'bg-rose-400'}`} />
    {label}
  </span>
);

export const LakehouseMark = ({ className = 'w-9 h-9' }) => (
  <svg viewBox="0 0 36 36" className={className} aria-hidden="true">
    <path d="M18 4 4 12l14 8 14-8-14-8Z" fill="#EAC569" />
    <path d="M18 15 4 23l14 8 14-8-14-8Z" fill="#C3C9D3" opacity="0.9" />
    <path d="M18 4 4 12l14 8 14-8-14-8Z" fill="none" stroke="#7C5B10" strokeOpacity="0.25" />
    <path d="M4 12v6l14 8v-6L4 12Z" fill="#E0AD7C" opacity="0.95" />
    <path d="M32 12v6l-14 8v-6l14-8Z" fill="#7C8595" opacity="0.85" />
  </svg>
);

export const IconButton = ({ children, className = '', ...rest }) => (
  <button
    className={`inline-flex items-center justify-center w-9 h-9 rounded-lg border border-ink-100 bg-white text-ink-500 hover:text-lake-700 hover:border-lake-200 hover:bg-lake-50 transition ${className}`}
    {...rest}
  >
    {children}
  </button>
);

export const Button = ({ variant = 'primary', className = '', children, ...rest }) => {
  const variants = {
    primary: 'bg-lake-600 hover:bg-lake-700 text-white shadow-sm',
    secondary: 'bg-white hover:bg-ink-50 text-ink-700 border border-ink-200',
    ghost: 'bg-transparent hover:bg-ink-50 text-ink-500',
    danger: 'bg-white hover:bg-rose-50 text-rose-600 border border-rose-200',
  };
  return (
    <button
      className={`inline-flex items-center justify-center gap-1.5 text-sm font-semibold px-4 py-2 rounded-lg transition disabled:opacity-50 disabled:cursor-not-allowed ${variants[variant]} ${className}`}
      {...rest}
    >
      {children}
    </button>
  );
};
