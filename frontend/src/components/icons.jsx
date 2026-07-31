import React from 'react';

/**
 * Bộ icon dạng outline tối giản, tự viết bằng SVG thuần — không cần cài thêm
 * package (lucide-react, heroicons...) để tránh phát sinh dependency mới.
 * Dùng: <Icon name="layers" className="w-4 h-4" />
 */
const paths = {
  grid: <><rect x="3" y="3" width="7" height="7" rx="1.5" /><rect x="14" y="3" width="7" height="7" rx="1.5" /><rect x="3" y="14" width="7" height="7" rx="1.5" /><rect x="14" y="14" width="7" height="7" rx="1.5" /></>,
  layers: <><path d="M12 3 3 8l9 5 9-5-9-5Z" /><path d="m3 13 9 5 9-5" /><path d="m3 18 9 5 9-5" /></>,
  gitBranch: <><circle cx="6" cy="6" r="2.2" /><circle cx="6" cy="18" r="2.2" /><circle cx="18" cy="9" r="2.2" /><path d="M6 8.2V15.8" /><path d="M6 15.8C6 12 9 9 12 9h3.8" /></>,
  history: <><path d="M3 12a9 9 0 1 0 3-6.7" /><path d="M3 4v5h5" /><path d="M12 7v5l3.5 2" /></>,
  users: <><circle cx="9" cy="8" r="3.2" /><path d="M2.5 20c0-3.6 2.9-6 6.5-6s6.5 2.4 6.5 6" /><path d="M16.5 4.6a3.2 3.2 0 0 1 0 6.2" /><path d="M20 20c0-2.9-1.8-5-4.2-5.7" /></>,
  uploadCloud: <><path d="M7 18a4.5 4.5 0 0 1-.6-8.96A6 6 0 0 1 18 9.5 4.5 4.5 0 0 1 17 18" /><path d="M12 12v7" /><path d="m9 15 3-3 3 3" /></>,
  file: <><path d="M7 3h7l4 4v13a1 1 0 0 1-1 1H7a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1Z" /><path d="M14 3v4h4" /></>,
  checkCircle: <><circle cx="12" cy="12" r="9" /><path d="m8.5 12.3 2.4 2.4 4.6-5.2" /></>,
  alertTriangle: <><path d="M10.6 3.9 2.4 18.1a1.4 1.4 0 0 0 1.2 2.1h16.8a1.4 1.4 0 0 0 1.2-2.1L13.4 3.9a1.4 1.4 0 0 0-2.8 0Z" /><path d="M12 9.5v4.2" /><path d="M12 16.8h.01" /></>,
  refresh: <><path d="M3.5 12a8.5 8.5 0 0 1 14.6-6" /><path d="M20.5 12a8.5 8.5 0 0 1-14.6 6" /><path d="M18 2.5V6h-3.5" /><path d="M6 21.5V18h3.5" /></>,
  search: <><circle cx="11" cy="11" r="7" /><path d="m21 21-4.3-4.3" /></>,
  download: <><path d="M12 3v12" /><path d="m7 10 5 5 5-5" /><path d="M4 20h16" /></>,
  x: <><path d="M6 6l12 12" /><path d="M18 6 6 18" /></>,
  lock: <><rect x="5" y="10.5" width="14" height="9.5" rx="1.8" /><path d="M8 10.5V7.5a4 4 0 0 1 8 0v3" /></>,
  unlock: <><rect x="5" y="10.5" width="14" height="9.5" rx="1.8" /><path d="M8 10.5V7.5a4 4 0 0 1 7.4-2.1" /></>,
  trash: <><path d="M4.5 7h15" /><path d="M9.5 7V4.8c0-.7.6-1.3 1.3-1.3h2.4c.7 0 1.3.6 1.3 1.3V7" /><path d="M6.5 7 7.3 20a1.5 1.5 0 0 0 1.5 1.4h6.4a1.5 1.5 0 0 0 1.5-1.4L17.5 7" /></>,
  externalLink: <><path d="M9 6H6a2 2 0 0 0-2 2v10a2 2 0 0 0 2 2h10a2 2 0 0 0 2-2v-3" /><path d="M14 4h6v6" /><path d="M10 14 20 4" /></>,
  logOut: <><path d="M14 8V5.8a1.8 1.8 0 0 0-1.8-1.8H6.3A1.8 1.8 0 0 0 4.5 5.8v12.4A1.8 1.8 0 0 0 6.3 20h5.9a1.8 1.8 0 0 0 1.8-1.8V16" /><path d="M9 12h11" /><path d="m17 8.5 3.5 3.5-3.5 3.5" /></>,
  chevronRight: <path d="m9 6 6 6-6 6" />,
  chevronDown: <path d="m6 9 6 6 6-6" />,
  chevronsLeft: <><path d="m11 17-5-5 5-5" /><path d="m18 17-5-5 5-5" /></>,
  chevronsRight: <><path d="m13 17 5-5-5-5" /><path d="m6 17 5-5-5-5" /></>,
  database: <><ellipse cx="12" cy="5.5" rx="7.5" ry="3" /><path d="M4.5 5.5v13c0 1.7 3.4 3 7.5 3s7.5-1.3 7.5-3v-13" /><path d="M4.5 12c0 1.7 3.4 3 7.5 3s7.5-1.3 7.5-3" /></>,
  activity: <path d="M3 12h4l2.5-7 5 14L17 12h4" />,
  inbox: <><path d="M4 13.5 6.5 5h11L20 13.5" /><path d="M4 13.5h4.3l1 2.5h5.4l1-2.5H20V19a1.5 1.5 0 0 1-1.5 1.5h-13A1.5 1.5 0 0 1 4 19v-5.5Z" /></>,
  plus: <><path d="M12 5v14" /><path d="M5 12h14" /></>,
  filter: <path d="M4 5h16l-6 8v5.5l-4 2V13L4 5Z" />,
  clock: <><circle cx="12" cy="12" r="9" /><path d="M12 7v5l3.5 2" /></>,
  target: <><circle cx="12" cy="12" r="8" /><circle cx="12" cy="12" r="4" /><circle cx="12" cy="12" r=".6" fill="currentColor" stroke="none" /></>,
  sparkline: <path d="M3 15 8 9l4 4 4-6 5 5" />,
  tag: <><path d="M11.3 3.5H5.8A2.3 2.3 0 0 0 3.5 5.8v5.5c0 .6.25 1.2.68 1.62l7.5 7.5a2.3 2.3 0 0 0 3.25 0l5.4-5.4a2.3 2.3 0 0 0 0-3.25l-7.5-7.5a2.3 2.3 0 0 0-1.53-.67Z" /><circle cx="8.3" cy="8.3" r="1.3" /></>,
};

export default function Icon({ name, className = 'w-4 h-4', strokeWidth = 1.8 }) {
  const path = paths[name];
  if (!path) return null;
  return (
    <svg
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={strokeWidth}
      strokeLinecap="round"
      strokeLinejoin="round"
      className={className}
      aria-hidden="true"
    >
      {path}
    </svg>
  );
}
