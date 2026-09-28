/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // Neutral canvas/text scale for the CUSC analytical workspace.
        ink: {
          50: '#F8FAFC',
          100: '#F1F5F9',
          200: '#E2E8F0',
          300: '#CBD5E1',
          400: '#94A3B8',
          500: '#64748B',
          600: '#475569',
          700: '#334155',
          800: '#1E293B',
          900: '#0F172A',
        },
        // Primary navigation — deep institutional navy.
        navy: {
          50: '#F4F7FB',
          100: '#E7EDF5',
          200: '#CAD7E7',
          300: '#A4B9D1',
          400: '#7895B6',
          500: '#577697',
          600: '#405A77',
          700: '#31455C',
          800: '#213145',
          900: '#17263A',
          950: '#0D1B2A',
        },
        // Primary action — CUSC cobalt blue.
        cobalt: {
          50: '#EFF6FF',
          100: '#DBEAFE',
          200: '#BFDBFE',
          300: '#93C5FD',
          400: '#60A5FA',
          500: '#3B82F6',
          600: '#2563EB',
          700: '#1D4ED8',
          800: '#1E40AF',
          900: '#1E3A8A',
        },
        // Secondary cyan highlight.
        lake: {
          50: '#F0F9FF',
          100: '#E0F2FE',
          200: '#BAE6FD',
          300: '#7DD3FC',
          400: '#38BDF8',
          500: '#0EA5E9',
          600: '#0284C7',
          700: '#0369A1',
          800: '#075985',
          900: '#0C4A6E',
        },
        // Application surfaces.
        canvas: '#F8FAFC',
        surface: '#FFFFFF',
        line: '#E2E8F0',
        // Medallion accents.
        bronze: { 100: '#FFF7ED', 200: '#FED7AA', 300: '#FDBA74', 500: '#B45309', 600: '#92400E', 700: '#78350F' },
        silver: { 100: '#F3F8FF', 200: '#CFE0F5', 300: '#B7D0ED', 500: '#7EA6D8', 600: '#5C7FA8', 700: '#456386' },
        gold:   { 100: '#FFFBEB', 200: '#FDE68A', 300: '#F6D365', 500: '#D4AF37', 600: '#A88412', 700: '#7F6510' },
      },
      fontFamily: {
        sans: ['Inter', 'ui-sans-serif', 'system-ui', 'sans-serif'],
        display: ['Manrope', 'Inter', 'ui-sans-serif', 'system-ui', 'sans-serif'],
        data: ['"JetBrains Mono"', 'ui-monospace', 'SFMono-Regular', 'monospace'],
      },
      boxShadow: {
        card: '0 1px 3px rgba(15,23,42,0.04), 0 1px 2px rgba(15,23,42,0.02)',
        elevated: '0 10px 28px -12px rgba(15,23,42,0.18), 0 4px 10px -6px rgba(15,23,42,0.08)',
        popover: '0 20px 25px -5px rgba(15,23,42,0.08), 0 8px 10px -6px rgba(15,23,42,0.03)',
        soft: '0 18px 48px -28px rgba(15,23,42,0.24)',
      },
      borderRadius: {
        control: '0.5rem',
        panel: '0.75rem',
        xl2: '1rem',
      },
    },
  },
  plugins: [],
}
