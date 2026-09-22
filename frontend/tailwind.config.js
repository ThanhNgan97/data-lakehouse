/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        // Nền & chữ trung tính (sidebar tối, chữ)
        ink: {
          50: '#F4F5F8',
          100: '#E7E9F0',
          200: '#C7CBDA',
          300: '#9AA1BA',
          400: '#6B7290',
          500: '#4A5070',
          600: '#363B57',
          700: '#272B40',
          800: '#181B2B',
          900: '#0B0D18',
        },
        // Màu thương hiệu chính — gợi "lakehouse" (mặt nước)
        lake: {
          50: '#EFFBFC',
          100: '#D7F3F6',
          200: '#AFE7ED',
          300: '#7BD5DF',
          400: '#3DB9C7',
          500: '#0F97A8',
          600: '#0B7A8C',
          700: '#0C6270',
          800: '#0F4E59',
          900: '#0B3940',
        },
        // 3 tầng dữ liệu — dùng ĐÚNG tông kim loại theo tên gọi
        bronze: { 100: '#F6E4D3', 200: '#EACBAA', 300: '#E0AD7C', 500: '#B4622A', 600: '#8F4C20', 700: '#6E3A18' },
        silver: { 100: '#EEF0F3', 200: '#DBDFE6', 300: '#C3C9D3', 500: '#7C8595', 600: '#5F6879', 700: '#464D5B' },
        gold:   { 100: '#FBEFCF', 200: '#F3DFA0', 300: '#EAC569', 500: '#C9971E', 600: '#A57A16', 700: '#7C5B10' },
      },
      fontFamily: {
        sans: ['"Plus Jakarta Sans"', 'ui-sans-serif', 'system-ui', 'sans-serif'],
        data: ['"IBM Plex Mono"', 'ui-monospace', 'SFMono-Regular', 'monospace'],
      },
      boxShadow: {
        card: '0 1px 2px rgba(11,13,24,0.04), 0 10px 28px -14px rgba(11,13,24,0.16)',
        popover: '0 8px 30px -6px rgba(11,13,24,0.25)',
      },
      borderRadius: {
        xl2: '1.25rem',
      },
    },
  },
  plugins: [],
}