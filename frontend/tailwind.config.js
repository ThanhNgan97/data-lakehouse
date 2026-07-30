/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",
    "./index.html"
  ],
  theme: {
    extend: {
      fontFamily: {
        sans: ['Inter', 'sans-serif'],
      },
      colors: {
        edu: {
          blue: '#1d4ed8', // blue-700
          teal: '#0f766e', // teal-700
        }
      }
    },
  },
  plugins: [],
}