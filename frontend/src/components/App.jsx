// import React, { useState } from 'react'; // THÊM CHỮ "React," VÀO ĐÂY
// import { BrowserRouter as Router, Routes, Route, Navigate } from 'react-router-dom';
// import Login from './pages/Login';
// import UserUpload from './pages/UserUpload';
// import AdminDashboard from './pages/AdminDashboard';

// function App() {
//   // Khởi tạo state để quản lý token và quyền người dùng
//   const [token, setToken] = useState(localStorage.getItem('token') || '');
//   const [role, setRole] = useState(localStorage.getItem('role') || '');

//   return (
//     <Router>
//       <Routes>
//         {/* Truyền setToken và setRole vào trang Login */}
//         <Route path="/" element={<Login setToken={setToken} setRole={setRole} />} />

//         {/* Phân quyền Router bảo mật */}
//         <Route path="/user" element={role ? <UserUpload /> : <Navigate to="/" />} />
//         <Route path="/admin" element={role === 'admin' ? <AdminDashboard /> : <Navigate to="/" />} />
//         <Route path="*" element={<Navigate to="/" />} />
//       </Routes>
//     </Router>
//   );
// }

// export default App;
