import { useEffect, useState } from "react";
import axios from "axios";
import {
  Navigate,
  Route,
  BrowserRouter as Router,
  Routes,
} from "react-router-dom";
import AdminDashboard from "./pages/AdminDashboard";
import CatalogHistoryTimeline from "./pages/CatalogHistoryTimeline";
import Login from "./pages/auth/Login";
import UserUpload from "./pages/UserUpload";
import UserHome from "./pages/UserHome";
import UserDatabase from "./pages/UserDatabase";
import UserSourcePending from "./pages/UserSourcePending";
import Landing from "./pages/Landing";

// Axios global 401 interceptor — tự động logout khi token hết hạn
axios.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      localStorage.removeItem("token");
      localStorage.removeItem("role");
      window.location.href = "/login";
    }
    return Promise.reject(error);
  }
);

function App() {
  const [token, setToken] = useState(localStorage.getItem("token") || "");
  const [role, setRole] = useState(localStorage.getItem("role") || "");

  // Đồng bộ state khi localStorage thay đổi (vd: logout từ tab khác)
  useEffect(() => {
    const sync = () => {
      setToken(localStorage.getItem("token") || "");
      setRole(localStorage.getItem("role") || "");
    };
    window.addEventListener("storage", sync);
    return () => window.removeEventListener("storage", sync);
  }, []);

  return (
    <Router>
      <Routes>
        <Route path="/" element={<Landing />} />
        <Route path="/login" element={<Login setToken={setToken} setRole={setRole} />} />

        <Route
          path="/user"
          element={token ? <UserHome /> : <Navigate to="/login" />}
        />

        <Route path="/user/file" element={token ? <UserUpload /> : <Navigate to="/login" />} />
        <Route path="/user/api" element={token ? <UserSourcePending name="API" /> : <Navigate to="/login" />} />
        <Route path="/user/database" element={token ? <UserDatabase /> : <Navigate to="/login" />} />
        <Route path="/user/iot" element={token ? <UserSourcePending name="IoT" /> : <Navigate to="/login" />} />

        <Route
          path="/admin"
          element={
            token && role === "admin" ? (
              <AdminDashboard />
            ) : (
              <Navigate to="/login" />
            )
          }
        />

        {/* /catalog được xử lý bên trong AdminDashboard (tab catalog),
            giữ route này để backward-compatible với link trực tiếp */}
        <Route
          path="/catalog"
          element={token ? <CatalogHistoryTimeline /> : <Navigate to="/login" />}
        />

        <Route path="*" element={<Navigate to="/" />} />
      </Routes>
    </Router>
  );
}

export default App;
