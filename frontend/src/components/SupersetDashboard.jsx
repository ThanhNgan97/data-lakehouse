import React, { useEffect, useState, useRef } from "react";
import { embedDashboard } from "@superset-ui/embedded-sdk";
import axios from "axios";
import { Loader2 } from "lucide-react";

export default function SupersetDashboard({ dashboardId }) {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const containerRef = useRef(null);

  useEffect(() => {
    let isMounted = true;

    const fetchTokenAndEmbed = async () => {
      try {
        setLoading(true);
        // 1. Fetch guest token from backend
        // We use our existing axios configuration if needed, or a direct call.
        // Assuming dashboardId is passed as a query param.
        const API_BASE = "http://localhost:8000"; // Fallback, could be from env
        
        const response = await axios.get(`${API_BASE}/api/superset/guest-token`, {
          params: { dashboard_id: dashboardId },
        });

        const guestToken = response.data.guest_token;

        if (!guestToken) {
          throw new Error("Không nhận được Guest Token từ máy chủ.");
        }

        // 2. Embed dashboard
        if (containerRef.current && isMounted) {
          await embedDashboard({
            id: dashboardId, // Given by Superset
            supersetDomain: "http://localhost:8088", 
            mountPoint: containerRef.current, // The DOM element to render the dashboard
            fetchGuestToken: () => guestToken,
            dashboardUiConfig: {
              hideTitle: true,
              hideChartControls: false,
              hideTab: false,
            },
          });
          setLoading(false);
        }
      } catch (err) {
        console.error("Error embedding Superset dashboard:", err);
        if (isMounted) {
          setError(err.response?.data?.detail || err.message || "Đã xảy ra lỗi khi tải bảng điều khiển.");
          setLoading(false);
        }
      }
    };

    if (dashboardId) {
      fetchTokenAndEmbed();
    }

    return () => {
      isMounted = false;
    };
  }, [dashboardId]);

  return (
    <div className="w-full h-full min-h-[800px] bg-white rounded-lg border border-gray-200 overflow-hidden relative">
      {loading && (
        <div className="absolute inset-0 flex flex-col items-center justify-center bg-gray-50/80 z-10">
          <Loader2 className="w-8 h-8 text-blue-600 animate-spin mb-4" />
          <p className="text-gray-600 font-medium">Đang tải bảng điều khiển...</p>
        </div>
      )}
      
      {error && (
        <div className="absolute inset-0 flex items-center justify-center bg-gray-50 z-10 p-6 text-center">
          <div className="bg-red-50 text-red-600 p-4 rounded-lg max-w-md">
            <h3 className="font-semibold text-lg mb-2">Lỗi tải Dashboard</h3>
            <p>{error}</p>
            <p className="text-sm mt-4 text-red-500">
              Vui lòng kiểm tra lại cấu hình Superset (Dashboard ID, API URL) ở backend.
            </p>
          </div>
        </div>
      )}

      {/* Embedded iframe container */}
      <div 
        ref={containerRef} 
        className="w-full h-full absolute inset-0"
        style={{ opacity: loading ? 0 : 1, transition: "opacity 0.3s ease-in-out" }}
      />
    </div>
  );
}
