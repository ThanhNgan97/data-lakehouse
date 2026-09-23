# -*- coding: utf-8 -*-
"""
api/routes/superset.py
------------------------------------------------------------
Router FastAPI: Cấp Guest Token cho Apache Superset Embedded Dashboards.
"""

import os
import requests
from fastapi import APIRouter, Depends, HTTPException
from api.dependencies import get_current_user

router = APIRouter()

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
SUPERSET_ADMIN_USER = os.getenv("SUPERSET_ADMIN_USER", "admin")
SUPERSET_ADMIN_PASS = os.getenv("SUPERSET_ADMIN_PASS", "admin")


@router.get("/guest-token", summary="Lấy Guest Token nhúng Superset Dashboard")
def get_superset_guest_token(current_user: dict = Depends(get_current_user)):
    """
    Tạo hoặc trả về Guest Token để nhúng (embed) Superset Dashboards lên ứng dụng Web/Frontend.
    """
    try:
        # Giả lập hoặc gọi Superset REST API lấy security token
        return {
            "status": "success",
            "token": "mock_superset_guest_token_ctu_ioc_2026",
            "superset_url": SUPERSET_URL,
            "message": "Cấp Guest Token Superset thành công."
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Lỗi khởi tạo Superset Guest Token: {str(e)}")
