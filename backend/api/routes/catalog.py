# -*- coding: utf-8 -*-
"""
api/routes/catalog.py
------------------------------------------------------------
Endpoint đọc lịch sử phiên bản (commit history) và danh sách
branch/tag từ Nessie, phục vụ hiển thị timeline versioning
trên AdminDashboard.

Đặt file này vào: backend/api/routes/catalog.py

Cài thêm thư viện (nếu chưa có):
    pip install httpx
------------------------------------------------------------
"""

import os
import httpx
from fastapi import APIRouter, Depends, HTTPException, Query

from api.dependencies import get_current_user

router = APIRouter()

# Có thể đưa biến này vào core/config.py để đồng bộ với các cấu hình khác.
# Vì Nessie chạy trong docker-compose và backend cũng nên gọi qua localhost
# (nếu backend chạy ngoài container) hoặc qua tên service 'demo-nessie'
# (nếu backend cũng được đóng gói trong cùng docker network).
# Server Nessie trong docker-compose (0.104.5) vẫn phục vụ tốt API v1, và
# nessie-spark-extensions 0.77.1 phía Spark BẮT BUỘC dùng v1 (đã xác nhận qua
# lỗi NessieApiCompatibilityException khi thử trỏ sang v2) -> dùng v1 xuyên suốt
# để nhất quán với phần Spark, tránh 2 client nói 2 "phương ngữ" REST khác nhau.
NESSIE_API_URL = os.environ.get("NESSIE_API_URL", "http://localhost:19120/api/v1")


@router.get("/catalog/history")
async def get_catalog_history(
    ref: str = Query(default="main", description="Tên branch/tag Nessie cần xem lịch sử"),
    limit: int = Query(default=50, le=200),
    current_user: dict = Depends(get_current_user),
):
    """
    Trả về lịch sử commit (version history) của catalog Nessie trên 1 ref
    (mặc định 'main'), phục vụ hiển thị timeline versioning trên dashboard.
    """
    url = f"{NESSIE_API_URL}/trees/tree/{ref}/log"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, params={"maxRecords": limit})
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Không thể kết nối tới Nessie: {str(e)}")

    commits = []
    for entry in data.get("logEntries", []):
        commit_meta = entry.get("commitMeta", {})
        commits.append({
            "hash": commit_meta.get("hash"),
            "author": commit_meta.get("author"),
            "message": commit_meta.get("message"),
            "commit_time": commit_meta.get("commitTime"),
            "properties": commit_meta.get("properties", {}),
        })

    return {"ref": ref, "total": len(commits), "commits": commits}


@router.get("/catalog/references")
async def get_catalog_references(current_user: dict = Depends(get_current_user)):
    """
    Trả về danh sách toàn bộ branch/tag hiện có trong Nessie, phục vụ
    dropdown chọn ref để xem lịch sử trên UI.
    """
    url = f"{NESSIE_API_URL}/trees"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            data = resp.json()
    except httpx.HTTPError as e:
        raise HTTPException(status_code=502, detail=f"Không thể kết nối tới Nessie: {str(e)}")

    references = [
        {"name": ref.get("name"), "type": ref.get("type"), "hash": ref.get("hash")}
        for ref in data.get("references", [])
    ]
    return {"references": references}