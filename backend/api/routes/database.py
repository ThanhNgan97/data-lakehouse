"""Browser-facing helpers for database source onboarding."""

from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from api.dependencies import get_current_user


router = APIRouter()


@router.get("/database/tool/windows", summary="Tải db-provisioner cho Windows")
async def download_windows_tool(current_user=Depends(get_current_user)):
    binary = Path(__file__).resolve().parents[3] / "lakehouse" / "airbyte" / "db-provisioner.exe"
    if not binary.is_file():
        raise HTTPException(status_code=404, detail="Bản cài đặt Windows chưa được build trên máy chủ.")

    return FileResponse(
        path=binary,
        filename="db-provisioner.exe",
        media_type="application/vnd.microsoft.portable-executable",
    )
