import httpx
from fastapi import APIRouter, HTTPException, Depends
from core.config import SUPERSET_API_URL, SUPERSET_ADMIN_USER, SUPERSET_ADMIN_PASSWORD

router = APIRouter()

@router.get("/guest-token")
async def get_guest_token(dashboard_id: str):
    """
    Lấy Guest Token từ Superset để nhúng vào frontend.
    Yêu cầu: SUPERSET_API_URL, SUPERSET_ADMIN_USER, SUPERSET_ADMIN_PASSWORD
    """
    async with httpx.AsyncClient() as client:
        try:
            # 1. Đăng nhập lấy access_token của admin
            login_resp = await client.post(
                f"{SUPERSET_API_URL}/api/v1/security/login",
                json={
                    "password": SUPERSET_ADMIN_PASSWORD,
                    "provider": "db",
                    "refresh": True,
                    "username": SUPERSET_ADMIN_USER
                },
                timeout=10.0
            )
            login_resp.raise_for_status()
            access_token = login_resp.json().get("access_token")
            
            if not access_token:
                raise HTTPException(status_code=500, detail="Superset login failed: No access token")
                
            # 2. Sinh Guest Token
            guest_payload = {
                "user": {
                    "username": "guest_user",
                    "first_name": "Guest",
                    "last_name": "User"
                },
                "resources": [
                    {
                        "type": "dashboard",
                        "id": dashboard_id
                    }
                ],
                "rls": []
            }
            
            token_resp = await client.post(
                f"{SUPERSET_API_URL}/api/v1/security/guest_token/",
                headers={"Authorization": f"Bearer {access_token}"},
                json=guest_payload,
                timeout=10.0
            )
            token_resp.raise_for_status()
            guest_token = token_resp.json().get("token")
            
            if not guest_token:
                raise HTTPException(status_code=500, detail="Failed to get guest token")
                
            return {"guest_token": guest_token}
            
        except httpx.HTTPError as e:
            raise HTTPException(status_code=500, detail=f"Lỗi giao tiếp với Superset: {str(e)}")
