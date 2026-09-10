from cryptography.fernet import Fernet, InvalidToken

from core.config import FERNET_KEY


def _get_fernet() -> Fernet:
    if not FERNET_KEY:
        raise RuntimeError(
            "FERNET_KEY chưa được cấu hình. "
            "Hãy thêm FERNET_KEY vào file .env."
        )

    try:
        return Fernet(FERNET_KEY.encode("utf-8"))
    except Exception as exc:
        raise RuntimeError("FERNET_KEY không hợp lệ.") from exc


def encrypt_secret(value: str) -> str:
    """Mã hóa secret trước khi lưu xuống database."""
    if value is None:
        raise ValueError("Secret không được phép là None.")

    return _get_fernet().encrypt(
        value.encode("utf-8")
    ).decode("utf-8")


def decrypt_secret(token: str) -> str:
    """Giải mã secret khi runtime thực sự cần sử dụng."""
    if not token:
        raise ValueError("Encrypted secret không được để trống.")

    try:
        return _get_fernet().decrypt(
            token.encode("utf-8")
        ).decode("utf-8")
    except InvalidToken as exc:
        raise ValueError(
            "Không thể giải mã credential. "
            "FERNET_KEY có thể không đúng."
        ) from exc