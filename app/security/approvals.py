# -*- coding: utf-8 -*-
from fastapi import Request, HTTPException, status
from sqlalchemy.orm import Session

from ..security.deps import login_required
from ..security.policy import ActionCode

# cố gắng import verify_pin; nếu thiếu sẽ báo lỗi cấu hình
try:
    from ..security.crypto import verify_pin as _verify_pin
except Exception:  # pragma: no cover
    _verify_pin = None

def check_manager_pin(request: Request, db: Session, pin: str, action: ActionCode) -> None:
    """
    Kiểm tra PIN người đang thực hiện hành động nhạy cảm (duyệt/ký/chốt).
    - Không thay đổi DB/migration.
    - Không phụ thuộc mô-đun khác ngoài crypto.verify_pin và Users.pin_hash (nếu có).
    - Ném HTTPException khi không đạt yêu cầu.
    """
    user = login_required(request, db)

    # Thiếu thư viện/bộ hàm xác thực PIN
    if not _verify_pin:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Hệ thống chưa sẵn sàng kiểm PIN (thiếu crypto.verify_pin). Hãy bật module crypto."
        )

    # Tài khoản chưa có trường/giá trị PIN
    if not hasattr(user, "pin_hash"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Tài khoản này chưa được cấu hình PIN (thiếu cột pin_hash)."
        )
    pin_hash = getattr(user, "pin_hash", None)
    if not pin_hash:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Tài khoản chưa thiết lập PIN. Vui lòng cấp PIN trong trang quản trị."
        )

    # PIN bắt buộc
    if not pin or not pin.strip():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Vui lòng nhập PIN để xác nhận hành động."
        )

    # Xác thực PIN
    if not _verify_pin(pin.strip(), pin_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="PIN không chính xác."
        )

    # Nếu cần, tại đây có thể ghi audit nội bộ theo action (không bắt buộc):
    # vd: log(u.id, action), nhưng để nguyên tắc “sửa tối thiểu” nên không ghi ở đây.
    return
