from fastapi import Depends, Request, HTTPException, status
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from ..database import SessionLocal
from ..models import SecretSessions, Users
from .policy import ActionCode, REQUIREMENT_BY_ACTION, TTL_MIN_BY_ACTION
from .crypto import verify_pin, verify_totp

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def _now_utc():
    # dùng naive UTC cho đơn giản
    return datetime.utcnow()

def _ttl_minutes(action: ActionCode) -> int:
    return TTL_MIN_BY_ACTION.get(action, 480)

def require_secret_lock(action: ActionCode):
    """
    Dependency: kiểm tra user đã có phiên secret mở cho action này chưa.
    Nếu chưa có -> trả 403 để router chuyển về /secret-lock?action=...
    """
    def _checker(request: Request, db: Session = Depends(get_db)) -> Users:
        user_id = request.session.get("user_id")
        if not user_id:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Chưa đăng nhập.")
        user = db.get(Users, user_id)
        if not user:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Tài khoản không hợp lệ.")

        now = _now_utc()
        q = db.query(SecretSessions).filter(
            SecretSessions.user_id == user.id,
            SecretSessions.expires_at > now
        )
        # có thể mở rộng filter theo action_scope trong tương lai
        has_valid = False
        for ss in q.all():
            # Cho phép phiên bất kỳ nếu còn hạn và người dùng vừa mở (đơn giản hoá)
            # hoặc kiểm tra đúng action_scope nếu muốn chặt chẽ:
            if not ss.action_scope or ss.action_scope == action.value:
                has_valid = True
                break
        if not has_valid:
            # Gợi ý client chuyển hướng tới trang mở khoá
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="SECRET_LOCK_REQUIRED")
        return user
    return _checker

def attempt_unlock(user: Users, db: Session, action: ActionCode, pin: str | None, otp: str | None) -> bool:
    """
    Xác minh theo yêu cầu của action -> tạo SecretSession nếu đúng.
    """
    req = REQUIREMENT_BY_ACTION.get(action, "NONE")

    # Kiểm tra theo req
    if req == "NONE":
        ok = True
    elif req == "PIN":
        ok = bool(user.pin_hash and pin and verify_pin(pin, user.pin_hash))
    elif req == "TOTP":
        ok = bool(user.totp_seed and otp and verify_totp(user.totp_seed, otp))
    elif req == "PIN+TOTP":
        ok = bool(user.pin_hash and pin and verify_pin(pin, user.pin_hash)) and bool(user.totp_seed and otp and verify_totp(user.totp_seed, otp))
    else:
        ok = False

    if not ok:
        return False

    ttl = _ttl_minutes(action)
    expires_at = _now_utc() + timedelta(minutes=ttl)

    ss = SecretSessions(user_id=user.id, factor_type=req, issued_at=_now_utc(), expires_at=expires_at, action_scope=action.value)
    db.add(ss); db.commit()
    return True
