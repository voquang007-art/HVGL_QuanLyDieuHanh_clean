from fastapi import Request, HTTPException, status
from sqlalchemy.orm import Session
from .database import SessionLocal
from .models import Users, UserStatus, Roles, RoleCode, UserRoles

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def get_current_user(request: Request, db: Session) -> Users | None:
    user_id = request.session.get("user_id")
    if not user_id:
        return None
    user = db.get(Users, user_id)
    if not user or user.status != UserStatus.ACTIVE:
        return None
    return user

def login_required(request: Request, db: Session) -> Users:
    user = get_current_user(request, db)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Vui lòng đăng nhập.")
    return user

def user_has_any_role(user: Users, db: Session, role_codes: list[RoleCode]) -> bool:
    # Lấy tất cả role của user rồi so sánh theo chuỗi chuẩn hóa để tương thích role cũ/mới.
    role_ids = [ur.role_id for ur in db.query(UserRoles).filter(UserRoles.user_id == user.id).all()]
    codes = set()
    for rid in role_ids:
        r = db.get(Roles, rid)
        if r and getattr(r, "code", None):
            codes.add(str(getattr(r.code, "value", r.code)).upper())
    wanted = {str(getattr(rc, "value", rc)).upper() for rc in role_codes}
    return bool(codes & wanted)
