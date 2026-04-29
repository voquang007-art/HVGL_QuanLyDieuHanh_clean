from __future__ import annotations

from sqlalchemy.orm import Session

from .database import Base, engine, SessionLocal
from .models import BlockCode, RoleCode, Roles, UnitCategory, Units, Users, UserStatus, UserRoles
from .security.crypto import hash_password

DEFAULT_ADMIN_USERNAME = "admin"
DEFAULT_ADMIN_PASSWORD = "HvgL@2026"


def _get_or_create_role(db: Session, code: str, name: str | None = None) -> Roles:
    role = db.query(Roles).filter(Roles.code == code).first()
    if role:
        return role
    role = Roles(code=code, name=name or code)
    db.add(role)
    db.flush()
    return role


def _ensure_core_units(db: Session):
    hdtv = db.query(Units).filter(Units.ten_don_vi == "HĐTV").first()
    if not hdtv:
        hdtv = Units(
            ten_don_vi="HĐTV",
            cap_do=1,
            block_code=BlockCode.HANH_CHINH,
            unit_category=UnitCategory.ROOT,
            path="/org/hdtv",
            order_index=1,
        )
        db.add(hdtv)
        db.flush()

    bgd = db.query(Units).filter(Units.ten_don_vi == "BGĐ").first()
    if not bgd:
        bgd = Units(
            ten_don_vi="BGĐ",
            cap_do=1,
            block_code=BlockCode.CHUYEN_MON,
            unit_category=UnitCategory.EXECUTIVE,
            parent_id=hdtv.id,
            path="/org/hdtv/bgd",
            order_index=2,
        )
        db.add(bgd)
        db.flush()


def initialize_system() -> None:
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        for role_code in RoleCode:
            _get_or_create_role(db, str(role_code.value), str(role_code.value))
        _ensure_core_units(db)

        admin = db.query(Users).filter(Users.username == DEFAULT_ADMIN_USERNAME).first()
        if not admin:
            admin = Users(
                username=DEFAULT_ADMIN_USERNAME,
                full_name="Quản trị hệ thống",
                password_hash=hash_password(DEFAULT_ADMIN_PASSWORD),
                status=UserStatus.ACTIVE,
            )
            db.add(admin)
            db.flush()

        admin_role = db.query(Roles).filter(Roles.code == RoleCode.ROLE_ADMIN).first()
        if admin_role and not db.query(UserRoles).filter(UserRoles.user_id == admin.id, UserRoles.role_id == admin_role.id).first():
            db.add(UserRoles(user_id=admin.id, role_id=admin_role.id))

        db.commit()
    finally:
        db.close()
