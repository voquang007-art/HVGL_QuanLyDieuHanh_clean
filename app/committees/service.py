from __future__ import annotations

from datetime import datetime
from typing import Iterable

from sqlalchemy import or_
from sqlalchemy.orm import Session

from ..models import CommitteeMembers, Committees, Roles, UserRoles

COMMITTEE_ROLE_TRUONG_BAN = "TRUONG_BAN"
COMMITTEE_ROLE_PHO_TRUONG_BAN = "PHO_TRUONG_BAN"
COMMITTEE_ROLE_THANH_VIEN = "THANH_VIEN"
COMMITTEE_MANAGER_ROLES = {COMMITTEE_ROLE_TRUONG_BAN, COMMITTEE_ROLE_PHO_TRUONG_BAN}
COMMITTEE_ACTIVE_STATUS = "ACTIVE"

COMMITTEE_MANAGED_BY_HDTV = "HDTV"
COMMITTEE_MANAGED_BY_BGD = "BGD"
COMMITTEE_MANAGED_BY_VALUES = {COMMITTEE_MANAGED_BY_HDTV, COMMITTEE_MANAGED_BY_BGD}

HDTV_ROLE_CODES = {
    "ROLE_LANH_DAO",
    "ROLE_TONG_GIAM_DOC",
    "ROLE_PHO_TONG_GIAM_DOC_THUONG_TRUC",
    "ROLE_PHO_TONG_GIAM_DOC",
}

BGD_ROLE_CODES = {
    "ROLE_GIAM_DOC",
    "ROLE_PHO_GIAM_DOC_TRUC",
    "ROLE_PHO_GIAM_DOC",
}


def normalize_committee_role(value: str | None) -> str:
    role = (value or "").strip().upper()
    if role in {COMMITTEE_ROLE_TRUONG_BAN, COMMITTEE_ROLE_PHO_TRUONG_BAN, COMMITTEE_ROLE_THANH_VIEN}:
        return role
    return COMMITTEE_ROLE_THANH_VIEN


def committee_role_label(value: str | None) -> str:
    role = normalize_committee_role(value)
    labels = {
        COMMITTEE_ROLE_TRUONG_BAN: "Trưởng ban",
        COMMITTEE_ROLE_PHO_TRUONG_BAN: "Phó trưởng ban",
        COMMITTEE_ROLE_THANH_VIEN: "Thành viên",
    }
    return labels.get(role, "Thành viên")


def normalize_committee_managed_by(value: str | None) -> str:
    managed_by = (value or "").strip().upper()
    if managed_by in COMMITTEE_MANAGED_BY_VALUES:
        return managed_by
    return COMMITTEE_MANAGED_BY_HDTV


def committee_managed_by_label(value: str | None) -> str:
    managed_by = normalize_committee_managed_by(value)
    if managed_by == COMMITTEE_MANAGED_BY_BGD:
        return "BGĐ"
    return "HĐTV"


def _role_code_value(value) -> str:
    return str(getattr(value, "value", value) or "").strip().upper()


def get_user_role_codes(db: Session, user_id: str) -> set[str]:
    rows = (
        db.query(Roles.code)
        .join(UserRoles, UserRoles.role_id == Roles.id)
        .filter(UserRoles.user_id == user_id)
        .all()
    )
    return {_role_code_value(code) for (code,) in rows if code}


def user_is_admin(db: Session, user_id: str) -> bool:
    return "ROLE_ADMIN" in get_user_role_codes(db, user_id)


def user_is_hdtv(db: Session, user_id: str) -> bool:
    role_codes = get_user_role_codes(db, user_id)
    return bool(role_codes & HDTV_ROLE_CODES)


def user_is_bgd(db: Session, user_id: str) -> bool:
    role_codes = get_user_role_codes(db, user_id)
    return bool(role_codes & BGD_ROLE_CODES)


def committee_is_active(committee: Committees | None) -> bool:
    if not committee:
        return False
    status = str(getattr(committee, "status", "") or "").upper()
    if status != COMMITTEE_ACTIVE_STATUS:
        return False
    if not bool(getattr(committee, "is_active", False)):
        return False
    end_date = getattr(committee, "end_date", None)
    if end_date and end_date < datetime.utcnow():
        return False
    return True


def get_user_committee_ids(db: Session, user_id: str, *, active_only: bool = True) -> list[str]:
    query = (
        db.query(CommitteeMembers.committee_id)
        .join(Committees, Committees.id == CommitteeMembers.committee_id)
        .filter(CommitteeMembers.user_id == user_id)
        .filter(CommitteeMembers.is_active == True)  # noqa: E712
    )
    if active_only:
        query = query.filter(Committees.status == COMMITTEE_ACTIVE_STATUS, Committees.is_active == True)  # noqa: E712
    rows = query.distinct().all()
    return [str(committee_id) for (committee_id,) in rows if committee_id]


def user_is_committee_member(db: Session, user_id: str, committee_id: str) -> bool:
    row = (
        db.query(CommitteeMembers.id)
        .join(Committees, Committees.id == CommitteeMembers.committee_id)
        .filter(CommitteeMembers.user_id == user_id)
        .filter(CommitteeMembers.committee_id == committee_id)
        .filter(CommitteeMembers.is_active == True)  # noqa: E712
        .filter(Committees.status == COMMITTEE_ACTIVE_STATUS, Committees.is_active == True)  # noqa: E712
        .first()
    )
    return row is not None


def user_is_committee_manager(db: Session, user_id: str, committee_id: str) -> bool:
    row = (
        db.query(CommitteeMembers.id)
        .join(Committees, Committees.id == CommitteeMembers.committee_id)
        .filter(CommitteeMembers.user_id == user_id)
        .filter(CommitteeMembers.committee_id == committee_id)
        .filter(CommitteeMembers.is_active == True)  # noqa: E712
        .filter(CommitteeMembers.committee_role.in_(list(COMMITTEE_MANAGER_ROLES)))
        .filter(Committees.status == COMMITTEE_ACTIVE_STATUS, Committees.is_active == True)  # noqa: E712
        .first()
    )
    return row is not None


def user_can_manage_committees(db: Session, user_id: str) -> bool:
    """
    Quản trị danh mục Ban kiêm nhiệm chỉ dành cho Admin.
    Trưởng ban/Phó trưởng ban điều hành nghiệp vụ trong Ban nhưng không quản trị danh sách Ban/thành viên ở bước nền.
    """
    return user_is_admin(db, user_id)


def user_can_supervise_committee(db: Session, user_id: str, committee: Committees | None) -> bool:
    """
    Quyền giám sát theo cơ quan quản lý Ban:
    - Admin: giám sát toàn bộ.
    - HĐTV: giám sát toàn bộ Ban.
    - BGĐ: chỉ giám sát Ban được cấu hình managed_by = BGD.
    """
    if not committee:
        return False

    if user_is_admin(db, user_id):
        return True

    if user_is_hdtv(db, user_id):
        return True

    managed_by = normalize_committee_managed_by(getattr(committee, "managed_by", None))
    if managed_by == COMMITTEE_MANAGED_BY_BGD and user_is_bgd(db, user_id):
        return True

    return False


def user_can_view_committee(db: Session, user_id: str, committee_id: str) -> bool:
    committee = db.get(Committees, committee_id)
    if not committee:
        return False

    if user_can_supervise_committee(db, user_id, committee):
        return True

    return user_is_committee_member(db, user_id, committee_id)


def allowed_committee_ids_for_user(db: Session, user_id: str) -> Iterable[str]:
    """
    Trả danh sách Ban user được phép thấy.
    Nguyên tắc:
    - Admin: toàn bộ.
    - HĐTV: toàn bộ.
    - BGĐ: chỉ Ban managed_by = BGD + Ban mà user là thành viên.
    - User thường: chỉ Ban mà user là thành viên.
    """
    if user_is_admin(db, user_id) or user_is_hdtv(db, user_id):
        rows = db.query(Committees.id).all()
        return [str(committee_id) for (committee_id,) in rows if committee_id]

    member_ids = set(get_user_committee_ids(db, user_id, active_only=True))

    if user_is_bgd(db, user_id):
        bgd_rows = (
            db.query(Committees.id)
            .filter(Committees.status == COMMITTEE_ACTIVE_STATUS)
            .filter(Committees.is_active == True)  # noqa: E712
            .filter(Committees.managed_by == COMMITTEE_MANAGED_BY_BGD)
            .all()
        )
        member_ids.update(str(committee_id) for (committee_id,) in bgd_rows if committee_id)

    return list(member_ids)


def user_has_any_committee_access(db: Session, user_id: str) -> bool:
    if user_is_admin(db, user_id) or user_is_hdtv(db, user_id):
        return True

    allowed_ids = list(allowed_committee_ids_for_user(db, user_id))
    return bool(allowed_ids)