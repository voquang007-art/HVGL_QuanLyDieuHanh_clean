# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import os
import unicodedata
import mimetypes
from urllib.parse import quote
from datetime import datetime, timedelta
from typing import List, Optional, Set

from fastapi import APIRouter, Depends, Form, HTTPException, Request, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, PlainTextResponse, FileResponse
from sqlalchemy.orm import Session

from app.chat.deps import get_display_name
from app.chat.models import (
    ChatAttachments,
    ChatGroupMembers,
    ChatGroups,
    ChatMessages,
    ChatMeetingAttendances,
    ChatMeetings,
    ChatMeetingLeaveRequests,
    ChatMeetingSpeakerRequests,
)
from app.models import (
    BlockCode,
    CommitteeMembers,
    Committees,
    RoleCode,
    Roles,
    UnitCategory,
    UserRoles,
    UserStatus,
    UserUnitMemberships,
    Users,
    Units,
)

from app.chat.realtime import manager
from app.chat.service import (
    add_member_to_group,
    remove_member_from_group,
    approve_speaker_request,
    assign_meeting_secretary,
    create_group,
    create_meeting_session,
    create_message,
    create_speaker_request,
    enrich_groups_for_list,
    ensure_meeting_attendance_rows,
    get_available_users_for_group,
    get_group_by_id,
    get_group_member_user_ids,
    get_group_members,
    get_group_messages,
    get_meeting_attendance_rows,
    get_meeting_by_group_id,
    get_message_attachments,
    get_user_meeting_groups,
    is_group_member,
    list_speaker_requests,
    mark_meeting_absent,
    mark_meeting_checkin,
    move_speaker_request,
    save_message_attachment,
    set_meeting_presence,
    transition_meeting_status_if_needed,
    assign_meeting_host,
    auto_assign_meeting_host,
    remove_absent_members_from_live_meeting,
)
from app.office_preview import (
    OfficePreviewError,
    ensure_office_pdf_preview,
    is_office_previewable,
)
from app.config import settings
from app.database import Base, engine, get_db
from app.security.deps import login_required
from app.committees.service import (
    COMMITTEE_MANAGER_ROLES,
    get_user_committee_ids,
    user_can_view_committee,
    user_is_committee_manager,
)
from starlette.templating import Jinja2Templates

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "..", "templates"))

def _ensure_meeting_tables() -> None:
    Base.metadata.create_all(
        bind=engine,
        tables=[
            ChatGroups.__table__,
            ChatGroupMembers.__table__,
            ChatMessages.__table__,
            ChatAttachments.__table__,
            ChatMeetings.__table__,
            ChatMeetingAttendances.__table__,
            ChatMeetingSpeakerRequests.__table__,
            ChatMeetingLeaveRequests.__table__,
        ],
        checkfirst=True,
    )
    
def _company_name() -> str:
    return getattr(settings, "COMPANY_NAME", "") or "Bệnh viện Hùng Vương Gia Lai"


def _app_name() -> str:
    return getattr(settings, "APP_NAME", "") or "ƯNGD DỤNG QUẢN LÝ, ĐIỀU HÀNH CÔNG VIỆC"


def _load_role_codes_for_user(db: Session, user_id: str) -> Set[str]:
    rows = (
        db.query(Roles.code)
        .join(UserRoles, UserRoles.role_id == Roles.id)
        .filter(UserRoles.user_id == user_id)
        .all()
    )
    result: Set[str] = set()
    for (code,) in rows:
        raw = getattr(code, "value", code)
        result.add(str(raw or "").strip().upper())
    return result

ROLE_ADMIN_BOARD = {
    str(RoleCode.ROLE_LANH_DAO.value),
    str(RoleCode.ROLE_TONG_GIAM_DOC.value),
    str(RoleCode.ROLE_PHO_TONG_GIAM_DOC_THUONG_TRUC.value),
    str(RoleCode.ROLE_PHO_TONG_GIAM_DOC.value),
}
ROLE_EXECUTIVE_BOARD = {
    str(RoleCode.ROLE_LANH_DAO.value),
    str(RoleCode.ROLE_GIAM_DOC.value),
    str(RoleCode.ROLE_PHO_GIAM_DOC_TRUC.value),
    str(RoleCode.ROLE_PHO_GIAM_DOC.value),
}
ROLE_ADMIN_DEPT = {
    str(RoleCode.ROLE_TRUONG_PHONG.value),
    str(RoleCode.ROLE_PHO_PHONG.value),
}
ROLE_ADMIN_TEAM = {
    str(RoleCode.ROLE_TO_TRUONG.value),
    str(RoleCode.ROLE_PHO_TO.value),
}
ROLE_CLINICAL_DEPT = {
    str(RoleCode.ROLE_TRUONG_KHOA.value),
    str(RoleCode.ROLE_PHO_TRUONG_KHOA.value),
    str(RoleCode.ROLE_DIEU_DUONG_TRUONG.value),
    str(RoleCode.ROLE_KY_THUAT_VIEN_TRUONG.value),
}
ROLE_CLINICAL_GROUP = {
    str(RoleCode.ROLE_TRUONG_NHOM.value),
    str(RoleCode.ROLE_PHO_NHOM.value),
}
ROLE_CLINICAL_UNIT = {
    str(RoleCode.ROLE_TRUONG_DON_VI.value),
    str(RoleCode.ROLE_PHO_DON_VI.value),
    str(RoleCode.ROLE_DIEU_DUONG_TRUONG_DON_VI.value),
    str(RoleCode.ROLE_KY_THUAT_VIEN_TRUONG_DON_VI.value),
}

ROLE_BOARD = ROLE_ADMIN_BOARD
ROLE_DEPT = ROLE_ADMIN_DEPT
ROLE_TEAM = ROLE_ADMIN_TEAM
ROLE_MEETING_MANAGERS = (
    ROLE_ADMIN_BOARD
    | ROLE_EXECUTIVE_BOARD
    | ROLE_ADMIN_DEPT
    | ROLE_ADMIN_TEAM
    | ROLE_CLINICAL_DEPT
    | ROLE_CLINICAL_GROUP
    | ROLE_CLINICAL_UNIT
)

SCOPE_ADMIN_DEPARTMENT = "ADMIN_DEPARTMENT"
SCOPE_ADMIN_TEAM = "ADMIN_TEAM"
SCOPE_ADMIN_CROSS_DEPARTMENT = "ADMIN_CROSS_DEPARTMENT"
SCOPE_ADMIN_CROSS_TEAM = "ADMIN_CROSS_TEAM"
SCOPE_ADMIN_BOARD = "ADMIN_BOARD"

SCOPE_CLINICAL_DEPARTMENT = "CLINICAL_DEPARTMENT"
SCOPE_CLINICAL_GROUP = "CLINICAL_GROUP"
SCOPE_CLINICAL_UNIT = "CLINICAL_UNIT"
SCOPE_CLINICAL_CROSS_DEPARTMENT = "CLINICAL_CROSS_DEPARTMENT"
SCOPE_CLINICAL_CROSS_GROUP = "CLINICAL_CROSS_GROUP"
SCOPE_CLINICAL_CROSS_UNIT = "CLINICAL_CROSS_UNIT"
SCOPE_CLINICAL_BOARD = "CLINICAL_BOARD"
SCOPE_COMMITTEE = "COMMITTEE"


def _user_has_any_role(role_codes: Set[str], role_set: Set[str]) -> bool:
    return bool(role_codes & role_set)


def _unit_block_code(unit) -> str:
    if not unit:
        return ""
    value = getattr(unit, "block_code", None)
    return str(getattr(value, "value", value) or "")


def _unit_category(unit) -> str:
    if not unit:
        return ""
    value = getattr(unit, "unit_category", None)
    return str(getattr(value, "value", value) or "")


def _get_parent_unit(db: Session, unit):
    parent_id = _unit_parent_id(unit)
    if not parent_id:
        return None
    return db.get(Units, parent_id)


def _is_admin_board_role_codes(role_codes: Set[str]) -> bool:
    return _user_has_any_role(role_codes, ROLE_ADMIN_BOARD)


def _is_executive_board_role_codes(role_codes: Set[str]) -> bool:
    return _user_has_any_role(role_codes, ROLE_EXECUTIVE_BOARD)


def _is_admin_department_role_codes(role_codes: Set[str]) -> bool:
    return _user_has_any_role(role_codes, ROLE_ADMIN_DEPT)


def _is_admin_team_role_codes(role_codes: Set[str]) -> bool:
    return _user_has_any_role(role_codes, ROLE_ADMIN_TEAM)


def _is_clinical_department_role_codes(role_codes: Set[str]) -> bool:
    return _user_has_any_role(role_codes, ROLE_CLINICAL_DEPT)


def _is_clinical_group_role_codes(role_codes: Set[str]) -> bool:
    return _user_has_any_role(role_codes, ROLE_CLINICAL_GROUP)


def _is_clinical_unit_role_codes(role_codes: Set[str]) -> bool:
    return _user_has_any_role(role_codes, ROLE_CLINICAL_UNIT)


def _get_user_scope_context(db: Session, user_id: str) -> dict:
    primary_unit = _get_user_primary_unit(db, user_id)
    parent_unit = _get_parent_unit(db, primary_unit)
    role_codes = _load_role_codes_for_user(db, user_id)

    admin_department_unit = None
    admin_team_unit = None
    clinical_department_unit = None
    clinical_group_unit = None
    clinical_unit = None
    executive_unit = None
    root_unit = None

    primary_category = _unit_category(primary_unit)
    primary_block = _unit_block_code(primary_unit)
    parent_category = _unit_category(parent_unit)
    parent_block = _unit_block_code(parent_unit)

    if primary_category == UnitCategory.ROOT.value and primary_block == BlockCode.HANH_CHINH.value:
        root_unit = primary_unit

    if primary_category == UnitCategory.EXECUTIVE.value and primary_block == BlockCode.CHUYEN_MON.value:
        executive_unit = primary_unit

    if primary_category == UnitCategory.PHONG.value and primary_block == BlockCode.HANH_CHINH.value:
        admin_department_unit = primary_unit
    elif primary_category == UnitCategory.SUBUNIT.value and primary_block == BlockCode.HANH_CHINH.value:
        admin_team_unit = primary_unit
        if parent_category == UnitCategory.PHONG.value and parent_block == BlockCode.HANH_CHINH.value:
            admin_department_unit = parent_unit

    if primary_category == UnitCategory.KHOA.value and primary_block == BlockCode.CHUYEN_MON.value:
        clinical_department_unit = primary_unit
    elif primary_category == UnitCategory.SUBUNIT.value and primary_block == BlockCode.CHUYEN_MON.value:
        if parent_category == UnitCategory.KHOA.value and parent_block == BlockCode.CHUYEN_MON.value:
            clinical_department_unit = parent_unit
            if _is_clinical_unit_role_codes(role_codes):
                clinical_unit = primary_unit
            else:
                clinical_group_unit = primary_unit

    block = ""
    if admin_department_unit or admin_team_unit or root_unit:
        block = BlockCode.HANH_CHINH.value
    elif clinical_department_unit or clinical_group_unit or clinical_unit or executive_unit:
        block = BlockCode.CHUYEN_MON.value

    return {
        "primary_unit": primary_unit,
        "parent_unit": parent_unit,
        "role_codes": role_codes,
        "block": block,
        "root_unit": root_unit,
        "executive_unit": executive_unit,
        "admin_department_unit": admin_department_unit,
        "admin_team_unit": admin_team_unit,
        "clinical_department_unit": clinical_department_unit,
        "clinical_group_unit": clinical_group_unit,
        "clinical_unit": clinical_unit,
    }


def _same_unit(left, right) -> bool:
    return bool(_unit_id(left) and _unit_id(left) == _unit_id(right))


def _can_create_scope(role_codes: Set[str], meeting_scope: str) -> bool:
    scope = (meeting_scope or "").strip().upper()

    if scope == SCOPE_ADMIN_TEAM:
        return _is_admin_team_role_codes(role_codes)

    if scope == SCOPE_ADMIN_DEPARTMENT:
        return _is_admin_department_role_codes(role_codes)

    if scope == SCOPE_ADMIN_CROSS_TEAM:
        return _is_admin_team_role_codes(role_codes)

    if scope == SCOPE_ADMIN_CROSS_DEPARTMENT:
        return _is_admin_department_role_codes(role_codes)

    if scope == SCOPE_ADMIN_BOARD:
        return _is_admin_board_role_codes(role_codes) or _is_admin_department_role_codes(role_codes)

    if scope == SCOPE_CLINICAL_GROUP:
        return _is_clinical_group_role_codes(role_codes)

    if scope == SCOPE_CLINICAL_UNIT:
        return _is_clinical_unit_role_codes(role_codes)

    if scope == SCOPE_CLINICAL_DEPARTMENT:
        return _is_clinical_department_role_codes(role_codes)

    if scope == SCOPE_CLINICAL_CROSS_GROUP:
        return _is_clinical_group_role_codes(role_codes)

    if scope == SCOPE_CLINICAL_CROSS_UNIT:
        return _is_clinical_unit_role_codes(role_codes)

    if scope == SCOPE_CLINICAL_CROSS_DEPARTMENT:
        return _is_clinical_department_role_codes(role_codes)

    if scope == SCOPE_CLINICAL_BOARD:
        return _is_executive_board_role_codes(role_codes) or _is_clinical_department_role_codes(role_codes)

    if scope == "TEAM":
        return _user_has_any_role(role_codes, ROLE_TEAM)

    if scope == "DEPARTMENT":
        return _user_has_any_role(role_codes, ROLE_DEPT)

    if scope == "HDTV":
        return _user_has_any_role(role_codes, ROLE_BOARD | ROLE_DEPT)

    if scope == "CROSS_DEPARTMENT":
        return _user_has_any_role(role_codes, ROLE_DEPT)

    if scope == "CROSS_TEAM":
        return _user_has_any_role(role_codes, ROLE_TEAM)

    if scope == SCOPE_COMMITTEE:
        return True

    return False

def _unit_id(unit) -> Optional[str]:
    if not unit:
        return None
    return str(getattr(unit, "id", "") or "").strip() or None


def _unit_parent_id(unit) -> Optional[str]:
    if not unit:
        return None
    value = getattr(unit, "parent_id", None)
    return str(value or "").strip() or None


def _unit_name(unit) -> str:
    if not unit:
        return "Đơn vị"
    return (
        getattr(unit, "ten_don_vi", None)
        or getattr(unit, "name", None)
        or f"Đơn vị #{getattr(unit, 'id', '')}"
    )


def _get_user_primary_membership(db: Session, user_id: str):
    return (
        db.query(UserUnitMemberships)
        .filter(UserUnitMemberships.user_id == user_id)
        .order_by(UserUnitMemberships.is_primary.desc())
        .first()
    )


def _get_user_department_unit(db: Session, user_id: str):
    unit = _get_user_primary_unit(db, user_id)
    if not unit:
        return None

    parent_id = _unit_parent_id(unit)
    if parent_id:
        return db.get(Units, parent_id) or unit
    return unit


def _get_user_team_unit(db: Session, user_id: str):
    unit = _get_user_primary_unit(db, user_id)
    if not unit:
        return None
    return unit if _unit_parent_id(unit) else None


def _get_user_membership_scope(db: Session, user_id: str) -> tuple[Set[str], Set[str]]:
    rows = (
        db.query(UserUnitMemberships.unit_id, Units.parent_id)
        .join(Units, Units.id == UserUnitMemberships.unit_id)
        .filter(UserUnitMemberships.user_id == user_id)
        .all()
    )

    unit_ids: Set[str] = set()
    parent_ids: Set[str] = set()

    for unit_id, parent_id in rows:
        uid = str(unit_id or "").strip()
        pid = str(parent_id or "").strip()
        if uid:
            unit_ids.add(uid)
        if pid:
            parent_ids.add(pid)

    return unit_ids, parent_ids
    
    
def _format_participant_label(db: Session, user_obj: Users) -> str:
    display_name = get_display_name(user_obj)
    unit = _get_user_primary_unit(db, user_obj.id)
    unit_text = _unit_name(unit) if unit else "Chưa có đơn vị"
    return f"{display_name} - {unit_text}"


def _list_scope_participant_users(
    db: Session,
    creator: Users,
    meeting_scope: str,
) -> List[Users]:
    scope = (meeting_scope or SCOPE_ADMIN_TEAM).strip().upper()
    creator_ctx = _get_user_scope_context(db, creator.id)

    all_users = (
        db.query(Users)
        .filter(Users.status == UserStatus.ACTIVE)
        .order_by(Users.full_name.asc(), Users.username.asc())
        .all()
    )

    result: List[Users] = []
    seen: Set[str] = set()

    for user_obj in all_users:
        uid = str(getattr(user_obj, "id", "") or "").strip()
        if not uid or uid in seen:
            continue

        user_ctx = _get_user_scope_context(db, uid)
        roles = user_ctx["role_codes"]
        allow = False

        if scope == SCOPE_ADMIN_DEPARTMENT:
            allow = bool(
                _is_admin_board_role_codes(roles)
                or _same_unit(user_ctx["admin_department_unit"], creator_ctx["admin_department_unit"])
                or (
                    user_ctx["admin_team_unit"]
                    and _same_unit(user_ctx["admin_department_unit"], creator_ctx["admin_department_unit"])
                )
            )

        elif scope == SCOPE_ADMIN_TEAM:
            allow = bool(
                creator_ctx["admin_team_unit"]
                and _same_unit(user_ctx["admin_team_unit"], creator_ctx["admin_team_unit"])
            )

        elif scope == SCOPE_ADMIN_CROSS_DEPARTMENT:
            allow = bool(
                _is_admin_department_role_codes(roles)
                and _unit_category(user_ctx["primary_unit"]) == UnitCategory.PHONG.value
                and _unit_block_code(user_ctx["primary_unit"]) == BlockCode.HANH_CHINH.value
            )

        elif scope == SCOPE_ADMIN_CROSS_TEAM:
            allow = bool(
                _is_admin_team_role_codes(roles)
                and creator_ctx["admin_department_unit"]
                and _same_unit(user_ctx["admin_department_unit"], creator_ctx["admin_department_unit"])
            )

        elif scope == SCOPE_ADMIN_BOARD:
            allow = bool(
                _is_admin_department_role_codes(roles)
                and _unit_category(user_ctx["primary_unit"]) == UnitCategory.PHONG.value
                and _unit_block_code(user_ctx["primary_unit"]) == BlockCode.HANH_CHINH.value
            )

        elif scope == SCOPE_CLINICAL_DEPARTMENT:
            same_khoa = bool(
                creator_ctx["clinical_department_unit"]
                and _same_unit(user_ctx["clinical_department_unit"], creator_ctx["clinical_department_unit"])
            )
            allow = bool(
                _is_admin_board_role_codes(roles)
                or _is_executive_board_role_codes(roles)
                or (
                    same_khoa
                    and (
                        _same_unit(user_ctx["primary_unit"], creator_ctx["clinical_department_unit"])
                        or user_ctx["clinical_group_unit"]
                        or (
                            user_ctx["clinical_unit"]
                            and _is_clinical_unit_role_codes(roles)
                        )
                    )
                )
            )

        elif scope == SCOPE_CLINICAL_GROUP:
            allow = bool(
                creator_ctx["clinical_group_unit"]
                and _same_unit(user_ctx["primary_unit"], creator_ctx["clinical_group_unit"])
            )

        elif scope == SCOPE_CLINICAL_UNIT:
            allow = bool(
                creator_ctx["clinical_unit"]
                and _same_unit(user_ctx["primary_unit"], creator_ctx["clinical_unit"])
            )

        elif scope == SCOPE_CLINICAL_CROSS_DEPARTMENT:
            allow = bool(
                _is_clinical_department_role_codes(roles)
                and _unit_category(user_ctx["primary_unit"]) == UnitCategory.KHOA.value
                and _unit_block_code(user_ctx["primary_unit"]) == BlockCode.CHUYEN_MON.value
            )

        elif scope == SCOPE_CLINICAL_CROSS_GROUP:
            allow = bool(
                _is_clinical_group_role_codes(roles)
                and creator_ctx["clinical_department_unit"]
                and _same_unit(user_ctx["clinical_department_unit"], creator_ctx["clinical_department_unit"])
            )

        elif scope == SCOPE_CLINICAL_CROSS_UNIT:
            allow = bool(
                _is_clinical_unit_role_codes(roles)
                and creator_ctx["clinical_department_unit"]
                and _same_unit(user_ctx["clinical_department_unit"], creator_ctx["clinical_department_unit"])
            )

        elif scope == SCOPE_CLINICAL_BOARD:
            allow = bool(
                _is_clinical_department_role_codes(roles)
                and _unit_category(user_ctx["primary_unit"]) == UnitCategory.KHOA.value
                and _unit_block_code(user_ctx["primary_unit"]) == BlockCode.CHUYEN_MON.value
            )

        elif scope == "TEAM":
            creator_team = _get_user_team_unit(db, creator.id)
            creator_team_id = _unit_id(creator_team)
            primary_unit = _get_user_primary_unit(db, uid)
            primary_unit_id = _unit_id(primary_unit)
            allow = bool(creator_team_id and primary_unit_id == creator_team_id)

        elif scope == "DEPARTMENT":
            creator_dept = _get_user_department_unit(db, creator.id)
            creator_dept_id = _unit_id(creator_dept)
            primary_unit = _get_user_primary_unit(db, uid)
            primary_unit_id = _unit_id(primary_unit)
            primary_parent_id = _unit_parent_id(primary_unit)
            membership_unit_ids, membership_parent_ids = _get_user_membership_scope(db, uid)
            allow = bool(
                creator_dept_id
                and (
                    primary_unit_id == creator_dept_id
                    or primary_parent_id == creator_dept_id
                    or creator_dept_id in membership_unit_ids
                    or creator_dept_id in membership_parent_ids
                )
            )

        elif scope == "HDTV":
            allow = bool(roles & (ROLE_BOARD | ROLE_DEPT | ROLE_TEAM))

        elif scope == "CROSS_DEPARTMENT":
            allow = bool(roles & ROLE_DEPT)

        elif scope == "CROSS_TEAM":
            creator_dept = _get_user_department_unit(db, creator.id)
            creator_dept_id = _unit_id(creator_dept)
            primary_unit = _get_user_primary_unit(db, uid)
            primary_parent_id = _unit_parent_id(primary_unit)
            allow = bool(
                creator_dept_id
                and primary_parent_id == creator_dept_id
                and roles & ROLE_TEAM
            )

        if allow:
            result.append(user_obj)
            seen.add(uid)

    return result


def _get_manageable_committee_ids_for_meeting(db: Session, user_id: str) -> list[str]:
    """
    Ban được phép tạo họp:
    - Admin không tự động tạo họp Ban thay Ban ở bước này.
    - Trưởng ban/Phó trưởng ban được tạo họp trong Ban mình quản lý.
    """
    rows = (
        db.query(CommitteeMembers.committee_id)
        .join(Committees, Committees.id == CommitteeMembers.committee_id)
        .filter(CommitteeMembers.user_id == user_id)
        .filter(CommitteeMembers.is_active == True)  # noqa: E712
        .filter(CommitteeMembers.committee_role.in_(list(COMMITTEE_MANAGER_ROLES)))
        .filter(Committees.status == "ACTIVE", Committees.is_active == True)  # noqa: E712
        .filter(Committees.allow_meetings == True)  # noqa: E712
        .distinct()
        .all()
    )
    return [str(committee_id) for (committee_id,) in rows if committee_id]


def _get_meeting_committee_options(db: Session, user_id: str) -> list[Committees]:
    committee_ids = _get_manageable_committee_ids_for_meeting(db, user_id)
    if not committee_ids:
        return []

    return (
        db.query(Committees)
        .filter(Committees.id.in_(committee_ids))
        .filter(Committees.status == "ACTIVE", Committees.is_active == True)  # noqa: E712
        .filter(Committees.allow_meetings == True)  # noqa: E712
        .order_by(Committees.name.asc())
        .all()
    )


def _list_committee_participant_users(db: Session, committee_id: str) -> List[Users]:
    committee_id = str(committee_id or "").strip()
    if not committee_id:
        return []

    rows = (
        db.query(Users)
        .join(CommitteeMembers, CommitteeMembers.user_id == Users.id)
        .filter(CommitteeMembers.committee_id == committee_id)
        .filter(CommitteeMembers.is_active == True)  # noqa: E712
        .filter(Users.status == UserStatus.ACTIVE)
        .order_by(Users.full_name.asc(), Users.username.asc())
        .all()
    )

    dedup: dict[str, Users] = {}
    for user_obj in rows:
        if user_obj and getattr(user_obj, "id", None):
            dedup[str(user_obj.id)] = user_obj

    return list(dedup.values())


def _committee_member_role(db: Session, committee_id: str, user_id: str) -> str:
    row = (
        db.query(CommitteeMembers.committee_role)
        .filter(CommitteeMembers.committee_id == committee_id)
        .filter(CommitteeMembers.user_id == user_id)
        .filter(CommitteeMembers.is_active == True)  # noqa: E712
        .first()
    )
    if not row:
        return ""
    return str(row[0] or "").strip().upper()


def _pick_committee_host_and_secretary(
    db: Session,
    committee_id: str,
    participant_ids: List[str],
    creator_id: str,
) -> tuple[Optional[str], Optional[str]]:
    """
    Chủ trì mặc định:
    - Ưu tiên Trưởng ban trong thành phần dự họp.
    - Nếu không có Trưởng ban, chọn Phó trưởng ban.
    - Nếu không có, chọn người tạo.
    """
    manager_ids = []
    deputy_ids = []

    for uid in participant_ids:
        role = _committee_member_role(db, committee_id, uid)
        if role == "TRUONG_BAN":
            manager_ids.append(uid)
        elif role == "PHO_TRUONG_BAN":
            deputy_ids.append(uid)

    if manager_ids:
        return manager_ids[0], None

    if deputy_ids:
        return deputy_ids[0], None

    return creator_id, None


def _build_allowed_scope_options_for_user(role_codes: Set[str]) -> List[tuple[str, str]]:
    options: List[tuple[str, str]] = []

    if _is_admin_team_role_codes(role_codes):
        options.append((SCOPE_ADMIN_TEAM, "Họp tổ"))
        options.append((SCOPE_ADMIN_CROSS_TEAM, "Họp liên tổ"))

    if _is_admin_department_role_codes(role_codes):
        options.append((SCOPE_ADMIN_DEPARTMENT, "Họp phòng"))
        options.append((SCOPE_ADMIN_CROSS_DEPARTMENT, "Họp liên phòng"))

    if _is_admin_board_role_codes(role_codes) or _is_admin_department_role_codes(role_codes):
        options.append((SCOPE_ADMIN_BOARD, "Họp do HĐTV triệu tập"))

    if _is_clinical_group_role_codes(role_codes):
        options.append((SCOPE_CLINICAL_GROUP, "Họp nhóm"))
        options.append((SCOPE_CLINICAL_CROSS_GROUP, "Họp liên nhóm"))

    if _is_clinical_unit_role_codes(role_codes):
        options.append((SCOPE_CLINICAL_UNIT, "Họp đơn vị"))
        options.append((SCOPE_CLINICAL_CROSS_UNIT, "Họp liên đơn vị"))

    if _is_clinical_department_role_codes(role_codes):
        options.append((SCOPE_CLINICAL_DEPARTMENT, "Họp khoa"))
        options.append((SCOPE_CLINICAL_CROSS_DEPARTMENT, "Họp liên khoa"))

    if _is_executive_board_role_codes(role_codes) or _is_clinical_department_role_codes(role_codes):
        options.append((SCOPE_CLINICAL_BOARD, "Họp do HĐTV-BGĐ triệu tập"))

    return options


def _find_first_participant_by_roles(
    db: Session,
    participant_ids: List[str],
    wanted_roles: Set[str],
) -> Optional[str]:
    for user_id in participant_ids:
        codes = _load_role_codes_for_user(db, user_id)
        if codes & wanted_roles:
            return user_id
    return None
    
def _find_participants_by_roles(
    db: Session,
    participant_ids: List[str],
    wanted_roles: Set[str],
) -> List[str]:
    result: List[str] = []
    for user_id in participant_ids:
        codes = _load_role_codes_for_user(db, user_id)
        if codes & wanted_roles:
            result.append(user_id)
    return result

    
def _role_priority(role_codes: Set[str]) -> int:
    if "ROLE_LANH_DAO" in role_codes:
        return 1
    if "ROLE_TRUONG_PHONG" in role_codes:
        return 2
    if "ROLE_PHO_PHONG" in role_codes:
        return 3
    if "ROLE_TO_TRUONG" in role_codes:
        return 4
    if "ROLE_PHO_TO" in role_codes:
        return 5
    return 99


def _can_create_meeting(role_codes: Set[str]) -> bool:
    return bool(_build_allowed_scope_options_for_user(role_codes))


def _get_attendance_row_for_user(db: Session, meeting_id: str, user_id: str):
    rows = get_meeting_attendance_rows(db, meeting_id)
    for row in rows:
        if row.user_id == user_id:
            return row
    return None


def _can_manage_meeting_schedule(meeting, user_id: str) -> bool:
    if not meeting or not user_id:
        return False
    return user_id in {
        getattr(meeting, "host_user_id", None),
        getattr(meeting, "secretary_user_id", None),
        getattr(meeting, "designed_by_user_id", None),
    }

def _can_assign_meeting_host(meeting, user_id: str) -> bool:
    if not meeting or not user_id:
        return False
    return user_id in {
        getattr(meeting, "host_user_id", None),
        getattr(meeting, "secretary_user_id", None),
        getattr(meeting, "designed_by_user_id", None),
    }


def _request_wants_json(request: Request) -> bool:
    xrw = str(request.headers.get("X-Requested-With", "") or "").strip().lower()
    accept = str(request.headers.get("Accept", "") or "").strip().lower()
    return xrw == "xmlhttprequest" or "application/json" in accept


def _json_or_redirect(request: Request, url: str, payload: dict):
    if _request_wants_json(request):
        return JSONResponse({"ok": True, **payload})
    return RedirectResponse(url=url, status_code=303)


def _attendance_is_available(row) -> bool:
    status = (getattr(row, "attendance_status", "") or "PENDING").upper()
    return status not in {"ABSENT", "LEFT"}


def _pick_first_available_user_by_role_sets(
    db: Session,
    attendance_rows: List,
    role_sets: List[Set[str]],
) -> Optional[str]:
    for wanted_roles in role_sets:
        for row in attendance_rows:
            if not _attendance_is_available(row):
                continue
            codes = _load_role_codes_for_user(db, row.user_id)
            if codes & wanted_roles:
                return row.user_id
    return None


def _resolve_fallback_host_user_id(db: Session, meeting, attendance_rows: List) -> Optional[str]:
    if not meeting:
        return None

    # 1) Ưu tiên HĐTV
    fallback_user_id = _pick_first_available_user_by_role_sets(
        db,
        attendance_rows,
        [ROLE_ADMIN_BOARD],
    )
    if fallback_user_id:
        return fallback_user_id

    # 2) Ưu tiên BGĐ
    fallback_user_id = _pick_first_available_user_by_role_sets(
        db,
        attendance_rows,
        [ROLE_EXECUTIVE_BOARD],
    )
    if fallback_user_id:
        return fallback_user_id

    # 3) Ưu tiên Thư ký nếu còn hợp lệ
    secretary_user_id = getattr(meeting, "secretary_user_id", None)
    if secretary_user_id:
        secretary_row = next((r for r in attendance_rows if str(r.user_id) == str(secretary_user_id)), None)
        if secretary_row and _attendance_is_available(secretary_row):
            return secretary_user_id

    # 4) Theo loại cuộc họp
    scope = (getattr(meeting, "meeting_scope", "") or "").strip().upper()

    if scope in {SCOPE_ADMIN_TEAM, "TEAM"}:
        fallback_user_id = _pick_first_available_user_by_role_sets(
            db,
            attendance_rows,
            [ROLE_ADMIN_TEAM],
        )
        if fallback_user_id:
            return fallback_user_id

    elif scope in {SCOPE_ADMIN_DEPARTMENT, "DEPARTMENT"}:
        fallback_user_id = _pick_first_available_user_by_role_sets(
            db,
            attendance_rows,
            [ROLE_ADMIN_DEPT],
        )
        if fallback_user_id:
            return fallback_user_id

    elif scope in {
        SCOPE_CLINICAL_DEPARTMENT,
        SCOPE_CLINICAL_CROSS_DEPARTMENT,
        SCOPE_CLINICAL_BOARD,
    }:
        fallback_user_id = _pick_first_available_user_by_role_sets(
            db,
            attendance_rows,
            [ROLE_CLINICAL_DEPT],
        )
        if fallback_user_id:
            return fallback_user_id

    elif scope in {SCOPE_CLINICAL_GROUP, SCOPE_CLINICAL_CROSS_GROUP}:
        fallback_user_id = _pick_first_available_user_by_role_sets(
            db,
            attendance_rows,
            [ROLE_CLINICAL_GROUP],
        )
        if fallback_user_id:
            return fallback_user_id

    elif scope in {SCOPE_CLINICAL_UNIT, SCOPE_CLINICAL_CROSS_UNIT}:
        fallback_user_id = _pick_first_available_user_by_role_sets(
            db,
            attendance_rows,
            [ROLE_CLINICAL_UNIT],
        )
        if fallback_user_id:
            return fallback_user_id

    # 5) Cuối cùng mới đến Người thiết kế
    designed_by_user_id = getattr(meeting, "designed_by_user_id", None)
    if designed_by_user_id:
        designed_row = next((r for r in attendance_rows if str(r.user_id) == str(designed_by_user_id)), None)
        if designed_row and _attendance_is_available(designed_row):
            return designed_by_user_id

    return None

def _ensure_meeting_runtime_rules(db: Session, meeting):
    if not meeting:
        return None

    meeting = transition_meeting_status_if_needed(db, meeting)
    if not meeting:
        return None

    attendance_rows = get_meeting_attendance_rows(db, meeting.id)
    attendance_map = {row.user_id: row for row in attendance_rows}

    host_user_id = getattr(meeting, "host_user_id", None)
    host_row = attendance_map.get(host_user_id)
    host_status = (getattr(host_row, "attendance_status", "") or "PENDING").upper() if host_row else "PENDING"

    if (meeting.meeting_status or "").upper() == "LIVE":
        # Loại ngay người báo vắng ra khỏi cuộc họp khi họp đã LIVE
        remove_absent_members_from_live_meeting(db, meeting.id)

        # Nếu Chủ trì đã báo vắng hoặc đã bị loại, auto fallback ngay
        current_host_still_member = bool(host_user_id and is_group_member(db, meeting.group_id, host_user_id))
        if host_status in {"ABSENT", "LEFT"} or not current_host_still_member:
            fallback_host_id = _resolve_fallback_host_user_id(db, meeting, attendance_rows)
            if fallback_host_id and fallback_host_id != host_user_id:
                meeting = assign_meeting_host(db, meeting.id, fallback_host_id) or meeting

    refreshed = db.get(ChatMeetings, meeting.id)
    return refreshed or meeting
    
    
def _to_datetime_local_value(dt_value) -> str:
    if not dt_value:
        return ""
    try:
        return (dt_value + timedelta(hours=7)).strftime("%Y-%m-%dT%H:%M")
    except Exception:
        return ""

def _attendance_is_checked_in(attendance_row) -> bool:
    status = (getattr(attendance_row, "attendance_status", "") or "PENDING").upper()
    return status == "CHECKED_IN"


def _consume_current_speaker_permission(db: Session, meeting_id: str, user_id: str) -> bool:
    speaker_rows = list_speaker_requests(db, meeting_id)
    for row in speaker_rows:
        if row.user_id != user_id:
            continue
        status = (getattr(row, "request_status", "") or "").upper()
        if status in {"APPROVED", "SPEAKING"}:
            row.request_status = "DONE"
            db.add(row)
            db.commit()
            return True
    return False


def _can_user_send_meeting_message(
    db: Session,
    meeting,
    user_id: str,
    attendance_row=None,
) -> bool:
    if not meeting or not user_id:
        return False

    if (meeting.meeting_status or "").upper() != "LIVE":
        return False

    if attendance_row is None:
        attendance_row = _get_attendance_row_for_user(db, meeting.id, user_id)

    if not _attendance_is_checked_in(attendance_row):
        return False

    if meeting.host_user_id == user_id:
        return True

    speaker_rows = list_speaker_requests(db, meeting.id)
    for row in speaker_rows:
        if row.user_id != user_id:
            continue
        status = (getattr(row, "request_status", "") or "").upper()
        if status in {"APPROVED", "SPEAKING"}:
            return True

    return False
    
    
def _is_browser_previewable(filename: str) -> bool:
    name = (filename or "").strip().lower()
    return (
        name.endswith(".pdf")
        or name.endswith(".png")
        or name.endswith(".jpg")
        or name.endswith(".jpeg")
        or name.endswith(".gif")
        or name.endswith(".webp")
        or name.endswith(".txt")
        or is_office_previewable(filename)
    )


def _build_content_disposition(disposition: str, filename: str) -> str:
    raw_name = (filename or "file").strip() or "file"

    ascii_fallback = raw_name.encode("ascii", "ignore").decode("ascii").strip()
    if not ascii_fallback:
        ascii_fallback = "file"

    ascii_fallback = ascii_fallback.replace("\\", "_").replace('"', "_")
    encoded_name = quote(raw_name, safe="")

    return f"{disposition}; filename=\"{ascii_fallback}\"; filename*=UTF-8''{encoded_name}"


def _static_url_to_abs_path(url_path: str) -> str:
    clean_path = str(url_path or "").split("?", 1)[0].lstrip("/").replace("/", os.sep)
    if not clean_path:
        return ""

    return os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", clean_path)
    )


def _meeting_attachment_preview_url(attachment_id: str) -> str:
    attachment_id = str(attachment_id or "").strip()
    return f"/meetings/attachments/{attachment_id}/preview" if attachment_id else ""


def _meeting_attachment_download_url(attachment_id: str) -> str:
    attachment_id = str(attachment_id or "").strip()
    return f"/meetings/attachments/{attachment_id}/download" if attachment_id else ""


def _ensure_meeting_attachment_access(db: Session, attachment_id: str, user_id: str) -> tuple[ChatAttachments, ChatMessages]:
    attachment = db.get(ChatAttachments, attachment_id)
    if not attachment:
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp.")

    message = db.get(ChatMessages, attachment.message_id)
    if not message:
        raise HTTPException(status_code=404, detail="Không tìm thấy tin nhắn chứa tệp.")

    group_id = getattr(message, "group_id", None)
    if not group_id or not is_group_member(db, group_id, user_id):
        raise HTTPException(status_code=403, detail="Bạn không có quyền truy cập tệp này.")

    return attachment, message


def _build_meeting_documents(messages: List[dict]) -> List[dict]:
    documents: List[dict] = []
    for msg in messages:
        if (msg.get("message_type") or "").upper() != "MEETING_DOC":
            continue

        for att in msg.get("attachments", []) or []:
            attachment_id = str(att.get("id") or "").strip()
            filename = att.get("filename") or "Tệp đính kèm"

            documents.append({
                "id": attachment_id,
                "filename": filename,
                "path": att.get("path") or "#",
                "preview_url": _meeting_attachment_preview_url(attachment_id),
                "download_url": _meeting_attachment_download_url(attachment_id),
                "is_previewable": bool(att.get("is_previewable", False)) or _is_browser_previewable(filename),
                "sender_name": msg.get("sender_name") or "Người dùng",
                "created_at_text": msg.get("created_at_text") or "",
            })

    return documents

def _format_vn_dt_text(dt_value) -> str:
    if not dt_value:
        return "—"
    try:
        return (dt_value + timedelta(hours=7)).strftime("%d/%m/%Y %H:%M")
    except Exception:
        return "—"


def _get_latest_meeting_conclusion_message(db: Session, group_id: str):
    if not group_id:
        return None

    return (
        db.query(ChatMessages)
        .filter(ChatMessages.group_id == group_id)
        .filter(ChatMessages.message_type == "MEETING_CONCLUSION")
        .order_by(ChatMessages.created_at.desc())
        .first()
    )


def _build_minutes_speaker_sections(messages: List[dict]) -> List[dict]:
    sections: List[dict] = []
    by_user: dict[str, dict] = {}

    for msg in messages or []:
        message_type = (msg.get("message_type") or "").strip().upper()
        if message_type not in {"TEXT", "FILE"}:
            continue

        sender_name = (msg.get("sender_name") or "Người dùng").strip() or "Người dùng"
        bucket = by_user.get(sender_name)
        if not bucket:
            bucket = {
                "sender_name": sender_name,
                "entries": [],
            }
            by_user[sender_name] = bucket
            sections.append(bucket)

        content = (msg.get("content") or "").strip()
        attachments = msg.get("attachments") or []

        parts: list[str] = []
        if content:
            parts.append(content)

        if attachments:
            file_names = []
            for att in attachments:
                file_name = (att.get("filename") or "").strip()
                if file_name:
                    file_names.append(file_name)
            if file_names:
                parts.append("Tệp trao đổi: " + ", ".join(file_names))

        if not parts:
            continue

        created_at_text = (msg.get("created_at_text") or "").strip()
        merged_text = " ".join(parts).strip()

        bucket["entries"].append({
            "created_at_text": created_at_text,
            "text": merged_text,
        })

    return sections


def _build_meeting_minutes_text(detail: dict) -> str:
    meeting = detail.get("meeting")
    host = detail.get("host")
    secretary = detail.get("secretary")
    designed_by = detail.get("designed_by")
    attendance_rows = detail.get("attendance_rows") or []
    leave_requests = detail.get("leave_requests") or []
    messages = detail.get("messages") or []
    conclusion_text = (detail.get("conclusion_text") or "").strip()

    invited_count = len(detail.get("member_ids") or [])
    checked_in_count = int(detail.get("attendance_checked_in_count") or 0)
    absent_count = int(detail.get("attendance_absent_count") or 0)

    leave_approved_count = 0
    for row in leave_requests:
        status = (getattr(row, "request_status", "") or "").strip().upper()
        if status == "APPROVED":
            leave_approved_count += 1

    sections = _build_minutes_speaker_sections(messages)

    lines: list[str] = []
    lines.append("BIÊN BẢN HỌP TRỰC TUYẾN")
    lines.append("")
    lines.append(f"Tên cuộc họp: {getattr(detail.get('group'), 'name', '') or '—'}")
    lines.append(f"Loại cuộc họp: {detail.get('scope_label') or '—'}")
    lines.append(f"Thời gian bắt đầu: {_format_vn_dt_text(getattr(meeting, 'scheduled_start_at', None))}")
    lines.append(f"Thời gian kết thúc: {_format_vn_dt_text(getattr(meeting, 'scheduled_end_at', None))}")
    lines.append(f"Chủ trì: {get_display_name(host) if host else '—'}")
    lines.append(f"Thư ký: {get_display_name(secretary) if secretary else 'Chưa chỉ định'}")
    lines.append(f"Người thiết kế: {get_display_name(designed_by) if designed_by else '—'}")
    lines.append(f"Số người mời: {invited_count}")
    lines.append(f"Số người có mặt: {checked_in_count}")
    lines.append(f"Số người báo vắng: {absent_count}")
    lines.append(f"Số người xin rời cuộc họp: {leave_approved_count}")
    lines.append("")
    lines.append("I. THÀNH PHẦN THAM DỰ / HIỆN DIỆN")
    if attendance_rows:
        for idx, row in enumerate(attendance_rows, start=1):
            full_name = get_display_name(row.user) if getattr(row, "user", None) else (row.user_id or "Người dùng")
            attendance_label = getattr(row, "attendance_status_label", None) or _attendance_status_label(getattr(row, "attendance_status", None))
            presence_label = getattr(row, "presence_status_label", None) or ("Đang ở phòng" if (getattr(row, "presence_status", "") or "").upper() == "ONLINE" else "Ngoài phòng")
            absent_reason = (getattr(row, "absent_reason", "") or "").strip()

            extra = []
            if attendance_label:
                extra.append(attendance_label)
            if presence_label:
                extra.append(presence_label)
            if absent_reason:
                extra.append("Lý do: " + absent_reason)

            lines.append(f"{idx}. {full_name} - " + " - ".join(extra))
    else:
        lines.append("Không có dữ liệu thành phần tham dự.")
    lines.append("")
    lines.append("II. TỔNG HỢP Ý KIẾN TRAO ĐỔI THEO NGƯỜI PHÁT BIỂU")
    if sections:
        for idx, section in enumerate(sections, start=1):
            lines.append(f"{idx}. {section['sender_name']}:")
            for entry_idx, entry in enumerate(section["entries"], start=1):
                prefix = f"   {idx}.{entry_idx}"
                if entry.get("created_at_text"):
                    lines.append(f"{prefix} [{entry['created_at_text']}] {entry['text']}")
                else:
                    lines.append(f"{prefix} {entry['text']}")
    else:
        lines.append("Chưa có nội dung trao đổi được ghi nhận.")
    lines.append("")
    lines.append("III. KẾT LUẬN CỦA CHỦ TRÌ")
    if conclusion_text:
        for line in conclusion_text.splitlines():
            clean_line = line.rstrip()
            lines.append(clean_line if clean_line else "")
    else:
        lines.append("Chưa có kết luận cuộc họp.")
    lines.append("")

    return "\n".join(lines)
    
def _get_user_primary_unit(db: Session, user_id: str):
    membership = (
        db.query(UserUnitMemberships)
        .filter(UserUnitMemberships.user_id == user_id)
        .order_by(UserUnitMemberships.is_primary.desc())
        .first()
    )
    return membership.unit if membership else None


def _pick_host_and_secretary(
    db: Session,
    creator: Users,
    participant_ids: List[str],
    meeting_scope: str,
    selected_host_user_id: Optional[str] = None,
) -> tuple[Optional[str], Optional[str]]:
    scope = (meeting_scope or "TEAM").strip().upper()
    creator_roles = _load_role_codes_for_user(db, creator.id)
    selected_host_user_id = str(selected_host_user_id or "").strip()

    if scope == "TEAM":
        host_user_id = _find_first_participant_by_roles(db, participant_ids, {"ROLE_TO_TRUONG"})
        if not host_user_id:
            host_user_id = _find_first_participant_by_roles(db, participant_ids, {"ROLE_PHO_TO"})
        if not host_user_id:
            host_user_id = creator.id
        return host_user_id, None

    if scope == "DEPARTMENT":
        host_user_id = _find_first_participant_by_roles(db, participant_ids, {"ROLE_TRUONG_PHONG"})
        if not host_user_id:
            host_user_id = _find_first_participant_by_roles(db, participant_ids, {"ROLE_PHO_PHONG"})
        if not host_user_id:
            host_user_id = creator.id
        return host_user_id, None

    if scope == "HDTV":
        if _user_has_any_role(creator_roles, ROLE_BOARD):
            return creator.id, None

        board_participant_ids = _find_participants_by_roles(db, participant_ids, ROLE_BOARD)

        # Trưởng phòng / Phó phòng thiết kế, có từ 2 HĐTV trở lên -> bắt buộc chọn 1 HĐTV làm Chủ trì
        if _user_has_any_role(creator_roles, ROLE_DEPT) and len(board_participant_ids) >= 2:
            if not selected_host_user_id:
                raise HTTPException(
                    status_code=400,
                    detail="Cuộc họp có từ 2 vị trí HĐTV trở lên. Phải chọn 1 vị trí HĐTV làm Chủ trì.",
                )
            if selected_host_user_id not in board_participant_ids:
                raise HTTPException(
                    status_code=400,
                    detail="Người được chọn làm Chủ trì phải là vị trí HĐTV thuộc thành phần dự họp.",
                )
            return selected_host_user_id, None

        if len(board_participant_ids) == 1:
            return board_participant_ids[0], None

        if board_participant_ids:
            return board_participant_ids[0], None

        return creator.id, None

    if scope in {"CROSS_DEPARTMENT", "CROSS_TEAM"}:
        return creator.id, None

    return creator.id, None


def _meeting_status_label(value: str) -> str:
    return {
        "UPCOMING": "Sắp họp",
        "LIVE": "Đang họp",
        "ENDED": "Đã kết thúc",
    }.get((value or "").strip().upper(), value or "—")


def _attendance_status_label(value: str) -> str:
    return {
        "PENDING": "Chưa phản hồi",
        "ABSENT": "Báo vắng",
        "CHECKED_IN": "Đã điểm danh",
        "LEFT": "Đã rời họp",
    }.get((value or "").strip().upper(), value or "—")


def _leave_request_status_label(value: str) -> str:
    return {
        "PENDING": "Chờ Chủ trì duyệt",
        "APPROVED": "Đã chấp thuận",
        "REJECTED": "Không chấp thuận",
    }.get((value or "").strip().upper(), value or "—")


def _get_pending_leave_requests(db: Session, meeting_id: str) -> List[ChatMeetingLeaveRequests]:
    return (
        db.query(ChatMeetingLeaveRequests)
        .filter(ChatMeetingLeaveRequests.meeting_id == meeting_id)
        .filter(ChatMeetingLeaveRequests.request_status == "PENDING")
        .order_by(ChatMeetingLeaveRequests.created_at.asc())
        .all()
    )
    
    
def _meeting_scope_label(value: str) -> str:
    return {
        "TEAM": "Họp tổ",
        "DEPARTMENT": "Họp phòng",
        "HDTV": "Họp do HĐTV triệu tập",
        "CROSS_DEPARTMENT": "Họp liên phòng",
        "CROSS_TEAM": "Họp liên tổ",
        SCOPE_ADMIN_TEAM: "Họp tổ",
        SCOPE_ADMIN_DEPARTMENT: "Họp phòng",
        SCOPE_ADMIN_CROSS_TEAM: "Họp liên tổ",
        SCOPE_ADMIN_CROSS_DEPARTMENT: "Họp liên phòng",
        SCOPE_ADMIN_BOARD: "Họp do HĐTV triệu tập",
        SCOPE_CLINICAL_GROUP: "Họp nhóm",
        SCOPE_CLINICAL_UNIT: "Họp đơn vị",
        SCOPE_CLINICAL_DEPARTMENT: "Họp khoa",
        SCOPE_CLINICAL_CROSS_GROUP: "Họp liên nhóm",
        SCOPE_CLINICAL_CROSS_UNIT: "Họp liên đơn vị",
        SCOPE_CLINICAL_CROSS_DEPARTMENT: "Họp liên khoa",
        SCOPE_CLINICAL_BOARD: "Họp do HĐTV-BGĐ triệu tập",
    }.get((value or "").strip().upper(), value or "Cuộc họp")


def _build_message_vm(db: Session, message, current_user_id: str) -> dict:
    sender_name = get_display_name(message.sender) if getattr(message, "sender", None) else "Người dùng"
    attachments = []
    for att in get_message_attachments(db, message.id):
        if getattr(att, "deleted_by_owner", False):
            continue
        filename = getattr(att, "filename", "") or ""
        path = getattr(att, "path", "") or "#"
        attachments.append({
            "id": att.id,
            "filename": filename,
            "path": path,
            "preview_url": _meeting_attachment_preview_url(att.id),
            "download_url": _meeting_attachment_download_url(att.id),
            "is_previewable": _is_browser_previewable(filename),
        })
    created_text = ""
    if getattr(message, "created_at", None):
        created_text = (message.created_at + timedelta(hours=7)).strftime("%d/%m/%Y %H:%M")
    return {
        "id": message.id,
        "sender_name": sender_name,
        "content": message.content or "",
        "message_type": message.message_type,
        "created_at_text": created_text,
        "is_mine": message.sender_user_id == current_user_id,
        "attachments": attachments,
        "recalled": bool(getattr(message, "recalled", False)),
    }

def _can_delete_meeting(meeting, user_id: str) -> bool:
    if not meeting or not user_id:
        return False
    return user_id in {
        getattr(meeting, "designed_by_user_id", None),
        getattr(meeting, "host_user_id", None),
        getattr(meeting, "secretary_user_id", None),
    }

def _can_delete_orphan_meeting_group(group, user_id: str) -> bool:
    if not group or not user_id:
        return False
    return user_id == getattr(group, "owner_user_id", None)
    
    
def _prepare_meeting_groups_for_sidebar(
    db: Session,
    groups: List,
    current_user_id: str,
) -> List:
    prepared: List = []
    for group in groups or []:
        meeting = get_meeting_by_group_id(db, group.id)
        if meeting:
            meeting = _ensure_meeting_runtime_rules(db, meeting)

        sort_dt = (
            getattr(meeting, "scheduled_start_at", None)
            or getattr(meeting, "created_at", None)
            or getattr(group, "created_at", None)
            or datetime.utcnow()
        )

        group.meeting_row = meeting
        group.meeting_sort_at = sort_dt
        group.list_status_label = _meeting_status_label(getattr(meeting, "meeting_status", "")) if meeting else "Cuộc họp"
        group.can_delete_meeting = (
            _can_delete_meeting(meeting, current_user_id)
            if meeting
            else _can_delete_orphan_meeting_group(group, current_user_id)
        )
        prepared.append(group)

    prepared.sort(
        key=lambda item: getattr(item, "meeting_sort_at", None) or datetime.min,
        reverse=True,
    )
    return prepared


def _build_meeting_groups_by_month(
    groups: List,
    selected_id: str = "",
) -> List[dict]:
    buckets: List[dict] = []
    year_map: dict[str, dict] = {}

    for group in groups or []:
        sort_dt = getattr(group, "meeting_sort_at", None) or getattr(group, "created_at", None) or datetime.utcnow()
        year_key = f"{sort_dt.year:04d}"
        month_key = f"{sort_dt.year:04d}-{sort_dt.month:02d}"

        year_bucket = year_map.get(year_key)
        if not year_bucket:
            year_bucket = {
                "year_key": year_key,
                "year_label": f"Năm {sort_dt.year}",
                "months": [],
                "is_open": False,
            }
            year_map[year_key] = year_bucket
            buckets.append(year_bucket)

        month_bucket = None
        for item in year_bucket["months"]:
            if item["month_key"] == month_key:
                month_bucket = item
                break

        if not month_bucket:
            month_bucket = {
                "month_key": month_key,
                "month_label": f"Tháng {sort_dt.month:02d}",
                "groups": [],
                "count": 0,
                "is_open": False,
            }
            year_bucket["months"].append(month_bucket)

        month_bucket["groups"].append(group)
        month_bucket["count"] += 1

        if str(getattr(group, "id", "")) == str(selected_id or ""):
            month_bucket["is_open"] = True
            year_bucket["is_open"] = True

    if not selected_id and buckets:
        buckets[0]["is_open"] = True
        if buckets[0]["months"]:
            buckets[0]["months"][0]["is_open"] = True

    return buckets


def _remove_meeting_group_attachment_files(db: Session, group_id: str) -> None:
    rows = (
        db.query(ChatAttachments.path)
        .join(ChatMessages, ChatMessages.id == ChatAttachments.message_id)
        .filter(ChatMessages.group_id == group_id)
        .all()
    )

    for (path_value,) in rows:
        try:
            rel_path = str(path_value or "").lstrip("/").replace("/", os.sep)
            if not rel_path:
                continue
            abs_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", rel_path))
            if os.path.isfile(abs_path):
                os.remove(abs_path)
        except Exception:
            continue
            
@router.get("/meetings", response_class=HTMLResponse)
def meetings_index(
    request: Request,
    selected_id: str = "",
    db: Session = Depends(get_db),
):
    _ensure_meeting_tables()
    current_user = login_required(request, db)

    groups = get_user_meeting_groups(db, current_user.id)
    groups = enrich_groups_for_list(db, groups, current_user.id)
    groups = _prepare_meeting_groups_for_sidebar(db, groups, current_user.id)
    meeting_groups_by_month = _build_meeting_groups_by_month(groups, selected_id)

    total_meeting_count = len(groups)
    invited_meeting_count = 0
    for g in groups:
        meeting_row = getattr(g, "meeting_row", None)
        designed_by_user_id = str(getattr(meeting_row, "designed_by_user_id", "") or "").strip() if meeting_row else ""
        if designed_by_user_id and designed_by_user_id != str(current_user.id):
            invited_meeting_count += 1

    selected_group = None
    selected_meeting = None
    if selected_id:
        selected_group = get_group_by_id(db, selected_id)
        if selected_group and selected_group.group_type == "MEETING" and is_group_member(db, selected_group.id, current_user.id):
            selected_meeting = get_meeting_by_group_id(db, selected_group.id)
            selected_meeting = _ensure_meeting_runtime_rules(db, selected_meeting)
            if not is_group_member(db, selected_group.id, current_user.id):
                selected_group = None
                selected_meeting = None
        else:
            selected_group = None
    elif groups:
        selected_group = groups[0]
        selected_meeting = get_meeting_by_group_id(db, selected_group.id)
        selected_meeting = _ensure_meeting_runtime_rules(db, selected_meeting)
        if not is_group_member(db, selected_group.id, current_user.id):
            selected_group = None
            selected_meeting = None

    current_user_role_codes = _load_role_codes_for_user(db, current_user.id)
    meeting_scope_options = _build_allowed_scope_options_for_user(current_user_role_codes)
    committee_meeting_options = _get_meeting_committee_options(db, current_user.id)
    if committee_meeting_options:
        meeting_scope_options.append((SCOPE_COMMITTEE, "Họp Ban kiêm nhiệm"))
    current_user_can_create_meeting = bool(meeting_scope_options)

    meeting_participant_options_map: dict[str, dict] = {}
    for scope_value, _scope_label in meeting_scope_options:
        if scope_value == SCOPE_COMMITTEE:
            continue

        for user_obj in _list_scope_participant_users(db, current_user, scope_value):
            user_role_codes = _load_role_codes_for_user(db, user_obj.id)
            item = meeting_participant_options_map.get(user_obj.id)
            if not item:
                item = {
                    "id": user_obj.id,
                    "label": _format_participant_label(db, user_obj),
                    "scopes": [],
                    "committee_ids": [],
                    "is_board": _is_admin_board_role_codes(user_role_codes),
                }
                meeting_participant_options_map[user_obj.id] = item
            if scope_value not in item["scopes"]:
                item["scopes"].append(scope_value)

    for committee in committee_meeting_options:
        for user_obj in _list_committee_participant_users(db, committee.id):
            user_role_codes = _load_role_codes_for_user(db, user_obj.id)
            item = meeting_participant_options_map.get(user_obj.id)
            if not item:
                item = {
                    "id": user_obj.id,
                    "label": _format_participant_label(db, user_obj),
                    "scopes": [],
                    "committee_ids": [],
                    "is_board": _is_admin_board_role_codes(user_role_codes),
                }
                meeting_participant_options_map[user_obj.id] = item

            if SCOPE_COMMITTEE not in item["scopes"]:
                item["scopes"].append(SCOPE_COMMITTEE)

            if committee.id not in item["committee_ids"]:
                item["committee_ids"].append(committee.id)
                item["scopes"].append(scope_value)

    meeting_participant_options = sorted(
        meeting_participant_options_map.values(),
        key=lambda x: x["label"].casefold(),
    )
    
    detail = None
    if selected_group and selected_meeting:
        members = get_group_members(db, selected_group.id)
        attendance_rows = get_meeting_attendance_rows(db, selected_meeting.id)
        attendance_map = {row.user_id: row for row in attendance_rows}
        speaker_requests = list_speaker_requests(db, selected_meeting.id)
        leave_requests = _get_pending_leave_requests(db, selected_meeting.id)
        all_messages = [
            _build_message_vm(db, msg, current_user.id)
            for msg in get_group_messages(db, selected_group.id, limit=150)
        ]
        messages = [
            msg for msg in all_messages
            if (msg.get("message_type") or "").upper() != "MEETING_DOC"
        ]
        member_ids = [m.user_id for m in members]
        available_secretaries = [m.user for m in members if getattr(m, "user", None)]
        available_hosts = []
        for m in members:
            user_obj = getattr(m, "user", None)
            if not user_obj:
                continue
            attendance_row = attendance_map.get(m.user_id)
            attendance_status = (getattr(attendance_row, "attendance_status", "") or "PENDING").upper()
            if attendance_status == "ABSENT":
                continue
            available_hosts.append(user_obj)

        current_attendance = attendance_map.get(current_user.id)
        host = db.get(Users, selected_meeting.host_user_id) if selected_meeting.host_user_id else None
        secretary = db.get(Users, selected_meeting.secretary_user_id) if selected_meeting.secretary_user_id else None
        designed_by = db.get(Users, selected_meeting.designed_by_user_id) if selected_meeting.designed_by_user_id else None

        for row in attendance_rows:
            row.attendance_status_label = _attendance_status_label(row.attendance_status)
            row.presence_status_label = "Đang ở phòng" if (row.presence_status or "").upper() == "ONLINE" else "Ngoài phòng"

        for row in speaker_requests:
            row.user_name = get_display_name(row.user) if getattr(row, "user", None) else row.user_id
            row.request_status_label = {
                "PENDING": "Đang chờ",
                "APPROVED": "Đã cho phép",
                "SPEAKING": "Đang phát biểu",
                "DONE": "Đã phát biểu",
            }.get((row.request_status or "").upper(), row.request_status or "—")
            
        for row in leave_requests:
            row.user_name = get_display_name(row.user) if getattr(row, "user", None) else row.user_id
            row.request_status_label = _leave_request_status_label(row.request_status)
            
        current_attendance_status = (getattr(current_attendance, "attendance_status", "") or "PENDING").upper()
        host_attendance = attendance_map.get(selected_meeting.host_user_id) if selected_meeting.host_user_id else None
        host_attendance_status = (getattr(host_attendance, "attendance_status", "") or "PENDING").upper() if host_attendance else "PENDING"

        can_upload_documents = current_user.id in {
            selected_meeting.host_user_id,
            selected_meeting.secretary_user_id,
        }
        can_manage_schedule = _can_manage_meeting_schedule(selected_meeting, current_user.id)
        can_send_meeting_message = _can_user_send_meeting_message(
            db,
            selected_meeting,
            current_user.id,
            attendance_row=current_attendance,
        )
        can_register_speaker = (
            (selected_meeting.meeting_status or "").upper() == "LIVE"
            and current_attendance_status == "CHECKED_IN"
            and not can_send_meeting_message
)

        has_pending_leave_request = (
            db.query(ChatMeetingLeaveRequests.id)
            .filter(ChatMeetingLeaveRequests.meeting_id == selected_meeting.id)
            .filter(ChatMeetingLeaveRequests.user_id == current_user.id)
            .filter(ChatMeetingLeaveRequests.request_status == "PENDING")
            .first()
            is not None
        )

        can_request_leave = (
            (selected_meeting.meeting_status or "").upper() == "LIVE"
            and current_attendance_status == "CHECKED_IN"
            and current_user.id != selected_meeting.host_user_id
            and not has_pending_leave_request
        )

        attendance_pending_count = 0
        attendance_absent_count = 0
        attendance_checked_in_count = 0
        for row in attendance_rows:
            status = (getattr(row, "attendance_status", "") or "PENDING").upper()
            if status == "ABSENT":
                attendance_absent_count += 1
            elif status == "CHECKED_IN":
                attendance_checked_in_count += 1
            else:
                attendance_pending_count += 1
                
        latest_conclusion_message = _get_latest_meeting_conclusion_message(db, selected_group.id)
        conclusion_text = (getattr(latest_conclusion_message, "content", "") or "").strip()
        conclusion_updated_text = ""
        if getattr(latest_conclusion_message, "created_at", None):
            conclusion_updated_text = _format_vn_dt_text(latest_conclusion_message.created_at)
            
        detail = {
            "group": selected_group,
            "meeting": selected_meeting,
            "messages": messages,
            "meeting_documents": _build_meeting_documents(all_messages),
            "members": members,
            "attendance_rows": attendance_rows,
            "speaker_requests": speaker_requests,
            "host": host,
            "secretary": secretary,
            "designed_by": designed_by,
            "available_secretaries": available_secretaries,
            "available_hosts": available_hosts,
            "available_users": get_available_users_for_group(db, selected_group.id),
            "status_label": _meeting_status_label(selected_meeting.meeting_status),
            "scope_label": _meeting_scope_label(selected_meeting.meeting_scope),
            "current_attendance": current_attendance,
            "current_attendance_status": current_attendance_status,
            "attendance_pending_count": attendance_pending_count,
            "attendance_absent_count": attendance_absent_count,
            "attendance_checked_in_count": attendance_checked_in_count,
            "is_host": selected_meeting.host_user_id == current_user.id,
            "can_assign_host": _can_assign_meeting_host(selected_meeting, current_user.id),
            "host_attendance_status": host_attendance_status,
            "can_upload_documents": can_upload_documents,
            "can_manage_schedule": can_manage_schedule,
            "can_end_now": (
                (selected_meeting.meeting_status or "").upper() == "LIVE"
                and current_user.id in {selected_meeting.host_user_id, selected_meeting.secretary_user_id}
                and current_attendance_status == "CHECKED_IN"
            ),
            "can_send_meeting_message": can_send_meeting_message,
            "can_register_speaker": can_register_speaker,
            "scheduled_start_value": _to_datetime_local_value(selected_meeting.scheduled_start_at),
            "scheduled_end_value": _to_datetime_local_value(selected_meeting.scheduled_end_at),
            "member_ids": member_ids,
            "conclusion_text": conclusion_text,
            "conclusion_updated_text": conclusion_updated_text,
            "can_edit_conclusion": selected_meeting.host_user_id == current_user.id,
            "can_export_minutes": selected_meeting.secretary_user_id == current_user.id,
            "can_delete_meeting": _can_delete_meeting(selected_meeting, current_user.id),
            "leave_requests": leave_requests,
            "can_request_leave": can_request_leave,
            "has_pending_leave_request": has_pending_leave_request,
        }

    return request.app.state.templates.TemplateResponse(
        "meetings/index.html",
        {
            "request": request,
            "company_name": _company_name(),
            "app_name": _app_name(),
            "current_user": current_user,
            "current_user_display_name": get_display_name(current_user),
            "current_user_can_create_meeting": current_user_can_create_meeting,
            "current_user_is_dept_manager": _user_has_any_role(current_user_role_codes, ROLE_DEPT),
            "groups": groups,
            "meeting_groups_by_month": meeting_groups_by_month,
            "total_meeting_count": total_meeting_count,
            "invited_meeting_count": invited_meeting_count,
            "selected_id": selected_group.id if selected_group else "",
            "selected_detail": detail,
            "meeting_participant_options": meeting_participant_options,
            "meeting_scope_options": meeting_scope_options,
            "committee_meeting_options": committee_meeting_options,
        },
    )

@router.get("/meetings/api/nav-badge")
def meeting_nav_badge(
    request: Request,
    db: Session = Depends(get_db),
):
    _ensure_meeting_tables()
    current_user = login_required(request, db)

    groups = get_user_meeting_groups(db, current_user.id)
    groups = enrich_groups_for_list(db, groups, current_user.id)
    groups = _prepare_meeting_groups_for_sidebar(db, groups, current_user.id)

    total_meeting_count = len(groups)
    invited_meeting_count = 0

    for group in groups:
        meeting_row = getattr(group, "meeting_row", None)
        designed_by_user_id = str(getattr(meeting_row, "designed_by_user_id", "") or "").strip() if meeting_row else ""
        if designed_by_user_id and designed_by_user_id != str(current_user.id):
            invited_meeting_count += 1

    return JSONResponse({
        "ok": True,
        "total_meeting_count": total_meeting_count,
        "invited_meeting_count": invited_meeting_count,
    })

@router.post("/meetings/create")
def create_meeting(
    request: Request,
    name: str = Form(...),
    meeting_scope: str = Form(SCOPE_ADMIN_TEAM),
    committee_id: str = Form(""),
    scheduled_start_at: str = Form(...),
    scheduled_end_at: str = Form(""),
    agenda: str = Form(""),
    host_user_id: str = Form(""),
    participant_ids: List[str] = Form([]),
    db: Session = Depends(get_db),
):
    _ensure_meeting_tables()
    current_user = login_required(request, db)
    current_user_role_codes = _load_role_codes_for_user(db, current_user.id)
    scope = (meeting_scope or SCOPE_ADMIN_TEAM).strip().upper()
    clean_committee_id = (committee_id or "").strip()

    if scope == SCOPE_COMMITTEE:
        if not clean_committee_id:
            raise HTTPException(status_code=400, detail="Phải chọn Ban kiêm nhiệm khi tạo Họp Ban kiêm nhiệm.")

        committee = db.get(Committees, clean_committee_id)
        if not committee or not bool(getattr(committee, "is_active", False)) or (committee.status or "").upper() != "ACTIVE":
            raise HTTPException(status_code=404, detail="Không tìm thấy Ban kiêm nhiệm đang hoạt động.")

        if not bool(getattr(committee, "allow_meetings", True)):
            raise HTTPException(status_code=403, detail="Ban kiêm nhiệm này chưa được phép sử dụng chức năng Họp trực tuyến.")

        if not user_is_committee_manager(db, current_user.id, clean_committee_id):
            raise HTTPException(status_code=403, detail="Chỉ Trưởng ban hoặc Phó trưởng ban mới được tạo Họp Ban kiêm nhiệm.")
    else:
        clean_committee_id = ""
        if not _can_create_scope(current_user_role_codes, scope):
            raise HTTPException(status_code=403, detail="Bạn không có quyền tạo loại cuộc họp này.")

    clean_name = (name or "").strip()
    if not clean_name:
        raise HTTPException(status_code=400, detail="Tên cuộc họp không được để trống.")

    try:
        start_local = datetime.strptime((scheduled_start_at or "").strip(), "%Y-%m-%dT%H:%M")
        start_dt = start_local - timedelta(hours=7)
    except Exception:
        raise HTTPException(status_code=400, detail="Thời gian bắt đầu không hợp lệ.")

    end_dt = None
    if (scheduled_end_at or "").strip():
        try:
            end_local = datetime.strptime((scheduled_end_at or "").strip(), "%Y-%m-%dT%H:%M")
            end_dt = end_local - timedelta(hours=7)
        except Exception:
            raise HTTPException(status_code=400, detail="Thời gian kết thúc không hợp lệ.")

    if scope == SCOPE_COMMITTEE:
        allowed_users = _list_committee_participant_users(db, clean_committee_id)
    else:
        allowed_users = _list_scope_participant_users(db, current_user, scope)

    allowed_user_ids = {str(u.id) for u in allowed_users}

    clean_participants: List[str] = []
    for user_id in participant_ids or []:
        uid = str(user_id or "").strip()
        if not uid:
            continue
        if uid not in allowed_user_ids:
            continue
        if uid not in clean_participants:
            clean_participants.append(uid)

    if current_user.id not in clean_participants:
        clean_participants.insert(0, current_user.id)

    if scope == SCOPE_COMMITTEE:
        host_user_id, secretary_user_id = _pick_committee_host_and_secretary(
            db,
            clean_committee_id,
            clean_participants,
            current_user.id,
        )
    else:
        host_user_id, secretary_user_id = _pick_host_and_secretary(
            db,
            current_user,
            clean_participants,
            scope,
            selected_host_user_id=host_user_id,
        )

    creator_ctx = _get_user_scope_context(db, current_user.id)
    group_unit = None
    if scope != SCOPE_COMMITTEE:
        group_unit = (
            creator_ctx["admin_department_unit"]
            or creator_ctx["admin_team_unit"]
            or creator_ctx["clinical_department_unit"]
            or creator_ctx["clinical_group_unit"]
            or creator_ctx["clinical_unit"]
            or creator_ctx["executive_unit"]
            or creator_ctx["root_unit"]
            or creator_ctx["primary_unit"]
        )

    group = create_group(
        db,
        name=clean_name,
        owner_user_id=current_user.id,
        group_type="MEETING",
        unit_id=_unit_id(group_unit),
    )

    for uid in clean_participants:
        if uid == current_user.id:
            continue
        add_member_to_group(
            db,
            group_id=group.id,
            user_id=uid,
            member_role="member",
            mark_as_new=True,
        )

    meeting = create_meeting_session(
        db,
        group_id=group.id,
        designed_by_user_id=current_user.id,
        host_user_id=host_user_id,
        secretary_user_id=secretary_user_id,
        meeting_scope=scope,
        scheduled_start_at=start_dt,
        scheduled_end_at=end_dt,
        agenda=agenda,
        committee_id=clean_committee_id or None,
    )
    ensure_meeting_attendance_rows(db, meeting.id, clean_participants)

    return RedirectResponse(url=f"/meetings?selected_id={group.id}", status_code=303)

@router.post("/meetings/{group_id}/delete")
async def meeting_delete(
    group_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)

    group = get_group_by_id(db, group_id)
    if not group or (group.group_type or "").upper() != "MEETING":
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này.")

    meeting = get_meeting_by_group_id(db, group_id)

    if meeting:
        if not _can_delete_meeting(meeting, current_user.id):
            raise HTTPException(
                status_code=403,
                detail="Chỉ Người thiết kế, Chủ trì hoặc Thư ký mới được xóa cuộc họp.",
            )
    else:
        if not _can_delete_orphan_meeting_group(group, current_user.id):
            raise HTTPException(
                status_code=403,
                detail="Chỉ người đã tạo cuộc họp lỗi này mới được xóa.",
            )

    if meeting and not _can_delete_meeting(meeting, current_user.id):
        raise HTTPException(
            status_code=403,
            detail="Chỉ Người thiết kế, Chủ trì hoặc Thư ký mới được xóa cuộc họp.",
        )

    deleted_group_id = str(group_id or "").strip()
    deleted_group_name = str(getattr(group, "name", "") or "").strip()
    deleted_meeting_id = str(getattr(meeting, "id", "") or "").strip() if meeting else ""
    notify_user_ids = get_group_member_user_ids(db, deleted_group_id)

    _remove_meeting_group_attachment_files(db, deleted_group_id)

    if meeting:
        db.query(ChatMeetingSpeakerRequests).filter(
            ChatMeetingSpeakerRequests.meeting_id == meeting.id
        ).delete(synchronize_session=False)

        db.query(ChatMeetingAttendances).filter(
            ChatMeetingAttendances.meeting_id == meeting.id
        ).delete(synchronize_session=False)

        db.delete(meeting)

    db.delete(group)
    db.commit()

    payload = {
        "type": "meeting_deleted",
        "group_id": deleted_group_id,
        "meeting_id": deleted_meeting_id,
        "group_name": deleted_group_name,
        "deleted_by_user_id": str(current_user.id),
        "deleted_by_user_name": get_display_name(current_user),
        "redirect_url": "/meetings",
    }

    await manager.broadcast_group_text(deleted_group_id, json.dumps(payload, ensure_ascii=False))
    await manager.notify_users_json(notify_user_ids, {"module": "meeting", **payload})

    if _request_wants_json(request):
        return JSONResponse({"ok": True, **payload})

    return RedirectResponse(url="/meetings", status_code=status.HTTP_303_SEE_OTHER)
    
@router.post("/meetings/{group_id}/schedule")
async def meeting_update_schedule(
    group_id: str,
    request: Request,
    scheduled_start_at: str = Form(...),
    scheduled_end_at: str = Form(""),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)

    meeting = get_meeting_by_group_id(db, group_id)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này.")

    if not _can_manage_meeting_schedule(meeting, current_user.id):
        raise HTTPException(
            status_code=403,
            detail="Chỉ Chủ trì, Thư ký hoặc người thiết kế cuộc họp mới được điều chỉnh thời gian."
        )

    try:
        start_local = datetime.strptime((scheduled_start_at or "").strip(), "%Y-%m-%dT%H:%M")
        start_dt = start_local - timedelta(hours=7)
    except Exception:
        raise HTTPException(status_code=400, detail="Thời gian bắt đầu không hợp lệ.")

    end_dt = None
    if (scheduled_end_at or "").strip():
        try:
            end_local = datetime.strptime((scheduled_end_at or "").strip(), "%Y-%m-%dT%H:%M")
            end_dt = end_local - timedelta(hours=7)
        except Exception:
            raise HTTPException(status_code=400, detail="Thời gian kết thúc không hợp lệ.")

    if end_dt and end_dt < start_dt:
        raise HTTPException(status_code=400, detail="Thời gian kết thúc không được trước thời gian bắt đầu.")

    meeting.scheduled_start_at = start_dt
    meeting.scheduled_end_at = end_dt
    db.add(meeting)
    db.commit()
    db.refresh(meeting)

    payload = {
        "type": "meeting_schedule_updated",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "scheduled_start_value": _to_datetime_local_value(meeting.scheduled_start_at),
        "scheduled_end_value": _to_datetime_local_value(meeting.scheduled_end_at),
        "scheduled_start_text": _format_vn_dt_text(meeting.scheduled_start_at),
        "scheduled_end_text": _format_vn_dt_text(meeting.scheduled_end_at),
        "meeting_status": meeting.meeting_status,
        "meeting_status_label": _meeting_status_label(meeting.meeting_status),
    }

    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    await manager.notify_users_json(get_group_member_user_ids(db, group_id), {"module": "meeting", **payload})

    return JSONResponse({"ok": True, **payload})
    

@router.post("/meetings/{group_id}/end-now")
async def meeting_end_now(group_id: str, request: Request, db: Session = Depends(get_db)):
    current_user = login_required(request, db)

    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này.")

    meeting = _ensure_meeting_runtime_rules(db, get_meeting_by_group_id(db, group_id))
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if current_user.id not in {getattr(meeting, "host_user_id", None), getattr(meeting, "secretary_user_id", None)}:
        raise HTTPException(status_code=403, detail="Chỉ Chủ trì hoặc Thư ký mới được kết thúc cuộc họp.")

    current_attendance = _get_attendance_row_for_user(db, meeting.id, current_user.id)
    if not _attendance_is_checked_in(current_attendance):
        raise HTTPException(status_code=400, detail="Phải điểm danh trước khi kết thúc cuộc họp.")

    if (meeting.meeting_status or "").upper() == "ENDED":
        raise HTTPException(status_code=400, detail="Cuộc họp đã kết thúc.")

    meeting.scheduled_end_at = datetime.utcnow()
    meeting.meeting_status = "ENDED"
    db.add(meeting)
    db.commit()
    db.refresh(meeting)

    payload = {
        "type": "meeting_schedule_updated",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "meeting_status": meeting.meeting_status,
        "meeting_status_label": _meeting_status_label(meeting.meeting_status),
        "scheduled_start_value": _to_datetime_local_value(meeting.scheduled_start_at),
        "scheduled_end_value": _to_datetime_local_value(meeting.scheduled_end_at),
        "scheduled_start_text": _format_vn_dt_text(meeting.scheduled_start_at),
        "scheduled_end_text": _format_vn_dt_text(meeting.scheduled_end_at),
    }

    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    await manager.notify_users_json(get_group_member_user_ids(db, group_id), {"module": "meeting", **payload})
    return JSONResponse({"ok": True, **payload})
    
@router.post("/meetings/{group_id}/documents/upload")
async def meeting_upload_document(
    group_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)

    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này.")

    meeting = get_meeting_by_group_id(db, group_id)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if current_user.id not in {meeting.host_user_id, meeting.secretary_user_id}:
        raise HTTPException(status_code=403, detail="Chỉ Chủ trì hoặc Thư ký mới được tải tài liệu phục vụ cuộc họp.")

    form = await request.form()
    upload = form.get("file")
    if not upload or not getattr(upload, "filename", ""):
        raise HTTPException(status_code=400, detail="Chưa chọn file.")

    ext = os.path.splitext(upload.filename)[1].lower()
    stored_name = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S%f')}{ext}"
    rel_dir = os.path.join("chat_uploads", group_id, "meeting_docs")
    abs_dir = os.path.join(os.path.dirname(__file__), "..", "static", rel_dir)
    abs_dir = os.path.abspath(abs_dir)
    os.makedirs(abs_dir, exist_ok=True)

    abs_path = os.path.join(abs_dir, stored_name)
    content = await upload.read()

    with open(abs_path, "wb") as f:
        f.write(content)

    message = create_message(
        db,
        group_id=group_id,
        sender_user_id=current_user.id,
        content=upload.filename,
        message_type="MEETING_DOC",
        reply_to_message_id=None,
    )

    rel_url = "/" + os.path.join("static", rel_dir, stored_name).replace("\\", "/")

    attachment = save_message_attachment(
        db,
        message_id=message.id,
        filename=upload.filename,
        stored_name=stored_name,
        path=rel_url,
        mime_type=getattr(upload, "content_type", None),
        size_bytes=len(content),
    )

    message_payload = _build_message_vm(db, message, current_user.id)

    payload = {
        "type": "meeting_document_uploaded",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "document": {
            "id": attachment.id,
            "filename": attachment.filename,
            "path": attachment.path,
            "preview_url": _meeting_attachment_preview_url(attachment.id),
            "download_url": _meeting_attachment_download_url(attachment.id),
            "is_previewable": _is_browser_previewable(attachment.filename),
            "sender_name": get_display_name(current_user),
            "created_at_text": message_payload.get("created_at_text") or "",
        },
    }

    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    return JSONResponse({"ok": True, **payload})


@router.post("/meetings/{group_id}/messages/send")
async def meeting_send_message(
    group_id: str,
    request: Request,
    content: str = Form(...),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)

    meeting = get_meeting_by_group_id(db, group_id)
    meeting = _ensure_meeting_runtime_rules(db, meeting)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này hoặc đã bị loại khỏi cuộc họp do báo vắng.")
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    clean_content = (content or "").strip()
    if not clean_content:
        raise HTTPException(status_code=400, detail="Nội dung tin nhắn không được để trống.")

    attendance_row = _get_attendance_row_for_user(db, meeting.id, current_user.id)
    if not _can_user_send_meeting_message(db, meeting, current_user.id, attendance_row=attendance_row):
        raise HTTPException(
            status_code=403,
            detail="Bạn chỉ được gửi nội dung phát biểu sau khi được Chủ trì cho phép."
        )

    message = create_message(
        db,
        group_id=group_id,
        sender_user_id=current_user.id,
        content=clean_content,
        message_type="TEXT",
        reply_to_message_id=None,
    )

    if meeting.host_user_id != current_user.id:
        _consume_current_speaker_permission(db, meeting.id, current_user.id)

    message_payload = _build_message_vm(db, message, current_user.id)

    payload = {
        "type": "new_message",
        "message": message_payload,
        "can_send_meeting_message": _can_user_send_meeting_message(
            db,
            meeting,
            current_user.id,
            attendance_row=attendance_row,
        ),
    }

    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    return JSONResponse({"ok": True, **payload})


@router.post("/meetings/{group_id}/conclusion")
async def meeting_save_conclusion(
    group_id: str,
    request: Request,
    content: str = Form(""),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)
    meeting = get_meeting_by_group_id(db, group_id)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if meeting.host_user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Chỉ Chủ trì mới được cập nhật kết luận cuộc họp.")

    clean_content = (content or "").strip()
    if not clean_content:
        raise HTTPException(status_code=400, detail="Nội dung kết luận cuộc họp không được để trống.")

    latest_row = _get_latest_meeting_conclusion_message(db, group_id)
    if latest_row:
        latest_row.content = clean_content
        latest_row.updated_at = datetime.utcnow()
        db.add(latest_row)
        db.commit()
        db.refresh(latest_row)
        row = latest_row
    else:
        row = create_message(
            db,
            group_id=group_id,
            sender_user_id=current_user.id,
            content=clean_content,
            message_type="MEETING_CONCLUSION",
        )

    payload = {
        "type": "meeting_conclusion_updated",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "conclusion_text": row.content or "",
        "updated_at_text": _format_vn_dt_text(getattr(row, "updated_at", None) or getattr(row, "created_at", None)),
        "updated_by_user_id": current_user.id,
        "updated_by_name": get_display_name(current_user),
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    await manager.notify_users_json(get_group_member_user_ids(db, group_id), payload)
    return JSONResponse({"ok": True, **payload})
    
@router.get("/meetings/{group_id}/minutes.txt")
def export_meeting_minutes_txt(
    group_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)
    selected_group = get_group_by_id(db, group_id)
    if not selected_group or selected_group.group_type != "MEETING":
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không có quyền xem biên bản cuộc họp này.")

    selected_meeting = get_meeting_by_group_id(db, selected_group.id)
    selected_meeting = _ensure_meeting_runtime_rules(db, selected_meeting)
    if not selected_meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy dữ liệu cuộc họp.")

    if selected_meeting.secretary_user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Chỉ Thư ký cuộc họp mới được xuất biên bản.")

    all_messages_raw = get_group_messages(db, selected_group.id, limit=1000)
    all_messages = [_build_message_vm(db, row, current_user.id) for row in all_messages_raw]

    messages = []
    for msg in all_messages:
        message_type = (msg.get("message_type") or "").upper()
        if message_type in {"MEETING_DOC", "MEETING_CONCLUSION"}:
            continue
        messages.append(msg)

    members = get_group_members(db, selected_group.id)
    attendance_rows = get_meeting_attendance_rows(db, selected_meeting.id)
    attendance_map = {row.user_id: row for row in attendance_rows}
    speaker_requests = list_speaker_requests(db, selected_meeting.id)
    leave_requests = (
        db.query(ChatMeetingLeaveRequests)
        .filter(ChatMeetingLeaveRequests.meeting_id == selected_meeting.id)
        .order_by(ChatMeetingLeaveRequests.created_at.desc())
        .all()
    )

    for row in attendance_rows:
        row.attendance_status_label = _attendance_status_label(row.attendance_status)
        row.presence_status_label = "Đang ở phòng" if (row.presence_status or "").upper() == "ONLINE" else "Ngoài phòng"

    host = db.get(Users, selected_meeting.host_user_id) if selected_meeting.host_user_id else None
    secretary = db.get(Users, selected_meeting.secretary_user_id) if selected_meeting.secretary_user_id else None
    designed_by = db.get(Users, selected_meeting.designed_by_user_id) if selected_meeting.designed_by_user_id else None

    current_attendance = attendance_map.get(current_user.id)
    current_attendance_status = (getattr(current_attendance, "attendance_status", "") or "PENDING").upper()

    attendance_pending_count = 0
    attendance_absent_count = 0
    attendance_checked_in_count = 0
    for row in attendance_rows:
        status = (getattr(row, "attendance_status", "") or "PENDING").upper()
        if status == "ABSENT":
            attendance_absent_count += 1
        elif status == "CHECKED_IN":
            attendance_checked_in_count += 1
        else:
            attendance_pending_count += 1

    latest_conclusion_message = _get_latest_meeting_conclusion_message(db, selected_group.id)
    conclusion_text = (getattr(latest_conclusion_message, "content", "") or "").strip()
    conclusion_updated_text = ""
    if getattr(latest_conclusion_message, "created_at", None):
        conclusion_updated_text = _format_vn_dt_text(latest_conclusion_message.created_at)

    detail = {
        "group": selected_group,
        "meeting": selected_meeting,
        "messages": messages,
        "meeting_documents": _build_meeting_documents(all_messages),
        "members": members,
        "attendance_rows": attendance_rows,
        "speaker_requests": speaker_requests,
        "host": host,
        "secretary": secretary,
        "designed_by": designed_by,
        "current_attendance_status": current_attendance_status,
        "attendance_pending_count": attendance_pending_count,
        "attendance_absent_count": attendance_absent_count,
        "attendance_checked_in_count": attendance_checked_in_count,
        "scope_label": _meeting_scope_label(selected_meeting.meeting_scope),
        "member_ids": [member.user_id for member in members],
        "leave_requests": leave_requests,
        "conclusion_text": conclusion_text,
        "conclusion_updated_text": conclusion_updated_text,
    }

    text_content = _build_meeting_minutes_text(detail)
    file_name = f"bien_ban_hop_{group_id}.txt"
    headers = {
        "Content-Disposition": f'attachment; filename="{file_name}"'
    }
    return PlainTextResponse(text_content, headers=headers)    
    
@router.post("/meetings/{group_id}/presence/join")
async def meeting_presence_join(group_id: str, request: Request, db: Session = Depends(get_db)):
    current_user = login_required(request, db)

    meeting = get_meeting_by_group_id(db, group_id)
    meeting = _ensure_meeting_runtime_rules(db, meeting)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này hoặc đã bị loại khỏi cuộc họp do báo vắng.")
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    row = set_meeting_presence(db, meeting.id, current_user.id, True)
    payload = {
        "type": "meeting_presence_joined",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "user_id": current_user.id,
        "user_name": get_display_name(current_user),
        "meeting_status": meeting.meeting_status,
        "meeting_status_label": _meeting_status_label(meeting.meeting_status),
        "action_mode": "checkin" if meeting.meeting_status == "LIVE" else "absent",
        "attendance_status": getattr(row, "attendance_status", "PENDING"),
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    return JSONResponse({"ok": True, **payload})


@router.post("/meetings/{group_id}/presence/leave")
async def meeting_presence_leave(group_id: str, request: Request, db: Session = Depends(get_db)):
    current_user = login_required(request, db)
    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này.")
    meeting = get_meeting_by_group_id(db, group_id)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")
    set_meeting_presence(db, meeting.id, current_user.id, False)
    payload = {
        "type": "meeting_presence_left",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "user_id": current_user.id,
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    return JSONResponse({"ok": True, **payload})


@router.post("/meetings/{group_id}/sync")
async def meeting_sync(group_id: str, request: Request, db: Session = Depends(get_db)):
    current_user = login_required(request, db)

    meeting = _ensure_meeting_runtime_rules(db, get_meeting_by_group_id(db, group_id))
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if not is_group_member(db, group_id, current_user.id):
        payload = {
            "type": "meeting_status_sync",
            "group_id": group_id,
            "meeting_id": meeting.id,
            "meeting_status": meeting.meeting_status,
            "meeting_status_label": _meeting_status_label(meeting.meeting_status),
            "action_mode": "closed" if meeting.meeting_status == "ENDED" else ("checkin" if meeting.meeting_status == "LIVE" else "absent"),
            "removed_from_meeting": True,
            "redirect_url": "/meetings",
            "detail": "Anh/chị đã báo vắng và đã bị đưa ra khỏi cuộc họp khi đến giờ họp.",
        }
        return JSONResponse({"ok": True, **payload})

    current_attendance = _get_attendance_row_for_user(db, meeting.id, current_user.id)
    current_attendance_status = (getattr(current_attendance, "attendance_status", "") or "PENDING").upper()

    if meeting.meeting_status == "LIVE":
        action_mode = "checkin"
    elif meeting.meeting_status == "UPCOMING":
        action_mode = "absent"
    else:
        action_mode = "closed"

    can_send_meeting_message = _can_user_send_meeting_message(
        db,
        meeting,
        current_user.id,
        attendance_row=current_attendance,
    )

    payload = {
        "type": "meeting_status_sync",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "meeting_status": meeting.meeting_status,
        "meeting_status_label": _meeting_status_label(meeting.meeting_status),
        "action_mode": action_mode,
        "current_attendance_status": current_attendance_status,
        "can_register_speaker": meeting.meeting_status == "LIVE" and current_attendance_status == "CHECKED_IN" and not can_send_meeting_message,
        "can_send_meeting_message": can_send_meeting_message,
}
    return JSONResponse({"ok": True, **payload})


@router.post("/meetings/{group_id}/absent")
async def meeting_absent(group_id: str, request: Request, reason: str = Form(""), db: Session = Depends(get_db)):
    current_user = login_required(request, db)
    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này.")

    meeting = transition_meeting_status_if_needed(db, get_meeting_by_group_id(db, group_id))
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")
    if meeting.meeting_status != "UPCOMING":
        raise HTTPException(status_code=400, detail="Chỉ được báo vắng trước giờ họp.")

    row = mark_meeting_absent(db, meeting.id, current_user.id, reason=reason)
    set_meeting_presence(db, meeting.id, current_user.id, False)

    meeting = _ensure_meeting_runtime_rules(db, meeting) or meeting
    removed_from_meeting = not is_group_member(db, group_id, current_user.id)

    host_user = db.get(Users, getattr(meeting, "host_user_id", None)) if getattr(meeting, "host_user_id", None) else None

    payload = {
        "type": "meeting_absent_reported",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "user_id": current_user.id,
        "user_name": get_display_name(current_user),
        "attendance_status": getattr(row, "attendance_status", "ABSENT"),
        "attendance_status_label": _attendance_status_label(getattr(row, "attendance_status", "ABSENT")),
        "reason": (reason or "").strip(),
        "removed_from_meeting": removed_from_meeting,
        "host_user_id": getattr(meeting, "host_user_id", None),
        "host_user_name": get_display_name(host_user) if host_user else "",
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    await manager.notify_users_json(get_group_member_user_ids(db, group_id), {"module": "meeting", **payload})
    return JSONResponse({"ok": True, **payload})

@router.post("/meetings/{group_id}/absent/cancel")
async def meeting_cancel_absent(group_id: str, request: Request, db: Session = Depends(get_db)):
    current_user = login_required(request, db)
    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này.")

    meeting = transition_meeting_status_if_needed(db, get_meeting_by_group_id(db, group_id))
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")
    if meeting.meeting_status != "UPCOMING":
        raise HTTPException(status_code=400, detail="Chỉ được hủy báo vắng trước giờ họp.")

    row = _get_attendance_row_for_user(db, meeting.id, current_user.id)
    if not row:
        raise HTTPException(status_code=404, detail="Không tìm thấy trạng thái tham dự của bạn.")

    row.attendance_status = "PENDING"
    row.absent_reason = None
    db.add(row)
    db.commit()
    db.refresh(row)

    payload = {
        "type": "meeting_absent_cancelled",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "user_id": current_user.id,
        "user_name": get_display_name(current_user),
        "attendance_status": getattr(row, "attendance_status", "PENDING"),
        "attendance_status_label": _attendance_status_label(getattr(row, "attendance_status", "PENDING")),
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    return JSONResponse({"ok": True, **payload})
    
    
@router.post("/meetings/{group_id}/checkin")
async def meeting_checkin(group_id: str, request: Request, db: Session = Depends(get_db)):
    current_user = login_required(request, db)

    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này.")

    meeting = transition_meeting_status_if_needed(db, get_meeting_by_group_id(db, group_id))
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if (meeting.meeting_status or "").upper() != "LIVE":
        raise HTTPException(status_code=400, detail="Chỉ được điểm danh khi cuộc họp đang diễn ra.")

    row = mark_meeting_checkin(db, meeting.id, current_user.id)
    set_meeting_presence(db, meeting.id, current_user.id, True)

    payload = {
        "type": "meeting_checkin_done",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "user_id": current_user.id,
        "user_name": get_display_name(current_user),
        "attendance_status": getattr(row, "attendance_status", "CHECKED_IN"),
        "attendance_status_label": _attendance_status_label(getattr(row, "attendance_status", "CHECKED_IN")),
        "presence_status": "ONLINE",
        "presence_status_label": "Đang ở phòng",
        "can_send_meeting_message": _can_user_send_meeting_message(db, meeting, current_user.id, attendance_row=row),
        "can_register_speaker": (
            (meeting.meeting_status or "").upper() == "LIVE"
            and (getattr(row, "attendance_status", "") or "").upper() == "CHECKED_IN"
            and not _can_user_send_meeting_message(db, meeting, current_user.id, attendance_row=row)
        ),
    }

    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    await manager.notify_users_json(get_group_member_user_ids(db, group_id), {"module": "meeting", **payload})
    return JSONResponse({"ok": True, **payload})


@router.post("/meetings/{group_id}/leave/request")
async def meeting_request_leave(
    group_id: str,
    request: Request,
    note: str = Form(""),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)

    meeting = get_meeting_by_group_id(db, group_id)
    meeting = _ensure_meeting_runtime_rules(db, meeting)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này.")

    if (meeting.meeting_status or "").upper() != "LIVE":
        raise HTTPException(status_code=400, detail="Chỉ được xin rời khi cuộc họp đang diễn ra.")

    if str(current_user.id) == str(getattr(meeting, "host_user_id", None) or ""):
        raise HTTPException(status_code=400, detail="Chủ trì không sử dụng chức năng xin rời họp này.")

    attendance_row = _get_attendance_row_for_user(db, meeting.id, current_user.id)
    attendance_status = (getattr(attendance_row, "attendance_status", "") or "PENDING").upper()
    if attendance_status in {"ABSENT", "LEFT"}:
        raise HTTPException(status_code=400, detail="Bạn không còn ở trạng thái dự họp hợp lệ.")

    existing = (
        db.query(ChatMeetingLeaveRequests)
        .filter(ChatMeetingLeaveRequests.meeting_id == meeting.id)
        .filter(ChatMeetingLeaveRequests.user_id == current_user.id)
        .filter(ChatMeetingLeaveRequests.request_status == "PENDING")
        .first()
    )
    if existing:
        raise HTTPException(status_code=400, detail="Bạn đã gửi yêu cầu xin rời họp và đang chờ Chủ trì duyệt.")

    row = ChatMeetingLeaveRequests(
        meeting_id=meeting.id,
        user_id=current_user.id,
        request_status="PENDING",
        note=(note or "").strip() or None,
    )
    db.add(row)
    db.commit()
    db.refresh(row)

    payload = {
        "type": "meeting_leave_requested",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "leave_request_id": row.id,
        "user_id": current_user.id,
        "user_name": get_display_name(current_user),
        "note": row.note or "",
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    return JSONResponse({"ok": True, **payload})


@router.post("/meetings/{group_id}/leave/{leave_request_id}/approve")
async def meeting_approve_leave(
    group_id: str,
    leave_request_id: str,
    request: Request,
    response_note: str = Form(""),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)

    meeting = get_meeting_by_group_id(db, group_id)
    meeting = _ensure_meeting_runtime_rules(db, meeting)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if str(current_user.id) != str(getattr(meeting, "host_user_id", None) or ""):
        raise HTTPException(status_code=403, detail="Chỉ Chủ trì mới được chấp thuận cho rời họp.")

    row = db.get(ChatMeetingLeaveRequests, leave_request_id)
    if not row or str(row.meeting_id) != str(meeting.id):
        raise HTTPException(status_code=404, detail="Không tìm thấy yêu cầu xin rời họp.")

    if (row.request_status or "").upper() != "PENDING":
        raise HTTPException(status_code=400, detail="Yêu cầu này không còn ở trạng thái chờ duyệt.")

    attendance_row = _get_attendance_row_for_user(db, meeting.id, row.user_id)
    if attendance_row:
        attendance_row.attendance_status = "LEFT"
        attendance_row.presence_status = "OFFLINE"
        attendance_row.updated_at = datetime.utcnow()
        db.add(attendance_row)

    row.request_status = "APPROVED"
    row.response_note = (response_note or "").strip() or None
    row.decided_by_user_id = current_user.id
    row.decided_at = datetime.utcnow()
    row.updated_at = datetime.utcnow()
    db.add(row)
    db.commit()

    remove_member_from_group(db, group_id=group_id, user_id=row.user_id)
    host_user = db.get(Users, getattr(meeting, "host_user_id", None)) if getattr(meeting, "host_user_id", None) else None

    payload = {
        "type": "meeting_leave_approved",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "leave_request_id": row.id,
        "user_id": row.user_id,
        "response_note": row.response_note or "",
        "host_user_id": getattr(meeting, "host_user_id", None),
        "host_user_name": get_display_name(host_user) if host_user else "",
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    await manager.notify_users_json(get_group_member_user_ids(db, group_id), {"module": "meeting", **payload})
    return _json_or_redirect(request, f"/meetings?selected_id={group_id}", payload)
    

@router.post("/meetings/{group_id}/leave/{leave_request_id}/reject")
async def meeting_reject_leave(
    group_id: str,
    leave_request_id: str,
    request: Request,
    response_note: str = Form(""),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)

    meeting = get_meeting_by_group_id(db, group_id)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if str(current_user.id) != str(getattr(meeting, "host_user_id", None) or ""):
        raise HTTPException(status_code=403, detail="Chỉ Chủ trì mới được từ chối yêu cầu rời họp.")

    row = db.get(ChatMeetingLeaveRequests, leave_request_id)
    if not row or str(row.meeting_id) != str(meeting.id):
        raise HTTPException(status_code=404, detail="Không tìm thấy yêu cầu xin rời họp.")

    if (row.request_status or "").upper() != "PENDING":
        raise HTTPException(status_code=400, detail="Yêu cầu này không còn ở trạng thái chờ duyệt.")

    row.request_status = "REJECTED"
    row.response_note = (response_note or "").strip() or None
    row.decided_by_user_id = current_user.id
    row.decided_at = datetime.utcnow()
    row.updated_at = datetime.utcnow()
    db.add(row)
    db.commit()

    payload = {
        "type": "meeting_leave_rejected",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "leave_request_id": row.id,
        "user_id": row.user_id,
        "response_note": row.response_note or "",
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    await manager.notify_users_json(get_group_member_user_ids(db, group_id), {"module": "meeting", **payload})
    return _json_or_redirect(request, f"/meetings?selected_id={group_id}", payload)

    
@router.post("/meetings/{group_id}/speaker/request")
async def meeting_speaker_request(group_id: str, request: Request, note: str = Form(""), db: Session = Depends(get_db)):
    current_user = login_required(request, db)
    meeting = get_meeting_by_group_id(db, group_id)
    meeting = _ensure_meeting_runtime_rules(db, meeting)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc cuộc họp này hoặc đã bị loại khỏi cuộc họp do báo vắng.")
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")
    if meeting.meeting_status != "LIVE":
        raise HTTPException(status_code=400, detail="Chỉ được đăng ký phát biểu khi cuộc họp đang diễn ra.")

    attendance_row = _get_attendance_row_for_user(db, meeting.id, current_user.id)
    attendance_status = (getattr(attendance_row, "attendance_status", "") or "PENDING").upper()
    if attendance_status == "ABSENT":
        raise HTTPException(status_code=400, detail="Bạn đã báo vắng nên không được đăng ký phát biểu.")

    row = create_speaker_request(db, meeting.id, current_user.id, note=note)
    payload = {
        "type": "meeting_speaker_requested",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "speaker_request_id": row.id,
        "user_id": current_user.id,
        "user_name": get_display_name(current_user),
        "note": (note or "").strip(),
        "queue_no": getattr(row, "queue_no", None),
        "request_status": getattr(row, "request_status", "PENDING"),
        "request_status_label": getattr(row, "request_status_label", "Chờ Chủ trì cho phép"),
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    notify_ids = get_group_member_user_ids(db, group_id)
    await manager.notify_users_json(notify_ids, {"module": "meeting", **payload})
    return JSONResponse({"ok": True, **payload})


@router.post("/meetings/{group_id}/speaker/{speaker_request_id}/approve")
async def meeting_speaker_approve(group_id: str, speaker_request_id: str, request: Request, db: Session = Depends(get_db)):
    current_user = login_required(request, db)
    meeting = get_meeting_by_group_id(db, group_id)
    if not meeting or meeting.host_user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Chỉ Chủ trì mới được cho phép phát biểu.")

    row = approve_speaker_request(db, speaker_request_id)
    if not row or row.meeting_id != meeting.id:
        raise HTTPException(status_code=404, detail="Không tìm thấy đăng ký phát biểu.")

    payload = {
        "type": "meeting_speaker_approved",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "speaker_request_id": row.id,
        "user_id": row.user_id,
        "can_send_meeting_message": True,
        "can_register_speaker": False,
        "request_status": getattr(row, "request_status", "APPROVED"),
        "request_status_label": getattr(row, "request_status_label", "Đã được Chủ trì cho phép phát biểu"),
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    await manager.notify_users_json(get_group_member_user_ids(db, group_id), {"module": "meeting", **payload})
    return JSONResponse({"ok": True, **payload})


@router.post("/meetings/{group_id}/speaker/{speaker_request_id}/move")
async def meeting_speaker_move(
    group_id: str,
    speaker_request_id: str,
    request: Request,
    direction: str = Form(...),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)
    meeting = get_meeting_by_group_id(db, group_id)
    if not meeting or meeting.host_user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Chỉ Chủ trì mới được sắp xếp thứ tự phát biểu.")
    row = move_speaker_request(db, speaker_request_id, (direction or "").strip().lower())
    if not row or row.meeting_id != meeting.id:
        raise HTTPException(status_code=404, detail="Không tìm thấy đăng ký phát biểu.")
    payload = {
        "type": "meeting_speaker_reordered",
        "group_id": group_id,
        "meeting_id": meeting.id,
        "speaker_request_id": row.id,
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    return JSONResponse({"ok": True, **payload})



@router.get("/meetings/attachments/{attachment_id}/preview")
def meeting_attachment_preview(
    attachment_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    _ensure_meeting_tables()
    current_user = login_required(request, db)

    attachment, _message = _ensure_meeting_attachment_access(db, attachment_id, current_user.id)

    filename = getattr(attachment, "filename", None) or getattr(attachment, "stored_name", None) or "tep_dinh_kem"
    file_path = _static_url_to_abs_path(getattr(attachment, "path", "") or "")

    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp trên máy chủ.")

    if is_office_previewable(filename):
        try:
            preview_path = ensure_office_pdf_preview(
                source_path=file_path,
                preview_key=f"meeting_{attachment_id}",
                original_name=filename,
            )
        except OfficePreviewError as ex:
            raise HTTPException(
                status_code=500,
                detail=(
                    "Không tạo được bản xem trước PDF. "
                    "Vui lòng kiểm tra LibreOffice/soffice trên máy chủ. "
                    f"Chi tiết: {str(ex)}"
                ),
            )

        preview_filename = f"{os.path.splitext(filename)[0]}.pdf"
        return FileResponse(
            preview_path,
            media_type="application/pdf",
            headers={"Content-Disposition": _build_content_disposition("inline", preview_filename)},
        )

    media_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
    return FileResponse(
        file_path,
        media_type=media_type,
        headers={"Content-Disposition": _build_content_disposition("inline", filename)},
    )


@router.get("/meetings/attachments/{attachment_id}/download")
def meeting_attachment_download(
    attachment_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    _ensure_meeting_tables()
    current_user = login_required(request, db)

    attachment, _message = _ensure_meeting_attachment_access(db, attachment_id, current_user.id)

    filename = getattr(attachment, "filename", None) or getattr(attachment, "stored_name", None) or "tep_dinh_kem"
    file_path = _static_url_to_abs_path(getattr(attachment, "path", "") or "")

    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp trên máy chủ.")

    media_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

    return FileResponse(
        file_path,
        media_type=media_type,
        headers={"Content-Disposition": _build_content_disposition("attachment", filename)},
    )

@router.post("/meetings/{group_id}/host")
async def meeting_assign_host(
    group_id: str,
    request: Request,
    host_user_id: str = Form(""),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)
    meeting = get_meeting_by_group_id(db, group_id)
    if not meeting:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    if not _can_assign_meeting_host(meeting, current_user.id):
        raise HTTPException(status_code=403, detail="Chỉ Chủ trì hoặc Người thiết kế mới được trao quyền Chủ trì.")

    clean_host_user_id = (host_user_id or "").strip()
    if not clean_host_user_id:
        raise HTTPException(status_code=400, detail="Chưa chọn người nhận quyền Chủ trì.")

    if not is_group_member(db, group_id, clean_host_user_id):
        raise HTTPException(status_code=400, detail="Người được chọn không còn thuộc cuộc họp.")

    attendance_row = _get_attendance_row_for_user(db, meeting.id, clean_host_user_id)
    attendance_status = (getattr(attendance_row, "attendance_status", "") or "PENDING").upper()
    if attendance_status == "ABSENT":
        raise HTTPException(status_code=400, detail="Không thể trao quyền Chủ trì cho người đã báo vắng.")

    updated = assign_meeting_host(db, meeting.id, clean_host_user_id)
    if not updated:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    host_user = db.get(Users, clean_host_user_id)
    payload = {
        "type": "meeting_host_updated",
        "group_id": group_id,
        "meeting_id": updated.id,
        "host_user_id": clean_host_user_id,
        "host_user_name": get_display_name(host_user) if host_user else "",
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    await manager.notify_users_json(get_group_member_user_ids(db, group_id), {"module": "meeting", **payload})
    return _json_or_redirect(request, f"/meetings?selected_id={group_id}", payload)
    
    
@router.post("/meetings/{group_id}/secretary")
async def meeting_assign_secretary(
    group_id: str,
    request: Request,
    secretary_user_id: str = Form(""),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)
    meeting = get_meeting_by_group_id(db, group_id)
    if not meeting or meeting.host_user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Chỉ Chủ trì mới được chỉ định thư ký.")

    updated = assign_meeting_secretary(db, meeting.id, secretary_user_id or None)
    if not updated:
        raise HTTPException(status_code=404, detail="Không tìm thấy cuộc họp.")

    secretary_user = db.get(Users, secretary_user_id) if (secretary_user_id or "").strip() else None
    payload = {
        "type": "meeting_secretary_updated",
        "group_id": group_id,
        "meeting_id": updated.id,
        "secretary_user_id": (secretary_user_id or "").strip() or None,
        "secretary_user_name": get_display_name(secretary_user) if secretary_user else "Chưa chỉ định",
    }
    await manager.broadcast_group_text(group_id, json.dumps(payload, ensure_ascii=False))
    await manager.notify_users_json(get_group_member_user_ids(db, group_id), {"module": "meeting", **payload})
    return _json_or_redirect(request, f"/meetings?selected_id={group_id}", payload)
