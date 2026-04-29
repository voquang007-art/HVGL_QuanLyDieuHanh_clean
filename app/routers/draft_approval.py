from __future__ import annotations

import os
import shutil
import uuid
import logging
import mimetypes
from datetime import datetime
from pathlib import Path
from urllib.parse import quote
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from ..chat.realtime import manager
from ..config import settings
from ..database import Base, engine, get_db
from ..models import (
    CommitteeMembers,
    Committees,
    DocumentDraftActions,
    DocumentDraftFiles,
    DocumentDrafts,
    Files,
    RoleCode,
    Roles,
    UnitStatus,
    Units,
    UserRoles,
    UserStatus,
    UserUnitMemberships,
    Users,
)
from ..security.deps import login_required
from ..work_access import APPROVER_ROLES, BOARD_ROLES, DRAFT_APPROVAL_HIDDEN_ROLES
from ..office_preview import (
    OfficePreviewError,
    ensure_office_pdf_preview,
    is_office_previewable,
)

logger = logging.getLogger("app.draft_approval")

router = APIRouter()
templates = Jinja2Templates(directory="app/templates")

_ALLOWED_EXTENSIONS = {
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".jpg", ".jpeg", ".png", ".webp", ".txt"
}
_INLINE_MIME_PREFIXES = ("image/", "text/")
_INLINE_MIME_EXACT = {
    "application/pdf",
}
_COORDINATION_ACTION = "COORDINATE"

DRAFT_SCOPE_UNIT = "UNIT"
DRAFT_SCOPE_COMMITTEE = "COMMITTEE"

COMMITTEE_ROLE_TRUONG_BAN = "TRUONG_BAN"
COMMITTEE_ROLE_PHO_TRUONG_BAN = "PHO_TRUONG_BAN"
COMMITTEE_ROLE_THANH_VIEN = "THANH_VIEN"

BGD_ROLE_CODES = {"ROLE_GIAM_DOC", "ROLE_PHO_GIAM_DOC_TRUC", "ROLE_PHO_GIAM_DOC"}
HDTV_ROLE_CODES = {
    "ROLE_LANH_DAO",
    "ROLE_TONG_GIAM_DOC",
    "ROLE_PHO_TONG_GIAM_DOC_THUONG_TRUC",
    "ROLE_PHO_TONG_GIAM_DOC",
}

_STATUS_LABELS = {
    "DRAFT": "Nháp",
    "RETURNED_FOR_EDIT": "Trả lại để chỉnh sửa",
    "SUBMITTED_TO_TO_MANAGER": "Chờ QL tổ xử lý",
    "SUBMITTED_TO_DEPT_MANAGER": "Chờ QL phòng xử lý",
    "SUBMITTED_TO_HDTV": "Chờ HĐTV phê duyệt",
    "SUBMITTED_TO_COMMITTEE_DEPUTY": "Chờ Phó ban xử lý",
    "SUBMITTED_TO_COMMITTEE_HEAD": "Chờ Trưởng ban xử lý",
    "SUBMITTED_TO_BGD": "Chờ BGĐ phê duyệt",
    "IN_COORDINATION": "Đang phối hợp",
    "FINISHED": "Đã kết thúc",
}

_ACTION_LABELS = {
    "CREATE": "Tạo hồ sơ",
    "SUBMIT": "Trình dự thảo",
    "UPLOAD_REPLACEMENT": "Cập nhật tài liệu dự thảo",
    "COORDINATE": "Gửi phối hợp",
    "COORDINATE_REPLY": "Phản hồi phối hợp",
    "APPROVE_FORWARD": "Đồng ý và trình cấp trên",
    "RETURN_FOR_EDIT": "Trả lại để tự sửa",
    "RETURN_WITH_EDITED_FILE": "Trả lại kèm file đã sửa",
    "COMMITTEE_COMPLETED": "Ban kiêm nhiệm hoàn thành xử lý",
    "BGD_APPROVED": "BGĐ phê duyệt nội dung",
    "HDTV_APPROVED": "HĐTV phê duyệt nội dung",
    "FINISHED": "Kết thúc hồ sơ",
}

_FILE_ROLE_LABELS = {
    "DRAFT_UPLOAD": "Tài liệu dự thảo",
    "RETURNED_EDITED_FILE": "Tài liệu trả lại đã sửa",
}

def _ensure_tables() -> None:
    Base.metadata.create_all(
        bind=engine,
        tables=[
            DocumentDrafts.__table__,
            DocumentDraftFiles.__table__,
            DocumentDraftActions.__table__,
        ],
        checkfirst=True,
    )


def _now() -> datetime:
    return datetime.utcnow()


def _project_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def _upload_root() -> str:
    path = os.path.join(_project_root(), "data", "draft_approvals")
    os.makedirs(path, exist_ok=True)
    return path


def _normalize_role_code(value: object) -> str:
    if value is None:
        return ""
    raw = getattr(value, "value", value)
    return str(raw).strip().upper()


def _load_role_codes_for_user(db: Session, user_id: str) -> Set[str]:
    rows = (
        db.query(Roles.code)
        .join(UserRoles, UserRoles.role_id == Roles.id)
        .filter(UserRoles.user_id == user_id)
        .all()
    )
    return {_normalize_role_code(code) for (code,) in rows}


def _is_admin(role_codes: Set[str]) -> bool:
    return "ROLE_ADMIN" in role_codes


def _is_board(role_codes: Set[str]) -> bool:
    return bool(BOARD_ROLES & role_codes) or _is_admin(role_codes)


def _is_room_manager(role_codes: Set[str]) -> bool:
    return bool({"ROLE_TRUONG_PHONG", "ROLE_PHO_PHONG"} & role_codes)


def _is_team_manager(role_codes: Set[str]) -> bool:
    return bool({"ROLE_TO_TRUONG", "ROLE_PHO_TO"} & role_codes)
    
def _is_truong_phong(role_codes: Set[str]) -> bool:
    return "ROLE_TRUONG_PHONG" in role_codes


def _is_pho_phong(role_codes: Set[str]) -> bool:
    return "ROLE_PHO_PHONG" in role_codes


def _is_to_truong(role_codes: Set[str]) -> bool:
    return "ROLE_TO_TRUONG" in role_codes


def _is_pho_to(role_codes: Set[str]) -> bool:
    return "ROLE_PHO_TO" in role_codes   


def _is_employee(role_codes: Set[str]) -> bool:
    return "ROLE_NHAN_VIEN" in role_codes


_SPECIALIST_SUBUNIT_MANAGER_ROLES: Set[str] = {
    "ROLE_TRUONG_NHOM",
    "ROLE_PHO_NHOM",
    "ROLE_TRUONG_DON_VI",
    "ROLE_PHO_DON_VI",
    "ROLE_DIEU_DUONG_TRUONG_DON_VI",
    "ROLE_KY_THUAT_VIEN_TRUONG_DON_VI",
}

_SPECIALIST_DEPT_MANAGER_ROLES: Set[str] = {
    "ROLE_TRUONG_KHOA",
    "ROLE_PHO_TRUONG_KHOA",
    "ROLE_DIEU_DUONG_TRUONG",
    "ROLE_KY_THUAT_VIEN_TRUONG",
}

_SPECIALIST_STAFF_ROLES: Set[str] = {
    "ROLE_NHAN_VIEN",
    "ROLE_DIEU_DUONG",
    "ROLE_KY_THUAT_VIEN",
    "ROLE_DUOC_SI",
    "ROLE_HO_LY",
    "ROLE_BAC_SI",
    "ROLE_THU_KY_Y_KHOA",
    "ROLE_QL_CHAT_LUONG",
    "ROLE_QL_KY_THUAT",
    "ROLE_QL_AN_TOAN",
    "ROLE_QL_VAT_TU",
    "ROLE_QL_TRANG_THIET_BI",
    "ROLE_QL_MOI_TRUONG",
    "ROLE_QL_CNTT",
}


def _unit_block_code(unit: Optional[Units]) -> str:
    if not unit:
        return ""
    return _normalize_role_code(getattr(unit, "block_code", None))


def _unit_category_code(unit: Optional[Units]) -> str:
    if not unit:
        return ""
    return _normalize_role_code(getattr(unit, "unit_category", None))


def _is_hanh_chinh_unit(unit: Optional[Units]) -> bool:
    return _unit_block_code(unit) == "HANH_CHINH"


def _is_chuyen_mon_unit(unit: Optional[Units]) -> bool:
    return _unit_block_code(unit) == "CHUYEN_MON"


def _is_specialist_subunit_manager(role_codes: Set[str]) -> bool:
    return bool(_SPECIALIST_SUBUNIT_MANAGER_ROLES & role_codes)


def _is_specialist_department_manager(role_codes: Set[str]) -> bool:
    return bool(_SPECIALIST_DEPT_MANAGER_ROLES & role_codes)


def _is_specialist_staff(role_codes: Set[str]) -> bool:
    return bool(_SPECIALIST_STAFF_ROLES & role_codes) and not _is_specialist_subunit_manager(role_codes) and not _is_specialist_department_manager(role_codes)


def _is_specialist_approver(role_codes: Set[str]) -> bool:
    return _is_specialist_subunit_manager(role_codes) or _is_specialist_department_manager(role_codes)


def _can_access_draft_module(role_codes: Set[str]) -> bool:
    if _is_admin(role_codes) or _is_board(role_codes):
        return True
    return not bool(DRAFT_APPROVAL_HIDDEN_ROLES & role_codes)


def _role_label_from_codes(role_codes: Set[str]) -> str:
    if _is_board(role_codes):
        return "HĐTV/BGĐ"
    if _is_truong_phong(role_codes):
        return "Trưởng phòng"
    if _is_pho_phong(role_codes):
        return "Phó phòng"
    if _is_to_truong(role_codes):
        return "Tổ trưởng"
    if _is_pho_to(role_codes):
        return "Tổ phó"
    if "ROLE_TRUONG_KHOA" in role_codes:
        return "Trưởng khoa"
    if "ROLE_PHO_TRUONG_KHOA" in role_codes:
        return "Phó khoa"
    if "ROLE_DIEU_DUONG_TRUONG" in role_codes:
        return "Điều dưỡng trưởng"
    if "ROLE_KY_THUAT_VIEN_TRUONG" in role_codes:
        return "KTV trưởng"
    if "ROLE_TRUONG_DON_VI" in role_codes:
        return "Trưởng đơn vị"
    if "ROLE_PHO_DON_VI" in role_codes:
        return "Phó đơn vị"
    if "ROLE_DIEU_DUONG_TRUONG_DON_VI" in role_codes:
        return "Điều dưỡng trưởng đơn vị"
    if "ROLE_KY_THUAT_VIEN_TRUONG_DON_VI" in role_codes:
        return "KTV trưởng đơn vị"
    if "ROLE_TRUONG_NHOM" in role_codes:
        return "Trưởng nhóm"
    if "ROLE_PHO_NHOM" in role_codes:
        return "Phó nhóm"
    if "ROLE_DIEU_DUONG" in role_codes:
        return "Điều dưỡng"
    if "ROLE_KY_THUAT_VIEN" in role_codes:
        return "Kỹ thuật viên"
    if "ROLE_HO_LY" in role_codes:
        return "Hộ lý"
    if "ROLE_BAC_SI" in role_codes:
        return "Bác sĩ"
    if "ROLE_THU_KY_Y_KHOA" in role_codes:
        return "Thư ký y khoa"
    return "Nhân viên"

def _normalize_draft_scope(value: Optional[str]) -> str:
    scope = (value or "").strip().upper()
    if scope == DRAFT_SCOPE_COMMITTEE:
        return DRAFT_SCOPE_COMMITTEE
    return DRAFT_SCOPE_UNIT


def _draft_scope_of(draft: Optional[DocumentDrafts]) -> str:
    if not draft:
        return DRAFT_SCOPE_UNIT
    return _normalize_draft_scope(getattr(draft, "scope_type", None))


def _is_committee_draft(draft: Optional[DocumentDrafts]) -> bool:
    return _draft_scope_of(draft) == DRAFT_SCOPE_COMMITTEE


def _is_bgd_role(role_codes: Set[str]) -> bool:
    return bool(BGD_ROLE_CODES & role_codes)


def _is_hdtv_role(role_codes: Set[str]) -> bool:
    return bool(HDTV_ROLE_CODES & role_codes) or _is_admin(role_codes)


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


def _is_committee_member(db: Session, committee_id: str, user_id: str) -> bool:
    return bool(_committee_member_role(db, committee_id, user_id))


def _committee_user_options_by_role(db: Session, committee_id: str, committee_role: str) -> List[Users]:
    rows = (
        db.query(Users)
        .join(CommitteeMembers, CommitteeMembers.user_id == Users.id)
        .filter(CommitteeMembers.committee_id == committee_id)
        .filter(CommitteeMembers.is_active == True)  # noqa: E712
        .filter(CommitteeMembers.committee_role == committee_role)
        .filter(Users.status == UserStatus.ACTIVE)
        .order_by(Users.full_name.asc(), Users.username.asc())
        .all()
    )
    return rows


def _committee_options_for_draft_create(db: Session, user: Users) -> List[Committees]:
    rows = (
        db.query(Committees)
        .join(CommitteeMembers, CommitteeMembers.committee_id == Committees.id)
        .filter(CommitteeMembers.user_id == user.id)
        .filter(CommitteeMembers.is_active == True)  # noqa: E712
        .filter(Committees.status == "ACTIVE", Committees.is_active == True)  # noqa: E712
        .filter(Committees.allow_draft_approval == True)  # noqa: E712
        .order_by(Committees.name.asc())
        .all()
    )
    unique: Dict[str, Committees] = {}
    for row in rows:
        unique[str(row.id)] = row
    return list(unique.values())


def _find_bgd_users(db: Session) -> List[Users]:
    rows = (
        db.query(Users)
        .join(UserRoles, UserRoles.user_id == Users.id)
        .join(Roles, Roles.id == UserRoles.role_id)
        .filter(
            Users.status == UserStatus.ACTIVE,
            Roles.code.in_([
                RoleCode.ROLE_GIAM_DOC,
                RoleCode.ROLE_PHO_GIAM_DOC_TRUC,
                RoleCode.ROLE_PHO_GIAM_DOC,
            ]),
        )
        .order_by(Users.full_name.asc(), Users.username.asc())
        .all()
    )
    unique: Dict[str, Users] = {}
    for row in rows:
        unique[str(row.id)] = row
    return list(unique.values())


def _find_hdtv_users(db: Session) -> List[Users]:
    rows = (
        db.query(Users)
        .join(UserRoles, UserRoles.user_id == Users.id)
        .join(Roles, Roles.id == UserRoles.role_id)
        .filter(
            Users.status == UserStatus.ACTIVE,
            Roles.code.in_([
                RoleCode.ROLE_LANH_DAO,
                RoleCode.ROLE_TONG_GIAM_DOC,
                RoleCode.ROLE_PHO_TONG_GIAM_DOC_THUONG_TRUC,
                RoleCode.ROLE_PHO_TONG_GIAM_DOC,
            ]),
        )
        .order_by(Users.full_name.asc(), Users.username.asc())
        .all()
    )
    unique: Dict[str, Users] = {}
    for row in rows:
        unique[str(row.id)] = row
    return list(unique.values())


def _build_committee_submit_candidates(
    db: Session,
    draft: DocumentDrafts,
    user: Users,
    role_codes: Set[str],
) -> List[Dict[str, Any]]:
    committee_id = str(getattr(draft, "committee_id", "") or "")
    if not committee_id:
        return []

    current_role = _committee_member_role(db, committee_id, user.id)
    candidates_raw: List[Tuple[Users, Optional[Units], str]] = []

    # Nhân viên Ban → ưu tiên Phó ban, nếu không có thì Trưởng ban.
    if current_role == COMMITTEE_ROLE_THANH_VIEN:
        deputies = _committee_user_options_by_role(db, committee_id, COMMITTEE_ROLE_PHO_TRUONG_BAN)
        heads = _committee_user_options_by_role(db, committee_id, COMMITTEE_ROLE_TRUONG_BAN)

        for item in deputies:
            if item.id != user.id:
                candidates_raw.append((item, None, "SUBMITTED_TO_COMMITTEE_DEPUTY"))

        if not candidates_raw:
            for item in heads:
                if item.id != user.id:
                    candidates_raw.append((item, None, "SUBMITTED_TO_COMMITTEE_HEAD"))

    # Phó ban → Trưởng ban hoặc BGĐ.
    elif current_role == COMMITTEE_ROLE_PHO_TRUONG_BAN:
        heads = _committee_user_options_by_role(db, committee_id, COMMITTEE_ROLE_TRUONG_BAN)
        for item in heads:
            if item.id != user.id:
                candidates_raw.append((item, None, "SUBMITTED_TO_COMMITTEE_HEAD"))

        for item in _find_bgd_users(db):
            candidates_raw.append((item, None, "SUBMITTED_TO_BGD"))

    # Trưởng ban → BGĐ.
    elif current_role == COMMITTEE_ROLE_TRUONG_BAN:
        for item in _find_bgd_users(db):
            candidates_raw.append((item, None, "SUBMITTED_TO_BGD"))

    # BGĐ → HĐTV.
    elif _is_bgd_role(role_codes):
        for item in _find_hdtv_users(db):
            candidates_raw.append((item, None, "SUBMITTED_TO_HDTV"))

    candidates: List[Dict[str, Any]] = []
    seen: Set[Tuple[str, str]] = set()
    for target_user, target_unit, next_status in candidates_raw:
        key = (str(target_user.id), next_status)
        if key in seen:
            continue
        seen.add(key)

        target_role_codes = _load_role_codes_for_user(db, target_user.id)
        label_parts = [target_user.full_name or target_user.username or str(target_user.id)]

        committee_role = _committee_member_role(db, committee_id, target_user.id)
        if committee_role == COMMITTEE_ROLE_TRUONG_BAN:
            label_parts.append("Trưởng ban")
        elif committee_role == COMMITTEE_ROLE_PHO_TRUONG_BAN:
            label_parts.append("Phó trưởng ban")
        elif _is_bgd_role(target_role_codes):
            label_parts.append("BGĐ")
        elif _is_hdtv_role(target_role_codes):
            label_parts.append("HĐTV")

        candidates.append({
            "user": target_user,
            "unit": target_unit,
            "next_status": next_status,
            "display_label": " - ".join([x for x in label_parts if x]),
        })

    return candidates


def _committee_draft_notify_user_ids(db: Session, draft: DocumentDrafts, *extra_user_ids: str) -> List[str]:
    ids: List[str] = []
    if getattr(draft, "created_by", None):
        ids.append(str(draft.created_by))
    if getattr(draft, "current_handler_user_id", None):
        ids.append(str(draft.current_handler_user_id))
    if getattr(draft, "last_submitter_id", None):
        ids.append(str(draft.last_submitter_id))

    committee_id = str(getattr(draft, "committee_id", "") or "")
    if committee_id:
        rows = (
            db.query(CommitteeMembers.user_id)
            .filter(CommitteeMembers.committee_id == committee_id)
            .filter(CommitteeMembers.is_active == True)  # noqa: E712
            .all()
        )
        ids.extend([str(row[0]) for row in rows if row and row[0]])

    ids.extend([str(x) for x in extra_user_ids if x])
    clean: List[str] = []
    for uid in ids:
        if uid and uid not in clean:
            clean.append(uid)
    return clean


def _committee_name(db: Session, committee_id: Optional[str]) -> str:
    if not committee_id:
        return ""
    committee = db.get(Committees, committee_id)
    return getattr(committee, "name", "") or ""

def _get_latest_coordination_owner_user_id(db: Session, draft_id: str) -> str:
    row = (
        db.query(DocumentDraftActions)
        .filter(
            DocumentDraftActions.draft_id == draft_id,
            DocumentDraftActions.action_type == _COORDINATION_ACTION,
        )
        .order_by(DocumentDraftActions.created_at.desc())
        .first()
    )
    return str(getattr(row, "from_user_id", "") or "")


def _get_primary_membership(db: Session, user_id: str) -> Optional[UserUnitMemberships]:
    membership = (
        db.query(UserUnitMemberships)
        .filter(UserUnitMemberships.user_id == user_id)
        .order_by(UserUnitMemberships.is_primary.desc(), UserUnitMemberships.unit_id.asc())
        .first()
    )
    return membership


def _get_membership_units(db: Session, user_id: str) -> List[Units]:
    return (
        db.query(Units)
        .join(UserUnitMemberships, UserUnitMemberships.unit_id == Units.id)
        .filter(UserUnitMemberships.user_id == user_id)
        .all()
    )


def _get_accessible_unit_ids(db: Session, user: Users, role_codes: Set[str]) -> Set[str]:
    member_units = _get_membership_units(db, user.id)
    member_ids = {u.id for u in member_units}
    if _is_board(role_codes):
        return {row[0] for row in db.query(Units.id).all()}
    if _is_room_manager(role_codes) or _is_specialist_department_manager(role_codes):
        child_ids = {
            row[0]
            for row in db.query(Units.id).filter(Units.parent_id.in_(list(member_ids))).all()
        } if member_ids else set()
        return member_ids | child_ids
    if _is_team_manager(role_codes) or _is_specialist_subunit_manager(role_codes):
        return member_ids
    return member_ids


def _get_unit(db: Session, unit_id: Optional[str]) -> Optional[Units]:
    if not unit_id:
        return None
    return db.get(Units, unit_id)


def _unit_label(unit: Optional[Units]) -> str:
    if not unit:
        return ""
    return getattr(unit, "ten_don_vi", None) or unit.id


def _status_label(value: Optional[str]) -> str:
    return _STATUS_LABELS.get((value or '').strip(), value or '—')


def _action_label(value: Optional[str]) -> str:
    return _ACTION_LABELS.get((value or '').strip(), value or '—')


def _file_role_label(value: Optional[str]) -> str:
    return _FILE_ROLE_LABELS.get((value or '').strip(), value or 'Tài liệu')


async def _notify_draft_users(user_ids: Iterable[str], payload: Dict[str, Any]) -> None:
    """
    Realtime cho module draft approval.
    Dùng await trực tiếp, tránh lặp lại lỗi from_thread.run như task/inbox trước đó.
    """
    clean_ids: List[str] = []
    for raw_user_id in user_ids:
        uid = str(raw_user_id or "").strip()
        if uid and uid not in clean_ids:
            clean_ids.append(uid)

    if not clean_ids:
        return

    await manager.notify_users_json(clean_ids, payload)


def _user_label(user: Optional[Users]) -> str:
    if not user:
        return ""
    return user.full_name or user.username or user.id

def _safe_filename(filename: str) -> str:
    return Path(filename or "file").name.replace("..", "_")


def _is_allowed_file(filename: str) -> bool:
    return Path(filename or "").suffix.lower() in _ALLOWED_EXTENSIONS


def _save_upload(upload: UploadFile, draft_id: str) -> Tuple[str, int, str]:
    original_name = _safe_filename(upload.filename or "tep_dinh_kem")
    ext = Path(original_name).suffix.lower()
    if ext not in _ALLOWED_EXTENSIONS:
        raise HTTPException(status_code=400, detail="Định dạng tệp không được hỗ trợ.")
    folder = os.path.join(_upload_root(), draft_id)
    os.makedirs(folder, exist_ok=True)
    stored_name = f"{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_{uuid.uuid4().hex}{ext}"
    dest = os.path.join(folder, stored_name)
    with open(dest, "wb") as fh:
        shutil.copyfileobj(upload.file, fh)
    size = os.path.getsize(dest)
    mime_type = upload.content_type or mimetypes.guess_type(original_name)[0] or "application/octet-stream"
    return dest, size, mime_type


def _deactivate_active_files(db: Session, draft_id: str) -> None:
    rows = (
        db.query(DocumentDraftFiles)
        .filter(DocumentDraftFiles.draft_id == draft_id, DocumentDraftFiles.is_deleted.is_(False), DocumentDraftFiles.is_active.is_(True))
        .all()
    )
    for row in rows:
        row.is_active = False
        db.add(row)


def _add_file_record(
    db: Session,
    draft: DocumentDrafts,
    upload: UploadFile,
    uploaded_by: str,
    file_role: str,
    activate: bool = True,
) -> DocumentDraftFiles:
    if activate:
        _deactivate_active_files(db, draft.id)
    path, size, mime_type = _save_upload(upload, draft.id)
    rec = DocumentDraftFiles(
        draft_id=draft.id,
        file_name=_safe_filename(upload.filename or "tep_dinh_kem"),
        file_path=path,
        mime_type=mime_type,
        size_bytes=size,
        file_role=file_role,
        uploaded_by=uploaded_by,
        is_active=activate,
        is_deleted=False,
    )
    db.add(rec)
    db.flush()
    return rec


def _log_action(
    db: Session,
    draft: DocumentDrafts,
    action_type: str,
    from_user_id: Optional[str] = None,
    to_user_id: Optional[str] = None,
    from_unit_id: Optional[str] = None,
    to_unit_id: Optional[str] = None,
    comment: str = "",
    linked_file_id: Optional[str] = None,
    is_pending: bool = False,
    response_text: Optional[str] = None,
    responded_at: Optional[datetime] = None,
) -> DocumentDraftActions:
    action = DocumentDraftActions(
        draft_id=draft.id,
        action_type=action_type,
        from_user_id=from_user_id,
        to_user_id=to_user_id,
        from_unit_id=from_unit_id,
        to_unit_id=to_unit_id,
        comment=(comment or "").strip(),
        linked_file_id=linked_file_id,
        is_pending=is_pending,
        response_text=response_text,
        responded_at=responded_at,
    )
    db.add(action)
    db.flush()
    return action


def _active_file(db: Session, draft_id: str) -> Optional[DocumentDraftFiles]:
    return (
        db.query(DocumentDraftFiles)
        .filter(
            DocumentDraftFiles.draft_id == draft_id,
            DocumentDraftFiles.is_deleted.is_(False),
            DocumentDraftFiles.is_active.is_(True),
        )
        .order_by(DocumentDraftFiles.uploaded_at.desc())
        .first()
    )


def _find_team_manager(db: Session, team_unit_id: str) -> Optional[Users]:
    rows = (
        db.query(Users)
        .join(UserUnitMemberships, UserUnitMemberships.user_id == Users.id)
        .join(UserRoles, UserRoles.user_id == Users.id)
        .join(Roles, Roles.id == UserRoles.role_id)
        .filter(
            Users.status == UserStatus.ACTIVE,
            UserUnitMemberships.unit_id == team_unit_id,
            Roles.code.in_([RoleCode.ROLE_TO_TRUONG, RoleCode.ROLE_PHO_TO]),
        )
        .order_by(Users.full_name.asc(), Users.username.asc())
        .all()
    )
    return rows[0] if rows else None


def _find_room_manager(db: Session, room_unit_id: str) -> Optional[Users]:
    rows = (
        db.query(Users)
        .join(UserUnitMemberships, UserUnitMemberships.user_id == Users.id)
        .join(UserRoles, UserRoles.user_id == Users.id)
        .join(Roles, Roles.id == UserRoles.role_id)
        .filter(
            Users.status == UserStatus.ACTIVE,
            UserUnitMemberships.unit_id == room_unit_id,
            Roles.code.in_([RoleCode.ROLE_TRUONG_PHONG, RoleCode.ROLE_PHO_PHONG]),
        )
        .order_by(Users.full_name.asc(), Users.username.asc())
        .all()
    )
    return rows[0] if rows else None


def _find_specialist_subunit_managers(db: Session, subunit_id: str) -> List[Users]:
    rows = (
        db.query(Users)
        .join(UserUnitMemberships, UserUnitMemberships.user_id == Users.id)
        .join(UserRoles, UserRoles.user_id == Users.id)
        .join(Roles, Roles.id == UserRoles.role_id)
        .filter(
            Users.status == UserStatus.ACTIVE,
            UserUnitMemberships.unit_id == subunit_id,
            UserUnitMemberships.is_primary.is_(True),
            Roles.code.in_([
                RoleCode.ROLE_TRUONG_NHOM,
                RoleCode.ROLE_PHO_NHOM,
                RoleCode.ROLE_TRUONG_DON_VI,
                RoleCode.ROLE_PHO_DON_VI,
                RoleCode.ROLE_DIEU_DUONG_TRUONG_DON_VI,
                RoleCode.ROLE_KY_THUAT_VIEN_TRUONG_DON_VI,
            ]),
        )
        .order_by(Users.full_name.asc(), Users.username.asc())
        .all()
    )
    return rows


def _find_specialist_department_managers(db: Session, dept_unit_id: str) -> List[Users]:
    rows = (
        db.query(Users)
        .join(UserUnitMemberships, UserUnitMemberships.user_id == Users.id)
        .join(UserRoles, UserRoles.user_id == Users.id)
        .join(Roles, Roles.id == UserRoles.role_id)
        .filter(
            Users.status == UserStatus.ACTIVE,
            UserUnitMemberships.unit_id == dept_unit_id,
            Roles.code.in_([
                RoleCode.ROLE_TRUONG_KHOA,
                RoleCode.ROLE_PHO_TRUONG_KHOA,
                RoleCode.ROLE_DIEU_DUONG_TRUONG,
                RoleCode.ROLE_KY_THUAT_VIEN_TRUONG,
            ]),
        )
        .order_by(Users.full_name.asc(), Users.username.asc())
        .all()
    )
    return rows


def _find_board_users(db: Session) -> List[Users]:
    rows = (
        db.query(Users)
        .join(UserRoles, UserRoles.user_id == Users.id)
        .join(Roles, Roles.id == UserRoles.role_id)
        .filter(
            Users.status == UserStatus.ACTIVE,
            Roles.code.in_([
                RoleCode.ROLE_LANH_DAO,
                RoleCode.ROLE_ADMIN,
                RoleCode.ROLE_TONG_GIAM_DOC,
                RoleCode.ROLE_PHO_TONG_GIAM_DOC_THUONG_TRUC,
                RoleCode.ROLE_PHO_TONG_GIAM_DOC,
                RoleCode.ROLE_GIAM_DOC,
                RoleCode.ROLE_PHO_GIAM_DOC_TRUC,
                RoleCode.ROLE_PHO_GIAM_DOC,
            ]),
        )
        .order_by(Users.full_name.asc(), Users.username.asc())
        .all()
    )
    return rows


def _find_board_user(db: Session) -> Optional[Users]:
    rows = _find_board_users(db)
    return rows[0] if rows else None


def _append_submit_candidate(
    candidates: List[Tuple[Users, Optional[Units], str]],
    target_user: Optional[Users],
    target_unit: Optional[Units],
    next_status: str,
) -> None:
    if not target_user or not next_status:
        return
    key = (str(target_user.id), str(getattr(target_unit, "id", "") or ""), next_status)
    if any((str(item[0].id), str(getattr(item[1], "id", "") or ""), item[2]) == key for item in candidates):
        return
    candidates.append((target_user, target_unit, next_status))


def _find_submit_target(db: Session, user: Users, role_codes: Set[str], primary_unit: Optional[Units]) -> Tuple[Optional[Users], Optional[Units], str]:
    candidates = _get_submit_candidates(db, user, role_codes, primary_unit)
    if not candidates:
        return None, None, "Không xác định được tuyến trình phù hợp cho tài khoản này."
    first = candidates[0]
    return first["user"], first["unit"], first["next_status"]


def _get_submit_candidates(
    db: Session,
    user: Users,
    role_codes: Set[str],
    primary_unit: Optional[Units],
) -> List[Dict[str, Any]]:
    """
    Danh sách người nhận hợp lệ cho dropdown Trình duyệt / Đồng ý và trình cấp trên.
    Trả về list[{"user": Users, "unit": Units|None, "next_status": str, "display_label": str}]
    """
    # Luồng Ban kiêm nhiệm sẽ được xử lý riêng theo draft cụ thể trong index/submit/approve.
    # Hàm này giữ nguyên cho luồng đơn vị hành chính/chuyên môn.    
    candidates_raw: List[Tuple[Users, Optional[Units], str]] = []

    if not primary_unit:
        return []

    # Khối hành chính
    if _is_hanh_chinh_unit(primary_unit):
        if _is_team_manager(role_codes):
            if primary_unit.parent_id:
                room_unit = db.get(Units, primary_unit.parent_id)
                if room_unit:
                    for item in (
                        db.query(Users)
                        .join(UserUnitMemberships, UserUnitMemberships.user_id == Users.id)
                        .join(UserRoles, UserRoles.user_id == Users.id)
                        .join(Roles, Roles.id == UserRoles.role_id)
                        .filter(
                            Users.status == UserStatus.ACTIVE,
                            UserUnitMemberships.unit_id == room_unit.id,
                            Roles.code.in_([RoleCode.ROLE_TRUONG_PHONG, RoleCode.ROLE_PHO_PHONG]),
                        )
                        .order_by(Users.full_name.asc(), Users.username.asc())
                        .all()
                    ):
                        _append_submit_candidate(candidates_raw, item, room_unit, "SUBMITTED_TO_DEPT_MANAGER")

        elif _is_room_manager(role_codes):
            for item in _find_board_users(db):
                target_membership = _get_primary_membership(db, item.id)
                target_unit = db.get(Units, target_membership.unit_id) if target_membership and target_membership.unit_id else None
                _append_submit_candidate(candidates_raw, item, target_unit, "SUBMITTED_TO_HDTV")

        elif getattr(primary_unit, "cap_do", None) == 3:
            for item in (
                db.query(Users)
                .join(UserUnitMemberships, UserUnitMemberships.user_id == Users.id)
                .join(UserRoles, UserRoles.user_id == Users.id)
                .join(Roles, Roles.id == UserRoles.role_id)
                .filter(
                    Users.status == UserStatus.ACTIVE,
                    UserUnitMemberships.unit_id == primary_unit.id,
                    Roles.code.in_([RoleCode.ROLE_TO_TRUONG, RoleCode.ROLE_PHO_TO]),
                )
                .order_by(Users.full_name.asc(), Users.username.asc())
                .all()
            ):
                _append_submit_candidate(candidates_raw, item, primary_unit, "SUBMITTED_TO_TO_MANAGER")

        elif getattr(primary_unit, "cap_do", None) == 2:
            for item in (
                db.query(Users)
                .join(UserUnitMemberships, UserUnitMemberships.user_id == Users.id)
                .join(UserRoles, UserRoles.user_id == Users.id)
                .join(Roles, Roles.id == UserRoles.role_id)
                .filter(
                    Users.status == UserStatus.ACTIVE,
                    UserUnitMemberships.unit_id == primary_unit.id,
                    Roles.code.in_([RoleCode.ROLE_TRUONG_PHONG, RoleCode.ROLE_PHO_PHONG]),
                )
                .order_by(Users.full_name.asc(), Users.username.asc())
                .all()
            ):
                _append_submit_candidate(candidates_raw, item, primary_unit, "SUBMITTED_TO_DEPT_MANAGER")

    # Khối chuyên môn
    elif _is_chuyen_mon_unit(primary_unit):
        unit_category = _unit_category_code(primary_unit)

        if _is_specialist_subunit_manager(role_codes):
            parent_unit = db.get(Units, primary_unit.parent_id) if primary_unit.parent_id else None
            if parent_unit:
                for item in _find_specialist_department_managers(db, parent_unit.id):
                    _append_submit_candidate(candidates_raw, item, parent_unit, "SUBMITTED_TO_DEPT_MANAGER")

        elif _is_specialist_department_manager(role_codes):
            for item in _find_board_users(db):
                target_membership = _get_primary_membership(db, item.id)
                target_unit = db.get(Units, target_membership.unit_id) if target_membership and target_membership.unit_id else None
                _append_submit_candidate(candidates_raw, item, target_unit, "SUBMITTED_TO_HDTV")

        elif unit_category == "SUBUNIT":
            for item in _find_specialist_subunit_managers(db, primary_unit.id):
                _append_submit_candidate(candidates_raw, item, primary_unit, "SUBMITTED_TO_TO_MANAGER")

        elif unit_category == "KHOA":
            for item in _find_specialist_department_managers(db, primary_unit.id):
                _append_submit_candidate(candidates_raw, item, primary_unit, "SUBMITTED_TO_DEPT_MANAGER")

    candidates: List[Dict[str, Any]] = []
    for target_user, target_unit, next_status in candidates_raw:
        target_role_codes = _load_role_codes_for_user(db, target_user.id)
        label_parts = [target_user.full_name or target_user.username or str(target_user.id)]
        label_parts.append(_role_label_from_codes(target_role_codes))
        if target_unit:
            label_parts.append(_unit_label(target_unit))
        candidates.append({
            "user": target_user,
            "unit": target_unit,
            "next_status": next_status,
            "display_label": " - ".join([x for x in label_parts if x]),
        })

    return candidates


def _get_coordination_candidates(db: Session, user: Users, role_codes: Set[str], primary_unit: Optional[Units]) -> List[Users]:
    """
    Phối hợp ngang cấp theo đúng khối và đúng cấp xử lý.
    """
    if not primary_unit:
        return []

    candidates: List[Users] = []
    unit_category = _unit_category_code(primary_unit)

    # Khối hành chính
    if _is_hanh_chinh_unit(primary_unit):
        if _is_employee(role_codes) and getattr(primary_unit, "cap_do", None) == 3:
            candidates = (
                db.query(Users)
                .join(UserUnitMemberships, UserUnitMemberships.user_id == Users.id)
                .join(UserRoles, UserRoles.user_id == Users.id)
                .join(Roles, Roles.id == UserRoles.role_id)
                .filter(
                    Users.status == UserStatus.ACTIVE,
                    UserUnitMemberships.unit_id == primary_unit.id,
                    UserUnitMemberships.is_primary.is_(True),
                    Roles.code == RoleCode.ROLE_NHAN_VIEN,
                    Users.id != user.id,
                )
                .order_by(Users.full_name.asc(), Users.username.asc())
                .all()
            )
        elif _is_employee(role_codes) and getattr(primary_unit, "cap_do", None) == 2:
            candidates = (
                db.query(Users)
                .join(UserUnitMemberships, UserUnitMemberships.user_id == Users.id)
                .join(UserRoles, UserRoles.user_id == Users.id)
                .join(Roles, Roles.id == UserRoles.role_id)
                .join(Units, Units.id == UserUnitMemberships.unit_id)
                .filter(
                    Users.status == UserStatus.ACTIVE,
                    UserUnitMemberships.unit_id == primary_unit.id,
                    UserUnitMemberships.is_primary.is_(True),
                    Units.cap_do == 2,
                    Roles.code == RoleCode.ROLE_NHAN_VIEN,
                    Users.id != user.id,
                )
                .order_by(Users.full_name.asc(), Users.username.asc())
                .all()
            )
        elif _is_team_manager(role_codes):
            room_id = primary_unit.parent_id
            if room_id:
                team_ids = [
                    row[0]
                    for row in db.query(Units.id)
                    .filter(
                        Units.parent_id == room_id,
                        Units.id != primary_unit.id,
                        Units.trang_thai == UnitStatus.ACTIVE,
                        Units.block_code == primary_unit.block_code,
                    )
                    .all()
                ]
                if team_ids:
                    candidates = (
                        db.query(Users)
                        .join(UserUnitMemberships, UserUnitMemberships.user_id == Users.id)
                        .join(UserRoles, UserRoles.user_id == Users.id)
                        .join(Roles, Roles.id == UserRoles.role_id)
                        .filter(
                            Users.status == UserStatus.ACTIVE,
                            UserUnitMemberships.unit_id.in_(team_ids),
                            UserUnitMemberships.is_primary.is_(True),
                            Roles.code.in_([RoleCode.ROLE_TO_TRUONG, RoleCode.ROLE_PHO_TO]),
                            Users.id != user.id,
                        )
                        .order_by(Users.full_name.asc(), Users.username.asc())
                        .all()
                    )
        elif _is_room_manager(role_codes):
            room_ids = [
                row[0]
                for row in db.query(Units.id)
                .filter(
                    Units.cap_do == 2,
                    Units.id != primary_unit.id,
                    Units.trang_thai == UnitStatus.ACTIVE,
                    Units.block_code == primary_unit.block_code,
                )
                .all()
            ]
            if room_ids:
                candidates = (
                    db.query(Users)
                    .join(UserUnitMemberships, UserUnitMemberships.user_id == Users.id)
                    .join(UserRoles, UserRoles.user_id == Users.id)
                    .join(Roles, Roles.id == UserRoles.role_id)
                    .filter(
                        Users.status == UserStatus.ACTIVE,
                        UserUnitMemberships.unit_id.in_(room_ids),
                        UserUnitMemberships.is_primary.is_(True),
                        Roles.code.in_([RoleCode.ROLE_TRUONG_PHONG, RoleCode.ROLE_PHO_PHONG]),
                        Users.id != user.id,
                    )
                    .order_by(Users.full_name.asc(), Users.username.asc())
                    .all()
                )

    # Khối chuyên môn
    elif _is_chuyen_mon_unit(primary_unit):
        if _is_specialist_staff(role_codes) and unit_category == "SUBUNIT":
            candidates = (
                db.query(Users)
                .join(UserUnitMemberships, UserUnitMemberships.user_id == Users.id)
                .join(UserRoles, UserRoles.user_id == Users.id)
                .join(Roles, Roles.id == UserRoles.role_id)
                .filter(
                    Users.status == UserStatus.ACTIVE,
                    UserUnitMemberships.unit_id == primary_unit.id,
                    UserUnitMemberships.is_primary.is_(True),
                    Roles.code.notin_(list(_SPECIALIST_SUBUNIT_MANAGER_ROLES | _SPECIALIST_DEPT_MANAGER_ROLES | BOARD_ROLES)),
                    Users.id != user.id,
                )
                .order_by(Users.full_name.asc(), Users.username.asc())
                .all()
            )
        elif _is_specialist_staff(role_codes) and unit_category == "KHOA":
            candidates = (
                db.query(Users)
                .join(UserUnitMemberships, UserUnitMemberships.user_id == Users.id)
                .join(UserRoles, UserRoles.user_id == Users.id)
                .join(Roles, Roles.id == UserRoles.role_id)
                .filter(
                    Users.status == UserStatus.ACTIVE,
                    UserUnitMemberships.unit_id == primary_unit.id,
                    UserUnitMemberships.is_primary.is_(True),
                    Roles.code.notin_(list(_SPECIALIST_SUBUNIT_MANAGER_ROLES | _SPECIALIST_DEPT_MANAGER_ROLES | BOARD_ROLES)),
                    Users.id != user.id,
                )
                .order_by(Users.full_name.asc(), Users.username.asc())
                .all()
            )
        elif _is_specialist_subunit_manager(role_codes):
            parent_id = primary_unit.parent_id
            if parent_id:
                peer_subunit_ids = [
                    row[0]
                    for row in db.query(Units.id)
                    .filter(
                        Units.parent_id == parent_id,
                        Units.id != primary_unit.id,
                        Units.trang_thai == UnitStatus.ACTIVE,
                        Units.block_code == primary_unit.block_code,
                    )
                    .all()
                ]
                if peer_subunit_ids:
                    candidates = (
                        db.query(Users)
                        .join(UserUnitMemberships, UserUnitMemberships.user_id == Users.id)
                        .join(UserRoles, UserRoles.user_id == Users.id)
                        .join(Roles, Roles.id == UserRoles.role_id)
                        .filter(
                            Users.status == UserStatus.ACTIVE,
                            UserUnitMemberships.unit_id.in_(peer_subunit_ids),
                            UserUnitMemberships.is_primary.is_(True),
                            Roles.code.in_(list(_SPECIALIST_SUBUNIT_MANAGER_ROLES)),
                            Users.id != user.id,
                        )
                        .order_by(Users.full_name.asc(), Users.username.asc())
                        .all()
                    )
        elif _is_specialist_department_manager(role_codes):
            executive_id = primary_unit.parent_id
            if executive_id:
                peer_dept_ids = [
                    row[0]
                    for row in db.query(Units.id)
                    .filter(
                        Units.parent_id == executive_id,
                        Units.id != primary_unit.id,
                        Units.trang_thai == UnitStatus.ACTIVE,
                        Units.block_code == primary_unit.block_code,
                    )
                    .all()
                ]
                if peer_dept_ids:
                    candidates = (
                        db.query(Users)
                        .join(UserUnitMemberships, UserUnitMemberships.user_id == Users.id)
                        .join(UserRoles, UserRoles.user_id == Users.id)
                        .join(Roles, Roles.id == UserRoles.role_id)
                        .filter(
                            Users.status == UserStatus.ACTIVE,
                            UserUnitMemberships.unit_id.in_(peer_dept_ids),
                            Roles.code.in_(list(_SPECIALIST_DEPT_MANAGER_ROLES)),
                            Users.id != user.id,
                        )
                        .order_by(Users.full_name.asc(), Users.username.asc())
                        .all()
                    )

    unique: Dict[str, Users] = {}
    for item in candidates:
        unique[item.id] = item
    return list(unique.values())


def _get_pending_coordination_for_user(db: Session, draft_id: str, user_id: str) -> List[DocumentDraftActions]:
    return (
        db.query(DocumentDraftActions)
        .filter(
            DocumentDraftActions.draft_id == draft_id,
            DocumentDraftActions.action_type == _COORDINATION_ACTION,
            DocumentDraftActions.to_user_id == user_id,
            DocumentDraftActions.is_pending.is_(True),
        )
        .order_by(DocumentDraftActions.created_at.asc())
        .all()
    )


def _user_is_in_draft_flow(db: Session, draft_id: str, user_id: str) -> bool:
    if not draft_id or not user_id:
        return False

    existed = (
        db.query(DocumentDraftActions.id)
        .filter(
            DocumentDraftActions.draft_id == draft_id,
            (
                (DocumentDraftActions.from_user_id == user_id)
                | (DocumentDraftActions.to_user_id == user_id)
            ),
        )
        .first()
    )
    return existed is not None


def _can_view_draft(db: Session, draft: DocumentDrafts, user: Users, role_codes: Set[str]) -> bool:
    """
    Siết quyền xem hồ sơ dự thảo:
    - Không xem rộng theo đơn vị/phòng/khoa.
    - Chỉ user trực tiếp liên quan trong luồng mới được thấy.
    - Với hồ sơ Ban kiêm nhiệm: thành viên Ban được xem hồ sơ của Ban.
    """
    if not draft or not user:
        return False

    user_id = str(user.id)

    if str(draft.created_by or "") == user_id:
        return True

    if str(draft.current_handler_user_id or "") == user_id:
        return True

    if _get_pending_coordination_for_user(db, draft.id, user_id):
        return True

    if _user_is_in_draft_flow(db, draft.id, user_id):
        return True

    if _is_committee_draft(draft) and getattr(draft, "committee_id", None):
        if _is_committee_member(db, str(draft.committee_id), user_id):
            return True

    return False


def _can_edit_draft(draft: DocumentDrafts, user: Users) -> bool:
    return draft.created_by == user.id and draft.current_handler_user_id == user.id and draft.current_status in {"DRAFT", "RETURNED_FOR_EDIT"}


def _can_approve_forward(draft: DocumentDrafts, user: Users) -> bool:
    return draft.current_handler_user_id == user.id and draft.current_status in {
        "SUBMITTED_TO_TO_MANAGER",
        "SUBMITTED_TO_DEPT_MANAGER",
        "SUBMITTED_TO_HDTV",
        "SUBMITTED_TO_COMMITTEE_DEPUTY",
        "SUBMITTED_TO_COMMITTEE_HEAD",
        "SUBMITTED_TO_BGD",
        "IN_COORDINATION",
    }

def _can_finish_draft(db: Session, draft: DocumentDrafts, user: Users, role_codes: Set[str], coordination_owner_user_id: str = "") -> bool:
    """
    Quyền kết thúc hồ sơ:
    - Luồng đơn vị: giữ logic cũ.
    - Luồng Ban kiêm nhiệm:
      + Phó ban được Hoàn thành khi đang giữ hồ sơ ở trạng thái chờ Phó ban.
      + Trưởng ban được Hoàn thành khi đang giữ hồ sơ ở trạng thái chờ Trưởng ban.
      + BGĐ/HĐTV phê duyệt kết thúc bằng route approve, không dùng nút Hoàn thành.
    """
    if draft.current_handler_user_id != user.id:
        return False

    if draft.current_status == "IN_COORDINATION":
        return str(coordination_owner_user_id or "") == str(user.id)

    if _is_committee_draft(draft):
        committee_id = str(getattr(draft, "committee_id", "") or "")
        committee_role = _committee_member_role(db, committee_id, user.id)
        if committee_role == COMMITTEE_ROLE_PHO_TRUONG_BAN and draft.current_status == "SUBMITTED_TO_COMMITTEE_DEPUTY":
            return True
        if committee_role == COMMITTEE_ROLE_TRUONG_BAN and draft.current_status == "SUBMITTED_TO_COMMITTEE_HEAD":
            return True
        return False

    if not bool(APPROVER_ROLES & role_codes) and not _is_board(role_codes):
        return False

    if _is_board(role_codes) and draft.current_status == "SUBMITTED_TO_HDTV":
        return True

    if (_is_team_manager(role_codes) or _is_specialist_subunit_manager(role_codes)) and draft.current_status == "SUBMITTED_TO_TO_MANAGER":
        return True

    if (_is_room_manager(role_codes) or _is_specialist_department_manager(role_codes)) and draft.current_status == "SUBMITTED_TO_DEPT_MANAGER":
        return True

    return False

def _build_draft_row(db: Session, draft: DocumentDrafts) -> Dict[str, object]:
    active_file = _active_file(db, draft.id)
    creator = db.get(Users, draft.created_by) if draft.created_by else None
    handler = db.get(Users, draft.current_handler_user_id) if draft.current_handler_user_id else None
    created_unit = db.get(Units, draft.created_unit_id) if draft.created_unit_id else None
    pending_coord_count = (
        db.query(DocumentDraftActions)
        .filter(
            DocumentDraftActions.draft_id == draft.id,
            DocumentDraftActions.action_type == _COORDINATION_ACTION,
            DocumentDraftActions.is_pending.is_(True),
        )
        .count()
    )
    scope_name = _unit_label(created_unit)
    if _is_committee_draft(draft):
        scope_name = _committee_name(db, getattr(draft, "committee_id", None)) or "Ban kiêm nhiệm"

    return {
        "obj": draft,
        "id": draft.id,
        "title": draft.title,
        "document_type": draft.document_type,
        "creator_name": _user_label(creator),
        "created_unit_name": scope_name,
        "handler_name": _user_label(handler),
        "status": draft.current_status,
        "status_label": _status_label(draft.current_status),
        "active_file": active_file,
        "pending_coord_count": pending_coord_count,
    }


def _load_visible_drafts(db: Session, user: Users, role_codes: Set[str], only_mode: str = "") -> List[Dict[str, object]]:
    rows = (
        db.query(DocumentDrafts)
        .filter(DocumentDrafts.is_deleted.is_(False))
        .order_by(DocumentDrafts.updated_at.desc())
        .all()
    )

    result: List[Dict[str, object]] = []
    user_id = str(user.id)

    for draft in rows:
        if not _can_view_draft(db, draft, user, role_codes):
            continue

        if only_mode == "mine" and str(draft.created_by or "") != user_id:
            continue

        if only_mode == "pending":
            is_current_handler = str(draft.current_handler_user_id or "") == user_id
            has_pending_coordination = bool(_get_pending_coordination_for_user(db, draft.id, user_id))
            if not is_current_handler and not has_pending_coordination:
                continue

        if only_mode == "finished" and draft.current_status != "FINISHED":
            continue

        if only_mode not in {"", "mine", "pending", "finished"} and draft.current_status != only_mode:
            continue

        result.append(_build_draft_row(db, draft))

    return result


def _view_media_type(file_rec: DocumentDraftFiles) -> str:
    return file_rec.mime_type or mimetypes.guess_type(file_rec.file_name or "")[0] or "application/octet-stream"


def _build_content_disposition(disposition: str, filename: str) -> str:
    raw_name = (filename or "file").strip() or "file"

    ascii_fallback = raw_name.encode("ascii", "ignore").decode("ascii").strip()
    if not ascii_fallback:
        ascii_fallback = "file"

    ascii_fallback = ascii_fallback.replace("\\", "_").replace('"', "_")
    encoded_name = quote(raw_name, safe="")

    return f"{disposition}; filename=\"{ascii_fallback}\"; filename*=UTF-8''{encoded_name}"


def _draft_file_preview_url(file_rec: Optional[DocumentDraftFiles]) -> str:
    if not file_rec or not getattr(file_rec, "id", None):
        return "#"

    filename = getattr(file_rec, "file_name", "") or ""
    if is_office_previewable(filename):
        return f"/draft-approvals/file/{file_rec.id}/preview"

    return f"/draft-approvals/file/{file_rec.id}/view"


def _ensure_draft_access(db: Session, draft_id: str, user: Users, role_codes: Set[str]) -> DocumentDrafts:
    draft = db.get(DocumentDrafts, draft_id)
    if not draft or draft.is_deleted:
        raise HTTPException(status_code=404, detail="Không tìm thấy hồ sơ dự thảo.")
    if not _can_view_draft(db, draft, user, role_codes):
        raise HTTPException(status_code=403, detail="Bạn không có quyền truy cập hồ sơ này.")
    return draft


def _load_status_options() -> List[Tuple[str, str]]:
    return [
        ("", "Tất cả"),
        ("mine", "Do tôi tạo"),
        ("pending", "Chờ tôi xử lý"),
        ("finished", "Đã kết thúc"),
        ("DRAFT", "Nháp"),
        ("RETURNED_FOR_EDIT", "Bị trả lại"),
        ("SUBMITTED_TO_TO_MANAGER", "Chờ QL tổ"),
        ("SUBMITTED_TO_DEPT_MANAGER", "Chờ QL phòng"),
        ("SUBMITTED_TO_HDTV", "Chờ HĐTV"),
        ("IN_COORDINATION", "Đang phối hợp"),
        ("FINISHED", "Kết thúc"),
    ]

def _ensure_draft_module_allowed(role_codes: Set[str]) -> None:
    if _can_access_draft_module(role_codes):
        return
    raise HTTPException(status_code=403, detail="Bạn không có quyền truy cập tab Phê duyệt dự thảo văn bản.")





@router.get("", response_class=HTMLResponse)
def draft_approval_index(
    request: Request,
    selected_id: str = "",
    status: str = "",
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    _ensure_draft_module_allowed(role_codes)
    primary_membership = _get_primary_membership(db, user.id)
    primary_unit = db.get(Units, primary_membership.unit_id) if primary_membership and primary_membership.unit_id else None

    drafts = _load_visible_drafts(db, user, role_codes, status)
    selected_draft = None
    if selected_id:
        selected_draft = _ensure_draft_access(db, selected_id, user, role_codes)
    elif drafts:
        selected_draft = drafts[0]["obj"]

    detail = None
    if selected_draft:
        active_file = _active_file(db, selected_draft.id)
        files = (
            db.query(DocumentDraftFiles)
            .filter(DocumentDraftFiles.draft_id == selected_draft.id, DocumentDraftFiles.is_deleted.is_(False))
            .order_by(DocumentDraftFiles.uploaded_at.desc())
            .all()
        )
        actions = (
            db.query(DocumentDraftActions)
            .filter(DocumentDraftActions.draft_id == selected_draft.id)
            .order_by(DocumentDraftActions.created_at.asc())
            .all()
        )
        pending_coord = _get_pending_coordination_for_user(db, selected_draft.id, user.id)
        coordination_owner_user_id = _get_latest_coordination_owner_user_id(db, selected_draft.id)
        if selected_draft.current_handler_user_id == user.id:
            if _is_committee_draft(selected_draft):
                submit_candidates = _build_committee_submit_candidates(db, selected_draft, user, role_codes)
                coord_candidates = []
            else:
                submit_candidates = _get_submit_candidates(db, user, role_codes, primary_unit)
                coord_candidates = _get_coordination_candidates(db, user, role_codes, primary_unit)
        else:
            submit_candidates = []
            coord_candidates = []
        if active_file:
            setattr(active_file, "preview_url", _draft_file_preview_url(active_file))

        for file_row in files:
            setattr(file_row, "file_role_label", _file_role_label(getattr(file_row, "file_role", None)))
            setattr(file_row, "preview_url", _draft_file_preview_url(file_row))

        for action_row in actions:
            setattr(action_row, "action_label", _action_label(getattr(action_row, "action_type", None)))
            linked_file = getattr(action_row, "linked_file", None)
            if linked_file:
                setattr(linked_file, "preview_url", _draft_file_preview_url(linked_file))
        detail = {
            "draft": selected_draft,
            "active_file": active_file,
            "files": files,
            "actions": actions,
            "status_label": _status_label(selected_draft.current_status),
            "pending_coord": pending_coord,
            "coord_candidates": [] if _is_admin(role_codes) else coord_candidates,
            "submit_candidates": [] if _is_admin(role_codes) else submit_candidates,
            "can_edit": _can_edit_draft(selected_draft, user),
            "can_approve_forward": _can_approve_forward(selected_draft, user),
            "can_finish": _can_finish_draft(db, selected_draft, user, role_codes, coordination_owner_user_id),
            "coordination_owner_user_id": coordination_owner_user_id,
            "is_hdtv_handler": selected_draft.current_handler_user_id == user.id and _is_hdtv_role(role_codes),
            "is_bgd_handler": selected_draft.current_handler_user_id == user.id and _is_bgd_role(role_codes),
            "is_committee_draft": _is_committee_draft(selected_draft),
            "committee_name": _committee_name(db, getattr(selected_draft, "committee_id", None)),
        }

    return templates.TemplateResponse(
        "draft_approval.html",
        {
            "request": request,
            "app_name": getattr(settings, "APP_NAME", "HVGL_Workspace"),
            "company_name": getattr(settings, "COMPANY_NAME", ""),
            "draft_rows": drafts,
            "selected_detail": detail,
            "status_options": _load_status_options(),
            "selected_status": status,
            "selected_id": selected_draft.id if selected_draft else "",
            "me": user,
            "me_role_codes": role_codes,
            "primary_unit": primary_unit,
            "committee_create_options": _committee_options_for_draft_create(db, user),
        },
    )


@router.post("/create")
async def create_draft(
    request: Request,
    title: str = Form(...),
    document_type: str = Form("Dự thảo văn bản"),
    summary: str = Form(""),
    scope_type: str = Form(DRAFT_SCOPE_UNIT),
    committee_id: str = Form(""),
    upfile: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    _ensure_draft_module_allowed(role_codes)
    primary_membership = _get_primary_membership(db, user.id)
    if not primary_membership or not primary_membership.unit_id:
        return RedirectResponse(url="/draft-approvals?error=Tài khoản chưa được gán đơn vị chính.", status_code=302)

    primary_unit = db.get(Units, primary_membership.unit_id)

    clean_scope_type = _normalize_draft_scope(scope_type)
    clean_committee_id = (committee_id or "").strip()

    if clean_scope_type == DRAFT_SCOPE_COMMITTEE:
        committee = db.get(Committees, clean_committee_id)
        if not committee or str(getattr(committee, "status", "") or "").upper() != "ACTIVE" or not bool(getattr(committee, "is_active", False)):
            return RedirectResponse(url="/draft-approvals?error=Ban kiêm nhiệm không hợp lệ hoặc đã kết thúc.", status_code=302)
        if not bool(getattr(committee, "allow_draft_approval", True)):
            return RedirectResponse(url="/draft-approvals?error=Ban kiêm nhiệm này chưa được phép sử dụng phê duyệt dự thảo.", status_code=302)
        if not _is_committee_member(db, clean_committee_id, user.id):
            return RedirectResponse(url="/draft-approvals?error=Bạn không thuộc Ban kiêm nhiệm đã chọn.", status_code=302)
    else:
        clean_committee_id = ""

    if not upfile or not upfile.filename or not _is_allowed_file(upfile.filename):
        return RedirectResponse(url="/draft-approvals?error=Chưa chọn đúng định dạng tài liệu.", status_code=302)

    draft = DocumentDrafts(
        title=(title or "").strip(),
        document_type=(document_type or "Dự thảo văn bản").strip(),
        summary=(summary or "").strip(),
        created_by=user.id,
        created_unit_id=primary_unit.id if primary_unit else None,
        scope_type=clean_scope_type,
        committee_id=clean_committee_id or None,
        current_status="DRAFT",
        current_handler_user_id=user.id,
        current_handler_unit_id=primary_unit.id if primary_unit else None,
        current_role_code="CREATOR",
        last_submitter_id=user.id,
    )
    db.add(draft)
    db.flush()
    file_rec = _add_file_record(db, draft, upfile, user.id, "DRAFT_UPLOAD", activate=True)
    _log_action(
        db,
        draft,
        action_type="CREATE",
        from_user_id=user.id,
        to_user_id=user.id,
        from_unit_id=primary_unit.id if primary_unit else None,
        to_unit_id=primary_unit.id if primary_unit else None,
        comment="Tạo hồ sơ dự thảo.",
        linked_file_id=file_rec.id,
    )
    db.commit()

    try:
        payload = {
            "module": "draft",
            "type": "draft_created",
            "draft_id": str(draft.id),
            "from_user_id": str(user.id),
            "to_user_id": str(user.id),
            "timestamp": datetime.utcnow().isoformat(),
        }
        notify_user_ids = (
            _committee_draft_notify_user_ids(db, draft, str(user.id))
            if _is_committee_draft(draft)
            else [str(user.id)]
        )
        await _notify_draft_users(notify_user_ids, payload)
    except Exception as ex:
        logger.exception("[draft_approval] Lỗi realtime sau tạo/trình dự thảo: %s", ex)
    return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&msg=Tạo hồ sơ dự thảo thành công.", status_code=302)


@router.post("/{draft_id}/upload")
async def upload_replacement_file(
    draft_id: str,
    request: Request,
    upfile: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    _ensure_draft_module_allowed(role_codes)
    draft = _ensure_draft_access(db, draft_id, user, role_codes)
    if not _can_edit_draft(draft, user):
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Bạn không có quyền upload thay thế ở bước hiện tại.", status_code=302)
    if not upfile or not upfile.filename or not _is_allowed_file(upfile.filename):
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Định dạng tài liệu không được hỗ trợ.", status_code=302)

    rec = _add_file_record(db, draft, upfile, user.id, "DRAFT_UPLOAD", activate=True)
    draft.updated_at = _now()
    _log_action(db, draft, "UPLOAD_REPLACEMENT", from_user_id=user.id, to_user_id=user.id, comment="Cập nhật tài liệu dự thảo.", linked_file_id=rec.id)
    db.commit()

    try:
        payload = {
            "module": "draft",
            "type": "draft_updated",
            "draft_id": str(draft.id),
            "from_user_id": str(user.id),
            "to_user_id": str(draft.current_handler_user_id or ""),
            "timestamp": datetime.utcnow().isoformat(),
        }
        notify_user_ids = (
            _committee_draft_notify_user_ids(db, draft, str(user.id), str(draft.current_handler_user_id or ""))
            if _is_committee_draft(draft)
            else [str(user.id), str(draft.current_handler_user_id or "")]
        )
        await _notify_draft_users(notify_user_ids, payload)
    except Exception as ex:
        logger.exception("[draft_approval] Lỗi realtime sau cập nhật tài liệu dự thảo: %s", ex)

    return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&msg=Upload tài liệu thành công.", status_code=302)


@router.post("/{draft_id}/submit")
async def submit_draft(
    draft_id: str,
    request: Request,
    recipient_id: str = Form(...),
    comment: str = Form(""),
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    _ensure_draft_module_allowed(role_codes)
    draft = _ensure_draft_access(db, draft_id, user, role_codes)
    if draft.current_handler_user_id != user.id:
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Hồ sơ này không nằm ở bước xử lý của bạn.", status_code=302)

    primary_membership = _get_primary_membership(db, user.id)
    primary_unit = db.get(Units, primary_membership.unit_id) if primary_membership and primary_membership.unit_id else None

    if _is_committee_draft(draft):
        submit_candidates = _build_committee_submit_candidates(db, draft, user, role_codes)
    else:
        submit_candidates = _get_submit_candidates(db, user, role_codes, primary_unit)

    candidate_map = {str(item["user"].id): item for item in submit_candidates if item.get("user")}
    chosen = candidate_map.get(str(recipient_id or "").strip())

    if not chosen:
        return RedirectResponse(
            url=f"/draft-approvals?selected_id={draft.id}&error=Người nhận trình duyệt không hợp lệ.",
            status_code=302,
        )

    target_user = chosen["user"]
    target_unit = chosen["unit"]
    next_status = chosen["next_status"]

    draft.current_status = next_status
    draft.current_handler_user_id = target_user.id
    draft.current_handler_unit_id = target_unit.id if target_unit else None
    draft.current_role_code = ",".join(sorted(_load_role_codes_for_user(db, target_user.id)))
    draft.last_submitter_id = user.id
    draft.last_submitted_at = _now()
    draft.updated_at = _now()
    _log_action(
        db,
        draft,
        action_type="SUBMIT",
        from_user_id=user.id,
        to_user_id=target_user.id,
        from_unit_id=primary_unit.id if primary_unit else None,
        to_unit_id=target_unit.id if target_unit else None,
        comment=(comment or "").strip() or "Trình dự thảo lên cấp phê duyệt tiếp theo.",
        linked_file_id=_active_file(db, draft.id).id if _active_file(db, draft.id) else None,
    )
    db.commit()

    try:
        payload = {
            "module": "draft",
            "type": "draft_submitted",
            "draft_id": str(draft.id),
            "from_user_id": str(user.id),
            "to_user_id": str(target_user.id),
            "timestamp": datetime.utcnow().isoformat(),
        }
        await _notify_draft_users(
            [str(target_user.id), str(user.id)],
            payload,
        )
    except Exception as ex:
        logger.exception("[draft_approval] Lỗi realtime sau trình dự thảo: %s", ex)

    return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&msg=Trình dự thảo thành công.", status_code=302)


@router.post("/{draft_id}/approve")
async def approve_forward(
    draft_id: str,
    request: Request,
    recipient_id: str = Form(""),
    comment: str = Form(""),
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    _ensure_draft_module_allowed(role_codes)
    draft = _ensure_draft_access(db, draft_id, user, role_codes)
    if not _can_approve_forward(draft, user):
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Bạn không có quyền phê duyệt hồ sơ này.", status_code=302)

    active_file = _active_file(db, draft.id)
    primary_membership = _get_primary_membership(db, user.id)
    primary_unit = db.get(Units, primary_membership.unit_id) if primary_membership and primary_membership.unit_id else None

    if _is_committee_draft(draft) and _is_hdtv_role(role_codes):
        draft.current_status = "FINISHED"
        draft.current_handler_user_id = draft.last_submitter_id or draft.created_by
        draft.current_handler_unit_id = draft.created_unit_id
        draft.current_role_code = "FINISHED"
        draft.finished_at = _now()
        draft.updated_at = _now()
        _log_action(
            db,
            draft,
            action_type="HDTV_APPROVED",
            from_user_id=user.id,
            to_user_id=draft.last_submitter_id or draft.created_by,
            from_unit_id=primary_unit.id if primary_unit else None,
            to_unit_id=draft.current_handler_unit_id,
            comment=(comment or "").strip() or "HĐTV phê duyệt nội dung văn bản Ban kiêm nhiệm. Hồ sơ kết thúc.",
            linked_file_id=active_file.id if active_file else None,
        )

    elif _is_committee_draft(draft) and _is_bgd_role(role_codes) and not (recipient_id or "").strip():
        draft.current_status = "FINISHED"
        draft.current_handler_user_id = draft.last_submitter_id or draft.created_by
        draft.current_handler_unit_id = draft.created_unit_id
        draft.current_role_code = "FINISHED"
        draft.finished_at = _now()
        draft.updated_at = _now()
        _log_action(
            db,
            draft,
            action_type="BGD_APPROVED",
            from_user_id=user.id,
            to_user_id=draft.last_submitter_id or draft.created_by,
            from_unit_id=primary_unit.id if primary_unit else None,
            to_unit_id=draft.current_handler_unit_id,
            comment=(comment or "").strip() or "BGĐ phê duyệt nội dung văn bản Ban kiêm nhiệm. Hồ sơ kết thúc.",
            linked_file_id=active_file.id if active_file else None,
        )

    elif _is_board(role_codes) and not _is_committee_draft(draft):
        draft.current_status = "FINISHED"
        draft.current_handler_user_id = draft.last_submitter_id
        draft.current_handler_unit_id = draft.created_unit_id
        draft.current_role_code = "FINISHED"
        draft.finished_at = _now()
        draft.updated_at = _now()
        _log_action(
            db,
            draft,
            action_type="HDTV_APPROVED",
            from_user_id=user.id,
            to_user_id=draft.last_submitter_id,
            from_unit_id=primary_unit.id if primary_unit else None,
            to_unit_id=draft.current_handler_unit_id,
            comment=(comment or "").strip() or "HĐTV phê duyệt nội dung văn bản. Hồ sơ tại tab này kết thúc.",
            linked_file_id=active_file.id if active_file else None,
        )
    else:
        if _is_committee_draft(draft):
            submit_candidates = _build_committee_submit_candidates(db, draft, user, role_codes)
        else:
            submit_candidates = _get_submit_candidates(db, user, role_codes, primary_unit)
        candidate_map = {str(item["user"].id): item for item in submit_candidates if item.get("user")}
        chosen = candidate_map.get(str(recipient_id or "").strip())

        if not chosen:
            return RedirectResponse(
                url=f"/draft-approvals?selected_id={draft.id}&error=Người nhận trình duyệt không hợp lệ.",
                status_code=302,
            )

        target_user = chosen["user"]
        target_unit = chosen["unit"]
        next_status = chosen["next_status"]
        draft.current_status = next_status
        draft.current_handler_user_id = target_user.id
        draft.current_handler_unit_id = target_unit.id if target_unit else None
        draft.current_role_code = ",".join(sorted(_load_role_codes_for_user(db, target_user.id)))
        draft.last_submitter_id = user.id
        draft.last_submitted_at = _now()
        draft.updated_at = _now()
        _log_action(
            db,
            draft,
            action_type="APPROVE_FORWARD",
            from_user_id=user.id,
            to_user_id=target_user.id,
            from_unit_id=primary_unit.id if primary_unit else None,
            to_unit_id=target_unit.id if target_unit else None,
            comment=(comment or "").strip() or "Đồng ý nội dung và trình cấp trên tiếp theo.",
            linked_file_id=active_file.id if active_file else None,
        )
    db.commit()

    try:
        if _is_committee_draft(draft):
            payload = {
                "module": "draft",
                "type": "draft_approved" if draft.current_status == "FINISHED" else "draft_submitted",
                "draft_id": str(draft.id),
                "from_user_id": str(user.id),
                "to_user_id": str(draft.current_handler_user_id or ""),
                "timestamp": datetime.utcnow().isoformat(),
            }
            await _notify_draft_users(
                _committee_draft_notify_user_ids(db, draft, str(user.id), str(draft.current_handler_user_id or "")),
                payload,
            )
        elif _is_board(role_codes):
            payload = {
                "module": "draft",
                "type": "draft_approved",
                "draft_id": str(draft.id),
                "from_user_id": str(user.id),
                "to_user_id": str(draft.last_submitter_id or ""),
                "timestamp": datetime.utcnow().isoformat(),
            }
            await _notify_draft_users(
                [str(draft.last_submitter_id or ""), str(user.id)],
                payload,
            )
        else:
            payload = {
                "module": "draft",
                "type": "draft_submitted",
                "draft_id": str(draft.id),
                "from_user_id": str(user.id),
                "to_user_id": str(target_user.id),
                "timestamp": datetime.utcnow().isoformat(),
            }
            await _notify_draft_users(
                [str(target_user.id), str(user.id)],
                payload,
            )
    except Exception as ex:
        logger.exception("[draft_approval] Lỗi realtime sau phê duyệt/chuyển tiếp dự thảo: %s", ex)

    return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&msg=Xử lý phê duyệt thành công.", status_code=302)

@router.post("/{draft_id}/finish")
async def finish_draft(
    draft_id: str,
    request: Request,
    comment: str = Form(""),
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    _ensure_draft_module_allowed(role_codes)
    draft = _ensure_draft_access(db, draft_id, user, role_codes)

    coordination_owner_user_id = _get_latest_coordination_owner_user_id(db, draft.id)
    if not _can_finish_draft(db, draft, user, role_codes, coordination_owner_user_id):
        return RedirectResponse(
            url=f"/draft-approvals?selected_id={draft.id}&error=Bạn không có quyền kết thúc hồ sơ này.",
            status_code=302,
        )

    primary_membership = _get_primary_membership(db, user.id)
    primary_unit = db.get(Units, primary_membership.unit_id) if primary_membership and primary_membership.unit_id else None
    active_file = _active_file(db, draft.id)

    draft.current_status = "FINISHED"
    draft.current_handler_user_id = draft.created_by
    draft.current_handler_unit_id = draft.created_unit_id
    draft.current_role_code = "FINISHED"
    draft.finished_at = _now()
    draft.updated_at = _now()

    _log_action(
        db,
        draft,
        action_type="COMMITTEE_COMPLETED" if _is_committee_draft(draft) else "FINISHED",
        from_user_id=user.id,
        to_user_id=draft.created_by,
        from_unit_id=primary_unit.id if primary_unit else None,
        to_unit_id=draft.created_unit_id,
        comment=(comment or "").strip() or (
            "Ban kiêm nhiệm hoàn thành xử lý hồ sơ." if _is_committee_draft(draft)
            else "Kết thúc phê duyệt hoàn thành hồ sơ."
        ),
        linked_file_id=active_file.id if active_file else None,
    )

    db.commit()

    try:
        payload = {
            "module": "draft",
            "type": "draft_approved",
            "draft_id": str(draft.id),
            "from_user_id": str(user.id),
            "to_user_id": str(draft.created_by or ""),
            "timestamp": datetime.utcnow().isoformat(),
        }
        notify_user_ids = (
            _committee_draft_notify_user_ids(db, draft, str(draft.created_by or ""), str(user.id))
            if _is_committee_draft(draft)
            else [str(draft.created_by or ""), str(user.id)]
        )
        await _notify_draft_users(
            notify_user_ids,
            payload,
        )
    except Exception as ex:
        logger.exception("[draft_approval] Lỗi realtime sau kết thúc hồ sơ dự thảo: %s", ex)

    return RedirectResponse(
        url=f"/draft-approvals?selected_id={draft.id}&msg=Đã kết thúc phê duyệt hoàn thành.",
        status_code=302,
    )
    
@router.post("/{draft_id}/return")
async def return_for_edit(
    draft_id: str,
    request: Request,
    comment: str = Form(...),
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    _ensure_draft_module_allowed(role_codes)
    draft = _ensure_draft_access(db, draft_id, user, role_codes)
    if draft.current_handler_user_id != user.id:
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Bạn không phải người đang xử lý hồ sơ.", status_code=302)
    if not (comment or "").strip():
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Phải nhập ý kiến sửa đổi, bổ sung khi trả lại.", status_code=302)

    primary_membership = _get_primary_membership(db, user.id)
    primary_unit = db.get(Units, primary_membership.unit_id) if primary_membership and primary_membership.unit_id else None
    return_user_id = draft.last_submitter_id or draft.created_by
    return_membership = _get_primary_membership(db, return_user_id) if return_user_id else None
    draft.current_status = "RETURNED_FOR_EDIT"
    draft.current_handler_user_id = return_user_id
    draft.current_handler_unit_id = return_membership.unit_id if return_membership else draft.created_unit_id
    draft.current_role_code = "RETURNED"
    draft.updated_at = _now()
    _log_action(
        db,
        draft,
        action_type="RETURN_FOR_EDIT",
        from_user_id=user.id,
        to_user_id=return_user_id,
        from_unit_id=primary_unit.id if primary_unit else None,
        to_unit_id=draft.current_handler_unit_id,
        comment=(comment or "").strip(),
        linked_file_id=_active_file(db, draft.id).id if _active_file(db, draft.id) else None,
    )
    db.commit()

    try:
        payload = {
            "module": "draft",
            "type": "draft_returned",
            "draft_id": str(draft.id),
            "from_user_id": str(user.id),
            "to_user_id": str(return_user_id or ""),
            "timestamp": datetime.utcnow().isoformat(),
        }
        await _notify_draft_users(
            [str(return_user_id or ""), str(user.id)],
            payload,
        )
    except Exception as ex:
        logger.exception("[draft_approval] Lỗi realtime sau trả lại hồ sơ dự thảo: %s", ex)

    return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&msg=Đã trả lại hồ sơ để chỉnh sửa.", status_code=302)


@router.post("/{draft_id}/return-edited")
async def return_with_edited_file(
    draft_id: str,
    request: Request,
    comment: str = Form(...),
    upfile: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    _ensure_draft_module_allowed(role_codes)
    draft = _ensure_draft_access(db, draft_id, user, role_codes)
    if draft.current_handler_user_id != user.id:
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Bạn không phải người đang xử lý hồ sơ.", status_code=302)
    if not (comment or "").strip():
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Phải nhập ý kiến sửa đổi, bổ sung khi trả lại.", status_code=302)
    if not upfile or not upfile.filename or not _is_allowed_file(upfile.filename):
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Phải upload file đã sửa hợp lệ khi trả lại theo luồng này.", status_code=302)

    primary_membership = _get_primary_membership(db, user.id)
    primary_unit = db.get(Units, primary_membership.unit_id) if primary_membership and primary_membership.unit_id else None
    return_user_id = draft.last_submitter_id or draft.created_by
    return_membership = _get_primary_membership(db, return_user_id) if return_user_id else None
    returned_file = _add_file_record(db, draft, upfile, user.id, "RETURNED_EDITED_FILE", activate=True)
    draft.current_status = "RETURNED_FOR_EDIT"
    draft.current_handler_user_id = return_user_id
    draft.current_handler_unit_id = return_membership.unit_id if return_membership else draft.created_unit_id
    draft.current_role_code = "RETURNED"
    draft.updated_at = _now()
    _log_action(
        db,
        draft,
        action_type="RETURN_WITH_EDITED_FILE",
        from_user_id=user.id,
        to_user_id=return_user_id,
        from_unit_id=primary_unit.id if primary_unit else None,
        to_unit_id=draft.current_handler_unit_id,
        comment=(comment or "").strip(),
        linked_file_id=returned_file.id,
    )
    db.commit()

    try:
        payload = {
            "module": "draft",
            "type": "draft_returned",
            "draft_id": str(draft.id),
            "from_user_id": str(user.id),
            "to_user_id": str(return_user_id or ""),
            "timestamp": datetime.utcnow().isoformat(),
        }
        await _notify_draft_users(
            [str(return_user_id or ""), str(user.id)],
            payload,
        )
    except Exception as ex:
        logger.exception("[draft_approval] Lỗi realtime sau trả lại hồ sơ kèm file sửa: %s", ex)

    return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&msg=Đã trả lại hồ sơ kèm file đã sửa.", status_code=302)


@router.post("/{draft_id}/coordinate")
async def send_for_coordination(
    draft_id: str,
    request: Request,
    recipient_ids: List[str] = Form([]),
    comment: str = Form(""),
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    _ensure_draft_module_allowed(role_codes)
    draft = _ensure_draft_access(db, draft_id, user, role_codes)
    if draft.current_handler_user_id != user.id:
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Chỉ người đang xử lý chính mới được gửi phối hợp.", status_code=302)
    clean_recipient_ids: List[str] = []
    for rid in recipient_ids or []:
        rid = str(rid or "").strip()
        if rid and rid not in clean_recipient_ids:
            clean_recipient_ids.append(rid)

    if not clean_recipient_ids:
        return RedirectResponse(
            url=f"/draft-approvals?selected_id={draft.id}&error=Chưa chọn người nhận phối hợp.",
            status_code=302,
        )

    primary_membership = _get_primary_membership(db, user.id)
    primary_unit = db.get(Units, primary_membership.unit_id) if primary_membership and primary_membership.unit_id else None

    coord_candidates = _get_coordination_candidates(db, user, role_codes, primary_unit)
    candidate_map = {str(item.id): item for item in coord_candidates}

    invalid_ids = [rid for rid in clean_recipient_ids if rid not in candidate_map]
    if invalid_ids:
        return RedirectResponse(
            url=f"/draft-approvals?selected_id={draft.id}&error=Có người nhận phối hợp không hợp lệ.",
            status_code=302,
        )

    chosen_ids = clean_recipient_ids
    if not chosen_ids:
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&error=Danh sách nhận phối hợp không hợp lệ.", status_code=302)

    draft.current_status = "IN_COORDINATION"
    draft.updated_at = _now()
    linked_file = _active_file(db, draft.id)
    for recipient_id in chosen_ids:
        rec_m = _get_primary_membership(db, recipient_id)
        _log_action(
            db,
            draft,
            action_type=_COORDINATION_ACTION,
            from_user_id=user.id,
            to_user_id=recipient_id,
            from_unit_id=primary_unit.id if primary_unit else None,
            to_unit_id=rec_m.unit_id if rec_m else None,
            comment=(comment or "").strip() or "Đề nghị phối hợp góp ý dự thảo văn bản.",
            linked_file_id=linked_file.id if linked_file else None,
            is_pending=True,
        )
    db.commit()

    try:
        payload = {
            "module": "draft",
            "type": "draft_coordination_requested",
            "draft_id": str(draft.id),
            "from_user_id": str(user.id),
            "to_user_id": ",".join(chosen_ids),
            "timestamp": datetime.utcnow().isoformat(),
        }
        await _notify_draft_users(
            [str(rid) for rid in chosen_ids] + [str(user.id)],
            payload,
        )
    except Exception as ex:
        logger.exception("[draft_approval] Lỗi realtime sau gửi phối hợp dự thảo: %s", ex)

    return RedirectResponse(url=f"/draft-approvals?selected_id={draft.id}&msg=Đã gửi phối hợp thành công.", status_code=302)


@router.post("/{draft_id}/coordinate-reply/{action_id}")
async def reply_coordination(
    draft_id: str,
    action_id: str,
    request: Request,
    response_text: str = Form(...),
    db: Session = Depends(get_db),
):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    _ensure_draft_module_allowed(role_codes)
    _ = _ensure_draft_access(db, draft_id, user, role_codes)
    action = db.get(DocumentDraftActions, action_id)
    if not action or action.draft_id != draft_id or action.action_type != _COORDINATION_ACTION:
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft_id}&error=Không tìm thấy yêu cầu phối hợp.", status_code=302)
    if action.to_user_id != user.id:
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft_id}&error=Bạn không phải người nhận phối hợp của yêu cầu này.", status_code=302)
    if not (response_text or "").strip():
        return RedirectResponse(url=f"/draft-approvals?selected_id={draft_id}&error=Phải nhập ý kiến phản hồi phối hợp.", status_code=302)

    action.response_text = (response_text or "").strip()
    action.responded_at = _now()
    action.is_pending = False
    db.add(action)
    db.commit()

    try:
        payload = {
            "module": "draft",
            "type": "draft_coordination_replied",
            "draft_id": str(draft_id),
            "from_user_id": str(user.id),
            "to_user_id": str(action.from_user_id or ""),
            "timestamp": datetime.utcnow().isoformat(),
        }
        await _notify_draft_users(
            [str(action.from_user_id or ""), str(user.id)],
            payload,
        )
    except Exception as ex:
        logger.exception("[draft_approval] Lỗi realtime sau phản hồi phối hợp dự thảo: %s", ex)

    return RedirectResponse(url=f"/draft-approvals?selected_id={draft_id}&msg=Đã phản hồi phối hợp.", status_code=302)

@router.get("/file/{file_id}/preview")
def draft_file_preview(file_id: str, request: Request, db: Session = Depends(get_db)):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    _ensure_draft_module_allowed(role_codes)

    rec = db.get(DocumentDraftFiles, file_id)
    if not rec or rec.is_deleted:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài liệu.")

    _ensure_draft_access(db, rec.draft_id, user, role_codes)

    if not rec.file_path or not os.path.exists(rec.file_path):
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp trên máy chủ.")

    filename = rec.file_name or os.path.basename(rec.file_path)

    if is_office_previewable(filename):
        try:
            preview_path = ensure_office_pdf_preview(
                source_path=rec.file_path,
                preview_key=f"draft_{rec.id}",
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

    media_type = _view_media_type(rec)
    if media_type in _INLINE_MIME_EXACT or any(media_type.startswith(p) for p in _INLINE_MIME_PREFIXES):
        return FileResponse(
            rec.file_path,
            media_type=media_type,
            headers={"Content-Disposition": _build_content_disposition("inline", filename)},
        )

    raise HTTPException(status_code=400, detail="Định dạng tệp này không hỗ trợ xem trước.")


@router.get("/file/{file_id}/download")
def draft_file_download(file_id: str, request: Request, db: Session = Depends(get_db)):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    _ensure_draft_module_allowed(role_codes)

    rec = db.get(DocumentDraftFiles, file_id)
    if not rec or rec.is_deleted:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài liệu.")

    draft = _ensure_draft_access(db, rec.draft_id, user, role_codes)
    _ = draft

    if not rec.file_path or not os.path.exists(rec.file_path):
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp trên máy chủ.")

    filename = rec.file_name or os.path.basename(rec.file_path)
    return FileResponse(
        rec.file_path,
        media_type=_view_media_type(rec),
        headers={"Content-Disposition": _build_content_disposition("attachment", filename)},
    )

@router.get("/file/{file_id}/view")
def draft_file_view(file_id: str, request: Request, db: Session = Depends(get_db)):
    _ensure_tables()
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)
    _ensure_draft_module_allowed(role_codes)

    rec = db.get(DocumentDraftFiles, file_id)
    if not rec or rec.is_deleted:
        raise HTTPException(status_code=404, detail="Không tìm thấy tài liệu.")

    _ensure_draft_access(db, rec.draft_id, user, role_codes)

    if not rec.file_path or not os.path.exists(rec.file_path):
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp trên máy chủ.")

    filename = rec.file_name or os.path.basename(rec.file_path)
    media_type = _view_media_type(rec)

    return FileResponse(
        rec.file_path,
        media_type=media_type,
        headers={"Content-Disposition": _build_content_disposition("inline", filename)},
    )
