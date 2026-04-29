# -*- coding: utf-8 -*-
"""
app/routers/files.py

Tab "Tài liệu" = Kho tài liệu sử dụng chung cho đơn vị.

Quy tắc hiển thị:
- Admin / HĐTV: không xem kho tài liệu phòng / tổ.
- Nếu user thuộc PHÒNG tải file lên:
  + file gắn unit_id = PHÒNG đó
  + thành viên của PHÒNG thấy
  + thành viên các TỔ trực thuộc PHÒNG cũng thấy
- Nếu user thuộc TỔ tải file lên:
  + file gắn unit_id = TỔ đó
  + chỉ thành viên TỔ đó thấy
  + QL phòng của PHÒNG mẹ thấy
  + nhân viên thuộc PHÒNG mẹ không thấy
- User đơn vị khác không thấy / không tải / không xem được.
- Xóa file: chỉ cho phép khi file thuộc đúng đơn vị chính hiện tại của người dùng.

Phạm vi cập nhật:
- Không đổi DB/migration.
- Không đụng Inbox/Task.
- Bổ sung: danh sách file, tìm kiếm theo tên, lọc theo loại file, sắp xếp, phân trang.
- Hỗ trợ preview tốt hơn cho PDF / ảnh / video.
- Mở rộng hỗ trợ upload: doc, docx, xls, xlsx, pdf, ảnh, video.
- Kiểm tra định dạng file ở backend.
- Tránh ghi đè file khi trùng tên.
"""

import hashlib
import mimetypes
import os
import pathlib
from urllib.parse import quote
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Set, List

from fastapi import APIRouter, Depends, File, Form, HTTPException, Request, UploadFile
from sqlalchemy import func, or_
from sqlalchemy.orm import Session
from starlette.responses import FileResponse, RedirectResponse
from starlette.templating import Jinja2Templates

from ..config import settings
from ..models import (
    Files, UserUnitMemberships, Units, Roles, UserRoles, RoleCode,
    VisibilityGrants, Committees
)
from ..security.deps import get_db, login_required
from ..office_preview import (
    OfficePreviewError,
    ensure_office_pdf_preview,
    is_office_previewable,
)
from ..committees.service import (
    allowed_committee_ids_for_user,
    get_user_committee_ids,
    user_can_view_committee,
    user_is_admin,
    user_is_committee_manager,
    user_is_committee_member,
)

router = APIRouter()

templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(__file__), "..", "templates")
)

# =========================
# Cấu hình loại file cho phép
# =========================
ALLOWED_EXTENSIONS = {
    ".doc", ".docx",
    ".xls", ".xlsx",
    ".pdf",
    ".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp",
    ".mp4", ".mov", ".avi", ".mkv", ".wmv",
}

DOCUMENT_EXTENSIONS = {".doc", ".docx"}
SPREADSHEET_EXTENSIONS = {".xls", ".xlsx"}
PDF_EXTENSIONS = {".pdf"}
IMAGE_EXTENSIONS = {".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp"}
VIDEO_EXTENSIONS = {".mp4", ".mov", ".avi", ".mkv", ".wmv"}

SORT_FIELDS = {
    "name": "name",
    "type": "type",
    "owner": "owner",
    "uploaded_at": "uploaded_at",
    "size": "size",
}

ROLE_ADMIN = "ROLE_ADMIN"
ROLE_LANH_DAO = "ROLE_LANH_DAO"
ROLE_TRUONG_PHONG = "ROLE_TRUONG_PHONG"
ROLE_PHO_PHONG = "ROLE_PHO_PHONG"
ROLE_TO_TRUONG = "ROLE_TO_TRUONG"
ROLE_PHO_TO = "ROLE_PHO_TO"
ROLE_TRUONG_KHOA = "ROLE_TRUONG_KHOA"
ROLE_PHO_TRUONG_KHOA = "ROLE_PHO_TRUONG_KHOA"
ROLE_TRUONG_DON_VI = "ROLE_TRUONG_DON_VI"
ROLE_PHO_DON_VI = "ROLE_PHO_DON_VI"
ROLE_DIEU_DUONG_TRUONG = "ROLE_DIEU_DUONG_TRUONG"
ROLE_KY_THUAT_VIEN_TRUONG = "ROLE_KY_THUAT_VIEN_TRUONG"
ROLE_DIEU_DUONG_TRUONG_DON_VI = "ROLE_DIEU_DUONG_TRUONG_DON_VI"
ROLE_KY_THUAT_VIEN_TRUONG_DON_VI = "ROLE_KY_THUAT_VIEN_TRUONG_DON_VI"
ROLE_TRUONG_NHOM = "ROLE_TRUONG_NHOM"
ROLE_PHO_NHOM = "ROLE_PHO_NHOM"

FILE_SCOPE_UNIT = "UNIT"
FILE_SCOPE_COMMITTEE = "COMMITTEE"

# =========================
# Helpers
# =========================
def _ensure_dir(dir_path: str) -> None:
    pathlib.Path(dir_path).mkdir(parents=True, exist_ok=True)


def _get_upload_dir() -> str:
    default_upload_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "instance", "uploads")
    )
    upload_dir = getattr(settings, "UPLOAD_DIR", "") or default_upload_dir
    upload_dir = os.path.abspath(str(upload_dir))

    if "hvgl_workspace" in upload_dir.replace("\\", "/").lower():
        return default_upload_dir

    return upload_dir


def _get_max_file_bytes() -> int:
    max_mb = getattr(settings, "MAX_FILE_SIZE_MB", 25)
    try:
        max_mb = int(max_mb)
    except Exception:
        max_mb = 25
    return max_mb * 1024 * 1024


def _get_file_ext(filename: str) -> str:
    return os.path.splitext(filename or "")[1].lower().strip()


def _is_allowed_extension(filename: str) -> bool:
    return _get_file_ext(filename) in ALLOWED_EXTENSIONS


def _get_file_kind(filename: str) -> str:
    ext = _get_file_ext(filename)
    if ext in DOCUMENT_EXTENSIONS:
        return "document"
    if ext in SPREADSHEET_EXTENSIONS:
        return "spreadsheet"
    if ext in PDF_EXTENSIONS:
        return "pdf"
    if ext in IMAGE_EXTENSIONS:
        return "image"
    if ext in VIDEO_EXTENSIONS:
        return "video"
    return "other"


def _get_file_kind_label(filename: str) -> str:
    kind = _get_file_kind(filename)
    mapping = {
        "document": "Word",
        "spreadsheet": "Excel",
        "pdf": "PDF",
        "image": "Ảnh",
        "video": "Video",
        "other": "Khác",
    }
    return mapping.get(kind, "Khác")


def _can_inline_preview(filename: str) -> bool:
    return _get_file_kind(filename) in {"pdf", "image", "video"} or is_office_previewable(filename)


def _guess_mime(path: str) -> str:
    mt, _ = mimetypes.guess_type(path)
    return mt or "application/octet-stream"

def _build_content_disposition(disposition: str, filename: str) -> str:
    """
    Tạo header Content-Disposition an toàn với tên file có dấu tiếng Việt.
    Dùng RFC 5987: filename*=UTF-8''...
    Đồng thời giữ filename ASCII fallback để tương thích rộng hơn.
    """
    raw_name = (filename or "file").strip() or "file"

    ascii_fallback = raw_name.encode("ascii", "ignore").decode("ascii").strip()
    if not ascii_fallback:
        ascii_fallback = "file"

    ascii_fallback = ascii_fallback.replace("\\", "_").replace('"', "_")
    encoded_name = quote(raw_name, safe="")

    return f"{disposition}; filename=\"{ascii_fallback}\"; filename*=UTF-8''{encoded_name}"
    
    
def _format_size(size_bytes: Optional[int]) -> str:
    size = int(size_bytes or 0)
    units = ["B", "KB", "MB", "GB", "TB"]
    idx = 0
    value = float(size)
    while value >= 1024 and idx < len(units) - 1:
        value /= 1024.0
        idx += 1
    if idx == 0:
        return f"{int(value)} {units[idx]}"
    return f"{value:.1f} {units[idx]}"


def _safe_unique_path(local_dir: str, original_name: str) -> str:
    """
    Tránh ghi đè file trùng tên.
    """
    _ensure_dir(local_dir)
    base_name, ext = os.path.splitext(original_name or "file")
    candidate = os.path.join(local_dir, original_name)

    if not os.path.exists(candidate):
        return candidate

    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    counter = 1
    while True:
        new_name = f"{base_name}_{stamp}_{counter}{ext}"
        candidate = os.path.join(local_dir, new_name)
        if not os.path.exists(candidate):
            return candidate
        counter += 1


def _save_upload(local_dir: str, up: UploadFile) -> Tuple[str, int, str]:
    """
    Ghi file theo stream an toàn.
    Trả về: (đường_dẫn_tuyệt_đối, kích_thước_bytes, sha256_hex)
    """
    _ensure_dir(local_dir)
    dest = _safe_unique_path(local_dir, up.filename)
    size = 0
    h = hashlib.sha256()
    with open(dest, "wb") as f:
        while True:
            chunk = up.file.read(1024 * 1024)  # 1MB/chunk
            if not chunk:
                break
            size += len(chunk)
            f.write(chunk)
            h.update(chunk)
    up.file.close()
    return dest, size, h.hexdigest()


def _get_primary_unit_id(db: Session, user_id: str) -> Optional[str]:
    """
    Xác định đơn vị chính dùng cho Tab Tài liệu.

    Quy tắc:
    1) Nếu có membership is_primary=True => dùng đơn vị đó.
    2) Nếu không có is_primary:
       - Ưu tiên đơn vị sâu nhất trong cây tổ chức (cap_do lớn nhất),
         tức Tổ (cấp 3) ưu tiên hơn Phòng (cấp 2).
       - Nếu cùng cấp thì lấy bản ghi đầu tiên ổn định theo unit_id.
    """
    membership = (
        db.query(UserUnitMemberships)
        .filter(
            UserUnitMemberships.user_id == user_id,
            UserUnitMemberships.is_primary == True,  # noqa: E712
        )
        .first()
    )
    if membership and membership.unit_id:
        return membership.unit_id

    rows = (
        db.query(UserUnitMemberships, Units)
        .join(Units, Units.id == UserUnitMemberships.unit_id)
        .filter(UserUnitMemberships.user_id == user_id)
        .order_by(Units.cap_do.desc(), Units.id.asc())
        .all()
    )

    if rows:
        membership_obj, _unit_obj = rows[0]
        return membership_obj.unit_id

    return None


def _get_unit(db: Session, unit_id: Optional[str]) -> Optional[Units]:
    if not unit_id:
        return None
    return db.get(Units, unit_id)

def _unit_block_code(unit: Optional[Units]) -> str:
    if not unit:
        return ""
    return str(getattr(getattr(unit, "block_code", None), "value", getattr(unit, "block_code", "")) or "").upper()


def _unit_category(unit: Optional[Units]) -> str:
    if not unit:
        return ""
    return str(getattr(getattr(unit, "unit_category", None), "value", getattr(unit, "unit_category", "")) or "").upper()


def _is_hanh_chinh_unit(unit: Optional[Units]) -> bool:
    return _unit_block_code(unit) == "HANH_CHINH"


def _is_chuyen_mon_unit(unit: Optional[Units]) -> bool:
    return _unit_block_code(unit) == "CHUYEN_MON"


def _is_phong_unit(unit: Optional[Units]) -> bool:
    return _unit_category(unit) == "PHONG"


def _is_khoa_unit(unit: Optional[Units]) -> bool:
    return _unit_category(unit) == "KHOA"


def _is_subunit(unit: Optional[Units]) -> bool:
    return _unit_category(unit) == "SUBUNIT"


def _is_hanh_chinh_subunit(unit: Optional[Units]) -> bool:
    return _is_subunit(unit) and _is_hanh_chinh_unit(unit)


def _is_chuyen_mon_subunit(unit: Optional[Units]) -> bool:
    return _is_subunit(unit) and _is_chuyen_mon_unit(unit)


def _is_room_manager(role_codes: Set[str]) -> bool:
    return ROLE_TRUONG_PHONG in role_codes or ROLE_PHO_PHONG in role_codes


def _is_team_manager(role_codes: Set[str]) -> bool:
    return ROLE_TO_TRUONG in role_codes or ROLE_PHO_TO in role_codes


def _is_khoa_manager(role_codes: Set[str]) -> bool:
    return (
        ROLE_TRUONG_KHOA in role_codes
        or ROLE_PHO_TRUONG_KHOA in role_codes
        or ROLE_DIEU_DUONG_TRUONG in role_codes
        or ROLE_KY_THUAT_VIEN_TRUONG in role_codes
    )


def _is_chuyen_mon_subunit_manager(role_codes: Set[str]) -> bool:
    return (
        ROLE_TRUONG_DON_VI in role_codes
        or ROLE_PHO_DON_VI in role_codes
        or ROLE_DIEU_DUONG_TRUONG_DON_VI in role_codes
        or ROLE_KY_THUAT_VIEN_TRUONG_DON_VI in role_codes
        or ROLE_TRUONG_NHOM in role_codes
        or ROLE_PHO_NHOM in role_codes
    )


def _unit_scope_kind(unit: Optional[Units]) -> str:
    """
    Trả về 1 trong:
    - HANH_CHINH_ROOM
    - HANH_CHINH_TEAM
    - CHUYEN_MON_KHOA
    - CHUYEN_MON_SUBUNIT
    - OTHER
    """
    if not unit:
        return "OTHER"

    if _is_phong_unit(unit) and _is_hanh_chinh_unit(unit):
        return "HANH_CHINH_ROOM"

    if _is_hanh_chinh_subunit(unit):
        return "HANH_CHINH_TEAM"

    if _is_khoa_unit(unit) and _is_chuyen_mon_unit(unit):
        return "CHUYEN_MON_KHOA"

    if _is_chuyen_mon_subunit(unit):
        return "CHUYEN_MON_SUBUNIT"

    return "OTHER"

def _get_direct_child_unit_ids(db: Session, unit_id: Optional[str]) -> List[str]:
    if not unit_id:
        return []
    rows = (
        db.query(Units.id)
        .filter(Units.parent_id == unit_id)
        .all()
    )
    return [r[0] for r in rows]


def _user_primary_units(db: Session, user_id: str) -> List[Units]:
    mems = (
        db.query(UserUnitMemberships)
        .filter(UserUnitMemberships.user_id == user_id)
        .all()
    )
    prims = [m for m in mems if getattr(m, "is_primary", False)]
    ids = [m.unit_id for m in (prims or mems) if getattr(m, "unit_id", None)]
    if not ids:
        return []
    return db.query(Units).filter(Units.id.in_(ids)).all()


def _user_membership_units(db: Session, user_id: str) -> List[Units]:
    """
    Lấy toàn bộ đơn vị mà user đang thuộc (không phụ thuộc is_primary).
    Chỉ lấy đơn vị cấp Phòng (2) và Tổ (3).
    """
    rows = (
        db.query(Units)
        .join(UserUnitMemberships, UserUnitMemberships.unit_id == Units.id)
        .filter(UserUnitMemberships.user_id == user_id)
        .filter(Units.cap_do.in_([2, 3]))
        .all()
    )

    dedup = {}
    for u in rows:
        if u and getattr(u, "id", None):
            dedup[u.id] = u

    return list(dedup.values())
    
    
def _get_uploadable_units(db: Session, user_id: str, role_codes: Set[str]) -> List[Units]:
    """
    Quy tắc combobox Đơn vị tải lên:

    A. Khối hành chính
    - User thuộc Phòng: hiện Phòng
    - User thuộc Tổ/Nhóm hành chính: hiện đúng Tổ/Nhóm đó + Phòng mẹ

    B. Khối chuyên môn
    - User thuộc Khoa: hiện Khoa
    - User thuộc Đơn vị/Nhóm chuyên môn: hiện đúng Đơn vị/Nhóm đó + Khoa mẹ

    Admin / HĐTV: không dùng chức năng này.
    """
    if _is_admin_or_leader(role_codes):
        return []

    member_units = _user_membership_units(db, user_id)
    if not member_units:
        return []

    result = {}

    for u in member_units:
        if not u:
            continue

        scope_kind = _unit_scope_kind(u)

        if scope_kind == "HANH_CHINH_ROOM":
            result[u.id] = u

        elif scope_kind == "HANH_CHINH_TEAM":
            result[u.id] = u
            if u.parent_id:
                parent_unit = _get_unit(db, u.parent_id)
                if parent_unit and _unit_scope_kind(parent_unit) == "HANH_CHINH_ROOM":
                    result[parent_unit.id] = parent_unit

        elif scope_kind == "CHUYEN_MON_KHOA":
            result[u.id] = u

        elif scope_kind == "CHUYEN_MON_SUBUNIT":
            result[u.id] = u
            if u.parent_id:
                parent_unit = _get_unit(db, u.parent_id)
                if parent_unit and _unit_scope_kind(parent_unit) == "CHUYEN_MON_KHOA":
                    result[parent_unit.id] = parent_unit

    return sorted(
        result.values(),
        key=lambda u: (
            getattr(u, "cap_do", 0) or 0,
            getattr(u, "order_index", 0) or 0,
            getattr(u, "ten_don_vi", "") or "",
        )
    )




def _normalize_file_scope(value: str | None) -> str:
    scope = (value or "").strip().upper()
    if scope == FILE_SCOPE_COMMITTEE:
        return FILE_SCOPE_COMMITTEE
    return FILE_SCOPE_UNIT


def _get_file_scope(rec: Files | None) -> str:
    if not rec:
        return FILE_SCOPE_UNIT
    return _normalize_file_scope(getattr(rec, "scope_type", None))


def _get_uploadable_committees(db: Session, user_id: str) -> List[Committees]:
    """
    Ban được phép upload:
    - Admin: toàn bộ Ban đang hoạt động.
    - User thường/BGĐ/HĐTV: chỉ các Ban mà user là thành viên đang hoạt động.
    Chốt nghiệp vụ: tất cả thành viên Ban được upload file.
    """
    if user_is_admin(db, user_id):
        return (
            db.query(Committees)
            .filter(Committees.status == "ACTIVE", Committees.is_active == True)  # noqa: E712
            .order_by(Committees.name.asc())
            .all()
        )

    committee_ids = get_user_committee_ids(db, user_id, active_only=True)
    if not committee_ids:
        return []

    return (
        db.query(Committees)
        .filter(Committees.id.in_(committee_ids))
        .filter(Committees.status == "ACTIVE", Committees.is_active == True)  # noqa: E712
        .order_by(Committees.name.asc())
        .all()
    )


def _can_upload_to_committee(db: Session, user_id: str, committee_id: Optional[str]) -> bool:
    committee_id = (committee_id or "").strip()
    if not committee_id:
        return False

    committee = db.get(Committees, committee_id)
    if not committee:
        return False

    if str(getattr(committee, "status", "") or "").upper() != "ACTIVE":
        return False

    if not bool(getattr(committee, "is_active", False)):
        return False

    if user_is_admin(db, user_id):
        return True

    return user_is_committee_member(db, user_id, committee_id)


def _can_delete_committee_file(db: Session, user_id: str, rec: Files | None) -> bool:
    """
    Quyền xóa file Ban:
    - Admin: được xóa.
    - Trưởng ban/Phó trưởng ban: được xóa trong Ban mình quản lý.
    - Người upload: được xóa file mình upload.
    - Thành viên thường: không xóa file người khác.
    """
    if not rec or not getattr(rec, "committee_id", None):
        return False

    committee_id = str(rec.committee_id)

    if user_is_admin(db, user_id):
        return True

    if getattr(rec, "owner_id", None) == user_id:
        return True

    if user_is_committee_manager(db, user_id, committee_id):
        return True

    return False

def _can_upload_to_unit(db: Session, user_id: str, role_codes: Set[str], target_unit_id: Optional[str]) -> bool:
    if not target_unit_id:
        return False
    allowed_ids = {u.id for u in _get_uploadable_units(db, user_id, role_codes)}
    return target_unit_id in allowed_ids


def _load_role_codes_for_user(db: Session, user_id: str) -> Set[str]:
    rows = (
        db.query(Roles.code)
        .join(UserRoles, UserRoles.role_id == Roles.id)
        .filter(UserRoles.user_id == user_id)
        .all()
    )
    codes: Set[str] = set()
    for (c,) in rows:
        code_up = str(getattr(c, "value", c)).upper() if c is not None else ""
        if code_up:
            codes.add(code_up)
    return codes


def _user_membership_unit_ids(db: Session, user_id: str) -> List[str]:
    rows = (
        db.query(UserUnitMemberships.unit_id)
        .filter(UserUnitMemberships.user_id == user_id)
        .distinct()
        .all()
    )
    return [r[0] for r in rows if r and r[0]]


def _active_visibility_modes(db: Session, user_id: str) -> Set[str]:
    unit_ids = _user_membership_unit_ids(db, user_id)
    if not unit_ids:
        return set()

    now = datetime.utcnow()
    grants = (
        db.query(VisibilityGrants)
        .filter(VisibilityGrants.grantee_unit_id.in_(unit_ids))
        .all()
    )

    modes: Set[str] = set()
    for g in grants:
        if g.effective_from and g.effective_from > now:
            continue
        if g.effective_to and g.effective_to < now:
            continue
        mode_val = getattr(g.mode, "value", g.mode)
        if mode_val:
            modes.add(str(mode_val).upper())
    return modes


def _has_files_visibility_grant(db: Session, user_id: str) -> bool:
    modes = _active_visibility_modes(db, user_id)
    return ("VIEW_ALL" in modes) or ("FILES_ONLY" in modes)

def _is_admin_or_leader(role_codes: Set[str]) -> bool:
    return ROLE_ADMIN in role_codes or ROLE_LANH_DAO in role_codes


def _is_room_manager(role_codes: Set[str]) -> bool:
    return ROLE_TRUONG_PHONG in role_codes or ROLE_PHO_PHONG in role_codes


def _can_user_view_file_by_membership(
    *,
    db: Session,
    user_id: str,
    role_codes: Set[str],
    file_unit_id: Optional[str],
) -> bool:
    """
    Quy tắc nhìn thấy file theo membership thực tế:

    1) Admin / HĐTV: không thấy kho tài liệu đơn vị
    2) Khối hành chính:
       - File PHÒNG: thành viên PHÒNG thấy; thành viên Tổ/Nhóm trực thuộc PHÒNG thấy
       - File Tổ/Nhóm: chỉ thành viên Tổ/Nhóm đó thấy; QL phòng mẹ thấy
    3) Khối chuyên môn:
       - File KHOA: tất cả user thuộc KHOA và các Đơn vị/Nhóm trực thuộc KHOA thấy
       - File Đơn vị/Nhóm chuyên môn: chỉ thành viên đúng Đơn vị/Nhóm đó thấy;
         QL khoa mẹ thấy
    """
    if not file_unit_id:
        return False

    if _is_admin_or_leader(role_codes):
        return False

    file_unit = _get_unit(db, file_unit_id)
    if not file_unit:
        return False

    member_units = _user_membership_units(db, user_id)
    if not member_units:
        return False

    room_ids = set()
    team_ids = set()
    khoa_ids = set()
    subunit_ids = set()

    for u in member_units:
        if not u:
            continue

        kind = _unit_scope_kind(u)

        if kind == "HANH_CHINH_ROOM":
            room_ids.add(u.id)

        elif kind == "HANH_CHINH_TEAM":
            team_ids.add(u.id)
            if getattr(u, "parent_id", None):
                room_ids.add(u.parent_id)

        elif kind == "CHUYEN_MON_KHOA":
            khoa_ids.add(u.id)

        elif kind == "CHUYEN_MON_SUBUNIT":
            subunit_ids.add(u.id)
            if getattr(u, "parent_id", None):
                khoa_ids.add(u.parent_id)

    file_kind = _unit_scope_kind(file_unit)

    # A. File do PHÒNG phát hành
    if file_kind == "HANH_CHINH_ROOM":
        if file_unit.id in room_ids:
            return True
        if any(
            _unit_scope_kind(u) == "HANH_CHINH_TEAM" and getattr(u, "parent_id", None) == file_unit.id
            for u in member_units
        ):
            return True
        return False

    # B. File do TỔ/NHÓM hành chính phát hành
    if file_kind == "HANH_CHINH_TEAM":
        if file_unit.id in team_ids:
            return True
        if file_unit.parent_id in room_ids and _is_room_manager(role_codes):
            return True
        return False

    # C. File do KHOA phát hành
    if file_kind == "CHUYEN_MON_KHOA":
        if file_unit.id in khoa_ids:
            return True
        if any(
            _unit_scope_kind(u) == "CHUYEN_MON_SUBUNIT" and getattr(u, "parent_id", None) == file_unit.id
            for u in member_units
        ):
            return True
        return False

    # D. File do ĐƠN VỊ / NHÓM chuyên môn phát hành
    if file_kind == "CHUYEN_MON_SUBUNIT":
        if file_unit.id in subunit_ids:
            return True
        if file_unit.parent_id in khoa_ids and _is_khoa_manager(role_codes):
            return True
        return False

    return False
    

def _can_delete_file_by_membership(
    *,
    db: Session,
    user_id: str,
    role_codes: Set[str],
    file_unit_id: Optional[str],
) -> bool:
    """
    Quyền xóa chặt:
    - Admin / HĐTV: không xóa
    - File của PHÒNG/KHOA: chỉ user thuộc đúng đơn vị đó mới xóa được
    - File của TỔ/NHÓM hoặc ĐƠN VỊ/NHÓM chuyên môn:
      chỉ user thuộc đúng đơn vị phát hành mới xóa được
    - Không mở rộng quyền xóa cho đơn vị cha
    """
    if not file_unit_id:
        return False

    if _is_admin_or_leader(role_codes):
        return False

    member_units = _user_membership_units(db, user_id)
    if not member_units:
        return False

    member_unit_ids = {
        u.id for u in member_units
        if u and getattr(u, "id", None)
    }

    return file_unit_id in member_unit_ids
    
def _can_user_view_file(
    *,
    db: Session,
    current_unit_id: Optional[str],
    role_codes: Set[str],
    file_unit_id: Optional[str],
) -> bool:
    """
    Logic nhìn thấy file theo đúng yêu cầu:

    1) Admin / HĐTV: không thấy kho phòng/tổ
    2) File do PHÒNG phát hành:
       - user có đơn vị chính là PHÒNG đó thấy
       - user có đơn vị chính là TỔ con trực thuộc PHÒNG đó thấy
    3) File do TỔ phát hành:
       - user có đơn vị chính là TỔ đó thấy
       - user có đơn vị chính là PHÒNG mẹ và có role QL phòng thấy
       - nhân viên thuộc PHÒNG mẹ không thấy
    """
    if not current_unit_id or not file_unit_id:
        return False

    if _is_admin_or_leader(role_codes):
        return False

    current_unit = _get_unit(db, current_unit_id)
    file_unit = _get_unit(db, file_unit_id)

    if not current_unit or not file_unit:
        return False

    # File do PHÒNG phát hành
    if file_unit.cap_do == 2:
        if current_unit_id == file_unit.id:
            return True
        if current_unit.cap_do == 3 and current_unit.parent_id == file_unit.id:
            return True
        return False

    # File do TỔ phát hành
    if file_unit.cap_do == 3:
        if current_unit_id == file_unit.id:
            return True
        if (
            current_unit.cap_do == 2
            and current_unit.id == file_unit.parent_id
            and _is_room_manager(role_codes)
        ):
            return True
        return False

    # Cấp khác: không mở
    return False


def _ensure_view_access(
    rec: Files,
    user_id: str,
    role_codes: Set[str],
    db: Session
) -> None:
    if not rec or rec.is_deleted:
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp.")

    if _get_file_scope(rec) == FILE_SCOPE_COMMITTEE:
        if not getattr(rec, "committee_id", None):
            raise HTTPException(status_code=403, detail="Tài liệu Ban chưa có mã Ban hợp lệ.")
        if not user_can_view_committee(db, user_id, str(rec.committee_id)):
            raise HTTPException(status_code=403, detail="Bạn không có quyền truy cập tài liệu Ban này.")
        return

    if _has_files_visibility_grant(db, user_id):
        return

    if not _can_user_view_file_by_membership(
        db=db,
        user_id=user_id,
        role_codes=role_codes,
        file_unit_id=rec.unit_id,
    ):
        raise HTTPException(status_code=403, detail="Bạn không có quyền truy cập tệp này.")


def _ensure_delete_access(
    rec: Files,
    user_id: str,
    role_codes: Set[str],
    db: Session
) -> None:
    if not rec or rec.is_deleted:
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp.")

    if _get_file_scope(rec) == FILE_SCOPE_COMMITTEE:
        if not _can_delete_committee_file(db, user_id, rec):
            raise HTTPException(status_code=403, detail="Bạn không có quyền xóa tài liệu Ban này.")
        return

    if not _can_delete_file_by_membership(
        db=db,
        user_id=user_id,
        role_codes=role_codes,
        file_unit_id=rec.unit_id,
    ):
        raise HTTPException(status_code=403, detail="Bạn không có quyền xóa tệp này.")


def _parse_positive_int(raw: str, default: int, min_value: int = 1, max_value: int = 1000) -> int:
    try:
        value = int(raw)
    except Exception:
        value = default
    if value < min_value:
        value = min_value
    if value > max_value:
        value = max_value
    return value


def _to_vietnam_datetime(dt: Optional[datetime]) -> Optional[datetime]:
    """
    Chuẩn hóa datetime sang giờ Việt Nam (UTC+7) mà không phụ thuộc tzdata.

    Quy ước:
    - Nếu dt không có tzinfo => coi là UTC rồi cộng 7 giờ.
    - Nếu dt đã có tzinfo => chuyển về UTC rồi cộng 7 giờ.
    """
    if not dt:
        return None

    vn_offset = timedelta(hours=7)

    if dt.tzinfo is None:
        return dt + vn_offset

    dt_utc = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt_utc + vn_offset


def _serialize_file_row(
    rec: Files,
    db: Session,
    user_id: str,
    role_codes: Set[str]
) -> dict:
    owner_name = ""
    if getattr(rec, "owner", None):
        owner_name = rec.owner.full_name or rec.owner.username or ""

    ext = _get_file_ext(rec.original_name)
    uploaded_at_vn = _to_vietnam_datetime(rec.uploaded_at)
    file_kind = _get_file_kind(rec.original_name)
    is_office_file = is_office_previewable(rec.original_name)
    file_scope = _get_file_scope(rec)

    scope_label = "Đơn vị"
    scope_name = ""
    can_delete = _can_delete_file_by_membership(
        db=db,
        user_id=user_id,
        role_codes=role_codes,
        file_unit_id=rec.unit_id,
    )

    if file_scope == FILE_SCOPE_COMMITTEE:
        scope_label = "Ban kiêm nhiệm"
        committee = getattr(rec, "committee", None)
        if committee is None and getattr(rec, "committee_id", None):
            committee = db.get(Committees, rec.committee_id)
        scope_name = getattr(committee, "name", "") or ""
        can_delete = _can_delete_committee_file(db, user_id, rec)
    else:
        unit = getattr(rec, "unit", None)
        if unit is None and getattr(rec, "unit_id", None):
            unit = db.get(Units, rec.unit_id)
        scope_name = getattr(unit, "ten_don_vi", "") or ""

    return {
        "id": rec.id,
        "original_name": rec.original_name,
        "mime_type": rec.mime_type or "",
        "size_bytes": rec.size_bytes or 0,
        "size_display": _format_size(rec.size_bytes),
        "uploaded_at": uploaded_at_vn,
        "uploaded_at_display": uploaded_at_vn.strftime("%d/%m/%Y %H:%M") if uploaded_at_vn else "",
        "owner_name": owner_name,
        "path": rec.path,
        "file_ext": ext[1:].upper() if ext else "",
        "file_kind": file_kind,
        "file_kind_label": _get_file_kind_label(rec.original_name),
        "scope_type": file_scope,
        "scope_label": scope_label,
        "scope_name": scope_name,
        "can_preview_inline": _can_inline_preview(rec.original_name),
        "preview_url": f"/files/preview/{rec.id}" if is_office_file else f"/files/view/{rec.id}",
        "preview_kind": "pdf" if is_office_file else file_kind,
        "can_delete": can_delete,
    }


def _sort_rows(rows: list[dict], sort: str, direction: str) -> list[dict]:
    reverse = direction == "desc"

    if sort == "name":
        return sorted(rows, key=lambda x: (x["original_name"] or "").lower(), reverse=reverse)
    if sort == "type":
        return sorted(rows, key=lambda x: (x["file_kind_label"] or "").lower(), reverse=reverse)
    if sort == "owner":
        return sorted(rows, key=lambda x: (x["owner_name"] or "").lower(), reverse=reverse)
    if sort == "size":
        return sorted(rows, key=lambda x: int(x["size_bytes"] or 0), reverse=reverse)

    return sorted(
        rows,
        key=lambda x: x["uploaded_at"] or datetime.min,
        reverse=reverse
    )


def _paginate_rows(rows: list[dict], page: int, per_page: int) -> tuple[list[dict], int, int]:
    total = len(rows)
    total_pages = max(1, (total + per_page - 1) // per_page)
    if page > total_pages:
        page = total_pages
    start = (page - 1) * per_page
    end = start + per_page
    return rows[start:end], total, total_pages


# =========================
# Routes
# =========================
@router.get("", include_in_schema=False)
def files_home(request: Request, db: Session = Depends(get_db)):
    user = login_required(request, db)
    current_unit_id = _get_primary_unit_id(db, user.id)
    role_codes = _load_role_codes_for_user(db, user.id)
    has_files_grant = _has_files_visibility_grant(db, user.id)
    
    keyword = (request.query_params.get("q") or "").strip()
    kind = (request.query_params.get("kind") or "all").strip().lower()
    sort = (request.query_params.get("sort") or "uploaded_at").strip().lower()
    direction = (request.query_params.get("direction") or "desc").strip().lower()
    page = _parse_positive_int(request.query_params.get("page", "1"), default=1)
    per_page = _parse_positive_int(request.query_params.get("per_page", "10"), default=10, min_value=5, max_value=100)

    if sort not in SORT_FIELDS:
        sort = "uploaded_at"
    if direction not in {"asc", "desc"}:
        direction = "desc"
    if kind not in {"all", "document", "spreadsheet", "pdf", "image", "video"}:
        kind = "all"

    rows: list[dict] = []
    upload_units = _get_uploadable_units(db, user.id, role_codes)
    upload_committees = _get_uploadable_committees(db, user.id)
    allowed_committee_ids = list(allowed_committee_ids_for_user(db, user.id))

    # 1) Tài liệu theo đơn vị: giữ nguyên logic cũ
    if current_unit_id and not _is_admin_or_leader(role_codes):
        unit_query = (
            db.query(Files)
            .filter(Files.is_deleted == False)  # noqa: E712
            .filter(or_(Files.scope_type == FILE_SCOPE_UNIT, Files.scope_type == None))  # noqa: E711
            .order_by(Files.uploaded_at.desc())
        )

        if keyword:
            unit_query = unit_query.filter(func.lower(Files.original_name).like(f"%{keyword.lower()}%"))

        unit_records = unit_query.all()

        if not has_files_grant:
            unit_records = [
                rec for rec in unit_records
                if _can_user_view_file_by_membership(
                    db=db,
                    user_id=user.id,
                    role_codes=role_codes,
                    file_unit_id=rec.unit_id,
                )
            ]

        rows.extend([_serialize_file_row(rec, db, user.id, role_codes) for rec in unit_records])

    # 2) Tài liệu theo Ban kiêm nhiệm
    if allowed_committee_ids:
        committee_query = (
            db.query(Files)
            .filter(Files.is_deleted == False)  # noqa: E712
            .filter(Files.scope_type == FILE_SCOPE_COMMITTEE)
            .filter(Files.committee_id.in_(allowed_committee_ids))
            .order_by(Files.uploaded_at.desc())
        )

        if keyword:
            committee_query = committee_query.filter(func.lower(Files.original_name).like(f"%{keyword.lower()}%"))

        committee_records = committee_query.all()
        rows.extend([_serialize_file_row(rec, db, user.id, role_codes) for rec in committee_records])

    if kind != "all":
        rows = [r for r in rows if r["file_kind"] == kind]

    rows = _sort_rows(rows, sort=sort, direction=direction)

    paged_rows, total_files, total_pages = _paginate_rows(rows, page=page, per_page=per_page)

    return templates.TemplateResponse(
        "files.html",
        {
            "request": request,
            "app_name": getattr(settings, "APP_NAME", "QLCV_App"),
            "company_name": getattr(settings, "COMPANY_NAME", ""),
            "rows": paged_rows,
            "msg": request.query_params.get("msg", ""),
            "error": request.query_params.get("error", ""),
            "q": keyword,
            "kind": kind,
            "sort": sort,
            "direction": direction,
            "page": page,
            "per_page": per_page,
            "total_files": total_files,
            "total_pages": total_pages,
            "accept_types": ",".join(sorted(ALLOWED_EXTENSIONS)),
            "is_hidden_for_admin_or_leader": _is_admin_or_leader(role_codes) and not upload_committees,
            "upload_units": upload_units,
            "upload_committees": upload_committees,
            "current_unit_id": current_unit_id or "",
        }
    )


@router.post("/upload")
async def upload_file(
    request: Request,
    linked_object_type: str = Form("DOC"),
    linked_object_id: str = Form(""),
    scope_type: str = Form(FILE_SCOPE_UNIT),
    unit_id: str = Form(""),
    committee_id: str = Form(""),
    upfile: UploadFile = File(...),
    db: Session = Depends(get_db),
):
    user = login_required(request, db)
    current_unit_id = _get_primary_unit_id(db, user.id)
    role_codes = _load_role_codes_for_user(db, user.id)

    scope_type = _normalize_file_scope(scope_type)

    if scope_type == FILE_SCOPE_UNIT and _is_admin_or_leader(role_codes):
        return RedirectResponse(
            url="/files?error=Admin hoặc HĐTV không sử dụng kho tài liệu phòng/tổ.",
            status_code=302,
        )

    if scope_type == FILE_SCOPE_UNIT and not current_unit_id:
        return RedirectResponse(
            url="/files?error=Tài khoản chưa được gán đơn vị chính, không thể tải tài liệu đơn vị.",
            status_code=302,
        )

    if not upfile or not upfile.filename:
        return RedirectResponse(
            url="/files?error=Chưa chọn tệp để tải lên.",
            status_code=302,
        )

    if not _is_allowed_extension(upfile.filename):
        return RedirectResponse(
            url="/files?error=Định dạng tệp không được hỗ trợ.",
            status_code=302,
        )

    max_bytes = _get_max_file_bytes()

    target_unit_id = ""
    target_committee_id = ""

    if scope_type == FILE_SCOPE_COMMITTEE:
        target_committee_id = (committee_id or "").strip()
        if not _can_upload_to_committee(db, user.id, target_committee_id):
            return RedirectResponse(
                url="/files?error=Bạn không có quyền tải tài liệu lên Ban kiêm nhiệm đã chọn.",
                status_code=302,
            )

        sub_parts = ["COMMITTEE", str(target_committee_id), str(user.id)]

    else:
        target_unit_id = (unit_id or current_unit_id or "").strip()
        if not _can_upload_to_unit(db, user.id, role_codes, target_unit_id):
            return RedirectResponse(
                url="/files?error=Bạn không có quyền tải tài liệu lên đơn vị đã chọn.",
                status_code=302,
            )

        target_unit = _get_unit(db, target_unit_id)
        if not target_unit or _unit_scope_kind(target_unit) == "OTHER":
            return RedirectResponse(
                url="/files?error=Chỉ đơn vị thuộc phạm vi Phòng/Tổ hoặc Khoa/Đơn vị chuyên môn mới được sử dụng kho tài liệu này.",
                status_code=302,
            )

        sub_parts = [str(target_unit_id), str(user.id)]

    if linked_object_type:
        sub_parts.append(linked_object_type.strip())
    if linked_object_id:
        sub_parts.append(linked_object_id.strip())

    dest_dir = os.path.join(_get_upload_dir(), *sub_parts)
    dest, size, _sha = _save_upload(dest_dir, upfile)

    if size > max_bytes:
        try:
            os.remove(dest)
        except Exception:
            pass
        return RedirectResponse(
            url="/files?error=File vượt quá dung lượng cho phép.",
            status_code=302,
        )

    mime_type = _guess_mime(dest)

    rec = Files(
        original_name=upfile.filename,
        path=dest,
        mime_type=mime_type,
        size_bytes=size,
        owner_id=user.id,
        unit_id=target_unit_id or None,
        scope_type=scope_type,
        committee_id=target_committee_id or None,
    )
    db.add(rec)
    db.commit()

    return RedirectResponse(
        url="/files?msg=Tải tệp lên thành công.",
        status_code=302,
    )

@router.get("/preview/{file_id}")
def preview_file(request: Request, file_id: str, db: Session = Depends(get_db)):
    user = login_required(request, db)
    role_codes = _load_role_codes_for_user(db, user.id)

    rec = db.get(Files, file_id)
    _ensure_view_access(rec, user.id, role_codes, db)

    if not rec.path or not os.path.exists(rec.path):
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp.")

    filename = rec.original_name or os.path.basename(rec.path)

    if is_office_previewable(filename):
        try:
            preview_path = ensure_office_pdf_preview(
                source_path=rec.path,
                preview_key=str(rec.id),
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
            headers={"Content-Disposition": _build_content_disposition("inline", preview_filename)}
        )

    if _get_file_kind(filename) in {"pdf", "image", "video"}:
        return FileResponse(
            rec.path,
            media_type=rec.mime_type or _guess_mime(rec.path),
            headers={"Content-Disposition": _build_content_disposition("inline", filename)}
        )

    raise HTTPException(status_code=400, detail="Định dạng tệp này không hỗ trợ xem trước.")


@router.get("/download/{file_id}")
def download_file(request: Request, file_id: str, db: Session = Depends(get_db)):
    user = login_required(request, db)
    current_unit_id = _get_primary_unit_id(db, user.id)
    role_codes = _load_role_codes_for_user(db, user.id)

    rec = db.get(Files, file_id)
    _ensure_view_access(rec, user.id, role_codes, db)

    if not rec.path or not os.path.exists(rec.path):
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp.")

    filename = rec.original_name or os.path.basename(rec.path)
    media_type = rec.mime_type or "application/octet-stream"

    try:
        return FileResponse(
            rec.path,
            media_type=media_type,
            headers={"Content-Disposition": _build_content_disposition("attachment", filename)}
        )
    except TypeError:
        return FileResponse(
            rec.path,
            media_type=media_type,
            headers={"Content-Disposition": _build_content_disposition("attachment", filename)}
        )


@router.get("/view/{file_id}")
def view_file(request: Request, file_id: str, db: Session = Depends(get_db)):
    user = login_required(request, db)
    current_unit_id = _get_primary_unit_id(db, user.id)
    role_codes = _load_role_codes_for_user(db, user.id)

    rec = db.get(Files, file_id)
    _ensure_view_access(rec, user.id, role_codes, db)

    if not rec.path or not os.path.exists(rec.path):
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp.")

    filename = rec.original_name or os.path.basename(rec.path)
    media_type = rec.mime_type or "application/octet-stream"

    return FileResponse(
        rec.path,
        media_type=media_type,
        headers={"Content-Disposition": _build_content_disposition("inline", filename)}
    )


@router.post("/delete/{file_id}")
def delete_file(request: Request, file_id: str, db: Session = Depends(get_db)):
    user = login_required(request, db)
    current_unit_id = _get_primary_unit_id(db, user.id)
    role_codes = _load_role_codes_for_user(db, user.id)

    rec = db.get(Files, file_id)
    _ensure_delete_access(rec, user.id, role_codes, db)

    rec.is_deleted = True
    db.add(rec)
    db.commit()

    try:
        if rec.path and os.path.exists(rec.path):
            os.remove(rec.path)
    except Exception:
        pass

    return RedirectResponse(
        url="/files?msg=Xóa tệp thành công.",
        status_code=302,
    )