# app/routers/inbox.py
# Version: 2025-09-19 (+07) – Add: cờ overdue/due_soon theo NGÀY; cho phép phản hồi của người giao (as_feedback) → status_snapshot='FEEDBACK'
# Mục tiêu:
# - KHÔNG đổi route/URL/HTML/CSS/DB schema/deps.
# - /inbox trả cả "tasks" lẫn "items" để tương thích mọi inbox.html cũ/mới.
# - Báo cáo kèm file hiển thị ở trang người giao (nhờ tasks.html đọc _files/_latest_report_note).
# - "Hoàn thành" (assignee) đặt DONE + completed_at/finished_at (nếu có); KHÔNG đóng.
# - "Kết thúc" (assigner) đặt closed_at/archived_at/deleted_at → ẩn ở cả hai phía.

from __future__ import annotations
from typing import Any, Dict, Iterable, Optional, List
from datetime import datetime
import os
import uuid
import logging
import mimetypes
from urllib.parse import quote

from fastapi import APIRouter, Request, Depends, Form, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse, FileResponse
from sqlalchemy.orm import Session
from sqlalchemy import func, or_, and_

from app.chat.realtime import manager
from app.work_access import get_work_access_flags
from app.office_preview import (
    OfficePreviewError,
    ensure_office_pdf_preview,
    is_office_previewable,
)
logger = logging.getLogger("app.inbox")
router = APIRouter()

# ---------- get_db ----------
def _import_get_db():
    for mod in ("app.database","app.deps","database","deps"):
        try:
            m = __import__(mod, fromlist=["get_db"])
            fn = getattr(m, "get_db", None)
            if fn: return fn
        except Exception:
            continue
    raise ModuleNotFoundError("Không tìm thấy get_db")
get_db = _import_get_db()

# ---------- templates ----------
def _import_templates():
    for mod, attr in (("app.main","templates"),("main","templates")):
        try:
            m = __import__(mod, fromlist=[attr])
            t = getattr(m, attr, None)
            if t is not None:
                return t
        except Exception:
            continue
    from fastapi.templating import Jinja2Templates
    try:
        return Jinja2Templates(directory="app/templates")
    except Exception:
        return Jinja2Templates(directory="templates")
templates = _import_templates()
def _import_settings():
    for mod, attr in (("app.config", "settings"), ("config", "settings")):
        try:
            m = __import__(mod, fromlist=[attr])
            s = getattr(m, attr, None)
            if s is not None:
                return s
        except Exception:
            continue

    class _FallbackSettings:
        APP_NAME = "QLCV_App"
        COMPANY_NAME = ""

    return _FallbackSettings()

settings = _import_settings()
# ---------- models ----------
try:
    import app.models as models
except Exception:
    models = None

def _get_cls(cands: Iterable[str]):
    if not models: return None
    for nm in cands:
        if hasattr(models, nm):
            return getattr(models, nm)
    return None

Users   = _get_cls(["Users","User","Account"])
Tasks   = _get_cls(["Tasks","Task","WorkItem","Job"])
Reports = _get_cls(["TaskReports","TaskReport","Reports","Report"])
Files   = _get_cls(["Files","File","TaskFiles","Attachments","Attachment"])
Committees = _get_cls(["Committees","Committee"])
CommitteeMembers = _get_cls(["CommitteeMembers","CommitteeMember"])

TASK_SCOPE_UNIT = "UNIT"
TASK_SCOPE_COMMITTEE = "COMMITTEE"

# ---------- commons ----------
def _me_id(req: Request) -> Optional[int]:
    sess = getattr(req, "session", {}) or {}
    return sess.get("user_id") or (sess.get("user") or {}).get("id")

def _safe_hasattr(cls, name: str) -> bool:
    try:
        return hasattr(cls, name)
    except Exception:
        return False

def _set_if_exist(obj: Any, name: str, value: Any):
    if _safe_hasattr(obj.__class__, name):
        try:
            setattr(obj, name, value)
            return True
        except Exception:
            return False
    return False

def _task_by_id(db: Session, task_id: Any):
    if not Tasks: return None
    try:
        return db.query(Tasks).filter(getattr(Tasks,"id")==task_id).first()
    except Exception:
        return None

def _is_creator(task: Any, user_id: Any) -> bool:
    for fld in ("created_by","creator_user_id","owner_user_id"):
        v = getattr(task, fld, None)
        if v is not None and str(v) == str(user_id):
            return True
    return False

def _is_assignee(task: Any, user_id: Any) -> bool:
    for fld in ("assignee_id","assigned_user_id","assigned_to_user_id","receiver_user_id","assigned_to_user_id"):
        v = getattr(task, fld, None)
        if v is not None and str(v) == str(user_id):
            return True
    return False




def _normalize_task_scope(value: Any) -> str:
    scope = str(value or "").strip().upper()
    if scope == TASK_SCOPE_COMMITTEE:
        return TASK_SCOPE_COMMITTEE
    return TASK_SCOPE_UNIT


def _task_scope_of(task: Any) -> str:
    return _normalize_task_scope(getattr(task, "scope_type", None))


def _committee_ids_of_user_for_inbox(db: Session, user_id: Any) -> set[Any]:
    if not (CommitteeMembers and Committees and user_id):
        return set()

    try:
        rows = (
            db.query(getattr(CommitteeMembers, "committee_id"))
            .join(Committees, getattr(Committees, "id") == getattr(CommitteeMembers, "committee_id"))
            .filter(getattr(CommitteeMembers, "user_id") == user_id)
            .filter(getattr(CommitteeMembers, "is_active") == True)  # noqa: E712
            .filter(getattr(Committees, "status") == "ACTIVE")
            .filter(getattr(Committees, "is_active") == True)  # noqa: E712
            .all()
        )
        return {row[0] for row in rows if row and row[0]}
    except Exception:
        return set()


def _committee_name(db: Session, committee_id: Any) -> str:
    if not (Committees and committee_id):
        return ""

    try:
        committee = db.get(Committees, committee_id)
        return str(getattr(committee, "name", "") or "")
    except Exception:
        return ""


def _decorate_task_scope(db: Session, task: Any) -> None:
    scope = _task_scope_of(task)
    setattr(task, "_scope_type", scope)

    if scope == TASK_SCOPE_COMMITTEE:
        committee_id = getattr(task, "committee_id", None)
        setattr(task, "_scope_label", "Ban kiêm nhiệm")
        setattr(task, "_scope_name", _committee_name(db, committee_id))
    else:
        setattr(task, "_scope_label", "Đơn vị")
        setattr(task, "_scope_name", "")


def _file_obj_id(file_obj: Any) -> str:
    return str(getattr(file_obj, "id", "") or "")


def _file_obj_name(file_obj: Any) -> str:
    return (
        getattr(file_obj, "original_name", None)
        or getattr(file_obj, "file_name", None)
        or getattr(file_obj, "filename", None)
        or getattr(file_obj, "name", None)
        or "tep_dinh_kem"
    )


def _file_obj_path(file_obj: Any) -> str:
    return (
        getattr(file_obj, "path", None)
        or getattr(file_obj, "storage_path", None)
        or getattr(file_obj, "file_path", None)
        or ""
    )


def _build_content_disposition(disposition: str, filename: str) -> str:
    raw_name = (filename or "file").strip() or "file"

    ascii_fallback = raw_name.encode("ascii", "ignore").decode("ascii").strip()
    if not ascii_fallback:
        ascii_fallback = "file"

    ascii_fallback = ascii_fallback.replace("\\", "_").replace('"', "_")
    encoded_name = quote(raw_name, safe="")

    return f"{disposition}; filename=\"{ascii_fallback}\"; filename*=UTF-8''{encoded_name}"


def _work_file_preview_url(file_id: Any) -> str:
    file_id = str(file_id or "").strip()
    return f"/work-files/{file_id}/preview" if file_id else ""


def _work_file_download_url(file_id: Any) -> str:
    file_id = str(file_id or "").strip()
    return f"/work-files/{file_id}/download" if file_id else ""


def _work_file_dict_from_record(file_obj: Any) -> Dict[str, Any]:
    file_id = _file_obj_id(file_obj)
    name = _file_obj_name(file_obj)
    path = _file_obj_path(file_obj)

    return {
        "id": file_id,
        "name": name,
        "original_name": name,
        "path": path,
        "preview_url": _work_file_preview_url(file_id),
        "download_url": _work_file_download_url(file_id),
    }


def _extract_between_markers(text_value: str, start_marker: str) -> str:
    text_value = (text_value or "").replace("\\", "/")
    marker = start_marker.replace("\\", "/")
    idx = text_value.find(marker)
    if idx < 0:
        return ""

    remain = text_value[idx + len(marker):].strip("/")
    if not remain:
        return ""

    return remain.split("/", 1)[0].strip()


def _task_id_from_work_file(db: Session, file_obj: Any) -> Any:
    if not file_obj:
        return None

    for fld in ("task_id", "related_task_id", "file_task_id"):
        if hasattr(file_obj, fld):
            value = getattr(file_obj, fld, None)
            if value:
                return value

    for fld in ("report_id", "task_report_id", "related_report_id"):
        if hasattr(file_obj, fld):
            report_id = getattr(file_obj, fld, None)
            if report_id and Reports:
                try:
                    report = db.get(Reports, report_id)
                    task_id = getattr(report, "task_id", None) if report else None
                    if task_id:
                        return task_id
                except Exception:
                    pass

    path_value = _file_obj_path(file_obj)
    task_id = _extract_between_markers(path_value, "/TASK/")
    if task_id:
        return task_id

    report_id = _extract_between_markers(path_value, "/TASK_REPORT/")
    if report_id and Reports:
        try:
            report = db.get(Reports, report_id)
            task_id = getattr(report, "task_id", None) if report else None
            if task_id:
                return task_id
        except Exception:
            pass

    return None


def _ensure_work_file_access(db: Session, file_obj: Any, user_id: Any) -> Any:
    if not file_obj:
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp.")

    task_id = _task_id_from_work_file(db, file_obj)
    if not task_id:
        raise HTTPException(status_code=403, detail="Không xác định được công việc liên quan đến tệp này.")

    task = _task_by_id(db, task_id)
    if not task:
        raise HTTPException(status_code=404, detail="Không tìm thấy công việc liên quan đến tệp này.")

    if not (_is_assignee(task, user_id) or _is_creator(task, user_id)):
        raise HTTPException(status_code=403, detail="Bạn không có quyền truy cập tệp này.")

    return task

# ===== Helpers để chuẩn hóa trạng thái (PHỤC VỤ D-1/QUÁ HẠN) =====
def _status_str(x):
    if x is None:
        return ""
    s = getattr(x, "name", None)
    if s:
        return str(s).upper()
    return str(x).upper()

def _is_closed_status(x):
    key = _status_str(x)
    # Nhóm không cần cảnh báo
    return key in {"DONE", "CLOSED", "CANCELLED", "REJECTED"}

def _role_codes_for_user(db: Session, user_id: Any) -> set[str]:
    try:
        import app.models as _models
        Roles = getattr(_models, "Roles", None)
        UserRoles = getattr(_models, "UserRoles", None)
        if not (Roles and UserRoles):
            return set()
        rows = (
            db.query(getattr(Roles, "code"))
            .join(UserRoles, getattr(UserRoles, "role_id") == getattr(Roles, "id"))
            .filter(getattr(UserRoles, "user_id") == user_id)
            .all()
        )
        out = set()
        for (c,) in rows:
            out.add(str(getattr(c, "value", c)).upper())
        return out
    except Exception:
        return set()


def _is_hdtv_codes(codes: set[str]) -> bool:
    return bool({"ROLE_ADMIN", "ROLE_LANH_DAO"} & codes) and not bool(
        {"ROLE_TRUONG_PHONG", "ROLE_PHO_PHONG", "ROLE_TO_TRUONG", "ROLE_PHO_TO", "ROLE_NHAN_VIEN"} & codes
    )

def _is_truong_phong_codes(codes: set[str]) -> bool:
    return "ROLE_TRUONG_PHONG" in codes

def _is_pho_phong_codes(codes: set[str]) -> bool:
    return "ROLE_PHO_PHONG" in codes

def _is_to_truong_codes(codes: set[str]) -> bool:
    return "ROLE_TO_TRUONG" in codes

def _is_pho_to_codes(codes: set[str]) -> bool:
    return "ROLE_PHO_TO" in codes

def _is_nv_codes(codes: set[str]) -> bool:
    return "ROLE_NHAN_VIEN" in codes


_CM_BGD_CODES = {
    "ROLE_GIAM_DOC",
    "ROLE_PHO_GIAM_DOC_TRUC",
    "ROLE_PHO_GIAM_DOC",
}

def _unit_block_code(unit: Any) -> str:
    if not unit:
        return ""
    return str(getattr(getattr(unit, "block_code", None), "value", getattr(unit, "block_code", "")) or "").upper()

def _unit_category_code(unit: Any) -> str:
    if not unit:
        return ""
    return str(getattr(getattr(unit, "unit_category", None), "value", getattr(unit, "unit_category", "")) or "").upper()

def _primary_unit_id_of_user_exact(db: Session, user_id: Any) -> Optional[Any]:
    try:
        import app.models as _models
        UserUnitMemberships = getattr(_models, "UserUnitMemberships", None)
        if not UserUnitMemberships:
            return None
        q = db.query(getattr(UserUnitMemberships, "unit_id")).filter(
            getattr(UserUnitMemberships, "user_id") == user_id
        )
        if hasattr(UserUnitMemberships, "is_primary"):
            q = q.filter(getattr(UserUnitMemberships, "is_primary") == True)  # noqa: E712
        row = q.first()
        return row[0] if row else None
    except Exception:
        return None

def _primary_unit_obj_of_user(db: Session, user_id: Any) -> Optional[Any]:
    try:
        import app.models as _models
        Units = getattr(_models, "Units", None)
        uid = _primary_unit_id_of_user_exact(db, user_id)
        return db.get(Units, uid) if (Units and uid) else None
    except Exception:
        return None

def _parent_unit_obj(db: Session, unit_id: Any) -> Optional[Any]:
    try:
        import app.models as _models
        Units = getattr(_models, "Units", None)
        if not (Units and unit_id):
            return None
        u = db.get(Units, unit_id)
        if not u:
            return None
        pid = getattr(u, "parent_id", None)
        return db.get(Units, pid) if pid else None
    except Exception:
        return None

def _child_unit_ids(db: Session, parent_ids: Iterable[Any]) -> set[Any]:
    try:
        import app.models as _models
        Units = getattr(_models, "Units", None)
        if not Units:
            return set()
        q = db.query(getattr(Units, "id")).filter(getattr(Units, "parent_id").in_(list(parent_ids)))
        return {r[0] for r in q.all()}
    except Exception:
        return set()

def _role_codes_of_user_id(db: Session, user_id: Any) -> set[str]:
    return _role_codes_for_user(db, user_id)

def _primary_user_ids_in_units(db: Session, unit_ids: Iterable[Any]) -> set[Any]:
    try:
        import app.models as _models
        UserUnitMemberships = getattr(_models, "UserUnitMemberships", None)
        if not UserUnitMemberships:
            return set()
        q = db.query(getattr(UserUnitMemberships, "user_id")).filter(
            getattr(UserUnitMemberships, "unit_id").in_(list(unit_ids))
        )
        if hasattr(UserUnitMemberships, "is_primary"):
            q = q.filter(getattr(UserUnitMemberships, "is_primary") == True)  # noqa: E712
        return {r[0] for r in q.distinct().all()}
    except Exception:
        return set()

def _primary_user_ids_by_roles_in_units(db: Session, unit_ids: Iterable[Any], role_codes: Iterable[str]) -> set[Any]:
    try:
        import app.models as _models
        Roles = getattr(_models, "Roles", None)
        UserRoles = getattr(_models, "UserRoles", None)
        UserUnitMemberships = getattr(_models, "UserUnitMemberships", None)
        if not (Roles and UserRoles and UserUnitMemberships):
            return set()
        wanted = [str(c).upper() for c in role_codes]
        q = (
            db.query(getattr(UserRoles, "user_id"))
              .join(Roles, getattr(UserRoles, "role_id") == getattr(Roles, "id"))
              .join(UserUnitMemberships, getattr(UserUnitMemberships, "user_id") == getattr(UserRoles, "user_id"))
              .filter(func.upper(func.coalesce(getattr(Roles, "code"), "")).in_(wanted))
              .filter(getattr(UserUnitMemberships, "unit_id").in_(list(unit_ids)))
        )
        if hasattr(UserUnitMemberships, "is_primary"):
            q = q.filter(getattr(UserUnitMemberships, "is_primary") == True)  # noqa: E712
        return {r[0] for r in q.distinct().all()}
    except Exception:
        return set()



def _user_ids_by_roles_anywhere(db: Session, role_codes: Iterable[str]) -> set[Any]:
    try:
        import app.models as _models
        Roles = getattr(_models, "Roles", None)
        UserRoles = getattr(_models, "UserRoles", None)
        if not (Roles and UserRoles):
            return set()

        wanted = [str(c).upper() for c in role_codes]
        q = (
            db.query(getattr(UserRoles, "user_id"))
            .join(Roles, getattr(UserRoles, "role_id") == getattr(Roles, "id"))
            .filter(func.upper(func.coalesce(getattr(Roles, "code"), "")).in_(wanted))
        )
        return {r[0] for r in q.distinct().all()}
    except Exception:
        return set()

def _is_bgd_codes(codes: set[str]) -> bool:
    return bool(_CM_BGD_CODES & codes)

def _is_truong_khoa_codes(codes: set[str]) -> bool:
    return "ROLE_TRUONG_KHOA" in codes

def _is_pho_khoa_codes(codes: set[str]) -> bool:
    return "ROLE_PHO_TRUONG_KHOA" in codes

def _is_ddt_khoa_codes(codes: set[str]) -> bool:
    return "ROLE_DIEU_DUONG_TRUONG" in codes

def _is_ktvt_khoa_codes(codes: set[str]) -> bool:
    return "ROLE_KY_THUAT_VIEN_TRUONG" in codes

def _is_truong_donvi_codes(codes: set[str]) -> bool:
    return "ROLE_TRUONG_DON_VI" in codes

def _is_pho_donvi_codes(codes: set[str]) -> bool:
    return "ROLE_PHO_DON_VI" in codes

def _is_ddt_donvi_codes(codes: set[str]) -> bool:
    return "ROLE_DIEU_DUONG_TRUONG_DON_VI" in codes

def _is_ktvt_donvi_codes(codes: set[str]) -> bool:
    return "ROLE_KY_THUAT_VIEN_TRUONG_DON_VI" in codes

def _is_truong_nhom_codes(codes: set[str]) -> bool:
    return "ROLE_TRUONG_NHOM" in codes

def _is_pho_nhom_codes(codes: set[str]) -> bool:
    return "ROLE_PHO_NHOM" in codes

def _infer_cm_subunit_kind(db: Session, unit_id: Any) -> str:
    user_ids = _primary_user_ids_in_units(db, [unit_id])
    if not user_ids:
        return ""

    all_codes: set[str] = set()
    for uid in user_ids:
        all_codes |= _role_codes_of_user_id(db, uid)

    if all_codes & {
        "ROLE_TRUONG_DON_VI",
        "ROLE_PHO_DON_VI",
        "ROLE_DIEU_DUONG_TRUONG_DON_VI",
        "ROLE_KY_THUAT_VIEN_TRUONG_DON_VI",
    }:
        return "DONVI"

    if all_codes & {"ROLE_TRUONG_NHOM", "ROLE_PHO_NHOM"}:
        return "NHOM"

    return ""

def _split_cm_child_units(db: Session, khoa_id: Any) -> tuple[set[Any], set[Any]]:
    donvi_ids: set[Any] = set()
    nhom_ids: set[Any] = set()
    for uid in _child_unit_ids(db, [khoa_id]):
        kind = _infer_cm_subunit_kind(db, uid)
        if kind == "DONVI":
            donvi_ids.add(uid)
        elif kind == "NHOM":
            nhom_ids.add(uid)
    return donvi_ids, nhom_ids

def _allowed_creator_ids_for_inbox(db: Session, me_id: Any) -> set[Any]:
    try:
        import app.models as _models
        Roles = getattr(_models, "Roles", None)
        UserRoles = getattr(_models, "UserRoles", None)
        UserUnitMemberships = getattr(_models, "UserUnitMemberships", None)
        Units = getattr(_models, "Units", None)
        if not (Roles and UserRoles and UserUnitMemberships and Units):
            return set()

        my_codes = _role_codes_for_user(db, me_id)
        primary_unit = _primary_unit_obj_of_user(db, me_id)
        primary_block = _unit_block_code(primary_unit)
        primary_cat = _unit_category_code(primary_unit)

        # ===== KHỐI CHUYÊN MÔN =====
        if primary_block == "CHUYEN_MON":
            allowed_creator_ids: set[Any] = set()

            khoa_unit = None
            if primary_unit and primary_cat == "KHOA":
                khoa_unit = primary_unit
            elif primary_unit and primary_cat == "SUBUNIT":
                khoa_unit = _parent_unit_obj(db, getattr(primary_unit, "id", None))

            khoa_id = getattr(khoa_unit, "id", None) if khoa_unit else None
            subunit_id = getattr(primary_unit, "id", None) if (primary_unit and primary_cat == "SUBUNIT") else None

            # Nếu user là BGĐ/HĐTV thì nhận việc từ cấp HĐTV/Admin.
            # Không return sớm, vì user có thể đồng thời là Trưởng khoa và cần nhận việc theo tuyến Trưởng khoa.
            if _is_hdtv_codes(my_codes) or _is_bgd_codes(my_codes):
                allowed_creator_ids.update(
                    _user_ids_by_roles_anywhere(
                        db,
                        ["ROLE_ADMIN", "ROLE_LANH_DAO"]
                    )
                )

            if _is_truong_khoa_codes(my_codes):
                allowed_creator_ids.update(
                    _user_ids_by_roles_anywhere(
                        db,
                        ["ROLE_ADMIN", "ROLE_LANH_DAO", "ROLE_GIAM_DOC", "ROLE_PHO_GIAM_DOC_TRUC", "ROLE_PHO_GIAM_DOC"]
                    )
                )

            if _is_pho_khoa_codes(my_codes):
                allowed_creator_ids.update(
                    _user_ids_by_roles_anywhere(
                        db,
                        ["ROLE_ADMIN", "ROLE_LANH_DAO", "ROLE_GIAM_DOC", "ROLE_PHO_GIAM_DOC_TRUC", "ROLE_PHO_GIAM_DOC"]
                    )
                )
                allowed_creator_ids.update(
                    _primary_user_ids_by_roles_in_units(
                        db,
                        [khoa_id] if khoa_id else [],
                        ["ROLE_TRUONG_KHOA"]
                    )
                )

            if _is_ddt_khoa_codes(my_codes) or _is_ktvt_khoa_codes(my_codes):
                allowed_creator_ids.update(
                    _primary_user_ids_by_roles_in_units(
                        db,
                        [khoa_id] if khoa_id else [],
                        ["ROLE_TRUONG_KHOA", "ROLE_PHO_TRUONG_KHOA"]
                    )
                )

            if _is_truong_donvi_codes(my_codes):
                allowed_creator_ids.update(
                    _user_ids_by_roles_anywhere(
                        db,
                        ["ROLE_ADMIN", "ROLE_LANH_DAO", "ROLE_GIAM_DOC", "ROLE_PHO_GIAM_DOC_TRUC", "ROLE_PHO_GIAM_DOC"]
                    )
                )
                allowed_creator_ids.update(
                    _primary_user_ids_by_roles_in_units(
                        db,
                        [khoa_id] if khoa_id else [],
                        ["ROLE_TRUONG_KHOA", "ROLE_PHO_TRUONG_KHOA"]
                    )
                )

            if _is_pho_donvi_codes(my_codes):
                allowed_creator_ids.update(
                    _user_ids_by_roles_anywhere(
                        db,
                        ["ROLE_ADMIN", "ROLE_LANH_DAO", "ROLE_GIAM_DOC", "ROLE_PHO_GIAM_DOC_TRUC", "ROLE_PHO_GIAM_DOC"]
                    )
                )
                allowed_creator_ids.update(
                    _primary_user_ids_by_roles_in_units(
                        db,
                        [khoa_id] if khoa_id else [],
                        ["ROLE_TRUONG_KHOA", "ROLE_PHO_TRUONG_KHOA"]
                    )
                )
                allowed_creator_ids.update(
                    _primary_user_ids_by_roles_in_units(
                        db,
                        [subunit_id] if subunit_id else [],
                        ["ROLE_TRUONG_DON_VI"]
                    )
                )

            if _is_ddt_donvi_codes(my_codes) or _is_ktvt_donvi_codes(my_codes):
                allowed_creator_ids.update(
                    _primary_user_ids_by_roles_in_units(
                        db,
                        [subunit_id] if subunit_id else [],
                        ["ROLE_TRUONG_DON_VI", "ROLE_PHO_DON_VI"]
                    )
                )

            if _is_truong_nhom_codes(my_codes):
                allowed_creator_ids.update(
                    _primary_user_ids_by_roles_in_units(
                        db,
                        [khoa_id] if khoa_id else [],
                        ["ROLE_TRUONG_KHOA", "ROLE_PHO_TRUONG_KHOA", "ROLE_DIEU_DUONG_TRUONG", "ROLE_KY_THUAT_VIEN_TRUONG"]
                    )
                )

            if _is_pho_nhom_codes(my_codes):
                allowed_creator_ids.update(
                    _primary_user_ids_by_roles_in_units(
                        db,
                        [khoa_id] if khoa_id else [],
                        ["ROLE_TRUONG_KHOA", "ROLE_PHO_TRUONG_KHOA", "ROLE_DIEU_DUONG_TRUONG", "ROLE_KY_THUAT_VIEN_TRUONG"]
                    )
                )
                allowed_creator_ids.update(
                    _primary_user_ids_by_roles_in_units(
                        db,
                        [subunit_id] if subunit_id else [],
                        ["ROLE_TRUONG_NHOM"]
                    )
                )

            if primary_cat == "SUBUNIT":
                kind = _infer_cm_subunit_kind(db, subunit_id)
                if kind == "NHOM":
                    allowed_creator_ids.update(
                        _primary_user_ids_by_roles_in_units(
                            db,
                            [subunit_id],
                            ["ROLE_TRUONG_NHOM", "ROLE_PHO_NHOM"]
                        )
                    )
                if kind == "DONVI":
                    allowed_creator_ids.update(
                        _primary_user_ids_by_roles_in_units(
                            db,
                            [subunit_id],
                            ["ROLE_DIEU_DUONG_TRUONG_DON_VI", "ROLE_KY_THUAT_VIEN_TRUONG_DON_VI"]
                        )
                    )

            if primary_cat == "KHOA":
                allowed_creator_ids.update(
                    _primary_user_ids_by_roles_in_units(
                        db,
                        [khoa_id] if khoa_id else [],
                        ["ROLE_DIEU_DUONG_TRUONG", "ROLE_KY_THUAT_VIEN_TRUONG"]
                    )
                )

            return allowed_creator_ids - {me_id}

        # ===== KHỐI HÀNH CHÍNH (GIỮ NGUYÊN) =====
        unit_rows = db.query(getattr(UserUnitMemberships, "unit_id")).filter(
            getattr(UserUnitMemberships, "user_id") == me_id
        ).all()
        my_unit_ids = {r[0] for r in unit_rows if r and r[0]}

        phong_ids = set()
        to_ids = set()
        for uid in my_unit_ids:
            u = db.get(Units, uid)
            if not u:
                continue
            if getattr(u, "cap_do", None) == 2:
                phong_ids.add(uid)
            elif getattr(u, "cap_do", None) == 3:
                to_ids.add(uid)
                if getattr(u, "parent_id", None):
                    phong_ids.add(getattr(u, "parent_id"))

        def _ids_by_roles(role_codes: list[str], unit_ids: set[Any], expected_cap: Optional[int]) -> set[Any]:
            q = (
                db.query(getattr(UserRoles, "user_id"))
                .join(Roles, getattr(UserRoles, "role_id") == getattr(Roles, "id"))
                .join(UserUnitMemberships, getattr(UserUnitMemberships, "user_id") == getattr(UserRoles, "user_id"))
                .filter(func.upper(func.coalesce(getattr(Roles, "code"), "")).in_([str(x).upper() for x in role_codes]))
            )
            if unit_ids:
                q = q.filter(getattr(UserUnitMemberships, "unit_id").in_(list(unit_ids)))
            if expected_cap is not None:
                q = q.join(Units, getattr(Units, "id") == getattr(UserUnitMemberships, "unit_id")).filter(
                    getattr(Units, "cap_do") == expected_cap
                )
            return {r[0] for r in q.distinct().all()}

        if _is_hdtv_codes(my_codes):
            return set()

        if _is_truong_phong_codes(my_codes):
            return _ids_by_roles(["ROLE_LANH_DAO", "ROLE_ADMIN"], set(), None)

        if _is_pho_phong_codes(my_codes):
            return (
                _ids_by_roles(["ROLE_LANH_DAO", "ROLE_ADMIN"], set(), None)
                |
                _ids_by_roles(["ROLE_TRUONG_PHONG"], phong_ids, 2)
            )

        if _is_to_truong_codes(my_codes):
            return (
                _ids_by_roles(["ROLE_LANH_DAO", "ROLE_ADMIN"], set(), None)
                |
                _ids_by_roles(["ROLE_TRUONG_PHONG", "ROLE_PHO_PHONG"], phong_ids, 2)
            )

        if _is_pho_to_codes(my_codes):
            return (
                _ids_by_roles(["ROLE_LANH_DAO", "ROLE_ADMIN"], set(), None)
                |
                _ids_by_roles(["ROLE_TRUONG_PHONG", "ROLE_PHO_PHONG"], phong_ids, 2)
                |
                _ids_by_roles(["ROLE_TO_TRUONG"], to_ids, 3)
            )

        if _is_nv_codes(my_codes):
            if to_ids:
                return _ids_by_roles(["ROLE_TO_TRUONG", "ROLE_PHO_TO"], to_ids, 3)
            return _ids_by_roles(["ROLE_TRUONG_PHONG", "ROLE_PHO_PHONG"], phong_ids, 2)

        return set()
    except Exception:
        return set()
        
async def _notify_work_users(user_ids: Iterable[str], payload: Dict[str, Any]) -> None:
    """
    Phát sự kiện realtime cho module work qua notify socket hiện có của chat.
    Dùng await trực tiếp để tránh lỗi NoEventLoopError.
    """
    clean_ids: List[str] = []
    for raw_user_id in user_ids:
        uid = str(raw_user_id or "").strip()
        if uid and uid not in clean_ids:
            clean_ids.append(uid)

    if not clean_ids:
        return

    try:
        await manager.notify_users_json(clean_ids, payload)
    except Exception as ex:
        logger.exception("[/inbox] Notify realtime lỗi: %s", ex)
# ========================= INBOX VIEW =========================
@router.get("/inbox", response_class=HTMLResponse)
def inbox_view(request: Request, db: Session = Depends(get_db)):
    me = _me_id(request)
    if me is None:
        return RedirectResponse(url="/login", status_code=307)

    my_codes = _role_codes_for_user(db, me)
    work_flags = get_work_access_flags(
        my_codes,
        is_admin=bool(request.session.get("is_admin")),
        is_admin_or_leader=bool(request.session.get("is_admin_or_leader")),
    )
    if not work_flags.get("show_inbox"):
        return RedirectResponse(url=work_flags.get("default_path", "/plans"), status_code=302)

    rows: List[Any] = []
    try:
        if Tasks:
            # danh sách công việc được giao cho tôi, chưa bị close/archive/delete
            q = db.query(Tasks)
            ass_col = None
            for f in ("assignee_id","assigned_user_id","assigned_to_user_id","receiver_user_id"):
                if _safe_hasattr(Tasks, f):
                    ass_col = getattr(Tasks, f); break
            if ass_col is not None:
                q = q.filter(ass_col == me)
            creator_ids = _allowed_creator_ids_for_inbox(db, me)
            committee_ids = _committee_ids_of_user_for_inbox(db, me)

            scope_col = getattr(Tasks, "scope_type", None)
            committee_col = getattr(Tasks, "committee_id", None)
            created_by_col = getattr(Tasks, "created_by")

            filters = []
            if creator_ids:
                filters.append(created_by_col.in_(list(creator_ids)))

            if scope_col is not None and committee_col is not None and committee_ids:
                filters.append(
                    and_(
                        scope_col == TASK_SCOPE_COMMITTEE,
                        committee_col.in_(list(committee_ids)),
                    )
                )

            if filters:
                q = q.filter(or_(*filters))
            else:
                q = q.filter(getattr(Tasks, "id").is_(None))
            for f in ("closed_at","archived_at","deleted_at"):
                if _safe_hasattr(Tasks, f):
                    q = q.filter(getattr(Tasks, f).is_(None))
            order = getattr(Tasks,"created_at",None) or getattr(Tasks,"id")
            rows = q.order_by(order).all()
    except Exception as ex:
        logger.exception("[/inbox] Query lỗi: %s", ex)
        rows = []

    # Gắn cờ overdue/due_soon THEO NGÀY (D-1; quá hạn thì 'Đã trễ hạn') — LOẠI TRỪ các trạng thái đóng/hoàn tất
    try:
        today = datetime.utcnow().date()
        for t in rows:
            _decorate_task_scope(db, t)
            due = None
            for fld in ("due_date","deadline","han_hoan_thanh"):
                if _safe_hasattr(Tasks, fld):
                    try:
                        due = getattr(t, fld)
                        if due is not None: break
                    except Exception:
                        pass
            overdue = False
            due_soon = False
            try:
                if due is not None:
                    # Ép về date
                    if hasattr(due, "date"):
                        d = due.date()
                    else:
                        s = str(due)
                        d = datetime.strptime(s[:10], "%Y-%m-%d").date() if ("-" in s and len(s) >= 10) else None
                    if d:
                        delta = (d - today).days
                        # CHỈ cảnh báo khi task CHƯA thuộc nhóm hoàn tất/không cần cảnh báo
                        if not _is_closed_status(getattr(t, "status", None)):
                            overdue = (delta < 0)
                            due_soon = (delta == 1)  # D-1
            except Exception:
                pass
            try:
                setattr(t, "overdue", overdue)
                setattr(t, "due_soon", due_soon)
            except Exception:
                pass
    except Exception:
        pass

    # ======= BỔ SUNG: Enrich ghi chú & tệp gần nhất để người nhận thấy ý kiến chỉ đạo =======
    try:
        ids = [getattr(t,"id",None) for t in rows if getattr(t,"id",None) is not None]
        latest: Dict[Any, Any] = {}
        if Reports and ids:
            try:
                reps = db.query(Reports).filter(getattr(Reports, "task_id").in_(ids)).all()
                def _ts(x):
                    return getattr(x, "reported_at", None) or getattr(x, "created_at", None) or getattr(x, "id", 0)
                for r in reps:
                    tid = getattr(r, "task_id", None)
                    if tid is None: 
                        continue
                    if (tid not in latest) or (_ts(r) > _ts(latest[tid])):
                        latest[tid] = r
            except Exception:
                latest = {}

        files_by_task: Dict[Any, List[Dict[str, Any]]] = {tid: [] for tid in ids}
        if Files and ids:
            try:
                qf = db.query(Files)
                col = None
                for c in ("task_id","related_task_id","file_task_id"):
                    if _safe_hasattr(Files, c):
                        col = getattr(Files, c); break
                if col is not None:
                    for f in qf.filter(col.in_(ids)).all():
                        p = getattr(f, "path", None) or getattr(f, "storage_path", None)
                        nm = getattr(f, "original_name", None) or getattr(f, "file_name", None) or getattr(f, "name", None)
                        tid2 = getattr(f, "task_id", None) or getattr(f, "related_task_id", None) or getattr(f, "file_task_id", None)
                        if p and (tid2 in files_by_task):
                            files_by_task[tid2].append(_work_file_dict_from_record(f))
            except Exception:
                pass

        for tid, r in latest.items():
            try:
                fp = getattr(r, "file_path", None) or getattr(r, "path", None) or getattr(r, "storage_path", None)
                nm = getattr(r, "original_name", None) or getattr(r, "file_name", None) or getattr(r, "name", None)
                if fp:
                    files_by_task.setdefault(tid, []).append({
                        "id": "",
                        "path": fp,
                        "name": nm or "tệp",
                        "preview_url": "",
                        "download_url": "",
                    })
            except Exception:
                pass

        for t in rows:
            tid = getattr(t, "id", None)
            r = latest.get(tid)
            note = None
            if r is not None:
                note = getattr(r, "note", None) or getattr(r, "noi_dung", None) or getattr(r, "message", None) or getattr(r, "content", None)
            try:
                setattr(t, "_latest_report_note", note)
                setattr(t, "_files", files_by_task.get(tid, []))
            except Exception:
                pass
    except Exception:
        pass
    # ======= HẾT BỔ SUNG =======

    # ----- LỊCH SỬ BÁO CÁO (đầy đủ) – bổ sung theo yêu cầu -----
    try:
        ids = [getattr(t,"id",None) for t in rows if getattr(t,"id",None) is not None]
        if Reports and ids:
            # tải toàn bộ reports theo từng task, sắp theo thời gian tăng dần
            for t in rows:
                reports_list: List[Dict[str, Any]] = []
                try:
                    q = db.query(Reports).filter(
                        getattr(Reports, "task_id") == getattr(t, "id")
                    ).order_by(
                        getattr(Reports, "reported_at", getattr(Reports, "created_at", None)).asc()
                    )
                    for r in q.all():
                        rid  = getattr(r, "id", None)
                        rnote= getattr(r, "note", "") or getattr(r, "ghi_chu", "") or ""
                        rtime= getattr(r, "reported_at", None) or getattr(r, "created_at", None)
                        ruser= getattr(r, "user_display_name", None) or getattr(r, "user_name", None) or getattr(r, "created_by_name", None)
                        rfiles: List[Dict[str, Any]] = []
                        if Files:
                            try:
                                for colname in ("report_id", "task_report_id", "related_report_id"):
                                    if hasattr(Files, colname):
                                        fq = db.query(Files).filter(getattr(Files, colname) == rid)
                                        rfiles = [
                                            _work_file_dict_from_record(f)
                                            for f in fq.all()
                                        ]
                                        if rfiles:
                                            break
                            except Exception:
                                pass
                        reports_list.append({
                            "id": rid,
                            "note": rnote,
                            "at": rtime,
                            "user": ruser,
                            "files": rfiles,
                        })
                except Exception:
                    reports_list = []
                try:
                    setattr(t, "_reports", reports_list)
                except Exception:
                    pass
    except Exception:
        pass
    # ----- HẾT: LỊCH SỬ BÁO CÁO (đầy đủ) -----

    # Trả cả 'tasks' và 'items' để tương thích mọi phiên bản inbox.html
    ctx = {
        "request": request,
        "tasks": rows,
        "items": rows,
        "app_name": getattr(settings, "APP_NAME", "QLCV_App"),
        "company_name": getattr(settings, "COMPANY_NAME", ""),
    }
    return templates.TemplateResponse("inbox.html", ctx)

@router.get("/inbox/api/nav-badge")
def inbox_nav_badge(request: Request, db: Session = Depends(get_db)):
    me = _me_id(request)
    if me is None:
        return JSONResponse({"ok": False, "inbox_task_count": 0}, status_code=401)

    my_codes = _role_codes_for_user(db, me)
    work_flags = get_work_access_flags(
        my_codes,
        is_admin=bool(request.session.get("is_admin")),
        is_admin_or_leader=bool(request.session.get("is_admin_or_leader")),
    )
    if not work_flags.get("show_inbox"):
        return JSONResponse({"ok": True, "inbox_task_count": 0})

    count = 0
    task_owner_count = 0
    try:
        if Tasks:
            q = db.query(func.count(getattr(Tasks, "id")))
            ass_col = None
            for f in ("assignee_id", "assigned_user_id", "assigned_to_user_id", "receiver_user_id"):
                if _safe_hasattr(Tasks, f):
                    ass_col = getattr(Tasks, f)
                    break

            if ass_col is not None:
                q = q.filter(ass_col == me)
            else:
                q = q.filter(getattr(Tasks, "id").is_(None))

            creator_ids = _allowed_creator_ids_for_inbox(db, me)
            committee_ids = _committee_ids_of_user_for_inbox(db, me)

            scope_col = getattr(Tasks, "scope_type", None)
            committee_col = getattr(Tasks, "committee_id", None)
            created_by_col = getattr(Tasks, "created_by")

            filters = []
            if creator_ids:
                filters.append(created_by_col.in_(list(creator_ids)))

            if scope_col is not None and committee_col is not None and committee_ids:
                filters.append(
                    and_(
                        scope_col == TASK_SCOPE_COMMITTEE,
                        committee_col.in_(list(committee_ids)),
                    )
                )

            if filters:
                q = q.filter(or_(*filters))
            else:
                q = q.filter(getattr(Tasks, "id").is_(None))

            for f in ("closed_at", "archived_at", "deleted_at"):
                if _safe_hasattr(Tasks, f):
                    q = q.filter(getattr(Tasks, f).is_(None))

            count = int(q.scalar() or 0)
    except Exception as ex:
        logger.exception("[/inbox/api/nav-badge] Query lỗi: %s", ex)
        count = 0

    try:
        if Tasks:
            q_owner = db.query(func.count(getattr(Tasks, "id")))
            creator_col = None
            for f in ("created_by", "creator_user_id", "owner_user_id"):
                if _safe_hasattr(Tasks, f):
                    creator_col = getattr(Tasks, f)
                    break

            if creator_col is not None:
                q_owner = q_owner.filter(creator_col == me)
                for f in ("closed_at", "archived_at", "deleted_at"):
                    if _safe_hasattr(Tasks, f):
                        q_owner = q_owner.filter(getattr(Tasks, f).is_(None))
                task_owner_count = int(q_owner.scalar() or 0)
    except Exception as ex:
        logger.exception("[/inbox/api/nav-badge] Query task_owner_count lỗi: %s", ex)
        task_owner_count = 0

    return JSONResponse(
        {
            "ok": True,
            "inbox_task_count": count,
            "task_owner_count": task_owner_count,
        }
    )


# ========================= XEM / TẢI FILE CÔNG VIỆC =========================
@router.get("/work-files/{file_id}/preview")
def preview_work_file(request: Request, file_id: str, db: Session = Depends(get_db)):
    me = _me_id(request)
    if me is None:
        raise HTTPException(status_code=401, detail="Chưa đăng nhập.")

    if not Files:
        raise HTTPException(status_code=404, detail="Hệ thống chưa có bảng tệp.")

    rec = db.get(Files, file_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp.")

    _ensure_work_file_access(db, rec, me)

    file_path = _file_obj_path(rec)
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp trên máy chủ.")

    filename = _file_obj_name(rec)

    if is_office_previewable(filename):
        try:
            preview_path = ensure_office_pdf_preview(
                source_path=file_path,
                preview_key=f"work_{file_id}",
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


@router.get("/work-files/{file_id}/download")
def download_work_file(request: Request, file_id: str, db: Session = Depends(get_db)):
    me = _me_id(request)
    if me is None:
        raise HTTPException(status_code=401, detail="Chưa đăng nhập.")

    if not Files:
        raise HTTPException(status_code=404, detail="Hệ thống chưa có bảng tệp.")

    rec = db.get(Files, file_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp.")

    _ensure_work_file_access(db, rec, me)

    file_path = _file_obj_path(rec)
    if not file_path or not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp trên máy chủ.")

    filename = _file_obj_name(rec)
    media_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

    return FileResponse(
        file_path,
        media_type=media_type,
        headers={"Content-Disposition": _build_content_disposition("attachment", filename)},
    )


@router.post("/work-files/{file_id}/delete")
def delete_work_file(request: Request, file_id: str, db: Session = Depends(get_db)):
    me = _me_id(request)
    if me is None:
        raise HTTPException(status_code=401, detail="Chưa đăng nhập.")

    if not Files:
        raise HTTPException(status_code=404, detail="Hệ thống chưa có bảng tệp.")

    rec = db.get(Files, file_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Không tìm thấy tệp.")

    _ensure_work_file_access(db, rec, me)
    file_path = _file_obj_path(rec)

    try:
        if file_path and os.path.exists(file_path):
            os.remove(file_path)
    except Exception as ex:
        logger.exception("[/work-files/delete] Không xóa được file vật lý %s: %s", file_path, ex)

    try:
        db.delete(rec)
        db.commit()
    except Exception as ex:
        db.rollback()
        logger.exception("[/work-files/delete] Không xóa được bản ghi file %s: %s", file_id, ex)
        raise HTTPException(status_code=500, detail="Không xóa được tệp.")

    referer = request.headers.get("referer") or "/tasks"
    if not (referer.startswith("/tasks") or referer.startswith("/inbox") or referer.startswith(str(request.base_url))):
        referer = "/tasks"
    return RedirectResponse(url=referer, status_code=303)

# ========================= BÁO CÁO + FILE =========================
@router.post("/inbox/{task_id}/report")
async def report_task(
    request: Request,
    task_id: str,
    note: Optional[str] = Form(None),
    as_feedback: Optional[str] = Form(None),                 # BỔ SUNG: phân biệt phản hồi của người giao
    file: Optional[UploadFile] = File(None),                 # GIỮ NGUYÊN tham số cũ
    files: Optional[List[UploadFile]] = File(None),          # BỔ SUNG: khớp <input name="files" multiple>
    db: Session = Depends(get_db),
):
    """Gửi báo cáo / phản hồi: ghi TaskReports (nếu có), lưu file (nếu có), ghi Files (nếu có).
       KHÔNG đổi giao diện & form hiện tại."""
    me = _me_id(request)
    if me is None:
        return RedirectResponse(url="/login", status_code=307)

    t = _task_by_id(db, task_id)
    if not t:
        return RedirectResponse(url="/inbox", status_code=302)
    if not (_is_assignee(t, me) or _is_creator(t, me)):
        return RedirectResponse(url="/inbox", status_code=302)

    is_feedback = False
    try:
        if as_feedback is not None:
            is_feedback = str(as_feedback).strip().lower() in ("1","true","yes","y","on")
    except Exception:
        is_feedback = False

    rep = None
    try:
        if Reports:
            rep = Reports()
            _set_if_exist(rep, "task_id", getattr(t,"id", None))
            _set_if_exist(rep, "reported_by", me)
            _set_if_exist(rep, "note", note)
            _set_if_exist(rep, "reported_at", datetime.utcnow())
            _set_if_exist(rep, "created_at", datetime.utcnow())
            if is_feedback:
                _set_if_exist(rep, "status_snapshot", "FEEDBACK")
                _set_if_exist(rep, "type", "FEEDBACK")  # nếu có cột mềm 'type'
                _set_if_exist(rep, "is_feedback", True) # nếu có cột boolean
            db.add(rep)
            db.flush()  # để có id cho Files
    except Exception as ex:
        logger.exception("[/inbox] Lỗi tạo bản ghi báo cáo trước khi lưu tệp: %s", ex)
        rep = None

    # Gom tất cả upload (đơn/lots) theo đúng tên trường form
    uploads: List[UploadFile] = []
    try:
        if files:
            uploads.extend([u for u in (files or []) if u and getattr(u, "filename", "")])
        if (not uploads) and file and getattr(file, "filename", ""):
            uploads.append(file)
    except Exception as ex:
        logger.exception("[/inbox] Lỗi gom danh sách tệp upload báo cáo: %s", ex)

    # Lưu file nếu có: ĐÚNG pattern để template helper nhận ra: /TASK/{task_id}/REPORTS/
    try:
        default_upload_root = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "instance", "uploads")
        )
        base_root = getattr(settings, "UPLOAD_DIR", "") or default_upload_root
        base_root = os.path.abspath(str(base_root))

        if "hvgl_workspace" in base_root.replace("\\", "/").lower():
            base_root = default_upload_root

        base_dir = os.path.join(base_root, "TASK", str(getattr(t, "id", task_id)), "REPORTS")
        os.makedirs(base_dir, exist_ok=True)

        for up in uploads:
            orig_name = up.filename
            ext = os.path.splitext(orig_name)[1]
            safe_name = f"{uuid.uuid4().hex}{ext or ''}"
            full_path = os.path.join(base_dir, safe_name).replace("\\","/")

            with open(full_path, "wb") as f:
                f.write(await up.read())

            saved_path = full_path.replace("\\","/")

            if rep is not None:
                _set_if_exist(rep, "file_path", saved_path)
                _set_if_exist(rep, "original_name", orig_name)

            if Files:
                rec = Files()
                # Files hiện không có cột task_id/... -> chỉ lưu path + original_name; template sẽ quét theo pattern PATH
                _set_if_exist(rec, "path", saved_path) or _set_if_exist(rec, "storage_path", saved_path)
                _set_if_exist(rec, "original_name", orig_name) or _set_if_exist(rec, "file_name", orig_name)
                _set_if_exist(rec, "created_at", datetime.utcnow())
                _set_if_exist(rec, "created_by", me) or _set_if_exist(rec, "uploader_id", me)
                _set_if_exist(rec, "note", note)
                db.add(rec)

        db.commit()

        try:
            creator_user_id = (
                getattr(t, "created_by", None)
                or getattr(t, "creator_user_id", None)
                or getattr(t, "owner_user_id", None)
            )
            assignee_user_id = (
                getattr(t, "assigned_to_user_id", None)
                or getattr(t, "assigned_user_id", None)
                or getattr(t, "assignee_id", None)
                or getattr(t, "receiver_user_id", None)
            )

            event_type = "task_feedback_sent" if is_feedback else "task_reported"

            payload = {
                "module": "work",
                "type": event_type,
                "task_id": str(getattr(t, "id", "") or ""),
                "from_user_id": str(me or ""),
                "to_user_id": str(creator_user_id or ""),
                "timestamp": datetime.utcnow().isoformat(),
            }

            await _notify_work_users(
                [str(creator_user_id or ""), str(assignee_user_id or ""), str(me or "")],
                payload,
            )
        except Exception as ex:
            logger.exception("[/inbox] Notify realtime sau báo cáo lỗi: %s", ex)

    except Exception as ex:
        logger.exception("[/inbox] Lỗi lưu báo cáo/tệp: %s", ex)
        try: db.rollback()
        except Exception: pass

    # ======= BỔ SUNG: Điều hướng hợp lý sau khi gửi =======
    dest = "/inbox"
    try:
        if is_feedback and _is_creator(t, me):
            dest = "/tasks"
    except Exception:
        pass
    return RedirectResponse(url=dest, status_code=302)
    # ======= HẾT BỔ SUNG =======

# ========================= HOÀN THÀNH (ASSIGNEE) =========================
@router.post("/inbox/{task_id}/complete")
async def complete_task(request: Request, task_id: str, db: Session = Depends(get_db)):
    """Người được giao -> đánh dấu hoàn thành: set completed_at/finished_at (nếu có), status='DONE' (nếu có)."""
    me = _me_id(request)
    if me is None:
        return RedirectResponse(url="/login", status_code=307)

    t = _task_by_id(db, task_id)
    if not t:
        return RedirectResponse(url="/inbox", status_code=302)
    if not _is_assignee(t, me):
        return RedirectResponse(url="/inbox", status_code=302)

    try:
        now_ts = datetime.utcnow()

        # set completed_at/finished_at nếu có
        if not (_set_if_exist(t, "completed_at", now_ts) or _set_if_exist(t, "finished_at", now_ts)):
            pass
        # set status='DONE' nếu có
        _set_if_exist(t, "status", "DONE")
        if _safe_hasattr(Tasks, "updated_at"):
            _set_if_exist(t, "updated_at", now_ts)
        db.add(t)
        db.commit()

        try:
            creator_user_id = (
                getattr(t, "created_by", None)
                or getattr(t, "creator_user_id", None)
                or getattr(t, "owner_user_id", None)
            )
            assignee_user_id = (
                getattr(t, "assigned_to_user_id", None)
                or getattr(t, "assigned_user_id", None)
                or getattr(t, "assignee_id", None)
                or getattr(t, "receiver_user_id", None)
                or me
            )

            payload = {
                "module": "work",
                "type": "task_completed",
                "task_id": str(getattr(t, "id", "") or ""),
                "from_user_id": str(me or ""),
                "to_user_id": str(creator_user_id or ""),
                "timestamp": now_ts.isoformat(),
            }

            await _notify_work_users(
                [str(creator_user_id or ""), str(assignee_user_id or "")],
                payload,
            )
        except Exception as ex:
            logger.exception("[/inbox] Notify realtime sau hoàn thành lỗi: %s", ex)

    except Exception as ex:
        logger.exception("[/inbox] UPDATE lỗi complete: %s", ex)
        try: db.rollback()
        except Exception: pass

    return RedirectResponse(url="/inbox", status_code=302)

# ========================= KẾT THÚC (ASSIGNER) =========================
@router.post("/inbox/{task_id}/close")
async def close_task(request: Request, task_id: str, db: Session = Depends(get_db)):
    """Người giao -> 'Kết thúc': đánh dấu đóng (closed_at/archived_at/deleted_at)."""
    me = _me_id(request)
    if me is None:
        return RedirectResponse(url="/login", status_code=307)

    t = _task_by_id(db, task_id)
    if not t:
        return RedirectResponse(url="/tasks", status_code=302)
    if not _is_creator(t, me):
        return RedirectResponse(url="/tasks", status_code=302)

    try:
        ts = datetime.utcnow()
        if not (_set_if_exist(t, "closed_at", ts) or _set_if_exist(t, "archived_at", ts) or _set_if_exist(t, "deleted_at", ts)):
            _set_if_exist(t, "status", "CLOSED")
        if _safe_hasattr(Tasks, "updated_at"):
            _set_if_exist(t, "updated_at", ts)
        db.add(t)
        db.commit()

        try:
            creator_user_id = (
                getattr(t, "created_by", None)
                or getattr(t, "creator_user_id", None)
                or getattr(t, "owner_user_id", None)
                or me
            )
            assignee_user_id = (
                getattr(t, "assigned_to_user_id", None)
                or getattr(t, "assigned_user_id", None)
                or getattr(t, "assignee_id", None)
                or getattr(t, "receiver_user_id", None)
            )

            payload = {
                "module": "work",
                "type": "task_closed",
                "task_id": str(getattr(t, "id", "") or ""),
                "from_user_id": str(me or ""),
                "to_user_id": str(assignee_user_id or ""),
                "timestamp": ts.isoformat(),
            }

            await _notify_work_users(
                [str(creator_user_id or ""), str(assignee_user_id or "")],
                payload,
            )
        except Exception as ex:
            logger.exception("[/inbox] Notify realtime sau kết thúc lỗi: %s", ex)

    except Exception as ex:
        logger.exception("[/inbox] UPDATE lỗi close: %s", ex)
        try: db.rollback()
        except Exception: pass

    return RedirectResponse(url="/tasks", status_code=302)
