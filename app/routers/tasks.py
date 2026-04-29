# app/routers/tasks.py
# Version: 2025-10-24 (+07) – Lucky chỉnh:
# - Bổ sung cờ D-1 (due_soon) và quá hạn (overdue) CÓ XÉT TRẠNG THÁI (loại trừ DONE/CLOSED/CANCELLED/REJECTED)
# - Nạp đầy đủ lịch sử báo cáo (_reports) để phía NGƯỜI GIAO xem toàn bộ ghi chú
# - Giữ nguyên route/URL/DB/schema/import; không ảnh hưởng dashboard/plans

from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from datetime import datetime, timezone, date
import logging

from fastapi import APIRouter, Request, Depends, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session
from sqlalchemy import text, func, or_

from app.chat.realtime import manager
from app.work_access import get_work_access_flags

logger = logging.getLogger("app.tasks")
router = APIRouter()

# ---------- get_db ----------
def _import_get_db():
    for mod in ("app.database", "app.deps", "database", "deps"):
        try:
            m = __import__(mod, fromlist=["get_db"])
            fn = getattr(m, "get_db", None)
            if fn:
                return fn
        except Exception:
            continue
    raise ModuleNotFoundError("Không tìm thấy get_db")
get_db = _import_get_db()

# ---------- templates ----------
def _import_templates():
    for mod, attr in (("app.main", "templates"), ("main", "templates")):
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
Units   = _get_cls(["Units","Unit"])
Roles   = _get_cls(["Roles","Role"])
UserRoles = _get_cls(["UserRoles","UserRole"])
UserUnitMemberships = _get_cls(["UserUnitMemberships","UserUnits","Memberships"])
Tasks   = _get_cls(["Tasks","Task","WorkItem","Job"])
Files   = _get_cls(["Files","File","TaskFiles","Attachments","Attachment"])
Reports = _get_cls(["TaskReports","TaskReport","Reports","Report"])
Committees = _get_cls(["Committees","Committee"])
CommitteeMembers = _get_cls(["CommitteeMembers","CommitteeMember"])
UserStatus = getattr(models, "UserStatus", None)

TASK_SCOPE_UNIT = "UNIT"
TASK_SCOPE_COMMITTEE = "COMMITTEE"
COMMITTEE_MANAGER_ROLES = {"TRUONG_BAN", "PHO_TRUONG_BAN"}

# ---------- helpers chung ----------
def now_utc() -> datetime:
    return datetime.utcnow().replace(tzinfo=timezone.utc)

def _current_user_id(req: Request) -> Optional[int]:
    sess = getattr(req, "session", {}) or {}
    return sess.get("user_id") or (sess.get("user") or {}).get("id")

def _safe_get(o: Any, fields: Iterable[str]):
    for f in fields:
        if hasattr(o, f):
            try:
                v = getattr(o, f)
                if v is not None:
                    return v
            except Exception:
                pass
    return None


def _work_file_preview_url(file_id: Any) -> str:
    file_id = str(file_id or "").strip()
    return f"/work-files/{file_id}/preview" if file_id else ""


def _work_file_download_url(file_id: Any) -> str:
    file_id = str(file_id or "").strip()
    return f"/work-files/{file_id}/download" if file_id else ""


def _work_file_dict_from_record(file_obj: Any) -> Dict[str, Any]:
    file_id = str(getattr(file_obj, "id", "") or "")
    name = (
        getattr(file_obj, "original_name", None)
        or getattr(file_obj, "file_name", None)
        or getattr(file_obj, "filename", None)
        or getattr(file_obj, "name", None)
        or "tệp"
    )
    path = (
        getattr(file_obj, "path", None)
        or getattr(file_obj, "storage_path", None)
        or getattr(file_obj, "file_path", None)
        or ""
    )

    return {
        "id": file_id,
        "name": name,
        "original_name": name,
        "path": path,
        "preview_url": _work_file_preview_url(file_id),
        "download_url": _work_file_download_url(file_id),
    }



def _normalize_task_scope(value: Any) -> str:
    scope = str(value or "").strip().upper()
    if scope == TASK_SCOPE_COMMITTEE:
        return TASK_SCOPE_COMMITTEE
    return TASK_SCOPE_UNIT


def _task_scope_of(task: Any) -> str:
    return _normalize_task_scope(getattr(task, "scope_type", None))


def _committee_name(db: Session, committee_id: Any) -> str:
    if not Committees or not committee_id:
        return ""
    try:
        row = db.get(Committees, committee_id)
        return str(getattr(row, "name", "") or "")
    except Exception:
        return ""


def _committee_manager_options_for_task(db: Session, user_id: Any) -> List[Any]:
    if not (Committees and CommitteeMembers and user_id):
        return []

    try:
        rows = (
            db.query(Committees)
            .join(CommitteeMembers, getattr(CommitteeMembers, "committee_id") == getattr(Committees, "id"))
            .filter(getattr(CommitteeMembers, "user_id") == user_id)
            .filter(getattr(CommitteeMembers, "is_active") == True)  # noqa: E712
            .filter(getattr(CommitteeMembers, "committee_role").in_(list(COMMITTEE_MANAGER_ROLES)))
            .filter(getattr(Committees, "status") == "ACTIVE")
            .filter(getattr(Committees, "is_active") == True)  # noqa: E712
            .filter(getattr(Committees, "allow_tasks") == True)  # noqa: E712
            .order_by(getattr(Committees, "name").asc())
            .all()
        )
        return rows
    except Exception:
        return []


def _committee_member_users_for_task(db: Session, committee_id: Any, exclude_user_id: Any = None) -> List[Any]:
    if not (Users and CommitteeMembers and committee_id):
        return []

    try:
        q = (
            db.query(Users)
            .join(CommitteeMembers, getattr(CommitteeMembers, "user_id") == getattr(Users, "id"))
            .filter(getattr(CommitteeMembers, "committee_id") == committee_id)
            .filter(getattr(CommitteeMembers, "is_active") == True)  # noqa: E712
        )

        if UserStatus is not None and hasattr(Users, "status"):
            q = q.filter(getattr(Users, "status") == UserStatus.ACTIVE)

        rows = q.order_by(getattr(Users, "full_name").asc(), getattr(Users, "username").asc()).all()
    except Exception:
        rows = []

    result: List[Any] = []
    seen: Set[str] = set()
    for user_obj in rows:
        uid = str(getattr(user_obj, "id", "") or "")
        if not uid or uid in seen:
            continue
        if exclude_user_id is not None and uid == str(exclude_user_id):
            continue
        seen.add(uid)
        result.append(user_obj)

    return result


def _committee_member_ids_for_task(db: Session, committee_id: Any, exclude_user_id: Any = None) -> Set[str]:
    return {
        str(getattr(user_obj, "id", "") or "")
        for user_obj in _committee_member_users_for_task(db, committee_id, exclude_user_id=exclude_user_id)
        if getattr(user_obj, "id", None)
    }


def _can_assign_task_in_committee(db: Session, user_id: Any, committee_id: Any) -> bool:
    if not (Committees and CommitteeMembers and user_id and committee_id):
        return False

    try:
        committee = db.get(Committees, committee_id)
        if not committee:
            return False
        if str(getattr(committee, "status", "") or "").upper() != "ACTIVE":
            return False
        if not bool(getattr(committee, "is_active", False)):
            return False
        if not bool(getattr(committee, "allow_tasks", True)):
            return False

        row = (
            db.query(CommitteeMembers)
            .filter(getattr(CommitteeMembers, "committee_id") == committee_id)
            .filter(getattr(CommitteeMembers, "user_id") == user_id)
            .filter(getattr(CommitteeMembers, "is_active") == True)  # noqa: E712
            .filter(getattr(CommitteeMembers, "committee_role").in_(list(COMMITTEE_MANAGER_ROLES)))
            .first()
        )
        return row is not None
    except Exception:
        return False


def _decorate_task_scope(db: Session, task: Any) -> None:
    scope = _task_scope_of(task)
    setattr(task, "_scope_type", scope)

    if scope == TASK_SCOPE_COMMITTEE:
        committee_id = getattr(task, "committee_id", None)
        setattr(task, "_scope_label", "Ban kiêm nhiệm")
        setattr(task, "_scope_name", _committee_name(db, committee_id))
    else:
        unit_name = ""
        try:
            unit = getattr(task, "unit", None)
            if unit is None and Units and getattr(task, "unit_id", None):
                unit = db.get(Units, getattr(task, "unit_id"))
            unit_name = getattr(unit, "ten_don_vi", "") or ""
        except Exception:
            unit_name = ""
        setattr(task, "_scope_label", "Đơn vị")
        setattr(task, "_scope_name", unit_name)

def _assignee_id_of(t) -> Any:
    return _safe_get(t, ("assignee_id","assigned_user_id","assigned_to_user_id","receiver_user_id"))

def _due_of(t) -> Optional[datetime]:
    return _safe_get(t, ("due_date","deadline","han_hoan_thanh"))

# ===== Trạng thái (phục vụ D-1/Quá hạn) =====
def _status_str(x):
    if x is None:
        return ""
    s = getattr(x, "name", None)
    if s:
        return str(s).upper()
    return str(x).upper()

def _is_closed_status(x) -> bool:
    key = _status_str(x)
    return key in {"DONE", "CLOSED", "CANCELLED", "REJECTED"}

async def _notify_work_users(user_ids: Iterable[str], payload: Dict[str, Any]) -> None:
    """
    Phát sự kiện realtime dùng chung qua notify socket hiện có của chat.
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
        logger.exception("[tasks] Notify realtime lỗi: %s", ex)
        
# ---------- normalize & enrich ----------
def _normalize_status_data(db: Session):
    """Chuẩn hóa status bị lạc (COMPLETED/FINISHED -> DONE)."""
    if not Tasks or not hasattr(Tasks, "__tablename__") or not hasattr(Tasks, "status"):
        return
    try:
        table = getattr(Tasks, "__tablename__")
        db.execute(text(f"UPDATE {table} SET status='DONE' WHERE status IN ('COMPLETED','FINISHED')"))
        db.commit()
    except Exception as ex:
        logger.exception("[tasks] Lỗi chuẩn hóa trạng thái task COMPLETED/FINISHED sang DONE: %s", ex)
        try:
            db.rollback()
        except Exception:
            pass

def _enrich_reports_and_files(db: Session, tasks: List[Any]) -> Dict[Any, Dict[str, Any]]:
    out: Dict[Any, Dict[str, Any]] = {}
    if not tasks or (not Reports and not Files):
        return out
    ids = [getattr(t,"id",None) for t in tasks if getattr(t,"id",None) is not None]
    if not ids:
        return out

    latest: Dict[Any, Any] = {}
    if Reports:
        try:
            rows = db.query(Reports).filter(getattr(Reports,"task_id").in_(ids)).all()
            def ts(r): return _safe_get(r, ("reported_at","created_at")) or getattr(r,"id",0)
            for r in rows:
                tid = getattr(r, "task_id", None)
                if tid is None: 
                    continue
                if (tid not in latest) or (ts(r) > ts(latest[tid])):
                    latest[tid] = r
        except Exception:
            latest = {}

    files_by_task: Dict[Any, List[Dict[str, Any]]] = {tid: [] for tid in ids}
    if Files:
        try:
            q = db.query(Files)
            col = None
            for c in ("task_id","related_task_id","file_task_id"):
                if hasattr(Files, c):
                    col = getattr(Files, c); break
            if col is not None:
                for f in q.filter(col.in_(ids)).all():
                    p = _safe_get(f, ("path","storage_path"))
                    nm = _safe_get(f, ("original_name","file_name","name"))
                    tid2 = getattr(f, "task_id", None) or _safe_get(f, ("related_task_id","file_task_id"))
                    if p and (tid2 in files_by_task):
                        files_by_task[tid2].append(_work_file_dict_from_record(f))
        except Exception as ex:
            logger.exception("[tasks] Lỗi nạp tệp đính kèm theo task: %s", ex)

    for tid, r in latest.items():
        try:
            fp = _safe_get(r, ("file_path","path","storage_path"))
            nm = _safe_get(r, ("original_name","file_name","name"))
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

    for tid in ids:
        note = _safe_get(latest.get(tid) or {}, ("note","noi_dung","message","content"))
        out[tid] = {"latest_note": note, "files": files_by_task.get(tid, [])}
    return out

# ---------- quyền/role & phạm vi đơn vị ----------
def _current_user_from_request(request: Request, db: Session) -> Optional[Any]:
    uid = request.session.get("user_id") if hasattr(request, "session") else None
    if uid and Users:
        try:
            u = db.get(Users, uid)
            if u: return u
        except Exception:
            pass
    uname = request.session.get("username") if hasattr(request, "session") else None
    if uname and Users:
        try:
            u = db.query(Users).filter(getattr(Users, "username")==uname).first()
            if u: return u
        except Exception:
            pass
    return None

def _role_codes_for_user(db: Session, user: Optional[Any]) -> Set[str]:
    if not user or not (Roles and UserRoles):
        return set()
    codes: Set[str] = set()
    try:
        rows = (
            db.query(getattr(Roles,"code"))
              .join(UserRoles, getattr(UserRoles,"role_id")==getattr(Roles,"id"))
              .filter(getattr(UserRoles,"user_id")==getattr(user,"id"))
              .all()
        )
        for (c,) in rows:
            codes.add(str(getattr(c,"value", c)).upper())
    except Exception:
        pass
    return codes

def _is_hdtv(codes: Set[str]) -> bool:
    return bool({"ROLE_ADMIN", "ROLE_LANH_DAO"} & codes) and not bool(
        {"ROLE_TRUONG_PHONG", "ROLE_PHO_PHONG", "ROLE_TO_TRUONG", "ROLE_PHO_TO", "ROLE_NHAN_VIEN"} & codes
    )

def _is_truong_phong(codes: Set[str]) -> bool:
    return "ROLE_TRUONG_PHONG" in codes

def _is_pho_phong(codes: Set[str]) -> bool:
    return "ROLE_PHO_PHONG" in codes

def _is_ql_phong(codes: Set[str]) -> bool:
    return _is_truong_phong(codes) or _is_pho_phong(codes)

def _is_to_truong(codes: Set[str]) -> bool:
    return "ROLE_TO_TRUONG" in codes

def _is_pho_to(codes: Set[str]) -> bool:
    return "ROLE_PHO_TO" in codes

def _is_ql_to(codes: Set[str]) -> bool:
    return _is_to_truong(codes) or _is_pho_to(codes)

def _is_nv(codes: Set[str]) -> bool:
    return "ROLE_NHAN_VIEN" in codes

_CM_BGD_CODES = {
    "ROLE_GIAM_DOC",
    "ROLE_PHO_GIAM_DOC_TRUC",
    "ROLE_PHO_GIAM_DOC",
}

_CM_KHOA_MANAGER_CODES = {
    "ROLE_TRUONG_KHOA",
    "ROLE_PHO_TRUONG_KHOA",
    "ROLE_DIEU_DUONG_TRUONG",
    "ROLE_KY_THUAT_VIEN_TRUONG",
}

_CM_DONVI_MANAGER_CODES = {
    "ROLE_TRUONG_DON_VI",
    "ROLE_PHO_DON_VI",
    "ROLE_DIEU_DUONG_TRUONG_DON_VI",
    "ROLE_KY_THUAT_VIEN_TRUONG_DON_VI",
}

_CM_NHOM_MANAGER_CODES = {
    "ROLE_TRUONG_NHOM",
    "ROLE_PHO_NHOM",
}

_CM_ALL_APPROVER_CODES = (
    _CM_BGD_CODES
    | _CM_KHOA_MANAGER_CODES
    | _CM_DONVI_MANAGER_CODES
    | _CM_NHOM_MANAGER_CODES
    | {"ROLE_ADMIN", "ROLE_LANH_DAO"}
)

def _unit_block_code(unit: Any) -> str:
    if not unit:
        return ""
    return str(getattr(getattr(unit, "block_code", None), "value", getattr(unit, "block_code", "")) or "").upper()

def _unit_category_code(unit: Any) -> str:
    if not unit:
        return ""
    return str(getattr(getattr(unit, "unit_category", None), "value", getattr(unit, "unit_category", "")) or "").upper()

def _primary_unit_obj_of_user(db: Session, user_id: Any) -> Optional[Any]:
    uid = _primary_unit_id_of_user(db, user_id)
    if not uid or not Units:
        return None
    try:
        return db.get(Units, uid)
    except Exception:
        return None

def _parent_unit_obj(db: Session, unit_id: Any) -> Optional[Any]:
    if not unit_id or not Units:
        return None
    try:
        u = db.get(Units, unit_id)
        if not u:
            return None
        pid = getattr(u, "parent_id", None)
        return db.get(Units, pid) if pid else None
    except Exception:
        return None

def _child_unit_ids(db: Session, parent_ids: Iterable[Any]) -> Set[Any]:
    if not Units:
        return set()
    try:
        q = db.query(getattr(Units, "id")).filter(getattr(Units, "parent_id").in_(list(parent_ids)))
        return {r[0] for r in q.all()}
    except Exception:
        return set()

def _role_codes_for_user_id(db: Session, user_id: Any) -> Set[str]:
    if not user_id or not Users:
        return set()
    try:
        user = db.get(Users, user_id)
    except Exception:
        user = None
    return _role_codes_for_user(db, user) if user else set()

def _primary_user_ids_in_units(db: Session, unit_ids: Iterable[Any]) -> Set[Any]:
    if not UserUnitMemberships:
        return set()
    try:
        q = db.query(getattr(UserUnitMemberships, "user_id")).filter(
            getattr(UserUnitMemberships, "unit_id").in_(list(unit_ids))
        )
        if hasattr(UserUnitMemberships, "is_primary"):
            q = q.filter(getattr(UserUnitMemberships, "is_primary") == True)  # noqa: E712
        return {r[0] for r in q.distinct().all()}
    except Exception:
        return set()

def _primary_user_ids_by_roles_in_units(db: Session, unit_ids: Iterable[Any], role_codes: Iterable[str]) -> Set[Any]:
    if not (Roles and UserRoles and UserUnitMemberships):
        return set()
    wanted = [str(c).upper() for c in role_codes]
    try:
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

def _is_bgd(codes: Set[str]) -> bool:
    return bool(_CM_BGD_CODES & codes)

def _is_truong_khoa(codes: Set[str]) -> bool:
    return "ROLE_TRUONG_KHOA" in codes

def _is_pho_khoa(codes: Set[str]) -> bool:
    return "ROLE_PHO_TRUONG_KHOA" in codes

def _is_ddt_khoa(codes: Set[str]) -> bool:
    return "ROLE_DIEU_DUONG_TRUONG" in codes

def _is_ktvt_khoa(codes: Set[str]) -> bool:
    return "ROLE_KY_THUAT_VIEN_TRUONG" in codes

def _is_truong_donvi(codes: Set[str]) -> bool:
    return "ROLE_TRUONG_DON_VI" in codes

def _is_pho_donvi(codes: Set[str]) -> bool:
    return "ROLE_PHO_DON_VI" in codes

def _is_ddt_donvi(codes: Set[str]) -> bool:
    return "ROLE_DIEU_DUONG_TRUONG_DON_VI" in codes

def _is_ktvt_donvi(codes: Set[str]) -> bool:
    return "ROLE_KY_THUAT_VIEN_TRUONG_DON_VI" in codes

def _is_truong_nhom(codes: Set[str]) -> bool:
    return "ROLE_TRUONG_NHOM" in codes

def _is_pho_nhom(codes: Set[str]) -> bool:
    return "ROLE_PHO_NHOM" in codes

def _is_cm_non_approver(codes: Set[str]) -> bool:
    return bool(codes) and not bool(codes & _CM_ALL_APPROVER_CODES)

def _infer_cm_subunit_kind(db: Session, unit_id: Any) -> str:
    user_ids = _primary_user_ids_in_units(db, [unit_id])
    if not user_ids:
        return ""
    codes_all: Set[str] = set()
    for uid in user_ids:
        codes_all |= _role_codes_for_user_id(db, uid)

    if codes_all & {
        "ROLE_TRUONG_DON_VI",
        "ROLE_PHO_DON_VI",
        "ROLE_DIEU_DUONG_TRUONG_DON_VI",
        "ROLE_KY_THUAT_VIEN_TRUONG_DON_VI",
    }:
        return "DONVI"

    if codes_all & {
        "ROLE_TRUONG_NHOM",
        "ROLE_PHO_NHOM",
    }:
        return "NHOM"

    return ""

def _split_cm_child_units(db: Session, khoa_id: Any) -> Tuple[Set[Any], Set[Any]]:
    donvi_ids: Set[Any] = set()
    nhom_ids: Set[Any] = set()
    for uid in _child_unit_ids(db, [khoa_id]):
        kind = _infer_cm_subunit_kind(db, uid)
        if kind == "DONVI":
            donvi_ids.add(uid)
        elif kind == "NHOM":
            nhom_ids.add(uid)
    return donvi_ids, nhom_ids

def _primary_non_approver_ids_in_units(db: Session, unit_ids: Iterable[Any]) -> Set[Any]:
    out: Set[Any] = set()
    for uid in _primary_user_ids_in_units(db, unit_ids):
        codes = _role_codes_for_user_id(db, uid)
        if _is_cm_non_approver(codes):
            out.add(uid)
    return out

def _unit_scope_for_user(db: Session, user: Optional[Any], codes: Set[str]) -> Tuple[Set[str], str]:
    if not user or not (Units and UserUnitMemberships):
        return set(), "Chưa đăng nhập"

    base_ids: Set[str] = set()
    try:
        rows = db.query(getattr(UserUnitMemberships, "unit_id")).filter(
            getattr(UserUnitMemberships, "user_id") == getattr(user, "id")
        ).all()
        base_ids = {r[0] for r in rows}
    except Exception:
        base_ids = set()

    primary_unit = _primary_unit_obj_of_user(db, getattr(user, "id", None))
    primary_block = _unit_block_code(primary_unit)
    primary_cat = _unit_category_code(primary_unit)

    if primary_block == "CHUYEN_MON":
        scope_ids: Set[str] = set()
        labels: List[str] = []

        # BGĐ/HĐTV: có phạm vi toàn khối chuyên môn.
        # Không return sớm để user kiêm Trưởng khoa vẫn cộng thêm phạm vi Khoa của mình.
        if _is_hdtv(codes) or _is_bgd(codes):
            try:
                all_ids = {r[0] for r in db.query(getattr(Units, "id")).all()}
                scope_ids.update(all_ids)
                labels.append("HĐTV / BGĐ")
            except Exception:
                scope_ids.update(base_ids)
                labels.append("HĐTV / BGĐ")

        if primary_unit and primary_cat == "KHOA":
            child_ids = _child_unit_ids(db, [getattr(primary_unit, "id", None)])
            scope_ids.update(({getattr(primary_unit, "id", None)} | child_ids) - {None})
            labels.append("Quản lý cấp khoa")

        if primary_unit and primary_cat == "SUBUNIT":
            scope_ids.update({getattr(primary_unit, "id", None)} - {None})
            labels.append("Đơn vị/Nhóm của bạn")

        if not scope_ids:
            scope_ids.update(base_ids)
            labels.append("Khối chuyên môn")

        return scope_ids, " + ".join(labels)

    if _is_hdtv(codes):
        try:
            all_ids = {r[0] for r in db.query(getattr(Units, "id")).all()}
            return all_ids, "Hội đồng thành viên / Admin (toàn bộ hệ thống)"
        except Exception:
            return base_ids, "Hội đồng thành viên / Admin"

    if _is_ql_phong(codes):
        if not base_ids:
            return set(), "Quản lý cấp phòng (không có đơn vị)"
        try:
            child_ids = {r[0] for r in db.query(getattr(Units, "id")).filter(getattr(Units, "parent_id").in_(list(base_ids))).all()}
        except Exception:
            child_ids = set()
        return set(base_ids) | set(child_ids), "Quản lý cấp phòng (phòng và các tổ trực thuộc)"

    if _is_ql_to(codes):
        return base_ids, "Quản lý cấp tổ (tổ của bạn)"

    return base_ids, "Nhân viên (chỉ xem)"

def _query_users_in_units_by_roles(
    db: Session,
    unit_ids: Iterable[str],
    role_codes: Iterable[str],
    expected_unit_level: Optional[int] = None,
) -> List[Any]:
    if not (Users and Roles and UserRoles and UserUnitMemberships and Units):
        return []
    wanted = [str(c).upper() for c in role_codes]
    try:
        q = (
            db.query(Users)
              .join(UserUnitMemberships, getattr(UserUnitMemberships,"user_id")==getattr(Users,"id"))
              .join(UserRoles, getattr(UserRoles,"user_id")==getattr(Users,"id"))
              .join(Roles, getattr(Roles,"id")==getattr(UserRoles,"role_id"))
              .join(Units, getattr(Units,"id")==getattr(UserUnitMemberships,"unit_id"))
              .filter(getattr(UserUnitMemberships,"unit_id").in_(list(unit_ids)))
              .filter(func.upper(func.coalesce(getattr(Roles,"code"), "")).in_(wanted))
        )
        if UserStatus and hasattr(Users, "status"):
            q = q.filter(getattr(Users,"status")==UserStatus.ACTIVE)
        if expected_unit_level is not None and hasattr(Units, "cap_do"):
            q = q.filter(getattr(Units,"cap_do")==expected_unit_level)

        order_col = (
            (getattr(Users, "full_name", None) or
             getattr(Users, "name", None) or
             getattr(Users, "username", None) or
             getattr(Users, "id"))
        )
        return q.distinct().order_by(order_col).all()
    except Exception:
        return []

# ====== BỔ SUNG: NV trực thuộc PHÒNG (cap_do=2) cho QL PHÒNG ======
def _query_nv_truc_thuoc_phong(db: Session, unit_ids: Iterable[str]) -> List[Any]:
    """
    Lấy NHÂN VIÊN thuộc PHÒNG, không thuộc TỔ.
    Quy ước dữ liệu hiện tại:
    - Nhân viên thuộc phòng: membership chính (is_primary=True) ở đơn vị cấp 2
    - Nhân viên thuộc tổ: membership chính ở đơn vị cấp 3, phòng cha chỉ là membership phụ
    => Vì vậy ở đây phải lọc membership CHÍNH tại PHÒNG.
    """
    if not (Users and Roles and UserRoles and UserUnitMemberships and Units):
        return []

    try:
        wanted = ["ROLE_NHAN_VIEN"]

        q = (
            db.query(Users)
              .join(UserUnitMemberships, getattr(UserUnitMemberships, "user_id") == getattr(Users, "id"))
              .join(Units, getattr(Units, "id") == getattr(UserUnitMemberships, "unit_id"))
              .join(UserRoles, getattr(UserRoles, "user_id") == getattr(Users, "id"))
              .join(Roles, getattr(Roles, "id") == getattr(UserRoles, "role_id"))
              .filter(func.upper(func.coalesce(getattr(Roles, "code"), "")).in_(wanted))
        )

        # Chỉ lấy membership ở đơn vị cấp PHÒNG
        if hasattr(Units, "cap_do"):
            q = q.filter(getattr(Units, "cap_do") == 2)

        q = q.filter(getattr(Units, "id").in_(list(unit_ids)))

        # Chỉ lấy membership CHÍNH của user ở PHÒNG
        if hasattr(UserUnitMemberships, "is_primary"):
            q = q.filter(getattr(UserUnitMemberships, "is_primary") == True)  # noqa: E712

        if UserStatus and hasattr(Users, "status"):
            q = q.filter(getattr(Users, "status") == UserStatus.ACTIVE)

        order_col = (
            (getattr(Users, "full_name", None) or
             getattr(Users, "name", None) or
             getattr(Users, "username", None) or
             getattr(Users, "id"))
        )

        return q.distinct().order_by(order_col).all()

    except Exception:
        return []

def _user_ids_by_roles_in_units(
    db: Session,
    role_codes: Iterable[str],
    unit_ids: Optional[Iterable[Any]] = None,
    expected_unit_level: Optional[int] = None,
) -> Set[Any]:
    """
    Trả về tập user_id theo role trong phạm vi đơn vị.
    - role_codes: danh sách mã role, ví dụ ["ROLE_PHO_PHONG"]
    - unit_ids: phạm vi đơn vị cần lọc
    - expected_unit_level:
        2 = Phòng
        3 = Tổ
        None = không ép cấp đơn vị
    """
    if not (Users and Roles and UserRoles and UserUnitMemberships):
        return set()

    wanted = [str(c).upper() for c in role_codes]

    try:
        q = (
            db.query(getattr(UserRoles, "user_id"))
              .join(Roles, getattr(UserRoles, "role_id") == getattr(Roles, "id"))
              .filter(func.upper(func.coalesce(getattr(Roles, "code"), "")).in_(wanted))
        )

        if unit_ids:
            q = (
                q.join(
                    UserUnitMemberships,
                    getattr(UserUnitMemberships, "user_id") == getattr(UserRoles, "user_id")
                )
                .filter(getattr(UserUnitMemberships, "unit_id").in_(list(unit_ids)))
            )

            if expected_unit_level is not None and Units and hasattr(Units, "cap_do"):
                q = (
                    q.join(Units, getattr(Units, "id") == getattr(UserUnitMemberships, "unit_id"))
                     .filter(getattr(Units, "cap_do") == expected_unit_level)
                )

        return {r[0] for r in q.distinct().all()}
    except Exception:
        return set()
        
def _primary_unit_id_of_user(db: Session, user_id: Any) -> Optional[Any]:
    if not UserUnitMemberships:
        return None
    try:
        q = db.query(getattr(UserUnitMemberships, "unit_id")).filter(
            getattr(UserUnitMemberships, "user_id") == user_id
        )
        if hasattr(UserUnitMemberships, "is_primary"):
            q = q.order_by(getattr(UserUnitMemberships, "is_primary").desc())
        row = q.first()
        return row[0] if row else None
    except Exception:
        return None


def _child_to_ids_of_phong(db: Session, phong_ids: Iterable[Any]) -> Set[Any]:
    if not Units:
        return set()
    try:
        q = db.query(getattr(Units, "id")).filter(getattr(Units, "parent_id").in_(list(phong_ids)))
        if hasattr(Units, "cap_do"):
            q = q.filter(getattr(Units, "cap_do") == 3)
        return {r[0] for r in q.all()}
    except Exception:
        return set()


def _recipient_ids_for_assign(db: Session, me: Any, codes: Set[str], unit_scope: Set[str]) -> Set[Any]:
    me_id = getattr(me, "id", None) if me else None
    if me_id is None:
        return set()

    primary_unit = _primary_unit_obj_of_user(db, me_id)
    primary_block = _unit_block_code(primary_unit)
    primary_cat = _unit_category_code(primary_unit)

    # ===== KHỐI CHUYÊN MÔN =====
    if primary_block == "CHUYEN_MON":
        recipient_ids: Set[Any] = set()

        # BGĐ/HĐTV: giao cho Trưởng/Phó khoa và Trưởng/Phó đơn vị.
        # Không return sớm để user kiêm Trưởng khoa vẫn cộng thêm danh sách cấp Khoa.
        if _is_hdtv(codes) or _is_bgd(codes):
            recipient_ids.update(
                _primary_user_ids_by_roles_in_units(db, unit_scope, ["ROLE_TRUONG_KHOA", "ROLE_PHO_TRUONG_KHOA"])
            )
            recipient_ids.update(
                _primary_user_ids_by_roles_in_units(db, unit_scope, ["ROLE_TRUONG_DON_VI", "ROLE_PHO_DON_VI"])
            )

        khoa_unit = None
        if primary_unit and primary_cat == "KHOA":
            khoa_unit = primary_unit
        elif primary_unit and primary_cat == "SUBUNIT":
            khoa_unit = _parent_unit_obj(db, getattr(primary_unit, "id", None))

        khoa_id = getattr(khoa_unit, "id", None) if khoa_unit else None
        donvi_ids, nhom_ids = _split_cm_child_units(db, khoa_id) if khoa_id else (set(), set())

        # Trưởng khoa
        if _is_truong_khoa(codes) and khoa_id:
            recipient_ids.update(
                _primary_user_ids_by_roles_in_units(
                    db,
                    [khoa_id],
                    ["ROLE_PHO_TRUONG_KHOA", "ROLE_DIEU_DUONG_TRUONG", "ROLE_KY_THUAT_VIEN_TRUONG"]
                )
            )
            recipient_ids.update(
                _primary_user_ids_by_roles_in_units(db, donvi_ids, ["ROLE_TRUONG_DON_VI", "ROLE_PHO_DON_VI"])
            )
            recipient_ids.update(
                _primary_user_ids_by_roles_in_units(db, nhom_ids, ["ROLE_TRUONG_NHOM", "ROLE_PHO_NHOM"])
            )

        # Phó khoa
        if _is_pho_khoa(codes) and khoa_id:
            recipient_ids.update(
                _primary_user_ids_by_roles_in_units(
                    db,
                    [khoa_id],
                    ["ROLE_DIEU_DUONG_TRUONG", "ROLE_KY_THUAT_VIEN_TRUONG"]
                )
            )
            recipient_ids.update(
                _primary_user_ids_by_roles_in_units(db, donvi_ids, ["ROLE_TRUONG_DON_VI", "ROLE_PHO_DON_VI"])
            )
            recipient_ids.update(
                _primary_user_ids_by_roles_in_units(db, nhom_ids, ["ROLE_TRUONG_NHOM", "ROLE_PHO_NHOM"])
            )

        # Điều dưỡng trưởng / KTV trưởng cấp khoa
        if (_is_ddt_khoa(codes) or _is_ktvt_khoa(codes)) and khoa_id:
            recipient_ids.update(_primary_non_approver_ids_in_units(db, [khoa_id]))
            recipient_ids.update(
                _primary_user_ids_by_roles_in_units(db, nhom_ids, ["ROLE_TRUONG_NHOM", "ROLE_PHO_NHOM"])
            )

        # Trưởng đơn vị
        if _is_truong_donvi(codes) and primary_unit:
            uid = getattr(primary_unit, "id", None)
            recipient_ids.update(
                _primary_user_ids_by_roles_in_units(
                    db,
                    [uid],
                    ["ROLE_PHO_DON_VI", "ROLE_DIEU_DUONG_TRUONG_DON_VI", "ROLE_KY_THUAT_VIEN_TRUONG_DON_VI"]
                )
            )

        # Phó đơn vị
        if _is_pho_donvi(codes) and primary_unit:
            uid = getattr(primary_unit, "id", None)
            recipient_ids.update(
                _primary_user_ids_by_roles_in_units(
                    db,
                    [uid],
                    ["ROLE_DIEU_DUONG_TRUONG_DON_VI", "ROLE_KY_THUAT_VIEN_TRUONG_DON_VI"]
                )
            )

        # Điều dưỡng trưởng / KTV trưởng đơn vị
        if (_is_ddt_donvi(codes) or _is_ktvt_donvi(codes)) and primary_unit:
            uid = getattr(primary_unit, "id", None)
            recipient_ids.update(_primary_non_approver_ids_in_units(db, [uid]))

        # Trưởng nhóm
        if _is_truong_nhom(codes) and primary_unit:
            uid = getattr(primary_unit, "id", None)
            recipient_ids.update(_primary_user_ids_by_roles_in_units(db, [uid], ["ROLE_PHO_NHOM"]))
            recipient_ids.update(_primary_non_approver_ids_in_units(db, [uid]))

        # Phó nhóm
        if _is_pho_nhom(codes) and primary_unit:
            uid = getattr(primary_unit, "id", None)
            recipient_ids.update(_primary_non_approver_ids_in_units(db, [uid]))

        return recipient_ids - {me_id}

    # ===== KHỐI HÀNH CHÍNH (GIỮ NGUYÊN) =====
    if _is_hdtv(codes):
        return (
            _user_ids_by_roles_in_units(db, ["ROLE_TRUONG_PHONG", "ROLE_PHO_PHONG"], None, expected_unit_level=2)
            |
            _user_ids_by_roles_in_units(db, ["ROLE_TO_TRUONG", "ROLE_PHO_TO"], None, expected_unit_level=3)
        ) - {me_id}

    if _is_truong_phong(codes):
        phong_ids = set()
        for uid in unit_scope:
            if uid is None:
                continue
            u = db.get(Units, uid) if Units else None
            if u and getattr(u, "cap_do", None) == 2:
                phong_ids.add(uid)

        to_ids = _child_to_ids_of_phong(db, phong_ids)

        pho_phong_ids = _user_ids_by_roles_in_units(
            db,
            ["ROLE_PHO_PHONG"],
            phong_ids,
            expected_unit_level=2
        )

        nv_phong_users = _query_nv_truc_thuoc_phong(db, phong_ids)
        nv_phong_ids = {
            getattr(u, "id", None)
            for u in nv_phong_users
            if getattr(u, "id", None) is not None
        }

        ql_to_ids = _user_ids_by_roles_in_units(
            db,
            ["ROLE_TO_TRUONG", "ROLE_PHO_TO"],
            to_ids,
            expected_unit_level=3
        )

        return (pho_phong_ids | nv_phong_ids | ql_to_ids) - {me_id}

    if _is_pho_phong(codes):
        phong_ids = set()
        for uid in unit_scope:
            if uid is None:
                continue
            u = db.get(Units, uid) if Units else None
            if u and getattr(u, "cap_do", None) == 2:
                phong_ids.add(uid)

        to_ids = _child_to_ids_of_phong(db, phong_ids)

        nv_phong_users = _query_nv_truc_thuoc_phong(db, phong_ids)
        nv_phong_ids = {
            getattr(u, "id", None)
            for u in nv_phong_users
            if getattr(u, "id", None) is not None
        }

        ql_to_ids = _user_ids_by_roles_in_units(
            db,
            ["ROLE_TO_TRUONG", "ROLE_PHO_TO"],
            to_ids,
            expected_unit_level=3
        )

        return (nv_phong_ids | ql_to_ids) - {me_id}

    if _is_to_truong(codes):
        pho_to_ids = _user_ids_by_roles_in_units(db, ["ROLE_PHO_TO"], unit_scope, expected_unit_level=3)
        nv_to_ids = _user_ids_by_roles_in_units(db, ["ROLE_NHAN_VIEN"], unit_scope, expected_unit_level=3)
        return (pho_to_ids | nv_to_ids) - {me_id}

    if _is_pho_to(codes):
        nv_to_ids = _user_ids_by_roles_in_units(db, ["ROLE_NHAN_VIEN"], unit_scope, expected_unit_level=3)
        return nv_to_ids - {me_id}

    return set()
    
def _compute_assignment_context(db: Session, user: Optional[Any], unit_scope: Set[str], scope_label: str, codes: Set[str]) -> Dict[str, Any]:
    primary_unit = _primary_unit_obj_of_user(db, getattr(user, "id", None) if user else None)
    primary_block = _unit_block_code(primary_unit)

    committee_task_options = _committee_manager_options_for_task(
        db,
        getattr(user, "id", None) if user else None,
    )

    committee_task_recipients: List[Dict[str, Any]] = []
    for committee in committee_task_options:
        members_payload = []
        for member in _committee_member_users_for_task(
            db,
            getattr(committee, "id", None),
            exclude_user_id=getattr(user, "id", None) if user else None,
        ):
            members_payload.append({
                "id": getattr(member, "id", ""),
                "label": (
                    getattr(member, "full_name", None)
                    or getattr(member, "username", None)
                    or str(getattr(member, "id", "") or "")
                ),
                "username": getattr(member, "username", "") or "",
            })

        committee_task_recipients.append({
            "id": getattr(committee, "id", ""),
            "name": getattr(committee, "name", "") or "",
            "members": members_payload,
        })

    ctx: Dict[str, Any] = {
        "can_assign": False,
        "scope_units": list(unit_scope),
        "scope_label": scope_label,
        "recipients_truongpho": [],
        "recipients_to_nv": [],
        "recipients_nv": [],
        "committee_task_options": committee_task_options,
        "committee_task_recipients": committee_task_recipients,
        "has_committee_task_scope": bool(committee_task_recipients),
        "label_group_a": "Cấp quản lý chính",
        "label_group_b": "Cấp quản lý trực tiếp",
        "label_group_c": "Nhân sự thực hiện",
    }
    if primary_block == "CHUYEN_MON":
        ctx["label_group_a"] = "Trưởng/Phó khoa, Trưởng/Phó đơn vị"
        ctx["label_group_b"] = "Điều dưỡng/KTV trưởng, Trưởng/Phó nhóm"
        ctx["label_group_c"] = "Nhân sự thực hiện"

    if not user:
        return ctx

    recipient_ids = _recipient_ids_for_assign(db, user, codes, unit_scope)
    if not recipient_ids:
        if ctx.get("has_committee_task_scope"):
            ctx["can_assign"] = True
        return ctx

    ctx["can_assign"] = True

    users = []
    try:
        users = db.query(Users).filter(getattr(Users, "id").in_(list(recipient_ids))).all()
    except Exception:
        users = []

    group_a = []
    group_b = []
    group_c = []

    for u in users:
        ucodes = _role_codes_for_user(db, u)

        if primary_block == "CHUYEN_MON":
            if _is_truong_khoa(ucodes) or _is_pho_khoa(ucodes) or _is_truong_donvi(ucodes) or _is_pho_donvi(ucodes):
                group_a.append(u)
            elif (
                _is_ddt_khoa(ucodes) or _is_ktvt_khoa(ucodes) or
                _is_ddt_donvi(ucodes) or _is_ktvt_donvi(ucodes) or
                _is_truong_nhom(ucodes) or _is_pho_nhom(ucodes)
            ):
                group_b.append(u)
            else:
                group_c.append(u)
        else:
            if _is_truong_phong(ucodes) or _is_pho_phong(ucodes):
                group_a.append(u)
            elif _is_to_truong(ucodes) or _is_pho_to(ucodes):
                group_b.append(u)
            elif _is_nv(ucodes):
                primary_uid = _primary_unit_id_of_user(db, getattr(u, "id", None))
                unit_obj = db.get(Units, primary_uid) if (Units and primary_uid) else None
                if unit_obj and getattr(unit_obj, "cap_do", None) == 3:
                    group_b.append(u)
                else:
                    group_c.append(u)

    def _sort_users(items: List[Any]) -> List[Any]:
        return sorted(
            items,
            key=lambda x: (
                (getattr(x, "full_name", None) or getattr(x, "username", None) or "").strip().lower(),
                str(getattr(x, "id", "")),
            )
        )

    ctx["recipients_truongpho"] = _sort_users(group_a)
    ctx["recipients_to_nv"] = _sort_users(group_b)
    ctx["recipients_nv"] = _sort_users(group_c)
    return ctx
# ========================= ROUTES =========================
@router.get("/tasks", response_class=HTMLResponse)
def tasks_list(request: Request, db: Session = Depends(get_db)):
    me_id = _current_user_id(request)
    if me_id is None:
        return RedirectResponse(url="/login", status_code=307)

    _normalize_status_data(db)

    me = _current_user_from_request(request, db)
    codes = _role_codes_for_user(db, me)

    work_flags = get_work_access_flags(
        codes,
        is_admin=bool(request.session.get("is_admin")),
        is_admin_or_leader=bool(request.session.get("is_admin_or_leader")),
    )

    committee_task_options = _committee_manager_options_for_task(db, getattr(me, "id", None) if me else None)
    if not work_flags.get("show_tasks") and not committee_task_options:
        return RedirectResponse(url=work_flags.get("default_path", "/plans"), status_code=302)

    unit_scope, scope_label = _unit_scope_for_user(db, me, codes)
    assign_ctx = _compute_assignment_context(db, me, unit_scope, scope_label, codes)

    rows: List[Any] = []
    try:
        if Tasks:
            q = db.query(Tasks)
            for fld in ("closed_at","archived_at","deleted_at"):
                if hasattr(Tasks, fld):
                    q = q.filter(getattr(Tasks, fld).is_(None))
            creator = None
            for f in ("created_by","creator_user_id","owner_user_id"):
                if hasattr(Tasks, f):
                    creator = getattr(Tasks, f); break
            if creator is not None:
                q = q.filter(creator == me_id)
            order = getattr(Tasks,"created_at",None) or getattr(Tasks,"id")
            rows = q.order_by(order).all()
    except Exception as ex:
        logger.exception("[/tasks] Query lỗi: %s", ex)
        rows = []

    # ===== Tên người nhận =====
    assignee_names: Dict[Any,str] = {}
    try:
        if Users:
            ids = list({ _assignee_id_of(t) for t in rows if _assignee_id_of(t) is not None })
            if ids:
                for u in db.query(Users).filter(getattr(Users,"id").in_(ids)).all():
                    name = _safe_get(u, ("full_name","display_name","username","name","email")) or "-"
                    assignee_names[getattr(u,"id",None)] = name
    except Exception:
        pass

    # ===== Ghi chú gần nhất + files (giữ nguyên) =====
    info = _enrich_reports_and_files(db, rows)

    # ===== Lịch sử báo cáo đầy đủ (bổ sung) =====
    reports_map: Dict[Any, List[Dict[str, Any]]] = {}
    try:
        if Reports and rows:
            task_ids = [getattr(t,"id",None) for t in rows if getattr(t,"id",None) is not None]
            if task_ids:
                q = db.query(Reports).filter(getattr(Reports, "task_id").in_(task_ids))
                # sắp theo thời gian tăng dần để đọc mạch lạc
                q = q.order_by(getattr(Reports, "reported_at", getattr(Reports, "created_at", None)).asc())
                for r in q.all():
                    tid = getattr(r, "task_id", None)
                    if tid is None:
                        continue
                    rec = {
                        "id": getattr(r, "id", None),
                        "note": _safe_get(r, ("note","ghi_chu")) or "",
                        "at": _safe_get(r, ("reported_at","created_at")),
                        "user": _safe_get(r, ("user_display_name","user_name","created_by_name")),
                        "files": [],  # có thể gắn nếu có bảng Files liên kết theo report_id; để trống nếu không có
                    }
                    reports_map.setdefault(tid, []).append(rec)
    except Exception:
        reports_map = {}

    # ===== Cờ D-1/Quá hạn theo trạng thái (bổ sung loại trừ) =====
    today = datetime.utcnow().date()
    for t in rows:
        _decorate_task_scope(db, t)
        setattr(t, "_assignee_name", assignee_names.get(_assignee_id_of(t)))
        ii = info.get(getattr(t,"id",None), {})
        setattr(t, "_latest_report_note", ii.get("latest_note"))
        setattr(t, "_files", ii.get("files", []))
        # Lịch sử đầy đủ
        setattr(t, "_reports", reports_map.get(getattr(t,"id",None), []))

        due = _due_of(t)
        overdue = False; due_soon = False
        try:
            if due is not None:
                d = due.date() if hasattr(due,"date") else None
                if not d and isinstance(due, str) and "-" in due:
                    d = datetime.strptime(due[:10], "%Y-%m-%d").date()
                if d:
                    # CHỈ cảnh báo khi chưa thuộc nhóm hoàn tất/không cần cảnh báo
                    if not _is_closed_status(_safe_get(t, ("status","trang_thai"))):
                        delta = (d - today).days
                        overdue = (delta < 0)
                        due_soon = (delta == 1)
        except Exception:
            pass
        setattr(t, "_overdue", overdue)
        setattr(t, "_due_soon", due_soon)

    ctx = {
        "request": request,
        "tasks": rows,
        "app_name": getattr(settings, "APP_NAME", "QLCV_App"),
        "company_name": getattr(settings, "COMPANY_NAME", ""),
        **assign_ctx,
    }
    return templates.TemplateResponse("tasks.html", ctx)

@router.post("/tasks/assign")
async def assign_task(
    request: Request,
    title: str = Form(...),
    assignee_id: str = Form(...),
    due_date: Optional[str] = Form(None),
    content: Optional[str] = Form(None),
    scope_type: str = Form(TASK_SCOPE_UNIT),
    committee_id: str = Form(""),
    db: Session = Depends(get_db),
):
    """Giữ nguyên hành vi; giao cho người cụ thể; không đổi form/route."""
    me = _current_user_from_request(request, db)
    if me is None:
        return RedirectResponse(url="/login", status_code=307)

    codes = _role_codes_for_user(db, me)

    work_flags = get_work_access_flags(
        codes,
        is_admin=bool(request.session.get("is_admin")),
        is_admin_or_leader=bool(request.session.get("is_admin_or_leader")),
    )

    clean_scope_type = _normalize_task_scope(scope_type)
    clean_committee_id = str(committee_id or "").strip()

    if clean_scope_type == TASK_SCOPE_COMMITTEE:
        if not _can_assign_task_in_committee(db, getattr(me, "id", None), clean_committee_id):
            logger.warning(
                "[assign] Từ chối giao việc Ban ngoài phạm vi. user=%s committee=%s",
                getattr(me, "id", None),
                clean_committee_id,
            )
            return RedirectResponse(url="/tasks", status_code=302)

        allowed_recipient_ids = _committee_member_ids_for_task(
            db,
            clean_committee_id,
            exclude_user_id=getattr(me, "id", None),
        )
    else:
        clean_committee_id = ""
        if not work_flags.get("show_tasks"):
            return RedirectResponse(url=work_flags.get("default_path", "/plans"), status_code=302)

        unit_scope, _ = _unit_scope_for_user(db, me, codes)
        allowed_recipient_ids = _recipient_ids_for_assign(db, me, codes, unit_scope)

    if assignee_id not in {str(x) for x in allowed_recipient_ids}:
        logger.warning("[assign] Từ chối giao việc ngoài phạm vi. user=%s assignee=%s", getattr(me, "id", None), assignee_id)
        return RedirectResponse(url="/tasks", status_code=302)
        
    def _first_unit_id_of_user(user_id: Any) -> Optional[Any]:
        return _primary_unit_id_of_user(db, user_id)

    unit_id = None
    if me:
        unit_id = _first_unit_id_of_user(getattr(me,"id",None))
    if unit_id is None:
        unit_id = _first_unit_id_of_user(assignee_id)

    due_dt = None
    if due_date:
        try:
            due_dt = datetime.strptime(due_date, "%Y-%m-%d")
        except Exception:
            due_dt = None

    try:
        TasksCls = Tasks
        if TasksCls is None and models is not None:
            for name in ("Tasks","Task","WorkItem","Job"):
                if hasattr(models, name):
                    TasksCls = getattr(models, name); break
        if TasksCls is None:
            logger.warning("[assign] Không tìm thấy lớp Tasks – bỏ qua ghi DB.")
            return RedirectResponse(url="/tasks", status_code=302)

        t = TasksCls()

        if unit_id is not None and hasattr(t, "unit_id"):
            t.unit_id = unit_id
        if hasattr(t, "scope_type"):
            t.scope_type = clean_scope_type
        if hasattr(t, "committee_id"):
            t.committee_id = clean_committee_id or None
        if hasattr(t, "title"): t.title = title
        elif hasattr(t, "name"): t.name = title

        if content:
            if hasattr(t, "description"): t.description = content
            elif hasattr(t, "content"):   t.content = content

        if due_dt is not None:
            for fld in ("due_date","deadline"):
                if hasattr(t, fld): setattr(t, fld, due_dt); break

        if me:
            for fld in ("created_by","creator_user_id","owner_user_id"):
                if hasattr(t, fld): setattr(t, fld, getattr(me,"id",None)); break

        for fld in ("assigned_to_user_id","assigned_user_id","assignee_id","receiver_user_id"):
            if hasattr(t, fld): setattr(t, fld, assignee_id); break

        now = datetime.utcnow()
        for fld in ("assigned_at","received_at","created_at","created"):
            if hasattr(t, fld) and getattr(t, fld, None) in (None, ""):
                setattr(t, fld, now)

        db.add(t)
        db.commit()

        try:
            task_id = getattr(t, "id", None)
            creator_id = getattr(me, "id", None) if me else None
            assignee_user_id = (
                getattr(t, "assigned_to_user_id", None)
                or getattr(t, "assigned_user_id", None)
                or getattr(t, "assignee_id", None)
                or getattr(t, "receiver_user_id", None)
                or assignee_id
            )

            payload = {
                "module": "work",
                "type": "task_assigned",
                "task_id": str(task_id or ""),
                "from_user_id": str(creator_id or ""),
                "to_user_id": str(assignee_user_id or ""),
                "title": title or "",
                "scope_type": clean_scope_type,
                "committee_id": clean_committee_id or "",
                "timestamp": datetime.utcnow().isoformat(),
            }

            await _notify_work_users(
                [str(assignee_user_id or ""), str(creator_id or "")],
                payload,
            )
        except Exception as ex:
            logger.exception("[assign] Notify realtime lỗi: %s", ex)

    except Exception as ex:
        logger.exception("[assign] Lỗi ghi DB: %s", ex)
        try:
            db.rollback()
        except Exception:
            pass

    return RedirectResponse(url="/tasks", status_code=302)
