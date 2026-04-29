# app/routers/dashboard.py
# KPI & Biểu đồ:
#   - Nhóm loại trừ nhau: DONE, OVERDUE, IN_PROGRESS (TOTAL = DONE + OVERDUE + IN_PROGRESS)
#   - Xu hướng 12 tháng: tôn trọng f_from/f_to (nếu có), nếu không thì mặc định 12 tháng gần nhất.
#   - Trend dùng trend_q xuất phát từ assign_q/inbox_q (đã áp bộ lọc chung) + ràng vai trò/đơn vị/assignee.

from __future__ import annotations
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple
from datetime import date, datetime, timedelta
import logging

from fastapi import APIRouter, Request, Depends, Query
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session
from sqlalchemy import func, or_

logger = logging.getLogger("app.dashboard")
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

Users   = _get_cls(["Users","User"])
Units   = _get_cls(["Units","Unit"])
Roles   = _get_cls(["Roles","Role"])
UserRoles = _get_cls(["UserRoles","UserRole"])
UserUnitMemberships = _get_cls(["UserUnitMemberships","UserUnits","Memberships"])
Tasks   = _get_cls(["Tasks","Task","WorkItem","Job"])
TaskStatus = getattr(models, "TaskStatus", None)

# ---------- helpers session/roles ----------
def _get_session_roles(request: Request) -> List[str]:
    r0 = getattr(request, "session", {}).get("roles")
    if isinstance(r0, str):
        return [s.strip() for s in r0.replace("|", ",").replace(";", ",").split(",") if s.strip()]
    return list(r0 or [])

def _roles_flat(roles: Iterable[str]) -> str:
    return "|" + "|".join([str(r).strip() for r in roles]) + "|"

def _user_id(request: Request) -> Optional[int]:
    sess = getattr(request, "session", {}) or {}
    uid = sess.get("user_id")
    if uid is not None:
        try:
            return int(uid)
        except Exception:
            return uid
    user_obj = sess.get("user") or {}
    return user_obj.get("id")

def _is_ql_phong(flat: str) -> bool:
    return ("|ROLE_TRUONG_PHONG|" in flat) or ("|ROLE_PHO_PHONG|" in flat)

def _is_ql_to(flat: str) -> bool:
    return ("|ROLE_TO_TRUONG|" in flat) or ("|ROLE_PHO_TO|" in flat)

def _is_nv(flat: str) -> bool:
    return ("|ROLE_NHAN_VIEN|" in flat)

def _is_board(flat: str, sess: dict) -> bool:
    is_admin_or_leader = sess.get("is_admin_or_leader")
    return (
        ("|ROLE_LANH_DAO|" in flat)
        and (not _is_ql_phong(flat))
        and (not _is_ql_to(flat))
        and (not _is_nv(flat))
    ) or (
        is_admin_or_leader
        and (not _is_ql_phong(flat))
        and (not _is_ql_to(flat))
        and (not _is_nv(flat))
    )

def _status_value(st: Any) -> str:
    if st is None:
        return ""
    try:
        return getattr(st, "value")
    except Exception:
        return str(st)

# ---------- roles/unit helpers ----------
def _user_ids_by_roles_in_units(db: Session, role_codes: Iterable[str], unit_ids: Optional[Iterable[Any]]=None, expected_unit_level: Optional[int]=None) -> Set[Any]:
    if not (Users and Roles and UserRoles):
        return set()
    wanted = [str(c).upper() for c in role_codes]
    try:
        q = (
            db.query(getattr(UserRoles,"user_id"))
              .join(Roles, getattr(UserRoles,"role_id")==getattr(Roles,"id"))
        ).filter(func.upper(func.coalesce(getattr(Roles,"code"),"")).in_(wanted))
        if unit_ids and UserUnitMemberships:
            q = q.join(UserUnitMemberships, getattr(UserUnitMemberships,"user_id")==getattr(UserRoles,"user_id"))\
                 .filter(getattr(UserUnitMemberships,"unit_id").in_(list(unit_ids)))
            if expected_unit_level is not None and Units and hasattr(Units, "cap_do"):
                q = q.join(Units, getattr(Units,"id")==getattr(UserUnitMemberships,"unit_id"))\
                     .filter(getattr(Units,"cap_do")==expected_unit_level)
        return {r[0] for r in q.distinct().all()}
    except Exception:
        return set()

def _unit_name_column():
    if Units is None:
        return None
    if hasattr(Units, "ten_don_vi"):
        return getattr(Units, "ten_don_vi")
    if hasattr(Units, "name"):
        return getattr(Units, "name")
    return None

def _unit_scope_for_user(db: Session, uid: Any, flat: str, sess: dict) -> Tuple[Set[Any], str, Set[Any], Set[Any], Set[Any], Set[Any]]:
    unit_ids: Set[Any] = set()
    label = "Phạm vi xem"
    if _is_board(flat, sess) or ("|ROLE_ADMIN|" in flat) or ("|ROLE_LANH_DAO|" in flat):
        if Units:
            try:
                all_ids = [r[0] for r in db.query(Units.id).all()]
                unit_ids = set(all_ids)
                label = "Hội đồng thành viên/Lãnh đạo (toàn viện)"
            except Exception:
                unit_ids = set()
    else:
        mem_cls = None
        for nm in ("UserUnitMemberships", "UserUnits", "Memberships"):
            if hasattr(models, nm):
                mem_cls = getattr(models, nm); break
        base_ids: Set[Any] = set()
        if mem_cls and hasattr(mem_cls, "unit_id") and hasattr(mem_cls, "user_id"):
            try:
                base_ids = {r[0] for r in db.query(mem_cls.unit_id).filter(mem_cls.user_id==uid).all()}
            except Exception:
                base_ids = set()
        unit_ids = set(base_ids)
        if _is_ql_phong(flat) and Units:
            try:
                child_q = db.query(Units.id)
                if hasattr(Units,"parent_id"):
                    child_q = child_q.filter(Units.parent_id.in_(list(base_ids)))
                if hasattr(Units,"cap_do"):
                    child_q = child_q.filter(Units.cap_do==3)
                unit_ids |= {r[0] for r in child_q.all()}
                label = "Quản lý cấp phòng (phòng & tổ trực thuộc)"
            except Exception:
                label = "Quản lý cấp phòng"
        elif _is_ql_to(flat):
            label = "Quản lý cấp tổ (tổ của bạn)"
        else:
            label = "Nhân viên / phạm vi hạn chế"

    board_ids   = _user_ids_by_roles_in_units(db, ["ROLE_LANH_DAO","ROLE_ADMIN"], None, None)
    qlphong_ids = _user_ids_by_roles_in_units(db, ["ROLE_TRUONG_PHONG","ROLE_PHO_PHONG"], unit_ids, expected_unit_level=2)
    qlto_ids    = _user_ids_by_roles_in_units(db, ["ROLE_TO_TRUONG","ROLE_PHO_TO"], unit_ids, expected_unit_level=3)
    nv_ids      = _user_ids_by_roles_in_units(db, ["ROLE_NHAN_VIEN"], unit_ids, expected_unit_level=3) | \
                  _user_ids_by_roles_in_units(db, ["ROLE_NHAN_VIEN"], unit_ids, expected_unit_level=2)

    return unit_ids, label, board_ids, qlphong_ids, qlto_ids, nv_ids

# ---------- bộ lọc ----------
VN_STATUS_LABELS = {
    "NEW": "Chưa bắt đầu",
    "IN_PROGRESS": "Đang thực hiện",
    "SUBMITTED": "Đã nộp",
    "DONE": "Hoàn thành",
    "CLOSED": "Đã đóng",
    "REJECTED": "Bị từ chối",
    "CANCELLED": "Đã hủy",
}

def _apply_filters(q, f_from: Optional[date], f_to: Optional[date], f_unit: Optional[Any], f_status: Optional[str], f_assignee: Optional[Any], *, ignore_unit: bool=False):
    if (not ignore_unit) and f_unit is not None and f_unit != "" and hasattr(Tasks,"unit_id"):
        q = q.filter(getattr(Tasks,"unit_id")==f_unit)
    if f_from:
        if hasattr(Tasks,"due_date"):
            q = q.filter(or_(getattr(Tasks,"due_date").is_(None), getattr(Tasks,"due_date")>=f_from))
        elif hasattr(Tasks,"created_at"):
            q = q.filter(getattr(Tasks,"created_at")>=f_from)
    if f_to:
        if hasattr(Tasks,"due_date"):
            q = q.filter(or_(getattr(Tasks,"due_date").is_(None), getattr(Tasks,"due_date")<=f_to))
        elif hasattr(Tasks,"created_at"):
            q = q.filter(getattr(Tasks,"created_at")<=f_to)
    if f_status and hasattr(Tasks,"status"):
        q = q.filter(func.upper(func.coalesce(getattr(Tasks,"status"),""))==f_status.upper())
    if f_assignee and hasattr(Tasks,"assigned_to_user_id"):
        q = q.filter(getattr(Tasks,"assigned_to_user_id")==f_assignee)
    return q

# ---------- phân loại exclusive ----------
def _normalize_date(d: Any) -> Optional[date]:
    if d is None: return None
    if isinstance(d, datetime): return d.date()
    if isinstance(d, date): return d
    try:
        return datetime.fromisoformat(str(d)).date()
    except Exception:
        return None

def _classify_exclusive(status: Any, due: Any, *, today_d: date) -> str:
    sv = _status_value(status).upper()
    if sv in ("DONE", "CLOSED"):
        return "DONE"
    if sv in ("CANCELLED", "REJECTED"):
        return "OTHER"
    due_d = _normalize_date(due)
    if due_d is not None and due_d < today_d:
        return "OVERDUE"
    return "IN_PROGRESS"

def _accumulate(rows: Iterable[Tuple[Any, Any]], *, today_d: date) -> Dict[str, int]:
    out = {"IN_PROGRESS": 0, "DONE": 0, "OVERDUE": 0, "OTHER": 0}
    for st, due in rows:
        cat = _classify_exclusive(st, due, today_d=today_d)
        out[cat] += 1
    return out

# ---------- tiện ích xu hướng ----------
def _month_start(d: date) -> date:
    return date(d.year, d.month, 1)

def _month_end(d: date) -> date:
    # ngày cuối tháng
    if d.month == 12:
        return date(d.year, 12, 31)
    return date(d.year, d.month+1, 1) - timedelta(days=1)

def _build_month_labels(start_d: date, end_d: date) -> List[str]:
    """Tạo nhãn YYYY-MM từ start đến end (bao gồm), tối đa 24 nhãn để an toàn."""
    labels: List[str] = []
    cur = date(start_d.year, start_d.month, 1)
    cap = 24
    while cur <= end_d and len(labels) < cap:
        labels.append(cur.strftime("%Y-%m"))
        if cur.month == 12:
            cur = date(cur.year+1, 1, 1)
        else:
            cur = date(cur.year, cur.month+1, 1)
    return labels

def _assignees_in_unit_cap_phong(db: Session, unit_id: Any) -> Set[Any]:
    """Trả về tập user_id là thành viên (thường là QL phòng) thuộc đơn vị cấp phòng unit_id."""
    if not (UserUnitMemberships and Units):
        return set()
    q = (db.query(getattr(UserUnitMemberships, "user_id"))
           .join(Units, getattr(Units, "id") == getattr(UserUnitMemberships, "unit_id"))
           .filter(Units.cap_do == 2, getattr(UserUnitMemberships, "unit_id") == unit_id))
    return {r[0] for r in q.distinct().all()}

def _assignees_in_unit_cap_to(db: Session, unit_id: Any) -> Set[Any]:
    """Trả về tập user_id là thành viên thuộc đơn vị cấp tổ unit_id."""
    if not (UserUnitMemberships and Units):
        return set()
    q = (db.query(getattr(UserUnitMemberships, "user_id"))
           .join(Units, getattr(Units, "id") == getattr(UserUnitMemberships, "unit_id"))
           .filter(Units.cap_do == 3, getattr(UserUnitMemberships, "unit_id") == unit_id))
    return {r[0] for r in q.distinct().all()}

# ---------- xây phân bố giao việc (exclusive counts) ----------
def _build_assign_categories(
    db: Session,
    base_query,
    flat: str,
    sess: dict,
    unit_scope: Set[Any],
    uid: Any,
    qlphong_ids: Set[Any],
    qlto_ids: Set[Any],
    f_unit: Optional[Any],
    f_assignee: Optional[Any],
) -> Tuple[List[str], List[Dict[str,int]]]:
    labels: List[str] = []
    counts: List[Dict[str,int]] = []
    today_d = date.today()

    # ===== HĐTV: nhóm theo PHÒNG của assignee (cap_do=2) =====
    if _is_board(flat, sess) and Units:
        rows = base_query.with_entities(
            getattr(Tasks,"assigned_to_user_id"),
            getattr(Tasks,"status"),
            getattr(Tasks,"due_date")
        ).all()
        if not rows:
            return labels, counts

        # map user -> unit cấp phòng
        assignees: Set[Any] = {r[0] for r in rows if r[0] is not None}
        user_room: Dict[Any, Any] = {}
        if UserUnitMemberships and hasattr(Units,"cap_do"):
            q = db.query(getattr(UserUnitMemberships,"user_id"), getattr(UserUnitMemberships,"unit_id"))\
                  .join(Units, getattr(Units,"id")==getattr(UserUnitMemberships,"unit_id"))\
                  .filter(getattr(Units,"cap_do")==2)
            if assignees:
                q = q.filter(getattr(UserUnitMemberships,"user_id").in_(list(assignees)))
            for uid2, unit_id in q.all():
                user_room.setdefault(uid2, unit_id)

        # gom theo phòng
        bucket: Dict[Any, List[Tuple[Any,Any]]] = {}
        for assignee_id, st, due in rows:
            room_id = user_room.get(assignee_id)
            if room_id is None:
                continue
            if f_unit and str(f_unit) != str(room_id):
                continue
            bucket.setdefault(room_id, []).append((st, due))

        if not bucket:
            return labels, counts

        # tên phòng
        name_col = _unit_name_column()
        name_map: Dict[Any,str] = {}
        qn = db.query(Units.id)
        if name_col is not None: qn = qn.add_columns(name_col)
        qn = qn.filter(Units.id.in_(list(bucket.keys())))
        for r in qn.all():
            uid2 = r[0]; nm = r[1] if len(r)>1 else None
            name_map[uid2] = nm if isinstance(nm, str) else str(uid2)

        for room_id in sorted(bucket.keys(), key=lambda x: str(x)):
            labels.append(name_map.get(room_id, f"Phòng #{room_id}"))
            counts.append(_accumulate(bucket[room_id], today_d=today_d))
        return labels, counts

    # ===== QL PHÒNG: mỗi tổ + NV trực thuộc phòng (không thuộc tổ) =====
    if _is_ql_phong(flat) and Units:
        name_col = _unit_name_column()
        q = db.query(Units.id)
        if name_col is not None: q = q.add_columns(name_col)
        if unit_scope: q = q.filter(Units.id.in_(list(unit_scope)))
        if hasattr(Units,"cap_do"): q = q.filter(Units.cap_do==3)
        if f_unit: q = q.filter(Units.id==f_unit)
        to_rows = q.all()

        # map unit -> members
        unit_member: Dict[Any, Set[Any]] = {}
        if UserUnitMemberships:
            try:
                qmem = db.query(getattr(UserUnitMemberships,"unit_id"), getattr(UserUnitMemberships,"user_id"))
                if unit_scope:
                    qmem = qmem.filter(getattr(UserUnitMemberships,"unit_id").in_(list(unit_scope)))
                for u_id, usr in qmem.all():
                    unit_member.setdefault(u_id, set()).add(usr)
            except Exception:
                unit_member = {}

        # NV trực thuộc phòng (không thuộc tổ)
        nv_phong_ids = _user_ids_by_roles_in_units(db, ["ROLE_NHAN_VIEN"], unit_scope, expected_unit_level=2)
        if f_assignee:
            nv_phong_ids = {x for x in nv_phong_ids if x == f_assignee}

        # tổ -> exclusive counts
        for r in to_rows:
            tid = r[0]
            tname = (r[1] if len(r)>1 and isinstance(r[1],str) else f"Tổ #{tid}")
            members = unit_member.get(tid, set())
            if not members:
                continue
            rows = base_query.with_entities(getattr(Tasks,"status"), getattr(Tasks,"due_date"))\
                             .filter(getattr(Tasks,"assigned_to_user_id").in_(list(members))).all()
            agg = _accumulate(rows, today_d=today_d)
            labels.append(tname)
            counts.append(agg)

        # NV trực thuộc phòng (không thuộc tổ)
        if nv_phong_ids:
            name_map: Dict[Any,str] = {}
            if Users:
                for u in db.query(Users).filter(Users.id.in_(list(nv_phong_ids))).all():
                    name_map[getattr(u,"id")] = getattr(u,"full_name",None) or getattr(u,"username",None) or getattr(u,"email",None) or f"NV #{getattr(u,'id')}"
            for rid in nv_phong_ids:
                rows = base_query.with_entities(getattr(Tasks,"status"), getattr(Tasks,"due_date"))\
                                 .filter(getattr(Tasks,"assigned_to_user_id")==rid).all()
                agg = _accumulate(rows, today_d=today_d)
                labels.append(name_map.get(rid, f"NV #{rid}"))
                counts.append(agg)
        return labels, counts

    # ===== QL TỔ: mỗi NV trong tổ =====
    if _is_ql_to(flat):
        nv_to_ids = _user_ids_by_roles_in_units(db, ["ROLE_NHAN_VIEN"], unit_scope, expected_unit_level=3)
        if f_assignee:
            nv_to_ids = {x for x in nv_to_ids if x == f_assignee}
        if Users and nv_to_ids:
            name_map: Dict[Any,str] = {}
            for u in db.query(Users).filter(Users.id.in_(list(nv_to_ids))).all():
                name_map[getattr(u,"id")] = getattr(u,"full_name",None) or getattr(u,"username",None) or getattr(u,"email",None) or f"NV #{getattr(u,'id')}"
            for rid in nv_to_ids:
                rows = base_query.with_entities(getattr(Tasks,"status"), getattr(Tasks,"due_date"))\
                                 .filter(getattr(Tasks,"assigned_to_user_id")==rid).all()
                agg = _accumulate(rows, today_d=date.today())
                labels.append(name_map.get(rid, f"NV #{rid}"))
                counts.append(agg)
        return labels, counts

    return labels, counts

def _build_board_room_eval(
    db: Session,
    base_query,
    *,
    f_unit: Optional[Any] = None,
) -> Tuple[List[str], List[int], List[int], List[int], List[int]]:
    """
    Biểu đồ đánh giá Phòng dành cho HĐTV:
    - Tổng số công việc được giao
    - Hoàn thành: chỉ tính task đã KẾT THÚC (CLOSED hoặc có closed_at)
    - Quá hạn: due_date < today và chưa CLOSED
    - Đang thực hiện: phần còn lại đang mở
    """
    if not (Units and UserUnitMemberships and Tasks):
        return [], [], [], [], []

    today_d = date.today()

    rows = base_query.with_entities(
        getattr(Tasks, "assigned_to_user_id"),
        getattr(Tasks, "status"),
        getattr(Tasks, "due_date"),
        getattr(Tasks, "closed_at") if hasattr(Tasks, "closed_at") else None,
    ).all()

    if not rows:
        return [], [], [], [], []

    assignees: Set[Any] = {r[0] for r in rows if r and r[0] is not None}
    if not assignees:
        return [], [], [], [], []

    # map user -> phòng chính (cap_do=2)
    user_room: Dict[Any, Any] = {}
    q = (
        db.query(getattr(UserUnitMemberships, "user_id"), getattr(UserUnitMemberships, "unit_id"))
        .join(Units, getattr(Units, "id") == getattr(UserUnitMemberships, "unit_id"))
        .filter(getattr(Units, "cap_do") == 2)
        .filter(getattr(UserUnitMemberships, "user_id").in_(list(assignees)))
    )
    if hasattr(UserUnitMemberships, "is_primary"):
        q = q.order_by(getattr(UserUnitMemberships, "user_id").asc(), getattr(UserUnitMemberships, "is_primary").desc())

    for uid2, unit_id in q.all():
        user_room.setdefault(uid2, unit_id)

    bucket: Dict[Any, Dict[str, int]] = {}
    for assignee_id, st, due, closed_at in rows:
        room_id = user_room.get(assignee_id)
        if room_id is None:
            continue
        if f_unit and str(f_unit) != str(room_id):
            continue

        stat = bucket.setdefault(room_id, {
            "total": 0,
            "closed": 0,
            "overdue": 0,
            "in_progress": 0,
        })
        stat["total"] += 1

        sv = _status_value(st).upper()
        due_d = _normalize_date(due)

        is_closed = (sv == "CLOSED") or (closed_at is not None)
        if is_closed:
            stat["closed"] += 1
        elif due_d is not None and due_d < today_d:
            stat["overdue"] += 1
        else:
            stat["in_progress"] += 1

    if not bucket:
        return [], [], [], [], []

    name_col = _unit_name_column()
    name_map: Dict[Any, str] = {}
    qn = db.query(Units.id)
    if name_col is not None:
        qn = qn.add_columns(name_col)
    qn = qn.filter(Units.id.in_(list(bucket.keys())))
    for r in qn.all():
        uid2 = r[0]
        nm = r[1] if len(r) > 1 else None
        name_map[uid2] = nm if isinstance(nm, str) else str(uid2)

    room_ids_sorted = sorted(bucket.keys(), key=lambda x: str(name_map.get(x, x)))

    labels = [name_map.get(rid, f"Phòng #{rid}") for rid in room_ids_sorted]
    totals = [int(bucket[rid]["total"]) for rid in room_ids_sorted]
    closeds = [int(bucket[rid]["closed"]) for rid in room_ids_sorted]
    overdues = [int(bucket[rid]["overdue"]) for rid in room_ids_sorted]
    inprogs = [int(bucket[rid]["in_progress"]) for rid in room_ids_sorted]

    return labels, totals, closeds, overdues, inprogs
# ----------------------------------- ROUTE -----------------------------------

@router.get("/dashboard", response_class=HTMLResponse)
def dashboard(
    request: Request,
    db: Session = Depends(get_db),
    f_from: Optional[str] = Query(None),
    f_to: Optional[str]   = Query(None),
    f_unit: Optional[str] = Query(None),
    f_status: Optional[str]= Query(None),
    f_assignee: Optional[str] = Query(None),
):
    uid = _user_id(request)
    if not uid:
        return RedirectResponse(url="/login", status_code=307)

    roles = _get_session_roles(request)
    flat = _roles_flat(roles)
    sess = getattr(request, "session", {}) or {}

    # Chuẩn hoá tham số lọc
    def _parse_date(s: Optional[str]) -> Optional[date]:
        if not s: return None
        try:
            return datetime.strptime(s, "%Y-%m-%d").date()
        except Exception:
            return None
    _from = _parse_date(f_from)
    _to   = _parse_date(f_to)
    _unit = f_unit
    _assignee = None
    try:
        _assignee = int(f_assignee) if f_assignee not in (None, "") else None
    except Exception:
        _assignee = None

    unit_scope, scope_label, board_ids, qlphong_ids, qlto_ids, nv_ids = _unit_scope_for_user(db, uid, flat, sess)

    # ===== Danh sách đơn vị cho bộ lọc =====
    unit_opts: List[Tuple[Any,str]] = []
    if Units:
        name_col = _unit_name_column()
        q = db.query(Units.id)
        if name_col is not None:
            q = q.add_columns(name_col)
        if _is_board(flat, sess) and hasattr(Units, "cap_do"):
            q = q.filter(Units.cap_do == 2)  # HĐTV chỉ cấp phòng
        else:
            if unit_scope:
                q = q.filter(Units.id.in_(list(unit_scope)))
        rows = q.order_by(Units.id.asc()).all()
        for r in rows:
            uid2 = r[0]
            nm = r[1] if len(r) > 1 else None
            unit_opts.append((uid2, nm if isinstance(nm, str) else str(uid2)))

    # ===== Danh sách Người nhận cho bộ lọc =====
    assignee_opts: List[Tuple[Any, str]] = []
    if Users:
        want_ids: Set[Any] = set()
        if _is_board(flat, sess):
            want_ids = set(qlphong_ids)   # HĐTV lọc theo QL phòng
        elif _is_ql_phong(flat):
            nv_phong_ids = _user_ids_by_roles_in_units(db, ["ROLE_NHAN_VIEN"], unit_scope, expected_unit_level=2)
            want_ids = set(qlto_ids) | set(nv_phong_ids)
        elif _is_ql_to(flat):
            want_ids = _user_ids_by_roles_in_units(db, ["ROLE_NHAN_VIEN"], unit_scope, expected_unit_level=3)
        if want_ids:
            for u in db.query(Users).filter(Users.id.in_(list(want_ids))).order_by(Users.id.asc()).all():
                nm = getattr(u,"full_name",None) or getattr(u,"username",None) or getattr(u,"email",None) or f"#{getattr(u,'id')}"
                assignee_opts.append((getattr(u,"id"), nm))

    today = date.today()

    def _vals(names: Iterable[str]) -> List[Any]:
        out: List[Any] = []
        for n in names:
            if hasattr(TaskStatus, n):
                out.append(getattr(TaskStatus, n))
        return out
    S_DONE   = _vals(["DONE","CLOSED"])

    # --- 1) GIAO VIỆC (assign) ---
    assign_q = db.query(Tasks)
    show_assign = False
    if _is_board(flat, sess):
        assign_q = assign_q.filter(
            getattr(Tasks, "created_by").in_(list(board_ids))
            if board_ids else getattr(Tasks, "id").isnot(None)
        )

        board_assignee_ids = set(qlphong_ids) | set(qlto_ids)
        if board_assignee_ids:
            assign_q = assign_q.filter(getattr(Tasks, "assigned_to_user_id").in_(list(board_assignee_ids)))
        else:
            assign_q = assign_q.filter(getattr(Tasks, "id").is_(None))

        show_assign = True
        assign_q = _apply_filters(assign_q, _from, _to, _unit, f_status, _assignee, ignore_unit=True)
    elif _is_ql_phong(flat):
        if unit_scope:
            assign_q = assign_q.filter(getattr(Tasks,"unit_id").in_(list(unit_scope)))
        nv_phong_ids = _user_ids_by_roles_in_units(db, ["ROLE_NHAN_VIEN"], unit_scope, expected_unit_level=2)
        assignee_set = set(qlto_ids) | set(nv_phong_ids)
        assign_q = assign_q.filter(getattr(Tasks,"created_by")==uid,
                                   getattr(Tasks,"assigned_to_user_id").in_(list(assignee_set)))
        show_assign = True
        assign_q = _apply_filters(assign_q, _from, _to, _unit, f_status, _assignee)
    elif _is_ql_to(flat):
        if unit_scope:
            assign_q = assign_q.filter(getattr(Tasks,"unit_id").in_(list(unit_scope)))
        nv_to_ids = _user_ids_by_roles_in_units(db, ["ROLE_NHAN_VIEN"], unit_scope, expected_unit_level=3)
        assign_q = assign_q.filter(getattr(Tasks,"created_by")==uid,
                                   getattr(Tasks,"assigned_to_user_id").in_(list(nv_to_ids)))
        show_assign = True
        assign_q = _apply_filters(assign_q, _from, _to, _unit, f_status, _assignee)
    else:
        assign_q = assign_q.filter(getattr(Tasks,"id").is_(None))
        show_assign = False

    # Phân bố "đối tượng được giao"
    assign_cat_labels: List[str] = []
    assign_cat_counts: List[Dict[str,int]] = []
    if show_assign:
        assign_cat_labels, assign_cat_counts = _build_assign_categories(
            db=db, base_query=assign_q, flat=flat, sess=sess, unit_scope=unit_scope,
            uid=uid, qlphong_ids=qlphong_ids, qlto_ids=qlto_ids, f_unit=_unit, f_assignee=_assignee
        )

    # KPI từ aggregate (exclusive)
    kpi_assign_inprog = sum(int(a.get("IN_PROGRESS",0)) for a in assign_cat_counts) if assign_cat_counts else 0
    kpi_assign_overdue = sum(int(a.get("OVERDUE",0)) for a in assign_cat_counts) if assign_cat_counts else 0
    kpi_assign_done = sum(int(a.get("DONE",0)) for a in assign_cat_counts) if assign_cat_counts else 0
    kpi_assign_total = kpi_assign_inprog + kpi_assign_overdue + kpi_assign_done

    # =========== XU HƯỚNG (Assign) 12 THÁNG ===========
    # Cửa sổ thời gian cho trend: ưu tiên f_from/f_to, nếu không có thì 12 tháng gần nhất.
    trend_date_max = _to or today
    base_start = _month_start(today) - timedelta(days=365 - 1)  # ~12 tháng
    trend_date_min = _from or base_start
    if trend_date_min > trend_date_max:
        trend_date_min = trend_date_max

    ym_created = func.strftime("%Y-%m", getattr(Tasks,"created_at"))
    ym_updated = func.strftime("%Y-%m", getattr(Tasks,"updated_at"))

    created_series: List[int] = []
    done_series: List[int] = []
    labels_m: List[str] = _build_month_labels(_month_start(trend_date_min), _month_start(trend_date_max))

    if show_assign:
        trend_q = assign_q  # đã áp phần lớn bộ lọc

        # Bổ sung ràng buộc theo vai trò (đảm bảo khớp với phân quyền hiển thị)
        if _is_board(flat, sess):
            # Board: nếu có f_unit (phòng), ràng assignee ∈ thành viên của phòng đó
            if _unit:
                assignees = _assignees_in_unit_cap_phong(db, _unit)
                trend_q = trend_q.filter(getattr(Tasks, "assigned_to_user_id").in_(list(assignees))) if assignees else trend_q.filter(getattr(Tasks,"id").is_(None))
            # Nếu có f_assignee (QL phòng cụ thể), áp luôn
            if _assignee:
                trend_q = trend_q.filter(getattr(Tasks, "assigned_to_user_id")==_assignee)

        elif _is_ql_phong(flat):
            # Nếu lọc theo tổ (cap_do=3)
            if _unit:
                members = _assignees_in_unit_cap_to(db, _unit)
                trend_q = trend_q.filter(getattr(Tasks, "assigned_to_user_id").in_(list(members))) if members else trend_q.filter(getattr(Tasks,"id").is_(None))
            # Nếu lọc theo cá nhân
            if _assignee:
                trend_q = trend_q.filter(getattr(Tasks, "assigned_to_user_id")==_assignee)

        elif _is_ql_to(flat):
            if _assignee:
                trend_q = trend_q.filter(getattr(Tasks, "assigned_to_user_id")==_assignee)

        # Áp khoảng thời gian cho trend
        trend_q_created = trend_q.filter(getattr(Tasks,"created_at")>=trend_date_min, getattr(Tasks,"created_at")<=_month_end(trend_date_max))
        trend_q_done    = trend_q.filter(getattr(Tasks,"updated_at")>=trend_date_min, getattr(Tasks,"updated_at")<=_month_end(trend_date_max), getattr(Tasks,"status").in_(S_DONE))

        trend_created_rows = (trend_q_created
                              .with_entities(ym_created.label("ym"), func.count(getattr(Tasks,"id")).label("c"))
                              .group_by("ym").order_by("ym").all())
        trend_done_rows = (trend_q_done
                           .with_entities(ym_updated.label("ym"), func.count(getattr(Tasks,"id")).label("c"))
                           .group_by("ym").order_by("ym").all())

        trend_created_map = {r.ym: r.c for r in trend_created_rows}
        trend_done_map    = {r.ym: r.c for r in trend_done_rows}

        created_series = [trend_created_map.get(m, 0) for m in labels_m]
        done_series    = [trend_done_map.get(m, 0) for m in labels_m]
    else:
        created_series = [0 for _ in labels_m]
        done_series    = [0 for _ in labels_m]

    # Biểu đồ giao việc (stacked)
    assign_cat_labels = assign_cat_labels  # giữ nguyên
    stacked_orange = [int(x.get("IN_PROGRESS",0)) for x in assign_cat_counts]
    stacked_blue   = [int(x.get("DONE",0)) for x in assign_cat_counts]
    stacked_red    = [int(x.get("OVERDUE",0)) for x in assign_cat_counts]

    # Chế độ single selection
    assign_chart_mode = "multi" if len(assign_cat_labels) != 1 else "single"
    if len(assign_cat_labels) == 1:
        agg0 = assign_cat_counts[0]
        single_inprog  = int(agg0.get("IN_PROGRESS",0))
        single_done    = int(agg0.get("DONE",0))
        single_overdue = int(agg0.get("OVERDUE",0))
        single_total   = single_inprog + single_done + single_overdue
    else:
        single_inprog = single_done = single_overdue = single_total = 0
        
    # ===== Biểu đồ đánh giá Phòng dành cho HĐTV =====
    board_room_labels: List[str] = []
    board_room_total: List[int] = []
    board_room_closed: List[int] = []
    board_room_overdue: List[int] = []
    board_room_inprog: List[int] = []

    if _is_board(flat, sess) and show_assign:
        (
            board_room_labels,
            board_room_total,
            board_room_closed,
            board_room_overdue,
            board_room_inprog,
        ) = _build_board_room_eval(
            db,
            assign_q,
            f_unit=_unit,
        )        

    # --- 2) NHẬN VIỆC (inbox) ---
    inbox_q = db.query(Tasks)
    show_inbox = False
    if unit_scope and not _is_board(flat, sess):
        inbox_q = inbox_q.filter(getattr(Tasks,"unit_id").in_(list(unit_scope)))

    if _is_board(flat, sess):
        inbox_q = inbox_q.filter(getattr(Tasks,"id").is_(None))
        show_inbox = False
    elif _is_ql_phong(flat):
        inbox_q = inbox_q.filter(getattr(Tasks,"created_by").in_(list(_user_ids_by_roles_in_units(db, ["ROLE_LANH_DAO","ROLE_ADMIN"]))),
                                 getattr(Tasks,"assigned_to_user_id")==uid)
        show_inbox = True
    elif _is_ql_to(flat):
        inbox_q = inbox_q.filter(getattr(Tasks,"created_by").in_(list(_user_ids_by_roles_in_units(db, ["ROLE_TRUONG_PHONG","ROLE_PHO_PHONG"], unit_scope, expected_unit_level=2))),
                                 getattr(Tasks,"assigned_to_user_id")==uid)
        show_inbox = True
    else:
        inbox_q = inbox_q.filter(getattr(Tasks,"assigned_to_user_id")==uid,
                                 getattr(Tasks,"created_by").in_(list(_user_ids_by_roles_in_units(db, ["ROLE_TO_TRUONG","ROLE_PHO_TO"], unit_scope, expected_unit_level=3) |
                                                                    _user_ids_by_roles_in_units(db, ["ROLE_TRUONG_PHONG","ROLE_PHO_PHONG"], unit_scope, expected_unit_level=2))))
        show_inbox = True

    inbox_q = _apply_filters(inbox_q, _from, _to, _unit, f_status, None)

    # KPI Inbox (exclusive)
    total_inbox = inprog_inbox = overdue_inbox = 0
    if show_inbox:
        rows = inbox_q.with_entities(getattr(Tasks,"status"), getattr(Tasks,"due_date")).all()
        acc = _accumulate(rows, today_d=today)
        inprog_inbox = int(acc.get("IN_PROGRESS",0))
        overdue_inbox = int(acc.get("OVERDUE",0))
        done_inbox = int(acc.get("DONE",0))
        total_inbox = inprog_inbox + overdue_inbox + done_inbox

    # =========== XU HƯỚNG (Inbox) ===========
    inbox_created_series: List[int] = []
    inbox_done_series: List[int] = []
    if show_inbox:
        inbox_trend_q = inbox_q  # đã áp bộ lọc
        inbox_trend_q_created = inbox_trend_q.filter(getattr(Tasks,"created_at")>=trend_date_min, getattr(Tasks,"created_at")<=_month_end(trend_date_max))
        inbox_trend_q_done    = inbox_trend_q.filter(getattr(Tasks,"updated_at")>=trend_date_min, getattr(Tasks,"updated_at")<=_month_end(trend_date_max), getattr(Tasks,"status").in_(S_DONE))

        inbox_created_rows = (inbox_trend_q_created
                              .with_entities(ym_created.label("ym"), func.count(getattr(Tasks,"id")).label("c"))
                              .group_by("ym").order_by("ym").all())
        inbox_done_rows = (inbox_trend_q_done
                           .with_entities(ym_updated.label("ym"), func.count(getattr(Tasks,"id")).label("c"))
                           .group_by("ym").order_by("ym").all())

        inbox_created_map = {r.ym: r.c for r in inbox_created_rows}
        inbox_done_map    = {r.ym: r.c for r in inbox_done_rows}

        inbox_created_series = [inbox_created_map.get(m, 0) for m in labels_m]
        inbox_done_series    = [inbox_done_map.get(m, 0) for m in labels_m]
    else:
        inbox_created_series = [0 for _ in labels_m]
        inbox_done_series    = [0 for _ in labels_m]

    # ===== Trạng thái (VN) =====
    status_codes_order = ["NEW","IN_PROGRESS","SUBMITTED","DONE","CLOSED","REJECTED","CANCELLED"]
    statuses_for_form = [(code, VN_STATUS_LABELS.get(code, code)) for code in status_codes_order]

    ctx = {
        "request": request,
        "app_name": getattr(settings, "APP_NAME", "QLCV_App"),
        "company_name": getattr(settings, "COMPANY_NAME", ""),
        "role_label": scope_label,
        "filters": {
            "f_from": f_from or "",
            "f_to": f_to or "",
            "f_unit": f_unit or "",
            "f_status": f_status or "",
            "f_assignee": f_assignee or "",
        },
        "units": unit_opts,
        "assignees": assignee_opts,
        "statuses": statuses_for_form,

        # KPI Giao việc
        "kpi_assign": {
            "total": kpi_assign_total,
            "in_progress": kpi_assign_inprog,
            "overdue": kpi_assign_overdue,
            "done": kpi_assign_done,
        },
        # KPI Nhận việc
        "kpi_inbox": {
            "total": total_inbox,
            "in_progress": inprog_inbox,
            "overdue": overdue_inbox,
        },

        # Xu hướng tháng (khớp bộ lọc)
        "trend_labels": labels_m,
        "trend_created": created_series,
        "trend_done": done_series,
        "inbox_trend_created": inbox_created_series,
        "inbox_trend_done": inbox_done_series,

        # Biểu đồ giao việc (stacked)
        "assign_chart_mode": "multi" if len(assign_cat_labels) != 1 else "single",
        "assign_cat_labels": assign_cat_labels,
        "stacked_orange": stacked_orange,
        "stacked_blue":   stacked_blue,
        "stacked_red":    stacked_red,

        # 4 cột khi lọc 1 đối tượng
        "single_total": single_total,
        "single_done": single_done,
        "single_overdue": single_overdue,
        "single_inprog": single_inprog,

        # Biểu đồ đánh giá Phòng dành cho HĐTV
        "board_room_labels": board_room_labels,
        "board_room_total": board_room_total,
        "board_room_closed": board_room_closed,
        "board_room_overdue": board_room_overdue,
        "board_room_inprog": board_room_inprog,

        "details_assign": [],
        "details_inbox": [],
        "show_assign": True if show_assign else False,
        "show_inbox": True if show_inbox else False,
    }

    # (Tuỳ chọn) Bảng chi tiết NHẬN VIỆC
    if show_inbox:
        details_inbox: List[Dict[str,Any]] = []
        dq = inbox_q.order_by(func.coalesce(getattr(Tasks,"due_date"), date(2999,1,1)).asc(),
                              getattr(Tasks,"created_at").desc()).limit(200)
        rows = dq.all()
        user_ids: Set[Any] = set()
        unit_ids_map: Set[Any] = set()
        for t in rows:
            user_ids.add(getattr(t,"created_by",None))
            user_ids.add(getattr(t,"assigned_to_user_id",None))
            unit_ids_map.add(getattr(t,"unit_id",None))
        name_user: Dict[Any,str] = {}
        name_unit: Dict[Any,str] = {}
        if Users and user_ids:
            for u in db.query(Users).filter(Users.id.in_(list({x for x in user_ids if x is not None}))).all():
                nm = getattr(u,"full_name",None) or getattr(u,"username",None) or getattr(u,"email",None) or "-"
                name_user[getattr(u,"id")] = nm
        if Units and unit_ids_map:
            name_col = _unit_name_column()
            q = db.query(Units.id)
            if name_col is not None: q = q.add_columns(name_col)
            for u in q.filter(Units.id.in_(list({x for x in unit_ids_map if x is not None}))).all():
                uid2 = u[0]; nm = u[1] if len(u) > 1 else None
                name_unit[uid2] = nm if isinstance(nm, str) else str(uid2)
        for t in rows:
            details_inbox.append({
                "id": getattr(t,"id"),
                "title": getattr(t,"title",None) or getattr(t,"name",None) or "(Không có tiêu đề)",
                "creator": name_user.get(getattr(t,"created_by"), "-"),
                "assignee": name_user.get(getattr(t,"assigned_to_user_id"), "-"),
                "unit": name_unit.get(getattr(t,"unit_id"), "-"),
                "start_date": (getattr(t,"created_at").date().isoformat() if getattr(t,"created_at",None) and hasattr(getattr(t,"created_at"),"date") else (str(getattr(t,"created_at",""))[:10] or "")),
                "due_date": (getattr(t,"due_date").date().isoformat() if getattr(t,"due_date",None) and hasattr(getattr(t,"due_date"),"date") else (str(getattr(t,"due_date",""))[:10] or "")),
                "status": _status_value(getattr(t,"status","")),
            })
        ctx["details_inbox"] = details_inbox

    return templates.TemplateResponse("dashboard.html", ctx)
