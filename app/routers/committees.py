from __future__ import annotations

import os
from datetime import datetime
from anyio import from_thread

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request, status
from sqlalchemy import func, or_
from sqlalchemy.orm import Session
from starlette.responses import RedirectResponse
from starlette.templating import Jinja2Templates

from ..committees.service import (
    COMMITTEE_ACTIVE_STATUS,
    COMMITTEE_MANAGED_BY_BGD,
    COMMITTEE_MANAGED_BY_HDTV,
    COMMITTEE_ROLE_PHO_TRUONG_BAN,
    COMMITTEE_ROLE_THANH_VIEN,
    COMMITTEE_ROLE_TRUONG_BAN,
    allowed_committee_ids_for_user,
    committee_managed_by_label,
    committee_role_label,
    normalize_committee_managed_by,
    normalize_committee_role,
    user_can_manage_committees,
    user_can_view_committee,
)
from ..models import CommitteeMembers, Committees, Roles, Units, UserRoles, Users, UserStatus
from ..security.deps import get_db, login_required
from ..chat.realtime import manager

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "..", "templates"))

COMMITTEE_TYPES = [
    ("BAN", "Ban"),
    ("HOI_DONG", "Hội đồng"),
    ("TO_CONG_TAC", "Tổ công tác"),
    ("BAN_CHI_DAO", "Ban chỉ đạo"),
    ("BAN_QUAN_LY_DU_AN", "Ban Quản lý dự án"),
]

COMMITTEE_ROLES = [
    (COMMITTEE_ROLE_TRUONG_BAN, "Trưởng ban"),
    (COMMITTEE_ROLE_PHO_TRUONG_BAN, "Phó trưởng ban"),
    (COMMITTEE_ROLE_THANH_VIEN, "Thành viên"),
]



COMMITTEE_MANAGED_BY_OPTIONS = [
    (COMMITTEE_MANAGED_BY_HDTV, "HĐTV"),
    (COMMITTEE_MANAGED_BY_BGD, "BGĐ"),
]


def _parse_date(value: str | None):
    value = (value or "").strip()
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Định dạng ngày không hợp lệ.")


def _require_committee_admin(user: Users, db: Session) -> None:
    if not user_can_manage_committees(db, user.id):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Chỉ Admin được quản trị Ban kiêm nhiệm.")


def _committee_url(committee_id: str | None = None, message: str | None = None) -> str:
    parts = []
    if committee_id:
        parts.append(f"committee_id={committee_id}")
    if message:
        parts.append(f"msg={message}")
    return "/committees" + (("?" + "&".join(parts)) if parts else "")


def _role_code_value(value) -> str:
    return str(getattr(value, "value", value) or "").strip().upper()


def _admin_leader_user_ids(db: Session) -> list[str]:
    rows = (
        db.query(UserRoles.user_id)
        .join(Roles, Roles.id == UserRoles.role_id)
        .filter(Roles.code.in_(["ROLE_ADMIN", "ROLE_LANH_DAO"]))
        .distinct()
        .all()
    )
    return [str(user_id) for (user_id,) in rows if user_id]


def _committee_member_user_ids(db: Session, committee_id: str) -> list[str]:
    rows = (
        db.query(CommitteeMembers.user_id)
        .filter(CommitteeMembers.committee_id == committee_id)
        .filter(CommitteeMembers.is_active == True)  # noqa: E712
        .distinct()
        .all()
    )
    return [str(user_id) for (user_id,) in rows if user_id]


def _emit_committee_work_realtime(
    db: Session,
    *,
    committee_id: str,
    actor_user_id: str | None = None,
    changed_user_id: str | None = None,
    action: str = "",
) -> None:
    """
    Phát realtime khi Admin tạo/sửa Ban hoặc thêm/sửa/xóa thành viên Ban.

    Các trang Plans/Tasks/Inbox đang mở sẽ tự cập nhật theo websocket.
    Không dùng polling mới, không yêu cầu người dùng bấm F5.
    """
    target_user_ids = set(_admin_leader_user_ids(db))
    target_user_ids.update(_committee_member_user_ids(db, committee_id))

    if actor_user_id:
        target_user_ids.add(str(actor_user_id))
    if changed_user_id:
        target_user_ids.add(str(changed_user_id))

    payload = {
        "module": "work",
        "type": "committee_access_changed",
        "action": action or "changed",
        "committee_id": str(committee_id or ""),
        "changed_user_id": str(changed_user_id or ""),
        "actor_user_id": str(actor_user_id or ""),
        "requires_session_refresh": False,
    }

    if not target_user_ids:
        return

    try:
        from_thread.run(manager.notify_users_json, list(target_user_ids), payload)
    except Exception:
        pass

def _active_users_query(db: Session, keyword: str | None):
    query = db.query(Users).filter(Users.status == UserStatus.ACTIVE)
    keyword = (keyword or "").strip()
    if keyword:
        like = f"%{keyword.lower()}%"
        query = query.filter(
            or_(
                func.lower(func.coalesce(Users.full_name, "")).like(like),
                func.lower(func.coalesce(Users.username, "")).like(like),
                func.lower(func.coalesce(Users.email, "")).like(like),
                func.lower(func.coalesce(Users.phone, "")).like(like),
            )
        )
    return query.order_by(Users.full_name.asc(), Users.username.asc()).limit(200).all()


@router.get("")
def committee_manage(
    request: Request,
    committee_id: str | None = Query(None),
    q: str | None = Query(None),
    msg: str | None = Query(None),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)
    can_manage = user_can_manage_committees(db, current_user.id)
    allowed_ids = list(allowed_committee_ids_for_user(db, current_user.id))

    if can_manage:
        committees = db.query(Committees).order_by(Committees.is_active.desc(), Committees.name.asc()).all()
    elif allowed_ids:
        committees = (
            db.query(Committees)
            .filter(Committees.id.in_(allowed_ids))
            .order_by(Committees.is_active.desc(), Committees.name.asc())
            .all()
        )
    else:
        committees = []

    selected_committee = None
    if committee_id:
        selected_committee = db.get(Committees, committee_id)
        if selected_committee and not user_can_view_committee(db, current_user.id, selected_committee.id):
            raise HTTPException(status_code=403, detail="Bạn không có quyền xem Ban này.")
    elif committees:
        selected_committee = committees[0]

    members = []
    member_user_ids = set()
    if selected_committee:
        members = (
            db.query(CommitteeMembers)
            .filter(CommitteeMembers.committee_id == selected_committee.id)
            .order_by(CommitteeMembers.is_active.desc(), CommitteeMembers.committee_role.asc(), CommitteeMembers.joined_at.asc())
            .all()
        )
        member_user_ids = {m.user_id for m in members if m.is_active}

    users = _active_users_query(db, q) if can_manage else []
    available_users = [u for u in users if u.id not in member_user_ids]
    units = db.query(Units).order_by(Units.cap_do.asc(), Units.ten_don_vi.asc()).all()

    return templates.TemplateResponse(
        "committees/manage.html",
        {
            "request": request,
            "current_user": current_user,
            "committees": committees,
            "selected_committee": selected_committee,
            "members": members,
            "users": available_users,
            "units": units,
            "q": q or "",
            "msg": msg or "",
            "can_manage": can_manage,
            "committee_types": COMMITTEE_TYPES,
            "committee_roles": COMMITTEE_ROLES,
            "committee_managed_by_options": COMMITTEE_MANAGED_BY_OPTIONS,
            "committee_role_label": committee_role_label,
            "committee_managed_by_label": committee_managed_by_label,
        },
    )


@router.post("/create")
def create_committee(
    request: Request,
    name: str = Form(...),
    code: str = Form(""),
    committee_type: str = Form("BAN"),
    managed_by: str = Form(COMMITTEE_MANAGED_BY_HDTV),
    decision_no: str = Form(""),
    decision_date: str = Form(""),
    description: str = Form(""),
    start_date: str = Form(""),
    end_date: str = Form(""),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)
    _require_committee_admin(current_user, db)

    clean_name = (name or "").strip()
    if not clean_name:
        raise HTTPException(status_code=400, detail="Tên Ban kiêm nhiệm không được để trống.")

    committee = Committees(
        name=clean_name,
        code=(code or "").strip() or None,
        committee_type=(committee_type or "BAN").strip().upper(),
        managed_by=normalize_committee_managed_by(managed_by),
        decision_no=(decision_no or "").strip() or None,
        decision_date=_parse_date(decision_date),
        description=(description or "").strip() or None,
        start_date=_parse_date(start_date),
        end_date=_parse_date(end_date),
        status=COMMITTEE_ACTIVE_STATUS,
        is_active=True,
        created_by=current_user.id,
    )
    db.add(committee)
    db.commit()
    db.refresh(committee)

    _emit_committee_work_realtime(
        db,
        committee_id=committee.id,
        actor_user_id=current_user.id,
        action="committee_created",
    )

    return RedirectResponse(url=_committee_url(committee.id, "created"), status_code=303)


@router.post("/{committee_id}/update")
def update_committee(
    request: Request,
    committee_id: str,
    name: str = Form(...),
    code: str = Form(""),
    committee_type: str = Form("BAN"),
    managed_by: str = Form(COMMITTEE_MANAGED_BY_HDTV),
    decision_no: str = Form(""),
    decision_date: str = Form(""),
    description: str = Form(""),
    start_date: str = Form(""),
    end_date: str = Form(""),
    status_value: str = Form("ACTIVE"),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)
    _require_committee_admin(current_user, db)

    committee = db.get(Committees, committee_id)
    if not committee:
        raise HTTPException(status_code=404, detail="Không tìm thấy Ban kiêm nhiệm.")

    clean_name = (name or "").strip()
    if not clean_name:
        raise HTTPException(status_code=400, detail="Tên Ban kiêm nhiệm không được để trống.")

    committee.name = clean_name
    committee.code = (code or "").strip() or None
    committee.committee_type = (committee_type or "BAN").strip().upper()
    committee.managed_by = normalize_committee_managed_by(managed_by)
    committee.decision_no = (decision_no or "").strip() or None
    committee.decision_date = _parse_date(decision_date)
    committee.description = (description or "").strip() or None
    committee.start_date = _parse_date(start_date)
    committee.end_date = _parse_date(end_date)
    committee.status = (status_value or "ACTIVE").strip().upper()
    committee.is_active = committee.status == COMMITTEE_ACTIVE_STATUS
    committee.updated_at = datetime.utcnow()

    db.add(committee)
    db.commit()

    _emit_committee_work_realtime(
        db,
        committee_id=committee.id,
        actor_user_id=current_user.id,
        action="committee_updated",
    )

    return RedirectResponse(url=_committee_url(committee.id, "updated"), status_code=303)


@router.post("/{committee_id}/members/add")
def add_committee_member(
    request: Request,
    committee_id: str,
    user_id: str = Form(...),
    committee_role: str = Form(COMMITTEE_ROLE_THANH_VIEN),
    member_title: str = Form(""),
    note: str = Form(""),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)
    _require_committee_admin(current_user, db)

    committee = db.get(Committees, committee_id)
    if not committee:
        raise HTTPException(status_code=404, detail="Không tìm thấy Ban kiêm nhiệm.")

    user = db.get(Users, user_id)
    if not user or user.status != UserStatus.ACTIVE:
        raise HTTPException(status_code=404, detail="Không tìm thấy user đang hoạt động.")

    existed = (
        db.query(CommitteeMembers)
        .filter(CommitteeMembers.committee_id == committee_id)
        .filter(CommitteeMembers.user_id == user_id)
        .filter(CommitteeMembers.is_active == True)  # noqa: E712
        .first()
    )
    if existed:
        raise HTTPException(status_code=400, detail="User này đã là thành viên đang hoạt động của Ban.")

    member = CommitteeMembers(
        committee_id=committee_id,
        user_id=user_id,
        committee_role=normalize_committee_role(committee_role),
        member_title=(member_title or "").strip() or None,
        note=(note or "").strip() or None,
        joined_at=datetime.utcnow(),
        is_active=True,
        added_by=current_user.id,
    )
    db.add(member)
    committee.updated_at = datetime.utcnow()
    db.add(committee)
    db.commit()

    _emit_committee_work_realtime(
        db,
        committee_id=committee_id,
        actor_user_id=current_user.id,
        changed_user_id=user_id,
        action="member_added",
    )

    return RedirectResponse(url=_committee_url(committee_id, "member_added"), status_code=303)


@router.post("/{committee_id}/members/{member_id}/update")
def update_committee_member(
    request: Request,
    committee_id: str,
    member_id: str,
    committee_role: str = Form(COMMITTEE_ROLE_THANH_VIEN),
    member_title: str = Form(""),
    note: str = Form(""),
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)
    _require_committee_admin(current_user, db)

    member = db.get(CommitteeMembers, member_id)
    if not member or member.committee_id != committee_id:
        raise HTTPException(status_code=404, detail="Không tìm thấy thành viên Ban.")

    member.committee_role = normalize_committee_role(committee_role)
    member.member_title = (member_title or "").strip() or None
    member.note = (note or "").strip() or None
    member.updated_at = datetime.utcnow()
    db.add(member)
    db.commit()

    _emit_committee_work_realtime(
        db,
        committee_id=committee_id,
        actor_user_id=current_user.id,
        changed_user_id=member.user_id,
        action="member_updated",
    )

    return RedirectResponse(url=_committee_url(committee_id, "member_updated"), status_code=303)


@router.post("/{committee_id}/members/{member_id}/deactivate")
def deactivate_committee_member(
    request: Request,
    committee_id: str,
    member_id: str,
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)
    _require_committee_admin(current_user, db)

    member = db.get(CommitteeMembers, member_id)
    if not member or member.committee_id != committee_id:
        raise HTTPException(status_code=404, detail="Không tìm thấy thành viên Ban.")

    changed_user_id = member.user_id

    member.is_active = False
    member.left_at = datetime.utcnow()
    member.updated_at = datetime.utcnow()
    db.add(member)
    db.commit()

    _emit_committee_work_realtime(
        db,
        committee_id=committee_id,
        actor_user_id=current_user.id,
        changed_user_id=changed_user_id,
        action="member_removed",
    )

    return RedirectResponse(url=_committee_url(committee_id, "member_removed"), status_code=303)