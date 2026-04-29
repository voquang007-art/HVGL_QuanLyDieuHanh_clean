# -*- coding: utf-8 -*-
"""
app/routers/chat.py

Router giao diện cho module chat - giai đoạn 1.
Chỉ dựng khung màn hình:
- /chat
- /chat/{group_id}

Chưa triển khai sâu quyền đơn vị và WebSocket.
"""

from __future__ import annotations
from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from sqlalchemy.orm import Session

from app.config import settings
from app.database import SessionLocal, get_db
from app.security.deps import login_required
from app.chat.deps import get_display_name
from app.chat.realtime import manager
from app.models import UserUnitMemberships, Units, UserRoles, Roles, Users
from app.chat.service import (
    enrich_groups_for_list,
    get_available_users_for_group,
    get_group_by_id,
    get_group_members,
    get_group_messages,
    get_user_groups,
    is_group_member,
    list_message_reactions,
    mark_group_as_read,
    get_group_pinned_items,
)

from starlette.templating import Jinja2Templates
import os

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "..", "templates"))


def _company_name() -> str:
    return getattr(settings, "COMPANY_NAME", "") or "Bệnh viện Hùng Vương Gia Lai"


def _app_name() -> str:
    return getattr(settings, "APP_NAME", "") or "ỨNG DỤNG QUẢN LÝ, ĐIỀU HÀNH CÔNG VIỆC"

ROLE_CHAT_BOARD = {"ROLE_LANH_DAO"}


def _chat_attachment_preview_url(attachment_id: str) -> str:
    attachment_id = str(attachment_id or "").strip()
    return f"/chat/api/attachments/{attachment_id}/preview" if attachment_id else ""


def _chat_attachment_download_url(attachment_id: str) -> str:
    attachment_id = str(attachment_id or "").strip()
    return f"/chat/api/attachments/{attachment_id}/download" if attachment_id else ""

def _chat_load_role_codes(db: Session, user_id: str) -> set[str]:
    rows = (
        db.query(Roles.code)
        .join(UserRoles, UserRoles.role_id == Roles.id)
        .filter(UserRoles.user_id == user_id)
        .all()
    )
    result: set[str] = set()
    for (code,) in rows:
        raw = getattr(code, "value", code)
        result.add(str(raw or "").strip().upper())
    return result


def _chat_user_option_label(user_obj) -> str:
    full_name = (getattr(user_obj, "full_name", None) or "").strip()
    username = (getattr(user_obj, "username", None) or "").strip()
    if full_name and username:
        return f"{full_name} ({username})"
    return full_name or username or "Người dùng"


def _build_available_users_tree(db: Session, users: list[Users]) -> dict:
    user_ids = [str(getattr(u, "id", "") or "").strip() for u in (users or []) if getattr(u, "id", None)]
    if not user_ids:
        return {"board_users": [], "departments": [], "others": []}

    membership_rows = (
        db.query(UserUnitMemberships)
        .filter(UserUnitMemberships.user_id.in_(user_ids))
        .order_by(UserUnitMemberships.user_id.asc(), UserUnitMemberships.is_primary.desc())
        .all()
    )

    primary_membership_by_user: dict[str, UserUnitMemberships] = {}
    unit_ids: set[str] = set()

    for row in membership_rows:
        uid = str(getattr(row, "user_id", "") or "").strip()
        unit_id = str(getattr(row, "unit_id", "") or "").strip()
        if uid and uid not in primary_membership_by_user:
            primary_membership_by_user[uid] = row
        if unit_id:
            unit_ids.add(unit_id)

    units_map: dict[str, Units] = {}
    if unit_ids:
        first_pass_units = db.query(Units).filter(Units.id.in_(list(unit_ids))).all()
        for unit in first_pass_units:
            units_map[str(unit.id)] = unit

        parent_ids: set[str] = set()
        for unit in first_pass_units:
            parent_id = str(getattr(unit, "parent_id", "") or "").strip()
            if parent_id:
                parent_ids.add(parent_id)

        missing_parent_ids = [pid for pid in parent_ids if pid not in units_map]
        if missing_parent_ids:
            for unit in db.query(Units).filter(Units.id.in_(missing_parent_ids)).all():
                units_map[str(unit.id)] = unit

    role_rows = (
        db.query(UserRoles.user_id, Roles.code)
        .join(Roles, Roles.id == UserRoles.role_id)
        .filter(UserRoles.user_id.in_(user_ids))
        .all()
    )
    role_map: dict[str, set[str]] = {}
    for user_id, code in role_rows:
        uid = str(user_id or "").strip()
        raw = getattr(code, "value", code)
        role_map.setdefault(uid, set()).add(str(raw or "").strip().upper())

    board_users: list[dict] = []
    departments_map: dict[str, dict] = {}
    others: list[dict] = []

    for user_obj in users or []:
        uid = str(getattr(user_obj, "id", "") or "").strip()
        if not uid:
            continue

        item = {
            "id": uid,
            "label": _chat_user_option_label(user_obj),
            "search_text": (
                f"{getattr(user_obj, 'full_name', '')} {getattr(user_obj, 'username', '')}"
            ).strip().lower(),
        }

        role_codes = role_map.get(uid, set())
        if role_codes & ROLE_CHAT_BOARD:
            board_users.append(item)
            continue

        membership = primary_membership_by_user.get(uid)
        unit = units_map.get(str(getattr(membership, "unit_id", "") or "").strip()) if membership else None

        if not unit:
            others.append(item)
            continue

        cap_do = int(getattr(unit, "cap_do", 0) or 0)

        # Người thuộc phòng, không thuộc tổ
        if cap_do == 2:
            dept_id = str(unit.id)
            dept_bucket = departments_map.setdefault(
                dept_id,
                {
                    "id": dept_id,
                    "name": getattr(unit, "ten_don_vi", None) or "Phòng",
                    "teams": {},
                    "direct_users": [],
                },
            )
            dept_bucket["direct_users"].append(item)
            continue

        # Người thuộc tổ
        if cap_do == 3:
            dept_id = str(getattr(unit, "parent_id", "") or "").strip()
            dept_unit = units_map.get(dept_id)
            if not dept_id or not dept_unit:
                others.append(item)
                continue

            dept_bucket = departments_map.setdefault(
                dept_id,
                {
                    "id": dept_id,
                    "name": getattr(dept_unit, "ten_don_vi", None) or "Phòng",
                    "teams": {},
                    "direct_users": [],
                },
            )

            team_id = str(unit.id)
            team_bucket = dept_bucket["teams"].setdefault(
                team_id,
                {
                    "id": team_id,
                    "name": getattr(unit, "ten_don_vi", None) or "Tổ",
                    "users": [],
                },
            )
            team_bucket["users"].append(item)
            continue

        others.append(item)

    board_users.sort(key=lambda x: x["label"].casefold())
    others.sort(key=lambda x: x["label"].casefold())

    departments: list[dict] = []
    for dept in departments_map.values():
        dept["direct_users"].sort(key=lambda x: x["label"].casefold())
        teams = list(dept["teams"].values())
        for team in teams:
            team["users"].sort(key=lambda x: x["label"].casefold())
        teams.sort(key=lambda x: x["name"].casefold())
        dept["teams"] = teams
        departments.append(dept)

    departments.sort(key=lambda x: x["name"].casefold())

    # Fallback: nếu vì lý do dữ liệu đơn vị không dựng được cây, vẫn phải hiện được danh sách user
    if not board_users and not departments and not others:
        fallback_users = []
        for user_obj in users or []:
            uid = str(getattr(user_obj, "id", "") or "").strip()
            if not uid:
                continue
            fallback_users.append(
                {
                    "id": uid,
                    "label": _chat_user_option_label(user_obj),
                    "search_text": (
                        f"{getattr(user_obj, 'full_name', '')} {getattr(user_obj, 'username', '')}"
                    ).strip().lower(),
                }
            )
        fallback_users.sort(key=lambda x: x["label"].casefold())
        others = fallback_users

    return {
        "board_users": board_users,
        "departments": departments,
        "others": others,
    }


def _ws_session_user_id(websocket: WebSocket) -> str | None:
    session = websocket.scope.get("session") or {}

    user_id = session.get("user_id")
    if user_id:
        return str(user_id)

    user_obj = session.get("user")
    if isinstance(user_obj, dict):
        uid = user_obj.get("id")
        if uid:
            return str(uid)

    uid = session.get("uid")
    if uid:
        return str(uid)

    return None
    
    
@router.get("/chat", response_class=HTMLResponse)
def chat_index(
    request: Request,
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)

    groups = get_user_groups(db, current_user.id)
    groups = enrich_groups_for_list(db, groups, current_user.id)

    return request.app.state.templates.TemplateResponse(
        "chat/index.html",
        {
            "request": request,
            "company_name": _company_name(),
            "app_name": _app_name(),
            "current_user": current_user,
            "current_user_display_name": get_display_name(current_user),
            "groups": groups,
            "active_group": None,
            "messages": [],
            "chat_notice": "Khung phân hệ chat đã sẵn sàng. Chúc làm việc vui vẻ.",
        },
    )


@router.get("/chat/{group_id}", response_class=HTMLResponse)
def chat_room(
    group_id: str,
    request: Request,
    db: Session = Depends(get_db),
):
    current_user = login_required(request, db)

    group = get_group_by_id(db, group_id)
    if not group:
        raise HTTPException(status_code=404, detail="Không tìm thấy nhóm chat.")

    if not is_group_member(db, group_id, current_user.id):
        raise HTTPException(status_code=403, detail="Bạn không thuộc nhóm chat này.")

    mark_group_as_read(db, group_id, current_user.id)

    groups = get_user_groups(db, current_user.id)
    groups = enrich_groups_for_list(db, groups, current_user.id)
    messages = get_group_messages(db, group_id, limit=100)
    group_members = get_group_members(db, group_id)
    available_users = get_available_users_for_group(db, group_id)
    available_users_tree = _build_available_users_tree(db, available_users)
    pinned_items = get_group_pinned_items(db, group_id)

    reaction_map = list_message_reactions(db, [m.id for m in messages])

    for msg in messages:
        msg.reaction_counts = reaction_map.get(msg.id, {"like": 0, "heart": 0, "laugh": 0})

    for msg in messages:
        if getattr(msg, "created_at", None):
            msg.created_at_vn = msg.created_at + timedelta(hours=7)
        else:
            msg.created_at_vn = None

        for att in getattr(msg, "attachments", []) or []:
            attachment_id = str(getattr(att, "id", "") or "")
            setattr(att, "preview_url", _chat_attachment_preview_url(attachment_id))
            setattr(att, "download_url", _chat_attachment_download_url(attachment_id))

    return request.app.state.templates.TemplateResponse(
        "chat/room.html",
        {
            "request": request,
            "company_name": _company_name(),
            "app_name": _app_name(),
            "current_user": current_user,
            "current_user_display_name": get_display_name(current_user),
            "groups": groups,
            "active_group": group,
            "messages": messages,
            "group_members": group_members,
            "available_users": available_users,
            "available_users_tree": available_users_tree,
            "pinned_items": pinned_items,
            "chat_notice": " Đây là giao diện phòng chat. Hãy gửi tin hoặc file để trao đổi công việc nhóm.",
        },
    )


@router.websocket("/ws/chat/groups/{group_id}")
async def websocket_chat_group(
    websocket: WebSocket,
    group_id: str,
):
    user_id = _ws_session_user_id(websocket)
    if not user_id:
        await websocket.close(code=1008)
        return

    db = SessionLocal()
    try:
        if not is_group_member(db, group_id, user_id):
            await websocket.close(code=1008)
            return
    finally:
        db.close()

    await manager.connect_group(group_id, websocket)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect_group(group_id, websocket)
    except Exception:
        manager.disconnect_group(group_id, websocket)
        try:
            await websocket.close()
        except Exception:
            pass
            
@router.websocket("/ws/chat/notify")
async def websocket_chat_notify(
    websocket: WebSocket,
):
    user_id = _ws_session_user_id(websocket)
    if not user_id:
        await websocket.close(code=1008)
        return

    await manager.connect_notify(user_id, websocket)

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect_notify(user_id, websocket)
    except Exception:
        manager.disconnect_notify(user_id, websocket)
        try:
            await websocket.close()
        except Exception:
            pass            