# -*- coding: utf-8 -*-
from fastapi import APIRouter, Request, Depends, Form, HTTPException, status
from starlette.templating import Jinja2Templates
from starlette.responses import RedirectResponse
from sqlalchemy.orm import Session
import os
import sys
import json
import datetime as dt
import secrets

from ..security.deps import get_db, login_required
from ..models import Users, Units, UnitStatus  # dùng khi reset PIN và liên kết đơn vị
from ..security.scope import is_all_units_access  # coi quyền xem toàn đơn vị như admin

# hash_password: nếu không có → tắt tính năng PIN
try:
    from ..security.crypto import hash_password
except Exception:
    hash_password = None

router = APIRouter(prefix="/account/secrets", tags=["account-secrets"])

# Templating
TEMPLATES_DIR = os.path.join(os.path.dirname(__file__), "..", "templates")
templates = Jinja2Templates(directory=TEMPLATES_DIR)


# =========================
# THƯ MỤC LƯU KHÓA / CONFIG
# =========================
def _runtime_root_dir() -> str:
    """
    Thư mục gốc vận hành:
    - Khi chạy bản đóng gói (.exe): theo thư mục chứa executable
    - Khi chạy source/dev: theo BASE_DIR của dự án
    """
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


RUNTIME_ROOT_DIR = _runtime_root_dir()
APP_DATA_DIR = os.path.join(RUNTIME_ROOT_DIR, "data")
DEFAULT_KEYS_DIR = os.path.join(APP_DATA_DIR, "keys")
STORE_CONFIG_PATH = os.path.join(APP_DATA_DIR, "secret_store_config.json")


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _normalize_storage_dir(raw: str | None) -> str:
    """
    Chuẩn hóa thư mục lưu khóa.
    - Nếu rỗng: dùng thư mục dữ liệu vận hành mặc định, không dùng đường dẫn source/dev cứng.
    - Nếu có giá trị: chuẩn hóa về absolute path.
    """
    val = (raw or "").strip()
    if not val:
        return DEFAULT_KEYS_DIR
    return os.path.abspath(os.path.expanduser(val))


def _load_store_config() -> dict:
    try:
        if os.path.exists(STORE_CONFIG_PATH):
            with open(STORE_CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
                if isinstance(data, dict):
                    return data
    except Exception:
        pass
    return {}


def _save_store_config(storage_dir: str) -> None:
    _ensure_dir(APP_DATA_DIR)
    with open(STORE_CONFIG_PATH, "w", encoding="utf-8") as f:
        json.dump({"storage_dir": storage_dir}, f, ensure_ascii=False, indent=2)


def _build_storage_choices(saved_dir: str | None = None) -> list[dict]:
    """
    Danh sách thư mục có thể CHỌN trên giao diện web.
    Lưu ý: web app không thể mở hộp thoại duyệt thư mục của máy chủ như app desktop,
    nên dùng danh sách đường dẫn vận hành an toàn để người dùng chọn.
    """
    choices = []
    seen = set()

    def add(path: str, label: str):
        norm = _normalize_storage_dir(path)
        if norm not in seen:
            seen.add(norm)
            choices.append({"value": norm, "label": label})

    add(DEFAULT_KEYS_DIR, "Mặc định ứng dụng (cạnh thư mục chạy exe / data/keys)")

    user_home = os.path.expanduser("~")
    documents_dir = os.path.join(user_home, "Documents", "QLCV_App", "keys")
    add(documents_dir, "Tài liệu người dùng (Documents\\QLCV_App\\keys)")

    desktop_dir = os.path.join(user_home, "Desktop", "QLCV_App", "keys")
    add(desktop_dir, "Desktop người dùng (Desktop\\QLCV_App\\keys)")

    if saved_dir:
        add(saved_dir, "Thư mục đang dùng / đã lưu")

    return choices


def _get_storage_dir(requested_dir: str | None = None) -> str:
    """
    Xác định thư mục lưu khóa theo thứ tự:
    1) thư mục vừa CHỌN trên form
    2) thư mục đã lưu trong secret_store_config.json
    3) thư mục mặc định dữ liệu vận hành
    """
    if requested_dir and str(requested_dir).strip():
        storage_dir = _normalize_storage_dir(requested_dir)
        _ensure_dir(storage_dir)
        _save_store_config(storage_dir)
        return storage_dir

    cfg = _load_store_config()
    saved = _normalize_storage_dir(cfg.get("storage_dir"))
    _ensure_dir(saved)
    return saved


def _get_store_paths(storage_dir: str | None = None) -> dict:
    current_dir = _get_storage_dir(storage_dir)
    return {
        "storage_dir": current_dir,
        "global_key_path": os.path.join(current_dir, "position_secret.key"),
        "unit_keys_path": os.path.join(current_dir, "position_secrets.json"),
        "log_path": os.path.join(current_dir, "migrate.log"),
    }


# =========================
# TIỆN ÍCH / LOG / SECRET
# =========================
def _now_utc_str():
    return dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _log(line: str, storage_dir: str | None = None):
    paths = _get_store_paths(storage_dir)
    with open(paths["log_path"], "a", encoding="utf-8") as f:
        f.write(line.rstrip() + "\n")


def _generate_easy_secret(length: int = 10) -> str:
    """
    Sinh mã dễ nhập:
    - tối đa 10 ký tự
    - chỉ gồm chữ in hoa và số
    - loại các ký tự dễ nhầm: 0/O, 1/I/L
    """
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(secrets.choice(alphabet) for _ in range(length))


# --- QUYỀN TRUY CẬP: chấp nhận các biến thể admin thực tế ---
_ALLOWED_ADMIN_TOKENS = {"admin", "lanhdao", "tphc", "sysadmin", "quantri"}


def _is_admin_profile(user) -> bool:
    # cờ boolean phổ biến
    for flag in ("is_admin", "is_superuser", "is_staff"):
        if hasattr(user, flag) and bool(getattr(user, flag)):
            return True
    # id=1 thường là superuser
    try:
        if int(getattr(user, "id", 0)) == 1:
            return True
    except Exception:
        pass
    # username đặc biệt
    uname = (getattr(user, "username", "") or "").strip().lower()
    if uname in _ALLOWED_ADMIN_TOKENS:
        return True
    # các thuộc tính ký hiệu vai trò
    for attr in ("role", "group_code", "position_code", "title_code"):
        if hasattr(user, attr) and getattr(user, attr):
            val = str(getattr(user, attr)).strip().lower()
            if val in _ALLOWED_ADMIN_TOKENS:
                return True
            for tk in _ALLOWED_ADMIN_TOKENS:
                if tk in val:
                    return True
    return False


def _assert_admin(user, db: Session):
    try:
        has_all_units = is_all_units_access(db, user)
    except Exception:
        has_all_units = False
    if not (_is_admin_profile(user) or has_all_units):
        raise HTTPException(status_code=403, detail="Chỉ quản trị mới được truy cập.")


def _load_unit_options(db: Session) -> list[dict]:
    """
    Lấy danh sách Phòng/Tổ đang ACTIVE từ bảng Units để render dropdown chọn đơn vị cấp khóa.
    Chỉ cho phép cấp khóa cho:
    - cap_do = 2: Phòng
    - cap_do = 3: Tổ
    """
    units = (
        db.query(Units)
        .filter(Units.trang_thai == UnitStatus.ACTIVE, Units.cap_do.in_([2, 3]))
        .order_by(Units.cap_do, Units.order_index, Units.ten_don_vi)
        .all()
    )

    parent_map = {}
    parent_ids = {u.parent_id for u in units if getattr(u, "parent_id", None)}
    if parent_ids:
        parents = db.query(Units).filter(Units.id.in_(list(parent_ids))).all()
        parent_map = {p.id: p for p in parents}

    options = []
    for u in units:
        if u.cap_do == 2:
            label = f"Phòng: {u.ten_don_vi}"
        else:
            parent_name = ""
            parent = parent_map.get(u.parent_id)
            if parent and getattr(parent, "ten_don_vi", None):
                parent_name = parent.ten_don_vi
            label = f"Tổ: {u.ten_don_vi}"
            if parent_name:
                label += f" (thuộc {parent_name})"
        options.append({
            "value": u.id,
            "label": label,
            "unit_name": u.ten_don_vi,
            "cap_do": u.cap_do,
            "parent_id": u.parent_id,
        })
    return options


def _build_unit_key_rows(unit_keys: dict, db: Session) -> list[dict]:
    rows = []
    if not unit_keys:
        return rows

    unit_ids = list(unit_keys.keys())
    units = db.query(Units).filter(Units.id.in_(unit_ids)).all()
    unit_map = {u.id: u for u in units}

    parent_ids = {u.parent_id for u in units if getattr(u, "parent_id", None)}
    parent_map = {}
    if parent_ids:
        parents = db.query(Units).filter(Units.id.in_(list(parent_ids))).all()
        parent_map = {p.id: p for p in parents}

    for uid in sorted(unit_keys.keys()):
        raw = unit_keys.get(uid)

        if isinstance(raw, dict):
            stored_secret = (raw.get("secret") or "").strip()
            stored_unit_name = (raw.get("unit_name") or "").strip()
            stored_unit_type = (raw.get("unit_type") or "").strip()
            stored_parent_name = (raw.get("parent_unit_name") or "").strip()
        else:
            stored_secret = str(raw or "").strip()
            stored_unit_name = ""
            stored_unit_type = ""
            stored_parent_name = ""

        u = unit_map.get(uid)

        if stored_unit_name:
            label = f"{stored_unit_type}: {stored_unit_name}" if stored_unit_type else stored_unit_name
            if stored_parent_name:
                label += f" (thuộc {stored_parent_name})"
        elif u:
            if u.cap_do == 2:
                label = f"Phòng: {u.ten_don_vi}"
            elif u.cap_do == 3:
                parent_name = ""
                parent = parent_map.get(u.parent_id)
                if parent and getattr(parent, "ten_don_vi", None):
                    parent_name = parent.ten_don_vi
                label = f"Tổ: {u.ten_don_vi}"
                if parent_name:
                    label += f" (thuộc {parent_name})"
            else:
                label = f"Đơn vị cấp {u.cap_do}: {u.ten_don_vi}"
        else:
            label = f"Đơn vị không còn tồn tại / không tìm thấy ({uid})"

        rows.append({
            "unit_id": uid,
            "label": label,
            "secret": stored_secret,
        })

    return rows


@router.get("")
def secrets_home(request: Request, db: Session = Depends(get_db)):
    user = login_required(request, db)
    _assert_admin(user, db)

    paths = _get_store_paths()
    global_key_exists = os.path.exists(paths["global_key_path"])
    unit_keys = {}
    if os.path.exists(paths["unit_keys_path"]):
        try:
            with open(paths["unit_keys_path"], "r", encoding="utf-8") as f:
                unit_keys = json.load(f) or {}
        except Exception:
            unit_keys = {}

    features = {
        "pin_supported": bool(hash_password) and hasattr(Users, "pin_hash"),
        "totp_supported": hasattr(Users, "totp_seed"),
        "recovery_supported": hasattr(Users, "recovery_codes") or hasattr(Users, "recovery_codes_json"),
    }
    unit_options = _load_unit_options(db)
    unit_key_rows = _build_unit_key_rows(unit_keys, db)

    return templates.TemplateResponse("account_secrets.html", {
        "request": request,
        "global_key_exists": global_key_exists,
        "global_key_path": paths["global_key_path"],
        "unit_keys": unit_keys,
        "unit_key_rows": unit_key_rows,
        "unit_options": unit_options,
        "unit_keys_path": paths["unit_keys_path"],
        "log_path": paths["log_path"],
        "storage_dir": paths["storage_dir"],
        "store_config_path": STORE_CONFIG_PATH,
        "storage_choices": _build_storage_choices(paths["storage_dir"]),
        "features": features,
    })


@router.post("/generate-global")
def generate_global_secret(
    request: Request,
    db: Session = Depends(get_db),
    force: int = Form(0),
    storage_dir: str = Form(""),
):
    user = login_required(request, db)
    _assert_admin(user, db)

    paths = _get_store_paths(storage_dir)
    if os.path.exists(paths["global_key_path"]) and not force:
        return RedirectResponse(url="/account/secrets", status_code=status.HTTP_303_SEE_OTHER)

    secret = _generate_easy_secret(10)
    with open(paths["global_key_path"], "w", encoding="utf-8") as f:
        f.write(secret + "\n")
    _log(
        f"[{_now_utc_str()}] generate_global_position_secret by={getattr(user,'username',user.id)} "
        f"force={force} storage_dir={paths['storage_dir']}",
        paths["storage_dir"],
    )
    return RedirectResponse(url="/account/secrets", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/generate-unit")
def generate_unit_secret(
    request: Request,
    unit_id: str = Form(...),
    storage_dir: str = Form(""),
    db: Session = Depends(get_db),
):
    user = login_required(request, db)
    _assert_admin(user, db)

    unit_id = (unit_id or "").strip()
    if not unit_id:
        raise HTTPException(status_code=400, detail="Thiếu unit_id.")

    target_unit = db.get(Units, unit_id)
    if not target_unit or getattr(target_unit, "trang_thai", None) != UnitStatus.ACTIVE:
        raise HTTPException(status_code=400, detail="Đơn vị không tồn tại hoặc không còn hoạt động.")
    if getattr(target_unit, "cap_do", None) not in (2, 3):
        raise HTTPException(status_code=400, detail="Chỉ được cấp khóa cho Phòng hoặc Tổ.")

    paths = _get_store_paths(storage_dir)
    data = {}
    if os.path.exists(paths["unit_keys_path"]):
        try:
            with open(paths["unit_keys_path"], "r", encoding="utf-8") as f:
                data = json.load(f) or {}
        except Exception:
            data = {}

    secret = _generate_easy_secret(10)

    parent_name = ""
    if getattr(target_unit, "parent_id", None):
        parent = db.get(Units, target_unit.parent_id)
        if parent and getattr(parent, "ten_don_vi", None):
            parent_name = parent.ten_don_vi

    data[unit_id] = {
        "secret": secret,
        "unit_name": getattr(target_unit, "ten_don_vi", "") or "",
        "unit_type": "Phòng" if getattr(target_unit, "cap_do", None) == 2 else "Tổ",
        "parent_unit_name": parent_name,
        "issued_at": _now_utc_str(),
    }

    with open(paths["unit_keys_path"], "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    _log(
        f"[{_now_utc_str()}] generate_unit_position_secret unit_id={unit_id} unit_name={getattr(target_unit,'ten_don_vi','')} "
        f"cap_do={getattr(target_unit,'cap_do','')} by={getattr(user,'username',user.id)} storage_dir={paths['storage_dir']}",
        paths["storage_dir"],
    )
    return RedirectResponse(url="/account/secrets", status_code=status.HTTP_303_SEE_OTHER)


@router.post("/reset-pin")
def reset_pin_for_user(
    request: Request,
    user_id: str = Form(None),
    username: str = Form(None),
    db: Session = Depends(get_db),
):
    """
    Cấp lại PIN 6 số cho 1 user quản lý.
    - Nhận user_id HOẶC username (điền một trong hai).
    - Lưu HASH vào Users.pin_hash (không lưu PIN thô).
    - Sau khi cấp: HIỂN THỊ PIN **MỘT LẦN** trên trang cho admin copy và chuyển cho người giao việc.
    """
    admin = login_required(request, db)
    _assert_admin(admin, db)

    if not (hash_password and hasattr(Users, "pin_hash")):
        raise HTTPException(status_code=400, detail="Hệ thống chưa hỗ trợ đặt PIN (thiếu pin_hash hoặc crypto).")
    if not user_id and not username:
        raise HTTPException(status_code=400, detail="Thiếu user_id hoặc username.")

    # Tìm user mục tiêu
    target = None
    if user_id:
        target = db.get(Users, user_id)
    if not target and username:
        target = db.query(Users).filter(Users.username == username).first()
    if not target:
        raise HTTPException(status_code=404, detail="Không tìm thấy người dùng.")

    # Sinh PIN 6 số, tránh các mẫu quá dễ đoán
    import random
    import string
    bad = {"000000", "111111", "222222", "333333", "444444", "555555", "666666", "777777", "888888", "999999", "123456", "654321"}
    while True:
        pin = "".join(random.choice(string.digits) for _ in range(6))
        if pin not in bad:
            break

    # Ghi HASH vào DB (không lưu PIN thô)
    target.pin_hash = hash_password(pin)
    if hasattr(target, "pin_updated_at"):
        from datetime import datetime as _dt
        target.pin_updated_at = _dt.utcnow()
    db.add(target)
    db.commit()

    _log(f"[{_now_utc_str()}] reset_pin user_id={getattr(target,'id',None)} by={getattr(admin,'username',admin.id)}")

    # Trả về trang HIỂN THỊ PIN MỘT LẦN cho admin copy
    return templates.TemplateResponse("account_pin_issued.html", {
        "request": request,
        "target_id": getattr(target, "id", None),
        "target_username": getattr(target, "username", None),
        "target_fullname": getattr(target, "full_name", None),
        "pin": pin,  # hiển thị 1 lần; không lưu plaintext ở bất cứ đâu
    })
