from __future__ import annotations

from typing import Dict, List

BLOCK_HANH_CHINH = "HANH_CHINH"
BLOCK_CHUYEN_MON = "CHUYEN_MON"

UNIT_CATEGORY_ROOT = "ROOT"
UNIT_CATEGORY_EXECUTIVE = "EXECUTIVE"
UNIT_CATEGORY_PHONG = "PHONG"
UNIT_CATEGORY_KHOA = "KHOA"
UNIT_CATEGORY_SUBUNIT = "SUBUNIT"

POSITION_DEFS: Dict[str, dict] = {
    "TONG_GIAM_DOC": {
        "label": "Tổng Giám đốc",
        "block_codes": [BLOCK_HANH_CHINH],
        "official_roles": ["ROLE_TONG_GIAM_DOC"],
        "menu_roles": ["ROLE_LANH_DAO"],
        "allowed_unit_categories": [UNIT_CATEGORY_ROOT],
        "allow_self_register": False,
        "power_rank": 100,
    },
    "PHO_TONG_GIAM_DOC_THUONG_TRUC": {
        "label": "Phó Tổng Giám đốc thường trực",
        "block_codes": [BLOCK_HANH_CHINH],
        "official_roles": ["ROLE_PHO_TONG_GIAM_DOC_THUONG_TRUC"],
        "menu_roles": ["ROLE_LANH_DAO"],
        "allowed_unit_categories": [UNIT_CATEGORY_ROOT],
        "allow_self_register": False,
        "power_rank": 95,
    },
    "PHO_TONG_GIAM_DOC": {
        "label": "Phó Tổng Giám đốc",
        "block_codes": [BLOCK_HANH_CHINH],
        "official_roles": ["ROLE_PHO_TONG_GIAM_DOC"],
        "menu_roles": ["ROLE_LANH_DAO"],
        "allowed_unit_categories": [UNIT_CATEGORY_ROOT],
        "allow_self_register": False,
        "power_rank": 90,
    },
    "GIAM_DOC": {
        "label": "Giám đốc",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_GIAM_DOC"],
        "menu_roles": [],
        "allowed_unit_categories": [UNIT_CATEGORY_EXECUTIVE],
        "allow_self_register": False,
        "power_rank": 80,
    },
    "PHO_GIAM_DOC_TRUC": {
        "label": "Phó Giám đốc trực",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_PHO_GIAM_DOC_TRUC"],
        "menu_roles": [],
        "allowed_unit_categories": [UNIT_CATEGORY_EXECUTIVE],
        "allow_self_register": False,
        "power_rank": 75,
    },
    "PHO_GIAM_DOC": {
        "label": "Phó Giám đốc",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_PHO_GIAM_DOC"],
        "menu_roles": [],
        "allowed_unit_categories": [UNIT_CATEGORY_EXECUTIVE],
        "allow_self_register": False,
        "power_rank": 70,
    },

    "NHAN_VIEN": {
        "label": "Nhân viên",
        "block_codes": [BLOCK_HANH_CHINH, BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_NHAN_VIEN"],
        "menu_roles": ["ROLE_NHAN_VIEN"],
        "allowed_unit_categories": [UNIT_CATEGORY_PHONG, UNIT_CATEGORY_KHOA, UNIT_CATEGORY_SUBUNIT],
        "allow_self_register": True,
        "power_rank": 10,
    },

    "TRUONG_PHONG": {
        "label": "Trưởng phòng",
        "block_codes": [BLOCK_HANH_CHINH],
        "official_roles": ["ROLE_TRUONG_PHONG"],
        "menu_roles": ["ROLE_TRUONG_PHONG"],
        "allowed_unit_categories": [UNIT_CATEGORY_PHONG],
        "allow_self_register": True,
        "power_rank": 60,
    },
    "PHO_PHONG": {
        "label": "Phó phòng",
        "block_codes": [BLOCK_HANH_CHINH],
        "official_roles": ["ROLE_PHO_PHONG"],
        "menu_roles": ["ROLE_PHO_PHONG"],
        "allowed_unit_categories": [UNIT_CATEGORY_PHONG],
        "allow_self_register": True,
        "power_rank": 55,
    },
    "TO_TRUONG": {
        "label": "Tổ trưởng",
        "block_codes": [BLOCK_HANH_CHINH],
        "official_roles": ["ROLE_TO_TRUONG"],
        "menu_roles": ["ROLE_TO_TRUONG"],
        "allowed_unit_categories": [UNIT_CATEGORY_SUBUNIT],
        "allow_self_register": True,
        "power_rank": 45,
    },
    "PHO_TO": {
        "label": "Tổ phó",
        "block_codes": [BLOCK_HANH_CHINH],
        "official_roles": ["ROLE_PHO_TO"],
        "menu_roles": ["ROLE_PHO_TO"],
        "allowed_unit_categories": [UNIT_CATEGORY_SUBUNIT],
        "allow_self_register": True,
        "power_rank": 40,
    },

    "TRUONG_KHOA": {
        "label": "Trưởng khoa",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_TRUONG_KHOA"],
        "menu_roles": [],
        "allowed_unit_categories": [UNIT_CATEGORY_KHOA],
        "allow_self_register": True,
        "power_rank": 60,
    },
    "PHO_KHOA": {
        "label": "Phó khoa",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_PHO_TRUONG_KHOA"],
        "menu_roles": [],
        "allowed_unit_categories": [UNIT_CATEGORY_KHOA],
        "allow_self_register": True,
        "power_rank": 55,
    },

    "KY_THUAT_VIEN_TRUONG": {
        "label": "Kỹ thuật viên trưởng",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_KY_THUAT_VIEN_TRUONG"],
        "menu_roles": ["ROLE_NHAN_VIEN"],
        "allowed_unit_categories": [UNIT_CATEGORY_KHOA],
        "allow_self_register": True,
        "power_rank": 35,
    },

    "BAC_SI": {
        "label": "Bác sĩ",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_BAC_SI"],
        "menu_roles": ["ROLE_NHAN_VIEN"],
        "allowed_unit_categories": [UNIT_CATEGORY_KHOA, UNIT_CATEGORY_SUBUNIT],
        "allow_self_register": True,
        "power_rank": 20,
    },
    "DIEU_DUONG_TRUONG": {
        "label": "Điều dưỡng trưởng",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_DIEU_DUONG_TRUONG"],
        "menu_roles": ["ROLE_NHAN_VIEN"],
        "allowed_unit_categories": [UNIT_CATEGORY_KHOA],
        "allow_self_register": True,
        "power_rank": 20,
    },
    "DIEU_DUONG": {
        "label": "Điều dưỡng",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_DIEU_DUONG"],
        "menu_roles": ["ROLE_NHAN_VIEN"],
        "allowed_unit_categories": [UNIT_CATEGORY_KHOA, UNIT_CATEGORY_SUBUNIT],
        "allow_self_register": True,
        "power_rank": 20,
    },
    "KY_THUAT_VIEN": {
        "label": "Kỹ thuật viên",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_KY_THUAT_VIEN"],
        "menu_roles": ["ROLE_NHAN_VIEN"],
        "allowed_unit_categories": [UNIT_CATEGORY_KHOA, UNIT_CATEGORY_SUBUNIT],
        "allow_self_register": True,
        "power_rank": 20,
    },
    "DUOC_SI": {
        "label": "Dược sĩ",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_DUOC_SI"],
        "menu_roles": ["ROLE_NHAN_VIEN"],
        "allowed_unit_categories": [UNIT_CATEGORY_KHOA, UNIT_CATEGORY_SUBUNIT],
        "allow_self_register": True,
        "power_rank": 20,
    },
    "THU_KY_Y_KHOA": {
        "label": "Thư ký y khoa",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_THU_KY_Y_KHOA"],
        "menu_roles": ["ROLE_NHAN_VIEN"],
        "allowed_unit_categories": [UNIT_CATEGORY_KHOA, UNIT_CATEGORY_SUBUNIT],
        "allow_self_register": True,
        "power_rank": 20,
    },
    "HO_LY": {
        "label": "Hộ lý",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_HO_LY"],
        "menu_roles": ["ROLE_NHAN_VIEN"],
        "allowed_unit_categories": [UNIT_CATEGORY_KHOA, UNIT_CATEGORY_SUBUNIT],
        "allow_self_register": True,
        "power_rank": 20,
    },

    "QL_CHAT_LUONG": {
        "label": "Quản lý chất lượng",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_QL_CHAT_LUONG"],
        "menu_roles": ["ROLE_NHAN_VIEN"],
        "allowed_unit_categories": [UNIT_CATEGORY_KHOA],
        "allow_self_register": True,
        "power_rank": 30,
    },
    "QL_KY_THUAT": {
        "label": "Quản lý kỹ thuật",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_QL_KY_THUAT"],
        "menu_roles": ["ROLE_NHAN_VIEN"],
        "allowed_unit_categories": [UNIT_CATEGORY_KHOA],
        "allow_self_register": True,
        "power_rank": 30,
    },
    "QL_AN_TOAN": {
        "label": "Quản lý an toàn",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_QL_AN_TOAN"],
        "menu_roles": ["ROLE_NHAN_VIEN"],
        "allowed_unit_categories": [UNIT_CATEGORY_KHOA],
        "allow_self_register": True,
        "power_rank": 30,
    },
    "QL_VAT_TU": {
        "label": "Quản lý vật tư",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_QL_VAT_TU"],
        "menu_roles": ["ROLE_NHAN_VIEN"],
        "allowed_unit_categories": [UNIT_CATEGORY_KHOA],
        "allow_self_register": True,
        "power_rank": 30,
    },
    "QL_TRANG_THIET_BI": {
        "label": "Quản lý trang thiết bị",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_QL_TRANG_THIET_BI"],
        "menu_roles": ["ROLE_NHAN_VIEN"],
        "allowed_unit_categories": [UNIT_CATEGORY_KHOA],
        "allow_self_register": True,
        "power_rank": 30,
    },
    "QL_MOI_TRUONG": {
        "label": "Quản lý môi trường",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_QL_MOI_TRUONG"],
        "menu_roles": ["ROLE_NHAN_VIEN"],
        "allowed_unit_categories": [UNIT_CATEGORY_KHOA],
        "allow_self_register": True,
        "power_rank": 30,
    },
    "QL_CNTT": {
        "label": "Quản lý CNTT",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_QL_CNTT"],
        "menu_roles": ["ROLE_NHAN_VIEN"],
        "allowed_unit_categories": [UNIT_CATEGORY_KHOA],
        "allow_self_register": True,
        "power_rank": 30,
    },

    "TRUONG_NHOM": {
        "label": "Trưởng nhóm",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_TRUONG_NHOM"],
        "menu_roles": ["ROLE_TO_TRUONG"],
        "allowed_unit_categories": [UNIT_CATEGORY_SUBUNIT],
        "allow_self_register": True,
        "power_rank": 45,
    },
    "PHO_NHOM": {
        "label": "Phó nhóm",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_PHO_NHOM"],
        "menu_roles": ["ROLE_PHO_TO"],
        "allowed_unit_categories": [UNIT_CATEGORY_SUBUNIT],
        "allow_self_register": True,
        "power_rank": 40,
    },
    "TRUONG_DON_VI": {
        "label": "Trưởng đơn vị",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_TRUONG_DON_VI"],
        "menu_roles": ["ROLE_TO_TRUONG"],
        "allowed_unit_categories": [UNIT_CATEGORY_SUBUNIT],
        "allow_self_register": True,
        "power_rank": 45,
    },
    "PHO_DON_VI": {
        "label": "Phó đơn vị",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_PHO_DON_VI"],
        "menu_roles": ["ROLE_PHO_TO"],
        "allowed_unit_categories": [UNIT_CATEGORY_SUBUNIT],
        "allow_self_register": True,
        "power_rank": 40,
    },
    "DIEU_DUONG_TRUONG_DON_VI": {
        "label": "Điều dưỡng trưởng đơn vị",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_DIEU_DUONG_TRUONG_DON_VI"],
        "menu_roles": ["ROLE_NHAN_VIEN"],
        "allowed_unit_categories": [UNIT_CATEGORY_SUBUNIT],
        "allow_self_register": True,
        "power_rank": 26,
    },
    "KY_THUAT_VIEN_TRUONG_DON_VI": {
        "label": "Kỹ thuật viên trưởng đơn vị",
        "block_codes": [BLOCK_CHUYEN_MON],
        "official_roles": ["ROLE_KY_THUAT_VIEN_TRUONG_DON_VI"],
        "menu_roles": ["ROLE_NHAN_VIEN"],
        "allowed_unit_categories": [UNIT_CATEGORY_SUBUNIT],
        "allow_self_register": True,
        "power_rank": 25,
    },
}

POSITION_LABELS = {code: cfg["label"] for code, cfg in POSITION_DEFS.items()}

def positions_for_block(block_code: str, self_register_only: bool = True) -> List[dict]:
    block_code = (block_code or "").strip().upper()
    data = []
    for code, cfg in POSITION_DEFS.items():
        if block_code not in cfg.get("block_codes", []):
            continue
        if self_register_only and not cfg.get("allow_self_register", False):
            continue
        data.append({"code": code, "label": cfg["label"]})
    return data

def position_label(code: str) -> str:
    return POSITION_LABELS.get((code or "").strip().upper(), "-")

def is_position_allowed_for_block(code: str, block_code: str, self_register_only: bool = True) -> bool:
    cfg = POSITION_DEFS.get((code or "").strip().upper())
    if not cfg:
        return False
    if self_register_only and not cfg.get("allow_self_register", False):
        return False
    return (block_code or "").strip().upper() in cfg.get("block_codes", [])

def allowed_unit_categories_for_position(code: str) -> List[str]:
    cfg = POSITION_DEFS.get((code or "").strip().upper())
    if not cfg:
        return []
    return list(cfg.get("allowed_unit_categories", []))