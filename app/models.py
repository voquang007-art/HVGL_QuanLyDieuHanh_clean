import enum
import uuid
from datetime import datetime, timedelta, date

from sqlalchemy import (
    Column, String, Enum, DateTime, Boolean, ForeignKey, Integer, Text, UniqueConstraint
)
from sqlalchemy.orm import relationship, synonym

from .database import Base

# =========================
# ENUMS dùng chung
# =========================

class UserStatus(str, enum.Enum):
    ACTIVE = "ACTIVE"                 # đang hoạt động
    PENDING_APPROVAL = "PENDING"      # chờ duyệt/kích hoạt
    LOCKED = "LOCKED"                 # bị khoá

class UnitStatus(str, enum.Enum):
    ACTIVE = "ACTIVE"
    RETIRED = "RETIRED"

class BlockCode(str, enum.Enum):
    HANH_CHINH = "HANH_CHINH"
    CHUYEN_MON = "CHUYEN_MON"

class UnitCategory(str, enum.Enum):
    ROOT = "ROOT"
    EXECUTIVE = "EXECUTIVE"
    PHONG = "PHONG"
    KHOA = "KHOA"
    SUBUNIT = "SUBUNIT"


class RoleCode(str, enum.Enum):
    ROLE_ADMIN = "ROLE_ADMIN"
    ROLE_LANH_DAO = "ROLE_LANH_DAO"

    ROLE_TONG_GIAM_DOC = "ROLE_TONG_GIAM_DOC"
    ROLE_PHO_TONG_GIAM_DOC_THUONG_TRUC = "ROLE_PHO_TONG_GIAM_DOC_THUONG_TRUC"
    ROLE_PHO_TONG_GIAM_DOC = "ROLE_PHO_TONG_GIAM_DOC"

    ROLE_GIAM_DOC = "ROLE_GIAM_DOC"
    ROLE_PHO_GIAM_DOC_TRUC = "ROLE_PHO_GIAM_DOC_TRUC"
    ROLE_PHO_GIAM_DOC = "ROLE_PHO_GIAM_DOC"

    ROLE_TRUONG_PHONG = "ROLE_TRUONG_PHONG"
    ROLE_PHO_PHONG = "ROLE_PHO_PHONG"
    ROLE_TO_TRUONG = "ROLE_TO_TRUONG"
    ROLE_PHO_TO = "ROLE_PHO_TO"

    ROLE_TRUONG_KHOA = "ROLE_TRUONG_KHOA"
    ROLE_PHO_TRUONG_KHOA = "ROLE_PHO_TRUONG_KHOA"

    ROLE_BAC_SI = "ROLE_BAC_SI"
    ROLE_DUOC_SI = "ROLE_DUOC_SI"
    ROLE_DIEU_DUONG_TRUONG = "ROLE_DIEU_DUONG_TRUONG"
    ROLE_KY_THUAT_VIEN_TRUONG = "ROLE_KY_THUAT_VIEN_TRUONG"
    ROLE_DIEU_DUONG = "ROLE_DIEU_DUONG"
    ROLE_KY_THUAT_VIEN = "ROLE_KY_THUAT_VIEN"
    ROLE_THU_KY_Y_KHOA = "ROLE_THU_KY_Y_KHOA"
    ROLE_HO_LY = "ROLE_HO_LY"

    ROLE_QL_CHAT_LUONG = "ROLE_QL_CHAT_LUONG"
    ROLE_QL_KY_THUAT = "ROLE_QL_KY_THUAT"
    ROLE_QL_AN_TOAN = "ROLE_QL_AN_TOAN"
    ROLE_QL_VAT_TU = "ROLE_QL_VAT_TU"
    ROLE_QL_TRANG_THIET_BI = "ROLE_QL_TRANG_THIET_BI"
    ROLE_QL_MOI_TRUONG = "ROLE_QL_MOI_TRUONG"
    ROLE_QL_CNTT = "ROLE_QL_CNTT"

    ROLE_TRUONG_NHOM = "ROLE_TRUONG_NHOM"
    ROLE_PHO_NHOM = "ROLE_PHO_NHOM"
    ROLE_TRUONG_DON_VI = "ROLE_TRUONG_DON_VI"
    ROLE_PHO_DON_VI = "ROLE_PHO_DON_VI"
    ROLE_DIEU_DUONG_TRUONG_DON_VI = "ROLE_DIEU_DUONG_TRUONG_DON_VI"
    ROLE_KY_THUAT_VIEN_TRUONG_DON_VI = "ROLE_KY_THUAT_VIEN_TRUONG_DON_VI"
    ROLE_NHAN_VIEN = "ROLE_NHAN_VIEN"
    

class ScopeCode(str, enum.Enum):
    ALL_UNITS = "ALL_UNITS"          # xem toàn hệ thống
    OWN_UNIT = "OWN_UNIT"            # chỉ chính đơn vị của mình
    OWN_UNIT_TREE = "OWN_UNIT_TREE"  # đơn vị + cây con

class VisibilityMode(str, enum.Enum):
    VIEW_ALL = "VIEW_ALL"                 # mở xem toàn bộ trong phạm vi grant
    FILES_ONLY = "FILES_ONLY"             # chỉ xem tab Tài liệu
    PLANS_ONLY = "PLANS_ONLY"             # chỉ xem tab Kế hoạch
    EVALUATION_ONLY = "EVALUATION_ONLY"   # chỉ xem tab Đánh giá

# ===== Trạng thái Kế hoạch =====
class PlanStatus(str, enum.Enum):
    DRAFT = "DRAFT"                  # bản nháp tại đơn vị
    SUBMITTED = "SUBMITTED"          # đã gửi lãnh đạo
    APPROVED = "APPROVED"            # lãnh đạo phê duyệt
    REJECTED = "REJECTED"            # trả về

# ===== Trạng thái Công việc =====
class TaskStatus(str, enum.Enum):
    NEW = "NEW"                      # mới giao
    IN_PROGRESS = "IN_PROGRESS"      # đang thực hiện
    DONE = "DONE"                    # hoàn thành (giữ tương thích cũ)
    SUBMITTED = "SUBMITTED"          # tuyến dưới báo hoàn thành
    CLOSED = "CLOSED"                # tuyến trên kết thúc/phê duyệt
    REJECTED = "REJECTED"            # tuyến trên trả về
    CANCELLED = "CANCELLED"          # huỷ

# =========================
# USERS
# =========================

class Users(Base):
    __tablename__ = "users"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    username = Column(String, unique=True, nullable=False, index=True)
    full_name = Column(String)
    email = Column(String)
    phone = Column(String)
    password_hash = Column(String, nullable=False)
    status = Column(Enum(UserStatus), nullable=False, default=UserStatus.PENDING_APPROVAL)
    requested_block_code = Column(Enum(BlockCode), nullable=True)
    requested_unit_id = Column(String, ForeignKey("units.id"), nullable=True)
    requested_subunit_id = Column(String, ForeignKey("units.id"), nullable=True)
    requested_position_code = Column(String, nullable=True)
    approved_block_code = Column(Enum(BlockCode), nullable=True)
    approved_unit_id = Column(String, ForeignKey("units.id"), nullable=True)
    approved_subunit_id = Column(String, ForeignKey("units.id"), nullable=True)
    approved_position_code = Column(String, nullable=True)
    approved_by_user_id = Column(String, ForeignKey("users.id"), nullable=True)
    approved_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    requested_unit = relationship("Units", foreign_keys=[requested_unit_id])
    requested_subunit = relationship("Units", foreign_keys=[requested_subunit_id])
    approved_unit = relationship("Units", foreign_keys=[approved_unit_id])
    approved_subunit = relationship("Units", foreign_keys=[approved_subunit_id])

    # --- PIN 6 số (hash) ---
    pin_hash = Column(String, nullable=True)
    pin_updated_at = Column(DateTime, nullable=True)

    memberships = relationship("UserUnitMemberships", back_populates="user", cascade="all, delete-orphan")
    user_roles = relationship("UserRoles", back_populates="user", cascade="all, delete-orphan")

# =========================
# UNITS (Phòng/Tổ)
# =========================

class Units(Base):
    __tablename__ = "units"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    ten_don_vi = Column(String, nullable=False)
    cap_do = Column(Integer, nullable=False)  # 1=root, 2=phòng/khoa, 3=tổ/nhóm hoặc đơn vị trực thuộc
    block_code = Column(Enum(BlockCode), nullable=True)
    unit_category = Column(Enum(UnitCategory), nullable=True, default=UnitCategory.SUBUNIT)
    parent_id = Column(String, ForeignKey("units.id"), nullable=True)
    path = Column(String, index=True)
    trang_thai = Column(Enum(UnitStatus), nullable=False, default=UnitStatus.ACTIVE)
    order_index = Column(Integer, nullable=False, default=0)

    parent = relationship("Units", remote_side=[id], backref="children")
    memberships = relationship("UserUnitMemberships", back_populates="unit", cascade="all, delete-orphan")

# =========================
# User-Unit Membership
# =========================

class UserUnitMemberships(Base):
    __tablename__ = "user_unit_memberships"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, ForeignKey("users.id"), nullable=False)
    unit_id = Column(String, ForeignKey("units.id"), nullable=False)
    is_primary = Column(Boolean, default=True)

    user = relationship("Users", back_populates="memberships")
    unit = relationship("Units", back_populates="memberships")

    __table_args__ = (
        UniqueConstraint("user_id", "unit_id", name="uq_user_unit"),
    )

# =========================
# Roles & UserRoles
# =========================

class Roles(Base):
    __tablename__ = "roles"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    code = Column(Enum(RoleCode), unique=True, nullable=False)
    name = Column(String, nullable=False)

    user_roles = relationship("UserRoles", back_populates="role", cascade="all, delete-orphan")

class UserRoles(Base):
    __tablename__ = "user_roles"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, ForeignKey("users.id"), nullable=False)
    role_id = Column(String, ForeignKey("roles.id"), nullable=False)
    scope_code = Column(Enum(ScopeCode), nullable=True)

    user = relationship("Users", back_populates="user_roles")
    role = relationship("Roles", back_populates="user_roles")

    __table_args__ = (
        UniqueConstraint("user_id", "role_id", name="uq_user_role"),
    )

# =========================
# Committees / Ban kiêm nhiệm
# =========================

class Committees(Base):
    __tablename__ = "committees"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=False)
    code = Column(String, nullable=True, unique=True, index=True)
    committee_type = Column(String, nullable=False, default="BAN")
    decision_no = Column(String, nullable=True)
    decision_date = Column(DateTime, nullable=True)
    description = Column(Text, nullable=True)
    managed_by = Column(String, nullable=False, default="HDTV")
    created_by = Column(String, ForeignKey("users.id"), nullable=True)
    start_date = Column(DateTime, nullable=True)
    end_date = Column(DateTime, nullable=True)
    status = Column(String, nullable=False, default="ACTIVE")
    is_active = Column(Boolean, default=True)

    allow_documents = Column(Boolean, default=True)
    allow_plans = Column(Boolean, default=True)
    allow_tasks = Column(Boolean, default=True)
    allow_draft_approval = Column(Boolean, default=True)
    allow_meetings = Column(Boolean, default=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

    creator = relationship("Users", foreign_keys=[created_by])
    members = relationship("CommitteeMembers", back_populates="committee", cascade="all, delete-orphan")


class CommitteeMembers(Base):
    __tablename__ = "committee_members"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    committee_id = Column(String, ForeignKey("committees.id"), nullable=False)
    user_id = Column(String, ForeignKey("users.id"), nullable=False)
    committee_role = Column(String, nullable=False, default="THANH_VIEN")
    member_title = Column(String, nullable=True)
    joined_at = Column(DateTime, default=datetime.utcnow)
    left_at = Column(DateTime, nullable=True)
    is_active = Column(Boolean, default=True)
    added_by = Column(String, ForeignKey("users.id"), nullable=True)
    note = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

    committee = relationship("Committees", back_populates="members")
    user = relationship("Users", foreign_keys=[user_id])
    added_by_user = relationship("Users", foreign_keys=[added_by])

    __table_args__ = (
        UniqueConstraint("committee_id", "user_id", "is_active", name="uq_committee_active_member"),
    )

# =========================
# Visibility Grants
# =========================

# =========================
# Visibility Grants
# =========================

class VisibilityGrants(Base):
    __tablename__ = "visibility_grants"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    grantee_unit_id = Column(String, ForeignKey("units.id"), nullable=False)
    mode = Column(Enum(VisibilityMode), nullable=False, default=VisibilityMode.VIEW_ALL)
    effective_from = Column(DateTime, nullable=True)
    effective_to = Column(DateTime, nullable=True)

    grantee_unit = relationship("Units")
    
    
class VisibilityUserGrants(Base):
    __tablename__ = "visibility_user_grants"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))

    # User được phép xem
    grantee_user_id = Column(String, ForeignKey("users.id"), nullable=False)

    # Phạm vi đơn vị mà user này được phép xem
    target_unit_id = Column(String, ForeignKey("units.id"), nullable=False)

    # Loại quyền xem
    mode = Column(Enum(VisibilityMode), nullable=False, default=VisibilityMode.VIEW_ALL)

    effective_from = Column(DateTime, nullable=True)
    effective_to = Column(DateTime, nullable=True)

    grantee_user = relationship("Users", foreign_keys=[grantee_user_id])
    target_unit = relationship("Units", foreign_keys=[target_unit_id])

    __table_args__ = (
        UniqueConstraint(
            "grantee_user_id",
            "target_unit_id",
            "mode",
            "effective_from",
            "effective_to",
            name="uq_visibility_user_grant",
        ),
    )
# =========================
# Files (Tài liệu)
# =========================

class Files(Base):
    __tablename__ = "files"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    original_name = Column(String, nullable=False)
    path = Column(String, nullable=False)
    mime_type = Column(String, nullable=True)
    size_bytes = Column(Integer, default=0)
    owner_id = Column(String, ForeignKey("users.id"), nullable=True)
    unit_id = Column(String, ForeignKey("units.id"), nullable=True)
    scope_type = Column(String, nullable=False, default="UNIT", index=True)
    committee_id = Column(String, ForeignKey("committees.id"), nullable=True, index=True)
    uploaded_at = Column(DateTime, default=datetime.utcnow)
    is_deleted = Column(Boolean, default=False)

    owner = relationship("Users")
    unit = relationship("Units")
    committee = relationship("Committees")

    file_name = synonym("original_name")
    filename = synonym("original_name")
    file_path = synonym("path")
    stored_path = synonym("path")
    uploader_id = synonym("owner_id")
    created_at = synonym("uploaded_at")

# =========================
# SecretSessions (mở khoá phiên cho “Khóa bí mật”)
# =========================

class SecretSessions(Base):
    __tablename__ = "secret_sessions"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, ForeignKey("users.id"), nullable=False)

    # === BỔ SUNG để khớp secret_lock.py ===
    # secret_lock.py tạo SecretSessions(..., factor_type=..., issued_at=..., action_scope=...)
    factor_type = Column(String, nullable=True)           # 'PIN' / 'TOTP' / ...
    action_scope = Column(String, nullable=True)          # ví dụ: 'ASSIGN_TASK_DOWNSTREAM'
    issued_at = Column(DateTime, default=datetime.utcnow) # thời điểm cấp “mở khóa”

    # Giữ tương thích cũ
    required_role = Column(Enum(RoleCode), nullable=True)
    unit_id = Column(String, ForeignKey("units.id"), nullable=True)
    unlocked = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=True)

    user = relationship("Users")
    unit = relationship("Units")

# =========================
# PLANS (Kế hoạch tháng)
# =========================
# Synonym tương thích:
# - year/month <-> nam/thang
# - title      <-> ten_ke_hoach
# - description <-> noi_dung
# - status     <-> trang_thai
# - unit_id    <-> don_vi_id

class Plans(Base):
    __tablename__ = "plans"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    unit_id = Column(String, ForeignKey("units.id"), nullable=False)
    scope_type = Column(String, nullable=False, default="UNIT", index=True)
    committee_id = Column(String, ForeignKey("committees.id"), nullable=True, index=True)
    year = Column(Integer, nullable=False)
    month = Column(Integer, nullable=False)
    title = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    plan_kind = Column(String, nullable=True)   # PHONG / TO / NHANVIEN / COMMITTEE
    status = Column(Enum(PlanStatus), nullable=False, default=PlanStatus.DRAFT)
    created_by = Column(String, ForeignKey("users.id"), nullable=False)
    approved_by = Column(String, ForeignKey("users.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

    unit = relationship("Units")
    committee = relationship("Committees", foreign_keys=[committee_id])
    creator = relationship("Users", foreign_keys=[created_by])
    approver = relationship("Users", foreign_keys=[approved_by])

    nam = synonym("year")
    thang = synonym("month")
    ten_ke_hoach = synonym("title")
    noi_dung = synonym("description")
    trang_thai = synonym("status")
    don_vi_id = synonym("unit_id")

class PlanItems(Base):
    __tablename__ = "plan_items"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    plan_id = Column(String, ForeignKey("plans.id"), nullable=False)

    # Nội dung hiển thị + tag trạng thái/thời gian đang dùng hiện tại
    content = Column(Text, nullable=False)
    due_date = Column(DateTime, nullable=True)

    # ===== BỔ SUNG NỀN DỮ LIỆU KẾ THỪA / CHUYỂN KỲ =====
    # Mã định danh ổn định của cùng một công việc xuyên các kỳ
    work_key = Column(String, nullable=True, index=True)

    # Dòng nguồn gần nhất mà dòng hiện tại được kế thừa từ đó
    source_item_id = Column(String, ForeignKey("plan_items.id"), nullable=True)

    # Số lần công việc đã bị chuyển kỳ sau tính đến dòng hiện tại
    carried_forward_count = Column(Integer, nullable=False, default=0)

    # Cờ quản trị: công việc này đã từng bị chuyển kỳ sau hay chưa
    was_ever_carried_forward = Column(Boolean, nullable=False, default=False)

    assignee_unit_id = Column(String, ForeignKey("units.id"), nullable=True)
    assignee_user_id = Column(String, ForeignKey("users.id"), nullable=True)
    progress_pct = Column(Integer, default=0)
    note = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

    plan = relationship("Plans")
    assignee_unit = relationship("Units", foreign_keys=[assignee_unit_id])
    assignee_user = relationship("Users", foreign_keys=[assignee_user_id])

    # Quan hệ tự tham chiếu để lần theo chuỗi kế thừa
    source_item = relationship("PlanItems", remote_side=[id], foreign_keys=[source_item_id])

    noi_dung = synonym("content")
    han_hoan_thanh = synonym("due_date")
    don_vi_giao = synonym("assignee_unit_id")
    nguoi_giao = synonym("assignee_user_id")
    ti_le_hoan_thanh = synonym("progress_pct")

# =========================
# TASKS (Giao việc tuyến dọc)
# =========================

class Tasks(Base):
    __tablename__ = "tasks"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    unit_id = Column(String, ForeignKey("units.id"), nullable=False)
    scope_type = Column(String, nullable=False, default="UNIT", index=True)
    committee_id = Column(String, ForeignKey("committees.id"), nullable=True, index=True)
    title = Column(String, nullable=False)
    description = Column(Text, nullable=True)
    due_date = Column(DateTime, nullable=True)
    status = Column(Enum(TaskStatus), nullable=False, default=TaskStatus.NEW)
    created_by = Column(String, ForeignKey("users.id"), nullable=False)
    assigned_to_user_id = Column(String, ForeignKey("users.id"), nullable=True)
    assigned_to_unit_id = Column(String, ForeignKey("units.id"), nullable=True)
    parent_task_id = Column(String, ForeignKey("tasks.id"), nullable=True)
    submitted_by = Column(String, ForeignKey("users.id"), nullable=True)
    submitted_at = Column(DateTime, nullable=True)
    closed_by = Column(String, ForeignKey("users.id"), nullable=True)
    closed_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

    unit = relationship("Units", foreign_keys=[unit_id])
    committee = relationship("Committees", foreign_keys=[committee_id])
    creator = relationship("Users", foreign_keys=[created_by])
    assignee_user = relationship("Users", foreign_keys=[assigned_to_user_id])
    assignee_unit = relationship("Units", foreign_keys=[assigned_to_unit_id])
    parent_task = relationship("Tasks", remote_side=[id])
    reports = relationship("TaskReports", back_populates="task", cascade="all, delete-orphan")

    tieu_de = synonym("title")
    noi_dung = synonym("description")
    trang_thai = synonym("status")
    han_hoan_thanh = synonym("due_date")

class TaskReports(Base):
    __tablename__ = "task_reports"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    task_id = Column(String, ForeignKey("tasks.id"), nullable=False)
    reported_by = Column(String, ForeignKey("users.id"), nullable=False)
    reported_at = Column(DateTime, default=datetime.utcnow)
    note = Column(Text, nullable=True)
    progress_pct = Column(Integer, nullable=True)
    status_snapshot = Column(String, nullable=True)
    ack_by = Column(String, ForeignKey("users.id"), nullable=True)
    ack_at = Column(DateTime, nullable=True)

    task = relationship("Tasks", back_populates="reports")


# =========================
# DOCUMENT DRAFT APPROVALS
# =========================

class DocumentDrafts(Base):
    __tablename__ = "document_drafts"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    title = Column(String, nullable=False)
    document_type = Column(String, nullable=True)
    summary = Column(Text, nullable=True)
    created_by = Column(String, ForeignKey("users.id"), nullable=False)
    created_unit_id = Column(String, ForeignKey("units.id"), nullable=True)
    scope_type = Column(String, nullable=False, default="UNIT", index=True)
    committee_id = Column(String, ForeignKey("committees.id"), nullable=True, index=True)
    current_status = Column(String, nullable=False, default="DRAFT")
    current_handler_user_id = Column(String, ForeignKey("users.id"), nullable=True)
    current_handler_unit_id = Column(String, ForeignKey("units.id"), nullable=True)
    current_role_code = Column(String, nullable=True)
    last_submitter_id = Column(String, ForeignKey("users.id"), nullable=True)
    last_submitted_at = Column(DateTime, nullable=True)
    finished_at = Column(DateTime, nullable=True)
    is_deleted = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow)

    creator = relationship("Users", foreign_keys=[created_by])
    current_handler = relationship("Users", foreign_keys=[current_handler_user_id])
    last_submitter = relationship("Users", foreign_keys=[last_submitter_id])
    created_unit = relationship("Units", foreign_keys=[created_unit_id])
    current_handler_unit = relationship("Units", foreign_keys=[current_handler_unit_id])
    committee = relationship("Committees", foreign_keys=[committee_id])


class DocumentDraftFiles(Base):
    __tablename__ = "document_draft_files"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    draft_id = Column(String, ForeignKey("document_drafts.id"), nullable=False)
    file_name = Column(String, nullable=False)
    file_path = Column(String, nullable=False)
    mime_type = Column(String, nullable=True)
    size_bytes = Column(Integer, default=0)
    file_role = Column(String, nullable=True)
    uploaded_by = Column(String, ForeignKey("users.id"), nullable=True)
    uploaded_at = Column(DateTime, default=datetime.utcnow)
    is_active = Column(Boolean, default=True)
    is_deleted = Column(Boolean, default=False)

    draft = relationship("DocumentDrafts")
    uploader = relationship("Users")


class DocumentDraftActions(Base):
    __tablename__ = "document_draft_actions"
    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    draft_id = Column(String, ForeignKey("document_drafts.id"), nullable=False)
    action_type = Column(String, nullable=False)
    from_user_id = Column(String, ForeignKey("users.id"), nullable=True)
    to_user_id = Column(String, ForeignKey("users.id"), nullable=True)
    from_unit_id = Column(String, ForeignKey("units.id"), nullable=True)
    to_unit_id = Column(String, ForeignKey("units.id"), nullable=True)
    comment = Column(Text, nullable=True)
    linked_file_id = Column(String, ForeignKey("document_draft_files.id"), nullable=True)
    is_pending = Column(Boolean, default=False)
    response_text = Column(Text, nullable=True)
    responded_at = Column(DateTime, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    draft = relationship("DocumentDrafts")
    from_user = relationship("Users", foreign_keys=[from_user_id])
    to_user = relationship("Users", foreign_keys=[to_user_id])
    from_unit = relationship("Units", foreign_keys=[from_unit_id])
    to_unit = relationship("Units", foreign_keys=[to_unit_id])
    linked_file = relationship("DocumentDraftFiles")

# =========================
# CHAT MODELS (giai đoạn 1)
# =========================
from .chat.models import (
    ChatGroups,
    ChatGroupMembers,
    ChatMessages,
    ChatMessageLikes,
    ChatAttachments,
)