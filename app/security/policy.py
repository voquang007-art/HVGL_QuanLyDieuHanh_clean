from enum import Enum

class ActionCode(str, Enum):
    APPROVE_DEPT_PLAN = "APPROVE_DEPT_PLAN"
    GRANT_VIEW_ALL = "GRANT_VIEW_ALL"
    EXPORT_ALL_UNITS = "EXPORT_ALL_UNITS"
    ASSIGN_TASK_DOWNSTREAM = "ASSIGN_TASK_DOWNSTREAM"

# Yêu cầu yếu tố bí mật theo action (có thể điều chỉnh theo chính sách)
# NONE | PIN | TOTP | PIN+TOTP
REQUIREMENT_BY_ACTION: dict[ActionCode, str] = {
    ActionCode.APPROVE_DEPT_PLAN: "PIN+TOTP",
    ActionCode.GRANT_VIEW_ALL: "TOTP",
    ActionCode.EXPORT_ALL_UNITS: "TOTP",
    ActionCode.ASSIGN_TASK_DOWNSTREAM: "PIN",  # giao việc xuống cấp dưới
}
# TTL (phút) cho phiên mở khoá của từng action (mặc định 480 nếu không khai báo)
TTL_MIN_BY_ACTION: dict[ActionCode, int] = {
    ActionCode.APPROVE_DEPT_PLAN: 60,
    ActionCode.GRANT_VIEW_ALL: 30,
    ActionCode.EXPORT_ALL_UNITS: 30,
    ActionCode.ASSIGN_TASK_DOWNSTREAM: 120,
}
