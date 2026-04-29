# app/routers/admin_users.py
from fastapi import APIRouter
from starlette.responses import RedirectResponse

router = APIRouter()

@router.get("/users", include_in_schema=False)
def admin_users_redirect():
    # Dùng chung giao diện & logic tại /account/users
    return RedirectResponse(url="/account/users", status_code=307)
