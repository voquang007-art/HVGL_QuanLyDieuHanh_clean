from fastapi import APIRouter, Request, Depends, Form
from starlette.responses import RedirectResponse
from starlette.templating import Jinja2Templates
from sqlalchemy.orm import Session
import os

from ..security.secret_lock import get_db, attempt_unlock
from ..security.deps import login_required
from ..security.policy import ActionCode
from ..config import settings

router = APIRouter()
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "..", "templates"))

@router.get("/secret-lock")
def secret_lock_form(request: Request, action: str):
    try:
        _ = ActionCode(action)
    except Exception:
        return templates.TemplateResponse("secret_lock.html", {"request": request, "app_name": settings.APP_NAME, "error": "Action không hợp lệ.", "action": action})
    return templates.TemplateResponse("secret_lock.html", {"request": request, "app_name": settings.APP_NAME, "action": action})

@router.post("/secret-lock")
def secret_lock_submit(request: Request,
                       action: str = Form(...),
                       pin: str = Form(""),
                       otp: str = Form(""),
                       db: Session = Depends(get_db)):
    user = login_required(request, db)
    try:
        act = ActionCode(action)
    except Exception:
        return templates.TemplateResponse("secret_lock.html", {"request": request, "app_name": settings.APP_NAME, "error": "Action không hợp lệ.", "action": action})

    ok = attempt_unlock(user, db, act, pin or None, otp or None)
    if not ok:
        return templates.TemplateResponse("secret_lock.html", {"request": request, "app_name": settings.APP_NAME, "error": "PIN/OTP không hợp lệ.", "action": action})

    # Quay lại trang tương ứng (đơn giản: trả về dashboard; thực tế có thể dùng next=)
    return RedirectResponse(url="/dashboard", status_code=302)
