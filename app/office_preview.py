# -*- coding: utf-8 -*-
"""
app/office_preview.py

Chuyển file Office nội bộ sang PDF để xem trước trên trình duyệt.
Áp dụng cho: .doc, .docx, .xls, .xlsx

Nguyên tắc:
- Không đưa file ra dịch vụ bên ngoài.
- Không đổi DB.
- Không sửa logic phân quyền của từng module.
- Chỉ tạo file PDF cache khi người dùng bấm Xem.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Optional

from .config import settings


OFFICE_PREVIEW_EXTENSIONS = {".doc", ".docx", ".xls", ".xlsx"}


class OfficePreviewError(RuntimeError):
    pass


def is_office_previewable(filename: str | None) -> bool:
    ext = Path(filename or "").suffix.lower().strip()
    return ext in OFFICE_PREVIEW_EXTENSIONS


def _project_root() -> str:
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def _office_preview_root() -> str:
    configured = getattr(settings, "OFFICE_PREVIEW_DIR", None)
    if configured:
        root = os.path.abspath(str(configured))
    else:
        root = os.path.join(_project_root(), "instance", "office_previews")

    os.makedirs(root, exist_ok=True)
    return root


def _safe_key(value: str) -> str:
    raw = str(value or "").strip()
    if not raw:
        raw = "office-preview"

    safe = []
    for ch in raw:
        if ch.isalnum() or ch in {"-", "_"}:
            safe.append(ch)
        else:
            safe.append("_")

    result = "".join(safe).strip("_")
    if not result:
        result = hashlib.sha256(raw.encode("utf-8")).hexdigest()

    return result[:120]


def _find_soffice() -> str:
    configured = (
        getattr(settings, "SOFFICE_PATH", None)
        or getattr(settings, "LIBREOFFICE_PATH", None)
        or os.environ.get("SOFFICE_PATH")
        or os.environ.get("LIBREOFFICE_PATH")
    )

    candidates: list[str] = []

    if configured:
        candidates.append(str(configured))

    which_soffice = shutil.which("soffice") or shutil.which("soffice.exe")
    if which_soffice:
        candidates.append(which_soffice)

    # Dò Windows Registry nếu LibreOffice được cài bằng bộ cài chuẩn.
    if os.name == "nt":
        try:
            import winreg

            registry_locations = [
                (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\LibreOffice\LibreOffice"),
                (winreg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WOW6432Node\LibreOffice\LibreOffice"),
                (winreg.HKEY_CURRENT_USER, r"SOFTWARE\LibreOffice\LibreOffice"),
            ]

            for root_key, sub_key in registry_locations:
                try:
                    with winreg.OpenKey(root_key, sub_key) as key:
                        install_path, _ = winreg.QueryValueEx(key, "Path")
                        if install_path:
                            candidates.append(os.path.join(str(install_path), "program", "soffice.exe"))
                            candidates.append(os.path.join(str(install_path), "soffice.exe"))
                except Exception:
                    continue
        except Exception:
            pass

    candidates.extend(
        [
            r"C:\Program Files\LibreOffice\program\soffice.exe",
            r"C:\Program Files (x86)\LibreOffice\program\soffice.exe",
            "/usr/bin/soffice",
            "/usr/local/bin/soffice",
            "/opt/libreoffice/program/soffice",
        ]
    )

    # Quét thêm trong Program Files để nhận cả trường hợp cài khác thư mục con.
    if os.name == "nt":
        scan_roots = [
            os.environ.get("PROGRAMFILES", ""),
            os.environ.get("PROGRAMFILES(X86)", ""),
            os.environ.get("LOCALAPPDATA", ""),
        ]

        for root in scan_roots:
            root = str(root or "").strip()
            if not root or not os.path.isdir(root):
                continue

            likely_roots = [
                os.path.join(root, "LibreOffice"),
                os.path.join(root, "The Document Foundation"),
            ]

            for likely_root in likely_roots:
                if not os.path.isdir(likely_root):
                    continue

                for current_dir, _dirnames, filenames in os.walk(likely_root):
                    if "soffice.exe" in filenames:
                        candidates.append(os.path.join(current_dir, "soffice.exe"))
                        break

    seen: set[str] = set()
    for path in candidates:
        clean_path = os.path.abspath(str(path or "").strip().strip('"'))
        if not clean_path or clean_path in seen:
            continue
        seen.add(clean_path)

        if os.path.isfile(clean_path):
            return clean_path

    raise OfficePreviewError(
        "Không tìm thấy LibreOffice/soffice. "
        "Có thể xử lý bằng một trong các cách sau: "
        "1) cài LibreOffice; "
        "2) thêm thư mục program của LibreOffice vào PATH; "
        "3) khai báo SOFFICE_PATH trong config.py hoặc biến môi trường."
    )

def _source_signature(source_path: str) -> dict:
    stat = os.stat(source_path)
    return {
        "source_path": os.path.abspath(source_path),
        "size": int(stat.st_size),
        "mtime": float(stat.st_mtime),
    }


def _meta_matches(meta_path: str, signature: dict) -> bool:
    try:
        if not os.path.exists(meta_path):
            return False
        with open(meta_path, "r", encoding="utf-8") as f:
            saved = json.load(f) or {}
        return (
            saved.get("source_path") == signature.get("source_path")
            and int(saved.get("size") or 0) == int(signature.get("size") or 0)
            and float(saved.get("mtime") or 0) == float(signature.get("mtime") or 0)
        )
    except Exception:
        return False


def _write_meta(meta_path: str, signature: dict) -> None:
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(signature, f, ensure_ascii=False, indent=2)


def ensure_office_pdf_preview(
    source_path: str,
    preview_key: str,
    original_name: Optional[str] = None,
    timeout_seconds: int = 90,
) -> str:
    """
    Trả về đường dẫn PDF preview.
    Nếu cache đã đúng source hiện tại thì dùng lại.
    Nếu chưa có hoặc source thay đổi thì gọi LibreOffice chuyển lại.
    """
    if not source_path:
        raise OfficePreviewError("Đường dẫn file nguồn rỗng.")

    abs_source = os.path.abspath(source_path)
    if not os.path.exists(abs_source):
        raise OfficePreviewError("Không tìm thấy file nguồn để tạo preview.")

    display_name = original_name or os.path.basename(abs_source)
    if not is_office_previewable(display_name):
        raise OfficePreviewError("Định dạng file không thuộc nhóm Office hỗ trợ preview.")

    preview_root = _office_preview_root()
    safe_key = _safe_key(preview_key)
    preview_pdf = os.path.join(preview_root, f"{safe_key}.pdf")
    preview_meta = os.path.join(preview_root, f"{safe_key}.json")

    signature = _source_signature(abs_source)
    if os.path.exists(preview_pdf) and _meta_matches(preview_meta, signature):
        return preview_pdf

    soffice_path = _find_soffice()
    source_ext = Path(display_name).suffix.lower() or Path(abs_source).suffix.lower()
    if source_ext not in OFFICE_PREVIEW_EXTENSIONS:
        source_ext = Path(abs_source).suffix.lower()

    with tempfile.TemporaryDirectory(prefix="office_preview_") as tmp_dir:
        tmp_source = os.path.join(tmp_dir, f"source{source_ext}")
        shutil.copy2(abs_source, tmp_source)

        cmd = [
            soffice_path,
            "--headless",
            "--nologo",
            "--nodefault",
            "--nofirststartwizard",
            "--convert-to",
            "pdf",
            "--outdir",
            tmp_dir,
            tmp_source,
        ]

        proc = subprocess.run(
            cmd,
            cwd=tmp_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=timeout_seconds,
        )

        expected_pdf = os.path.join(tmp_dir, "source.pdf")
        if proc.returncode != 0 or not os.path.exists(expected_pdf):
            stdout = (proc.stdout or "").strip()
            stderr = (proc.stderr or "").strip()
            detail = stderr or stdout or "LibreOffice không tạo được file PDF."
            raise OfficePreviewError(f"Lỗi chuyển Office sang PDF: {detail}")

        tmp_preview_pdf = f"{preview_pdf}.tmp"
        shutil.copy2(expected_pdf, tmp_preview_pdf)
        os.replace(tmp_preview_pdf, preview_pdf)
        _write_meta(preview_meta, signature)

    return preview_pdf