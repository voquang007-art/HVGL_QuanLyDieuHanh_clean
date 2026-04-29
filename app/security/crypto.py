from passlib.context import CryptContext
import pyotp, secrets, string, re

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def hash_password(password: str) -> str:
    return pwd_context.hash(password)

def verify_password(password: str, password_hash: str) -> bool:
    return pwd_context.verify(password, password_hash)

# --- TOTP ---
def generate_totp_seed() -> str:
    return pyotp.random_base32()

def verify_totp(totp_seed: str, code: str) -> bool:
    try:
        totp = pyotp.TOTP(totp_seed)
        return totp.verify(code, valid_window=1)
    except Exception:
        return False

# --- PIN (6-8 số) ---
PIN_PATTERN = re.compile(r"^[0-9]{6,8}$")

def hash_pin(pin: str) -> str:
    if not PIN_PATTERN.fullmatch(pin or ""):
        raise ValueError("PIN không hợp lệ (6–8 chữ số).")
    return pwd_context.hash(pin)

def verify_pin(pin: str, pin_hash: str) -> bool:
    if not pin_hash:
        return False
    return pwd_context.verify(pin, pin_hash)

# --- Recovery Codes ---
RC_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # bỏ I,O,0,1
def generate_recovery_codes(n: int = 10) -> list[str]:
    codes = []
    for _ in range(n):
        raw = "".join(secrets.choice(RC_ALPHABET) for __ in range(10))
        # hiển thị kiểu XXXX-XXXX-XX
        disp = f"{raw[0:4]}-{raw[4:8]}-{raw[8:10]}"
        codes.append(disp)
    return codes
