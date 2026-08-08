"""Хеширование паролей (PBKDF2-HMAC-SHA256) — общее для агента и центра.

Формат хранения самодостаточный: ``pbkdf2$<iters>$<salt>$<hash>``.
"""

import hashlib
import hmac
import secrets

_PBKDF2_ITERATIONS = 200_000


def hash_password(password: str) -> str:
    """Хеш пароля с индивидуальной солью."""
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, _PBKDF2_ITERATIONS)
    return f"pbkdf2${_PBKDF2_ITERATIONS}${salt.hex()}${digest.hex()}"


def verify_password(password: str, stored: str) -> bool:
    """Проверка пароля против хеша (сравнение постоянного времени)."""
    try:
        scheme, iterations_s, salt_hex, digest_hex = stored.split("$")
        if scheme != "pbkdf2":
            return False
        digest = hashlib.pbkdf2_hmac(
            "sha256", password.encode(), bytes.fromhex(salt_hex), int(iterations_s)
        )
        return hmac.compare_digest(digest.hex(), digest_hex)
    except (ValueError, TypeError):
        return False
