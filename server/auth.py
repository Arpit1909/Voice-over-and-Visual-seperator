"""Simple shared-password auth via signed cookie."""
import secrets
from threading import Lock

from fastapi import HTTPException, Request

from .config import APP_PASSWORD, APP_USERNAME

_sessions: set[str] = set()
_lock = Lock()


def auth_enabled() -> bool:
    return bool(APP_PASSWORD)


def is_authenticated(request: Request) -> bool:
    if not auth_enabled():
        return True
    token = request.cookies.get('session')
    if not token:
        return False
    with _lock:
        return token in _sessions


def require_auth(request: Request):
    if not is_authenticated(request):
        raise HTTPException(status_code=401, detail="Not authenticated")


def login(username: str, password: str):
    """Return token on success, False on bad creds, None if auth disabled."""
    if not auth_enabled():
        return None
    if username != APP_USERNAME or password != APP_PASSWORD:
        return False
    token = secrets.token_urlsafe(32)
    with _lock:
        _sessions.add(token)
    return token


def logout(token: str | None):
    if not token:
        return
    with _lock:
        _sessions.discard(token)
