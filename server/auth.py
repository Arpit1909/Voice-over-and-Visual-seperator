"""Google Workspace OAuth sign-in — restricted to a single domain.

Auth is enabled when GOOGLE_OAUTH_CLIENT_ID + SECRET are set in `.env`.
When enabled, only accounts whose hosted-domain (`hd` claim) or email
domain matches ALLOWED_DOMAIN can sign in. The signed-in user dict is
stored in `request.session["user"]` by Starlette's SessionMiddleware
(signed-cookie based, survives PM2 restarts).
"""
from urllib.parse import urlencode

import requests
from fastapi import HTTPException, Request
from google.auth.transport import requests as google_requests
from google.oauth2 import id_token as id_token_lib

from .config import (
    ALLOWED_DOMAIN, GOOGLE_OAUTH_CLIENT_ID, GOOGLE_OAUTH_CLIENT_SECRET,
    GOOGLE_OAUTH_REDIRECT_URI,
)

AUTH_URI = "https://accounts.google.com/o/oauth2/v2/auth"
TOKEN_URI = "https://oauth2.googleapis.com/token"
SCOPES = "openid email profile"


def auth_enabled() -> bool:
    """OAuth is active only when both client ID and secret are configured."""
    return bool(GOOGLE_OAUTH_CLIENT_ID and GOOGLE_OAUTH_CLIENT_SECRET)


def get_user(request: Request) -> dict | None:
    """Return the signed-in user dict, or None."""
    return request.session.get("user")


def is_authenticated(request: Request) -> bool:
    if not auth_enabled():
        # If OAuth isn't configured, treat everyone as authenticated so the
        # app still runs in dev. Production MUST configure OAuth.
        return True
    return get_user(request) is not None


def require_auth(request: Request):
    if not is_authenticated(request):
        raise HTTPException(status_code=401, detail="Not authenticated")


def domain_ok(email: str, hd: str | None) -> bool:
    """Allow when ALLOWED_DOMAIN matches `hd` claim or the email suffix."""
    if not ALLOWED_DOMAIN:
        return True
    if hd and str(hd).lower() == ALLOWED_DOMAIN:
        return True
    return email.lower().endswith("@" + ALLOWED_DOMAIN)


def build_google_auth_url(state: str) -> str:
    params = {
        "client_id": GOOGLE_OAUTH_CLIENT_ID,
        "redirect_uri": GOOGLE_OAUTH_REDIRECT_URI,
        "response_type": "code",
        "scope": SCOPES,
        "access_type": "online",
        "prompt": "select_account",
        "state": state,
    }
    if ALLOWED_DOMAIN:
        # `hd` is a hint to Google to restrict the account chooser to one
        # Workspace domain. We re-verify on the server side too.
        params["hd"] = ALLOWED_DOMAIN
    return AUTH_URI + "?" + urlencode(params)


def exchange_code(code: str) -> dict:
    r = requests.post(
        TOKEN_URI,
        data={
            "code": code,
            "client_id": GOOGLE_OAUTH_CLIENT_ID,
            "client_secret": GOOGLE_OAUTH_CLIENT_SECRET,
            "redirect_uri": GOOGLE_OAUTH_REDIRECT_URI,
            "grant_type": "authorization_code",
        },
        timeout=30,
    )
    r.raise_for_status()
    return r.json()


def verify_id_token(token_str: str) -> dict:
    return id_token_lib.verify_oauth2_token(
        token_str, google_requests.Request(), audience=GOOGLE_OAUTH_CLIENT_ID
    )
