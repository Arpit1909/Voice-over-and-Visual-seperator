import os
from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / '.env')

DATA_DIR = ROOT / 'data'
ANALYSES_DIR = DATA_DIR / 'analyses'
DB_PATH = DATA_DIR / 'index.db'

DATA_DIR.mkdir(exist_ok=True)
ANALYSES_DIR.mkdir(exist_ok=True)

STORAGE_LIMIT_GB = int(os.getenv('STORAGE_LIMIT_GB', '50'))
STORAGE_LIMIT_BYTES = STORAGE_LIMIT_GB * 1024 * 1024 * 1024

# ── Google Workspace OAuth ─────────────────────────────────────────────────
# Sign-in is handled by Google. Only accounts whose email is on
# ALLOWED_DOMAIN (or whose `hd` claim matches) can sign in.
GOOGLE_OAUTH_CLIENT_ID = os.getenv('GOOGLE_OAUTH_CLIENT_ID', '').strip()
GOOGLE_OAUTH_CLIENT_SECRET = os.getenv('GOOGLE_OAUTH_CLIENT_SECRET', '').strip()
GOOGLE_OAUTH_REDIRECT_URI = os.getenv('GOOGLE_OAUTH_REDIRECT_URI', '').strip()
ALLOWED_DOMAIN = os.getenv('ALLOWED_DOMAIN', '').strip().lower()

# Signed-cookie session secret. Generate with:
#   python -c "import secrets; print(secrets.token_urlsafe(48))"
SESSION_SECRET = os.getenv('SESSION_SECRET', '').strip()
SESSION_MAX_AGE = int(os.getenv('SESSION_MAX_AGE', str(30 * 86400)))
# Set to "0" only for local-HTTP development; production must keep "1".
SESSION_HTTPS_ONLY = os.getenv('SESSION_HTTPS_ONLY', '1').strip() != '0'

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
VERTEX_AI_LOCATION = os.getenv('VERTEX_AI_LOCATION', 'us-central1')

MAX_UPLOAD_BYTES = int(os.getenv('MAX_UPLOAD_GB', '5')) * 1024 * 1024 * 1024
