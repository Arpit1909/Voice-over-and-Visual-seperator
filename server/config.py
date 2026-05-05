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

APP_USERNAME = os.getenv('APP_USERNAME', 'team')
APP_PASSWORD = os.getenv('APP_PASSWORD', '')
SESSION_MAX_AGE = int(os.getenv('SESSION_MAX_AGE', str(30 * 86400)))

GCP_PROJECT_ID = os.getenv('GCP_PROJECT_ID')
GCS_BUCKET_NAME = os.getenv('GCS_BUCKET_NAME')
VERTEX_AI_LOCATION = os.getenv('VERTEX_AI_LOCATION', 'us-central1')

MAX_UPLOAD_BYTES = int(os.getenv('MAX_UPLOAD_GB', '5')) * 1024 * 1024 * 1024
