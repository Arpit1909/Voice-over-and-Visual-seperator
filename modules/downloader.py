import os
import json
import re
import subprocess
import sys
import tempfile

MAX_DURATION_SECONDS = 3600  # 1 hour

# Matches anything that looks like a URL: full http(s), or bare youtu.be/youtube.com/www.
# `\S+` stops at whitespace, so newlines/spaces in pasted blobs cleanly split title from URL.
_URL_RE = re.compile(
    r'(?:https?://|www\.|youtu\.be/|youtube\.com/|m\.youtube\.com/)\S+',
    re.IGNORECASE,
)


def extract_url(text: str) -> str:
    """Pull the first URL out of a possibly-messy paste.

    YouTube's mobile share button copies a blob like:
        Kids Lead Police To Mom's Murder Secret\n\nyoutu.be/kAuPzEUT2i4?si=...
    Without this, the entire blob (title + newlines + URL) reaches yt-dlp and
    fails parsing. Returns '' when no URL is found so callers can error cleanly.
    """
    if not text:
        return ''
    text = text.strip()
    # Fast path: clean single-token URL.
    if text.startswith(('http://', 'https://')) and not any(c in text for c in ' \n\r\t'):
        return text
    m = _URL_RE.search(text)
    if not m:
        return ''
    url = m.group(0).rstrip(').,;:!?"\'>]')
    if not url.lower().startswith(('http://', 'https://')):
        url = 'https://' + url
    return url

SUPPORTED_EXTENSIONS = {'.mp4', '.avi', '.mov', '.mkv', '.webm', '.flv', '.wmv', '.mpg', '.mpeg', '.3gp', '.m4v'}

# Flags shared by every yt-dlp call. Mirrors the pattern that works on bot-flagged
# datacenter IPs without bgutil/PO-Token plumbing:
#   --js-runtimes node  → lets yt-dlp solve the YouTube `n` JS challenge
#   -4                  → force IPv4 (datacenter v6 ranges are flagged harder)
#   --no-check-certificate → tolerate intermediate proxies/MITM in some networks
# We deliberately do NOT pin a `youtube:player_client=...` — yt-dlp picks the
# right client for the cookies in use, and forcing one usually makes things
# worse on flagged IPs.
_YT_BASE_ARGS = [
    '--no-playlist',
    '--no-check-certificate',
    '--js-runtimes', 'node',
    '-4',
]

# Format selector: progressive mp4 first, then split video+audio merged to mp4,
# then any best. -S biases ranking toward avc1/aac for downstream ffmpeg compat.
_YT_FORMAT_ARGS = [
    '-f', 'bv*[ext=mp4]+ba[ext=m4a]/b[ext=mp4]/best',
    '-S', 'ext:mp4:m4a,res,codec:avc1:acodec:aac',
]

def _ytdlp_cmd() -> list:
    """Use the same Python interpreter's yt_dlp module — avoids PATH issues with venvs."""
    return [sys.executable, '-m', 'yt_dlp']


def _cookies_args() -> list:
    """Return ['--cookies', path] when a YouTube cookies file is configured.

    Datacenter IPs are bot-flagged; cookies prove the request is from a logged-in
    user. Resolution order:
      1. $YT_DLP_COOKIES env var (explicit path).
      2. cookies.txt next to the project root (auto-detected).
    Cookies typically expire every 30–60 days — re-export when downloads start
    failing again with "Sign in to confirm you're not a bot".
    """
    path = os.getenv('YT_DLP_COOKIES', '').strip()
    if not path:
        default = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'cookies.txt')
        if os.path.isfile(default):
            path = default
    if path and os.path.isfile(path):
        return ['--cookies', path]
    return []

# Common ffmpeg install locations on Windows
_FFMPEG_PATHS = [
    r'C:\Program Files\ffmpeg\bin',
    r'C:\ffmpeg\bin',
    r'C:\ProgramData\chocolatey\bin',
]


def get_video(input_path: str) -> tuple[str, str, dict]:
    """Return (local_path, title, meta) for a YouTube URL or local file.

    `meta` describes the source for downstream export decisions:
      - {'source': 'youtube', 'url': ..., 'video_id': ..., 'playable_in_embed': bool}
      - {'source': 'local'}
    """
    candidate = extract_url(input_path)
    if candidate:
        return _download_youtube(candidate)
    path, title = _validate_local(input_path)
    return path, title, {'source': 'local'}


def _env_with_ffmpeg() -> dict:
    """Return env dict with ffmpeg added to PATH if not already there."""
    env = os.environ.copy()
    for p in _FFMPEG_PATHS:
        if os.path.isdir(p) and p not in env.get('PATH', ''):
            env['PATH'] = p + os.pathsep + env.get('PATH', '')
    return env


def _ffmpeg_dir() -> str:
    """Return the directory containing ffmpeg.exe, or empty string if not found."""
    import shutil
    path = shutil.which('ffmpeg', path=_env_with_ffmpeg().get('PATH', ''))
    if path:
        return os.path.dirname(path)
    for p in _FFMPEG_PATHS:
        if os.path.isfile(os.path.join(p, 'ffmpeg.exe')):
            return p
    return ''


def _download_youtube(url: str) -> tuple[str, str, dict]:
    env = _env_with_ffmpeg()

    print("Fetching video info from YouTube...")
    info = _yt_dump_json(url, env)

    duration = info.get('duration', 0)
    if duration > MAX_DURATION_SECONDS:
        raise ValueError(f"Video is {duration // 60} minutes long. Maximum allowed is 60 minutes.")

    title      = info.get('title', 'video')
    video_id   = info.get('id', '')
    embeddable = info.get('playable_in_embed', True)
    meta = {
        'source': 'youtube',
        'url': url,
        'video_id': video_id,
        'playable_in_embed': bool(embeddable),
    }

    output_dir = tempfile.mkdtemp(prefix='video_analyzer_')
    output_tpl = os.path.join(output_dir, '%(id)s.%(ext)s')

    print(f"Downloading: {title}")

    ffmpeg = _ffmpeg_dir()
    cmd = (
        _ytdlp_cmd()
        + _YT_BASE_ARGS
        + _YT_FORMAT_ARGS
        + ['--merge-output-format', 'mp4', '-o', output_tpl]
        + _cookies_args()
    )
    if ffmpeg:
        cmd += ['--ffmpeg-location', ffmpeg]
    cmd.append(url)

    # Don't trust exit code alone — if a file landed in output_dir, the download
    # succeeded even when yt-dlp returns non-zero due to GetPOT noise.
    proc = subprocess.run(cmd, check=False, env=env, capture_output=True, text=True)

    files = [f for f in os.listdir(output_dir)
             if os.path.isfile(os.path.join(output_dir, f))]
    if not files:
        raise RuntimeError(
            f"yt-dlp download failed.\n"
            f"URL: {url}\n"
            f"yt-dlp exit: {proc.returncode}\n"
            f"yt-dlp said (last 2000 chars):\n{(proc.stderr or '').strip()[-2000:]}\n"
            f"Make sure ffmpeg + node are installed and cookies.txt is fresh."
        )

    path = os.path.join(output_dir, files[0])
    if not path.endswith('.mp4'):
        path = _convert_to_mp4(path, env)
    return path, title, meta


def _yt_dump_json(url: str, env: dict) -> dict:
    """Fetch video metadata. Trusts stdout JSON regardless of exit code — yt-dlp
    2026.x emits non-zero alongside floods of [GetPOT]/n-challenge warnings even
    when the JSON was successfully extracted via a fallback client.
    """
    cookies = _cookies_args()
    result = subprocess.run(
        _ytdlp_cmd() + ['--dump-json'] + _YT_BASE_ARGS + cookies + [url],
        capture_output=True, text=True, check=False, env=env,
    )
    out = result.stdout.strip()
    if out:
        try:
            return json.loads(out)
        except json.JSONDecodeError:
            pass
    stderr = (result.stderr or '').strip()
    raise RuntimeError(
        f"Could not fetch video info from YouTube.\n"
        f"URL: {url}\n"
        f"yt-dlp exit: {result.returncode}\n"
        f"yt-dlp said (last 3000 chars):\n{stderr[-3000:]}\n"
        f"Cookies in use: {'yes' if cookies else 'NO — set YT_DLP_COOKIES or drop cookies.txt at project root'}\n"
        f"If this persists, update yt-dlp:  pip install -U yt-dlp"
    )


def _validate_local(path: str) -> tuple[str, str]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

    ext = os.path.splitext(path)[1].lower()
    if ext not in SUPPORTED_EXTENSIONS:
        raise ValueError(
            f"Unsupported format '{ext}'. Supported: {', '.join(sorted(SUPPORTED_EXTENSIONS))}"
        )

    duration = _get_duration(path)
    if duration > MAX_DURATION_SECONDS:
        raise ValueError(f"Video is {duration // 60:.0f} minutes long. Maximum allowed is 60 minutes.")

    title = os.path.splitext(os.path.basename(path))[0]

    if ext != '.mp4':
        path = _convert_to_mp4(path)

    return path, title


def _get_duration(path: str) -> float:
    env = _env_with_ffmpeg()
    result = subprocess.run(
        ['ffprobe', '-v', 'quiet', '-print_format', 'json', '-show_format', path],
        capture_output=True, text=True, check=True, env=env
    )
    info = json.loads(result.stdout)
    return float(info['format'].get('duration', 0))


def _convert_to_mp4(path: str, env: dict = None) -> str:
    env = env or _env_with_ffmpeg()
    output_dir  = tempfile.mkdtemp(prefix='video_analyzer_')
    output_path = os.path.join(output_dir, 'converted.mp4')
    print("Converting to MP4...")
    subprocess.run([
        'ffmpeg', '-i', path,
        '-c:v', 'libx264', '-c:a', 'aac',
        '-y', output_path
    ], check=True, capture_output=True, env=env)
    return output_path
