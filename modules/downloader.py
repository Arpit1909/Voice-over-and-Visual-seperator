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

# Format attempts in order — each is tried until one succeeds
_YT_FORMAT_ATTEMPTS = [
    'best[ext=mp4]',
    'bestvideo[vcodec^=avc1][ext=mp4]+bestaudio[ext=m4a]',
    'bestvideo[vcodec^=avc]+bestaudio',
    'best',
]

# Player clients tried in order. Ordered so the ones that DON'T require a PO
# Token come first — cookies + one of these is enough on a flagged datacenter
# IP. `web` is last because it now hard-requires a PO Token in 2026.
_YT_PLAYER_CLIENTS = [
    'mweb',          # mobile web — most reliable with cookies in 2026
    'tv',            # TV interface — no PO token, no JS challenge
    'tv_embedded',   # TV embed — fallback
    'web_safari',    # Safari client — sometimes unblocked
    'web_embedded',  # web embed — fallback
    'android',       # restricted but occasionally works
    'ios',           # heavily restricted in 2026
    'web',           # last resort — needs PO token via bgutil
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

    ffmpeg     = _ffmpeg_dir()
    last_error = None

    cookies = _cookies_args()
    for client in _YT_PLAYER_CLIENTS:
        for fmt in _YT_FORMAT_ATTEMPTS:
            try:
                cmd = _ytdlp_cmd() + [
                    '--format', fmt,
                    '--merge-output-format', 'mp4',
                    '--output', output_tpl,
                    '--no-playlist',
                    '--no-warnings',
                    '--extractor-args', f'youtube:player_client={client}',
                ] + cookies
                if ffmpeg:
                    cmd += ['--ffmpeg-location', ffmpeg]
                cmd.append(url)
                subprocess.run(cmd, check=True, env=env)

                files = [f for f in os.listdir(output_dir)
                         if os.path.isfile(os.path.join(output_dir, f))]
                if files:
                    path = os.path.join(output_dir, files[0])
                    if not path.endswith('.mp4'):
                        path = _convert_to_mp4(path, env)
                    return path, title, meta

            except subprocess.CalledProcessError as e:
                last_error = e
                print(f"  [{client}] Format '{fmt}' failed, trying next...")
                for f in os.listdir(output_dir):
                    try:
                        os.remove(os.path.join(output_dir, f))
                    except OSError:
                        pass

    raise RuntimeError(
        f"All download attempts failed for this video.\n"
        f"Last error: {last_error}\n"
        f"Make sure ffmpeg is installed:\n"
        f"  macOS:  brew install ffmpeg\n"
        f"  Linux:  sudo apt install ffmpeg\n"
        f"  Windows: winget install Gyan.FFmpeg"
    )


def _yt_dump_json(url: str, env: dict) -> dict:
    """Fetch video metadata, trying each player client until one works."""
    per_client_errors: list[str] = []
    cookies = _cookies_args()
    for client in _YT_PLAYER_CLIENTS:
        try:
            result = subprocess.run(
                _ytdlp_cmd() + [
                    '--dump-json', '--no-playlist',
                    '--extractor-args', f'youtube:player_client={client}',
                ] + cookies + [url],
                capture_output=True, text=True, check=True, env=env,
            )
            return json.loads(result.stdout)
        except subprocess.CalledProcessError as e:
            # Capture per-client stderr — when ALL clients fail you need to see
            # which one died how, not just the last one. Keep only the final
            # ERROR line per client so the message stays readable.
            err = (e.stderr or '').strip().splitlines()
            tail = next((ln for ln in reversed(err) if 'ERROR' in ln), err[-1] if err else '(empty)')
            per_client_errors.append(f"  [{client}] {tail[:300]}")
            continue
    joined = '\n'.join(per_client_errors) or '(no stderr captured)'
    raise RuntimeError(
        f"Could not fetch video info from YouTube.\n"
        f"URL: {url}\n"
        f"All player clients failed:\n{joined}\n"
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
