import os
import json
import subprocess
import sys
import tempfile

MAX_DURATION_SECONDS = 3600  # 1 hour

SUPPORTED_EXTENSIONS = {'.mp4', '.avi', '.mov', '.mkv', '.webm', '.flv', '.wmv', '.mpg', '.mpeg', '.3gp', '.m4v'}

# Format attempts in order — each is tried until one succeeds
_YT_FORMAT_ATTEMPTS = [
    'best[ext=mp4]',
    'bestvideo[vcodec^=avc1][ext=mp4]+bestaudio[ext=m4a]',
    'bestvideo[vcodec^=avc]+bestaudio',
    'best',
]

# Player clients tried in order to bypass YouTube bot-detection (403 errors)
_YT_PLAYER_CLIENTS = [
    'tv_embedded',
    'web_embedded',
    'ios',
    'android',
    'web',
]

def _ytdlp_cmd() -> list:
    """Use the same Python interpreter's yt_dlp module — avoids PATH issues with venvs."""
    return [sys.executable, '-m', 'yt_dlp']

# Common ffmpeg install locations on Windows
_FFMPEG_PATHS = [
    r'C:\Program Files\ffmpeg\bin',
    r'C:\ffmpeg\bin',
    r'C:\ProgramData\chocolatey\bin',
]


def get_video(input_path: str) -> tuple[str, str]:
    """Return (local_path, title) for a YouTube URL or local file."""
    if input_path.startswith(('http://', 'https://', 'www.', 'youtu')):
        return _download_youtube(input_path)
    return _validate_local(input_path)


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


def _download_youtube(url: str) -> tuple[str, str]:
    env = _env_with_ffmpeg()

    print("Fetching video info from YouTube...")
    info = _yt_dump_json(url, env)

    duration = info.get('duration', 0)
    if duration > MAX_DURATION_SECONDS:
        raise ValueError(f"Video is {duration // 60} minutes long. Maximum allowed is 60 minutes.")

    title      = info.get('title', 'video')
    output_dir = tempfile.mkdtemp(prefix='video_analyzer_')
    output_tpl = os.path.join(output_dir, '%(id)s.%(ext)s')

    print(f"Downloading: {title}")

    ffmpeg     = _ffmpeg_dir()
    last_error = None

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
                ]
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
                    return path, title

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
    last_error = None
    for client in _YT_PLAYER_CLIENTS:
        try:
            result = subprocess.run(
                _ytdlp_cmd() + [
                    '--dump-json', '--no-playlist',
                    '--extractor-args', f'youtube:player_client={client}',
                    url,
                ],
                capture_output=True, text=True, check=True, env=env,
            )
            return json.loads(result.stdout)
        except subprocess.CalledProcessError as e:
            last_error = e
            continue
    raise RuntimeError(f"Could not fetch video info from YouTube.\nLast error: {last_error}")


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
