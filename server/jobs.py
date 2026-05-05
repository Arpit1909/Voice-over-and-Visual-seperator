"""Background job runner. Wraps existing analyzer modules; never modifies them."""
import json
import re
import shutil
import sys
import threading
import traceback
from pathlib import Path
from queue import Queue

from . import storage
from .config import (
    ANALYSES_DIR, GCP_PROJECT_ID, GCS_BUCKET_NAME, VERTEX_AI_LOCATION,
)

# Make `modules` importable even when uvicorn launches from elsewhere.
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


_progress: dict[str, dict] = {}
_progress_lock = threading.Lock()
_queue: "Queue[str]" = Queue()
_worker_started = False
_worker_lock = threading.Lock()


def _set_progress(job_id: str, **kw):
    with _progress_lock:
        cur = _progress.get(job_id, {})
        cur.update(kw)
        _progress[job_id] = cur


def get_progress(job_id: str) -> dict:
    with _progress_lock:
        return dict(_progress.get(job_id, {}))


def _worker_loop():
    while True:
        job_id = _queue.get()
        try:
            _run(job_id)
        except Exception:
            traceback.print_exc()
        finally:
            _queue.task_done()


def start_worker():
    global _worker_started
    with _worker_lock:
        if _worker_started:
            return
        t = threading.Thread(target=_worker_loop, daemon=True, name='analysis-worker')
        t.start()
        _worker_started = True


def enqueue(job_id: str):
    start_worker()
    _set_progress(job_id, stage='queued', progress=0, message='Queued for analysis')
    _queue.put(job_id)


_YT_ID_RE = re.compile(r'(?:v=|youtu\.be/|embed/|shorts/)([a-zA-Z0-9_-]{11})')


def _extract_yt_id(url: str) -> str:
    m = _YT_ID_RE.search(url or '')
    return m.group(1) if m else ''


def _run(job_id: str):
    a = storage.get_analysis(job_id)
    if not a:
        return

    if not (GCP_PROJECT_ID and GCS_BUCKET_NAME):
        storage.update_analysis(job_id, status='error',
                                error_message='GCP_PROJECT_ID or GCS_BUCKET_NAME missing in .env')
        _set_progress(job_id, stage='error', progress=0,
                      message='Missing GCP credentials in .env')
        return

    storage.update_analysis(job_id, status='running')
    folder = ANALYSES_DIR / job_id
    folder.mkdir(exist_ok=True, parents=True)

    try:
        from modules.downloader import _get_duration, get_video
        from modules.vertex_analyzer import (
            CHUNK_THRESHOLD, analyze_video_chunked, delete_from_gcs, run_gemini, upload_video,
        )

        # 1. Acquire video file
        _set_progress(job_id, stage='downloading', progress=5,
                      message='Loading video from source...')
        video_path, title = get_video(a['source_url'])
        storage.update_analysis(job_id, title=title)

        target_video = folder / 'video.mp4'
        if Path(video_path).resolve() != target_video.resolve():
            shutil.copy2(video_path, target_video)

        duration = _get_duration(str(target_video))
        yt_id = _extract_yt_id(a['source_url']) if a['source'] == 'youtube' else ''
        storage.update_analysis(job_id, duration_secs=duration, yt_id=yt_id)

        # 2. Analyze with Gemini
        is_long = duration > CHUNK_THRESHOLD
        _set_progress(
            job_id, stage='analyzing', progress=15,
            message=(f'Analyzing {duration / 60:.1f} min video '
                     f'({"chunked" if is_long else "single pass"}) with Gemini 2.5 Pro...'),
        )

        if is_long:
            result = analyze_video_chunked(
                str(target_video), GCP_PROJECT_ID, GCS_BUCKET_NAME,
                VERTEX_AI_LOCATION, duration,
            )
        else:
            gcs_uri = upload_video(str(target_video), GCS_BUCKET_NAME)
            try:
                result = run_gemini(gcs_uri, GCP_PROJECT_ID,
                                    VERTEX_AI_LOCATION, duration)
            finally:
                try:
                    delete_from_gcs(gcs_uri, GCS_BUCKET_NAME)
                except Exception:
                    pass

        # 3. Persist result
        _set_progress(job_id, stage='saving', progress=92,
                      message='Writing analysis results to disk...')
        with open(folder / 'data.json', 'w', encoding='utf-8') as f:
            json.dump(result, f, ensure_ascii=False, indent=2)

        sections_count = len(result.get('sections', []))
        beats_count = sum(len(s.get('beats', [])) for s in result.get('sections', []))
        size_bytes = storage.folder_size(job_id)

        storage.update_analysis(
            job_id,
            status='done',
            sections_count=sections_count,
            beats_count=beats_count,
            size_bytes=size_bytes,
        )
        _set_progress(job_id, stage='done', progress=100,
                      message=f'Done — {sections_count} sections, {beats_count} beats')

    except Exception as e:
        traceback.print_exc()
        msg = str(e)[:500] or e.__class__.__name__
        storage.update_analysis(job_id, status='error', error_message=msg)
        _set_progress(job_id, stage='error', progress=0, message=msg[:200])
