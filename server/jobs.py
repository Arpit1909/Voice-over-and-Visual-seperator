"""Background job runner. Wraps existing analyzer modules; never modifies them."""
import contextlib
import json
import re
import shutil
import sys
import threading
import time
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

_MAX_LOG_LINES = 500
_logs: dict[str, dict] = {}  # job_id -> {'seq': int, 'lines': [{seq, ts, line}]}
_logs_lock = threading.Lock()


def _set_progress(job_id: str, **kw):
    with _progress_lock:
        cur = _progress.get(job_id, {})
        cur.update(kw)
        _progress[job_id] = cur


def get_progress(job_id: str) -> dict:
    with _progress_lock:
        return dict(_progress.get(job_id, {}))


def _append_log(job_id: str, line: str):
    """Append a single log line for a job (bounded ring buffer)."""
    if not line:
        return
    with _logs_lock:
        d = _logs.setdefault(job_id, {'seq': 0, 'lines': []})
        d['seq'] += 1
        d['lines'].append({'seq': d['seq'], 'ts': time.time(), 'line': line[:1000]})
        if len(d['lines']) > _MAX_LOG_LINES:
            d['lines'] = d['lines'][-_MAX_LOG_LINES:]


def get_logs(job_id: str, since: int = 0) -> dict:
    """Return the latest log lines (with seq > `since`) and current head seq."""
    with _logs_lock:
        d = _logs.get(job_id)
        if not d:
            return {'seq': 0, 'lines': []}
        if since <= 0:
            return {'seq': d['seq'], 'lines': list(d['lines'])}
        return {
            'seq': d['seq'],
            'lines': [ln for ln in d['lines'] if ln['seq'] > since],
        }


def _reset_logs(job_id: str):
    with _logs_lock:
        _logs[job_id] = {'seq': 0, 'lines': []}


class _JobStdoutTee:
    """Mirrors writes to the original stdout AND appends complete lines to the
    job's log buffer. Per-thread buffers prevent interleaved writes from the
    parallel chunk workers from being smashed together.
    """

    def __init__(self, job_id: str, original):
        self._job_id   = job_id
        self._orig     = original
        self._tls      = threading.local()

    def _buf(self) -> list:
        # threading.local instances per thread — no cross-thread contamination.
        if not hasattr(self._tls, 'buf'):
            self._tls.buf = ''
        return self._tls

    def write(self, s):
        if not isinstance(s, str):
            try: s = str(s)
            except Exception: return 0
        try: self._orig.write(s)
        except Exception: pass

        tls = self._buf()
        tls.buf += s
        while '\n' in tls.buf:
            line, tls.buf = tls.buf.split('\n', 1)
            line = line.rstrip('\r').rstrip()
            if line:
                _append_log(self._job_id, line)
        return len(s)

    def flush(self):
        try: self._orig.flush()
        except Exception: pass

    # Some libraries probe these attributes — pass them through.
    def isatty(self):
        try: return self._orig.isatty()
        except Exception: return False

    @property
    def encoding(self):
        return getattr(self._orig, 'encoding', 'utf-8')


@contextlib.contextmanager
def _capture_stdout(job_id: str):
    """Tee sys.stdout for the duration of an analysis so users can watch the
    chunk-by-chunk logs in the UI. Single-worker queue means no two jobs
    overlap, so swapping the global stdout is safe."""
    orig = sys.stdout
    sys.stdout = _JobStdoutTee(job_id, orig)
    try:
        yield
    finally:
        sys.stdout = orig


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
    _reset_logs(job_id)
    _append_log(job_id, f'Queued analysis {job_id}')
    _set_progress(job_id, stage='queued', progress=0, message='Queued for analysis')
    _queue.put(job_id)


_YT_ID_RE = re.compile(r'(?:v=|youtu\.be/|embed/|shorts/)([a-zA-Z0-9_-]{11})')


def _extract_yt_id(url: str) -> str:
    m = _YT_ID_RE.search(url or '')
    return m.group(1) if m else ''


def _make_chunk_callback(job_id: str, folder: Path, duration_secs: float):
    """Build a thread-safe callback that streams partial results to partial.json
    and updates the job's progress message after each main chunk completes."""
    from modules.vertex_analyzer import _merge_sections, _secs_to_ts

    state = {'chunks': {}}  # idx -> chunk_data
    lock = threading.Lock()
    partial_path = folder / 'partial.json'

    def _on_chunk_done(idx: int, total: int, chunk_data: dict):
        with lock:
            state['chunks'][idx] = chunk_data
            ordered_idxs = sorted(state['chunks'].keys())
            done_count = len(ordered_idxs)

            all_sections = []
            all_peaks = []
            all_highlights = []
            summaries = []
            title = ''
            for i in ordered_idxs:
                cd = state['chunks'][i]
                if not title and cd.get('title'):
                    title = cd['title']
                all_sections.extend(cd.get('sections', []))
                all_peaks.extend(cd.get('peak_moments', []))
                all_highlights.extend(cd.get('highlights', []))
                if cd.get('summary'):
                    summaries.append(cd['summary'])

            partial = {
                'title': title,
                'total_duration': _secs_to_ts(int(duration_secs)),
                'sections': _merge_sections(all_sections),
                'summary': '\n\n'.join(summaries),
                'peak_moments': all_peaks,
                'highlights': list(dict.fromkeys(all_highlights)),
                '_partial': True,
                '_chunks_done': done_count,
                '_chunks_total': total,
            }

            tmp = partial_path.with_suffix('.json.tmp')
            with open(tmp, 'w', encoding='utf-8') as pf:
                json.dump(partial, pf, ensure_ascii=False, indent=2)
            tmp.replace(partial_path)

            beats_count = sum(len(s.get('beats', [])) for s in partial['sections'])
            # Map analyze stage 15% → 90% across all main chunks.
            pct = 15 + int(75 * (done_count / max(total, 1)))
            _set_progress(
                job_id, stage='analyzing', progress=pct,
                message=(f'Analyzed clip {done_count}/{total} · '
                         f'{beats_count} beats so far · final cleanup pending'),
            )

    return _on_chunk_done


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

    with _capture_stdout(job_id):
      try:
        from modules.downloader import _get_duration, get_video
        from modules.vertex_analyzer import (
            CHUNK_THRESHOLD, analyze_video_chunked, delete_from_gcs, run_gemini, upload_video,
        )

        # 1. Acquire video file
        _set_progress(job_id, stage='downloading', progress=5,
                      message='Loading video from source...')
        video_path, title, _meta = get_video(a['source_url'])
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
                on_chunk_done=_make_chunk_callback(job_id, folder, duration),
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
        # Final result supersedes the streaming preview.
        try:
            (folder / 'partial.json').unlink()
        except FileNotFoundError:
            pass
        except Exception as e:
            print(f"  Could not remove partial.json: {e}")

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
        _append_log(job_id, f'ERROR: {msg[:300]}')
