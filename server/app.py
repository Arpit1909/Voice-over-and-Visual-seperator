"""FastAPI application: routes, video streaming, exports, comments, history."""
import json
import re
from pathlib import Path

from fastapi import (
    Depends, FastAPI, File, Form, HTTPException, Request, Response, UploadFile,
)
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

from . import auth, jobs, storage
from .config import (
    ANALYSES_DIR, APP_USERNAME, MAX_UPLOAD_BYTES, SESSION_MAX_AGE,
)

app = FastAPI(title='VO and Visual Extractor', docs_url=None, redoc_url=None)

storage.init_db()
storage.mark_orphan_jobs_failed()
jobs.start_worker()

STATIC_DIR = Path(__file__).parent / 'static'
app.mount('/static', StaticFiles(directory=str(STATIC_DIR)), name='static')

_VIDEO_EXTS = {'.mp4', '.mov', '.mkv', '.avi', '.webm', '.flv', '.wmv',
               '.mpg', '.mpeg', '.3gp', '.m4v'}


# ── Pages ─────────────────────────────────────────────────────────────────────

@app.get('/healthz')
async def healthz():
    return {"ok": True}


_NOCACHE = {'Cache-Control': 'no-store, must-revalidate', 'Pragma': 'no-cache'}


@app.get('/')
async def root(request: Request):
    page = 'index.html' if auth.is_authenticated(request) else 'login.html'
    return FileResponse(STATIC_DIR / page, headers=_NOCACHE)


@app.get('/login')
async def login_page():
    return FileResponse(STATIC_DIR / 'login.html', headers=_NOCACHE)


# ── Auth ──────────────────────────────────────────────────────────────────────

@app.get('/api/me')
async def api_me(request: Request):
    return {
        "authenticated": auth.is_authenticated(request),
        "auth_required": auth.auth_enabled(),
        "username": APP_USERNAME if auth.is_authenticated(request) and auth.auth_enabled() else None,
    }


@app.post('/api/login')
async def api_login(username: str = Form(...), password: str = Form(...)):
    token = auth.login(username, password)
    if token is False:
        raise HTTPException(401, "Invalid username or password")
    response = JSONResponse({"ok": True, "auth_disabled": token is None})
    if token:
        response.set_cookie(
            'session', token, httponly=True, samesite='lax',
            max_age=SESSION_MAX_AGE, path='/',
        )
    return response


@app.post('/api/logout')
async def api_logout(request: Request):
    auth.logout(request.cookies.get('session'))
    response = JSONResponse({"ok": True})
    response.delete_cookie('session', path='/')
    return response


# ── History + storage ────────────────────────────────────────────────────────

@app.get('/api/history')
async def api_history(_: None = Depends(auth.require_auth)):
    return {"items": storage.list_analyses()}


@app.get('/api/storage')
async def api_storage(_: None = Depends(auth.require_auth)):
    return storage.storage_check()


@app.delete('/api/history/{analysis_id}')
async def api_delete(analysis_id: str, _: None = Depends(auth.require_auth)):
    if not storage.get_analysis(analysis_id):
        raise HTTPException(404, "Analysis not found")
    storage.delete_analysis(analysis_id)
    return {"ok": True}


# ── Submission ────────────────────────────────────────────────────────────────

@app.post('/api/analyze')
async def api_analyze(
    request: Request,
    url: str | None = Form(None),
    file: UploadFile | None = File(None),
    _: None = Depends(auth.require_auth),
):
    s = storage.storage_check()
    if s['available_bytes'] <= 0:
        raise HTTPException(
            507, f"Storage full ({s['used_gb']}/{s['limit_gb']} GB). "
                 "Delete old analyses first.")

    if url and url.strip():
        url = url.strip()
        if not (url.startswith(('http://', 'https://')) or 'youtu' in url[:30]):
            raise HTTPException(400, "URL must start with http:// or https://")
        analysis_id = storage.create_analysis(source='youtube', source_url=url)
    elif file and file.filename:
        ext = Path(file.filename).suffix.lower()
        if ext not in _VIDEO_EXTS:
            raise HTTPException(400, f"Unsupported file type: {ext}")
        analysis_id = storage.create_analysis(
            source='upload', source_url=file.filename,
            title=Path(file.filename).stem,
        )
        target = ANALYSES_DIR / analysis_id / f'upload{ext}'
        target.parent.mkdir(parents=True, exist_ok=True)
        bytes_written = 0
        try:
            with open(target, 'wb') as out:
                while True:
                    chunk = await file.read(1024 * 1024)
                    if not chunk:
                        break
                    bytes_written += len(chunk)
                    if bytes_written > MAX_UPLOAD_BYTES:
                        raise HTTPException(413, "File exceeds upload size limit")
                    if s['used_bytes'] + bytes_written > s['limit_bytes']:
                        raise HTTPException(507, "Upload would exceed storage limit")
                    out.write(chunk)
        except HTTPException:
            target.unlink(missing_ok=True)
            storage.delete_analysis(analysis_id)
            raise
        storage.update_analysis(analysis_id, source_url=str(target))
    else:
        raise HTTPException(400, "Provide either a URL or upload a file")

    jobs.enqueue(analysis_id)
    return {"id": analysis_id}


# ── Job status ────────────────────────────────────────────────────────────────

@app.get('/api/jobs/{analysis_id}')
async def api_job(analysis_id: str, _: None = Depends(auth.require_auth)):
    a = storage.get_analysis(analysis_id)
    if not a:
        raise HTTPException(404, "Not found")
    return {**a, "progress": jobs.get_progress(analysis_id)}


# ── Result data ───────────────────────────────────────────────────────────────

@app.get('/api/results/{analysis_id}/data')
async def api_data(analysis_id: str, _: None = Depends(auth.require_auth)):
    a = storage.get_analysis(analysis_id)
    if not a:
        raise HTTPException(404, "Not found")
    data_path = ANALYSES_DIR / analysis_id / 'data.json'
    if not data_path.exists():
        raise HTTPException(404, "Analysis is not complete yet")
    with open(data_path, 'r', encoding='utf-8') as f:
        result = json.load(f)
    return {"meta": a, "data": result}


def _find_video(analysis_id: str) -> Path | None:
    folder = ANALYSES_DIR / analysis_id
    if not folder.exists():
        return None
    primary = folder / 'video.mp4'
    if primary.exists():
        return primary
    for ext in _VIDEO_EXTS:
        candidate = folder / f'upload{ext}'
        if candidate.exists():
            return candidate
    for p in folder.iterdir():
        if p.suffix.lower() in _VIDEO_EXTS and p.is_file():
            return p
    return None


@app.get('/api/results/{analysis_id}/video')
async def api_video(
    analysis_id: str, request: Request, _: None = Depends(auth.require_auth),
):
    """Serve the video with HTTP Range support so seeking works."""
    video = _find_video(analysis_id)
    if not video:
        raise HTTPException(404, "Video file not found")

    file_size = video.stat().st_size
    range_header = request.headers.get('range')

    if range_header:
        m = re.match(r'bytes=(\d+)-(\d*)', range_header)
        if not m:
            raise HTTPException(416, "Invalid range")
        start = int(m.group(1))
        end = int(m.group(2)) if m.group(2) else file_size - 1
        end = min(end, file_size - 1)
        if start > end:
            raise HTTPException(416, "Invalid range")
        length = end - start + 1

        def chunked():
            with open(video, 'rb') as f:
                f.seek(start)
                remaining = length
                while remaining > 0:
                    chunk = f.read(min(64 * 1024, remaining))
                    if not chunk:
                        break
                    remaining -= len(chunk)
                    yield chunk

        return StreamingResponse(
            chunked(), status_code=206, media_type='video/mp4',
            headers={
                'Content-Range': f'bytes {start}-{end}/{file_size}',
                'Accept-Ranges': 'bytes',
                'Content-Length': str(length),
            },
        )

    return FileResponse(
        str(video), media_type='video/mp4',
        headers={'Accept-Ranges': 'bytes'},
    )


# ── Exports (regenerate static files on demand) ──────────────────────────────

@app.get('/api/results/{analysis_id}/export/{fmt}')
async def api_export(
    analysis_id: str, fmt: str,
    _: None = Depends(auth.require_auth),
):
    if fmt not in ('html', 'pdf', 'docx', 'txt'):
        raise HTTPException(400, "Format must be html, pdf, docx, or txt")

    a = storage.get_analysis(analysis_id)
    if not a:
        raise HTTPException(404, "Not found")
    data_path = ANALYSES_DIR / analysis_id / 'data.json'
    if not data_path.exists():
        raise HTTPException(404, "Analysis is not complete yet")

    with open(data_path, 'r', encoding='utf-8') as f:
        result = json.load(f)

    out_dir = ANALYSES_DIR / analysis_id / 'exports'
    out_dir.mkdir(exist_ok=True)

    title = a.get('title') or 'analysis'
    video = _find_video(analysis_id)
    video_arg = video.name if video else ''

    if fmt == 'html':
        from modules.html_exporter import export_to_html
        path = export_to_html(result, str(out_dir), title, video_arg)
    elif fmt == 'pdf':
        from modules.pdf_exporter import export_to_pdf
        path = export_to_pdf(result, str(out_dir), title)
    elif fmt == 'docx':
        from modules.exporter import export_to_docx
        path = export_to_docx(result, str(out_dir), title)
    else:
        from modules.exporter import export_to_txt
        path = export_to_txt(result, str(out_dir), title)

    return FileResponse(path, filename=Path(path).name)


# ── Comments ──────────────────────────────────────────────────────────────────

@app.get('/api/results/{analysis_id}/comments')
async def api_comments_list(
    analysis_id: str, _: None = Depends(auth.require_auth),
):
    if not storage.get_analysis(analysis_id):
        raise HTTPException(404, "Not found")
    return {"items": storage.list_comments(analysis_id)}


@app.post('/api/results/{analysis_id}/comments')
async def api_comments_add(
    analysis_id: str, request: Request,
    _: None = Depends(auth.require_auth),
):
    if not storage.get_analysis(analysis_id):
        raise HTTPException(404, "Not found")
    payload = await request.json()
    try:
        beat_index = int(payload.get('beat_index', 0))
    except (TypeError, ValueError):
        raise HTTPException(400, "beat_index must be an integer")
    body = (payload.get('body') or '').strip()
    if not body:
        raise HTTPException(400, "Comment body is empty")
    if len(body) > 4000:
        raise HTTPException(400, "Comment exceeds 4000 characters")
    author = (payload.get('author') or '').strip()[:60]
    cid = storage.add_comment(analysis_id, beat_index, body, author)
    return {"id": cid, "ok": True}


@app.put('/api/comments/{comment_id}')
async def api_comments_edit(
    comment_id: int, request: Request,
    _: None = Depends(auth.require_auth),
):
    if not storage.get_comment(comment_id):
        raise HTTPException(404, "Comment not found")
    payload = await request.json()
    body = (payload.get('body') or '').strip()
    if not body:
        raise HTTPException(400, "Comment body is empty")
    if len(body) > 4000:
        raise HTTPException(400, "Comment exceeds 4000 characters")
    storage.update_comment(comment_id, body)
    return {"ok": True, "body": body}


@app.delete('/api/comments/{comment_id}')
async def api_comments_delete(
    comment_id: int, _: None = Depends(auth.require_auth),
):
    storage.delete_comment(comment_id)
    return {"ok": True}
