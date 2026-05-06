/* VO and Visual Extractor — SPA frontend
   Routes:  #/new   #/job/<id>   #/view/<id>
   ───────────────────────────────────────────────────────────────────────── */

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

const fmtBytes = b => {
  if (!b) return '0 B';
  const u = ['B', 'KB', 'MB', 'GB', 'TB'];
  let i = 0; let n = b;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return `${n.toFixed(n < 10 && i > 0 ? 1 : 0)} ${u[i]}`;
};

const fmtDuration = s => {
  if (!s || s < 0) return '—';
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = Math.floor(s % 60);
  return (h > 0 ? `${h}h ` : '') + (m > 0 ? `${m}m ` : '') + `${sec}s`;
};

const fmtClock = s => {
  if (!Number.isFinite(s) || s < 0) s = 0;
  const h = Math.floor(s / 3600);
  const m = Math.floor((s % 3600) / 60);
  const sec = Math.floor(s % 60);
  const pad = n => String(n).padStart(2, '0');
  return (h > 0 ? `${pad(h)}:` : '') + `${pad(m)}:${pad(sec)}`;
};

const tsToSec = ts => {
  if (!ts) return 0;
  const p = String(ts).split(':');
  try {
    if (p.length === 3) return (+p[0]) * 3600 + (+p[1]) * 60 + parseFloat(p[2]);
    if (p.length === 2) return (+p[0]) * 60 + parseFloat(p[1]);
  } catch { /* */ }
  return 0;
};

const fmtRel = unix => {
  const diff = Date.now() / 1000 - unix;
  if (diff < 60) return 'just now';
  if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
  if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
  if (diff < 86400 * 7) return `${Math.floor(diff / 86400)}d ago`;
  return new Date(unix * 1000).toLocaleDateString();
};

const esc = s => String(s == null ? '' : s)
  .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
  .replace(/"/g, '&quot;').replace(/'/g, '&#39;');

// ── Toast ─────────────────────────────────────────────────────────────────────
function toast(msg, kind = 'info', ms = 4000) {
  const stack = $('#toast-stack');
  const t = document.createElement('div');
  t.className = `toast toast-${kind}`;
  t.textContent = msg;
  stack.appendChild(t);
  requestAnimationFrame(() => t.classList.add('toast--in'));
  setTimeout(() => {
    t.classList.remove('toast--in');
    setTimeout(() => t.remove(), 200);
  }, ms);
}

// ── API helpers ───────────────────────────────────────────────────────────────
async function api(path, opts = {}) {
  const r = await fetch(path, { credentials: 'same-origin', ...opts });
  if (r.status === 401) {
    window.location.href = '/login';
    throw new Error('unauthorized');
  }
  if (!r.ok) {
    let detail = '';
    try { detail = (await r.json()).detail || ''; } catch { /* */ }
    throw new Error(detail || `HTTP ${r.status}`);
  }
  if (r.status === 204) return null;
  const ct = r.headers.get('content-type') || '';
  if (ct.includes('application/json')) return r.json();
  return r;
}

// ── View routing ──────────────────────────────────────────────────────────────
const views = {
  new: $('[data-view="new"]'),
  job: $('[data-view="job"]'),
  viewer: $('[data-view="viewer"]'),
};

function showView(name) {
  Object.entries(views).forEach(([k, el]) => {
    el.hidden = (k !== name);
  });
}

let _viewerCleanup = null;
let _jobPoll = null;

// Tracks the most recent viewer load so concurrent loads can detect
// staleness when the user rapid-clicks between history cards.
let _viewerLoadToken = 0;

// Tiny LRU cache of analyses already fetched in this session, keyed by id.
// Eliminates network + JSON parse latency when re-opening a recent analysis.
const _analysisCache = new Map();
const _ANALYSIS_CACHE_MAX = 6;

function _cacheGet(id) {
  if (!_analysisCache.has(id)) return null;
  const entry = _analysisCache.get(id);
  // Refresh recency
  _analysisCache.delete(id);
  _analysisCache.set(id, entry);
  return entry;
}

function _cacheSet(id, entry) {
  if (_analysisCache.has(id)) _analysisCache.delete(id);
  _analysisCache.set(id, entry);
  while (_analysisCache.size > _ANALYSIS_CACHE_MAX) {
    const first = _analysisCache.keys().next().value;
    _analysisCache.delete(first);
  }
}

function _cacheInvalidate(id) {
  _analysisCache.delete(id);
}

function clearTimers() {
  if (_jobPoll) { clearInterval(_jobPoll); _jobPoll = null; }
  if (_viewerCleanup) { _viewerCleanup(); _viewerCleanup = null; }
}

function navigate(hash) {
  if (window.location.hash === hash) {
    onRouteChange();
  } else {
    window.location.hash = hash;
  }
}

function onRouteChange() {
  clearTimers();
  const h = window.location.hash || '#/new';
  if (h.startsWith('#/job/')) {
    const id = h.slice('#/job/'.length);
    return openJobView(id);
  }
  if (h.startsWith('#/view/')) {
    const id = h.slice('#/view/'.length);
    return openViewer(id);
  }
  showView('new');
  highlightHistory(null);
}

window.addEventListener('hashchange', onRouteChange);

// ── Sidebar: storage + history ────────────────────────────────────────────────
async function refreshStorage() {
  try {
    const s = await api('/api/storage');
    $('#storage-amount').textContent = `${s.used_gb} / ${s.limit_gb} GB`;
    const fill = $('#storage-fill');
    fill.style.width = `${Math.min(100, s.percent_used)}%`;
    fill.classList.toggle('storage-fill--warn', s.percent_used >= 75);
    fill.classList.toggle('storage-fill--crit', s.percent_used >= 90);
    $('#storage-foot').textContent = `${s.available_gb} GB available`;
  } catch (e) { /* ignore */ }
}

function highlightHistory(activeId) {
  $$('#history-list .hist-item').forEach(el => {
    el.classList.toggle('hist-item--active', el.dataset.id === activeId);
  });
}

// Replace a history item's title with an inline input. Saves on Enter or blur,
// cancels on Escape. Updates the viewer header too if you're viewing this id.
function startRename(item) {
  if (!item || item.querySelector('.hist-title-input')) return;
  const id = item.dataset.id;
  const titleEl = item.querySelector('.hist-title');
  const original = titleEl.textContent;

  const input = document.createElement('input');
  input.type = 'text';
  input.className = 'hist-title-input';
  input.value = original;
  input.maxLength = 200;
  titleEl.replaceWith(input);
  input.focus();
  input.select();

  let settled = false;
  const restore = (text) => {
    const el = document.createElement('div');
    el.className = 'hist-title';
    el.title = text;
    el.textContent = text;
    input.replaceWith(el);
  };

  const commit = async () => {
    if (settled) return;
    settled = true;
    const next = input.value.trim();
    if (!next || next === original) { restore(original); return; }
    restore(next);
    try {
      await api(`/api/history/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ title: next }),
      });
      // Reflect in the viewer header if we're currently looking at this id.
      const titleNode = document.getElementById('viewer-title');
      if (titleNode && window.location.hash === `#/view/${id}`) {
        titleNode.textContent = next;
      }
      toast('Renamed', 'success');
    } catch (err) {
      toast(`Rename failed: ${err.message}`, 'error');
      // Roll back the optimistic UI update.
      const el = item.querySelector('.hist-title');
      if (el) { el.textContent = original; el.title = original; }
    }
  };

  input.addEventListener('keydown', (e) => {
    if (e.key === 'Enter')  { e.preventDefault(); commit(); }
    if (e.key === 'Escape') { e.preventDefault(); settled = true; restore(original); }
  });
  input.addEventListener('blur', commit);
  input.addEventListener('click', e => e.stopPropagation());
}

async function refreshHistory() {
  try {
    const { items } = await api('/api/history');
    const list = $('#history-list');
    if (!items.length) {
      list.innerHTML = '<div class="history-empty">No analyses yet.<br>Click <strong>+ New Analysis</strong> to start.</div>';
      return;
    }
    list.innerHTML = items.map(it => {
      const status = it.status;
      const badge = {
        done: '<span class="hist-badge hist-badge--done">Ready</span>',
        running: '<span class="hist-badge hist-badge--run">Running</span>',
        queued: '<span class="hist-badge hist-badge--run">Queued</span>',
        error: '<span class="hist-badge hist-badge--err">Failed</span>',
      }[status] || '';
      return `
        <div class="hist-item" data-id="${esc(it.id)}" data-status="${esc(status)}">
          <div class="hist-item-main">
            <div class="hist-title" title="${esc(it.title || 'Untitled')}">${esc(it.title || 'Untitled')}</div>
            <div class="hist-meta">
              ${badge}
              <span>${fmtDuration(it.duration_secs)}</span>
              <span>·</span>
              <span>${esc(fmtRel(it.created_at))}</span>
            </div>
          </div>
          <div class="hist-actions">
            <button class="hist-edit" data-id="${esc(it.id)}" title="Rename" aria-label="Rename">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
                <path d="M12 20h9"/>
                <path d="M16.5 3.5a2.121 2.121 0 0 1 3 3L7 19l-4 1 1-4L16.5 3.5z"/>
              </svg>
            </button>
            <button class="hist-del" data-id="${esc(it.id)}" title="Delete from server" aria-label="Delete">
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
                <path d="M3 6h18"/>
                <path d="M8 6V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>
                <path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/>
                <path d="M10 11v6"/>
                <path d="M14 11v6"/>
              </svg>
            </button>
          </div>
        </div>`;
    }).join('');

    $$('#history-list .hist-item').forEach(el => {
      el.addEventListener('click', e => {
        // Don't navigate when clicking action buttons or while editing the title.
        if (e.target.closest('.hist-actions')) return;
        if (e.target.classList.contains('hist-title-input')) return;
        const id = el.dataset.id;
        const st = el.dataset.status;
        if (st === 'done') navigate(`#/view/${id}`);
        else navigate(`#/job/${id}`);
      });
    });
    $$('#history-list .hist-edit').forEach(b => {
      b.addEventListener('click', e => {
        e.stopPropagation();
        startRename(b.closest('.hist-item'));
      });
    });
    $$('#history-list .hist-del').forEach(b => {
      b.addEventListener('click', async e => {
        e.stopPropagation();
        const id = b.dataset.id;
        const item = b.closest('.hist-item');
        const title = $('.hist-title', item).textContent;
        if (!confirm(`Delete "${title}"?\n\nThis removes the video, analysis, and all comments from the server. This cannot be undone.`)) return;
        try {
          await api(`/api/history/${id}`, { method: 'DELETE' });
          _cacheInvalidate(id);
          toast('Analysis deleted', 'success');
          await Promise.all([refreshHistory(), refreshStorage()]);
          // If we were viewing the deleted one, go home
          const cur = window.location.hash;
          if (cur.endsWith(id)) navigate('#/new');
        } catch (err) {
          toast(`Delete failed: ${err.message}`, 'error');
        }
      });
    });

    // Re-highlight if we're on a known view
    const h = window.location.hash;
    if (h.startsWith('#/view/')) highlightHistory(h.slice('#/view/'.length));
    else if (h.startsWith('#/job/')) highlightHistory(h.slice('#/job/'.length));
  } catch (e) {
    $('#history-list').innerHTML = `<div class="history-empty">Failed to load: ${esc(e.message)}</div>`;
  }
}

// ── New analysis ─────────────────────────────────────────────────────────────
function setupNewView() {
  $$('.tab-btn').forEach(b => {
    b.addEventListener('click', () => {
      $$('.tab-btn').forEach(o => o.classList.toggle('active', o === b));
      $$('.tab-pane').forEach(p => { p.hidden = (p.dataset.tab !== b.dataset.tab); });
    });
  });

  // URL submit
  $('#submit-url').addEventListener('click', async () => {
    const url = $('#yt-url').value.trim();
    if (!url) return showSubmitError('Enter a YouTube URL.');
    await submitJob({ url });
  });
  $('#yt-url').addEventListener('keydown', e => {
    if (e.key === 'Enter') $('#submit-url').click();
  });

  // File upload
  const input = $('#file-input');
  const zone = $('#upload-zone');
  const titleEl = $('#upload-title');
  const submitBtn = $('#submit-file');

  function setFile(f) {
    if (!f) {
      titleEl.textContent = 'Click or drag a video here';
      submitBtn.disabled = true;
      input.value = '';
      return;
    }
    titleEl.innerHTML = `<strong>${esc(f.name)}</strong><br><span class="muted">${fmtBytes(f.size)}</span>`;
    submitBtn.disabled = false;
  }

  input.addEventListener('change', () => setFile(input.files[0]));
  zone.addEventListener('dragover', e => { e.preventDefault(); zone.classList.add('upload-zone--over'); });
  zone.addEventListener('dragleave', () => zone.classList.remove('upload-zone--over'));
  zone.addEventListener('drop', e => {
    e.preventDefault();
    zone.classList.remove('upload-zone--over');
    if (e.dataTransfer.files[0]) {
      input.files = e.dataTransfer.files;
      setFile(e.dataTransfer.files[0]);
    }
  });

  submitBtn.addEventListener('click', async () => {
    const f = input.files[0];
    if (!f) return showSubmitError('Choose a file first.');
    await submitJob({ file: f });
  });
}

function showSubmitError(msg) {
  const el = $('#submit-error');
  el.textContent = msg;
  el.hidden = false;
  setTimeout(() => { el.hidden = true; }, 6000);
}

function uploadWithProgress(path, fd, onProgress) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open('POST', path);
    xhr.withCredentials = true;
    xhr.responseType = 'text';
    if (onProgress && xhr.upload) {
      xhr.upload.addEventListener('progress', e => {
        if (e.lengthComputable) onProgress(e.loaded, e.total);
      });
    }
    xhr.onload = () => {
      if (xhr.status === 401) {
        window.location.href = '/login';
        return reject(new Error('unauthorized'));
      }
      let body = null;
      try { body = JSON.parse(xhr.responseText); } catch { /* */ }
      if (xhr.status >= 200 && xhr.status < 300) return resolve(body);
      reject(new Error((body && body.detail) || `HTTP ${xhr.status}`));
    };
    xhr.onerror = () => reject(new Error('Network error during upload'));
    xhr.onabort = () => reject(new Error('Upload cancelled'));
    xhr.send(fd);
  });
}

function showSubmitProgress({ stage, determinate, hint }) {
  $('#submit-error').hidden = true;
  const card = $('.submit-card');
  card.classList.add('submit-card--uploading');
  $('#submit-progress').hidden = false;
  $('#submit-progress-stage').textContent = stage;
  $('#submit-progress-hint').textContent = hint || '';
  $('#submit-progress-hint').hidden = !hint;

  const fill = $('#submit-progress-fill');
  fill.classList.toggle('submit-progress-fill--indet', !determinate);
  fill.classList.remove('submit-progress-fill--err');
  fill.style.width = determinate ? '0%' : '';

  $('#submit-progress-pct').textContent = determinate ? '0%' : '';
  $('#submit-progress-bytes').textContent = '';
  $('#submit-progress-speed').textContent = '';
  $('#submit-progress-dot1').hidden = !determinate;
  $('#submit-progress-dot2').hidden = !determinate;
}

function updateSubmitProgress(loaded, total, startedAt) {
  const pct = total ? (loaded / total) * 100 : 0;
  $('#submit-progress-fill').style.width = `${pct.toFixed(1)}%`;
  $('#submit-progress-pct').textContent = `${pct.toFixed(0)}%`;
  $('#submit-progress-bytes').textContent = total
    ? `${fmtBytes(loaded)} / ${fmtBytes(total)}`
    : fmtBytes(loaded);

  const elapsed = (performance.now() - startedAt) / 1000;
  if (elapsed > 0.5 && loaded > 0) {
    const speed = loaded / elapsed;
    let etaTxt = '';
    if (total && speed > 0) {
      const eta = (total - loaded) / speed;
      if (eta > 1) etaTxt = ` · ${fmtDuration(eta)} left`;
    }
    $('#submit-progress-speed').textContent = `${fmtBytes(speed)}/s${etaTxt}`;
  }
}

function setSubmitProgressDone(stage) {
  $('#submit-progress-stage').textContent = stage;
  const fill = $('#submit-progress-fill');
  fill.classList.remove('submit-progress-fill--indet');
  fill.style.width = '100%';
  $('#submit-progress-pct').textContent = '100%';
  $('#submit-progress-speed').textContent = '';
}

function hideSubmitProgress() {
  $('.submit-card').classList.remove('submit-card--uploading');
  $('#submit-progress').hidden = true;
  $('#submit-progress-fill').classList.remove('submit-progress-fill--indet', 'submit-progress-fill--err');
}

async function submitJob({ url, file }) {
  $('#submit-error').hidden = true;
  const btns = $$('#submit-url, #submit-file');
  btns.forEach(b => b.disabled = true);

  const fd = new FormData();
  if (url) fd.append('url', url);
  if (file) fd.append('file', file);

  if (file) {
    showSubmitProgress({
      stage: 'Uploading video…',
      determinate: true,
      hint: 'Streaming to the server — keep this tab open until upload completes.',
    });
  } else {
    showSubmitProgress({
      stage: 'Submitting…',
      determinate: false,
      hint: 'Sending request to the analyzer.',
    });
  }

  const startedAt = performance.now();

  try {
    const r = await uploadWithProgress('/api/analyze', fd, file
      ? (loaded, total) => updateSubmitProgress(loaded, total, startedAt)
      : null);

    if (file) {
      setSubmitProgressDone('Upload complete — starting analysis…');
    } else {
      setSubmitProgressDone('Submitted — starting analysis…');
    }

    toast('Analysis started', 'success');
    refreshHistory();
    refreshStorage();

    setTimeout(() => {
      hideSubmitProgress();
      navigate(`#/job/${r.id}`);
    }, file ? 600 : 200);
  } catch (e) {
    hideSubmitProgress();
    showSubmitError(e.message);
  } finally {
    btns.forEach(b => b.disabled = false);
  }
}

// ── Job progress view ────────────────────────────────────────────────────────
async function openJobView(id) {
  showView('job');
  highlightHistory(id);

  const stageEl = $('#progress-stage');
  const fillEl = $('#progress-fill');
  const msgEl = $('#progress-message');
  const titleEl = $('#job-title');
  const sourceEl = $('#job-source');
  const statusEl = $('#job-status');
  const durEl = $('#job-duration');
  const startedEl = $('#job-started');
  const actionsEl = $('#job-actions');
  $('#job-open').onclick = () => navigate(`#/view/${id}`);

  // Reset live preview each time we open a job
  $('#live-preview').hidden = true;
  $('#live-preview-list').innerHTML = '';
  $('#live-preview-meta').textContent = '—';

  // Reset analyzer log panel
  $('#log-body').innerHTML = '<div class="log-empty">Waiting for output…</div>';
  $('#log-meta').textContent = 'waiting…';
  $('#log-clear').onclick = () => {
    $('#log-body').innerHTML = '<div class="log-empty">Cleared. New lines will appear below.</div>';
  };

  let lastStatus = null;
  let lastPartialFetch = 0;
  let renderedBeatCount = 0;
  let lastLogSeq = 0;
  let lastLogFetch = 0;

  async function poll() {
    try {
      const j = await api(`/api/jobs/${id}`);
      titleEl.textContent = j.title || 'Analyzing video…';
      sourceEl.textContent = j.source === 'youtube' ? j.source_url : `Local upload · ${j.source_url}`;
      statusEl.textContent = j.status;
      durEl.textContent = fmtDuration(j.duration_secs);
      startedEl.textContent = fmtRel(j.created_at);

      const p = j.progress || {};
      const stage = (p.stage || j.status || 'queued').toUpperCase();
      stageEl.textContent = stage;
      fillEl.style.width = `${Math.min(100, p.progress || 0)}%`;
      msgEl.textContent = p.message || '—';

      // While analyzing, fetch the streaming preview every ~4s
      if (j.status === 'running' && p.stage === 'analyzing'
          && Date.now() - lastPartialFetch > 4000) {
        lastPartialFetch = Date.now();
        fetchPartial(id);
      }

      // Always poll logs (even before analysis starts — download stage too).
      // 1.5s while running/queued, slower after job ends.
      const logIntervalMs = (j.status === 'queued' || j.status === 'running') ? 1500 : 6000;
      if (Date.now() - lastLogFetch > logIntervalMs) {
        lastLogFetch = Date.now();
        fetchLogs(id);
      }

      if (j.status === 'done') {
        actionsEl.hidden = false;
        fillEl.style.width = '100%';
        if (lastStatus !== 'done') {
          toast('Analysis complete', 'success');
          refreshHistory(); refreshStorage();
          // auto-redirect after a beat
          setTimeout(() => {
            if (window.location.hash === `#/job/${id}`) navigate(`#/view/${id}`);
          }, 1500);
        }
        clearInterval(_jobPoll); _jobPoll = null;
      } else if (j.status === 'error') {
        actionsEl.hidden = true;
        stageEl.textContent = 'ERROR';
        fillEl.classList.add('progress-fill--err');
        msgEl.textContent = j.error_message || p.message || 'Analysis failed';
        if (lastStatus !== 'error') {
          toast(`Analysis failed: ${j.error_message || ''}`, 'error', 8000);
          refreshHistory();
        }
        clearInterval(_jobPoll); _jobPoll = null;
      }
      lastStatus = j.status;
    } catch (e) {
      msgEl.textContent = `Lost connection: ${e.message}`;
    }
  }

  async function fetchPartial(jobId) {
    let payload;
    try {
      const r = await fetch(`/api/results/${jobId}/partial`, { credentials: 'same-origin' });
      if (!r.ok) return;             // 404 until first chunk completes
      payload = await r.json();
    } catch { return; }
    if (!payload || !payload.data) return;

    const data = payload.data;
    const beats = (data.sections || []).flatMap((s, si) =>
      (s.beats || []).map((b, bi) => ({
        beat: b,
        section: s.title || '',
        sectionIdx: si,
        beatIdx: bi,
      }))
    );
    if (!beats.length) return;
    if (beats.length === renderedBeatCount) return;  // nothing new

    const card = $('#live-preview');
    const list = $('#live-preview-list');
    const meta = $('#live-preview-meta');

    const wasNearBottom = (list.scrollHeight - list.scrollTop - list.clientHeight) < 80;

    card.hidden = false;
    meta.textContent = `${beats.length} beats · clip ${data._chunks_done || 0}/${data._chunks_total || '?'} done`;
    list.innerHTML = beats.map(entry => renderLiveBeat(entry)).join('');
    renderedBeatCount = beats.length;

    if (wasNearBottom) list.scrollTop = list.scrollHeight;
  }

  async function fetchLogs(jobId) {
    let payload;
    try {
      const r = await fetch(`/api/jobs/${jobId}/logs?since=${lastLogSeq}`,
                            { credentials: 'same-origin' });
      if (!r.ok) return;
      payload = await r.json();
    } catch { return; }
    if (!payload) return;

    const newLines = payload.lines || [];
    if (!newLines.length) return;
    lastLogSeq = payload.seq || lastLogSeq;

    const body = $('#log-body');
    const empty = body.querySelector('.log-empty');
    if (empty) empty.remove();

    const followEl = $('#log-autoscroll');
    const stickyBottom = followEl.checked
      || (body.scrollHeight - body.scrollTop - body.clientHeight < 60);

    const html = newLines.map(({ seq, ts, line }) => {
      const cls = classifyLogLine(line);
      const t = new Date(ts * 1000);
      const stamp = `${String(t.getHours()).padStart(2, '0')}:${String(t.getMinutes()).padStart(2, '0')}:${String(t.getSeconds()).padStart(2, '0')}`;
      return `<div class="log-line ${cls}" data-seq="${seq}"><span class="log-line-ts">${stamp}</span><span class="log-line-text">${esc(line)}</span></div>`;
    }).join('');
    body.insertAdjacentHTML('beforeend', html);

    // Cap the rendered DOM at the latest 800 lines to keep things snappy
    const lines = body.querySelectorAll('.log-line');
    if (lines.length > 800) {
      for (let i = 0; i < lines.length - 800; i++) lines[i].remove();
    }

    $('#log-meta').textContent = `${lastLogSeq} line${lastLogSeq === 1 ? '' : 's'}`;
    if (stickyBottom) body.scrollTop = body.scrollHeight;
  }

  await poll();
  _jobPoll = setInterval(poll, 1500);
}

function classifyLogLine(line) {
  const s = line || '';
  if (/^ERROR\b|\bexception\b|\btraceback\b/i.test(s)) return 'log-line--err';
  if (/✓|complete\b|\bdone\b|saved|filled with \d+ beats/i.test(s)) return 'log-line--ok';
  if (/⚠|warning\b|cooling down|rate limit|retrying|under-density|JSON repaired/i.test(s)) return 'log-line--warn';
  if (/===\s*Clip\s+\d+/i.test(s) || /^\s*Clip\s+\d+\/\d+/i.test(s) || /Trying model:/i.test(s)) return 'log-line--info';
  if (/^\s*Gap\s+\d+\/\d+|coverage gap/i.test(s)) return 'log-line--info';
  return '';
}

function renderLiveBeat({ beat, section }) {
  const vo = beat.vo || {};
  const viz = beat.visual || {};
  const ts = vo.timestamp_start || viz.timestamp_start || '';
  const beatType = beat.beat_type || 'narration';
  const typeLabel = ({
    narration: 'VO',
    visual_only: 'VIS',
    ad_read: 'AD',
  })[beatType] || 'VO';

  let text = '';
  if (beatType === 'narration' || beatType === 'ad_read') {
    text = vo.text || viz.description || '';
  } else {
    text = viz.description || '';
    const dlg = (viz.dialogue || []).filter(d => d && d.quote);
    if (dlg.length) {
      const first = dlg[0];
      text = `${first.speaker || 'Speaker'}: "${first.quote}"` + (dlg.length > 1 ? ` (+${dlg.length - 1} more)` : '');
    }
  }
  const truncated = text.length > 220 ? text.slice(0, 220) + '…' : text;
  const sectionTag = section
    ? `<span class="live-beat-section" title="Section">${esc(section)}</span>` : '';

  return `
    <div class="live-beat live-beat--${esc(beatType)}">
      <span class="live-beat-ts">${esc(ts || '—')}</span>
      <span class="live-beat-type live-beat-type--${esc(beatType)}">${typeLabel}</span>
      <div class="live-beat-body">
        ${sectionTag}
        <span class="live-beat-text">${esc(truncated) || '<em class="muted">(no text)</em>'}</span>
      </div>
    </div>`;
}

// ── Viewer ───────────────────────────────────────────────────────────────────
function _showViewerLoadingState(id) {
  // Wipe stale content so the user gets instant visual feedback on click,
  // even before the network response arrives.
  $('#viewer-title').textContent = 'Loading…';
  $('#viewer-meta').innerHTML =
    '<span class="viewer-loading-pill">Fetching analysis…</span>';
  const root = $('#viewer-script');
  if (root) {
    root.innerHTML = `
      <div class="viewer-skeleton" data-id="${esc(id)}">
        <div class="skeleton-line skeleton-line--lg"></div>
        <div class="skeleton-line skeleton-line--md"></div>
        <div class="skeleton-line skeleton-line--md"></div>
        <div class="skeleton-line skeleton-line--sm"></div>
      </div>`;
  }
}

async function openViewer(id) {
  // Bump the load token so any in-flight openViewer call for a previous id
  // can detect that it's now stale and bail out before touching the DOM.
  const token = ++_viewerLoadToken;

  showView('viewer');
  highlightHistory(id);

  // Fast path: cache hit — paint immediately, skip network entirely.
  const cached = _cacheGet(id);
  if (cached) {
    if (token !== _viewerLoadToken) return;
    _renderViewer(id, cached.payload, cached.comments, token);
    return;
  }

  // Cache miss: show loading state right now so the click feels instant.
  _showViewerLoadingState(id);

  let payload, comments;
  try {
    [payload, comments] = await Promise.all([
      api(`/api/results/${id}/data`),
      api(`/api/results/${id}/comments`),
    ]);
  } catch (e) {
    if (token !== _viewerLoadToken) return; // user already navigated away
    if (e.message.includes('not complete')) {
      toast('Analysis is still running', 'info');
      return navigate(`#/job/${id}`);
    }
    toast(`Could not load: ${e.message}`, 'error');
    return navigate('#/new');
  }

  if (token !== _viewerLoadToken) return; // a newer click won — abort
  _cacheSet(id, { payload, comments });
  _renderViewer(id, payload, comments, token);
}

function _renderViewer(id, payload, comments, token) {
  if (token !== _viewerLoadToken) return;
  const meta = payload.meta;
  const data = payload.data;

  $('#viewer-title').textContent = data.title || meta.title || 'Untitled';
  $('#viewer-meta').innerHTML = `
    <span>⏱ ${esc(data.total_duration || fmtClock(meta.duration_secs))}</span>
    <span>·</span>
    <span>📁 ${meta.sections_count} sections</span>
    <span>·</span>
    <span>🎞 ${meta.beats_count} beats</span>
    <span>·</span>
    <span>${esc(fmtRel(meta.created_at))}</span>`;

  $('#viewer-delete').onclick = async () => {
    if (!confirm(`Delete "${data.title || meta.title}"?\nThis removes the video, analysis, and all comments. Cannot be undone.`)) return;
    try {
      await api(`/api/history/${id}`, { method: 'DELETE' });
      _cacheInvalidate(id);
      toast('Deleted', 'success');
      await Promise.all([refreshHistory(), refreshStorage()]);
      navigate('#/new');
    } catch (e) {
      toast(`Delete failed: ${e.message}`, 'error');
    }
  };

  $$('.export-group [data-export]').forEach(b => {
    b.onclick = () => {
      const fmt = b.dataset.export;
      window.open(`/api/results/${id}/export/${fmt}`, '_blank');
    };
  });

  $('#viewer-share').onclick = async () => {
    const link = `${window.location.origin}/#/view/${id}`;
    let copied = false;
    try {
      await navigator.clipboard.writeText(link);
      copied = true;
    } catch {
      // Fallback for non-HTTPS contexts where clipboard API is blocked
      const ta = document.createElement('textarea');
      ta.value = link;
      ta.style.position = 'fixed';
      ta.style.opacity = '0';
      document.body.appendChild(ta);
      ta.select();
      try { copied = document.execCommand('copy'); } catch { /* */ }
      ta.remove();
    }
    if (copied) {
      toast('Link copied — your teammate signs in once and lands on this analysis', 'success', 5000);
    } else {
      window.prompt('Copy this link:', link);
    }
  };

  // Index comments by beat AND by id (for popover lookup).
  const commentsByBeat = {};
  for (const c of (comments?.items || [])) {
    (commentsByBeat[c.beat_index] = commentsByBeat[c.beat_index] || []).push(c);
  }
  _indexComments(comments?.items);
  _currentAnalysisId = id;
  _startCommentsPoll(id);

  // Flatten beats with global index
  const beats = [];
  (data.sections || []).forEach((s, si) => {
    (s.beats || []).forEach((b, bi) => {
      beats.push({
        section: s.title || `Section ${si + 1}`,
        sectionIdx: si,
        sectionFirst: bi === 0,
        sectionCount: s.beats.length,
        beat: b,
      });
    });
  });

  // Render script
  const root = $('#viewer-script');
  root.innerHTML = beats.map((entry, gi) => renderBeat(entry, gi, beats, commentsByBeat[gi] || [])).join('') + renderSummary(data);

  // Wire seek handlers + comment forms
  attachViewerHandlers(id, root, beats);

  // Setup player
  setupPlayer(id, meta, beats);
}

function renderBeat(entry, idx, allBeats, comments) {
  const { beat, section, sectionFirst, sectionCount, sectionIdx } = entry;
  const vo = beat.vo || {};
  const viz = beat.visual || beat.visual_after || {};

  const voS = vo.timestamp_start || '';
  const voE = vo.timestamp_end || '';
  const vS = viz.timestamp_start || '';
  const vE = viz.timestamp_end || '';

  const startSec = tsToSec(voS) || tsToSec(vS);
  const endSec = tsToSec(vE) || tsToSec(voE);

  const tone = vo.tone ? `<span class="tone-tag">${esc(vo.tone)}</span>` : '';

  // Group anchored comments by field; keep beat-level (no field) separate.
  const live = (comments || []).filter(c => !c.resolved);
  const byField = {};
  const beatLevel = [];
  for (const c of live) {
    if (c.field) (byField[c.field] = byField[c.field] || []).push(c);
    else beatLevel.push(c);
  }

  const voText = renderHighlighted(vo.text || '', byField['vo']);
  const visDesc = renderHighlighted(viz.description || '', byField['desc']);
  const ostText = renderHighlighted(viz.on_screen_text || '', byField['ost']);
  const audioText = renderHighlighted(viz.audio_notes || '', byField['audio']);
  const summaryText = renderHighlighted(viz.summary || '', byField['summary']);

  const ost = (viz.on_screen_text && !['NONE', 'N/A'].includes(String(viz.on_screen_text).toUpperCase()))
    ? `<div class="viz-meta"><span class="viz-label">On-screen text</span><span class="viz-val" data-comment-field="ost">${ostText}</span></div>` : '';
  const audio = viz.audio_notes
    ? `<div class="viz-meta"><span class="viz-label">Audio</span><span class="viz-val" data-comment-field="audio">${audioText}</span></div>` : '';
  const summary = viz.summary
    ? `<div class="viz-summary"><span data-comment-field="summary">${summaryText}</span></div>` : '';

  const dialogueRows = (viz.dialogue || [])
    .map((d, di) => ({ d, di }))
    .filter(x => x.d && x.d.quote)
    .map(({ d, di }) => {
      const dialogueText = renderHighlighted(d.quote, byField[`dialogue:${di}`]);
      return `<div class="dlg-row"><span class="dlg-speaker">${esc(d.speaker || '')}:</span><span class="dlg-quote" data-comment-field="dialogue:${di}">&ldquo;${dialogueText}&rdquo;</span></div>`;
    })
    .join('');
  const dialogueBlock = dialogueRows
    ? `<div class="dialogue-block"><div class="dlg-label">On-camera dialogue</div>${dialogueRows}</div>` : '';

  const voBadge = voS
    ? `<span class="ts-badge ts-seek" data-seek="${startSec}" title="Seek to ${esc(voS)}">▶ ${esc(voS)}${voE ? ' – ' + esc(voE) : ''}</span>`
    : `<span class="ts-badge ts-empty">No VO</span>`;
  const visBadge = vS
    ? `<span class="ts-badge ts-seek vis-ts-seek" data-seek="${tsToSec(vS)}" title="Seek to ${esc(vS)}">▶ ${esc(vS)}${vE ? ' – ' + esc(vE) : ''}</span>`
    : `<span class="ts-badge ts-empty">—</span>`;

  const sectionHeader = sectionFirst ? `
    <div class="section-header" id="section-${sectionIdx}">
      <span class="section-number">Section ${sectionIdx + 1}</span>
      <h2 class="section-title">${esc(section)}</h2>
      <span class="section-meta">${sectionCount} beat${sectionCount === 1 ? '' : 's'}</span>
    </div>` : '';

  const beatStartTs = voS || vS;
  const beatEndTs = vE || voE;
  const beatRange = beatStartTs && beatEndTs && beatStartTs !== beatEndTs
    ? `${esc(beatStartTs)} <span class="arrow">→</span> ${esc(beatEndTs)}`
    : (beatStartTs ? esc(beatStartTs) : '');
  const beatTypeLabel = ({
    narration: 'Narration',
    visual_only: 'On-camera / Visual',
    ad_read: 'Sponsor read',
  })[beat.beat_type] || 'Beat';
  const beatTypeClass = `beat-type beat-type--${beat.beat_type || 'narration'}`;

  // Beat-level legacy notes — render as a small pin in the header.
  const beatPin = beatLevel.length
    ? `<button class="beat-note-pin" data-beat-pin="${idx}" title="${beatLevel.length} note${beatLevel.length === 1 ? '' : 's'} on this beat">💬 ${beatLevel.length}</button>`
    : '';

  return `
${sectionHeader}
<article class="beat" id="beat-${idx}" data-idx="${idx}" data-start="${startSec}" data-end="${endSec}">
  <header class="beat-head">
    <span class="beat-number-pill">Beat ${idx + 1}</span>
    <span class="${beatTypeClass}">${beatTypeLabel}</span>
    ${beatRange ? `<span class="beat-time-range" title="Beat span">⏱ ${beatRange}</span>` : ''}
    <span class="beat-head-spacer"></span>
    ${beatPin}
  </header>
  <div class="beat-body">
    <div class="vo-col">
      <div class="col-header vo-header">
        <span class="col-label">VO · Voice-over</span>
        ${voBadge}
      </div>
      <div class="col-content">
        ${tone}
        <p class="vo-text" data-comment-field="vo">${voText || '<em class="muted">— no voice-over —</em>'}</p>
      </div>
    </div>
    <div class="vis-col">
      <div class="col-header vis-header">
        <span class="col-label">VISUAL · On-screen</span>
        ${visBadge}
      </div>
      <div class="col-content">
        <p class="vis-desc" data-comment-field="desc">${visDesc}</p>
        ${dialogueBlock}
        ${ost}
        ${audio}
        ${summary}
      </div>
    </div>
  </div>
</article>`;
}

// Wrap [start_offset, end_offset) ranges of `text` in <mark> tags for each
// non-resolved comment. Overlapping ranges → first wins (later highlights
// inside an earlier one are dropped). Renders escaped HTML safe to inject.
function renderHighlighted(text, comments) {
  const safe = (s) => esc(s);
  if (!text) return '';
  if (!comments || !comments.length) return safe(text);

  const ranges = comments
    .filter(c => Number.isInteger(c.start_offset) && Number.isInteger(c.end_offset))
    .filter(c => c.start_offset >= 0 && c.end_offset <= text.length && c.end_offset > c.start_offset)
    .sort((a, b) => a.start_offset - b.start_offset);

  let out = '';
  let cursor = 0;
  for (const r of ranges) {
    if (r.start_offset < cursor) continue;  // overlapping → drop later one
    out += safe(text.slice(cursor, r.start_offset));
    const slice = safe(text.slice(r.start_offset, r.end_offset));
    out += `<mark class="cmt-highlight" data-cid="${r.id}">${slice}</mark>`;
    cursor = r.end_offset;
  }
  out += safe(text.slice(cursor));
  return out;
}

// ── Comment popover system ─────────────────────────────────────────────────
// Google-Docs-style: select text → "Add comment" floats near selection → click
// to open a popover form. Existing highlights (and beat-level legacy notes)
// open the same popover when clicked.

let _floatingAdd = null;        // floating "Add comment" button
let _popover = null;            // currently open popover element
let _currentAnalysisId = null;  // set by _renderViewer
const _commentsCache = {};      // cid -> comment object, refreshed each render

function _indexComments(items) {
  for (const k of Object.keys(_commentsCache)) delete _commentsCache[k];
  for (const c of (items || [])) _commentsCache[c.id] = c;
}

function _hideFloatingAdd() { if (_floatingAdd) { _floatingAdd.remove(); _floatingAdd = null; } }
function _hidePopover()     { if (_popover)     { _popover.remove();     _popover = null; } }

// Character offset of (node, offset) within the field's textContent.
function _offsetWithin(field, targetNode, targetOffset) {
  let total = 0;
  const walker = document.createTreeWalker(field, NodeFilter.SHOW_TEXT);
  let node;
  while ((node = walker.nextNode())) {
    if (node === targetNode) return total + targetOffset;
    total += node.nodeValue.length;
  }
  // If target is the field itself (rare), fall back to length-so-far + offset.
  return targetNode === field ? targetOffset : -1;
}

function _maybeShowAddButton() {
  const sel = window.getSelection();
  if (!sel || sel.isCollapsed || !sel.rangeCount) { _hideFloatingAdd(); return; }
  const range = sel.getRangeAt(0);
  if (range.collapsed) { _hideFloatingAdd(); return; }
  const startEl = range.startContainer.nodeType === 1
    ? range.startContainer : range.startContainer.parentElement;
  const endEl = range.endContainer.nodeType === 1
    ? range.endContainer : range.endContainer.parentElement;
  const field = startEl?.closest('[data-comment-field]');
  if (!field || field !== endEl?.closest('[data-comment-field]')) {
    _hideFloatingAdd(); return;
  }
  const beatEl = field.closest('.beat');
  if (!beatEl) { _hideFloatingAdd(); return; }
  const start = _offsetWithin(field, range.startContainer, range.startOffset);
  const end   = _offsetWithin(field, range.endContainer,   range.endOffset);
  if (start < 0 || end < 0 || end <= start) { _hideFloatingAdd(); return; }
  const quote = sel.toString();
  if (!quote.trim()) { _hideFloatingAdd(); return; }

  const rect = range.getBoundingClientRect();
  _hideFloatingAdd();
  const btn = document.createElement('button');
  btn.className = 'cmt-add-floating';
  btn.type = 'button';
  btn.innerHTML = '<span>💬</span> Add comment';
  document.body.appendChild(btn);
  btn.style.top = `${rect.bottom + 6}px`;
  btn.style.left = `${rect.left + rect.width / 2 - btn.offsetWidth / 2}px`;
  // Keep selection alive when the user mouses down on the button.
  btn.addEventListener('mousedown', e => e.preventDefault());
  btn.addEventListener('click', () => {
    const anchorRect = rect;
    _hideFloatingAdd();
    sel.removeAllRanges();
    _openNewCommentPopover({
      beatIdx: +beatEl.dataset.idx,
      field: field.dataset.commentField,
      quote, start, end, anchorRect,
    });
  });
  _floatingAdd = btn;
}

function _buildPopover(rect) {
  _hidePopover();
  const pop = document.createElement('div');
  pop.className = 'cmt-popover';
  document.body.appendChild(pop);
  // Position once we know the size.
  requestAnimationFrame(() => {
    const w = pop.offsetWidth;
    const h = pop.offsetHeight;
    const margin = 12;
    let top  = rect.bottom + 8;
    let left = rect.left + rect.width / 2 - w / 2;
    left = Math.max(margin, Math.min(left, window.innerWidth - w - margin));
    if (top + h + margin > window.innerHeight) {
      top = Math.max(margin, rect.top - h - 8);
    }
    pop.style.top  = `${top}px`;
    pop.style.left = `${left}px`;
  });
  _popover = pop;
  return pop;
}

function _openNewCommentPopover({ beatIdx, field, quote, start, end, anchorRect }) {
  const pop = _buildPopover(anchorRect);
  pop.innerHTML = `
    <div class="cmt-pop-quote">${esc(quote)}</div>
    <form class="cmt-pop-form">
      <input type="text" class="cmt-pop-author" placeholder="Your name (optional)" maxlength="60">
      <textarea class="cmt-pop-body" placeholder="Add a comment…" rows="3" required></textarea>
      <div class="cmt-pop-actions">
        <button type="button" class="link-btn cmt-pop-cancel">Cancel</button>
        <button type="submit" class="btn-primary btn-small">Comment</button>
      </div>
    </form>`;
  const ta = pop.querySelector('.cmt-pop-body');
  ta.focus();
  pop.querySelector('.cmt-pop-cancel').addEventListener('click', _hidePopover);
  pop.querySelector('form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const body = ta.value.trim();
    if (!body) return;
    const author = pop.querySelector('.cmt-pop-author').value.trim();
    try {
      await api(`/api/results/${_currentAnalysisId}/comments`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          beat_index: beatIdx, field, body, author, quote,
          start_offset: start, end_offset: end,
        }),
      });
      _hidePopover();
      toast('Comment added', 'success');
      _refetchAndRerender();
    } catch (err) {
      toast(`Could not add: ${err.message}`, 'error');
    }
  });
}

// Show one or more existing comments in a popover (handles both anchored
// highlights — `cids` is single — and beat-level legacy pins — `cids` is many).
function _openExistingPopover(cids, anchorRect) {
  const items = cids.map(id => _commentsCache[id]).filter(Boolean);
  if (!items.length) return;
  const pop = _buildPopover(anchorRect);
  pop.innerHTML = items.map(_renderThreadItem).join('');
  pop.addEventListener('click', (e) => {
    const item = e.target.closest('.cmt-thread-item');
    if (!item) return;
    const cid = +item.dataset.cid;
    if (e.target.closest('.cmt-pop-edit'))    _enterPopoverEdit(cid, item);
    if (e.target.closest('.cmt-pop-resolve')) _toggleResolve(cid);
    if (e.target.closest('.cmt-pop-del'))     _deleteComment(cid);
  });
}

function _renderThreadItem(c) {
  const quoteHtml = c.quote ? `<div class="cmt-pop-quote">${esc(c.quote)}</div>` : '';
  return `
    <div class="cmt-thread-item" data-cid="${c.id}">
      ${quoteHtml}
      <div class="cmt-pop-meta">
        <span class="cmt-pop-author">${esc(c.author || 'Anonymous')}</span>
        <span class="cmt-pop-time">${esc(fmtRel(c.created_at))}</span>
      </div>
      <div class="cmt-pop-body-text">${esc(c.body)}</div>
      <div class="cmt-pop-actions">
        <button type="button" class="link-btn cmt-pop-edit">Edit</button>
        <button type="button" class="link-btn cmt-pop-resolve">${c.resolved ? 'Reopen' : 'Resolve'}</button>
        <button type="button" class="link-btn cmt-pop-del">Delete</button>
      </div>
    </div>`;
}

function _enterPopoverEdit(cid, itemEl) {
  const c = _commentsCache[cid];
  if (!c) return;
  const quoteHtml = c.quote ? `<div class="cmt-pop-quote">${esc(c.quote)}</div>` : '';
  itemEl.innerHTML = `
    ${quoteHtml}
    <div class="cmt-pop-meta">
      <span class="cmt-pop-author">${esc(c.author || 'Anonymous')}</span>
      <span class="cmt-pop-time">${esc(fmtRel(c.created_at))}</span>
    </div>
    <textarea class="cmt-pop-body cmt-pop-edit-input" rows="3">${esc(c.body)}</textarea>
    <div class="cmt-pop-actions">
      <button type="button" class="link-btn cmt-pop-edit-cancel">Cancel</button>
      <button type="button" class="btn-primary btn-small cmt-pop-edit-save">Save</button>
    </div>`;
  const ta = itemEl.querySelector('.cmt-pop-edit-input');
  ta.focus();
  ta.setSelectionRange(ta.value.length, ta.value.length);
  itemEl.querySelector('.cmt-pop-edit-cancel').addEventListener('click', _hidePopover);
  itemEl.querySelector('.cmt-pop-edit-save').addEventListener('click', async () => {
    const next = ta.value.trim();
    if (!next) return;
    try {
      await api(`/api/comments/${cid}`, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ body: next }),
      });
      _hidePopover();
      toast('Comment updated', 'success');
      _refetchAndRerender();
    } catch (err) {
      toast(`Update failed: ${err.message}`, 'error');
    }
  });
}

async function _toggleResolve(cid) {
  const c = _commentsCache[cid];
  if (!c) return;
  try {
    await api(`/api/comments/${cid}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ resolved: !c.resolved }),
    });
    _hidePopover();
    toast(c.resolved ? 'Reopened' : 'Resolved', 'success');
    _refetchAndRerender();
  } catch (err) {
    toast(`Failed: ${err.message}`, 'error');
  }
}

async function _deleteComment(cid) {
  if (!confirm('Delete this comment?')) return;
  try {
    await api(`/api/comments/${cid}`, { method: 'DELETE' });
    _hidePopover();
    toast('Comment deleted', 'success');
    _refetchAndRerender();
  } catch (err) {
    toast(`Delete failed: ${err.message}`, 'error');
  }
}

async function _refetchAndRerender() {
  // Silent in-place refresh: fetch new comments, swap only the highlight HTML
  // inside the existing field elements, never re-render the viewer or reset
  // the player. Scroll position cannot move because nothing layouts.
  const h = window.location.hash;
  if (!h.startsWith('#/view/')) return;
  const id = h.slice('#/view/'.length);
  const cached = _cacheGet(id);
  if (!cached?.payload) {
    // Fall back only when the cache was evicted between viewer-open and now.
    _cacheInvalidate(id);
    return openViewer(id);
  }
  try {
    const fresh = await api(`/api/results/${id}/comments`);
    cached.comments = fresh;
    _cacheSet(id, cached);
    _patchCommentsInPlace(fresh.items || []);
    _commentsPollSig = _commentsSignature(fresh.items || []);
  } catch (e) {
    toast(`Could not refresh: ${e.message}`, 'error');
  }
}

// Swap ONLY the comment-highlight markup inside each [data-comment-field],
// leaving every other DOM node — including the player, the headers, the
// scroll position — untouched. textContent strips existing <mark> wrappers
// and gives back the raw text whose offsets the comments were anchored to,
// so re-rendering is safe and idempotent.
function _patchCommentsInPlace(items) {
  _indexComments(items);

  // beat_index → field → comments[]
  const byBeatField = {};
  for (const c of (items || [])) {
    if (c.resolved || !c.field) continue;
    const m = byBeatField[c.beat_index] = byBeatField[c.beat_index] || {};
    (m[c.field] = m[c.field] || []).push(c);
  }

  document.querySelectorAll('.beat [data-comment-field]').forEach(el => {
    const beatEl = el.closest('.beat');
    if (!beatEl) return;
    const idx = parseInt((beatEl.id || '').replace('beat-', ''), 10);
    if (Number.isNaN(idx)) return;
    const field = el.dataset.commentField;
    const fieldComments = byBeatField[idx]?.[field] || [];
    const rawText = el.textContent; // <mark> children flatten to plain text
    el.innerHTML = renderHighlighted(rawText, fieldComments);
  });
}

// ── Comments live-refresh ──────────────────────────────────────────────────
// Polls the comments endpoint every few seconds while the viewer is open so
// teammates' adds/edits/deletes show up without a manual reload. Skipped
// while a popover is open to avoid wiping the user's in-progress typing.
let _commentsPollHandle = null;
let _commentsPollSig    = '';

function _startCommentsPoll(id) {
  _stopCommentsPoll();
  // Seed the signature with what we just rendered so the first poll doesn't
  // false-positive into an immediate re-render.
  _commentsPollSig = _commentsSignature(Object.values(_commentsCache));

  _commentsPollHandle = setInterval(async () => {
    // Aborted: navigated away or popover open (don't yank the form mid-type).
    if (id !== _currentAnalysisId) return _stopCommentsPoll();
    if (_popover || _floatingAdd) return;
    if (document.hidden) return; // tab in background — skip the request

    try {
      const fresh = await api(`/api/results/${id}/comments`);
      const sig   = _commentsSignature(fresh.items || []);
      if (sig === _commentsPollSig) return; // no change
      _commentsPollSig = sig;

      const cached = _cacheGet(id);
      if (cached) {
        cached.comments = fresh;
        _cacheSet(id, cached);
      }
      // Silent in-place patch — never re-renders the viewer, can't shift
      // scroll position. A teammate's add/edit/delete just appears.
      _patchCommentsInPlace(fresh.items || []);
    } catch {
      // Network blip — keep polling, don't toast (would spam).
    }
  }, 6000);
}

function _stopCommentsPoll() {
  if (_commentsPollHandle) {
    clearInterval(_commentsPollHandle);
    _commentsPollHandle = null;
  }
}

// Stable hash of the comment list (id + body + resolved + edited timestamp if
// present). Two snapshots produce the same string only if nothing changed.
function _commentsSignature(items) {
  if (!items || !items.length) return '';
  return items
    .map(c => `${c.id}:${c.body?.length || 0}:${c.resolved ? 1 : 0}:${c.created_at || 0}`)
    .sort()
    .join('|');
}

function setupCommentSelection() {
  if (setupCommentSelection._done) return;
  setupCommentSelection._done = true;
  document.addEventListener('mouseup', (e) => {
    if (e.target.closest('.cmt-popover, .cmt-add-floating')) return;
    setTimeout(_maybeShowAddButton, 0);
  });
  document.addEventListener('selectionchange', () => {
    const sel = window.getSelection();
    if (!sel || sel.isCollapsed) _hideFloatingAdd();
  });
  document.addEventListener('mousedown', (e) => {
    if (!_popover) return;
    if (e.target.closest('.cmt-popover')) return;
    if (e.target.closest('.cmt-highlight, .beat-note-pin, .cmt-add-floating')) return;
    _hidePopover();
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') { _hidePopover(); _hideFloatingAdd(); return; }

    // Space-bar = play/pause the analysis video, but only when the user isn't
    // typing into a comment field, an input, or has another modifier held.
    if (e.code === 'Space' && !e.repeat && !e.ctrlKey && !e.metaKey && !e.altKey) {
      const t = e.target;
      const typing = t && (
        t.tagName === 'INPUT' || t.tagName === 'TEXTAREA' ||
        t.isContentEditable || t.closest?.('.cmt-popover')
      );
      if (typing) return;
      if (window._player?.togglePlay) {
        e.preventDefault();   // stop the page from scrolling on space
        window._player.togglePlay();
      }
    }
  });
}

function renderSummary(data) {
  const summary = esc(data.summary || '');
  const peaks = (data.peak_moments || []).map(pm => `
    <div class="peak-item" data-seek="${tsToSec(pm.timestamp || '')}" title="Jump to ${esc(pm.timestamp || '')}">
      <span class="peak-ts">▶ ${esc(pm.timestamp || '')}</span>
      <span class="peak-desc">${esc(pm.description || '')}</span>
    </div>`).join('') || '<p class="empty">None identified.</p>';
  const highs = (data.highlights || []).map(h => `<li class="highlight-item">${esc(h)}</li>`).join('')
    || '<li class="empty">None identified.</li>';

  return `
  <div class="summary-section">
    <h2 class="summary-title">Summary &amp; Highlights</h2>
    <div class="summary-grid">
      <div class="sum-card sum-card--full">
        <div class="sum-card-header">Full Summary</div>
        <p class="sum-body">${summary || '<em class="muted">No summary generated.</em>'}</p>
      </div>
      <div class="sum-card">
        <div class="sum-card-header">Peak Moments <small>(click to jump)</small></div>
        <div class="peaks-list">${peaks}</div>
      </div>
      <div class="sum-card">
        <div class="sum-card-header">Key Highlights</div>
        <ul class="highlights-list">${highs}</ul>
      </div>
    </div>
  </div>`;
}

function attachViewerHandlers(id, root, beats) {
  setupCommentSelection();

  root.addEventListener('click', e => {
    // Click on a highlight → open its comment popover.
    const hl = e.target.closest('.cmt-highlight');
    if (hl) {
      e.stopPropagation();
      const cid = +hl.dataset.cid;
      _openExistingPopover([cid], hl.getBoundingClientRect());
      return;
    }
    // Click on a beat-level legacy pin → open all beat-level comments here.
    const pin = e.target.closest('.beat-note-pin');
    if (pin) {
      e.stopPropagation();
      const beatIdx = +pin.dataset.beatPin;
      const cids = Object.values(_commentsCache)
        .filter(c => c.beat_index === beatIdx && !c.field && !c.resolved)
        .map(c => c.id);
      _openExistingPopover(cids, pin.getBoundingClientRect());
      return;
    }
    // Seek
    const seekEl = e.target.closest('[data-seek]');
    if (seekEl) {
      e.stopPropagation();
      const t = parseFloat(seekEl.dataset.seek) || 0;
      try { window._highlight?.(t); } catch { /* */ }
      window._player?.seek(t);
      return;
    }
    // Beat card click → seek to its start. Skip while a text selection
    // exists, otherwise users can't select text without seeking.
    const beatEl = e.target.closest('.beat');
    if (beatEl && window.getSelection().isCollapsed) {
      const start = parseFloat(beatEl.dataset.start) || 0;
      try { window._highlight?.(start); } catch { /* */ }
      window._player?.seek(start);
    }
  });
}

// ── Player setup ─────────────────────────────────────────────────────────────
// YouTube-first: if the analysis came from YouTube, embed the YT iframe so the
// video plays in-page (no tab switch). If YT blocks embedding (errors 101/150)
// or the API can't load, transparently fall back to the local downloaded file.
function setupPlayer(id, meta, beats) {
  const wrap = $('#player-wrap');
  wrap.innerHTML = '';

  const onTime = makeTimeTracker(beats);
  // Expose globally so beat clicks can always update the highlight
  // even if the player hasn't initialized yet.
  window._highlight = onTime;

  const useLocal = () => mountLocalVideo(id, wrap, onTime);

  if (meta.yt_id) {
    mountYouTubeEmbed(meta.yt_id, wrap, onTime, useLocal);
  } else {
    useLocal();
  }
}

function makeTimeTracker(beats) {
  const autoscroll = $('#autoscroll');
  let activeIdx = -1;
  let lastScrollIdx = -1;

  const findActive = (t) => {
    for (let i = 0; i < beats.length; i++) {
      const el = $(`#beat-${i}`);
      if (!el) continue;
      const s = parseFloat(el.dataset.start) || 0;
      const next = i + 1 < beats.length
        ? (parseFloat($(`#beat-${i + 1}`)?.dataset.start) || Infinity)
        : Infinity;
      const e = parseFloat(el.dataset.end) || next;
      if (t >= s && t < Math.max(e, next)) return i;
    }
    return -1;
  };

  return (t) => {
    if (!Number.isFinite(t)) return;
    $('#cur-time').textContent = fmtClock(t);
    const i = findActive(t);
    if (i === activeIdx) return;
    if (activeIdx >= 0) $(`#beat-${activeIdx}`)?.classList.remove('beat--active');
    activeIdx = i;
    if (i >= 0) {
      const el = $(`#beat-${i}`);
      el?.classList.add('beat--active');
      $('#cur-beat').textContent = `#${i + 1}`;
      if (autoscroll?.checked && i !== lastScrollIdx) {
        el?.scrollIntoView({ behavior: 'smooth', block: 'center' });
        lastScrollIdx = i;
      }
    } else {
      $('#cur-beat').textContent = '—';
    }
  };
}

function mountLocalVideo(id, wrap, onTime) {
  wrap.innerHTML = '';
  const video = document.createElement('video');
  video.id = 'main-video';
  video.controls = true;
  video.preload = 'metadata';
  video.src = `/api/results/${id}/video`;
  wrap.appendChild(video);

  const tick = () => onTime(video.currentTime);
  video.addEventListener('timeupdate', tick);
  video.addEventListener('seeked', tick);
  video.addEventListener('loadedmetadata', tick);
  video.addEventListener('error', () => showVideoUnavailable(wrap));

  window._player = {
    kind: 'local',
    seek(t) {
      try {
        const target = Math.max(0, t);
        video.currentTime = target;
        onTime(target); // optimistic highlight (don't wait for the seeked event)
        const p = video.play();
        if (p && p.catch) p.catch(() => { /* autoplay block — ignore */ });
        if (window.innerWidth < 1100) wrap.scrollIntoView({ behavior: 'smooth', block: 'start' });
      } catch { /* */ }
    },
    togglePlay() {
      try {
        if (video.paused) {
          const p = video.play();
          if (p && p.catch) p.catch(() => { /* autoplay block — ignore */ });
        } else {
          video.pause();
        }
      } catch { /* */ }
    },
  };

  _viewerCleanup = () => {
    try { video.pause(); video.removeAttribute('src'); video.load(); } catch { /* */ }
    window._player = null;
  };

  setPlayerHint('Local file · click any beat to seek · plays continuously');
}

function mountYouTubeEmbed(ytId, wrap, onTime, fallback) {
  const ytDiv = document.createElement('div');
  ytDiv.id = 'yt-player-host';
  ytDiv.style.cssText = 'position:absolute;inset:0;width:100%;height:100%;';
  wrap.appendChild(ytDiv);

  setPlayerHint('YouTube embed · click any beat to seek · plays continuously');

  let ytPlayer = null;
  let pollTimer = null;
  let resolved = false;

  const cleanup = () => {
    if (pollTimer) { clearInterval(pollTimer); pollTimer = null; }
    if (ytPlayer) { try { ytPlayer.destroy(); } catch { /* */ } ytPlayer = null; }
  };

  const fallTo = (reason) => {
    if (resolved && reason !== 'error') return;
    resolved = true;
    cleanup();
    console.warn(`[player] YouTube embed unavailable (${reason}), using local file`);
    setPlayerHint('YouTube blocked embedding · using local copy');
    fallback();
  };

  const create = () => {
    try {
      ytPlayer = new YT.Player('yt-player-host', {
        width: '100%',
        height: '100%',
        videoId: ytId,
        playerVars: {
          autoplay: 0, rel: 0, modestbranding: 1, playsinline: 1, enablejsapi: 1,
        },
        events: {
          onReady: () => {
            resolved = true;
            pollTimer = setInterval(() => {
              try {
                const t = ytPlayer.getCurrentTime();
                onTime(t);
              } catch { /* */ }
            }, 250);

            window._player = {
              kind: 'youtube',
              seek(t) {
                try {
                  const target = Math.max(0, t);
                  ytPlayer.seekTo(target, true);
                  ytPlayer.playVideo();
                  onTime(target); // optimistic highlight (don't wait for the next poll tick)
                  if (window.innerWidth < 1100) wrap.scrollIntoView({ behavior: 'smooth', block: 'start' });
                } catch { /* */ }
              },
              togglePlay() {
                try {
                  const playing = ytPlayer.getPlayerState() === 1; // YT.PlayerState.PLAYING
                  if (playing) ytPlayer.pauseVideo(); else ytPlayer.playVideo();
                } catch { /* */ }
              },
            };

            _viewerCleanup = () => {
              cleanup();
              window._player = null;
            };
          },
          onError: (e) => {
            // 100 = video not found/private, 101/150 = embedding disabled,
            //   2 = invalid param, 5 = HTML5 error
            console.warn('[player] YT onError code', e?.data);
            fallTo('error');
          },
        },
      });
    } catch (err) {
      console.warn('[player] YT.Player construction failed:', err);
      fallTo('exception');
    }
  };

  // Safety: if onReady never fires within 8s, drop to local file
  setTimeout(() => { if (!resolved) fallTo('timeout'); }, 8000);

  if (window.YT && window.YT.Player) {
    create();
  } else {
    // Chain into any existing onYouTubeIframeAPIReady callback
    const prev = window.onYouTubeIframeAPIReady;
    window.onYouTubeIframeAPIReady = () => { if (prev) try { prev(); } catch { /* */ } create(); };
    if (!document.querySelector('script[data-yt-api]')) {
      const s = document.createElement('script');
      s.src = 'https://www.youtube.com/iframe_api';
      s.dataset.ytApi = '1';
      s.onerror = () => fallTo('api-load-failed');
      document.head.appendChild(s);
    }
  }
}

function setPlayerHint(text) {
  const el = document.querySelector('.player-hint');
  if (el) el.textContent = text;
}

function showVideoUnavailable(wrap) {
  wrap.innerHTML = `
    <div class="player-error">
      <div class="player-error-icon">📼</div>
      <p>Video file is unavailable.</p>
      <p class="muted">It may have been deleted or failed to download.</p>
    </div>`;
}

// ── Boot ─────────────────────────────────────────────────────────────────────
async function boot() {
  setupNewView();
  $('#new-btn').addEventListener('click', () => navigate('#/new'));
  $('#refresh-btn').addEventListener('click', () => {
    refreshHistory();
    refreshStorage();
  });
  $('#logout-btn').addEventListener('click', async () => {
    try {
      await api('/api/logout', { method: 'POST' });
    } catch { /* */ }
    window.location.href = '/login';
  });

  // ── Sidebar collapse toggle (state persists across reloads) ────────────────
  const SIDEBAR_KEY = 'sidebar-collapsed';
  const shell = document.querySelector('.app-shell');
  const toggleBtn = $('#sidebar-toggle');
  const applySidebarState = (collapsed) => {
    shell.classList.toggle('sidebar-collapsed', collapsed);
    toggleBtn.textContent = collapsed ? '☰' : '✕';
    toggleBtn.title = collapsed ? 'Show sidebar' : 'Hide sidebar';
  };
  applySidebarState(localStorage.getItem(SIDEBAR_KEY) === '1');
  toggleBtn.addEventListener('click', () => {
    const next = !shell.classList.contains('sidebar-collapsed');
    localStorage.setItem(SIDEBAR_KEY, next ? '1' : '0');
    applySidebarState(next);
  });

  await Promise.all([refreshHistory(), refreshStorage()]);
  setInterval(refreshStorage, 30000);

  // Auto-refresh history while a job is running
  setInterval(() => {
    if ($$('#history-list .hist-badge--run').length) refreshHistory();
  }, 4000);

  if (!window.location.hash) window.location.hash = '#/new';
  onRouteChange();
}

boot();
