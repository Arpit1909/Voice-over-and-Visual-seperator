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
          <button class="hist-del" data-id="${esc(it.id)}" title="Delete from server">✕</button>
        </div>`;
    }).join('');

    $$('#history-list .hist-item').forEach(el => {
      el.addEventListener('click', e => {
        if (e.target.classList.contains('hist-del')) return;
        const id = el.dataset.id;
        const st = el.dataset.status;
        if (st === 'done') navigate(`#/view/${id}`);
        else navigate(`#/job/${id}`);
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

async function submitJob({ url, file }) {
  $('#submit-error').hidden = true;
  const btns = $$('.submit-btn');
  btns.forEach(b => b.disabled = true);
  try {
    const fd = new FormData();
    if (url) fd.append('url', url);
    if (file) fd.append('file', file);
    const r = await api('/api/analyze', { method: 'POST', body: fd });
    toast('Analysis started', 'success');
    refreshHistory();
    refreshStorage();
    navigate(`#/job/${r.id}`);
  } catch (e) {
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

  let lastStatus = null;

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

  await poll();
  _jobPoll = setInterval(poll, 1500);
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

  // Index comments by beat
  const commentsByBeat = {};
  for (const c of (comments?.items || [])) {
    (commentsByBeat[c.beat_index] = commentsByBeat[c.beat_index] || []).push(c);
  }

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
  const voText = esc(vo.text || '');

  const ost = (viz.on_screen_text && !['NONE', 'N/A'].includes(String(viz.on_screen_text).toUpperCase()))
    ? `<div class="viz-meta"><span class="viz-label">On-screen text</span><span class="viz-val">${esc(viz.on_screen_text)}</span></div>` : '';
  const audio = viz.audio_notes ? `<div class="viz-meta"><span class="viz-label">Audio</span><span class="viz-val">${esc(viz.audio_notes)}</span></div>` : '';
  const summary = viz.summary ? `<div class="viz-summary"><span>${esc(viz.summary)}</span></div>` : '';

  const dialogueRows = (viz.dialogue || [])
    .filter(d => d && d.quote)
    .map(d => `<div class="dlg-row"><span class="dlg-speaker">${esc(d.speaker || '')}:</span><span class="dlg-quote">&ldquo;${esc(d.quote)}&rdquo;</span></div>`)
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

  const commentBlock = renderCommentBlock(idx, comments);

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

  return `
${sectionHeader}
<article class="beat" id="beat-${idx}" data-idx="${idx}" data-start="${startSec}" data-end="${endSec}">
  <header class="beat-head">
    <span class="beat-number-pill">Beat ${idx + 1}</span>
    <span class="${beatTypeClass}">${beatTypeLabel}</span>
    ${beatRange ? `<span class="beat-time-range" title="Beat span">⏱ ${beatRange}</span>` : ''}
    <span class="beat-head-spacer"></span>
  </header>
  <div class="beat-body">
    <div class="vo-col">
      <div class="col-header vo-header">
        <span class="col-label">VO · Voice-over</span>
        ${voBadge}
      </div>
      <div class="col-content">
        ${tone}
        <p class="vo-text">${voText || '<em class="muted">— no voice-over —</em>'}</p>
      </div>
    </div>
    <div class="vis-col">
      <div class="col-header vis-header">
        <span class="col-label">VISUAL · On-screen</span>
        ${visBadge}
      </div>
      <div class="col-content">
        <p class="vis-desc">${esc(viz.description || '')}</p>
        ${dialogueBlock}
        ${ost}
        ${audio}
        ${summary}
      </div>
    </div>
  </div>
  ${commentBlock}
</article>`;
}

function renderCommentBlock(beatIdx, comments) {
  const count = comments.length;
  const list = comments.map(c => `
    <div class="comment" data-cid="${c.id}">
      <div class="comment-head">
        <span class="comment-author">${esc(c.author || 'Anonymous')}</span>
        <span class="comment-time">${esc(fmtRel(c.created_at))}</span>
        <div class="comment-actions">
          <button class="comment-edit" data-cid="${c.id}" title="Edit">✎</button>
          <button class="comment-del" data-cid="${c.id}" title="Delete">✕</button>
        </div>
      </div>
      <div class="comment-body" data-original="${esc(c.body)}">${esc(c.body)}</div>
    </div>`).join('');

  return `
  <div class="comment-block">
    <button class="comment-toggle" data-toggle="${beatIdx}">
      💬 ${count === 0 ? 'Add note' : `${count} note${count === 1 ? '' : 's'}`}
    </button>
    <div class="comment-panel" id="cmt-${beatIdx}" hidden>
      <div class="comment-list">${list || '<p class="comment-empty">No notes yet.</p>'}</div>
      <form class="comment-form" data-form="${beatIdx}">
        <input type="text" name="author" placeholder="Your name (optional)" maxlength="60">
        <textarea name="body" placeholder="Add a note for the team…" rows="2" required></textarea>
        <button type="submit" class="btn-primary btn-small">Post note</button>
      </form>
    </div>
  </div>`;
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
  root.addEventListener('click', e => {
    // Seek
    const seekEl = e.target.closest('[data-seek]');
    if (seekEl) {
      e.stopPropagation();
      const t = parseFloat(seekEl.dataset.seek) || 0;
      try { window._highlight?.(t); } catch { /* */ }
      window._player?.seek(t);
      return;
    }
    // Comment toggle
    const tog = e.target.closest('.comment-toggle');
    if (tog) {
      const panel = $(`#cmt-${tog.dataset.toggle}`);
      panel.hidden = !panel.hidden;
      return;
    }
    // Comment delete
    const del = e.target.closest('.comment-del');
    if (del) {
      e.stopPropagation();
      if (!confirm('Delete this note?')) return;
      api(`/api/comments/${del.dataset.cid}`, { method: 'DELETE' })
        .then(() => {
          _cacheInvalidate(id);
          del.closest('.comment').remove();
          updateCommentCounts(root);
          toast('Note deleted', 'success');
        })
        .catch(err => toast(`Delete failed: ${err.message}`, 'error'));
      return;
    }
    // Comment edit (enter edit mode)
    const editBtn = e.target.closest('.comment-edit');
    if (editBtn) {
      e.stopPropagation();
      enterCommentEditMode(editBtn.closest('.comment'));
      return;
    }
    // Comment edit — save
    const saveBtn = e.target.closest('.comment-save');
    if (saveBtn) {
      e.stopPropagation();
      saveCommentEdit(saveBtn.closest('.comment'), root);
      return;
    }
    // Comment edit — cancel
    const cancelBtn = e.target.closest('.comment-cancel');
    if (cancelBtn) {
      e.stopPropagation();
      exitCommentEditMode(cancelBtn.closest('.comment'));
      return;
    }
    // Beat card click → seek to its start (continuous play)
    const beatEl = e.target.closest('.beat');
    if (beatEl && !e.target.closest('.comment-block')) {
      const start = parseFloat(beatEl.dataset.start) || 0;
      // Always update highlight immediately, even if player isn't ready yet
      try { window._highlight?.(start); } catch { /* */ }
      window._player?.seek(start);
    }
  });

  // Comment forms (delegated submit)
  root.addEventListener('submit', async e => {
    const form = e.target.closest('.comment-form');
    if (!form) return;
    e.preventDefault();
    const idx = +form.dataset.form;
    const fd = new FormData(form);
    const body = (fd.get('body') || '').toString().trim();
    if (!body) return;
    const author = (fd.get('author') || '').toString().trim();
    try {
      const r = await api(`/api/results/${id}/comments`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ beat_index: idx, body, author }),
      });
      _cacheInvalidate(id);
      // Optimistic insert
      const list = form.parentElement.querySelector('.comment-list');
      if (list.querySelector('.comment-empty')) list.innerHTML = '';
      const html = `
        <div class="comment" data-cid="${r.id}">
          <div class="comment-head">
            <span class="comment-author">${esc(author || 'Anonymous')}</span>
            <span class="comment-time">just now</span>
            <button class="comment-del" data-cid="${r.id}" title="Delete">✕</button>
          </div>
          <div class="comment-body">${esc(body)}</div>
        </div>`;
      list.insertAdjacentHTML('beforeend', html);
      form.reset();
      updateCommentCounts(root);
    } catch (err) {
      toast(`Could not post note: ${err.message}`, 'error');
    }
  });
}

function updateCommentCounts(root) {
  $$('.comment-block', root).forEach(block => {
    const count = $$('.comment', block).length;
    const tog = $('.comment-toggle', block);
    tog.textContent = count === 0
      ? '💬 Add note'
      : `💬 ${count} note${count === 1 ? '' : 's'}`;
  });
}

function enterCommentEditMode(commentEl) {
  if (!commentEl || commentEl.classList.contains('comment--editing')) return;
  commentEl.classList.add('comment--editing');
  const bodyEl = $('.comment-body', commentEl);
  const original = bodyEl.dataset.original ?? bodyEl.textContent;
  bodyEl.dataset.original = original;
  bodyEl.innerHTML = `
    <textarea class="comment-edit-input" rows="2">${esc(original)}</textarea>
    <div class="comment-edit-actions">
      <button type="button" class="btn-primary btn-small comment-save">Save</button>
      <button type="button" class="link-btn comment-cancel">Cancel</button>
    </div>`;
  const ta = $('.comment-edit-input', commentEl);
  ta.focus();
  ta.setSelectionRange(ta.value.length, ta.value.length);
  // Submit on Cmd/Ctrl+Enter
  ta.addEventListener('keydown', (ev) => {
    if ((ev.metaKey || ev.ctrlKey) && ev.key === 'Enter') {
      ev.preventDefault();
      saveCommentEdit(commentEl, document);
    } else if (ev.key === 'Escape') {
      ev.preventDefault();
      exitCommentEditMode(commentEl);
    }
  });
}

function exitCommentEditMode(commentEl) {
  if (!commentEl) return;
  commentEl.classList.remove('comment--editing');
  const bodyEl = $('.comment-body', commentEl);
  const original = bodyEl.dataset.original ?? '';
  bodyEl.textContent = original;
}

async function saveCommentEdit(commentEl, root) {
  if (!commentEl) return;
  const cid = commentEl.dataset.cid;
  const ta = $('.comment-edit-input', commentEl);
  if (!ta) return;
  const next = ta.value.trim();
  if (!next) {
    toast('Note can\'t be empty', 'error');
    return;
  }
  ta.disabled = true;
  try {
    await api(`/api/comments/${cid}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ body: next }),
    });
    const bodyEl = $('.comment-body', commentEl);
    bodyEl.dataset.original = next;
    commentEl.classList.remove('comment--editing');
    bodyEl.textContent = next;
    // mark as edited
    let timeEl = $('.comment-time', commentEl);
    if (timeEl && !timeEl.textContent.includes('edited')) {
      timeEl.textContent = `${timeEl.textContent} · edited`;
    }
    toast('Note updated', 'success');
  } catch (err) {
    ta.disabled = false;
    toast(`Update failed: ${err.message}`, 'error');
  }
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
