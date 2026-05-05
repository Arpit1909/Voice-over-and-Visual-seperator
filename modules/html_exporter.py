import os
import re
import html as html_lib
import urllib.parse
from datetime import datetime


def export_to_html(data: dict, output_dir: str, title: str, video_source: str = None) -> str:
    doc = _build(data, title, video_source or '')
    out = os.path.join(output_dir, f"{_safe(title)}_analysis.html")
    with open(out, 'w', encoding='utf-8') as f:
        f.write(doc)
    print(f"HTML saved: {out}")
    return out


# ── core ──────────────────────────────────────────────────────────────────────

def _build(data: dict, title: str, video_source: str) -> str:
    doc_title   = _esc(data.get('title', title))
    duration    = _esc(data.get('total_duration', 'N/A'))
    sections    = data.get('sections', [])
    total_beats = sum(len(s.get('beats', [])) for s in sections)

    player_html, player_type, yt_id = _video_player(video_source)
    has_player  = player_type != 'none'
    layout_cls  = 'app-layout' if has_player else 'app-layout no-player'
    yt_id_js    = yt_id  # passed into template for JS fallback

    # Build player card HTML (or empty string)
    if has_player:
        player_section = f'''
  <div class="player-pane" id="player-section">
    <div class="player-card">
      <div class="player-card-header">
        <span>&#9654; Video Player</span>
        <span class="player-hint">Click timestamps to seek (or open on YouTube)</span>
      </div>
      {player_html}
      <div class="player-footer">
        <div>
          <span class="current-time-label">Time: </span>
          <span id="current-time-display">00:00</span>
        </div>
        <div>
          <span class="active-beat-label">Beat: </span>
          <span id="active-beat-display">-</span>
        </div>
      </div>
    </div>
  </div>'''
    else:
        player_section = ''

    # YouTube API script tag
    yt_api_tag = '<script src="https://www.youtube.com/iframe_api"></script>' if player_type == 'youtube' else ''

    toc_html     = _toc(sections)
    body_html    = _sections(sections)
    summary_html = _summary(data)
    generated    = datetime.now().strftime('%B %d, %Y  %H:%M')

    return _TEMPLATE.format(
        doc_title      = doc_title,
        duration       = duration,
        section_count  = len(sections),
        beat_count     = total_beats,
        layout_cls     = layout_cls,
        toc            = toc_html,
        body           = body_html,
        summary        = summary_html,
        player_section = player_section,
        player_type    = player_type,
        yt_api_tag     = yt_api_tag,
        yt_id          = yt_id_js,
        generated      = generated,
    )


def _video_player(source: str) -> tuple:
    if not source:
        return '', 'none', ''

    # Base64 data URL — video is embedded directly in the HTML.
    # Reuse the 'local' player type; the same <video> JS handles seek/pause.
    if source.startswith('data:'):
        html = f'''
<div class="player-wrap">
  <video id="local-video" controls preload="metadata" style="background:#000">
    <source src="{source}" type="video/mp4">
  </video>
</div>'''
        return html, 'local', ''

    yt_id = _extract_yt_id(source)
    if yt_id:
        html = f'''
<div class="player-wrap">
  <iframe id="yt-iframe"
    src="https://www.youtube.com/embed/{yt_id}?enablejsapi=1&rel=0"
    frameborder="0"
    allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture"
    allowfullscreen></iframe>
</div>'''
        return html, 'youtube', yt_id

    # Use just the filename as a relative path — HTML and video live in the same output folder
    file_url  = os.path.basename(source)
    file_name = file_url
    ext  = os.path.splitext(source)[1].lower().lstrip('.')
    mime = {'mp4': 'video/mp4', 'webm': 'video/webm', 'mov': 'video/mp4',
            'avi': 'video/mp4', 'mkv': 'video/mp4'}.get(ext, 'video/mp4')
    html = f'''
<div class="player-wrap" id="local-player-wrap">
  <video id="local-video" controls preload="metadata"
         onerror="showLocalMissing()" style="background:#000">
    <source src="{file_url}" type="{mime}"
            onerror="showLocalMissing()">
  </video>
</div>
<script>
var LOCAL_FILENAME = {repr(file_name)};
function showLocalMissing() {{
  var wrap = document.getElementById('local-player-wrap');
  if (!wrap || wrap.dataset.missing) return;
  wrap.dataset.missing = '1';
  wrap.style.paddingBottom = '0';
  wrap.style.height = 'auto';
  wrap.innerHTML =
    '<div style="padding:20px 16px;text-align:center;background:#0f172a;color:#f1f5f9;">' +
    '<div style="font-size:28px;margin-bottom:8px;">&#128249;</div>' +
    '<p style="font-size:13px;color:#94a3b8;margin-bottom:6px;">Local video file not found.</p>' +
    '<p style="font-size:11px;color:#64748b;">Place <strong style="color:#e2e8f0;">' +
    LOCAL_FILENAME + '</strong> in the same folder as this HTML file to enable the player.</p>' +
    '</div>';
}}
</script>'''
    return html, 'local', ''


def _toc(sections: list) -> str:
    out = ''
    for i, s in enumerate(sections):
        slug  = _slug(s.get('title', f'section-{i}'))
        name  = _esc(s.get('title', ''))
        count = len(s.get('beats', []))
        out += f'<a href="#{slug}" class="toc-item"><span class="toc-name">{name}</span><span class="toc-count">{count} beats</span></a>\n'
    return out


def _sections(sections: list) -> str:
    # Flatten all beats across all sections to compute play_until (next VO start)
    all_beats = []
    for sec in sections:
        for beat in sec.get('beats', []):
            all_beats.append(beat)

    out = ''
    beat_global_idx = 0

    for i, sec in enumerate(sections):
        slug   = _slug(sec.get('title', f'section-{i}'))
        title  = _esc(sec.get('title', ''))
        beats  = sec.get('beats', [])
        beats_html = ''

        for j, beat in enumerate(beats):
            # play_until = start of next beat (VO first, visual as fallback, 0 = play to end)
            next_global = beat_global_idx + 1
            if next_global < len(all_beats):
                nb       = all_beats[next_global]
                nb_vo_ts = nb.get('vo', {}).get('timestamp_start', '')
                nb_vis   = nb.get('visual') or nb.get('visual_after') or {}
                nb_vis_ts = nb_vis.get('timestamp_start', '')
                play_until = _ts_to_sec(nb_vo_ts) or _ts_to_sec(nb_vis_ts)
            else:
                play_until = 0

            beats_html += _beat(beat, beat_global_idx + 1, play_until)
            beat_global_idx += 1

        out += f'''
<section class="section" id="{slug}">
  <div class="section-header">
    <span class="section-number">Section {i+1}</span>
    <h2 class="section-title">{title}</h2>
    <span class="section-meta">{len(beats)} beat{"s" if len(beats)!=1 else ""}</span>
  </div>
  <div class="beats">{beats_html}</div>
</section>'''

    return out


def _beat(beat: dict, n: int, play_until: int) -> str:
    vo  = beat.get('vo', {})
    viz = beat.get('visual') or beat.get('visual_after') or {}

    vo_ts_s  = vo.get('timestamp_start', '')
    vo_ts_e  = vo.get('timestamp_end', '')
    vis_ts_s = viz.get('timestamp_start', '')
    vis_ts_e = viz.get('timestamp_end', '')

    vo_start_s  = _ts_to_sec(vo_ts_s)
    vo_end_s    = _ts_to_sec(vo_ts_e)
    vis_start_s = _ts_to_sec(vis_ts_s)
    vis_end_s   = _ts_to_sec(vis_ts_e)

    # Use string check (not numeric) so "00:00:00" → 0 is handled correctly
    seek_start    = vo_start_s  if vo_ts_s  else vis_start_s
    highlight_end = vis_end_s   if vis_ts_e else vo_end_s

    vo_tone  = _esc(vo.get('tone', ''))
    vo_text  = _esc(vo.get('text', ''))
    
    tone_tag = f'<span class="tone-tag">{vo_tone}</span>' if vo_tone else ''

    v_desc  = _esc(viz.get('description', ''))
    v_ost   = _esc(viz.get('on_screen_text', ''))
    v_audio = _esc(viz.get('audio_notes', ''))
    v_summ  = _esc(viz.get('summary', ''))

    ost_block   = f'<div class="viz-meta"><span class="viz-label">On-screen text</span><span class="viz-val">{v_ost}</span></div>' if v_ost and v_ost.upper() not in ('NONE', 'N/A') else ''
    audio_block = f'<div class="viz-meta"><span class="viz-label">Audio</span><span class="viz-val">{v_audio}</span></div>' if v_audio else ''
    summ_block  = f'<div class="viz-summary"><span>{v_summ}</span></div>' if v_summ else ''

    dialogue = viz.get('dialogue') or []
    dialogue_block = ''
    if dialogue:
        rows = ''.join(
            f'<div class="dlg-row"><span class="dlg-speaker">{_esc(d.get("speaker",""))}:</span>'
            f'<span class="dlg-quote">&ldquo;{_esc(d.get("quote",""))}&rdquo;</span></div>'
            for d in dialogue if d.get('quote')
        )
        if rows:
            dialogue_block = f'<div class="dialogue-block"><div class="dlg-label">On-camera dialogue</div>{rows}</div>'

    # VO badge: only show if VO has timestamps
    if vo_ts_s:
        vo_label = _esc(f"{vo_ts_s} – {vo_ts_e}" if vo_ts_e else vo_ts_s)
        vo_badge = (f'<span class="ts-badge ts-seek" '
                    f'onclick="event.stopPropagation(); seekTo({seek_start},{play_until})" '
                    f'title="Seek to {vo_label}">&#9654; {vo_label}</span>')
    else:
        vo_badge = '<span class="ts-badge" style="opacity:0.3; cursor:default;">No VO</span>'

    # Visual badge: always clickable, seeks to visual start and stops at visual end
    if vis_ts_s:
        vis_label      = _esc(f"{vis_ts_s} – {vis_ts_e}" if vis_ts_e else vis_ts_s)
        vis_play_until = vis_end_s if vis_ts_e else play_until
        vis_badge = (f'<span class="ts-badge ts-seek vis-ts-seek" '
                     f'onclick="event.stopPropagation(); seekTo({vis_start_s},{vis_play_until})" '
                     f'title="Seek to visual: {vis_label}">&#9654; {vis_label}</span>')
    else:
        vis_badge = f'<span class="ts-badge">–</span>'

    return f'''
<div class="beat" id="beat-{n}" data-start="{seek_start}" data-end="{highlight_end}"
     onclick="seekTo({seek_start},{play_until})"
     style="cursor:pointer;" title="Click card or timestamps to seek">
  <div class="beat-number">#{n}</div>
  <div class="beat-body">

    <div class="vo-col">
      <div class="col-header vo-header">
        <span class="col-label">VO</span>
        {vo_badge}
      </div>
      <div class="col-content">
        {tone_tag}
        <p class="vo-text">{vo_text}</p>
      </div>
    </div>

    <div class="arrow-divider">&#9654;</div>

    <div class="vis-col">
      <div class="col-header vis-header">
        <span class="col-label">VISUAL</span>
        {vis_badge}
      </div>
      <div class="col-content">
        <p class="vis-desc">{v_desc}</p>
        {dialogue_block}
        {ost_block}
        {audio_block}
        {summ_block}
      </div>
    </div>

  </div>
</div>'''


def _summary(data: dict) -> str:
    summary = _esc(data.get('summary', ''))
    peaks   = data.get('peak_moments', [])
    highs   = data.get('highlights', [])

    peaks_html = ''
    for pm in peaks:
        ts_s = _ts_to_sec(pm.get('timestamp', ''))
        ts   = _esc(pm.get('timestamp', ''))
        desc = _esc(pm.get('description', ''))
        peaks_html += f'<div class="peak-item" onclick="seekTo({ts_s}, 0)" title="Jump to {ts}"><span class="peak-ts">&#9654; {ts}</span><span class="peak-desc">{desc}</span></div>\n'

    highs_html = ''.join(f'<li class="highlight-item">{_esc(h)}</li>' for h in highs)

    return f'''
<div class="summary-section" id="summary-anchor">
  <h2 class="summary-title">Summary &amp; Highlights</h2>
  <div class="summary-grid">
    <div class="sum-card">
      <div class="sum-card-header">Full Summary</div>
      <p class="sum-body">{summary}</p>
    </div>
    <div class="sum-card">
      <div class="sum-card-header">Peak Moments <small>(click to jump)</small></div>
      <div class="peaks-list">{peaks_html or "<p class='empty'>None identified.</p>"}</div>
    </div>
    <div class="sum-card">
      <div class="sum-card-header">Key Highlights</div>
      <ul class="highlights-list">{highs_html or "<li class='empty'>None identified.</li>"}</ul>
    </div>
  </div>
</div>'''


# ── helpers ────────────────────────────────────────────────────────────────────

def _extract_yt_id(url: str) -> str:
    m = re.search(r'(?:v=|youtu\.be/|embed/|shorts/)([a-zA-Z0-9_-]{11})', url)
    return m.group(1) if m else ''

def _local_url(path: str) -> str:
    path = path.replace('\\', '/')
    if not path.startswith('/'):
        path = '/' + path
    return 'file://' + urllib.parse.quote(path, safe='/:')

def _ts_to_sec(ts: str) -> int:
    parts = str(ts).strip().split(':')
    try:
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(float(parts[2]))
        if len(parts) == 2:
            return int(parts[0]) * 60 + int(float(parts[1]))
    except (ValueError, IndexError):
        pass
    return 0

def _esc(s: str) -> str:
    return html_lib.escape(str(s)) if s else ''

def _slug(s: str) -> str:
    return re.sub(r'[^\w]+', '-', s.lower()).strip('-')

def _safe(name: str) -> str:
    return re.sub(r'[^\w\s\-]', '', name).strip().replace(' ', '_')[:60]


# ── HTML template ──────────────────────────────────────────────────────────────

_TEMPLATE = '''\
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>{doc_title}</title>
  <style>
    *,*::before,*::after{{box-sizing:border-box;margin:0;padding:0}}
    :root{{
      --bg:#f0f2f5;--card:#fff;--border:#e2e8f0;--text:#1a202c;--muted:#718096;
      --accent:#4f46e5;
      --vo-bg:#fffbeb;--vo-border:#f59e0b;--vo-label:#b45309;
      --vis-bg:#f0fdf4;--vis-border:#22c55e;--vis-label:#15803d;
      --sec-bg:#1e293b;--radius:10px;
      --sh:0 1px 3px rgba(0,0,0,.08),0 4px 12px rgba(0,0,0,.06);
      --sh-lg:0 4px 6px rgba(0,0,0,.07),0 10px 30px rgba(0,0,0,.10);
    }}
    html{{scroll-behavior:smooth}}
    body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Arial,sans-serif;
      background:var(--bg);color:var(--text);line-height:1.6;font-size:14px}}

    /* Header */
    .page-header{{background:linear-gradient(135deg,#0f172a 0%,#1e3a5f 100%);
      color:#f8fafc;padding:36px 40px 28px}}
    .header-inner{{max-width:1500px;margin:0 auto}}
    .header-badge{{display:inline-block;background:rgba(79,70,229,.4);
      border:1px solid rgba(79,70,229,.6);color:#a5b4fc;
      font-size:11px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;
      padding:3px 12px;border-radius:20px;margin-bottom:10px}}
    .header-title{{font-size:clamp(18px,2.5vw,30px);font-weight:700;
      line-height:1.25;margin-bottom:12px;max-width:900px}}
    .header-meta{{display:flex;flex-wrap:wrap;gap:10px}}
    .meta-pill{{display:flex;align-items:center;gap:5px;
      background:rgba(255,255,255,.1);border:1px solid rgba(255,255,255,.15);
      border-radius:20px;padding:3px 12px;font-size:12px;color:#cbd5e1}}
    .meta-pill strong{{color:#f1f5f9}}

    /* TOC */
    .toc-wrapper{{background:var(--card);border-bottom:1px solid var(--border);
      position:sticky;top:0;z-index:200;box-shadow:0 2px 8px rgba(0,0,0,.06)}}
    .toc-inner{{max-width:1500px;margin:0 auto;padding:0 40px;
      display:flex;align-items:center;overflow-x:auto;scrollbar-width:none;gap:4px}}
    .toc-inner::-webkit-scrollbar{{display:none}}
    .toc-label{{font-size:10px;font-weight:700;letter-spacing:.08em;
      text-transform:uppercase;color:var(--muted);white-space:nowrap;
      padding:10px 12px 10px 0;border-right:2px solid var(--border);margin-right:8px}}
    .toc-item{{display:flex;flex-direction:column;align-items:center;
      padding:6px 12px;border-radius:6px;text-decoration:none;
      color:var(--text);white-space:nowrap;font-size:11px;
      transition:background .15s,color .15s}}
    .toc-item:hover{{background:#f1f5f9;color:var(--accent)}}
    .toc-name{{font-weight:600}}
    .toc-count{{font-size:9px;color:var(--muted)}}

    /* App layout */
    .app-layout{{max-width:1500px;margin:0 auto;padding:24px 40px 60px;
      display:grid;grid-template-columns:1fr 360px;gap:24px;align-items:start}}
    .app-layout.no-player{{grid-template-columns:1fr}}

    /* Player */
    .player-pane{{position:sticky;top:50px}}
    .player-card{{background:var(--card);border:1px solid var(--border);
      border-radius:var(--radius);box-shadow:var(--sh-lg);overflow:hidden}}
    .player-card-header{{background:#0f172a;color:#f1f5f9;padding:9px 14px;
      display:flex;align-items:center;justify-content:space-between;font-size:12px;font-weight:600}}
    .player-hint{{font-size:10px;color:#94a3b8}}
    .player-wrap{{position:relative;padding-bottom:56.25%;height:0;background:#000}}
    .player-wrap iframe,.player-wrap video{{position:absolute;top:0;left:0;width:100%;height:100%}}
    .player-footer{{padding:9px 14px;background:#f8fafc;border-top:1px solid var(--border);
      display:flex;align-items:center;justify-content:space-between;font-size:11px}}
    .current-time-label,.active-beat-label{{color:var(--muted)}}
    #current-time-display{{font-weight:700;font-variant-numeric:tabular-nums;
      color:var(--accent);font-size:13px}}
    #active-beat-display{{font-weight:600;color:#0f172a}}

    /* Section */
    .section{{margin-bottom:36px}}
    .section-header{{display:flex;align-items:center;gap:12px;
      background:var(--sec-bg);color:#f1f5f9;padding:12px 18px;
      border-radius:var(--radius) var(--radius) 0 0}}
    .section-number{{font-size:10px;font-weight:700;letter-spacing:.08em;
      text-transform:uppercase;color:#94a3b8}}
    .section-title{{font-size:14px;font-weight:700;flex:1}}
    .section-meta{{font-size:10px;color:#64748b;background:rgba(255,255,255,.08);
      padding:2px 10px;border-radius:12px}}

    /* Beat */
    .beat{{display:flex;align-items:stretch;
      border:1px solid var(--border);border-top:none;background:var(--card);
      transition:box-shadow .2s}}
    .beat:last-child{{border-radius:0 0 var(--radius) var(--radius);overflow:hidden}}
    .beat:hover{{box-shadow:var(--sh-lg);position:relative;z-index:1;border-left:3px solid var(--accent)}}
    .beat--active{{border-left:4px solid var(--accent)!important}}
    .beat--active .beat-number{{color:var(--accent);background:rgba(79,70,229,.1)}}
    .beat-number{{writing-mode:vertical-rl;text-orientation:mixed;transform:rotate(180deg);
      font-size:9px;font-weight:700;letter-spacing:.1em;color:var(--muted);
      background:#f8fafc;border-right:1px solid var(--border);
      padding:10px 5px;min-width:22px;text-align:center;user-select:none;
      transition:color .15s,background .15s}}
    .beat-body{{flex:1;display:grid;grid-template-columns:1fr 22px 1fr}}

    /* Columns */
    .vo-col{{background:var(--vo-bg)}}
    .vis-col{{background:var(--vis-bg)}}
    .col-header{{display:flex;align-items:center;justify-content:space-between;
      padding:8px 12px;border-bottom:2px solid;gap:8px}}
    .vo-header{{border-color:var(--vo-border);background:rgba(245,158,11,.08)}}
    .vis-header{{border-color:var(--vis-border);background:rgba(34,197,94,.08)}}
    .col-label{{font-size:9px;font-weight:800;letter-spacing:.1em;text-transform:uppercase}}
    .vo-col .col-label{{color:var(--vo-label)}}
    .vis-col .col-label{{color:var(--vis-label)}}
    .ts-badge{{font-size:10px;font-weight:600;font-variant-numeric:tabular-nums;
      padding:3px 9px;border-radius:20px;white-space:nowrap}}
    .vo-col .ts-badge{{background:rgba(245,158,11,.15);color:var(--vo-label)}}
    .vis-col .ts-badge{{background:rgba(34,197,94,.15);color:var(--vis-label)}}
    .ts-seek{{cursor:pointer;transition:background .15s,transform .1s}}
    .ts-seek:hover{{background:rgba(79,70,229,.2)!important;color:var(--accent)!important;transform:scale(1.04)}}
    .vis-ts-seek:hover{{background:rgba(34,197,94,.25)!important;color:var(--vis-label)!important;transform:scale(1.04)}}
    .col-content{{padding:12px 14px}}
    .tone-tag{{display:inline-block;font-size:10px;font-style:italic;color:#7c3aed;
      background:rgba(124,58,237,.07);border:1px solid rgba(124,58,237,.2);
      padding:2px 10px;border-radius:20px;margin-bottom:8px}}
    .vo-text{{font-size:13px;line-height:1.75;color:#1a202c}}
    .vis-desc{{font-size:13px;line-height:1.75;color:#14532d;margin-bottom:8px}}
    .viz-meta{{display:flex;gap:8px;align-items:baseline;font-size:11px;
      padding:4px 0;border-top:1px solid rgba(34,197,94,.15)}}
    .viz-label{{font-weight:600;color:var(--vis-label);white-space:nowrap;min-width:100px}}
    .viz-val{{color:#374151}}
    .viz-summary{{display:flex;gap:8px;align-items:flex-start;margin-top:8px;
      padding:7px 10px;background:rgba(34,197,94,.1);
      border-left:3px solid var(--vis-border);border-radius:0 6px 6px 0;
      font-size:11px;font-style:italic;color:#166534;line-height:1.5}}
    .dialogue-block{{margin:8px 0;padding:8px 10px;background:#fef3c7;
      border-left:3px solid #f59e0b;border-radius:0 6px 6px 0}}
    .dlg-label{{font-size:10px;font-weight:700;letter-spacing:.08em;
      text-transform:uppercase;color:#92400e;margin-bottom:5px}}
    .dlg-row{{font-size:12px;line-height:1.6;margin-bottom:3px;color:#78350f}}
    .dlg-speaker{{font-weight:700;margin-right:6px;color:#b45309}}
    .dlg-quote{{font-style:italic}}
    .arrow-divider{{display:flex;align-items:center;justify-content:center;
      background:#f1f5f9;border-left:1px solid var(--border);
      border-right:1px solid var(--border);color:#94a3b8;font-size:11px;user-select:none}}

    /* Summary */
    .summary-section{{margin-top:44px;padding-top:28px;border-top:2px solid var(--border)}}
    .summary-title{{font-size:20px;font-weight:700;margin-bottom:18px}}
    .summary-grid{{display:grid;gap:14px}}
    @media(min-width:900px){{
      .summary-grid{{grid-template-columns:1.6fr 1fr}}
      .sum-card:first-child{{grid-column:1/-1}}
    }}
    .sum-card{{background:var(--card);border:1px solid var(--border);
      border-radius:var(--radius);overflow:hidden;box-shadow:var(--sh)}}
    .sum-card-header{{background:#f8fafc;border-bottom:1px solid var(--border);
      padding:10px 16px;font-size:13px;font-weight:700}}
    .sum-card-header small{{font-weight:400;color:var(--muted);font-size:11px}}
    .sum-body{{padding:14px 16px;font-size:13px;line-height:1.8;color:#374151}}
    .peaks-list{{padding:10px 14px;display:flex;flex-direction:column;gap:7px}}
    .peak-item{{display:flex;gap:10px;align-items:flex-start;padding:8px 10px;
      background:#fffbeb;border:1px solid #fbbf24;border-radius:8px;
      font-size:12px;cursor:pointer;transition:background .15s}}
    .peak-item:hover{{background:#fef3c7}}
    .peak-ts{{font-weight:700;color:#92400e;white-space:nowrap;
      background:rgba(251,191,36,.25);padding:2px 8px;border-radius:12px}}
    .peak-desc{{color:#78350f;line-height:1.5}}
    .highlights-list{{list-style:none;padding:10px 14px;display:flex;flex-direction:column;gap:7px}}
    .highlight-item{{display:flex;gap:8px;align-items:flex-start;font-size:12px;
      color:#1e40af;padding:7px 10px;background:#eff6ff;
      border:1px solid #93c5fd;border-radius:8px;line-height:1.5}}
    .highlight-item::before{{content:"\\2726";flex-shrink:0;color:#3b82f6}}
    .empty{{color:var(--muted);font-style:italic;font-size:12px;padding:8px}}

    /* Footer */
    .page-footer{{text-align:center;padding:24px 40px;
      border-top:1px solid var(--border);font-size:11px;color:var(--muted)}}

    @media(max-width:1100px){{
      .app-layout{{grid-template-columns:1fr}}
      .player-pane{{position:static}}
    }}
    @media(max-width:640px){{
      .page-header,.app-layout{{padding-left:16px;padding-right:16px}}
      .toc-inner{{padding:0 16px}}
      .beat-body{{grid-template-columns:1fr}}
      .arrow-divider{{display:none}}
      .vis-col{{border-top:2px dashed var(--border)}}
    }}
    @media print{{
      .toc-wrapper,.player-pane{{display:none}}
      .app-layout{{grid-template-columns:1fr}}
    }}
  </style>
</head>
<body>

<header class="page-header">
  <div class="header-inner">
    <div class="header-badge">Video Script Analysis</div>
    <h1 class="header-title">{doc_title}</h1>
    <div class="header-meta">
      <div class="meta-pill">&#9201; Duration: <strong>{duration}</strong></div>
      <div class="meta-pill">&#128194; <strong>{section_count}</strong> sections</div>
      <div class="meta-pill">&#127916; <strong>{beat_count}</strong> VO / Visual beats</div>
    </div>
  </div>
</header>

<nav class="toc-wrapper">
  <div class="toc-inner">
    <span class="toc-label">Jump to</span>
    {toc}
    <a href="#summary-anchor" class="toc-item">
      <span class="toc-name">Summary</span>
      <span class="toc-count">highlights</span>
    </a>
  </div>
</nav>

<div class="{layout_cls}">
  <div class="script-pane">
    {body}
    {summary}
  </div>
{player_section}
</div>

<footer class="page-footer">Generated on {generated} &nbsp;&middot;&nbsp; VO and Visual Extractor</footer>

{yt_api_tag}
<script>
var PLAYER_TYPE    = '{player_type}';
var YT_ID          = '{yt_id}';
var ytPlayer       = null;
var ytEmbedBlocked = false;
var stopAt         = 0;
var pollTimer      = null;

function onYouTubeIframeAPIReady() {{
  ytPlayer = new YT.Player('yt-iframe', {{
    events: {{
      onReady: function() {{ startPolling(); }},
      onError: function(e) {{
        // 100=not found, 101/150/153=embedding disabled, 2/5=bad param
        ytEmbedBlocked = true;
        showYtFallback();
      }}
    }}
  }});
}}

function showYtFallback() {{
  var wrap = document.querySelector('.player-wrap');
  if (!wrap) return;
  wrap.style.paddingBottom = '0';
  wrap.style.height = 'auto';
  wrap.innerHTML =
    '<div style="padding:20px 16px;text-align:center;background:#0f172a;color:#f1f5f9;">' +
    '<div style="font-size:28px;margin-bottom:8px;">&#128679;</div>' +
    '<p style="font-size:13px;color:#94a3b8;margin-bottom:14px;">This video cannot be embedded.<br>Click any VO timestamp to open it on YouTube at that exact moment.</p>' +
    '<a href="https://www.youtube.com/watch?v=' + YT_ID + '" target="_blank" ' +
    'style="display:inline-block;background:#ef4444;color:#fff;font-weight:700;font-size:13px;' +
    'padding:9px 22px;border-radius:6px;text-decoration:none;">&#9654; Watch on YouTube</a>' +
    '</div>';
  var hint = document.querySelector('.player-hint');
  if (hint) hint.textContent = 'Click any VO timestamp to open on YouTube';
}}

function startPolling() {{
  if (pollTimer) clearInterval(pollTimer);
  pollTimer = setInterval(function() {{
    if (!ytPlayer || !ytPlayer.getCurrentTime) return;
    var t = ytPlayer.getCurrentTime();
    updateUI(t);
    if (stopAt > 0 && t >= stopAt) {{
      ytPlayer.pauseVideo();
      stopAt = 0;
    }}
  }}, 250);
}}

document.addEventListener('DOMContentLoaded', function() {{
  if (PLAYER_TYPE === 'local') {{
    var v = document.getElementById('local-video');
    if (!v) return;
    v.addEventListener('timeupdate', function() {{
      updateUI(v.currentTime);
      if (stopAt > 0 && v.currentTime >= stopAt) {{
        v.pause();
        stopAt = 0;
      }}
    }});
  }}
}});

function seekTo(seconds, until) {{
  var t = Math.max(0, seconds);
  if (PLAYER_TYPE === 'youtube') {{
    if (ytEmbedBlocked || !ytPlayer || !ytPlayer.seekTo) {{
      window.open('https://www.youtube.com/watch?v=' + YT_ID + '&t=' + t + 's', '_blank');
      return;
    }}
    stopAt = until || 0;
    ytPlayer.seekTo(t, true);
    ytPlayer.playVideo();
    if (window.innerWidth < 1100) {{
      var ps = document.getElementById('player-section');
      if (ps) ps.scrollIntoView({{behavior:'smooth',block:'start'}});
    }}
    return;
  }} else if (PLAYER_TYPE === 'local') {{
    stopAt = until || 0;
    var v = document.getElementById('local-video');
    if (v) {{ v.currentTime = t; v.play(); }}
  }}
  if (window.innerWidth < 1100) {{
    var ps = document.getElementById('player-section');
    if (ps) ps.scrollIntoView({{behavior:'smooth',block:'start'}});
  }}
}}

function updateUI(t) {{
  var td = document.getElementById('current-time-display');
  if (td) td.textContent = fmt(t);

  var beats = document.querySelectorAll('.beat');
  var active = null;
  beats.forEach(function(b) {{
    var s = parseFloat(b.dataset.start || 0);
    var e = parseFloat(b.dataset.end   || 0);
    var on = e > s && t >= s && t <= e;
    b.classList.toggle('beat--active', on);
    if (on) active = b.id.replace('beat-', '');
  }});

  var abd = document.getElementById('active-beat-display');
  if (abd) abd.textContent = active ? '#' + active : '-';
}}

function fmt(s) {{
  var h = Math.floor(s/3600), m = Math.floor((s%3600)/60), sec = Math.floor(s%60);
  return (h > 0 ? pad(h)+':' : '') + pad(m) + ':' + pad(sec);
}}
function pad(n) {{ return String(n).padStart(2,'0'); }}
</script>
</body>
</html>'''
