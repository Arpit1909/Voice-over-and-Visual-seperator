import copy
import os
import json
import re
import shutil
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from google import genai
from google.genai import types
from google.cloud import storage


# Try these models in order until one works
MODELS = [
    "gemini-2.5-pro",                  # stable channel — try first
    "gemini-2.5-pro-preview-05-06",    # preview version fallback
    "gemini-2.5-pro-001",              # explicit version fallback
    "gemini-1.5-pro-002",              # last resort (widely available on Vertex AI)
]

_ANALYSIS_RULES = """
You are analyzing a documentary video. Produce a precise beat-by-beat script breakdown.
Your #1 goal is to correctly distinguish FOUR audio sources and never confuse them.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
THE FOUR AUDIO SOURCES — MEMORIZE THESE
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

(A) NARRATOR — off-camera studio voice
   - Professional narrator speaking in 3rd person ABOUT the case.
   - Studio-clean audio: zero background noise, zero room echo.
   - Narrator is NEVER on screen. Uses "he/she/they/the officer", NOT "I/me/my".
   - Example: "Investigators arrive at the scene and begin processing evidence."
   → beat_type: "narration"
   → vo.text = verbatim narrator words. visual.dialogue = []

(B) ON-CAMERA SPEECH — field audio from inside the footage
   - Anyone speaking while visible on screen OR speaking into bodycam/911/phone.
   - Has background noise, wind, radio static, room echo, ambient sound.
   - Officer, suspect, witness, 911 caller, interviewee — all go here.
   - Example: Officer: "Sir, stop right there."  Alan: "Okay, officer."
   → beat_type: "visual_only"
   → vo.text = "" (MUST be empty)
   → visual.dialogue = [{"speaker":"Officer","quote":"Sir, stop right there."}, ...]

(C) SILENT FOOTAGE / MUSIC / AMBIENT
   - No speech at all. Just music, ambient sound, title cards, or silence.
   → beat_type: "visual_only"
   → vo.text = "" . visual.dialogue = []

(D) AD READ / SPONSOR — interruption from the narrator
   - Suddenly switches to FIRST PERSON ("I felt...", "I tried...", "my mornings")
   - Promotes a product, app, service (therapy, sleep, VPN, wellness, etc.)
   - Breaks narrative flow. Often starts with "You ever feel..." or "I hit a point where..."
   → beat_type: "ad_read"
   → vo.text = verbatim sponsor text. visual.dialogue = []

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
THE ONLY TEST YOU NEED
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
For every moment in the video, ask:
  "Whose voice is playing, and where is that voice physically coming from?"

 - Clean studio voice, 3rd person about the case → (A) narration
 - Voice from someone visible on-screen or bodycam → (B) visual_only with dialogue
 - No voice at all → (C) visual_only, silent
 - Clean studio voice, 1st person promoting something → (D) ad_read

CRITICAL DIFFERENCES:
 - Narrator says "Alan walked toward the truck." = (A)
 - Alan says "I walked toward the truck." on camera = (B) — put in visual.dialogue
 - Narrator says "I struggled with anxiety for years..." = (D) — this is a sponsor, not narration

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SPLIT BEATS AT EVERY AUDIO SOURCE CHANGE — NO EXCEPTIONS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
A new beat starts the INSTANT the audio source changes, even if only for 2 seconds.

Common pattern — narrator speaks, then footage plays with on-camera dialogue, then narrator resumes:
  → Beat 1: narration (narrator's words)
  → Beat 2: visual_only (on-camera dialogue in visual.dialogue)
  → Beat 3: narration (narrator resumes)
NEVER merge these into one beat. NEVER.

If narrator pauses and footage plays with silence/music — that silent gap is its own visual_only beat.
If captions appear on-screen over footage, that is on-camera speech → visual_only with dialogue.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
COMPLETE SENTENCES ONLY — NO FRAGMENTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
 - vo.text MUST start with a capital letter and end with . ? or !
 - NEVER start vo.text with "..." or a lowercase word.
 - If a narrator sentence spans a chunk boundary, include the entire sentence in the chunk where it starts — even if it bleeds slightly past.
 - Each narration beat must contain one or more COMPLETE sentences.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DIALOGUE CAPTURE — BE THOROUGH
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
For every visual_only beat with on-camera speech, fill visual.dialogue with EVERY speaker exchange:
  "dialogue": [
    {"speaker": "Deputy Smith", "quote": "What are you doing out here tonight?"},
    {"speaker": "Alan", "quote": "Just checking for thieves, officer."}
  ]
If the speaker's name isn't known, use a role like "Officer", "Man", "Witness", "911 Caller".
If the beat is silent/music-only, visual.dialogue = [].

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TIMESTAMPS — STRICT HH:MM:SS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ALL timestamps strict HH:MM:SS format. Hours is "00" for clips under 60 minutes.
  ✓ "00:05:32"   ✗ "05:32:00"
vo.timestamp_start/end = EXACT time the narrator's voice starts/stops.
visual.timestamp_start/end = EXACT time the footage shot starts/ends.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BEAT TYPES — SUMMARY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"narration"   → off-camera studio narrator about the case. vo.text filled. dialogue empty.
"visual_only" → footage playing — on-camera speech, music, or silence. vo.text empty. dialogue filled if anyone speaks on-camera.
"ad_read"     → narrator's clean voice pitching a sponsor/product/app. vo.text filled with ad copy. dialogue empty.
"""

_ANALYSIS_JSON_FORMAT = """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Return ONLY valid JSON — no markdown, no explanation:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

{
  "title": "descriptive title of the video",
  "total_duration": "HH:MM:SS",
  "sections": [
    {
      "title": "Section Name",
      "beats": [
        {
          "beat_type": "narration",
          "vo": {
            "timestamp_start": "00:00:06",
            "timestamp_end": "00:00:12",
            "tone": "Ominous",
            "text": "This Idaho state officer is pointing his flashlight at 70-year-old Alan Bruce."
          },
          "visual": {
            "timestamp_start": "00:00:00",
            "timestamp_end": "00:00:25",
            "description": "Officer shines flashlight at a man on a dark road.",
            "on_screen_text": "ALAN BRUCE, 70",
            "audio_notes": "Tense music underscore.",
            "dialogue": [],
            "summary": "Officer confronts Alan Bruce at night."
          }
        },
        {
          "beat_type": "visual_only",
          "vo": {"timestamp_start": "", "timestamp_end": "", "tone": "", "text": ""},
          "visual": {
            "timestamp_start": "00:00:25",
            "timestamp_end": "00:00:38",
            "description": "Officer questions Alan on a dark road. Alan says he was checking for thieves.",
            "on_screen_text": "NONE",
            "audio_notes": "Bodycam field audio.",
            "dialogue": [
              {"speaker": "Officer", "quote": "Okay, so what are you doing out here tonight?"},
              {"speaker": "Alan",    "quote": "I was just checking for thieves."}
            ],
            "summary": "Officer questions Alan."
          }
        },
        {
          "beat_type": "ad_read",
          "vo": {
            "timestamp_start": "00:10:32",
            "timestamp_end": "00:11:05",
            "tone": "Personal",
            "text": "I hit a point where trying to move on wasn't working. That's when I tried BetterHelp..."
          },
          "visual": {
            "timestamp_start": "00:10:32",
            "timestamp_end": "00:11:05",
            "description": "Sponsor visuals: BetterHelp logo and lifestyle clips.",
            "on_screen_text": "BETTERHELP",
            "audio_notes": "Ad read by the narrator.",
            "dialogue": [],
            "summary": "BetterHelp sponsor segment."
          }
        }
      ]
    }
  ],
  "summary": "A thorough summary of the whole case.",
  "peak_moments": [{"timestamp": "00:00:00", "description": "..."}],
  "highlights": ["..."]
}
"""

# Full prompt for non-chunked videos
ANALYSIS_PROMPT = _ANALYSIS_RULES + _ANALYSIS_JSON_FORMAT


def _get_video_duration(video_path: str) -> float:
    """Return video duration in seconds via ffprobe, or 0 on failure."""
    try:
        r = subprocess.run(
            ['ffprobe', '-v', 'error', '-show_entries', 'format=duration',
             '-of', 'default=noprint_wrappers=1:nokey=1', video_path],
            capture_output=True, text=True, timeout=30
        )
        return float(r.stdout.strip())
    except Exception:
        return 0.0


def analyze_video(video_path: str, project_id: str, bucket_name: str,
                  location: str = "us-central1",
                  upload_progress_cb=None) -> dict:
    """Full pipeline. Auto-uses chunked analysis for videos longer than CHUNK_THRESHOLD."""
    duration_secs = _get_video_duration(video_path)
    if duration_secs > CHUNK_THRESHOLD:
        return analyze_video_chunked(video_path, project_id, bucket_name, location, duration_secs)
    gcs_uri = upload_video(video_path, bucket_name, upload_progress_cb)
    try:
        return run_gemini(gcs_uri, project_id, location, duration_secs)
    finally:
        delete_from_gcs(gcs_uri, bucket_name)


def upload_video(video_path: str, bucket_name: str, progress_cb=None) -> str:
    """Upload video to GCS. Returns gs:// URI."""
    client   = storage.Client()
    bucket   = client.bucket(bucket_name)
    filename = os.path.basename(video_path)
    blob     = bucket.blob(f"video-analyzer-temp/{filename}")
    # Large chunks = fewer HTTP round-trips = much faster upload
    blob.chunk_size = 256 * 1024 * 1024  # 256 MB per chunk

    file_size = os.path.getsize(video_path)
    wrapper   = _ProgressFile(video_path, file_size, progress_cb)
    blob.upload_from_file(wrapper, size=file_size, content_type="video/mp4", timeout=3600)
    return f"gs://{bucket_name}/video-analyzer-temp/{filename}"


CHUNK_MINUTES      = 8     # minutes per clip segment (smaller = less token pressure, more detail)
CHUNK_THRESHOLD    = 600   # seconds — videos longer than this get clipped & chunked
CHUNK_WORKERS      = 1     # sequential to avoid 404/quota collisions on parallel calls
CHUNK_MAX_TOKENS   = 65536 # output tokens per chunk — upper bound
GAP_THRESHOLD_SECS = 90    # re-analyze any coverage gap larger than this
MIN_BEATS_PER_MIN  = 2.5   # if under-density detected, split clip and retry
MAX_SPLIT_DEPTH    = 2     # max recursive halvings per clip (e.g. 8min → 4min → 2min)


def run_gemini(gcs_uri: str, project_id: str, location: str = "us-central1",
               duration_secs: float = 0) -> dict:
    """Send video to Gemini. Automatically chunks videos longer than 15 minutes."""
    if duration_secs > CHUNK_THRESHOLD:
        return _run_chunked(gcs_uri, project_id, location, duration_secs)
    data = _run_analysis_with_prompt(gcs_uri, project_id, location, ANALYSIS_PROMPT)
    data = _weave_tracks_into_beats(data)
    if duration_secs > 0:
        data = _fix_timestamps(data, duration_secs)
    data['sections'] = _sanitize_vo_beats(data.get('sections', []))
    return data


def _secs_to_ts(s: float) -> str:
    s = int(s)
    return f"{s // 3600:02d}:{(s % 3600) // 60:02d}:{s % 60:02d}"


def _build_clip_header(clip_secs: float) -> str:
    """Stricter density expectation: 3 beats/min, 10-25s per beat, hard max 30s."""
    expected_min = max(6, int((clip_secs / 60) * 3))
    expected_max = max(12, int((clip_secs / 60) * 8))
    return (
        f"CLIP SCOPE: This is a {clip_secs/60:.2f}-minute excerpt (internally "
        f"00:00:00 → {_secs_to_ts(int(clip_secs))}). "
        f"ALL timestamps MUST be within 00:00:00 and {_secs_to_ts(int(clip_secs))}. "
        f"You MUST cover the ENTIRE clip start→finish — do NOT skip any portion.\n\n"
        f"DENSITY REQUIREMENT (MANDATORY):\n"
        f"  - Produce between {expected_min} and {expected_max} beats for this clip.\n"
        f"  - A NEW BEAT begins every time the audio source changes (narrator vs on-camera)\n"
        f"    OR every time a different person starts speaking on camera\n"
        f"    OR every time the visual shot/location changes meaningfully.\n"
        f"  - Max beat duration: 25 seconds. HARD CAP: 30 seconds.\n"
        f"  - NEVER bundle multiple on-camera speaker-turns into one beat.\n"
        f"  - NEVER bundle narrator+on-camera audio into one beat — they are separate beats.\n"
        f"  - If in doubt whether to split a beat, SPLIT IT.\n\n"
    )


def _analyze_clip_adaptively(video_path: str, start_s: float, end_s: float,
                             project_id: str, bucket_name: str, location: str,
                             clip_tag: str, depth: int = 0) -> dict:
    """Clip → upload → analyze. On truncation or under-density, split the clip in half and recurse.
    Returns data with timestamps already offset to the global (original-video) timeline.
    """
    clip_secs = end_s - start_s
    clip_path = _clip_video(video_path, start_s, end_s, hash(clip_tag) & 0xFFFF)
    uri = None
    try:
        fname = f"clip_{clip_tag}_{os.path.basename(video_path)}"
        uri = _upload_clip_to_gcs(clip_path, bucket_name, fname)

        prompt = _build_clip_header(clip_secs) + ANALYSIS_PROMPT
        data = _run_analysis_with_prompt(uri, project_id, location,
                                         prompt, max_tokens=CHUNK_MAX_TOKENS)
        was_repaired = bool(data.pop('_was_repaired', False))
        data = _weave_tracks_into_beats(data)
        beats_count = sum(len(s.get('beats', [])) for s in data.get('sections', []))
        expected_min = max(6, int((clip_secs / 60) * MIN_BEATS_PER_MIN))

        under_dense = beats_count < expected_min
        can_split   = depth < MAX_SPLIT_DEPTH and clip_secs > 150

        if (was_repaired or under_dense) and can_split:
            reason = "JSON truncation" if was_repaired else f"under-density ({beats_count} beats < {expected_min} expected)"
            print(f"  ⚠ Clip {clip_tag} failed quality check ({reason}). "
                  f"Splitting in half and re-analyzing (depth {depth+1}/{MAX_SPLIT_DEPTH})...")
            # free up this clip before recursing
            if uri:
                try: _delete_from_gcs(uri, bucket_name)
                except Exception: pass
                uri = None
            try: shutil.rmtree(os.path.dirname(clip_path), ignore_errors=True)
            except Exception: pass
            mid = start_s + clip_secs / 2
            print(f"    Cooling down 30s before split sub-clips...")
            time.sleep(30)
            left  = _analyze_clip_adaptively(video_path, start_s, mid, project_id, bucket_name,
                                             location, f"{clip_tag}a", depth + 1)
            print(f"    Cooling down 30s before second half...")
            time.sleep(30)
            right = _analyze_clip_adaptively(video_path, mid, end_s, project_id, bucket_name,
                                             location, f"{clip_tag}b", depth + 1)
            return _combine_two_clip_results(left, right)

        data = _fix_timestamps(data, clip_secs)
        data['sections'] = _sanitize_vo_beats(data.get('sections', []))
        return _offset_timestamps(data, start_s)
    finally:
        if uri:
            try: _delete_from_gcs(uri, bucket_name)
            except Exception: pass
        try: shutil.rmtree(os.path.dirname(clip_path), ignore_errors=True)
        except Exception: pass


def _combine_two_clip_results(a: dict, b: dict) -> dict:
    """Concatenate sections/peaks/highlights from two already-offset clip results."""
    out = {
        'title': a.get('title') or b.get('title', ''),
        'sections': list(a.get('sections', [])) + list(b.get('sections', [])),
        'peak_moments': list(a.get('peak_moments', [])) + list(b.get('peak_moments', [])),
        'highlights': list(a.get('highlights', [])) + list(b.get('highlights', [])),
        'summary': '\n\n'.join(x for x in (a.get('summary',''), b.get('summary','')) if x),
    }
    return out


def analyze_video_chunked(video_path: str, project_id: str, bucket_name: str,
                          location: str, duration_secs: float) -> dict:
    """
    Long-video pipeline: clip → upload → analyze → adaptively split under-detailed clips → merge.
    Each Gemini call sees only its own short clip starting at 00:00, eliminating
    cross-chunk timestamp confusion from seeing the full video.
    """
    chunk_secs = CHUNK_MINUTES * 60
    chunks: list[tuple[float, float]] = []
    s = 0.0
    while s < duration_secs:
        e = min(s + chunk_secs, duration_secs)
        chunks.append((s, e))
        s = e

    n = len(chunks)
    print(f"  {duration_secs / 60:.1f} min video → {n} clips × {CHUNK_MINUTES} min  "
          f"[adaptive subdivision: up to {MAX_SPLIT_DEPTH} levels deep]")

    ordered: list[dict | None] = [None] * n

    def _process(i: int):
        cs, ce = chunks[i]
        print(f"\n  === Clip {i + 1}/{n}: {_secs_to_ts(cs)} → {_secs_to_ts(ce)} ===")
        return i, _analyze_clip_adaptively(
            video_path, cs, ce, project_id, bucket_name, location,
            clip_tag=f"{i:02d}", depth=0
        )

    if CHUNK_WORKERS == 1:
        for i in range(n):
            if i > 0:
                print(f"  Cooling down 30s before clip {i+1} to avoid rate limits...")
                time.sleep(30)
            idx, chunk_data = _process(i)
            ordered[idx] = chunk_data
    else:
        with ThreadPoolExecutor(max_workers=min(CHUNK_WORKERS, n)) as pool:
            futures = {pool.submit(_process, i): i for i in range(n)}
            for fut in as_completed(futures):
                idx, chunk_data = fut.result()
                ordered[idx] = chunk_data

    # ── Merge ordered results ─────────────────────────────────────────────────
    all_sections:   list = []
    all_peaks:      list = []
    all_highlights: list = []
    all_summaries:  list = []
    title = ""

    for chunk_data in ordered:
        if not title:
            title = chunk_data.get('title', '')
        all_sections.extend(chunk_data.get('sections', []))
        all_peaks.extend(chunk_data.get('peak_moments', []))
        all_highlights.extend(chunk_data.get('highlights', []))
        if chunk_data.get('summary'):
            all_summaries.append(chunk_data['summary'])

    merged = {
        'title': title,
        'total_duration': _secs_to_ts(duration_secs),
        'sections': _merge_sections(all_sections),
        'summary': '\n\n'.join(all_summaries),
        'peak_moments': all_peaks,
        'highlights': list(dict.fromkeys(all_highlights)),
    }

    # ── Step 4: Detect and fill coverage gaps (loop until none remain) ────────
    MAX_GAP_PASSES = 4
    for pass_num in range(MAX_GAP_PASSES):
        all_beats = [b for s in merged['sections'] for b in s.get('beats', [])]
        gaps = _find_gaps(all_beats, duration_secs)
        if not gaps:
            break

        print(f"\n  Found {len(gaps)} coverage gap(s) > {GAP_THRESHOLD_SECS}s "
              f"(pass {pass_num + 1}/{MAX_GAP_PASSES}). Re-analyzing...")
        gap_clips: list[str] = []
        gap_uris:  list[str] = []
        try:
            for gi, (gs, ge) in enumerate(gaps):
                print(f"    Gap {gi+1}/{len(gaps)}: {_secs_to_ts(int(gs))} → {_secs_to_ts(int(ge))} "
                      f"({(ge-gs)/60:.1f} min)")
                gp = _clip_video(video_path, gs, ge, 1000 + pass_num * 10 + gi)
                gap_clips.append(gp)
                fname = f"gap_p{pass_num}_{gi:02d}_{os.path.basename(video_path)}"
                gu = _upload_clip_to_gcs(gp, bucket_name, fname)
                gap_uris.append(gu)
                print(f"    Uploaded gap clip {gi+1}/{len(gaps)}")

            for gi, (gs, ge) in enumerate(gaps):
                gap_secs = ge - gs
                if gi > 0:
                    print(f"  Cooling down 30s before gap clip {gi+1}...")
                    time.sleep(30)
                prompt = _build_clip_header(gap_secs) + ANALYSIS_PROMPT
                gap_data = _run_analysis_with_prompt(gap_uris[gi], project_id, location,
                                                     prompt, max_tokens=CHUNK_MAX_TOKENS)
                gap_data.pop('_was_repaired', None)
                gap_data = _weave_tracks_into_beats(gap_data)
                gap_data = _fix_timestamps(gap_data, gap_secs)
                gap_data['sections'] = _sanitize_vo_beats(gap_data.get('sections', []))
                gap_data = _offset_timestamps(gap_data, gs)

                gap_beats = [b for s in gap_data.get('sections', []) for b in s.get('beats', [])]
                print(f"    Gap {gi+1}/{len(gaps)} filled with {len(gap_beats)} beats.")

                if merged['sections']:
                    merged['sections'][0]['beats'].extend(gap_beats)
                else:
                    merged['sections'] = [{'title': 'Analysis', 'beats': gap_beats}]

            # Re-sort and deduplicate after injection
            merged['sections'] = _merge_sections(merged['sections'])

        finally:
            for gu in gap_uris:
                if gu:
                    try: _delete_from_gcs(gu, bucket_name)
                    except Exception: pass
            for gp in gap_clips:
                if gp:
                    try: shutil.rmtree(os.path.dirname(gp), ignore_errors=True)
                    except Exception: pass

        if pass_num > 0:
            print(f"  Cooling down 45s before next gap-fill pass...")
            time.sleep(45)

    # ── Step 5: Final cleanup — merge fragments across chunk boundaries, detect ad reads ──
    merged['sections'] = _merge_fragment_continuations(merged['sections'])
    merged['sections'] = _detect_ad_reads(merged['sections'])

    return merged


def _beat_start_secs(beat: dict) -> float:
    """Return the earliest timestamp in a beat as seconds (for sorting)."""
    ts = (beat.get('vo', {}).get('timestamp_start', '')
          or beat.get('visual', {}).get('timestamp_start', '')
          or '00:00:00')
    return _ts_str_to_secs(ts)


def _beat_end_secs(beat: dict) -> float:
    """Return the latest end timestamp in a beat as seconds."""
    ts = (beat.get('visual', {}).get('timestamp_end', '')
          or beat.get('vo', {}).get('timestamp_end', '')
          or '00:00:00')
    return _ts_str_to_secs(ts)


def _ts_str_to_secs(ts: str) -> float:
    try:
        parts = ts.strip().split(':')
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + float(parts[2])
        if len(parts) == 2:
            return int(parts[0]) * 60 + float(parts[1])
    except Exception:
        pass
    return 0.0


def _merge_sections(sections: list) -> list:
    """Merge consecutive sections with the same title, then sort beats by timestamp."""
    merged = []
    for sec in sections:
        if merged and merged[-1].get('title', '').upper() == sec.get('title', '').upper():
            merged[-1]['beats'].extend(sec.get('beats', []))
        else:
            merged.append(dict(title=sec.get('title', ''), beats=list(sec.get('beats', []))))

    # Sort beats within each section by start timestamp
    for sec in merged:
        sec['beats'].sort(key=_beat_start_secs)

    # Deduplicate overlapping beats at chunk boundaries, preserving section structure
    prev_end = 0.0
    for sec in merged:
        clean = []
        for beat in sec['beats']:
            this_start = _beat_start_secs(beat)
            this_end   = _beat_end_secs(beat)
            if this_start < prev_end:
                # Overlap with previous beat (cross-section or within section)
                if this_end > prev_end:
                    clean.append(beat)   # longer — keep it
                    prev_end = this_end
                # else: shorter duplicate — drop it
            else:
                clean.append(beat)
                prev_end = max(prev_end, this_end)
        sec['beats'] = clean

    # Sort sections by earliest beat timestamp so chunks appear in chronological order
    merged.sort(key=lambda s: _beat_start_secs(s['beats'][0]) if s['beats'] else 0)

    return [s for s in merged if s['beats']]


def _find_gaps(beats: list, total_secs: float) -> list[tuple[float, float]]:
    """Return (start, end) pairs for coverage gaps > GAP_THRESHOLD_SECS."""
    gaps = []
    prev_end = 0.0
    for beat in sorted(beats, key=_beat_start_secs):
        start = _beat_start_secs(beat)
        if start - prev_end > GAP_THRESHOLD_SECS:
            gaps.append((prev_end, start))
        prev_end = max(prev_end, _beat_end_secs(beat))
    if total_secs - prev_end > GAP_THRESHOLD_SECS:
        gaps.append((prev_end, total_secs))
    return gaps


def _clip_video(src: str, start_s: float, end_s: float, idx: int) -> str:
    """Cut a segment with frame-accurate re-encoding to avoid keyframe timestamp drift.
    Downsamples to 480p 1fps to make encoding incredibly fast since Gemini samples at 1fps natively anyway."""
    out_dir  = tempfile.mkdtemp(prefix='va_clip_')
    out_path = os.path.join(out_dir, f'clip_{idx:02d}.mp4')
    duration = end_s - start_s
    try:
        subprocess.run([
            'ffmpeg',
            '-ss', str(start_s),
            '-i', src,
            '-t', str(duration),
            '-vf', 'scale=-2:480,fps=1',
            '-c:v', 'libx264',
            '-preset', 'ultrafast',
            '-crf', '28',
            '-c:a', 'aac',
            '-b:a', '128k',
            '-avoid_negative_ts', 'make_zero',
            '-y', out_path,
        ], check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.decode(errors='replace') if e.stderr else ''
        raise RuntimeError(f"ffmpeg failed clipping segment {idx}: {stderr}") from e
    return out_path


def _upload_clip_to_gcs(clip_path: str, bucket_name: str, filename: str) -> str:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob   = bucket.blob(f"video-analyzer-temp/{filename}")
    blob.chunk_size = 256 * 1024 * 1024
    blob.upload_from_filename(clip_path, content_type='video/mp4', timeout=3600)
    return f"gs://{bucket_name}/video-analyzer-temp/{filename}"

def _weave_tracks_into_beats(data: dict) -> dict:
    """Takes ai-generated vo_track and visual_track and merges them chronologically."""
    if isinstance(data, list):
        data = {'vo_track': [], 'visual_track': [], 'sections': []}
    if 'sections' in data and data.get('sections'):
        return data

    vo_track = data.get('vo_track', [])
    vis_track = data.get('visual_track', [])

    for v in vo_track:
        v['start_sec'] = _ts_str_to_secs(v.get('timestamp_start', ''))
        v['end_sec'] = _ts_str_to_secs(v.get('timestamp_end', ''))
    for vis in vis_track:
        vis['start_sec'] = _ts_str_to_secs(vis.get('timestamp_start', ''))
        vis['end_sec'] = _ts_str_to_secs(vis.get('timestamp_end', ''))
        vis['used'] = False

    vo_track.sort(key=lambda x: x['start_sec'])
    vis_track.sort(key=lambda x: x['start_sec'])

    beats = []

    for vo in vo_track:
        best_vis = None
        best_overlap = -999999
        vo_s = vo['start_sec']
        vo_e = vo['end_sec']

        for vis in vis_track:
            vis_s = vis['start_sec']
            vis_e = vis['end_sec']
            overlap = min(vo_e, vis_e) - max(vo_s, vis_s)
            score = overlap - (abs(vo_s - vis_s) * 0.1)

            if score > best_overlap:
                best_overlap = score
                best_vis = vis

        clean_vo = {k: v for k, v in vo.items() if k not in ['start_sec', 'end_sec']}

        if best_vis and best_overlap > -10:
            clean_vis = {k: v for k, v in best_vis.items() if k not in ['start_sec', 'end_sec', 'used']}
            best_vis['used'] = True
        else:
            clean_vis = {}

        beats.append({
            "beat_type": "narration",
            "sort_sec": vo_s,
            "vo": clean_vo,
            "visual": clean_vis
        })

    for vis in vis_track:
        if not vis.get('used', False):
            clean_vis = {k: v for k, v in vis.items() if k not in ['start_sec', 'end_sec', 'used']}
            beats.append({
                "beat_type": "visual_only",
                "sort_sec": vis['start_sec'],
                "vo": {"timestamp_start": "", "timestamp_end": "", "tone": "", "text": ""},
                "visual": clean_vis
            })

    beats.sort(key=lambda b: b['sort_sec'])
    for b in beats:
        b.pop('sort_sec', None)

    data['sections'] = [{"title": "Analysis", "beats": beats}]
    return data

def _fix_timestamps(data: dict, max_secs: float) -> dict:
    """
    Correct timestamps where Gemini used MM:SS:00 instead of HH:MM:SS.
    E.g. "01:57:00" (meant 1 min 57 sec) gets corrected to "00:01:57".
    Only touches timestamps that exceed max_secs AND have a non-zero hours field.
    """
    tolerance = max_secs * 1.05  # allow 5% over (rounding at end of segment)

    def _fix(ts: str) -> str:
        if not ts or ':' not in ts:
            return ts
        s = _ts_str_to_secs(ts)
        if s <= tolerance:
            return ts  # already valid
        parts = ts.strip().split(':')
        # Only reinterpret if hours field is non-zero (genuine MM:SS:00 confusion)
        if len(parts) == 3 and parts[0] not in ('00', '0', ''):
            try:
                reinterp = int(parts[0]) * 60 + int(parts[1])
                if reinterp <= tolerance:
                    return _secs_to_ts(int(reinterp))
            except Exception:
                pass
        # If hours=00 but value still exceeds clip, clamp to clip end
        return _secs_to_ts(int(max_secs))

    data = copy.deepcopy(data)
    for sec in data.get('sections', []):
        valid = []
        for beat in sec.get('beats', []):
            for field in ('vo', 'visual', 'visual_after'):
                obj = beat.get(field)
                if not obj or not isinstance(obj, dict):
                    continue
                for key in ('timestamp_start', 'timestamp_end'):
                    if obj.get(key):
                        obj[key] = _fix(obj[key])
                # Repair inverted ranges: if end < start, set end = start (Gemini hallucination)
                s_ts = obj.get('timestamp_start', '')
                e_ts = obj.get('timestamp_end', '')
                if s_ts and e_ts:
                    s_sec = _ts_str_to_secs(s_ts)
                    e_sec = _ts_str_to_secs(e_ts)
                    if e_sec < s_sec:
                        obj['timestamp_end'] = s_ts
            # Drop beats with no timestamp info at all
            has_ts = (
                beat.get('vo', {}).get('timestamp_start')
                or beat.get('vo', {}).get('timestamp_end')
                or beat.get('visual', {}).get('timestamp_start')
                or beat.get('visual', {}).get('timestamp_end')
            )
            if not has_ts:
                continue
            # Drop beats that start beyond the clip (hallucinated timestamps)
            beat_s = _ts_str_to_secs(
                beat.get('vo', {}).get('timestamp_start', '')
                or beat.get('visual', {}).get('timestamp_start', '')
                or '00:00:00'
            )
            if beat_s <= max_secs * 1.05:
                valid.append(beat)
        sec['beats'] = valid
    for pm in data.get('peak_moments', []):
        if pm.get('timestamp'):
            pm['timestamp'] = _fix(pm['timestamp'])
    return data


def _merge_fragment_continuations(sections: list) -> list:
    """Merge narration/ad_read beats whose vo.text is a sentence continuation
    (starts with '...' or lowercase). Works across sections by searching the
    immediately-preceding same-type beat regardless of section boundary."""
    all_pairs = []  # (section_idx, beat)
    for si, sec in enumerate(sections):
        for beat in sec.get('beats', []):
            all_pairs.append((si, beat))

    keep = [True] * len(all_pairs)
    last_by_type = {}  # beat_type -> index into all_pairs

    for idx, (_si, beat) in enumerate(all_pairs):
        vo_text = (beat.get('vo', {}).get('text') or '').strip()
        bt = beat.get('beat_type')
        is_fragment = vo_text and (vo_text.startswith('...') or vo_text[0].islower())

        if is_fragment and bt in ('narration', 'ad_read'):
            prev_idx = last_by_type.get(bt)
            if prev_idx is not None and keep[prev_idx]:
                prev = all_pairs[prev_idx][1]
                prev_vo = (prev.get('vo', {}).get('text') or '').strip()
                if prev_vo:
                    cleaned = vo_text.lstrip('. ').strip()
                    prev['vo']['text'] = (prev_vo.rstrip() + ' ' + cleaned).strip() if cleaned else prev_vo
                    new_end = beat.get('vo', {}).get('timestamp_end', '') or prev['vo'].get('timestamp_end', '')
                    if new_end:
                        prev['vo']['timestamp_end'] = new_end
                    if beat.get('visual', {}).get('timestamp_end'):
                        prev.setdefault('visual', {})['timestamp_end'] = beat['visual']['timestamp_end']
                    keep[idx] = False
                    continue
            # Can't merge — drop the stray fragment
            keep[idx] = False
            continue

        # Track this beat as the most recent of its type
        if vo_text and bt in ('narration', 'ad_read'):
            last_by_type[bt] = idx

    # Rebuild sections preserving only kept beats
    for sec in sections:
        sec['beats'] = []
    for idx, (si, beat) in enumerate(all_pairs):
        if keep[idx]:
            sections[si]['beats'].append(beat)
    return sections


_AD_READ_KEYWORDS = (
    'betterhelp', 'cerebral', 'talkspace', 'hims', 'roman', 'hers',
    'nordvpn', 'expressvpn', 'surfshark', 'protonvpn',
    'squarespace', 'raycon', 'audible', 'skillshare', 'masterclass',
    'raid shadow', 'world of tanks', 'world of warships',
    'helix sleep', 'manscaped', 'athletic greens', 'ag1', 'factor meals',
    'this video is sponsored', 'today\'s sponsor', 'use code', 'promo code',
    'link in the description', 'first month free', 'sign up today',
    'visit the link',
)
_AD_READ_FIRST_PERSON = ('i tried', 'i started', 'i used', 'i felt', 'i hit a point',
                        'my mornings', 'my evenings', 'my anxiety', 'my sleep',
                        'i was struggling', 'i couldn\'t', 'i didn\'t really have')


def _detect_ad_reads(sections: list) -> list:
    """Reclassify narration beats that look like sponsor reads to beat_type='ad_read'."""
    for sec in sections:
        for beat in sec.get('beats', []):
            if beat.get('beat_type') != 'narration':
                continue
            vo_text = (beat.get('vo', {}).get('text') or '').lower()
            if not vo_text:
                continue
            has_keyword = any(k in vo_text for k in _AD_READ_KEYWORDS)
            first_person_pattern = any(p in vo_text for p in _AD_READ_FIRST_PERSON)
            if has_keyword or first_person_pattern:
                beat['beat_type'] = 'ad_read'
    return sections


def _sanitize_vo_beats(sections: list) -> list:
    """
    If Gemini puts clear on-camera speech in vo.text, move it to visual.description.
    """
    _ONCAM_RE = re.compile(
        r'(\bI\s+\w+.*\?)'           # "I did... ?" — first person + question
        r'|(\byeah\b.*\?)'            # yeah + question mark
        r'|(\?\s*[Yy]eah\b)'          # "? Yeah" — answer to question
        r'|(\bwe\s+\w+.*\?)',         # "We did...?"
        re.DOTALL | re.IGNORECASE
    )
    result = []
    for sec in sections:
        new_beats = []
        for beat in sec.get('beats', []):
            vo  = beat.get('vo', {})
            vis = beat.get('visual') or {}
            vo_text = (vo.get('text') or '').strip()
            # If the user specifically wants absolutely NO conversational stuff in VO,
            # this catches the most obvious conversational flags.
            if vo_text and _ONCAM_RE.search(vo_text):
                beat = copy.deepcopy(beat)
                beat['beat_type'] = 'visual_only'
                beat['vo'] = {'timestamp_start': '', 'timestamp_end': '', 'tone': '', 'text': ''}
                vis_obj = beat.setdefault('visual', {})
                vis_desc = vis_obj.get('description') or ''
                vis_obj['description'] = (
                    (vis_desc + '\n' if vis_desc else '') + f'[On-camera]: {vo_text}'
                ).strip()
            new_beats.append(beat)
        result.append({**sec, 'beats': new_beats})
    return result


def _offset_timestamps(data: dict, offset_s: float) -> dict:
    """Add offset_s seconds to every timestamp and ensure HH:MM:SS format."""
    data = copy.deepcopy(data)
    for sec in data.get('sections', []):
        for beat in sec.get('beats', []):
            for field in ('vo', 'visual'):
                obj = beat.get(field, {})
                for key in ('timestamp_start', 'timestamp_end'):
                    ts = obj.get(key, '')
                    if ts:
                        obj[key] = _secs_to_ts(_ts_str_to_secs(ts) + offset_s)
    for pm in data.get('peak_moments', []):
        ts = pm.get('timestamp', '')
        if ts:
            pm['timestamp'] = _secs_to_ts(_ts_str_to_secs(ts) + offset_s)
    return data


def delete_from_gcs(gcs_uri: str, bucket_name: str):
    """Delete the temporary GCS file."""
    _delete_from_gcs(gcs_uri, bucket_name)


def _run_analysis_with_prompt(gcs_uri: str, project_id: str, location: str,
                              prompt: str, max_tokens: int = 65536) -> dict:
    client     = genai.Client(vertexai=True, project=project_id, location=location)
    video_part = types.Part.from_uri(file_uri=gcs_uri, mime_type="video/mp4")
    config     = types.GenerateContentConfig(
        temperature=0,
        max_output_tokens=max_tokens,
        response_mime_type="application/json",
    )

    MAX_RETRIES = 5
    last_error = None

    for model in MODELS:
        for attempt in range(MAX_RETRIES):
            try:
                print(f"  Trying model: {model}")
                response = client.models.generate_content(
                    model=model,
                    contents=[video_part, prompt],
                    config=config,
                )
                return _parse(response.text, model)
            except Exception as e:
                msg = str(e)
                last_error = e

                if "404" in msg or "not found" in msg.lower():
                    if attempt == 0:
                        # First 404 may be transient — wait briefly and retry once
                        print(f"  Model {model} returned 404, retrying in 15s...")
                        time.sleep(15)
                        continue
                    print(f"  Model {model} not available, trying next...")
                    break  # try next model after second failure

                if "429" in msg or "resource_exhausted" in msg.lower() or "quota" in msg.lower():
                    wait = 20 * (2 ** attempt)  # 20s, 40s, 80s, 160s, 320s
                    print(f"  Rate limit hit. Waiting {wait}s before retry ({attempt + 1}/{MAX_RETRIES})...")
                    time.sleep(wait)
                    continue  # retry same model

                raise  # any other error — fail immediately

    raise RuntimeError(
        f"No Gemini model was accessible on project '{project_id}' in '{location}'.\n"
        f"Last error: {last_error}\n\n"
        f"Fix: Go to GCP Console -> Vertex AI -> Enable API, then ensure your\n"
        f"service account has the 'Vertex AI User' role."
    )


def _repair_json(raw: str) -> str:
    """
    Multi-strategy JSON repair:
    1. Insert missing closing bracket(s) at the exact error position.
    2. Walk backwards from end to find the last fully-closed beat, then close.
    Handles both mid-JSON syntax errors (missing `}`) and end-truncation.
    """
    # Strategy 1: insert missing bracket at exact error position
    try:
        json.loads(raw)
        return raw
    except json.JSONDecodeError as e:
        err_pos = e.pos
        for insert in ('}', '},', '}\n', '},\n'):
            patched = raw[:err_pos] + insert + raw[err_pos:]
            try:
                json.loads(patched)
                return patched
            except json.JSONDecodeError:
                pass
        # Try inserting before any `]` near the error
        search_start = max(0, err_pos - 5)
        bracket_pos  = raw.find(']', search_start, err_pos + 10)
        if bracket_pos != -1:
            for insert in ('}', '},\n'):
                patched = raw[:bracket_pos] + insert + raw[bracket_pos:]
                try:
                    json.loads(patched)
                    return patched
                except json.JSONDecodeError:
                    pass

    # Strategy 2: truncation repair — walk backwards for last clean prefix
    pos = len(raw)
    while pos > 0:
        pos = raw.rfind('}', 0, pos)
        if pos == -1:
            break
        candidate = raw[:pos + 1]
        depth_sq = candidate.count('[') - candidate.count(']')
        depth_cu = candidate.count('{') - candidate.count('}')
        if depth_sq < 0 or depth_cu < 0:
            pos -= 1
            continue
        closing = (']' * depth_sq) + ('}' * depth_cu)
        try:
            json.loads(candidate + closing)
            return candidate + closing
        except json.JSONDecodeError:
            pos -= 1
            continue

    return raw  # give up — caller will raise


def _parse(raw: str, model: str) -> dict:
    raw = raw.strip()
    raw = re.sub(r'^```json\s*', '', raw)
    raw = re.sub(r'^```\s*',    '', raw)
    raw = re.sub(r'\s*```$',    '', raw)

    try:
        result = json.loads(raw.strip())
        print(f"  Analysis complete using {model}.")
        if isinstance(result, dict):
            result['_was_repaired'] = False
        return result
    except json.JSONDecodeError:
        # Try json-repair first (handles missing brackets, truncation, etc.)
        try:
            from json_repair import repair_json
            repaired = repair_json(raw.strip())
            result = json.loads(repaired)
            print(f"  Analysis complete using {model} (JSON repaired — likely truncation).")
            if isinstance(result, dict):
                result['_was_repaired'] = True
            return result
        except Exception:
            pass
        # Fallback: manual bracket-repair
        repaired = _repair_json(raw.strip())
        try:
            result = json.loads(repaired)
            print(f"  Analysis complete using {model} (JSON repaired — recovered all complete beats).")
            if isinstance(result, dict):
                result['_was_repaired'] = True
            return result
        except json.JSONDecodeError as e:
            with open("gemini_raw_output.txt", "w", encoding="utf-8") as f:
                f.write(raw)
            raise RuntimeError(
                f"Gemini returned invalid JSON.\n"
                f"Raw output saved to gemini_raw_output.txt\n"
                f"Parse error: {e}"
            )


def _upload_to_gcs(local_path: str, bucket_name: str) -> str:
    client   = storage.Client()
    bucket   = client.bucket(bucket_name)
    filename = os.path.basename(local_path)
    blob     = bucket.blob(f"video-analyzer-temp/{filename}")
    blob.upload_from_filename(local_path, timeout=600)
    return f"gs://{bucket_name}/video-analyzer-temp/{filename}"


def _delete_from_gcs(gcs_uri: str, bucket_name: str):
    try:
        client    = storage.Client()
        bucket    = client.bucket(bucket_name)
        blob_name = gcs_uri.replace(f"gs://{bucket_name}/", "")
        bucket.blob(blob_name).delete()
    except Exception as e:
        print(f"Warning: Could not delete GCS file: {e}")


class _ProgressFile:
    """File wrapper that reports read progress via callback, throttled to every 10 MB."""

    REPORT_EVERY = 10 * 1024 * 1024  # fire callback at most once per 10 MB

    def __init__(self, path: str, size: int, callback):
        self._f            = open(path, 'rb')
        self._size         = size
        self._sent         = 0
        self._last_report  = 0
        self._callback     = callback

    def read(self, n=-1):
        data = self._f.read(n)
        if data and self._callback:
            self._sent += len(data)
            if self._sent - self._last_report >= self.REPORT_EVERY:
                self._callback(self._sent, self._size)
                self._last_report = self._sent
        return data

    def seek(self, *a):  return self._f.seek(*a)
    def tell(self):      return self._f.tell()
    def close(self):     return self._f.close()
    def __enter__(self): return self
    def __exit__(self, *a): self._f.close()
