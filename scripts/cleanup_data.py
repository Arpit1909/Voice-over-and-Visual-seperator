#!/usr/bin/env python3
"""
One-shot fixer for existing data.json files produced by older builds.

Removes "stacked narration" artifacts: groups of narration / ad_read beats
that share an identical (timestamp_start, timestamp_end) tuple. The first
occurrence keeps its slot; sibling beats have their text concatenated into it
and are then dropped. Already-clean data.json files are left untouched.

Usage:
  python scripts/cleanup_data.py /path/to/data.json
  python scripts/cleanup_data.py /root/NB_Projects/script/data/analyses
  python scripts/cleanup_data.py --dry-run /path/to/data.json
  python scripts/cleanup_data.py --in-place /root/NB_Projects/script/data/analyses
"""
from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path


def _merge_stacked_narrations(sections: list) -> tuple[list, int]:
    """Returns (cleaned_sections, num_beats_dropped)."""
    dropped = 0
    for sec in sections:
        beats = sec.get('beats') or []
        if not beats:
            continue
        keep: list[dict] = []
        peer_idx: dict[tuple, int] = {}
        for beat in beats:
            bt = beat.get('beat_type', 'narration')
            if bt not in ('narration', 'ad_read'):
                keep.append(beat)
                continue
            vo = beat.get('vo') or {}
            vs = vo.get('timestamp_start', '')
            ve = vo.get('timestamp_end', '')
            if not vs or not ve:
                keep.append(beat)
                continue
            key = (vs, ve, bt)
            if key in peer_idx:
                prev = keep[peer_idx[key]]
                prev_text = ((prev.get('vo') or {}).get('text') or '').strip()
                cur_text = (vo.get('text') or '').strip()
                if cur_text and cur_text not in prev_text:
                    combined = (prev_text + ' ' + cur_text).strip() if prev_text else cur_text
                    prev.setdefault('vo', {})['text'] = combined
                dropped += 1
                continue
            peer_idx[key] = len(keep)
            keep.append(beat)
        sec['beats'] = keep
    return sections, dropped


def _read_json_tolerant(path: Path) -> tuple[dict, str]:
    """Read a JSON file, sniffing common encodings (utf-8, utf-8-sig, utf-16).
    Returns (data, detected_encoding)."""
    raw = path.read_bytes()
    if raw.startswith(b'\xff\xfe') or raw.startswith(b'\xfe\xff'):
        text = raw.decode('utf-16')
        return json.loads(text), 'utf-16'
    if raw.startswith(b'\xef\xbb\xbf'):
        text = raw.decode('utf-8-sig')
        return json.loads(text), 'utf-8-sig'
    text = raw.decode('utf-8')
    return json.loads(text), 'utf-8'


def _process_file(path: Path, in_place: bool, dry_run: bool) -> dict:
    try:
        data, encoding = _read_json_tolerant(path)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        return {'path': str(path), 'status': 'parse_error', 'error': str(e)}

    sections = data.get('sections') or []
    before = sum(len(s.get('beats', [])) for s in sections)
    cleaned, dropped = _merge_stacked_narrations(sections)
    after = sum(len(s.get('beats', [])) for s in cleaned)

    if dropped == 0:
        return {'path': str(path), 'status': 'clean', 'beats': before}

    if dry_run:
        return {
            'path': str(path), 'status': 'would_fix',
            'before': before, 'after': after, 'dropped': dropped,
        }

    data['sections'] = cleaned
    if in_place:
        backup = path.with_suffix(path.suffix + f'.bak.{datetime.now().strftime("%Y%m%d-%H%M%S")}')
        shutil.copy2(path, backup)
        path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding='utf-8')
        return {
            'path': str(path), 'status': 'fixed_in_place',
            'before': before, 'after': after, 'dropped': dropped,
            'backup': str(backup),
        }
    out = path.with_name(path.stem + '.cleaned' + path.suffix)
    out.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding='utf-8')
    return {
        'path': str(path), 'status': 'fixed_to_new_file',
        'before': before, 'after': after, 'dropped': dropped,
        'output': str(out),
    }


def _iter_targets(target: Path):
    if target.is_file():
        yield target
        return
    if target.is_dir():
        for sub in sorted(target.rglob('data.json')):
            yield sub


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('target', help='Path to a data.json or a directory containing analyses/<id>/data.json')
    ap.add_argument('--dry-run', action='store_true', help='Show what would change without writing')
    ap.add_argument('--in-place', action='store_true',
                    help='Overwrite the original (a .bak.<ts> backup is created automatically). '
                         'Without this flag, fixes are written to <name>.cleaned.json next to the original.')
    args = ap.parse_args()

    target = Path(args.target).expanduser().resolve()
    if not target.exists():
        print(f"error: {target} does not exist", file=sys.stderr)
        return 2

    files = list(_iter_targets(target))
    if not files:
        print(f"no data.json files found under {target}", file=sys.stderr)
        return 1

    print(f"scanning {len(files)} data.json file(s)...")
    summary = {'clean': 0, 'fixed': 0, 'errors': 0, 'total_dropped': 0}
    for f in files:
        res = _process_file(f, in_place=args.in_place, dry_run=args.dry_run)
        s = res['status']
        if s == 'clean':
            print(f"  ok      {f.relative_to(target.parent) if target.is_file() else f.relative_to(target)}  ({res['beats']} beats, no stacking)")
            summary['clean'] += 1
        elif s == 'parse_error':
            print(f"  ERROR   {f}  ({res['error']})", file=sys.stderr)
            summary['errors'] += 1
        else:
            arrow = "would fix" if s == 'would_fix' else "fixed"
            print(f"  {arrow:9} {f}  ({res['before']} -> {res['after']} beats, dropped {res['dropped']} duplicates)")
            if 'backup' in res:
                print(f"            backup: {res['backup']}")
            if 'output' in res:
                print(f"            output: {res['output']}")
            summary['fixed'] += 1
            summary['total_dropped'] += res['dropped']

    print()
    print(f"clean: {summary['clean']}  fixed: {summary['fixed']}  errors: {summary['errors']}  beats removed: {summary['total_dropped']}")
    return 0


if __name__ == '__main__':
    sys.exit(main())
