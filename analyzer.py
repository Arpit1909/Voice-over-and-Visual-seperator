#!/usr/bin/env python3
"""
VO and Visual Extractor
Analyzes YouTube videos or local video files and generates a two-column
VO/Visuals script document using Vertex AI Gemini.

Usage:
    python analyzer.py <youtube_url_or_local_path> [options]

Examples:
    python analyzer.py "https://www.youtube.com/watch?v=..."
    python analyzer.py "C:/Videos/my_video.mp4"
    python analyzer.py "C:/Videos/my_video.mp4" --output results --format docx
"""

import argparse
import json
import os
import sys

from dotenv import load_dotenv

load_dotenv()


def main():
    parser = argparse.ArgumentParser(
        description='Analyze a video and generate a two-column VO/Visuals script.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    parser.add_argument('input', help='YouTube URL or path to local video file')
    parser.add_argument('--output', '-o', default='output',
                        help='Output folder for results (default: ./output)')
    parser.add_argument('--format', '-f',
                        choices=['html', 'docx', 'txt', 'all'],
                        default='html',
                        help='Output format: html (default), docx, txt, all')
    parser.add_argument('--project', default=os.getenv('GCP_PROJECT_ID'),
                        help='Google Cloud Project ID (or set GCP_PROJECT_ID in .env)')
    parser.add_argument('--bucket', default=os.getenv('GCS_BUCKET_NAME'),
                        help='GCS bucket name for temporary video upload (or set GCS_BUCKET_NAME in .env)')
    parser.add_argument('--location', default=os.getenv('VERTEX_AI_LOCATION', 'us-central1'),
                        help='Vertex AI location (default: us-central1)')
    parser.add_argument('--save-json', action='store_true',
                        help='Also save the raw analysis JSON')
    args = parser.parse_args()

    # Validate required config
    if not args.project:
        print("ERROR: GCP Project ID required. Set --project or GCP_PROJECT_ID in .env")
        sys.exit(1)
    if not args.bucket:
        print("ERROR: GCS Bucket name required. Set --bucket or GCS_BUCKET_NAME in .env")
        sys.exit(1)

    # Lazy imports (faster startup for --help)
    from modules.downloader import get_video
    from modules.vertex_analyzer import analyze_video
    from modules.exporter import export_to_docx, export_to_txt
    from modules.html_exporter import export_to_html

    print(f"\n{'='*60}")
    print("  VO and Visual Extractor")
    print(f"{'='*60}")

    # Step 1: Get video
    print(f"\n[1/3] Loading video: {args.input}")
    try:
        video_path, title, source_meta = get_video(args.input)
        print(f"      Title: {title}")
        print(f"      Path:  {video_path}")
    except Exception as e:
        print(f"\nERROR loading video: {e}")
        sys.exit(1)

    # Step 2: Analyze with Vertex AI
    print(f"\n[2/3] Analyzing with Vertex AI Gemini...")
    try:
        result = analyze_video(video_path, args.project, args.bucket, args.location)
        sections_count = len(result.get('sections', []))
        beats_count = sum(len(s.get('beats', [])) for s in result.get('sections', []))
        print(f"      Found {sections_count} sections, {beats_count} beats (VO + Visual pairs)")
    except Exception as e:
        print(f"\nERROR during analysis: {e}")
        sys.exit(1)

    # Step 3: Export results
    print(f"\n[3/3] Exporting results to '{args.output}/'...")
    os.makedirs(args.output, exist_ok=True)

    if args.save_json:
        import re
        safe = re.sub(r'[^\w\s\-]', '', title).strip().replace(' ', '_')[:60]
        json_path = os.path.join(args.output, f"{safe}_analysis.json")
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(result, f, indent=2, ensure_ascii=False)
        print(f"      JSON saved → {json_path}")

    html_source = _pick_html_source_cli(args.input, video_path, source_meta)

    try:
        if args.format in ('html', 'all'):
            export_to_html(result, args.output, title, html_source)
        if args.format in ('docx', 'all'):
            export_to_docx(result, args.output, title)
        if args.format in ('txt', 'all'):
            export_to_txt(result, args.output, title)
    except Exception as e:
        print(f"\nERROR exporting results: {e}")
        sys.exit(1)

    print(f"\n{'='*60}")
    print(f"  Done! Results saved to: {os.path.abspath(args.output)}/")
    print(f"{'='*60}\n")


def _pick_html_source_cli(video_input: str, video_path: str, meta: dict) -> str:
    """Choose what the exported HTML player should reference.

    - YouTube + embeddable → original URL (slim HTML, YT iframe)
    - YouTube + blocked    → base64 data URL (self-contained HTML)
    - Local file           → local path (mp4 sits next to the HTML)
    """
    src = meta.get('source')
    if src == 'youtube' and meta.get('playable_in_embed', True):
        return video_input
    if src == 'youtube':
        print("      YouTube embed disabled by uploader — embedding video directly into HTML.")
        from modules.video_embedder import compress_for_embed, to_data_url
        from modules.downloader import _env_with_ffmpeg
        preset = os.getenv('EMBED_PRESET', 'balanced')
        compressed = compress_for_embed(video_path, preset=preset,
                                        ffmpeg_env=_env_with_ffmpeg())
        return to_data_url(compressed)
    return video_path


if __name__ == '__main__':
    main()
