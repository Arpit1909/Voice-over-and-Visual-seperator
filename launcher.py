#!/usr/bin/env python3
import os
import re
import sys
import shutil
import subprocess
import threading
import time

# Auto-install rich if missing
try:
    from rich.console import Console
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'rich', '-q'])
    from rich.console import Console

from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Prompt
from rich.rule import Rule
from rich.padding import Padding
from rich.progress import (
    Progress, BarColumn, TextColumn, TimeElapsedColumn,
    SpinnerColumn, FileSizeColumn, TotalFileSizeColumn,
    TransferSpeedColumn, TaskProgressColumn,
)
from rich import box

from dotenv import load_dotenv
load_dotenv()

console = Console()
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output')


def cls():
    os.system('cls' if os.name == 'nt' else 'clear')


def header():
    cls()
    console.print()
    console.print(Padding(Panel(
        "[bold bright_white]VO AND VISUAL EXTRACTOR[/bold bright_white]\n"
        "[dim]Powered by Vertex AI Gemini  •  YouTube & Local Video Support[/dim]",
        border_style="bright_blue",
        expand=False,
        padding=(1, 8),
    ), (0, 2)))
    console.print()


def step_rule(text: str):
    console.print(Padding(Rule(f"[bold bright_blue]{text}[/bold bright_blue]",
                               style="bright_blue"), (0, 2)))
    console.print()


def ok(msg: str):
    console.print(f"  [bold green]✓[/bold green]  {msg}")


def err(msg: str):
    console.print(f"  [bold red]✗[/bold red]  [red]{msg}[/red]")


def info(msg: str):
    console.print(f"  [dim]{msg}[/dim]")


def _browse_file() -> str:
    """Open a native file picker (tkinter) and return the selected path."""
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        path = filedialog.askopenfilename(
            title="Select a video file",
            filetypes=[
                ("Video files", "*.mp4 *.avi *.mov *.mkv *.webm *.flv *.wmv *.mpg *.mpeg *.3gp *.m4v"),
                ("All files", "*.*"),
            ]
        )
        root.destroy()
        return path or ''
    except Exception as e:
        console.print(f"  [dim]File picker unavailable: {e}[/dim]")
        raw = Prompt.ask("  [bright_blue]▶  Paste file path instead[/bright_blue]")
        return raw.strip().strip('"').strip("'").strip()


def run():
    while True:
        header()

        # ── Step 1: Video input ───────────────────────────────────────────
        step_rule("Step 1 of 3  —  Video Input")

        src_t = Table(box=box.SIMPLE, show_header=False,
                      border_style="dim", padding=(0, 2), expand=False)
        src_t.add_column("opt",  width=4,  style="bold cyan", justify="center")
        src_t.add_column("desc", style="bright_white")
        src_t.add_row("1", "Paste a YouTube URL")
        src_t.add_row("2", "Browse local video file  [dim](opens file picker)[/dim]")
        console.print(Padding(src_t, (0, 2)))
        info("Local formats: MP4 · AVI · MOV · MKV · WebM · FLV  |  Max: 1 hour")
        console.print()

        src_choice = Prompt.ask("  [bright_blue]▶  Source[/bright_blue]", default="1").strip()

        if src_choice == "2":
            video_input = _browse_file()
            if not video_input:
                err("No file selected.")
                input("\n  Press Enter to try again...")
                continue
            console.print(f"  [dim]Selected:[/dim] [bright_white]{video_input}[/bright_white]")
        else:
            raw = Prompt.ask("  [bright_blue]▶  YouTube URL[/bright_blue]")
            video_input = raw.strip().strip('"').strip("'").strip()
            if not video_input:
                err("No URL provided.")
                input("\n  Press Enter to try again...")
                continue

        console.print()

        # ── Step 2: Output format ─────────────────────────────────────────
        step_rule("Step 2 of 3  —  Output Format")

        # ── Categorization legend ─────────────────────────────────────────
        cat = Table(box=box.SIMPLE_HEAD, show_header=True,
                    header_style="bold white",
                    border_style="dim",
                    padding=(0, 2),
                    expand=False,
                    title="[bold dim]How output is categorized[/bold dim]",
                    title_justify="left")
        cat.add_column("Column",    width=12, style="bold")
        cat.add_column("Contains",  width=34)
        cat.add_column("Example",   style="dim")

        cat.add_row(
            "[yellow]VO[/yellow]",
            "Narrator's scripted voice-over ONLY\n[dim](off-camera narration track)[/dim]",
            "[dim]\"This is the story of…\"[/dim]",
        )
        cat.add_row(
            "[green]VISUAL[/green]",
            "Everything shown on screen\n[dim]B-roll · interviews · title cards · graphics[/dim]",
            "[dim]Bodycam footage, suspect\nspeaking in interrogation[/dim]",
        )
        cat.add_row(
            "[dim]VO = blank[/dim]",
            "[dim]No narrator — footage/clip plays alone[/dim]",
            "[dim]911 call audio over still image[/dim]",
        )

        console.print(Padding(cat, (0, 2)))
        console.print()

        # ── Format picker ─────────────────────────────────────────────────
        t = Table(box=box.ROUNDED, show_header=True,
                  header_style="bold bright_blue",
                  border_style="bright_blue",
                  padding=(0, 3),
                  expand=False)
        t.add_column("#",       width=4,  justify="center", style="bold cyan")
        t.add_column("Format",  width=8,  style="bold white")
        t.add_column("Description",       style="dim")

        t.add_row("1", "HTML",
                  "[green]Browser viewer — embedded video player + clickable timestamps[/green]  ← default")
        t.add_row("2", "PDF",  "Script document — two-column VO / Visuals table  (matches your script format)")
        t.add_row("3", "DOCX", "Microsoft Word — two-column VO / Visuals table")
        t.add_row("4", "TXT",  "Plain text")
        t.add_row("5", "ALL",  "HTML + PDF + DOCX + TXT")

        console.print(Padding(t, (0, 2)))
        console.print()

        choice = Prompt.ask("  [bright_blue]▶  Format[/bright_blue]", default="1")
        fmt = {"1": "html", "2": "pdf", "3": "docx", "4": "txt", "5": "all"}.get(choice.strip(), "html")

        console.print()

        # ── Step 3: Run ───────────────────────────────────────────────────
        step_rule("Step 3 of 3  —  Running Analysis")

        success = _run_analysis(video_input, fmt)

        console.print()
        console.print(Padding(Rule(style="dim"), (0, 2)))
        console.print()

        again = Prompt.ask("  [bright_blue]▶  Run another video?[/bright_blue]",
                           choices=["y", "n"], default="n")
        if again.lower() != "y":
            break

    console.print()
    console.print(Padding(Panel(
        "[bold]Thanks for using VO and Visual Extractor[/bold]",
        border_style="dim", expand=False, padding=(0, 4)
    ), (0, 2)))
    console.print()
    input("  Press Enter to exit...")


def _run_analysis(video_input: str, fmt: str) -> bool:
    project  = os.getenv('GCP_PROJECT_ID')
    bucket   = os.getenv('GCS_BUCKET_NAME')
    location = os.getenv('VERTEX_AI_LOCATION', 'us-central1')

    if not project:
        err("GCP_PROJECT_ID not found in .env")
        return False
    if not bucket:
        err("GCS_BUCKET_NAME not found in .env")
        return False

    from modules.downloader      import get_video
    from modules.vertex_analyzer import upload_video, run_gemini, delete_from_gcs, \
                                        analyze_video_chunked, CHUNK_THRESHOLD
    from modules.exporter        import export_to_docx, export_to_txt
    from modules.html_exporter   import export_to_html
    from modules.pdf_exporter    import export_to_pdf

    # ── Download ──────────────────────────────────────────────────────────
    console.print("  [bold bright_blue][1/3][/bold bright_blue]  Loading video...")
    info("Connecting to source...")
    console.print()

    try:
        video_path, title, source_meta = get_video(video_input)
        console.print()
        ok(f"[bold]{title}[/bold]")
    except Exception as e:
        console.print()
        err(f"Failed to load video: {e}")
        return False

    # Get video duration for smart chunked analysis
    duration_secs = 0
    try:
        from modules.downloader import _get_duration
        duration_secs = _get_duration(video_path)
        mins = duration_secs / 60
        if duration_secs > 900:
            n_chunks = int(duration_secs // 900) + (1 if duration_secs % 900 else 0)
            ok(f"Duration: [bold]{mins:.1f} min[/bold] — will clip & analyze in [bold]{n_chunks}[/bold] parallel segments.")
        else:
            ok(f"Duration: [bold]{mins:.1f} min[/bold]")
    except Exception:
        pass

    # ── Upload & Analyze ──────────────────────────────────────────────────
    console.print()
    console.print("  [bold bright_blue][2/3][/bold bright_blue]  Uploading & Analyzing...")
    console.print()

    is_long      = duration_secs > CHUNK_THRESHOLD
    gcs_uri      = None
    result_ref   = [None]
    error_holder = [None]
    done_event   = threading.Event()

    import math
    n_chunks = max(1, math.ceil(duration_secs / CHUNK_THRESHOLD)) if is_long else 1

    if is_long:
        # Long video: clip locally → upload each clip → analyze in parallel
        info(f"Long video — clipping into {n_chunks} segments, uploading & analyzing in parallel...")
        console.print()

        def _do_analysis():
            try:
                result_ref[0] = analyze_video_chunked(
                    video_path, project, bucket, location, duration_secs
                )
            except Exception as exc:
                error_holder[0] = exc
            finally:
                done_event.set()

    else:
        # Short video: upload once then analyze
        try:
            file_size = os.path.getsize(video_path)
            with Progress(
                SpinnerColumn(style="bright_blue"),
                TextColumn("  [bright_blue]{task.description}[/bright_blue]"),
                BarColumn(bar_width=36, complete_style="bright_blue", finished_style="green"),
                TaskProgressColumn(),
                TransferSpeedColumn(),
                FileSizeColumn(),
                TextColumn("of"),
                TotalFileSizeColumn(),
                TimeElapsedColumn(),
                console=console,
                transient=False,
            ) as up_prog:
                upload_task = up_prog.add_task("Uploading to Cloud Storage", total=file_size)

                def on_upload(sent, _):
                    up_prog.update(upload_task, completed=sent)

                gcs_uri = upload_video(video_path, bucket, on_upload)
                up_prog.update(upload_task, completed=file_size,
                               description="[green]Upload complete[/green]")
            ok("Video uploaded to Cloud Storage.")
        except Exception as e:
            err(f"Upload failed: {e}")
            return False

        def _do_analysis():
            try:
                result_ref[0] = run_gemini(gcs_uri, project, location, duration_secs)
            except Exception as exc:
                error_holder[0] = exc
            finally:
                if gcs_uri:
                    delete_from_gcs(gcs_uri, bucket)
                done_event.set()

    # ── Analysis progress spinner ─────────────────────────────────────────
    console.print()
    threading.Thread(target=_do_analysis, daemon=True).start()

    ESTIMATED = max(300, n_chunks * 240)

    with Progress(
        SpinnerColumn(spinner_name="dots2", style="bright_blue"),
        TextColumn("  [bright_blue]{task.description}[/bright_blue]"),
        BarColumn(bar_width=40, complete_style="bright_blue", finished_style="green"),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    ) as an_prog:
        analysis_label = (
            f"Gemini 2.5 Pro — clipping & analyzing {n_chunks} segments in parallel..."
            if is_long else "Gemini 2.5 Pro analyzing video..."
        )
        task = an_prog.add_task(analysis_label, total=ESTIMATED)

        while not done_event.is_set():
            done_event.wait(timeout=0.4)
            elapsed = an_prog.tasks[0].elapsed or 0
            target  = min(elapsed, ESTIMATED * 0.95)
            an_prog.update(task, completed=target)

        an_prog.update(task, completed=ESTIMATED,
                       description="[green]Analysis complete[/green]")

    if error_holder[0]:
        err(f"Analysis failed: {error_holder[0]}")
        return False

    result   = result_ref[0]
    sections = len(result.get('sections', []))
    beats    = sum(len(s.get('beats', [])) for s in result.get('sections', []))
    ok(f"[bold]{sections}[/bold] sections · [bold]{beats}[/bold] beats found.")

    # ── Export ────────────────────────────────────────────────────────────
    console.print()
    console.print("  [bold bright_blue][3/3][/bold bright_blue]  Exporting results...")
    console.print()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    safe_name  = re.sub(r'[^\w\s\-]', '', title).strip().replace(' ', '_')[:60]

    # Pick what the HTML player will reference:
    # - YouTube + embeddable  → YT iframe (slim, shareable HTML)
    # - YouTube + blocked     → compressed + base64 embed (self-contained)
    # - Local file            → copy mp4 next to the HTML
    html_video_source = _pick_html_source(video_input, video_path, title,
                                          source_meta, safe_name)

    # Save raw JSON so PDF can be regenerated without re-analyzing
    import json as _json
    json_path = os.path.join(OUTPUT_DIR, safe_name + '_data.json')
    try:
        with open(json_path, 'w', encoding='utf-8') as jf:
            _json.dump(result, jf, ensure_ascii=False, indent=2)
    except Exception as e:
        info(f"Could not save JSON: {e}")

    outputs = []

    try:
        if fmt in ('html', 'all'):
            outputs.append(export_to_html(result, OUTPUT_DIR, title, html_video_source))
            outputs.append(export_to_pdf(result, OUTPUT_DIR, title))  # always include PDF with HTML
        elif fmt in ('pdf', 'all'):
            outputs.append(export_to_pdf(result, OUTPUT_DIR, title))
        if fmt in ('docx', 'all'):
            outputs.append(export_to_docx(result, OUTPUT_DIR, title))
        if fmt in ('txt', 'all'):
            outputs.append(export_to_txt(result, OUTPUT_DIR, title))
    except Exception as e:
        err(f"Export failed: {e}")
        return False

    # ── Success panel ─────────────────────────────────────────────────────
    files_text = "\n".join(f"  [cyan]→[/cyan]  {os.path.basename(p)}" for p in outputs)
    console.print()
    console.print(Padding(Panel(
        f"[bold green]Analysis Complete![/bold green]\n\n"
        f"{files_text}\n\n"
        f"[dim]Saved to:[/dim] [bright_white]{OUTPUT_DIR}[/bright_white]",
        border_style="green",
        title="[bold green] Done [/bold green]",
        expand=False,
        padding=(1, 4),
    ), (0, 2)))

    # Open output folder (cross-platform)
    try:
        if sys.platform == 'win32':
            os.startfile(OUTPUT_DIR)
        elif sys.platform == 'darwin':
            subprocess.run(['open', OUTPUT_DIR], check=False)
        else:
            subprocess.run(['xdg-open', OUTPUT_DIR], check=False)
    except Exception:
        pass

    return True


def _pick_html_source(video_input: str, video_path: str, title: str,
                      source_meta: dict, safe_name: str) -> str:
    """Choose what the HTML player will reference.

    Returns one of:
      - the original YouTube URL       (YT iframe, slim HTML)
      - a `data:video/mp4;base64,...`  (self-contained HTML)
      - a local file path              (mp4 alongside HTML)
    """
    src = source_meta.get('source')

    if src == 'youtube' and source_meta.get('playable_in_embed', True):
        return video_input

    if src == 'youtube':
        console.print()
        info("YouTube has disabled embedding for this video — embedding it directly into the HTML instead.")
        return _build_base64_source(video_path)

    local_video = os.path.join(OUTPUT_DIR, safe_name + '.mp4')
    try:
        shutil.copy2(video_path, local_video)
        ok("Video saved for local playback.")
        return local_video
    except Exception as e:
        info(f"Could not save local video copy: {e}")
        return video_input


def _build_base64_source(video_path: str) -> str:
    """Compress the video and return it as a base64 data URL."""
    from modules.video_embedder import compress_for_embed, to_data_url
    from modules.downloader import _env_with_ffmpeg

    preset = os.getenv('EMBED_PRESET', 'balanced')  # heavy | balanced | light
    info(f"Compressing video for embed (preset: {preset})...")
    try:
        compressed = compress_for_embed(video_path, preset=preset,
                                        ffmpeg_env=_env_with_ffmpeg())
    except Exception as e:
        err(f"Compression failed: {e}")
        raise

    size_mb = os.path.getsize(compressed) / (1024 * 1024)
    ok(f"Compressed to {size_mb:.1f} MB — encoding to base64...")
    data_url = to_data_url(compressed)
    ok(f"Base64 embed ready (~{size_mb * 1.33:.0f} MB added to HTML).")
    return data_url


if __name__ == '__main__':
    try:
        run()
    except KeyboardInterrupt:
        console.print("\n\n  [dim]Cancelled.[/dim]\n")
        sys.exit(0)
