import os
import re

from docx import Document
from docx.shared import Pt, RGBColor, Inches
from docx.enum.text import WD_ALIGN_PARAGRAPH
from docx.oxml.ns import qn
from docx.oxml import OxmlElement


# Colours
C_GREY     = RGBColor(0x80, 0x80, 0x80)
C_DARK     = RGBColor(0x33, 0x33, 0x33)
C_BLUE     = RGBColor(0x1a, 0x56, 0xb0)
C_PURPLE   = RGBColor(0x6a, 0x0d, 0xad)
C_GREEN    = RGBColor(0x1a, 0x7a, 0x3c)

BG_HEADER  = 'FFD0D0'   # pink  – column headers
BG_SECTION = 'E8EAF6'   # indigo tint – section row
BG_VO      = 'FFF9E6'   # warm cream – VO rows
BG_VISUAL  = 'E8F5E9'   # light green – Visual rows


def export_to_docx(data: dict, output_dir: str, title: str) -> str:
    doc = Document()

    # Landscape-ish wide page
    sec = doc.sections[0]
    sec.page_width   = Inches(11)
    sec.page_height  = Inches(8.5)
    sec.left_margin  = sec.right_margin = Inches(0.6)
    sec.top_margin   = sec.bottom_margin = Inches(0.6)

    # ── Title block ──────────────────────────────────────────────────────────
    p = doc.add_paragraph()
    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r = p.add_run(data.get('title', title))
    r.bold = True; r.font.size = Pt(16)

    p2 = doc.add_paragraph()
    p2.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r2 = p2.add_run(f"Duration: {data.get('total_duration', 'N/A')}")
    r2.italic = True; r2.font.size = Pt(10); r2.font.color.rgb = C_GREY

    doc.add_paragraph()

    # ── Main two-column table ─────────────────────────────────────────────────
    tbl = doc.add_table(rows=1, cols=2)
    tbl.style = 'Table Grid'
    _set_col_width(tbl, 0, Inches(4.7))
    _set_col_width(tbl, 1, Inches(4.7))

    # Column header row
    hdr = tbl.rows[0].cells
    _bg(hdr[0], BG_HEADER); _bg(hdr[1], BG_HEADER)
    hdr[0].paragraphs[0].add_run('VO').bold = True
    hdr[1].paragraphs[0].add_run('Visuals  (plays AFTER the VO)').bold = True

    for section in data.get('sections', []):
        # Section title spanning both columns
        sr = tbl.add_row()
        mc = sr.cells[0].merge(sr.cells[1])
        _bg(mc, BG_SECTION)
        run = mc.paragraphs[0].add_run(section.get('title', '').upper())
        run.bold = True; run.font.size = Pt(10); run.font.color.rgb = C_BLUE

        for beat in section.get('beats', []):
            vo   = beat.get('vo', {})
            viz  = beat.get('visual') or beat.get('visual_after') or {}

            row = tbl.add_row()
            vo_cell, vis_cell = row.cells[0], row.cells[1]

            # ── VO cell ──────────────────────────────────────────────────────
            _bg(vo_cell, BG_VO)
            vp = vo_cell.paragraphs[0]

            # Timestamp
            ts = f"[{vo.get('timestamp_start','')} – {vo.get('timestamp_end','')}]"
            r = vp.add_run(ts + '\n'); r.font.size = Pt(8); r.font.color.rgb = C_GREY

            # Tone
            tone = vo.get('tone', '')
            if tone:
                r = vp.add_run(tone + '\n'); r.italic = True
                r.font.size = Pt(9); r.font.color.rgb = RGBColor(0x66,0x66,0x66)

            # VO text
            vp.add_run(vo.get('text', ''))

            # ── Visual cell ──────────────────────────────────────────────────
            _bg(vis_cell, BG_VISUAL)
            vizp = vis_cell.paragraphs[0]

            # Timestamp
            vts = f"({viz.get('timestamp_start','')} – {viz.get('timestamp_end','')})"
            r = vizp.add_run(vts + '\n'); r.bold = True
            r.font.size = Pt(9); r.font.color.rgb = C_PURPLE

            # Description
            desc = viz.get('description', '')
            if desc:
                r = vizp.add_run(desc + '\n\n')
                r.font.size = Pt(10)

            # On-screen text
            ost = viz.get('on_screen_text', '')
            if ost and ost.upper() != 'NONE':
                r = vizp.add_run('On-screen text: '); r.bold = True; r.font.size = Pt(9)
                r = vizp.add_run(ost + '\n'); r.font.size = Pt(9); r.font.color.rgb = C_DARK

            # Audio notes
            audio = viz.get('audio_notes', '')
            if audio:
                r = vizp.add_run('Audio: '); r.bold = True; r.font.size = Pt(9)
                r = vizp.add_run(audio + '\n'); r.font.size = Pt(9); r.font.color.rgb = C_GREY

            # Visual summary
            vsumm = viz.get('summary', '')
            if vsumm:
                r = vizp.add_run('\nSummary: '); r.bold = True; r.italic = True; r.font.size = Pt(9)
                r = vizp.add_run(vsumm); r.italic = True; r.font.size = Pt(9)
                r.font.color.rgb = C_GREEN

    # ── Summary page ──────────────────────────────────────────────────────────
    doc.add_page_break()

    h = doc.add_heading('Summary & Highlights', 1)
    h.alignment = WD_ALIGN_PARAGRAPH.LEFT

    _labeled(doc, 'Full Summary', data.get('summary', ''))

    peaks = data.get('peak_moments', [])
    if peaks:
        doc.add_paragraph()
        _labeled(doc, 'Peak Moments', '')
        for pm in peaks:
            p = doc.add_paragraph(style='List Bullet')
            p.add_run(f"[{pm.get('timestamp','')}] ").bold = True
            p.add_run(pm.get('description', ''))

    highlights = data.get('highlights', [])
    if highlights:
        doc.add_paragraph()
        _labeled(doc, 'Key Highlights', '')
        for hl in highlights:
            doc.add_paragraph(hl, style='List Bullet')

    out = os.path.join(output_dir, f"{_safe(title)}_analysis.docx")
    doc.save(out)
    print(f"DOCX saved → {out}")
    return out


def export_to_txt(data: dict, output_dir: str, title: str) -> str:
    W = 80
    sep  = '=' * W
    thin = '─' * W

    lines = [sep, f"VIDEO ANALYSIS: {data.get('title', title)}",
             f"Duration: {data.get('total_duration','N/A')}", sep, '']

    for section in data.get('sections', []):
        lines += ['', thin, f"  SECTION: {section.get('title','').upper()}", thin, '']

        for i, beat in enumerate(section.get('beats', []), 1):
            vo  = beat.get('vo', {})
            viz = beat.get('visual') or beat.get('visual_after') or {}

            lines.append(f"  BEAT {i}")
            lines.append(f"  ┌─ VO  [{vo.get('timestamp_start','')} – {vo.get('timestamp_end','')}]")
            if vo.get('tone'):
                lines.append(f"  │   Tone : {vo['tone']}")
            lines.append(f"  │   Text : {vo.get('text','')}")
            lines.append( "  │")
            lines.append(f"  └─ VISUAL  ({viz.get('timestamp_start','')} – {viz.get('timestamp_end','')})")
            lines.append(f"      Description : {viz.get('description','')}")
            if viz.get('on_screen_text') and viz['on_screen_text'].upper() != 'NONE':
                lines.append(f"      On-screen   : {viz['on_screen_text']}")
            if viz.get('audio_notes'):
                lines.append(f"      Audio       : {viz['audio_notes']}")
            if viz.get('summary'):
                lines.append(f"      Summary     : {viz['summary']}")
            lines.append('')

    lines += ['', sep, 'SUMMARY', sep, data.get('summary',''), '']

    if data.get('peak_moments'):
        lines += [thin, 'PEAK MOMENTS', thin]
        for pm in data['peak_moments']:
            lines.append(f"  [{pm.get('timestamp','')}]  {pm.get('description','')}")
        lines.append('')

    if data.get('highlights'):
        lines += [thin, 'KEY HIGHLIGHTS', thin]
        for hl in data['highlights']:
            lines.append(f"  • {hl}")

    out = os.path.join(output_dir, f"{_safe(title)}_analysis.txt")
    with open(out, 'w', encoding='utf-8') as f:
        f.write('\n'.join(lines))
    print(f"TXT saved  → {out}")
    return out


# ── helpers ──────────────────────────────────────────────────────────────────

def _bg(cell, hex_color: str):
    tc = cell._tc
    tcPr = tc.get_or_add_tcPr()
    shd = OxmlElement('w:shd')
    shd.set(qn('w:fill'), hex_color)
    shd.set(qn('w:val'), 'clear')
    tcPr.append(shd)


def _set_col_width(table, idx: int, width):
    for row in table.rows:
        row.cells[idx].width = width


def _labeled(doc, label: str, content: str):
    p = doc.add_paragraph()
    r = p.add_run(label + '\n'); r.bold = True; r.font.size = Pt(12)
    if content:
        p.add_run(content)


def _safe(name: str) -> str:
    return re.sub(r'[^\w\s\-]', '', name).strip().replace(' ', '_')[:60]
