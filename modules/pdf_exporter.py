import os
import re

from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.units import inch
from reportlab.lib.colors import HexColor
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from reportlab.platypus import (
    SimpleDocTemplate, TableStyle, Paragraph, Spacer, PageBreak,
    KeepTogether,
)
from reportlab.platypus import LongTable as Table


PAGE_SIZE = landscape(A4)  # 297 × 210 mm — standard A4 in landscape


def _ts_to_seconds(ts: str) -> int:
    """Parse 'HH:MM:SS' / 'MM:SS' / 'SS' to whole seconds. 0 on bad input."""
    if not ts:
        return 0
    parts = [p for p in str(ts).strip().split(':') if p.strip()]
    try:
        nums = [int(float(p)) for p in parts]
    except ValueError:
        return 0
    if len(nums) == 3:
        h, m, s = nums
    elif len(nums) == 2:
        h, m, s = 0, nums[0], nums[1]
    elif len(nums) == 1:
        h, m, s = 0, 0, nums[0]
    else:
        return 0
    return h * 3600 + m * 60 + s


def _yt_link(yt_id: str, ts: str) -> str:
    if not yt_id:
        return ''
    return f"https://youtu.be/{yt_id}?t={_ts_to_seconds(ts)}s"

# ── Palette ────────────────────────────────────────────────────────────────────
C_HEADER_BG   = HexColor('#F4B8B8')
C_SECTION_BG  = HexColor('#F2F2F2')
C_SECTION_FG  = HexColor('#555555')
C_TONE_FG     = HexColor('#7C3AED')
C_BODY        = HexColor('#1A1A1A')
C_TS_VO       = HexColor('#B45309')
C_TS_VIS      = HexColor('#15803D')
C_BORDER      = HexColor('#CCCCCC')
C_EMPTY       = HexColor('#CCCCCC')
C_AUDIO_FG    = HexColor('#1D4ED8')
C_SUMM_FG     = HexColor('#166534')
C_SUMM_BG     = HexColor('#DCFCE7')
C_PEAK_BG     = HexColor('#FEF3C7')
C_PEAK_FG     = HexColor('#92400E')
C_HL_BG       = HexColor('#EFF6FF')
C_HL_FG       = HexColor('#1E40AF')
C_CARD_HDR_BG = HexColor('#F8FAFC')
C_CARD_BORDER = HexColor('#E2E8F0')


def _styles():
    def s(name, **kw):
        return ParagraphStyle(name, **kw)

    return {
        'doc_title':   s('DT',  fontSize=16, fontName='Helvetica-Bold',
                          alignment=TA_CENTER, textColor=C_BODY, spaceAfter=4),
        'col_header':  s('PCH', fontSize=11, fontName='Helvetica-Bold',
                          alignment=TA_CENTER, textColor=C_BODY),
        'section':     s('PS',  fontSize=9,  fontName='Helvetica-Bold',
                          textColor=C_SECTION_FG),
        'tone':        s('PTone', fontSize=9, fontName='Helvetica-Oblique',
                          textColor=C_TONE_FG),
        'vo':          s('PVO', fontSize=10, fontName='Helvetica',
                          textColor=C_BODY, leading=15),
        'vis':         s('PVis', fontSize=10, fontName='Helvetica',
                          textColor=C_BODY, leading=15),
        'ts':          s('PTS', fontSize=8,  fontName='Helvetica-Bold',
                          textColor=C_TS_VO),
        'empty':       s('PEm', fontSize=12, fontName='Helvetica',
                          textColor=C_EMPTY, alignment=TA_CENTER),
        'sum_title':   s('STi', fontSize=14, fontName='Helvetica-Bold',
                          textColor=C_BODY, spaceBefore=12, spaceAfter=8),
        'card_header': s('SCH', fontSize=11, fontName='Helvetica-Bold',
                          textColor=C_BODY, spaceAfter=4),
        'sum_body':    s('SB',  fontSize=10, fontName='Helvetica',
                          textColor=HexColor('#374151'), leading=16),
        'peak':        s('SPk', fontSize=10, fontName='Helvetica',
                          textColor=C_PEAK_FG, leading=14),
        'highlight':   s('SHL', fontSize=10, fontName='Helvetica',
                          textColor=C_HL_FG,  leading=14),
    }


# ── Public ─────────────────────────────────────────────────────────────────────
def export_to_pdf(data: dict, output_dir: str, title: str, yt_id: str = '') -> str:
    safe = re.sub(r'[^\w\s\-]', '', title).strip().replace(' ', '_')[:60]
    out  = os.path.join(output_dir, f"{safe}_analysis.pdf")

    doc = SimpleDocTemplate(
        out,
        pagesize=PAGE_SIZE,                 # A4 landscape
        leftMargin=0.55 * inch,
        rightMargin=0.55 * inch,
        topMargin=0.65 * inch,
        bottomMargin=0.65 * inch,
    )

    st    = _styles()
    story = []

    story.append(Paragraph(_x(data.get('title', title)), st['doc_title']))
    dur = data.get('total_duration', '')
    if dur:
        story.append(Paragraph(f'<font size="9" color="#718096">Duration: {_x(dur)}</font>',
                                st['sum_body']))
    story.append(Spacer(1, 10))

    PAGE_W = PAGE_SIZE[0] - 1.1 * inch
    VO_W   = PAGE_W * 0.44
    VIS_W  = PAGE_W * 0.56

    rows     = []
    row_cmds = []

    # Column headers
    rows.append([
        Paragraph('VO',      st['col_header']),
        Paragraph('Visuals', st['col_header']),
    ])
    row_cmds.append(('header', 0))

    sections = data.get('sections', [])
    for sec in sections:
        ri = len(rows)
        rows.append([
            Paragraph(_x(sec.get('title', '')), st['section']),
            Paragraph('', st['vis']),
        ])
        row_cmds.append(('section', ri))

        for beat in sec.get('beats', []):
            vo  = beat.get('vo', {})
            vis = beat.get('visual') or beat.get('visual_after') or {}

            vo_text   = (vo.get('text')             or '').strip()
            vo_tone   = (vo.get('tone')             or '').strip()
            vo_ts_s   = (vo.get('timestamp_start')  or '').strip()
            vo_ts_e   = (vo.get('timestamp_end')    or '').strip()
            vis_desc  = (vis.get('description')     or '').strip()
            vis_ost   = (vis.get('on_screen_text')  or '').strip()
            vis_audio = (vis.get('audio_notes')     or '').strip()
            vis_summ  = (vis.get('summary')         or '').strip()
            vis_ts_s  = (vis.get('timestamp_start') or '').strip()
            vis_ts_e  = (vis.get('timestamp_end')   or '').strip()

            # ── Visual cell ────────────────────────────────────────────────────
            vis_xml = ''
            if vis_ts_s or vis_ts_e:
                ts_str = vis_ts_s + (f' – {vis_ts_e}' if vis_ts_e else '')
                vis_link = _yt_link(yt_id, vis_ts_s)
                if vis_link:
                    vis_xml += (f'<link href="{vis_link}">'
                                f'<font size="8" color="#15803D"><b><u>({_x(ts_str)})</u></b></font>'
                                f'</link><br/>')
                else:
                    vis_xml += (f'<font size="8" color="#15803D"><b>({_x(ts_str)})</b></font>'
                                f'<br/>')
            if vis_desc:
                vis_xml += _x(vis_desc)
            if vis_ost and vis_ost.upper() not in ('NONE', 'N/A', ''):
                vis_xml += (f'<br/><font size="8" color="#555555"><b>On-screen: </b>'
                            f'{_x(vis_ost)}</font>')
            if vis_audio:
                vis_xml += (f'<br/><font size="8" color="#1D4ED8"><b>Audio: </b>'
                            f'{_x(vis_audio)}</font>')
            if vis_summ:
                vis_xml += (f'<br/><font size="8" color="#166534"><i>{_x(vis_summ)}</i></font>')
            vis_cell = Paragraph(vis_xml, st['vis']) if vis_xml else Paragraph('', st['vis'])

            # ── VO cell — include timestamps + split long text ─────────────────
            chunks = _split_text(vo_text, 600) if vo_text else ['']
            for idx, chunk in enumerate(chunks):
                if idx == 0:
                    vo_xml = ''
                    if vo_ts_s or vo_ts_e:
                        ts_str = vo_ts_s + (f' – {vo_ts_e}' if vo_ts_e else '')
                        vo_link = _yt_link(yt_id, vo_ts_s)
                        if vo_link:
                            vo_xml += (f'<link href="{vo_link}">'
                                       f'<font size="8" color="#B45309"><b><u>[{_x(ts_str)}]</u></b></font>'
                                       f'</link><br/>')
                        else:
                            vo_xml += (f'<font size="8" color="#B45309"><b>[{_x(ts_str)}]</b>'
                                       f'</font><br/>')
                    if vo_tone:
                        vo_xml += f'<font size="9" color="#7C3AED"><i>[{_x(vo_tone)}]</i></font><br/>'
                    vo_xml += _x(chunk) if chunk else ''
                    vo_cell = Paragraph(vo_xml, st['vo']) if (chunk or vo_xml) else Paragraph('—', st['empty'])
                    rows.append([vo_cell, vis_cell])
                else:
                    rows.append([Paragraph(_x(chunk), st['vo']), Paragraph('', st['vis'])])
                row_cmds.append(('beat', len(rows) - 1))

    # ── Build main table ───────────────────────────────────────────────────────
    tbl = Table(rows, colWidths=[VO_W, VIS_W], repeatRows=1, splitByRow=1)

    ts = TableStyle([
        ('VALIGN',        (0, 0), (-1, -1), 'TOP'),
        ('GRID',          (0, 0), (-1, -1), 0.4, C_BORDER),
        ('TOPPADDING',    (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('LEFTPADDING',   (0, 0), (-1, -1), 10),
        ('RIGHTPADDING',  (0, 0), (-1, -1), 10),
        ('BACKGROUND',    (0, 0), (-1, 0),  C_HEADER_BG),
        ('TOPPADDING',    (0, 0), (-1, 0),  10),
        ('BOTTOMPADDING', (0, 0), (-1, 0),  10),
    ])
    for kind, ri in row_cmds:
        if kind == 'section':
            ts.add('BACKGROUND', (0, ri), (-1, ri), C_SECTION_BG)
            ts.add('SPAN',       (0, ri), (-1, ri))

    tbl.setStyle(ts)
    story.append(tbl)

    # ── Summary page ───────────────────────────────────────────────────────────
    summary   = (data.get('summary') or '').strip()
    peaks     = data.get('peak_moments', [])
    highlights = data.get('highlights', [])

    if summary or peaks or highlights:
        story.append(PageBreak())
        story.append(Paragraph('Summary &amp; Highlights', st['sum_title']))
        story.append(Spacer(1, 6))

        if summary:
            # Split into one Paragraph per natural paragraph so reportlab can paginate
            summary_paras = [
                Paragraph(_x(p.strip()), st['sum_body'])
                for p in re.split(r'\n\n+', summary) if p.strip()
            ] or [Paragraph(_x(summary), st['sum_body'])]
            story += _card('Full Summary', summary_paras, PAGE_W, st)
            story.append(Spacer(1, 10))

        if peaks:
            peak_items = []
            for pm in peaks:
                ts_str = pm.get('timestamp', '')
                desc   = pm.get('description', '')
                link   = _yt_link(yt_id, ts_str)
                if link:
                    ts_xml = (f'<link href="{link}">'
                              f'<font color="#92400E"><b>&#9654; <u>{_x(ts_str)}</u></b></font>'
                              f'</link>')
                else:
                    ts_xml = f'<font color="#92400E"><b>&#9654; {_x(ts_str)}</b></font>'
                peak_items.append(Paragraph(f'{ts_xml}&nbsp;&nbsp;{_x(desc)}', st['peak']))
            story += _card('Peak Moments', peak_items, PAGE_W, st)
            story.append(Spacer(1, 10))

        if highlights:
            hl_items = [
                Paragraph(f'&#9726;&nbsp;&nbsp;{_x(h)}', st['highlight'])
                for h in highlights
            ]
            story += _card('Key Highlights', hl_items, PAGE_W, st)

    doc.build(story)
    print(f"PDF saved: {out}")
    return out


def _card(header: str, items: list, width: float, st: dict) -> list:
    rows = [[Paragraph(header, st['card_header'])]]
    for item in items:
        rows.append([item])
    tbl = Table(rows, colWidths=[width], splitByRow=1)
    tbl.setStyle(TableStyle([
        ('GRID',          (0, 0), (-1, -1), 0.4, C_CARD_BORDER),
        ('BACKGROUND',    (0, 0), (-1, 0),  C_CARD_HDR_BG),
        ('TOPPADDING',    (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('LEFTPADDING',   (0, 0), (-1, -1), 12),
        ('RIGHTPADDING',  (0, 0), (-1, -1), 12),
        ('VALIGN',        (0, 0), (-1, -1), 'TOP'),
    ]))
    return [tbl]


def _x(text: str) -> str:
    if not text:
        return ''
    return (str(text)
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;'))


def _split_text(text: str, max_chars: int) -> list:
    if len(text) <= max_chars:
        return [text]
    chunks, current = [], ''
    for sentence in re.split(r'(?<=[.!?])\s+', text):
        if len(current) + len(sentence) + 1 > max_chars and current:
            chunks.append(current.strip())
            current = sentence
        else:
            current = (current + ' ' + sentence).strip()
    if current:
        chunks.append(current.strip())
    return chunks or [text]
