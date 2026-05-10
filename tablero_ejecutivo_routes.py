from datetime import date, datetime, timedelta
import html as html_lib
import os
from io import BytesIO
from urllib.parse import urlencode

from flask import Blueprint, request, session, send_file

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import Image as RLImage, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from db_utils import get_db


tablero_ejecutivo_bp = Blueprint("tablero_ejecutivo", __name__)

_OK_ESTADOS = {
    "OK",
    "APROBADO",
    "OBS",
    "OBSERVACION",
    "OBSERVACION",
    "OM",
    "OP MEJORA",
    "OPORTUNIDAD DE MEJORA",
}


def _e(value):
    return html_lib.escape(str(value or ""))


def _to_float(value, default=0.0):
    try:
        txt = str(value or "").strip().replace(",", ".")
        if not txt:
            return float(default)
        return float(txt)
    except Exception:
        return float(default)


def _to_date(value):
    txt = str(value or "").strip()
    if not txt:
        return None
    try:
        return datetime.strptime(txt[:10], "%Y-%m-%d").date()
    except Exception:
        return None


def _semana_label(year, week):
    return f"{year}-S{int(week):02d}"


def _week_start(year, week):
    return date.fromisocalendar(int(year), int(week), 1)


def _pct(numerator, denominator):
    if denominator <= 0:
        return 0.0
    return (numerator / denominator) * 100.0


def _fmt_kg(v):
    return f"{_to_float(v):,.0f}".replace(",", ".")


def _fmt_tn(v):
    return f"{_to_float(v) / 1000.0:,.2f}".replace(",", ".")


def _fmt_hs(v):
    return f"{_to_float(v):,.1f}".replace(",", ".")


def _fmt_pct(v):
    return f"{_to_float(v):.1f}%"


def _fmt_signed_pct(v):
    val = _to_float(v)
    sign = "+" if val > 0 else ""
    return f"{sign}{val:.1f}%"


def _fmt_ratio(v):
    return f"{_to_float(v):.2f}"


def _progress_for_period(start_d, end_d, today_d):
    if not start_d or not end_d:
        return 0.0
    if end_d < start_d:
        end_d = start_d
    total_days = (end_d - start_d).days + 1
    if total_days <= 0:
        return 0.0
    if today_d < start_d:
        return 0.0
    if today_d > end_d:
        return 1.0
    elapsed = (today_d - start_d).days + 1
    return max(0.0, min(1.0, elapsed / total_days))


def _iso_week_range(today_d, weeks_back=10):
    current = today_d - timedelta(days=today_d.weekday())
    starts = []
    for i in range(weeks_back - 1, -1, -1):
        starts.append(current - timedelta(weeks=i))
    return starts


def _build_weekly_trend_svg(points_real, points_programado):
    width = 900
    height = 220
    pad_left = 42
    pad_right = 16
    pad_top = 12
    pad_bottom = 34
    inner_w = width - pad_left - pad_right
    inner_h = height - pad_top - pad_bottom

    all_vals = [v for _, v in points_real] + [v for _, v in points_programado]
    max_v = max(all_vals) if all_vals else 0.0
    max_v = max(max_v, 1.0)

    def _xy(idx, val, total):
        x = pad_left + (inner_w * idx / max(total - 1, 1))
        y = pad_top + inner_h - (inner_h * (val / max_v))
        return x, y

    n = max(len(points_real), len(points_programado), 1)

    poly_real = " ".join(
        f"{_xy(i, val, n)[0]:.1f},{_xy(i, val, n)[1]:.1f}" for i, (_, val) in enumerate(points_real)
    )
    poly_prog = " ".join(
        f"{_xy(i, val, n)[0]:.1f},{_xy(i, val, n)[1]:.1f}" for i, (_, val) in enumerate(points_programado)
    )

    labels = []
    for i, (lbl, _) in enumerate(points_real):
        x, _ = _xy(i, 0, n)
        labels.append(f'<text x="{x:.1f}" y="{height-10}" text-anchor="middle" font-size="11" fill="#64748b">{_e(lbl)}</text>')

    y_ticks = []
    for t in range(0, 5):
        val = (max_v / 4) * t
        y = pad_top + inner_h - (inner_h * (val / max_v))
        y_ticks.append(f'<line x1="{pad_left}" y1="{y:.1f}" x2="{width-pad_right}" y2="{y:.1f}" stroke="#e2e8f0" stroke-width="1"/>')
        y_ticks.append(f'<text x="4" y="{y+4:.1f}" font-size="10" fill="#64748b">{int(val)}</text>')

    return f"""
    <svg viewBox=\"0 0 {width} {height}\" style=\"width:100%;height:auto;display:block;\" aria-label=\"Tendencia semanal\">
      <rect x=\"0\" y=\"0\" width=\"{width}\" height=\"{height}\" fill=\"#ffffff\"/>
      {''.join(y_ticks)}
      <line x1=\"{pad_left}\" y1=\"{pad_top+inner_h}\" x2=\"{width-pad_right}\" y2=\"{pad_top+inner_h}\" stroke=\"#cbd5e1\" stroke-width=\"1.2\"/>
    <polyline fill=\"none\" stroke=\"#f59e0b\" stroke-width=\"3\" points=\"{poly_prog}\"/>
    <polyline fill=\"none\" stroke=\"#f97316\" stroke-width=\"3\" points=\"{poly_real}\"/>
      {''.join(labels)}
    </svg>
    """


def _build_pdf_report(data, obra, tipo):
    m = data.get("metrics", {})
    weekly = data.get("weekly_compare", [])
    nc_items = data.get("nc_por_proceso", [])
    weekly_trend = data.get("weekly_trend", [])

    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=A4,
        topMargin=12 * mm,
        bottomMargin=12 * mm,
        leftMargin=12 * mm,
        rightMargin=12 * mm,
    )

    styles = getSampleStyleSheet()
    s_title = ParagraphStyle("te_title", parent=styles["Heading1"], fontSize=17, leading=20, textColor=colors.HexColor("#7C2D12"))
    s_sub = ParagraphStyle("te_sub", parent=styles["Normal"], fontSize=9.5, leading=12.5, textColor=colors.HexColor("#9A3412"))
    s_h2 = ParagraphStyle("te_h2", parent=styles["Heading2"], fontSize=11.5, leading=14, textColor=colors.HexColor("#7C2D12"), spaceBefore=7, spaceAfter=4)
    s_norm = ParagraphStyle("te_norm", parent=styles["Normal"], fontSize=9, leading=11.8, textColor=colors.HexColor("#0F172A"))

    filtro_obra = obra.strip() if obra else "Todas"
    filtro_tipo = tipo.strip() if tipo else "Todos"
    hoy_txt = date.today().strftime("%d/%m/%Y")

    app_dir = os.path.dirname(os.path.abspath(__file__))
    logo_path = os.path.join(app_dir, "LOGO.png")

    elements = []
    logo_cell = ""
    if os.path.isfile(logo_path):
        try:
            logo_cell = RLImage(logo_path, width=30 * mm, height=18 * mm)
        except Exception:
            logo_cell = ""

    hero = Table(
        [
            [logo_cell, Paragraph("Tablero Ejecutivo Integral", s_title), Paragraph(f"Fecha: {hoy_txt}<br/>Obra: {html_lib.escape(filtro_obra)}<br/>Tipo: {html_lib.escape(filtro_tipo)}", s_sub)],
            ["", Paragraph("Reporte ejecutivo PREVISTO (OT) / PROGRAMADO (Planificacion) / REAL (Ejecucion)", s_sub), ""],
        ],
        colWidths=[34 * mm, 98 * mm, 50 * mm],
    )
    hero.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#FFF7ED")),
                ("SPAN", (1, 1), (2, 1)),
                ("BOX", (0, 0), (-1, -1), 0.8, colors.HexColor("#FDBA74")),
                ("LEFTPADDING", (0, 0), (-1, -1), 8),
                ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                ("TOPPADDING", (0, 0), (-1, -1), 7),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
                ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
            ]
        )
    )
    elements.append(hero)
    elements.append(Spacer(1, 3 * mm))

    alertas = m.get("alertas", [])
    if alertas:
        alert_txt = " | ".join(str(a) for a in alertas)
        alert_table = Table([[Paragraph(f"Alertas automaticas: {html_lib.escape(alert_txt)}", s_norm)]], colWidths=[182 * mm])
        alert_table.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#FFEDD5")),
                    ("BOX", (0, 0), (-1, -1), 0.6, colors.HexColor("#FB923C")),
                    ("LEFTPADDING", (0, 0), (-1, -1), 8),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 8),
                    ("TOPPADDING", (0, 0), (-1, -1), 6),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
                ]
            )
        )
        elements.append(alert_table)
        elements.append(Spacer(1, 2 * mm))

    elements.append(Paragraph("1. KPIs ejecutivos", s_h2))
    resumen_rows = [
        ["Avance global", _fmt_pct(m.get("avance_real_pct", 0)), "%"],
        ["Desvio x obra (prom.)", _fmt_signed_pct(m.get("desvio_x_obra_prom_pct", 0)), "%"],
        ["KG fabricados", _fmt_tn(m.get("kg_real_total", 0)), "tn"],
        ["KG/HH", _fmt_ratio(m.get("kpi_kg_hh_real", 0)), "kg por hh"],
        ["Eficiencia HH", _fmt_pct(m.get("eficiencia_prev_real_pct", 0)), "%"],
    ]
    t_resumen = Table([["KPI", "Valor", "Unidad"]] + resumen_rows, colWidths=[80 * mm, 50 * mm, 52 * mm])
    t_resumen.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#FDBA74")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#FED7AA")),
                ("FONTNAME", (1, 1), (1, -1), "Helvetica-Bold"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ]
        )
    )
    elements.append(t_resumen)

    elements.append(Paragraph("2. Tendencias", s_h2))
    trend_rows = [[r["semana"], _fmt_pct(r["avance_pct"]), _fmt_kg(r["kg"]), _fmt_hs(r["hh"]), _fmt_ratio(r["kg_hh"])] for r in weekly_trend[:10]]
    if not trend_rows:
        trend_rows = [["-", "0.0%", "0", "0", "0.00"]]
    t_trend = Table([["Semana", "Avance semanal", "KG/semana", "HH/semana", "KG/HH"]] + trend_rows, colWidths=[30 * mm, 36 * mm, 35 * mm, 35 * mm, 46 * mm])
    t_trend.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#FDBA74")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#FED7AA")),
                ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#FFF7ED")]),
            ]
        )
    )
    elements.append(t_trend)

    elements.append(Paragraph("3. Produccion", s_h2))
    criticas = data.get("ot_criticas", [])
    bottlenecks = data.get("cuellos_botella", [])
    ranking = data.get("ranking_productividad", [])
    crit_rows = [[c["ot"], c["obra"], _fmt_signed_pct(c["desvio_pct"])] for c in criticas[:6]]
    if not crit_rows:
        crit_rows = [["-", "Sin OTs criticas", "0.0%"]]
    t_crit = Table([["OT", "Obra", "Desvio"]] + crit_rows, colWidths=[22 * mm, 118 * mm, 42 * mm])
    t_crit.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#FDBA74")), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white), ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#FED7AA")), ("ALIGN", (2, 1), (2, -1), "RIGHT")]))
    elements.append(Paragraph("OT criticas", s_norm))
    elements.append(t_crit)
    elements.append(Spacer(1, 1.6 * mm))

    if bottlenecks:
        btxt = ", ".join([f"{b['proceso']} ({_fmt_pct(b['ratio_pct'])})" for b in bottlenecks[:2]])
    else:
        btxt = "Sin cuellos detectados"
    elements.append(Paragraph(f"Cuellos de botella: {html_lib.escape(btxt)}", s_norm))
    elements.append(Spacer(1, 1.2 * mm))

    rank_rows = [[str(i + 1), r["obra"], _fmt_ratio(r["kg_hh"]) ] for i, r in enumerate(ranking[:8])]
    if not rank_rows:
        rank_rows = [["-", "Sin ranking", "0.00"]]
    t_rank = Table([["#", "Obra", "KG/HH"]] + rank_rows, colWidths=[14 * mm, 128 * mm, 40 * mm])
    t_rank.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#FDBA74")), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white), ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#FED7AA")), ("ALIGN", (0, 1), (0, -1), "CENTER"), ("ALIGN", (2, 1), (2, -1), "RIGHT")]))
    elements.append(Paragraph("Ranking de productividad", s_norm))
    elements.append(t_rank)

    elements.append(Paragraph("4. Calidad", s_h2))
    cal_rows = [
        ["NC abiertas", str(int(m.get("nc_abiertas", 0)))],
        ["Retrabajos", str(int(m.get("retrabajos", 0)))],
        ["Impacto HH", _fmt_hs(m.get("hh_perdidas", 0))],
        ["Indice de calidad", _fmt_pct(m.get("indice_calidad", 0))],
    ]
    t_cal = Table([["KPI", "Valor"]] + cal_rows, colWidths=[92 * mm, 90 * mm])
    t_cal.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#FDBA74")), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white), ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#FED7AA")), ("FONTNAME", (1, 1), (1, -1), "Helvetica-Bold")]))
    elements.append(t_cal)

    elements.append(Paragraph("5. Eficiencia economica (planteado)", s_h2))
    elements.append(Paragraph("Pendiente de integracion de costos (HH, retrabajos, desperdicio y logistica) para calcular impacto economico por OT y obra.", s_norm))

    if nc_items:
        elements.append(Spacer(1, 1.5 * mm))
        nc_rows = [[proc, str(int(cnt))] for proc, cnt in nc_items[:8]]
        t_nc = Table([["NC por proceso", "Cantidad"]] + nc_rows, colWidths=[140 * mm, 42 * mm])
        t_nc.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#FDBA74")), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white), ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("GRID", (0, 0), (-1, -1), 0.35, colors.HexColor("#FED7AA")), ("ALIGN", (1, 1), (1, -1), "RIGHT")]))
        elements.append(t_nc)

    if weekly:
        elements.append(Spacer(1, 1.5 * mm))
        top_weekly = weekly[:25]
        rows_weekly = [[r["semana"], r["obra"], r["tipo"], _fmt_kg(r["kg"]), _fmt_hs(r["hh"]), _fmt_ratio(r["kg_hh"])] for r in top_weekly]
        t_weekly = Table([["Semana", "Obra", "Tipo", "KG", "HH", "KG/HH"]] + rows_weekly, colWidths=[20 * mm, 48 * mm, 40 * mm, 24 * mm, 24 * mm, 26 * mm])
        t_weekly.setStyle(TableStyle([("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#FDBA74")), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white), ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("GRID", (0, 0), (-1, -1), 0.3, colors.HexColor("#FED7AA")), ("ALIGN", (3, 1), (-1, -1), "RIGHT"), ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#FFF7ED")]), ("FONTSIZE", (0, 0), (-1, -1), 8.2)]))
        elements.append(t_weekly)

    doc.build(elements)
    buffer.seek(0)
    return buffer


def _fetch_dashboard_data(db, obra_filter, tipo_filter):
    today_d = date.today()

    ots_rows = db.execute(
        """
        SELECT id,
               COALESCE(obra, ''),
               COALESCE(titulo, ''),
               COALESCE(fecha_entrega, ''),
               COALESCE(fecha_creacion, ''),
               COALESCE(estado, ''),
               COALESCE(estado_avance, 0),
               COALESCE(hs_previstas, 0),
               COALESCE(tipo_estructura, '')
        FROM ordenes_trabajo
        WHERE (fecha_cierre IS NULL OR TRIM(COALESCE(fecha_cierre, '')) = '')
          AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
        ORDER BY id ASC
        """
    ).fetchall()

    ot_ids = []
    ots = {}
    obras = set()
    tipos = set()
    for row in ots_rows:
        ot_id = int(row[0] or 0)
        obra = str(row[1] or "").strip()
        tipo = str(row[8] or "").strip()
        if obra_filter and obra.lower() != obra_filter.lower():
            continue
        if tipo_filter and tipo.lower() != tipo_filter.lower():
            continue
        ot_ids.append(ot_id)
        obras.add(obra)
        tipos.add(tipo)
        ots[ot_id] = {
            "obra": obra,
            "titulo": str(row[2] or ""),
            "fecha_entrega": _to_date(row[3]),
            "fecha_creacion": _to_date(row[4]),
            "estado": str(row[5] or ""),
            "estado_avance": _to_float(row[6]),
            "hs_previstas": _to_float(row[7]),
            "tipo_estructura": tipo,
        }

    if not ot_ids:
        return {
            "obras": sorted([o for o in obras if o]),
            "tipos": sorted([t for t in tipos if t]),
            "ots": {},
            "ot_ids": [],
            "metrics": {},
            "weekly_compare": [],
            "weekly_trend": [],
            "trend_real": [],
            "trend_programado": [],
            "nc_por_proceso": [],
            "ot_criticas": [],
            "cuellos_botella": [],
            "ranking_productividad": [],
        }

    ph = ",".join("?" * len(ot_ids))
    ok_tuple = tuple(_OK_ESTADOS)
    ok_ph = ",".join("?" * len(ok_tuple))

    base_rows = db.execute(
        f"""
        SELECT ot_id,
               TRIM(COALESCE(posicion, '')) AS pos,
               MAX(COALESCE(cantidad, 1)) AS cant,
               MAX(COALESCE(peso, 0)) AS peso
        FROM procesos
        WHERE ot_id IN ({ph})
          AND eliminado = 0
          AND TRIM(COALESCE(posicion, '')) != ''
        GROUP BY ot_id, TRIM(COALESCE(posicion, ''))
        """,
        ot_ids,
    ).fetchall()

    kg_total_by_ot = {}
    total_piezas = 0
    for ot_id, _pos, cant, peso in base_rows:
        total_piezas += 1
        oid = int(ot_id)
        kg_total_by_ot.setdefault(oid, 0.0)
        kg_total_by_ot[oid] += max(0.0, _to_float(cant, 1.0) * _to_float(peso, 0.0))

    kg_real_rows = db.execute(
        f"""
        SELECT ot_id, TRIM(COALESCE(posicion, '')), MAX(COALESCE(cantidad, 1)), MAX(COALESCE(peso, 0))
        FROM procesos
        WHERE ot_id IN ({ph})
          AND proceso = 'ARMADO'
          AND UPPER(TRIM(COALESCE(estado, ''))) IN ({ok_ph})
          AND eliminado = 0
          AND TRIM(COALESCE(posicion, '')) != ''
        GROUP BY ot_id, TRIM(COALESCE(posicion, ''))
        """,
        ot_ids + list(ok_tuple),
    ).fetchall()

    kg_real_by_ot = {}
    for ot_id, _pos, cant, peso in kg_real_rows:
        oid = int(ot_id)
        kg_real_by_ot.setdefault(oid, 0.0)
        kg_real_by_ot[oid] += max(0.0, _to_float(cant, 1.0) * _to_float(peso, 0.0))

    kg_desp_rows = db.execute(
        f"""
        SELECT ot_id, TRIM(COALESCE(posicion, '')), MAX(COALESCE(cantidad, 1)), MAX(COALESCE(peso, 0))
        FROM procesos
        WHERE ot_id IN ({ph})
          AND proceso = 'DESPACHO'
          AND UPPER(TRIM(COALESCE(estado, ''))) IN ({ok_ph})
          AND eliminado = 0
          AND TRIM(COALESCE(posicion, '')) != ''
        GROUP BY ot_id, TRIM(COALESCE(posicion, ''))
        """,
        ot_ids + list(ok_tuple),
    ).fetchall()

    kg_desp_by_ot = {}
    for ot_id, _pos, cant, peso in kg_desp_rows:
        oid = int(ot_id)
        kg_desp_by_ot.setdefault(oid, 0.0)
        kg_desp_by_ot[oid] += max(0.0, _to_float(cant, 1.0) * _to_float(peso, 0.0))

    prog_rows = db.execute(
        f"""
        SELECT id, ot_id, COALESCE(fecha_inicio, ''), COALESCE(fecha_fin, ''), COALESCE(hs_programadas, 0)
        FROM programacion
        WHERE ot_id IN ({ph})
        ORDER BY fecha_inicio ASC, id ASC
        """,
        ot_ids,
    ).fetchall()

    prog_by_ot = {}
    for prog_id, ot_id, f_ini, f_fin, hs_prog in prog_rows:
        oid = int(ot_id)
        prog_by_ot.setdefault(oid, []).append(
            {"id": int(prog_id or 0), "ini": _to_date(f_ini), "fin": _to_date(f_fin), "hs": max(0.0, _to_float(hs_prog))}
        )

    hh_real_rows = db.execute(
        f"""
        SELECT ot_id, COALESCE(fecha, ''), COALESCE(horas, 0)
        FROM partes_trabajo
        WHERE ot_id IN ({ph})
        """,
        ot_ids,
    ).fetchall()

    hh_real_total = 0.0
    hh_by_week_obra_tipo = {}
    hh_by_ot = {}
    for ot_id, fecha_txt, horas in hh_real_rows:
        oid = int(ot_id or 0)
        if oid not in ots:
            continue
        horas_f = max(0.0, _to_float(horas))
        hh_real_total += horas_f
        hh_by_ot[oid] = hh_by_ot.get(oid, 0.0) + horas_f
        d = _to_date(fecha_txt)
        if not d:
            continue
        y, w, _ = d.isocalendar()
        obra = ots[oid]["obra"]
        tipo = ots[oid]["tipo_estructura"]
        key = (int(y), int(w), obra, tipo)
        hh_by_week_obra_tipo[key] = hh_by_week_obra_tipo.get(key, 0.0) + horas_f

    hs_prev_total = sum(max(0.0, v.get("hs_previstas", 0.0)) for v in ots.values())
    hs_prog_total = sum(max(0.0, item["hs"]) for plist in prog_by_ot.values() for item in plist)

    kg_total_prev = sum(kg_total_by_ot.get(oid, 0.0) for oid in ot_ids)
    kg_real_total = sum(kg_real_by_ot.get(oid, 0.0) for oid in ot_ids)
    kg_desp_total = sum(kg_desp_by_ot.get(oid, 0.0) for oid in ot_ids)
    kg_pend_total = max(0.0, kg_total_prev - kg_desp_total)

    avance_real_pct = _pct(kg_real_total, kg_total_prev)

    expected_num = 0.0
    expected_den = 0.0
    expected_by_ot = {}
    for oid in ot_ids:
        plist = prog_by_ot.get(oid, [])
        if not plist:
            expected_by_ot[oid] = 0.0
            continue
        num_ot = 0.0
        den_ot = 0.0
        for item in plist:
            weight = item["hs"] if item["hs"] > 0 else 1.0
            p = _progress_for_period(item["ini"], item["fin"], today_d)
            num_ot += weight * p
            den_ot += weight
        expected_ot = (num_ot / den_ot) if den_ot > 0 else 0.0
        expected_by_ot[oid] = expected_ot * 100.0
        expected_num += num_ot
        expected_den += den_ot

    avance_programado_pct = (expected_num / expected_den * 100.0) if expected_den > 0 else 0.0
    desvio_avance = avance_real_pct - avance_programado_pct

    avance_previsto_pct = 0.0
    if ot_ids:
        avance_previsto_pct = sum(max(0.0, min(100.0, ots[oid]["estado_avance"])) for oid in ot_ids) / len(ot_ids)

    # Desvio por obra
    obra_vals = {}
    for oid in ot_ids:
        obra_k = ots[oid]["obra"] or "SIN_OBRA"
        real_ot = _pct(kg_real_by_ot.get(oid, 0.0), max(kg_total_by_ot.get(oid, 0.0), 1e-9))
        exp_ot = expected_by_ot.get(oid, 0.0)
        d_ot = real_ot - exp_ot
        obra_vals.setdefault(obra_k, []).append(d_ot)
    desvio_por_obra = []
    for obra_k, vals in obra_vals.items():
        prom = sum(vals) / len(vals) if vals else 0.0
        desvio_por_obra.append({"obra": obra_k, "desvio_pct": prom})
    desvio_por_obra.sort(key=lambda x: abs(x["desvio_pct"]), reverse=True)
    desvio_x_obra_prom_pct = (sum(v["desvio_pct"] for v in desvio_por_obra) / len(desvio_por_obra)) if desvio_por_obra else 0.0

    # OTs atrasadas y criticas
    ots_atrasadas = 0
    ot_criticas = []
    for oid in ot_ids:
        real_ot = _pct(kg_real_by_ot.get(oid, 0.0), max(kg_total_by_ot.get(oid, 0.0), 1e-9))
        expected_ot = expected_by_ot.get(oid, 0.0)
        desvio_ot = real_ot - expected_ot
        fecha_entrega = ots[oid].get("fecha_entrega")
        atrasada_por_fecha = bool(fecha_entrega and fecha_entrega < today_d and real_ot < 99.9)
        atrasada_por_desvio = expected_ot > 0 and (real_ot + 5.0) < expected_ot
        if atrasada_por_fecha or atrasada_por_desvio:
            ots_atrasadas += 1
            ot_criticas.append({"ot": oid, "obra": ots[oid]["obra"], "titulo": ots[oid]["titulo"], "desvio_pct": desvio_ot})
    ot_criticas.sort(key=lambda x: x["desvio_pct"])  # mas negativo primero

    week_starts = _iso_week_range(today_d, weeks_back=10)
    week_bounds = []
    for ws in week_starts:
        we = ws + timedelta(days=6)
        y, w, _ = ws.isocalendar()
        week_bounds.append((ws, we, int(y), int(w), _semana_label(y, w)))

    real_kg_by_week = {(y, w): 0.0 for _, _, y, w, _ in week_bounds}
    hh_by_week = {(y, w): 0.0 for _, _, y, w, _ in week_bounds}

    real_rows_by_date = db.execute(
        f"""
        SELECT ot_id, COALESCE(fecha, ''), TRIM(COALESCE(posicion, '')), MAX(COALESCE(cantidad, 1)), MAX(COALESCE(peso, 0))
        FROM procesos
        WHERE ot_id IN ({ph})
          AND proceso = 'ARMADO'
          AND UPPER(TRIM(COALESCE(estado, ''))) IN ({ok_ph})
          AND eliminado = 0
          AND TRIM(COALESCE(posicion, '')) != ''
        GROUP BY ot_id, COALESCE(fecha, ''), TRIM(COALESCE(posicion, ''))
        """,
        ot_ids + list(ok_tuple),
    ).fetchall()
    for _ot_id, ftxt, _pos, cant, peso in real_rows_by_date:
        d = _to_date(ftxt)
        if not d:
            continue
        y, w, _ = d.isocalendar()
        key = (int(y), int(w))
        if key in real_kg_by_week:
            real_kg_by_week[key] += max(0.0, _to_float(cant, 1.0) * _to_float(peso, 0.0))

    for _ot_id, fecha_txt, horas in hh_real_rows:
        d = _to_date(fecha_txt)
        if not d:
            continue
        y, w, _ = d.isocalendar()
        key = (int(y), int(w))
        if key in hh_by_week:
            hh_by_week[key] += max(0.0, _to_float(horas))

    # Programado semanal en kg aproximado
    prog_kg_by_week = {(y, w): 0.0 for _, _, y, w, _ in week_bounds}
    for oid in ot_ids:
        total_kg_ot = max(0.0, kg_total_by_ot.get(oid, 0.0))
        plist = prog_by_ot.get(oid, [])
        if not plist:
            continue
        total_hs_ot = sum(item["hs"] for item in plist)
        if total_hs_ot <= 0:
            total_hs_ot = float(len(plist))
        for item in plist:
            ini = item["ini"]
            fin = item["fin"]
            if not ini or not fin:
                continue
            if fin < ini:
                fin = ini
            dur_total = (fin - ini).days + 1
            if dur_total <= 0:
                continue
            weight = item["hs"] if item["hs"] > 0 else 1.0
            kg_task = total_kg_ot * (weight / total_hs_ot)
            kg_dia = kg_task / dur_total
            for ws, we, y, w, _lbl in week_bounds:
                overlap_start = max(ini, ws)
                overlap_end = min(fin, we)
                if overlap_end < overlap_start:
                    continue
                overlap_days = (overlap_end - overlap_start).days + 1
                prog_kg_by_week[(y, w)] += kg_dia * overlap_days

    trend_real = []
    trend_programado = []
    weekly_trend = []
    for _ws, _we, y, w, lbl in week_bounds:
        kg_w = real_kg_by_week.get((y, w), 0.0)
        hh_w = hh_by_week.get((y, w), 0.0)
        trend_real.append((lbl, kg_w))
        trend_programado.append((lbl, prog_kg_by_week.get((y, w), 0.0)))
        weekly_trend.append(
            {
                "semana": lbl,
                "kg": kg_w,
                "hh": hh_w,
                "kg_hh": (kg_w / hh_w) if hh_w > 0 else 0.0,
                "avance_pct": _pct(kg_w, kg_total_prev),
            }
        )
    weekly_trend = list(reversed(weekly_trend))

    kg_by_week_obra_tipo = {}
    for _ot_id, ftxt, _pos, cant, peso in real_rows_by_date:
        oid = int(_ot_id or 0)
        if oid not in ots:
            continue
        d = _to_date(ftxt)
        if not d:
            continue
        y, w, _ = d.isocalendar()
        obra = ots[oid]["obra"]
        tipo = ots[oid]["tipo_estructura"]
        key = (int(y), int(w), obra, tipo)
        kg_by_week_obra_tipo[key] = kg_by_week_obra_tipo.get(key, 0.0) + max(0.0, _to_float(cant, 1.0) * _to_float(peso, 0.0))

    weekly_keys = sorted(set(kg_by_week_obra_tipo.keys()) | set(hh_by_week_obra_tipo.keys()), reverse=True)
    weekly_compare = []
    for y, w, obra, tipo in weekly_keys:
        kg = kg_by_week_obra_tipo.get((y, w, obra, tipo), 0.0)
        hh = hh_by_week_obra_tipo.get((y, w, obra, tipo), 0.0)
        ratio = (kg / hh) if hh > 0 else 0.0
        weekly_compare.append({"semana": _semana_label(y, w), "obra": obra, "tipo": tipo, "kg": kg, "hh": hh, "kg_hh": ratio})
    weekly_compare = weekly_compare[:120]

    # Cuellos de botella por proceso (cross-db: contar distinct en Python)
    stage_map = ["ARMADO", "SOLDADURA", "PINTURA", "DESPACHO"]
    stage_ratios = []
    for stage in stage_map:
        stage_rows = db.execute(
            f"""
            SELECT ot_id, TRIM(COALESCE(posicion, ''))
            FROM procesos
            WHERE ot_id IN ({ph})
              AND UPPER(TRIM(COALESCE(proceso, ''))) = ?
              AND UPPER(TRIM(COALESCE(estado, ''))) IN ({ok_ph})
              AND eliminado = 0
              AND TRIM(COALESCE(posicion, '')) != ''
            """,
            ot_ids + [stage] + list(ok_tuple),
        ).fetchall()
        uniques = {(int(r[0] or 0), str(r[1] or "").strip()) for r in stage_rows if str(r[1] or "").strip()}
        value = len(uniques)
        stage_ratios.append({"proceso": stage, "ok": value, "ratio_pct": _pct(value, total_piezas)})
    cuellos_botella = sorted(stage_ratios, key=lambda x: x["ratio_pct"])

    # Ranking productividad por obra
    prod_by_obra = {}
    for oid in ot_ids:
        obra_k = ots[oid]["obra"] or "SIN_OBRA"
        prod_by_obra.setdefault(obra_k, {"kg": 0.0, "hh": 0.0})
        prod_by_obra[obra_k]["kg"] += kg_real_by_ot.get(oid, 0.0)
        prod_by_obra[obra_k]["hh"] += hh_by_ot.get(oid, 0.0)
    ranking_productividad = []
    for obra_k, vals in prod_by_obra.items():
        hh = vals["hh"]
        ratio = vals["kg"] / hh if hh > 0 else 0.0
        ranking_productividad.append({"obra": obra_k, "kg": vals["kg"], "hh": hh, "kg_hh": ratio})
    ranking_productividad.sort(key=lambda x: x["kg_hh"], reverse=True)

    # Calidad
    hall_rows = db.execute(
        """
        SELECT COALESCE(proceso, ''), UPPER(TRIM(COALESCE(tipo_hallazgo, ''))),
               UPPER(TRIM(COALESCE(estado_tratamiento, ''))), COALESCE(genero_retrabajo, 0), COALESCE(retrabajo_hs, 0)
        FROM hallazgos_calidad
        """
    ).fetchall()

    nc_abiertas = 0
    nc_cerradas = 0
    retrabajos = 0
    hh_perdidas = 0.0
    total_hallazgos = 0
    nc_total = 0
    nc_por_proceso = {}
    for proceso, tipo_h, estado_t, gen_retr, retr_hs in hall_rows:
        total_hallazgos += 1
        proceso_up = str(proceso or "").strip().upper() or "SIN_PROCESO"
        tipo_up = str(tipo_h or "").strip().upper()
        estado_up = str(estado_t or "").strip().upper()
        if tipo_up == "NC":
            nc_total += 1
            nc_por_proceso[proceso_up] = nc_por_proceso.get(proceso_up, 0) + 1
            if estado_up == "CERRADA":
                nc_cerradas += 1
            elif estado_up in ("ABIERTO", "EN PROCESO", ""):
                nc_abiertas += 1
        if int(_to_float(gen_retr, 0)) == 1:
            retrabajos += 1
            hh_perdidas += max(0.0, _to_float(retr_hs))

    indice_calidad = 100.0
    if total_hallazgos > 0:
        indice_calidad = max(0.0, (1.0 - (nc_total / float(total_hallazgos))) * 100.0)
    nc_por_proceso_items = sorted(nc_por_proceso.items(), key=lambda x: x[1], reverse=True)

    # Alertas automaticas
    alertas = []
    if avance_real_pct > avance_programado_pct:
        alertas.append("REAL > PROGRAMADO - ATRASO OPERATIVO")
    if kg_real_total > kg_total_prev:
        alertas.append("REAL > PREVISTO - PERDIDA ECONOMICA")
    if hs_prog_total > hs_prev_total:
        alertas.append("PROGRAMADO > PREVISTO - MALA PLANIFICACION")

    # Productividad semanal: comparar última semana vs promedio 4 anteriores.
    series_kg_hh = [w["kg_hh"] for w in weekly_trend if w["hh"] > 0]
    if len(series_kg_hh) >= 5:
        ult = series_kg_hh[-1]
        prom_4 = sum(series_kg_hh[-5:-1]) / 4.0
        if prom_4 > 0 and ult < prom_4:
            caida = ((prom_4 - ult) / prom_4) * 100.0
            if caida >= 10:
                alertas.append(f"La productividad cayo {caida:.0f}% respecto a la media de las ultimas 4 semanas.")

    if hh_real_total > 0 and hs_prev_total > 0 and hh_real_total > hs_prev_total * 1.15:
        extra_hh = hh_real_total - hs_prev_total
        alertas.append(f"Consumo de HH por encima del previsto (+{extra_hh:.1f} hh).")

    if not alertas:
        alertas.append("Sin desvíos críticos detectados en este corte")

    metrics = {
        "avance_real_pct": avance_real_pct,
        "avance_programado_pct": avance_programado_pct,
        "avance_previsto_pct": avance_previsto_pct,
        "desvio_avance": desvio_avance,
        "desvio_x_obra_prom_pct": desvio_x_obra_prom_pct,
        "ots_atrasadas": ots_atrasadas,
        "hs_prev_total": hs_prev_total,
        "hs_prog_total": hs_prog_total,
        "hh_real_total": hh_real_total,
        "desvio_hs_real_prev": hh_real_total - hs_prev_total,
        "eficiencia_prev_real_pct": _pct(hs_prev_total, hh_real_total) if hh_real_total > 0 else 0.0,
        "kpi_kg_hh_real": (kg_real_total / hh_real_total) if hh_real_total > 0 else 0.0,
        "kg_total_prev": kg_total_prev,
        "kg_real_total": kg_real_total,
        "kg_desp_total": kg_desp_total,
        "kg_pend_total": kg_pend_total,
        "ritmo_kg_semana": (sum(v for _, v in trend_real[-4:]) / 4.0) if trend_real else 0.0,
        "nc_abiertas": nc_abiertas,
        "nc_cerradas": nc_cerradas,
        "retrabajos": retrabajos,
        "hh_perdidas": hh_perdidas,
        "indice_calidad": indice_calidad,
        "alertas": alertas,
    }

    return {
        "obras": sorted([o for o in obras if o]),
        "tipos": sorted([t for t in tipos if t]),
        "ots": ots,
        "ot_ids": ot_ids,
        "metrics": metrics,
        "weekly_compare": weekly_compare,
        "weekly_trend": weekly_trend,
        "trend_real": trend_real,
        "trend_programado": trend_programado,
        "nc_por_proceso": nc_por_proceso_items,
        "ot_criticas": ot_criticas,
        "cuellos_botella": cuellos_botella,
        "ranking_productividad": ranking_productividad,
        "desvio_por_obra": desvio_por_obra,
    }


@tablero_ejecutivo_bp.route("/modulo/tablero-ejecutivo")
def tablero_ejecutivo_integral():
    role = str(session.get("user_role") or "").strip().lower()
    if role != "administrador":
        return (
            "<h3 style='font-family:Arial;padding:16px;color:#991b1b;'>Sin permisos para acceder al Tablero Ejecutivo Integral.</h3>",
            403,
        )

    obra = (request.args.get("obra") or "").strip()
    tipo = (request.args.get("tipo") or "").strip()

    db = get_db()
    data = _fetch_dashboard_data(db, obra, tipo)
    m = data.get("metrics", {})

    obras_opts = ['<option value="">Todas</option>']
    for item in data.get("obras", []):
        sel = " selected" if obra and item.lower() == obra.lower() else ""
        obras_opts.append(f'<option value="{_e(item)}"{sel}>{_e(item)}</option>')

    tipos_opts = ['<option value="">Todos</option>']
    for item in data.get("tipos", []):
        sel = " selected" if tipo and item.lower() == tipo.lower() else ""
        tipos_opts.append(f'<option value="{_e(item)}"{sel}>{_e(item)}</option>')

    trend_svg = _build_weekly_trend_svg(data.get("trend_real", []), data.get("trend_programado", []))

    nc_items = data.get("nc_por_proceso", [])
    if nc_items:
        nc_html = "".join(
            f"<tr><td>{_e(proc)}</td><td style='text-align:right;font-weight:700;'>{int(cnt)}</td></tr>" for proc, cnt in nc_items
        )
    else:
        nc_html = "<tr><td colspan='2' style='color:#64748b;'>Sin datos de NC por proceso</td></tr>"

    rows_kpi = []
    for row in data.get("weekly_compare", []):
        rows_kpi.append(
            "<tr>"
            f"<td>{_e(row['semana'])}</td>"
            f"<td>{_e(row['obra'])}</td>"
            f"<td>{_e(row['tipo'])}</td>"
            f"<td style='text-align:right'>{_fmt_kg(row['kg'])}</td>"
            f"<td style='text-align:right'>{_fmt_hs(row['hh'])}</td>"
            f"<td style='text-align:right;font-weight:800'>{_fmt_ratio(row['kg_hh'])}</td>"
            "</tr>"
        )

    if not rows_kpi:
        rows_kpi.append("<tr><td colspan='6' style='color:#9a3412;'>Sin datos de productividad para el filtro actual</td></tr>")

    trend_rows = []
    for row in data.get("weekly_trend", []):
        trend_rows.append(
            "<tr>"
            f"<td>{_e(row['semana'])}</td>"
            f"<td style='text-align:right'>{_fmt_pct(row['avance_pct'])}</td>"
            f"<td style='text-align:right'>{_fmt_kg(row['kg'])}</td>"
            f"<td style='text-align:right'>{_fmt_hs(row['hh'])}</td>"
            f"<td style='text-align:right;font-weight:800'>{_fmt_ratio(row['kg_hh'])}</td>"
            "</tr>"
        )
    if not trend_rows:
        trend_rows.append("<tr><td colspan='5' style='color:#9a3412;'>Sin tendencia semanal disponible</td></tr>")

    desvio_rows = []
    for row in data.get("desvio_por_obra", [])[:8]:
        desvio_rows.append(
            "<tr>"
            f"<td>{_e(row['obra'])}</td>"
            f"<td style='text-align:right;font-weight:800'>{_fmt_signed_pct(row['desvio_pct'])}</td>"
            "</tr>"
        )
    if not desvio_rows:
        desvio_rows.append("<tr><td colspan='2' style='color:#9a3412;'>Sin desvíos por obra</td></tr>")

    crit_rows = []
    for row in data.get("ot_criticas", [])[:8]:
        crit_rows.append(
            "<tr>"
            f"<td>{int(row['ot'])}</td>"
            f"<td>{_e(row['obra'])}</td>"
            f"<td>{_e(row['titulo'])}</td>"
            f"<td style='text-align:right;font-weight:800'>{_fmt_signed_pct(row['desvio_pct'])}</td>"
            "</tr>"
        )
    if not crit_rows:
        crit_rows.append("<tr><td colspan='4' style='color:#9a3412;'>Sin OTs críticas detectadas</td></tr>")

    cuello_rows = []
    for row in data.get("cuellos_botella", [])[:4]:
        cuello_rows.append(
            "<tr>"
            f"<td>{_e(row['proceso'])}</td>"
            f"<td style='text-align:right'>{int(row['ok'])}</td>"
            f"<td style='text-align:right;font-weight:800'>{_fmt_pct(row['ratio_pct'])}</td>"
            "</tr>"
        )
    if not cuello_rows:
        cuello_rows.append("<tr><td colspan='3' style='color:#9a3412;'>Sin información de procesos</td></tr>")

    ranking_rows = []
    for i, row in enumerate(data.get("ranking_productividad", [])[:8], start=1):
        ranking_rows.append(
            "<tr>"
            f"<td>{i}</td>"
            f"<td>{_e(row['obra'])}</td>"
            f"<td style='text-align:right'>{_fmt_kg(row['kg'])}</td>"
            f"<td style='text-align:right'>{_fmt_hs(row['hh'])}</td>"
            f"<td style='text-align:right;font-weight:800'>{_fmt_ratio(row['kg_hh'])}</td>"
            "</tr>"
        )
    if not ranking_rows:
        ranking_rows.append("<tr><td colspan='5' style='color:#9a3412;'>Sin ranking de productividad</td></tr>")

    alertas_html = "".join(
        f"<span class='alert-chip'>{_e(msg)}</span>" for msg in m.get("alertas", [])
    )

    qs_pdf = urlencode({"obra": obra, "tipo": tipo})
    pdf_href = f"/modulo/tablero-ejecutivo/export.pdf?{qs_pdf}"

    return f"""
    <html>
    <head>
      <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
      <title>Tablero Ejecutivo Integral</title>
      <style>
        * {{ box-sizing: border-box; }}
        body {{
          margin: 0;
          font-family: 'Segoe UI', Tahoma, Arial, sans-serif;
          background:
            radial-gradient(circle at 12% 8%, rgba(251,146,60,0.22), transparent 34%),
            radial-gradient(circle at 88% 12%, rgba(245,158,11,0.20), transparent 36%),
            linear-gradient(160deg, #fffaf5 0%, #ffedd5 48%, #fed7aa 100%);
          color: #7c2d12;
          padding: 16px;
        }}
        .wrap {{ max-width: 1360px; margin: 0 auto; }}
        .hero {{
          border: 1px solid #fb923c;
          background: linear-gradient(135deg, #fff7ed 0%, #ffedd5 55%, #fed7aa 100%);
          color: #7c2d12;
          border-radius: 16px;
          padding: 18px;
          box-shadow: 0 14px 28px rgba(154,52,18,0.16);
          margin-bottom: 16px;
        }}
        .hero h1 {{ margin: 0 0 8px 0; font-size: 1.65rem; }}
        .hero p {{ margin: 0; color: #9a3412; }}
        .legend {{
          display: grid;
          grid-template-columns: repeat(3, minmax(180px, 1fr));
          gap: 10px;
          margin-top: 12px;
        }}
        .legend .box {{
          border-radius: 10px;
          padding: 10px;
          border: 1px solid #fed7aa;
          font-size: 0.88rem;
          background: #ffffffc8;
        }}
        .previsto {{ background: #fff7ed; }}
        .programado {{ background: #ffedd5; }}
        .real {{ background: #fed7aa; }}

        .toolbar {{
          background: #ffffff;
          border: 1px solid #fed7aa;
          border-radius: 12px;
          padding: 12px;
          display: grid;
          gap: 10px;
          grid-template-columns: 1fr 1fr auto auto;
          align-items: end;
          margin-bottom: 14px;
        }}
        .toolbar label {{ font-size: 0.82rem; font-weight: 700; color: #9a3412; display: block; margin-bottom: 4px; }}
        .toolbar select {{ width: 100%; padding: 9px; border: 1px solid #fed7aa; border-radius: 8px; color: #7c2d12; }}
        .btn {{ padding: 10px 14px; border: 0; border-radius: 8px; cursor: pointer; font-weight: 700; }}
        .btn.primary {{ background: #f97316; color: #fff; }}
        .btn.back {{ background: #9a3412; color: #fff; text-decoration: none; display: inline-flex; align-items: center; }}
        .btn.pdf {{ background: #f59e0b; color: #fff; text-decoration: none; display: inline-flex; align-items: center; }}

        .kpis {{
          display: grid;
          grid-template-columns: repeat(4, minmax(180px, 1fr));
          gap: 10px;
          margin-bottom: 12px;
        }}
        .kpi {{
          background: #fff;
          border: 1px solid #fed7aa;
          border-radius: 12px;
          padding: 12px;
        }}
        .kpi small {{ color: #9a3412; display: block; margin-bottom: 6px; }}
        .kpi b {{ font-size: 1.35rem; }}

        .grid {{
          display: grid;
          gap: 12px;
          grid-template-columns: 1.25fr 1fr;
          margin-bottom: 12px;
        }}
        .card {{ background: #fff; border: 1px solid #fed7aa; border-radius: 12px; padding: 12px; }}
        .card h3 {{ margin: 0 0 10px 0; font-size: 1.03rem; }}

        table {{ width: 100%; border-collapse: collapse; }}
        th, td {{ padding: 8px; border-bottom: 1px solid #fed7aa; text-align: left; font-size: 0.9rem; }}
        th {{ background: #fff7ed; color: #9a3412; }}

        .alerts {{ display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 12px; }}
        .alert-chip {{
          display: inline-block;
          background: #fff7ed;
          color: #9a3412;
          border: 1px solid #fdba74;
          border-radius: 999px;
          padding: 7px 11px;
          font-size: 0.82rem;
          font-weight: 700;
        }}

        .section-title {{ margin: 16px 0 8px 0; color: #7c2d12; font-size: 1.05rem; }}

        @media (max-width: 1060px) {{
          .toolbar {{ grid-template-columns: 1fr 1fr; }}
          .kpis {{ grid-template-columns: repeat(2, minmax(180px, 1fr)); }}
          .grid {{ grid-template-columns: 1fr; }}
        }}
        @media (max-width: 680px) {{
          .legend {{ grid-template-columns: 1fr; }}
          .toolbar {{ grid-template-columns: 1fr; }}
          .kpis {{ grid-template-columns: 1fr; }}
          .hero h1 {{ font-size: 1.3rem; }}
        }}
      </style>
    </head>
    <body>
      <div class=\"wrap\">
        <div class=\"hero\">
          <h1>Tablero Ejecutivo Integral</h1>
          <p>Diferenciación operativa: PREVISTO (OT) vs PROGRAMADO (Planificación) vs REAL (Ejecución)</p>
          <div class=\"legend\">
            <div class=\"box previsto\"><b>PREVISTO</b><br>Fuente: módulo OT (kg y hs base).</div>
            <div class=\"box programado\"><b>PROGRAMADO</b><br>Fuente: módulo Programación (fechas y hs planificadas).</div>
            <div class=\"box real\"><b>REAL</b><br>Fuente: Producción, Despacho y Parte semanal.</div>
          </div>
        </div>

                <form class="toolbar" method="get">
          <div>
            <label>Obra</label>
            <select name=\"obra\">{''.join(obras_opts)}</select>
          </div>
          <div>
            <label>Tipo de estructura</label>
            <select name=\"tipo\">{''.join(tipos_opts)}</select>
          </div>
          <button class=\"btn primary\" type=\"submit\">Aplicar filtros</button>
                    <a class="btn pdf" href="{pdf_href}">Exportar PDF</a>
                    <a class="btn back" href="/">Volver</a>
        </form>

        <div class=\"alerts\">{alertas_html}</div>

        <h3 class=\"section-title\">1. Avance y planificación</h3>
        <div class=\"kpis\">
          <div class=\"kpi\"><small>Avance REAL</small><b>{_fmt_pct(m.get('avance_real_pct', 0))}</b><div>kg fabricados / kg totales</div></div>
          <div class=\"kpi\"><small>Avance ESPERADO (cronograma)</small><b>{_fmt_pct(m.get('avance_programado_pct', 0))}</b><div>según programación</div></div>
          <div class=\"kpi\"><small>Desvío avance (REAL - ESPERADO)</small><b>{_fmt_signed_pct(m.get('desvio_avance', 0))}</b><div>puntos porcentuales</div></div>
          <div class=\"kpi\"><small>Desvio x obra (prom.)</small><b>{_fmt_signed_pct(m.get('desvio_x_obra_prom_pct', 0))}</b><div>promedio de obras</div></div>
          <div class=\"kpi\"><small>OTs atrasadas</small><b>{int(m.get('ots_atrasadas', 0))}</b><div>cantidad</div></div>
        </div>

        <div class=\"card\">
          <h3>Tendencia temporal (kg/semana)</h3>
                    <div style=\"display:flex;gap:12px;align-items:center;margin-bottom:6px;font-size:0.86rem;color:#9a3412;\">
                        <span style=\"display:inline-flex;align-items:center;gap:6px;\"><span style=\"width:20px;height:3px;background:#f59e0b;display:inline-block;\"></span>Programado</span>
                        <span style=\"display:inline-flex;align-items:center;gap:6px;\"><span style=\"width:20px;height:3px;background:#f97316;display:inline-block;\"></span>Real</span>
          </div>
          {trend_svg}
        </div>

        <h3 class=\"section-title\">2. Mano de obra (HS)</h3>
        <div class=\"kpis\">
          <div class=\"kpi\"><small>HS previstas (OT)</small><b>{_fmt_hs(m.get('hs_prev_total', 0))}</b></div>
          <div class=\"kpi\"><small>HS programadas</small><b>{_fmt_hs(m.get('hs_prog_total', 0))}</b></div>
          <div class=\"kpi\"><small>HS reales (parte semanal)</small><b>{_fmt_hs(m.get('hh_real_total', 0))}</b></div>
          <div class=\"kpi\"><small>Desvío HS (REALES - PREVISTAS)</small><b>{_fmt_hs(m.get('desvio_hs_real_prev', 0))}</b></div>
        </div>
        <div class=\"kpis\" style=\"grid-template-columns:repeat(2,minmax(180px,1fr));\">
          <div class=\"kpi\"><small>% Eficiencia (PREVISTAS/REALES)</small><b>{_fmt_pct(m.get('eficiencia_prev_real_pct', 0))}</b></div>
          <div class=\"kpi\"><small>KPI KG/HH real (kg fabricados / hh reales)</small><b>{_fmt_ratio(m.get('kpi_kg_hh_real', 0))}</b></div>
        </div>

        <div class=\"card\">
          <h3>Tabla KPI KG/HH (semana a semana, obra a obra, tipo de estructura)</h3>
          <table>
            <tr>
              <th>Semana</th>
              <th>Obra</th>
              <th>Tipo</th>
              <th style=\"text-align:right;\">KG</th>
              <th style=\"text-align:right;\">HH</th>
              <th style=\"text-align:right;\">KG/HH</th>
            </tr>
            {''.join(rows_kpi)}
          </table>
        </div>

        <h3 class=\"section-title\">3. Producción (KG)</h3>
        <div class=\"kpis\">
          <div class=\"kpi\"><small>KG totales obra (previsto)</small><b>{_fmt_kg(m.get('kg_total_prev', 0))}</b></div>
          <div class=\"kpi\"><small>KG fabricados (real)</small><b>{_fmt_kg(m.get('kg_real_total', 0))}</b></div>
          <div class=\"kpi\"><small>KG despachados</small><b>{_fmt_kg(m.get('kg_desp_total', 0))}</b></div>
          <div class=\"kpi\"><small>KG pendientes</small><b>{_fmt_kg(m.get('kg_pend_total', 0))}</b></div>
        </div>
        <div class=\"kpis\" style=\"grid-template-columns:repeat(1,minmax(180px,1fr));\">
          <div class=\"kpi\"><small>Ritmo semanal</small><b>{_fmt_kg(m.get('ritmo_kg_semana', 0))} kg/semana</b></div>
        </div>

                <div class=\"grid\" style=\"margin-top:12px;\">
                    <div class=\"card\">
                        <h3>Avance semanal, KG/semana, HH/semana y KG/HH</h3>
                        <table>
                            <tr>
                                <th>Semana</th>
                                <th style=\"text-align:right;\">Avance</th>
                                <th style=\"text-align:right;\">KG/semana</th>
                                <th style=\"text-align:right;\">HH/semana</th>
                                <th style=\"text-align:right;\">KG/HH</th>
                            </tr>
                            {''.join(trend_rows)}
                        </table>
                    </div>
                    <div class=\"card\">
                        <h3>Desvío por obra</h3>
                        <table>
                            <tr><th>Obra</th><th style=\"text-align:right;\">Desvío</th></tr>
                            {''.join(desvio_rows)}
                        </table>
                    </div>
                </div>

                <div class=\"grid\" style=\"margin-top:12px;\">
                    <div class=\"card\">
                        <h3>OTs críticas</h3>
                        <table>
                            <tr><th>OT</th><th>Obra</th><th>Título</th><th style=\"text-align:right;\">Desvío</th></tr>
                            {''.join(crit_rows)}
                        </table>
                    </div>
                    <div class=\"card\">
                        <h3>Cuellos de botella</h3>
                        <table>
                            <tr><th>Proceso</th><th style=\"text-align:right;\">Posiciones OK</th><th style=\"text-align:right;\">Ratio</th></tr>
                            {''.join(cuello_rows)}
                        </table>
                    </div>
                </div>

                <div class=\"card\" style=\"margin-top:12px;\">
                    <h3>Ranking de productividad por obra</h3>
                    <table>
                        <tr><th>#</th><th>Obra</th><th style=\"text-align:right;\">KG</th><th style=\"text-align:right;\">HH</th><th style=\"text-align:right;\">KG/HH</th></tr>
                        {''.join(ranking_rows)}
                    </table>
                </div>

                <h3 class=\"section-title\">4. Calidad</h3>
        <div class=\"grid\">
          <div class=\"card\">
            <div class=\"kpis\" style=\"grid-template-columns:repeat(2,minmax(150px,1fr));\">
              <div class=\"kpi\"><small>NC abiertas</small><b>{int(m.get('nc_abiertas', 0))}</b></div>
              <div class=\"kpi\"><small>NC cerradas</small><b>{int(m.get('nc_cerradas', 0))}</b></div>
              <div class=\"kpi\"><small>Retrabajos</small><b>{int(m.get('retrabajos', 0))}</b></div>
              <div class=\"kpi\"><small>HH perdidas</small><b>{_fmt_hs(m.get('hh_perdidas', 0))}</b></div>
              <div class=\"kpi\" style=\"grid-column:1 / -1;\"><small>Indice de calidad</small><b>{_fmt_pct(m.get('indice_calidad', 0))}</b><div>1 - (NC / total hallazgos)</div></div>
            </div>
          </div>
          <div class=\"card\">
            <h3>NC por proceso</h3>
            <table>
              <tr><th>Proceso</th><th style=\"text-align:right;\">NC</th></tr>
              {nc_html}
            </table>
          </div>
        </div>

        <h3 class=\"section-title\">5. Eficiencia económica</h3>
                <div class=\"card\" style=\"border-left:4px solid #f97316;\">
                    <p style=\"margin:0;color:#7c2d12;\"><b>Planteado para siguiente etapa:</b> integrar costos de HH, retrabajos, desperdicio y logística para calcular margen operativo real por OT/obra.</p>
        </div>
      </div>
    </body>
    </html>
    """


@tablero_ejecutivo_bp.route("/modulo/tablero-ejecutivo/export.pdf")
def tablero_ejecutivo_export_pdf():
    role = str(session.get("user_role") or "").strip().lower()
    if role != "administrador":
        return (
            "<h3 style='font-family:Arial;padding:16px;color:#991b1b;'>Sin permisos para exportar el Tablero Ejecutivo.</h3>",
            403,
        )

    obra = (request.args.get("obra") or "").strip()
    tipo = (request.args.get("tipo") or "").strip()

    db = get_db()
    data = _fetch_dashboard_data(db, obra, tipo)
    pdf_buffer = _build_pdf_report(data, obra, tipo)

    suffix = []
    if obra:
        suffix.append(obra.replace(" ", "_"))
    if tipo:
        suffix.append(tipo.replace(" ", "_"))
    suf_txt = "_" + "_".join(suffix) if suffix else ""
    filename = f"tablero_ejecutivo_integral{suf_txt}.pdf"

    return send_file(
        pdf_buffer,
        mimetype="application/pdf",
        as_attachment=True,
        download_name=filename,
    )
