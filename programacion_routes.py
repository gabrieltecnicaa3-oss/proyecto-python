import html as html_lib
from datetime import datetime, timedelta, date
from calendar import monthrange
from flask import Blueprint, request, redirect
from db_utils import get_db

programacion_bp = Blueprint("programacion", __name__)

# ── Colores para identificar OTs en el Gantt ──────────────────────────────────
_COLORES_OT = [
    "#3b82f6", "#f97316", "#10b981", "#8b5cf6", "#ef4444",
    "#06b6d4", "#f59e0b", "#6366f1", "#84cc16", "#ec4899",
    "#14b8a6", "#a855f7", "#fb923c", "#22c55e", "#e11d48",
]

_DESVIOS = {
    "1": "Atraso provision de material",
    "2": "Atraso documentacion ingenieria A3",
    "3": "Tareas previas no finalizadas",
    "4": "Cambio de prioridades",
    "5": "Cambios en el proyecto",
    "6": "Mano de obra insuficiente",
    "7": "Rendimiento real mas bajo",
    "8": "Errores de programacion",
    "9": "Condiciones climaticas inadecuadas para pintura",
    "10": "Cuestiones ajenas a A3",
}


def _color_ot(ot_id):
    return _COLORES_OT[int(ot_id or 0) % len(_COLORES_OT)]


def _svg_donut_chart(stats, total, size=210):
    """Genera SVG de donut chart para distribución de causas de desvío."""
    import math
    if not stats or total <= 0:
        return (
            f'<svg viewBox="0 0 {size} {size}" style="width:{size}px;height:{size}px;">'
            f'<text x="{size//2}" y="{size//2}" text-anchor="middle" dominant-baseline="middle" '
            f'font-size="13" fill="#94a3b8" font-style="italic">Sin datos</text></svg>'
        )
    cx = cy = size / 2
    r_outer = size / 2 - 12
    r_inner = r_outer * 0.48
    colors = [
        "#6366f1", "#f97316", "#3b82f6", "#10b981", "#ef4444",
        "#8b5cf6", "#f59e0b", "#06b6d4", "#ec4899", "#14b8a6",
    ]
    slices = []
    for i, (code, label) in enumerate(_DESVIOS.items()):
        c = stats.get(code, 0)
        if c > 0:
            slices.append((code, label, c, colors[i % len(colors)]))
    paths = []
    start_angle = -90.0
    for code, label, count, color in slices:
        sweep = 360.0 * count / total
        end_angle = start_angle + sweep
        large_arc = 1 if sweep > 180 else 0
        x1 = cx + r_outer * math.cos(math.radians(start_angle))
        y1 = cy + r_outer * math.sin(math.radians(start_angle))
        x2 = cx + r_outer * math.cos(math.radians(end_angle))
        y2 = cy + r_outer * math.sin(math.radians(end_angle))
        x3 = cx + r_inner * math.cos(math.radians(end_angle))
        y3 = cy + r_inner * math.sin(math.radians(end_angle))
        x4 = cx + r_inner * math.cos(math.radians(start_angle))
        y4 = cy + r_inner * math.sin(math.radians(start_angle))
        d = (
            f"M {x1:.2f} {y1:.2f} "
            f"A {r_outer:.2f} {r_outer:.2f} 0 {large_arc} 1 {x2:.2f} {y2:.2f} "
            f"L {x3:.2f} {y3:.2f} "
            f"A {r_inner:.2f} {r_inner:.2f} 0 {large_arc} 0 {x4:.2f} {y4:.2f} Z"
        )
        paths.append(f'<path d="{d}" fill="{color}" stroke="#fff" stroke-width="1.5"/>')
        pct = count * 100.0 / total
        if pct > 7:
            mid = start_angle + sweep / 2
            lx = cx + (r_outer + r_inner) / 2 * math.cos(math.radians(mid))
            ly = cy + (r_outer + r_inner) / 2 * math.sin(math.radians(mid))
            paths.append(
                f'<text x="{lx:.1f}" y="{ly:.1f}" font-size="10" text-anchor="middle" '
                f'dominant-baseline="middle" fill="#fff" font-weight="700">{pct:.0f}%</text>'
            )
        start_angle = end_angle
    return (
        f'<svg viewBox="0 0 {size} {size}" style="width:{size}px;height:{size}px;">'
        + "".join(paths) + "</svg>"
    )


def _parse_date(s):
    if not s:
        return None
    try:
        return datetime.strptime(str(s).strip(), "%Y-%m-%d").date()
    except Exception:
        return None


def _fmt(d):
    if not d:
        return "—"
    if isinstance(d, str):
        d = _parse_date(d)
    if not d:
        return "—"
    return d.strftime("%d/%m/%Y")


# ── CSS compartido ─────────────────────────────────────────────────────────────
_CSS = """<style>
*{box-sizing:border-box;}
body{font-family:Arial,sans-serif;background:#fff7ed;padding:14px;margin:0;color:#431407;}
h2{color:#9a3412;border-bottom:3px solid #f97316;padding-bottom:10px;margin:0;}
h3{color:#9a3412;margin:0 0 12px 0;}
.hdr{display:flex;justify-content:space-between;align-items:center;margin-bottom:14px;gap:10px;flex-wrap:wrap;}
.btn{display:inline-block;background:#f97316;color:white;padding:9px 14px;text-decoration:none;border-radius:6px;font-weight:bold;border:none;cursor:pointer;font-size:14px;}
.btn:hover{background:#ea580c;}
.btn-sec{background:#fff;color:#9a3412;border:1px solid #fdba74;}
.btn-sec:hover{background:#fff7ed;}
.btn-danger{background:#dc2626;color:#fff;}
.btn-danger:hover{background:#b91c1c;}
.btn-sm{padding:5px 10px;font-size:12px;}
.panel{background:#fff;border:1px solid #fed7aa;border-radius:10px;padding:14px;margin-bottom:14px;}

/* ── Gantt ── */
.gantt-wrap{width:100%;overflow-x:auto;border:1px solid #fed7aa;border-radius:8px;}
.g-head{display:grid;grid-template-columns:260px 150px 1fr 80px;background:#f97316;color:#fff;border-radius:8px 8px 0 0;}
.g-label-h,.g-act-h{padding:8px 10px;font-weight:700;font-size:12px;display:flex;align-items:center;}
.g-timeline-h{position:relative;height:36px;overflow:hidden;}
.g-months-strip{position:absolute;inset:0;}
.g-month{position:absolute;top:0;height:50%;display:flex;align-items:center;padding:0 6px;font-size:10px;font-weight:700;border-left:1px solid rgba(255,255,255,0.35);white-space:nowrap;overflow:hidden;}
.g-weeks-strip{position:absolute;left:0;right:0;top:50%;height:50%;display:flex;}
.g-week-tick{position:absolute;top:0;height:100%;display:flex;align-items:center;font-size:9px;color:rgba(255,255,255,0.85);padding-left:3px;border-left:1px solid rgba(255,255,255,0.2);}
.g-body{background:#fff;border-radius:0 0 8px 8px;}
.g-row{display:grid;grid-template-columns:260px 150px 1fr 80px;border-bottom:1px solid #ffedd5;min-height:62px;}
.g-row:last-child{border-bottom:none;}
.g-row:hover{background:#fffaf5;}
.g-label{padding:8px 10px;display:flex;flex-direction:column;justify-content:center;gap:3px;}
.g-need{padding:8px 10px;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;color:#7c2d12;background:#fffaf5;border-left:1px solid #ffedd5;border-right:1px solid #ffedd5;}
.g-dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:5px;vertical-align:middle;flex-shrink:0;}
.g-ot{font-weight:700;color:#431407;font-size:13px;}
.g-sub{font-size:11px;color:#9a3412;margin-left:14px;}
.g-chips{display:flex;flex-wrap:wrap;gap:3px;margin-left:14px;margin-top:2px;}
.g-chip{background:#ffedd5;color:#9a3412;border:1px solid #fdba74;border-radius:999px;padding:1px 7px;font-size:10px;}
.g-track{position:relative;min-height:62px;}
.g-gridline{position:absolute;top:0;bottom:0;width:1px;background:#ffedd5;z-index:0;pointer-events:none;}
.g-today-line{position:absolute;top:0;bottom:0;width:2px;background:#ef4444;opacity:.7;z-index:2;pointer-events:none;}
.g-today-line::after{content:"hoy";position:absolute;top:2px;left:4px;font-size:9px;color:#ef4444;white-space:nowrap;}
.g-bar{position:absolute;top:50%;transform:translateY(-50%);height:28px;border-radius:6px;display:flex;align-items:center;font-size:10px;color:#fff;font-weight:700;padding:0 8px;white-space:nowrap;overflow:hidden;cursor:default;box-shadow:0 2px 5px rgba(0,0,0,.2);z-index:1;transition:filter .15s;}
.g-bar:hover{filter:brightness(1.14);}
.g-out-range{font-size:11px;color:#9a3412;padding:4px 8px;font-style:italic;position:absolute;top:50%;transform:translateY(-50%);}
.g-empty{padding:24px;text-align:center;color:#9a3412;font-style:italic;}
.g-act{display:flex;align-items:center;justify-content:center;gap:4px;padding:6px;}
.g-btn{font-size:14px;padding:4px 7px;border-radius:5px;background:#fff;border:1px solid #ddd;text-decoration:none;cursor:pointer;color:#333;}
.g-btn:hover{background:#ffedd5;border-color:#fdba74;}
.g-btn-red{color:#dc2626;}
.g-btn-red:hover{background:#fee2e2;border-color:#fca5a5;}

/* ── Table ── */
.tbl{width:100%;border-collapse:collapse;background:#fff;border-radius:8px;overflow:hidden;}
.tbl th,.tbl td{padding:10px;border-bottom:1px solid #ffedd5;text-align:left;font-size:13px;}
.tbl th{background:#f97316;color:#fff;font-weight:700;}
.tbl tr:hover{background:#fff7ed;}
.dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px;vertical-align:middle;}
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin-bottom:14px;}
.kpi{background:#fff;border:1px solid #fed7aa;border-radius:8px;padding:12px;}
.kpi .t{font-size:12px;color:#9a3412;}
.kpi .v{font-size:24px;font-weight:800;color:#7c2d12;}

/* ── Form ── */
.form-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;}
.form-group{display:flex;flex-direction:column;gap:5px;}
.form-group.full{grid-column:1/-1;}
label{font-size:13px;font-weight:700;color:#9a3412;}
input[type=text],input[type=date],input[type=number],select,textarea{padding:9px 12px;border:1px solid #fdba74;border-radius:6px;background:#fffaf5;font-size:14px;width:100%;}
input:focus,select:focus,textarea:focus{outline:none;border-color:#f97316;background:#fff;}
.rec-table{width:100%;border-collapse:collapse;margin-top:8px;}
.rec-table th,.rec-table td{padding:7px 10px;border-bottom:1px solid #ffedd5;font-size:13px;}
.rec-table th{background:#fff7ed;color:#9a3412;font-weight:700;}
.err{background:#fee2e2;border:1px solid #fecaca;color:#991b1b;padding:10px;border-radius:6px;margin-bottom:10px;}
.ok{background:#dcfce7;border:1px solid #86efac;color:#166534;padding:10px;border-radius:6px;margin-bottom:10px;}

@media(max-width:800px){
    .g-head,.g-row{grid-template-columns:180px 120px 1fr 52px;}
    .form-grid{grid-template-columns:1fr;}
}
</style>"""


# ── Gantt renderer ─────────────────────────────────────────────────────────────
def _gantt_html(entradas, fi_vista, ff_vista, operarios_disponibles=0):
    total_dias = (ff_vista - fi_vista).days + 1
    if total_dias <= 0 or total_dias > 1200:
        return "<div style='padding:10px;color:#9a3412;'>Rango de fechas inválido o muy extenso.</div>"

    # Month header labels
    meses_html = ""
    cur = date(fi_vista.year, fi_vista.month, 1)
    while cur <= ff_vista:
        mi = max(cur, fi_vista)
        _, ld = monthrange(cur.year, cur.month)
        mf = min(date(cur.year, cur.month, ld), ff_vista)
        left = (mi - fi_vista).days / total_dias * 100
        width = ((mf - mi).days + 1) / total_dias * 100
        nombre_mes = ["Ene", "Feb", "Mar", "Abr", "May", "Jun",
                      "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"][cur.month - 1]
        meses_html += f'<div class="g-month" style="left:{left:.2f}%;width:{width:.2f}%;">{nombre_mes} {cur.year}</div>'
        next_m = cur.month % 12 + 1
        next_y = cur.year + (1 if cur.month == 12 else 0)
        cur = date(next_y, next_m, 1)

    # Week tick marks (every Monday)
    weeks_html = ""
    d = fi_vista
    while d <= ff_vista:
        if d.weekday() == 0:
            left = (d - fi_vista).days / total_dias * 100
            weeks_html += f'<div class="g-week-tick" style="left:{left:.2f}%;">{d.day}/{d.month}</div>'
        d += timedelta(days=1)

    # Grid lines (every Monday) in track
    gridlines_html = ""
    d = fi_vista
    while d <= ff_vista:
        if d.weekday() == 0:
            left = (d - fi_vista).days / total_dias * 100
            gridlines_html += f'<div class="g-gridline" style="left:{left:.2f}%;"></div>'
        d += timedelta(days=1)

    # Today line
    today = date.today()
    today_html = ""
    if fi_vista <= today <= ff_vista:
        today_pct = (today - fi_vista).days / total_dias * 100
        today_html = f'<div class="g-today-line" style="left:{today_pct:.2f}%;"></div>'

    # Rows
    rows_html = ""
    for e in entradas:
        fi = _parse_date(e.get("fecha_inicio"))
        ff = _parse_date(e.get("fecha_fin"))
        fecha_nec = _parse_date(e.get("fecha_entrega"))
        ot_id = e["ot_id"]
        obra = html_lib.escape(str(e.get("obra") or f"OT {ot_id}"))
        titulo = html_lib.escape(str(e.get("titulo") or ""))
        cant_rec = int(e.get("cantidad_recursos") or 0)
        prog_id = e["id"]
        color = _color_ot(ot_id)

        if not fi or not ff:
            continue

        bar_start = max(fi, fi_vista)
        bar_end = min(ff, ff_vista)
        avance = max(0, min(100, int(e.get("avance") or 0)))
        es_sub = e.get("es_subcontrato", False)

        if bar_start > ff_vista or bar_end < fi_vista:
            bar_html = '<span class="g-out-range">fuera de rango visible</span>'
        else:
            left = (bar_start - fi_vista).days / total_dias * 100
            width = max(0.4, ((bar_end - bar_start).days + 1) / total_dias * 100)
            duracion = (ff - fi).days + 1
            hs = e.get("hs_programadas") or 0
            tip = (
                f"OT {ot_id} | {e.get('obra', '')}\n"
                f"{_fmt(fi)} → {_fmt(ff)}\n"
                f"Fecha necesidad: {_fmt(fecha_nec)}\n"
                f"{duracion} días | {hs} hs"
            )
            if cant_rec:
                tip += f"\n{cant_rec} recurso(s) asignado(s)"
            tip += f"\nAvance real: {avance}%"
            if es_sub:
                tip += "\n⚠ SUBCONTRATO (hs previstas = 0)"

            # ── Barra planificada (arriba) ──
            if es_sub:
                # Subcontrato: barra con rayas diagonales (patrón CSS), color gris azulado
                planned_bar = (
                    f'<div class="g-bar" style="left:{left:.2f}%;width:{width:.2f}%;top:30%;height:16px;'
                    f'background:repeating-linear-gradient(45deg,#64748b 0,#64748b 4px,#94a3b8 4px,#94a3b8 10px);'
                    f'opacity:0.85;" title="{html_lib.escape(tip)}"></div>'
                )
            else:
                planned_bar = (
                    f'<div class="g-bar" style="left:{left:.2f}%;width:{width:.2f}%;top:15%;height:14px;'
                    f'background:{color};opacity:0.35;border-radius:4px;" '
                    f'title="{html_lib.escape(tip)}"></div>'
                )

            # ── Barra avance real (abajo) ──
            if es_sub:
                # Subcontrato: mostrar avance como barra ámbar si > 0
                avance_color_sub = "#f59e0b"
                if avance > 0:
                    avance_width = width * avance / 100
                    if avance_width > 4:
                        inner_lbl = f"{avance}%"
                        outer_lbl = ""
                    else:
                        inner_lbl = ""
                        right_pos = left + avance_width
                        outer_lbl = (
                            f'<div style="position:absolute;left:{right_pos:.2f}%;top:58%;'
                            f'font-size:9px;font-weight:700;color:{avance_color_sub};'
                            f'white-space:nowrap;padding-left:3px;line-height:14px;">{avance}%</div>'
                        )
                    avance_bar = (
                        f'<div class="g-bar" style="left:{left:.2f}%;width:{avance_width:.2f}%;top:58%;height:14px;'
                        f'background:{avance_color_sub};border-radius:4px;opacity:0.9;" '
                        f'title="Avance real: {avance}%">{inner_lbl}</div>'
                        + outer_lbl
                    )
                else:
                    outer_lbl = (
                        f'<div style="position:absolute;left:{left:.2f}%;top:58%;'
                        f'font-size:9px;font-weight:700;color:#94a3b8;'
                        f'white-space:nowrap;padding-left:3px;line-height:14px;">0%</div>'
                    )
                    avance_bar = outer_lbl
            else:
                avance_width = width * avance / 100
                avance_color = "#16a34a"
                if avance > 0:
                    # Etiqueta dentro de la barra si hay espacio, si no fuera a la derecha
                    if avance_width > 4:
                        inner_lbl = f"{avance}%"
                        outer_lbl = ""
                    else:
                        inner_lbl = ""
                        right_pos = left + avance_width
                        outer_lbl = (
                            f'<div style="position:absolute;left:{right_pos:.2f}%;top:58%;'
                            f'font-size:9px;font-weight:700;color:{avance_color};'
                            f'white-space:nowrap;padding-left:3px;line-height:14px;">{avance}%</div>'
                        )
                    avance_bar = (
                        f'<div class="g-bar" style="left:{left:.2f}%;width:{avance_width:.2f}%;top:58%;height:14px;'
                        f'background:{avance_color};border-radius:4px;opacity:0.9;" '
                        f'title="Avance real: {avance}%">{inner_lbl}</div>'
                        + outer_lbl
                    )
                else:
                    # avance 0%: mostrar etiqueta "0%" a la izquierda del inicio de barra
                    outer_lbl = (
                        f'<div style="position:absolute;left:{left:.2f}%;top:58%;'
                        f'font-size:9px;font-weight:700;color:#64748b;'
                        f'white-space:nowrap;padding-left:3px;line-height:14px;">0%</div>'
                    )
                    avance_bar = outer_lbl

            # ── Etiqueta de fechas (encima de ambas barras) ──
            if not es_sub and width > 8:
                date_label = (
                    f'<div style="position:absolute;left:{left:.2f}%;width:{width:.2f}%;top:2px;'
                    f'font-size:9px;color:#431407;font-weight:700;white-space:nowrap;overflow:hidden;'
                    f'padding:0 4px;">{_fmt(fi)} – {_fmt(ff)}</div>'
                )
            else:
                date_label = ""

            bar_html = date_label + planned_bar + avance_bar

        avance_chip_color = "#f59e0b" if es_sub else "#16a34a"
        avance_chip = (
            f'<span class="g-chip" style="background:{avance_chip_color}1a;border-color:{avance_chip_color}66;'
            f'color:{avance_chip_color};font-weight:800;">{avance}% avance</span>'
        ) if avance > 0 else ""
        sub_chip = (
            '<span class="g-chip" style="background:#f1f5f9;border-color:#94a3b8;'
            'color:#475569;font-weight:700;">⚙ Subcontrato</span>'
        ) if es_sub else ""
        rec_chips = f'<span class="g-chip">{cant_rec} rec.</span>' if cant_rec else ""
        dot_style = "background:repeating-linear-gradient(45deg,#64748b 0,#64748b 3px,#94a3b8 3px,#94a3b8 6px);" if es_sub else f"background:{color};"

        rows_html += f"""
        <div class="g-row">
            <div class="g-label">
                <div><span class="g-dot" style="{dot_style}"></span><span class="g-ot">OT {ot_id} — {obra}</span></div>
                <div class="g-sub">{titulo}</div>
                <div class="g-chips">{rec_chips}{avance_chip}{sub_chip}</div>
            </div>
            <div class="g-need">{_fmt(fecha_nec)}</div>
            <div class="g-track">
                {gridlines_html}
                {today_html}
                {bar_html}
            </div>
            <div class="g-act">
                <a href="/modulo/programacion/editar/{prog_id}" class="g-btn" title="Editar">✏️</a>
                <form method="post" action="/modulo/programacion/eliminar" style="display:inline;"
                      onsubmit="return confirm('¿Eliminar esta programación?');">
                    <input type="hidden" name="id" value="{prog_id}">
                    <button type="submit" class="g-btn g-btn-red" title="Eliminar">🗑</button>
                </form>
            </div>
        </div>
        """

    if not rows_html:
        rows_html = (
            '<div class="g-empty">No hay programaciones en este período. '
            '<a href="/modulo/programacion/nueva">Agregar nueva →</a></div>'
        )

    # ── Weekly resource summary rows ──────────────────────────────────────────
    first_monday = fi_vista - timedelta(days=fi_vista.weekday())
    semanas = []
    d = first_monday
    while d <= ff_vista:
        week_end = d + timedelta(days=6)
        vis_s = max(d, fi_vista)
        vis_e = min(week_end, ff_vista)
        if vis_s <= vis_e:
            total_rec = 0
            for e in entradas:
                fi_e = _parse_date(e.get("fecha_inicio"))
                ff_e = _parse_date(e.get("fecha_fin"))
                if fi_e and ff_e and fi_e <= week_end and ff_e >= d and not e.get("es_subcontrato"):
                    total_rec += int(e.get("cantidad_recursos") or 0)
            semanas.append((vis_s, vis_e, total_rec))
        d += timedelta(days=7)

    assigned_cells = ""
    avail_cells = ""
    for vis_s, vis_e, rec_sum in semanas:
        left = (vis_s - fi_vista).days / total_dias * 100
        width = ((vis_e - vis_s).days + 1) / total_dias * 100
        # Color: green/yellow/red based on load vs disponibles
        if operarios_disponibles > 0 and rec_sum > 0:
            load = rec_sum / operarios_disponibles
            if load <= 0.7:
                bg, fg = "#dcfce7", "#166534"
            elif load <= 1.0:
                bg, fg = "#fef9c3", "#854d0e"
            else:
                bg, fg = "#fee2e2", "#991b1b"
        elif rec_sum > 0:
            bg, fg = "#dbeafe", "#1e40af"
        else:
            bg, fg = "#f8fafc", "#94a3b8"
        lbl = str(rec_sum) if rec_sum > 0 else ""
        assigned_cells += (
            f'<div style="position:absolute;left:{left:.2f}%;width:{width:.2f}%;height:100%;'
            f'display:flex;align-items:center;justify-content:center;'
            f'background:{bg};color:{fg};font-size:11px;font-weight:700;'
            f'border-right:1px solid rgba(0,0,0,.06);" title="{rec_sum} recursos asignados esta semana">{lbl}</div>'
        )
        a_lbl = str(operarios_disponibles) if operarios_disponibles > 0 else "—"
        avail_cells += (
            f'<div style="position:absolute;left:{left:.2f}%;width:{width:.2f}%;height:100%;'
            f'display:flex;align-items:center;justify-content:center;'
            f'background:#f0fdf4;color:#15803d;font-size:11px;font-weight:700;'
            f'border-right:1px solid rgba(0,0,0,.06);" title="{operarios_disponibles} operarios disponibles">{a_lbl}</div>'
        )

    col_grid = "260px 150px 1fr 80px"
    footer_html = f"""
    <div style="border-top:2px solid #f97316;">
        <div style="display:grid;grid-template-columns:{col_grid};background:#eff6ff;min-height:32px;border-bottom:1px solid #bfdbfe;">
            <div style="padding:0 10px;font-size:11px;font-weight:700;color:#1e40af;display:flex;align-items:center;gap:5px;white-space:nowrap;">
                📊 Rec. asignados / sem.
            </div>
            <div></div>
            <div style="position:relative;height:32px;">{assigned_cells}</div>
            <div></div>
        </div>
        <div style="display:grid;grid-template-columns:{col_grid};background:#f0fdf4;min-height:32px;">
            <div style="padding:0 10px;font-size:11px;font-weight:700;color:#15803d;display:flex;align-items:center;gap:5px;white-space:nowrap;">
                👷 Operarios disponibles
            </div>
            <div></div>
            <div style="position:relative;height:32px;">{avail_cells}</div>
            <div></div>
        </div>
    </div>"""

    return f"""
    <div class="gantt-wrap">
        <div class="g-head">
            <div class="g-label-h">OT / Obra</div>
            <div class="g-label-h">Fecha necesidad</div>
            <div class="g-timeline-h">
                <div class="g-months-strip">{meses_html}</div>
                <div class="g-weeks-strip">{weeks_html}</div>
            </div>
            <div class="g-act-h">Acc.</div>
        </div>
        <div class="g-body">
            {rows_html}
        </div>
        {footer_html}
    </div>
    """


# ── Form HTML ──────────────────────────────────────────────────────────────────
def _form_html(ots_activas, prog=None, error=""):
    es_edicion = prog is not None
    titulo_pag = "Editar Programación" if es_edicion else "Nueva Programación"
    action = f"/modulo/programacion/editar/{prog['id']}" if es_edicion else "/modulo/programacion/nueva"

    ots_opts = '<option value="">— Seleccionar OT —</option>'
    for ot_id, obra, titulo_ot, estado in ots_activas:
        sel = "selected" if es_edicion and int(prog["ot_id"]) == int(ot_id) else ""
        ots_opts += (
            f'<option value="{ot_id}" {sel}>'
            f'OT {ot_id} — {html_lib.escape(str(obra))} ({html_lib.escape(str(titulo_ot))})'
            f'</option>'
        )

    fi_val = str(prog.get("fecha_inicio") or "") if es_edicion else ""
    ff_val = str(prog.get("fecha_fin") or "") if es_edicion else ""
    cant_rec_val = int(prog.get("cantidad_recursos", 1)) if es_edicion else 1
    obs_val = html_lib.escape(str(prog.get("observaciones") or "")) if es_edicion else ""

    err_html = f'<div class="err">{html_lib.escape(error)}</div>' if error else ""
    btn_label = "Guardar cambios" if es_edicion else "Crear Programación"

    return f"""<!DOCTYPE html><html>
<head><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{titulo_pag}</title>{_CSS}
<style>
.hs-preview{{background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:12px 16px;
  font-size:15px;color:#9a3412;font-weight:700;margin-top:6px;display:flex;align-items:center;gap:10px;}}
.hs-preview .big{{font-size:28px;color:#7c2d12;}}
</style>
</head>
<body>
<div class="hdr">
    <h2>📅 {titulo_pag}</h2>
    <a href="/modulo/programacion" class="btn btn-sec">⬅️ Volver</a>
</div>
<div class="panel">
    {err_html}
    <form method="post" action="{action}" id="main-form">
    <input type="hidden" name="hs_programadas" id="hs-hidden" value="0">
    <div class="form-grid">
        <div class="form-group full">
            <label>Orden de Trabajo *</label>
            <select name="ot_id" required>{ots_opts}</select>
        </div>
        <div class="form-group">
            <label>Fecha de Inicio *</label>
            <input type="date" name="fecha_inicio" id="fi" value="{fi_val}" required oninput="calcHoras()">
        </div>
        <div class="form-group">
            <label>Fecha de Fin *</label>
            <input type="date" name="fecha_fin" id="ff" value="{ff_val}" required oninput="calcHoras()">
        </div>
        <div class="form-group">
            <label>Cantidad de Recursos *</label>
            <input type="number" name="cantidad_recursos" id="rec" value="{cant_rec_val}"
                   min="0" max="200" step="1" required oninput="calcHoras()">
        </div>
        <div class="form-group" style="align-self:end;">
            <div class="hs-preview" id="hs-preview-box">
                <span class="big" id="hs-val">—</span>
                <span id="hs-detail" style="font-size:12px;color:#9a3412;"></span>
            </div>
        </div>
        <div class="form-group full">
            <label>Observaciones</label>
            <textarea name="observaciones" rows="3"
                      placeholder="Notas u observaciones adicionales">{obs_val}</textarea>
        </div>
    </div>

    <div style="margin-top:20px;display:flex;gap:10px;flex-wrap:wrap;">
        <button type="submit" class="btn">{btn_label}</button>
        <a href="/modulo/programacion" class="btn btn-sec">Cancelar</a>
    </div>
    </form>
</div>

<script>
function calcHoras() {{
    const fi = document.getElementById('fi').value;
    const ff = document.getElementById('ff').value;
    const rec = parseInt(document.getElementById('rec').value) || 0;
    let hs = 0, dias = 0;
    if (fi && ff && ff >= fi) {{
        dias = Math.round((new Date(ff) - new Date(fi)) / 86400000) + 1;
        hs = rec * 10 * dias;
    }}
    document.getElementById('hs-val').textContent = hs > 0 ? hs + ' hs' : '—';
    document.getElementById('hs-detail').textContent =
        hs > 0 ? rec + ' rec. × 10 hs/día × ' + dias + ' días' : 'Completá fechas y recursos';
    document.getElementById('hs-hidden').value = hs;
}}
document.getElementById('main-form').addEventListener('submit', function(e) {{
    const fi = document.getElementById('fi').value;
    const ff = document.getElementById('ff').value;
    if (fi && ff && ff < fi) {{
        e.preventDefault();
        alert('La fecha de fin debe ser igual o posterior a la fecha de inicio.');
        return;
    }}
    calcHoras();
}});
calcHoras();
</script>
</body></html>"""


# ── Routes ─────────────────────────────────────────────────────────────────────
@programacion_bp.route("/modulo/programacion")
def programacion_index():
    db = get_db()
    today = date.today()

    vista = (request.args.get("vista") or "").strip()
    fi_raw = request.args.get("fi")
    ff_raw = request.args.get("ff")

    def_fi = date(today.year, today.month, 1)
    m3 = today.month + 2
    y3 = today.year + (1 if m3 > 12 else 0)
    m3 = m3 - 12 if m3 > 12 else m3
    _, ld = monthrange(y3, m3)
    def_ff = date(y3, m3, ld)

    # Vista presets cuando no hay fechas explícitas
    if not fi_raw and not ff_raw:
        if vista == "semana":
            def_fi = today - timedelta(days=today.weekday())
            def_ff = def_fi + timedelta(days=6)
        elif vista == "mensual":
            def_fi = date(today.year, today.month, 1)
            _, ld2 = monthrange(today.year, today.month)
            def_ff = date(today.year, today.month, ld2)
        # "trimestral" o sin vista: 3 meses (ya calculado arriba en def_fi/def_ff)

    fi_vista = _parse_date(fi_raw) or def_fi
    ff_vista = _parse_date(ff_raw) or def_ff
    if ff_vista <= fi_vista:
        ff_vista = fi_vista + timedelta(days=89)
    obra_fil = (request.args.get("obra") or "").strip()

    rows = db.execute("""
        SELECT p.id, p.ot_id, p.fecha_inicio, p.fecha_fin,
               COALESCE(p.hs_programadas, 0), COALESCE(p.cantidad_recursos, 0), COALESCE(p.observaciones, ''),
               COALESCE(ot.obra, ''), COALESCE(ot.titulo, ''),
               COALESCE(ot.cliente, ''), COALESCE(ot.estado, ''), COALESCE(ot.fecha_entrega, ''),
               COALESCE(ot.estado_avance, 0), COALESCE(ot.hs_previstas, 0)
        FROM programacion p
        LEFT JOIN ordenes_trabajo ot ON ot.id = p.ot_id
        ORDER BY p.fecha_inicio ASC, p.ot_id ASC
    """).fetchall()

    entradas = [
        {
            "id": r[0], "ot_id": r[1], "fecha_inicio": r[2], "fecha_fin": r[3],
            "hs_programadas": r[4], "cantidad_recursos": r[5], "observaciones": r[6],
            "obra": r[7], "titulo": r[8], "cliente": r[9], "estado_ot": r[10], "fecha_entrega": r[11],
            "avance": int(r[12] or 0),
            "es_subcontrato": float(r[13] or 0) == 0 or int(r[5] or 0) == 0,
        }
        for r in rows
    ]
    if obra_fil:
        entradas = [e for e in entradas if obra_fil.lower() in (e.get("obra") or "").lower()]

    def _lunes_semana(d):
        return d - timedelta(days=d.weekday())

    semana_sel = _parse_date(request.args.get("semana")) or _lunes_semana(today)
    semana_sel = _lunes_semana(semana_sel)
    semana_fin = semana_sel + timedelta(days=6)

    cumplimiento_rows = db.execute(
        """
        SELECT ot_id, semana_inicio, COALESCE(pct_cumplido, 0), COALESCE(desvio_codigo, '')
        FROM programacion_cumplimiento
        ORDER BY semana_inicio ASC, ot_id ASC
        """
    ).fetchall()
    cumpl_idx = {}
    for r in cumplimiento_rows:
        cumpl_idx[(int(r[0]), str(r[1]))] = (float(r[2] or 0), str(r[3] or ""))

    # Cumplimiento: mostrar OTs con actividad en la semana seleccionada O con avance registrado.
    ots_activas_cumpl = db.execute("""
        SELECT id, COALESCE(obra, ''), COALESCE(titulo, ''), COALESCE(fecha_entrega, ''), COALESCE(estado_avance, 0)
        FROM ordenes_trabajo
        WHERE fecha_cierre IS NULL AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
        ORDER BY id ASC
    """).fetchall()

    ot_ids_con_actividad_semana = set()
    for e in entradas:
        fi_e = _parse_date(e.get("fecha_inicio"))
        ff_e = _parse_date(e.get("fecha_fin"))
        if not fi_e or not ff_e:
            continue
        if fi_e <= semana_fin and ff_e >= semana_sel:
            try:
                ot_ids_con_actividad_semana.add(int(e.get("ot_id") or 0))
            except Exception:
                continue

    # Lista de obras para filtro (union de programadas + activas)
    obras_lista = sorted({
        str(r[7] or "").strip() for r in rows
        if str(r[7] or "").strip()
    } | {
        str(r[1] or "").strip() for r in ots_activas_cumpl
        if str(r[1] or "").strip()
    })
    entradas_semana = [
        {"ot_id": r[0], "obra": r[1], "titulo": r[2], "fecha_entrega": r[3]}
        for r in ots_activas_cumpl
        if int(r[0] or 0) in ot_ids_con_actividad_semana or int(r[4] or 0) > 0
    ]
    if obra_fil:
        entradas_semana = [e for e in entradas_semana if obra_fil.lower() in (e.get("obra") or "").lower()]

    semana_key = semana_sel.strftime("%Y-%m-%d")
    cumplimiento_rows_html = ""
    pcts_semana = []
    for e in entradas_semana:
        ot_id = int(e["ot_id"])
        obra = html_lib.escape(str(e.get("obra") or ""))
        titulo = html_lib.escape(str(e.get("titulo") or ""))
        pct_prev, desvio_prev = cumpl_idx.get((ot_id, semana_key), (100.0, ""))
        pct_val = max(0, min(100, int(round(pct_prev))))
        if pct_val < 100:
            pcts_semana.append(pct_val)
        else:
            pcts_semana.append(100)
        opts = '<option value="">-- Seleccionar desvío --</option>'
        for code, label in _DESVIOS.items():
            sel = "selected" if str(desvio_prev) == str(code) else ""
            opts += f'<option value="{code}" {sel}>{code} - {html_lib.escape(label)}</option>'
        show_desv = "" if pct_val < 100 else "display:none;"
        cumplimiento_rows_html += f"""
        <tr>
            <td><b>OT {ot_id}</b></td>
            <td>{obra}</td>
            <td>{titulo}</td>
            <td style="text-align:center;">
                <input type="number" min="0" max="100" step="1" value="{pct_val}" name="pct_{ot_id}"
                       style="width:90px;" oninput="toggleDesvio({ot_id}); calcCumplimientoKPIs();"> %
            </td>
            <td id="desv-wrap-{ot_id}" style="{show_desv}">
                <select name="desvio_{ot_id}" id="desv-{ot_id}" style="min-width:290px;">{opts}</select>
            </td>
        </tr>
        """

    todas_hasta_semana = []
    for r in cumplimiento_rows:
        sem = _parse_date(r[1])
        if sem and sem <= semana_sel:
            todas_hasta_semana.append(float(r[2] or 0))

    pct_semanal = (sum(pcts_semana) / len(pcts_semana)) if pcts_semana else None
    pct_acumulado = (sum(todas_hasta_semana) / len(todas_hasta_semana)) if todas_hasta_semana else None

    desvio_stats = {}
    total_desv = 0
    for r in cumplimiento_rows:
        sem = _parse_date(r[1])
        if not sem or sem > semana_sel:
            continue
        pct = float(r[2] or 0)
        cod = str(r[3] or "").strip()
        if pct < 100 and cod in _DESVIOS:
            desvio_stats[cod] = desvio_stats.get(cod, 0) + 1
            total_desv += 1

    _colors_desv = [
        "#6366f1", "#f97316", "#3b82f6", "#10b981", "#ef4444",
        "#8b5cf6", "#f59e0b", "#06b6d4", "#ec4899", "#14b8a6",
    ]
    desv_legend = ""
    if total_desv > 0:
        for i, (code, label) in enumerate(_DESVIOS.items()):
            c = desvio_stats.get(code, 0)
            if c <= 0:
                continue
            pct = (c * 100.0 / total_desv)
            col = _colors_desv[i % len(_colors_desv)]
            desv_legend += (
                f"<div style='display:flex;align-items:center;gap:7px;margin:5px 0;'>"
                f"<div style='width:14px;height:14px;border-radius:3px;background:{col};flex-shrink:0;'></div>"
                f"<div style='font-size:12px;color:#1e293b;flex:1;'>{code}. {html_lib.escape(label)}</div>"
                f"<div style='font-size:12px;font-weight:700;color:{col};white-space:nowrap;'>{c} ({pct:.0f}%)</div>"
                f"</div>"
            )
    else:
        desv_legend = "<div style='color:#64748b;font-style:italic;'>Sin desvíos registrados hasta la semana seleccionada.</div>"

    donut_svg = _svg_donut_chart(desvio_stats, total_desv)

    week_map = {}
    for r in cumplimiento_rows:
        sem = _parse_date(r[1])
        if not sem or sem > semana_sel:
            continue
        key = sem.strftime("%Y-%m-%d")
        if key not in week_map:
            week_map[key] = {"tot": 0, "desv": 0}
        week_map[key]["tot"] += 1
        if float(r[2] or 0) < 100:
            week_map[key]["desv"] += 1

    weeks_sorted = sorted(week_map.keys())
    svg_paths = ""
    svg_points_sem = []
    svg_points_ac = []
    acum_tot = 0
    acum_desv = 0
    if weeks_sorted:
        max_x = max(1, len(weeks_sorted) - 1)
        for i, wk in enumerate(weeks_sorted):
            dato = week_map[wk]
            sem_pct = (dato["desv"] * 100.0 / dato["tot"]) if dato["tot"] else 0.0
            acum_tot += dato["tot"]
            acum_desv += dato["desv"]
            ac_pct = (acum_desv * 100.0 / acum_tot) if acum_tot else 0.0
            x = 30 + (i * 520.0 / max_x)
            y_sem = 190 - (sem_pct * 1.5)
            y_ac = 190 - (ac_pct * 1.5)
            svg_points_sem.append(f"{x:.1f},{y_sem:.1f}")
            svg_points_ac.append(f"{x:.1f},{y_ac:.1f}")
            lbl = _fmt(_parse_date(wk))
            svg_paths += f"<text x='{x:.1f}' y='210' font-size='10' text-anchor='middle' fill='#6b7280'>{lbl}</text>"
        line_sem = " ".join(svg_points_sem)
        line_ac = " ".join(svg_points_ac)
        chart_svg = f"""
        <svg viewBox="0 0 560 220" style="width:100%;height:auto;background:#fff;border:1px solid #e5e7eb;border-radius:8px;">
            <line x1="30" y1="40" x2="30" y2="190" stroke="#cbd5e1" stroke-width="1"/>
            <line x1="30" y1="190" x2="550" y2="190" stroke="#cbd5e1" stroke-width="1"/>
            <text x="6" y="45" font-size="10" fill="#6b7280">100%</text>
            <text x="10" y="120" font-size="10" fill="#6b7280">50%</text>
            <text x="14" y="194" font-size="10" fill="#6b7280">0%</text>
            <polyline fill="none" stroke="#f97316" stroke-width="2.5" points="{line_sem}"/>
            <polyline fill="none" stroke="#2563eb" stroke-width="2.5" points="{line_ac}"/>
            {svg_paths}
            <rect x="340" y="14" width="12" height="3" fill="#f97316"/><text x="356" y="18" font-size="10" fill="#7c2d12">Desvío semanal %</text>
            <rect x="460" y="14" width="12" height="3" fill="#2563eb"/><text x="476" y="18" font-size="10" fill="#1e3a8a">Desvío acumulado %</text>
        </svg>
        """
    else:
        chart_svg = "<div style='color:#64748b;font-style:italic;'>Sin datos suficientes para graficar.</div>"

    operarios_count = db.execute(
        """
        SELECT COUNT(DISTINCT TRIM(COALESCE(nombre, '')))
        FROM empleados_parte
        WHERE (
            LOWER(TRIM(COALESCE(puesto_tipo, ''))) = 'operario'
            OR LOWER(TRIM(COALESCE(puesto, ''))) LIKE '%operario%'
            OR LOWER(TRIM(COALESCE(puesto, ''))) LIKE '%soldador%'
            OR LOWER(TRIM(COALESCE(puesto, ''))) LIKE '%armador%'
            OR LOWER(TRIM(COALESCE(puesto, ''))) LIKE '%medio%'
            OR LOWER(TRIM(COALESCE(puesto, ''))) LIKE '%ayudante%'
            OR LOWER(TRIM(COALESCE(puesto, ''))) LIKE '%pintor%'
        )
        AND TRIM(COALESCE(nombre, '')) <> ''
        AND LOWER(TRIM(COALESCE(puesto_tipo, puesto, ''))) NOT LIKE '%supervisor%'
        AND LOWER(TRIM(COALESCE(puesto_tipo, puesto, ''))) NOT LIKE '%encargado%'
        """
    ).fetchone()[0] or 0
    gantt = _gantt_html(entradas, fi_vista, ff_vista, operarios_disponibles=operarios_count)

    # KPIs
    total_hs = sum(float(e.get("hs_programadas") or 0) for e in entradas if not e.get("es_subcontrato"))
    ots_unicas = len({e["ot_id"] for e in entradas})
    en_curso = sum(
        1 for e in entradas
        if _parse_date(e["fecha_inicio"]) and _parse_date(e["fecha_fin"])
        and _parse_date(e["fecha_inicio"]) <= today <= _parse_date(e["fecha_fin"])
    )

    # Table rows
    tabla_rows = ""
    for e in entradas:
        fi = _parse_date(e["fecha_inicio"])
        ff = _parse_date(e["fecha_fin"])
        dur = ((ff - fi).days + 1) if fi and ff else "—"
        hs = float(e["hs_programadas"] or 0)
        cant_rec = int(e["cantidad_recursos"] or 0)
        fecha_nec = _parse_date(e.get("fecha_entrega"))
        color = _color_ot(e["ot_id"])
        # Highlight if active today
        row_style = "background:#fff7ed;" if (fi and ff and fi <= today <= ff) else ""
        tabla_rows += f"""
        <tr style="{row_style}">
            <td><span class="dot" style="background:{color}"></span><b>OT {e['ot_id']}</b></td>
            <td>{html_lib.escape(e['obra'])}</td>
            <td>{_fmt(fecha_nec)}</td>
            <td>{html_lib.escape(e['titulo'])}</td>
            <td>{_fmt(fi)}</td>
            <td>{_fmt(ff)}</td>
            <td style="text-align:center;">{dur} días</td>
            <td style="text-align:center;">{cant_rec} rec.</td>
            <td style="text-align:center;">{hs:.0f} hs</td>
            <td>
                <a href="/modulo/programacion/editar/{e['id']}" style="color:#2563eb;font-size:12px;">Editar</a>
                &nbsp;
                <form method="post" action="/modulo/programacion/eliminar" style="display:inline;"
                      onsubmit="return confirm('¿Eliminar esta programación?');">
                    <input type="hidden" name="id" value="{e['id']}">
                    <button type="submit" style="background:none;border:none;color:#dc2626;cursor:pointer;font-size:12px;padding:0;">
                        Eliminar
                    </button>
                </form>
            </td>
        </tr>
        """

    fi_str = fi_vista.strftime("%Y-%m-%d")
    ff_str = ff_vista.strftime("%Y-%m-%d")
    obra_qs = ("&obra=" + html_lib.escape(obra_fil)) if obra_fil else ""
    _btn_active = "background:#6366f1;color:#fff;border-color:#6366f1;"
    btn_trimestral_active = _btn_active if vista in ("", "trimestral") else ""
    btn_semana_active    = _btn_active if vista == "semana"    else ""
    btn_mensual_active   = _btn_active if vista == "mensual"   else ""
    obras_opts = '<option value="">— Todas las obras —</option>' + "".join(
        f'<option value="{html_lib.escape(o)}" {"selected" if o == obra_fil else ""}>'
        f'{html_lib.escape(o)}</option>'
        for o in obras_lista
    )

    no_prog = "<p style='color:#9a3412;font-style:italic;padding:10px 0;'>No hay programaciones cargadas. <a href='/modulo/programacion/nueva'>Agregar la primera →</a></p>"
    tabla_html = (
        f"<div style='overflow-x:auto;'><table class='tbl'>"
        f"<tr><th>OT</th><th>Obra</th><th>Fecha Necesidad</th><th>Título</th><th>Inicio</th><th>Fin</th>"
        f"<th>Duración</th><th>Recursos</th><th>Hs Plan.</th><th>Acciones</th></tr>"
        f"{tabla_rows}</table></div>"
        if entradas else no_prog
    )

    semana_str = semana_sel.strftime("%Y-%m-%d")
    cumpl_kpi_sem = f"{pct_semanal:.1f}%" if pct_semanal is not None else "—"
    cumpl_kpi_ac = f"{pct_acumulado:.1f}%" if pct_acumulado is not None else "—"
    cumplimiento_panel = f"""
<div class="panel" id="cumplimiento-section">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;">
        <h3 style="margin:0;">Cumplimiento de objetivos semanales</h3>
        <button onclick="printCumplimiento()" class="btn btn-sec btn-sm">🖨️ Imprimir Cumplimiento</button>
    </div>
    <form method="get" action="/modulo/programacion" style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;margin-bottom:10px;">
        <label>Semana (lunes):</label>
        <input type="date" name="semana" value="{semana_str}" style="width:170px;">
        <label>Obra:</label>
        <select name="obra" style="min-width:160px;">{obras_opts}</select>
        <input type="hidden" name="fi" value="{fi_str}">
        <input type="hidden" name="ff" value="{ff_str}">
        <button type="submit" class="btn btn-sec">Ver semana</button>
    </form>

    <div class="kpis" style="margin-top:0;">
        <div class="kpi"><div class="t">% cumplido semanal</div><div class="v" id="kpi-semanal">{cumpl_kpi_sem}</div></div>
        <div class="kpi"><div class="t">% cumplido acumulado</div><div class="v">{cumpl_kpi_ac}</div></div>
        <div class="kpi"><div class="t">Semana evaluada</div><div class="v" style="font-size:18px;">{_fmt(semana_sel)} a {_fmt(semana_fin)}</div></div>
    </div>

    <form method="post" action="/modulo/programacion/cumplimiento" onsubmit="return validarDesvios();">
        <input type="hidden" name="semana_inicio" value="{semana_str}">
        <input type="hidden" name="fi" value="{fi_str}">
        <input type="hidden" name="ff" value="{ff_str}">
        <div style="overflow-x:auto;">
            <table class="tbl">
                <tr><th>OT</th><th>Obra</th><th>Título</th><th>% Cumplido</th><th>Desvío (si &lt; 100%)</th></tr>
                {cumplimiento_rows_html if cumplimiento_rows_html else "<tr><td colspan='5' style='text-align:center;color:#64748b;'>No hay OTs programadas en la semana seleccionada.</td></tr>"}
            </table>
        </div>
        <div style="margin-top:10px;"><button type="submit" class="btn">Guardar cumplimiento semanal</button></div>
    </form>

    <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:14px;">
        <div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:10px;">
            <h4 style="margin:0 0 8px 0;color:#1e3a8a;">% de desvíos acumulados por semana</h4>
            {chart_svg}
        </div>
        <div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:10px;">
            <h4 style="margin:0 0 8px 0;color:#4338ca;">Distribución acumulada de causas de desvío</h4>
            <div style="display:flex;align-items:center;gap:14px;flex-wrap:wrap;">
                <div style="flex-shrink:0;">{donut_svg}</div>
                <div style="flex:1;min-width:160px;">{desv_legend}</div>
            </div>
        </div>
    </div>

    <script>
    function toggleDesvio(otId) {{
        var p = document.querySelector('input[name="pct_' + otId + '"]');
        var w = document.getElementById('desv-wrap-' + otId);
        var s = document.getElementById('desv-' + otId);
        var v = parseInt((p && p.value) || '0', 10);
        if (isNaN(v)) v = 0;
        if (v >= 100) {{
            if (w) w.style.display = 'none';
            if (s) s.value = '';
        }} else {{
            if (w) w.style.display = '';
        }}
    }}
    function validarDesvios() {{
        var ok = true;
        var rows = document.querySelectorAll('input[name^="pct_"]');
        for (var i = 0; i < rows.length; i++) {{
            var inp = rows[i];
            var otId = inp.name.replace('pct_', '');
            var pct = parseInt(inp.value || '0', 10);
            if (isNaN(pct)) pct = 0;
            var sel = document.getElementById('desv-' + otId);
            if (pct < 100 && sel && !sel.value) {{
                ok = false;
                sel.style.borderColor = '#dc2626';
            }} else if (sel) {{
                sel.style.borderColor = '';
            }}
        }}
        if (!ok) {{
            alert('Para OTs con cumplimiento menor a 100%, seleccioná un desvío.');
        }}
        return ok;
    }}
    function calcCumplimientoKPIs() {{
        var rows = document.querySelectorAll('input[name^="pct_"]');
        var sum = 0;
        var n = 0;
        for (var i = 0; i < rows.length; i++) {{
            var v = parseInt(rows[i].value || '0', 10);
            if (isNaN(v)) v = 0;
            if (v < 0) v = 0;
            if (v > 100) v = 100;
            sum += v;
            n += 1;
            var otId = rows[i].name.replace('pct_', '');
            toggleDesvio(otId);
        }}
        var k = document.getElementById('kpi-semanal');
        if (k) k.textContent = n > 0 ? ((sum / n).toFixed(1) + '%') : '—';
    }}
    document.addEventListener('DOMContentLoaded', calcCumplimientoKPIs);
    </script>
</div>
"""

    return f"""<!DOCTYPE html><html>
<head><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Programación de Fabricación</title>{_CSS}
<script>
function _printHeader(title) {{
    var origin = window.location.origin;
    var logoHtml = '<img src="' + origin + '/logo-a3" style="height:54px;display:block;">';
    return '<div style="display:flex;justify-content:space-between;align-items:center;padding:0 0 10px 0;border-bottom:2px solid #6366f1;margin-bottom:14px;">'
      + '<div style="display:flex;align-items:center;gap:12px;">'
        + logoHtml
        + '<div>'
          + '<div style="font-size:20px;font-weight:800;color:#1e3a8a;">' + title + '</div>'
          + '<div style="font-size:13px;color:#475569;font-style:italic;">Programaci\u00f3n \u00b7 Fabricaci\u00f3n Estructuras Met\u00e1licas</div>'
        + '</div>'
      + '</div>'
      + '<div style="display:flex;gap:8px;align-items:center;">'
        + '<div style="width:48px;height:48px;border-radius:50%;border:3px solid #1e3a8a;display:flex;flex-direction:column;align-items:center;justify-content:center;background:#fff;text-align:center;line-height:1;font-family:Arial;">'
          + '<span style="font-size:7px;color:#334155;">ISO</span><span style="font-size:12px;font-weight:800;color:#0f172a;">9001</span>'
        + '</div>'
        + '<div style="width:48px;height:48px;border-radius:50%;border:3px solid #be123c;display:flex;flex-direction:column;align-items:center;justify-content:center;background:#fff;text-align:center;line-height:1;font-family:Arial;">'
          + '<span style="font-size:7px;color:#334155;">ISO</span><span style="font-size:12px;font-weight:800;color:#0f172a;">45001</span>'
        + '</div>'
        + '<div style="width:48px;height:48px;border-radius:50%;border:3px solid #111827;display:flex;flex-direction:column;align-items:center;justify-content:center;background:#fff;text-align:center;line-height:1;font-family:Arial;">'
          + '<span style="font-size:7px;color:#334155;">ISO</span><span style="font-size:12px;font-weight:800;color:#0f172a;">37001</span>'
        + '</div>'
      + '</div>'
    + '</div>';
}}
function _openPrintWin(title, sectionId) {{
    var section = document.getElementById(sectionId);
    if (!section) return null;
    var printWin = window.open('', '_blank', 'width=1400,height=900');
    if (!printWin) {{ alert('El navegador bloqu\u00f3 la ventana emergente. Permit\u00ed ventanas emergentes para este sitio.'); return null; }}
    printWin.document.write('<html><head><meta charset="utf-8"><title>' + title + '</title>');
    var styles = document.querySelectorAll('style');
    styles.forEach(function(s) {{ printWin.document.write(s.outerHTML); }});
    printWin.document.write('<style>@page{{size:A4 landscape;margin:10mm;}}body{{padding:0;background:#fff;}}button,form,.btn,.g-btn{{display:none!important;}}input,select{{pointer-events:none;border:1px solid #ddd;}}.g-track{{overflow:visible!important;}}.panel{{border:none!important;padding:0!important;}}h3{{display:none!important;}}</style>');
    printWin.document.write('</head><body style="padding:8px;font-family:Arial,sans-serif;">');
    printWin.document.write(_printHeader(title));
    printWin.document.write(section.innerHTML);
    printWin.document.write('</body></html>');
    printWin.document.close();
    printWin.focus();
    return printWin;
}}
function printGantt() {{
    var pw = _openPrintWin('Programaci\u00f3n de Fabricaci\u00f3n', 'gantt-section');
    if (pw) setTimeout(function(){{ pw.print(); }}, 700);
}}
function printCumplimiento() {{
    var pw = _openPrintWin('Cumplimiento de Objetivos Semanales', 'cumplimiento-section');
    if (pw) setTimeout(function(){{ pw.print(); }}, 700);
}}
</script>
</head>
<body>
<div class="hdr">
    <h2>📅 Programación de Fabricación</h2>
    <div style="display:flex;gap:8px;flex-wrap:wrap;">
        <a href="/modulo/programacion/nueva" class="btn">➕ Nueva Programación</a>
        <a href="/" class="btn btn-sec">⬅️ Volver</a>
    </div>
</div>

<div class="kpis">
    <div class="kpi"><div class="t">Programaciones cargadas</div><div class="v">{len(entradas)}</div></div>
    <div class="kpi"><div class="t">OTs planificadas</div><div class="v">{ots_unicas}</div></div>
    <div class="kpi"><div class="t">En fabricación hoy</div><div class="v">{en_curso}</div></div>
    <div class="kpi"><div class="t">Hs totales planificadas</div><div class="v">{total_hs:.0f}</div></div>
</div>

<div class="panel">
    <form method="get" action="/modulo/programacion"
          style="display:flex;align-items:center;gap:10px;flex-wrap:wrap;">
        <label>Desde:</label>
        <input type="date" name="fi" value="{fi_str}" style="width:160px;">
        <label>Hasta:</label>
        <input type="date" name="ff" value="{ff_str}" style="width:160px;">
        <label>Obra:</label>
        <select name="obra" style="min-width:180px;">{obras_opts}</select>
        <button type="submit" class="btn">Aplicar</button>
        <a href="/modulo/programacion" class="btn btn-sec">Restablecer</a>
    </form>
</div>

<div class="panel" id="gantt-section">
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:10px;flex-wrap:wrap;gap:8px;">
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
            <h3 style="margin:0;">Diagrama de Gantt</h3>
            <a href="/modulo/programacion?vista=trimestral{obra_qs}" class="btn btn-sm" style="{btn_trimestral_active}">📊 Trimestral</a>
            <a href="/modulo/programacion?vista=mensual{obra_qs}" class="btn btn-sm" style="{btn_mensual_active}">📅 Mensual</a>
            <a href="/modulo/programacion?vista=semana{obra_qs}" class="btn btn-sm" style="{btn_semana_active}">📆 Semana</a>
        </div>
        <button onclick="printGantt()" class="btn btn-sec btn-sm">🖨️ Imprimir Gantt</button>
    </div>
    {gantt}
</div>

{cumplimiento_panel}

<div class="panel">
    <h3>Detalle — {len(entradas)} programaciones · {total_hs:.0f} hs planificadas totales</h3>
    {tabla_html}
</div>

</body></html>"""


@programacion_bp.route("/modulo/programacion/cumplimiento", methods=["POST"])
def programacion_cumplimiento():
    db = get_db()
    semana_inicio = (request.form.get("semana_inicio") or "").strip()
    fi = (request.form.get("fi") or "").strip()
    ff = (request.form.get("ff") or "").strip()

    semana_d = _parse_date(semana_inicio)
    if not semana_d:
        return redirect("/modulo/programacion")

    semana_key = semana_d.strftime("%Y-%m-%d")

    ots_rows = db.execute(
        """
        SELECT DISTINCT COALESCE(p.ot_id, 0)
        FROM programacion p
        WHERE p.ot_id IS NOT NULL
        """
    ).fetchall()

    for (ot_id_raw,) in ots_rows:
        ot_id = int(ot_id_raw or 0)
        if ot_id <= 0:
            continue

        pct_txt = (request.form.get(f"pct_{ot_id}") or "").strip()
        if pct_txt == "":
            continue

        try:
            pct = float(pct_txt)
        except Exception:
            pct = 0.0
        pct = max(0.0, min(100.0, pct))

        desvio = (request.form.get(f"desvio_{ot_id}") or "").strip()
        if pct >= 100:
            desvio = ""
        elif desvio not in _DESVIOS:
            desvio = "10"

        exists = db.execute(
            """
            SELECT id
            FROM programacion_cumplimiento
            WHERE ot_id = ? AND semana_inicio = ?
            LIMIT 1
            """,
            (ot_id, semana_key),
        ).fetchone()

        if exists:
            db.execute(
                """
                UPDATE programacion_cumplimiento
                SET pct_cumplido = ?, desvio_codigo = ?, fecha_actualizacion = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (pct, desvio, int(exists[0])),
            )
        else:
            db.execute(
                """
                INSERT INTO programacion_cumplimiento (ot_id, semana_inicio, pct_cumplido, desvio_codigo)
                VALUES (?, ?, ?, ?)
                """,
                (ot_id, semana_key, pct, desvio),
            )

    db.commit()

    qs = []
    if fi:
        qs.append(f"fi={fi}")
    if ff:
        qs.append(f"ff={ff}")
    qs.append(f"semana={semana_key}")
    return redirect("/modulo/programacion" + ("?" + "&".join(qs) if qs else ""))


@programacion_bp.route("/modulo/programacion/nueva", methods=["GET", "POST"])
def programacion_nueva():
    db = get_db()
    ots_activas = db.execute("""
        SELECT id, COALESCE(obra, ''), COALESCE(titulo, ''), COALESCE(estado, '')
        FROM ordenes_trabajo
        WHERE fecha_cierre IS NULL AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
        ORDER BY id DESC
    """).fetchall()

    if request.method == "POST":
        ot_id_txt = (request.form.get("ot_id") or "").strip()
        fecha_inicio = (request.form.get("fecha_inicio") or "").strip()
        fecha_fin = (request.form.get("fecha_fin") or "").strip()
        cant_rec_txt = (request.form.get("cantidad_recursos") or "0").strip()
        observaciones = (request.form.get("observaciones") or "").strip()

        error = ""
        if not ot_id_txt.isdigit():
            error = "Seleccioná una OT válida."
        elif not fecha_inicio or not fecha_fin:
            error = "Las fechas de inicio y fin son obligatorias."
        elif fecha_fin < fecha_inicio:
            error = "La fecha de fin debe ser igual o posterior a la fecha de inicio."

        if error:
            return _form_html(ots_activas, error=error)

        try:
            cant_rec = max(0, int(cant_rec_txt))
        except Exception:
            cant_rec = 0

        fi_d = _parse_date(fecha_inicio)
        ff_d = _parse_date(fecha_fin)
        dias = ((ff_d - fi_d).days + 1) if fi_d and ff_d else 0
        hs = cant_rec * 10 * dias

        db.execute(
            """
            INSERT INTO programacion (ot_id, fecha_inicio, fecha_fin, hs_programadas, cantidad_recursos, observaciones)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (int(ot_id_txt), fecha_inicio, fecha_fin, hs, cant_rec, observaciones),
        )
        db.commit()
        return redirect("/modulo/programacion")

    return _form_html(ots_activas)


@programacion_bp.route("/modulo/programacion/editar/<int:prog_id>", methods=["GET", "POST"])
def programacion_editar(prog_id):
    db = get_db()
    row = db.execute(
        "SELECT id, ot_id, fecha_inicio, fecha_fin, hs_programadas, COALESCE(cantidad_recursos, 1), observaciones "
        "FROM programacion WHERE id = ?",
        (prog_id,),
    ).fetchone()
    if not row:
        return redirect("/modulo/programacion")

    prog = {
        "id": row[0], "ot_id": row[1], "fecha_inicio": row[2], "fecha_fin": row[3],
        "hs_programadas": row[4], "cantidad_recursos": row[5], "observaciones": row[6],
    }

    ots_activas = db.execute("""
        SELECT id, COALESCE(obra, ''), COALESCE(titulo, ''), COALESCE(estado, '')
        FROM ordenes_trabajo
        WHERE fecha_cierre IS NULL AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
        ORDER BY id DESC
    """).fetchall()

    if request.method == "POST":
        ot_id_txt = (request.form.get("ot_id") or "").strip()
        fecha_inicio = (request.form.get("fecha_inicio") or "").strip()
        fecha_fin = (request.form.get("fecha_fin") or "").strip()
        cant_rec_txt = (request.form.get("cantidad_recursos") or "0").strip()
        observaciones = (request.form.get("observaciones") or "").strip()

        error = ""
        if not ot_id_txt.isdigit():
            error = "Seleccioná una OT válida."
        elif not fecha_inicio or not fecha_fin:
            error = "Las fechas son obligatorias."
        elif fecha_fin < fecha_inicio:
            error = "La fecha de fin debe ser igual o posterior a la fecha de inicio."

        if error:
            return _form_html(ots_activas, prog=prog, error=error)

        try:
            cant_rec = max(0, int(cant_rec_txt))
        except Exception:
            cant_rec = 0

        fi_d = _parse_date(fecha_inicio)
        ff_d = _parse_date(fecha_fin)
        dias = ((ff_d - fi_d).days + 1) if fi_d and ff_d else 0
        hs = cant_rec * 10 * dias

        db.execute(
            """
            UPDATE programacion
            SET ot_id=?, fecha_inicio=?, fecha_fin=?, hs_programadas=?, cantidad_recursos=?, observaciones=?
            WHERE id=?
            """,
            (int(ot_id_txt), fecha_inicio, fecha_fin, hs, cant_rec, observaciones, prog_id),
        )
        db.commit()
        return redirect("/modulo/programacion")

    return _form_html(ots_activas, prog=prog)


@programacion_bp.route("/modulo/programacion/eliminar", methods=["POST"])
def programacion_eliminar():
    db = get_db()
    id_txt = (request.form.get("id") or "").strip()
    if id_txt.isdigit():
        db.execute("DELETE FROM programacion WHERE id = ?", (int(id_txt),))
        db.commit()
    return redirect("/modulo/programacion")
