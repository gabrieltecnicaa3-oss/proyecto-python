import html as html_lib
import os as _os
import base64 as _base64
from datetime import datetime, timedelta, date
from calendar import monthrange
from urllib.parse import quote
from flask import Blueprint, request, redirect, session
from db_utils import get_db
from produccion_routes import calcular_avance_ot

programacion_bp = Blueprint("programacion", __name__)
_programacion_schema_ready = False


def _asegurar_schema_programacion():
    global _programacion_schema_ready
    if _programacion_schema_ready:
        return

    db = get_db()

    # Tablas base del modulo.
    try:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS programacion (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ot_id INTEGER NOT NULL,
                fecha_inicio DATE NOT NULL,
                fecha_fin DATE NOT NULL,
                hs_programadas REAL DEFAULT 0,
                cantidad_recursos INTEGER DEFAULT 1,
                hito_titulo TEXT,
                hito_fecha DATE,
                recursos TEXT,
                observaciones TEXT,
                fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    except Exception:
        pass

    try:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS programacion_cumplimiento (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ot_id INTEGER NOT NULL,
                semana_inicio DATE NOT NULL,
                pct_cumplido REAL DEFAULT 100,
                desvio_codigo TEXT,
                fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    except Exception:
        pass

    try:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS programacion_hitos (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prog_id INTEGER NOT NULL,
                titulo TEXT NOT NULL,
                fecha DATE NOT NULL,
                fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    except Exception:
        pass

    # Columnas criticas para consultas del modulo.
    for _sql in [
        "ALTER TABLE programacion ADD COLUMN orden INTEGER DEFAULT 0",
        "ALTER TABLE programacion ADD COLUMN cantidad_recursos INTEGER DEFAULT 1",
        "ALTER TABLE programacion ADD COLUMN hito_titulo TEXT",
        "ALTER TABLE programacion ADD COLUMN hito_fecha DATE",
        "ALTER TABLE ordenes_trabajo ADD COLUMN es_mantenimiento INTEGER DEFAULT 0",
        "ALTER TABLE ordenes_trabajo ADD COLUMN estado_avance INTEGER DEFAULT 0",
        "ALTER TABLE ordenes_trabajo ADD COLUMN hs_previstas REAL DEFAULT 0",
        "ALTER TABLE ordenes_trabajo ADD COLUMN fecha_cierre DATETIME",
    ]:
        try:
            db.execute(_sql)
        except Exception:
            pass

    try:
        db.execute("UPDATE programacion SET orden = id WHERE orden = 0 OR orden IS NULL")
    except Exception:
        pass

    try:
        db.execute("CREATE INDEX IF NOT EXISTS idx_programacion_hitos_prog_id ON programacion_hitos(prog_id)")
    except Exception:
        pass

    try:
        db.commit()
    except Exception:
        pass

    _programacion_schema_ready = True


@programacion_bp.before_request
def _programacion_before_request_schema():
    _asegurar_schema_programacion()


def _es_usuario_obra():
    return str(session.get("user_role") or "").strip().lower() == "obra"

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
    if isinstance(s, date):
        return s if not isinstance(s, datetime) else s.date()
    txt = str(s).strip()
    if not txt:
        return None
    for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            return datetime.strptime(txt, fmt).date()
        except Exception:
            pass
    try:
        return datetime.fromisoformat(txt.replace("Z", "+00:00")).date()
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


def _normalizar_hitos_desde_form(form):
    titulos = form.getlist("hito_titulo[]")
    fechas = form.getlist("hito_fecha[]")
    hitos = []
    n = max(len(titulos), len(fechas))
    for i in range(n):
        t = (titulos[i] if i < len(titulos) else "") or ""
        f = (fechas[i] if i < len(fechas) else "") or ""
        t = str(t).strip()
        f = str(f).strip()
        if not t and not f:
            continue
        hitos.append({"titulo": t, "fecha": f})
    return hitos


# ── CSS compartido ─────────────────────────────────────────────────────────────
_CSS = """<style>
*{box-sizing:border-box;}
body{
    font-family:'Segoe UI',Arial,sans-serif;
    background:
        radial-gradient(circle at 10% 15%, #ffd8a8 0%, rgba(255,216,168,0) 36%),
        radial-gradient(circle at 92% 10%, #ffb86b 0%, rgba(255,184,107,0) 32%),
        linear-gradient(140deg, #fff4e6 0%, #ffe4c7 45%, #ffd0a8 100%);
    min-height:100vh;
    padding:16px;
    margin:0;
    color:#431407;
}
h2{
    color:#7c2d12;
    font-size:1.6em;
    font-weight:800;
    margin:0;
    letter-spacing:-0.3px;
}
h3{
    color:#9a3412;
    margin:0 0 10px 0;
    font-size:1.05em;
    font-weight:700;
    display:flex;
    align-items:center;
    gap:6px;
}
h3::before{
    content:'';
    display:inline-block;
    width:4px;
    height:18px;
    background:linear-gradient(180deg,#f97316,#ea580c);
    border-radius:3px;
    flex-shrink:0;
}
h4{color:#9a3412;margin:0 0 8px 0;font-size:0.95em;font-weight:700;}

/* ── Header bar ── */
.hdr{
    display:flex;justify-content:space-between;align-items:center;
    margin-bottom:16px;gap:10px;flex-wrap:wrap;
    background:linear-gradient(110deg,rgba(255,255,255,0.95),rgba(255,247,237,0.92));
    border:1px solid #fdba74;
    border-radius:14px;
    padding:14px 18px;
    box-shadow:0 8px 20px rgba(154,52,18,0.12);
}
.hdr-title-wrap{display:flex;align-items:center;gap:12px;}
.hdr-chip{
    display:inline-block;background:linear-gradient(135deg,#f97316,#ea580c);
    color:#fff;font-weight:800;border-radius:999px;
    padding:4px 12px;font-size:0.78em;letter-spacing:0.4px;
    box-shadow:0 2px 6px rgba(234,88,12,0.35);
}

/* ── Buttons ── */
.btn{
    display:inline-block;
    background:linear-gradient(135deg,#f97316,#ea580c);
    color:white;padding:9px 15px;text-decoration:none;
    border-radius:7px;font-weight:700;border:none;cursor:pointer;
    font-size:13px;box-shadow:0 2px 6px rgba(234,88,12,0.3);
    transition:filter .15s,transform .1s;
}
.btn:hover{filter:brightness(1.08);transform:translateY(-1px);}
.btn-sec{
    background:#fff;color:#9a3412;
    border:1px solid #fdba74;
    box-shadow:0 1px 3px rgba(154,52,18,0.08);
}
.btn-sec:hover{background:#fff7ed;filter:none;transform:none;}
.btn-danger{background:linear-gradient(135deg,#dc2626,#b91c1c);color:#fff;box-shadow:0 2px 5px rgba(185,28,28,0.3);}
.btn-danger:hover{filter:brightness(1.06);}
.btn-sm{padding:5px 10px;font-size:12px;}

/* ── Panels ── */
.panel{
    background:linear-gradient(160deg,#ffffff,#fffdf9);
    border:1px solid #fed7aa;
    border-radius:12px;
    padding:16px;
    margin-bottom:16px;
    box-shadow:0 4px 14px rgba(154,52,18,0.08);
}

/* ── KPIs ── */
.kpis{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px;margin-bottom:16px;}
.kpi{
    background:linear-gradient(150deg,#fff,#fff7ed);
    border:1px solid #fed7aa;
    border-radius:10px;
    padding:14px 16px;
    box-shadow:0 3px 10px rgba(154,52,18,0.09);
    border-left:4px solid #f97316;
    transition:transform .15s;
}
.kpi:hover{transform:translateY(-2px);}
.kpi .t{font-size:11px;color:#9a3412;font-weight:600;text-transform:uppercase;letter-spacing:0.4px;margin-bottom:6px;}
.kpi .v{font-size:26px;font-weight:800;color:#7c2d12;line-height:1;}

/* ── Gantt ── */
:root{
    --g-col-label: 260px;
    --g-col-need: 150px;
    --g-col-actions: 80px;
}
.gantt-wrap{width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch;border:1px solid #fed7aa;border-radius:10px;box-shadow:0 3px 10px rgba(154,52,18,0.07);}
.gantt-canvas{min-width:980px;}
.g-head{display:grid;grid-template-columns:var(--g-col-label) var(--g-col-need) 1fr var(--g-col-actions);background:linear-gradient(90deg,#f97316,#ea580c);color:#fff;border-radius:10px 10px 0 0;}
.g-label-h,.g-act-h{padding:9px 10px;font-weight:700;font-size:12px;display:flex;align-items:center;}
.g-timeline-h{position:relative;height:38px;overflow:hidden;}
.g-months-strip{position:absolute;inset:0;}
.g-month{position:absolute;top:0;height:50%;display:flex;align-items:center;padding:0 6px;font-size:10px;font-weight:700;border-left:1px solid rgba(255,255,255,0.35);white-space:nowrap;overflow:hidden;}
.g-weeks-strip{position:absolute;left:0;right:0;top:50%;height:50%;display:flex;}
.g-week-tick{position:absolute;top:0;height:100%;display:flex;align-items:center;font-size:9px;color:rgba(255,255,255,0.88);padding-left:3px;border-left:1px solid rgba(255,255,255,0.2);}
.g-body{background:#fff;border-radius:0 0 10px 10px;}
.g-row{display:grid;grid-template-columns:var(--g-col-label) var(--g-col-need) 1fr var(--g-col-actions);border-bottom:1px solid #ffedd5;min-height:38px;}
.g-row:last-child{border-bottom:none;}
.g-row:hover{background:#fffcf8;}
.g-label{padding:9px 10px;display:flex;flex-direction:column;justify-content:center;gap:3px;}
.g-need{padding:9px 10px;display:flex;align-items:center;justify-content:center;font-size:12px;font-weight:700;color:#7c2d12;background:#fffaf5;border-left:1px solid #ffedd5;border-right:1px solid #ffedd5;}
.g-dot{display:inline-block;width:9px;height:9px;border-radius:50%;margin-right:5px;vertical-align:middle;flex-shrink:0;}
.g-ot{font-weight:800;color:#431407;font-size:13px;}
.g-sub{font-size:11px;color:#9a3412;margin-left:14px;}
.g-chips{display:flex;flex-wrap:wrap;gap:3px;margin-left:14px;margin-top:2px;}
.g-chip{background:#ffedd5;color:#9a3412;border:1px solid #fdba74;border-radius:999px;padding:1px 7px;font-size:10px;font-weight:600;}
.g-track{position:relative;min-height:38px;}
.g-gridline{position:absolute;top:0;bottom:0;width:1px;background:#ffedd5;z-index:0;pointer-events:none;}
.g-today-line{position:absolute;top:0;bottom:0;width:2px;background:#ef4444;opacity:.75;z-index:2;pointer-events:none;}
.g-today-line::after{content:"hoy";position:absolute;top:2px;left:4px;font-size:9px;color:#ef4444;white-space:nowrap;font-weight:700;}
.g-bar{position:absolute;top:50%;transform:translateY(-50%);height:20px;border-radius:4px;display:flex;align-items:center;font-size:10px;color:#fff;font-weight:700;padding:0 6px;white-space:nowrap;overflow:hidden;cursor:default;box-shadow:0 2px 6px rgba(0,0,0,.22);z-index:1;transition:filter .15s;-webkit-print-color-adjust:exact;print-color-adjust:exact;}
.g-bar:hover{filter:brightness(1.14);}
.g-out-range{font-size:11px;color:#9a3412;padding:4px 8px;font-style:italic;position:absolute;top:50%;transform:translateY(-50%);}
.g-empty{padding:28px;text-align:center;color:#9a3412;font-style:italic;font-size:14px;}
.g-act{display:flex;align-items:center;justify-content:center;gap:4px;padding:6px;}
.g-btn{font-size:14px;padding:4px 7px;border-radius:5px;background:#fff;border:1px solid #fdba74;text-decoration:none;cursor:pointer;color:#9a3412;}
.g-btn:hover{background:#ffedd5;border-color:#f97316;}
.g-btn-red{color:#dc2626;border-color:#fca5a5;}
.g-btn-red:hover{background:#fee2e2;border-color:#f87171;}

/* ── Table ── */
.tbl{width:100%;border-collapse:collapse;background:#fff;border-radius:10px;overflow:hidden;box-shadow:0 2px 8px rgba(154,52,18,0.07);}
.tbl th,.tbl td{padding:11px 10px;border-bottom:1px solid #ffedd5;text-align:left;font-size:13px;}
.tbl th{background:linear-gradient(90deg,#f97316,#ea580c);color:#fff;font-weight:700;font-size:12px;text-transform:uppercase;letter-spacing:0.3px;}
.tbl tr:last-child td{border-bottom:none;}
.tbl tr:hover td{background:#fff7ed;}
.dot{display:inline-block;width:10px;height:10px;border-radius:50%;margin-right:6px;vertical-align:middle;}

/* ── Form ── */
.form-grid{display:grid;grid-template-columns:1fr 1fr;gap:14px;}
.form-group{display:flex;flex-direction:column;gap:5px;}
.form-group.full{grid-column:1/-1;}
label{font-size:13px;font-weight:700;color:#9a3412;}
input[type=text],input[type=date],input[type=number],select,textarea{
    padding:9px 12px;border:1px solid #fdba74;border-radius:7px;
    background:#fffaf5;font-size:14px;width:100%;
    transition:border-color .15s,background .15s;
}
input:focus,select:focus,textarea:focus{outline:none;border-color:#f97316;background:#fff;box-shadow:0 0 0 3px rgba(249,115,22,0.12);}
.rec-table{width:100%;border-collapse:collapse;margin-top:8px;}
.rec-table th,.rec-table td{padding:8px 10px;border-bottom:1px solid #ffedd5;font-size:13px;}
.rec-table th{background:#fff7ed;color:#9a3412;font-weight:700;}
.err{background:#fee2e2;border:1px solid #fecaca;color:#991b1b;padding:10px;border-radius:8px;margin-bottom:10px;}
.ok{background:#dcfce7;border:1px solid #86efac;color:#166534;padding:10px;border-radius:8px;margin-bottom:10px;}

/* ── Section divider chip ── */
.section-chip{
    display:inline-flex;align-items:center;gap:6px;
    background:linear-gradient(135deg,#fff7ed,#ffedd5);
    border:1px solid #fdba74;border-radius:999px;
    padding:4px 12px;font-size:12px;font-weight:700;color:#9a3412;
    box-shadow:0 1px 4px rgba(154,52,18,0.1);
}

.tbl-compact td,.tbl-compact th{padding:5px 8px!important;font-size:12px!important;}
@media print{*{-webkit-print-color-adjust:exact!important;print-color-adjust:exact!important;}}
/* Helpers for responsive layout in index page */
.hdr-actions{display:flex;gap:8px;flex-wrap:wrap;}
.filters-form,.cumpl-filter-form{display:flex;align-items:center;gap:10px;flex-wrap:wrap;}
.gantt-toolbar,.cumpl-head{display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;flex-wrap:wrap;gap:8px;}
.cumpl-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-top:14px;}
.collapsible-header{display:flex;justify-content:space-between;align-items:center;cursor:pointer;user-select:none;padding:4px 0;}
.collapsible-header h3{margin:0;}
.collapsible-toggle{background:#fff7ed;border:1px solid #fdba74;border-radius:6px;padding:4px 12px;font-size:12px;font-weight:700;color:#9a3412;cursor:pointer;white-space:nowrap;flex-shrink:0;}
.collapsible-body{overflow:hidden;transition:max-height 0.3s ease;}
.desvio-legend-grid{display:grid;grid-template-columns:1fr 1fr;gap:0 12px;}
@media(max-width:800px){
    :root{
        --g-col-label: 210px;
        --g-col-need: 120px;
        --g-col-actions: 92px;
    }
    .gantt-canvas{min-width:900px;}
    .form-grid{grid-template-columns:1fr;}
    .hdr{padding:12px;}
    .kpi .v{font-size:22px;}
    .filters-form,.cumpl-filter-form{display:grid;grid-template-columns:1fr;gap:8px;}
    .filters-form input[type=date],.filters-form select,
    .cumpl-filter-form input[type=date],.cumpl-filter-form select,
    .filters-form .btn,.cumpl-filter-form .btn{width:100%!important;min-width:0!important;}
    .cumpl-grid{grid-template-columns:1fr;}
    .desvio-legend-grid{grid-template-columns:1fr;}
    .hdr-actions{width:100%;}
    .hdr-actions .btn{flex:1 1 100%;text-align:center;}
}
@media(max-width:560px){
    .gantt-canvas{min-width:840px;}
    .g-timeline-h{height:42px;}
    .g-row{min-height:42px;}
    .g-label-h,.g-act-h{font-size:11px;padding:8px 8px;}
    .g-label,.g-need{padding:8px;}
}
</style>"""


# ── Gantt renderer ─────────────────────────────────────────────────────────────
def _gantt_html(entradas, fi_vista, ff_vista, operarios_disponibles=0, es_obra=False):
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
        hitos = e.get("hitos") or []
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
                    f'<div class="g-bar" style="left:{left:.2f}%;width:{width:.2f}%;top:35%;height:8px;'
                    f'background:repeating-linear-gradient(45deg,#94a3b8 0,#94a3b8 3px,#cbd5e1 3px,#cbd5e1 7px);'
                    f'opacity:0.7;box-shadow:none;" title="{html_lib.escape(tip)}"></div>'
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

        # ── Marcador de fecha de entrega (rojo) ──
        entrega_html = ""
        if fecha_nec and fi_vista <= fecha_nec <= ff_vista:
            hito_pct = (fecha_nec - fi_vista).days / total_dias * 100
            hito_tip = html_lib.escape(f"HITO – Fecha de entrega: {_fmt(fecha_nec)}")
            entrega_html = (
                f'<div style="position:absolute;left:{hito_pct:.2f}%;top:1px;'
                f'transform:translateX(-50%);font-size:13px;color:#dc2626;z-index:4;'
                f'pointer-events:none;line-height:1;" title="{hito_tip}">◆</div>'
            )

        # ── HITOS custom: marcadores negros (fecha + titulo) ──
        hitos_html = ""
        hitos_label_items = []
        for h in hitos:
            h_fecha = _parse_date(h.get("fecha"))
            h_titulo = str(h.get("titulo") or "").strip() or "Hito"
            if not h_fecha or not (fi_vista <= h_fecha <= ff_vista):
                continue
            h_pct = (h_fecha - fi_vista).days / total_dias * 100
            h_tip = html_lib.escape(f"HITO: {h_titulo} | {_fmt(h_fecha)}")
            h_label = html_lib.escape(f"{_fmt(h_fecha)} - {h_titulo}")
            hitos_label_items.append(h_label)
            hitos_html += (
                f'<div style="position:absolute;left:{h_pct:.2f}%;top:2px;'
                f'transform:translateX(-50%) rotate(45deg);width:9px;height:9px;'
                f'background:#111827;z-index:5;pointer-events:auto;" title="{h_tip}"></div>'
            )

        hitos_label_html = ""
        if hitos_label_items:
            max_visible = 2
            visible_items = hitos_label_items[:max_visible]
            extra_count = len(hitos_label_items) - max_visible
            suffix = f" +{extra_count} más" if extra_count > 0 else ""
            hitos_text = " | ".join(visible_items) + suffix
            hitos_label_html = (
                f'<div style="margin-left:14px;margin-top:2px;font-size:10px;color:#111827;font-weight:700;line-height:1.2;">'
                f'<span style="display:inline-block;width:8px;height:8px;background:#111827;transform:rotate(45deg);margin-right:5px;vertical-align:middle;"></span>'
                f'HITO: {hitos_text}'
                f'</div>'
            )

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
                <div style="display:flex;align-items:baseline;gap:4px;flex-wrap:wrap;line-height:1.3;"><span class="g-dot" style="{dot_style}"></span><span class="g-ot">{obra}</span><span class="g-sub" style="margin-left:0;">OT{ot_id}‑{titulo}</span></div>
                {hitos_label_html}
                <div class="g-chips">{rec_chips}{avance_chip}{sub_chip}</div>
            </div>
            <div class="g-need">{_fmt(fecha_nec)}</div>
            <div class="g-track">
                {gridlines_html}
                {today_html}
                {bar_html}
                {entrega_html}
                {hitos_html}
            </div>
            {'' if es_obra else f'''<div class="g-act">
                <form method="post" action="/modulo/programacion/reordenar" style="display:inline;">
                    <input type="hidden" name="id" value="{prog_id}">
                    <input type="hidden" name="dir" value="up">
                    <button type="submit" class="g-btn" title="Subir">↑</button>
                </form>
                <form method="post" action="/modulo/programacion/reordenar" style="display:inline;">
                    <input type="hidden" name="id" value="{prog_id}">
                    <input type="hidden" name="dir" value="down">
                    <button type="submit" class="g-btn" title="Bajar">↓</button>
                </form>
                <a href="/modulo/programacion/editar/{prog_id}" class="g-btn" title="Editar">✏️</a>
                <form method="post" action="/modulo/programacion/eliminar" style="display:inline;"
                      onsubmit="return confirm(\'¿Eliminar esta programación?\');">
                    <input type="hidden" name="id" value="{prog_id}">
                    <button type="submit" class="g-btn g-btn-red" title="Eliminar">🗑</button>
                </form>
            </div>'''}
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

    col_grid = "var(--g-col-label) var(--g-col-need) 1fr" if es_obra else "var(--g-col-label) var(--g-col-need) 1fr var(--g-col-actions)"
    footer_html = f"""
    <div style="border-top:2px solid #f97316;">
        <div style="display:grid;grid-template-columns:{col_grid};background:#fff7ed;min-height:32px;border-bottom:1px solid #fed7aa;">
            <div style="padding:0 10px;font-size:11px;font-weight:700;color:#9a3412;display:flex;align-items:center;gap:5px;white-space:nowrap;">
                📊 Rec. asignados / sem.
            </div>
            <div></div>
            <div style="position:relative;height:32px;">{assigned_cells}</div>
            {'<div></div>' if not es_obra else ''}
        </div>
        <div style="display:grid;grid-template-columns:{col_grid};background:#fff7ed;min-height:32px;">
            <div style="padding:0 10px;font-size:11px;font-weight:700;color:#9a3412;display:flex;align-items:center;gap:5px;white-space:nowrap;">
                👷 Operarios disponibles
            </div>
            <div></div>
            <div style="position:relative;height:32px;">{avail_cells}</div>
            {'<div></div>' if not es_obra else ''}
        </div>
    </div>"""

    return f"""
    <div class="gantt-wrap">
        <div class="gantt-canvas">
            <div class="g-head">
                <div class="g-label-h">OT / Obra</div>
                <div class="g-label-h">Fecha necesidad</div>
                <div class="g-timeline-h">
                    <div class="g-months-strip">{meses_html}</div>
                    <div class="g-weeks-strip">{weeks_html}</div>
                </div>
                {'' if es_obra else '<div class="g-act-h">Acc.</div>'}
            </div>
            <div class="g-body">
                {rows_html}
            </div>
            {footer_html}
            <div style="padding:6px 14px 8px;font-size:11px;color:#6b7280;display:flex;gap:18px;align-items:center;flex-wrap:wrap;border-top:1px solid #ffedd5;background:#fffaf5;border-radius:0 0 10px 10px;">
                <span style="font-weight:700;color:#9a3412;">Leyenda:</span>
                <span><span style="display:inline-block;width:12px;height:3px;background:#ef4444;border-radius:2px;vertical-align:middle;margin-right:3px;"></span>Hoy</span>
                <span><span style="color:#dc2626;font-size:13px;vertical-align:middle;margin-right:3px;">◆</span>Fecha de entrega</span>
                <span><span style="display:inline-block;width:9px;height:9px;background:#111827;transform:rotate(45deg);vertical-align:middle;margin-right:5px;"></span>HITO</span>
                <span><span style="display:inline-block;width:12px;height:8px;background:#16a34a;border-radius:2px;vertical-align:middle;opacity:0.9;margin-right:3px;"></span>Avance real</span>
            </div>
        </div>
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
    hitos = prog.get("hitos", []) if es_edicion else []
    if not hitos:
        hitos = [{"titulo": "", "fecha": ""}]
    obs_val = html_lib.escape(str(prog.get("observaciones") or "")) if es_edicion else ""

    hitos_rows_html = ""
    for h in hitos:
        ht = html_lib.escape(str(h.get("titulo") or ""))
        hf = html_lib.escape(str(h.get("fecha") or ""))
        hitos_rows_html += f'''
        <div class="hito-row" style="display:grid;grid-template-columns:1fr 180px auto;gap:8px;margin-bottom:8px;">
            <input type="text" name="hito_titulo[]" value="{ht}" maxlength="120" placeholder="Título del hito">
            <input type="date" name="hito_fecha[]" value="{hf}">
            <button type="button" class="btn btn-sec btn-sm" style="width:auto;" onclick="eliminarHito(this)">Quitar</button>
        </div>
        '''

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
            <label>Hitos (opcional) - Podés agregar varios</label>
            <div id="hitos-wrap">{hitos_rows_html}</div>
            <button type="button" class="btn btn-sec btn-sm" style="width:auto;" onclick="agregarHito()">+ Agregar hito</button>
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
function agregarHito() {{
    const wrap = document.getElementById('hitos-wrap');
    const row = document.createElement('div');
    row.className = 'hito-row';
    row.style.display = 'grid';
    row.style.gridTemplateColumns = '1fr 180px auto';
    row.style.gap = '8px';
    row.style.marginBottom = '8px';
    row.innerHTML = '<input type="text" name="hito_titulo[]" maxlength="120" placeholder="Título del hito">'
        + '<input type="date" name="hito_fecha[]">'
        + '<button type="button" class="btn btn-sec btn-sm" style="width:auto;" onclick="eliminarHito(this)">Quitar</button>';
    wrap.appendChild(row);
}}
function eliminarHito(btn) {{
    const rows = document.querySelectorAll('#hitos-wrap .hito-row');
    if (rows.length <= 1) {{
        const r = rows[0];
        if (!r) return;
        const t = r.querySelector('input[name="hito_titulo[]"]');
        const f = r.querySelector('input[name="hito_fecha[]"]');
        if (t) t.value = '';
        if (f) f.value = '';
        return;
    }}
    btn.closest('.hito-row').remove();
}}
document.getElementById('main-form').addEventListener('submit', function(e) {{
    const fi = document.getElementById('fi').value;
    const ff = document.getElementById('ff').value;
    if (fi && ff && ff < fi) {{
        e.preventDefault();
        alert('La fecha de fin debe ser igual o posterior a la fecha de inicio.');
        return;
    }}
    const titulos = Array.from(document.querySelectorAll('input[name="hito_titulo[]"]'));
    const fechas = Array.from(document.querySelectorAll('input[name="hito_fecha[]"]'));
    for (let i = 0; i < Math.max(titulos.length, fechas.length); i++) {{
        const t = ((titulos[i] && titulos[i].value) || '').trim();
        const f = ((fechas[i] && fechas[i].value) || '').trim();
        if ((t && !f) || (!t && f)) {{
            e.preventDefault();
            alert('Cada HITO debe tener título y fecha.');
            return;
        }}
    }}
    calcHoras();
}});
calcHoras();
</script>
</body></html>"""


# ── Routes ─────────────────────────────────────────────────────────────────────
@programacion_bp.route("/modulo/programacion")
def programacion_index():
    es_obra = _es_usuario_obra()
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
               COALESCE(p.hito_titulo, ''), COALESCE(p.hito_fecha, ''),
               COALESCE(ot.obra, ''), COALESCE(ot.titulo, ''),
               COALESCE(ot.cliente, ''), COALESCE(ot.estado, ''), COALESCE(ot.fecha_entrega, ''),
               COALESCE(ot.hs_previstas, 0)
        FROM programacion p
        LEFT JOIN ordenes_trabajo ot ON ot.id = p.ot_id
        ORDER BY COALESCE(p.orden, p.id) ASC, p.id ASC
    """).fetchall()

    # Usar avance vivo (Produccion) para toda visualizacion del modulo.
    avance_live_by_ot = {}
    for r in rows:
        try:
            ot_id_row = int(r[1] or 0)
        except Exception:
            continue
        if ot_id_row <= 0 or ot_id_row in avance_live_by_ot:
            continue
        try:
            avance_live_by_ot[ot_id_row] = max(0, min(100, int(round(calcular_avance_ot(db, ot_id_row)))))
        except Exception:
            avance_live_by_ot[ot_id_row] = 0

    prog_ids = [int(r[0]) for r in rows]
    hitos_map = {pid: [] for pid in prog_ids}
    if prog_ids:
        placeholders = ",".join(["?"] * len(prog_ids))
        hitos_rows = db.execute(
            f"""
            SELECT prog_id, COALESCE(titulo, ''), COALESCE(fecha, '')
            FROM programacion_hitos
            WHERE prog_id IN ({placeholders})
            ORDER BY prog_id ASC, fecha ASC, id ASC
            """,
            prog_ids,
        ).fetchall()
        for pid, titulo_h, fecha_h in hitos_rows:
            hitos_map.setdefault(int(pid), []).append({"titulo": str(titulo_h or ""), "fecha": str(fecha_h or "")})

    entradas = [
        {
            "id": r[0], "ot_id": r[1], "fecha_inicio": r[2], "fecha_fin": r[3],
            "hs_programadas": r[4], "cantidad_recursos": r[5], "observaciones": r[6],
            "hito_titulo": r[7], "hito_fecha": r[8],
            "hitos": (hitos_map.get(int(r[0])) or ([{"titulo": str(r[7] or ""), "fecha": str(r[8] or "")}] if (str(r[7] or "").strip() and str(r[8] or "").strip()) else [])),
            "obra": r[9], "titulo": r[10], "cliente": r[11], "estado_ot": r[12], "fecha_entrega": r[13],
            "avance": int(avance_live_by_ot.get(int(r[1] or 0), 0)),
            "es_subcontrato": float(r[14] or 0) == 0 or int(r[5] or 0) == 0,
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
    semana_num = semana_sel.isocalendar()[1]
    edit_ot_txt = (request.args.get("edit_ot") or "").strip()
    edit_ot = int(edit_ot_txt) if edit_ot_txt.isdigit() else 0
    edit_pct_txt = (request.args.get("edit_pct") or "").strip()
    try:
        edit_pct = max(0, min(100, int(float(edit_pct_txt)))) if edit_pct_txt else None
    except Exception:
        edit_pct = None
    edit_desvio = (request.args.get("edit_desvio") or "").strip()

    # Logo embebido como base64 para que funcione en ventanas de impresión
    _logo_path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "LOGO.png")
    try:
        with open(_logo_path, "rb") as _lf:
            _logo_b64 = "data:image/png;base64," + _base64.b64encode(_lf.read()).decode()
    except Exception:
        _logo_b64 = ""

    cumplimiento_rows = db.execute(
        """
        SELECT ot_id, semana_inicio, COALESCE(pct_cumplido, 0), COALESCE(desvio_codigo, '')
        FROM programacion_cumplimiento
        ORDER BY semana_inicio ASC, ot_id ASC
        """
    ).fetchall()
    cumpl_idx = {}
    for r in cumplimiento_rows:
        sem_r = _parse_date(r[1])
        if not sem_r:
            continue
        cumpl_idx[(int(r[0]), sem_r.strftime("%Y-%m-%d"))] = (float(r[2] or 0), str(r[3] or ""))

    # Cumplimiento: mostrar OTs con actividad en la semana seleccionada O con avance registrado.
    ots_activas_cumpl = db.execute("""
        SELECT id, COALESCE(obra, ''), COALESCE(titulo, ''), COALESCE(fecha_entrega, '')
        FROM ordenes_trabajo
        WHERE fecha_cierre IS NULL AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
        ORDER BY id ASC
    """).fetchall()

    for r in ots_activas_cumpl:
        try:
            oid = int(r[0] or 0)
        except Exception:
            continue
        if oid <= 0 or oid in avance_live_by_ot:
            continue
        try:
            avance_live_by_ot[oid] = max(0, min(100, int(round(calcular_avance_ot(db, oid)))))
        except Exception:
            avance_live_by_ot[oid] = 0

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
        str(r[9] or "").strip() for r in rows
        if str(r[9] or "").strip()
    } | {
        str(r[1] or "").strip() for r in ots_activas_cumpl
        if str(r[1] or "").strip()
    })
    entradas_semana = [
        {"ot_id": r[0], "obra": r[1], "titulo": r[2], "fecha_entrega": r[3]}
        for r in ots_activas_cumpl
        if int(r[0] or 0) in ot_ids_con_actividad_semana or int(avance_live_by_ot.get(int(r[0] or 0), 0)) > 0
    ]
    if obra_fil:
        entradas_semana = [e for e in entradas_semana if obra_fil.lower() in (e.get("obra") or "").lower()]

    semana_key = semana_sel.strftime("%Y-%m-%d")
    cumplimiento_rows_html = ""
    pcts_semana = []
    oninput_cumplimiento = "" if es_obra else " oninput=\"toggleDesvio({ot_id}); calcCumplimientoKPIs();\""
    disabled_cumplimiento = " disabled" if es_obra else ""
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
        <tr style="font-size:11px;">
            <td style="padding:4px 6px;"><b>OT {ot_id}</b></td>
            <td style="padding:4px 6px;">{obra}</td>
            <td style="padding:4px 6px;">{titulo}</td>
            <td style="text-align:center;padding:4px 6px;">
                <input type="number" min="0" max="100" step="1" value="{pct_val}" name="pct_{ot_id}"
                       style="width:64px;font-size:11px;padding:3px 5px;"{disabled_cumplimiento}{oninput_cumplimiento.format(ot_id=ot_id)}> %
            </td>
            <td id="desv-wrap-{ot_id}" style="{show_desv}padding:4px 6px;">
                <select name="desvio_{ot_id}" id="desv-{ot_id}" style="min-width:240px;font-size:11px;padding:3px 5px;"{disabled_cumplimiento}>{opts}</select>
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
    for i, (code, label) in enumerate(_DESVIOS.items()):
        c = desvio_stats.get(code, 0)
        pct = (c * 100.0 / total_desv) if total_desv > 0 and c > 0 else 0
        col = _colors_desv[i % len(_colors_desv)]
        if c > 0:
            desv_legend += (
                f"<div style='display:flex;align-items:center;gap:6px;margin:3px 0;'>"
                f"<div style='width:11px;height:11px;border-radius:2px;background:{col};flex-shrink:0;'></div>"
                f"<div style='font-size:11px;color:#1e293b;flex:1;'>{code}. {html_lib.escape(label)}</div>"
                f"<div style='font-size:11px;font-weight:700;color:{col};white-space:nowrap;'>{c} ({pct:.0f}%)</div>"
                f"</div>"
            )
        else:
            desv_legend += (
                f"<div style='display:flex;align-items:center;gap:6px;margin:3px 0;opacity:0.35;'>"
                f"<div style='width:11px;height:11px;border-radius:2px;background:#cbd5e1;flex-shrink:0;'></div>"
                f"<div style='font-size:11px;color:#64748b;flex:1;'>{code}. {html_lib.escape(label)}</div>"
                f"<div style='font-size:11px;color:#94a3b8;white-space:nowrap;'>0</div>"
                f"</div>"
            )

    donut_svg = _svg_donut_chart(desvio_stats, total_desv, size=220)

    week_map = {}
    for r in cumplimiento_rows:
        sem = _parse_date(r[1])
        if not sem or sem > semana_sel:
            continue
        key = sem.strftime("%Y-%m-%d")
        if key not in week_map:
            week_map[key] = {"tot": 0, "desv": 0, "pct_sum": 0.0}
        week_map[key]["tot"] += 1
        week_map[key]["pct_sum"] += float(r[2] or 0)
        if float(r[2] or 0) < 100:
            week_map[key]["desv"] += 1

    weeks_sorted = sorted(week_map.keys())
    weeks_rows_html = ""
    svg_paths = ""
    svg_points_sem = []
    svg_points_ac = []
    acum_tot = 0
    acum_pct_sum = 0.0
    if weeks_sorted:
        max_x = max(1, len(weeks_sorted) - 1)
        for i, wk in enumerate(weeks_sorted):
            dato = week_map[wk]
            # % cumplimiento promedio de esa semana
            sem_pct = (dato["pct_sum"] / dato["tot"]) if dato["tot"] else 100.0
            acum_tot += dato["tot"]
            acum_pct_sum += dato["pct_sum"]
            # % cumplimiento acumulado (promedio de todas las semanas hasta este punto)
            ac_pct = (acum_pct_sum / acum_tot) if acum_tot else 100.0
            desv_pct = (dato["desv"] * 100.0 / dato["tot"]) if dato["tot"] else 0.0
            weeks_rows_html += (
                f"<tr>"
                f"<td>{_fmt(_parse_date(wk))}</td>"
                f"<td style='text-align:center;'>{dato['tot']}</td>"
                f"<td style='text-align:center;font-weight:700;color:#16a34a;'>{sem_pct:.1f}%</td>"
                f"<td style='text-align:center;color:#9a3412;'>{dato['desv']} ({desv_pct:.0f}%)</td>"
                f"<td style='text-align:center;font-weight:700;color:#7c2d12;'>{ac_pct:.1f}%</td>"
                f"</tr>"
            )
            x = 30 + (i * 520.0 / max_x) if max_x > 0 else 30 + i * 520.0
            # 100% -> y=40, 0% -> y=190  (range 150px = 100%)
            y_sem = 190 - (sem_pct * 1.5)
            y_ac = 190 - (ac_pct * 1.5)
            svg_points_sem.append(f"{x:.1f},{y_sem:.1f}")
            svg_points_ac.append(f"{x:.1f},{y_ac:.1f}")
            lbl = _fmt(_parse_date(wk))
            svg_paths += f"<text x='{x:.1f}' y='210' font-size='10' text-anchor='middle' fill='#6b7280'>{lbl}</text>"
        line_sem = " ".join(svg_points_sem)
        line_ac = " ".join(svg_points_ac)
        chart_svg = f"""
        <svg viewBox="0 0 560 220" style="width:100%;height:auto;background:#fff;border:1px solid #ffedd5;border-radius:8px;">
            <line x1="30" y1="40" x2="30" y2="190" stroke="#fed7aa" stroke-width="1"/>
            <line x1="30" y1="115" x2="550" y2="115" stroke="#e2e8f0" stroke-width="0.8" stroke-dasharray="3 3"/>
            <line x1="30" y1="190" x2="550" y2="190" stroke="#fed7aa" stroke-width="1"/>
            <text x="6" y="45" font-size="10" fill="#9a3412">100%</text>
            <text x="10" y="120" font-size="10" fill="#9a3412">50%</text>
            <text x="14" y="194" font-size="10" fill="#9a3412">0%</text>
            <polyline fill="none" stroke="#16a34a" stroke-width="2.5" points="{line_sem}"/>
            <polyline fill="none" stroke="#f97316" stroke-width="2.5" stroke-dasharray="6 3" points="{line_ac}"/>
            {svg_paths}
            <rect x="250" y="10" width="12" height="3" fill="#16a34a" rx="1"/><text x="266" y="17" font-size="10" fill="#166534" font-weight="700">Cumplimiento semanal</text>
            <rect x="400" y="10" width="12" height="3" fill="#f97316" rx="1"/><text x="416" y="17" font-size="10" fill="#9a3412" font-weight="700">Cumplimiento acumulado</text>
        </svg>
        """
    else:
        chart_svg = "<div style='color:#9a3412;font-style:italic;padding:10px;'>Sin datos guardados todav\u00eda. Carg\u00e1 un cumplimiento y hacé click en \"Guardar\".</div>"
        weeks_rows_html = "<tr><td colspan='5' style='text-align:center;color:#64748b;'>Sin semanas guardadas todav\u00eda.</td></tr>"

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
    gantt = _gantt_html(entradas, fi_vista, ff_vista, operarios_disponibles=operarios_count, es_obra=es_obra)

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
    <div class="collapsible-header" onclick="toggleSection('cumpl-body','cumpl-toggle')">
        <h3>📋 Cumplimiento de objetivos semanales</h3>
        <button class="collapsible-toggle" id="cumpl-toggle">▼ Mostrar</button>
    </div>
    <div class="collapsible-body" id="cumpl-body" style="max-height:0;display:none;">
    <div class="cumpl-head" style="margin-top:10px;">
        <div style="display:flex;gap:8px;flex-wrap:wrap;align-items:center;">
            <a href="/modulo/programacion/cumplimientos-historial" class="btn btn-sec btn-sm">📋 Ver historial completo</a>
            {'' if es_obra else '<button onclick="printCumplimiento()" class="btn btn-sec btn-sm">🖨️ Imprimir</button>'}
        </div>
    </div>
    <form method="get" action="/modulo/programacion" class="cumpl-filter-form" style="margin-bottom:10px;">
        <label>Semana (lunes):</label>
        <input type="date" id="semana" name="semana" value="{semana_str}" style="width:170px;">
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

    {
        "<div style='margin:6px 0 10px 0;padding:8px 10px;border:1px solid #fed7aa;background:#fff7ed;color:#9a3412;border-radius:8px;font-size:12px;font-weight:600;'>Vista solo lectura para usuario OBRA: el listado de OTs y la carga de % no están disponibles para este rol.</div>"
        if es_obra else
        (
            '<form method="post" action="/modulo/programacion/cumplimiento" onsubmit="return validarDesvios();">'
            + f'<input type="hidden" id="semana_inicio" name="semana_inicio" value="{semana_str}">'
            + f'<input type="hidden" name="fi" value="{fi_str}">'
            + f'<input type="hidden" name="ff" value="{ff_str}">'
            + '<div style="overflow-x:auto;">'
            + '<table class="tbl tbl-compact">'
            + '<tr><th>OT</th><th>Obra</th><th>Título</th><th>% Cumplido</th><th>Desvío (si &lt; 100%)</th></tr>'
            + (cumplimiento_rows_html if cumplimiento_rows_html else "<tr><td colspan='5' style='text-align:center;color:#64748b;'>No hay OTs programadas en la semana seleccionada.</td></tr>")
            + '</table>'
            + '</div>'
            + '<div style="margin-top:10px;"><button type="submit" class="btn">Guardar cumplimiento semanal</button></div>'
            + '</form>'
        )
    }

    <div class="cumpl-grid">
        <div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:10px;">
            <h4>% Cumplimiento promedio por semana</h4>
            <div style="font-size:12px;color:#475569;margin-bottom:8px;line-height:1.4;">
                <b style='color:#166534;'>&#9644; Verde</b>: promedio de % cumplido de esa semana.
                <b style='color:#ea580c;'>&#9644; Naranja punteado</b>: promedio acumulado desde la primera semana guardada.
                Se actualiza automáticamente al guardar cada semana.
            </div>
            {chart_svg}
            <div style="margin-top:10px;overflow-x:auto;">
                <table class="tbl" style="font-size:12px;">
                    <tr>
                        <th>Semana</th>
                        <th>OTs</th>
                        <th>% Cumplido semana</th>
                        <th>Con desv\u00edo</th>
                        <th>% Cumplido acum.</th>
                    </tr>
                    {weeks_rows_html}
                </table>
            </div>
        </div>
        <div style="background:#fff;border:1px solid #e5e7eb;border-radius:8px;padding:10px;display:flex;flex-direction:column;">
            <h4 style="margin:0 0 8px 0;">Distribución acumulada de causas de desvío</h4>
            <div style="display:flex;justify-content:center;margin-bottom:10px;">{donut_svg}</div>
            <div class="desvio-legend-grid">{desv_legend}</div>
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
    function aplicarEdicionDesdeQuery() {{
        var otId = {edit_ot};
        if (!otId) return;
        var semanaQuery = '{semana_str}';
        var pctQuery = {"null" if edit_pct is None else str(edit_pct)};
        var desvioQuery = '{html_lib.escape(edit_desvio)}';
        var semanaFiltro = document.getElementById('semana');
        var semanaHidden = document.getElementById('semana_inicio');
        if (semanaFiltro) semanaFiltro.value = semanaQuery;
        if (semanaHidden) semanaHidden.value = semanaQuery;
        var inpPct = document.getElementById('pct_' + otId);
        if (!inpPct) return;
        if (pctQuery !== null) inpPct.value = pctQuery;
        var selDesvio = document.getElementById('desv-' + otId);
        if (selDesvio) selDesvio.value = desvioQuery || '';
        toggleDesvio(otId);
        inpPct.focus();
        inpPct.scrollIntoView({{ behavior: 'smooth', block: 'center' }});
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
        if (k && n > 0) k.textContent = ((sum / n).toFixed(1) + '%');
    }}
    document.addEventListener('DOMContentLoaded', function() {{ calcCumplimientoKPIs(); aplicarEdicionDesdeQuery(); }});
    </script>
    </div>
</div>
"""

    return f"""<!DOCTYPE html><html>
<head><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Programación de Fabricación</title>{_CSS}
<script>
var _LOGO_URI = '{_logo_b64}';
var _SEMANA_NUM = {semana_num};
var _PUEDE_IMPRIMIR = {'false' if es_obra else 'true'};
function _printHeader(title) {{
    var logoHtml = _LOGO_URI ? '<img src="' + _LOGO_URI + '" style="height:54px;display:block;">' : '';
    var semanaTag = '<div style="display:inline-block;margin-top:4px;background:#fff7ed;border:1px solid #fdba74;border-radius:999px;padding:2px 10px;font-size:12px;font-weight:700;color:#9a3412;">Semana de control N\u00b0' + _SEMANA_NUM + '</div>';
    return '<div style="display:flex;justify-content:space-between;align-items:center;padding:0 0 12px 0;border-bottom:3px solid #f97316;margin-bottom:16px;">'
      + '<div style="display:flex;align-items:center;gap:12px;">'
        + logoHtml
        + '<div>'
          + '<div style="font-size:20px;font-weight:800;color:#7c2d12;">' + title + '</div>'
          + '<div style="font-size:13px;color:#9a3412;font-style:italic;margin-bottom:4px;">Programaci\u00f3n \u00b7 Fabricaci\u00f3n Estructuras Met\u00e1licas</div>'
          + semanaTag
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
    if (!_PUEDE_IMPRIMIR) {{
        alert('Sin permisos para imprimir.');
        return null;
    }}
    var section = document.getElementById(sectionId);
    if (!section) return null;
    var printWin = window.open('', '_blank', 'width=1400,height=900');
    if (!printWin) {{ alert('El navegador bloqu\u00f3 la ventana emergente. Permit\u00ed ventanas emergentes para este sitio.'); return null; }}
    printWin.document.write('<html><head><meta charset="utf-8"><title>' + title + '</title>');
    var styles = document.querySelectorAll('style');
    styles.forEach(function(s) {{ printWin.document.write(s.outerHTML); }});
    printWin.document.write('<style>@page{{size:A3 landscape;margin:8mm;}}body{{padding:0;background:#fff;}}button,form,.btn,.g-btn{{display:none!important;}}input,select{{pointer-events:none;border:1px solid #ddd;}}.g-track{{overflow:visible!important;}}.panel{{border:none!important;padding:0!important;}}h3{{display:none!important;}}*{{-webkit-print-color-adjust:exact!important;print-color-adjust:exact!important;}}</style>');
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
    if (pw) setTimeout(function(){{ pw.print(); }}, 800);
}}
function printCumplimiento() {{
    var pw = _openPrintWin('Cumplimiento de Objetivos Semanales', 'cumplimiento-section');
    if (pw) setTimeout(function(){{ pw.print(); }}, 800);
}}
</script>
</head>
<body>
<div class="hdr">
    <div class="hdr-title-wrap">
        <span class="hdr-chip">📆 Planificación</span>
        <h2>Programación de Fabricación</h2>
    </div>
    <div class="hdr-actions">
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
    <form method="get" action="/modulo/programacion" class="filters-form">
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
    <div class="gantt-toolbar">
        <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
            <h3>Diagrama de Gantt</h3>
            <a href="/modulo/programacion?vista=trimestral{obra_qs}" class="btn btn-sm" style="{btn_trimestral_active}">📊 Trimestral</a>
            <a href="/modulo/programacion?vista=mensual{obra_qs}" class="btn btn-sm" style="{btn_mensual_active}">📅 Mensual</a>
            <a href="/modulo/programacion?vista=semana{obra_qs}" class="btn btn-sm" style="{btn_semana_active}">📆 Semana</a>
        </div>
            {'' if es_obra else '<button onclick="printGantt()" class="btn btn-sec btn-sm">🖨️ Imprimir</button>'}
    </div>
    {gantt}
</div>

{cumplimiento_panel}

<div class="panel" id="detalle-wrapper">
    <div class="collapsible-header" onclick="toggleSection('detalle-body','detalle-toggle')">
        <h3>📊 Detalle — {len(entradas)} programaciones · {total_hs:.0f} hs planificadas totales</h3>
        <button class="collapsible-toggle" id="detalle-toggle">▼ Mostrar</button>
    </div>
    <div class="collapsible-body" id="detalle-body" style="max-height:0;display:none;">
        <div style="margin:8px 0 12px 0;"><span class="section-chip">📊 Tabla resumen</span></div>
        {tabla_html}
    </div>
</div>

<script>
function toggleSection(bodyId, toggleId) {{
    var body = document.getElementById(bodyId);
    var btn = document.getElementById(toggleId);
    if (!body) return;
    if (body.style.display === 'none' || body.style.maxHeight === '0px') {{
        body.style.display = 'block';
        body.style.maxHeight = body.scrollHeight + 'px';
        if (btn) btn.textContent = '▲ Ocultar';
    }} else {{
        body.style.maxHeight = '0';
        setTimeout(function(){{ body.style.display = 'none'; }}, 300);
        if (btn) btn.textContent = '▼ Mostrar';
    }}
}}
</script>

</body></html>"""


@programacion_bp.route("/modulo/programacion/cumplimiento", methods=["POST"])
def programacion_cumplimiento():
    if _es_usuario_obra():
        return redirect("/modulo/programacion")

    db = get_db()
    semana_inicio = (request.form.get("semana_inicio") or "").strip()
    fi = (request.form.get("fi") or "").strip()
    ff = (request.form.get("ff") or "").strip()

    semana_d = _parse_date(semana_inicio)
    if not semana_d:
        return redirect("/modulo/programacion")

    semana_key = semana_d.strftime("%Y-%m-%d")

    ot_ids_form = []
    for key in request.form:
        if key.startswith("pct_"):
            try:
                ot_ids_form.append(int(key[4:]))
            except Exception:
                pass

    for ot_id in ot_ids_form:
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
    # Excluir OTs que ya tienen al menos una entrada en programacion
    ya_programadas = {r[0] for r in db.execute(
        "SELECT DISTINCT ot_id FROM programacion"
    ).fetchall()}
    ots_activas_todas = db.execute("""
        SELECT id, COALESCE(obra, ''), COALESCE(titulo, ''), COALESCE(estado, '')
        FROM ordenes_trabajo
        WHERE fecha_cierre IS NULL AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
        ORDER BY id DESC
    """).fetchall()
    ots_activas = [r for r in ots_activas_todas if int(r[0]) not in ya_programadas]

    if request.method == "POST":
        ot_id_txt = (request.form.get("ot_id") or "").strip()
        fecha_inicio = (request.form.get("fecha_inicio") or "").strip()
        fecha_fin = (request.form.get("fecha_fin") or "").strip()
        cant_rec_txt = (request.form.get("cantidad_recursos") or "0").strip()
        hitos = _normalizar_hitos_desde_form(request.form)
        observaciones = (request.form.get("observaciones") or "").strip()

        error = ""
        if not ot_id_txt.isdigit():
            error = "Seleccioná una OT válida."
        elif not fecha_inicio or not fecha_fin:
            error = "Las fechas de inicio y fin son obligatorias."
        elif fecha_fin < fecha_inicio:
            error = "La fecha de fin debe ser igual o posterior a la fecha de inicio."
        else:
            for h in hitos:
                if not h.get("titulo") or not h.get("fecha"):
                    error = "Cada HITO debe tener título y fecha."
                    break
                if h["fecha"] < fecha_inicio or h["fecha"] > fecha_fin:
                    error = "La fecha de cada HITO debe quedar dentro del rango inicio-fin."
                    break

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

        # Asignar orden = max(orden)+1 para que aparezca al final
        max_orden = db.execute("SELECT COALESCE(MAX(COALESCE(orden, id)), 0) FROM programacion").fetchone()[0]
        cur = db.execute(
            """
            INSERT INTO programacion (ot_id, fecha_inicio, fecha_fin, hs_programadas, cantidad_recursos, hito_titulo, hito_fecha, observaciones, orden)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (int(ot_id_txt), fecha_inicio, fecha_fin, hs, cant_rec, "", None, observaciones, int(max_orden) + 1),
        )
        prog_id = int(cur.lastrowid)
        for h in hitos:
            db.execute(
                "INSERT INTO programacion_hitos (prog_id, titulo, fecha) VALUES (?, ?, ?)",
                (prog_id, h["titulo"], h["fecha"]),
            )
        db.commit()
        return redirect("/modulo/programacion")

    return _form_html(ots_activas)


@programacion_bp.route("/modulo/programacion/editar/<int:prog_id>", methods=["GET", "POST"])
def programacion_editar(prog_id):
    db = get_db()
    row = db.execute(
        "SELECT id, ot_id, fecha_inicio, fecha_fin, hs_programadas, COALESCE(cantidad_recursos, 1), COALESCE(hito_titulo, ''), COALESCE(hito_fecha, ''), observaciones "
        "FROM programacion WHERE id = ?",
        (prog_id,),
    ).fetchone()
    if not row:
        return redirect("/modulo/programacion")

    hitos_edit = db.execute(
        """
        SELECT COALESCE(titulo, ''), COALESCE(fecha, '')
        FROM programacion_hitos
        WHERE prog_id = ?
        ORDER BY fecha ASC, id ASC
        """,
        (prog_id,),
    ).fetchall()
    hitos = [{"titulo": str(h[0] or ""), "fecha": str(h[1] or "")} for h in hitos_edit]
    if not hitos and str(row[6] or "").strip() and str(row[7] or "").strip():
        hitos = [{"titulo": str(row[6] or ""), "fecha": str(row[7] or "")}]

    prog = {
        "id": row[0], "ot_id": row[1], "fecha_inicio": row[2], "fecha_fin": row[3],
        "hs_programadas": row[4], "cantidad_recursos": row[5], "hito_titulo": row[6], "hito_fecha": row[7], "observaciones": row[8],
        "hitos": hitos,
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
        hitos = _normalizar_hitos_desde_form(request.form)
        observaciones = (request.form.get("observaciones") or "").strip()

        error = ""
        if not ot_id_txt.isdigit():
            error = "Seleccioná una OT válida."
        elif not fecha_inicio or not fecha_fin:
            error = "Las fechas son obligatorias."
        elif fecha_fin < fecha_inicio:
            error = "La fecha de fin debe ser igual o posterior a la fecha de inicio."
        else:
            for h in hitos:
                if not h.get("titulo") or not h.get("fecha"):
                    error = "Cada HITO debe tener título y fecha."
                    break
                if h["fecha"] < fecha_inicio or h["fecha"] > fecha_fin:
                    error = "La fecha de cada HITO debe quedar dentro del rango inicio-fin."
                    break

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
            SET ot_id=?, fecha_inicio=?, fecha_fin=?, hs_programadas=?, cantidad_recursos=?, hito_titulo=?, hito_fecha=?, observaciones=?
            WHERE id=?
            """,
            (int(ot_id_txt), fecha_inicio, fecha_fin, hs, cant_rec, "", None, observaciones, prog_id),
        )
        db.execute("DELETE FROM programacion_hitos WHERE prog_id = ?", (prog_id,))
        for h in hitos:
            db.execute(
                "INSERT INTO programacion_hitos (prog_id, titulo, fecha) VALUES (?, ?, ?)",
                (prog_id, h["titulo"], h["fecha"]),
            )
        db.commit()
        return redirect("/modulo/programacion")

    return _form_html(ots_activas, prog=prog)


@programacion_bp.route("/modulo/programacion/eliminar", methods=["POST"])
def programacion_eliminar():
    db = get_db()
    id_txt = (request.form.get("id") or "").strip()
    if id_txt.isdigit():
        db.execute("DELETE FROM programacion_hitos WHERE prog_id = ?", (int(id_txt),))
        db.execute("DELETE FROM programacion WHERE id = ?", (int(id_txt),))
        db.commit()
    return redirect("/modulo/programacion")


@programacion_bp.route("/modulo/programacion/reordenar", methods=["POST"])
def programacion_reordenar():
    db = get_db()
    id_txt = (request.form.get("id") or "").strip()
    direction = (request.form.get("dir") or "").strip()
    if not id_txt.isdigit() or direction not in ("up", "down"):
        return redirect("/modulo/programacion")

    prog_id = int(id_txt)
    # Get all rows ordered as displayed
    all_rows = db.execute(
        "SELECT id, COALESCE(orden, id) as ord FROM programacion ORDER BY COALESCE(orden, id) ASC, id ASC"
    ).fetchall()
    ids_ordered = [r[0] for r in all_rows]

    if prog_id not in ids_ordered:
        return redirect("/modulo/programacion")

    idx = ids_ordered.index(prog_id)
    if direction == "up" and idx > 0:
        swap_idx = idx - 1
    elif direction == "down" and idx < len(ids_ordered) - 1:
        swap_idx = idx + 1
    else:
        return redirect("/modulo/programacion")

    # Swap orden values
    id_a = ids_ordered[idx]
    id_b = ids_ordered[swap_idx]
    db.execute("UPDATE programacion SET orden = ? WHERE id = ?", (swap_idx + 1, id_a))
    db.execute("UPDATE programacion SET orden = ? WHERE id = ?", (idx + 1, id_b))
    # Renormalize all others to avoid gaps
    remaining = [i for i in ids_ordered if i not in (id_a, id_b)]
    counter = len(ids_ordered) + 1
    for rid in remaining:
        db.execute("UPDATE programacion SET orden = ? WHERE id = ?", (counter, rid))
        counter += 1
    db.commit()
    return redirect("/modulo/programacion")


@programacion_bp.route("/modulo/programacion/cumplimientos-historial")
def programacion_cumplimientos_historial():
        db = get_db()
        obra_fil = (request.args.get("obra") or "").strip()
        semana_fil = (request.args.get("semana") or "").strip()

        condiciones = []
        params = []
        if obra_fil:
                condiciones.append("LOWER(TRIM(COALESCE(o.obra, ''))) LIKE ?")
                params.append(f"%{obra_fil.lower()}%")
        if semana_fil:
                condiciones.append("c.semana_inicio = ?")
                params.append(semana_fil)
        where_sql = f"WHERE {' AND '.join(condiciones)}" if condiciones else ""

        rows = db.execute(
                f"""
                SELECT c.ot_id,
                             c.semana_inicio,
                             COALESCE(c.pct_cumplido, 0),
                             COALESCE(c.desvio_codigo, ''),
                             COALESCE(o.obra, ''),
                             COALESCE(o.titulo, '')
                FROM programacion_cumplimiento c
                LEFT JOIN ordenes_trabajo o ON o.id = c.ot_id
                {where_sql}
                ORDER BY c.semana_inicio DESC, c.ot_id ASC
                """,
                params,
        ).fetchall()

        semanas = db.execute(
                """
                SELECT DISTINCT semana_inicio
                FROM programacion_cumplimiento
                WHERE semana_inicio IS NOT NULL AND TRIM(semana_inicio) <> ''
                ORDER BY semana_inicio DESC
                """
        ).fetchall()

        obras = db.execute(
                """
                SELECT DISTINCT TRIM(COALESCE(o.obra, ''))
                FROM programacion_cumplimiento c
                LEFT JOIN ordenes_trabajo o ON o.id = c.ot_id
                WHERE TRIM(COALESCE(o.obra, '')) <> ''
                ORDER BY TRIM(COALESCE(o.obra, '')) ASC
                """
        ).fetchall()

        opts_semana = '<option value="">Todas las semanas</option>'
        for (sem,) in semanas:
                sem_txt = str(sem or "").strip()
                sel = "selected" if sem_txt == semana_fil else ""
                opts_semana += f'<option value="{html_lib.escape(sem_txt)}" {sel}>{html_lib.escape(sem_txt)}</option>'

        opts_obra = '<option value="">Todas las obras</option>'
        for (obra,) in obras:
                obra_txt = str(obra or "").strip()
                sel = "selected" if obra_txt == obra_fil else ""
                opts_obra += f'<option value="{html_lib.escape(obra_txt)}" {sel}>{html_lib.escape(obra_txt)}</option>'

        filas_html = ""
        for ot_id, semana, pct, desvio, obra, titulo in rows:
                pct_i = max(0, min(100, int(round(float(pct or 0)))))
                desvio_txt = _DESVIOS.get(str(desvio or ""), str(desvio or "-")) if str(desvio or "").strip() else "-"
                edit_url = (
                        f"/modulo/programacion?semana={quote(str(semana or ''))}"
                        f"&edit_ot={int(ot_id or 0)}&edit_pct={pct_i}&edit_desvio={quote(str(desvio or ''))}"
                        f"#cumplimiento-section"
                )
                filas_html += f"""
                <tr>
                        <td>{html_lib.escape(str(semana or ''))}</td>
                        <td><b>{int(ot_id or 0)}</b></td>
                        <td>{html_lib.escape(str(obra or '-'))}</td>
                        <td>{html_lib.escape(str(titulo or '---'))}</td>
                        <td style="text-align:center;font-weight:700;">{pct_i}%</td>
                        <td>{html_lib.escape(desvio_txt)}</td>
                        <td style="text-align:center;">
                                <a href="{edit_url}" class="btn-mini">✏️ Editar</a>
                        </td>
                </tr>
                """

        if not filas_html:
                filas_html = "<tr><td colspan='7' style='text-align:center;color:#64748b;'>Sin registros para los filtros seleccionados.</td></tr>"

        return f"""
        <html>
        <head>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
        * {{ box-sizing: border-box; }}
        body {{ font-family: Arial; margin: 0; padding: 16px; background: #f3f4f6; color: #0f172a; }}
        .wrap {{ max-width: 1220px; margin: 0 auto; }}
        .top {{ display:flex; justify-content:space-between; align-items:center; gap:10px; flex-wrap:wrap; margin-bottom:12px; }}
        .btn {{ display:inline-block; background:#f97316; color:#fff; text-decoration:none; padding:10px 14px; border-radius:8px; border:none; cursor:pointer; }}
        .btn:hover {{ background:#ea580c; }}
        .panel {{ background:#fff; border:1px solid #e2e8f0; border-radius:12px; padding:14px; box-shadow:0 8px 20px rgba(15,23,42,0.06); margin-bottom:12px; }}
        .grid {{ display:grid; grid-template-columns: 1fr 1fr auto auto; gap:10px; align-items:end; }}
        label {{ display:block; font-weight:700; margin-bottom:6px; font-size:13px; color:#334155; }}
        select {{ width:100%; padding:10px; border:1px solid #cbd5e1; border-radius:8px; }}
        table {{ width:100%; border-collapse:collapse; }}
        th, td {{ border-bottom:1px solid #e5e7eb; padding:10px; text-align:left; font-size:13px; }}
        th {{ background:#f8fafc; }}
        .btn-mini {{ display:inline-block; background:#2563eb; color:#fff; text-decoration:none; padding:6px 10px; border-radius:6px; font-size:12px; }}
        .btn-mini:hover {{ background:#1d4ed8; }}
        @media (max-width: 900px) {{ .grid {{ grid-template-columns: 1fr; }} }}
        </style>
        </head>
        <body>
            <div class="wrap">
                <div class="top">
                    <h2 style="margin:0;">📋 Historial de cumplimientos guardados</h2>
                    <a href="/modulo/programacion" class="btn">⬅️ Volver a Programación</a>
                </div>

                <div class="panel">
                    <form method="get" class="grid">
                        <div>
                            <label>Obra</label>
                            <select name="obra">{opts_obra}</select>
                        </div>
                        <div>
                            <label>Semana</label>
                            <select name="semana">{opts_semana}</select>
                        </div>
                        <button type="submit" class="btn">Filtrar</button>
                        <a href="/modulo/programacion/cumplimientos-historial" class="btn" style="background:#64748b;">Limpiar</a>
                    </form>
                </div>

                <div class="panel" style="overflow-x:auto;">
                    <table>
                        <tr>
                            <th>Semana</th>
                            <th>OT</th>
                            <th>Obra</th>
                            <th>Título</th>
                            <th>% Cumplido</th>
                            <th>Desvío</th>
                            <th>Acción</th>
                        </tr>
                        {filas_html}
                    </table>
                </div>
            </div>
        </body>
        </html>
        """

@programacion_bp.route("/api/programacion/cumplimientos-historial")
def api_cumplimientos_historial():
    from db_utils import get_db
    from flask import jsonify
    db = get_db()
    cumplimientos = db.execute(
        "SELECT c.ot_id, c.semana_inicio, c.pct_cumplido, c.desvio_codigo, "
        "COALESCE(o.obra, ''), COALESCE(o.titulo, '') "
        "FROM programacion_cumplimiento c "
        "LEFT JOIN ordenes_trabajo o ON o.id = c.ot_id "
        "ORDER BY c.semana_inicio DESC, c.ot_id ASC"
    ).fetchall()
    result = {
        "cumplimientos": [
            {
                "ot_id": c[0],
                "semana": c[1],
                "pct_cumplido": int(c[2] or 0),
                "desvio_codigo": str(c[3] or ""),
                "obra": c[4],
                "titulo": c[5]
            }
            for c in cumplimientos
        ],
        "desvios": _DESVIOS
    }
    return jsonify(result)
