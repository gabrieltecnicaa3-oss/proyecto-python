from flask import Blueprint, request, Response, redirect, url_for
from db_utils import get_db
from datetime import date, timedelta, datetime

reportes_bp = Blueprint("reportes", __name__)

# ── Constantes de proceso ─────────────────────────────────────────────────────
STAGES  = ["ARMADO", "SOLDADURA", "PINTURA", "DESPACHO"]
ST_LBL  = {"ARMADO": "Armado", "SOLDADURA": "Soldadura", "PINTURA": "Pintura", "DESPACHO": "Despacho"}
ST_CLR  = {"ARMADO": "#3b82f6", "SOLDADURA": "#f97316", "PINTURA": "#22c55e", "DESPACHO": "#a855f7"}
ST_BG   = {"ARMADO": "#bfdbfe", "SOLDADURA": "#fed7aa", "PINTURA": "#bbf7d0", "DESPACHO": "#ddd6fe"}
ST_ABR  = {"ARMADO": "% A", "SOLDADURA": "% S", "PINTURA": "% P", "DESPACHO": "% D"}

_OK     = ("OK", "APROBADO", "OBS", "OBSERVACION", "OBSERVACIÓN", "OM", "OP MEJORA", "OPORTUNIDAD DE MEJORA")
_OK_PH  = "(" + ",".join("?" * len(_OK)) + ")"


# ── Helpers pequeños ──────────────────────────────────────────────────────────
def _week_range(y: int, w: int):
    d = date.fromisocalendar(y, w, 1)
    return d, d + timedelta(days=6)


def _pct(n, t):
    return round(n / t * 100) if t else 0


def _pct_clr(p):
    return "#22c55e" if p >= 80 else "#f59e0b" if p >= 30 else "#ef4444"


def _priority(fe_str):
  try:
    d = (datetime.strptime(str(fe_str)[:10], "%Y-%m-%d").date() - date.today()).days
    if d <= 15:
      return "URGENTE", "#ef4444", "#fef2f2"
    if d <= 30:
      return "ALTA", "#f59e0b", "#fffbeb"
  except Exception:
    pass
  return "NORMAL", "#6b7280", "#f9fafb"


def _fd(s):
    try:
        return datetime.strptime(str(s)[:10], "%Y-%m-%d").strftime("%d-%b-%Y")
    except Exception:
        return s or "–"


def _e(s):
    if s is None:
        return "–"
    return str(s).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


# ── Recolección de datos ──────────────────────────────────────────────────────
def _collect(db, obra, year, week, week_start, week_end):
    ws  = week_start.strftime("%Y-%m-%d")
    we  = week_end.strftime("%Y-%m-%d")
    six = (week_start - timedelta(weeks=5)).strftime("%Y-%m-%d")

    # OTs de una obra puntual o de todas las obras
    if obra:
      ots = db.execute(
        "SELECT id, titulo, tipo_estructura, fecha_entrega, cliente, hs_previstas, estado_avance "
        "FROM ordenes_trabajo WHERE obra=? AND estado != 'INACTIVO' "
        "ORDER BY fecha_entrega ASC, id ASC",
        (obra,)
      ).fetchall()
    else:
      ots = db.execute(
        "SELECT id, titulo, tipo_estructura, fecha_entrega, cliente, hs_previstas, estado_avance "
        "FROM ordenes_trabajo "
        "WHERE TRIM(COALESCE(obra,'')) != '' AND estado != 'INACTIVO' "
        "ORDER BY fecha_entrega ASC, id ASC"
      ).fetchall()

    if not ots:
        return None

    ot_ids  = [r[0] for r in ots]
    ph      = ",".join("?" * len(ot_ids))
    obra_label = obra if obra else "TODAS LAS OBRAS"
    cliente = next((r[4] for r in ots if r[4]), obra_label)

    ot_obra_rows = db.execute(
      f"SELECT id, COALESCE(obra, '') FROM ordenes_trabajo WHERE id IN ({ph})",
      ot_ids
    ).fetchall()
    ot_obra_by_id = {int(r[0]): str(r[1] or "") for r in ot_obra_rows}

    # Total piezas por OT
    rows = db.execute(
        f"SELECT ot_id, COUNT(DISTINCT posicion) FROM procesos "
        f"WHERE ot_id IN ({ph}) AND eliminado=0 GROUP BY ot_id",
        ot_ids
    ).fetchall()
    total_by_ot  = {r[0]: r[1] for r in rows}
    total_global = sum(total_by_ot.values())

    # Piezas aprobadas por etapa por OT
    appr = {oid: {s: 0 for s in STAGES} for oid in ot_ids}
    for stage in STAGES:
        srows = db.execute(
            f"SELECT ot_id, COUNT(DISTINCT posicion) FROM procesos "
            f"WHERE ot_id IN ({ph}) AND proceso=? "
            f"AND UPPER(TRIM(estado)) IN {_OK_PH} AND eliminado=0 GROUP BY ot_id",
            ot_ids + [stage] + list(_OK)
        ).fetchall()
        for r in srows:
            if r[0] in appr:
                appr[r[0]][stage] = r[1]

    # Avance global histórico (piezas por etapa)
    n_arm_g = sum(appr[oid]["ARMADO"]    for oid in ot_ids)
    n_sol_g = sum(appr[oid]["SOLDADURA"] for oid in ot_ids)
    n_pin_g = sum(appr[oid]["PINTURA"]   for oid in ot_ids)
    n_des_g = sum(appr[oid]["DESPACHO"]  for oid in ot_ids)
    avance_global_pct = _pct(n_arm_g + n_sol_g + n_pin_g + n_des_g, total_global * 4) if total_global else 0

    # HS
    hs_prev = db.execute(
        f"SELECT COALESCE(SUM(hs_previstas),0) FROM ordenes_trabajo WHERE id IN ({ph})", ot_ids
    ).fetchone()[0] or 0.0
    hs_cons = db.execute(
        f"SELECT COALESCE(SUM(horas),0) FROM partes_trabajo "
        f"WHERE ot_id IN ({ph}) AND fecha BETWEEN ? AND ?",
        ot_ids + [ws, we]
    ).fetchone()[0] or 0.0
    hs_segun    = avance_global_pct / 100.0 * hs_prev
    eficiencia  = round(hs_cons / hs_segun * 100, 1) if hs_segun else 0.0

    # KG en período
    kg_prod = db.execute(
        f"SELECT COALESCE(SUM(peso),0) FROM procesos "
        f"WHERE ot_id IN ({ph}) AND proceso='ARMADO' "
        f"AND UPPER(TRIM(estado)) IN {_OK_PH} AND fecha BETWEEN ? AND ? AND eliminado=0",
        ot_ids + list(_OK) + [ws, we]
    ).fetchone()[0] or 0.0
    kg_desp = db.execute(
        f"SELECT COALESCE(SUM(peso),0) FROM procesos "
        f"WHERE ot_id IN ({ph}) AND proceso='DESPACHO' "
        f"AND UPPER(TRIM(estado)) IN {_OK_PH} AND fecha BETWEEN ? AND ? AND eliminado=0",
        ot_ids + list(_OK) + [ws, we]
    ).fetchone()[0] or 0.0

    # Tonelaje total de la obra (suma peso de todas las piezas armadas registradas)
    kg_total = db.execute(
        f"SELECT COALESCE(SUM(peso),0) FROM procesos "
        f"WHERE ot_id IN ({ph}) AND proceso='ARMADO' AND eliminado=0",
        ot_ids
    ).fetchone()[0] or 0.0

    # Producción semanal últimas 6 semanas (piezas armadas aprobadas)
    weekly_data = {}
    week_labels  = []
    for i in range(5, -1, -1):
        wk_d = week_start - timedelta(weeks=i)
        y2, w2, _ = wk_d.isocalendar()
        key = (y2, w2)
        weekly_data[key] = {"n": 0, "label": f"S{w2}"}
        week_labels.append(f"S{w2}")

    prod_rows = db.execute(
        f"SELECT fecha, COUNT(DISTINCT posicion) FROM procesos "
        f"WHERE ot_id IN ({ph}) AND proceso='ARMADO' "
        f"AND UPPER(TRIM(estado)) IN {_OK_PH} "
        f"AND fecha >= ? AND eliminado=0 GROUP BY fecha",
        ot_ids + list(_OK) + [six]
    ).fetchall()
    for fecha_str, n in prod_rows:
        try:
            dd = datetime.strptime(str(fecha_str)[:10], "%Y-%m-%d").date()
            key = (dd.isocalendar()[0], dd.isocalendar()[1])
            if key in weekly_data:
                weekly_data[key]["n"] += n
        except Exception:
            pass
    week_vals = [weekly_data[k]["n"] for k in sorted(weekly_data.keys())]

    # Variación semanal vs semana anterior
    prev_ws = (week_start - timedelta(weeks=1)).strftime("%Y-%m-%d")
    prev_we = (week_start - timedelta(days=1)).strftime("%Y-%m-%d")
    n_prev = db.execute(
        f"SELECT COUNT(DISTINCT posicion) FROM procesos "
        f"WHERE ot_id IN ({ph}) AND proceso='ARMADO' "
        f"AND UPPER(TRIM(estado)) IN {_OK_PH} AND fecha BETWEEN ? AND ? AND eliminado=0",
        ot_ids + list(_OK) + [prev_ws, prev_we]
    ).fetchone()[0] or 0
    n_this = db.execute(
        f"SELECT COUNT(DISTINCT posicion) FROM procesos "
        f"WHERE ot_id IN ({ph}) AND proceso='ARMADO' "
        f"AND UPPER(TRIM(estado)) IN {_OK_PH} AND fecha BETWEEN ? AND ? AND eliminado=0",
        ot_ids + list(_OK) + [ws, we]
    ).fetchone()[0] or 0

    # Fecha desde (primer registro)
    first_fecha = db.execute(
        f"SELECT MIN(fecha) FROM procesos WHERE ot_id IN ({ph}) AND eliminado=0", ot_ids
    ).fetchone()[0]

    # Programación (Gantt)
    prog_rows = db.execute(
        f"SELECT p.ot_id, p.fecha_inicio, p.fecha_fin "
        f"FROM programacion p "
        f"WHERE p.ot_id IN ({ph}) "
        f"ORDER BY p.fecha_inicio ASC",
        ot_ids
    ).fetchall()

    # Estado por OT: cumplido / en término / atrasado
    today_d = date.today()
    n_cumplido = 0
    n_en_termino = 0
    n_atrasado = 0
    for ot in ots:
        ot_id, _, _, fe, _, _, _ = ot
        total = total_by_ot.get(ot_id, 0)
        n_des = appr[ot_id]["DESPACHO"]
        # Cumplido: todas las piezas despachadas
        if total > 0 and n_des >= total:
            n_cumplido += 1
            continue
        # Determinar si está atrasado
        try:
            fe_date = datetime.strptime(str(fe)[:10], "%Y-%m-%d").date()
            dias_restantes = (fe_date - today_d).days
        except Exception:
            dias_restantes = 999
        if dias_restantes < 0:
            # fecha ya pasó y no está completada
            n_atrasado += 1
        else:
            n_en_termino += 1

    # Avance por OT (alineado con Estado de Producción: estado_avance persistido)
    avance_by_ot = {}
    for ot in ots:
      ot_id = int(ot[0])
      avance_by_ot[ot_id] = max(0, min(100, int(ot[6] or 0)))

    # KG totales estimados por OT (base para referencia de la vista por KG)
    kg_total_by_ot = {}
    kg_rows = db.execute(
      f"""
      WITH base AS (
        SELECT ot_id,
             TRIM(COALESCE(posicion, '')) AS pos,
             MAX(COALESCE(cantidad, 1)) AS cant,
             MAX(COALESCE(peso, 0)) AS peso
        FROM procesos
        WHERE ot_id IN ({ph})
          AND eliminado = 0
          AND TRIM(COALESCE(posicion, '')) <> ''
        GROUP BY ot_id, TRIM(COALESCE(posicion, ''))
      )
      SELECT ot_id,
           COALESCE(SUM(CASE WHEN cant > 0 AND peso > 0 THEN cant * peso ELSE 0 END), 0) AS kg_total
      FROM base
      GROUP BY ot_id
      """,
      ot_ids,
    ).fetchall()
    for r in kg_rows:
      kg_total_by_ot[int(r[0])] = float(r[1] or 0.0)

    kg_avance_by_ot = {
      oid: round((kg_total_by_ot.get(oid, 0.0) * avance_by_ot.get(oid, 0)) / 100.0, 1)
      for oid in ot_ids
    }

    # Avance global ponderado por KG (peso físico de cada OT)
    kg_total_suma = sum(kg_total_by_ot.values())
    if kg_total_suma > 0:
      ponderado = sum(
        avance_by_ot.get(oid, 0) * kg_total_by_ot.get(oid, 0.0)
        for oid in ot_ids
      )
      avance_global_pct = round(ponderado / kg_total_suma)
    elif hs_prev > 0:
      # Fallback: ponderar por HS si no hay datos de KG
      ponderado = 0.0
      for ot in ots:
        oid = int(ot[0])
        hs_ot = float(ot[5] or 0.0)
        if hs_ot > 0:
          ponderado += avance_by_ot.get(oid, 0) * hs_ot
      avance_global_pct = round(ponderado / hs_prev)
    else:
      avance_global_pct = round(sum(avance_by_ot.values()) / len(avance_by_ot)) if avance_by_ot else 0

    hs_segun    = avance_global_pct / 100.0 * hs_prev
    eficiencia  = round(hs_cons / hs_segun * 100, 1) if hs_segun else 0.0

    return dict(
        obra=obra_label, cliente=cliente,
        ots=ots, ot_ids=ot_ids,
        all_obras=(obra is None), ot_obra_by_id=ot_obra_by_id,
        total_by_ot=total_by_ot, total_global=total_global,
        appr=appr,
        avance_global_pct=avance_global_pct,
        n_arm_g=n_arm_g, n_sol_g=n_sol_g, n_pin_g=n_pin_g, n_des_g=n_des_g,
        hs_prev=hs_prev, hs_cons=hs_cons, hs_segun=hs_segun, eficiencia=eficiencia,
        kg_prod=kg_prod, kg_desp=kg_desp, kg_total=kg_total,
        kg_total_by_ot=kg_total_by_ot, kg_avance_by_ot=kg_avance_by_ot,
        week_labels=week_labels, week_vals=week_vals,
        first_fecha=first_fecha, n_prev=n_prev, n_this=n_this,
        year=year, week=week, week_start=week_start, week_end=week_end,
        n_cumplido=n_cumplido, n_en_termino=n_en_termino, n_atrasado=n_atrasado,
        prog_rows=prog_rows, avance_by_ot=avance_by_ot,
    )


# ── SVG Gantt de programación ────────────────────────────────────────────────
def _svg_gantt(prog_rows, ots, avance_by_ot=None):
    """Genera SVG Gantt desde filas de programacion.
    prog_rows: list of (ot_id, fecha_inicio, fecha_fin)
    ots: list de OT tuples (id, titulo, tipo_est, fecha_entrega, ...)
    avance_by_ot: dict con porcentaje de avance por OT
    """
    if not prog_rows:
        return '<p style="color:#9ca3af;font-size:.85rem;margin:12px 0">Sin datos de programación para esta obra. Cargá fechas desde el módulo Programación.</p>'

    if avance_by_ot is None:
        avance_by_ot = {}
    
    ot_info = {ot[0]: ot for ot in ots}
    today   = date.today()

    fechas_i, fechas_f, valid_rows = [], [], []
    for r in prog_rows:
        try:
            fi = datetime.strptime(str(r[1])[:10], "%Y-%m-%d").date()
            ff = datetime.strptime(str(r[2])[:10], "%Y-%m-%d").date()
            fechas_i.append(fi)
            fechas_f.append(ff)
            valid_rows.append((r[0], fi, ff))
        except Exception:
            pass

    if not fechas_i:
        return '<p style="color:#9ca3af;font-size:.85rem;margin:12px 0">Sin datos de programación para esta obra.</p>'

    for ot in ots:
        if ot[3]:
            try:
                fe = datetime.strptime(str(ot[3])[:10], "%Y-%m-%d").date()
                fechas_f.append(fe)
            except Exception:
                pass

    date_min = min(fechas_i) - timedelta(days=3)
    date_max = max(fechas_f) + timedelta(days=10)
    total_days = max((date_max - date_min).days, 1)

    ROW_H, LABEL_W, CHART_W, HEADER_H = 30, 180, 680, 28
    COLORS = ["#3b82f6", "#f97316", "#22c55e", "#a855f7", "#ec4899", "#14b8a6", "#f59e0b", "#6366f1"]

    by_ot = {}
    for ot_id, fi, ff in valid_rows:
        by_ot.setdefault(ot_id, []).append((fi, ff))
    ot_order = sorted(by_ot.keys(), key=lambda oid: min(fi for fi, _ in by_ot[oid]))

    n_rows = len(ot_order)
    H = HEADER_H + n_rows * ROW_H + 24
    W = LABEL_W + CHART_W

    def day_x(d):
        return LABEL_W + int((d - date_min).days / total_days * CHART_W)

    grid = ""
    cur = date(date_min.year, date_min.month, 1)
    while cur <= date_max:
        x = day_x(cur)
        if x >= LABEL_W:
            grid += f'<line x1="{x}" y1="{HEADER_H}" x2="{x}" y2="{H-16}" stroke="#e5e7eb" stroke-width="1"/>'
            grid += f'<text x="{x+3}" y="{HEADER_H-6}" font-size="9" fill="#9ca3af">{cur.strftime("%b %Y")}</text>'
        if cur.month == 12:
            cur = date(cur.year + 1, 1, 1)
        else:
            cur = date(cur.year, cur.month + 1, 1)

    if date_min <= today <= date_max:
        tx = day_x(today)
        grid += f'<line x1="{tx}" y1="{HEADER_H}" x2="{tx}" y2="{H-16}" stroke="#ef4444" stroke-width="2" stroke-dasharray="4,3"/>'
        grid += f'<text x="{tx+2}" y="{HEADER_H-6}" font-size="8" fill="#ef4444" font-weight="700">HOY</text>'

    bars = ""
    # Definir patrón rayado para subcontratos en <defs> del SVG
    defs = '<defs><pattern id="subhatch" x="0" y="0" width="8" height="8" patternUnits="userSpaceOnUse"><rect width="8" height="8" fill="#e2e8f0"/><line x1="0" y1="0" x2="8" y2="8" stroke="#94a3b8" stroke-width="1.5"/></pattern></defs>'

    for i, ot_id in enumerate(ot_order):
        y_base = HEADER_H + i * ROW_H
        ot     = ot_info.get(ot_id)
        titulo = _e(ot[1])[:28] if ot else f"OT {ot_id}"
        clr    = COLORS[i % len(COLORS)]
        # Subcontrato = hs_previstas == 0 (igual que en módulo Programación y OTs)
        es_subcontrato = float(ot[5] or 0) == 0 if ot else False

        bars += (f'<text x="4" y="{y_base + 13}" font-size="10" fill="#e36c09" font-weight="700">OT {ot_id}</text>'
                 f'<text x="4" y="{y_base + 24}" font-size="8" fill="#6b7280">{titulo}</text>')

        for fi, ff in by_ot[ot_id]:
            x1 = day_x(fi)
            x2 = day_x(ff)
            bw = max(x2 - x1, 6)
            # Barra de programación (parte superior de la fila)
            bar_y   = y_base + 4
            bar_h   = 13
            prog_y  = y_base + 19  # barra de avance debajo
            prog_h  = 7
            if es_subcontrato:
                bars += f'<rect x="{x1}" y="{bar_y}" width="{bw}" height="{bar_h}" fill="url(#subhatch)" rx="3" stroke="#94a3b8" stroke-width="1"/>'
            else:
                bars += f'<rect x="{x1}" y="{bar_y}" width="{bw}" height="{bar_h}" fill="{clr}" rx="3" opacity="0.82"/>'

            # Barra de avance debajo de la barra de programación
            avance = avance_by_ot.get(ot_id, 0)
            # Track gris
            bars += f'<rect x="{x1}" y="{prog_y}" width="{bw}" height="{prog_h}" fill="#e5e7eb" rx="2"/>'
            if avance > 0:
                bw_avance = max(int(bw * avance / 100), 4)
                bars += f'<rect x="{x1}" y="{prog_y}" width="{bw_avance}" height="{prog_h}" fill="#22c55e" rx="2"/>'
                if bw_avance >= 18:
                    bars += f'<text x="{x1 + bw_avance/2}" y="{prog_y + prog_h - 1}" text-anchor="middle" font-size="7" fill="#fff" font-weight="700">{int(avance)}%</text>'

        if ot and ot[3]:
            try:
                fe_d = datetime.strptime(str(ot[3])[:10], "%Y-%m-%d").date()
                if date_min <= fe_d <= date_max:
                    fx = day_x(fe_d)
                    fy = y_base + ROW_H // 2
                    bars += f'<polygon points="{fx},{fy-6} {fx+5},{fy} {fx},{fy+6} {fx-5},{fy}" fill="#dc2626"/>'
            except Exception:
                pass

        bars += f'<line x1="0" y1="{y_base + ROW_H}" x2="{W}" y2="{y_base + ROW_H}" stroke="#f3f4f6" stroke-width="1"/>'

    lx = LABEL_W + 10
    ly = H - 8
    legend = (f'<line x1="{lx}" y1="{ly-5}" x2="{lx}" y2="{ly+5}" stroke="#ef4444" stroke-width="2" stroke-dasharray="3,2"/>'
              f'<text x="{lx+6}" y="{ly+4}" font-size="9" fill="#6b7280">Hoy</text>'
              f'<polygon points="{lx+42},{ly-5} {lx+47},{ly} {lx+42},{ly+5} {lx+37},{ly}" fill="#dc2626"/>'
              f'<text x="{lx+51}" y="{ly+4}" font-size="9" fill="#6b7280">F. Entrega</text>')

    return (f'<svg viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg" '
            f'style="width:100%;display:block;overflow:visible">'
            f'{defs}{grid}{bars}{legend}</svg>')


# ── SVG gráfico de barras semanal ─────────────────────────────────────────────
def _svg_bars(labels, values):
    if not values or max(values) == 0:
        return '<p style="color:#9ca3af;font-size:.85rem;margin:12px 0">Sin datos de producción en el período.</p>'
    W, CHART_H, BAR_W = 580, 80, 60
    max_v = max(values) or 1
    n     = len(values)
    gap   = (W - n * BAR_W) // (n + 1)
    rects = ""
    for i, (lbl, val) in enumerate(zip(labels, values)):
        x     = gap + i * (BAR_W + gap)
        bh    = max(2, int(val / max_v * CHART_H))
        y     = CHART_H - bh
        alpha = "ff" if val > 0 else "55"
        rects += f'<rect x="{x}" y="{y}" width="{BAR_W}" height="{bh}" fill="#e36c09{alpha}" rx="3"/>'
        if val > 0:
            rects += f'<text x="{x + BAR_W // 2}" y="{y - 5}" text-anchor="middle" font-size="11" fill="#1f2937" font-weight="600">{val}</text>'
        rects += f'<text x="{x + BAR_W // 2}" y="{CHART_H + 18}" text-anchor="middle" font-size="11" fill="#6b7280">{lbl}</text>'
    H_total = CHART_H + 30
    return (f'<svg viewBox="0 0 {W} {H_total}" xmlns="http://www.w3.org/2000/svg" '
            f'style="width:100%;max-width:{W}px;display:block">{rects}</svg>')


# ── Renderizado del reporte completo ──────────────────────────────────────────
def _render_html(d, tipo, periodo_tipo="SEMANAL"):
    obra        = _e(d["obra"])
    cliente     = _e(d["cliente"])
    ots         = d["ots"]
    is_interno  = tipo == "INTERNO"
    all_obras   = bool(d.get("all_obras", False))
    ot_obra_by_id = d.get("ot_obra_by_id", {})
    week_start  = d["week_start"]
    week_end    = d["week_end"]
    periodo     = f"{week_start.strftime('%d %b')} – {week_end.strftime('%d %b %Y')}"
    semana_lbl  = f"Semana {d['week']}"
    today_str   = date.today().strftime("%d de %B de %Y")
    avance_pct      = d["avance_global_pct"]
    total_g         = d["total_global"]
    n_ots           = len(ots)
    kg_total_obra   = d.get("kg_total", 0.0)
    kg_avance_total = sum(d.get("kg_avance_by_ot", {}).values())
    today_d         = date.today()

    datos_desde = "–"
    if d["first_fecha"]:
        try:
            datos_desde = datetime.strptime(str(d["first_fecha"])[:10], "%Y-%m-%d").strftime("%d/%m/%Y")
        except Exception:
            datos_desde = d["first_fecha"]

    is_mensual   = periodo_tipo == "MENSUAL"
    periodo_lbl  = week_start.strftime("%B %Y").capitalize() if is_mensual else f"Semana {d['week']}"
    report_title = "INFORME DE AVANCE MENSUAL" if is_mensual else "INFORME DE AVANCE SEMANAL"
    badge_css = "background:#fff3cd;color:#856404;border:1px solid #f0d06a" if not is_interno else "background:#d1f0e0;color:#166534;border:1px solid #86efac"

    # ── CSS ──────────────────────────────────────────────────────────────────
    css = """
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: Arial, Helvetica, sans-serif; background: #f8f9fa; color: #1f2937; font-size: 13px; }
    .report-wrap { max-width: 960px; margin: 0 auto; padding: 20px 16px 40px; }

    /* Header */
    .rpt-header { background: linear-gradient(135deg, #c95a06 0%, #e36c09 60%, #f59e0b 100%);
        color: #fff; border-radius: 10px 10px 0 0; padding: 18px 24px;
        display: flex; align-items: center; justify-content: space-between; }
    .rpt-header-left { display: flex; align-items: center; gap: 16px; }
    .rpt-header-logo { height: 44px; background: #fff; border-radius: 6px; padding: 4px 8px; object-fit: contain; }
    .rpt-title { font-size: 15px; font-weight: 700; letter-spacing: .3px; text-shadow: 0 1px 2px rgba(0,0,0,.2); }
    .rpt-subtitle { font-size: 12px; opacity: .88; margin-top: 3px; }
    .rpt-header-right { text-align: right; }
    .rpt-date { font-size: 12px; opacity: .85; }
    .rpt-badge { display: inline-block; padding: 3px 10px; border-radius: 20px; font-size: 11px; font-weight: 700;
        background: rgba(255,255,255,.22); border: 1px solid rgba(255,255,255,.4); margin-bottom: 4px; }
    .print-running-header { display: none; }
    .print-running-header-left { display: flex; align-items: center; gap: 10px; }
    .print-running-logo { height: 22px; object-fit: contain; }
    .print-running-title { font-size: 10.5px; font-weight: 700; color: #111827; }
    .print-running-meta { font-size: 9px; color: #6b7280; margin-top: 1px; }
    .print-running-right { text-align: right; }

    /* Ficha técnica */
    .ficha { background: #fff; border: 1px solid #e5e7eb; border-top: none; }
    .ficha-row { display: grid; grid-template-columns: 1fr 1fr 1fr; border-bottom: 1px solid #e5e7eb; }
    .ficha-row:last-child { border-bottom: none; }
    .ficha-cell { padding: 9px 16px; border-right: 1px solid #e5e7eb; }
    .ficha-cell:last-child { border-right: none; }
    .fc-label { display: block; font-size: 10.5px; color: #6b7280; font-weight: 600; text-transform: uppercase; letter-spacing: .5px; margin-bottom: 2px; }
    .fc-val { display: block; font-size: 13px; font-weight: 700; color: #1f2937; }
    .fc-val.orange { color: #e36c09; }
    .fc-val.big { font-size: 22px; }

    /* Secciones */
    .section { background: #fff; border: 1px solid #e5e7eb; border-top: none; padding: 0; }
    .section-header { background: #fff7ed; border-bottom: 1px solid #fed7aa; border-left: 4px solid #e36c09;
        padding: 8px 16px; font-size: 12px; font-weight: 700; color: #c95a06; text-transform: uppercase;
        letter-spacing: .5px; }
    .section-body { padding: 14px 16px; }

    /* KPIs */
    .kpi-row { display: grid; grid-template-columns: repeat(6, 1fr); gap: 10px; padding: 14px 16px; }
    .kpi-box { background: #f9fafb; border: 1px solid #e5e7eb; border-radius: 8px; padding: 12px 10px; text-align: center; }
    .kpi-box.hl { background: #fff7ed; border-color: #fed7aa; }
    .kpi-val { font-size: 18px; font-weight: 700; color: #e36c09; line-height: 1.1; }
    .kpi-lbl { font-size: 10px; color: #6b7280; margin-top: 4px; line-height: 1.3; }
    .kpi-note { font-size: 10.5px; color: #9ca3af; font-style: italic; padding: 0 16px 10px; }

    /* Tabla principal */
    .main-table { width: 100%; border-collapse: collapse; font-size: 12px; }
    .main-table th { background: #1f2937; color: #fff; padding: 7px 8px; text-align: center; font-size: 11px; white-space: nowrap; }
    .main-table td { padding: 6px 8px; border-bottom: 1px solid #f3f4f6; text-align: center; vertical-align: middle; }
    .main-table tbody tr:hover { background: #fafafa; }
    .main-table tfoot td { background: #f3f4f6; font-weight: 700; border-top: 2px solid #d1d5db; }
    .tc-ot { font-weight: 700; color: #e36c09; }
    .tc-titulo { text-align: left; font-weight: 600; max-width: 180px; }
    .tc-fe { white-space: nowrap; font-size: 11px; }
    .legend-row { font-size: 11px; color: #6b7280; padding: 8px 16px; border-top: 1px solid #f3f4f6; }
    .leg-dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 3px; vertical-align: middle; }

    /* Gráfico de barras horizontal */
    .bar-legend { display: flex; gap: 16px; padding: 10px 16px 0; flex-wrap: wrap; }
    .leg-seg { display: flex; align-items: center; gap: 5px; font-size: 11px; color: #374151; }
    .axis-labels { display: flex; justify-content: space-between; padding: 4px 16px 0 200px; font-size: 10px; color: #9ca3af; }
    .bar-row { display: flex; align-items: center; padding: 5px 16px; border-bottom: 1px solid #f3f4f6; min-height: 36px; }
    .bar-row:last-child { border-bottom: none; }
    .bar-ot-label { width: 184px; flex-shrink: 0; }
    .bar-ot-id { font-size: 11px; font-weight: 700; color: #e36c09; margin-right: 5px; }
    .bar-ot-titulo { font-size: 11px; color: #374151; }
    .bar-track { flex: 1; background: #f3f4f6; border-radius: 4px; height: 18px; overflow: hidden; position: relative; }
    .bar-fe { width: 80px; text-align: right; font-size: 10px; color: #6b7280; white-space: nowrap; margin-left: 8px; }
    .bar-real { width: 78px; text-align: right; font-size: 11px; color: #334155; white-space: nowrap; margin-left: 8px; font-weight: 800; }
    .print-new-page { }
    .print-keep-together { }

    /* Cronograma */
    .pri-badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 10px; font-weight: 700; }
    .mini-bar-wrap { display: inline-flex; align-items: center; gap: 5px; vertical-align: middle; }
    .mini-bar-bg { background: #e5e7eb; border-radius: 3px; height: 12px; width: 70px; display: inline-block; overflow: hidden; vertical-align: middle; }
    .mini-bar-fill { height: 100%; border-radius: 3px; display: block; }
    .estado-badge { display: inline-block; padding: 2px 7px; border-radius: 4px; font-size: 10px; background: #dbeafe; color: #1d4ed8; font-weight: 600; }
    .estado-badge.done { background: #dcfce7; color: #166534; }
    .estado-badge.none { background: #f3f4f6; color: #6b7280; }

    /* Tablero de desvíos */
    .desvio-wrap { padding: 12px 16px; }
    .desvio-kpis { display: grid; grid-template-columns: repeat(5, 1fr); gap: 10px; margin-bottom: 10px; }
    .desvio-kpi { border: 1px solid #e5e7eb; border-radius: 8px; padding: 10px; text-align: center; }
    .desvio-kpi .v { font-size: 18px; font-weight: 800; line-height: 1.1; }
    .desvio-kpi .l { font-size: 10px; color: #6b7280; margin-top: 4px; text-transform: uppercase; letter-spacing: .3px; }
    .desvio-kpi.crit { background: #fef2f2; border-color: #fecaca; }
    .desvio-kpi.risk { background: #fff7ed; border-color: #fed7aa; }
    .desvio-kpi.ok { background: #f0fdf4; border-color: #bbf7d0; }
    .desvio-kpi.np { background: #f8fafc; border-color: #e2e8f0; }
    .desvio-kpi.avg { background: #eff6ff; border-color: #bfdbfe; }
    .desvio-grid { display: grid; grid-template-columns: 1.5fr 1fr; gap: 12px; }
    .desvio-list-box { border: 1px solid #e5e7eb; border-radius: 8px; overflow: hidden; }
    .desvio-list-head { background: #f8fafc; border-bottom: 1px solid #e5e7eb; font-size: 11px; font-weight: 700; color: #334155; padding: 8px 10px; }
    .desvio-list { list-style: none; margin: 0; padding: 0; }
    .desvio-list li { display: flex; justify-content: space-between; gap: 8px; padding: 8px 10px; border-bottom: 1px solid #f1f5f9; font-size: 11px; }
    .desvio-list li:last-child { border-bottom: none; }
    .desvio-ot { color: #1f2937; font-weight: 700; }
    .desvio-meta { color: #6b7280; font-size: 10px; margin-top: 2px; }
    .desvio-delta { font-weight: 800; white-space: nowrap; }
    .desvio-hint { border: 1px dashed #cbd5e1; border-radius: 8px; padding: 10px; background: #f8fafc; font-size: 11px; color: #475569; line-height: 1.45; }

    /* Tendencia plan vs real */
    .trend-wrap { padding: 12px 16px; }
    .trend-kpis { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-bottom: 10px; }
    .trend-kpi { border: 1px solid #e5e7eb; border-radius: 8px; padding: 10px; text-align: center; background: #f8fafc; }
    .trend-kpi .v { font-size: 19px; font-weight: 800; line-height: 1.1; }
    .trend-kpi .l { font-size: 10px; color: #6b7280; margin-top: 4px; text-transform: uppercase; letter-spacing: .3px; }
    .trend-card { border: 1px solid #e5e7eb; border-radius: 8px; background: #fff; padding: 8px; }
    .trend-note { margin-top: 8px; font-size: 10px; color: #6b7280; }

    /* Producción semanal */
    .prod-section { padding: 14px 16px; }
    .prod-summary { display: grid; grid-template-columns: repeat(3,1fr); gap: 10px; margin-top: 12px; font-size: 12px; }
    .prod-kpi { background: #f9fafb; border-radius: 6px; padding: 10px 12px; text-align: center; border: 1px solid #e5e7eb; }
    .prod-kpi strong { display: block; font-size: 18px; font-weight: 700; color: #e36c09; }

    /* Observaciones */
    .obs-grid { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 0; }
    .obs-col { padding: 12px 14px; border-right: 1px solid #e5e7eb; }
    .obs-col:last-child { border-right: none; }
    .obs-col-header { font-size: 11px; font-weight: 700; text-transform: uppercase; margin-bottom: 8px; padding-bottom: 4px; border-bottom: 2px solid; }
    .obs-col.alertas .obs-col-header { color: #dc2626; border-color: #fca5a5; }
    .obs-col.situacion .obs-col-header { color: #16a34a; border-color: #86efac; }
    .obs-col.acciones .obs-col-header { color: #2563eb; border-color: #93c5fd; }
    .obs-col ul { list-style: none; padding: 0; }
    .obs-col ul li { font-size: 11.5px; color: #374151; padding: 3px 0; padding-left: 12px; position: relative; line-height: 1.4; }
    .obs-col ul li::before { content: "•"; position: absolute; left: 0; color: #9ca3af; }

    /* Footer firmas */
    .firma-row { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; padding: 20px 24px 10px; margin-top: 24px; max-width: 520px; margin-left: auto; margin-right: auto; }
    .firma-box { text-align: center; }
    .firma-line { border-top: 1px solid #374151; margin: 0 10%; margin-bottom: 6px; padding-top: 6px; }
    .firma-cargo { font-size: 11px; color: #6b7280; }
    .firma-nombre { font-size: 11.5px; font-weight: 600; color: #1f2937; margin-top: 2px; }

    .print-page-footer { display: none; }

    /* Print */
    @media print {
      html, body { background: #fff; font-size: 10.5px; }
      body { color: #111827; }
      .report-wrap { max-width: 100%; padding: 0 0 12mm 0; }
      .no-print { display: none !important; }
      .print-running-header { display: none !important; }

      .section, .ficha { break-inside: auto; page-break-inside: auto; overflow: visible !important; }
      .print-keep-together { break-inside: avoid; page-break-inside: avoid; }
      .print-new-page { break-before: page; page-break-before: always; }
      .rpt-header, .firma-row { break-inside: avoid; page-break-inside: avoid; }

      .rpt-header {
        border-radius: 0;
        box-shadow: none;
        padding: 10px 0 8px;
        background: transparent;
        color: #111827;
        border-bottom: 2px solid #cbd5e1;
        border-top: 3px solid #e36c09;
      }
      .rpt-title, .rpt-date, .rpt-subtitle { color: #111827; text-shadow: none; }
      .rpt-badge {
        background: #fff7ed;
        color: #9a3412;
        border: 1px solid #fdba74;
      }

      .ficha {
        border: none;
        margin-top: 4mm;
      }
      .ficha-row {
        border-bottom: 1px solid #dbe4ee;
      }
      .ficha-cell {
        padding: 7px 10px;
      }

      .section {
        border: none;
        border-top: 1px solid #dbe4ee;
        margin-top: 4mm;
        padding-top: 0;
      }
      .section-header {
        background: transparent;
        border-left: none;
        border-bottom: 1px solid #cbd5e1;
        color: #9a3412;
        padding: 6px 0 5px;
      }
      .section-body,
      .prod-section,
      .bar-legend,
      .legend-row,
      .axis-labels,
      .bar-row {
        overflow: visible !important;
        padding-left: 0;
        padding-right: 0;
      }

      .kpi-box, .prod-kpi {
        box-shadow: none;
        border-radius: 4px;
      }
      .main-table {
        width: 100%;
        page-break-inside: avoid;
        break-inside: avoid;
      }
      .main-table thead {
        display: table-header-group;
      }
      .main-table tfoot {
        display: table-footer-group;
      }
      .main-table tr {
        page-break-inside: avoid;
        break-inside: avoid;
      }
      .main-table th {
        background: #e5e7eb;
        color: #111827;
        border-bottom: 1px solid #cbd5e1;
      }
      .main-table td {
        border-bottom: 1px solid #e5e7eb;
      }
      .main-table tbody tr:hover {
        background: transparent;
      }
      .obs-grid {
        gap: 10px;
      }
      .obs-col {
        border-right: none;
        padding: 8px 0;
      }
      .firma-row {
        margin-top: 8mm;
        padding: 8mm 0 0;
        border-top: 1px solid #dbe4ee;
      }

      .print-page-footer {
        display: flex;
        position: fixed;
        left: 0;
        right: 0;
        bottom: 0;
        justify-content: space-between;
        align-items: center;
        font-size: 9px;
        color: #64748b;
        border-top: 1px solid #cbd5e1;
        padding: 2mm 0 0;
        background: #fff;
      }
      .print-page-number::after {
        content: "Página " counter(page);
      }

      @page {
        size: A4 landscape;
        margin: 12mm 12mm 14mm 12mm;
      }
      * { -webkit-print-color-adjust: exact !important; print-color-adjust: exact !important; }
    }
    """

    # ── Secciones ────────────────────────────────────────────────────────────

    # Ficha técnica
    ficha_html = f"""
<div class="ficha">
  <div class="ficha-row">
    <div class="ficha-cell"><span class="fc-label">Obra / Proyecto</span><span class="fc-val orange">{obra}</span></div>
    <div class="ficha-cell"><span class="fc-label">Cliente</span><span class="fc-val">{cliente}</span></div>
    <div class="ficha-cell"><span class="fc-label">Avance global</span><span class="fc-val orange big">{avance_pct}%</span><span style="font-size:10px;color:#9ca3af;display:block;margin-top:2px">Físico pond. por peso (kg)</span></div>
  </div>
  <div class="ficha-row">
    <div class="ficha-cell"><span class="fc-label">Período</span><span class="fc-val">{periodo}</span></div>
    <div class="ficha-cell"><span class="fc-label">OTs activas</span><span class="fc-val">{n_ots} OTs</span></div>
    <div class="ficha-cell"><span class="fc-label">Total piezas</span><span class="fc-val">{total_g} pzas</span></div>
  </div>
  <div class="ficha-row">
    <div class="ficha-cell"><span class="fc-label">Fecha emisión</span><span class="fc-val">{today_str}</span></div>
    <div class="ficha-cell"><span class="fc-label">Período</span><span class="fc-val">{periodo_lbl}</span></div>
    <div class="ficha-cell"><span class="fc-label">Datos desde</span><span class="fc-val">{datos_desde}</span></div>
  </div>
</div>"""

    # Sección 1 – KPIs de cumplimiento (visible en ambos tipos)
    sec_num = [0]

    def next_sec(title):
        sec_num[0] += 1
        return f"{sec_num[0]}. {title}"

    n_cum = d.get("n_cumplido", 0)
    n_ent = d.get("n_en_termino", 0)
    n_atr = d.get("n_atrasado", 0)

    h_kpi_estado = next_sec("ESTADO DE CUMPLIMIENTO – OTs")
    kpi_estado_html = f"""
<div class="section">
  <div class="section-header">{h_kpi_estado}</div>
  <div class="kpi-row" style="grid-template-columns:repeat(3,1fr)">
    <div class="kpi-box" style="background:#dcfce7;border-color:#86efac">
      <div class="kpi-val" style="color:#166534">{n_cum}</div>
      <div class="kpi-lbl">✔ Cumplidas / Despachadas</div>
    </div>
    <div class="kpi-box" style="background:#dbeafe;border-color:#93c5fd">
      <div class="kpi-val" style="color:#1d4ed8">{n_ent}</div>
      <div class="kpi-lbl">⏳ En término</div>
    </div>
    <div class="kpi-box" style="background:#fee2e2;border-color:#fca5a5">
      <div class="kpi-val" style="color:#dc2626">{n_atr}</div>
      <div class="kpi-lbl">⚠ Atrasadas (f. vencida)</div>
    </div>
  </div>
</div>"""

    kpi_html = ""
    if is_interno:
        h = next_sec("INDICADORES DE PRODUCCIÓN – PERÍODO ACTUAL")
        kpi_html = f"""
<div class="section">
  <div class="section-header">{h}</div>
  <div class="kpi-row">
    <div class="kpi-box"><div class="kpi-val">{d['hs_prev']:,.1f} hs</div><div class="kpi-lbl">HS Previstas (OTs activas)</div></div>
    <div class="kpi-box"><div class="kpi-val">{d['hs_cons']:,.1f} hs</div><div class="kpi-lbl">HS Consumidas (período)</div></div>
    <div class="kpi-box hl"><div class="kpi-val">{d['hs_segun']:,.1f} hs</div><div class="kpi-lbl">HS según Avance</div></div>
    <div class="kpi-box"><div class="kpi-val">{d['eficiencia']:.1f}%</div><div class="kpi-lbl">Eficiencia HS (%)</div></div>
    <div class="kpi-box"><div class="kpi-val">{d['kg_prod']:,.1f} kg</div><div class="kpi-lbl">KG Producidos</div></div>
    <div class="kpi-box"><div class="kpi-val">{d['kg_desp']:,.1f} kg</div><div class="kpi-lbl">KG Despachados</div></div>
  </div>
  <div class="kpi-row" style="grid-template-columns:repeat(4,1fr);background:#f0fdf4;border-top:1px solid #d1fae5;padding-top:10px;padding-bottom:10px">
    <div class="kpi-box" style="background:#f0fdf4;border-color:#86efac"><div class="kpi-val" style="color:#166534">{kg_total_obra/1000:.2f} tn</div><div class="kpi-lbl">Tonnage total obra</div></div>
    <div class="kpi-box" style="background:#f0fdf4;border-color:#86efac"><div class="kpi-val" style="color:#166534">{kg_avance_total/1000:.2f} tn</div><div class="kpi-lbl">Avanzado estimado (acum.)</div></div>
    <div class="kpi-box" style="background:#f0fdf4;border-color:#86efac"><div class="kpi-val" style="color:#166534">{d['kg_prod']/1000:.2f} tn</div><div class="kpi-lbl">Producidas (período)</div></div>
    <div class="kpi-box" style="background:#f0fdf4;border-color:#86efac"><div class="kpi-val" style="color:#166534">{d['kg_desp']/1000:.2f} tn</div><div class="kpi-lbl">Despachadas (período)</div></div>
  </div>
  <p class="kpi-note">Datos desde {datos_desde}. Las HS Consumidas se actualizan al registrar partes diarios en el sistema. Tonnage calculado desde registros de proceso ARMADO.</p>
</div>"""

    # Sección Estado OTs por proceso
    appr        = d["appr"]
    total_by_ot = d["total_by_ot"]

    table_rows = ""
    for ot in ots:
        ot_id, titulo, tipo_est, fe, _, _, _ = ot
        obra_ot = _e(ot_obra_by_id.get(int(ot_id), ""))
        titulo_mostrar = f"[{obra_ot}] {_e(titulo)}" if all_obras and obra_ot else _e(titulo)
        total = total_by_ot.get(ot_id, 0)
        cells = ""
        n_arm_ot = appr[ot_id]["ARMADO"]
        n_sol_ot = appr[ot_id]["SOLDADURA"]
        n_pin_ot = appr[ot_id]["PINTURA"]
        n_des_ot = appr[ot_id]["DESPACHO"]
        for stage in STAGES:
            n   = appr[ot_id][stage]
            pct = _pct(n, total)
            clr = _pct_clr(pct)
            cells += f'<td class="proc-frac">{n}/{total}</td><td style="color:{clr};font-weight:700">{pct}%</td>'
        pct_av_ot = max(0, min(100, int(d.get("avance_by_ot", {}).get(ot_id, 0))))
        av_clr = _pct_clr(pct_av_ot)
        fe_fmt = _fd(fe)
        tipo_s = _e(tipo_est) or "–"
        table_rows += f"""<tr>
      <td class="tc-ot">{ot_id}</td>
      <td class="tc-titulo">{titulo_mostrar}</td>
      <td>{tipo_s}</td>
      <td>{total}</td>
      {cells}
      <td style="color:{av_clr};font-weight:700;text-align:center">{pct_av_ot}%</td>
      <td class="tc-fe">{fe_fmt}</td>
    </tr>"""

    tot_cells = ""
    for stage in STAGES:
        nt  = sum(appr[oid][stage] for oid in d["ot_ids"])
        pt  = _pct(nt, total_g)
        clr = _pct_clr(pt)
        tot_cells += f'<td><b>{nt}/{total_g}</b></td><td style="color:{clr};font-weight:700"><b>{pt}%</b></td>'
    # Avance global alineado con Producción
    pct_av_g = max(0, min(100, int(avance_pct)))
    av_g_clr = _pct_clr(pct_av_g)
    tot_cells += f'<td style="color:{av_g_clr};font-weight:700"><b>{pct_av_g}%</b></td>'

    h_estado = next_sec("ESTADO DE OTs POR PROCESO")
    estado_html = f"""
<div class="section print-new-page print-keep-together">
  <div class="section-header">{h_estado}</div>
  <table class="main-table">
    <thead>
      <tr>
        <th>OT</th><th>Título</th><th>Tipo</th><th>Pzas</th>
        <th>Armado</th><th>% A</th>
        <th>Soldadura</th><th>% S</th>
        <th>Pintura</th><th>% P</th>
        <th>Despacho</th><th>% D</th>
        <th>% Avance</th><th>F. Entrega</th>
      </tr>
    </thead>
    <tbody>{table_rows}</tbody>
    <tfoot>
      <tr><td colspan="3"><b>TOTALES</b></td><td><b>{total_g}</b></td>{tot_cells}<td>–</td></tr>
    </tfoot>
  </table>
  <div class="legend-row">
    <span class="leg-dot" style="background:#22c55e"></span>≥80%&nbsp;
    <span class="leg-dot" style="background:#f59e0b"></span>30–79%&nbsp;
    <span class="leg-dot" style="background:#ef4444"></span>&lt;30%&nbsp;&nbsp;|&nbsp;&nbsp;
    A=Armado · S=Soldadura · P=Pintura · D=Despacho
  </div>
</div>"""

    # Sección Avance gráfico
    leg_items = "".join(
        f'<span class="leg-seg"><span class="leg-dot" style="background:{ST_CLR[s]};width:12px;height:12px;border-radius:3px"></span>{ST_LBL[s]}</span>'
        for s in STAGES
    )
    bar_rows = ""
    for ot in ots:
        ot_id, titulo, tipo_est, fe, _, _, _ = ot
        obra_ot = _e(ot_obra_by_id.get(int(ot_id), ""))
        titulo_mostrar = f"[{obra_ot}] {_e(titulo)}" if all_obras and obra_ot else _e(titulo)
        total = total_by_ot.get(ot_id, 0)
        avance_ot = max(0, min(100, int(d.get("avance_by_ot", {}).get(ot_id, 0))))
        segs_html = ""
        if total > 0:
            pct_stage = {
                s: _pct(appr[ot_id][s], total)
                for s in STAGES
            }
            pct_sum = sum(pct_stage.values())
            if pct_sum > 0:
                pos_left = 0.0
                av_ratio = float(avance_ot) / 100.0
                for s in STAGES:
                    p = pct_stage[s]
                    if p <= 0:
                        continue
                    w = 100.0 * (p / pct_sum)
                    # background segment (full width of this stage)
                    segs_html += (
                        f'<div style="position:absolute;left:{pos_left:.3f}%;width:{w:.3f}%;'
                        f'height:100%;background:{ST_BG[s]};" '
                        f'title="{ST_LBL[s]}: {w:.1f}%"></div>'
                    )
                    # foreground segment: progressive fill inside each stage
                    fg_l = pos_left
                    fg_w = w * av_ratio
                    if fg_w > 0:
                        segs_html += (
                            f'<div style="position:absolute;left:{fg_l:.3f}%;width:{fg_w:.3f}%;'
                            f'height:100%;background:{ST_CLR[s]};" '
                        f'title="{ST_LBL[s]} avance real: {avance_ot}%"></div>'
                        )
                    pos_left += w
        fe_fmt = _fd(fe)
        bar_rows += f"""<div class="bar-row">
      <div class="bar-ot-label"><span class="bar-ot-id">OT {ot_id}</span><span class="bar-ot-titulo">{titulo_mostrar}</span></div>
      <div class="bar-track">{segs_html}</div>
      <div class="bar-real">{avance_ot}% real</div>
      <div class="bar-fe">{fe_fmt}</div>
    </div>"""

    h_grafico = next_sec("AVANCE POR OT – VISTA GRÁFICA")
    grafico_html = f"""
<div class="section print-new-page print-keep-together">
  <div class="section-header">{h_grafico}</div>
  <div class="bar-legend">{leg_items}</div>
  <div class="axis-labels"><span>0%</span><span>25%</span><span>50%</span><span>75%</span><span>100%</span></div>
  {bar_rows}
  <div class="legend-row">La barra de fondo muestra la composición total por etapa en color claro. La barra superpuesta muestra el <b>% real</b> (campo <b>estado_avance</b>) en color más oscuro.</div>
</div>"""

    # Sección Producción semanal (solo INTERNO)
    prod_html = ""
    if is_interno:
        svg_chart  = _svg_bars(d["week_labels"], d["week_vals"])
        n_this     = d["n_this"]
        n_prev     = d["n_prev"]
        variacion  = n_this - n_prev
        var_sign   = "+" if variacion >= 0 else ""
        var_clr    = "#22c55e" if variacion >= 0 else "#ef4444"
        h_prod     = next_sec("PRODUCCIÓN SEMANAL – ÚLTIMAS 6 SEMANAS")
        prod_html  = f"""
<div class="section">
  <div class="section-header">{h_prod}</div>
  <div class="prod-section">
    {svg_chart}
    <div class="prod-summary">
      <div class="prod-kpi"><strong>{n_this}</strong>Piezas armadas esta semana</div>
      <div class="prod-kpi"><strong style="color:{var_clr}">{var_sign}{variacion}</strong>Variación vs semana anterior</div>
      <div class="prod-kpi"><strong>{d['kg_prod']:,.1f} kg</strong>KG producidos en período</div>
    </div>
  </div>
</div>"""

    # Sección Cronograma
    # Construir dict de programación por OT para cálculo de desvíos plan vs real
    prog_by_ot = {}
    for pr in d.get("prog_rows", []):
        ot_id_pr = pr[0]
        if ot_id_pr not in prog_by_ot:
            try:
                fi_pr = datetime.strptime(str(pr[1])[:10], "%Y-%m-%d").date()
                ff_pr = datetime.strptime(str(pr[2])[:10], "%Y-%m-%d").date()
                prog_by_ot[ot_id_pr] = (fi_pr, ff_pr)
            except Exception:
                pass

    tendencia_html = ""
    if prog_by_ot:
        prog_ids = [oid for oid in d.get("ot_ids", []) if oid in prog_by_ot]
        date_start = min(v[0] for v in prog_by_ot.values())
        date_end = max(v[1] for v in prog_by_ot.values())
        if date_end <= date_start:
            date_end = date_start + timedelta(days=1)

        weights_kg = d.get("kg_total_by_ot", {})
        w_sum = sum(float(weights_kg.get(oid, 0.0) or 0.0) for oid in prog_ids)
        use_equal_weights = w_sum <= 0
        if use_equal_weights:
            w_sum = float(max(len(prog_ids), 1))

        def _w(oid):
            return 1.0 if use_equal_weights else float(weights_kg.get(oid, 0.0) or 0.0)

        def _plan_pct_at(day_ref):
            tot = 0.0
            for oid in prog_ids:
                fi_ref, ff_ref = prog_by_ot[oid]
                den = max((ff_ref - fi_ref).days, 1)
                elapsed_ref = max(0, min((day_ref - fi_ref).days, den))
                pct_ref = (elapsed_ref / den) * 100.0
                tot += _w(oid) * pct_ref
            return round(tot / w_sum, 1) if w_sum > 0 else 0.0

        today_clamped = min(max(today_d, date_start), date_end)
        real_pct = float(max(0, min(100, int(avance_pct))))
        plan_hoy_pct = _plan_pct_at(today_clamped)
        desvio_hoy = round(real_pct - plan_hoy_pct, 1)
        desvio_sign = "+" if desvio_hoy >= 0 else ""
        desvio_clr = "#16a34a" if desvio_hoy >= 0 else "#dc2626"

        elapsed_days = max((today_clamped - date_start).days, 1)
        rem_days = max((date_end - today_clamped).days, 0)
        vel_real = real_pct / elapsed_days if elapsed_days > 0 else 0.0
        proj_end_pct = round(min(180.0, real_pct + vel_real * rem_days), 1)
        proj_clr = "#16a34a" if proj_end_pct >= 100 else "#ea580c" if proj_end_pct >= 85 else "#dc2626"

        fecha_estimada_cumpl = "Sin tendencia"
        if vel_real > 0:
            dias_hasta_100 = int(round((100.0 - real_pct) / vel_real)) if real_pct < 100 else 0
            if dias_hasta_100 <= 0:
                fecha_estimada_cumpl = today_clamped.strftime("%d-%b-%Y")
            else:
                fecha_estimada_cumpl = (today_clamped + timedelta(days=dias_hasta_100)).strftime("%d-%b-%Y")

        max_y = max(110.0, plan_hoy_pct + 12.0, real_pct + 12.0, proj_end_pct + 12.0)
        W, H = 760, 220
        ml, mr, mt, mb = 48, 18, 16, 34
        pw = W - ml - mr
        ph = H - mt - mb
        total_days = max((date_end - date_start).days, 1)

        def _x(day_ref):
            return ml + ((day_ref - date_start).days / total_days) * pw

        def _y(pct_ref):
            return mt + (1.0 - max(0.0, min(max_y, pct_ref)) / max_y) * ph

        y_ticks = [0, 25, 50, 75, 100]
        if max_y > 120:
            y_ticks.append(125)

        grid = ""
        for t in y_ticks:
            yt = _y(t)
            grid += f'<line x1="{ml}" y1="{yt:.1f}" x2="{W-mr}" y2="{yt:.1f}" stroke="#e5e7eb" stroke-width="1"/>'
            grid += f'<text x="{ml-6}" y="{yt+3:.1f}" text-anchor="end" font-size="10" fill="#64748b">{t}%</text>'

        x_today = _x(today_clamped)
        grid += f'<line x1="{x_today:.1f}" y1="{mt}" x2="{x_today:.1f}" y2="{H-mb}" stroke="#94a3b8" stroke-width="1.2" stroke-dasharray="4,3"/>'

        samples = []
        for i in range(0, 9):
            d_i = date_start + timedelta(days=int(round(total_days * i / 8)))
            samples.append((d_i, _plan_pct_at(d_i)))
        plan_points = " ".join(f"{_x(d):.1f},{_y(p):.1f}" for d, p in samples)

        real_points = f"{_x(date_start):.1f},{_y(0):.1f} {_x(today_clamped):.1f},{_y(real_pct):.1f}"
        proj_points = f"{_x(today_clamped):.1f},{_y(real_pct):.1f} {_x(date_end):.1f},{_y(proj_end_pct):.1f}"

        y_plan_today = _y(plan_hoy_pct)
        y_real_today = _y(real_pct)
        y_desv_top = min(y_plan_today, y_real_today)
        y_desv_bot = max(y_plan_today, y_real_today)

        svg_trend = f"""
<svg viewBox="0 0 {W} {H}" xmlns="http://www.w3.org/2000/svg" style="width:100%;display:block">
  <rect x="0" y="0" width="{W}" height="{H}" fill="#ffffff"/>
  {grid}
  <line x1="{ml}" y1="{H-mb}" x2="{W-mr}" y2="{H-mb}" stroke="#94a3b8" stroke-width="1.2"/>
  <line x1="{ml}" y1="{mt}" x2="{ml}" y2="{H-mb}" stroke="#94a3b8" stroke-width="1.2"/>

  <polyline points="{plan_points}" fill="none" stroke="#1d4ed8" stroke-width="2.2"/>
  <polyline points="{real_points}" fill="none" stroke="#22c55e" stroke-width="2.4"/>
  <polyline points="{proj_points}" fill="none" stroke="#ea580c" stroke-width="2.2" stroke-dasharray="6,4"/>

  <line x1="{x_today:.1f}" y1="{y_desv_top:.1f}" x2="{x_today:.1f}" y2="{y_desv_bot:.1f}" stroke="{desvio_clr}" stroke-width="2"/>
  <circle cx="{x_today:.1f}" cy="{y_plan_today:.1f}" r="3.5" fill="#1d4ed8"/>
  <circle cx="{x_today:.1f}" cy="{y_real_today:.1f}" r="4" fill="#22c55e"/>
  <circle cx="{_x(date_end):.1f}" cy="{_y(proj_end_pct):.1f}" r="3.5" fill="#ea580c"/>

  <text x="{ml}" y="{H-10}" font-size="10" fill="#475569">Inicio {date_start.strftime('%d-%b')}</text>
  <text x="{x_today:.1f}" y="{H-10}" text-anchor="middle" font-size="10" fill="#334155">Hoy {today_clamped.strftime('%d-%b')}</text>
  <text x="{W-mr}" y="{H-10}" text-anchor="end" font-size="10" fill="#475569">Fin plan {date_end.strftime('%d-%b')}</text>

  <rect x="{ml+10}" y="{mt+6}" width="10" height="3" fill="#1d4ed8"/><text x="{ml+25}" y="{mt+11}" font-size="10" fill="#334155">Avance actual (plan)</text>
  <rect x="{ml+170}" y="{mt+6}" width="10" height="3" fill="#22c55e"/><text x="{ml+185}" y="{mt+11}" font-size="10" fill="#334155">Avance real</text>
  <rect x="{ml+285}" y="{mt+6}" width="10" height="3" fill="#ea580c"/><text x="{ml+300}" y="{mt+11}" font-size="10" fill="#334155">Tendencia / proyección</text>
</svg>"""

        h_tend = next_sec("TENDENCIA DE AVANCE Y PROYECCIÓN DE DESVÍO")
        tendencia_html = f"""
<div class="section print-keep-together">
  <div class="section-header">{h_tend}</div>
  <div class="trend-wrap">
    <div class="trend-kpis">
      <div class="trend-kpi"><div class="v" style="color:#22c55e">{real_pct:.1f}%</div><div class="l">Avance real</div></div>
      <div class="trend-kpi"><div class="v" style="color:#1d4ed8">{plan_hoy_pct:.1f}%</div><div class="l">Avance actual (plan)</div></div>
      <div class="trend-kpi"><div class="v" style="color:{desvio_clr}">{desvio_sign}{desvio_hoy:.1f}%</div><div class="l">Desvío hoy</div></div>
      <div class="trend-kpi"><div class="v" style="color:{proj_clr}">{proj_end_pct:.1f}%</div><div class="l">Proyección al fin plan</div></div>
    </div>
    <div class="trend-card">{svg_trend}</div>
    <div class="trend-note">
      Proyección lineal usando la tendencia real desde el inicio de programación hasta hoy. Fecha estimada para alcanzar 100%: <b>{fecha_estimada_cumpl}</b>.
    </div>
  </div>
</div>"""
    ots_sorted = sorted(ots, key=lambda r: (r[3] or "9999-99-99", r[0]))
    crono_rows = ""
    desvio_stats = {
      "crit": 0,
      "riesgo": 0,
      "normal": 0,
      "sin_prog": 0,
    }
    desvio_rank = []
    desvio_vals = []
    for ot in ots_sorted:
        ot_id, titulo, tipo_est, fe, _, _, _ = ot
        obra_ot = _e(ot_obra_by_id.get(int(ot_id), ""))
        titulo_mostrar = f"[{obra_ot}] {_e(titulo)}" if all_obras and obra_ot else _e(titulo)
        total   = total_by_ot.get(ot_id, 0)
        n_sol   = appr[ot_id]["SOLDADURA"]
        n_pin   = appr[ot_id]["PINTURA"]
        n_des   = appr[ot_id]["DESPACHO"]
        n_arm   = appr[ot_id]["ARMADO"]
        pct_arm = _pct(n_arm, total)
        pct_sol = _pct(n_sol, total)
        pct_pin = _pct(n_pin, total)
        pct_des = _pct(n_des, total)
        # Avance total OT alineado con Estado de Producción
        pct_avance = max(0, min(100, int(d.get("avance_by_ot", {}).get(ot_id, 0))))
        pri_lbl, pri_col, pri_bg = _priority(fe)
        fe_fmt  = _fd(fe)

        def mb(pct, clr):
            return (f'<span class="mini-bar-wrap"><span style="color:{clr};font-weight:700;min-width:28px;display:inline-block">{pct}%</span>'
                    f'<span class="mini-bar-bg"><span class="mini-bar-fill" style="width:{pct}%;background:{clr}"></span></span></span>')

        if n_des == total and total > 0:
            est_cls, est_lbl = "done", "Completada"
        elif n_arm > 0:
            est_cls, est_lbl = "",     "En proceso"
        else:
            est_cls, est_lbl = "none", "Sin iniciar"

        avance_color = '#22c55e' if pct_avance >= 75 else ('#f97316' if pct_avance >= 40 else '#ef4444')
        # Desvío plan vs real – 3 niveles de criticidad
        desvio_pr     = None
        expected_pr   = None
        crit_key      = "sin_prog"
        crit_lbl      = ""
        crit_bg       = ""
        crit_clr      = ""
        desvio_html   = '<span style="color:#9ca3af">–</span>'
        if ot_id in prog_by_ot:
            fi_pr, ff_pr  = prog_by_ot[ot_id]
            total_days_pr = max((ff_pr - fi_pr).days, 1)
            elapsed_pr    = max(0, (today_d - fi_pr).days)
            expected_pr   = min(100, round(elapsed_pr / total_days_pr * 100))
            desvio_pr     = pct_avance - expected_pr
            d_sign        = "+" if desvio_pr >= 0 else ""
            # Días para fecha de entrega para combinar con desvío
            try:
                dias_fe = (datetime.strptime(str(fe)[:10], "%Y-%m-%d").date() - today_d).days
            except Exception:
                dias_fe = 999
            # Niveles: CRÍTICO = desvío < -20% Y fecha próxima (≤30d); RIESGO = -20% a -10%; NORMAL = > -10%
            if desvio_pr < -20 and dias_fe <= 30:
                crit_lbl = "⛔ CRÍTICO"
                crit_key = "crit"
                crit_bg  = "#fef2f2"
                crit_clr = "#dc2626"
            elif desvio_pr <= -10:
                crit_lbl = "⚠ RIESGO"
                crit_key = "riesgo"
                crit_bg  = "#fff7ed"
                crit_clr = "#ea580c"
            else:
                crit_lbl = "✔ NORMAL"
                crit_key = "normal"
                crit_bg  = "#f0fdf4"
                crit_clr = "#16a34a"
            desvio_html = (
                f'<div style="font-size:10px;line-height:1.6;min-width:100px">'
                f'<div><span style="color:#6b7280">Esp:&nbsp;&nbsp;</span><b style="color:#374151">{expected_pr}%</b></div>'
                f'<div><span style="color:#6b7280">Δ:&nbsp;&nbsp;&nbsp;&nbsp;</span><b style="color:{crit_clr}">{d_sign}{desvio_pr}%</b></div>'
                f'<div style="margin-top:2px"><span class="pri-badge" style="background:{crit_bg};color:{crit_clr};font-size:9px">{crit_lbl}</span></div>'
                f'</div>'
            )
            desvio_vals.append(desvio_pr)
            desvio_rank.append((desvio_pr, ot_id, titulo_mostrar, expected_pr, pct_avance, crit_lbl, crit_clr))
        desvio_stats[crit_key] += 1
        # Row background: criticidad de desvío (si hay dato) tiene precedencia, sino prioridad de fecha
        if crit_bg and crit_lbl not in ("✔ NORMAL",):
            row_bg_style = f' style="background:{crit_bg}"'
        elif pri_lbl != "NORMAL":
            row_bg_style = f' style="background:{pri_bg}"'
        else:
            row_bg_style = ""
        crono_rows += f"""<tr{row_bg_style}>
      <td><span class="pri-badge" style="background:{pri_bg};color:{pri_col}">{pri_lbl}</span></td>
      <td class="tc-ot">{ot_id}</td>
      <td class="tc-titulo">{titulo_mostrar}</td>
      <td>{total}</td>
      <td>{mb(pct_arm, '#3b82f6')}</td>
      <td>{mb(pct_sol, '#f97316')}</td>
      <td>{mb(pct_pin, '#22c55e')}</td>
      <td>{mb(pct_des, '#a855f7')}</td>
      <td style="text-align:center;font-weight:700;color:{avance_color}">{pct_avance}%</td>
      <td style="text-align:center;white-space:nowrap">{desvio_html}</td>
      <td><span class="estado-badge {est_cls}">{est_lbl}</span></td>
    </tr>"""

    desvio_rank.sort(key=lambda x: x[0])
    desvio_peores = desvio_rank[:5]
    avg_desvio = round(sum(desvio_vals) / len(desvio_vals), 1) if desvio_vals else 0.0
    avg_sign = "+" if avg_desvio >= 0 else ""
    avg_clr = "#16a34a" if avg_desvio >= 0 else "#dc2626"

    if desvio_peores:
        peores_items = ""
        for desvio_it, ot_id_it, titulo_it, esp_it, real_it, crit_lbl_it, crit_clr_it in desvio_peores:
            d_sign_it = "+" if desvio_it >= 0 else ""
            peores_items += (
                f'<li>'
                f'<div><div class="desvio-ot">OT {ot_id_it} · {titulo_it}</div>'
                f'<div class="desvio-meta">Esp. {esp_it}% · Real {real_it}%</div></div>'
                f'<div class="desvio-delta" style="color:{crit_clr_it}">{d_sign_it}{desvio_it}%</div>'
                f'</li>'
            )
    else:
        peores_items = '<li><div class="desvio-meta">No hay OTs con programación para calcular desvío.</div></li>'

    h_desvio = next_sec("TABLERO EJECUTIVO DE DESVÍOS PLAN VS REAL")
    desvio_html = f"""
<div class="section print-new-page print-keep-together">
  <div class="section-header">{h_desvio}</div>
  <div class="desvio-wrap">
    <div class="desvio-kpis">
      <div class="desvio-kpi crit"><div class="v" style="color:#dc2626">{desvio_stats['crit']}</div><div class="l">OTs críticas</div></div>
      <div class="desvio-kpi risk"><div class="v" style="color:#ea580c">{desvio_stats['riesgo']}</div><div class="l">OTs en riesgo</div></div>
      <div class="desvio-kpi ok"><div class="v" style="color:#16a34a">{desvio_stats['normal']}</div><div class="l">OTs normales</div></div>
      <div class="desvio-kpi np"><div class="v" style="color:#475569">{desvio_stats['sin_prog']}</div><div class="l">Sin programación</div></div>
      <div class="desvio-kpi avg"><div class="v" style="color:{avg_clr}">{avg_sign}{avg_desvio}%</div><div class="l">Desvío promedio</div></div>
    </div>
    <div class="desvio-grid">
      <div class="desvio-list-box">
        <div class="desvio-list-head">Top 5 OTs con mayor desvío negativo</div>
        <ul class="desvio-list">{peores_items}</ul>
      </div>
      <div class="desvio-hint">
        <b>Reglas de criticidad</b><br>
        ⛔ Crítico: desvío &lt; -20% y entrega en ≤ 30 días.<br>
        ⚠ Riesgo: desvío entre -10% y -20%.<br>
        ✔ Normal: desvío mayor a -10%.<br>
        El detalle completo de cada OT se muestra en la sección de cronograma.
      </div>
    </div>
  </div>
</div>"""

    h_crono = next_sec("CRONOGRAMA DE ENTREGAS Y PRIORIDADES")
    crono_html = f"""
<div class="section print-new-page print-keep-together">
  <div class="section-header">{h_crono}</div>
  <table class="main-table">
    <thead>
      <tr><th>Prior.</th><th>OT</th><th>Título</th><th>Pzas</th>
          <th>Armado %</th><th>Soldadura %</th><th>Pintura %</th><th>Despacho %</th>
          <th>Avance Real</th><th>Desvío Plan (Esp / Δ)</th><th>Estado</th></tr>
    </thead>
    <tbody>{crono_rows}</tbody>
  </table>
  <div class="legend-row" style="display:flex;gap:16px;align-items:center;flex-wrap:wrap;padding:8px 16px">
    <span style="font-weight:700;color:#374151;font-size:11px">Criticidad de desvío:</span>
    <span><span class="pri-badge" style="background:#fef2f2;color:#dc2626">⛔ CRÍTICO</span>
      <span style="font-size:10px;color:#6b7280">&nbsp;Desv &lt; −20% y entrega ≤ 30 días</span></span>
    <span><span class="pri-badge" style="background:#fff7ed;color:#ea580c">⚠ RIESGO</span>
      <span style="font-size:10px;color:#6b7280">&nbsp;Desv −10% a −20%</span></span>
    <span><span class="pri-badge" style="background:#f0fdf4;color:#16a34a">✔ NORMAL</span>
      <span style="font-size:10px;color:#6b7280">&nbsp;Desv &gt; −10%</span></span>
    <span style="font-size:10px;color:#9ca3af">&nbsp;|&nbsp; "–" = sin programación cargada</span>
  </div>
</div>"""

    # Sección Gantt
    h_gantt   = next_sec("PROGRAMACIÓN DE FABRICACIÓN – DIAGRAMA GANTT")
    gantt_svg = _svg_gantt(d.get("prog_rows", []), ots, d.get("avance_by_ot", {}))
    gantt_html = f"""
<div class="section">
  <div class="section-header">{h_gantt}</div>
  <div class="section-body" style="overflow-x:auto;padding:14px 12px">
    {gantt_svg}
    <div style="font-size:10px;color:#9ca3af;margin-top:8px;padding:0 12px">
      <strong>Leyenda:</strong> Barras coloreadas = Programación de fabricación | ▧ Rayado gris = Subcontratos | ▬ Verde (debajo) = % Avance total OT | ◆ Rojo = Fecha de entrega | — Roja punteada = Hoy
    </div>
  </div>
</div>"""

    # Sección Observaciones (solo INTERNO)
    obs_html = ""
    if is_interno:
        alertas   = []
        situacion = []
        acciones  = []

        if avance_pct < 20:
            alertas.append(f"Avance global {avance_pct}%: obra en etapa inicial.")

        sin_mov  = [f"OT {ot[0]}" for ot in ots if appr[ot[0]]["ARMADO"] == 0]
        urgentes = [ot for ot in ots if _priority(ot[3])[0] == "URGENTE"]

        if sin_mov:
            alertas.append(f"{len(sin_mov)} OTs sin piezas iniciadas (0%): {', '.join(sin_mov[:6])}.")
        if d["hs_cons"] == 0:
            alertas.append("0 hs consumidas registradas – verificar carga de partes diarios.")
        for ot in urgentes:
            alertas.append(f"OT {ot[0]} ({_e(ot[1])[:22]}): entrega {_fd(ot[3])}, requiere máxima atención.")

        ots_avanzadas = sorted(
            [(ot[0], ot[1], appr[ot[0]]) for ot in ots if appr[ot[0]]["ARMADO"] > 0],
            key=lambda x: x[2]["ARMADO"], reverse=True
        )
        for ot_id, titulo, ot_appr in ots_avanzadas[:4]:
            total  = total_by_ot.get(ot_id, 0)
            n_a    = ot_appr["ARMADO"]
            n_s    = ot_appr["SOLDADURA"]
            n_p    = ot_appr["PINTURA"]
            parts  = [f"{n_a}/{total} armado"]
            if n_s: parts.append(f"{n_s} soldadura")
            if n_p: parts.append(f"{n_p} pintura")
            situacion.append(f"OT {ot_id} ({_e(titulo)[:28]}): {', '.join(parts)}.")
        if d["kg_prod"] > 0:
            situacion.append(f"{d['kg_prod']:,.1f} kg producidos en el período.")

        if sin_mov:
            acciones.append(f"Activar inicio en OTs sin movimiento ({', '.join(sin_mov[:5])}).")
        if d["hs_cons"] == 0:
            acciones.append("Regularizar carga de hs consumidas en el sistema.")
        for ot in urgentes[:3]:
            acciones.append(f"Priorizar OT {ot[0]} – {_e(ot[1])[:22]} (entrega {_fd(ot[3])}).")
        if not acciones:
            acciones.append("Continuar plan de producción según programación vigente.")

        def _li(items):
            if not items:
                return '<li style="color:#9ca3af">Sin observaciones</li>'
            return "".join(f"<li>{i}</li>" for i in items)

        h_obs    = next_sec("OBSERVACIONES Y PRÓXIMOS PASOS")
        obs_html = f"""
<div class="section">
  <div class="section-header">{h_obs}</div>
  <div class="obs-grid">
    <div class="obs-col alertas">
      <div class="obs-col-header">■ Alertas / Desvíos</div>
      <ul>{_li(alertas)}</ul>
    </div>
    <div class="obs-col situacion">
      <div class="obs-col-header">✓ Situación actual</div>
      <ul>{_li(situacion)}</ul>
    </div>
    <div class="obs-col acciones">
      <div class="obs-col-header">→ Acciones semana próxima</div>
      <ul>{_li(acciones)}</ul>
    </div>
  </div>
</div>"""

    # Footer firmas con imágenes digitales
    firma_html = f"""
<div class="firma-row">
  <div class="firma-box">
    <img src="/firma-supervisor/002-Dani.png" alt="Firma Daniel Hereñu" style="height:50px;object-fit:contain;margin-bottom:8px;">
    <div class="firma-line"></div>
    <div class="firma-cargo">Jefe de Producción</div>
    <div class="firma-nombre">Daniel Hereñu</div>
  </div>
  <div class="firma-box">
    <img src="/firma-supervisor/003-Gabi.png" alt="Firma Gabriel Ibarra" style="height:50px;object-fit:contain;margin-bottom:8px;">
    <div class="firma-line"></div>
    <div class="firma-cargo">Ing. Responsable</div>
    <div class="firma-nombre">Gabriel Ibarra</div>
  </div>
</div>"""

    tipo_label = "INFORME INTERNO" if is_interno else "INFORME PARA CLIENTE"

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Informe de Avance – {obra}</title>
  <style>{css}</style>
</head>
<body>
<div class="report-wrap">

  <!-- Controles (no imprime) -->
  <div class="no-print" style="display:flex;gap:10px;margin-bottom:12px;align-items:center;flex-wrap:wrap">
    <button onclick="window.print()" style="padding:8px 20px;background:#e36c09;color:#fff;border:none;border-radius:8px;font-weight:700;cursor:pointer;font-size:13px">🖨 Imprimir / PDF</button>
    <a href="/modulo/reportes" style="display:inline-flex;align-items:center;gap:6px;padding:8px 18px;background:#e36c09;color:#fff;border-radius:8px;font-size:13px;font-weight:700;text-decoration:none">← Volver</a>
    <span style="font-size:11px;color:#9ca3af;margin-left:auto">{tipo_label}</span>
  </div>

  <div class="print-running-header">
    <div class="print-running-header-left">
      <img src="/logo-a3" alt="A3" class="print-running-logo">
      <div>
        <div class="print-running-title">{obra} · {report_title}</div>
        <div class="print-running-meta">{periodo_lbl} · {periodo}</div>
      </div>
    </div>
    <div class="print-running-right">
      <div class="print-running-title">{tipo_label}</div>
      <div class="print-running-meta">{week_end.strftime('%d %b %Y')}</div>
    </div>
  </div>

  <!-- Encabezado -->
  <div class="rpt-header">
    <div class="rpt-header-left">
      <img src="/logo-a3" alt="A3" class="rpt-header-logo">
      <div>
        <div class="rpt-title">{report_title} – FABRICACIÓN ESTRUCTURAS METÁLICAS</div>
        <div class="rpt-subtitle">{periodo_lbl} &nbsp;|&nbsp; {periodo}</div>
      </div>
    </div>
    <div class="rpt-header-right">
      <div><span class="rpt-badge">{tipo_label}</span></div>
      <div class="rpt-date">{week_end.strftime('%d %b %Y')}</div>
    </div>
  </div>

  {ficha_html}
  {kpi_estado_html}
  {kpi_html}
  {estado_html}
  {grafico_html}
  {prod_html}
  {tendencia_html}
  {desvio_html}
  {crono_html}
  {gantt_html}
  {obs_html}
  {firma_html}

</div>

<div class="print-page-footer">
  <span>{obra} · {periodo_lbl}</span>
  <span class="print-page-number"></span>
</div>
</body>
</html>"""


# ── Rutas ─────────────────────────────────────────────────────────────────────

@reportes_bp.route("/modulo/reportes")
def reportes_index():
    import calendar
    db     = get_db()
    obras  = [r[0] for r in db.execute(
        "SELECT DISTINCT obra FROM ordenes_trabajo WHERE obra IS NOT NULL AND obra != '' ORDER BY obra"
    ).fetchall()]

    today      = date.today()
    iso        = today.isocalendar()
    obra_sel   = request.args.get("obra", obras[0] if obras else "")
    tipo_sel   = request.args.get("tipo", "INTERNO")
    periodo_sel = request.args.get("periodo_tipo", "SEMANAL")

    obs_opts = "\n".join(
        f'<option value="{_e(o)}" {"selected" if o == obra_sel else ""}>{_e(o)}</option>'
        for o in obras
    )

    week_opts = ""
    cur_y, cur_w = iso[0], iso[1]
    for i in range(11, -1, -1):
        wd = today - timedelta(weeks=i)
        y, w, _ = wd.isocalendar()
        mon, sun = _week_range(y, w)
        lbl = f"Sem {w} – {mon.strftime('%d %b')} al {sun.strftime('%d %b %Y')}"
        sel = "selected" if y == cur_y and w == cur_w else ""
        week_opts += f'<option value="{y}-{w:02d}" {sel}>{lbl}</option>\n'

    month_opts = ""
    for i in range(11, -1, -1):
        # Go back i months
        m = today.month - i
        y = today.year
        while m <= 0:
            m += 12
            y -= 1
        lbl = date(y, m, 1).strftime("%B %Y").capitalize()
        sel = "selected" if i == 0 else ""
        month_opts += f'<option value="{y}-{m:02d}" {sel}>{lbl}</option>\n'

    int_act = "active" if tipo_sel == "INTERNO" else ""
    cli_act = "active" if tipo_sel == "CLIENTE" else ""
    all_act = "active" if tipo_sel == "INTERNO_SEMANAL_TODAS" else ""
    int_chk = "checked" if tipo_sel == "INTERNO" else ""
    cli_chk = "checked" if tipo_sel == "CLIENTE" else ""
    all_chk = "checked" if tipo_sel == "INTERNO_SEMANAL_TODAS" else ""
    sem_act = "active" if periodo_sel == "SEMANAL" else ""
    mes_act = "active" if periodo_sel == "MENSUAL" else ""
    sem_chk = "checked" if periodo_sel == "SEMANAL" else ""
    mes_chk = "checked" if periodo_sel == "MENSUAL" else ""

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Generar Informe – A3</title>
  <style>
    *{{box-sizing:border-box}}
    body{{font-family:Arial,sans-serif;background:#f3f4f6;min-height:100vh;display:flex;align-items:center;justify-content:center;margin:0}}
    .card{{background:#fff;border-radius:12px;padding:36px 40px;box-shadow:0 4px 24px rgba(0,0,0,.1);width:480px}}
    h2{{margin:0 0 4px;color:#1f2937;font-size:1.3rem}}
    .sub{{color:#6b7280;font-size:.85rem;margin:0 0 26px}}
    label.field-label{{display:block;font-size:.78rem;font-weight:700;color:#374151;margin-bottom:4px;text-transform:uppercase;letter-spacing:.5px}}
    select{{width:100%;padding:9px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:.9rem;margin-bottom:18px;color:#1f2937}}
    .tipo-group{{display:flex;gap:10px;margin-bottom:22px}}
    .tipo-btn{{flex:1;border:2px solid #e5e7eb;border-radius:8px;padding:12px 8px;text-align:center;cursor:pointer;transition:.15s;position:relative}}
    .tipo-btn input{{position:absolute;opacity:0;width:0;height:0}}
    .tipo-btn.active{{border-color:#e36c09;background:#fff7ed}}
    .tipo-btn.active-blue{{border-color:#2563eb;background:#eff6ff}}
    .tipo-btn strong{{display:block;font-size:.95rem;color:#1f2937}}
    .tipo-btn small{{color:#6b7280;font-size:.75rem}}
    .btn{{width:100%;padding:13px;background:#e36c09;color:#fff;border:none;border-radius:8px;font-size:1rem;font-weight:700;cursor:pointer}}
    .btn:hover{{background:#c95a06}}
    .logo-row{{display:flex;align-items:center;gap:12px;margin-bottom:22px}}
    .logo-row img{{height:34px;object-fit:contain}}
    .logo-row span{{color:#374151;font-weight:700;font-size:1rem}}
    .hidden{{display:none}}
  </style>
</head>
<body>
<div class="card">
  <div class="logo-row">
    <img src="/logo-a3" alt="A3">
    <span>A3 Servicios Constructivos</span>
  </div>
  <div style="margin-bottom:16px">
    <a href="/" style="font-size:.8rem;color:#6b7280;text-decoration:none;display:inline-flex;align-items:center;gap:4px">← Volver al panel</a>
  </div>
  <h2>Informe de Avance</h2>
  <p class="sub">Seleccioná la obra, el período y el tipo de informe.</p>
  <form action="/modulo/reportes/ver" method="GET">
    <div id="wrap_obra">
      <label class="field-label">Obra / Proyecto</label>
      <select name="obra">{obs_opts}</select>
    </div>

    <label class="field-label">Tipo de período</label>
    <div class="tipo-group" id="pg">
      <label class="tipo-btn {sem_act}" id="lb_sem">
        <input type="radio" name="periodo_tipo" value="SEMANAL" {sem_chk} id="r_sem">
        <strong>SEMANAL</strong>
        <small>Por semana ISO</small>
      </label>
      <label class="tipo-btn {mes_act}" id="lb_mes">
        <input type="radio" name="periodo_tipo" value="MENSUAL" {mes_chk} id="r_mes">
        <strong>MENSUAL</strong>
        <small>Por mes completo</small>
      </label>
    </div>

    <div id="wrap_sem">
      <label class="field-label">Semana evaluada</label>
      <select name="semana">{week_opts}</select>
    </div>
    <div id="wrap_mes" class="hidden">
      <label class="field-label">Mes evaluado</label>
      <select name="mes">{month_opts}</select>
    </div>

    <label class="field-label">Tipo de informe</label>
    <div class="tipo-group" id="tg">
      <label class="tipo-btn {int_act}">
        <input type="radio" name="tipo" value="INTERNO" {int_chk}>
        <strong>INTERNO</strong>
        <small>HS · Eficiencia · KG · Observaciones</small>
      </label>
      <label class="tipo-btn {cli_act}">
        <input type="radio" name="tipo" value="CLIENTE" {cli_chk}>
        <strong>CLIENTE</strong>
        <small>Solo avance de obra</small>
      </label>
      <label class="tipo-btn {all_act}" id="lb_all_obras">
        <input type="radio" name="tipo" value="INTERNO_SEMANAL_TODAS" {all_chk} id="r_all_obras">
        <strong>INTERNO SEMANAL (TODAS)</strong>
        <small>Consolida todas las obras y todos los indicadores</small>
      </label>
    </div>
    <button type="submit" class="btn">Generar Informe ↗</button>
  </form>
</div>
<script>
function togglePeriodo() {{
  const isSem = document.getElementById('r_sem').checked;
  document.getElementById('wrap_sem').classList.toggle('hidden', !isSem);
  document.getElementById('wrap_mes').classList.toggle('hidden', isSem);
  document.getElementById('lb_sem').classList.toggle('active', isSem);
  document.getElementById('lb_mes').classList.toggle('active', !isSem);
}}
document.querySelectorAll('#pg input').forEach(r => r.addEventListener('change', togglePeriodo));
togglePeriodo();
document.querySelectorAll('#tg .tipo-btn input').forEach(r => {{
  r.addEventListener('change', () => {{
    document.querySelectorAll('#tg .tipo-btn').forEach(b => b.classList.remove('active'));
    r.closest('.tipo-btn').classList.add('active');
    toggleScopeByTipo();
  }});
}});

function toggleScopeByTipo() {{
  const all = document.getElementById('r_all_obras').checked;
  document.getElementById('wrap_obra').classList.toggle('hidden', all);
  document.getElementById('lb_mes').classList.toggle('hidden', all);
  if (all) {{
    document.getElementById('r_sem').checked = true;
  }}
  togglePeriodo();
}}
toggleScopeByTipo();
</script>
</body>
</html>"""


@reportes_bp.route("/modulo/reportes/ver")
def reportes_ver():
    import calendar
    db          = get_db()
    obra        = request.args.get("obra", "").strip()
    tipo        = request.args.get("tipo", "INTERNO").upper()
    periodo_tipo = request.args.get("periodo_tipo", "SEMANAL").upper()
    semana      = request.args.get("semana", "")
    mes         = request.args.get("mes", "")

    modo_todas = tipo == "INTERNO_SEMANAL_TODAS"
    if not modo_todas and not obra:
      return redirect(url_for("reportes.reportes_index"))

    today = date.today()
    iso   = today.isocalendar()

    if modo_todas:
      periodo_tipo = "SEMANAL"

    if periodo_tipo == "MENSUAL":
        try:
            parts = mes.split("-")
            m_year, m_month = int(parts[0]), int(parts[1])
        except Exception:
            m_year, m_month = today.year, today.month
        week_start = date(m_year, m_month, 1)
        last_day   = calendar.monthrange(m_year, m_month)[1]
        week_end   = date(m_year, m_month, last_day)
        year       = m_year
        week       = week_start.isocalendar()[1]
    else:
        try:
            parts  = semana.split("-")
            year   = int(parts[0])
            week   = int(parts[1])
        except Exception:
            year, week = iso[0], iso[1]
        week_start, week_end = _week_range(year, week)

    d = _collect(db, None if modo_todas else obra, year, week, week_start, week_end)

    if d is None:
      if modo_todas:
        return "<h3>Sin datos para reporte semanal consolidado de todas las obras.</h3>", 404
        return f"<h3>Sin datos para la obra <b>{_e(obra)}</b>. Verificá que tenga OTs activas.</h3>", 404

    tipo_render = "INTERNO" if modo_todas else tipo
    html = _render_html(d, tipo_render, periodo_tipo)
    return Response(html, mimetype="text/html")
