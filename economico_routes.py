"""
Módulo Económico — Costos Previstos vs Reales, KPIs, Desvíos y Avance
"""
import html as html_lib
from datetime import datetime
from flask import Blueprint, request, redirect, session

from db_utils import get_db

economico_bp = Blueprint("economico", __name__)

_E = html_lib.escape

# ─────────────────────────────────────────────────────────────────────────────
# SCHEMA
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_schema(db):
    db.execute("""
    CREATE TABLE IF NOT EXISTS economico_presupuesto (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ot_id INTEGER NOT NULL UNIQUE,
        mat_previsto      REAL DEFAULT 0,
        pintura_previsto  REAL DEFAULT 0,
        mo_previsto       REAL DEFAULT 0,
        consumibles_previsto REAL DEFAULT 0,
        ingenieria_previsto  REAL DEFAULT 0,
        gastos_gen_previsto  REAL DEFAULT 0,
        impuestos_previsto   REAL DEFAULT 0,
        beneficio_previsto   REAL DEFAULT 0,
        updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
    )""")

    db.execute("""
    CREATE TABLE IF NOT EXISTS economico_costos_reales (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ot_id              INTEGER NOT NULL UNIQUE,
        mat_real           REAL DEFAULT 0,
        pintura_real       REAL DEFAULT 0,
        subcontratos_real  REAL DEFAULT 0,
        updated_at         DATETIME DEFAULT CURRENT_TIMESTAMP
    )""")

    db.execute("""
    CREATE TABLE IF NOT EXISTS economico_config (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        precio_hora_mo   REAL DEFAULT 0,
        precio_hora_cons REAL DEFAULT 0,
        pct_gastos_gen   REAL DEFAULT 5.0,
        pct_impuestos    REAL DEFAULT 3.0,
        updated_at       DATETIME DEFAULT CURRENT_TIMESTAMP
    )""")

    # Garantizar fila de config global
    row = db.execute("SELECT id FROM economico_config LIMIT 1").fetchone()
    if not row:
        db.execute(
            "INSERT INTO economico_config (precio_hora_mo, precio_hora_cons, pct_gastos_gen, pct_impuestos) VALUES (0, 0, 5.0, 3.0)"
        )
    db.commit()


# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _get_config(db):
    row = db.execute(
        "SELECT precio_hora_mo, precio_hora_cons, pct_gastos_gen, pct_impuestos FROM economico_config LIMIT 1"
    ).fetchone()
    return {
        "precio_hora_mo":   float(row[0] or 0) if row else 0.0,
        "precio_hora_cons": float(row[1] or 0) if row else 0.0,
        "pct_gastos_gen":   float(row[2] or 5.0) if row else 5.0,
        "pct_impuestos":    float(row[3] or 3.0) if row else 3.0,
    }


def _calc_economico(db, ot_id):
    """Retorna dict completo con presupuesto, reales, KPIs y desvíos para un OT."""
    cfg = _get_config(db)

    # ── Presupuesto ──────────────────────────────────────────────────────────
    pres = db.execute(
        """SELECT mat_previsto, pintura_previsto, mo_previsto, consumibles_previsto,
                  ingenieria_previsto, gastos_gen_previsto, impuestos_previsto, beneficio_previsto
           FROM economico_presupuesto WHERE ot_id = ?""",
        (ot_id,),
    ).fetchone()
    p_mat    = float(pres[0] or 0) if pres else 0.0
    p_pint   = float(pres[1] or 0) if pres else 0.0
    p_mo     = float(pres[2] or 0) if pres else 0.0
    p_cons   = float(pres[3] or 0) if pres else 0.0
    p_ing    = float(pres[4] or 0) if pres else 0.0
    p_gg     = float(pres[5] or 0) if pres else 0.0
    p_imp    = float(pres[6] or 0) if pres else 0.0
    p_ben    = float(pres[7] or 0) if pres else 0.0
    p_costo_directo = p_mat + p_pint + p_mo + p_cons + p_ing
    p_total_costos  = p_costo_directo + p_gg + p_imp
    p_precio_venta  = p_total_costos + p_ben

    # ── Costos reales manuales ────────────────────────────────────────────────
    real_m = db.execute(
        "SELECT mat_real, pintura_real, subcontratos_real FROM economico_costos_reales WHERE ot_id = ?",
        (ot_id,),
    ).fetchone()
    r_mat   = float(real_m[0] or 0) if real_m else 0.0
    r_pint  = float(real_m[1] or 0) if real_m else 0.0
    r_sub   = float(real_m[2] or 0) if real_m else 0.0

    # ── HH reales (de partes_trabajo) ────────────────────────────────────────
    hh_row   = db.execute(
        "SELECT COALESCE(SUM(horas), 0) FROM partes_trabajo WHERE ot_id = ?", (ot_id,)
    ).fetchone()
    hh_total = float(hh_row[0] or 0) if hh_row else 0.0

    # ── Costos reales automáticos ─────────────────────────────────────────────
    r_mo   = hh_total * cfg["precio_hora_mo"]
    r_cons = hh_total * cfg["precio_hora_cons"]
    r_costo_directo = r_mat + r_pint + r_sub + r_mo + r_cons
    r_gg   = r_costo_directo * cfg["pct_gastos_gen"] / 100.0
    r_imp  = r_costo_directo * cfg["pct_impuestos"] / 100.0
    r_total = r_costo_directo + r_gg + r_imp

    # ── Kg totales (primer registro por posicion para no duplicar) ─────────────
    kg_row = db.execute(
        """SELECT COALESCE(SUM(CAST(p.peso AS REAL)), 0)
           FROM (SELECT MIN(id) AS id FROM procesos WHERE ot_id = ? GROUP BY posicion) ids
           JOIN procesos p ON p.id = ids.id
           WHERE p.peso IS NOT NULL AND CAST(p.peso AS REAL) > 0""",
        (ot_id,),
    ).fetchone()
    kg_total = float(kg_row[0] or 0) if kg_row else 0.0

    # ── Avance físico (estado_avance en ordenes_trabajo) ──────────────────────
    ot_row = db.execute(
        "SELECT estado_avance FROM ordenes_trabajo WHERE id = ?", (ot_id,)
    ).fetchone()
    avance_fisico = float(ot_row[0] or 0) if ot_row else 0.0

    # ── Avance económico = gasto real / costo total presupuestado ─────────────
    avance_economico = (r_total / p_total_costos * 100.0) if p_total_costos > 0 else 0.0

    # ── KPIs ─────────────────────────────────────────────────────────────────
    usd_kg_prev = p_precio_venta / kg_total if kg_total > 0 else 0.0
    usd_kg_real = r_total / kg_total if kg_total > 0 else 0.0
    margen_prev = (p_ben / p_precio_venta * 100.0) if p_precio_venta > 0 else 0.0
    margen_real = ((p_precio_venta - r_total) / p_precio_venta * 100.0) if p_precio_venta > 0 else 0.0

    # ── Desvíos por rubro ─────────────────────────────────────────────────────
    def _desv(prev, real):
        d_abs = real - prev
        d_pct = (d_abs / prev * 100.0) if prev != 0 else (0.0 if real == 0 else 100.0)
        return d_abs, d_pct

    rubros = [
        ("Materiales",       p_mat,  r_mat),
        ("Pintura",          p_pint, r_pint),
        ("Mano de Obra",     p_mo,   r_mo),
        ("Consumibles",      p_cons, r_cons),
        ("Ingeniería",       p_ing,  0.0),
        ("Subcontratos",     0.0,    r_sub),
        ("Gastos Generales", p_gg,   r_gg),
        ("Impuestos",        p_imp,  r_imp),
    ]
    desvios = []
    for nombre, prev, real in rubros:
        d_abs, d_pct = _desv(prev, real)
        desvios.append({
            "nombre": nombre,
            "previsto": prev,
            "real":     real,
            "desv_abs": d_abs,
            "desv_pct": d_pct,
        })

    return {
        "presupuesto": {
            "mat": p_mat, "pintura": p_pint, "mo": p_mo, "consumibles": p_cons,
            "ingenieria": p_ing, "gastos_gen": p_gg, "impuestos": p_imp,
            "beneficio": p_ben, "costo_directo": p_costo_directo,
            "total_costos": p_total_costos, "precio_venta": p_precio_venta,
        },
        "real_manual": {"mat": r_mat, "pintura": r_pint, "subcontratos": r_sub},
        "real_auto":   {"mo": r_mo, "consumibles": r_cons, "gastos_gen": r_gg, "impuestos": r_imp},
        "real":        {"costo_directo": r_costo_directo, "total": r_total},
        "hh_total":    hh_total,
        "kg_total":    kg_total,
        "kpi": {
            "usd_kg_prev":     usd_kg_prev,
            "usd_kg_real":     usd_kg_real,
            "margen_prev":     margen_prev,
            "margen_real":     margen_real,
            "avance_fisico":   avance_fisico,
            "avance_economico": min(avance_economico, 999.9),
        },
        "desvios": desvios,
        "config": cfg,
    }


def _m(val):
    """Formatea como moneda argentina."""
    try:
        v = float(val or 0)
        return f"$ {v:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "$ 0"


def _pct(val):
    try:
        return f"{float(val or 0):.1f}%"
    except Exception:
        return "0.0%"


def _color_desv(d_abs):
    """Verde si igual/ahorro, rojo si sobrecosto."""
    if d_abs <= 0:
        return "#166534"   # verde
    return "#991b1b"       # rojo


def _color_margen(margen):
    if margen >= 10:
        return "#166534"
    if margen >= 0:
        return "#92400e"
    return "#991b1b"


def _progress_bar(pct, color="#3b82f6", height=10):
    w = min(max(float(pct or 0), 0), 100)
    return (
        f'<div style="background:#e5e7eb;border-radius:999px;height:{height}px;width:100%;min-width:60px;">'
        f'<div style="background:{color};border-radius:999px;height:{height}px;width:{w:.1f}%;transition:width .3s;"></div>'
        f'</div><span style="font-size:10px;color:#6b7280;">{w:.1f}%</span>'
    )


# ─────────────────────────────────────────────────────────────────────────────
# RUTA: CONFIG
# ─────────────────────────────────────────────────────────────────────────────

@economico_bp.route("/modulo/economico/config", methods=["GET", "POST"])
def economico_config():
    db = get_db()
    _ensure_schema(db)
    mensaje = ""
    error = ""

    if request.method == "POST":
        try:
            precio_mo   = float(request.form.get("precio_hora_mo", 0) or 0)
            precio_cons = float(request.form.get("precio_hora_cons", 0) or 0)
            pct_gg      = float(request.form.get("pct_gastos_gen", 5) or 5)
            pct_imp     = float(request.form.get("pct_impuestos", 3) or 3)
            db.execute(
                """UPDATE economico_config
                   SET precio_hora_mo=?, precio_hora_cons=?, pct_gastos_gen=?, pct_impuestos=?,
                       updated_at=CURRENT_TIMESTAMP""",
                (precio_mo, precio_cons, pct_gg, pct_imp),
            )
            db.commit()
            mensaje = "Configuración guardada correctamente."
        except Exception as exc:
            error = f"Error al guardar: {exc}"

    cfg = _get_config(db)

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Módulo Económico — Config</title>
  <style>
    body{{font-family:system-ui,sans-serif;background:#f1f5f9;margin:0;padding:24px;}}
    .card{{background:#fff;border-radius:12px;box-shadow:0 2px 10px rgba(0,0,0,.08);padding:28px;max-width:520px;margin:auto;}}
    h2{{margin:0 0 20px;color:#1e293b;font-size:1.25rem;}}
    .form-group{{margin-bottom:16px;}}
    label{{display:block;font-size:.85rem;color:#374151;font-weight:600;margin-bottom:4px;}}
    input[type=number]{{width:100%;box-sizing:border-box;padding:9px 12px;border:1px solid #d1d5db;border-radius:6px;font-size:.95rem;}}
    input[type=number]:focus{{outline:none;border-color:#6366f1;box-shadow:0 0 0 3px rgba(99,102,241,.15);}}
    .btn{{background:#6366f1;color:#fff;border:none;padding:10px 22px;border-radius:7px;font-size:.95rem;cursor:pointer;font-weight:600;}}
    .btn:hover{{background:#4f46e5;}}
    .msg-ok{{background:#dcfce7;color:#166534;border:1px solid #86efac;border-radius:6px;padding:8px 14px;margin-bottom:14px;font-size:.9rem;}}
    .msg-err{{background:#fee2e2;color:#991b1b;border:1px solid #fca5a5;border-radius:6px;padding:8px 14px;margin-bottom:14px;font-size:.9rem;}}
    .back{{display:inline-block;margin-bottom:18px;color:#6366f1;text-decoration:none;font-size:.9rem;font-weight:600;}}
    .back:hover{{text-decoration:underline;}}
    .hint{{font-size:.78rem;color:#9ca3af;margin-top:3px;}}
  </style>
</head>
<body>
  <a href="/modulo/economico" class="back">← Volver al módulo</a>
  <div class="card">
    <h2>⚙️ Configuración de Tasas</h2>
    {"<div class='msg-ok'>" + _E(mensaje) + "</div>" if mensaje else ""}
    {"<div class='msg-err'>" + _E(error) + "</div>" if error else ""}
    <form method="post">
      <div class="form-group">
        <label>Precio por HH — Mano de Obra ($)</label>
        <input type="number" name="precio_hora_mo" step="0.01" min="0" value="{cfg['precio_hora_mo']}">
        <p class="hint">Costo real de MO = HH reales × este valor</p>
      </div>
      <div class="form-group">
        <label>Precio por HH — Consumibles ($)</label>
        <input type="number" name="precio_hora_cons" step="0.01" min="0" value="{cfg['precio_hora_cons']}">
        <p class="hint">Costo real de consumibles = HH reales × este valor</p>
      </div>
      <div class="form-group">
        <label>% Gastos Generales sobre costo directo real</label>
        <input type="number" name="pct_gastos_gen" step="0.01" min="0" max="100" value="{cfg['pct_gastos_gen']}">
      </div>
      <div class="form-group">
        <label>% Impuestos sobre costo directo real</label>
        <input type="number" name="pct_impuestos" step="0.01" min="0" max="100" value="{cfg['pct_impuestos']}">
      </div>
      <button type="submit" class="btn">Guardar configuración</button>
    </form>
  </div>
</body>
</html>"""
    return html


# ─────────────────────────────────────────────────────────────────────────────
# RUTA: DASHBOARD
# ─────────────────────────────────────────────────────────────────────────────

@economico_bp.route("/modulo/economico")
def economico_dashboard():
    db = get_db()
    _ensure_schema(db)

    # Todas las OTs activas (no cerradas)
    ots = db.execute(
        """SELECT id, cliente, obra, titulo, tipo_estructura, estado, estado_avance
           FROM ordenes_trabajo
           ORDER BY id DESC"""
    ).fetchall()

    # KPI por tipo de estructura ($/kg promedio)
    tipo_kpis = {}     # tipo -> {count, sum_usdkg_prev, sum_usdkg_real, sum_margen}

    filas_html = ""
    for ot in ots:
        ot_id, cliente, obra, titulo, tipo, estado, av = ot
        data = _calc_economico(db, ot_id)
        kpi  = data["kpi"]

        tipo_lbl = str(tipo or "Sin tipo")
        if tipo_lbl not in tipo_kpis:
            tipo_kpis[tipo_lbl] = {"count": 0, "sum_usdkg_prev": 0.0, "sum_usdkg_real": 0.0, "sum_margen_real": 0.0, "sum_kg": 0.0}
        tipo_kpis[tipo_lbl]["count"] += 1
        tipo_kpis[tipo_lbl]["sum_usdkg_prev"] += kpi["usd_kg_prev"]
        tipo_kpis[tipo_lbl]["sum_usdkg_real"] += kpi["usd_kg_real"]
        tipo_kpis[tipo_lbl]["sum_margen_real"] += kpi["margen_real"]
        tipo_kpis[tipo_lbl]["sum_kg"] += data["kg_total"]

        margen_color = _color_margen(kpi["margen_real"])
        presup_cargado = data["presupuesto"]["precio_venta"] > 0

        af = kpi["avance_fisico"]
        ae = kpi["avance_economico"]
        # Color avance económico: si ae > af → sobrегаст
        ae_color = "#991b1b" if ae > af + 5 else ("#166534" if ae <= af else "#92400e")

        filas_html += f"""
        <tr>
          <td><a href="/modulo/economico/ot/{ot_id}" style="font-weight:700;color:#6366f1;text-decoration:none;">OT {ot_id}</a></td>
          <td>{_E(cliente or '-')}</td>
          <td>{_E(obra or '-')}</td>
          <td><span style="font-size:.78rem;background:#ede9fe;color:#5b21b6;padding:2px 7px;border-radius:999px;">{_E(tipo_lbl)}</span></td>
          <td style="text-align:right;">{data['kg_total']:,.1f}</td>
          <td style="text-align:right;">{"<span style='color:#6366f1;font-weight:600;'>" + _m(data['presupuesto']['precio_venta']) + "</span>" if presup_cargado else "<span style='color:#9ca3af;font-size:.8rem;'>sin presupuesto</span>"}</td>
          <td style="text-align:right;">{_m(data['real']['total'])}</td>
          <td style="text-align:right;font-weight:700;color:{margen_color};">{_pct(kpi['margen_real'])}</td>
          <td>{_progress_bar(af, '#3b82f6', 8)}</td>
          <td style="color:{ae_color};font-weight:600;">{_pct(ae)}</td>
          <td><a href="/modulo/economico/ot/{ot_id}" style="font-size:.8rem;padding:4px 10px;background:#6366f1;color:#fff;border-radius:5px;text-decoration:none;">Ver →</a></td>
        </tr>"""

    # Cards resumen por tipo de estructura
    tipo_cards_html = ""
    for tipo_lbl, tk in sorted(tipo_kpis.items()):
        n = tk["count"]
        avg_usd_kg_prev = tk["sum_usdkg_prev"] / n if n else 0
        avg_usd_kg_real = tk["sum_usdkg_real"] / n if n else 0
        avg_margen = tk["sum_margen_real"] / n if n else 0
        margen_color = _color_margen(avg_margen)
        tipo_cards_html += f"""
        <div style="background:#fff;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,.07);padding:18px 20px;min-width:180px;flex:1;">
          <div style="font-size:.8rem;color:#6b7280;font-weight:700;text-transform:uppercase;letter-spacing:.05em;">{_E(tipo_lbl)}</div>
          <div style="font-size:.75rem;color:#9ca3af;margin-bottom:10px;">{n} OT{"s" if n!=1 else ""} &nbsp;·&nbsp; {tk['sum_kg']:,.0f} kg total</div>
          <div style="display:flex;gap:14px;">
            <div>
              <div style="font-size:.7rem;color:#9ca3af;">$/kg Prev.</div>
              <div style="font-weight:700;color:#6366f1;">{_m(avg_usd_kg_prev)}</div>
            </div>
            <div>
              <div style="font-size:.7rem;color:#9ca3af;">$/kg Real</div>
              <div style="font-weight:700;color:#1e293b;">{_m(avg_usd_kg_real)}</div>
            </div>
            <div>
              <div style="font-size:.7rem;color:#9ca3af;">Margen prom.</div>
              <div style="font-weight:700;color:{margen_color};">{_pct(avg_margen)}</div>
            </div>
          </div>
        </div>"""

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Módulo Económico</title>
  <style>
    *{{box-sizing:border-box;}}
    body{{font-family:system-ui,sans-serif;background:#f1f5f9;margin:0;padding:0;}}
    .header{{background:linear-gradient(135deg,#6366f1,#8b5cf6);color:#fff;padding:20px 28px;display:flex;align-items:center;justify-content:space-between;gap:12px;flex-wrap:wrap;}}
    .header h1{{margin:0;font-size:1.3rem;}}
    .header a{{color:#fff;text-decoration:none;font-size:.85rem;background:rgba(255,255,255,.2);padding:7px 14px;border-radius:7px;}}
    .header a:hover{{background:rgba(255,255,255,.35);}}
    .body{{padding:24px;}}
    .tipo-cards{{display:flex;gap:14px;flex-wrap:wrap;margin-bottom:22px;}}
    table{{width:100%;border-collapse:collapse;background:#fff;border-radius:10px;overflow:hidden;box-shadow:0 2px 8px rgba(0,0,0,.07);}}
    th{{background:#6366f1;color:#fff;padding:10px 12px;text-align:left;font-size:.82rem;font-weight:700;white-space:nowrap;}}
    td{{padding:9px 12px;border-bottom:1px solid #f1f5f9;font-size:.87rem;vertical-align:middle;}}
    tr:last-child td{{border-bottom:none;}}
    tr:hover td{{background:#fafafe;}}
  </style>
</head>
<body>
  <div class="header">
    <div>
      <h1>💰 Módulo Económico</h1>
      <div style="font-size:.82rem;opacity:.85;margin-top:2px;">Costos previstos vs reales · KPIs · Desvíos · Avance</div>
    </div>
    <div style="display:flex;gap:8px;flex-wrap:wrap;">
      <a href="/modulo/economico/config">⚙️ Tasas y %</a>
      <a href="/">← Inicio</a>
    </div>
  </div>
  <div class="body">
    <div class="tipo-cards">{tipo_cards_html if tipo_cards_html else "<p style='color:#9ca3af;font-size:.9rem;'>Sin OTs con tipo de estructura definido.</p>"}</div>

    <table>
      <thead>
        <tr>
          <th>OT</th>
          <th>Cliente</th>
          <th>Obra</th>
          <th>Tipo</th>
          <th>Kg</th>
          <th>Precio Venta (Prev.)</th>
          <th>Costo Real</th>
          <th>Margen Real</th>
          <th>Avance Físico</th>
          <th>Avance Econ.</th>
          <th></th>
        </tr>
      </thead>
      <tbody>
        {filas_html if filas_html else '<tr><td colspan="11" style="text-align:center;color:#9ca3af;padding:28px;">Sin órdenes de trabajo.</td></tr>'}
      </tbody>
    </table>
  </div>
</body>
</html>"""
    return html


# ─────────────────────────────────────────────────────────────────────────────
# RUTA: DETALLE POR OT
# ─────────────────────────────────────────────────────────────────────────────

@economico_bp.route("/modulo/economico/ot/<int:ot_id>", methods=["GET", "POST"])
def economico_ot(ot_id):
    db = get_db()
    _ensure_schema(db)

    ot = db.execute(
        "SELECT id, cliente, obra, titulo, tipo_estructura, estado FROM ordenes_trabajo WHERE id = ?",
        (ot_id,),
    ).fetchone()
    if not ot:
        return "OT no encontrada", 404

    mensaje = ""
    error   = ""

    if request.method == "POST":
        accion = (request.form.get("accion") or "").strip()
        try:
            if accion == "guardar_presupuesto":
                campos = ["mat_previsto", "pintura_previsto", "mo_previsto", "consumibles_previsto",
                          "ingenieria_previsto", "gastos_gen_previsto", "impuestos_previsto", "beneficio_previsto"]
                vals = [float(request.form.get(c, 0) or 0) for c in campos]
                existente = db.execute(
                    "SELECT id FROM economico_presupuesto WHERE ot_id = ?", (ot_id,)
                ).fetchone()
                if existente:
                    db.execute(
                        f"""UPDATE economico_presupuesto
                            SET mat_previsto=?, pintura_previsto=?, mo_previsto=?,
                                consumibles_previsto=?, ingenieria_previsto=?,
                                gastos_gen_previsto=?, impuestos_previsto=?, beneficio_previsto=?,
                                updated_at=CURRENT_TIMESTAMP
                            WHERE ot_id=?""",
                        (*vals, ot_id),
                    )
                else:
                    db.execute(
                        f"""INSERT INTO economico_presupuesto
                            (ot_id, mat_previsto, pintura_previsto, mo_previsto, consumibles_previsto,
                             ingenieria_previsto, gastos_gen_previsto, impuestos_previsto, beneficio_previsto)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (ot_id, *vals),
                    )
                db.commit()
                mensaje = "Presupuesto guardado."

            elif accion == "guardar_costos_reales":
                mat   = float(request.form.get("mat_real", 0) or 0)
                pint  = float(request.form.get("pintura_real", 0) or 0)
                sub   = float(request.form.get("subcontratos_real", 0) or 0)
                existente = db.execute(
                    "SELECT id FROM economico_costos_reales WHERE ot_id = ?", (ot_id,)
                ).fetchone()
                if existente:
                    db.execute(
                        """UPDATE economico_costos_reales
                           SET mat_real=?, pintura_real=?, subcontratos_real=?,
                               updated_at=CURRENT_TIMESTAMP
                           WHERE ot_id=?""",
                        (mat, pint, sub, ot_id),
                    )
                else:
                    db.execute(
                        """INSERT INTO economico_costos_reales (ot_id, mat_real, pintura_real, subcontratos_real)
                           VALUES (?, ?, ?, ?)""",
                        (ot_id, mat, pint, sub),
                    )
                db.commit()
                mensaje = "Costos reales guardados."
        except Exception as exc:
            error = f"Error al guardar: {exc}"

    data = _calc_economico(db, ot_id)
    p    = data["presupuesto"]
    rm   = data["real_manual"]
    ra   = data["real_auto"]
    r    = data["real"]
    kpi  = data["kpi"]
    cfg  = data["config"]

    # ── Tabla de desvíos ─────────────────────────────────────────────────────
    desv_rows = ""
    for d in data["desvios"]:
        color = _color_desv(d["desv_abs"])
        icon  = "▼" if d["desv_abs"] < 0 else ("▲" if d["desv_abs"] > 0 else "–")
        desv_rows += f"""
        <tr>
          <td style="font-weight:600;">{_E(d['nombre'])}</td>
          <td style="text-align:right;">{_m(d['previsto'])}</td>
          <td style="text-align:right;">{_m(d['real'])}</td>
          <td style="text-align:right;font-weight:700;color:{color};">{icon} {_m(abs(d['desv_abs']))}</td>
          <td style="text-align:right;font-weight:700;color:{color};">{icon} {_pct(abs(d['desv_pct']))}</td>
        </tr>"""

    # ── KPI cards ─────────────────────────────────────────────────────────────
    af  = kpi["avance_fisico"]
    ae  = kpi["avance_economico"]
    ae_color = "#991b1b" if ae > af + 5 else ("#166534" if ae <= af else "#92400e")
    m_color  = _color_margen(kpi["margen_real"])

    def _kpi_card(titulo, valor, sub="", color="#6366f1"):
        return f"""<div style="background:#fff;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,.07);padding:16px 18px;flex:1;min-width:140px;border-top:3px solid {color};">
          <div style="font-size:.72rem;color:#6b7280;font-weight:700;text-transform:uppercase;">{titulo}</div>
          <div style="font-size:1.3rem;font-weight:800;color:{color};margin:6px 0 2px;">{valor}</div>
          <div style="font-size:.75rem;color:#9ca3af;">{sub}</div>
        </div>"""

    kpi_cards = (
        _kpi_card("$/kg Previsto",   _m(kpi["usd_kg_prev"]),    f"{data['kg_total']:,.1f} kg", "#6366f1") +
        _kpi_card("$/kg Real",       _m(kpi["usd_kg_real"]),    "", "#1e293b") +
        _kpi_card("Margen Previsto", _pct(kpi["margen_prev"]),  _m(p['beneficio']), "#3b82f6") +
        _kpi_card("Margen Real",     _pct(kpi["margen_real"]),  f"PV {_m(p['precio_venta'])}", m_color) +
        _kpi_card("Avance Físico",   _pct(af),                  "estado_avance OT", "#10b981") +
        _kpi_card("Avance Econ.",    _pct(ae),                   "gasto / presup.", ae_color)
    )

    # ── Campo valor previsto existente ────────────────────────────────────────
    def _fv(v):  # float → string para value en input
        return f"{v:.2f}" if v else ""

    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Económico — OT {ot_id}</title>
  <style>
    *{{box-sizing:border-box;}}
    body{{font-family:system-ui,sans-serif;background:#f1f5f9;margin:0;padding:0;}}
    .header{{background:linear-gradient(135deg,#6366f1,#8b5cf6);color:#fff;padding:18px 26px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:10px;}}
    .header h1{{margin:0;font-size:1.15rem;}}
    .header a{{color:#fff;text-decoration:none;font-size:.82rem;background:rgba(255,255,255,.2);padding:6px 12px;border-radius:6px;}}
    .header a:hover{{background:rgba(255,255,255,.35);}}
    .body{{padding:20px;display:flex;flex-direction:column;gap:18px;}}
    .section{{background:#fff;border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,.07);overflow:hidden;}}
    .section-title{{background:#f8fafc;border-bottom:1px solid #e5e7eb;padding:12px 18px;font-weight:700;font-size:.95rem;color:#1e293b;display:flex;align-items:center;gap:8px;}}
    .section-body{{padding:18px;}}
    .two-col{{display:grid;grid-template-columns:1fr 1fr;gap:18px;}}
    @media(max-width:700px){{.two-col{{grid-template-columns:1fr;}}}}
    .form-row{{display:flex;flex-direction:column;margin-bottom:12px;}}
    label{{font-size:.8rem;font-weight:600;color:#374151;margin-bottom:3px;}}
    input[type=number]{{padding:8px 10px;border:1px solid #d1d5db;border-radius:6px;font-size:.9rem;width:100%;}}
    input[type=number]:focus{{outline:none;border-color:#6366f1;box-shadow:0 0 0 2px rgba(99,102,241,.15);}}
    .btn{{background:#6366f1;color:#fff;border:none;padding:9px 20px;border-radius:7px;font-size:.9rem;cursor:pointer;font-weight:700;margin-top:6px;}}
    .btn:hover{{background:#4f46e5;}}
    .total-row{{background:#f1f5f9;font-weight:700;border-top:2px solid #e5e7eb;}}
    .auto-tag{{font-size:.7rem;background:#dbeafe;color:#1d4ed8;padding:1px 6px;border-radius:3px;margin-left:4px;}}
    .readonly-val{{background:#f8fafc;border:1px solid #e5e7eb;border-radius:6px;padding:8px 10px;font-size:.9rem;color:#374151;font-weight:600;}}
    .kpi-row{{display:flex;flex-wrap:wrap;gap:12px;}}
    .msg-ok{{background:#dcfce7;color:#166534;border:1px solid #86efac;border-radius:6px;padding:8px 14px;margin-bottom:14px;font-size:.88rem;}}
    .msg-err{{background:#fee2e2;color:#991b1b;border:1px solid #fca5a5;border-radius:6px;padding:8px 14px;margin-bottom:14px;font-size:.88rem;}}
    table{{width:100%;border-collapse:collapse;font-size:.87rem;}}
    th{{background:#6366f1;color:#fff;padding:9px 12px;text-align:left;font-size:.8rem;}}
    td{{padding:8px 12px;border-bottom:1px solid #f1f5f9;}}
    tr:last-child td{{border-bottom:none;}}
    .info-badge{{display:inline-block;background:#ede9fe;color:#5b21b6;padding:2px 8px;border-radius:999px;font-size:.75rem;font-weight:700;}}
  </style>
</head>
<body>
  <div class="header">
    <div>
      <h1>💰 Económico — OT {ot_id} &nbsp;<span style="font-size:.9rem;opacity:.8;">{_E(ot[1] or '')} / {_E(ot[2] or '')}</span></h1>
      <div style="font-size:.78rem;opacity:.8;margin-top:2px;">{_E(ot[3] or '')} &nbsp;·&nbsp; Tipo: {_E(ot[4] or 'N/D')}</div>
    </div>
    <div style="display:flex;gap:7px;">
      <a href="/modulo/economico">← Módulo</a>
      <a href="/">Inicio</a>
    </div>
  </div>

  <div class="body">
    {"<div class='msg-ok'>" + _E(mensaje) + "</div>" if mensaje else ""}
    {"<div class='msg-err'>" + _E(error) + "</div>" if error else ""}

    <!-- KPIs -->
    <div class="section">
      <div class="section-title">📊 KPIs · HH registradas: <strong>{data['hh_total']:,.1f} hs</strong> &nbsp;·&nbsp; Kg totales: <strong>{data['kg_total']:,.1f} kg</strong></div>
      <div class="section-body">
        <div class="kpi-row">{kpi_cards}</div>
        <div style="margin-top:14px;">
          <div style="font-size:.8rem;color:#374151;font-weight:700;margin-bottom:6px;">Avance Físico vs Económico</div>
          <div style="display:flex;align-items:center;gap:10px;margin-bottom:4px;">
            <span style="font-size:.75rem;color:#3b82f6;width:90px;">Físico</span>
            {_progress_bar(af, "#3b82f6", 12)}
          </div>
          <div style="display:flex;align-items:center;gap:10px;">
            <span style="font-size:.75rem;color:{ae_color};width:90px;">Económico</span>
            {_progress_bar(ae, ae_color, 12)}
          </div>
        </div>
      </div>
    </div>

    <!-- Presupuesto y Costos Reales -->
    <div class="two-col">

      <!-- Presupuesto -->
      <div class="section">
        <div class="section-title">📋 Costos Previstos (Presupuesto)</div>
        <div class="section-body">
          <form method="post">
            <input type="hidden" name="accion" value="guardar_presupuesto">
            <div class="form-row"><label>Materiales ($)</label>
              <input type="number" name="mat_previsto" step="0.01" min="0" value="{_fv(p['mat'])}"></div>
            <div class="form-row"><label>Pintura ($)</label>
              <input type="number" name="pintura_previsto" step="0.01" min="0" value="{_fv(p['pintura'])}"></div>
            <div class="form-row"><label>Mano de Obra ($)</label>
              <input type="number" name="mo_previsto" step="0.01" min="0" value="{_fv(p['mo'])}"></div>
            <div class="form-row"><label>Consumibles ($)</label>
              <input type="number" name="consumibles_previsto" step="0.01" min="0" value="{_fv(p['consumibles'])}"></div>
            <div class="form-row"><label>Ingeniería ($)</label>
              <input type="number" name="ingenieria_previsto" step="0.01" min="0" value="{_fv(p['ingenieria'])}"></div>
            <div class="form-row"><label>Gastos Generales ($)</label>
              <input type="number" name="gastos_gen_previsto" step="0.01" min="0" value="{_fv(p['gastos_gen'])}"></div>
            <div class="form-row"><label>Impuestos ($)</label>
              <input type="number" name="impuestos_previsto" step="0.01" min="0" value="{_fv(p['impuestos'])}"></div>
            <div class="form-row" style="margin-top:10px;"><label>Beneficio ($)</label>
              <input type="number" name="beneficio_previsto" step="0.01" min="0" value="{_fv(p['beneficio'])}"></div>
            <hr style="border:none;border-top:1px solid #e5e7eb;margin:10px 0;">
            <div style="display:flex;justify-content:space-between;align-items:center;">
              <span style="font-size:.82rem;color:#6b7280;">Costo directo</span>
              <span style="font-weight:700;color:#6366f1;">{_m(p['costo_directo'])}</span>
            </div>
            <div style="display:flex;justify-content:space-between;align-items:center;margin-top:4px;">
              <span style="font-size:.82rem;color:#6b7280;">Total costos</span>
              <span style="font-weight:700;color:#6366f1;">{_m(p['total_costos'])}</span>
            </div>
            <div style="display:flex;justify-content:space-between;align-items:center;margin-top:4px;">
              <span style="font-size:.82rem;color:#374151;font-weight:700;">Precio de Venta</span>
              <span style="font-weight:800;color:#6366f1;font-size:1.05rem;">{_m(p['precio_venta'])}</span>
            </div>
            <button type="submit" class="btn">💾 Guardar presupuesto</button>
          </form>
        </div>
      </div>

      <!-- Costos Reales -->
      <div class="section">
        <div class="section-title">💸 Costos Reales</div>
        <div class="section-body">

          <!-- Manual -->
          <form method="post" style="margin-bottom:16px;">
            <input type="hidden" name="accion" value="guardar_costos_reales">
            <div style="font-size:.82rem;font-weight:700;color:#374151;margin-bottom:8px;border-bottom:1px solid #e5e7eb;padding-bottom:5px;">Carga Manual</div>
            <div class="form-row"><label>Materiales reales ($)</label>
              <input type="number" name="mat_real" step="0.01" min="0" value="{_fv(rm['mat'])}"></div>
            <div class="form-row"><label>Pintura real ($)</label>
              <input type="number" name="pintura_real" step="0.01" min="0" value="{_fv(rm['pintura'])}"></div>
            <div class="form-row"><label>Subcontratos ($)</label>
              <input type="number" name="subcontratos_real" step="0.01" min="0" value="{_fv(rm['subcontratos'])}"></div>
            <button type="submit" class="btn" style="background:#0891b2;">💾 Guardar costos manuales</button>
          </form>

          <!-- Automáticos -->
          <div style="font-size:.82rem;font-weight:700;color:#374151;margin-bottom:8px;border-bottom:1px solid #e5e7eb;padding-bottom:5px;">
            Calculados automáticamente <span class="auto-tag">AUTO</span>
          </div>
          <div class="form-row">
            <label>Mano de Obra <span class="auto-tag">{data['hh_total']:,.1f} HH × ${cfg['precio_hora_mo']:,.2f}</span></label>
            <div class="readonly-val">{_m(ra['mo'])}</div>
          </div>
          <div class="form-row">
            <label>Consumibles <span class="auto-tag">{data['hh_total']:,.1f} HH × ${cfg['precio_hora_cons']:,.2f}</span></label>
            <div class="readonly-val">{_m(ra['consumibles'])}</div>
          </div>
          <div class="form-row">
            <label>Gastos Generales <span class="auto-tag">{cfg['pct_gastos_gen']:.1f}% costo directo</span></label>
            <div class="readonly-val">{_m(ra['gastos_gen'])}</div>
          </div>
          <div class="form-row">
            <label>Impuestos <span class="auto-tag">{cfg['pct_impuestos']:.1f}% costo directo</span></label>
            <div class="readonly-val">{_m(ra['impuestos'])}</div>
          </div>
          <hr style="border:none;border-top:1px solid #e5e7eb;margin:10px 0;">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-top:4px;">
            <span style="font-size:.82rem;color:#6b7280;">Costo directo real</span>
            <span style="font-weight:700;color:#1e293b;">{_m(r['costo_directo'])}</span>
          </div>
          <div style="display:flex;justify-content:space-between;align-items:center;margin-top:4px;">
            <span style="font-size:.82rem;font-weight:700;color:#374151;">Total Costo Real</span>
            <span style="font-weight:800;color:#1e293b;font-size:1.05rem;">{_m(r['total'])}</span>
          </div>
          <div style="margin-top:8px;padding:8px 10px;border-radius:7px;background:#fafafe;border:1px solid #e0e7ff;">
            <div style="font-size:.78rem;color:#6b7280;">Resultado (PV − Costo Real)</div>
            <div style="font-weight:800;font-size:1.1rem;color:{_color_margen(kpi['margen_real'])};">
              {_m(p['precio_venta'] - r['total'])} &nbsp;<span style="font-size:.85rem;">({_pct(kpi['margen_real'])})</span>
            </div>
          </div>
        </div>
      </div>
    </div><!-- /two-col -->

    <!-- Desvíos por rubro -->
    <div class="section">
      <div class="section-title">📉 Desvíos por Rubro</div>
      <div class="section-body" style="padding:0;">
        <table>
          <thead>
            <tr>
              <th>Rubro</th>
              <th style="text-align:right;">Previsto</th>
              <th style="text-align:right;">Real</th>
              <th style="text-align:right;">Desvío $</th>
              <th style="text-align:right;">Desvío %</th>
            </tr>
          </thead>
          <tbody>
            {desv_rows}
            <tr class="total-row">
              <td><strong>TOTAL COSTOS</strong></td>
              <td style="text-align:right;"><strong>{_m(p['total_costos'])}</strong></td>
              <td style="text-align:right;"><strong>{_m(r['total'])}</strong></td>
              <td style="text-align:right;font-weight:800;color:{_color_desv(r['total']-p['total_costos'])};">
                {"▲" if r['total'] > p['total_costos'] else "▼"} {_m(abs(r['total'] - p['total_costos']))}
              </td>
              <td style="text-align:right;font-weight:800;color:{_color_desv(r['total']-p['total_costos'])};">
                {_pct(abs((r['total']-p['total_costos'])/p['total_costos']*100) if p['total_costos'] else 0)}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>

  </div><!-- /body -->
</body>
</html>"""
    return html
