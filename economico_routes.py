"""
Módulo Económico — Costos previstos vs reales agrupados por Obra
KPIs: $/kg · Margen · Desvíos por rubro · Avance físico vs económico
"""
import html as html_lib
from flask import Blueprint, request

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
        mat_previsto         REAL DEFAULT 0,
        pintura_previsto     REAL DEFAULT 0,
        mo_previsto          REAL DEFAULT 0,
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
        ot_id             INTEGER NOT NULL UNIQUE,
        mat_real          REAL DEFAULT 0,
        pintura_real      REAL DEFAULT 0,
        subcontratos_real REAL DEFAULT 0,
        updated_at        DATETIME DEFAULT CURRENT_TIMESTAMP
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
    db.execute("""
    CREATE TABLE IF NOT EXISTS economico_config_obra (
        id               INTEGER PRIMARY KEY AUTOINCREMENT,
        obra             VARCHAR(255) NOT NULL UNIQUE,
        precio_hora_mo   REAL,
        precio_hora_cons REAL,
        pct_gastos_gen   REAL,
        pct_impuestos    REAL,
        updated_at       DATETIME DEFAULT CURRENT_TIMESTAMP
    )""")
    if not db.execute("SELECT id FROM economico_config LIMIT 1").fetchone():
        db.execute("INSERT INTO economico_config (precio_hora_mo,precio_hora_cons,pct_gastos_gen,pct_impuestos) VALUES (0,0,5.0,3.0)")
    try:
        db.execute("ALTER TABLE economico_costos_reales ADD COLUMN ingenieria_real REAL DEFAULT 0")
    except Exception:
        pass
    db.execute("""
    CREATE TABLE IF NOT EXISTS economico_costos_reales_mensual (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        ot_id      INTEGER NOT NULL,
        mes        VARCHAR(7)   NOT NULL,
        concepto   VARCHAR(100) NOT NULL,
        monto      REAL         DEFAULT 0,
        updated_at DATETIME     DEFAULT CURRENT_TIMESTAMP
    )""")
    # Migración única: mover datos del sistema anterior (fila única por OT) a la nueva tabla mensual
    try:
        import datetime as _dt
        _mes_hoy = _dt.date.today().strftime("%Y-%m")
        _old_rows = db.execute(
            "SELECT ot_id,mat_real,pintura_real,subcontratos_real,COALESCE(ingenieria_real,0) "
            "FROM economico_costos_reales "
            "WHERE (mat_real>0 OR pintura_real>0 OR subcontratos_real>0 OR COALESCE(ingenieria_real,0)>0)"
        ).fetchall()
        for _r in _old_rows:
            _oid, _mat, _pint, _sub, _ing = _r
            _ya = db.execute("SELECT COUNT(*) FROM economico_costos_reales_mensual WHERE ot_id=?", (_oid,)).fetchone()[0]
            if _ya == 0:
                for _conc, _val in [("Materiales",_mat),("Pintura",_pint),("Subcontratos",_sub),("Ingeniería",_ing)]:
                    if float(_val or 0) > 0:
                        db.execute("INSERT INTO economico_costos_reales_mensual(ot_id,mes,concepto,monto) VALUES(?,?,?,?)",
                                   (_oid, _mes_hoy, _conc, float(_val)))
    except Exception:
        pass
    db.execute("""
    CREATE TABLE IF NOT EXISTS economico_gastos_fijos (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        mes       VARCHAR(7)   NOT NULL,
        concepto  VARCHAR(255) NOT NULL,
        monto     REAL         DEFAULT 0,
        updated_at DATETIME   DEFAULT CURRENT_TIMESTAMP
    )""")
    db.commit()


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _get_global_config(db):
    row = db.execute("SELECT precio_hora_mo,precio_hora_cons,pct_gastos_gen,pct_impuestos FROM economico_config LIMIT 1").fetchone()
    return {
        "precio_hora_mo":   float(row[0] or 0)   if row else 0.0,
        "precio_hora_cons": float(row[1] or 0)   if row else 0.0,
        "pct_gastos_gen":   float(row[2] or 5.0) if row else 5.0,
        "pct_impuestos":    float(row[3] or 3.0) if row else 3.0,
    }


def _get_config_obra(db, obra):
    """Config para la obra; cae al global si no hay override."""
    gcfg = _get_global_config(db)
    row = db.execute(
        "SELECT precio_hora_mo,precio_hora_cons,pct_gastos_gen,pct_impuestos FROM economico_config_obra WHERE obra=?",
        (obra,),
    ).fetchone()
    if not row:
        return dict(gcfg)
    return {
        "precio_hora_mo":   float(row[0]) if row[0] is not None else gcfg["precio_hora_mo"],
        "precio_hora_cons": float(row[1]) if row[1] is not None else gcfg["precio_hora_cons"],
        "pct_gastos_gen":   float(row[2]) if row[2] is not None else gcfg["pct_gastos_gen"],
        "pct_impuestos":    float(row[3]) if row[3] is not None else gcfg["pct_impuestos"],
    }


def _save_config_obra(db, obra, pmo, pcons, pgg, pimp):
    ex = db.execute("SELECT id FROM economico_config_obra WHERE obra=?", (obra,)).fetchone()
    if ex:
        db.execute("""UPDATE economico_config_obra
            SET precio_hora_mo=?,precio_hora_cons=?,pct_gastos_gen=?,pct_impuestos=?,updated_at=CURRENT_TIMESTAMP
            WHERE obra=?""", (pmo, pcons, pgg, pimp, obra))
    else:
        db.execute("INSERT INTO economico_config_obra(obra,precio_hora_mo,precio_hora_cons,pct_gastos_gen,pct_impuestos) VALUES(?,?,?,?,?)",
                   (obra, pmo, pcons, pgg, pimp))
    db.commit()


# ─────────────────────────────────────────────────────────────────────────────
# CALC ECONÓMICO POR OT
# ─────────────────────────────────────────────────────────────────────────────

def _calc_economico(db, ot_id, cfg):
    pres = db.execute(
        """SELECT mat_previsto,pintura_previsto,mo_previsto,consumibles_previsto,
                  ingenieria_previsto,gastos_gen_previsto,impuestos_previsto,beneficio_previsto
           FROM economico_presupuesto WHERE ot_id=?""", (ot_id,)).fetchone()
    p_mat  = float(pres[0] or 0) if pres else 0.0
    p_pint = float(pres[1] or 0) if pres else 0.0
    p_mo   = float(pres[2] or 0) if pres else 0.0
    p_cons = float(pres[3] or 0) if pres else 0.0
    p_ing  = float(pres[4] or 0) if pres else 0.0
    p_gg   = float(pres[5] or 0) if pres else 0.0
    p_imp  = float(pres[6] or 0) if pres else 0.0
    p_ben  = float(pres[7] or 0) if pres else 0.0
    p_cd = p_mat + p_pint + p_mo + p_cons + p_ing
    p_tc = p_cd + p_gg + p_imp
    p_pv = p_tc + p_ben

    _rm_rows = db.execute(
        "SELECT concepto, COALESCE(SUM(monto),0) FROM economico_costos_reales_mensual "
        "WHERE ot_id=? GROUP BY concepto", (ot_id,)).fetchall()
    _rm = {row[0]: float(row[1] or 0) for row in _rm_rows}
    r_mat      = _rm.get("Materiales",   0.0)
    r_pint     = _rm.get("Pintura",      0.0)
    r_sub      = _rm.get("Subcontratos", 0.0)
    r_ing_real = _rm.get("Ingeniería",   0.0)

    hh = db.execute("SELECT COALESCE(SUM(horas),0) FROM partes_trabajo WHERE ot_id=?", (ot_id,)).fetchone()
    hh_total = float(hh[0] or 0) if hh else 0.0

    r_mo   = hh_total * cfg["precio_hora_mo"]
    r_cons = hh_total * cfg["precio_hora_cons"]
    r_cd   = r_mat + r_pint + r_sub + r_mo + r_cons + r_ing_real
    r_imp  = r_cd * cfg["pct_impuestos"] / 100.0
    r_tot  = r_cd + r_imp

    kg_row = db.execute(
        """SELECT COALESCE(SUM(CAST(p.peso AS REAL)),0)
           FROM (SELECT MIN(id) AS id FROM procesos WHERE ot_id=? GROUP BY posicion) ids
           JOIN procesos p ON p.id=ids.id
           WHERE p.peso IS NOT NULL AND CAST(p.peso AS REAL)>0""", (ot_id,)).fetchone()
    kg = float(kg_row[0] or 0) if kg_row else 0.0

    av = db.execute("SELECT estado_avance FROM ordenes_trabajo WHERE id=?", (ot_id,)).fetchone()
    avf = float(av[0] or 0) if av else 0.0

    return {
        "p":  {"mat":p_mat,"pintura":p_pint,"mo":p_mo,"cons":p_cons,
               "ing":p_ing,"gg":p_gg,"imp":p_imp,"ben":p_ben,"cd":p_cd,"tc":p_tc,"pv":p_pv},
        "rm": {"mat":r_mat,"pintura":r_pint,"sub":r_sub,"ing":r_ing_real},
        "ra": {"mo":r_mo,"cons":r_cons,"imp":r_imp},
        "r":  {"cd":r_cd,"tot":r_tot},
        "hh": hh_total, "kg": kg, "avf": avf,
        "ave": min((r_tot/p_tc*100.0) if p_tc>0 else 0.0, 999.9),
    }


def _aggregate_obra(ots_data):
    agg = {k:0.0 for k in ["p_mat","p_pint","p_mo","p_cons","p_ing","p_gg","p_imp","p_ben",
                            "p_cd","p_tc","p_pv","r_mat","r_pint","r_sub","r_ing","r_mo","r_cons",
                            "r_imp","r_cd","r_tot","hh","kg"]}
    avf_list = []
    for d in ots_data:
        for k,v in [("p_mat",d["p"]["mat"]),("p_pint",d["p"]["pintura"]),("p_mo",d["p"]["mo"]),
                    ("p_cons",d["p"]["cons"]),("p_ing",d["p"]["ing"]),("p_gg",d["p"]["gg"]),
                    ("p_imp",d["p"]["imp"]),("p_ben",d["p"]["ben"]),("p_cd",d["p"]["cd"]),
                    ("p_tc",d["p"]["tc"]),("p_pv",d["p"]["pv"]),
                    ("r_mat",d["rm"]["mat"]),("r_pint",d["rm"]["pintura"]),("r_sub",d["rm"]["sub"]),("r_ing",d["rm"]["ing"]),
                    ("r_mo",d["ra"]["mo"]),("r_cons",d["ra"]["cons"]),
                    ("r_imp",d["ra"]["imp"]),("r_cd",d["r"]["cd"]),("r_tot",d["r"]["tot"]),
                    ("hh",d["hh"]),("kg",d["kg"])]:
            agg[k] += v
        avf_list.append(d["avf"])
    agg["avf"] = sum(avf_list)/len(avf_list) if avf_list else 0.0
    agg["ave"] = min((agg["r_tot"]/agg["p_tc"]*100.0) if agg["p_tc"]>0 else 0.0, 999.9)
    return agg


# ─────────────────────────────────────────────────────────────────────────────
# FORMATTING
# ─────────────────────────────────────────────────────────────────────────────

def _m(v):
    try:
        return f"$ {float(v or 0):,.0f}".replace(",","X").replace(".",",").replace("X",".")
    except Exception:
        return "$ 0"

def _pct(v):
    try: return f"{float(v or 0):.1f}%"
    except Exception: return "0.0%"

def _cm(m):
    if float(m or 0)>=10: return "#166534"
    if float(m or 0)>=0:  return "#92400e"
    return "#991b1b"

def _cd(d):
    return "#991b1b" if float(d or 0)>0 else "#166534"

def _pb(pct, color="#3b82f6", h=10):
    w = min(max(float(pct or 0),0),100)
    return (f'<div style="background:#e5e7eb;border-radius:999px;height:{h}px;width:100%;">'
            f'<div style="background:{color};border-radius:999px;height:{h}px;width:{w:.1f}%;"></div>'
            f'</div><span style="font-size:10px;color:#6b7280;">{w:.1f}%</span>')

def _fv(v):
    f = float(v or 0)
    return f"{f:.2f}" if f else ""

_CSS_COMMON = """*{box-sizing:border-box;}body{font-family:system-ui,sans-serif;background:#f1f5f9;margin:0;padding:0;}
.hdr{background:linear-gradient(135deg,#6366f1,#8b5cf6);color:#fff;padding:16px 22px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;}
.hdr h1{margin:0;font-size:1.1rem;}.hdr a{color:#fff;text-decoration:none;font-size:.8rem;background:rgba(255,255,255,.2);padding:5px 10px;border-radius:6px;}
.hdr a:hover{background:rgba(255,255,255,.35);}
.body{padding:18px;display:flex;flex-direction:column;gap:16px;}
.card{background:#fff;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,.07);overflow:hidden;}
.ct{background:#f8fafc;border-bottom:1px solid #e5e7eb;padding:11px 16px;font-weight:700;font-size:.88rem;color:#1e293b;}
.cb{padding:16px;}
.two{display:grid;grid-template-columns:1fr 1fr;gap:16px;}
@media(max-width:680px){.two{grid-template-columns:1fr;}}
.fg{margin-bottom:12px;}label{font-size:.78rem;font-weight:600;color:#374151;display:block;margin-bottom:2px;}
input[type=number]{width:100%;padding:7px 9px;border:1px solid #d1d5db;border-radius:5px;font-size:.88rem;}
input[type=number]:focus{outline:none;border-color:#6366f1;}
.btn{border:none;padding:8px 16px;border-radius:6px;font-size:.85rem;cursor:pointer;font-weight:700;}
.rv{background:#f8fafc;border:1px solid #e5e7eb;border-radius:5px;padding:7px 9px;font-size:.88rem;font-weight:600;color:#374151;}
table{width:100%;border-collapse:collapse;font-size:.84rem;}
th{background:#6366f1;color:#fff;padding:8px 10px;text-align:left;font-size:.78rem;white-space:nowrap;}
td{padding:7px 10px;border-bottom:1px solid #f1f5f9;vertical-align:middle;}
tr:last-child td{border-bottom:none;}
.auto{font-size:.68rem;background:#dbeafe;color:#1d4ed8;padding:1px 5px;border-radius:3px;margin-left:3px;}
.ok{background:#dcfce7;color:#166534;border:1px solid #86efac;border-radius:6px;padding:8px 12px;font-size:.88rem;}
.er{background:#fee2e2;color:#991b1b;border:1px solid #fca5a5;border-radius:6px;padding:8px 12px;font-size:.88rem;}"""


# ─────────────────────────────────────────────────────────────────────────────
# RUTA: CONFIG GLOBAL
# ─────────────────────────────────────────────────────────────────────────────

@economico_bp.route("/modulo/economico/config", methods=["GET", "POST"])
def economico_config():
  db = get_db()
  _ensure_schema(db)
  mensaje = error = ""
  if request.method == "POST":
    try:
      db.execute(
        "UPDATE economico_config SET precio_hora_mo=?,precio_hora_cons=?,pct_impuestos=?,updated_at=CURRENT_TIMESTAMP",
        (
          float(request.form.get("precio_hora_mo") or 0),
          float(request.form.get("precio_hora_cons") or 0),
          float(request.form.get("pct_impuestos") or 3),
        ),
      )
      db.commit()
      mensaje = "Config global guardada."
    except Exception as exc:
      error = str(exc)
  cfg = _get_global_config(db)
  return f"""<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Config Global</title>
<style>{_CSS_COMMON}
.card{{max-width:480px;margin:auto;}}.card .cb{{padding:20px;}}
.hint{{font-size:.73rem;color:#9ca3af;margin-top:2px;}}
</style></head><body>
<a href="/modulo/economico" style="display:inline-block;margin:16px;color:#6366f1;text-decoration:none;font-size:.88rem;font-weight:600;">← Volver al módulo</a>
<div class="card"><div class="ct">⚙️ Tasas globales por defecto</div><div class="cb">
<p style="font-size:.82rem;color:#6b7280;margin-bottom:16px;">Aplican a todas las obras sin configuración propia.</p>
{"<div class='ok'>" + _E(mensaje) + "</div><br>" if mensaje else ""}
{"<div class='er'>" + _E(error) + "</div><br>" if error else ""}
<form method="post">
<div class="fg"><label>$/HH — Mano de Obra</label>
  <input type="number" name="precio_hora_mo" step="0.01" min="0" value="{cfg['precio_hora_mo']}">
  <p class="hint">MO real = HH × este valor</p></div>
<div class="fg"><label>$/HH — Consumibles</label>
  <input type="number" name="precio_hora_cons" step="0.01" min="0" value="{cfg['precio_hora_cons']}"></div>
<div class="fg"><label>% Impuestos (sobre costo directo real)</label>
  <input type="number" name="pct_impuestos" step="0.01" min="0" max="100" value="{cfg['pct_impuestos']}"></div>
<button type="submit" class="btn" style="background:#6366f1;color:#fff;">Guardar config global</button>
</form></div></div></body></html>"""


# ─────────────────────────────────────────────────────────────────────────────
# RUTA: DASHBOARD (agrupado por obra)
# ─────────────────────────────────────────────────────────────────────────────

@economico_bp.route("/modulo/economico", methods=["GET", "POST"])
def economico_dashboard():
    db = get_db(); _ensure_schema(db)
    mensaje = error = ""

    if request.method == "POST":
        obra_cfg = (request.form.get("obra_cfg") or "").strip()
        if obra_cfg:
            try:
                _save_config_obra(db, obra_cfg,
                    float(request.form.get("precio_hora_mo") or 0),
                    float(request.form.get("precio_hora_cons") or 0),
                    float(request.form.get("pct_gastos_gen") or 5),
                    float(request.form.get("pct_impuestos") or 3))
                mensaje = f"Tasas de '{obra_cfg}' guardadas."
            except Exception as exc: error = str(exc)

    ots = db.execute("SELECT id,cliente,obra,titulo,tipo_estructura FROM ordenes_trabajo ORDER BY obra,id").fetchall()

    obras_dict = {}
    for ot_id, cliente, obra, titulo, tipo in ots:
        k = str(obra or "Sin obra").strip()
        if k not in obras_dict:
            obras_dict[k] = {"cliente": cliente or "", "ots": []}
        obras_dict[k]["ots"].append({"id": ot_id, "titulo": titulo, "tipo": tipo})

    tipo_stats = {}
    obras_html = ""

    for obra_key in sorted(obras_dict.keys()):
        info = obras_dict[obra_key]
        cfg  = _get_config_obra(db, obra_key)
        ots_data = []
        for oi in info["ots"]:
            d = _calc_economico(db, oi["id"], cfg)
            d["ot_id"] = oi["id"]; d["tipo"] = oi["tipo"]
            ots_data.append(d)
            tl = str(oi["tipo"] or "Sin tipo")
            if tl not in tipo_stats:
                tipo_stats[tl] = {"n":0,"rt":0.0,"kg":0.0,"ms":0.0}
            ts = tipo_stats[tl]; ts["n"]+=1; ts["rt"]+=d["r"]["tot"]; ts["kg"]+=d["kg"]
            if d["p"]["pv"]>0: ts["ms"] += (d["p"]["pv"]-d["r"]["tot"])/d["p"]["pv"]*100

        agg = _aggregate_obra(ots_data)
        mg  = ((agg["p_pv"]-agg["r_tot"])/agg["p_pv"]*100.0) if agg["p_pv"]>0 else 0.0
        mc  = _cm(mg)
        af  = agg["avf"]; ae = agg["ave"]
        ac  = "#991b1b" if ae>af+5 else ("#166534" if ae<=af else "#92400e")

        badges = " ".join(
            f'<a href="/modulo/economico/ot/{d["ot_id"]}" style="font-size:.73rem;background:#e0e7ff;color:#4338ca;padding:2px 7px;border-radius:999px;text-decoration:none;font-weight:600;">OT {d["ot_id"]}</a>'
            for d in ots_data)

        obras_html += f"""
<div class="card" style="margin-bottom:14px;">
  <div style="background:linear-gradient(90deg,#f1f5f9,#e8edf5);padding:13px 18px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;border-bottom:1px solid #e5e7eb;">
    <div>
      <span style="font-weight:800;font-size:.98rem;color:#1e293b;">🏗 {_E(obra_key)}</span>
      <span style="font-size:.76rem;color:#64748b;margin-left:8px;">{_E(info['cliente'])}</span>
      <div style="margin-top:5px;display:flex;flex-wrap:wrap;gap:4px;">{badges}</div>
    </div>
    <a href="/modulo/economico/obra/{_E(obra_key)}" style="font-size:.8rem;background:#6366f1;color:#fff;padding:6px 13px;border-radius:7px;text-decoration:none;font-weight:700;white-space:nowrap;">Ver detalle →</a>
  </div>
  <div style="padding:12px 18px;display:flex;flex-wrap:wrap;gap:16px;align-items:center;border-bottom:1px solid #f1f5f9;">
    <div><div style="font-size:.68rem;color:#9ca3af;font-weight:700;">KG</div><div style="font-weight:700;">{agg['kg']:,.0f}</div></div>
    <div><div style="font-size:.68rem;color:#9ca3af;font-weight:700;">PV PREVISTO</div><div style="font-weight:700;color:#6366f1;">{_m(agg['p_pv'])}</div></div>
    <div><div style="font-size:.68rem;color:#9ca3af;font-weight:700;">COSTO REAL</div><div style="font-weight:700;">{_m(agg['r_tot'])}</div></div>
    <div><div style="font-size:.68rem;color:#9ca3af;font-weight:700;">MARGEN REAL</div><div style="font-weight:800;color:{mc};font-size:.98rem;">{_pct(mg)}</div></div>
    <div><div style="font-size:.68rem;color:#9ca3af;font-weight:700;">$/KG REAL</div><div style="font-weight:700;">{_m(agg['r_tot']/agg['kg'] if agg['kg']>0 else 0)}</div></div>
    <div style="flex:1;min-width:200px;">
      <div style="font-size:.68rem;color:#9ca3af;font-weight:700;margin-bottom:4px;">AV. FÍSICO / ECONÓMICO</div>
      <div style="display:flex;align-items:center;gap:7px;margin-bottom:3px;">
        <span style="font-size:.7rem;color:#3b82f6;width:72px;">Físico</span>{_pb(af,"#3b82f6",7)}
      </div>
      <div style="display:flex;align-items:center;gap:7px;">
        <span style="font-size:.7rem;color:{ac};width:72px;">Económico</span>{_pb(ae,ac,7)}
      </div>
    </div>
  </div>
  <form method="post" style="padding:10px 18px;background:#fafafa;display:flex;flex-wrap:wrap;align-items:flex-end;gap:10px;">
    <input type="hidden" name="obra_cfg" value="{_E(obra_key)}">
    <div><label style="font-size:.7rem;font-weight:700;color:#374151;display:block;margin-bottom:2px;">$/HH Mano de Obra</label>
      <input type="number" name="precio_hora_mo" step="0.01" min="0" value="{_fv(cfg['precio_hora_mo'])}" style="width:115px;padding:5px 8px;border:1px solid #d1d5db;border-radius:5px;font-size:.83rem;"></div>
    <div><label style="font-size:.7rem;font-weight:700;color:#374151;display:block;margin-bottom:2px;">$/HH Consumibles</label>
      <input type="number" name="precio_hora_cons" step="0.01" min="0" value="{_fv(cfg['precio_hora_cons'])}" style="width:115px;padding:5px 8px;border:1px solid #d1d5db;border-radius:5px;font-size:.83rem;"></div>
    <div><label style="font-size:.7rem;font-weight:700;color:#374151;display:block;margin-bottom:2px;">% Gastos Gen.</label>
      <input type="number" name="pct_gastos_gen" step="0.01" min="0" max="100" value="{_fv(cfg['pct_gastos_gen'])}" style="width:85px;padding:5px 8px;border:1px solid #d1d5db;border-radius:5px;font-size:.83rem;"></div>
    <div><label style="font-size:.7rem;font-weight:700;color:#374151;display:block;margin-bottom:2px;">% Impuestos</label>
      <input type="number" name="pct_impuestos" step="0.01" min="0" max="100" value="{_fv(cfg['pct_impuestos'])}" style="width:85px;padding:5px 8px;border:1px solid #d1d5db;border-radius:5px;font-size:.83rem;"></div>
    <button type="submit" class="btn" style="background:#0891b2;color:#fff;font-size:.8rem;padding:6px 12px;">💾 Guardar tasas</button>
  </form>
</div>"""

    tipo_cards = "".join(
        f'<div style="background:#fff;border-radius:9px;box-shadow:0 1px 6px rgba(0,0,0,.06);padding:14px 16px;flex:1;min-width:160px;">'
        f'<div style="font-size:.72rem;color:#6b7280;font-weight:700;text-transform:uppercase;">{_E(tl)}</div>'
        f'<div style="font-size:.7rem;color:#9ca3af;margin-bottom:7px;">{ts["n"]} OTs · {ts["kg"]:,.0f} kg</div>'
        f'<div style="display:flex;gap:12px;">'
        f'<div><div style="font-size:.66rem;color:#9ca3af;">$/kg Real</div><div style="font-weight:700;">{_m(ts["rt"]/ts["kg"] if ts["kg"]>0 else 0)}</div></div>'
        f'<div><div style="font-size:.66rem;color:#9ca3af;">Margen</div><div style="font-weight:700;color:{_cm(ts["ms"]/ts["n"] if ts["n"] else 0)};">{_pct(ts["ms"]/ts["n"] if ts["n"] else 0)}</div></div>'
        f'</div></div>'
        for tl, ts in sorted(tipo_stats.items()))

    msg = (f'<div class="ok" style="margin-bottom:12px;">{_E(mensaje)}</div>' if mensaje else "")
    err = (f'<div class="er" style="margin-bottom:12px;">{_E(error)}</div>' if error else "")

    return f"""<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Módulo Económico</title>
<style>{_CSS_COMMON}</style></head><body>
<div class="hdr">
  <div><h1>💰 Módulo Económico</h1>
    <div style="font-size:.76rem;opacity:.8;margin-top:2px;">Agrupado por obra · KPIs · Costos previstos vs reales</div></div>
  <div style="display:flex;gap:7px;flex-wrap:wrap;">
    <a href="/modulo/economico/dashboard-ejecutivo" style="background:#fff;color:#6366f1;font-weight:700;">📊 Dashboard Ejecutivo</a>
    <a href="/modulo/economico/gastos-fijos" style="background:#fef3c7;color:#92400e;font-weight:700;">🏭 Gastos Fijos</a>
    <a href="/modulo/economico/config">⚙️ Config global</a>
    <a href="/">← Inicio</a>
  </div>
</div>
<div class="body">
  {msg}{err}
  {"<div style='display:flex;flex-wrap:wrap;gap:10px;margin-bottom:6px;'>" + tipo_cards + "</div>" if tipo_cards else ""}
  {obras_html if obras_html else "<p style='color:#9ca3af;'>Sin órdenes de trabajo.</p>"}
</div></body></html>"""


# ─────────────────────────────────────────────────────────────────────────────
# RUTA: DETALLE OBRA
# ─────────────────────────────────────────────────────────────────────────────

@economico_bp.route("/modulo/economico/obra/<path:obra_nombre>", methods=["GET", "POST"])
def economico_obra(obra_nombre):
    db = get_db(); _ensure_schema(db)
    mensaje = error = ""

    if request.method == "POST" and (request.form.get("accion") or "") == "config_obra":
        try:
            _save_config_obra(db, obra_nombre,
                float(request.form.get("precio_hora_mo") or 0),
                float(request.form.get("precio_hora_cons") or 0),
                float(request.form.get("pct_gastos_gen") or 5),
                float(request.form.get("pct_impuestos") or 3))
            mensaje = "Tasas actualizadas."
        except Exception as exc: error = str(exc)

    cfg = _get_config_obra(db, obra_nombre)
    ots_rows = db.execute("SELECT id,cliente,titulo,tipo_estructura,estado FROM ordenes_trabajo WHERE obra=? ORDER BY id", (obra_nombre,)).fetchall()
    if not ots_rows:
        return f"<p>Sin OTs para <b>{_E(obra_nombre)}</b>.</p><a href='/modulo/economico'>← Volver</a>", 404

    cliente = ots_rows[0][1] or ""
    ots_data = []
    for ot_id, _, titulo, tipo, estado in ots_rows:
        d = _calc_economico(db, ot_id, cfg)
        d.update({"ot_id":ot_id,"titulo":titulo or "","tipo":tipo or "","estado":estado or ""})
        ots_data.append(d)

    agg = _aggregate_obra(ots_data)
    # Distribuir overhead real de toda la cartera a esta obra según costo directo real.
    ots_all = db.execute(
      "SELECT id, obra, COALESCE(es_mantenimiento,0) FROM ordenes_trabajo ORDER BY obra, id"
    ).fetchall()
    obras_all = {}
    mant_all = {}
    for _ot_id, _obra_key, _es_mant in ots_all:
      _obra_key = str(_obra_key or "Sin obra").strip()
      _dest = mant_all if _es_mant else obras_all
      if _obra_key not in _dest:
        _dest[_obra_key] = {"ots": []}
      _dest[_obra_key]["ots"].append(_ot_id)

    total_prod_cd = 0.0
    for _obra_key, _info in obras_all.items():
      _cfg_obra = _get_config_obra(db, _obra_key)
      _ots_d = []
      for _prod_ot_id in _info["ots"]:
        _d = _calc_economico(db, _prod_ot_id, _cfg_obra)
        _ots_d.append(_d)
      _agg_obra = _aggregate_obra(_ots_d)
      total_prod_cd += _agg_obra["r_cd"]

    total_mant_real = 0.0
    for _obra_key, _info in mant_all.items():
      _cfg_obra = _get_config_obra(db, _obra_key)
      _ots_d = []
      for _mant_ot_id in _info["ots"]:
        _d = _calc_economico(db, _mant_ot_id, _cfg_obra)
        _ots_d.append(_d)
      _agg_obra = _aggregate_obra(_ots_d)
      total_mant_real += _agg_obra["r_tot"]

    total_gf_real = db.execute("SELECT COALESCE(SUM(monto),0) FROM economico_gastos_fijos").fetchone()[0] or 0.0
    total_estructura_real = float(total_mant_real or 0.0) + float(total_gf_real or 0.0)
    gg_asig_obra = (total_estructura_real * (agg["r_cd"] / total_prod_cd)) if total_prod_cd > 0 else 0.0
    r_tot_adj = agg["r_tot"] + gg_asig_obra
    mg  = ((agg["p_pv"]-r_tot_adj)/agg["p_pv"]*100.0) if agg["p_pv"]>0 else 0.0
    mc  = _cm(mg)
    af  = agg["avf"]; ae = agg["ave"]
    ae_adj = min((r_tot_adj / agg["p_tc"] * 100.0) if agg["p_tc"] > 0 else 0.0, 999.9)
    ac  = "#991b1b" if ae_adj>af+5 else ("#166534" if ae_adj<=af else "#92400e")

    ots_filas = ""
    for d in ots_data:
        pv = d["p"]["pv"]; rt = d["r"]["tot"]
        mg_ot = ((pv-rt)/pv*100.0) if pv>0 else 0.0
        ac_ot = "#991b1b" if d["ave"]>d["avf"]+5 else "#166534"
        ots_filas += f"""<tr>
          <td><a href="/modulo/economico/ot/{d['ot_id']}" style="font-weight:700;color:#6366f1;text-decoration:none;">OT {d['ot_id']}</a></td>
          <td style="font-size:.78rem;">{_E(d['titulo'])}</td>
          <td><span style="font-size:.7rem;background:#ede9fe;color:#5b21b6;padding:1px 6px;border-radius:999px;">{_E(d['tipo'])}</span></td>
          <td style="text-align:right;">{d['kg']:,.1f}</td>
          <td style="text-align:right;">{d['hh']:,.1f}</td>
          <td style="text-align:right;color:#6366f1;font-weight:600;">{_m(pv)}</td>
          <td style="text-align:right;">{_m(rt)}</td>
          <td style="text-align:right;font-weight:700;color:{_cm(mg_ot)};">{_pct(mg_ot)}</td>
          <td>{_pb(d['avf'],'#3b82f6',7)}</td>
          <td style="color:{ac_ot};font-size:.8rem;">{_pct(d['ave'])}</td>
          <td><a href="/modulo/economico/ot/{d['ot_id']}" style="font-size:.75rem;padding:3px 8px;background:#6366f1;color:#fff;border-radius:5px;text-decoration:none;">Editar</a></td>
        </tr>"""
    ots_filas += f"""<tr style="background:#f1f5f9;font-weight:700;">
      <td colspan="3">TOTAL OBRA</td>
      <td style="text-align:right;">{agg['kg']:,.1f}</td><td style="text-align:right;">{agg['hh']:,.1f}</td>
      <td style="text-align:right;color:#6366f1;">{_m(agg['p_pv'])}</td>
      <td style="text-align:right;">{_m(r_tot_adj)}</td>
      <td style="text-align:right;color:{mc};">{_pct(mg)}</td>
      <td>{_pb(af,'#3b82f6',7)}</td><td style="color:{ac};">{_pct(ae)}</td><td></td>
    </tr>"""

    desv_filas = ""
    for nombre, prev, real in [
        ("Materiales",agg["p_mat"],agg["r_mat"]),("Pintura",agg["p_pint"],agg["r_pint"]),
        ("Mano de Obra",agg["p_mo"],agg["r_mo"]),("Consumibles",agg["p_cons"],agg["r_cons"]),
        ("Ingeniería",agg["p_ing"],agg["r_ing"]),("Subcontratos",0.0,agg["r_sub"]),
        ("Gastos Generales",agg["p_gg"],gg_asig_obra),("Impuestos",agg["p_imp"],agg["r_imp"])]:
        da = real-prev; dp = (da/prev*100.0) if prev!=0 else (0.0 if real==0 else 100.0)
        c = _cd(da); ic = "▲" if da>0 else ("▼" if da<0 else "–")
        desv_filas += f"""<tr>
          <td style="font-weight:600;">{_E(nombre)}</td>
          <td style="text-align:right;">{_m(prev)}</td><td style="text-align:right;">{_m(real)}</td>
          <td style="text-align:right;font-weight:700;color:{c};">{ic} {_m(abs(da))}</td>
          <td style="text-align:right;font-weight:700;color:{c};">{ic} {_pct(abs(dp))}</td>
        </tr>"""

    msg = (f'<div class="ok" style="margin-bottom:12px;">{_E(mensaje)}</div>' if mensaje else "")

    return f"""<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Económico {_E(obra_nombre)}</title>
<style>{_CSS_COMMON}</style></head><body>
<div class="hdr">
  <div><h1>💰 {_E(obra_nombre)} <span style="font-size:.82rem;opacity:.8;">— {_E(cliente)}</span></h1>
    <div style="font-size:.73rem;opacity:.75;margin-top:2px;">{len(ots_data)} OTs · {agg['kg']:,.1f} kg · {agg['hh']:,.1f} HH</div></div>
  <div style="display:flex;gap:6px;flex-wrap:wrap;">
    <a href="/modulo/economico">← Módulo</a><a href="/">Inicio</a></div>
</div>
<div class="body">
  {msg}
  <div class="card"><div class="ct">⚙️ Tasas de esta obra</div><div class="cb">
    <form method="post" style="display:flex;flex-wrap:wrap;gap:14px;align-items:flex-end;">
      <input type="hidden" name="accion" value="config_obra">
      <div style="min-width:130px;"><label>$/HH Mano de Obra</label>
        <input type="number" name="precio_hora_mo" step="0.01" min="0" value="{_fv(cfg['precio_hora_mo'])}"></div>
      <div style="min-width:130px;"><label>$/HH Consumibles</label>
        <input type="number" name="precio_hora_cons" step="0.01" min="0" value="{_fv(cfg['precio_hora_cons'])}"></div>
      <div style="min-width:110px;"><label>% Impuestos</label>
        <input type="number" name="pct_impuestos" step="0.01" min="0" max="100" value="{_fv(cfg['pct_impuestos'])}"></div>
      <button type="submit" class="btn" style="background:#0891b2;color:#fff;">💾 Guardar</button>
    </form>
  </div></div>
  <div class="card"><div class="ct">📊 KPIs consolidados</div><div class="cb" style="display:flex;flex-wrap:wrap;gap:14px;">
    <div style="flex:1;min-width:120px;border-top:3px solid #6366f1;padding-top:8px;">
      <div style="font-size:.66rem;color:#9ca3af;font-weight:700;">PRECIO VENTA PREV.</div>
      <div style="font-size:1.1rem;font-weight:800;color:#6366f1;">{_m(agg['p_pv'])}</div></div>
    <div style="flex:1;min-width:120px;border-top:3px solid #1e293b;padding-top:8px;">
      <div style="font-size:.66rem;color:#9ca3af;font-weight:700;">COSTO REAL TOTAL</div>
      <div style="font-size:1.1rem;font-weight:800;color:#1e293b;">{_m(r_tot_adj)}</div></div>
    <div style="flex:1;min-width:100px;border-top:3px solid {mc};padding-top:8px;">
      <div style="font-size:.66rem;color:#9ca3af;font-weight:700;">MARGEN REAL</div>
      <div style="font-size:1.1rem;font-weight:800;color:{mc};">{_pct(mg)}</div>
      <div style="font-size:.7rem;color:#9ca3af;">{_m(agg['p_pv']-r_tot_adj)}</div></div>
    <div style="flex:1;min-width:100px;border-top:3px solid #10b981;padding-top:8px;">
      <div style="font-size:.66rem;color:#9ca3af;font-weight:700;">$/KG REAL</div>
      <div style="font-size:1.1rem;font-weight:800;color:#10b981;">{_m(r_tot_adj/agg['kg'] if agg['kg']>0 else 0)}</div>
      <div style="font-size:.7rem;color:#9ca3af;">prev: {_m(agg['p_pv']/agg['kg'] if agg['kg']>0 else 0)}</div></div>
    <div style="flex:2;min-width:220px;border-top:3px solid #e5e7eb;padding-top:8px;">
      <div style="font-size:.66rem;color:#9ca3af;font-weight:700;margin-bottom:5px;">AVANCE FÍSICO vs ECONÓMICO</div>
      <div style="display:flex;align-items:center;gap:7px;margin-bottom:4px;">
        <span style="font-size:.7rem;color:#3b82f6;width:72px;">Físico</span>{_pb(af,'#3b82f6',10)}</div>
      <div style="display:flex;align-items:center;gap:7px;">
        <span style="font-size:.7rem;color:{ac};width:72px;">Económico</span>{_pb(ae_adj,ac,10)}</div>
    </div>
  </div></div>
  <div class="card"><div class="ct">📋 Órdenes de Trabajo</div>
    <div style="overflow-x:auto;"><table>
      <thead><tr><th>OT</th><th>Título</th><th>Tipo</th><th>Kg</th><th>HH</th>
        <th>PV Prev.</th><th>Costo Real</th><th>Margen</th><th>Av.Físico</th><th>Av.Econ.</th><th></th></tr></thead>
      <tbody>{ots_filas}</tbody>
    </table></div>
  </div>
  <div class="card"><div class="ct">📉 Desvíos por rubro — Consolidado obra</div>
    <div style="overflow-x:auto;"><table>
      <thead><tr><th>Rubro</th><th style="text-align:right;">Previsto</th><th style="text-align:right;">Real</th>
        <th style="text-align:right;">Desvío $</th><th style="text-align:right;">Desvío %</th></tr></thead>
      <tbody>{desv_filas}
        <tr style="background:#f1f5f9;font-weight:700;">
          <td>TOTAL COSTOS</td>
          <td style="text-align:right;">{_m(agg['p_tc'])}</td>
          <td style="text-align:right;">{_m(r_tot_adj)}</td>
          <td style="text-align:right;color:{_cd(r_tot_adj-agg['p_tc'])};">
            {"▲" if r_tot_adj>agg['p_tc'] else "▼"} {_m(abs(r_tot_adj-agg['p_tc']))}</td>
          <td style="text-align:right;color:{_cd(r_tot_adj-agg['p_tc'])};">
            {_pct(abs((r_tot_adj-agg['p_tc'])/agg['p_tc']*100) if agg['p_tc'] else 0)}</td>
        </tr>
      </tbody>
    </table></div>
  </div>
</div></body></html>"""


# ─────────────────────────────────────────────────────────────────────────────
# RUTA: DETALLE OT
# ─────────────────────────────────────────────────────────────────────────────

@economico_bp.route("/modulo/economico/ot/<int:ot_id>", methods=["GET", "POST"])
def economico_ot(ot_id):
    db = get_db(); _ensure_schema(db)
    ot = db.execute("SELECT id,cliente,obra,titulo,tipo_estructura,estado,COALESCE(es_mantenimiento,0) FROM ordenes_trabajo WHERE id=?", (ot_id,)).fetchone()
    if not ot:
        return "OT no encontrada", 404
    _, cliente, obra, titulo, tipo, estado, es_mantenimiento = ot
    obra = obra or ""
    cfg  = _get_config_obra(db, obra)
    mensaje = error = ""

    if request.method == "POST":
        accion = (request.form.get("accion") or "").strip()
        try:
            if accion == "guardar_presupuesto":
                vals = [float(request.form.get(c) or 0) for c in
                        ["mat_previsto","pintura_previsto","mo_previsto","consumibles_previsto",
                         "ingenieria_previsto","gastos_gen_previsto","impuestos_previsto","beneficio_previsto"]]
                ex = db.execute("SELECT id FROM economico_presupuesto WHERE ot_id=?", (ot_id,)).fetchone()
                if ex:
                    db.execute("""UPDATE economico_presupuesto SET
                        mat_previsto=?,pintura_previsto=?,mo_previsto=?,consumibles_previsto=?,
                        ingenieria_previsto=?,gastos_gen_previsto=?,impuestos_previsto=?,beneficio_previsto=?,
                        updated_at=CURRENT_TIMESTAMP WHERE ot_id=?""", (*vals, ot_id))
                else:
                    db.execute("""INSERT INTO economico_presupuesto
                        (ot_id,mat_previsto,pintura_previsto,mo_previsto,consumibles_previsto,
                         ingenieria_previsto,gastos_gen_previsto,impuestos_previsto,beneficio_previsto)
                        VALUES(?,?,?,?,?,?,?,?,?)""", (ot_id, *vals))
                db.commit(); mensaje = "Presupuesto guardado."
            elif accion == "agregar_costo_mensual":
                mes_c    = (request.form.get("mes") or "").strip()
                concepto = (request.form.get("concepto") or "").strip()
                monto_c  = float(request.form.get("monto") or 0)
                if not mes_c or not concepto or monto_c <= 0:
                    error = "Completá mes, concepto y monto mayor a 0."
                else:
                    db.execute(
                        "INSERT INTO economico_costos_reales_mensual(ot_id,mes,concepto,monto) VALUES(?,?,?,?)",
                        (ot_id, mes_c, concepto, monto_c))
                    db.commit(); mensaje = f"Costo agregado: {concepto} {mes_c}."
            elif accion == "eliminar_costo_mensual":
                costo_id = int(request.form.get("costo_id") or 0)
                db.execute("DELETE FROM economico_costos_reales_mensual WHERE id=? AND ot_id=?", (costo_id, ot_id))
                db.commit(); mensaje = "Entrada eliminada."
            elif accion == "config_obra":
                _save_config_obra(db, obra,
                    float(request.form.get("precio_hora_mo") or 0),
                    float(request.form.get("precio_hora_cons") or 0),
                    float(request.form.get("pct_gastos_gen") or 5),
                    float(request.form.get("pct_impuestos") or 3))
                cfg = _get_config_obra(db, obra); mensaje = "Tasas actualizadas."
        except Exception as exc: error = str(exc)

    data = _calc_economico(db, ot_id, cfg)
    p = data["p"]; rm = data["rm"]; ra = data["ra"]; r = data["r"]
    avf = data["avf"]

    # GG real distribuido por OT (solo para OTs productivas):
    # overhead real total * (CD real de la OT / CD real total de OTs productivas).
    gg_real_ot = 0.0
    if not int(es_mantenimiento or 0):
      prod_rows = db.execute(
        "SELECT id, obra FROM ordenes_trabajo WHERE COALESCE(es_mantenimiento,0)=0"
      ).fetchall()
      total_prod_cd = 0.0
      cfg_cache = {}
      for _id, _obra in prod_rows:
        _obra = str(_obra or "")
        if _obra not in cfg_cache:
          cfg_cache[_obra] = _get_config_obra(db, _obra)
        _d = _calc_economico(db, _id, cfg_cache[_obra])
        total_prod_cd += _d["r"]["cd"]

      mant_rows = db.execute(
        "SELECT id, obra FROM ordenes_trabajo WHERE COALESCE(es_mantenimiento,0)=1"
      ).fetchall()
      total_mant_real = 0.0
      for _id, _obra in mant_rows:
        _obra = str(_obra or "")
        if _obra not in cfg_cache:
          cfg_cache[_obra] = _get_config_obra(db, _obra)
        _d = _calc_economico(db, _id, cfg_cache[_obra])
        total_mant_real += _d["r"]["tot"]

      total_gf_real = float(db.execute("SELECT COALESCE(SUM(monto),0) FROM economico_gastos_fijos").fetchone()[0] or 0.0)
      total_estructura_real = total_mant_real + total_gf_real
      gg_real_ot = (total_estructura_real * (r["cd"] / total_prod_cd)) if total_prod_cd > 0 else 0.0

    r_tot_adj = r["tot"] + gg_real_ot
    ave = min((r_tot_adj / p["tc"] * 100.0) if p["tc"] > 0 else 0.0, 999.9)
    mg = ((p["pv"]-r_tot_adj)/p["pv"]*100.0) if p["pv"]>0 else 0.0
    mc = _cm(mg)
    ac = "#991b1b" if ave>avf+5 else ("#166534" if ave<=avf else "#92400e")

    def _kc(t,v,s="",c="#6366f1"):
        return (f'<div style="background:#fff;border-radius:8px;box-shadow:0 1px 5px rgba(0,0,0,.06);'
                f'padding:12px 14px;flex:1;min-width:120px;border-top:3px solid {c};">'
                f'<div style="font-size:.66rem;color:#9ca3af;font-weight:700;text-transform:uppercase;">{t}</div>'
                f'<div style="font-size:1rem;font-weight:800;color:{c};margin:4px 0 1px;">{v}</div>'
                f'<div style="font-size:.7rem;color:#9ca3af;">{s}</div></div>')

    kpi_html = (
        _kc("$/kg Prev.", _m(p["pv"]/data["kg"] if data["kg"]>0 else 0), f"{data['kg']:,.1f} kg") +
        _kc("$/kg Real",  _m(r_tot_adj/data["kg"] if data["kg"]>0 else 0), "", "#1e293b") +
        _kc("Margen Prev.", _pct(p["ben"]/p["pv"]*100 if p["pv"]>0 else 0), _m(p["ben"]), "#3b82f6") +
        _kc("Margen Real",  _pct(mg), f"PV {_m(p['pv'])}", mc) +
        _kc("Av.Físico", _pct(avf), "estado OT", "#10b981") +
        _kc("Av.Econ.", _pct(ave), "gasto/presup.", ac))

    desv_rows = ""
    for nombre, prev, real in [
        ("Materiales",p["mat"],rm["mat"]),("Pintura",p["pintura"],rm["pintura"]),
        ("Mano de Obra",p["mo"],ra["mo"]),("Consumibles",p["cons"],ra["cons"]),
        ("Ingeniería",p["ing"],rm["ing"]),("Subcontratos",0.0,rm["sub"]),
      ("Gastos Generales",p["gg"],gg_real_ot),("Impuestos",p["imp"],ra["imp"])]:
        da=real-prev; dp=(da/prev*100.0) if prev!=0 else (0.0 if real==0 else 100.0)
        c=_cd(da); ic="▲" if da>0 else ("▼" if da<0 else "–")
        desv_rows += f"""<tr>
          <td style="font-weight:600;">{_E(nombre)}</td>
          <td style="text-align:right;">{_m(prev)}</td><td style="text-align:right;">{_m(real)}</td>
          <td style="text-align:right;font-weight:700;color:{c};">{ic} {_m(abs(da))}</td>
          <td style="text-align:right;font-weight:700;color:{c};">{ic} {_pct(abs(dp))}</td>
        </tr>"""

    msg = (f'<div class="ok" style="margin-bottom:12px;">{_E(mensaje)}</div>' if mensaje else "")
    err = (f'<div class="er" style="margin-bottom:12px;">{_E(error)}</div>' if error else "")

    import datetime as _dt_ec
    _mes_actual = _dt_ec.date.today().strftime("%Y-%m")
    _cr_rows = db.execute(
        "SELECT id,mes,concepto,monto FROM economico_costos_reales_mensual "
        "WHERE ot_id=? ORDER BY mes DESC, concepto", (ot_id,)).fetchall()
    if _cr_rows:
        _hist_filas = "".join(
            f'<tr><td>{r[1]}</td><td>{_E(r[2])}</td>'
            f'<td style="text-align:right;">{_m(r[3])}</td>'
            f'<td><form method="post" style="margin:0;">'
            f'<input type="hidden" name="accion" value="eliminar_costo_mensual">'
            f'<input type="hidden" name="costo_id" value="{r[0]}">'
            f'<button type="submit" style="background:none;border:none;color:#991b1b;cursor:pointer;" '
            f'onclick="return confirm(\'Eliminar esta entrada?\');">&#128465;</button>'
            f'</form></td></tr>'
            for r in _cr_rows
        )
        _historial_html = (
            '<div style="font-size:.76rem;font-weight:700;color:#374151;margin-bottom:6px;'
            'border-bottom:1px solid #e5e7eb;padding-bottom:4px;">Historial de cargas</div>'
            '<div style="overflow-x:auto;"><table style="font-size:.8rem;width:100%;">'
            '<thead><tr><th>Mes</th><th>Concepto</th>'
            '<th style="text-align:right;">Monto</th><th></th></tr></thead>'
            f'<tbody>{_hist_filas}</tbody></table></div>'
        )
    else:
        _historial_html = '<div style="color:#9ca3af;font-size:.8rem;">Sin entradas cargadas aún.</div>'

    return f"""<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>Económico OT {ot_id}</title>
<style>{_CSS_COMMON}</style></head><body>
<div class="hdr">
  <div><h1>💰 OT {ot_id} — {_E(obra)} / {_E(cliente or '')}</h1>
    <div style="font-size:.7rem;opacity:.8;margin-top:2px;">{_E(titulo or '')} · {_E(tipo or 'Sin tipo')}</div></div>
  <div style="display:flex;gap:6px;flex-wrap:wrap;">
    <a href="/modulo/economico/obra/{_E(obra)}">← {_E(obra)}</a>
    <a href="/modulo/economico">Módulo</a></div>
</div>
<div class="body">
  {msg}{err}
  <div class="card"><div class="ct">📊 KPIs · {data['kg']:,.1f} kg · {data['hh']:,.1f} HH</div><div class="cb">
    <div style="display:flex;flex-wrap:wrap;gap:10px;margin-bottom:12px;">{kpi_html}</div>
    <div style="display:flex;align-items:center;gap:7px;margin-bottom:3px;">
      <span style="font-size:.7rem;color:#3b82f6;width:72px;">Físico</span>{_pb(avf,'#3b82f6',10)}</div>
    <div style="display:flex;align-items:center;gap:7px;">
      <span style="font-size:.7rem;color:{ac};width:72px;">Económico</span>{_pb(ave,ac,10)}</div>
  </div></div>
  <div class="card"><div class="ct">⚙️ Tasas de la obra {_E(obra)} <span style="font-size:.72rem;font-weight:400;color:#6b7280;">(aplica a todas las OTs de esta obra)</span></div><div class="cb">
    <form method="post" style="display:flex;flex-wrap:wrap;gap:12px;align-items:flex-end;">
      <input type="hidden" name="accion" value="config_obra">
      <div style="min-width:120px;"><label>$/HH Mano de Obra</label>
        <input type="number" name="precio_hora_mo" step="0.01" min="0" value="{_fv(cfg['precio_hora_mo'])}"></div>
      <div style="min-width:120px;"><label>$/HH Consumibles</label>
        <input type="number" name="precio_hora_cons" step="0.01" min="0" value="{_fv(cfg['precio_hora_cons'])}"></div>
      <div style="min-width:100px;"><label>% Gastos Generales</label>
        <input type="number" name="pct_gastos_gen" step="0.01" min="0" max="100" value="{_fv(cfg['pct_gastos_gen'])}"></div>
      <div style="min-width:100px;"><label>% Impuestos</label>
        <input type="number" name="pct_impuestos" step="0.01" min="0" max="100" value="{_fv(cfg['pct_impuestos'])}"></div>
      <button type="submit" class="btn" style="background:#0891b2;color:#fff;">💾 Guardar tasas</button>
    </form>
  </div></div>
  <div class="two">
    <div class="card"><div class="ct">📋 Costos Previstos</div><div class="cb">
      <form method="post">
        <input type="hidden" name="accion" value="guardar_presupuesto">
        <div class="fg"><label>Materiales ($)</label><input type="number" name="mat_previsto" step="0.01" min="0" value="{_fv(p['mat'])}"></div>
        <div class="fg"><label>Pintura ($)</label><input type="number" name="pintura_previsto" step="0.01" min="0" value="{_fv(p['pintura'])}"></div>
        <div class="fg"><label>Mano de Obra ($)</label><input type="number" name="mo_previsto" step="0.01" min="0" value="{_fv(p['mo'])}"></div>
        <div class="fg"><label>Consumibles ($)</label><input type="number" name="consumibles_previsto" step="0.01" min="0" value="{_fv(p['cons'])}"></div>
        <div class="fg"><label>Ingeniería ($)</label><input type="number" name="ingenieria_previsto" step="0.01" min="0" value="{_fv(p['ing'])}"></div>
        <div class="fg"><label>Gastos Generales ($)</label><input type="number" name="gastos_gen_previsto" step="0.01" min="0" value="{_fv(p['gg'])}"></div>
        <div class="fg"><label>Impuestos ($)</label><input type="number" name="impuestos_previsto" step="0.01" min="0" value="{_fv(p['imp'])}"></div>
        <div class="fg" style="margin-top:8px;"><label>Beneficio ($)</label><input type="number" name="beneficio_previsto" step="0.01" min="0" value="{_fv(p['ben'])}"></div>
        <hr style="border:none;border-top:1px solid #e5e7eb;margin:10px 0;">
        <div style="display:flex;justify-content:space-between;margin-bottom:3px;">
          <span style="font-size:.78rem;color:#6b7280;">Costo directo</span>
          <span style="font-weight:700;color:#6366f1;">{_m(p['cd'])}</span></div>
        <div style="display:flex;justify-content:space-between;">
          <span style="font-size:.78rem;font-weight:700;">Precio de Venta</span>
          <span style="font-weight:800;color:#6366f1;font-size:.98rem;">{_m(p['pv'])}</span></div>
        <button type="submit" class="btn" style="background:#6366f1;color:#fff;width:100%;margin-top:8px;">💾 Guardar presupuesto</button>
      </form>
    </div></div>
    <div class="card"><div class="ct">💸 Costos Reales — Carga Mensual</div><div class="cb">
      <form method="post" style="margin-bottom:16px;">
        <input type="hidden" name="accion" value="agregar_costo_mensual">
        <div style="font-size:.76rem;font-weight:700;color:#374151;margin-bottom:8px;border-bottom:1px solid #e5e7eb;padding-bottom:4px;">Agregar entrada</div>
        <div style="display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end;">
          <div><label style="font-size:.75rem;">Mes</label><br>
            <input type="month" name="mes" required style="padding:5px 8px;border:1px solid #d1d5db;border-radius:6px;font-size:.85rem;"
            value="{_mes_actual}"></div>
          <div><label style="font-size:.75rem;">Concepto</label><br>
            <select name="concepto" required style="padding:5px 8px;border:1px solid #d1d5db;border-radius:6px;font-size:.85rem;">
              <option>Materiales</option><option>Pintura</option>
              <option>Ingeniería</option><option>Subcontratos</option>
            </select></div>
          <div><label style="font-size:.75rem;">Monto ($)</label><br>
            <input type="number" name="monto" step="0.01" min="0.01" required
              style="padding:5px 8px;border:1px solid #d1d5db;border-radius:6px;font-size:.85rem;width:140px;"></div>
          <button type="submit" class="btn" style="background:#0891b2;color:#fff;">➕ Agregar</button>
        </div>
      </form>
      {_historial_html}
      <div style="font-size:.76rem;font-weight:700;color:#374151;margin-bottom:8px;border-bottom:1px solid #e5e7eb;padding-bottom:4px;">Calculados automáticamente <span class="auto">AUTO</span></div>
      <div class="fg"><label>Mano de Obra <span class="auto">{data['hh']:,.1f} HH × ${cfg['precio_hora_mo']:,.2f}</span></label><div class="rv">{_m(ra['mo'])}</div></div>
      <div class="fg"><label>Consumibles <span class="auto">{data['hh']:,.1f} HH × ${cfg['precio_hora_cons']:,.2f}</span></label><div class="rv">{_m(ra['cons'])}</div></div>
      <div class="fg"><label>Gastos Generales <span class="auto">distribución overhead real</span></label><div class="rv">{_m(gg_real_ot)}</div></div>
      <div class="fg"><label>Impuestos <span class="auto">{cfg['pct_impuestos']:.1f}% costo directo</span></label><div class="rv">{_m(ra['imp'])}</div></div>
      <hr style="border:none;border-top:1px solid #e5e7eb;margin:10px 0;">
      <div style="display:flex;justify-content:space-between;"><span style="font-size:.78rem;font-weight:700;">Total Costo Real</span>
        <span style="font-weight:800;">{_m(r_tot_adj)}</span></div>
      <div style="padding:8px;border-radius:6px;background:#fafafe;border:1px solid #e0e7ff;margin-top:6px;">
        <div style="font-size:.7rem;color:#6b7280;">Resultado (PV − Costo Real)</div>
        <div style="font-weight:800;font-size:1rem;color:{mc};">{_m(p['pv']-r_tot_adj)} <span style="font-size:.8rem;">({_pct(mg)})</span></div>
      </div>
    </div></div>
  </div>
  <div class="card"><div class="ct">📉 Desvíos por Rubro</div>
    <div style="overflow-x:auto;"><table>
      <thead><tr><th>Rubro</th><th style="text-align:right;">Previsto</th><th style="text-align:right;">Real</th>
        <th style="text-align:right;">Desvío $</th><th style="text-align:right;">Desvío %</th></tr></thead>
      <tbody>{desv_rows}
        <tr style="background:#f1f5f9;font-weight:700;">
          <td>TOTAL COSTOS</td>
          <td style="text-align:right;">{_m(p['tc'])}</td><td style="text-align:right;">{_m(r_tot_adj)}</td>
          <td style="text-align:right;color:{_cd(r_tot_adj-p['tc'])};">{"▲" if r_tot_adj>p["tc"] else "▼"} {_m(abs(r_tot_adj-p["tc"]))}</td>
          <td style="text-align:right;color:{_cd(r_tot_adj-p['tc'])};">{_pct(abs((r_tot_adj-p["tc"])/p["tc"]*100) if p["tc"] else 0)}</td>
        </tr>
      </tbody>
    </table></div>
  </div>
</div></body></html>"""


# ─────────────────────────────────────────────────────────────────────────────
# RUTA: GASTOS FIJOS DE ESTRUCTURA
# ─────────────────────────────────────────────────────────────────────────────

_GRUPOS_FIJOS = {
    "💼 Sueldos": [
        "Sueldo Coordinador",
        "Sueldo Jefe de Taller",
        "Sueldo Jefe de Calidad",
        "Sueldo Oficina Técnica",
        "Sueldo Administración",
    ],
    "🏢 Servicios y Gastos": [
        "Alquiler",
        "Electricidad",
        "Gas",
        "Agua",
        "Internet",
        "Limpieza",
        "Viandas",
        "Seguro",
        "Telefonía",
        "Mantenimiento edilicio",
    ],
}
_CONCEPTOS_FIJOS = [c for conceptos in _GRUPOS_FIJOS.values() for c in conceptos]
_CONCEPTOS_SUGERIDOS = _CONCEPTOS_FIJOS + ["Otros"]


@economico_bp.route("/modulo/economico/gastos-fijos", methods=["GET", "POST"])
def economico_gastos_fijos_page():
    db = get_db(); _ensure_schema(db)
    from datetime import date as _date
    mensaje = error = ""
    hoy = _date.today()
    mes_sel = request.args.get("mes") or request.form.get("mes_nav") or hoy.strftime("%Y-%m")
    anio_sel = mes_sel[:4]

    if request.method == "POST":
        accion = (request.form.get("accion") or "").strip()
        try:
            if accion == "guardar_mes":
                mes = (request.form.get("mes") or "").strip()
                if mes:
                    for i, concepto in enumerate(_CONCEPTOS_FIJOS):
                        monto = float(request.form.get(f"monto_{i}") or 0)
                        db.execute("DELETE FROM economico_gastos_fijos WHERE mes=? AND concepto=?", (mes, concepto))
                        if monto > 0:
                            db.execute("INSERT INTO economico_gastos_fijos (mes, concepto, monto) VALUES (?,?,?)",
                                       (mes, concepto, monto))
                    db.commit(); mensaje = f"Gastos fijos de {mes} guardados."
                    mes_sel = mes
            elif accion == "agregar_manual":
                mes = (request.form.get("mes") or "").strip()
                concepto = (request.form.get("concepto_manual") or "").strip()
                monto = float(request.form.get("monto_manual") or 0)
                if mes and concepto and monto > 0:
                    db.execute("INSERT INTO economico_gastos_fijos (mes, concepto, monto) VALUES (?,?,?)",
                               (mes, concepto, monto))
                    db.commit(); mensaje = "Gasto adicional agregado."
                    mes_sel = mes
                else:
                    error = "Completá concepto y monto."
            elif accion == "eliminar":
                gasto_id = int(request.form.get("gasto_id") or 0)
                if gasto_id:
                    db.execute("DELETE FROM economico_gastos_fijos WHERE id=?", (gasto_id,))
                    db.commit(); mensaje = "Gasto eliminado."
        except Exception as exc:
            error = str(exc)

    # Cargar valores del mes seleccionado
    filas_mes = db.execute(
        "SELECT id, concepto, monto FROM economico_gastos_fijos WHERE mes=? ORDER BY concepto",
        (mes_sel,)
    ).fetchall()
    vals_mes = {r[1]: (r[2], r[0]) for r in filas_mes}  # concepto → (monto, id)
    manuales_mes = [(r[0], r[1], r[2]) for r in filas_mes if r[1] not in _CONCEPTOS_FIJOS]

    # Totales por mes (todo el año)
    filas_anio = db.execute(
        "SELECT mes, SUM(monto) FROM economico_gastos_fijos WHERE mes LIKE ? GROUP BY mes ORDER BY mes",
        (f"{anio_sel}-%",)
    ).fetchall()
    por_mes = {r[0]: float(r[1] or 0) for r in filas_anio}
    total_anio  = sum(por_mes.values())
    prom_mes    = total_anio / len(por_mes) if por_mes else 0.0
    total_mes_sel = sum(v for v, _ in vals_mes.values())

    # Años disponibles para navegación
    anios_db = db.execute("SELECT DISTINCT substr(mes,1,4) FROM economico_gastos_fijos ORDER BY 1 DESC").fetchall()
    anios = sorted({r[0] for r in anios_db} | {str(hoy.year)}, reverse=True)

    # Generar campos fijos por grupo
    grupos_html = ""
    for grupo, conceptos in _GRUPOS_FIJOS.items():
        campos = ""
        for concepto in conceptos:
            idx = _CONCEPTOS_FIJOS.index(concepto)
            val = vals_mes.get(concepto, (0, None))[0]
            val_fmt = f"{val:.2f}" if val else ""
            campos += f"""<div class="fg">
              <label>{_E(concepto)}</label>
              <input type="number" name="monto_{idx}" step="0.01" min="0"
                     value="{val_fmt}" placeholder="0.00" class="monto-input">
            </div>"""
        grupos_html += f"""<div class="card" style="flex:1;min-width:220px;">
          <div class="ct" style="font-size:.82rem;">{grupo}</div>
          <div class="cb">{campos}</div>
        </div>"""

    # Tabla historial año
    hist_html = ""
    for mes_k in sorted(por_mes.keys()):
        activo = "background:#fef3c7;" if mes_k == mes_sel else ""
        hist_html += f"""<tr style="{activo}">
          <td><a href="?mes={mes_k}" style="font-weight:700;color:#92400e;text-decoration:none;">{mes_k}</a></td>
          <td style="text-align:right;">{_m(por_mes[mes_k])}</td>
          <td><a href="?mes={mes_k}" style="font-size:.75rem;padding:2px 8px;background:#fef3c7;color:#92400e;border-radius:4px;text-decoration:none;">Editar</a></td>
        </tr>"""

    # Manuales del mes
    manuales_html = ""
    for gid, concepto, monto in manuales_mes:
        manuales_html += f"""<tr>
          <td style="color:#374151;">{_E(concepto)}</td>
          <td style="text-align:right;">{_m(float(monto or 0))}</td>
          <td style="text-align:center;">
            <form method="post" style="display:inline;" onsubmit="return confirm('¿Eliminar?')">
              <input type="hidden" name="accion" value="eliminar">
              <input type="hidden" name="gasto_id" value="{gid}">
              <input type="hidden" name="mes_nav" value="{mes_sel}">
              <button type="submit" style="background:none;border:none;cursor:pointer;color:#dc2626;">🗑️</button>
            </form>
          </td>
        </tr>"""

    anio_tabs = "".join(
        f'<a href="?mes={a}-01" style="padding:4px 10px;border-radius:5px;text-decoration:none;font-size:.8rem;font-weight:600;'
        f'{"background:#f59e0b;color:#fff;" if a==anio_sel else "background:#fef3c7;color:#92400e;"}">{a}</a>'
        for a in anios)

    msg     = f'<div style="background:#d1fae5;border-left:4px solid #10b981;padding:8px 14px;border-radius:4px;margin-bottom:12px;font-weight:700;">{_E(mensaje)}</div>' if mensaje else ""
    err_html= f'<div style="background:#fee2e2;border-left:4px solid #ef4444;padding:8px 14px;border-radius:4px;margin-bottom:12px;">{_E(error)}</div>' if error else ""

    return f"""<!DOCTYPE html><html lang="es"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Gastos Fijos — {mes_sel}</title>
<style>{_CSS_COMMON}
  .two {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; }}
  @media(max-width:700px){{.two{{grid-template-columns:1fr;}}}}
  .fg {{ margin-bottom:10px; }}
  .fg label {{ font-size:.74rem; font-weight:700; color:#374151; display:block; margin-bottom:3px; }}
  .fg input {{ width:100%; padding:6px 9px; border:1px solid #d1d5db; border-radius:5px; font-size:.88rem; }}
  .fg input:focus {{ border-color:#f59e0b; outline:none; box-shadow:0 0 0 2px #fef3c7; }}
</style></head><body>
<div class="hdr" style="background:linear-gradient(135deg,#78350f,#92400e);">
  <div>
    <h1>🏭 Gastos Fijos de Estructura</h1>
    <div style="font-size:.74rem;opacity:.75;margin-top:2px;">Sueldos · Alquiler · Servicios · y más</div>
  </div>
  <div style="display:flex;gap:7px;flex-wrap:wrap;">
    <a href="/modulo/economico/dashboard-ejecutivo">📊 Dashboard</a>
    <a href="/modulo/economico">← Módulo</a>
  </div>
</div>
<div class="body">
  {msg}{err_html}

  <!-- KPIs año -->
  <div style="display:flex;flex-wrap:wrap;gap:12px;">
    <div style="background:#fff;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,.07);padding:14px 18px;flex:1;min-width:120px;border-left:4px solid #f59e0b;">
      <div style="font-size:.68rem;color:#9ca3af;font-weight:700;text-transform:uppercase;">Total {anio_sel}</div>
      <div style="font-size:1.3rem;font-weight:900;color:#92400e;">{_m(total_anio)}</div>
    </div>
    <div style="background:#fff;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,.07);padding:14px 18px;flex:1;min-width:120px;border-left:4px solid #6366f1;">
      <div style="font-size:.68rem;color:#9ca3af;font-weight:700;text-transform:uppercase;">Promedio mensual</div>
      <div style="font-size:1.3rem;font-weight:900;color:#6366f1;">{_m(prom_mes)}</div>
    </div>
    <div style="background:#fff;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,.07);padding:14px 18px;flex:1;min-width:120px;border-left:4px solid #1e293b;">
      <div style="font-size:.68rem;color:#9ca3af;font-weight:700;text-transform:uppercase;">Mes actual — {mes_sel}</div>
      <div style="font-size:1.3rem;font-weight:900;color:#1e293b;">{_m(total_mes_sel)}</div>
    </div>
    <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap;">{anio_tabs}</div>
  </div>

  <!-- Selector de mes + formulario principal -->
  <form method="post" id="frmMes">
    <input type="hidden" name="accion" value="guardar_mes">
    <input type="hidden" name="mes" value="{mes_sel}">

    <!-- Navegación de mes -->
    <div class="card">
      <div class="ct" style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;">
        <span>📅 Cargando mes: <b>{mes_sel}</b></span>
        <div style="display:flex;gap:6px;align-items:center;">
          <input type="month" id="irMes" value="{mes_sel}"
                 style="padding:5px 9px;border:1px solid #d1d5db;border-radius:5px;font-size:.85rem;"
                 onchange="window.location='?mes='+this.value">
        </div>
      </div>
    </div>

    <!-- Grupos de campos -->
    <div style="display:flex;flex-wrap:wrap;gap:16px;">
      {grupos_html}
    </div>

    <!-- Total y guardar -->
    <div class="card">
      <div class="cb" style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px;">
        <div>
          <div style="font-size:.72rem;color:#6b7280;font-weight:700;text-transform:uppercase;">Total mes {mes_sel}</div>
          <div style="font-size:1.4rem;font-weight:900;color:#92400e;" id="totalMes">{_m(total_mes_sel)}</div>
        </div>
        <button type="submit" class="btn" style="background:#f59e0b;color:#fff;font-size:.95rem;padding:10px 24px;">
          💾 Guardar mes {mes_sel}
        </button>
      </div>
    </div>
  </form>

  <!-- Otros gastos adicionales -->
  <div class="card">
    <div class="ct">➕ Otros gastos adicionales — {mes_sel}
      <span style="font-size:.72rem;font-weight:400;color:#6b7280;margin-left:8px;">Gastos que no están en la lista fija</span>
    </div>
    <div class="cb">
      <form method="post" style="display:flex;flex-wrap:wrap;gap:10px;align-items:flex-end;margin-bottom:14px;">
        <input type="hidden" name="accion" value="agregar_manual">
        <input type="hidden" name="mes" value="{mes_sel}">
        <div class="fg" style="min-width:200px;flex:1;">
          <label>Concepto</label>
          <input type="text" name="concepto_manual" placeholder="Ej: Reparación calderas…" required>
        </div>
        <div class="fg" style="min-width:130px;">
          <label>Monto ($)</label>
          <input type="number" name="monto_manual" step="0.01" min="0.01" placeholder="0.00" required>
        </div>
        <button type="submit" class="btn" style="background:#6366f1;color:#fff;">➕ Agregar</button>
      </form>
      {"<table style='font-size:.85rem;'><thead><tr><th>Concepto</th><th style='text-align:right;'>Monto</th><th></th></tr></thead><tbody>" + manuales_html + "</tbody></table>" if manuales_mes else '<p style="color:#9ca3af;font-size:.82rem;">Sin gastos adicionales este mes.</p>'}
    </div>
  </div>

  <!-- Historial del año -->
  <div class="card">
    <div class="ct">📋 Historial — {anio_sel}</div>
    <div style="overflow-x:auto;">
      <table>
        <thead><tr><th>Mes</th><th style="text-align:right;">Total</th><th></th></tr></thead>
        <tbody>{hist_html if hist_html else '<tr><td colspan="3" style="text-align:center;color:#9ca3af;padding:20px;">Sin datos.</td></tr>'}</tbody>
      </table>
    </div>
  </div>
</div>

<script>
// Recalcular total en tiempo real
document.querySelectorAll('.monto-input').forEach(inp => {{
  inp.addEventListener('input', () => {{
    let tot = 0;
    document.querySelectorAll('.monto-input').forEach(i => {{ tot += parseFloat(i.value || 0); }});
    const el = document.getElementById('totalMes');
    if (el) el.textContent = '$ ' + tot.toLocaleString('es-AR', {{minimumFractionDigits:0, maximumFractionDigits:0}});
  }});
}});
</script>
</body></html>"""


    db = get_db(); _ensure_schema(db)
    from datetime import date as _date
    mensaje = error = ""
    anio_sel = request.args.get("anio") or str(_date.today().year)

    if request.method == "POST":
        accion = (request.form.get("accion") or "").strip()
        try:
            if accion == "agregar":
                mes = (request.form.get("mes") or "").strip()
                concepto = (request.form.get("concepto") or "").strip()
                monto = float(request.form.get("monto") or 0)
                if mes and concepto and monto > 0:
                    db.execute(
                        "INSERT INTO economico_gastos_fijos (mes, concepto, monto) VALUES (?,?,?)",
                        (mes, concepto, monto))
                    db.commit(); mensaje = "Gasto agregado."
                else:
                    error = "Completá mes, concepto y monto."
            elif accion == "eliminar":
                gasto_id = int(request.form.get("gasto_id") or 0)
                if gasto_id:
                    db.execute("DELETE FROM economico_gastos_fijos WHERE id=?", (gasto_id,))
                    db.commit(); mensaje = "Gasto eliminado."
        except Exception as exc:
            error = str(exc)

    filas = db.execute(
        "SELECT id, mes, concepto, monto FROM economico_gastos_fijos WHERE mes LIKE ? ORDER BY mes, concepto",
        (f"{anio_sel}-%",)
    ).fetchall()

    # Totales por mes
    from collections import defaultdict as _dd
    por_mes = _dd(float)
    for _, mes, _, monto in filas:
        por_mes[mes] += float(monto or 0)

    # Años disponibles
    anios_db = db.execute(
        "SELECT DISTINCT substr(mes,1,4) FROM economico_gastos_fijos ORDER BY 1 DESC"
    ).fetchall()
    anios = list({r[0] for r in anios_db} | {str(_date.today().year)})
    anios.sort(reverse=True)

    filas_html = ""
    mes_actual = None
    for gid, mes, concepto, monto in filas:
        if mes != mes_actual:
            mes_actual = mes
            total_mes = por_mes[mes]
            filas_html += f"""<tr style="background:#f1f5f9;">
              <td colspan="2" style="font-weight:700;color:#1e293b;">{mes}</td>
              <td style="text-align:right;font-weight:700;color:#6366f1;">{_m(total_mes)}</td>
              <td></td>
            </tr>"""
        filas_html += f"""<tr>
          <td style="padding-left:20px;color:#374151;">{_E(concepto)}</td>
          <td></td>
          <td style="text-align:right;">{_m(float(monto or 0))}</td>
          <td style="text-align:center;">
            <form method="post" style="display:inline;" onsubmit="return confirm('¿Eliminar este gasto?')">
              <input type="hidden" name="accion" value="eliminar">
              <input type="hidden" name="gasto_id" value="{gid}">
              <button type="submit" style="background:none;border:none;cursor:pointer;color:#dc2626;font-size:1rem;">🗑️</button>
            </form>
          </td>
        </tr>"""

    total_anio = sum(por_mes.values())
    prom_mensual = total_anio / len(por_mes) if por_mes else 0.0

    anio_tabs = "".join(
        f'<a href="?anio={a}" style="padding:5px 12px;border-radius:5px;text-decoration:none;font-size:.82rem;font-weight:600;'
        f'{"background:#f59e0b;color:#fff;" if a==anio_sel else "background:#fef3c7;color:#92400e;"}">{a}</a>'
        for a in anios)




# ─────────────────────────────────────────────────────────────────────────────
# RUTA: DASHBOARD EJECUTIVO DE RENTABILIDAD
# ─────────────────────────────────────────────────────────────────────────────

def _semaforo(mg, ae, af):
    """Retorna (emoji, color_fondo, color_texto, etiqueta, detalle_riesgo)."""
    en_desvio = ae > af + 5
    en_desvio_critico = ae > af + 15
    if mg < 5 or en_desvio_critico:
        return "🔴", "#fef2f2", "#991b1b", "Crítico",    "Costo superior al previsto" if mg < 5 else "Desvío de avance crítico"
    if mg < 10 or en_desvio:
        return "🟠", "#fff7ed", "#92400e", "Atención",   "Mayor consumo de mano de obra" if en_desvio else "Margen ajustado"
    return "🟢", "#f0fdf4", "#166534", "En control", "Dentro del presupuesto"


@economico_bp.route("/modulo/economico/dashboard-ejecutivo")
def economico_dashboard_ejecutivo():
    db = get_db(); _ensure_schema(db)

    ots_rows = db.execute(
        "SELECT id, cliente, obra, tipo_estructura, COALESCE(es_mantenimiento,0) FROM ordenes_trabajo ORDER BY obra, id"
    ).fetchall()

    obras_dict = {}   # obras productivas
    mant_dict  = {}   # obras de mantenimiento / overhead
    for ot_id, cliente, obra, tipo, es_mant in ots_rows:
        k = str(obra or "Sin obra").strip()
        dest = mant_dict if es_mant else obras_dict
        if k not in dest:
            dest[k] = {"cliente": cliente or "", "ots": []}
        dest[k]["ots"].append({"id": ot_id, "tipo": tipo})

    # ── Acumular datos por obra ───────────────────────────────────────────────
    obras_data = []   # lista de dicts por obra
    rubros_global = {r: {"prev": 0.0, "real": 0.0} for r in
                     ["Materiales","Pintura","Mano de Obra","Consumibles",
                      "Ingeniería","Subcontratos","Gastos Generales","Impuestos"]}

    for obra_key in sorted(obras_dict.keys()):
        info = obras_dict[obra_key]
        cfg  = _get_config_obra(db, obra_key)
        ots_d = []
        for oi in info["ots"]:
            d = _calc_economico(db, oi["id"], cfg)
            d["ot_id"] = oi["id"]
            ots_d.append(d)
        agg = _aggregate_obra(ots_d)
        mg  = ((agg["p_pv"] - agg["r_tot"]) / agg["p_pv"] * 100.0) if agg["p_pv"] > 0 else 0.0
        af  = agg["avf"]; ae = agg["ave"]
        # Margen proyectado a la finalización (extrapolando costo actual)
        if af > 0:
            costo_proy = agg["r_tot"] / (af / 100.0)
            mg_proy = ((agg["p_pv"] - costo_proy) / agg["p_pv"] * 100.0) if agg["p_pv"] > 0 else 0.0
        else:
            mg_proy = mg
        sem_em, sem_bg, sem_tc, sem_lbl, sem_det = _semaforo(mg, ae, af)

        obras_data.append({
            "obra": obra_key, "cliente": info["cliente"],
            "n_ots": len(ots_d), "kg": agg["kg"], "hh": agg["hh"],
            "pv": agg["p_pv"], "p_tc": agg["p_tc"], "r_tot": agg["r_tot"], "r_cd": agg["r_cd"],
            "mg": mg, "mg_proy": mg_proy, "af": af, "ae": ae,
            "sem_em": sem_em, "sem_bg": sem_bg, "sem_tc": sem_tc,
            "sem_lbl": sem_lbl, "sem_det": sem_det,
            "agg": agg,
        })

        # Acumular rubros globales
        for nm, prev, real in [
            ("Materiales",      agg["p_mat"],  agg["r_mat"]),
            ("Pintura",         agg["p_pint"], agg["r_pint"]),
            ("Mano de Obra",    agg["p_mo"],   agg["r_mo"]),
            ("Consumibles",     agg["p_cons"], agg["r_cons"]),
            ("Ingeniería",      agg["p_ing"],  agg["r_ing"]),
            ("Subcontratos",    0.0,           agg["r_sub"]),
            ("Impuestos",       agg["p_imp"],  agg["r_imp"]),
        ]:
            rubros_global[nm]["prev"] += prev
            rubros_global[nm]["real"] += real

    n_obras = len(obras_data)
    if n_obras == 0:
        return "<p>Sin datos de obras.</p><a href='/modulo/economico'>← Volver</a>"

    # ── Mantenimiento / Overhead ──────────────────────────────────────────────
    mant_data = []
    for obra_key in sorted(mant_dict.keys()):
        info = mant_dict[obra_key]
        cfg  = _get_config_obra(db, obra_key)
        ots_d = []
        for oi in info["ots"]:
            d = _calc_economico(db, oi["id"], cfg)
            d["ot_id"] = oi["id"]
            ots_d.append(d)
        agg = _aggregate_obra(ots_d)
        mant_data.append({
            "obra": obra_key, "cliente": info["cliente"],
            "n_ots": len(ots_d), "hh": agg["hh"],
            "p_tc": agg["p_tc"], "r_tot": agg["r_tot"], "agg": agg,
        })

    total_mant_prev = sum(d["p_tc"]  for d in mant_data)
    total_mant_real = sum(d["r_tot"] for d in mant_data)
    total_prod_real = sum(o["r_tot"] for o in obras_data)
    pct_overhead    = (total_mant_real / total_prod_real * 100.0) if total_prod_real > 0 else 0.0
    # Gastos Generales de los proyectos productivos (deberían cubrir el overhead total)
    total_gg_prev   = sum(o["agg"]["p_gg"] for o in obras_data)
    total_gg_real   = 0.0  # GG ya no se calcula por %; se usa el overhead distribuido
    # Nota: saldo_real se recalcula después de obtener total_gf_real
    # Por ahora inicializar; se actualizará con total_estructura_real en el panel HTML

    # Chart mensual de mantenimiento — HH × precio por mes + gastos fijos
    from db_utils import DB_ENGINE as _DB_ENGINE3
    from collections import defaultdict as _dd3
    import json as _json3
    _mysql3 = (_DB_ENGINE3 == "mysql")
    fmt_mes3 = "DATE_FORMAT(fecha, '%Y-%m')" if _mysql3 else "strftime('%Y-%m', fecha)"
    mant_ot_ids = [oi["id"] for info in mant_dict.values() for oi in info["ots"]]
    mant_mes_costs = _dd3(float)
    if mant_ot_ids:
        _ph = ",".join("?" * len(mant_ot_ids))
        _mant_hh = db.execute(
            f"""SELECT ot_id, {fmt_mes3} AS mes, SUM(horas) AS hh
                FROM partes_trabajo
                WHERE ot_id IN ({_ph}) AND fecha IS NOT NULL AND fecha != ''
                GROUP BY ot_id, mes ORDER BY mes""",
            mant_ot_ids
        ).fetchall()
        _mant_cfg_cache = {}
        for _ot_id, _mes, _hh in _mant_hh:
            if _ot_id not in _mant_cfg_cache:
                _r = db.execute("SELECT obra FROM ordenes_trabajo WHERE id=?", (_ot_id,)).fetchone()
                _mant_cfg_cache[_ot_id] = _get_config_obra(db, (_r[0] or "") if _r else "")
            _cfg3 = _mant_cfg_cache[_ot_id]
            mant_mes_costs[_mes] += float(_hh or 0) * (_cfg3["precio_hora_mo"] + _cfg3["precio_hora_cons"])

    # Gastos fijos por mes
    gf_rows = db.execute(
        "SELECT mes, SUM(monto) FROM economico_gastos_fijos GROUP BY mes ORDER BY mes"
    ).fetchall()
    gf_mes_costs = {str(r[0]): float(r[1] or 0) for r in gf_rows}
    total_gf_real = sum(gf_mes_costs.values())

    # Ahora calculamos saldo con total de estructura (mantenimiento + gastos fijos)
    total_estructura_real = total_mant_real + total_gf_real
    saldo_prev   = total_gg_prev - total_estructura_real
    pct_cob_prev = min((total_gg_prev / total_estructura_real * 100.0) if total_estructura_real > 0 else 100.0, 200.0)
    # Ranking de desvíos: GG reales vs previstos (real = overhead total de estructura)
    rubros_global["Gastos Generales"]["prev"] = total_gg_prev
    rubros_global["Gastos Generales"]["real"] = total_estructura_real

    # Series para el chart — unión de todos los meses con datos
    _all_meses = sorted(set(mant_mes_costs.keys()) | set(gf_mes_costs.keys()))
    mant_mes_js   = _json3.dumps(_all_meses)
    mant_costs_js = _json3.dumps([round(mant_mes_costs.get(m, 0), 0) for m in _all_meses])
    gf_costs_js   = _json3.dumps([round(gf_mes_costs.get(m, 0), 0) for m in _all_meses])

    # ── KPIs globales ─────────────────────────────────────────────────────────
    n_riesgo    = sum(1 for o in obras_data if o["sem_lbl"] in ("Crítico","Atención"))
    n_criticos  = sum(1 for o in obras_data if o["sem_lbl"] == "Crítico")
    costo_cd      = sum(o["r_cd"]          for o in obras_data)
    costo_cd_prev = sum(o["agg"]["p_cd"]   for o in obras_data)
    ae_prom       = sum(o["ae"]             for o in obras_data) / n_obras
    # Margen proyectado del portfolio: ponderado por valor de obra (mismo criterio
    # que la fila TOTAL). Evita que obras sin avance (af=0, costo=0) inflen el promedio
    # con un 100% ficticio, y que obras pequeñas pesen igual que grandes.
    _pv_total   = sum(o["pv"] for o in obras_data)
    _cproy_total = sum(
        o["r_tot"] / (o["af"] / 100.0) if o["af"] > 0 else o["r_tot"]
        for o in obras_data
    )
    mg_prom = ((_pv_total - _cproy_total) / _pv_total * 100.0) if _pv_total > 0 else 0.0
    # Riesgo global — basado en margen promedio proyectado del portfolio
    # El margen promedio refleja la compensación entre obras buenas y malas.
    # pct_riesgo actúa solo como agravante secundario en casos extremos.
    pct_riesgo  = n_riesgo / n_obras
    if mg_prom < 5:
        riesgo_em, riesgo_lbl, riesgo_c = "🔴", "Alto", "#991b1b"
    elif mg_prom < 15:
        if pct_riesgo >= 0.5:
            riesgo_em, riesgo_lbl, riesgo_c = "🔴", "Alto", "#991b1b"
        else:
            riesgo_em, riesgo_lbl, riesgo_c = "🟠", "Medio", "#92400e"
    else:
        if pct_riesgo >= 0.5:
            riesgo_em, riesgo_lbl, riesgo_c = "🟠", "Medio", "#92400e"
        else:
            riesgo_em, riesgo_lbl, riesgo_c = "🟢", "Bajo", "#166534"

    # ── Ranking de desvíos ────────────────────────────────────────────────────
    # Rubros que siempre aparecen aunque prev=0 y real=0
    RUBROS_SIEMPRE = {"Subcontratos", "Materiales", "Pintura", "Mano de Obra", "Consumibles"}
    ranking = []
    for nm, vals in rubros_global.items():
        prev = vals["prev"]; real = vals["real"]
        if prev == 0 and real == 0 and nm not in RUBROS_SIEMPRE:
            continue
        desv_pct = ((real - prev) / prev * 100.0) if prev > 0 else (100.0 if real > 0 else 0.0)
        ranking.append({"nombre": nm, "prev": prev, "real": real, "desv_pct": desv_pct})
    ranking.sort(key=lambda x: (-abs(x["desv_pct"]), x["nombre"]))

    # ── Resumen ejecutivo ─────────────────────────────────────────────────────
    n_ok       = sum(1 for o in obras_data if o["sem_lbl"] == "En control")
    n_atencion = sum(1 for o in obras_data if o["sem_lbl"] == "Atención")
    obras_mo   = [o["obra"] for o in obras_data if o["sem_det"] and "mano de obra" in o["sem_det"].lower()]

    resumen_items = [
        f"{n_obras} obra{'s' if n_obras != 1 else ''} activa{'s' if n_obras != 1 else ''}.",
        f"{n_ok} obra{'s' if n_ok != 1 else ''} dentro del presupuesto.",
    ]
    if n_atencion:
        resumen_items.append(f"{n_atencion} obra{'s' if n_atencion!=1 else ''} con margen ajustado o desvío de avance.")
    if obras_mo:
        resumen_items.append(f"{', '.join(obras_mo)} presenta{'n' if len(obras_mo)>1 else ''} consumo de mano de obra superior al previsto.")
    resumen_items.append(f"El margen promedio proyectado es del {mg_prom:.1f} %.")
    if n_criticos == 0:
        resumen_items.append("No se detectan riesgos críticos de rentabilidad.")
    else:
        resumen_items.append(f"⚠️ {n_criticos} obra{'s requieren' if n_criticos>1 else ' requiere'} atención inmediata.")

    resumen_html = "\n".join(f"<li>{item}</li>" for item in resumen_items)

    # ── Distribución de gastos de estructura por costo directo ────────────────
    # Cada proyecto absorbe overhead en proporción a su CD real.
    # Costo ajustado = r_cd + GG_dist + r_imp.
    _sum_r_cd = sum(o["r_cd"] for o in obras_data)
    dist_rows_html = ""
    total_gg_dist_check = 0.0
    gg_dist_map = {}
    costo_real_aj_map = {}
    costo_proy_aj_map = {}
    mg_proy_aj_map = {}
    for o in obras_data:
        _pct_cd = (o["r_cd"] / _sum_r_cd * 100.0) if _sum_r_cd > 0 else 0.0
        _gg_dist = total_estructura_real * (o["r_cd"] / _sum_r_cd) if _sum_r_cd > 0 else 0.0
        total_gg_dist_check += _gg_dist
        _imp = o["agg"]["r_imp"]
        _costo_aj = o["r_cd"] + _gg_dist + _imp
        _costo_proy_aj = (_costo_aj / (o["af"] / 100.0)) if o["af"] > 0 else _costo_aj
        _mg_aj = ((o["pv"] - _costo_proy_aj) / o["pv"] * 100.0) if o["pv"] > 0 else 0.0
        _mc_aj = _cm(_mg_aj)
        _delta = _mg_aj - o["mg_proy"]
        _delta_c = "#166534" if _delta >= 0 else "#991b1b"
        _delta_s = f'{"▲" if _delta>=0 else "▼"} {abs(_delta):.1f}pp'

        gg_dist_map[o["obra"]] = _gg_dist
        costo_real_aj_map[o["obra"]] = _costo_aj
        costo_proy_aj_map[o["obra"]] = _costo_proy_aj
        mg_proy_aj_map[o["obra"]] = _mg_aj
        o["gg_dist"] = _gg_dist
        o["r_tot_aj"] = _costo_aj
        o["costo_proy_aj"] = _costo_proy_aj
        o["mg_proy_aj"] = _mg_aj

        dist_rows_html += f"""<tr>
          <td style="font-weight:700;color:#6366f1;">{_E(o['obra'])}</td>
          <td style="text-align:right;">{_m(o['r_cd'])}</td>
          <td style="text-align:right;color:#6b7280;">{_pct_cd:.1f}%</td>
          <td style="text-align:right;font-weight:700;color:#f59e0b;">{_m(_gg_dist)}</td>
          <td style="text-align:right;">{_m(_costo_aj)}</td>
          <td style="text-align:right;">{_m(o['pv'])}</td>
          <td style="text-align:right;font-weight:700;color:{_mc_aj};">{_mg_aj:.1f}%</td>
          <td style="text-align:right;font-size:.75rem;color:{_delta_c};">{_delta_s}</td>
        </tr>"""

    _total_costo_proy_aj = sum(costo_proy_aj_map.values()) if costo_proy_aj_map else 0.0
    _mg_total_aj = ((_pv_total - _total_costo_proy_aj) / _pv_total * 100.0) if _pv_total > 0 else 0.0
    mg_prom = _mg_total_aj

    # ── HTML ──────────────────────────────────────────────────────────────────
    # Tabla de obras
    tabla_obras = ""
    for o in obras_data:
        af_bar = f'<div style="background:#e5e7eb;border-radius:3px;height:8px;width:100%;min-width:50px;"><div style="background:#3b82f6;border-radius:3px;height:8px;width:{min(o["af"],100):.1f}%;"></div></div><span style="font-size:.72rem;color:#6b7280;">{o["af"]:.1f}%</span>'
        ae_c   = "#991b1b" if o["ae"] > o["af"]+5 else ("#166534" if o["ae"] <= o["af"] else "#92400e")
        ae_bar = f'<div style="background:#e5e7eb;border-radius:3px;height:8px;width:100%;min-width:50px;"><div style="background:{ae_c};border-radius:3px;height:8px;width:{min(o["ae"],100):.1f}%;"></div></div><span style="font-size:.72rem;color:{ae_c};">{o["ae"]:.1f}%</span>'
        mg_aj  = o.get("mg_proy_aj", o["mg_proy"])
        mc     = _cm(mg_aj)
        tabla_obras += f"""<tr>
          <td style="font-weight:700;"><a href="/modulo/economico/obra/{_E(o['obra'])}" style="color:#6366f1;text-decoration:none;">{_E(o['obra'])}</a></td>
          <td style="font-size:.75rem;color:#6b7280;">{_E(o['cliente'])}</td>
          <td>{af_bar}</td>
          <td>{ae_bar}</td>
          <td style="text-align:right;font-weight:700;color:{mc};">{mg_aj:.1f}%</td>
          <td style="text-align:right;color:#6b7280;">{_m(o['pv'])}</td>
          <td style="text-align:right;color:#6b7280;">{_m(o.get('r_tot_aj', o['r_tot']))}</td>
          <td style="text-align:center;font-size:1.3rem;">{o['sem_em']}</td>
        </tr>"""
    # Fila de totales — reutiliza _pv_total y _cproy_total calculados en KPIs globales
    total_pv   = _pv_total
    total_real = sum(o.get('r_tot_aj', o['r_tot']) for o in obras_data)
    total_mg   = mg_prom  # idéntico cálculo ponderado
    mc_tot     = _cm(total_mg)
    tabla_obras += f"""<tr style="background:#f1f5f9;font-weight:700;border-top:2px solid #cbd5e1;">
      <td colspan="2" style="font-size:.82rem;color:#374151;">TOTAL ({n_obras} obras)</td>
      <td></td><td></td>
      <td style="text-align:right;color:{mc_tot};">{total_mg:.1f}%</td>
      <td style="text-align:right;">{_m(total_pv)}</td>
      <td style="text-align:right;">{_m(total_real)}</td>
      <td></td>
    </tr>"""

    # Semáforos
    semaforos_html = ""
    for o in obras_data:
        semaforos_html += f"""
        <div style="background:{o['sem_bg']};border:1px solid;border-color:{o['sem_tc']}44;border-radius:10px;padding:12px 16px;display:flex;align-items:center;gap:12px;">
          <span style="font-size:1.8rem;line-height:1;">{o['sem_em']}</span>
          <div>
            <div style="font-weight:700;color:{o['sem_tc']};font-size:.95rem;">{_E(o['obra'])}</div>
            <div style="font-size:.78rem;color:{o['sem_tc']};opacity:.85;">{_E(o['sem_det'])}</div>
          </div>
          <div style="margin-left:auto;text-align:right;">
            <div style="font-size:.7rem;color:#6b7280;">Margen proy.</div>
            <div style="font-weight:800;color:{o['sem_tc']};font-size:.95rem;">{o['mg_proy']:.1f}%</div>
          </div>
        </div>"""

    # Ranking desvíos
    ranking_html = ""
    for r in ranking[:8]:
        pct = r["desv_pct"]
        c   = "#991b1b" if pct > 0 else "#166534"
        ic  = "▲" if pct > 0 else "▼"
        bar_w = min(abs(pct), 50)
        bar_c = "#fca5a5" if pct > 0 else "#86efac"
        ranking_html += f"""<tr>
          <td style="font-weight:600;font-size:.82rem;">{_E(r['nombre'])}</td>
          <td style="text-align:right;font-size:.78rem;color:#6b7280;">{_m(r['prev'])}</td>
          <td style="text-align:right;font-size:.78rem;">{_m(r['real'])}</td>
          <td>
            <div style="display:flex;align-items:center;gap:5px;">
              <div style="background:#f1f5f9;border-radius:3px;height:8px;flex:1;max-width:60px;">
                <div style="background:{bar_c};border-radius:3px;height:8px;width:{bar_w*2:.0f}%;"></div>
              </div>
              <span style="font-weight:700;color:{c};font-size:.8rem;white-space:nowrap;">{ic} {abs(pct):.1f}%</span>
            </div>
          </td>
        </tr>"""

    # Datos para Chart.js — Avance
    chart_labels = [o["obra"] for o in obras_data]
    chart_af     = [round(o["af"], 1) for o in obras_data]
    chart_ae     = [round(o["ae"], 1) for o in obras_data]

    import json as _json
    chart_labels_js = _json.dumps(chart_labels)
    chart_af_js     = _json.dumps(chart_af)
    chart_ae_js     = _json.dumps(chart_ae)

    # ── Chart Precio de Venta vs Costo Real por obra ────────────────────────
    import json as _json2
    chart2_labels_js = _json2.dumps([o["obra"] for o in obras_data])
    chart2_pv_js     = _json2.dumps([round(o["pv"], 0) for o in obras_data])
    chart2_costo_js  = _json2.dumps([round(o.get("r_tot_aj", o["r_tot"]), 0) for o in obras_data])
    chart2_colors_js = _json2.dumps([
      "rgba(16,185,129,0.75)" if o["pv"] >= o.get("r_tot_aj", o["r_tot"]) else "rgba(239,68,68,0.75)"
        for o in obras_data
    ])

    # ── Panel Mantenimiento HTML ──────────────────────────────────────────────
    total_estructura_real = total_mant_real + total_gf_real
    saldo_real_c  = "#166534" if saldo_prev >= 0 else "#991b1b"
    saldo_real_ic = "▲" if saldo_prev >= 0 else "▼"
    pct_oh_c = "#991b1b" if pct_overhead > 25 else ("#92400e" if pct_overhead > 15 else "#166534")
    if mant_data or total_gf_real > 0:
        # Cobertura GG (solo real — sin presupuesto de estructura)
        saldo_real_c  = "#166534" if saldo_prev >= 0 else "#991b1b"
        saldo_real_ic = "▲" if saldo_prev >= 0 else "▼"
        bar_real_w    = min(pct_cob_prev, 100)
        bar_real_c    = "#16a34a" if pct_cob_prev >= 100 else ("#f59e0b" if pct_cob_prev >= 70 else "#dc2626")
        mant_filas = ""
        for d in mant_data:
            mant_filas += f"""<tr>
              <td style="font-weight:600;"><a href="/modulo/economico/obra/{_E(d['obra'])}" style="color:#6366f1;text-decoration:none;">{_E(d['obra'])}</a></td>
              <td style="text-align:right;">{_m(d['r_tot'])}</td>
              <td style="text-align:right;font-size:.78rem;color:#6b7280;">{d['hh']:,.1f} HH</td>
            </tr>"""
        gf_link = '<a href="/modulo/economico/gastos-fijos" style="font-size:.76rem;background:#fef3c7;color:#92400e;padding:3px 10px;border-radius:5px;text-decoration:none;font-weight:700;margin-left:10px;">+ Cargar gastos fijos</a>'
        pct_total_overhead = (total_estructura_real / total_prod_real * 100.0) if total_prod_real > 0 else 0.0
        pct_total_c = "#991b1b" if pct_total_overhead > 25 else ("#92400e" if pct_total_overhead > 15 else "#166534")
        mant_panel_html = f"""
    <!-- Costos de Estructura / Mantenimiento -->
    <div class="card" style="border-top:3px solid #f59e0b;">
      <div class="ct" style="background:#fefce8;color:#92400e;font-size:.95rem;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:6px;">
        <span>🏭 Costos de Estructura &amp; Mantenimiento
          <span style="font-size:.78rem;font-weight:400;color:#a16207;margin-left:8px;">Overhead — sin ingreso directo</span>
        </span>
        {gf_link}
      </div>
      <div class="cb">
        <!-- KPIs fila 1: presupuesto vs realidad -->
        <div style="display:flex;flex-wrap:wrap;gap:14px;margin-bottom:16px;">
          <div style="background:#f0fdf4;border-radius:10px;padding:16px 20px;flex:1;min-width:160px;border-left:5px solid #6366f1;">
            <div style="font-size:.8rem;color:#6b7280;font-weight:700;text-transform:uppercase;margin-bottom:6px;">GG Previstos en proyectos</div>
            <div style="font-size:1.6rem;font-weight:900;color:#6366f1;line-height:1;">{_m(total_gg_prev)}</div>
            <div style="font-size:.78rem;color:#6b7280;margin-top:4px;">presupuestados para cubrir overhead</div>
          </div>
          <div style="background:#fff7ed;border-radius:10px;padding:16px 20px;flex:1;min-width:160px;border-left:5px solid #f59e0b;">
            <div style="font-size:.8rem;color:#6b7280;font-weight:700;text-transform:uppercase;margin-bottom:6px;">Total Estructura Real</div>
            <div style="font-size:1.6rem;font-weight:900;color:#92400e;line-height:1;">{_m(total_estructura_real)}</div>
            <div style="font-size:.78rem;color:#6b7280;margin-top:4px;">mant. {_m(total_mant_real)} + gastos fijos {_m(total_gf_real)}</div>
          </div>
          <div style="background:{'#f0fdf4' if saldo_prev>=0 else '#fef2f2'};border-radius:10px;padding:16px 20px;flex:1;min-width:160px;border-left:5px solid {saldo_real_c};">
            <div style="font-size:.8rem;color:#6b7280;font-weight:700;text-transform:uppercase;margin-bottom:6px;">{'✅ Saldo positivo' if saldo_prev>=0 else '❌ Déficit'}</div>
            <div style="font-size:1.6rem;font-weight:900;color:{saldo_real_c};line-height:1;">{saldo_real_ic} {_m(abs(saldo_prev))}</div>
            <div style="font-size:.78rem;color:#6b7280;margin-top:4px;">{pct_cob_prev:.0f}% del overhead cubierto por GG presupuestados</div>
          </div>
          <div style="background:#fff;border-radius:10px;padding:16px 20px;flex:1;min-width:160px;border-left:5px solid {pct_total_c};">
            <div style="font-size:.8rem;color:#6b7280;font-weight:700;text-transform:uppercase;margin-bottom:6px;">Overhead / Obras productivas</div>
            <div style="font-size:1.6rem;font-weight:900;color:{pct_total_c};line-height:1;">{pct_total_overhead:.1f}%</div>
            <div style="font-size:.78rem;color:#6b7280;margin-top:4px;">de {_m(total_prod_real)} en costos reales</div>
          </div>
        </div>
        <!-- KPIs fila 2: breakdown mantenimiento vs gastos fijos -->
        <div style="display:flex;flex-wrap:wrap;gap:14px;margin-bottom:16px;">
          <div style="background:#fff;border-radius:10px;padding:14px 18px;flex:1;min-width:180px;border:1px solid #fde68a;">
            <div style="font-size:.8rem;color:#92400e;font-weight:700;margin-bottom:8px;">⚒️ Obras de Mantenimiento</div>
            <div style="display:flex;justify-content:space-between;font-size:.85rem;margin-bottom:4px;">
              <span style="color:#6b7280;">Previsto</span>
              <span style="font-weight:700;">{_m(total_mant_prev)}</span>
            </div>
            <div style="display:flex;justify-content:space-between;font-size:.85rem;margin-bottom:6px;">
              <span style="color:#6b7280;">Real</span>
              <span style="font-weight:700;">{_m(total_mant_real)}</span>
            </div>
            {(lambda d,p: f'<div style="font-size:.82rem;font-weight:700;color:{"#991b1b" if d>0 else "#166534"};">{"▲" if d>0 else "▼"} {_m(abs(d))} ({"+" if d>0 else ""}{(d/p*100):.1f}%)</div>' if p>0 else ''
              )(total_mant_real-total_mant_prev, total_mant_prev)}
            <div style="font-size:.76rem;color:#9ca3af;margin-top:4px;">{sum(d['hh'] for d in mant_data):,.0f} HH acumuladas</div>
          </div>
          <div style="background:#fff;border-radius:10px;padding:14px 18px;flex:1;min-width:180px;border:1px solid #fca5a5;">
            <div style="font-size:.8rem;color:#dc2626;font-weight:700;margin-bottom:8px;">🏢 Gastos Fijos de Estructura</div>
            <div style="display:flex;justify-content:space-between;font-size:.85rem;margin-bottom:4px;">
              <span style="color:#6b7280;">Acumulado real</span>
              <span style="font-weight:700;">{_m(total_gf_real)}</span>
            </div>
            <div style="font-size:.76rem;color:#9ca3af;margin-top:4px;">sueldos · alquiler · servicios</div>
          </div>
        </div>
        <!-- Barra de cobertura -->
        <div style="background:#f8fafc;border:1px solid #e5e7eb;border-radius:8px;padding:14px 16px;margin-bottom:16px;">
          <div style="font-size:.84rem;font-weight:700;color:#374151;margin-bottom:10px;">
            📊 Cobertura del overhead: <span style="color:{saldo_real_c};">{pct_cob_prev:.0f}%</span>
            <span style="font-weight:400;color:#6b7280;font-size:.78rem;margin-left:6px;">GG presupuestados vs estructura real</span>
          </div>
          <div style="background:#e5e7eb;border-radius:6px;height:16px;margin-bottom:8px;position:relative;">
            <div style="background:{bar_real_c};border-radius:6px;height:16px;width:{bar_real_w:.1f}%;transition:width .3s;"></div>
            <div style="position:absolute;top:0;left:0;right:0;bottom:0;display:flex;align-items:center;justify-content:center;font-size:.72rem;font-weight:700;color:#fff;text-shadow:0 1px 2px rgba(0,0,0,.4);">{pct_cob_prev:.0f}%</div>
          </div>
          <div style="display:flex;justify-content:space-between;font-size:.82rem;">
            <span style="color:#6366f1;font-weight:700;">GG Prev: {_m(total_gg_prev)}</span>
            <span style="color:#92400e;font-weight:700;">Estructura: {_m(total_estructura_real)}</span>
          </div>
        </div>
        <!-- Tabla OTs + Gráfico -->
        <div class="two" style="margin-bottom:0;">
          <div>
            {"<table style='font-size:.85rem;'><thead><tr><th>Obra mantenimiento</th><th style='text-align:right;'>Costo Real</th><th style='text-align:right;'>HH</th></tr></thead><tbody>" + mant_filas + "</tbody></table>" if mant_filas else ""}
          </div>
          <div style="position:relative;min-height:180px;">
            <div style="font-size:.82rem;font-weight:700;color:#374151;margin-bottom:6px;">Evolución mensual — estructura total</div>
            <div style="position:relative;height:160px;"><canvas id="chartMant"></canvas></div>
          </div>
        </div>
      </div>
    </div>"""
    else:
        mant_panel_html = ""

    # KPI cards top
    def _kpi(titulo, valor, color="#6366f1", sub=""):
        return (f'<div style="background:#fff;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,.07);'
                f'padding:16px 20px;flex:1;min-width:150px;border-left:4px solid {color};">'
                f'<div style="font-size:.72rem;color:#6b7280;font-weight:700;text-transform:uppercase;letter-spacing:.04em;">{titulo}</div>'
                f'<div style="font-size:1.45rem;font-weight:900;color:{color};margin:6px 0 2px;line-height:1;">{valor}</div>'
                f'<div style="font-size:.72rem;color:#9ca3af;">{sub}</div>'
                f'</div>')

    kpi_html = (
        _kpi("Obras con desvío",         f"{'🔴 ' if n_riesgo>0 else '🟢 '}{n_riesgo}",
             "#991b1b" if n_riesgo>0 else "#166534", f"de {n_obras} obras") +
        _kpi("Margen prom. proyectado",  f"{'🟢' if mg_prom>=10 else '🟠' if mg_prom>=5 else '🔴'} {mg_prom:.1f}%",
             _cm(mg_prom), "a la finalización") +
        _kpi("Costo directo ejecutado",  _m(costo_cd),      "#1e293b", "acumulado") +
        _kpi("Costo directo previsto",   _m(costo_cd_prev), "#3b82f6", "presupuestado") +
        _kpi("Av. económico promedio",   f"{ae_prom:.1f}%", "#3b82f6", "sobre presupuesto") +
        _kpi("Riesgo global",            f"{riesgo_em} {riesgo_lbl}", riesgo_c, f"mg prom. {mg_prom:.1f}% · {n_criticos} obra{'s' if n_criticos!=1 else ''} crítica{'s' if n_criticos!=1 else ''}")
    )

    return f"""<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>Dashboard Ejecutivo — Rentabilidad</title>
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.3/dist/chart.umd.min.js"></script>
  <style>
    *{{box-sizing:border-box;}}
    body{{font-family:system-ui,sans-serif;background:#f1f5f9;margin:0;padding:0;}}
    .hdr{{background:linear-gradient(135deg,#1e293b,#334155);color:#fff;padding:16px 24px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;}}
    .hdr h1{{margin:0;font-size:1.1rem;letter-spacing:-.01em;}}
    .hdr a{{color:#fff;text-decoration:none;font-size:.8rem;background:rgba(255,255,255,.15);padding:5px 11px;border-radius:6px;}}
    .hdr a:hover{{background:rgba(255,255,255,.28);}}
    .body{{padding:18px;display:flex;flex-direction:column;gap:16px;}}
    .kpi-row{{display:flex;flex-wrap:wrap;gap:12px;}}
    .card{{background:#fff;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,.07);overflow:hidden;}}
    .ct{{background:#f8fafc;border-bottom:1px solid #e5e7eb;padding:11px 16px;font-weight:700;font-size:.88rem;color:#1e293b;}}
    .cb{{padding:16px;}}
    .two{{display:grid;grid-template-columns:3fr 2fr;gap:16px;}}
    @media(max-width:800px){{.two{{grid-template-columns:1fr;}}}}
    .three{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:16px;}}
    @media(max-width:800px){{.three{{grid-template-columns:1fr;}}}}
    table{{width:100%;border-collapse:collapse;font-size:.83rem;}}
    th{{background:#1e293b;color:#fff;padding:8px 10px;text-align:left;font-size:.76rem;white-space:nowrap;}}
    td{{padding:7px 10px;border-bottom:1px solid #f1f5f9;vertical-align:middle;}}
    tr:last-child td{{border-bottom:none;}}
    tr:hover td{{background:#f8fafc;}}
    .resumen{{list-style:none;margin:0;padding:0;display:flex;flex-direction:column;gap:8px;}}
    .resumen li{{padding:8px 12px;background:#f8fafc;border-left:3px solid #6366f1;border-radius:0 6px 6px 0;font-size:.85rem;color:#1e293b;}}
  </style>
</head>
<body>
  <div class="hdr">
    <div>
      <h1>📊 Dashboard Ejecutivo de Rentabilidad de Obras</h1>
      <div style="font-size:.74rem;opacity:.7;margin-top:2px;">Visión consolidada · {n_obras} obras activas</div>
    </div>
    <div style="display:flex;gap:7px;flex-wrap:wrap;">
      <a href="/modulo/economico">← Módulo Económico</a>
      <a href="/">Inicio</a>
    </div>
  </div>

  <div class="body">

    <!-- KPI superiores -->
    <div class="kpi-row">{kpi_html}</div>

    <!-- Tabla de obras + Semáforos -->
    <div class="two">
      <div class="card">
        <div class="ct">📋 Estado de cada obra</div>
        <div style="overflow-x:auto;">
          <table>
            <thead><tr>
              <th>Obra</th><th>Cliente</th>
              <th style="min-width:110px;">Av. Físico <span title="Estado de Avance ingresado manualmente en la OT (0–100%)" style="cursor:help;opacity:.7;font-weight:400;">ⓘ</span></th>
              <th style="min-width:110px;">Av. Económico <span title="Costo Total Real / Costo Total Presupuestado × 100. Superar 100% indica sobrecosto." style="cursor:help;opacity:.7;font-weight:400;">ⓘ</span></th>
              <th style="text-align:right;">Margen Proy.</th>
              <th style="text-align:right;">Precio de Venta</th>
              <th style="text-align:right;">Costo Real</th>
              <th style="text-align:center;">Estado</th>
            </tr></thead>
            <tbody>{tabla_obras}</tbody>
          </table>
        </div>
      </div>
      <div class="card">
        <div class="ct">🚦 Semáforo de obras</div>
        <div class="cb" style="display:flex;flex-direction:column;gap:8px;">
          {semaforos_html}
        </div>
      </div>
    </div>

    <!-- Gráfico avance -->
    <div class="card">
      <div class="ct">📈 Avance físico vs Avance económico — Todas las obras</div>
      <div class="cb" style="position:relative;height:220px;">
        <canvas id="chartAvance"></canvas>
      </div>
    </div>

    <!-- Gráfico Precio de Venta vs Costo Real por Obra -->
    <div class="card">
      <div class="ct">💵 Precio de Venta vs Costo Real — por obra</div>
      <div style="padding:6px 16px 0;font-size:.75rem;color:#6b7280;">
        Verde = obra rentable (PV &gt; Costo Real). Rojo = costo supera el precio de venta presupuestado.
      </div>
      <div class="cb" style="position:relative;height:260px;">
        <canvas id="chartIngEgr"></canvas>
      </div>
    </div>

    <div class="card">
      <div class="ct">🏭 Gastos Generales — Previsto vs Real</div>
      <div class="cb">
        <div style="display:flex;flex-wrap:wrap;gap:12px;">
          <div style="flex:1;min-width:160px;background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;padding:14px 16px;">
            <div style="font-size:.72rem;color:#166534;font-weight:700;text-transform:uppercase;">GG previstos</div>
            <div style="font-size:1.35rem;font-weight:900;color:#166534;">{_m(total_gg_prev)}</div>
            <div style="font-size:.72rem;color:#6b7280;">presupuestados en obras</div>
          </div>
          <div style="flex:1;min-width:160px;background:#fff7ed;border:1px solid #fed7aa;border-radius:10px;padding:14px 16px;">
            <div style="font-size:.72rem;color:#92400e;font-weight:700;text-transform:uppercase;">GG reales</div>
            <div style="font-size:1.35rem;font-weight:900;color:#92400e;">{_m(total_estructura_real)}</div>
            <div style="font-size:.72rem;color:#6b7280;">mantenimiento + gastos fijos</div>
          </div>
          <div style="flex:1;min-width:160px;background:{'#f0fdf4' if saldo_prev >= 0 else '#fef2f2'};border:1px solid {'#bbf7d0' if saldo_prev >= 0 else '#fecaca'};border-radius:10px;padding:14px 16px;">
            <div style="font-size:.72rem;color:{'#166534' if saldo_prev >= 0 else '#991b1b'};font-weight:700;text-transform:uppercase;">Saldo / cobertura</div>
            <div style="font-size:1.35rem;font-weight:900;color:{saldo_real_c};">{saldo_real_ic} {_m(abs(saldo_prev))}</div>
            <div style="font-size:.72rem;color:#6b7280;">{pct_cob_prev:.0f}% de cobertura</div>
          </div>
        </div>
      </div>
    </div>

    <!-- Ranking desvíos + Resumen ejecutivo -->
    <div class="two">
      <div class="card">
        <div class="ct">📉 Ranking de desvíos por rubro</div>
        <div style="overflow-x:auto;">
          <table>
            <thead><tr>
              <th>Concepto</th>
              <th style="text-align:right;">Previsto</th>
              <th style="text-align:right;">Real</th>
              <th>Desvío</th>
            </tr></thead>
            <tbody>{ranking_html}</tbody>
          </table>
        </div>
      </div>
      <div class="card">
        <div class="ct">📝 Resumen ejecutivo</div>
        <div class="cb">
          <div style="font-size:.8rem;font-weight:700;color:#374151;margin-bottom:10px;">Resumen de situación actual</div>
          <ul class="resumen">{resumen_html}</ul>
        </div>
      </div>
    </div>

    {mant_panel_html}

    <!-- Distribución de Gastos de Estructura por Obra -->
    <div class="card" style="border-top:3px solid #f59e0b;">
      <div class="ct" style="background:#fefce8;color:#92400e;">
        ⚖️ Distribución de Gastos de Estructura por Obra
        <span style="font-size:.72rem;font-weight:400;color:#a16207;margin-left:8px;">
          overhead real ({_m(total_estructura_real)}) prorrateado por costo directo real de cada proyecto
        </span>
      </div>
      <div style="overflow-x:auto;">
        <table>
          <thead><tr>
            <th>Obra</th>
            <th style="text-align:right;">CD Real</th>
            <th style="text-align:right;">% del total</th>
            <th style="text-align:right;background:#fef3c7;color:#92400e;">GG asignado</th>
            <th style="text-align:right;">Costo ajustado</th>
            <th style="text-align:right;">Precio de Venta</th>
            <th style="text-align:right;">Margen ajustado</th>
            <th style="text-align:right;">Δ vs proy.</th>
          </tr></thead>
          <tbody>
            {dist_rows_html}
            <tr style="background:#f1f5f9;font-weight:700;border-top:2px solid #cbd5e1;">
              <td>TOTAL</td>
              <td style="text-align:right;">{_m(_sum_r_cd)}</td>
              <td style="text-align:right;">100%</td>
              <td style="text-align:right;color:#f59e0b;">{_m(total_estructura_real)}</td>
              <td></td>
              <td style="text-align:right;">{_m(_pv_total)}</td>
              <td></td><td></td>
            </tr>
          </tbody>
        </table>
      </div>
      <div style="padding:10px 16px;font-size:.74rem;color:#6b7280;border-top:1px solid #e5e7eb;">
        <b>Costo ajustado</b> = CD Real + GG asignado + Impuestos propios.
        <b>Δ vs proy.</b> = diferencia en puntos porcentuales respecto al margen proyectado por presupuesto.
        Overhead total incluye obras de mantenimiento + gastos fijos de estructura.
      </div>
    </div>

  </div>

  <script>
  (function() {{
    // Chart 1: Avance
    const ctx1 = document.getElementById('chartAvance').getContext('2d');
    new Chart(ctx1, {{
      type: 'bar',
      data: {{
        labels: {chart_labels_js},
        datasets: [
          {{
            label: 'Avance Físico %',
            data: {chart_af_js},
            backgroundColor: 'rgba(59,130,246,0.7)',
            borderColor: 'rgba(59,130,246,1)',
            borderWidth: 1, borderRadius: 4,
          }},
          {{
            label: 'Avance Económico %',
            data: {chart_ae_js},
            backgroundColor: 'rgba(239,68,68,0.6)',
            borderColor: 'rgba(239,68,68,1)',
            borderWidth: 1, borderRadius: 4,
          }}
        ]
      }},
      options: {{
        responsive: true, maintainAspectRatio: false,
        plugins: {{
          legend: {{ position: 'top', labels: {{ font: {{ size: 11 }} }} }},
          tooltip: {{ callbacks: {{ label: c => ` ${{c.dataset.label}}: ${{c.parsed.y.toFixed(1)}}%` }} }}
        }},
        scales: {{
          y: {{ beginAtZero: true, max: 110,
                ticks: {{ callback: v => v + '%', font: {{ size: 10 }} }},
                grid: {{ color: '#f1f5f9' }} }},
          x: {{ ticks: {{ font: {{ size: 10 }} }} }}
        }}
      }}
    }});

    // Chart 2: Precio de Venta vs Costo Real por obra
    const ctx2 = document.getElementById('chartIngEgr').getContext('2d');
    new Chart(ctx2, {{
      type: 'bar',
      data: {{
        labels: {chart2_labels_js},
        datasets: [
          {{
            label: 'Precio de Venta (Presupuestado)',
            data: {chart2_pv_js},
            backgroundColor: 'rgba(99,102,241,0.7)',
            borderColor: 'rgba(99,102,241,1)',
            borderWidth: 1, borderRadius: 4,
          }},
          {{
            label: 'Costo Real',
            data: {chart2_costo_js},
            backgroundColor: {chart2_colors_js},
            borderWidth: 1, borderRadius: 4,
          }}
        ]
      }},
      options: {{
        responsive: true, maintainAspectRatio: false,
        plugins: {{
          legend: {{ position: 'top', labels: {{ font: {{ size: 11 }} }} }},
          tooltip: {{
            callbacks: {{
              label: c => ` ${{c.dataset.label}}: ${{(c.parsed.y/1000000).toFixed(2)}} M`
            }}
          }}
        }},
        scales: {{
          y: {{
            beginAtZero: true,
            ticks: {{ callback: v => '$' + (v/1000000).toFixed(1) + 'M', font: {{ size: 10 }} }},
            grid: {{ color: '#f1f5f9' }}
          }},
          x: {{ ticks: {{ font: {{ size: 10 }}, maxRotation: 45 }} }}
        }}
      }}
    }});

    // Chart 3: Mantenimiento — evolución mensual (apilado: HH + gastos fijos)
    const ctxMant = document.getElementById('chartMant');
    if (ctxMant) {{
      new Chart(ctxMant.getContext('2d'), {{
        type: 'bar',
        data: {{
          labels: {mant_mes_js},
          datasets: [
            {{
              label: 'Obras mant. (HH)',
              data: {mant_costs_js},
              backgroundColor: 'rgba(245,158,11,0.75)',
              borderColor: 'rgba(245,158,11,1)',
              borderWidth: 1, borderRadius: 2,
              stack: 'estructura',
            }},
            {{
              label: 'Gastos fijos',
              data: {gf_costs_js},
              backgroundColor: 'rgba(239,68,68,0.7)',
              borderColor: 'rgba(239,68,68,1)',
              borderWidth: 1, borderRadius: 2,
              stack: 'estructura',
            }}
          ]
        }},
        options: {{
          responsive: true, maintainAspectRatio: false,
          plugins: {{
            legend: {{ position: 'top', labels: {{ font: {{ size: 9 }}, boxWidth: 10 }} }},
            tooltip: {{ callbacks: {{ label: c => ` ${{c.dataset.label}}: ${{(c.parsed.y/1000).toFixed(0)}}k` }} }}
          }},
          scales: {{
            y: {{ beginAtZero: true, stacked: true,
                  ticks: {{ callback: v => '$' + (v/1000).toFixed(0) + 'k', font: {{ size: 9 }} }},
                  grid: {{ color: '#f1f5f9' }} }},
            x: {{ stacked: true, ticks: {{ font: {{ size: 9 }}, maxRotation: 45 }} }}
          }}
        }}
      }});
    }}
  }})();
  </script>
</body>
</html>"""
