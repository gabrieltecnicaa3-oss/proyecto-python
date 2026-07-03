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

    rm = db.execute("SELECT mat_real,pintura_real,subcontratos_real FROM economico_costos_reales WHERE ot_id=?", (ot_id,)).fetchone()
    r_mat  = float(rm[0] or 0) if rm else 0.0
    r_pint = float(rm[1] or 0) if rm else 0.0
    r_sub  = float(rm[2] or 0) if rm else 0.0

    hh = db.execute("SELECT COALESCE(SUM(horas),0) FROM partes_trabajo WHERE ot_id=?", (ot_id,)).fetchone()
    hh_total = float(hh[0] or 0) if hh else 0.0

    r_mo   = hh_total * cfg["precio_hora_mo"]
    r_cons = hh_total * cfg["precio_hora_cons"]
    r_cd   = r_mat + r_pint + r_sub + r_mo + r_cons
    r_gg   = r_cd * cfg["pct_gastos_gen"] / 100.0
    r_imp  = r_cd * cfg["pct_impuestos"] / 100.0
    r_tot  = r_cd + r_gg + r_imp

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
        "rm": {"mat":r_mat,"pintura":r_pint,"sub":r_sub},
        "ra": {"mo":r_mo,"cons":r_cons,"gg":r_gg,"imp":r_imp},
        "r":  {"cd":r_cd,"tot":r_tot},
        "hh": hh_total, "kg": kg, "avf": avf,
        "ave": min((r_tot/p_tc*100.0) if p_tc>0 else 0.0, 999.9),
    }


def _aggregate_obra(ots_data):
    agg = {k:0.0 for k in ["p_mat","p_pint","p_mo","p_cons","p_ing","p_gg","p_imp","p_ben",
                            "p_cd","p_tc","p_pv","r_mat","r_pint","r_sub","r_mo","r_cons",
                            "r_gg","r_imp","r_cd","r_tot","hh","kg"]}
    avf_list = []
    for d in ots_data:
        for k,v in [("p_mat",d["p"]["mat"]),("p_pint",d["p"]["pintura"]),("p_mo",d["p"]["mo"]),
                    ("p_cons",d["p"]["cons"]),("p_ing",d["p"]["ing"]),("p_gg",d["p"]["gg"]),
                    ("p_imp",d["p"]["imp"]),("p_ben",d["p"]["ben"]),("p_cd",d["p"]["cd"]),
                    ("p_tc",d["p"]["tc"]),("p_pv",d["p"]["pv"]),
                    ("r_mat",d["rm"]["mat"]),("r_pint",d["rm"]["pintura"]),("r_sub",d["rm"]["sub"]),
                    ("r_mo",d["ra"]["mo"]),("r_cons",d["ra"]["cons"]),("r_gg",d["ra"]["gg"]),
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
    db = get_db(); _ensure_schema(db)
    mensaje = error = ""
    if request.method == "POST":
        try:
            db.execute("UPDATE economico_config SET precio_hora_mo=?,precio_hora_cons=?,pct_gastos_gen=?,pct_impuestos=?,updated_at=CURRENT_TIMESTAMP",
                (float(request.form.get("precio_hora_mo") or 0),
                 float(request.form.get("precio_hora_cons") or 0),
                 float(request.form.get("pct_gastos_gen") or 5),
                 float(request.form.get("pct_impuestos") or 3)))
            db.commit(); mensaje = "Config global guardada."
        except Exception as exc: error = str(exc)
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
<div class="fg"><label>% Gastos Generales (sobre costo directo real)</label>
  <input type="number" name="pct_gastos_gen" step="0.01" min="0" max="100" value="{cfg['pct_gastos_gen']}"></div>
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
    mg  = ((agg["p_pv"]-agg["r_tot"])/agg["p_pv"]*100.0) if agg["p_pv"]>0 else 0.0
    mc  = _cm(mg)
    af  = agg["avf"]; ae = agg["ave"]
    ac  = "#991b1b" if ae>af+5 else ("#166534" if ae<=af else "#92400e")

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
      <td style="text-align:right;">{_m(agg['r_tot'])}</td>
      <td style="text-align:right;color:{mc};">{_pct(mg)}</td>
      <td>{_pb(af,'#3b82f6',7)}</td><td style="color:{ac};">{_pct(ae)}</td><td></td>
    </tr>"""

    desv_filas = ""
    for nombre, prev, real in [
        ("Materiales",agg["p_mat"],agg["r_mat"]),("Pintura",agg["p_pint"],agg["r_pint"]),
        ("Mano de Obra",agg["p_mo"],agg["r_mo"]),("Consumibles",agg["p_cons"],agg["r_cons"]),
        ("Ingeniería",agg["p_ing"],0.0),("Subcontratos",0.0,agg["r_sub"]),
        ("Gastos Generales",agg["p_gg"],agg["r_gg"]),("Impuestos",agg["p_imp"],agg["r_imp"])]:
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
      <div style="min-width:110px;"><label>% Gastos Generales</label>
        <input type="number" name="pct_gastos_gen" step="0.01" min="0" max="100" value="{_fv(cfg['pct_gastos_gen'])}"></div>
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
      <div style="font-size:1.1rem;font-weight:800;color:#1e293b;">{_m(agg['r_tot'])}</div></div>
    <div style="flex:1;min-width:100px;border-top:3px solid {mc};padding-top:8px;">
      <div style="font-size:.66rem;color:#9ca3af;font-weight:700;">MARGEN REAL</div>
      <div style="font-size:1.1rem;font-weight:800;color:{mc};">{_pct(mg)}</div>
      <div style="font-size:.7rem;color:#9ca3af;">{_m(agg['p_pv']-agg['r_tot'])}</div></div>
    <div style="flex:1;min-width:100px;border-top:3px solid #10b981;padding-top:8px;">
      <div style="font-size:.66rem;color:#9ca3af;font-weight:700;">$/KG REAL</div>
      <div style="font-size:1.1rem;font-weight:800;color:#10b981;">{_m(agg['r_tot']/agg['kg'] if agg['kg']>0 else 0)}</div>
      <div style="font-size:.7rem;color:#9ca3af;">prev: {_m(agg['p_pv']/agg['kg'] if agg['kg']>0 else 0)}</div></div>
    <div style="flex:2;min-width:220px;border-top:3px solid #e5e7eb;padding-top:8px;">
      <div style="font-size:.66rem;color:#9ca3af;font-weight:700;margin-bottom:5px;">AVANCE FÍSICO vs ECONÓMICO</div>
      <div style="display:flex;align-items:center;gap:7px;margin-bottom:4px;">
        <span style="font-size:.7rem;color:#3b82f6;width:72px;">Físico</span>{_pb(af,'#3b82f6',10)}</div>
      <div style="display:flex;align-items:center;gap:7px;">
        <span style="font-size:.7rem;color:{ac};width:72px;">Económico</span>{_pb(ae,ac,10)}</div>
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
          <td style="text-align:right;">{_m(agg['r_tot'])}</td>
          <td style="text-align:right;color:{_cd(agg['r_tot']-agg['p_tc'])};">
            {"▲" if agg['r_tot']>agg['p_tc'] else "▼"} {_m(abs(agg['r_tot']-agg['p_tc']))}</td>
          <td style="text-align:right;color:{_cd(agg['r_tot']-agg['p_tc'])};">
            {_pct(abs((agg['r_tot']-agg['p_tc'])/agg['p_tc']*100) if agg['p_tc'] else 0)}</td>
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
    ot = db.execute("SELECT id,cliente,obra,titulo,tipo_estructura,estado FROM ordenes_trabajo WHERE id=?", (ot_id,)).fetchone()
    if not ot:
        return "OT no encontrada", 404
    _, cliente, obra, titulo, tipo, estado = ot
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
            elif accion == "guardar_costos_reales":
                mat = float(request.form.get("mat_real") or 0)
                pint = float(request.form.get("pintura_real") or 0)
                sub  = float(request.form.get("subcontratos_real") or 0)
                ex = db.execute("SELECT id FROM economico_costos_reales WHERE ot_id=?", (ot_id,)).fetchone()
                if ex:
                    db.execute("UPDATE economico_costos_reales SET mat_real=?,pintura_real=?,subcontratos_real=?,updated_at=CURRENT_TIMESTAMP WHERE ot_id=?", (mat,pint,sub,ot_id))
                else:
                    db.execute("INSERT INTO economico_costos_reales(ot_id,mat_real,pintura_real,subcontratos_real) VALUES(?,?,?,?)", (ot_id,mat,pint,sub))
                db.commit(); mensaje = "Costos reales guardados."
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
    avf = data["avf"]; ave = data["ave"]
    mg = ((p["pv"]-r["tot"])/p["pv"]*100.0) if p["pv"]>0 else 0.0
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
        _kc("$/kg Real",  _m(r["tot"]/data["kg"] if data["kg"]>0 else 0), "", "#1e293b") +
        _kc("Margen Prev.", _pct(p["ben"]/p["pv"]*100 if p["pv"]>0 else 0), _m(p["ben"]), "#3b82f6") +
        _kc("Margen Real",  _pct(mg), f"PV {_m(p['pv'])}", mc) +
        _kc("Av.Físico", _pct(avf), "estado OT", "#10b981") +
        _kc("Av.Econ.", _pct(ave), "gasto/presup.", ac))

    desv_rows = ""
    for nombre, prev, real in [
        ("Materiales",p["mat"],rm["mat"]),("Pintura",p["pintura"],rm["pintura"]),
        ("Mano de Obra",p["mo"],ra["mo"]),("Consumibles",p["cons"],ra["cons"]),
        ("Ingeniería",p["ing"],0.0),("Subcontratos",0.0,rm["sub"]),
        ("Gastos Generales",p["gg"],ra["gg"]),("Impuestos",p["imp"],ra["imp"])]:
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
    <div class="card"><div class="ct">💸 Costos Reales</div><div class="cb">
      <form method="post" style="margin-bottom:16px;">
        <input type="hidden" name="accion" value="guardar_costos_reales">
        <div style="font-size:.76rem;font-weight:700;color:#374151;margin-bottom:8px;border-bottom:1px solid #e5e7eb;padding-bottom:4px;">Carga Manual</div>
        <div class="fg"><label>Materiales reales ($)</label><input type="number" name="mat_real" step="0.01" min="0" value="{_fv(rm['mat'])}"></div>
        <div class="fg"><label>Pintura real ($)</label><input type="number" name="pintura_real" step="0.01" min="0" value="{_fv(rm['pintura'])}"></div>
        <div class="fg"><label>Subcontratos ($)</label><input type="number" name="subcontratos_real" step="0.01" min="0" value="{_fv(rm['sub'])}"></div>
        <button type="submit" class="btn" style="background:#0891b2;color:#fff;width:100%;">💾 Guardar costos manuales</button>
      </form>
      <div style="font-size:.76rem;font-weight:700;color:#374151;margin-bottom:8px;border-bottom:1px solid #e5e7eb;padding-bottom:4px;">Calculados automáticamente <span class="auto">AUTO</span></div>
      <div class="fg"><label>Mano de Obra <span class="auto">{data['hh']:,.1f} HH × ${cfg['precio_hora_mo']:,.2f}</span></label><div class="rv">{_m(ra['mo'])}</div></div>
      <div class="fg"><label>Consumibles <span class="auto">{data['hh']:,.1f} HH × ${cfg['precio_hora_cons']:,.2f}</span></label><div class="rv">{_m(ra['cons'])}</div></div>
      <div class="fg"><label>Gastos Generales <span class="auto">{cfg['pct_gastos_gen']:.1f}% costo directo</span></label><div class="rv">{_m(ra['gg'])}</div></div>
      <div class="fg"><label>Impuestos <span class="auto">{cfg['pct_impuestos']:.1f}% costo directo</span></label><div class="rv">{_m(ra['imp'])}</div></div>
      <hr style="border:none;border-top:1px solid #e5e7eb;margin:10px 0;">
      <div style="display:flex;justify-content:space-between;"><span style="font-size:.78rem;font-weight:700;">Total Costo Real</span>
        <span style="font-weight:800;">{_m(r['tot'])}</span></div>
      <div style="padding:8px;border-radius:6px;background:#fafafe;border:1px solid #e0e7ff;margin-top:6px;">
        <div style="font-size:.7rem;color:#6b7280;">Resultado (PV − Costo Real)</div>
        <div style="font-weight:800;font-size:1rem;color:{mc};">{_m(p['pv']-r['tot'])} <span style="font-size:.8rem;">({_pct(mg)})</span></div>
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
          <td style="text-align:right;">{_m(p['tc'])}</td><td style="text-align:right;">{_m(r['tot'])}</td>
          <td style="text-align:right;color:{_cd(r['tot']-p['tc'])};">{"▲" if r["tot"]>p["tc"] else "▼"} {_m(abs(r["tot"]-p["tc"]))}</td>
          <td style="text-align:right;color:{_cd(r['tot']-p['tc'])};">{_pct(abs((r["tot"]-p["tc"])/p["tc"]*100) if p["tc"] else 0)}</td>
        </tr>
      </tbody>
    </table></div>
  </div>
</div></body></html>"""
