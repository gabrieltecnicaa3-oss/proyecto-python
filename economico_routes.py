
"""
Módulo Económico — Costos previstos vs reales agrupados por Obra
KPIs: $/kg · Margen · Desvíos por rubro · Avance físico vs económico
"""
import html as html_lib
from flask import Blueprint, request, redirect

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
    """Pantalla principal: dashboard agrupado por obra con config de tasas."""
    return economico_obras()


@economico_bp.route("/modulo/economico/obras", methods=["GET", "POST"])
def economico_obras():
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
    <a href="/modulo/economico/certificados" style="background:#fff;color:#10b981;font-weight:700;">📜 Certificados</a>
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
        "SELECT id, cliente, obra, tipo_estructura FROM ordenes_trabajo ORDER BY obra, id"
    ).fetchall()

    obras_dict = {}
    for ot_id, cliente, obra, tipo in ots_rows:
        k = str(obra or "Sin obra").strip()
        if k not in obras_dict:
            obras_dict[k] = {"cliente": cliente or "", "ots": []}
        obras_dict[k]["ots"].append({"id": ot_id, "tipo": tipo})

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
            "pv": agg["p_pv"], "r_tot": agg["r_tot"], "r_cd": agg["r_cd"],
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
            ("Ingeniería",      agg["p_ing"],  0.0),
            ("Subcontratos",    0.0,           agg["r_sub"]),
            ("Gastos Generales",agg["p_gg"],   agg["r_gg"]),
            ("Impuestos",       agg["p_imp"],  agg["r_imp"]),
        ]:
            rubros_global[nm]["prev"] += prev
            rubros_global[nm]["real"] += real

    n_obras = len(obras_data)
    if n_obras == 0:
        return "<p>Sin datos de obras.</p><a href='/modulo/economico'>← Volver</a>"

    # ── KPIs globales ─────────────────────────────────────────────────────────
    n_riesgo    = sum(1 for o in obras_data if o["sem_lbl"] in ("Crítico","Atención"))
    n_criticos  = sum(1 for o in obras_data if o["sem_lbl"] == "Crítico")
    mg_prom     = sum(o["mg_proy"] for o in obras_data) / n_obras
    costo_cd    = sum(o["r_cd"]    for o in obras_data)
    ae_prom     = sum(o["ae"]      for o in obras_data) / n_obras
    # Riesgo global
    pct_riesgo  = n_riesgo / n_obras
    if n_criticos > 0 or pct_riesgo >= 0.5:
        riesgo_em, riesgo_lbl, riesgo_c = "🔴", "Alto", "#991b1b"
    elif pct_riesgo >= 0.25:
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

    # ── HTML ──────────────────────────────────────────────────────────────────
    # Tabla de obras
    tabla_obras = ""
    for o in obras_data:
        af_bar = f'<div style="background:#e5e7eb;border-radius:3px;height:8px;width:100%;min-width:50px;"><div style="background:#3b82f6;border-radius:3px;height:8px;width:{min(o["af"],100):.1f}%;"></div></div><span style="font-size:.72rem;color:#6b7280;">{o["af"]:.1f}%</span>'
        ae_c   = "#991b1b" if o["ae"] > o["af"]+5 else ("#166534" if o["ae"] <= o["af"] else "#92400e")
        ae_bar = f'<div style="background:#e5e7eb;border-radius:3px;height:8px;width:100%;min-width:50px;"><div style="background:{ae_c};border-radius:3px;height:8px;width:{min(o["ae"],100):.1f}%;"></div></div><span style="font-size:.72rem;color:{ae_c};">{o["ae"]:.1f}%</span>'
        mc     = _cm(o["mg_proy"])
        tabla_obras += f"""<tr>
          <td style="font-weight:700;"><a href="/modulo/economico/obra/{_E(o['obra'])}" style="color:#6366f1;text-decoration:none;">{_E(o['obra'])}</a></td>
          <td style="font-size:.75rem;color:#6b7280;">{_E(o['cliente'])}</td>
          <td>{af_bar}</td>
          <td>{ae_bar}</td>
          <td style="text-align:right;font-weight:700;color:{mc};">{o['mg_proy']:.1f}%</td>
          <td style="text-align:right;color:#6b7280;">{_m(o['r_tot'])}</td>
          <td style="text-align:center;font-size:1.3rem;">{o['sem_em']}</td>
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

    # ── Chart Ingresos vs Egresos por período ────────────────────────────────
    periodo_sel = (request.args.get("periodo") or "mes").strip().lower()
    if periodo_sel not in ("semana", "mes", "trimestre"):
        periodo_sel = "mes"

    from db_utils import DB_ENGINE
    _mysql = (DB_ENGINE == "mysql")

    if periodo_sel == "semana":
        fmt_sql = "DATE_FORMAT(fecha, '%Y-%u')" if _mysql else "strftime('%Y-%W', fecha)"
        fmt_lbl = "Semana"
    elif periodo_sel == "trimestre":
        fmt_sql = ("CONCAT(YEAR(fecha), '-Q', QUARTER(fecha))" if _mysql
                   else "strftime('%Y', fecha) || '-Q' || CAST((CAST(strftime('%m', fecha) AS INTEGER) + 2) / 3 AS TEXT)")
        fmt_lbl = "Trimestre"
    else:
        fmt_sql = "DATE_FORMAT(fecha, '%Y-%m')" if _mysql else "strftime('%Y-%m', fecha)"
        fmt_lbl = "Mes"

    # HH por OT por período
    hh_rows = db.execute(
        f"""SELECT ot_id, {fmt_sql} AS periodo, SUM(horas) AS hh
            FROM partes_trabajo
            WHERE fecha IS NOT NULL AND fecha != ''
            GROUP BY ot_id, periodo
            ORDER BY periodo"""
    ).fetchall()

    # Mapa de hs_previstas y precio_venta por OT
    ots_meta = {}
    for r in db.execute(
        """SELECT ot.id, COALESCE(ot.hs_previstas, 0),
                  COALESCE(ep.mat_previsto,0)+COALESCE(ep.pintura_previsto,0)+
                  COALESCE(ep.mo_previsto,0)+COALESCE(ep.consumibles_previsto,0)+
                  COALESCE(ep.ingenieria_previsto,0)+COALESCE(ep.gastos_gen_previsto,0)+
                  COALESCE(ep.impuestos_previsto,0)+COALESCE(ep.beneficio_previsto,0) AS pv,
                  ot.obra
           FROM ordenes_trabajo ot
           LEFT JOIN economico_presupuesto ep ON ep.ot_id = ot.id"""
    ).fetchall():
        ot_id_, hs_prev, pv_, obra_ = r
        cfg_ot = _get_config_obra(db, obra_ or "")
        # costos reales manuales
        rm_row = db.execute(
            "SELECT COALESCE(mat_real,0)+COALESCE(pintura_real,0)+COALESCE(subcontratos_real,0) FROM economico_costos_reales WHERE ot_id=?",
            (ot_id_,)).fetchone()
        manual_real = float(rm_row[0] or 0) if rm_row else 0.0
        # total HH de la OT
        tot_hh_row = db.execute("SELECT COALESCE(SUM(horas),0) FROM partes_trabajo WHERE ot_id=?", (ot_id_,)).fetchone()
        tot_hh = float(tot_hh_row[0] or 0) if tot_hh_row else 0.0
        ots_meta[ot_id_] = {
            "hs_prev":     float(hs_prev or 0),
            "pv":          float(pv_ or 0),
            "cfg":         cfg_ot,
            "manual_real": manual_real,
            "tot_hh":      tot_hh,
        }

    # Acumular ingresos y egresos por período
    from collections import defaultdict
    periodos_ing  = defaultdict(float)  # ingresos (valor ganado) por período
    periodos_egr  = defaultdict(float)  # egresos (costos) por período

    for ot_id_, periodo, hh_p in hh_rows:
        ot_id_ = int(ot_id_); hh_p = float(hh_p or 0)
        meta = ots_meta.get(ot_id_)
        if not meta:
            continue
        cfg_ot  = meta["cfg"]
        hs_prev = meta["hs_prev"]
        pv_ot   = meta["pv"]
        tot_hh  = meta["tot_hh"]

        # Ingresos del período = valor ganado incremental
        if hs_prev > 0 and pv_ot > 0:
            periodos_ing[periodo] += pv_ot * (hh_p / hs_prev)

        # Egresos del período = costos auto + proporción de costos manuales
        auto_cost  = hh_p * (cfg_ot["precio_hora_mo"] + cfg_ot["precio_hora_cons"])
        frac_hh    = (hh_p / tot_hh) if tot_hh > 0 else 0.0
        manual_p   = meta["manual_real"] * frac_hh
        directo_p  = auto_cost + manual_p
        gg_p       = directo_p * cfg_ot["pct_gastos_gen"] / 100.0
        imp_p      = directo_p * cfg_ot["pct_impuestos"]  / 100.0
        periodos_egr[periodo] += directo_p + gg_p + imp_p

    # Ordenar períodos y construir series acumuladas
    all_periods = sorted(set(periodos_ing.keys()) | set(periodos_egr.keys()))
    cum_ing = []; cum_egr = []; running_i = 0.0; running_e = 0.0
    for p in all_periods:
        running_i += periodos_ing.get(p, 0.0)
        running_e += periodos_egr.get(p, 0.0)
        cum_ing.append(round(running_i, 0))
        cum_egr.append(round(running_e, 0))

    chart2_labels_js = _json.dumps(all_periods)
    chart2_ing_js    = _json.dumps(cum_ing)
    chart2_egr_js    = _json.dumps(cum_egr)

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
        _kpi("Costo directo ejecutado",  _m(costo_cd), "#1e293b", "acumulado") +
        _kpi("Av. económico promedio",   f"{ae_prom:.1f}%", "#3b82f6", "sobre presupuesto") +
        _kpi("Riesgo global",            f"{riesgo_em} {riesgo_lbl}", riesgo_c, f"{n_criticos} crítico{'s' if n_criticos!=1 else ''}")
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
      <a href="/modulo/economico/certificados" style="background:#10b981;color:#fff;font-weight:700;">📜 Certificados</a>
      <a href="/modulo/economico/obras">📊 Obras / Costos</a>
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
              <th style="min-width:110px;">Av. Físico</th>
              <th style="min-width:110px;">Av. Económico</th>
              <th style="text-align:right;">Margen Proy.</th>
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

    <!-- Gráfico Ingresos vs Egresos -->
    <div class="card">
      <div class="ct" style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;">
        <span>💵 Ingresos vs Egresos acumulados — Valor ganado</span>
        <div style="display:flex;gap:6px;">
          <a href="?periodo=semana" style="font-size:.75rem;padding:3px 10px;border-radius:5px;text-decoration:none;font-weight:600;
             {'background:#6366f1;color:#fff;' if periodo_sel=='semana' else 'background:#e0e7ff;color:#4338ca;'}">Semanal</a>
          <a href="?periodo=mes" style="font-size:.75rem;padding:3px 10px;border-radius:5px;text-decoration:none;font-weight:600;
             {'background:#6366f1;color:#fff;' if periodo_sel=='mes' else 'background:#e0e7ff;color:#4338ca;'}">Mensual</a>
          <a href="?periodo=trimestre" style="font-size:.75rem;padding:3px 10px;border-radius:5px;text-decoration:none;font-weight:600;
             {'background:#6366f1;color:#fff;' if periodo_sel=='trimestre' else 'background:#e0e7ff;color:#4338ca;'}">Trimestral</a>
        </div>
      </div>
      <div style="padding:8px 16px;font-size:.75rem;color:#6b7280;">
        Ingresos = Valor ganado (precio venta × HH ejecutadas / HH previstas). Egresos = costos reales acumulados por período.
      </div>
      <div class="cb" style="position:relative;height:260px;">
        <canvas id="chartIngEgr"></canvas>
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

    // Chart 2: Ingresos vs Egresos acumulados
    const ctx2 = document.getElementById('chartIngEgr').getContext('2d');
    const ing_data = {chart2_ing_js};
    const egr_data = {chart2_egr_js};
    new Chart(ctx2, {{
      type: 'line',
      data: {{
        labels: {chart2_labels_js},
        datasets: [
          {{
            label: 'Ingresos (Valor Ganado)',
            data: ing_data,
            borderColor: 'rgba(16,185,129,1)',
            backgroundColor: 'rgba(16,185,129,0.08)',
            borderWidth: 2.5, pointRadius: 4,
            fill: true, tension: 0.3,
          }},
          {{
            label: 'Egresos (Costos Reales)',
            data: egr_data,
            borderColor: 'rgba(239,68,68,1)',
            backgroundColor: 'rgba(239,68,68,0.06)',
            borderWidth: 2.5, pointRadius: 4,
            fill: true, tension: 0.3,
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
  }})();
  </script>
</body>
</html>"""


# ═══════════════════════════════════════════════════════════════════════════════
# MÓDULO CERTIFICADOS DE AVANCE
# ═══════════════════════════════════════════════════════════════════════════════

def _ensure_schema_cert(db):
    db.execute("""
    CREATE TABLE IF NOT EXISTS certificados (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        obra        VARCHAR(255) NOT NULL,
        quincena    VARCHAR(80)  NOT NULL,
        fecha       DATE,
        created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
    )""")
    db.execute("""
    CREATE TABLE IF NOT EXISTS certificados_items (
        id                   INTEGER PRIMARY KEY AUTOINCREMENT,
        certificado_id       INTEGER NOT NULL,
        ot_id                INTEGER NOT NULL,
        descripcion          TEXT,
        pct_avance_acumulado REAL DEFAULT 0,
        updated_at           DATETIME DEFAULT CURRENT_TIMESTAMP
    )""")
    db.commit()


def _pv_ot(db, ot_id):
    """Precio de venta del presupuesto de la OT."""
    r = db.execute(
        """SELECT COALESCE(mat_previsto,0)+COALESCE(pintura_previsto,0)+COALESCE(mo_previsto,0)+
                  COALESCE(consumibles_previsto,0)+COALESCE(ingenieria_previsto,0)+
                  COALESCE(gastos_gen_previsto,0)+COALESCE(impuestos_previsto,0)+COALESCE(beneficio_previsto,0)
           FROM economico_presupuesto WHERE ot_id=?""", (ot_id,)).fetchone()
    return float(r[0] or 0) if r else 0.0


def _costo_pres_ot(db, ot_id):
    """Costo total previsto (sin beneficio) de la OT."""
    r = db.execute(
        """SELECT COALESCE(mat_previsto,0)+COALESCE(pintura_previsto,0)+COALESCE(mo_previsto,0)+
                  COALESCE(consumibles_previsto,0)+COALESCE(ingenieria_previsto,0)+
                  COALESCE(gastos_gen_previsto,0)+COALESCE(impuestos_previsto,0)
           FROM economico_presupuesto WHERE ot_id=?""", (ot_id,)).fetchone()
    return float(r[0] or 0) if r else 0.0


def _pct_ant_ot(db, obra, ot_id, exclude_id=None):
    """% acumulado del certificado inmediatamente anterior para esta OT/obra."""
    if exclude_id:
        r = db.execute(
            """SELECT ci.pct_avance_acumulado
               FROM certificados_items ci
               JOIN certificados c ON c.id = ci.certificado_id
               WHERE c.obra=? AND ci.ot_id=? AND c.id != ?
               ORDER BY c.id DESC LIMIT 1""",
            (obra, ot_id, exclude_id)).fetchone()
    else:
        r = db.execute(
            """SELECT ci.pct_avance_acumulado
               FROM certificados_items ci
               JOIN certificados c ON c.id = ci.certificado_id
               WHERE c.obra=? AND ci.ot_id=?
               ORDER BY c.id DESC LIMIT 1""",
            (obra, ot_id)).fetchone()
    return float(r[0] or 0) if r else 0.0


def _cert_kpis(db, cert_id):
    """Calcula PV y Costo para un certificado dado."""
    items = db.execute(
        "SELECT ot_id, pct_avance_acumulado FROM certificados_items WHERE certificado_id=?",
        (cert_id,)).fetchall()
    cert = db.execute("SELECT obra FROM certificados WHERE id=?", (cert_id,)).fetchone()
    obra = cert[0] if cert else ""
    pv_total = costo_total = 0.0
    for ot_id, pct in items:
        pct = float(pct or 0)
        pv_total    += _pv_ot(db, ot_id)    * pct / 100.0
        costo_total += _costo_pres_ot(db, ot_id) * pct / 100.0
    return pv_total, costo_total


# ─────────────────────────────────────────────────────────────────────────────
# RUTA: LISTA DE CERTIFICADOS
# ─────────────────────────────────────────────────────────────────────────────

@economico_bp.route("/modulo/economico/certificados")
def economico_certificados():
    db = get_db()
    try:
        _ensure_schema(db); _ensure_schema_cert(db)
    except Exception as exc_s:
        return f"<p>Error schema: {exc_s}</p>", 500

    certs = db.execute(
        "SELECT id, obra, quincena, fecha FROM certificados ORDER BY obra, id DESC"
    ).fetchall()

    # KPIs globales: último certificado por obra vs el anterior
    total_pv_act = total_costo_act = 0.0
    total_pv_ant = total_costo_ant = 0.0
    obras_con_cert = set()

    for cert_id, obra, quincena, fecha in certs:
        obras_con_cert.add(obra)

    for obra in obras_con_cert:
        certs_obra = db.execute(
            "SELECT id FROM certificados WHERE obra=? ORDER BY id DESC LIMIT 2", (obra,)
        ).fetchall()
        if certs_obra:
            pv0, c0 = _cert_kpis(db, certs_obra[0][0])
            total_pv_act    += pv0
            total_costo_act += c0
        if len(certs_obra) > 1:
            pv1, c1 = _cert_kpis(db, certs_obra[1][0])
            total_pv_ant    += pv1
            total_costo_ant += c1

    def _dsf(act, ant):
        if ant == 0: return 0.0
        return (act - ant) / ant * 100.0

    dsf_pv    = _dsf(total_pv_act, total_pv_ant)
    dsf_costo = _dsf(total_costo_act, total_costo_ant)

    # Tabla de certificados
    filas = ""
    for cert_id, obra, quincena, fecha in certs:
        pv, costo = _cert_kpis(db, cert_id)
        filas += f"""<tr>
          <td style="font-weight:700;">{_E(obra)}</td>
          <td>{_E(quincena)}</td>
          <td style="color:#6b7280;font-size:.82rem;">{_E(fecha or '-')}</td>
          <td style="text-align:right;color:#6366f1;font-weight:600;">{_m(pv)}</td>
          <td style="text-align:right;">{_m(costo)}</td>
          <td style="text-align:right;font-weight:600;color:{_cm((pv-costo)/pv*100 if pv>0 else 0)};">{_pct((pv-costo)/pv*100 if pv>0 else 0)}</td>
          <td>
            <a href="/modulo/economico/certificados/{cert_id}"
               style="font-size:.78rem;padding:4px 10px;background:#6366f1;color:#fff;border-radius:5px;text-decoration:none;">Ver / Editar</a>
          </td>
        </tr>"""

    if not filas:
        filas = '<tr><td colspan="7" style="text-align:center;color:#9ca3af;padding:28px;">Sin certificados cargados aún.</td></tr>'

    def _kpi_cert(titulo, valor, ant_lbl, ant_val, dsf, color):
        dsf_c  = '#166534' if dsf >= 0 else '#991b1b'
        dsf_bg = '#dcfce7' if dsf >= 0 else '#fee2e2'
        dsf_ic = '▲' if dsf > 0 else ('▼' if dsf < 0 else '–')
        return f"""
        <div style="background:#fff;border-radius:12px;box-shadow:0 2px 8px rgba(0,0,0,.07);
                    padding:18px 22px;flex:1;min-width:220px;border-left:4px solid {color};">
          <div style="font-size:.72rem;color:#6b7280;font-weight:700;text-transform:uppercase;">{titulo}</div>
          <div style="font-size:1.6rem;font-weight:900;color:{color};margin:8px 0 4px;">{valor}</div>
          <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
            <span style="font-size:.78rem;color:#9ca3af;">{ant_lbl}: {ant_val}</span>
            <span style="font-size:.75rem;padding:2px 8px;border-radius:999px;background:{dsf_bg};color:{dsf_c};font-weight:700;">
              {dsf_ic} {abs(dsf):.1f}% desfasaje
            </span>
          </div>
        </div>"""

    kpi_html = (
        _kpi_cert("💵 Precio de Venta certificado",
                  _m(total_pv_act), "Q anterior", _m(total_pv_ant), dsf_pv, "#6366f1") +
        _kpi_cert("🏭 Costo previsto certificado",
                  _m(total_costo_act), "Q anterior", _m(total_costo_ant), dsf_costo, "#f97316")
    )

    return f"""<!DOCTYPE html>
<html lang="es"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Certificados de Avance</title>
<style>
*{{box-sizing:border-box;}}body{{font-family:system-ui,sans-serif;background:#f1f5f9;margin:0;padding:0;}}
.hdr{{background:linear-gradient(135deg,#10b981,#059669);color:#fff;padding:16px 24px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;}}
.hdr h1{{margin:0;font-size:1.1rem;}}
.hdr a{{color:#fff;text-decoration:none;font-size:.82rem;background:rgba(255,255,255,.2);padding:6px 12px;border-radius:6px;}}
.hdr a:hover{{background:rgba(255,255,255,.35);}}
.body{{padding:20px;display:flex;flex-direction:column;gap:16px;}}
.kpi-row{{display:flex;flex-wrap:wrap;gap:14px;}}
.card{{background:#fff;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,.07);overflow:hidden;}}
.ct{{background:#f8fafc;border-bottom:1px solid #e5e7eb;padding:11px 16px;font-weight:700;font-size:.9rem;color:#1e293b;display:flex;align-items:center;justify-content:space-between;}}
table{{width:100%;border-collapse:collapse;font-size:.85rem;}}
th{{background:#10b981;color:#fff;padding:9px 12px;text-align:left;font-size:.8rem;}}
td{{padding:8px 12px;border-bottom:1px solid #f1f5f9;vertical-align:middle;}}
tr:hover td{{background:#f0fdf4;}}
</style></head><body>
<div class="hdr">
  <div>
    <h1>📜 Certificados de Avance de Obra</h1>
    <div style="font-size:.76rem;opacity:.8;margin-top:2px;">Certificaciones por quincena · PV y Costo por avance</div>
  </div>
  <div style="display:flex;gap:7px;flex-wrap:wrap;">
    <a href="/modulo/economico/certificados/nueva" style="background:rgba(255,255,255,.35);font-weight:700;">+ Nueva Quincena</a>
    <a href="/modulo/economico/dashboard-ejecutivo">← Dashboard</a>
    <a href="/">Inicio</a>
  </div>
</div>
<div class="body">
  <div class="kpi-row">{kpi_html}</div>
  <div class="card">
    <div class="ct">
      <span>📋 Historial de certificados</span>
    </div>
    <div style="overflow-x:auto;">
      <table>
        <thead><tr>
          <th>Obra</th><th>Quincena</th><th>Fecha</th>
          <th style="text-align:right;">PV Certificado</th>
          <th style="text-align:right;">Costo Prev.</th>
          <th style="text-align:right;">Margen</th>
          <th></th>
        </tr></thead>
        <tbody>{filas}</tbody>
      </table>
    </div>
  </div>
</div></body></html>"""


# ─────────────────────────────────────────────────────────────────────────────
# RUTA: NUEVA QUINCENA / EDITAR CERTIFICADO
# ─────────────────────────────────────────────────────────────────────────────

@economico_bp.route("/modulo/economico/certificados/nueva", methods=["GET", "POST"])
def economico_certificados_nueva():
    db = get_db()
    try:
        _ensure_schema(db)
        _ensure_schema_cert(db)
    except Exception as exc_schema:
        import traceback
        return f"<p>Error al inicializar tablas: {exc_schema}</p><pre>{traceback.format_exc()}</pre>", 500
    mensaje = error = ""

    if request.method == "POST" and (request.form.get("accion") or "") == "crear_cert":
        obra     = (request.form.get("obra") or "").strip()
        quincena = (request.form.get("quincena") or "").strip()
        fecha    = (request.form.get("fecha") or "").strip() or None
        if not obra or not quincena:
            error = "Debe indicar la obra y la quincena."
        else:
            try:
                cur = db.execute(
                    "INSERT INTO certificados (obra, quincena, fecha) VALUES (?,?,?)",
                    (obra, quincena, fecha))
                db.commit()
                cert_id = cur.lastrowid
                if not cert_id:
                    # Fallback para MySQL: obtener el ID recién insertado
                    row = db.execute("SELECT MAX(id) FROM certificados WHERE obra=? AND quincena=?",
                                     (obra, quincena)).fetchone()
                    cert_id = int(row[0]) if row and row[0] else 1
                return redirect(f"/modulo/economico/certificados/{cert_id}")
            except Exception as exc:
                error = str(exc)

    # Obras disponibles
    obras = db.execute(
        "SELECT DISTINCT obra FROM ordenes_trabajo WHERE obra IS NOT NULL AND obra != '' ORDER BY obra"
    ).fetchall()
    obras_opts = "".join(f'<option value="{_E(r[0])}">{_E(r[0])}</option>' for r in obras)

    # Sugerencia de nombre de quincena
    from datetime import date
    hoy = date.today()
    q_sug = f"{hoy.strftime('%b %Y')} - {'1ra' if hoy.day <= 15 else '2da'}"

    msg_html = (f'<div style="background:#fee2e2;color:#991b1b;border:1px solid #fca5a5;border-radius:6px;padding:8px 12px;margin-bottom:12px;">{_E(error)}</div>'
                if error else "")

    return f"""<!DOCTYPE html>
<html lang="es"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Nueva Quincena</title>
<style>
*{{box-sizing:border-box;}}body{{font-family:system-ui,sans-serif;background:#f1f5f9;margin:0;padding:24px;}}
.card{{background:#fff;border-radius:12px;box-shadow:0 2px 10px rgba(0,0,0,.08);padding:28px;max-width:520px;margin:auto;}}
h2{{margin:0 0 20px;color:#1e293b;}}
.fg{{margin-bottom:16px;}}label{{display:block;font-size:.82rem;font-weight:600;color:#374151;margin-bottom:3px;}}
select,input{{width:100%;padding:8px 10px;border:1px solid #d1d5db;border-radius:6px;font-size:.9rem;}}
.btn{{background:#10b981;color:#fff;border:none;padding:10px 22px;border-radius:7px;font-size:.9rem;cursor:pointer;font-weight:700;}}
.back{{display:inline-block;margin-bottom:16px;color:#10b981;text-decoration:none;font-size:.88rem;font-weight:600;}}
</style></head><body>
<a href="/modulo/economico/certificados" class="back">← Volver a certificados</a>
<div class="card">
  <h2>📜 Nueva Quincena</h2>
  {msg_html}
  <form method="post">
    <input type="hidden" name="accion" value="crear_cert">
    <div class="fg"><label>Obra</label><select name="obra"><option value="">-- Seleccionar obra --</option>{obras_opts}</select></div>
    <div class="fg"><label>Quincena (etiqueta)</label><input name="quincena" value="{_E(q_sug)}" required></div>
    <div class="fg"><label>Fecha</label><input type="date" name="fecha" value="{date.today().isoformat()}"></div>
    <button type="submit" class="btn">Crear y cargar avance →</button>
  </form>
</div></body></html>"""


# ─────────────────────────────────────────────────────────────────────────────
# RUTA: DETALLE / EDICIÓN DE CERTIFICADO
# ─────────────────────────────────────────────────────────────────────────────

@economico_bp.route("/modulo/economico/certificados/<int:cert_id>", methods=["GET", "POST"])
def economico_certificado_detalle(cert_id):
    db = get_db()
    try:
        _ensure_schema(db); _ensure_schema_cert(db)
    except Exception as exc_s:
        return f"<p>Error schema: {exc_s}</p>", 500

    cert = db.execute(
        "SELECT id, obra, quincena, fecha FROM certificados WHERE id=?", (cert_id,)
    ).fetchone()
    if not cert:
        return "Certificado no encontrado", 404
    _, obra, quincena, fecha = cert

    mensaje = error = ""

    if request.method == "POST" and (request.form.get("accion") or "") == "guardar":
        try:
            ot_ids = request.form.getlist("ot_id")
            for ot_id_s in ot_ids:
                if not ot_id_s.isdigit():
                    continue
                ot_id = int(ot_id_s)
                desc  = (request.form.get(f"desc_{ot_id}") or "").strip()
                pct_s = (request.form.get(f"pct_{ot_id}") or "0").strip()
                pct   = min(max(float(pct_s), 0.0), 100.0)
                ex = db.execute(
                    "SELECT id FROM certificados_items WHERE certificado_id=? AND ot_id=?",
                    (cert_id, ot_id)).fetchone()
                if ex:
                    db.execute(
                        "UPDATE certificados_items SET descripcion=?, pct_avance_acumulado=?, updated_at=CURRENT_TIMESTAMP WHERE id=?",
                        (desc, pct, ex[0]))
                else:
                    db.execute(
                        "INSERT INTO certificados_items (certificado_id, ot_id, descripcion, pct_avance_acumulado) VALUES (?,?,?,?)",
                        (cert_id, ot_id, desc, pct))
            db.commit()
            mensaje = "Certificado guardado."
        except Exception as exc:
            error = str(exc)

    # OTs de la obra
    ots = db.execute(
        """SELECT id, titulo, tipo_estructura FROM ordenes_trabajo
           WHERE obra=? ORDER BY id""", (obra,)
    ).fetchall()

    # Items existentes
    items_map = {}
    for row in db.execute(
        "SELECT ot_id, descripcion, pct_avance_acumulado FROM certificados_items WHERE certificado_id=?",
        (cert_id,)
    ).fetchall():
        items_map[int(row[0])] = {"desc": row[1] or "", "pct": float(row[2] or 0)}

    # Tabla de OTs
    rows_html = ""
    pv_q_total = costo_q_total = pv_ant_total = costo_ant_total = 0.0
    for ot_id, titulo, tipo in ots:
        pct_acc  = items_map.get(ot_id, {}).get("pct", 0.0)
        pct_ant  = _pct_ant_ot(db, obra, ot_id, exclude_id=cert_id)
        pct_act  = max(pct_acc - pct_ant, 0.0)
        desc_val = items_map.get(ot_id, {}).get("desc", titulo or "")
        pv       = _pv_ot(db, ot_id)
        costo    = _costo_pres_ot(db, ot_id)

        pv_q_total    += pv    * pct_acc / 100.0
        costo_q_total += costo * pct_acc / 100.0
        pv_ant_total  += pv    * pct_ant / 100.0
        costo_ant_total += costo * pct_ant / 100.0

        rows_html += f"""<tr>
          <td style="font-weight:700;color:#6366f1;">OT {ot_id}</td>
          <td><input name="desc_{ot_id}" value="{_E(desc_val)}"
                style="width:100%;padding:5px 8px;border:1px solid #e5e7eb;border-radius:4px;font-size:.82rem;"></td>
          <td style="text-align:center;color:#6b7280;font-size:.85rem;">{pct_ant:.1f}%</td>
          <td id="q_act_{ot_id}" style="text-align:center;font-weight:600;color:#0891b2;font-size:.85rem;">{pct_act:.1f}%</td>
          <td style="text-align:center;">
            <input type="number" name="pct_{ot_id}" value="{pct_acc:.1f}" min="0" max="100" step="0.1"
                   oninput="updQAct({ot_id}, {pct_ant:.2f}, this.value)"
                   style="width:80px;padding:5px 8px;border:1.5px solid #6366f1;border-radius:5px;font-size:.88rem;text-align:center;font-weight:700;">
          </td>
          <td style="text-align:right;font-size:.8rem;color:#6366f1;">{_m(pv)}</td>
          <input type="hidden" name="ot_id" value="{ot_id}">
        </tr>"""

    # KPIs del certificado
    def _kc2(titulo, valor, sub_lbl, sub_val, dsf_val, color):
        dsf_c  = '#166534' if dsf_val >= 0 else '#991b1b'
        dsf_bg = '#dcfce7' if dsf_val >= 0 else '#fee2e2'
        dsf_ic = '▲' if dsf_val > 0 else ('▼' if dsf_val < 0 else '–')
        return f"""<div style="background:#fff;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,.07);
                              padding:16px 20px;flex:1;min-width:200px;border-left:4px solid {color};">
          <div style="font-size:.7rem;color:#6b7280;font-weight:700;text-transform:uppercase;">{titulo}</div>
          <div style="font-size:1.4rem;font-weight:900;color:{color};margin:6px 0 4px;">{valor}</div>
          <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap;">
            <span style="font-size:.76rem;color:#9ca3af;">{sub_lbl}: {sub_val}</span>
            <span style="font-size:.72rem;padding:2px 7px;border-radius:999px;background:{dsf_bg};color:{dsf_c};font-weight:700;">
              {dsf_ic} {abs(dsf_val):.1f}% desfasaje
            </span>
          </div>
        </div>"""

    def _dsf2(act, ant):
        return (act - ant) / ant * 100.0 if ant > 0 else 0.0

    kpi_html = (
        _kc2("💵 Precio de Venta certificado",
             _m(pv_q_total), "Q anterior", _m(pv_ant_total),
             _dsf2(pv_q_total, pv_ant_total), "#6366f1") +
        _kc2("🏭 Costo previsto certificado",
             _m(costo_q_total), "Q anterior", _m(costo_ant_total),
             _dsf2(costo_q_total, costo_ant_total), "#f97316")
    )

    msg_html = ""
    if mensaje:
        msg_html = f'<div style="background:#dcfce7;color:#166534;border:1px solid #86efac;border-radius:6px;padding:8px 12px;margin-bottom:12px;">{_E(mensaje)}</div>'
    if error:
        msg_html += f'<div style="background:#fee2e2;color:#991b1b;border:1px solid #fca5a5;border-radius:6px;padding:8px 12px;margin-bottom:12px;">{_E(error)}</div>'

    return f"""<!DOCTYPE html>
<html lang="es"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Certificado — {_E(obra)} · {_E(quincena)}</title>
<style>
*{{box-sizing:border-box;}}body{{font-family:system-ui,sans-serif;background:#f1f5f9;margin:0;padding:0;}}
.hdr{{background:linear-gradient(135deg,#10b981,#059669);color:#fff;padding:16px 24px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px;}}
.hdr h1{{margin:0;font-size:1rem;}}
.hdr a{{color:#fff;text-decoration:none;font-size:.8rem;background:rgba(255,255,255,.2);padding:5px 10px;border-radius:6px;}}
.body{{padding:18px;display:flex;flex-direction:column;gap:14px;}}
.kpi-row{{display:flex;flex-wrap:wrap;gap:12px;}}
.card{{background:#fff;border-radius:10px;box-shadow:0 2px 8px rgba(0,0,0,.07);overflow:hidden;}}
.ct{{background:#f8fafc;border-bottom:1px solid #e5e7eb;padding:11px 16px;font-weight:700;font-size:.9rem;color:#1e293b;}}
table{{width:100%;border-collapse:collapse;font-size:.84rem;}}
th{{background:#10b981;color:#fff;padding:9px 12px;text-align:left;font-size:.79rem;white-space:nowrap;}}
td{{padding:8px 12px;border-bottom:1px solid #f1f5f9;vertical-align:middle;}}
tr:hover td{{background:#f0fdf4;}}
.btn{{background:#10b981;color:#fff;border:none;padding:10px 22px;border-radius:7px;font-size:.9rem;cursor:pointer;font-weight:700;}}
.btn:hover{{background:#059669;}}
</style></head><body>
<div class="hdr">
  <div>
    <h1>📜 {_E(obra)} · {_E(quincena)}</h1>
    <div style="font-size:.74rem;opacity:.75;margin-top:2px;">{_E(fecha or '')}</div>
  </div>
  <div style="display:flex;gap:6px;flex-wrap:wrap;">
    <a href="/modulo/economico/certificados">← Certificados</a>
    <a href="/modulo/economico/dashboard-ejecutivo">Dashboard</a>
  </div>
</div>
<div class="body">
  {msg_html}
  <div class="kpi-row">{kpi_html}</div>
  <div class="card">
    <div class="ct">📋 Avance por OT — <span style="color:#10b981;">{_E(obra)}</span> · {_E(quincena)}</div>
    <form method="post">
      <input type="hidden" name="accion" value="guardar">
      <div style="overflow-x:auto;">
        <table>
          <thead><tr>
            <th>OT</th>
            <th>Descripción</th>
            <th style="text-align:center;min-width:110px;">% Av. Q Anterior</th>
            <th style="text-align:center;min-width:110px;">% Av. Q Actual</th>
            <th style="text-align:center;min-width:120px;">% Av. Acumulado ✏️</th>
            <th style="text-align:right;">PV OT</th>
          </tr></thead>
          <tbody>{rows_html}</tbody>
        </table>
      </div>
      <div style="padding:14px 16px;display:flex;justify-content:flex-end;">
        <button type="submit" class="btn">💾 Guardar certificado</button>
      </div>
    </form>
  </div>
</div>
<script>
function updQAct(ot_id, pct_ant, val) {{
  var act = Math.max(parseFloat(val)||0, 0) - pct_ant;
  act = Math.max(act, 0).toFixed(1);
  var el = document.getElementById('q_act_' + ot_id);
  if (el) el.textContent = act + '%';
}}
</script>
</body></html>"""
