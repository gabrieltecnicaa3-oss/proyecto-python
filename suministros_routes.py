"""
Módulo Suministros / Compras — Flujo completo
──────────────────────────────────────────────
Flujo de estados:
  OP: Pendiente → Pedido Precios → Con OC → Cerrada / Cancelada
  OC: Envio OC → Para Despachar → Recibido OK / Recibido Parcial

Entidades:
  articulos_sum   — catálogo/lista de materiales
  ordenes_pedido  — pedido interno (OP) con ítems y criticidad
  items_op        — ítems de cada OP
  ordenes_compra  — orden de compra formal (OC) generada por compras
  items_oc        — ítems de la OC con precio unitario y cantidad recibida
"""

from flask import Blueprint, request, redirect
from db_utils import get_db
import html as _html
from datetime import date
import os
import csv
import io

try:
    from articulos_seed import ARTICULOS_SEED
except Exception:
    ARTICULOS_SEED = []

try:
    import openpyxl
    _HAS_OPENPYXL = True
except ImportError:
    _HAS_OPENPYXL = False

_APP_DIR = os.path.dirname(os.path.abspath(__file__))

suministros_bp = Blueprint("suministros", __name__, url_prefix="/modulo/suministros")

CRITICIDADES = ["Normal", "Alta", "Urgente"]
ESTADOS_OP   = ["Pendiente", "Aprobada", "Pedido Precios", "Con OC", "Cerrada", "Cancelada"]
ESTADOS_OC   = ["Envio OC", "OC confirmada", "Para Despachar", "Recibido Parcial", "Recibido OK", "Pagado"]
SUPERVISORES = ["Gabriel Ibarra", "Carlos Rodríguez", "María González", "Pablo Martínez", "Fernando García", "Daniel Pérez"]  # editar con nombres reales

PROVEEDORES_EXCEL = [
    "MESS S.A.",
    "MYCROS S.R.L.",
    "CONSTRUCCIONES SCG 1887 S. R. L. (CEPSA)",
    "G.C. INDUSTRIAL S.R.L.",
    "ADRIAN ESTEBAN MARTINELLI",
    "DIEGO EDUARDO MOREIRA",
    "PABLO ADALBERTO VICENTE",
    "B.N. INGENIERIA INFORMATICA S.R.L. (Hernan Tome)",
    "FABRICA DE PILETAS Y MESADAS DE ACERO INOXIDABLE GAMBINI S R L",
    "INOXIDABLE E INFRAESTRUCTURA SRL (ex Rosario inoxidable)",
    "L&H INGENIEROS S.R.L. (matafuegos y arnes JM)",
    "TESSER Y TESSER S A",
    "FABIAN MARCELO TAVELLA (Firextintores)",
    "METALURGICA FOREST SOCIEDAD DE RESPONSABILIDAD LIMITADA",
    "ACTIS CAPORALE CRISTIAN, ACTIS CAPORALE FABIO Y ACTIS CAPORALE MARISOL S.H.",
    "DADAMO ROBERTO RODOLFO",
    "VICTORIO ALTOBELLO",
    "ZING-TECH S.R.L.",
    "ORTIZ FISCHER Y CIA S.A.",
    "CHIPPED S.R.L.",
    "CLAUDIO RODRIGO BARCIA (CASABARCIA)",
    "FERRETERIA INDUSTRIAL LOPEZ FORCINITI SOCIEDAD  AN",
    "INDUFER SA",
    "MIGUEL ANGEL CANALIS (Ferre del Soldador)",
    "MONTARFE S R L (FERRETERIA - PTO VENTA 7-8-9)",
    "SERVICE VIAL S A",
    "HILTI ARGENTINA S.R.L",
    "ACINDAR INDUSTRIA ARGENTINA DE ACEROS S A",
    "BERARDI Y COMPAÑIA",
    "CDSA S.A",
    "D'ALESSIO HIERROS SRL",
    "SIDERCO S. A.",
    "SIPAR ACEROS S.A.",
    "ECHEVERRIA MARIANO GERMAN",
    "MADERAS AMIANO S.A.",
    "MADERERA JC S.A.S. (Cordoba)",
    "MADERISA S A C I F I A",
    "CORTADORAS ARGENTINAS SA (Argencort)",
    "ACCESANIGA SA",
    "BALCARCE 54 S.A.",
    "MACCAFERRI DE ARGENTINA SOCIEDAD ANONIMA",
    "SERVITUBOS S.R.L. LUZZI",
    "TODOPOR S.R.L.",
    "CCLV Materiales SA",
    "HIPERMERCADO DE LA CONSTRUCCION S.R.L. (Labrador)",
    "LA CASA DE LA CONSTRUCCION S.R.L. (material de Corralon)",
    "MESSINEO VICENTE OSCAR",
    "TERRALON S.R.L. (LDC JM)",
    "ZAYA JORGE AGUSTIN (Cordoba)",
    "ARGELEC CENTRO S.R.L.",
    "ELECTRINET SRL",
    "INGENIERIA ELECTRICA S A",
    "LA YESERA ROSARINA  S R L",
    "ACEROS COCO S.A.",
    "ACEROS CUFER  S. A.",
    "CENTRO DE CHAPAS ROSARIO S A",
    "CORMETAL S A",
    "DICO ACEROS S R L",
    "ELEM S.A. (Cordoba)",
    "ESTABLECIMIENTOS CHIAZA S A",
    "GALEA S.R.L.",
    "INDUSTRIA SEGHIMET SA",
    "INGENIERIA PRIDA HILBING SRL",
    "ORLANDI INDUSTRIAL Y COMERCIAL S.A.",
    "PERFORMA S.A.",
    "PLEMETAL SRL",
    "ROGIRO ACEROS S A",
    "FIJAR SOLUCIONES S.R.L.",
    "LARRAYA BULONES S.R.L.",
    "BOREAL PINTURAS S.A.",
    "CP S.A (Cañada Pinturerias)",
    "HECAN (ROSARIO COLOR)",
    "INDUSTRIAS QUIMICAS B.G. SRL. (THAXOL)",
    "SISTEMAS DE PINTADO S.R.L.",
    "SUC. DE LOPEZ ROSA BEATRIZ",
    "COMPAÑIA AMERICANA SRL",
    "DAVID DARIO PELLEGRINI (PROTEX - KAUFER)",
    "MATERIALES FUNES  S. R. L.",
    "FAMIQ S.R.L.",
    "NESTOR JAVIER MORILLAS (PROSERMET)",
    "KRAH AMERICA LATINA S A (JM)",
    "CHICAGO Blower Argentina S.A.",
    "CS INGENIERIA S.R.L.",
    "JOEL PEREYRA (LA INDUSTRIAL - TALLER A3)",
    "EMBOTELLADORA LA NICOLEÑA S.A. (Bunge SJ)",
    "LUCIANO JAVIER MARTIN (CIMES) - Taller A3",
    "TRAVERSO GERMAN ANDRES/MARCELO FABIAN S H (AGUA MAR AZUL EN GERDAU)",
    "CARNEVALI MARCHI SRL",
    "LH SERVICIOS INDUSTIALES SRL",
    "HUGO SANCHEZ  S A (Sullair Arg)",
    "CANTARUTTI RUBEN DARIO",
    "CD CONSTRUCCIONES SRL",
    "LEITEN S. A.",
    "NUCOR S. A. S.",
    "SINIS S. A. (tiene certif. de NO RETENCION)",
    "SORRENTO MAQUINARIAS SRL",
    "MONTARFE S R L (SERVICIOS HIDRO)",
    "PELTEC SA",
    "RENTAL SUR SRL",
    "TRANSPORTE MC SOCIEDAD SIMPLE S. CAP I SECC IV",
    "TRANSPORTE Y SERVICIOS SAWCZUCK SOCIEDAD ANONIMA",
    "ELECTROGER GASES S.R.L.",
    "CLEANING WORK S.A.S",
    "HORVEZ S.R.L",
    "LA CASA DE LA CONSTRUCCION S.R.L. (volquete)",
    "AC INGENIERIA SOCIEDAD POR ACCIONES SIMPLIFICADA S. A. S. (CESAR CAGLIERO)",
    "MAULION SANTIAGO (Carpintero)",
    "PROOFLINE SOCIEDAD ANONIMA",
    "FERNANDO MANGIANTINI",
    "FRANCO MOLINA (Ingenieria) certificado",
    "GERARDO WALTER TOSORATTI (certificado)",
    "HERNAN GONZALO GUTIERREZ",
    "INGEN SA (JOSE ORENGO)",
    "Know How ING Y CONSTRUCCIONES SRL",
    "MARTIN LUCAS CENTENARO",
    "RAUL OSVALDO ZAMBONI",
    "RICARDO JAVIER CUTULI",
    "ROBERTO MARCELO GATTO",
    "ROUCO PABLO",
    "TEGLIA RAMIRO EZEQUIEL (Fabricacion de escaleras, rejas, albañales en acero)",
    "BRUKE S.A.",
    "LAMORSAN SA",
    "MIRTA SUSANA ROBLEDO",
    "EASY PAQ EN FORMACION S. A.",
    "EMANUEL MAXIMILIANO RODRIGUEZ (MSR)",
    "JORGE ALBERTO CROVARA",
    "RAIMUNDO ESTANISLAO AGUILAR",
    "RODRIGO FABIAN WIRSCH",
    "DRUETTA HNOS  SA",
    "GTM S.R.L. Servicios Industriales",
    "CESAR LUIS CACZURAK",
    "DUTRA SERGIO DAVID",
    "FELCARO WALTER ANGEL",
    "NESTOR DAVID FERNANDEZ",
    "DAWI SOCIEDAD RESPONSABILIDAD LIMITADA (carrocerías, remolques, semirremolques, acoplados)",
    "ELVETE",
    "MAXIMILIANO RENE TURNATURI",
]

# ═══════════════════════════ TABLAS ════════════════════════════

def _ensure_tables(db):
    db.execute("""CREATE TABLE IF NOT EXISTS articulos_sum (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        codigo TEXT, descripcion TEXT NOT NULL,
        unidad TEXT DEFAULT 'u', categoria TEXT, activo INTEGER DEFAULT 1)""")
    db.execute("""CREATE TABLE IF NOT EXISTS ordenes_pedido (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero TEXT, fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
        solicitante TEXT NOT NULL, obra TEXT, sector TEXT,
        criticidad TEXT DEFAULT 'Normal', observaciones TEXT,
        estado TEXT DEFAULT 'Pendiente')""")
    db.execute("""CREATE TABLE IF NOT EXISTS items_op (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        op_id INTEGER NOT NULL, articulo_id INTEGER,
        descripcion TEXT NOT NULL, cantidad REAL NOT NULL,
        unidad TEXT DEFAULT 'u', fecha_necesaria DATE,
        criticidad TEXT DEFAULT 'Normal', observaciones TEXT)""")
    db.execute("""CREATE TABLE IF NOT EXISTS ordenes_compra (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        numero TEXT, op_id INTEGER NOT NULL,
        fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
        proveedor TEXT NOT NULL, condiciones_pago TEXT,
        plazo_entrega TEXT, observaciones TEXT,
        estado TEXT DEFAULT 'Envio OC',
        fecha_despacho DATE, fecha_recepcion DATE,
        remito_proveedor TEXT)""")
    # Agregar columna remito_proveedor si no existe (migracion)
    try:
        db.execute("ALTER TABLE ordenes_compra ADD COLUMN remito_proveedor TEXT")
    except Exception:
        pass
    # Migraciones items_op: largo y peso
    try:
        db.execute("ALTER TABLE articulos_sum ADD COLUMN kg_per_m REAL")
    except Exception:
        pass
    try:
        db.execute("ALTER TABLE items_op ADD COLUMN largo REAL DEFAULT 6.0")
    except Exception:
        pass
    try:
        db.execute("ALTER TABLE items_op ADD COLUMN peso_kg REAL")
    except Exception:
        pass
    # Migraciones OC: moneda y unidad_precio
    try:
        db.execute("ALTER TABLE ordenes_compra ADD COLUMN moneda TEXT")
    except Exception:
        pass
    try:
        db.execute("ALTER TABLE items_oc ADD COLUMN unidad_precio TEXT")
    except Exception:
        pass
    try:
        db.execute("ALTER TABLE items_oc ADD COLUMN articulo_id INTEGER")
    except Exception:
        pass
    try:
        db.execute("ALTER TABLE ordenes_compra ADD COLUMN responsable_recepcion TEXT")
    except Exception:
        pass
    try:
        db.execute("ALTER TABLE items_oc ADD COLUMN largo REAL")
    except Exception:
        pass
    try:
        db.execute("ALTER TABLE items_oc ADD COLUMN peso_kg REAL")
    except Exception:
        pass
    try:
        db.execute("ALTER TABLE ordenes_compra ADD COLUMN fecha_pago TEXT")
    except Exception:
        pass
    # Migración: columna codigo en articulos_sum (para tablas existentes en producción)
    try:
        db.execute("ALTER TABLE articulos_sum ADD COLUMN codigo TEXT")
    except Exception:
        pass
    # Migración: control de stock
    try:
        db.execute("ALTER TABLE items_oc ADD COLUMN estado_stock TEXT DEFAULT 'Pendiente'")
    except Exception:
        pass
    try:
        db.execute("UPDATE items_oc SET estado_stock='Pendiente' WHERE estado_stock IS NULL OR estado_stock=''")
    except Exception:
        pass
    # Migrar estado 'En aprobación' → 'Aprobada' en registros existentes
    db.execute("UPDATE ordenes_pedido SET estado='Aprobada' WHERE estado='En aprobaci\u00f3n'")
    db.execute("""CREATE TABLE IF NOT EXISTS items_oc (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        oc_id INTEGER NOT NULL, descripcion TEXT NOT NULL,
        cantidad REAL NOT NULL, unidad TEXT DEFAULT 'u',
        precio_unitario REAL DEFAULT 0, cantidad_recibida REAL DEFAULT 0,
        estado_stock TEXT DEFAULT 'Pendiente')""")
    # Reintentar migración de estado_stock después de CREATE TABLE para bases nuevas
    try:
        db.execute("ALTER TABLE items_oc ADD COLUMN estado_stock TEXT DEFAULT 'Pendiente'")
    except Exception:
        pass
    try:
        db.execute("UPDATE items_oc SET estado_stock='Pendiente' WHERE estado_stock IS NULL OR estado_stock=''")
    except Exception:
        pass
    # Tablas legacy (backward compat)
    db.execute("""CREATE TABLE IF NOT EXISTS solicitudes_compra (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        fecha_solicitud DATETIME DEFAULT CURRENT_TIMESTAMP,
        solicitante TEXT NOT NULL, obra TEXT, sector TEXT,
        prioridad TEXT DEFAULT 'Media', estado TEXT DEFAULT 'Pendiente',
        observaciones TEXT)""")
    db.execute("""CREATE TABLE IF NOT EXISTS items_solicitud (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        solicitud_id INTEGER NOT NULL, descripcion TEXT NOT NULL,
        cantidad REAL NOT NULL, unidad TEXT, proveedor_sugerido TEXT,
        fecha_necesaria DATE, estado_item TEXT DEFAULT 'Pendiente',
        observaciones TEXT)""")
    db.execute("""CREATE TABLE IF NOT EXISTS proveedores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        cuit TEXT,
        telefono TEXT,
        email TEXT,
        contacto TEXT,
        observaciones TEXT,
        activo INTEGER DEFAULT 1)"""
    )
    db.execute("""CREATE TABLE IF NOT EXISTS proveedores (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        cuit TEXT,
        telefono TEXT,
        email TEXT,
        contacto TEXT,
        observaciones TEXT,
        activo INTEGER DEFAULT 1)"""
    )
    for sql in [
        "CREATE INDEX IF NOT EXISTS idx_iop ON items_op(op_id)",
        "CREATE INDEX IF NOT EXISTS idx_ioc ON items_oc(oc_id)",
    ]:
        try:
            db.execute(sql)
        except Exception:
            pass
    # Seed/sync proveedores: re-insertar si el conteo no coincide con la lista maestra
    _prov_count = db.execute("SELECT COUNT(*) FROM proveedores").fetchone()[0]
    if _prov_count != len(PROVEEDORES_EXCEL):
        db.execute("DELETE FROM proveedores")
        for _n in PROVEEDORES_EXCEL:
            db.execute("INSERT INTO proveedores (nombre, activo) VALUES (?,1)", (_n,))
    # Auto-seed articulos_sum si la tabla está vacía (primera vez en producción MySQL)
    if ARTICULOS_SEED:
        _art_count = db.execute("SELECT COUNT(*) FROM articulos_sum").fetchone()[0]
        if _art_count == 0:
            for _cod, _desc, _unid, _cat, _act, _kg in ARTICULOS_SEED:
                try:
                    db.execute(
                        "INSERT INTO articulos_sum (codigo,descripcion,unidad,categoria,activo,kg_per_m) VALUES (?,?,?,?,?,?)",
                        (_cod, _desc, _unid, _cat, _act, _kg))
                except Exception:
                    pass
    db.commit()

# ═══════════════════════════ HELPERS ═══════════════════════════

def _e(s):
    return _html.escape(str(s or ""))

def _fmt(v):
    try:
        f = float(v or 0)
        return str(int(f)) if f == int(f) else "{:.2f}".format(f).rstrip("0").rstrip(".")
    except Exception:
        return "0"

_BC = {
    "Pendiente":        ("#fef9c3", "#854d0e"),
    "Aprobada":         ("#fce7f3", "#9d174d"),
    "Pedido Precios":   ("#dbeafe", "#1e40af"),
    "Con OC":           ("#e0e7ff", "#3730a3"),
    "Cerrada":          ("#dcfce7", "#166534"),
    "Cancelada":        ("#fee2e2", "#991b1b"),
    "Envio OC":         ("#e0e7ff", "#3730a3"),
    "OC confirmada":    ("#ccfbf1", "#0f766e"),
    "Para Despachar":   ("#fef3c7", "#92400e"),
    "Recibido Parcial": ("#ffe4e6", "#9f1239"),
    "Recibido OK":      ("#dcfce7", "#166534"),
    "Pagado":           ("#d1fae5", "#065f46"),
}

def _badge(estado):
    bg, fg = _BC.get(str(estado), ("#f1f5f9", "#334155"))
    return ('<span style="background:{bg};color:{fg};padding:3px 11px;'
            'border-radius:999px;font-size:12px;font-weight:700">{e}</span>').format(
        bg=bg, fg=fg, e=_e(estado))

_CSS = """<meta name="viewport" content="width=device-width,initial-scale=1"><style>
*{box-sizing:border-box}
body{font-family:Arial,sans-serif;background:#fff7ed;margin:0;padding:16px;color:#1c0a00}
.w{max-width:1200px;margin:0 auto} h2,h3{margin-top:0;color:#7c2d12}
a.b,button.b{display:inline-block;text-decoration:none;padding:9px 14px;border-radius:8px;
    font-size:14px;font-weight:600;margin-right:6px;margin-bottom:6px;border:none;cursor:pointer;color:#fff}
.sm{padding:5px 10px!important;font-size:12px!important}
.or{background:#f97316} .or:hover{background:#ea580c}
.bl{background:#2563eb} .gr{background:#4b5563}
.am{background:#d97706} .gn{background:#16a34a} .rd{background:#dc2626} .pu{background:#7c3aed}
.tl{background:#f97316}
.card{background:#fff;border:1px solid #fed7aa;border-radius:10px;padding:14px;margin-bottom:12px;
    box-shadow:0 1px 4px rgba(249,115,22,.08)}
.kgr{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:10px;margin-bottom:14px}
.kn{font-size:28px;font-weight:800;margin-top:4px;color:#ea580c}
.fg{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:8px}
table{width:100%;border-collapse:collapse}
th,td{padding:9px 10px;border-bottom:1px solid #fed7aa;text-align:left;font-size:14px}
th{background:#fff7ed;font-weight:700;color:#9a3412;border-bottom:2px solid #fb923c}
input,select,textarea{padding:8px;border:1px solid #fdba74;border-radius:7px;width:100%;font-size:14px;background:#fff}
input:focus,select:focus,textarea:focus{outline:none;border-color:#f97316;box-shadow:0 0 0 2px rgba(249,115,22,.2)}
.err{background:#fee2e2;color:#991b1b;padding:10px;border-radius:8px;margin-bottom:10px;border:1px solid #fecaca}
.hl tr:hover td{background:#fff7ed}
.top-nav{background:#fff;border:1px solid #fed7aa;border-radius:10px;padding:10px 14px;margin-bottom:14px}
/* --- DASHBOARD BUTTONS ESTILO IMAGEN --- */
.main-btns{display:flex;gap:32px;justify-content:center;margin-bottom:18px;margin-top:10px}
/* --- DASHBOARD BUTTONS ESTILO IMAGEN --- */
.main-btn-card{background:#fff;border:2px solid #fb923c;border-radius:18px;box-shadow:0 2px 12px rgba(249,115,22,0.10);padding:28px 22px 22px 22px;display:flex;flex-direction:column;align-items:center;justify-content:center;width:260px;min-height:210px;transition:box-shadow .18s,border .18s;position:relative;text-decoration:none}
.main-btn-card:hover{box-shadow:0 4px 24px rgba(249,115,22,0.18);border-color:#f97316;text-decoration:none}
.main-btn-card .icon{font-size:48px;margin-bottom:10px}
.main-btn-card .title{font-size:22px;font-weight:800;color:#b45309;margin-bottom:6px;text-align:center}
.main-btn-card .desc{font-size:15px;color:#7c2d12;text-align:center}
.volver-btn{position:absolute;top:18px;right:18px;background:#fff;color:#b45309;border:1.5px solid #fb923c;padding:7px 16px;font-size:15px;font-weight:600;border-radius:8px;box-shadow:0 1px 4px rgba(249,115,22,.08);z-index:10;transition:background .15s,border .15s;text-decoration:none}
.volver-btn:hover{background:#fff7ed;border-color:#f97316;color:#ea580c;text-decoration:none}
@media(max-width:768px){
body{padding:6px}
h2{font-size:17px}h3{font-size:14px}
.card{padding:10px}
.fg{grid-template-columns:1fr!important}
.kgr{grid-template-columns:repeat(2,1fr)!important}
table{display:block;overflow-x:auto;-webkit-overflow-scrolling:touch;white-space:nowrap}
th,td{padding:5px 7px;font-size:12px}
a.b,button.b{padding:6px 9px!important;font-size:12px!important;margin-right:4px;margin-bottom:4px}
.sm{padding:4px 7px!important;font-size:11px!important}
.main-btns{flex-direction:column;align-items:center;gap:14px}
.main-btn-card{width:90vw;min-height:auto;padding:16px 14px}
.main-btn-card .icon{font-size:34px;margin-bottom:6px}
.main-btn-card .title{font-size:17px}
.main-btn-card .desc{font-size:13px}
.volver-btn{top:8px;right:8px;padding:5px 10px;font-size:12px}
input,select,textarea{font-size:14px}
}
</style>"""

def _page(title, body):
    return "<!DOCTYPE html><html lang='es'><head><title>{t}</title>{css}</head><body><div class='w'>{b}</div></body></html>".format(
        t=_e(title), css=_CSS, b=body)


# ══════════════════════════════════════════════════════════════
#  DASHBOARD
# ══════════════════════════════════════════════════════════════

@suministros_bp.route("/")
def dashboard():
    db = get_db()
    _ensure_tables(db)

    from datetime import date as _date_d
    hoy_d = _date_d.today().isoformat()
    total_op  = int((db.execute("SELECT COUNT(1) FROM ordenes_pedido").fetchone() or [0])[0])
    pend_op   = int((db.execute("SELECT COUNT(1) FROM ordenes_pedido WHERE estado='Pendiente'").fetchone() or [0])[0])
    enviadas  = int((db.execute("SELECT COUNT(1) FROM ordenes_compra WHERE estado IN ('Envio OC','OC confirmada')").fetchone() or [0])[0])
    despacho  = int((db.execute("SELECT COUNT(1) FROM ordenes_compra WHERE estado='Para Despachar'").fetchone() or [0])[0])
    recibidas = int((db.execute("SELECT COUNT(1) FROM ordenes_compra WHERE estado='Recibido OK'").fetchone() or [0])[0])
    parciales = int((db.execute("SELECT COUNT(1) FROM ordenes_compra WHERE estado='Recibido Parcial'").fetchone() or [0])[0])
    vencidas  = int((db.execute(
        "SELECT COUNT(1) FROM ordenes_compra WHERE estado NOT IN ('Recibido OK','Cancelada','Pagado') AND fecha_despacho IS NOT NULL AND fecha_despacho < ?",
        (hoy_d,)).fetchone() or [0])[0])

    # KPIs por responsable actual
    n_coord   = int((db.execute("SELECT COUNT(1) FROM ordenes_pedido WHERE estado='Pendiente'").fetchone() or [0])[0])
    n_taller  = int((db.execute("SELECT COUNT(1) FROM ordenes_pedido WHERE estado='Aprobada'").fetchone() or [0])[0])
    n_compras = int((db.execute("SELECT COUNT(1) FROM ordenes_compra WHERE estado IN ('Pedido Precios','Envio OC','OC confirmada')").fetchone() or [0])[0])
    n_pagos   = int((db.execute("SELECT COUNT(1) FROM ordenes_compra WHERE estado='Recibido OK'").fetchone() or [0])[0])
    pagadas   = int((db.execute("SELECT COUNT(1) FROM ordenes_compra WHERE estado='Pagado'").fetchone() or [0])[0])

    # Indicadores ejecutivos
    from datetime import timedelta as _td2
    hace5_s = (_date_d.today() - _td2(days=5)).isoformat()
    ops_sin_atender = int((db.execute(
        "SELECT COUNT(1) FROM ordenes_pedido WHERE estado IN ('Pendiente','Aprobada')").fetchone() or [0])[0])
    oc_riesgo = int((db.execute(
        "SELECT COUNT(1) FROM ordenes_compra WHERE estado IN ('Envio OC','OC confirmada')"
        " AND (fecha IS NULL OR fecha < ?)", (hace5_s,)).fetchone() or [0])[0])
    total_oc_cerradas_mes = int((db.execute(
        "SELECT COUNT(1) FROM ordenes_compra WHERE estado IN ('Recibido OK','Pagado')"
        " AND fecha_recepcion >= ?", ((_date_d.today().replace(day=1)).isoformat(),)).fetchone() or [0])[0])
    total_oc_vencidas_mes = int((db.execute(
        "SELECT COUNT(1) FROM ordenes_compra WHERE fecha_despacho IS NOT NULL"
        " AND fecha_despacho < fecha_recepcion AND fecha_recepcion >= ?",
        ((_date_d.today().replace(day=1)).isoformat(),)).fetchone() or [0])[0])
    pct_entrega = int(round(100 * (total_oc_cerradas_mes - total_oc_vencidas_mes) / total_oc_cerradas_mes)) if total_oc_cerradas_mes > 0 else 100
    pct_color = "#22c55e" if pct_entrega >= 85 else ("#f59e0b" if pct_entrega >= 65 else "#ef4444")

    # Datos para el mini kanban
    ops = db.execute(
        "SELECT id, COALESCE(numero,''), COALESCE(obra,''), estado FROM ordenes_pedido ORDER BY id DESC"
    ).fetchall()
    ocs = db.execute(
        "SELECT oc.id, COALESCE(oc.numero,''), COALESCE(oc.proveedor,''), oc.estado,"
        " COALESCE(op.obra,'') FROM ordenes_compra oc"
        " LEFT JOIN ordenes_pedido op ON op.id=oc.op_id ORDER BY oc.id DESC"
    ).fetchall()
    op_por_estado = {}
    for r in ops:
        op_por_estado.setdefault(str(r[3]), []).append(r)
    oc_por_estado = {}
    for r in ocs:
        oc_por_estado.setdefault(str(r[3]), []).append(r)

    FLUJO_MINI = [
        ("Pendiente",        "op"),
        ("Aprobada",         "op"),
        ("Pedido Precios",   "op"),
        ("Envio OC",         "oc"),
        ("Para Despachar",   "oc"),
        ("Recibido Parcial", "oc"),
        ("Recibido OK",      "oc"),
    ]

    def mk_op_card(r):
        return (
            "<div style='background:#fff;border-radius:6px;border:1px solid #e2e8f0;"
            "padding:6px 8px;margin-bottom:5px'>"
            "<a href='/modulo/suministros/ordenes-pedido/{id}' style='font-weight:700;"
            "color:#7c2d12;text-decoration:none;font-size:12px'>OP {num}</a>"
            "<div style='color:#6b7280;font-size:11px;white-space:nowrap;overflow:hidden;"
            "text-overflow:ellipsis;max-width:150px'>{obra}</div>"
            "</div>"
        ).format(id=int(r[0]), num=_e(r[1]), obra=_e(r[2]))

    def mk_oc_card(r):
        return (
            "<div style='background:#fff;border-radius:6px;border:1px solid #bfdbfe;"
            "padding:6px 8px;margin-bottom:5px'>"
            "<a href='/modulo/suministros/ordenes-compra/{id}' style='font-weight:700;"
            "color:#1e3a5f;text-decoration:none;font-size:12px'>OC {num}</a>"
            "<div style='color:#6b7280;font-size:11px;white-space:nowrap;overflow:hidden;"
            "text-overflow:ellipsis;max-width:150px'>{prov}</div>"
            "</div>"
        ).format(id=int(r[0]), num=_e(r[1]), prov=_e(r[2]))

    kanban_cols = ""
    for (e, tipo) in FLUJO_MINI:
        if tipo == "op":
            items = op_por_estado.get(e, [])
            cards = "".join(mk_op_card(r) for r in items[:5])
        else:
            items = oc_por_estado.get(e, [])
            cards = "".join(mk_oc_card(r) for r in items[:5])
        extra = len(items) - 5
        if extra > 0:
            cards += "<a href='/modulo/suministros/kanban' style='display:block;text-align:center;font-size:11px;color:#64748b;padding:4px;background:#f1f5f9;border-radius:4px;text-decoration:none;margin-top:2px'>+{} m\u00e1s...</a>".format(extra)
        if not cards:
            cards = "<div style='color:#cbd5e1;font-size:11px;font-style:italic;text-align:center;padding:6px 0'>\u2014</div>"
        bg, fg = _BC.get(e, ("#f1f5f9", "#334155"))
        hdr = (
            "<div style='background:{bg};color:{fg};border-radius:7px 7px 0 0;padding:6px 10px;"
            "font-weight:700;font-size:11px;display:flex;justify-content:space-between;align-items:center'>"
            "<span>{e}</span>"
            "<span style='background:{fg};color:{bg};border-radius:999px;padding:1px 6px;font-size:10px'>{n}</span>"
            "</div>"
        ).format(bg=bg, fg=fg, e=_e(e), n=len(items))
        col_body = (
            "<div style='background:#f8fafc;border:1px solid #e2e8f0;border-top:none;"
            "border-radius:0 0 7px 7px;padding:6px;min-height:36px;max-height:52vh;overflow-y:auto'>"
            + cards + "</div>"
        )
        kanban_cols += "<div style='min-width:155px;max-width:180px;flex:0 0 auto'>" + hdr + col_body + "</div>"

    flow = " &rarr; ".join([
        "<span style='background:{bg};color:{fg};padding:3px 10px;border-radius:999px;"
        "font-size:13px;font-weight:700'>{e}</span>".format(bg=_BC[e][0], fg=_BC[e][1], e=e)
        for e in ["Pendiente", "Aprobada", "Pedido Precios",
                  "Envio OC", "OC confirmada", "Para Despachar", "Recibido OK"]
    ])

    # ── Alertas y próximas entregas ──────────────────────────────
    from datetime import timedelta as _td
    hoy_obj = _date_d.today()
    en7_s   = (hoy_obj + _td(days=7)).isoformat()

    ocs_venc = db.execute(
        "SELECT oc.id, COALESCE(oc.numero,''), COALESCE(op.obra,''), oc.fecha_despacho"
        " FROM ordenes_compra oc LEFT JOIN ordenes_pedido op ON op.id=oc.op_id"
        " WHERE oc.estado NOT IN ('Recibido OK','Cancelada')"
        " AND oc.fecha_despacho IS NOT NULL AND oc.fecha_despacho < ?"
        " ORDER BY oc.fecha_despacho LIMIT 5", (hoy_d,)
    ).fetchall()

    ocs_prox7 = db.execute(
        "SELECT oc.id, COALESCE(oc.numero,''), COALESCE(op.obra,''), oc.fecha_despacho"
        " FROM ordenes_compra oc LEFT JOIN ordenes_pedido op ON op.id=oc.op_id"
        " WHERE oc.estado NOT IN ('Recibido OK','Cancelada')"
        " AND oc.fecha_despacho BETWEEN ? AND ? ORDER BY oc.fecha_despacho LIMIT 5", (hoy_d, en7_s)
    ).fetchall()

    ops_crit = db.execute(
        "SELECT id, COALESCE(numero,''), COALESCE(obra,''), criticidad"
        " FROM ordenes_pedido WHERE criticidad IN ('Alta','Urgente')"
        " AND estado NOT IN ('Cerrada','Cancelada')"
        " ORDER BY CASE criticidad WHEN 'Urgente' THEN 0 ELSE 1 END LIMIT 4"
    ).fetchall()

    ocs_prox_desp = db.execute(
        "SELECT oc.id, COALESCE(oc.numero,''), COALESCE(op.obra,''), oc.fecha_despacho"
        " FROM ordenes_compra oc LEFT JOIN ordenes_pedido op ON op.id=oc.op_id"
        " WHERE oc.estado NOT IN ('Recibido OK','Cancelada') AND oc.fecha_despacho >= ?"
        " ORDER BY oc.fecha_despacho LIMIT 7", (hoy_d,)
    ).fetchall()

    def _dd(fecha_str):
        try:
            diff = (_date_d.fromisoformat(str(fecha_str)) - hoy_obj).days
            if diff < 0:  return "{} d. vencida".format(-diff), "#ef4444"
            if diff == 0: return "hoy", "#f97316"
            if diff <= 3: return "+{}d".format(diff), "#f59e0b"
            return "+{}d".format(diff), "#22c55e"
        except Exception:
            return "\u2014", "#64748b"

    alert_items = ""
    for r in ocs_venc:
        dt, _ = _dd(r[3])
        alert_items += (
            "<div style='border-left:3px solid #ef4444;padding:5px 0 5px 10px;margin-bottom:7px'>"
            "<div style='font-size:12px;font-weight:700'>"
            "<a href='/modulo/suministros/ordenes-compra/{id}' style='color:#dc2626;text-decoration:none'>{num}</a>"
            " <span style='color:#64748b;font-weight:400;font-size:11px'>\u2014 {dt}</span></div>"
            "<div style='font-size:11px;color:#64748b'>Vencida &middot; {obra}</div>"
            "</div>"
        ).format(id=int(r[0]), num=_e(r[1]), obra=_e(r[2] or ""), dt=dt)

    for r in ocs_prox7:
        dt, _ = _dd(r[3])
        alert_items += (
            "<div style='border-left:3px solid #f97316;padding:5px 0 5px 10px;margin-bottom:7px'>"
            "<div style='font-size:12px;font-weight:700'>"
            "<a href='/modulo/suministros/ordenes-compra/{id}' style='color:#c2410c;text-decoration:none'>{num}</a>"
            " <span style='color:#64748b;font-weight:400;font-size:11px'>\u2014 {dt}</span></div>"
            "<div style='font-size:11px;color:#64748b'>Pr\u00f3xima &middot; {obra}</div>"
            "</div>"
        ).format(id=int(r[0]), num=_e(r[1]), obra=_e(r[2] or ""), dt=dt)

    for r in ops_crit:
        crit = str(r[3] or "")
        cc = "#ef4444" if crit == "Urgente" else "#f97316"
        alert_items += (
            "<div style='border-left:3px solid {cc};padding:5px 0 5px 10px;margin-bottom:7px'>"
            "<div style='font-size:12px;font-weight:700'>"
            "<a href='/modulo/suministros/ordenes-pedido/{id}' style='color:#92400e;text-decoration:none'>OP {num}</a>"
            " <span style='color:{cc};font-weight:700;font-size:10px;margin-left:4px'>{crit}</span></div>"
            "<div style='font-size:11px;color:#64748b'>{obra}</div>"
            "</div>"
        ).format(id=int(r[0]), num=_e(r[1]), obra=_e(r[2] or ""), cc=cc, crit=_e(crit))

    if not alert_items:
        alert_items = "<div style='color:#64748b;font-size:12px;font-style:italic'>Sin alertas activas.</div>"

    prox_items = "".join(
        "<div style='display:flex;justify-content:space-between;align-items:center;"
        "padding:5px 0;border-bottom:1px solid #fed7aa'>"
        "<a href='/modulo/suministros/ordenes-compra/{id}' style='color:#c2410c;font-weight:700;"
        "font-size:12px;text-decoration:none'>{num}</a>"
        "<span style='font-size:11px;color:#64748b;flex:1;margin:0 6px;white-space:nowrap;"
        "overflow:hidden;text-overflow:ellipsis'>{obra}</span>"
        "<span style='font-size:11px;font-weight:700;color:{dc}'>{dt}</span>"
        "</div>".format(
            id=int(r[0]), num=_e(r[1]), obra=_e((r[2] or "")[:14]),
            dt=_dd(r[3])[0], dc=_dd(r[3])[1])
        for r in ocs_prox_desp
    ) or "<div style='color:#64748b;font-size:12px;font-style:italic'>Sin OCs programadas.</div>"

    right_panel = (
        "<div style='display:flex;gap:10px;flex-wrap:wrap;margin-bottom:10px'>"
        # Dashboard de compras — mismo estilo card que Alertas / Próximas Entregas
        "<div class='card' style='flex:1;min-width:220px;padding:14px'>"
        "<div style='font-size:11px;font-weight:700;letter-spacing:1.5px;color:#9a3412;"
        "margin-bottom:10px;border-bottom:2px solid #fed7aa;padding-bottom:6px'>&#128202; DASHBOARD COMPRAS</div>"
        "<div style='margin-bottom:9px'>"
        "<div style='font-size:10px;font-weight:700;letter-spacing:1px;color:#92400e;margin-bottom:2px'>OPS SIN ATENDER</div>"
        "<div style='color:#f97316;font-size:22px;font-weight:800;line-height:1.1'>{ops_sin_atender}"
        "<span style='font-size:11px;color:#64748b;font-weight:400'> / {total_op}</span></div>"
        "</div>"
        "<div style='margin-bottom:9px'>"
        "<div style='font-size:10px;font-weight:700;letter-spacing:1px;color:#92400e;margin-bottom:2px'>OC EN RIESGO</div>"
        "<div style='color:#ef4444;font-size:22px;font-weight:800;line-height:1.1'>{oc_riesgo}"
        "<span style='font-size:11px;color:#64748b;font-weight:400'> &gt;5 d\u00edas</span></div>"
        "</div>"
        "<div>"
        "<div style='font-size:10px;font-weight:700;letter-spacing:1px;color:#92400e;margin-bottom:2px'>ENTREGA A TIEMPO</div>"
        "<div style='color:{pct_color};font-size:22px;font-weight:800;line-height:1.1'>{pct_entrega}%"
        "<span style='font-size:11px;color:#64748b;font-weight:400'> meta 85%</span></div>"
        "</div>"
        "</div>"
        # Alertas
        "<div class='card' style='flex:1;min-width:220px;padding:14px'>"
        "<div style='font-size:11px;font-weight:700;letter-spacing:1.5px;color:#9a3412;"
        "margin-bottom:10px;border-bottom:2px solid #fed7aa;padding-bottom:6px'>&#9888; ALERTAS</div>"
        + alert_items +
        "</div>"
        # Próximas entregas
        "<div class='card' style='flex:1;min-width:220px;padding:14px'>"
        "<div style='font-size:11px;font-weight:700;letter-spacing:1.5px;color:#9a3412;"
        "margin-bottom:10px;border-bottom:2px solid #fed7aa;padding-bottom:6px'>&#128666; PR\u00d3XIMAS ENTREGAS</div>"
        + prox_items +
        "</div>"
        "</div>"
    )

    body = (
        "<div style='display:flex;align-items:center;gap:14px;margin-bottom:10px;flex-wrap:wrap'>"
        "<h2 style='margin:0'>Suministros / Compras</h2>"
        "<a class='volver-btn' href='/'>&#8617; Volver</a>"
        "</div>"
        # Botones de navegación pequeños
        "<div style='display:flex;gap:7px;flex-wrap:wrap;margin-bottom:14px'>"
        "<a class='b sm bl' href='/modulo/suministros/ordenes-pedido'>Orden de Pedido</a>"
        "<a class='b sm tl' href='/modulo/suministros/ordenes-compra'>Orden de Compra</a>"
        "<a class='b sm or' href='/modulo/suministros/articulos'>Lista de Materiales</a>"
        "<a class='b sm pu' href='/modulo/suministros/proveedores'>Proveedores</a>"
        "<a class='b sm gr' href='/modulo/suministros/kanban'>Kanban completo</a>"
        "<a class='b sm' style='background:#0f766e' href='/modulo/suministros/control-stock'>&#128230; Control de Stock</a>"
        "<a class='b sm' style='background:#065f46' href='/modulo/suministros/ordenes-compra/cerradas'>&#10003; OC Cerradas</a>"
        "</div>"
        # KPIs compactos
        "<div class='kgr' style='margin-bottom:10px'>"
        "<div class='card' style='padding:8px 12px'><div style='font-size:11px;color:#64748b'>Total OPs</div><div class='kn' style='font-size:20px'>{total_op}</div></div>"
        "<div class='card' style='padding:8px 12px'><div style='font-size:11px;color:#64748b'>OP Pendientes</div><div class='kn' style='font-size:20px;color:#d97706'>{pend_op}</div></div>"
        "<div class='card' style='padding:8px 12px'><div style='font-size:11px;color:#64748b'>OC enviadas</div><div class='kn' style='font-size:20px'>{enviadas}</div></div>"
        "<div class='card' style='padding:8px 12px'><div style='font-size:11px;color:#64748b'>Para Despachar</div><div class='kn' style='font-size:20px;color:#92400e'>{despacho}</div></div>"
        "<div class='card' style='padding:8px 12px'><div style='font-size:11px;color:#64748b'>Recibido OK</div><div class='kn' style='font-size:20px;color:#166534'>{recibidas}</div></div>"
        "<div class='card' style='padding:8px 12px'><div style='font-size:11px;color:#64748b'>Recibido Parcial</div><div class='kn' style='font-size:20px;color:#9f1239'>{parciales}</div></div>"
        "<div class='card' style='padding:8px 12px'><div style='font-size:11px;color:#64748b'>OC Vencidas</div><div class='kn' style='font-size:20px;color:#dc2626'>{vencidas}</div></div>"
        "</div>"
        # KPIs por responsable compactos
        "<div style='font-size:11px;font-weight:700;color:#9a3412;letter-spacing:0.5px;"
        "margin-bottom:6px;border-left:3px solid #f97316;padding-left:8px'>Responsable actual</div>"
        "<div class='kgr' style='margin-bottom:14px'>"
        "<div class='card' style='padding:8px 12px'><div style='font-size:11px;color:#64748b'>COORD.</div>"
        "<div class='kn' style='font-size:20px;color:#7c3aed'>{n_coord}</div><div style='font-size:10px;color:#64748b'>Pendientes</div></div>"
        "<div class='card' style='padding:8px 12px'><div style='font-size:11px;color:#64748b'>COMPRAS</div>"
        "<div class='kn' style='font-size:20px;color:#b45309'>{n_taller}</div><div style='font-size:10px;color:#64748b'>Aprobadas</div></div>"
        "<div class='card' style='padding:8px 12px'><div style='font-size:11px;color:#64748b'>COMPRAS</div>"
        "<div class='kn' style='font-size:20px;color:#1d4ed8'>{n_compras}</div><div style='font-size:10px;color:#64748b'>OC activas</div></div>"
        "<div class='card' style='padding:8px 12px'><div style='font-size:11px;color:#64748b'>TALLER EEMM</div>"
        "<div class='kn' style='font-size:20px;color:#92400e'>{despacho}</div><div style='font-size:10px;color:#64748b'>Para Despachar</div></div>"
        "<div class='card' style='padding:8px 12px'><div style='font-size:11px;color:#64748b'>PAGOS</div>"
        "<div class='kn' style='font-size:20px;color:#166534'>{n_pagos}</div><div style='font-size:10px;color:#64748b'>Recibido OK</div></div>"
        "<div class='card' style='padding:8px 12px'><div style='font-size:11px;color:#64748b'>CERRADAS</div>"
        "<div class='kn' style='font-size:20px;color:#065f46'>{pagadas}</div><div style='font-size:10px;color:#64748b'><a href='/modulo/suministros/ordenes-compra/cerradas' style='color:#065f46'>Pagadas</a></div></div>"
        "</div>"
        # Kanban + Panel de alertas
        "<div style='margin-bottom:10px'>"
        + right_panel +
        "</div>"
        "<div style='margin-bottom:14px'>"
        "<div style='font-size:13px;font-weight:700;color:#9a3412;letter-spacing:0.5px;"
        "margin-bottom:8px;border-left:3px solid #f97316;padding-left:8px'>Flujo activo de compras</div>"
        "<div style='overflow-x:auto;padding-bottom:4px'>"
        "<div style='display:flex;gap:8px;align-items:flex-start'>"
        + kanban_cols +
        "</div></div></div>"
        # Flujo de estados
        "<div class='card'><b>Flujo:</b>"
        "<div style='margin-top:8px;line-height:2.2'>" + flow + "</div></div>"
        "</div>"
    ).format(
        total_op=total_op, pend_op=pend_op, enviadas=enviadas,
        despacho=despacho, recibidas=recibidas, parciales=parciales,
        vencidas=vencidas, hoy_d=hoy_d,
        ops_sin_atender=ops_sin_atender, oc_riesgo=oc_riesgo,
        pct_entrega=pct_entrega, pct_color=pct_color, pagadas=pagadas,
        n_coord=n_coord, n_taller=n_taller, n_compras=n_compras, n_pagos=n_pagos)
    return _page("Suministros", body)


# ══════════════════════════════════════════════════════════════
#  PROVEEDORES

@suministros_bp.route("/proveedores")
def proveedores():
    db = get_db()
    _ensure_tables(db)
    rows = db.execute(
        "SELECT id, nombre, COALESCE(cuit,''), COALESCE(telefono,''), COALESCE(email,''),"
        " COALESCE(contacto,''), activo FROM proveedores ORDER BY nombre"
    ).fetchall()

    filas = "".join(
        "<tr>"
        "<td><b>{nom}</b></td><td>{cuit}</td><td>{tel}</td>"
        "<td><a href='mailto:{email}'>{email}</a></td><td>{cont}</td>"
        "<td style='white-space:nowrap'>"
        "<a class='b bl sm' href='/modulo/suministros/proveedores/{id}/editar'>Editar</a> "
        "<form method='post' action='/modulo/suministros/proveedores/{id}/toggle' style='display:inline'>"
        "<button class='b sm {cls}' onclick=\"return confirm('Confirmar?')\">{txt}</button>"
        "</form></td></tr>".format(
            id=int(r[0]), nom=_e(r[1]), cuit=_e(r[2]), tel=_e(r[3]),
            email=_e(r[4]), cont=_e(r[5]),
            cls="rd" if r[6] else "gn", txt="Desactivar" if r[6] else "Activar")
        for r in rows
    ) or "<tr><td colspan='6'>Sin proveedores cargados.</td></tr>"

    body = (
        "<h2>Proveedores</h2>"
        "<a class='b or' href='/modulo/suministros/proveedores/nuevo'>+ Nuevo Proveedor</a>"
        "<a class='b gr' href='/modulo/suministros'>Dashboard</a>"
        "<div class='card' style='margin-top:12px;overflow-x:auto'>"
        "<table class='hl'><thead><tr>"
        "<th>Nombre</th><th>CUIT</th><th>Tel\u00e9fono</th><th>Email</th><th>Contacto</th><th></th>"
        "</tr></thead><tbody>{filas}</tbody></table></div>"
    ).format(filas=filas)
    return _page("Proveedores", body)


@suministros_bp.route("/proveedores/nuevo", methods=["GET", "POST"])
def proveedor_nuevo():
    db = get_db()
    _ensure_tables(db)
    error = ""
    if request.method == "POST":
        nombre   = (request.form.get("nombre") or "").strip()
        cuit     = (request.form.get("cuit") or "").strip()
        telefono = (request.form.get("telefono") or "").strip()
        email    = (request.form.get("email") or "").strip()
        contacto = (request.form.get("contacto") or "").strip()
        obs      = (request.form.get("observaciones") or "").strip()
        if not nombre:
            error = "El nombre es obligatorio."
        else:
            db.execute(
                "INSERT INTO proveedores (nombre,cuit,telefono,email,contacto,observaciones,activo) VALUES (?,?,?,?,?,?,1)",
                (nombre, cuit, telefono, email, contacto, obs))
            db.commit()
            return redirect("/modulo/suministros/proveedores")
    body = (
        "<h2>Nuevo Proveedor</h2>"
        "<a class='b gr' href='/modulo/suministros/proveedores'>Volver</a>"
        "{err}"
        "<div class='card' style='max-width:520px;margin-top:12px'><form method='post'>"
        "<div class='fg'>"
        "<div><label>Nombre *</label><input name='nombre' required></div>"
        "<div><label>CUIT</label><input name='cuit' placeholder='20-12345678-9'></div>"
        "<div><label>Tel\u00e9fono</label><input name='telefono'></div>"
        "<div><label>Email</label><input name='email' type='email'></div>"
        "<div><label>Contacto</label><input name='contacto' placeholder='Nombre del contacto'></div>"
        "<div><label>Observaciones</label><textarea name='observaciones' rows='3'></textarea></div>"
        "</div>"
        "<div style='margin-top:10px'><button class='b tl'>Guardar</button></div>"
        "</form></div>"
    ).format(err="<div class='err'>{}</div>".format(_e(error)) if error else "")
    return _page("Nuevo Proveedor", body)


@suministros_bp.route("/proveedores/<int:prov_id>/editar", methods=["GET", "POST"])
def proveedor_editar(prov_id):
    db = get_db()
    _ensure_tables(db)
    row = db.execute(
        "SELECT id, nombre, COALESCE(cuit,''), COALESCE(telefono,''), COALESCE(email,''),"
        " COALESCE(contacto,''), COALESCE(observaciones,'') FROM proveedores WHERE id=?", (prov_id,)
    ).fetchone()
    if not row:
        return _page("Error", "<p>Proveedor no encontrado.</p><a class='b gr' href='/modulo/suministros/proveedores'>Volver</a>"), 404
    error = ""
    if request.method == "POST":
        nombre   = (request.form.get("nombre") or "").strip()
        cuit     = (request.form.get("cuit") or "").strip()
        telefono = (request.form.get("telefono") or "").strip()
        email    = (request.form.get("email") or "").strip()
        contacto = (request.form.get("contacto") or "").strip()
        obs      = (request.form.get("observaciones") or "").strip()
        if not nombre:
            error = "El nombre es obligatorio."
        else:
            db.execute(
                "UPDATE proveedores SET nombre=?,cuit=?,telefono=?,email=?,contacto=?,observaciones=? WHERE id=?",
                (nombre, cuit, telefono, email, contacto, obs, prov_id))
            db.commit()
            return redirect("/modulo/suministros/proveedores")
    body = (
        "<h2>Editar Proveedor</h2>"
        "<a class='b gr' href='/modulo/suministros/proveedores'>Volver</a>"
        "{err}"
        "<div class='card' style='max-width:520px;margin-top:12px'><form method='post'>"
        "<div class='fg'>"
        "<div><label>Nombre *</label><input name='nombre' required value='{nom}'></div>"
        "<div><label>CUIT</label><input name='cuit' value='{cuit}'></div>"
        "<div><label>Tel\u00e9fono</label><input name='telefono' value='{tel}'></div>"
        "<div><label>Email</label><input name='email' type='email' value='{email}'></div>"
        "<div><label>Contacto</label><input name='contacto' value='{cont}'></div>"
        "<div><label>Observaciones</label><textarea name='observaciones' rows='3'>{obs}</textarea></div>"
        "</div>"
        "<div style='margin-top:10px'><button class='b tl'>Guardar</button></div>"
        "</form></div>"
    ).format(
        nom=_e(row[1]), cuit=_e(row[2]), tel=_e(row[3]),
        email=_e(row[4]), cont=_e(row[5]), obs=_e(row[6]),
        err="<div class='err'>{}</div>".format(_e(error)) if error else "")
    return _page("Editar Proveedor", body)


@suministros_bp.route("/proveedores/<int:prov_id>/toggle", methods=["POST"])
def proveedor_toggle(prov_id):
    db = get_db()
    _ensure_tables(db)
    row = db.execute("SELECT activo FROM proveedores WHERE id=?", (prov_id,)).fetchone()
    if row:
        db.execute("UPDATE proveedores SET activo=? WHERE id=?", (0 if row[0] else 1, prov_id))
        db.commit()
    return redirect("/modulo/suministros/proveedores")


#  ARTÍCULOS (lista de materiales)
# ══════════════════════════════════════════════════════════════

@suministros_bp.route("/articulos")
def articulos():
    db = get_db()
    _ensure_tables(db)
    rows = db.execute(
        "SELECT id, COALESCE(codigo,''), descripcion, COALESCE(unidad,'u'), COALESCE(categoria,''), activo, COALESCE(kg_per_m,0) "
        "FROM articulos_sum ORDER BY descripcion"
    ).fetchall()

    filas = "".join(
        "<tr>"
        "<td>{id}</td><td>{cod}</td><td><b>{desc}</b></td><td>{unid}</td><td style='text-align:right'>{kgm}</td><td>{cat}</td>"
        "<td>{act}</td>"
        "<td>"
        "<a class='b bl sm' href='/modulo/suministros/articulos/{id}/editar'>Editar</a>"
        "<form method='post' action='/modulo/suministros/articulos/{id}/toggle' style='display:inline'>"
        "<button class='b sm {cls}' onclick=\"return confirm('Confirmar?')\">{txt}</button>"
        "</form></td></tr>".format(
            id=int(r[0]), cod=_e(r[1]), desc=_e(r[2]), unid=_e(r[3]),
            kgm="{:.3f}".format(float(r[6])) if r[6] else "—",
            cat=_e(r[4]), act="Si" if r[5] else "No",
            cls="rd" if r[5] else "gn", txt="Desactivar" if r[5] else "Activar")
        for r in rows
    ) or "<tr><td colspan='8'>Sin articulos cargados.</td></tr>"

    body = (
        "<h2>Lista de Materiales</h2>"
        "<a class='b or' href='/modulo/suministros/articulos/nuevo'>+ Nuevo articulo</a>"
        "<a class='b am' href='/modulo/suministros/articulos/importar'>Importar CSV/Excel</a>"
        "<form method='post' action='/modulo/suministros/articulos/autocodigos' style='display:inline'>"
        "<button class='b gr sm' onclick=\"return confirm('Solo se asignaran codigos a los articulos que aun NO tienen codigo asignado. Continuar?')\">Generar Codigos Auto</button>"
        "</form>"
        "<a class='b gr' href='/modulo/suministros'>Dashboard</a>"
        "<div class='card' style='margin-top:12px'>"
        "<table class='hl'><thead><tr>"
        "<th>ID</th><th>Codigo</th><th>Descripcion</th><th>Unidad</th><th>Kg/m</th><th>Categoria</th><th>Activo</th><th>Acciones</th>"
        "</tr></thead><tbody>{filas}</tbody></table></div>"
    ).format(filas=filas)
    return _page("Lista de Materiales", body)


@suministros_bp.route("/articulos/nuevo", methods=["GET", "POST"])
def articulo_nuevo():
    db = get_db()
    _ensure_tables(db)
    error = ""
    if request.method == "POST":
        desc      = (request.form.get("descripcion") or "").strip()
        codigo    = (request.form.get("codigo") or "").strip()
        unidad    = (request.form.get("unidad") or "u").strip()
        categoria = (request.form.get("categoria") or "").strip()
        try:
            kg_per_m = float((request.form.get("kg_per_m") or "0").replace(",", "."))
        except Exception:
            kg_per_m = None
        if not desc:
            error = "La descripcion es obligatoria."
        else:
            db.execute(
                "INSERT INTO articulos_sum (codigo,descripcion,unidad,categoria,activo,kg_per_m) VALUES (?,?,?,?,1,?)",
                (codigo, desc, unidad, categoria, kg_per_m if kg_per_m else None))
            db.commit()
            # Si no tiene código, auto-asignar ART-XXXX
            if not codigo:
                new_id = int((db.execute("SELECT MAX(id) FROM articulos_sum").fetchone() or [0])[0])
                db.execute("UPDATE articulos_sum SET codigo=? WHERE id=? AND (codigo IS NULL OR codigo='')",
                           ("ART-{:04d}".format(new_id), new_id))
                db.commit()
            return redirect("/modulo/suministros/articulos")
    body = (
        "<h2>Nuevo articulo</h2>"
        "<a class='b gr' href='/modulo/suministros/articulos'>Volver</a>"
        "{err}"
        "<div class='card' style='max-width:520px;margin-top:12px'><form method='post'>"
        "<div class='fg'>"
        "<div><label>Descripcion *</label><input name='descripcion' required></div>"
        "<div><label>Codigo</label><input name='codigo' placeholder='Ej: ACE-001'></div>"
        "<div><label>Unidad</label><input name='unidad' placeholder='u, kg, m, lt' value='u'></div>"
        "<div><label>Categoria</label><input name='categoria' placeholder='Ej: Acero'></div>"
        "<div><label>Kg/m (peso lineal)</label><input name='kg_per_m' type='number' step='0.001' min='0' placeholder='0'></div>"
        "</div>"
        "<div style='margin-top:10px'><button class='b tl'>Guardar</button></div>"
        "</form></div>"
    ).format(err="<div class='err'>{}</div>".format(_e(error)) if error else "")
    return _page("Nuevo articulo", body)


@suministros_bp.route("/articulos/<int:art_id>/editar", methods=["GET", "POST"])
def articulo_editar(art_id):
    db = get_db()
    _ensure_tables(db)
    row = db.execute(
        "SELECT id, COALESCE(codigo,''), descripcion, COALESCE(unidad,'u'), COALESCE(categoria,''), COALESCE(kg_per_m,0) "
        "FROM articulos_sum WHERE id=?", (art_id,)
    ).fetchone()
    if not row:
        return _page("Error", "<p>Articulo no encontrado.</p><a class='b gr' href='/modulo/suministros/articulos'>Volver</a>"), 404
    error = ""
    if request.method == "POST":
        desc      = (request.form.get("descripcion") or "").strip()
        codigo    = (request.form.get("codigo") or "").strip()
        unidad    = (request.form.get("unidad") or "u").strip()
        categoria = (request.form.get("categoria") or "").strip()
        try:
            kg_per_m = float((request.form.get("kg_per_m") or "0").replace(",", "."))
        except Exception:
            kg_per_m = None
        if not desc:
            error = "La descripcion es obligatoria."
        else:
            db.execute(
                "UPDATE articulos_sum SET codigo=?,descripcion=?,unidad=?,categoria=?,kg_per_m=? WHERE id=?",
                (codigo, desc, unidad, categoria, kg_per_m if kg_per_m else None, art_id))
            # Si no tiene código, auto-asignar ART-XXXX
            if not codigo:
                db.execute("UPDATE articulos_sum SET codigo=? WHERE id=? AND (codigo IS NULL OR codigo='')",
                           ("ART-{:04d}".format(art_id), art_id))
            db.commit()
            return redirect("/modulo/suministros/articulos")
    body = (
        "<h2>Editar articulo #{id}</h2>"
        "<a class='b gr' href='/modulo/suministros/articulos'>Volver</a>"
        "{err}"
        "<div class='card' style='max-width:520px;margin-top:12px'><form method='post'>"
        "<div class='fg'>"
        "<div><label>Descripcion *</label><input name='descripcion' required value='{desc}'></div>"
        "<div><label>Codigo</label><input name='codigo' value='{cod}'></div>"
        "<div><label>Unidad</label><input name='unidad' value='{unid}'></div>"
        "<div><label>Categoria</label><input name='categoria' value='{cat}'></div>"
        "<div><label>Kg/m (peso lineal)</label><input name='kg_per_m' type='number' step='0.001' min='0' value='{kgm}'></div>"
        "</div>"
        "<div style='margin-top:10px'><button class='b tl'>Guardar</button></div>"
        "</form></div>"
    ).format(
        id=art_id, desc=_e(row[2]), cod=_e(row[1]), unid=_e(row[3]), cat=_e(row[4]),
        kgm=float(row[5]) if row[5] else 0,
        err="<div class='err'>{}</div>".format(_e(error)) if error else "")
    return _page("Editar articulo", body)


@suministros_bp.route("/articulos/<int:art_id>/toggle", methods=["POST"])
def articulo_toggle(art_id):
    db = get_db()
    _ensure_tables(db)
    db.execute("UPDATE articulos_sum SET activo = CASE WHEN activo=1 THEN 0 ELSE 1 END WHERE id=?", (art_id,))
    db.commit()
    return redirect("/modulo/suministros/articulos")


@suministros_bp.route("/articulos/autocodigos", methods=["POST"])
def articulos_autocodigos():
    """Genera codigos secuenciales para todos los articulos sin codigo."""
    db = get_db()
    _ensure_tables(db)
    rows = db.execute(
        "SELECT id FROM articulos_sum WHERE codigo IS NULL OR codigo='' ORDER BY id"
    ).fetchall()
    for idx, r in enumerate(rows, start=1):
        db.execute(
            "UPDATE articulos_sum SET codigo=? WHERE id=?",
            ("ART-{:04d}".format(int(r[0])), int(r[0])))
    db.commit()
    return redirect("/modulo/suministros/articulos")


@suministros_bp.route("/articulos/importar", methods=["GET", "POST"])
def articulos_importar():
    """Importa artículos desde CSV (sep ';' o ',') o Excel (.xlsx).
    Columnas esperadas: descripcion, codigo (opc), unidad (opc), categoria (opc)
    """
    db = get_db()
    _ensure_tables(db)
    msg = ""
    error = ""

    if request.method == "POST":
        f = request.files.get("archivo")
        if not f or not f.filename:
            error = "Selecciona un archivo."
        else:
            fname = f.filename.lower()
            rows_to_insert = []
            try:
                if fname.endswith(".xlsx") or fname.endswith(".xls"):
                    if not _HAS_OPENPYXL:
                        error = "openpyxl no esta instalado."
                    else:
                        wb = openpyxl.load_workbook(f, read_only=True, data_only=True)
                        ws = wb.active
                        headers = None
                        for row in ws.iter_rows(values_only=True):
                            if headers is None:
                                headers = [str(c or "").strip().lower() for c in row]
                                continue
                            d = {headers[i]: str(row[i] or "").strip() for i in range(len(headers)) if i < len(row)}
                            desc = d.get("descripcion", "").strip()
                            if desc:
                                rows_to_insert.append((
                                    d.get("codigo", "").strip(),
                                    desc,
                                    d.get("unidad", "u").strip() or "u",
                                    d.get("categoria", "").strip()))
                elif fname.endswith(".csv") or fname.endswith(".txt"):
                    content = f.read().decode("utf-8-sig", errors="replace")
                    # Detectar separador
                    sep = ";" if ";" in content.splitlines()[0] else ","
                    reader = csv.DictReader(io.StringIO(content), delimiter=sep)
                    # Normalizar headers
                    reader.fieldnames = [h.strip().lower() for h in (reader.fieldnames or [])]
                    for row in reader:
                        desc = (row.get("descripcion") or "").strip()
                        if desc:
                            rows_to_insert.append((
                                (row.get("codigo") or "").strip(),
                                desc,
                                (row.get("unidad") or "u").strip() or "u",
                                (row.get("categoria") or "").strip()))
                else:
                    error = "Formato no soportado. Usa .xlsx, .csv o .txt"
            except Exception as ex:
                error = "Error al leer el archivo: {}".format(str(ex))

            if not error and rows_to_insert:
                inserted = 0
                for cod, desc, unid, cat in rows_to_insert:
                    # Evitar duplicados por descripcion exacta
                    exists = db.execute(
                        "SELECT 1 FROM articulos_sum WHERE LOWER(TRIM(descripcion))=LOWER(TRIM(?))", (desc,)
                    ).fetchone()
                    if not exists:
                        db.execute(
                            "INSERT INTO articulos_sum (codigo,descripcion,unidad,categoria,activo) VALUES (?,?,?,?,1)",
                            (cod, desc, unid, cat))
                        inserted += 1
                db.commit()
                msg = "Se importaron {} articulos ({} ya existian).".format(inserted, len(rows_to_insert) - inserted)
            elif not error:
                error = "El archivo no contiene filas validas o le falta la columna 'descripcion'."

    fmt_hint = (
        "<div class='card' style='font-size:13px;border-left:4px solid #f97316;max-width:600px'>"
        "<b>Formato requerido:</b><br>"
        "Columnas (la primera fila debe ser el encabezado):<br>"
        "&nbsp;&nbsp;<code>descripcion ; codigo ; unidad ; categoria</code><br><br>"
        "<b>Excel (.xlsx):</b> la primera fila debe contener los nombres de columna.<br>"
        "<b>CSV / TXT:</b> separador <code>;</code> o <code>,</code>. "
        "Guardarlo con codificacion UTF-8.<br><br>"
        "Solo <b>descripcion</b> es obligatoria. Los duplicados (mismo nombre) se omiten."
        "</div>")

    body = (
        "<h2>Importar Lista de Materiales</h2>"
        "<a class='b gr' href='/modulo/suministros/articulos'>Volver</a>"
        "{msg}{err}"
        "<div class='card' style='max-width:520px;margin-top:12px'>"
        "<form method='post' enctype='multipart/form-data'>"
        "<label><b>Archivo Excel (.xlsx) o CSV/TXT</b></label>"
        "<input type='file' name='archivo' accept='.xlsx,.xls,.csv,.txt' style='margin-top:6px'>"
        "<div style='margin-top:12px'>"
        "<button class='b or'>Importar</button>"
        "</div></form></div>"
        "{fmt_hint}"
    ).format(
        msg="<div style='background:#dcfce7;color:#166534;padding:10px;border-radius:8px;margin-bottom:10px'>{}</div>".format(msg) if msg else "",
        err="<div class='err'>{}</div>".format(_e(error)) if error else "",
        fmt_hint=fmt_hint)
    return _page("Importar articulos", body)


# ══════════════════════════════════════════════════════════════
#  ORDENES DE PEDIDO (OP)
# ══════════════════════════════════════════════════════════════

@suministros_bp.route("/ordenes-pedido")
def op_lista():
    db = get_db()
    _ensure_tables(db)

    # Traer fecha de necesidad mínima por OP
    rows = db.execute(
        "SELECT op.id, COALESCE(op.numero,''), op.fecha, op.solicitante, COALESCE(op.obra,''),"
        " op.criticidad, op.estado, COUNT(i.id), MIN(i.fecha_necesaria)"
        " FROM ordenes_pedido op"
        " LEFT JOIN items_op i ON i.op_id=op.id"
        " GROUP BY op.id ORDER BY op.id DESC"
    ).fetchall()

    estado_f = request.args.get("estado", "")
    q = request.args.get("q", "").strip()
    try:
        page = max(1, int(request.args.get("page", 1) or 1))
    except (ValueError, TypeError):
        page = 1
    if estado_f:
        rows = [r for r in rows if r[6] == estado_f]
    if q:
        rows = [r for r in rows if q.lower() in (r[1] or "").lower()]
    per_page    = 20
    total       = len(rows)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page        = min(page, total_pages)
    offset      = (page - 1) * per_page
    rows_page   = rows[offset:offset + per_page]
    def _pg_url_op(p):
        parts = []
        if estado_f: parts.append("estado=" + estado_f)
        if q:        parts.append("q=" + q)
        if p > 1:    parts.append("page=" + str(p))
        return "?" + "&".join(parts) if parts else "?"
    pag_info  = "Mostrando {}-{} de {}".format(offset + 1, min(offset + per_page, total), total)
    pag_links = ""
    if page > 1:
        pag_links += "<a href='{}' style='color:#64748b;text-decoration:none'>&laquo; Ant</a> ".format(_pg_url_op(page - 1))
    for _p in range(max(1, page - 3), min(total_pages, page + 3) + 1):
        _sty = "font-weight:700;color:#f97316" if _p == page else "color:#64748b"
        pag_links += "<a href='{}' style='{}; text-decoration:none; padding:2px 6px'>{}</a>".format(_pg_url_op(_p), _sty, _p)
    if page < total_pages:
        pag_links += " <a href='{}' style='color:#64748b;text-decoration:none'>Sig &raquo;</a>".format(_pg_url_op(page + 1))
    paginador = ("<div style='display:flex;align-items:center;gap:12px;padding:8px 0;font-size:13px;flex-wrap:wrap'>"
        "<span style='color:#64748b'>{}</span>{}</div>").format(pag_info, pag_links)

    from datetime import date as _date, datetime as _dt
    hoy = _date.today()

    def _resp_op(estado):
        m = {"Pendiente": "COORD.", "Aprobada": "COMPRAS",
             "Pedido Precios": "COMPRAS", "Con OC": "COMPRAS",
             "Cerrada": "\u2014", "Cancelada": "\u2014"}
        return m.get(str(estado), "\u2014")

    filas = "".join(
        "<tr>"
        "<td><a href='/modulo/suministros/ordenes-pedido/{id}'><b>{num}</b></a></td>"
        "<td>{fecha}</td><td>{sol}</td><td>{obra}</td>"
        "<td>{crit}</td><td>{est}</td>"
        "<td style='font-weight:600;color:#1d4ed8;font-size:12px'>{resp}</td>"
        "<td>{items}</td>"
        "<td>{dias}</td>"
        "<td style='white-space:nowrap'>"
        "<a class='b bl sm' href='/modulo/suministros/ordenes-pedido/{id}'>Ver</a> "
        "<form method='post' action='/modulo/suministros/ordenes-pedido/{id}/eliminar' style='display:inline' onsubmit=\"return confirm('¿Eliminar esta OP?')\">"
        "<button class='b rd sm' type='submit'>Eliminar</button>"
        "</form>"
        "</td>"
        "</tr>".format(
            id=int(r[0]), num=_e(r[1]), fecha=_e(str(r[2] or "")[:10]),
            sol=_e(r[3]), obra=_e(r[4]), crit=_badge(r[5]),
            est=_badge(r[6]), resp=_resp_op(r[6]), items=int(r[7]),
            dias=(lambda fn: (str(((_dt.strptime(fn, '%Y-%m-%d').date() - hoy).days) if fn else '—'))
                if fn and len(fn)>=8 else '—')(str(r[8])[:10])
        )
        for r in rows_page
    ) or "<tr><td colspan='10'>Sin ordenes de pedido.</td></tr>"

    opts = "<option value=''>Todos</option>" + "".join(
        "<option value='{e}' {sel}>{e}</option>".format(e=_e(e), sel="selected" if e == estado_f else "")
        for e in ESTADOS_OP)

    body = (
        "<div style='display:flex;align-items:center;gap:14px;margin-bottom:12px'>"
        "<a class='b tl' href='/modulo/suministros/ordenes-pedido/nueva' style='font-size:16px;padding:11px 22px;box-shadow:0 2px 8px #fbbf2460;white-space:nowrap'>+ Nueva OP</a>"
        "<h2 style='margin:0'>Órdenes de Pedido</h2>"
        "</div>"
        "<a class='b gr' href='/modulo/suministros'>Dashboard</a>"
        "<form method='get' style='display:inline-flex;gap:8px;flex-wrap:wrap;align-items:center;margin:8px 0'>"
        "<select name='estado' onchange='this.form.submit()' style='width:auto;padding:8px'>{opts}</select>"
        "<input name='q' value='{q_val}' placeholder='Buscar N\u00ba OP...' style='width:160px;padding:8px'>"
        "<button type='submit' class='b or sm'>Buscar</button>"
        "<a class='b gr sm' href='/modulo/suministros/ordenes-pedido'>Limpiar</a></form>"
        "<div class='card' style='margin-top:4px'>"
        "<table class='hl'><thead><tr>"
        "<th>Nro</th><th>Fecha</th><th>Solicitante</th><th>Obra</th>"
        "<th>Criticidad</th><th>Estado</th><th>Responsable</th><th>Items</th><th>D\u00edas restantes</th><th></th>"
        "</tr></thead><tbody>{filas}</tbody></table></div>"
        "{paginador}"
    ).format(opts=opts, q_val=_e(q), filas=filas, paginador=paginador)
    return _page("Ordenes de Pedido", body)


@suministros_bp.route("/ordenes-pedido/nueva", methods=["GET", "POST"])
def op_nueva():
    db = get_db()
    _ensure_tables(db)
    error = ""

    # Catálogo de artículos
    arts = db.execute(
        "SELECT id, descripcion, COALESCE(unidad,'u'), COALESCE(kg_per_m,0), COALESCE(codigo,'') FROM articulos_sum WHERE activo=1 ORDER BY descripcion"
    ).fetchall()

    # Lista de solicitantes (supervisores + Gabriel Ibarra explícito)
    supervisores = db.execute(
        "SELECT nombre FROM usuarios WHERE (rol='supervisor' OR nombre='Gabriel Ibarra') AND activo=1 ORDER BY nombre"
    ).fetchall()
    sup_list = [r[0] for r in supervisores]

    # Lista de obras activas
    obras_rows = db.execute(
        "SELECT DISTINCT obra, COALESCE(cliente,'') FROM ordenes_trabajo WHERE obra IS NOT NULL AND obra!='' ORDER BY obra DESC"
    ).fetchall()
    obras_list = [(r[0], r[1]) for r in obras_rows]

    if request.method == "POST":
        solicitante   = (request.form.get("solicitante") or "").strip()
        obra          = (request.form.get("obra") or "").strip()
        sector        = (request.form.get("sector") or "Taller EEMM").strip()
        criticidad    = (request.form.get("criticidad") or "Normal").strip()
        observaciones = (request.form.get("observaciones") or "").strip()
        # Fecha de necesidad global (aplicada a todos los ítems)
        fecha_global  = (request.form.get("fecha_necesaria_global") or "").strip() or None

        descs   = request.form.getlist("item_desc[]")
        cants   = request.form.getlist("item_cant[]")
        largos  = request.form.getlist("item_largo[]")
        pesos   = request.form.getlist("item_peso[]")
        crits   = request.form.getlist("item_crit[]")
        obs_i   = request.form.getlist("item_obs[]")
        art_ids = request.form.getlist("item_art_id[]")

        items_ok = []
        for idx, d in enumerate(descs):
            desc = (d or "").strip()
            if not desc:
                continue
            try:
                cant = int(round(float((cants[idx] if idx < len(cants) else "0").replace(",", "."))))
            except Exception:
                cant = 0
            if cant <= 0:
                continue
            try:
                largo = float((largos[idx] if idx < len(largos) else "6").replace(",", "."))
            except Exception:
                largo = 6.0
            try:
                peso = float((pesos[idx] if idx < len(pesos) else "0").replace(",", "."))
            except Exception:
                peso = 0.0
            art_id_val = None
            if idx < len(art_ids) and str(art_ids[idx]).strip().isdigit():
                art_id_val = int(art_ids[idx])
            items_ok.append((
                art_id_val, desc, cant, "barra", fecha_global, largo, peso,
                (crits[idx] if idx < len(crits) else criticidad).strip(),
                (obs_i[idx] if idx < len(obs_i) else "").strip(),
            ))

        if not solicitante:
            error = "El solicitante es obligatorio."
        elif criticidad not in CRITICIDADES:
            error = "Criticidad invalida."
        elif not items_ok:
            error = "Agrega al menos un item con descripcion y cantidad."
        else:
            cur = db.execute(
                "INSERT INTO ordenes_pedido (solicitante,obra,sector,criticidad,observaciones,estado)"
                " VALUES (?,?,?,?,?,'Pendiente')",
                (solicitante, obra, sector, criticidad, observaciones))
            op_id = int(getattr(cur, "lastrowid", 0) or 0)
            if op_id <= 0:
                op_id = int((db.execute("SELECT MAX(id) FROM ordenes_pedido").fetchone() or [0])[0])
            db.execute("UPDATE ordenes_pedido SET numero=? WHERE id=?", ("OP-{:04d}".format(op_id), op_id))
            for art_id_v, desc, cant, unidad, fecha_nec, largo, peso, crit, obs_item in items_ok:
                db.execute(
                    "INSERT INTO items_op (op_id,articulo_id,descripcion,cantidad,unidad,fecha_necesaria,largo,peso_kg,criticidad,observaciones)"
                    " VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (op_id, art_id_v, desc, cant, unidad, fecha_nec, largo, peso or None, crit, obs_item))
            db.commit()
            return redirect("/modulo/suministros/ordenes-pedido/{}".format(op_id))

    opts_crit = "".join("<option value='{c}'>{c}</option>".format(c=c) for c in CRITICIDADES)
    # Selector de solicitante (supervisores + libre)
    sup_opts = "<option value=''>-- seleccionar --</option>" + "".join(
        "<option value='{n}'>{n}</option>".format(n=_e(n)) for n in sup_list)
    # Selector de obra
    obra_opts = "<option value=''>-- seleccionar obra --</option>" + "".join(
        "<option value='{o}'>{o}{cli}</option>".format(
            o=_e(r[0]), cli=" ({})".format(_e(r[1])) if r[1] else "")
        for r in obras_list)

    def _jstr(s):
        return str(s or "").replace("\\", "\\\\").replace('"', '\\"')
    arts_json = "[" + ",".join(
        '{{"id":{i},"text":"{d}","u":"{u}","kgm":{k},"cod":"{c}"}}'.format(
            i=r[0], d=_jstr(r[1]), u=_jstr(r[2]), k=float(r[3] if len(r) > 3 else 0), c=_jstr(r[4]))
        for r in arts) + "]"

    # (datalist eliminado — se usa dropdown custom en JS)

    body = (
        "<h2>Nueva Orden de Pedido</h2>"
        "<a class='b gr' href='/modulo/suministros/ordenes-pedido'>Volver</a>"
        "{err}"
        "<form method='post'>"
        "<div class='card'>"
        "<div class='fg'>"
        "<div><label>Solicitante *</label>"
        "<select name='solicitante' required>{sup_opts}</select>"
        "</div>"
        "<div><label>Obra</label>"
        "<select name='obra'>{obra_opts}</select>"
        "</div>"
        "<div><label>Sector</label><input name='sector' value='Taller EEMM'></div>"
        "<div><label>Criticidad general</label>"
        "<select name='criticidad' id='crit-global' onchange='updateCrits()'>{opts_crit}</select>"
        "</div>"
        "<div><label>Fecha de necesidad</label>"
        "<input name='fecha_necesaria_global' type='date' id='fecha-global' onchange='updateFechas()'>"
        "<small style='color:#7c2d12;font-size:11px'>Se aplica a todos los items</small>"
        "</div>"
        "</div>"
        "<div style='margin-top:8px'><label>Observaciones</label>"
        "<textarea name='observaciones' rows='2'></textarea></div>"
        "</div>"
        "<div class='card'>"
        "<div style='display:flex;justify-content:space-between;align-items:center;margin-bottom:10px'>"
        "<h3 style='margin:0'>Items solicitados</h3>"
        "<button type='button' class='b or sm' onclick='addRow()'>+ Agregar item</button>"
        "</div>"
        "<div style='overflow-x:auto'>"
        "<table>"
        "<thead><tr>"
        "<th style='min-width:280px'>Descripcion * <small style='font-weight:400'>(escribir para buscar en catalogo)</small></th>"
        "<th style='min-width:90px'>Cant. barras *</th>"
        "<th style='min-width:80px'>Largo (m)</th>"
        "<th style='min-width:90px'>Peso (kg)</th>"
        "<th style='min-width:110px'>Criticidad</th><th>Obs.</th><th></th>"
        "</tr></thead>"
        "<tbody id='ib'></tbody></table></div></div>"
        "<button class='b or' type='submit'>Guardar Orden de Pedido</button>"
        "</form>"
        "<script>"
        "const ARTS={arts_json};"
        "const CRITS={crits_json};"
        "const ARTS_MAP={{}}; ARTS.forEach(function(a){{ARTS_MAP[a.text]=a;}});"
        "function critOpts(s){{return CRITS.map(c=>`<option value='${{c}}' ${{c===s?'selected':''}}>${{c}}</option>`).join('');}}"
        "function globalCrit(){{return document.getElementById('crit-global').value||'Normal';}}"
        "function updateCrits(){{"
        "  var c=globalCrit();"
        "  document.querySelectorAll('select[name=\"item_crit[]\"]').forEach(function(s){{s.value=c;}});"
        "}}"
        "function defaultLargo(desc){{"
        "  var d=(desc||'').trim().toUpperCase();"
        "  if(/^UPN\\b/.test(d)||/^IPN\\b/.test(d)||/^W\\b/.test(d)||/^W\\d/.test(d)) return 12;"
        "  if(/^C\\s+\\d/.test(d)) return 12;"
        "  return 6;"
        "}}"
        "function calcPeso(tr){{"
        "  if(tr._pesoEdited) return;"
        "  var desc=(tr.querySelector('[name=\"item_desc[]\"]')||{{}}).value||'';"
        "  var found=ARTS_MAP[desc.trim()];"
        "  var kgm=found?found.kgm:0;"
        "  if(kgm<=0) return;"
        "  var cant=parseInt(tr.querySelector('[name=\"item_cant[]\"]').value)||0;"
        "  var largo=parseFloat(tr.querySelector('[name=\"item_largo[]\"]').value)||0;"
        "  var pi=tr.querySelector('[name=\"item_peso[]\"]');"
        "  if(pi&&cant>0&&largo>0) pi.value=(cant*largo*kgm).toFixed(2);"
        "}}"
        "function matchArt(input){{"
        "  var tr=input.closest('tr');"
        "  var val=input.value.trim();"
        "  var found=ARTS_MAP[val];"
        "  var hid=tr.querySelector('[name=\"item_art_id[]\"]');"
        "  var codDisp=tr.querySelector('.art-cod-disp');"
        "  var li=tr.querySelector('[name=\"item_largo[]\"]');"
        "  if(found){{"
        "    if(hid)hid.value=found.id;"
        "    if(codDisp)codDisp.textContent=found.cod||'';"
        "    if(li&&!li._edited){{li.value=defaultLargo(val);li.dispatchEvent(new Event('input'));}}"
        "  }}else{{"
        "    if(hid)hid.value='';"
        "    if(codDisp)codDisp.textContent='';"
        "  }}"
        "  calcPeso(tr);"
        "}}"
        "function addRow(desc,cant,largo,peso,crit,obs,aid){{"
        "  desc=desc||'';cant=cant||'';largo=largo||'';peso=peso||'';crit=crit||globalCrit();obs=obs||'';aid=aid||'';"
        "  var initCod='';"
        "  if(aid&&ARTS_MAP){{"
        "    var aFound=Object.values(ARTS_MAP).find(function(a){{return String(a.id)===String(aid);}});"
        "    if(aFound)initCod=aFound.cod||'';"
        "  }}"
        "  const tb=document.getElementById('ib');"
        "  const tr=document.createElement('tr');"
        "  tr.innerHTML=`"
        "    <td style='min-width:280px'>"
        "      <div class='art-cod-disp' style='font-size:11px;font-weight:700;color:#7c2d12;min-height:14px;margin-bottom:2px'>${{initCod}}</div>"
        "      <input name='item_desc[]' required value='${{desc}}'"
        "             oninput='showArtAC(this)' onchange='matchArt(this)'"
        "             placeholder='Escribir para buscar...' autocomplete='off' onblur='hideArtAC()'>"
        "      <input type='hidden' name='item_art_id[]' value='${{aid}}'></td>"
        "    <td style='min-width:90px'><input name='item_cant[]' type='number' step='1' min='1' required"
        "         value='${{cant}}' style='width:80px' oninput='calcPeso(this.closest(\"tr\"))'></td>"
        "    <td style='min-width:80px'><input name='item_largo[]' type='number' step='0.01' min='0'"
        "         value='${{largo||defaultLargo(desc)}}' style='width:72px'"
        "         oninput='this._edited=true;calcPeso(this.closest(\"tr\"))'></td>"
        "    <td style='min-width:90px'><input name='item_peso[]' type='number' step='0.01' min='0'"
        "         value='${{peso}}' style='width:82px;background:#fffbeb'"
        "         oninput='this.closest(\"tr\")._pesoEdited=true'></td>"
        "    <td style='min-width:110px'><select name='item_crit[]'>${{critOpts(crit)}}</select></td>"
        "    <td><input name='item_obs[]' value='${{obs}}'></td>"
        "    <td><button type='button' class='b rd sm' onclick='this.closest(\"tr\").remove()'>x</button></td>`;"
        "  tb.appendChild(tr);"
        "}}"
        "var _acTm;"
        "function showArtAC(inp){{"
        "  matchArt(inp);clearTimeout(_acTm);"
        "  var val=inp.value.trim().toLowerCase();"
        "  var dd=document.getElementById('_art_dd');if(dd)dd.remove();"
        "  if(!val)return;"
        "  var m=[];"
        "  ARTS.forEach(function(a){{"
        "    var tl=a.text.toLowerCase();"
        "    if(tl.indexOf(val)>=0)m.push({{art:a,sw:tl.startsWith(val)}});"
        "  }});"
        "  m.sort(function(a,b){{if(a.sw&&!b.sw)return -1;if(!a.sw&&b.sw)return 1;return a.art.text<b.art.text?-1:1;}});"
        "  m=m.slice(0,25);"
        "  if(!m.length)return;"
        "  var dd2=document.createElement('div');"
        "  dd2.id='_art_dd';"
        "  dd2.style.cssText='position:fixed;background:#1f2937;border:1px solid #4b5563;border-radius:4px;z-index:9999;max-height:220px;overflow-y:auto;box-shadow:0 4px 12px rgba(0,0,0,.4)';"
        "  var rect=inp.getBoundingClientRect();"
        "  dd2.style.top=rect.bottom+'px';dd2.style.left=rect.left+'px';"
        "  dd2.style.width=Math.max(rect.width,340)+'px';"
        "  m.forEach(function(x){{"
        "    var d=document.createElement('div');"
        "    d.textContent=x.art.text;"
        "    d.style.cssText='padding:7px 12px;cursor:pointer;color:#f3f4f6;font-size:13px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis';"
        "    d.onmouseenter=function(){{this.style.background='#374151';}};"
        "    d.onmouseleave=function(){{this.style.background='';}};"
        "    d.onmousedown=function(e){{e.preventDefault();inp.value=x.art.text;matchArt(inp);dd2.remove();}};"
        "    dd2.appendChild(d);"
        "  }});"
        "  document.body.appendChild(dd2);"
        "}}"
        "function hideArtAC(){{_acTm=setTimeout(function(){{var d=document.getElementById('_art_dd');if(d)d.remove();}},150);}}"
        "document.addEventListener('scroll',function(){{var d=document.getElementById('_art_dd');if(d)d.remove();}},true);"
        "addRow();"
        "</script>"
    ).format(
        err="<div class='err'>{}</div>".format(_e(error)) if error else "",
        opts_crit=opts_crit, sup_opts=sup_opts, obra_opts=obra_opts,
        arts_json=arts_json, crits_json=str(CRITICIDADES).replace("'", '"'))
    return _page("Nueva Orden de Pedido", body)


@suministros_bp.route("/ordenes-pedido/<int:op_id>/eliminar", methods=["POST"])
def op_eliminar(op_id):
    db = get_db()
    _ensure_tables(db)
    db.execute("DELETE FROM items_op WHERE op_id=?", (op_id,))
    db.execute("DELETE FROM ordenes_pedido WHERE id=?", (op_id,))
    db.commit()
    return redirect("/modulo/suministros/ordenes-pedido")


@suministros_bp.route("/ordenes-pedido/<int:op_id>", methods=["GET", "POST"])
def op_ver(op_id):
    db = get_db()
    _ensure_tables(db)

    if request.method == "POST":
        accion = (request.form.get("accion") or "").strip()
        if accion == "aprobar":
            db.execute("UPDATE ordenes_pedido SET estado='Aprobada' WHERE id=? AND estado='Pendiente'", (op_id,))
            db.commit()
        elif accion == "pedir_precios":
            db.execute("UPDATE ordenes_pedido SET estado='Pedido Precios' WHERE id=? AND estado='Aprobada'", (op_id,))
            db.commit()
        elif accion == "cancelar":
            db.execute("UPDATE ordenes_pedido SET estado='Cancelada' WHERE id=?", (op_id,))
            db.commit()
        return redirect("/modulo/suministros/ordenes-pedido/{}".format(op_id))

    row = db.execute(
        "SELECT id, COALESCE(numero,''), COALESCE(fecha,''), solicitante,"
        " COALESCE(obra,''), COALESCE(sector,''), criticidad, COALESCE(estado,'Pendiente'),"
        " COALESCE(observaciones,'')"
        " FROM ordenes_pedido WHERE id=?", (op_id,)
    ).fetchone()
    if not row:
        return _page("Error", "<p>OP no encontrada.</p><a class='b gr' href='/modulo/suministros/ordenes-pedido'>Volver</a>"), 404

    items = db.execute(
        "SELECT io.descripcion, io.cantidad, COALESCE(io.unidad,'barra'), COALESCE(io.fecha_necesaria,'\u2014'),"
        " io.criticidad, COALESCE(io.observaciones,''), COALESCE(io.largo,''), COALESCE(io.peso_kg,''),"
        " COALESCE(a.codigo,''), io.articulo_id"
        " FROM items_op io LEFT JOIN articulos_sum a ON a.id=io.articulo_id"
        " WHERE io.op_id=? ORDER BY io.id", (op_id,)
    ).fetchall()

    filas_items = "".join(
        "<tr><td style='color:#7c2d12;font-size:12px'><b>{cod}</b></td><td>{d}</td><td>{c}</td><td>{l}</td><td>{p}</td><td>{f}</td><td>{crit}</td><td>{o}</td></tr>".format(
            cod=_e(r[8]) if r[8] else ("ID:{}".format(r[9]) if r[9] else '\u2014'),
            d=_e(r[0]), c=_fmt(r[1]), l=_fmt(r[6]) if r[6]!='' else '\u2014',
            p=_fmt(r[7]) if r[7]!='' else '\u2014', f=_e(r[3]), crit=_badge(r[4]), o=_e(r[5]))
        for r in items
    ) or "<tr><td colspan='8'>Sin items.</td></tr>"

    estado = row[7]
    oc = db.execute("SELECT id, COALESCE(numero,''), proveedor, estado FROM ordenes_compra WHERE op_id=?", (op_id,)).fetchone()

    acciones = ""
    if estado == "Pendiente":
        acciones += (
            "<form method='post' style='display:inline'>"
            "<input type='hidden' name='accion' value='aprobar'>"
            "<button class='b gn' onclick=\"return confirm('Aprobar esta OP?')\">&#10003; Aprobar OP</button>"
            "</form>")
    if estado == "Aprobada":
        acciones += (
            "<form method='post' style='display:inline'>"
            "<input type='hidden' name='accion' value='pedir_precios'>"
            "<button class='b bl' onclick=\"return confirm('Marcar como Pedido Precios?')\">Pedir Precios &rarr;</button>"
            "</form>")
    if estado == "Pedido Precios":
        acciones += "<a class='b or sm' href='/modulo/suministros/ordenes-pedido/{id}/pdf-pedido' target='_blank'>&#128438; PDF Pedido de Precios</a>".format(id=op_id)
        # Preparar email con resumen de la OP
        import urllib.parse as _up
        mail_sub = _up.quote("Solicitud de Cotizacion - {num} - Obra: {obra}".format(
            num=row[1], obra=row[4] or ""))
        items_txt = "\n".join(
            "  - {cod}{desc}: {cant} barras x {l}m ({p} kg)".format(
                cod="[{}] ".format(r[8]) if r[8] else "",
                desc=r[0], cant=int(round(float(r[1]))) if r[1] else 0,
                l=r[6] if r[6] != '' else '?', p=r[7] if r[7] != '' else '?')
            for r in items)
        mail_body = _up.quote(
            "Estimado proveedor,\n\nLes solicitamos cotizacion para los siguientes materiales:\n\n"
            + items_txt +
            "\n\nObra: {obra}\nSector: {sec}\nSolicitante: {sol}\n\nQuedamos atentos.\nSaludos.".format(
                obra=row[4] or "—", sec=row[5] or "—", sol=row[3]))
        acciones += "<a class='b am sm' href='mailto:?subject={s}&body={b}'>&#9993; Preparar Email</a>".format(
            s=mail_sub, b=mail_body)
    if estado in ("Aprobada", "Pedido Precios") and not oc:
        acciones += "<a class='b tl' href='/modulo/suministros/ordenes-compra/nueva/{id}'>Armar Orden de Compra</a>".format(id=op_id)
    if estado not in ("Cerrada", "Cancelada"):
        acciones += (
            "<form method='post' style='display:inline'>"
            "<input type='hidden' name='accion' value='cancelar'>"
            "<button class='b rd sm' onclick=\"return confirm('Cancelar esta OP?')\">Cancelar</button>"
            "</form>")

    oc_info = ""
    if oc:
        oc_info = (
            "<div class='card' style='border-left:4px solid #7c3aed'>"
            "<b>Orden de Compra asociada:</b> "
            "<a class='b pu sm' href='/modulo/suministros/ordenes-compra/{id}'>{num} &mdash; {prov} &mdash; {est}</a>"
            "</div>"
        ).format(id=int(oc[0]), num=_e(oc[1]), prov=_e(oc[2]), est=_badge(oc[3]))

    body = (
        "<h2>Orden de Pedido {num}</h2>"
        "<a class='b gr' href='/modulo/suministros/ordenes-pedido'>Volver</a>"
        "<a class='b am sm' href='/modulo/suministros'>Dashboard</a>"
        "{oc_info}"
        "<div class='card'>"
        "<div style='display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin-bottom:12px'>"
        "<div style='background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 12px'>"
        "<div style='font-size:10px;font-weight:700;color:#9a3412;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px'>Numero</div>"
        "<div style='font-size:16px;font-weight:800;color:#1c0a00'>{num}</div></div>"
        "<div style='background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 12px'>"
        "<div style='font-size:10px;font-weight:700;color:#9a3412;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px'>Fecha</div>"
        "<div style='font-size:14px;font-weight:600;color:#1c0a00'>{fecha}</div></div>"
        "<div style='background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 12px'>"
        "<div style='font-size:10px;font-weight:700;color:#9a3412;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px'>Solicitante</div>"
        "<div style='font-size:14px;font-weight:700;color:#7c2d12'>{sol}</div></div>"
        "<div style='background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 12px'>"
        "<div style='font-size:10px;font-weight:700;color:#9a3412;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px'>Obra</div>"
        "<div style='font-size:14px;font-weight:700;color:#1c0a00'>{obra}</div></div>"
        "<div style='background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 12px'>"
        "<div style='font-size:10px;font-weight:700;color:#9a3412;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px'>Sector</div>"
        "<div style='font-size:14px;font-weight:600;color:#1c0a00'>{sector}</div></div>"
        "<div style='background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 12px'>"
        "<div style='font-size:10px;font-weight:700;color:#9a3412;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px'>Criticidad</div>"
        "<div style='margin-top:3px'>{crit}</div></div>"
        "<div style='background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 12px'>"
        "<div style='font-size:10px;font-weight:700;color:#9a3412;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px'>Estado</div>"
        "<div style='margin-top:3px'>{est}</div></div>"
        "</div>"
        "{obs_div}"
        "<div style='margin-top:12px'>{acciones}</div>"
        "</div>"
        "<div class='card'><h3>Items pedidos</h3>"
        "<div style='overflow-x:auto'>"
        "<table class='hl'><thead><tr>"
        "<th>Codigo</th><th>Descripcion</th><th>Cant. barras</th><th>Largo (m)</th><th>Peso (kg)</th><th>Fecha necesidad</th><th>Criticidad</th><th>Obs.</th>"
        "</tr></thead><tbody>{filas}</tbody></table></div></div>"
    ).format(
        num=_e(row[1]), fecha=_e(str(row[2])[:10]), sol=_e(row[3]),
        obra=_e(row[4]) or "—", sector=_e(row[5]) or "—",
        crit=_badge(row[6]), est=_badge(estado),
        obs_div="<div style='margin-bottom:8px'><span style='font-size:10px;font-weight:700;color:#9a3412;text-transform:uppercase;letter-spacing:.5px'>Observaciones</span><br><span style='font-size:13px'>{}</span></div>".format(_e(row[8])) if row[8] else "",
        acciones=acciones, oc_info=oc_info, filas=filas_items)
    return _page("OP {}".format(row[1]), body)


@suministros_bp.route("/ordenes-pedido/<int:op_id>/pdf-pedido")
def op_pdf_pedido(op_id):
    """PDF de Pedido de Precios — se envía al proveedor para que cotice."""
    db = get_db()
    _ensure_tables(db)

    row = db.execute(
        "SELECT id, COALESCE(numero,''), COALESCE(fecha,''), solicitante,"
        " COALESCE(obra,''), COALESCE(sector,''), criticidad,"
        " COALESCE(observaciones,''), COALESCE(fecha,''), estado"
        " FROM ordenes_pedido WHERE id=?", (op_id,)
    ).fetchone()
    if not row:
        return "<h3>OP no encontrada</h3>", 404

    items = db.execute(
        "SELECT io.descripcion, io.cantidad, COALESCE(io.unidad,'barra'), COALESCE(io.fecha_necesaria,''), io.criticidad,"
        " COALESCE(io.largo,''), COALESCE(io.peso_kg,''), COALESCE(a.codigo,'')"
        " FROM items_op io LEFT JOIN articulos_sum a ON a.id=io.articulo_id"
        " WHERE io.op_id=? ORDER BY io.id", (op_id,)
    ).fetchall()

    # Logo base64
    logo_html = ""
    logo_path = os.path.join(_APP_DIR, "LOGO.png")
    if os.path.exists(logo_path):
        import base64
        with open(logo_path, "rb") as lf:
            logo_b64 = base64.b64encode(lf.read()).decode()
        logo_html = "<img src='data:image/png;base64,{b64}' style='height:50px;object-fit:contain;max-width:160px' alt='Logo'>".format(b64=logo_b64)

    from datetime import date as _date
    fecha_doc = _date.today().strftime("%d/%m/%Y")

    filas = "".join(
        "<tr>"
        "<td style='width:40px;text-align:center'>{n}</td>"
        "<td style='color:#7c2d12;font-size:11px'><b>{cod}</b></td>"
        "<td>{d}</td>"
        "<td style='text-align:center'>{c}</td>"
        "<td style='text-align:center'>{l}</td>"
        "<td style='text-align:center'>{p}</td>"
        "<td style='text-align:center'>{f}</td>"
        "<td></td>"
        "<td></td>"
        "</tr>".format(
            n=idx + 1, cod=_e(r[7]) if r[7] else '\u2014',
            d=_e(r[0]), c=int(round(float(r[1]))) if r[1] else 0,
            l=_fmt(r[5]) if r[5]!='' else '\u2014',
            p=_fmt(r[6]) if r[6]!='' else '\u2014',
            f=_e(r[3]) or "\u2014")
        for idx, r in enumerate(items))

    return (
        "<!DOCTYPE html><html lang='es'><head><meta charset='utf-8'>"
        "<title>Pedido de Precios {num}</title>"
        "<style>"
        "@page{{margin:15mm}}"
        "@media print{{.np{{display:none}}body{{padding:0;margin:0}}"
        "  *{{-webkit-print-color-adjust:exact!important;print-color-adjust:exact!important}}}}"
        "body{{font-family:Arial,sans-serif;margin:20px 30px;color:#1c0a00;font-size:13px}}"
        ".header{{display:flex;justify-content:space-between;align-items:center;"
        "  background:#fb923c;padding:12px 18px;border-radius:8px 8px 0 0}}"
        ".header-title{{color:#fff;font-size:18px;font-weight:800;letter-spacing:0.5px}}"
        ".header-sub{{color:rgba(255,255,255,0.9);font-size:12px;margin-top:3px}}"
        ".subheader{{background:#fff7ed;border:1px solid #fed7aa;border-top:none;"
        "  padding:8px 18px;border-radius:0 0 8px 8px;margin-bottom:16px;font-size:12px;color:#7c2d12}}"
        ".meta{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:14px;"
        "  border:1px solid #fed7aa;border-radius:6px;padding:10px 14px;background:#fff}}"
        ".ml{{font-size:10px;color:#9a3412;text-transform:uppercase;letter-spacing:0.3px}}"
        ".mv{{font-weight:700;margin-top:2px;color:#1c0a00;font-size:13px}}"
        "table{{width:100%;border-collapse:collapse;margin-top:4px}}"
        "th{{background:#fb923c;color:#fff;padding:8px 10px;font-size:12px;font-weight:700;text-align:left;border:1px solid #fdba74}}"
        "td{{padding:7px 10px;border:1px solid #fed7aa;font-size:12px;vertical-align:top}}"
        "tr:nth-child(even) td{{background:#fff7ed}}"
        ".firma{{margin-top:36px;display:grid;grid-template-columns:1fr 1fr 1fr;gap:30px}}"
        ".firma-box{{border-top:2px solid #fb923c;padding-top:6px;font-size:11px;color:#7c2d12;text-align:center}}"
        ".aviso{{background:#fff7ed;border:1px solid #fdba74;border-radius:6px;padding:10px 14px;"
        "  font-size:12px;color:#7c2d12;margin-bottom:14px}}"
        "button{{padding:8px 16px;background:#fb923c;color:#fff;border:none;border-radius:6px;"
        "  cursor:pointer;font-size:14px;margin-right:8px}}"
        "button:hover{{background:#f97316}}"
        "</style></head><body>"
        "<div class='np' style='margin-bottom:14px'>"
        "<button onclick='window.print()'>&#128438; Imprimir / Guardar PDF</button>"
        "<a href='/modulo/suministros/ordenes-pedido/{op_id}' "
        "  style='color:#fb923c;font-size:13px'>&larr; Volver a la OP</a>"
        "</div>"
        "<div class='header'>"
        "{logo_html}"
        "<div style='text-align:right'>"
        "<div class='header-title'>SOLICITUD DE COTIZACION</div>"
        "<div class='header-sub'>Ref: {num} &nbsp;&bull;&nbsp; Fecha: {fecha_doc}</div>"
        "</div>"
        "</div>"
        "<div class='subheader'>"
        "<b>Obra:</b> {obra} &nbsp;|&nbsp; "
        "<b>Sector:</b> {sector} &nbsp;|&nbsp; "
        "<b>Solicitante:</b> {sol} &nbsp;|&nbsp; "
        "<b>Criticidad:</b> {crit}"
        "</div>"
        "<div class='aviso'>"
        "Estimado proveedor, les solicitamos cotizaci&oacute;n para los siguientes materiales. "
        "Por favor completar precio unitario, subtotal y condiciones, y devolver este documento por email o WhatsApp."
        "{obs_div}"
        "</div>"
        "<table>"
        "<thead><tr>"
        "<th style='width:36px'>#</th>"
        "<th style='width:80px'>Codigo</th>"
        "<th>Descripcion / Especificacion</th>"
        "<th style='width:60px;text-align:center'>Cant. barras</th>"
        "<th style='width:70px;text-align:center'>Largo (m)</th>"
        "<th style='width:70px;text-align:center'>Peso (kg)</th>"
        "<th style='width:90px;text-align:center'>Fecha necesidad</th>"
        "<th style='width:100px;text-align:right'>P. Unitario ($)</th>"
        "<th style='width:100px;text-align:right'>Subtotal ($)</th>"
        "</tr></thead>"
        "<tbody>{filas}</tbody>"
        "<tfoot>"
        "<tr><td colspan='7' style='text-align:right;font-weight:700;background:#fff7ed'>TOTAL</td>"
        "<td style='background:#fff7ed'></td>"
        "<td style='font-weight:700;background:#fff7ed'></td></tr>"
        "</tfoot>"
        "</table>"
        "<div class='firma'>"
        "<div class='firma-box'>Condiciones de pago propuestas</div>"
        "<div class='firma-box'>Plazo de entrega estimado</div>"
        "<div class='firma-box'>Firma y sello del proveedor</div>"
        "</div>"
        "</body></html>"
    ).format(
        num=_e(row[1]), op_id=op_id, fecha_doc=fecha_doc,
        logo_html=logo_html, obra=_e(row[4]) or "—",
        sector=_e(row[5]) or "—", sol=_e(row[3]), crit=_e(row[6]),
        obs_div="<br><b>Observaciones:</b> {}".format(_e(row[7])) if row[7] else "",
        filas=filas)


# ══════════════════════════════════════════════════════════════
#  ORDENES DE COMPRA (OC)
# ══════════════════════════════════════════════════════════════

@suministros_bp.route("/ordenes-compra/nueva/<int:op_id>", methods=["GET", "POST"])
def oc_nueva(op_id):
    db = get_db()
    _ensure_tables(db)

    op = db.execute(
        "SELECT id, COALESCE(numero,''), solicitante, COALESCE(obra,''), estado "
        "FROM ordenes_pedido WHERE id=?", (op_id,)
    ).fetchone()
    if not op:
        return _page("Error", "<p>OP no encontrada.</p>"), 404
    if op[4] == "Cancelada":
        return _page("Error", "<p>Esta OP esta cancelada.</p>"), 400

    items_op = db.execute(
        "SELECT io.id, io.descripcion, io.cantidad, COALESCE(io.unidad,'barra'),"
        " COALESCE(io.largo,6.0), COALESCE(io.peso_kg,0), COALESCE(io.articulo_id,0),"
        " COALESCE(a.codigo,'')"
        " FROM items_op io LEFT JOIN articulos_sum a ON a.id=io.articulo_id"
        " WHERE io.op_id=? ORDER BY io.id",
        (op_id,)
    ).fetchall()
    error = ""

    if request.method == "POST":
        proveedor     = (request.form.get("proveedor") or "").strip()
        cond_pago     = (request.form.get("condiciones_pago") or "").strip()
        plazo         = (request.form.get("plazo_entrega") or "").strip()
        obs_oc        = (request.form.get("observaciones") or "").strip()
        moneda        = (request.form.get("moneda") or "$").strip()
        precios       = request.form.getlist("precio_unitario[]")
        unids_precio  = request.form.getlist("unidad_precio[]")
        cantidades_oc = request.form.getlist("cantidad_oc[]")

        if not proveedor:
            error = "El proveedor es obligatorio."
        else:
            cur = db.execute(
                "INSERT INTO ordenes_compra (op_id,proveedor,condiciones_pago,plazo_entrega,observaciones,estado,moneda)"
                " VALUES (?,?,?,?,?,'Envio OC',?)",
                (op_id, proveedor, cond_pago, plazo, obs_oc, moneda))
            oc_id = int(getattr(cur, "lastrowid", 0) or 0)
            if oc_id <= 0:
                oc_id = int((db.execute("SELECT MAX(id) FROM ordenes_compra").fetchone() or [0])[0])
            db.execute("UPDATE ordenes_compra SET numero=? WHERE id=?", ("OC-{:04d}".format(oc_id), oc_id))

            for idx, item in enumerate(items_op):
                try:
                    precio = float((precios[idx] if idx < len(precios) else "0").replace(",", "."))
                except Exception:
                    precio = 0
                try:
                    cant_oc = float((cantidades_oc[idx] if idx < len(cantidades_oc) else str(item[2])).replace(",", "."))
                except Exception:
                    cant_oc = float(item[2])
                u_precio = (unids_precio[idx] if idx < len(unids_precio) else "barra").strip() or "barra"
                largo_oc = float(item[4]) if item[4] else None
                peso_per_bar = (float(item[5]) / float(item[2])) if (item[5] and item[2] and float(item[2]) > 0) else 0
                peso_oc = round(peso_per_bar * cant_oc, 4) if peso_per_bar else None
                db.execute(
                    "INSERT INTO items_oc (oc_id,descripcion,cantidad,unidad,precio_unitario,unidad_precio,articulo_id,largo,peso_kg) VALUES (?,?,?,?,?,?,?,?,?)",
                    (oc_id, item[1], cant_oc, item[3], precio, u_precio,
                     item[6] if item[6] else None, largo_oc, peso_oc))
            db.execute("UPDATE ordenes_pedido SET estado='Con OC' WHERE id=?", (op_id,))
            db.commit()
            return redirect("/modulo/suministros/ordenes-compra/{}".format(oc_id))

    rows_items = "".join(
        "<tr class='oc-item' data-cb='{cb}' data-l='{lraw}' data-p='{praw}'>"
        "<td style='color:#7c2d12;font-size:12px'><b>{cod}</b></td>"
        "<td>{d}</td>"
        "<td style='text-align:right'><b>{cb}</b></td>"
        "<td style='text-align:right'>{l}</td>"
        "<td style='text-align:right'>{p}</td>"
        "<td><input class='cant-oc' name='cantidad_oc[]' type='number' step='0.01' min='0.01' value='{c}' style='width:90px' required></td>"
        "<td><select class='unidad-precio' name='unidad_precio[]' style='width:90px'>"
        "<option value='barra'>$/barra</option>"
        "<option value='kg'>$/kg</option>"
        "<option value='ml'>$/ml</option>"
        "<option value='u'>$/U</option>"
        "</select></td>"
        "<td><input class='precio-unit' name='precio_unitario[]' type='number' step='0.01' min='0' value='0' style='width:110px'></td>"
        "<td style='text-align:right'><b class='imp-row'>0.00</b></td>"
        "</tr>".format(
            cod=_e(r[7]) if r[7] else ("ID:{}".format(r[6]) if r[6] and int(r[6]) > 0 else '\u2014'),
            d=_e(r[1]),
            cb=int(round(float(r[2]))) if r[2] else 0,
            l=_fmt(r[4]), p=_fmt(r[5]),
            c=_fmt(r[2]), u=_e(r[3]),
            lraw=float(r[4]) if r[4] is not None else 0,
            praw=float(r[5]) if r[5] is not None else 0)
        for r in items_op
    ) or "<tr><td colspan='9'>Sin items en la OP.</td></tr>"

    # Datalist de proveedores para autocomplete
    _provs = db.execute("SELECT nombre FROM proveedores WHERE activo=1 ORDER BY nombre").fetchall()
    prov_dl = "".join("<option value='{n}'>".format(n=_e(r[0])) for r in _provs)

    body = (
        "<h2>Armar Orden de Compra &mdash; {op_num}</h2>"
        "<a class='b gr' href='/modulo/suministros/ordenes-pedido/{op_id}'>Volver a OP</a>"
        "{err}"
        "<datalist id='prov-dl'>{prov_dl}</datalist>"
        "<form method='post'>"
        "<div class='card'><div class='fg'>"
        "<div><label>Proveedor *</label><input name='proveedor' required list='prov-dl' autocomplete='off' placeholder='Escribir para buscar...'></div>"
        "<div><label>Condiciones de pago</label><input name='condiciones_pago' placeholder='Ej: 30 dias'></div>"
        "<div><label>Plazo de entrega</label><input name='plazo_entrega' placeholder='Ej: 5 dias habiles'></div>"
        "<div><label>Moneda</label>"
        "<select name='moneda' style='width:100px'>"
        "<option value='$' selected>$ (Pesos)</option>"
        "<option value='USD'>USD (Dolar)</option>"
        "</select></div>"
        "</div>"
        "<div style='margin-top:8px'><label>Observaciones</label><textarea name='observaciones' rows='2'></textarea></div>"
        "</div>"
        "<div class='card'><h3>Items y precios unitarios</h3>"
        "<div style='overflow-x:auto'><table>"
        "<thead><tr>"
        "<th>Codigo</th><th>Descripcion</th>"
        "<th style='text-align:right'>Cant.<br>barras</th>"
        "<th style='text-align:right'>Largo<br>(m)</th>"
        "<th style='text-align:right'>Peso<br>(kg)</th>"
        "<th>Cant. OC</th><th>Tipo precio</th><th>Precio unitario</th><th>Total</th>"
        "</tr></thead>"
        "<tbody>{rows}</tbody>"
        "</table></div>"
        "<div style='margin-top:10px;text-align:right;font-weight:700;color:#7c2d12'>Importe OC: <span id='imp-oc-total'>0.00</span></div>"
        "</div>"
        "<script>"
        "(function(){{"
        "  function fnum(v){{v=(v||'').toString().replace(',', '.'); var n=parseFloat(v); return isNaN(n)?0:n;}}"
        "  function fmt(n){{return (Math.round(n*100)/100).toFixed(2);}}"
        "  function calcRow(tr){{"
        "    var cb=fnum(tr.getAttribute('data-cb'));"
        "    var l=fnum(tr.getAttribute('data-l'));"
        "    var p=fnum(tr.getAttribute('data-p'));"
        "    var cant=fnum((tr.querySelector('.cant-oc')||{{value:'0'}}).value);"
        "    var tipo=(tr.querySelector('.unidad-precio')||{{value:'barra'}}).value;"
        "    var pu=fnum((tr.querySelector('.precio-unit')||{{value:'0'}}).value);"
        "    var base=0;"
        "    if(tipo==='kg'){{ base=(cb>0 ? (p/cb)*cant : 0); }}"
        "    else if(tipo==='ml'){{ base=l*cant; }}"
        "    else{{ base=cant; }}"
        "    var imp=base*pu;"
        "    var out=tr.querySelector('.imp-row'); if(out) out.textContent=fmt(imp);"
        "    return imp;"
        "  }}"
        "  function calcAll(){{"
        "    var mon = (document.querySelector(\"select[name='moneda']\")||{{value:'$'}}).value;"
        "    var total=0;"
        "    document.querySelectorAll('tr.oc-item').forEach(function(tr){{ total += calcRow(tr); }});"
        "    var out=document.getElementById('imp-oc-total');"
        "    if(out) out.textContent=mon + ' ' + fmt(total);"
        "  }}"
        "  document.addEventListener('input', function(ev){{"
        "    if(ev.target.closest('tr.oc-item') || ev.target.name==='moneda') calcAll();"
        "  }});"
        "  document.addEventListener('change', function(ev){{"
        "    if(ev.target.closest('tr.oc-item') || ev.target.name==='moneda') calcAll();"
        "  }});"
        "  calcAll();"
        "}})();"
        "</script>"
        "<button class='b tl'>Generar OC &rarr; Envio OC</button>"
        "</form>"
    ).format(
        op_num=_e(op[1]), op_id=op_id,
        err="<div class='err'>{}</div>".format(_e(error)) if error else "",
        prov_dl=prov_dl, rows=rows_items)
    return _page("Armar OC", body)


@suministros_bp.route("/ordenes-compra/<int:oc_id>/editar", methods=["GET", "POST"])
def oc_editar(oc_id):
    db = get_db()
    _ensure_tables(db)
    oc = db.execute(
        "SELECT id, COALESCE(numero,''), COALESCE(proveedor,''), COALESCE(condiciones_pago,''),"
        " COALESCE(plazo_entrega,''), COALESCE(observaciones,''), COALESCE(moneda,'$')"
        " FROM ordenes_compra WHERE id=?", (oc_id,)
    ).fetchone()
    if not oc:
        return _page("Error", "<p>OC no encontrada.</p>"), 404

    error = ""
    if request.method == "POST":
        proveedor = (request.form.get("proveedor") or "").strip()
        if not proveedor:
            error = "El proveedor es requerido."
        else:
            db.execute(
                "UPDATE ordenes_compra SET numero=?, proveedor=?, condiciones_pago=?, plazo_entrega=?, moneda=?, observaciones=? WHERE id=?",
                ((request.form.get("numero") or "").strip() or None,
                 proveedor,
                 (request.form.get("condiciones_pago") or "").strip() or None,
                 (request.form.get("plazo_entrega") or "").strip() or None,
                 (request.form.get("moneda") or "$"),
                 (request.form.get("observaciones") or "").strip() or None,
                 oc_id))
            # Actualizar items
            iids  = request.form.getlist("iid[]")
            icants = request.form.getlist("icant[]")
            iprecios = request.form.getlist("iprecio[]")
            ilargos = request.form.getlist("ilargo[]")
            for iid, icant, iprecio, ilargo in zip(iids, icants, iprecios, ilargos):
                try:
                    db.execute(
                        "UPDATE items_oc SET cantidad=?, precio_unitario=?, largo=? WHERE id=? AND oc_id=?",
                        (float(icant) if icant else None,
                         float(iprecio) if iprecio else None,
                         float(ilargo) if ilargo else None,
                         int(iid), oc_id))
                except (ValueError, TypeError):
                    pass
            db.commit()
            return redirect("/modulo/suministros/ordenes-compra/{}".format(oc_id))

    items = db.execute(
        "SELECT ic.id, ic.descripcion, COALESCE(ic.cantidad,''), ic.unidad,"
        " COALESCE(ic.precio_unitario,''), COALESCE(ic.unidad_precio,'barra'),"
        " COALESCE(ic.largo,''), COALESCE(ic.peso_kg,''), COALESCE(a.codigo,'')"
        " FROM items_oc ic LEFT JOIN articulos_sum a ON a.id=ic.articulo_id"
        " WHERE ic.oc_id=? ORDER BY ic.id", (oc_id,)
    ).fetchall()

    _provs = db.execute("SELECT nombre FROM proveedores WHERE activo=1 ORDER BY nombre").fetchall()
    prov_opts = "".join(
        "<option value='{n}' {sel}>{n}</option>".format(
            n=_e(r[0]), sel="selected" if r[0] == oc[2] else "")
        for r in _provs)

    filas_items = "".join(
        "<tr>"
        "<td><input type='hidden' name='iid[]' value='{iid}'>"
        "<span style='font-size:11px;font-weight:700;color:#7c2d12'>{cod}</span></td>"
        "<td>{desc}</td>"
        "<td><input name='icant[]' type='number' step='0.01' min='0' value='{cant}' style='width:80px'></td>"
        "<td style='font-size:12px;color:#64748b'>{u}</td>"
        "<td><input name='ilargo[]' type='number' step='0.01' min='0' value='{largo}' style='width:72px'></td>"
        "<td style='font-size:12px;color:#64748b'>{up}</td>"
        "<td><input name='iprecio[]' type='number' step='0.01' min='0' value='{precio}' style='width:90px'></td>"
        "</tr>".format(
            iid=r[0],
            cod=_e(r[8]) if r[8] else "—",
            desc=_e(r[1]), cant=_fmt(r[2]) if r[2] != '' else '',
            u=_e(r[3]), largo=_fmt(r[6]) if r[6] != '' else '',
            up=_e(r[5]), precio=_fmt(r[4]) if r[4] != '' else '')
        for r in items
    ) or "<tr><td colspan='7'>Sin items.</td></tr>"

    body = (
        "<h2>Editar OC {num}</h2>"
        "<a class='b gr' href='/modulo/suministros/ordenes-compra/{id}'>Cancelar</a>"
        "{err}"
        "<form method='post'>"
        "<div class='card'>"
        "<div class='fg'>"
        "<div><label>Numero OC</label><input name='numero' value='{num}'></div>"
        "<div><label>Proveedor *</label><select name='proveedor' required>{prov_opts}</select></div>"
        "<div><label>Condiciones de pago</label><input name='condiciones_pago' value='{cond}'></div>"
        "<div><label>Plazo de entrega</label><input name='plazo_entrega' value='{plazo}'></div>"
        "<div><label>Moneda</label>"
        "<select name='moneda'>"
        "<option value='$' {sel_ars}>$ (Pesos)</option>"
        "<option value='USD' {sel_usd}>USD (Dolar)</option>"
        "</select></div>"
        "</div>"
        "<div style='margin-top:8px'>"
        "<label>Observaciones</label>"
        "<textarea name='observaciones' rows='2' style='width:100%'>{obs}</textarea>"
        "</div>"
        "</div>"
        "<div class='card'><h3>Items</h3>"
        "<div style='overflow-x:auto'>"
        "<table class='hl'><thead><tr>"
        "<th>Codigo</th><th>Descripcion</th><th>Cantidad</th><th>Unidad</th>"
        "<th>Largo (m)</th><th>Tipo precio</th><th>Precio Unitario</th>"
        "</tr></thead><tbody>{filas}</tbody></table>"
        "</div></div>"
        "<button class='b tl'>Guardar cambios</button>"
        "</form>"
    ).format(
        id=oc_id, num=_e(oc[1]),
        err="<div class='err'>{}</div>".format(_e(error)) if error else "",
        prov_opts=prov_opts,
        cond=_e(oc[3]), plazo=_e(oc[4]),
        sel_ars="selected" if oc[6] == "$" else "",
        sel_usd="selected" if oc[6] == "USD" else "",
        obs=_e(oc[5]), filas=filas_items)
    return _page("Editar OC {}".format(oc[1]), body)


@suministros_bp.route("/ordenes-compra/<int:oc_id>/eliminar", methods=["POST"])
def oc_eliminar(oc_id):
    db = get_db()
    _ensure_tables(db)
    db.execute("DELETE FROM items_oc WHERE oc_id=?", (oc_id,))
    db.execute("DELETE FROM ordenes_compra WHERE id=?", (oc_id,))
    db.commit()
    return redirect("/modulo/suministros/ordenes-compra")


@suministros_bp.route("/kanban")
def kanban():
    db = get_db()
    _ensure_tables(db)

    # Flujo unificado de compras en orden lógico
    # tipo "op" = se muestran OPs en ese estado, tipo "oc" = se muestran OCs
    FLUJO = [
        ("Pendiente",        "op"),
        ("Aprobada",         "op"),
        ("Pedido Precios",   "op"),
        ("Con OC",           "op"),
        ("Envio OC",         "oc"),
        ("OC confirmada",    "oc"),
        ("Para Despachar",   "oc"),
        ("Recibido Parcial", "oc"),
        ("Recibido OK",      "oc"),
        ("Cerrada",          "op"),
        ("Cancelada",        "op"),
    ]

    ops = db.execute(
        "SELECT id, COALESCE(numero,''), COALESCE(obra,''), COALESCE(observaciones,''), estado, criticidad"
        " FROM ordenes_pedido ORDER BY id DESC"
    ).fetchall()

    ocs = db.execute(
        "SELECT oc.id, COALESCE(oc.numero,''), oc.proveedor, oc.estado, oc.fecha,"
        " COALESCE(op.numero,''), COALESCE(op.obra,'')"
        " FROM ordenes_compra oc LEFT JOIN ordenes_pedido op ON op.id=oc.op_id ORDER BY oc.id DESC"
    ).fetchall()

    op_por_estado = {}
    for r in ops:
        op_por_estado.setdefault(str(r[4]), []).append(r)

    oc_por_estado = {}
    for r in ocs:
        oc_por_estado.setdefault(str(r[3]), []).append(r)

    def op_card(r):
        crit = str(r[5] or "Normal")
        crit_color = {"Alta": "#f97316", "Urgente": "#dc2626"}.get(crit, "#64748b")
        return (
            "<div style='background:#fff;border-radius:10px;border:1px solid #e2e8f0;"
            "padding:10px 12px;margin-bottom:8px;box-shadow:0 1px 3px #0001'>"
            "<div style='display:flex;justify-content:space-between;align-items:center'>"
            "<a href='/modulo/suministros/ordenes-pedido/{id}' style='font-weight:700;color:#7c2d12;"
            "text-decoration:none;font-size:13px'>\U0001f4c4 OP {num}</a>"
            "<span style='font-size:11px;color:{cc};font-weight:700'>{crit}</span></div>"
            "<div style='font-size:12px;color:#374151;margin-top:4px'>{obra}</div>"
            "<div style='font-size:11px;color:#6b7280;margin-top:2px;white-space:nowrap;"
            "overflow:hidden;text-overflow:ellipsis'>{obs}</div>"
            "</div>"
        ).format(id=int(r[0]), num=_e(r[1]), obra=_e(r[2]),
                 obs=_e(r[3][:60] if r[3] else ""), cc=crit_color, crit=_e(crit))

    def oc_card(r):
        return (
            "<div style='background:#fff;border-radius:10px;border:1px solid #bfdbfe;"
            "padding:10px 12px;margin-bottom:8px;box-shadow:0 1px 3px #0001'>"
            "<div style='display:flex;justify-content:space-between;align-items:center'>"
            "<a href='/modulo/suministros/ordenes-compra/{id}' style='font-weight:700;color:#1e3a5f;"
            "text-decoration:none;font-size:13px'>\U0001f6d2 OC {num}</a>"
            "<span style='font-size:11px;color:#64748b'>{fecha}</span></div>"
            "<div style='font-size:12px;color:#374151;margin-top:4px;font-weight:600'>{prov}</div>"
            "<div style='font-size:11px;color:#6b7280;margin-top:2px'>{obra} &middot; OP {op}</div>"
            "</div>"
        ).format(id=int(r[0]), num=_e(r[1]), prov=_e(r[2]),
                 fecha=_e(str(r[4] or "")[:10]), obra=_e(r[6]), op=_e(r[5]))

    cols = ""
    for (e, tipo) in FLUJO:
        if tipo == "op":
            items = op_por_estado.get(e, [])
            cards = "".join(op_card(r) for r in items)
        else:
            items = oc_por_estado.get(e, [])
            cards = "".join(oc_card(r) for r in items)

        if not cards:
            cards = "<div style='color:#cbd5e1;font-size:12px;font-style:italic;text-align:center;padding:8px 0'>vacío</div>"

        bg, fg = _BC.get(e, ("#f1f5f9", "#334155"))
        sep = ""
        if e == "Envio OC":
            sep = "<div style='min-width:2px;align-self:stretch;background:#e2e8f0;border-radius:2px;margin:0 2px'></div>"

        col_header = (
            "<div style='background:{bg};color:{fg};border-radius:8px 8px 0 0;padding:8px 12px;"
            "font-weight:700;font-size:12px;display:flex;justify-content:space-between;align-items:center'>"
            "<span>{e}</span>"
            "<span style='background:{fg};color:{bg};border-radius:999px;padding:1px 8px;font-size:11px'>{n}</span>"
            "</div>"
        ).format(bg=bg, fg=fg, e=_e(e), n=len(items))

        col_body = (
            "<div style='background:#f8fafc;border:1px solid #e2e8f0;border-top:none;"
            "border-radius:0 0 8px 8px;padding:8px;min-height:60px;max-height:65vh;overflow-y:auto'>"
            + cards + "</div>"
        )

        cols += sep + "<div style='min-width:200px;max-width:230px;flex:0 0 auto'>" + col_header + col_body + "</div>"

    body = (
        "<h2>Kanban de Compras</h2>"
        "<a class='b gr' href='/modulo/suministros'>Dashboard</a>"
        "<a class='b bl' href='/modulo/suministros/ordenes-pedido'>Ver OPs</a>"
        "<a class='b pu' href='/modulo/suministros/ordenes-compra'>Tablero OC</a>"
        "<p style='color:#64748b;font-size:13px;margin:8px 0 14px'>"
        "\U0001f4c4 Orden de Pedido &nbsp;|&nbsp; \U0001f6d2 Orden de Compra &nbsp;&mdash;&nbsp;"
        "El separador vertical indica el inicio de la gesti\u00f3n de OC.</p>"
        "<div style='display:flex;gap:10px;overflow-x:auto;padding-bottom:16px;align-items:flex-start'>"
    ) + cols + "</div>"
    return _page("Kanban Compras", body)


@suministros_bp.route("/ordenes-compra/cerradas")
def oc_cerradas():
    db = get_db()
    _ensure_tables(db)
    rows = db.execute(
        "SELECT oc.id, COALESCE(oc.numero,''), oc.fecha, oc.proveedor,"
        " COALESCE(op.numero,''), COALESCE(op.obra,''), COALESCE(oc.moneda,'$'),"
        " COALESCE(oc.fecha_recepcion,''), COALESCE(oc.fecha_pago,'')"
        " FROM ordenes_compra oc LEFT JOIN ordenes_pedido op ON op.id=oc.op_id"
        " WHERE oc.estado='Pagado' ORDER BY oc.id DESC"
    ).fetchall()
    filas = "".join(
        "<tr>"
        "<td><a href='/modulo/suministros/ordenes-compra/{id}'><b>{num}</b></a></td>"
        "<td>{op}</td><td>{obra}</td><td>{prov}</td>"
        "<td>{fecha}</td><td>{frec}</td><td>{fpago}</td>"
        "<td><a class='b gr sm' href='/modulo/suministros/ordenes-compra/{id}/pdf' target='_blank'>PDF</a></td>"
        "</tr>".format(
            id=int(r[0]), num=_e(r[1]), op=_e(r[4]), obra=_e(r[5]),
            prov=_e(r[3]), fecha=_e(str(r[2] or "")[:10]),
            frec=_e(str(r[7] or "")[:10]) or "\u2014",
            fpago=_e(str(r[8] or "")[:10]) or "\u2014")
        for r in rows
    ) or "<tr><td colspan='8'>Sin OC cerradas.</td></tr>"
    body = (
        "<h2>&#10003; OC Cerradas / Pagadas</h2>"
        "<a class='b gr' href='/modulo/suministros/ordenes-compra'>Tablero OC</a>"
        "<a class='b am sm' href='/modulo/suministros'>Dashboard</a>"
        "<div class='card' style='overflow-x:auto;margin-top:10px'>"
        "<table class='hl'><thead><tr>"
        "<th>N\u00ba OC</th><th>OP</th><th>Obra</th><th>Proveedor</th>"
        "<th>Fecha emisi\u00f3n</th><th>F. Recepci\u00f3n</th><th>F. Pago</th><th></th>"
        "</tr></thead><tbody>{filas}</tbody></table></div>"
    ).format(filas=filas)
    return _page("OC Cerradas", body)


@suministros_bp.route("/ordenes-compra")
def oc_tablero():
    db = get_db()
    _ensure_tables(db)

    rows = db.execute(
        "SELECT oc.id, COALESCE(oc.numero,''), oc.fecha, oc.proveedor,"
        " oc.estado, COALESCE(oc.fecha_despacho,''), COALESCE(oc.fecha_recepcion,''),"
        " COALESCE(op.numero,''), COALESCE(op.obra,''), COALESCE(oc.moneda,'$'),"
        " COALESCE((SELECT SUM(ic.cantidad*ic.precio_unitario) FROM items_oc ic WHERE ic.oc_id=oc.id),0)"
        " FROM ordenes_compra oc"
        " LEFT JOIN ordenes_pedido op ON op.id=oc.op_id"
        " WHERE oc.estado != 'Pagado'"
        " ORDER BY oc.id DESC"
    ).fetchall()

    estado_f = request.args.get("estado", "")
    obra_f   = request.args.get("obra", "")
    prov_f   = request.args.get("proveedor", "")
    q        = request.args.get("q", "").strip()
    try:
        page = max(1, int(request.args.get("page", 1) or 1))
    except (ValueError, TypeError):
        page = 1

    if estado_f:
        rows = [r for r in rows if r[4] == estado_f]
    if obra_f:
        rows = [r for r in rows if (r[8] or "") == obra_f]
    if prov_f:
        rows = [r for r in rows if (r[3] or "") == prov_f]
    if q:
        rows = [r for r in rows if q.lower() in (r[1] or "").lower()]

    per_page    = 20
    total       = len(rows)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page        = min(page, total_pages)
    offset      = (page - 1) * per_page
    rows_page   = rows[offset:offset + per_page]
    def _pg_url_oc(p):
        parts = []
        if estado_f: parts.append("estado=" + estado_f)
        if obra_f:   parts.append("obra=" + obra_f)
        if prov_f:   parts.append("proveedor=" + prov_f)
        if q:        parts.append("q=" + q)
        if p > 1:    parts.append("page=" + str(p))
        return "?" + "&".join(parts) if parts else "?"
    pag_info  = "Mostrando {}-{} de {}".format(offset + 1, min(offset + per_page, total), total)
    pag_links = ""
    if page > 1:
        pag_links += "<a href='{}' style='color:#64748b;text-decoration:none'>&laquo; Ant</a> ".format(_pg_url_oc(page - 1))
    for _p in range(max(1, page - 3), min(total_pages, page + 3) + 1):
        _sty = "font-weight:700;color:#f97316" if _p == page else "color:#64748b"
        pag_links += "<a href='{}' style='{}; text-decoration:none; padding:2px 6px'>{}</a>".format(_pg_url_oc(_p), _sty, _p)
    if page < total_pages:
        pag_links += " <a href='{}' style='color:#64748b;text-decoration:none'>Sig &raquo;</a>".format(_pg_url_oc(page + 1))
    paginador_oc = ("<div style='display:flex;align-items:center;gap:12px;padding:8px 0;font-size:13px;flex-wrap:wrap'>"
        "<span style='color:#64748b'>{}</span>{}</div>").format(pag_info, pag_links)

    opts_estado = "<option value=''>Todos los estados</option>" + "".join(
        "<option value='{e}' {sel}>{e}</option>".format(
            e=_e(e), sel="selected" if e == estado_f else "")
        for e in ESTADOS_OC)

    # Listas únicas para filtros
    all_rows_raw = db.execute(
        "SELECT DISTINCT COALESCE(op.obra,'') FROM ordenes_compra oc LEFT JOIN ordenes_pedido op ON op.id=oc.op_id ORDER BY 1"
    ).fetchall()
    obras_uniq = [r[0] for r in all_rows_raw if r[0]]
    opts_obra = "<option value=''>Todas las obras</option>" + "".join(
        "<option value='{o}' {sel}>{o}</option>".format(o=_e(o), sel="selected" if o == obra_f else "")
        for o in obras_uniq)

    provs_uniq_raw = db.execute("SELECT DISTINCT proveedor FROM ordenes_compra ORDER BY 1").fetchall()
    provs_uniq = [r[0] for r in provs_uniq_raw if r[0]]
    opts_prov = "<option value=''>Todos los proveedores</option>" + "".join(
        "<option value='{p}' {sel}>{p}</option>".format(p=_e(p), sel="selected" if p == prov_f else "")
        for p in provs_uniq)

    def responsable(estado):
        if estado == "Aprobada":
            return "TALLER EEMM"
        if estado in ("Pedido Precios", "Envio OC", "OC confirmada", "Para Despachar"):
            return "COMPRAS"
        if estado in ("Recibido Parcial",):
            return "TALLER EEMM"
        if estado == "Recibido OK":
            return "PAGOS"
        return "—"

    from datetime import date as _dt_hoy, datetime as _dtm
    hoy = _dt_hoy.today()

    def semaforo_dias(fecha_str):
        """Devuelve (dias, html_badge) desde la fecha de emisión."""
        try:
            f = _dtm.strptime(str(fecha_str or "")[:10], "%Y-%m-%d").date()
            dias = (hoy - f).days
        except Exception:
            return "—", "<span style='color:#64748b'>—</span>"
        if dias <= 7:
            color, bg = "#166534", "#dcfce7"
        elif dias <= 14:
            color, bg = "#92400e", "#fef3c7"
        else:
            color, bg = "#991b1b", "#fee2e2"
        return dias, ("<span style='background:{bg};color:{c};padding:2px 8px;border-radius:999px;"
                      "font-size:12px;font-weight:700'>{d}d</span>").format(bg=bg, c=color, d=dias)

    filas = "".join(
        (lambda dias_val, sem_badge: (
        "<tr>"
        "<td><a href='/modulo/suministros/ordenes-compra/{id}' style='font-weight:700;color:#7c2d12'>{num}</a>"
        "<div style='font-size:11px;color:#64748b;margin-top:2px'>OP: {op}</div></td>"
        "<td><b>{prov}</b><div style='font-size:11px;color:#64748b;margin-top:2px'>{obra}</div></td>"
        "<td>{est}</td>"
        "<td>{dias_badge}</td>"
        "<td style='font-size:12px'><div>&#128666; {fd}</div><div style='color:#64748b'>&#10003; {fr}</div></td>"
        "<td style='text-align:right;white-space:nowrap'><b>{mon} {monto}</b></td>"
        "<td style='font-size:12px;color:#64748b'>{resp}</td>"
        "<td style='white-space:nowrap'>"
        "<a class='b bl sm' href='/modulo/suministros/ordenes-compra/{id}'>Ver</a> "
        "<a class='b gr sm' href='/modulo/suministros/ordenes-compra/{id}/pdf' target='_blank'>PDF</a> "
        "<form method='post' action='/modulo/suministros/ordenes-compra/{id}/eliminar' style='display:inline' onsubmit=\"return confirm('¿Eliminar esta OC?')\">"
        "<button class='b rd sm' type='submit'>X</button>"
        "</form>"
        "</td></tr>"
        ).format(
            id=int(r[0]), num=_e(r[1]),
            op=_e(r[7]), prov=_e(r[3]), obra=_e(r[8]),
            est=_badge(r[4]), dias_badge=sem_badge,
            fd=_e(r[5]) or "—", fr=_e(r[6]) or "—",
            mon=_e(r[9]), monto=_fmt(r[10]),
            resp=responsable(str(r[4]))
        ))(*semaforo_dias(r[2]))
        for r in rows_page
    ) or "<tr><td colspan='8'>Sin ordenes de compra.</td></tr>"

    # Contadores por estado
    conteos = {}
    for (e,) in db.execute("SELECT estado FROM ordenes_compra").fetchall():
        conteos[e] = conteos.get(e, 0) + 1
    pills = " ".join(
        "<a href='/modulo/suministros/ordenes-compra?estado={e}' style='text-decoration:none'>{badge} <b>{n}</b></a>".format(
            e=_e(e), badge=_badge(e), n=conteos.get(e, 0))
        for e in ESTADOS_OC)

    body = (
        "<h2>Tablero de Ordenes de Compra</h2>"
        "<a class='b tl' href='/modulo/suministros/ordenes-pedido/nueva'>+ Nueva OP</a>"
        "<a class='b gr' href='/modulo/suministros'>Dashboard</a>"
        "<a class='b pu' href='/modulo/suministros/kanban'>Kanban</a>"
        "<div class='card' style='margin-bottom:10px'><b>Por estado:</b>&nbsp;&nbsp;{pills}"
        "&nbsp;&nbsp;<a href='/modulo/suministros/ordenes-compra' style='font-size:13px;color:#64748b'>Ver todas</a>"
        "</div>"
        "<form method='get' style='margin-bottom:10px;display:flex;gap:8px;flex-wrap:wrap;align-items:center'>"
        "<select name='estado' onchange='this.form.submit()' style='width:auto;padding:8px'>{opts_estado}</select>"
        "<select name='obra' onchange='this.form.submit()' style='width:auto;padding:8px'>{opts_obra}</select>"
        "<select name='proveedor' onchange='this.form.submit()' style='width:auto;padding:8px'>{opts_prov}</select>"
        "<input name='q' value='{q_val}' placeholder='Buscar N\u00ba OC...' style='width:160px;padding:8px'>"
        "<button type='submit' class='b or sm'>Buscar</button>"
        "<a class='b gr sm' href='/modulo/suministros/ordenes-compra'>Limpiar</a>"
        "</form>"
        "<div class='card' style='overflow-x:auto'><table class='hl'><thead><tr>"
        "<th>OC / OP</th><th>Proveedor / Obra</th><th>Estado</th><th>Antig.</th>"
        "<th>Despacho / Recepci\u00f3n</th><th style='text-align:right'>Monto</th>"
        "<th>Responsable</th><th></th>"
        "</tr></thead><tbody>{filas}</tbody></table></div>"
        "{paginador_oc}"
    ).format(pills=pills, opts_estado=opts_estado, opts_obra=opts_obra, opts_prov=opts_prov,
             q_val=_e(q), filas=filas, paginador_oc=paginador_oc)
    return _page("Tablero OC", body)


@suministros_bp.route("/ordenes-compra/<int:oc_id>", methods=["GET", "POST"])
def oc_ver(oc_id):
    db = get_db()
    _ensure_tables(db)

    if request.method == "POST":
        accion = (request.form.get("accion") or "").strip()
        if accion == "confirmada":
            db.execute("UPDATE ordenes_compra SET estado='OC confirmada' WHERE id=?", (oc_id,))
            db.commit()
        elif accion == "despacho":
            fecha_d = (request.form.get("fecha_despacho") or "").strip()
            db.execute(
                "UPDATE ordenes_compra SET estado='Para Despachar', fecha_despacho=? WHERE id=?",
                (fecha_d or None, oc_id))
            db.commit()
        elif accion == "pagar":
            from datetime import date as _d_pag
            db.execute(
                "UPDATE ordenes_compra SET estado='Pagado', fecha_pago=? WHERE id=? AND estado='Recibido OK'",
                (_d_pag.today().isoformat(), oc_id))
            db.commit()
        return redirect("/modulo/suministros/ordenes-compra/{}".format(oc_id))

    oc = db.execute(
        "SELECT oc.id, COALESCE(oc.numero,''), oc.fecha, oc.proveedor,"
        " COALESCE(oc.condiciones_pago,''), COALESCE(oc.plazo_entrega,''),"
        " COALESCE(oc.observaciones,''), oc.estado,"
        " COALESCE(oc.fecha_despacho,''), COALESCE(oc.fecha_recepcion,''),"
        " oc.op_id, COALESCE(op.numero,''), COALESCE(op.obra,''),"
        " COALESCE(oc.remito_proveedor,''), COALESCE(oc.moneda,'$')"
        " FROM ordenes_compra oc"
        " LEFT JOIN ordenes_pedido op ON op.id=oc.op_id"
        " WHERE oc.id=?", (oc_id,)
    ).fetchone()
    if not oc:
        return _page("Error", "<p>OC no encontrada.</p><a class='b gr' href='/modulo/suministros/ordenes-compra'>Volver</a>"), 404

    items = db.execute(
        "SELECT ic.descripcion, ic.cantidad, ic.unidad, ic.precio_unitario, COALESCE(ic.cantidad_recibida,0),"
        " COALESCE(ic.unidad_precio,'barra'), COALESCE(a.codigo,''), ic.articulo_id,"
        " COALESCE(ic.largo,''), COALESCE(ic.peso_kg,'')"
        " FROM items_oc ic LEFT JOIN articulos_sum a ON a.id=ic.articulo_id"
        " WHERE ic.oc_id=? ORDER BY ic.id", (oc_id,)
    ).fetchall()

    moneda = oc[14]

    def _sub_oc(r):
        cant = float(r[1] or 0)
        pu   = float(r[3] or 0)
        up   = (r[5] or 'barra')
        peso = float(r[9]) if r[9] != '' else 0
        largo = float(r[8]) if r[8] != '' else 0
        if up == 'kg':   return pu * peso
        elif up == 'ml': return pu * largo * cant
        return pu * cant

    total = sum(_sub_oc(r) for r in items)

    filas_items = "".join(
        "<tr>"
        "<td style='color:#7c2d12;font-size:12px'><b>{cod}</b></td>"
        "<td>{d}</td>"
        "<td style='text-align:right'>{c}</td>"
        "<td style='text-align:right;font-size:12px;color:#64748b'>{l}</td>"
        "<td style='text-align:right;font-size:12px;color:#64748b'>{p}</td>"
        "<td>{u}</td>"
        "<td style='text-align:center;font-size:12px;color:#7c2d12'>{up}</td>"
        "<td style='text-align:right'>{mon} {pu}</td>"
        "<td style='text-align:right'>{mon} {sub}</td>"
        "<td style='text-align:right'>{cr}</td>"
        "</tr>".format(
            cod=_e(r[6]) if r[6] else ("ID:{}".format(r[7]) if r[7] else '\u2014'),
            d=_e(r[0]), c=_fmt(r[1]),
            l=_fmt(r[8]) if r[8] != '' else '\u2014',
            p=_fmt(r[9]) if r[9] != '' else '\u2014',
            u=_e(r[2]), up=_e(r[5]), mon=_e(moneda),
            pu=_fmt(r[3]), sub=_fmt(_sub_oc(r)),
            cr=_fmt(r[4]) if r[4] is not None else "\u2014")
        for r in items
    ) or "<tr><td colspan='10'>Sin items.</td></tr>"

    estado = oc[7]
    acciones = "<a class='b gr sm' href='/modulo/suministros/ordenes-compra/{id}/pdf' target='_blank'>Ver/Imprimir PDF</a>".format(id=oc_id)
    acciones += "<a class='b bl sm' href='/modulo/suministros/ordenes-compra/{id}/editar'>Editar OC</a>".format(id=oc_id)

    if estado == "Envio OC":
        mail_sub = "Orden de Compra {} - {}".format(_e(oc[1]), _e(oc[3]))
        mail_body = "Estimado proveedor, adjuntamos la OC {}. Condiciones: {}. Plazo: {}.".format(
            _e(oc[1]), _e(oc[4]), _e(oc[5]))
        acciones += "<a class='b bl sm' href='mailto:?subject={s}&body={b}'>Preparar Email</a>".format(
            s=mail_sub, b=mail_body)
        acciones += (
            "<form method='post' style='display:inline;vertical-align:middle'>"
            "<input type='hidden' name='accion' value='confirmada'>"
            "<button class='b tl sm' onclick=\"return confirm('Confirmar que el proveedor recibió la OC?')\">&#10003; Confirmar OC recibida</button>"
            "</form>")

    if estado == "OC confirmada":
        acciones += (
            "<form method='post' style='display:inline;vertical-align:middle'>"
            "<input type='hidden' name='accion' value='despacho'>"
            "<label style='font-size:12px;font-weight:600;margin-right:5px;color:#92400e'>Fecha despacho:</label>"
            "<input type='date' name='fecha_despacho' style='width:auto;display:inline;padding:7px;margin-right:4px' required>"
            "<button class='b am sm' onclick=\"return confirm('Confirmar fecha de despacho?')\">Confirmar Despacho &rarr;</button>"
            "</form>")

    if estado == "Para Despachar":
        acciones += "<a class='b gn' href='/modulo/suministros/ordenes-compra/{id}/recepcion'>Registrar Recepcion</a>".format(id=oc_id)

    if estado == "Recibido Parcial":
        acciones += "<a class='b am' href='/modulo/suministros/ordenes-compra/{id}/recepcion'>Registrar Nueva Recepcion</a>".format(id=oc_id)

    if estado == "Recibido OK":
        acciones += (
            "<form method='post' style='display:inline;vertical-align:middle'>"
            "<input type='hidden' name='accion' value='pagar'>"
            "<button class='b gn' onclick=\"return confirm('Marcar esta OC como Pagada y archivarla?')\">&#10003; Marcar Pagado</button>"
            "</form>")

    op_link = "<a href='/modulo/suministros/ordenes-pedido/{id}'>{num}</a>".format(
        id=int(oc[10]), num=_e(oc[11])) if oc[10] else "—"

    body = (
        "<h2>Orden de Compra {num}</h2>"
        "<a class='b gr' href='/modulo/suministros/ordenes-compra'>Tablero</a>"
        "<a class='b am sm' href='/modulo/suministros'>Dashboard</a>"
        "<div class='card'>"
        "<div style='display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin-bottom:12px'>"
        "<div style='background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 12px'>"
        "<div style='font-size:10px;font-weight:700;color:#9a3412;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px'>Numero</div>"
        "<div style='font-size:16px;font-weight:800;color:#1c0a00'>{num}</div></div>"
        "<div style='background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 12px'>"
        "<div style='font-size:10px;font-weight:700;color:#9a3412;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px'>Fecha</div>"
        "<div style='font-size:14px;font-weight:600;color:#1c0a00'>{fecha}</div></div>"
        "<div style='background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 12px'>"
        "<div style='font-size:10px;font-weight:700;color:#9a3412;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px'>Proveedor</div>"
        "<div style='font-size:15px;font-weight:800;color:#7c2d12'>{prov}</div></div>"
        "<div style='background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 12px'>"
        "<div style='font-size:10px;font-weight:700;color:#9a3412;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px'>Cond. pago</div>"
        "<div style='font-size:14px;font-weight:600;color:#1c0a00'>{cond}</div></div>"
        "<div style='background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 12px'>"
        "<div style='font-size:10px;font-weight:700;color:#9a3412;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px'>Plazo entrega</div>"
        "<div style='font-size:14px;font-weight:600;color:#1c0a00'>{plazo}</div></div>"
        "<div style='background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 12px'>"
        "<div style='font-size:10px;font-weight:700;color:#9a3412;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px'>Moneda</div>"
        "<div style='font-size:15px;font-weight:800;color:#1d4ed8'>{moneda}</div></div>"
        "<div style='background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 12px'>"
        "<div style='font-size:10px;font-weight:700;color:#9a3412;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px'>Estado</div>"
        "<div style='margin-top:2px'>{est}</div></div>"
        "<div style='background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 12px'>"
        "<div style='font-size:10px;font-weight:700;color:#9a3412;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px'>OP Origen</div>"
        "<div style='font-size:13px;font-weight:600;color:#1c0a00'>{op_link}<br><span style='color:#64748b;font-size:12px'>{obra}</span></div></div>"
        "<div style='background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 12px'>"
        "<div style='font-size:10px;font-weight:700;color:#9a3412;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px'>F. Despacho</div>"
        "<div style='font-size:14px;font-weight:600;color:#1c0a00'>{fd}</div></div>"
        "<div style='background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 12px'>"
        "<div style='font-size:10px;font-weight:700;color:#9a3412;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px'>F. Recepcion</div>"
        "<div style='font-size:14px;font-weight:600;color:#1c0a00'>{fr}</div></div>"
        "<div style='background:#fff7ed;border:1px solid #fed7aa;border-radius:8px;padding:10px 12px'>"
        "<div style='font-size:10px;font-weight:700;color:#9a3412;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px'>Remito Proveedor</div>"
        "<div style='font-size:14px;font-weight:600;color:#1c0a00'>{remito_prov}</div></div>"
        "</div>"
        "{obs}"
        "<div style='margin-top:12px'>{acciones}</div>"
        "</div>"
        "<div class='card'><h3>Items</h3>"
        "<div style='overflow-x:auto'>"
        "<table class='hl'><thead><tr>"
        "<th>Codigo</th><th>Descripcion</th><th style='text-align:right'>Cant.</th>"
        "<th style='text-align:right'>Largo(m)</th><th style='text-align:right'>Peso(kg)</th>"
        "<th>Unidad</th>"
        "<th style='text-align:center'>Tipo precio</th>"
        "<th style='text-align:right'>P. Unitario</th><th style='text-align:right'>Subtotal</th>"
        "<th style='text-align:right'>Cant. Recibida</th>"
        "</tr></thead><tbody>{filas}</tbody>"
        "<tfoot><tr style='font-weight:700;background:#f1f5f9'>"
        "<td colspan='8' style='text-align:right'>TOTAL {moneda}</td>"
        "<td style='text-align:right'>{moneda} {total}</td><td></td>"
        "</tr></tfoot></table></div></div>"
    ).format(
        num=_e(oc[1]), fecha=_e(str(oc[2] or "")[:10]), prov=_e(oc[3]),
        cond=_e(oc[4]) or "—", plazo=_e(oc[5]) or "—",
        op_link=op_link, obra=_e(oc[12]),
        est=_badge(estado), fd=_e(oc[8]) or "—", fr=_e(oc[9]) or "—",
        remito_prov=_e(oc[13]) or "—", moneda=_e(moneda),
        obs="<div><b>Observaciones:</b> {}</div>".format(_e(oc[6])) if oc[6] else "",
        acciones=acciones, filas=filas_items, total=_fmt(total))
    return _page("OC {}".format(oc[1]), body)


# ─── PDF de OC ────────────────────────────────────────────────

@suministros_bp.route("/ordenes-compra/<int:oc_id>/pdf")
def oc_pdf(oc_id):
    db = get_db()
    _ensure_tables(db)

    oc = db.execute(
        "SELECT oc.id, COALESCE(oc.numero,''), oc.fecha, oc.proveedor,"
        " COALESCE(oc.condiciones_pago,''), COALESCE(oc.plazo_entrega,''),"
        " COALESCE(oc.observaciones,''), oc.estado,"
        " COALESCE(op.numero,''), COALESCE(op.obra,''), COALESCE(op.solicitante,''),"
        " COALESCE(oc.moneda,'$')"
        " FROM ordenes_compra oc"
        " LEFT JOIN ordenes_pedido op ON op.id=oc.op_id"
        " WHERE oc.id=?", (oc_id,)
    ).fetchone()
    if not oc:
        return "<h3>OC no encontrada</h3>", 404

    items = db.execute(
        "SELECT ic.descripcion, ic.cantidad, ic.unidad, ic.precio_unitario,"
        " COALESCE(ic.unidad_precio,'barra'), COALESCE(a.codigo,''),"
        " ic.articulo_id, COALESCE(ic.largo,''), COALESCE(ic.peso_kg,'')"
        " FROM items_oc ic LEFT JOIN articulos_sum a ON a.id=ic.articulo_id"
        " WHERE ic.oc_id=? ORDER BY ic.id",
        (oc_id,)
    ).fetchall()

    moneda = oc[11]

    def _sub_pdf(r):
        cant  = float(r[1] or 0)
        pu    = float(r[3] or 0)
        up    = (r[4] or 'barra')
        peso  = float(r[8]) if r[8] != '' else 0
        largo = float(r[7]) if r[7] != '' else 0
        if up == 'kg':   return pu * peso
        elif up == 'ml': return pu * largo * cant
        return pu * cant

    total = sum(_sub_pdf(r) for r in items)

    filas = "".join(
        "<tr><td style='color:#7c2d12;font-size:11px'><b>{cod}</b></td><td>{d}</td>"
        "<td class='r'>{c}</td>"
        "<td class='r' style='font-size:11px;color:#64748b'>{l}</td>"
        "<td class='r' style='font-size:11px;color:#64748b'>{p}</td>"
        "<td class='r' style='color:#7c2d12;font-size:11px'>{up}</td>"
        "<td class='r'>{mon} {pu}</td><td class='r'>{mon} {sub}</td></tr>".format(
            cod=_e(r[5]) if r[5] else ("ID:{}".format(r[6]) if r[6] else '\u2014'),
            d=_e(r[0]), c=_fmt(r[1]),
            l=_fmt(r[7]) if r[7] != '' else '\u2014',
            p=_fmt(r[8]) if r[8] != '' else '\u2014',
            up=_e(r[4]), mon=_e(moneda), pu=_fmt(r[3]), sub=_fmt(_sub_pdf(r)))
        for r in items)

    # Logo como base64 si existe
    logo_html = ""
    logo_path = os.path.join(_APP_DIR, "LOGO.png")
    if os.path.exists(logo_path):
        import base64
        with open(logo_path, "rb") as lf:
            logo_b64 = base64.b64encode(lf.read()).decode()
        logo_html = "<img src='data:image/png;base64,{b64}' style='height:50px;object-fit:contain;max-width:160px' alt='Logo'>".format(b64=logo_b64)

    return (
        "<!DOCTYPE html><html lang='es'><head><meta charset='utf-8'>"
        "<title>OC {num}</title>"
        "<style>"
        "@page{{margin:15mm}}"
        "@media print{{.np{{display:none}}body{{padding:0;margin:0}}"
        "  *{{-webkit-print-color-adjust:exact!important;print-color-adjust:exact!important}}}}"
        "body{{font-family:Arial,sans-serif;margin:20px 30px;color:#1c0a00;font-size:13px}}"
        ".header{{display:flex;justify-content:space-between;align-items:center;"
        "  background:#fb923c;padding:12px 16px;border-radius:8px 8px 0 0;margin-bottom:0}}"
        ".header-title{{color:#fff;font-size:20px;font-weight:800;letter-spacing:0.5px}}"
        ".header-sub{{color:rgba(255,255,255,0.85);font-size:12px;margin-top:2px}}"
        ".subheader{{background:#fff7ed;border:1px solid #fed7aa;border-top:none;"
        "  padding:8px 16px;border-radius:0 0 8px 8px;margin-bottom:14px;font-size:12px;color:#7c2d12}}"
        ".meta{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:8px;margin-bottom:14px;"
        "  border:1px solid #fed7aa;border-radius:6px;padding:10px;background:#fff}}"
        ".ml{{font-size:11px;color:#9a3412}}.mv{{font-weight:600;margin-top:2px;color:#1c0a00}}"
        "table{{width:100%;border-collapse:collapse;margin-top:8px}}"
        "th{{background:#fb923c;color:#fff;padding:8px;font-size:12px;font-weight:700;text-align:left}}"
        "td{{padding:7px 8px;border-bottom:1px solid #fed7aa;font-size:12px}}"
        "tr:nth-child(even) td{{background:#fff7ed}}.r{{text-align:right}}"
        "tfoot tr td{{font-weight:700;background:#fff7ed;border-top:2px solid #fdba74}}"
        ".sign{{margin-top:40px;display:grid;grid-template-columns:1fr 1fr;gap:40px}}"
        ".sign-box{{border-top:2px solid #fb923c;padding-top:6px;font-size:11px;color:#7c2d12}}"
        "button{{padding:8px 16px;background:#fb923c;color:#fff;border:none;border-radius:6px;cursor:pointer;font-size:14px;margin-right:8px}}"
        "button:hover{{background:#f97316}}"
        "a.back{{color:#fb923c;font-size:13px}}"
        "</style></head><body>"
        "<div class='np' style='margin-bottom:12px'>"
        "<button onclick='window.print()'>&#128438; Imprimir / Guardar PDF</button>"
        "<a class='back' href='/modulo/suministros/ordenes-compra/{oc_id}'>&larr; Volver</a>"
        "</div>"
        "<div class='header'>"
        "{logo_html}"
        "<div style='text-align:right'>"
        "<div class='header-title'>ORDEN DE COMPRA &mdash; {num}</div>"
        "<div class='header-sub'>Estado: {est}</div>"
        "</div>"
        "</div>"
        "<div class='subheader'>"
        "<b>Fecha:</b> {fecha} &nbsp;|&nbsp; <b>OP:</b> {op} &nbsp;|&nbsp; <b>Obra:</b> {obra} &nbsp;|&nbsp; <b>Solicitante:</b> {sol}"
        "</div>"
        "<div class='meta'>"
        "<div><div class='ml'>Proveedor</div><div class='mv'>{prov}</div></div>"
        "<div><div class='ml'>Condiciones de pago</div><div class='mv'>{cond}</div></div>"
        "<div><div class='ml'>Plazo de entrega</div><div class='mv'>{plazo}</div></div>"
        "{obs_meta}"
        "</div>"
        "<table>"
        "<thead><tr><th>Codigo</th><th>Descripcion</th><th class='r'>Cant.Barras</th>"
        "<th class='r'>Largo(m)</th><th class='r'>Peso(kg)</th>"
        "<th class='r'>Tipo precio</th>"
        "<th class='r'>P. Unitario ({mon})</th><th class='r'>Subtotal ({mon})</th></tr></thead>"
        "<tbody>{filas}</tbody>"
        "<tfoot><tr><td colspan='7' class='r'>TOTAL {mon}</td><td class='r'>{mon} {total}</td></tr></tfoot>"
        "</table>"
        "<div class='sign'>"
        "<div class='sign-box'>Compras &mdash; Firma y aclaracion</div>"
        "<div class='sign-box'>Proveedor &mdash; Conformidad</div>"
        "</div>"
        "</body></html>"
    ).format(
        num=_e(oc[1]), oc_id=oc_id, est=_e(oc[7]), logo_html=logo_html,
        prov=_e(oc[3]), fecha=_e(str(oc[2] or "")[:10]),
        op=_e(oc[8]), obra=_e(oc[9]) or "—", sol=_e(oc[10]),
        cond=_e(oc[4]) or "—", plazo=_e(oc[5]) or "—",
        mon=_e(moneda),
        obs_meta="<div style='grid-column:span 3'><div class='ml'>Observaciones</div><div class='mv'>{}</div></div>".format(_e(oc[6])) if oc[6] else "",
        filas=filas, total=_fmt(total))


# ─── Recepcion / Checklist ─────────────────────────────────────

@suministros_bp.route("/control-stock")
def control_stock():
    db = get_db()
    _ensure_tables(db)

    rows = db.execute(
        "SELECT ic.id, COALESCE(oc.numero,''), COALESCE(oc.fecha_recepcion,''),"
        " COALESCE(a.codigo,''), COALESCE(ic.descripcion,''), COALESCE(a.categoria,''),"
        " COALESCE(ic.cantidad_recibida,0), COALESCE(ic.unidad,'u'),"
        " COALESCE(op.obra,''), COALESCE(ic.estado_stock,'Pendiente')"
        " FROM items_oc ic"
        " JOIN ordenes_compra oc ON oc.id=ic.oc_id"
        " LEFT JOIN articulos_sum a ON a.id=ic.articulo_id"
        " LEFT JOIN ordenes_pedido op ON op.id=oc.op_id"
        " WHERE COALESCE(ic.cantidad_recibida,0) > 0"
        " AND ("
        "   UPPER(COALESCE(a.categoria,'')) LIKE 'PERFIL%'"
        "   OR UPPER(COALESCE(ic.descripcion,'')) LIKE '%PERFIL%'"
        "   OR UPPER(COALESCE(a.categoria,'')) LIKE '%BULON%'"
        "   OR UPPER(COALESCE(ic.descripcion,'')) LIKE '%BULON%'"
        " )"
        " ORDER BY oc.id DESC, ic.id DESC"
    ).fetchall()

    def _badge_stock(v):
        est = str(v or "Pendiente")
        if est == "Procesado":
            return "<span style='background:#dcfce7;color:#166534;padding:3px 10px;border-radius:999px;font-size:12px;font-weight:700'>Procesado</span>"
        return "<span style='background:#fef3c7;color:#92400e;padding:3px 10px;border-radius:999px;font-size:12px;font-weight:700'>Pendiente</span>"

    filas = "".join(
        "<tr>"
        "<td>{oc}</td>"
        "<td>{fr}</td>"
        "<td style='color:#7c2d12;font-size:12px'><b>{cod}</b></td>"
        "<td><b>{desc}</b></td>"
        "<td>{cat}</td>"
        "<td style='text-align:right'>{cant}</td>"
        "<td>{u}</td>"
        "<td>{obra}</td>"
        "<td>{estado}</td>"
        "<td style='white-space:nowrap'>{acc}</td>"
        "</tr>".format(
            oc=_e(r[1]),
            fr=_e(str(r[2])[:10]) or "—",
            cod=_e(r[3]) or "—",
            desc=_e(r[4]),
            cat=_e(r[5]) or "—",
            cant=_fmt(r[6]),
            u=_e(r[7]),
            obra=_e(r[8]) or "—",
            estado=_badge_stock(r[9]),
            acc=(
                "<form method='post' action='/modulo/suministros/control-stock/{iid}/procesado' style='display:inline'>"
                "<button class='b gn sm' type='submit'>Marcar procesado</button>"
                "</form>"
            ).format(iid=int(r[0])) if str(r[9] or "Pendiente") != "Procesado" else "<span style='color:#16a34a;font-size:12px;font-weight:700'>OK</span>"
        )
        for r in rows
    ) or "<tr><td colspan='10'>Sin materiales recibidos de perfiles/bulones.</td></tr>"

    body = (
        "<h2>Control de Stock</h2>"
        "<a class='b gr' href='/modulo/suministros'>Dashboard Compras</a>"
        "<a class='b bl' href='/modulo/suministros/ordenes-compra'>Tablero OC</a>"
        "<div class='card' style='margin-top:10px;overflow-x:auto'>"
        "<table class='hl'><thead><tr>"
        "<th>OC</th><th>F. Recepcion</th><th>Codigo</th><th>Descripcion</th><th>Categoria</th>"
        "<th style='text-align:right'>Cant. Recibida</th><th>Unidad</th><th>Obra</th><th>Estado</th><th></th>"
        "</tr></thead><tbody>{filas}</tbody></table>"
        "</div>"
        "<div class='card' style='font-size:13px;color:#64748b'>"
        "Solo se listan materiales recibidos vinculados a perfiles y bulones."
        "</div>"
    ).format(filas=filas)
    return _page("Control de Stock", body)


@suministros_bp.route("/control-stock/<int:item_id>/procesado", methods=["POST"])
def control_stock_marcar_procesado(item_id):
    db = get_db()
    _ensure_tables(db)
    db.execute("UPDATE items_oc SET estado_stock='Procesado' WHERE id=?", (item_id,))
    db.commit()
    return redirect("/modulo/suministros/control-stock")

@suministros_bp.route("/ordenes-compra/<int:oc_id>/recepcion", methods=["GET", "POST"])
def oc_recepcion(oc_id):
    db = get_db()
    _ensure_tables(db)

    oc = db.execute(
        "SELECT id, COALESCE(numero,''), proveedor, estado, COALESCE(remito_proveedor,'')"
        " FROM ordenes_compra WHERE id=?", (oc_id,)
    ).fetchone()
    if not oc:
        return _page("Error", "<p>OC no encontrada.</p>"), 404
    if oc[3] not in ("Para Despachar", "Recibido Parcial"):
        return redirect("/modulo/suministros/ordenes-compra/{}".format(oc_id))

    items = db.execute(
        "SELECT id, descripcion, cantidad, unidad, COALESCE(cantidad_recibida,0)"
        " FROM items_oc WHERE oc_id=? ORDER BY id", (oc_id,)
    ).fetchall()

    if request.method == "POST":
        remito_prov   = (request.form.get("remito_proveedor") or "").strip()
        recibidas_raw = request.form.getlist("cant_recibida[]")
        ids_items     = request.form.getlist("item_id[]")
        estado_manual = (request.form.get("estado_oc") or "").strip()

        all_ok = True
        for idx, iid_str in enumerate(ids_items):
            try:
                iid  = int(iid_str)
                cant = float(recibidas_raw[idx]) if idx < len(recibidas_raw) else 0.0
            except (ValueError, IndexError):
                cant = 0.0
            db.execute("UPDATE items_oc SET cantidad_recibida=? WHERE id=? AND oc_id=?",
                       (cant, iid, oc_id))
            item_row = next((r for r in items if r[0] == iid), None)
            if item_row and cant < float(item_row[2] or 0):
                all_ok = False

        if estado_manual in ("Recibido OK", "Recibido Parcial", "Rechazado"):
            nuevo_estado = estado_manual
        else:
            nuevo_estado = "Recibido OK" if all_ok else "Recibido Parcial"

        responsable_rec = (request.form.get("responsable") or "").strip()
        from datetime import date as _date_d
        hoy = _date_d.today().isoformat()
        db.execute(
            "UPDATE ordenes_compra SET estado=?, fecha_recepcion=?, remito_proveedor=?, responsable_recepcion=? WHERE id=?",
            (nuevo_estado, hoy, remito_prov or None, responsable_rec or None, oc_id))
        db.commit()
        return redirect("/modulo/suministros/ordenes-compra/{}".format(oc_id))

    # GET - construir tabla de items con inputs
    # Misma lista que Solicitante en OP nueva (usuarios supervisores de BD)
    _sups_db = db.execute(
        "SELECT nombre FROM usuarios WHERE (rol='supervisor' OR nombre='Gabriel Ibarra') AND activo=1 ORDER BY nombre"
    ).fetchall()
    _sup_names = [r[0] for r in _sups_db] or SUPERVISORES
    opts_sups = "".join(
        "<option value='{s}'>{s}</option>".format(s=_e(s)) for s in _sup_names)
    filas = "".join(
        "<tr>"
        "<td><input type='hidden' name='item_id[]' value='{iid}'>{desc}</td>"
        "<td style='text-align:right'>{qty}</td>"
        "<td>{unit}</td>"
        "<td><input name='cant_recibida[]' type='number' value='{recib}' min='0' step='1'"
        " style='width:80px;border:1px solid #d1d5db;border-radius:4px;padding:3px 6px'></td>"
        "<td class='estado-cell'></td>"
        "</tr>".format(
            iid=int(r[0]), desc=_e(r[1]), qty=r[2], unit=_e(r[3] or ""), recib=r[4])
        for r in items
    ) or "<tr><td colspan='5'>Sin items.</td></tr>"

    body = (
        "<h2>Recepcion &mdash; {num}</h2>"
        "<a class='b gr' href='/modulo/suministros/ordenes-compra/{oc_id}'>Volver a OC</a>"
        "<div class='card' style='margin-top:10px;border-left:4px solid #f97316'>"
        "<b>Proveedor:</b> {prov} &nbsp;|&nbsp; <b>Estado actual:</b> {est}"
        "<p style='font-size:13px;margin:6px 0 0'>Completa las cantidades recibidas. "
        "Si todas cubren el 100% &rarr; <b>Recibido OK</b>. Si alguna es menor &rarr; <b>Recibido Parcial</b>.</p>"
        "</div>"
        "<form method='post'>"
        "<div class='card' style='max-width:400px'>"
        "<label><b>Remito del Proveedor</b></label>"
        "<input name='remito_proveedor' value='{remito_prov}' placeholder='Ej: REM-0042 / 00001-00000123' style='max-width:300px'>"
        "<label style='margin-top:10px'><b>Responsable de recepci\u00f3n</b></label>"
        "<select name='responsable' style='width:100%;margin-bottom:10px'>"
        "<option value=''>-- Seleccionar supervisor --</option>"
        "{opts_sups}"
        "</select>"
        "<label style='margin-top:10px'><b>Estado de la OC</b></label>"
        "<select name='estado_oc' style='width:100%;margin-bottom:10px'>"
        "<option value=''>Autom\u00e1tico</option>"
        "<option value='Recibido OK'>Recibido OK</option>"
        "<option value='Recibido Parcial'>Recibido Parcial</option>"
        "<option value='Rechazado'>Rechazado</option>"
        "</select>"
        "</div>"
        "<div class='card' style='overflow-x:auto'>"
        "<table class='hl'><thead><tr>"
        "<th>Descripcion</th><th style='text-align:right'>Cant. Pedida</th>"
        "<th>Unidad</th><th>Cant. Recibida</th><th>Estado</th>"
        "</tr></thead><tbody>{filas}</tbody></table>"
        "</div>"
        "<button class='b or'>Confirmar Recepcion</button>"
        "</form>"
        "<script>"
        "document.querySelectorAll('input[name=\"cant_recibida[]\"]').forEach(function(inp, idx) {{"
        "  var rows = document.querySelectorAll('tbody tr');"
        "  function check() {{"
        "    var pedida = parseFloat(rows[idx].querySelectorAll('td')[1].innerText.replace(',','.')) || 0;"
        "    var recibida = parseFloat(inp.value) || 0;"
        "    var cell = rows[idx].querySelector('td:last-child');"
        "    if (recibida >= pedida) {{"
        "      cell.innerHTML = '<span style=\"color:#166534;font-weight:700\">OK</span>';"
        "    }} else {{"
        "      cell.innerHTML = '<span style=\"color:#9f1239;font-weight:700\">Parcial</span>';"
        "    }}"
        "  }}"
        "  inp.addEventListener('input', check);"
        "  check();"
        "}});"
        "</script>"
    ).format(
        num=_e(oc[1]), oc_id=oc_id, prov=_e(oc[2]), est=_badge(oc[3]),
        remito_prov=_e(oc[4]), opts_sups=opts_sups, filas=filas)
    return _page("Recepcion", body)

