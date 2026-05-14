import os
from io import BytesIO
from flask import Blueprint, request, jsonify, send_file, session
from datetime import date, timedelta, datetime
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Image, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, A3
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.graphics.shapes import Drawing, String
from reportlab.graphics.charts.barcharts import VerticalBarChart
from reportlab.graphics.charts.piecharts import Pie
from db_utils import get_db, _guardar_pdf_databook as _db_guardar_pdf_databook
from proceso_utils import _proceso_aprobado

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
_DATABOOKS_DIR = os.path.join(_APP_DIR, "Reportes Produccion")
_DATABOOK_SECCIONES = {
    "calidad_recepcion": os.path.join("1-Calidad (Data Book)", "1.1-Recepcion de material"),
    "calidad_corte_perfiles": os.path.join("1-Calidad (Data Book)", "1.2-Corte perfiles"),
    "calidad_armado_soldadura": os.path.join("1-Calidad (Data Book)", "1.3-Armado y soldadura"),
    "calidad_pintura": os.path.join("1-Calidad (Data Book)", "1.4-Pintura"),
    "calidad_despacho": os.path.join("1-Calidad (Data Book)", "1.5-Despacho"),
    "remitos": "2-Remitos de despacho",
}


def _guardar_pdf_databook(obra, seccion_key, filename, pdf_bytes, ot_id=None):
    return _db_guardar_pdf_databook(obra, seccion_key, filename, pdf_bytes, _DATABOOKS_DIR, _DATABOOK_SECCIONES, ot_id=ot_id)


estado_bp = Blueprint("estado", __name__)

# ============================================
# PALETA DE COLORES POR PROCESO
# ============================================
COLORES_PROCESOS = {
    "ARMADO": {
        "bg": "rgba(59, 130, 246, 0.7)",  # Azul
        "border": "#1e40af",
        "light_bg": "rgba(59, 130, 246, 0.15)",
    },
    "SOLDADURA": {
        "bg": "rgba(249, 115, 22, 0.7)",  # Naranja
        "border": "#c2410c",
        "light_bg": "rgba(249, 115, 22, 0.15)",
    },
    "PINTURA": {
        "bg": "rgba(34, 197, 94, 0.7)",  # Verde
        "border": "#15803d",
        "light_bg": "rgba(34, 197, 94, 0.15)",
    },
    "DESPACHO": {
        "bg": "rgba(168, 85, 247, 0.7)",  # Violeta
        "border": "#6d28d9",
        "light_bg": "rgba(168, 85, 247, 0.15)",
    },
    "P/DESPACHO": {
        "bg": "rgba(168, 85, 247, 0.7)",  # Violeta (igual a DESPACHO)
        "border": "#6d28d9",
        "light_bg": "rgba(168, 85, 247, 0.15)",
    },
    "ARMADO Y SOLDADURA": {
        "bg": "rgba(100, 120, 230, 0.7)",  # Azul+Naranja blend
        "border": "#1e40af",
        "light_bg": "rgba(100, 120, 230, 0.15)",
    },
}

def _obtener_color_proceso(proceso_nombre):
    """Devuelve diccionario de colores para un proceso específico."""
    nombre = str(proceso_nombre or "").strip().upper()
    return COLORES_PROCESOS.get(nombre, COLORES_PROCESOS.get("DESPACHO", {}))


def _es_usuario_obra():
    return str(session.get("user_role") or "").strip().lower() == "obra"


def _ot_has_column(db, column_name):
    objetivo = str(column_name or "").strip().lower()
    if not objetivo:
        return False

    # MySQL
    try:
        row = db.execute(
            """
            SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = DATABASE()
                            AND LOWER(TABLE_NAME) = 'ordenes_trabajo'
              AND LOWER(COLUMN_NAME) = ?
            """,
            (objetivo,),
        ).fetchone()
        if row and int(row[0] or 0) > 0:
            return True
    except Exception:
        pass

    # SQLite
    try:
        rows = db.execute("PRAGMA table_info(ordenes_trabajo)").fetchall()
        for row in rows:
            try:
                col = str(row[1] or "").strip().lower()
            except Exception:
                col = ""
            if col == objetivo:
                return True
    except Exception:
        return False
    return False


def _safe_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return float(default)


def _reglas_tipo_estructura(tipo_estructura):
    tipo = str(tipo_estructura or "").strip().upper()
    reglas = {
        "TIPO I": {
            "desvio_ok_min": -4.0,
            "desvio_alerta_min": -8.0,
            "proy_ok_min": 100.0,
            "proy_alerta_min": 96.0,
            "descripcion": "Ritmo alto esperado. Control estricto del desvio.",
        },
        "TIPO II": {
            "desvio_ok_min": -7.0,
            "desvio_alerta_min": -12.0,
            "proy_ok_min": 98.0,
            "proy_alerta_min": 90.0,
            "descripcion": "Ritmo intermedio. Tolerancia moderada de desvio.",
        },
        "TIPO III": {
            "desvio_ok_min": -10.0,
            "desvio_alerta_min": -16.0,
            "proy_ok_min": 95.0,
            "proy_alerta_min": 85.0,
            "descripcion": "Ritmo variable por series. Mayor ventana de tolerancia.",
        },
    }
    base = reglas.get(tipo)
    if base is None:
        return {
            "tipo": "GENERAL",
            "desvio_ok_min": -6.0,
            "desvio_alerta_min": -12.0,
            "proy_ok_min": 98.0,
            "proy_alerta_min": 90.0,
            "descripcion": "Regla general sin filtro de tipo.",
        }
    return {"tipo": tipo, **base}


def _clasificar_tendencia(tipo_estructura, desvio_hoy, proj_fin):
    reglas = _reglas_tipo_estructura(tipo_estructura)
    desvio = _safe_float(desvio_hoy, 0.0)
    proy = _safe_float(proj_fin, 0.0)

    if desvio >= reglas["desvio_ok_min"] and proy >= reglas["proy_ok_min"]:
        return {
            "codigo": "OK",
            "label": "En objetivo",
            "color": "#166534",
            "bg": "#dcfce7",
            "border": "#86efac",
            "reglas": reglas,
        }
    if desvio >= reglas["desvio_alerta_min"] and proy >= reglas["proy_alerta_min"]:
        return {
            "codigo": "ALERTA",
            "label": "Desvio controlado",
            "color": "#9a3412",
            "bg": "#ffedd5",
            "border": "#fdba74",
            "reglas": reglas,
        }
    return {
        "codigo": "CRITICO",
        "label": "Accion requerida",
        "color": "#991b1b",
        "bg": "#fee2e2",
        "border": "#fecaca",
        "reglas": reglas,
    }


def _calcular_tendencia_programacion(ots, prog_rows, tipo_estructura=""):
    reglas = _reglas_tipo_estructura(tipo_estructura)

    if not ots or not prog_rows:
        return {
            "habilitado": False,
            "motivo": "Sin programacion cargada",
            "tipo_estructura": reglas["tipo"],
            "reglas": reglas,
        }

    avance_by_ot = {}
    hs_prev_by_ot = {}
    for row in ots:
        ot_id = int(row[0] or 0)
        hs_prev_by_ot[ot_id] = max(0.0, _safe_float(row[3], 0.0))
        avance_by_ot[ot_id] = max(0.0, min(100.0, _safe_float(row[5], 0.0)))

    prog_by_ot = {}
    for pr in prog_rows:
        try:
            ot_id = int(pr[0] or 0)
            fi = datetime.strptime(str(pr[1])[:10], "%Y-%m-%d").date()
            ff = datetime.strptime(str(pr[2])[:10], "%Y-%m-%d").date()
        except Exception:
            continue
        if ot_id not in avance_by_ot:
            continue
        if ff < fi:
            fi, ff = ff, fi
        curr = prog_by_ot.get(ot_id)
        if curr is None:
            prog_by_ot[ot_id] = (fi, ff)
        else:
            prog_by_ot[ot_id] = (min(curr[0], fi), max(curr[1], ff))

    if not prog_by_ot:
        return {
            "habilitado": False,
            "motivo": "Sin programacion valida",
            "tipo_estructura": reglas["tipo"],
            "reglas": reglas,
        }

    ot_ids_prog = list(prog_by_ot.keys())
    date_start = min(v[0] for v in prog_by_ot.values())
    date_end = max(v[1] for v in prog_by_ot.values())
    if date_end <= date_start:
        date_end = date_start + timedelta(days=1)

    weight_sum = sum(hs_prev_by_ot.get(oid, 0.0) for oid in ot_ids_prog)
    use_equal = weight_sum <= 0
    if use_equal:
        weight_sum = float(max(len(ot_ids_prog), 1))

    def _w(oid):
        return 1.0 if use_equal else hs_prev_by_ot.get(oid, 0.0)

    def _plan_pct(day_ref):
        acc = 0.0
        for oid in ot_ids_prog:
            fi, ff = prog_by_ot[oid]
            den = max((ff - fi).days, 1)
            elapsed = max(0, min((day_ref - fi).days, den))
            acc += _w(oid) * (elapsed / den * 100.0)
        return round(acc / weight_sum, 1) if weight_sum > 0 else 0.0

    real_now = round(
        sum(_w(oid) * avance_by_ot.get(oid, 0.0) for oid in ot_ids_prog) / weight_sum,
        1,
    ) if weight_sum > 0 else 0.0

    today_d = date.today()
    today_clamped = min(max(today_d, date_start), date_end)
    plan_now = _plan_pct(today_clamped)
    desvio_now = round(real_now - plan_now, 1)

    elapsed_days = max((today_clamped - date_start).days, 1)
    rem_days = max((date_end - today_clamped).days, 0)
    vel_real = real_now / elapsed_days if elapsed_days > 0 else 0.0
    proj_end = round(min(180.0, real_now + vel_real * rem_days), 1)

    fecha_objetivo = "Sin tendencia"
    if vel_real > 0:
        dias_hasta_100 = int(round((100.0 - real_now) / vel_real)) if real_now < 100 else 0
        fecha_objetivo = (today_clamped + timedelta(days=max(dias_hasta_100, 0))).strftime("%d-%m-%Y")

    semaforo = _clasificar_tendencia(reglas["tipo"], desvio_now, proj_end)

    return {
        "habilitado": True,
        "tipo_estructura": reglas["tipo"],
        "reglas": reglas,
        "inicio_label": date_start.strftime("%d-%m"),
        "hoy_label": today_clamped.strftime("%d-%m"),
        "fin_label": date_end.strftime("%d-%m"),
        "plan_inicio": 0.0,
        "plan_hoy": plan_now,
        "plan_fin": 100.0,
        "real_inicio": 0.0,
        "real_hoy": real_now,
        "proj_fin": proj_end,
        "desvio_hoy": desvio_now,
        "fecha_objetivo": fecha_objetivo,
        "ot_programadas": len(ot_ids_prog),
        "semaforo": semaforo,
    }


def _resumen_tipos_estructura(ots, prog_rows):
    tipos = ["TIPO I", "TIPO II", "TIPO III"]
    salida = []
    for tipo in tipos:
        ots_tipo = [row for row in ots if str(row[6] or "").strip().upper() == tipo]
        ot_ids_tipo = {int(row[0]) for row in ots_tipo if row and row[0] is not None}
        prog_tipo = [pr for pr in prog_rows if int(pr[0] or 0) in ot_ids_tipo]
        tendencia = _calcular_tendencia_programacion(ots_tipo, prog_tipo, tipo)

        hs_previstas = round(sum(_safe_float(r[3], 0.0) for r in ots_tipo), 1)
        hs_cargadas = round(sum(_safe_float(r[4], 0.0) for r in ots_tipo), 1)
        eficiencia = round((hs_cargadas / hs_previstas * 100.0), 1) if hs_previstas > 0 else 0.0

        salida.append({
            "tipo": tipo,
            "descripcion": _reglas_tipo_estructura(tipo)["descripcion"],
            "ots_activas": len(ots_tipo),
            "hs_previstas": hs_previstas,
            "hs_cargadas": hs_cargadas,
            "eficiencia_hs": eficiencia,
            "tendencia": tendencia,
        })
    return salida


def _calcular_kg_por_estacion_y_despachados(rows):
    # Reglas pedidas:
    # - Si SOLDADURA esta aprobada, la pieza cuenta en PINTURA.
    # - Si DESPACHO esta aprobado, la pieza cuenta en P/DESPACHO y en KG despachados.
    piezas = {}
    for row in rows:
        pos = str(row[0] or "").strip()
        obra = str(row[1] or "").strip()
        ot_id = row[2]
        proceso = str(row[3] or "").strip().upper()
        estado = str(row[4] or "").strip().upper()
        reins = str(row[5] or "")
        peso = _safe_float(row[6], 0.0)

        if not pos:
            continue

        key = (pos, obra, int(ot_id or 0))
        data = piezas.get(key)
        if not data:
            data = {
                "peso": 0.0,
                "armado_ok": False,
                "soldadura_ok": False,
                "pintura_ok": False,
                "despacho_ok": False,
            }
            piezas[key] = data

        if peso > data["peso"]:
            data["peso"] = peso

        if _proceso_aprobado(estado, reins):
            if proceso == "ARMADO":
                data["armado_ok"] = True
            elif proceso == "SOLDADURA":
                data["soldadura_ok"] = True
            elif proceso in ("PINTURA", "PINTURA_FONDO"):
                data["pintura_ok"] = True
            elif proceso == "DESPACHO":
                data["despacho_ok"] = True

    kg_por_estacion = {"ARMADO Y SOLDADURA": 0.0, "PINTURA": 0.0, "P/DESPACHO": 0.0}
    kg_despachados = 0.0

    for data in piezas.values():
        peso = _safe_float(data.get("peso"), 0.0)
        if peso <= 0:
            continue

        if data.get("despacho_ok"):
            kg_por_estacion["P/DESPACHO"] += peso
            kg_despachados += peso
        elif data.get("soldadura_ok") or data.get("pintura_ok"):
            kg_por_estacion["PINTURA"] += peso
        else:
            kg_por_estacion["ARMADO Y SOLDADURA"] += peso

    for k in kg_por_estacion:
        kg_por_estacion[k] = round(_safe_float(kg_por_estacion[k]), 2)
    kg_despachados = round(_safe_float(kg_despachados), 2)
    return kg_por_estacion, kg_despachados


@estado_bp.route("/modulo/estado")
def estado_produccion():
    btn_pdf_html = "" if _es_usuario_obra() else '<a id="btn-pdf" href="#" onclick="exportarVistaPDF(); return false;" class="btn btn-pdf">📄 Generar reporte PDF</a>'
    html = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Estado de Producción</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/html2canvas@1.4.1/dist/html2canvas.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/jspdf@2.5.1/dist/jspdf.umd.min.js"></script>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    font-family: 'Segoe UI', Arial, sans-serif;
    background: linear-gradient(135deg, #fff4e6 0%, #ffe4c7 50%, #ffd0a8 100%);
    min-height: 100vh;
    padding: 20px;
}
.container { max-width: 1300px; margin: 0 auto; }
.top-bar {
    display: flex; justify-content: space-between; align-items: center;
    background: rgba(255,255,255,0.92); border-radius: 14px; padding: 16px 22px;
    border: 1px solid #fdba74; box-shadow: 0 6px 20px rgba(154,52,18,0.1);
    margin-bottom: 20px;
}
.top-title { display: flex; align-items: center; gap: 12px; }
.top-title img {
    width: 58px;
    height: 34px;
    object-fit: contain;
    border-radius: 6px;
    background: #fff;
    border: 1px solid #fed7aa;
    padding: 2px;
}
.top-bar h2 { color: #7c2d12; font-size: 1.45em; }
.btn {
    display: inline-block; background: #f97316; color: white;
    padding: 9px 18px; border-radius: 8px; text-decoration: none;
    font-weight: bold; font-size: 0.9em;
}
.btn:hover { background: #ea580c; }
.period-bar {
    display: flex; gap: 10px; margin-bottom: 20px; flex-wrap: wrap;
    background: rgba(255,255,255,0.88); border-radius: 12px;
    padding: 14px 18px; border: 1px solid #fdba74;
    box-shadow: 0 4px 12px rgba(154,52,18,0.08);
    align-items: center;
}
.period-bar > span { font-weight: bold; color: #9a3412; margin-right: 6px; }
.period-btn {
    padding: 9px 24px; border: 2px solid #f97316; border-radius: 22px;
    background: white; color: #f97316; font-weight: bold; cursor: pointer;
    font-size: 0.9em; transition: all 0.18s;
}
.period-btn.active, .period-btn:hover { background: #f97316; color: white; }
.filtro-tipo {
    padding: 8px 10px;
    border: 1px solid #fdba74;
    border-radius: 8px;
    color: #7c2d12;
    background: #fff;
    font-weight: 600;
}
.tipo-desc {
    width: 100%;
    margin-top: 8px;
    color: #9a3412;
    font-size: 0.8em;
}
.fecha-desde { margin-left: auto; color: #9a3412; font-size: 0.85em; font-style: italic; }
.kpi-row {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
    gap: 16px; margin-bottom: 20px;
}
.kpi-card {
    background: white; border-radius: 12px; padding: 18px 14px;
    box-shadow: 0 4px 12px rgba(154,52,18,0.1);
    border-left: 5px solid #f97316; text-align: center;
}
.kpi-valor { font-size: 1.9em; font-weight: bold; color: #ea580c; }
.kpi-label { font-size: 0.82em; color: #9a3412; margin-top: 5px; }
.chart-full {
    background: white; border-radius: 14px; padding: 22px;
    box-shadow: 0 6px 18px rgba(154,52,18,0.1); border: 1px solid #ffedd5;
    margin-bottom: 20px;
}
.chart-full h3 {
    color: #7c2d12; margin-bottom: 16px; font-size: 1.1em;
    border-bottom: 2px solid #ffedd5; padding-bottom: 8px;
}
.chart-full canvas { max-height: 360px; }
.trend-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 10px;
    margin-bottom: 12px;
}
.trend-kpi {
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    padding: 10px;
    text-align: center;
}
.trend-kpi .v {
    font-size: 1.35em;
    font-weight: 800;
    line-height: 1.1;
}
.trend-kpi .l {
    font-size: 0.72em;
    margin-top: 4px;
    color: #64748b;
    text-transform: uppercase;
    letter-spacing: 0.25px;
}
.trend-note {
    margin-top: 8px;
    font-size: 0.8em;
    color: #64748b;
}
.tipos-board {
    background: white;
    border-radius: 14px;
    padding: 22px;
    box-shadow: 0 6px 18px rgba(154,52,18,0.1);
    border: 1px solid #ffedd5;
    margin-bottom: 20px;
}
.tipos-board h3 {
    color: #7c2d12;
    margin-bottom: 12px;
    font-size: 1.1em;
    border-bottom: 2px solid #ffedd5;
    padding-bottom: 8px;
}
.tipos-grid {
    display: grid;
    grid-template-columns: repeat(3, minmax(220px, 1fr));
    gap: 12px;
}
.tipo-card {
    border: 1px solid #e2e8f0;
    border-radius: 12px;
    padding: 12px;
    background: #f8fafc;
}
.tipo-head {
    display: flex;
    justify-content: space-between;
    align-items: center;
    gap: 8px;
    margin-bottom: 8px;
}
.tipo-title {
    font-weight: 800;
    color: #0f172a;
    font-size: 0.95em;
}
.tipo-badge {
    padding: 4px 8px;
    border-radius: 999px;
    font-size: 0.68em;
    font-weight: 800;
    border: 1px solid transparent;
    text-transform: uppercase;
}
.tipo-badge.ok { background: #dcfce7; color: #166534; border-color: #86efac; }
.tipo-badge.alerta { background: #ffedd5; color: #9a3412; border-color: #fdba74; }
.tipo-badge.critico { background: #fee2e2; color: #991b1b; border-color: #fecaca; }
.tipo-desc {
    color: #475569;
    font-size: 0.76em;
    margin-bottom: 10px;
}
.tipo-kpis {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 8px;
}
.tipo-kpi {
    background: white;
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    padding: 8px;
}
.tipo-kpi .v {
    font-size: 1.05em;
    font-weight: 800;
    color: #0f172a;
}
.tipo-kpi .l {
    margin-top: 2px;
    font-size: 0.68em;
    color: #64748b;
    text-transform: uppercase;
}
.tipo-umbrales {
    margin-top: 8px;
    font-size: 0.7em;
    color: #64748b;
    line-height: 1.35;
}
.charts-row {
    display: grid; grid-template-columns: 1fr 1fr;
    gap: 20px; margin-bottom: 20px;
}
.chart-card {
    background: white; border-radius: 14px; padding: 22px;
    box-shadow: 0 6px 18px rgba(154,52,18,0.1); border: 1px solid #ffedd5;
}
.chart-card h3 {
    color: #7c2d12; margin-bottom: 16px; font-size: 1.1em;
    border-bottom: 2px solid #ffedd5; padding-bottom: 8px;
}
.chart-card canvas { max-height: 300px; }
.no-data-msg {
    text-align: center; padding: 30px; color: #9a3412;
    background: #fff7ed; border-radius: 8px; font-style: italic;
}
.btn-pdf {
    background: #7c2d12; display: inline-flex; align-items: center; gap: 6px;
}
.btn-pdf:hover { background: #9a3412; }
.filter-card {
    background: rgba(255,255,255,0.88);
    border-radius: 12px;
    padding: 14px 18px;
    border: 1px solid #fdba74;
    margin-bottom: 20px;
    box-shadow: 0 4px 12px rgba(154,52,18,0.08);
}
.filter-fecha {
    display: flex;
    gap: 12px;
    align-items: center;
    flex-wrap: wrap;
}
.filter-fecha input[type="date"] {
    padding: 8px 10px;
    border: 1px solid #fdba74;
    border-radius: 6px;
    color: #7c2d12;
}
.filter-fecha button {
    padding: 8px 16px;
    border: none;
    border-radius: 6px;
    cursor: pointer;
    font-weight: bold;
}
.compare-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 20px;
}
.compare-variations {
    display: grid;
    grid-template-columns: 1fr 1fr 1fr 1fr;
    gap: 12px;
}
.pdf-export .top-title img {
    width: 96px;
    height: 56px;
}
.pdf-export .top-actions {
    display: none !important;
}
@media print {
    .top-actions {
        display: none !important;
    }
    .top-title img {
        width: 96px;
        height: 56px;
    }
}
@media (max-width: 900px) {
    .top-bar {
        flex-direction: column;
        align-items: flex-start;
        gap: 12px;
    }
    .top-actions {
        width: 100%;
        justify-content: flex-end;
        flex-wrap: wrap;
    }
    .compare-grid {
        grid-template-columns: 1fr;
    }
    .compare-variations {
        grid-template-columns: 1fr 1fr;
    }
}
@media (max-width: 768px) {
    .charts-row { grid-template-columns: 1fr; }
    .tipos-grid { grid-template-columns: 1fr; }
    .period-bar {
        display: grid;
        grid-template-columns: 1fr;
        gap: 8px;
    }
    .fecha-desde {
        margin-left: 0;
    }
    .filter-fecha {
        display: grid;
        grid-template-columns: 1fr;
        gap: 8px;
    }
    .filter-fecha button,
    .filter-fecha input[type="date"] {
        width: 100%;
    }
}
@media (max-width: 520px) {
    body { padding: 12px; }
    .top-title h2 { font-size: 1.15em; }
    .compare-variations { grid-template-columns: 1fr; }
    .kpi-row { grid-template-columns: 1fr 1fr; }
    .trend-grid { grid-template-columns: 1fr 1fr; }
    .tipo-kpis { grid-template-columns: 1fr; }
}
</style>
</head>
<body>
<div class="container">

  <div class="top-bar">
        <div class="top-title">
            <img src="/logo-a3" alt="Logo empresa">
            <h2>📊 Estado de Producción</h2>
        </div>
                                <div class="top-actions" style="display:flex;gap:10px;">
                                                                                                __BTN_PDF_HTML__
      <a href="/" class="btn">⬅️ Volver</a>
    </div>
  </div>

  <div class="period-bar">
    <span>🗓 Período:</span>
    <button class="period-btn" onclick="cambiarPeriodo(this,'semana')">Semana</button>
    <button class="period-btn active" onclick="cambiarPeriodo(this,'mes')">Mes</button>
    <button class="period-btn" onclick="cambiarPeriodo(this,'trimestre')">Trimestre</button>
    <button class="period-btn" onclick="cambiarPeriodo(this,'actual')">Actual</button>
        <span style="margin-left: 10px; font-weight: bold; color: #9a3412;">Tipo Estructura:</span>
        <select id="filtro-tipo-obra" class="filtro-tipo" onchange="cambiarTipoObra()">
            <option value="">Todos</option>
            <option value="TIPO I">TIPO I</option>
            <option value="TIPO II">TIPO II</option>
            <option value="TIPO III">TIPO III</option>
        </select>
        <span style="margin-left: 10px; font-weight: bold; color: #9a3412;">Obra:</span>
        <select id="filtro-obra" class="filtro-tipo" onchange="cambiarObra()">
            <option value="">Todas</option>
        </select>
    <span class="fecha-desde" id="fecha-desde-txt"></span>
        <div class="tipo-desc" id="tipo-desc-text">Seleccione un tipo para ver la descripción.</div>
  </div>
  
    <div class="filter-card filter-fecha">
    <span style="font-weight:bold; color:#9a3412; margin-right:8px;">📅 Filtro por fechas:</span>
    <label style="color:#9a3412; font-weight:bold;">Desde:</label>
    <input type="date" id="filtro-fecha-inicio">
    <label style="color:#9a3412; font-weight:bold; margin-left:8px;">Hasta:</label>
        <input type="date" id="filtro-fecha-fin">
        <button onclick="aplicarFiltroFechas()" style="margin-left:8px; background:#f97316; color:white;">Aplicar</button>
        <button onclick="limpiarFiltroFechas()" style="background:#999; color:white;">Limpiar</button>
  </div>
  
    <div class="filter-card">
    <label style="color:#9a3412; font-weight:bold; margin-right:16px;">📊 Comparar Períodos:</label>
    <select id="comparar-periodo-selector" onchange="mostrarComparacion()" style="padding:8px 12px; border:1px solid #fdba74; border-radius:6px; color:#7c2d12; font-weight:bold;">
      <option value="none">Sin comparación</option>
      <option value="mes-anterior">Este mes vs Mes anterior</option>
      <option value="semana-anterior">Esta semana vs Semana anterior</option>
      <option value="mes-ano">Este mes vs Mismo mes año anterior</option>
    </select>
  </div>
  
  <div id="comparacion-seccion" style="display:none; background:rgba(255,255,255,0.88); border-radius:12px; padding:20px; border:2px solid #f97316; margin-bottom:20px; box-shadow:0 6px 18px rgba(154,52,18,0.1);">
    <h3 style="color:#7c2d12; margin-bottom:16px; text-align:center;">📈 Comparación de Períodos</h3>
    <div class="compare-grid">
      <div>
        <h4 style="color:#9a3412; text-align:center; margin-bottom:12px;" id="periodo-1-label">Período 1</h4>
        <div style="background:#fff7ed; padding:14px; border-radius:8px; border-left:4px solid #f97316;">
          <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px;">
            <div>
              <div style="font-size:0.8em; color:#9a3412; font-weight:bold;">HS Consumidas</div>
              <div style="font-size:1.6em; font-weight:bold; color:#ea580c;" id="p1-hs-carg">—</div>
            </div>
            <div>
              <div style="font-size:0.8em; color:#9a3412; font-weight:bold;">KG Producidos</div>
              <div style="font-size:1.6em; font-weight:bold; color:#ea580c;" id="p1-kg">—</div>
            </div>
            <div>
              <div style="font-size:0.8em; color:#9a3412; font-weight:bold;">KG/HS</div>
              <div style="font-size:1.6em; font-weight:bold; color:#ea580c;" id="p1-kg-hs">—</div>
            </div>
            <div>
              <div style="font-size:0.8em; color:#9a3412; font-weight:bold;">Eficiencia</div>
              <div style="font-size:1.6em; font-weight:bold; color:#ea580c;" id="p1-efe">—</div>
            </div>
          </div>
        </div>
      </div>
      <div>
        <h4 style="color:#9a3412; text-align:center; margin-bottom:12px;" id="periodo-2-label">Período 2</h4>
        <div style="background:#fff7ed; padding:14px; border-radius:8px; border-left:4px solid #fb923c;">
          <div style="display:grid; grid-template-columns:1fr 1fr; gap:10px;">
            <div>
              <div style="font-size:0.8em; color:#9a3412; font-weight:bold;">HS Consumidas</div>
              <div style="font-size:1.6em; font-weight:bold; color:#fb923c;" id="p2-hs-carg">—</div>
            </div>
            <div>
              <div style="font-size:0.8em; color:#9a3412; font-weight:bold;">KG Producidos</div>
              <div style="font-size:1.6em; font-weight:bold; color:#fb923c;" id="p2-kg">—</div>
            </div>
            <div>
              <div style="font-size:0.8em; color:#9a3412; font-weight:bold;">KG/HS</div>
              <div style="font-size:1.6em; font-weight:bold; color:#fb923c;" id="p2-kg-hs">—</div>
            </div>
            <div>
              <div style="font-size:0.8em; color:#9a3412; font-weight:bold;">Eficiencia</div>
              <div style="font-size:1.6em; font-weight:bold; color:#fb923c;" id="p2-efe">—</div>
            </div>
          </div>
        </div>
      </div>
    </div>
    <div style="background:#fff; padding:14px; border-radius:8px; margin-top:16px; border:1px solid #fdba74;">
      <h4 style="color:#7c2d12; margin-bottom:10px;">📊 Variación (Período 2 vs Período 1)</h4>
    <div class="compare-variations">
        <div style="text-align:center;">
          <div style="font-size:0.8em; color:#9a3412; font-weight:bold;">HS Var. %</div>
          <div style="font-size:1.4em; font-weight:bold;" id="var-hs-pct">—</div>
        </div>
        <div style="text-align:center;">
          <div style="font-size:0.8em; color:#9a3412; font-weight:bold;">KG Var. %</div>
          <div style="font-size:1.4em; font-weight:bold;" id="var-kg-pct">—</div>
        </div>
        <div style="text-align:center;">
          <div style="font-size:0.8em; color:#9a3412; font-weight:bold;">KG/HS Var. %</div>
          <div style="font-size:1.4em; font-weight:bold;" id="var-kg-hs-pct">—</div>
        </div>
        <div style="text-align:center;">
          <div style="font-size:0.8em; color:#9a3412; font-weight:bold;">Eficiencia Var. %</div>
          <div style="font-size:1.4em; font-weight:bold;" id="var-efe-pct">—</div>
        </div>
      </div>
    </div>
  </div>

    <div class="tipos-board">
        <h3>🏗 Tablero por Tipo de Estructura</h3>
        <div class="tipos-grid" id="tipos-grid"></div>
    </div>

  <div class="kpi-row">
    <div class="kpi-card">
      <div class="kpi-valor" id="kpi-hs-prev">—</div>
      <div class="kpi-label">HS Previstas (OTs activas)</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-valor" id="kpi-hs-carg">—</div>
            <div class="kpi-label">HS Consumidas (período)</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-valor" id="kpi-hs-segun-av">—</div>
      <div class="kpi-label">HS según Avance</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-valor" id="kpi-eficiencia">—</div>
      <div class="kpi-label">Eficiencia HS (%)</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-valor" id="kpi-kg-total">—</div>
      <div class="kpi-label">KG producidos (período)</div>
    </div>
        <div class="kpi-card">
            <div class="kpi-valor" id="kpi-kg-desp">—</div>
            <div class="kpi-label">KG despachados</div>
        </div>
        <div class="kpi-card">
            <div class="kpi-valor" id="kpi-kg-hs">—</div>
            <div class="kpi-label">KG/HS</div>
        </div>
  </div>

    <div class="chart-full">
        <h3>📉 Tendencia de Avance y Proyección</h3>
        <div class="trend-grid">
            <div class="trend-kpi"><div class="v" id="tend-real">—</div><div class="l">Avance real</div></div>
            <div class="trend-kpi"><div class="v" id="tend-plan">—</div><div class="l">Avance actual (plan)</div></div>
            <div class="trend-kpi"><div class="v" id="tend-desvio">—</div><div class="l">Desvío hoy</div></div>
            <div class="trend-kpi"><div class="v" id="tend-proy">—</div><div class="l">Proyección fin plan</div></div>
        </div>
        <div id="no-data-tend" class="no-data-msg" style="display:none">Sin programación suficiente para calcular tendencia.</div>
        <canvas id="chartTendencia"></canvas>
        <div class="trend-note" id="tend-note"></div>
    </div>

        <div class="chart-full">
                                <h3>⏱ HS Consumidas vs HS Previstas por Obra (OTs agrupadas)</h3>
        <div id="no-data-hs" class="no-data-msg" style="display:none">Sin datos de horas por obra para el período seleccionado.</div>
        <canvas id="chartHS"></canvas>
    </div>

  <div class="charts-row">
    <div class="chart-card">
      <h3>⚖️ KG procesados por Estación</h3>
      <div id="no-data-kg" class="no-data-msg" style="display:none">Sin datos de kg para el período.</div>
      <canvas id="chartKg"></canvas>
    </div>
    <div class="chart-card">
      <h3>📈 Distribución de KG en Planta</h3>
      <canvas id="chartKgDona"></canvas>
    </div>
  </div>

</div>

<script>
let chartHS = null, chartKg = null, chartKgDona = null, chartTendencia = null;

let periodoActivo = 'mes';
let tipoObraActivo = '';
let obraActiva = '';
let filtroFechaInicio = null;
let filtroFechaFin = null;

function cambiarPeriodo(btn, periodo) {
    document.querySelectorAll('.period-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    periodoActivo = periodo;
    cargarDatos(periodo);
}

function cambiarTipoObra() {
    tipoObraActivo = document.getElementById('filtro-tipo-obra').value || '';
    actualizarDescripcionTipo(tipoObraActivo);
    cargarDatos(periodoActivo);
}

function cambiarObra() {
    obraActiva = document.getElementById('filtro-obra').value || '';
    cargarDatos(periodoActivo);
}

function actualizarDescripcionTipo(tipo) {
    const el = document.getElementById('tipo-desc-text');
    const descripciones = {
        'TIPO I': 'TIPO I: ritmo alto | OK si desvio >= -4% y proyeccion >= 100%.',
        'TIPO II': 'TIPO II: ritmo intermedio | OK si desvio >= -7% y proyeccion >= 98%.',
        'TIPO III': 'TIPO III: ritmo variable | OK si desvio >= -10% y proyeccion >= 95%.'
    };
    el.textContent = descripciones[tipo] || 'Vista general: cada tipo se evalua con umbrales distintos de desvio y proyeccion.';
}

function _semaforoClase(codigo) {
    const c = String(codigo || '').toUpperCase();
    if (c === 'OK') return 'ok';
    if (c === 'ALERTA') return 'alerta';
    return 'critico';
}

function _renderTarjetasTipo(resumenTipos) {
    const host = document.getElementById('tipos-grid');
    if (!host) return;
    const rows = Array.isArray(resumenTipos) ? resumenTipos : [];
    if (rows.length === 0) {
        host.innerHTML = '<div class="no-data-msg">Sin datos por tipo para el filtro actual.</div>';
        return;
    }

    host.innerHTML = rows.map(row => {
        const tipo = String(row.tipo || 'TIPO');
        const desc = String(row.descripcion || '');
        const ots = Number(row.ots_activas || 0);
        const hsPrev = Number(row.hs_previstas || 0).toFixed(1);
        const hsCarg = Number(row.hs_cargadas || 0).toFixed(1);
        const efic = Number(row.eficiencia_hs || 0).toFixed(1);
        const tend = row.tendencia || {};
        const sem = tend.semaforo || {};
        const regs = sem.reglas || tend.reglas || {};
        const desvio = Number(tend.desvio_hoy || 0).toFixed(1);
        const proy = Number(tend.proj_fin || 0).toFixed(1);
        const badgeClass = _semaforoClase(sem.codigo);
        const badgeLabel = String(sem.codigo || 'CRITICO');
        const semLabel = String(sem.label || 'Sin tendencia');
        const umbralTxt = 'OK: desvio >= ' + Number(regs.desvio_ok_min || 0).toFixed(1) + '% | proy >= ' + Number(regs.proy_ok_min || 0).toFixed(1) + '%';
        const alertaTxt = 'Alerta: desvio >= ' + Number(regs.desvio_alerta_min || 0).toFixed(1) + '% | proy >= ' + Number(regs.proy_alerta_min || 0).toFixed(1) + '%';

        return `
            <div class="tipo-card">
                <div class="tipo-head">
                    <div class="tipo-title">${tipo}</div>
                    <span class="tipo-badge ${badgeClass}">${badgeLabel}</span>
                </div>
                <div class="tipo-desc">${desc}</div>
                <div class="tipo-kpis">
                    <div class="tipo-kpi"><div class="v">${ots}</div><div class="l">OTs activas</div></div>
                    <div class="tipo-kpi"><div class="v">${efic}%</div><div class="l">Eficiencia HS</div></div>
                    <div class="tipo-kpi"><div class="v">${hsPrev} hs</div><div class="l">HS previstas</div></div>
                    <div class="tipo-kpi"><div class="v">${hsCarg} hs</div><div class="l">HS consumidas</div></div>
                    <div class="tipo-kpi"><div class="v">${desvio}%</div><div class="l">Desvio hoy</div></div>
                    <div class="tipo-kpi"><div class="v">${proy}%</div><div class="l">Proyeccion fin</div></div>
                </div>
                <div class="tipo-umbrales">${semLabel}<br>${umbralTxt}<br>${alertaTxt}</div>
            </div>
        `;
    }).join('');
}

async function exportarVistaPDF() {
    const btn = document.getElementById('btn-pdf');
    const objetivo = document.querySelector('.container');
    const textoOriginal = btn.textContent;
    btn.textContent = '⏳ Generando...';
    objetivo.classList.add('pdf-export');

    try {
        const canvas = await html2canvas(objetivo, {
            scale: 2,
            useCORS: true,
            backgroundColor: '#fff4e6'
        });

        const { jsPDF } = window.jspdf;
        const pdf = new jsPDF({
            orientation: 'landscape',
            unit: 'mm',
            format: 'a3'
        });

        const pageW = pdf.internal.pageSize.getWidth();
        const pageH = pdf.internal.pageSize.getHeight();
        const margin = 8;
        const maxW = pageW - margin * 2;
        const maxH = pageH - margin * 2;

        const imgW = canvas.width;
        const imgH = canvas.height;
        const ratio = Math.min(maxW / imgW, maxH / imgH);
        const drawW = imgW * ratio;
        const drawH = imgH * ratio;
        const x = (pageW - drawW) / 2;
        const y = (pageH - drawH) / 2;

        const imgData = canvas.toDataURL('image/png', 1.0);
        pdf.addImage(imgData, 'PNG', x, y, drawW, drawH, undefined, 'FAST');

        const fecha = new Date();
        const yyyymmdd = fecha.getFullYear().toString() +
            String(fecha.getMonth() + 1).padStart(2, '0') +
            String(fecha.getDate()).padStart(2, '0');
        pdf.save('estado_produccion_pantalla_' + periodoActivo + '_' + yyyymmdd + '.pdf');
    } catch (err) {
        alert('No se pudo generar el PDF de pantalla.');
        console.error(err);
    } finally {
        objetivo.classList.remove('pdf-export');
        btn.textContent = textoOriginal;
    }
}

function cargarDatos(periodo) {
    const params = new URLSearchParams();
    params.append('periodo', periodo);
    params.append('tipo_obra', tipoObraActivo);
    params.append('obra', obraActiva);
    
    // Si hay filtro de fechas, usarlo en lugar del período
    if (filtroFechaInicio && filtroFechaFin) {
        params.set('periodo', 'custom');
        params.append('fecha_inicio', filtroFechaInicio);
        params.append('fecha_fin', filtroFechaFin);
    }
    
    const url = '/api/dashboard-estado?' + params.toString();
    console.log('URL:', url);
    
    fetch(url)
        .then(r => {
            console.log('Status:', r.status);
            return r.json();
        })
        .then(data => {
            console.log('Data recibida - HS por OT:', data.hs_por_ot ? data.hs_por_ot.length : 0, 'OTs');
            renderDashboard(data);
        })
        .catch(err => {
            console.error('Error en API:', err);
            // alert('Error cargando datos'); // Silenciado: solo loguea en consola
        });
}

function aplicarFiltroFechas() {
    const inicio = document.getElementById('filtro-fecha-inicio').value;
    const fin = document.getElementById('filtro-fecha-fin').value;
    
    console.log('Aplicar filtro - Inicio:', inicio, 'Fin:', fin);
    
    if (!inicio || !fin) {
        alert('Por favor seleccione ambas fechas');
        return;
    }
    
    const fechaInicio = new Date(inicio);
    const fechaFin = new Date(fin);
    
    if (fechaInicio > fechaFin) {
        alert('La fecha de inicio no puede ser mayor que la de fin');
        return;
    }
    
    // Guardar fechas de filtro
    filtroFechaInicio = inicio;
    filtroFechaFin = fin;
    console.log('Filtro guardado - Inicio:', filtroFechaInicio, 'Fin:', filtroFechaFin);
    
    // Cargar datos con el período actual + el nuevo filtro de fechas
    cargarDatos(periodoActivo);
}

function limpiarFiltroFechas() {
    document.getElementById('filtro-fecha-inicio').value = '';
    document.getElementById('filtro-fecha-fin').value = '';
    
    filtroFechaInicio = null;
    filtroFechaFin = null;
    console.log('Filtro limpiado');
    
    // Recargar con el período actual sin filtro de fechas
    cargarDatos(periodoActivo);
}

function mostrarComparacion() {
    const selector = document.getElementById('comparar-periodo-selector').value;
    const seccion = document.getElementById('comparacion-seccion');
    comparacionActiva = selector;
    
    if (selector === 'none') {
        seccion.style.display = 'none';
        return;
    }
    
    seccion.style.display = 'block';
    cargarComparacion(selector);
}

async function cargarComparacion(tipo) {
    const today = new Date();
    let p1_inicio, p1_fin, p2_inicio, p2_fin, p1_label, p2_label;
    
    if (tipo === 'mes-anterior') {
        p1_fin = new Date(today.getFullYear(), today.getMonth() + 1, 0);
        p1_inicio = new Date(today.getFullYear(), today.getMonth(), 1);
        p2_fin = new Date(today.getFullYear(), today.getMonth(), 0);
        p2_inicio = new Date(today.getFullYear(), today.getMonth() - 1, 1);
        p1_label = 'Este mes';
        p2_label = 'Mes anterior';
    } else if (tipo === 'semana-anterior') {
        p1_fin = today;
        p1_inicio = new Date(today.getTime() - 7 * 24 * 60 * 60 * 1000);
        p2_fin = new Date(p1_inicio.getTime() - 24 * 60 * 60 * 1000);
        p2_inicio = new Date(p2_fin.getTime() - 7 * 24 * 60 * 60 * 1000);
        p1_label = 'Esta semana';
        p2_label = 'Semana anterior';
    } else if (tipo === 'mes-ano') {
        p1_fin = new Date(today.getFullYear(), today.getMonth() + 1, 0);
        p1_inicio = new Date(today.getFullYear(), today.getMonth(), 1);
        p2_fin = new Date(today.getFullYear() - 1, today.getMonth() + 1, 0);
        p2_inicio = new Date(today.getFullYear() - 1, today.getMonth(), 1);
        p1_label = 'Este mes (2026)';
        p2_label = 'Mismo mes (2025)';
    }
    
    const formatFecha = (d) => d.toISOString().split('T')[0];
    
    try {
        const res1 = await fetch(`/api/dashboard-estado/comparar?fecha_inicio=${formatFecha(p1_inicio)}&fecha_fin=${formatFecha(p1_fin)}`);
        const data1 = await res1.json();
        
        const res2 = await fetch(`/api/dashboard-estado/comparar?fecha_inicio=${formatFecha(p2_inicio)}&fecha_fin=${formatFecha(p2_fin)}`);
        const data2 = await res2.json();
        
        document.getElementById('periodo-1-label').textContent = p1_label;
        document.getElementById('periodo-2-label').textContent = p2_label;
        
        // Período 1
        document.getElementById('p1-hs-carg').textContent = data1.hs_consumidas.toFixed(1) + ' hs';
        document.getElementById('p1-kg').textContent = data1.kg_total.toFixed(1) + ' kg';
        document.getElementById('p1-kg-hs').textContent = (data1.kg_total / data1.hs_consumidas).toFixed(1);
        document.getElementById('p1-efe').textContent = (data1.eficiencia * 100).toFixed(1) + '%';
        
        // Período 2
        document.getElementById('p2-hs-carg').textContent = data2.hs_consumidas.toFixed(1) + ' hs';
        document.getElementById('p2-kg').textContent = data2.kg_total.toFixed(1) + ' kg';
        document.getElementById('p2-kg-hs').textContent = (data2.kg_total / data2.hs_consumidas).toFixed(1);
        document.getElementById('p2-efe').textContent = (data2.eficiencia * 100).toFixed(1) + '%';
        
        // Variaciones
        const varHs = ((data2.hs_consumidas - data1.hs_consumidas) / data1.hs_consumidas * 100).toFixed(1);
        const varKg = ((data2.kg_total - data1.kg_total) / data1.kg_total * 100).toFixed(1);
        const varKgHs = (((data2.kg_total / data2.hs_consumidas) - (data1.kg_total / data1.hs_consumidas)) / (data1.kg_total / data1.hs_consumidas) * 100).toFixed(1);
        const varEfe = ((data2.eficiencia - data1.eficiencia) / data1.eficiencia * 100).toFixed(1);
        
        const colorPct = (val) => parseFloat(val) >= 0 ? '#22c55e' : '#ef4444';
        
        document.getElementById('var-hs-pct').textContent = (varHs > 0 ? '+' : '') + varHs + '%';
        document.getElementById('var-hs-pct').style.color = colorPct(varHs);
        
        document.getElementById('var-kg-pct').textContent = (varKg > 0 ? '+' : '') + varKg + '%';
        document.getElementById('var-kg-pct').style.color = colorPct(varKg);
        
        document.getElementById('var-kg-hs-pct').textContent = (varKgHs > 0 ? '+' : '') + varKgHs + '%';
        document.getElementById('var-kg-hs-pct').style.color = colorPct(varKgHs);
        
        document.getElementById('var-efe-pct').textContent = (varEfe > 0 ? '+' : '') + varEfe + '%';
        document.getElementById('var-efe-pct').style.color = colorPct(varEfe);
    } catch (err) {
        console.error('Error cargando comparación:', err);
        alert('Error cargando datos de comparación');
    }
}

function renderDashboard(data) {
    const fd = data.fecha_desde.split('-');
    let dateLabel = 'Desde: ' + fd[2] + '/' + fd[1] + '/' + fd[0];
    
    // Si hay fecha_hasta (período personalizado), mostrarla también
    if (data.fecha_hasta) {
        const fh = data.fecha_hasta.split('-');
        dateLabel += ' - Hasta: ' + fh[2] + '/' + fh[1] + '/' + fh[0];
    }
    
    document.getElementById('fecha-desde-txt').textContent = dateLabel;

    const hs = data.hs_por_ot;
    const hsObra = data.hs_por_obra || [];
    const kg = data.kg_por_estacion;
    const kgDespachados = Number(data.kg_despachados || 0);
    _renderTarjetasTipo(data.resumen_tipos || []);

    // Filtro por obra: mantener opciones sincronizadas con backend
    const filtroObra = document.getElementById('filtro-obra');
    if (filtroObra) {
        const obras = data.obras_disponibles || [];
        const valorActual = obraActiva || '';
        filtroObra.innerHTML = '<option value="">Todas</option>';
        obras.forEach(o => {
            const opt = document.createElement('option');
            opt.value = o;
            opt.textContent = o;
            if (o === valorActual) opt.selected = true;
            filtroObra.appendChild(opt);
        });
    }

    // KPIs
    const totalPrev = hs.reduce((s, o) => s + o.hs_previstas, 0);
    const totalCarg = hs.reduce((s, o) => s + o.hs_cargadas, 0);
    const totalSegunAv = hs.reduce((s, o) => s + (o.hs_segun_avance || 0), 0);
    const totalKg   = Object.values(kg).reduce((s, v) => s + v, 0);
    const efic      = totalPrev > 0 ? ((totalCarg / totalPrev) * 100).toFixed(1) : '—';
    const kgHs      = totalCarg > 0 ? (totalKg / totalCarg).toFixed(1) : '—';

    document.getElementById('kpi-hs-prev').textContent  = totalPrev.toFixed(1) + ' hs';
    document.getElementById('kpi-hs-carg').textContent  = totalCarg.toFixed(1) + ' hs';
    document.getElementById('kpi-hs-segun-av').textContent = totalSegunAv.toFixed(1) + ' hs';
    document.getElementById('kpi-eficiencia').textContent = efic !== '—' ? efic + '%' : '—';
    document.getElementById('kpi-kg-total').textContent = totalKg.toFixed(1) + ' kg';
    document.getElementById('kpi-kg-desp').textContent = kgDespachados.toFixed(1) + ' kg';
    document.getElementById('kpi-kg-hs').textContent = kgHs !== '—' ? kgHs + ' kg/hs' : '—';

    // === Chart Tendencia plan-real-proyeccion ===
    const tend = data.tendencia || {};
    const noTend = document.getElementById('no-data-tend');
    const cvTend = document.getElementById('chartTendencia');
    const noteTend = document.getElementById('tend-note');

    if (chartTendencia) chartTendencia.destroy();

    if (!tend.habilitado) {
        noTend.style.display = 'block';
        cvTend.style.display = 'none';
        noteTend.textContent = tend.motivo || '';
        document.getElementById('tend-real').textContent = '—';
        document.getElementById('tend-plan').textContent = '—';
        document.getElementById('tend-desvio').textContent = '—';
        document.getElementById('tend-proy').textContent = '—';
    } else {
        noTend.style.display = 'none';
        cvTend.style.display = 'block';

        const sem = tend.semaforo || {};
        const reglas = sem.reglas || tend.reglas || {};
        const desvio = Number(tend.desvio_hoy || 0);
        const desvioSign = desvio >= 0 ? '+' : '';
        const desvioColor = String(sem.color || (desvio >= 0 ? '#16a34a' : '#dc2626'));
        const proy = Number(tend.proj_fin || 0);
        const proyColor = String(sem.color || (proy >= 100 ? '#16a34a' : (proy >= 85 ? '#ea580c' : '#dc2626')));

        document.getElementById('tend-real').textContent = Number(tend.real_hoy || 0).toFixed(1) + '%';
        document.getElementById('tend-real').style.color = '#16a34a';
        document.getElementById('tend-plan').textContent = Number(tend.plan_hoy || 0).toFixed(1) + '%';
        document.getElementById('tend-plan').style.color = '#1d4ed8';
        document.getElementById('tend-desvio').textContent = desvioSign + desvio.toFixed(1) + '%';
        document.getElementById('tend-desvio').style.color = desvioColor;
        document.getElementById('tend-proy').textContent = proy.toFixed(1) + '%';
        document.getElementById('tend-proy').style.color = proyColor;

        noteTend.textContent =
            'Estado: ' + String(sem.label || 'Sin clasificacion') +
            ' · Fecha estimada 100%: ' + (tend.fecha_objetivo || 'Sin tendencia') +
            ' · OTs programadas: ' + String(tend.ot_programadas || 0) +
            ' · Umbral OK: desvio >= ' + Number(reglas.desvio_ok_min || 0).toFixed(1) +
            '% y proy >= ' + Number(reglas.proy_ok_min || 0).toFixed(1) + '%';

        chartTendencia = new Chart(cvTend, {
            type: 'line',
            data: {
                labels: [
                    'Inicio ' + (tend.inicio_label || ''),
                    'Hoy ' + (tend.hoy_label || ''),
                    'Fin ' + (tend.fin_label || '')
                ],
                datasets: [
                    {
                        label: 'Avance actual (plan)',
                        data: [Number(tend.plan_inicio || 0), Number(tend.plan_hoy || 0), Number(tend.plan_fin || 100)],
                        borderColor: '#0284c7',
                        backgroundColor: 'rgba(2, 132, 199, 0.08)',
                        borderWidth: 3,
                        tension: 0.3,
                        pointRadius: 5,
                        pointBackgroundColor: '#0284c7',
                        pointBorderColor: '#fff',
                        pointBorderWidth: 2
                    },
                    {
                        label: 'Avance real',
                        data: [Number(tend.real_inicio || 0), Number(tend.real_hoy || 0), null],
                        borderColor: '#16a34a',
                        backgroundColor: 'rgba(22, 163, 74, 0.08)',
                        borderWidth: 3,
                        tension: 0.3,
                        pointRadius: 5,
                        pointBackgroundColor: '#16a34a',
                        pointBorderColor: '#fff',
                        pointBorderWidth: 2,
                        spanGaps: false
                    },
                    {
                        label: 'Tendencia / proyección',
                        data: [null, Number(tend.real_hoy || 0), Number(tend.proj_fin || 0)],
                        borderColor: '#dc2626',
                        backgroundColor: 'rgba(220, 38, 38, 0.08)',
                        borderWidth: 3,
                        borderDash: [7, 5],
                        tension: 0.3,
                        pointRadius: 5,
                        pointBackgroundColor: '#dc2626',
                        pointBorderColor: '#fff',
                        pointBorderWidth: 2,
                        spanGaps: true
                    }
                ]
            },
            options: {
                responsive: true,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { position: 'top', labels: { font: { size: 12, weight: 'bold' }, usePointStyle: true, padding: 15 } },
                    tooltip: {
                        backgroundColor: 'rgba(0,0,0,0.8)',
                        padding: 12,
                        titleFont: { size: 13, weight: 'bold' },
                        bodyFont: { size: 12 },
                        callbacks: {
                            label: ctx => ctx.dataset.label + ': ' + Number(ctx.parsed.y || 0).toFixed(1) + '%'
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        suggestedMax: 110,
                        ticks: { font: { size: 11 } },
                        grid: { color: 'rgba(200,200,200,0.15)' },
                        title: { display: true, text: 'Avance (%)', font: { size: 12, weight: 'bold' } }
                    },
                    x: { 
                        ticks: { font: { size: 11 } },
                        grid: { display: false }
                    }
                }
            }
        });
    }

    // === Chart HS por OBRA (OTs agrupadas) ===
    if (chartHS) chartHS.destroy();
    if (hsObra.length === 0) {
        document.getElementById('no-data-hs').style.display = 'block';
        document.getElementById('chartHS').style.display = 'none';
    } else {
        document.getElementById('no-data-hs').style.display = 'none';
        document.getElementById('chartHS').style.display = 'block';
        chartHS = new Chart(document.getElementById('chartHS'), {
            type: 'bar',
            data: {
                labels: hsObra.map(o => o.label),
                datasets: [
                    {
                        label: 'HS Totales Previstas',
                        data: hsObra.map(o => o.hs_previstas),
                        backgroundColor: 'rgba(59, 130, 246, 0.18)', // azul claro translúcido
                        borderColor: 'rgba(59, 130, 246, 0.35)',
                        borderWidth: 2,
                        borderRadius: 8,
                        barPercentage: 1.0,
                        categoryPercentage: 1.0,
                        type: 'bar',
                        order: 1,
                        z: 1
                    },
                    {
                        label: 'HS Según Avance Actual',
                        data: hsObra.map(o => o.hs_segun_avance || 0),
                        backgroundColor: 'rgba(59, 130, 246, 0.85)', // azul intenso
                        borderColor: '#1e40af',
                        borderWidth: 2,
                        borderRadius: 8,
                        barPercentage: 0.7,
                        categoryPercentage: 0.7,
                        type: 'bar',
                        order: 2,
                        z: 2
                    },
                    {
                        label: 'HS Reales Totales',
                        data: hsObra.map(o => o.hs_cargadas),
                        backgroundColor: 'rgba(34, 197, 94, 0.85)', // verde
                        borderColor: '#15803d',
                        borderWidth: 2,
                        borderRadius: 8,
                        barPercentage: 0.45,
                        categoryPercentage: 0.45,
                        type: 'bar',
                        order: 3,
                        z: 3
                    }
                ]
            },
            options: {
                responsive: true,
                indexAxis: 'y', // BARRAS HORIZONTALES
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { position: 'top', labels: { font: { size: 12, weight: 'bold' }, usePointStyle: true, padding: 15 } },
                    tooltip: { 
                        backgroundColor: 'rgba(0,0,0,0.8)',
                        padding: 12,
                        titleFont: { size: 13, weight: 'bold' },
                        bodyFont: { size: 12 },
                        callbacks: { 
                            label: ctx => {
                                let label = ctx.dataset.label || '';
                                if (label) label += ': ';
                                label += ctx.parsed.x.toFixed(1) + ' hs';
                                return label;
                            }
                        }
                    }
                },
                scales: {
                    x: { 
                        beginAtZero: true, 
                        ticks: { font: { size: 11 } },
                        grid: { color: 'rgba(200,200,200,0.15)' },
                        title: { display: true, text: 'Horas', font: { size: 12, weight: 'bold' } }
                    },
                    y: { 
                        ticks: { font: { size: 11 } },
                        grid: { display: false }
                    }
                }
            }
        });
    }

    // === Chart KG bar ===
    const estaciones = ['ARMADO Y SOLDADURA', 'PINTURA', 'P/DESPACHO'];
    const colores_procesos = {
        'ARMADO Y SOLDADURA': 'rgba(100, 120, 230, 0.7)',
        'PINTURA': 'rgba(34, 197, 94, 0.7)',
        'P/DESPACHO': 'rgba(168, 85, 247, 0.7)'
    };
    const colores_bordes = {
        'ARMADO Y SOLDADURA': '#1e40af',
        'PINTURA': '#15803d',
        'P/DESPACHO': '#6d28d9'
    };
    const colores = estaciones.map(e => colores_procesos[e]);
    const bordes = estaciones.map(e => colores_bordes[e]);
    const kgVals = estaciones.map(e => kg[e] || 0);
    const hayKg = kgVals.some(v => v > 0);

    if (chartKg) chartKg.destroy();
    if (!hayKg) {
        document.getElementById('no-data-kg').style.display = 'block';
        document.getElementById('chartKg').style.display = 'none';
    } else {
        document.getElementById('no-data-kg').style.display = 'none';
        document.getElementById('chartKg').style.display = 'block';
        chartKg = new Chart(document.getElementById('chartKg'), {
            type: 'bar',
            data: {
                labels: estaciones,
                datasets: [{
                    label: 'KG',
                    data: kgVals,
                    backgroundColor: colores,
                    borderColor: bordes,
                    borderWidth: 2,
                    borderRadius: 8
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: { display: false },
                    tooltip: { 
                        backgroundColor: 'rgba(0,0,0,0.8)',
                        padding: 12,
                        titleFont: { size: 13, weight: 'bold' },
                        bodyFont: { size: 12 },
                        callbacks: { label: ctx => ctx.parsed.y.toFixed(1) + ' kg' } 
                    }
                },
                scales: { 
                    y: { 
                        beginAtZero: true,
                        ticks: { font: { size: 11 } },
                        grid: { color: 'rgba(200,200,200,0.15)' },
                        title: { display: true, text: 'KG (Kg)', font: { size: 12, weight: 'bold' } } 
                    },
                    x: { 
                        ticks: { font: { size: 11 } },
                        grid: { display: false }
                    }
                }
            }
        });
    }

    // === Chart KG dona ===
    if (chartKgDona) chartKgDona.destroy();
    chartKgDona = new Chart(document.getElementById('chartKgDona'), {
        type: 'doughnut',
        data: {
            labels: estaciones,
            datasets: [{
                data: kgVals,
                backgroundColor: colores,
                borderWidth: 3,
                borderColor: '#fff'
            }]
        },
        options: {
            responsive: true,
            cutout: '60%',
            plugins: {
                legend: { 
                    position: 'bottom',
                    labels: { font: { size: 12, weight: 'bold' }, padding: 15, usePointStyle: true }
                },
                tooltip: { 
                    backgroundColor: 'rgba(0,0,0,0.8)',
                    padding: 12,
                    titleFont: { size: 13, weight: 'bold' },
                    bodyFont: { size: 12 },
                    callbacks: { label: ctx => ctx.label + ': ' + ctx.parsed.toFixed(1) + ' kg' } 
                }
            }
        }
    });
}

// Cargar datos iniciales
cargarDatos('mes');
actualizarDescripcionTipo(tipoObraActivo);
</script>
</body>
</html>
"""
    html = html.replace("__BTN_PDF_HTML__", btn_pdf_html)
    return html


@estado_bp.route("/api/dashboard-estado")
def api_dashboard_estado():
    periodo = request.args.get("periodo", "mes")
    tipo_obra_raw = (request.args.get("tipo_obra") or "").strip().upper()
    tipo_obra = tipo_obra_raw if tipo_obra_raw and tipo_obra_raw != "TODAS" else ""
    obra = (request.args.get("obra") or "").strip()

    today = date.today()
    fecha_hasta = None
    
    if periodo == "custom":
        fecha_inicio_str = request.args.get("fecha_inicio", "")
        fecha_fin_str = request.args.get("fecha_fin", "")
        if fecha_inicio_str and fecha_fin_str:
            fecha_desde_str = fecha_inicio_str
            fecha_hasta = fecha_fin_str
        else:
            fecha_desde_str = str(today.replace(day=1))
            fecha_hasta = None
    elif periodo == "semana":
        fecha_desde = today - timedelta(days=7)
        fecha_desde_str = str(fecha_desde)
    elif periodo == "trimestre":
        fecha_desde = today - timedelta(days=90)
        fecha_desde_str = str(fecha_desde)
    elif periodo == "actual":
        fecha_desde_str = None  # No filtrar por fecha, solo OTs activas
    else:
        fecha_desde = today.replace(day=1)
        fecha_desde_str = str(fecha_desde)
    
    db = get_db()

    tiene_hs_previstas = _ot_has_column(db, "hs_previstas")
    tiene_estado_avance = _ot_has_column(db, "estado_avance")
    hs_prev_expr = "COALESCE(ot.hs_previstas, 0)" if tiene_hs_previstas else "0"
    estado_av_expr = "COALESCE(ot.estado_avance, 0)" if tiene_estado_avance else "0"

    # Construir filtros dinámicos
    fecha_filter_sql_pt = ""
    fecha_filter_sql_proc = ""
    fecha_query_params = []
    if periodo != "actual":
        fecha_query_params = [fecha_desde_str]
        if fecha_hasta:
            fecha_filter_sql_pt = " AND pt.fecha <= ?"
            fecha_filter_sql_proc = " AND fecha <= ?"
            fecha_query_params.append(fecha_hasta)
    
    tipo_filter_sql = ""
    tipo_params = ()
    if tipo_obra:
        tipo_filter_sql = " AND UPPER(COALESCE(ot.tipo_estructura, '')) = ?"
        tipo_params = (tipo_obra,)
    
    obra_filter_sql = ""
    obra_params = ()
    if obra:
        obra_filter_sql = " AND LOWER(TRIM(ot.obra)) = LOWER(?)"
        obra_params = (obra,)

    if periodo == "actual":
        ots = db.execute(f"""
            SELECT ot.id,
                   COALESCE(NULLIF(TRIM(ot.obra),''), NULLIF(TRIM(ot.titulo),''), 'OT ' || ot.id) AS nombre,
                   COALESCE(NULLIF(TRIM(ot.obra),''), 'SIN OBRA') AS obra_nombre,
                   {hs_prev_expr} AS hs_previstas,
                   0 AS hs_cargadas,
                   {estado_av_expr} AS estado_avance,
                   UPPER(TRIM(COALESCE(ot.tipo_estructura, ''))) AS tipo_estructura
            FROM ordenes_trabajo ot
            WHERE ot.fecha_cierre IS NULL {tipo_filter_sql}{obra_filter_sql}
            ORDER BY ot.id DESC
        """, tipo_params + obra_params).fetchall()
    else:
        ots = db.execute(f"""
            WITH hs_ot AS (
                SELECT ot.id,
                       COALESCE(NULLIF(TRIM(ot.obra),''), NULLIF(TRIM(ot.titulo),''), 'OT ' || ot.id) AS nombre,
                       COALESCE(NULLIF(TRIM(ot.obra),''), 'SIN OBRA') AS obra_nombre,
                       {hs_prev_expr} AS hs_previstas,
                       COALESCE(SUM(CASE WHEN pt.fecha >= ? {fecha_filter_sql_pt} THEN pt.horas ELSE 0 END), 0) AS hs_cargadas,
                       {estado_av_expr} AS estado_avance,
                       UPPER(TRIM(COALESCE(ot.tipo_estructura, ''))) AS tipo_estructura
                FROM ordenes_trabajo ot
                LEFT JOIN partes_trabajo pt ON pt.ot_id = ot.id
                WHERE ot.fecha_cierre IS NULL {tipo_filter_sql}{obra_filter_sql}
                GROUP BY ot.id
            )
            SELECT id, nombre, obra_nombre, hs_previstas, hs_cargadas, estado_avance, tipo_estructura
            FROM hs_ot
            WHERE hs_previstas > 0 OR hs_cargadas > 0
            ORDER BY id DESC
        """, tuple(fecha_query_params) + tipo_params + obra_params).fetchall()

    hs_por_ot = []
    for row in ots:
        nombre = str(row[1] or '')[:22]
        obra_nombre = str(row[2] or 'SIN OBRA')
        hs_previstas = round(float(row[3] or 0), 1)
        avance_pct = int(row[5] or 0)
        hs_segun_avance = round((avance_pct / 100.0) * hs_previstas, 1)
        hs_por_ot.append({
            "ot_id": row[0],
            "obra": obra_nombre,
            "label": f"OT {row[0]} · {nombre}",
            "hs_previstas": hs_previstas,
            "hs_cargadas":  round(float(row[4] or 0), 1),
            "hs_segun_avance": hs_segun_avance,
            "avance_pct": avance_pct,
            "tipo_estructura": str(row[6] or '').strip().upper(),
        })

    if periodo == "actual":
        obras = db.execute(f"""
            WITH hs_ot AS (
                SELECT ot.id,
                       COALESCE(NULLIF(TRIM(ot.obra),''), 'SIN OBRA') AS obra,
                       {hs_prev_expr} AS hs_previstas,
                       0 AS hs_cargadas
                FROM ordenes_trabajo ot
                WHERE ot.fecha_cierre IS NULL {tipo_filter_sql}{obra_filter_sql}
                GROUP BY ot.id
            )
            SELECT obra,
                   SUM(hs_previstas) AS hs_previstas,
                   SUM(hs_cargadas) AS hs_cargadas
            FROM hs_ot
            GROUP BY obra
            HAVING SUM(hs_previstas) > 0 OR SUM(hs_cargadas) > 0
            ORDER BY SUM(hs_cargadas) DESC, obra ASC
        """, tipo_params + obra_params).fetchall()
    else:
        obras = db.execute(f"""
            WITH hs_ot AS (
                SELECT ot.id,
                       COALESCE(NULLIF(TRIM(ot.obra),''), 'SIN OBRA') AS obra,
                       {hs_prev_expr} AS hs_previstas,
                       COALESCE(SUM(CASE WHEN pt.fecha >= ? {fecha_filter_sql_pt} THEN pt.horas ELSE 0 END), 0) AS hs_cargadas
                FROM ordenes_trabajo ot
                LEFT JOIN partes_trabajo pt ON pt.ot_id = ot.id
                WHERE ot.fecha_cierre IS NULL {tipo_filter_sql}{obra_filter_sql}
                GROUP BY ot.id
            )
            SELECT obra,
                   SUM(hs_previstas) AS hs_previstas,
                   SUM(hs_cargadas) AS hs_cargadas
            FROM hs_ot
            GROUP BY obra
            HAVING SUM(hs_previstas) > 0 OR SUM(hs_cargadas) > 0
            ORDER BY SUM(hs_cargadas) DESC, obra ASC
        """, tuple(fecha_query_params) + tipo_params + obra_params).fetchall()

    obras_disponibles_rows = db.execute(f"""
        SELECT DISTINCT TRIM(COALESCE(ot.obra, '')) AS obra
        FROM ordenes_trabajo ot
        WHERE ot.fecha_cierre IS NULL AND TRIM(COALESCE(ot.obra, '')) <> '' {tipo_filter_sql}
        ORDER BY obra ASC
    """, tipo_params).fetchall()
    obras_disponibles = [str(r[0]) for r in obras_disponibles_rows if str(r[0] or '').strip()]

    hs_por_obra = []
    for row in obras:
        obra_nombre = str(row[0] or 'SIN OBRA')
        hs_segun_avance_suma = sum(
            item["hs_segun_avance"] for item in hs_por_ot 
            if str(item.get("obra") or "") == obra_nombre
        )
        hs_por_obra.append({
            "label": obra_nombre[:24],
            "hs_previstas": round(float(row[1] or 0), 1),
            "hs_cargadas": round(float(row[2] or 0), 1),
            "hs_segun_avance": round(hs_segun_avance_suma, 1)
        })

    tendencia = {"habilitado": False, "motivo": "Sin OTs activas", "tipo_estructura": (tipo_obra or "GENERAL")}
    prog_rows = []
    ot_ids_act = [int(row[0]) for row in ots if row and row[0] is not None]
    if ot_ids_act:
        ph = ",".join(["?"] * len(ot_ids_act))
        prog_rows = db.execute(
            f"""
            SELECT p.ot_id, p.fecha_inicio, p.fecha_fin
            FROM programacion p
            WHERE p.ot_id IN ({ph})
            ORDER BY p.ot_id, p.fecha_inicio
            """,
            tuple(ot_ids_act),
        ).fetchall()
        tendencia = _calcular_tendencia_programacion(ots, prog_rows, tipo_obra)

    resumen_tipos = _resumen_tipos_estructura(ots, prog_rows)

    if periodo == "actual":
        ot_ids = [int(row[0]) for row in ots]
        if ot_ids:
            format_ids = ",".join(["?"] * len(ot_ids))
            kg_source_rows = db.execute(
                f"""
                SELECT TRIM(COALESCE(p.posicion, '')),
                       TRIM(COALESCE(p.obra, '')),
                       COALESCE(p.ot_id, 0),
                       UPPER(TRIM(COALESCE(p.proceso, ''))),
                       UPPER(TRIM(COALESCE(p.estado, ''))),
                       COALESCE(p.re_inspeccion, ''),
                       COALESCE(CAST(p.peso AS REAL), 0)
                FROM procesos p
                WHERE COALESCE(p.eliminado, 0) = 0
                  AND COALESCE(p.escaneado_qr, 0) = 1
                  AND COALESCE(p.ot_id, 0) IN ({format_ids})
                  AND UPPER(TRIM(COALESCE(p.proceso, ''))) IN ('ARMADO', 'SOLDADURA', 'PINTURA', 'PINTURA_FONDO', 'DESPACHO')
                """,
                tuple(ot_ids),
            ).fetchall()
        else:
            kg_source_rows = []
    else:
        filtros_extra = ""
        params_proc = []
        if fecha_hasta:
            filtros_extra += " AND p.fecha <= ?"
            params_proc.append(fecha_hasta)

        kg_source_rows = db.execute(
            f"""
            SELECT TRIM(COALESCE(p.posicion, '')),
                   TRIM(COALESCE(p.obra, '')),
                   COALESCE(p.ot_id, 0),
                   UPPER(TRIM(COALESCE(p.proceso, ''))),
                   UPPER(TRIM(COALESCE(p.estado, ''))),
                   COALESCE(p.re_inspeccion, ''),
                   COALESCE(CAST(p.peso AS REAL), 0)
            FROM procesos p
            LEFT JOIN ordenes_trabajo ot ON ot.id = p.ot_id
            WHERE COALESCE(p.eliminado, 0) = 0
              AND COALESCE(p.escaneado_qr, 0) = 1
              AND p.fecha >= ?
              {filtros_extra}
              AND UPPER(TRIM(COALESCE(p.proceso, ''))) IN ('ARMADO', 'SOLDADURA', 'PINTURA', 'PINTURA_FONDO', 'DESPACHO')
              AND (? = '' OR UPPER(COALESCE(ot.tipo_estructura, '')) = ?)
              AND (? = '' OR LOWER(TRIM(COALESCE(p.obra, ''))) = LOWER(?))
            """,
            (fecha_desde_str,) + tuple(params_proc) + (tipo_obra, tipo_obra, obra, obra),
        ).fetchall()

    kg_por_estacion, kg_despachados = _calcular_kg_por_estacion_y_despachados(kg_source_rows)

    # Evitar enviar None en fechas para el frontend
    fecha_desde_resp = fecha_desde_str if fecha_desde_str else ""
    fecha_hasta_resp = fecha_hasta if fecha_hasta else ""
    if periodo == "actual":
        fecha_desde_resp = str(date.today())
        fecha_hasta_resp = ""
    return jsonify({
        "periodo": periodo,
        "fecha_desde": fecha_desde_resp,
        "fecha_hasta": fecha_hasta_resp,
        "tipo_obra": tipo_obra,
        "obra": obra,
        "obras_disponibles": obras_disponibles,
        "hs_por_ot": hs_por_ot,
        "hs_por_obra": hs_por_obra,
        "tendencia": tendencia,
        "resumen_tipos": resumen_tipos,
        "kg_por_estacion": kg_por_estacion,
        "kg_despachados": kg_despachados,
    })


@estado_bp.route("/api/dashboard-estado/comparar")
def api_dashboard_comparar():
    fecha_inicio_str = request.args.get("fecha_inicio", "")
    fecha_fin_str = request.args.get("fecha_fin", "")
    
    if not fecha_inicio_str or not fecha_fin_str:
        return jsonify({"error": "Fechas requeridas"}), 400
    
    db = get_db()
    tiene_hs_previstas = _ot_has_column(db, "hs_previstas")
    hs_prev_expr = "COALESCE(ot.hs_previstas, 0)" if tiene_hs_previstas else "0"
    
    # Obtener HS consumidas
    hs_cargadas = db.execute("""
        SELECT COALESCE(SUM(pt.horas), 0) as total
        FROM partes_trabajo pt
        WHERE pt.fecha >= ? AND pt.fecha <= ?
    """, (fecha_inicio_str, fecha_fin_str)).fetchone()
    hs_consumidas = float(hs_cargadas[0] or 0)
    
    # Obtener KG producidos
    kg_rows = db.execute("""
        WITH ultima_ubicacion AS (
            SELECT 
                posicion,
                obra,
                peso,
                ROW_NUMBER() OVER (PARTITION BY posicion, obra ORDER BY fecha DESC, id DESC) as rn
            FROM procesos
            WHERE escaneado_qr = 1 
              AND fecha >= ?
              AND fecha <= ?
              AND proceso IN ('ARMADO', 'SOLDADURA', 'PINTURA', 'DESPACHO')
        )
        SELECT SUM(COALESCE(CAST(uu.peso AS REAL), 0)) AS total_kg
        FROM ultima_ubicacion uu
        WHERE uu.rn = 1
    """, (fecha_inicio_str, fecha_fin_str)).fetchone()
    kg_total = float(kg_rows[0] or 0)
    
    # Eficiencia: OTs activas con HS previstas
    ots = db.execute("""
        SELECT ot.id, {hs_prev_expr} as hs_previstas
        FROM ordenes_trabajo ot
        WHERE ot.fecha_cierre IS NULL AND {hs_prev_expr} > 0
    """.format(hs_prev_expr=hs_prev_expr)).fetchall()
    
    hs_previstas_total = sum(float(row[1] or 0) for row in ots)
    eficiencia = (hs_consumidas / hs_previstas_total) if hs_previstas_total > 0 else 0
    
    return jsonify({
        "hs_consumidas": hs_consumidas,
        "kg_total": kg_total,
        "eficiencia": eficiencia
    })


@estado_bp.route("/api/dashboard-estado/pdf")
def dashboard_estado_pdf():
    if _es_usuario_obra():
        return jsonify({"error": "Sin permisos para generar PDF"}), 403

    periodo = request.args.get("periodo", "mes")
    today = date.today()
    if periodo == "semana":
        fecha_desde = today - timedelta(days=7)
        periodo_label = "Semana"
    elif periodo == "trimestre":
        fecha_desde = today - timedelta(days=90)
        periodo_label = "Trimestre"
    else:
        fecha_desde = today.replace(day=1)
        periodo_label = "Mes"

    fecha_desde_str = str(fecha_desde)
    db = get_db()
    tiene_hs_previstas = _ot_has_column(db, "hs_previstas")
    hs_prev_expr = "COALESCE(ot.hs_previstas, 0)" if tiene_hs_previstas else "0"

    ots = db.execute("""
        SELECT ot.id,
               COALESCE(NULLIF(TRIM(ot.obra),''), NULLIF(TRIM(ot.titulo),''), 'OT ' || ot.id) AS nombre,
               {hs_prev_expr} AS hs_previstas,
               COALESCE(SUM(CASE WHEN pt.fecha >= ? THEN pt.horas ELSE 0 END), 0) AS hs_consumidas
        FROM ordenes_trabajo ot
        LEFT JOIN partes_trabajo pt ON pt.ot_id = ot.id
        GROUP BY ot.id
        HAVING {hs_prev_expr} > 0 OR hs_consumidas > 0
        ORDER BY hs_consumidas DESC, ot.id DESC
    """.format(hs_prev_expr=hs_prev_expr), (fecha_desde_str,)).fetchall()

    hs_por_ot = []
    for row in ots:
        hs_previstas = round(float(row[2] or 0), 1)
        avance_pct = calcular_avance_ot(db, row[0])
        hs_segun_avance = round((avance_pct / 100.0) * hs_previstas, 1)
        hs_por_ot.append({
            "label": f"OT {row[0]} · {str(row[1] or '')[:24]}",
            "hs_previstas": hs_previstas,
            "hs_consumidas": round(float(row[3] or 0), 1),
            "hs_segun_avance": hs_segun_avance,
            "avance_pct": avance_pct
        })

    kg_rows = db.execute("""
        SELECT pr.proceso,
               SUM(COALESCE(CAST(pd.peso AS REAL), 0)) AS total_kg
        FROM procesos pr
        LEFT JOIN (
            SELECT posicion,
                   COALESCE(obra, '') AS obra,
                   MAX(COALESCE(CAST(peso AS REAL), 0)) AS peso
            FROM procesos
            WHERE COALESCE(escaneado_qr, 0) = 1
            GROUP BY posicion, COALESCE(obra, '')
        ) pd ON pr.posicion = pd.posicion
             AND COALESCE(pr.obra, '') = pd.obra
        WHERE pr.proceso IN ('ARMADO','SOLDADURA','PINTURA','DESPACHO')
          AND pr.fecha >= ?
          AND COALESCE(pr.escaneado_qr, 0) = 1
        GROUP BY pr.proceso
    """, (fecha_desde_str,)).fetchall()

    kg_por_estacion = {"ARMADO": 0.0, "SOLDADURA": 0.0, "PINTURA": 0.0, "DESPACHO": 0.0}
    for row in kg_rows:
        if row[0] in kg_por_estacion:
            kg_por_estacion[row[0]] = round(float(row[1] or 0), 2)

    total_prev = sum(o["hs_previstas"] for o in hs_por_ot)
    total_cons = sum(o["hs_consumidas"] for o in hs_por_ot)
    total_kg = sum(kg_por_estacion.values())
    efic_str = f"{(total_cons / total_prev * 100):.1f}%" if total_prev > 0 else "—"
    kg_hs_str = f"{(total_kg / total_cons):.1f}" if total_cons > 0 else "—"

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=landscape(A3),
        leftMargin=14,
        rightMargin=14,
        topMargin=12,
        bottomMargin=12
    )
    story = []
    styles = getSampleStyleSheet()

    title_style = ParagraphStyle(
        'DashTitleVisual',
        parent=styles['Heading1'],
        fontSize=24,
        leading=28,
        textColor=colors.HexColor('#7c2d12'),
        spaceAfter=2,
        fontName='Helvetica-Bold'
    )
    subtitle_style = ParagraphStyle(
        'DashSubVisual',
        parent=styles['Normal'],
        fontSize=11,
        leading=14,
        textColor=colors.HexColor('#7c2d12')
    )
    card_title_style = ParagraphStyle(
        'CardTitle',
        parent=styles['Normal'],
        fontSize=11,
        leading=13,
        textColor=colors.HexColor('#9a3412'),
        alignment=1,
        fontName='Helvetica-Bold'
    )
    card_value_style = ParagraphStyle(
        'CardValue',
        parent=styles['Normal'],
        fontSize=22,
        leading=24,
        textColor=colors.HexColor('#ea580c'),
        alignment=1,
        fontName='Helvetica-Bold'
    )

    logo_path = os.path.join(_APP_DIR, "LOGO.png")
    logo_flow = Image(logo_path, width=78*mm, height=40*mm) if os.path.exists(logo_path) else Paragraph("<b>A3</b>", subtitle_style)

    logo_header = Table([[logo_flow]], colWidths=[390*mm])
    logo_header.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#fff7ed')),
        ('BOX', (0, 0), (-1, -1), 1.1, colors.HexColor('#f97316')),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('LEFTPADDING', (0, 0), (-1, -1), 6),
        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
    ]))
    story.append(logo_header)
    story.append(Spacer(1, 5))

    header_copy = Paragraph(
        f"<b>Estado de Producción</b><br/>"
        f"<font size='12'>Período: {periodo_label} &nbsp;&nbsp;&nbsp; Desde: {fecha_desde.strftime('%d/%m/%Y')} &nbsp;&nbsp;&nbsp; Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}</font>",
        title_style
    )
    header_text = Table([[header_copy]], colWidths=[390*mm])
    header_text.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#fff7ed')),
        ('BOX', (0, 0), (-1, -1), 1.1, colors.HexColor('#f97316')),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('LEFTPADDING', (0, 0), (-1, -1), 8),
        ('RIGHTPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
    ]))
    story.append(header_text)
    story.append(Spacer(1, 9))

    cards = [
        [Paragraph('HS PREVISTAS', card_title_style), Paragraph(f"{total_prev:.1f}<br/><font size='11'>hs</font>", card_value_style)],
        [Paragraph('HS CONSUMIDAS', card_title_style), Paragraph(f"{total_cons:.1f}<br/><font size='11'>hs</font>", card_value_style)],
        [Paragraph('EFICIENCIA HS', card_title_style), Paragraph(efic_str, card_value_style)],
        [Paragraph('KG PROCESADOS', card_title_style), Paragraph(f"{total_kg:.1f}<br/><font size='11'>kg</font>", card_value_style)],
        [Paragraph('KG / HS', card_title_style), Paragraph(kg_hs_str, card_value_style)],
    ]
    cards_table = Table([cards], colWidths=[78*mm, 78*mm, 78*mm, 78*mm, 78*mm])
    cards_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.white),
        ('BOX', (0, 0), (-1, -1), 0.9, colors.HexColor('#fdba74')),
        ('INNERGRID', (0, 0), (-1, -1), 0.6, colors.HexColor('#fed7aa')),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
    ]))
    story.append(cards_table)
    story.append(Spacer(1, 10))

    top_rows = hs_por_ot[:8]
    hs_prev = [o['hs_previstas'] for o in top_rows] or [0]
    hs_cons = [o['hs_consumidas'] for o in top_rows] or [0]
    hs_segun_av = [o['hs_segun_avance'] for o in top_rows] or [0]
    hs_labels = [o['label'][:16] for o in top_rows] or ['Sin datos']

    estaciones = ['ARMADO Y SOLDADURA', 'PINTURA', 'P/DESPACHO']
    kg_vals = [kg_por_estacion[e] for e in estaciones]

    hs_box = Drawing(820, 250)
    hs_box.add(String(30, 226, 'HS Consumidas vs HS Previstas vs HS según Avance (Top 8 OTs)', fontSize=12, fillColor=colors.HexColor('#7c2d12')))
    hs_chart = VerticalBarChart()
    hs_chart.x = 48
    hs_chart.y = 36
    hs_chart.width = 730
    hs_chart.height = 165
    hs_chart.data = [hs_prev, hs_cons, hs_segun_av]
    hs_chart.categoryAxis.categoryNames = hs_labels
    hs_chart.categoryAxis.labels.angle = 30
    hs_chart.categoryAxis.labels.boxAnchor = 'ne'
    hs_chart.categoryAxis.labels.dx = 8
    hs_chart.categoryAxis.labels.dy = -2
    hs_chart.categoryAxis.labels.fontSize = 8
    hs_chart.valueAxis.valueMin = 0
    hs_chart.valueAxis.valueStep = max(1, int(max(hs_prev + hs_cons + hs_segun_av + [1]) / 6))
    hs_chart.barSpacing = 3
    hs_chart.groupSpacing = 8
    hs_chart.bars[0].fillColor = colors.HexColor('#fdba74')
    hs_chart.bars[1].fillColor = colors.HexColor('#ea580c')
    hs_chart.bars[2].fillColor = colors.HexColor('#86efac')
    hs_box.add(hs_chart)

    hs_container = Table([[hs_box]], colWidths=[390*mm])
    hs_container.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#fffaf5')),
        ('BOX', (0, 0), (-1, -1), 0.9, colors.HexColor('#fdba74')),
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ]))
    story.append(hs_container)
    story.append(Spacer(1, 8))

    kg_bar_draw = Drawing(390, 230)
    kg_bar_draw.add(String(42, 205, 'KG procesados por estación', fontSize=12, fillColor=colors.HexColor('#7c2d12')))
    kg_bar = VerticalBarChart()
    kg_bar.x = 40
    kg_bar.y = 34
    kg_bar.width = 320
    kg_bar.height = 150
    kg_bar.data = [kg_vals if sum(kg_vals) > 0 else [0, 0, 0, 0]]
    kg_bar.categoryAxis.categoryNames = estaciones
    kg_bar.categoryAxis.labels.fontSize = 8
    kg_bar.valueAxis.valueMin = 0
    kg_bar.valueAxis.valueStep = max(1, int(max(kg_vals + [1]) / 5))
    kg_bar.barWidth = 40
    kg_bar.barSpacing = 16
    kg_bar.groupSpacing = 14
    kg_bar.bars[0].fillColor = colors.HexColor('#f97316')
    kg_bar_draw.add(kg_bar)

    kg_pie_draw = Drawing(390, 230)
    kg_pie_draw.add(String(85, 205, 'Distribución de KG en planta', fontSize=12, fillColor=colors.HexColor('#7c2d12')))
    pie = Pie()
    pie.x = 125
    pie.y = 30
    pie.width = 150
    pie.height = 150
    if sum(kg_vals) > 0:
        pie.data = kg_vals
        pie.labels = [f"{estaciones[i]} {kg_vals[i]:.1f}" for i in range(len(estaciones))]
    else:
        pie.data = [1]
        pie.labels = ['Sin datos']
    pie_colors = [colors.HexColor('#f97316'), colors.HexColor('#ea580c'), colors.HexColor('#c2410c'), colors.HexColor('#7c2d12')]
    for i in range(len(pie.data)):
        pie.slices[i].fillColor = pie_colors[i % len(pie_colors)]
    kg_pie_draw.add(pie)

    estaciones_cards = []
    for idx, est in enumerate(estaciones):
        kg_v = kg_por_estacion[est]
        est_cell = Table([[Paragraph(f"<b>{est}</b><br/><font size='13'>{kg_v:.1f} kg</font>", subtitle_style)]], colWidths=[90*mm])
        est_colors = ['#fff7ed', '#ffedd5', '#fed7aa', '#fdba74']
        est_cell.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor(est_colors[idx % len(est_colors)])),
            ('BOX', (0, 0), (-1, -1), 0.7, colors.HexColor('#f97316')),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 9),
        ]))
        estaciones_cards.append(est_cell)

    estaciones_table = Table([[estaciones_cards[0], estaciones_cards[1]], [estaciones_cards[2], estaciones_cards[3]]], colWidths=[90*mm, 90*mm])
    estaciones_table.setStyle(TableStyle([
        ('LEFTPADDING', (0, 0), (-1, -1), 4),
        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]))

    lower_left = Table([[kg_bar_draw]], colWidths=[186*mm])
    lower_left.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.white),
        ('BOX', (0, 0), (-1, -1), 0.8, colors.HexColor('#fdba74')),
    ]))

    right_stack = Table([[kg_pie_draw], [Spacer(1, 3)], [estaciones_table]], colWidths=[186*mm])
    right_stack.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.white),
        ('BOX', (0, 0), (-1, 0), 0.8, colors.HexColor('#fdba74')),
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ]))

    lower = Table([[lower_left, right_stack]], colWidths=[186*mm, 186*mm])
    lower.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ]))
    story.append(lower)

    doc.build(story)
    buf.seek(0)

    fname = f"estado_produccion_visual_{periodo}_{today.strftime('%Y%m%d')}.pdf"
    _guardar_pdf_databook("GENERAL", "produccion", fname, buf.getvalue())
    buf.seek(0)
    return send_file(
        buf,
        mimetype="application/pdf",
        as_attachment=True,
        download_name=fname
    )
