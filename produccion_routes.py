import os

from flask import Blueprint
from db_utils import get_db
from proceso_utils import obtener_procesos_completados
from qr_utils import find_col, load_clean_excel

produccion_bp = Blueprint("produccion", __name__)
_APP_DIR = os.path.dirname(os.path.abspath(__file__))
_DATABOOKS_DIR = os.path.join(_APP_DIR, "Reportes Produccion")


def _to_float(value, default=0.0):
    try:
        if value is None:
            return default
        return float(str(value).replace(",", "."))
    except Exception:
        return default


def _pos_base(posicion):
    pos = str(posicion or "").strip().upper()
    if "-" in pos:
        return pos.split("-", 1)[0].strip()
    return pos


def _buscar_excel_armado(obra):
    obra_txt = str(obra or "").strip()
    if not obra_txt:
        return ""

    carpeta_obra = os.path.join(_DATABOOKS_DIR, obra_txt)
    if not os.path.isdir(carpeta_obra):
        return ""

    candidatos = []
    for carpeta_ot in os.listdir(carpeta_obra):
        ruta_ot = os.path.join(carpeta_obra, carpeta_ot)
        if not os.path.isdir(ruta_ot) or not str(carpeta_ot).upper().startswith("OT "):
            continue
        for raiz, _, archivos in os.walk(ruta_ot):
            for nombre in archivos:
                ext = os.path.splitext(nombre)[1].lower()
                if ext in (".xlsx", ".xls"):
                    ruta = os.path.join(raiz, nombre)
                    if os.path.isfile(ruta):
                        candidatos.append(ruta)

    if not candidatos:
        return ""

    candidatos.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    return candidatos[0]


def _kg_por_pos_desde_excel_armado(excel_path):
    if not excel_path:
        return {}
    try:
        df = load_clean_excel(excel_path)
        col_pos = find_col(df, "POS")
        col_cant = find_col(df, "CANT")
        col_peso = find_col(df, "PESO")
        if not col_pos or not col_peso or not col_cant:
            return {}

        kg_por_pos = {}
        for _, row in df.iterrows():
            pos = str(row.get(col_pos, "") or "").strip()
            if not pos:
                continue
            cant = _to_float(row.get(col_cant, 0), 0.0)
            peso = _to_float(row.get(col_peso, 0), 0.0)
            if cant <= 0 or peso <= 0:
                continue
            kg = cant * peso
            # Si la posición se repite en planilla, acumulamos sus kg.
            kg_por_pos[pos] = kg_por_pos.get(pos, 0.0) + kg
        return kg_por_pos
    except Exception:
        return {}


def _avance_desde_excel_armado(excel_path):
    if not excel_path:
        return 0.0, 0.0, False
    try:
        df = load_clean_excel(excel_path)
        col_cant = find_col(df, "CANT")
        col_peso = find_col(df, "PESO")
        col_total = find_col(df, "TOTAL")
        if not col_cant or not col_peso:
            return 0.0, 0.0, False

        total_kg = 0.0
        procesado_kg = 0.0
        for _, row in df.iterrows():
            cant = _to_float(row.get(col_cant, 0), 0.0)
            peso = _to_float(row.get(col_peso, 0), 0.0)
            if cant <= 0 or peso <= 0:
                continue

            total_kg += cant * peso

            if col_total:
                procesado_kg += max(0.0, _to_float(row.get(col_total, 0), 0.0))

        return total_kg, procesado_kg, bool(col_total)
    except Exception:
        return 0.0, 0.0, False


def _avance_ratio_desde_aprobados(aprobados):
    ratio = 0.0
    if "ARMADO" in aprobados:
        ratio += 0.70
    if "SOLDADURA" in aprobados:
        ratio += 0.10
    if "PINTURA" in aprobados:
        ratio += 0.15
    if "DESPACHO" in aprobados:
        ratio += 0.05
    return ratio


def _avance_estimado_excel_sin_total(db, ot_id, excel_path):
    if not excel_path:
        return 0.0, 0.0
    try:
        df = load_clean_excel(excel_path)
        col_pos = find_col(df, "POS")
        col_cant = find_col(df, "CANT")
        col_peso = find_col(df, "PESO")
        if not col_pos or not col_cant or not col_peso:
            return 0.0, 0.0

        # Progreso por posición real registrada en BD, consolidado por posición base.
        posiciones_bd = db.execute(
            """
            SELECT DISTINCT TRIM(COALESCE(posicion, ''))
            FROM procesos
            WHERE ot_id = ?
              AND TRIM(COALESCE(posicion, '')) <> ''
            """,
            (ot_id,),
        ).fetchall()

        ratio_por_base = {}
        for (pos_real,) in posiciones_bd:
            pos_real_txt = str(pos_real or "").strip()
            if not pos_real_txt:
                continue
            base = _pos_base(pos_real_txt)
            aprobados = set(obtener_procesos_completados(pos_real_txt, ot_id=ot_id))
            ratio = _avance_ratio_desde_aprobados(aprobados)
            ratio_por_base[base] = max(ratio_por_base.get(base, 0.0), ratio)

        total_kg = 0.0
        procesado_kg = 0.0
        for _, row in df.iterrows():
            pos_excel = str(row.get(col_pos, "") or "").strip()
            if not pos_excel:
                continue
            base = _pos_base(pos_excel)

            cant = _to_float(row.get(col_cant, 0), 0.0)
            peso = _to_float(row.get(col_peso, 0), 0.0)
            if cant <= 0 or peso <= 0:
                continue

            # Denominador: kg totales de planilla (cantidad * peso)
            total_kg += cant * peso

            # Numerador sin columna TOTAL: se toma peso por fila * avance de la posición.
            ratio = ratio_por_base.get(base, 0.0)
            procesado_kg += peso * ratio

        return total_kg, procesado_kg
    except Exception:
        return 0.0, 0.0


def calcular_avance_ot(db, ot_id):
    obra_row = db.execute(
        "SELECT TRIM(COALESCE(obra, '')) FROM ordenes_trabajo WHERE id = ?",
        (ot_id,),
    ).fetchone()
    obra = (obra_row[0] if obra_row else "") or ""

    # Fuente principal: Excel de ARMADO en DataBooks/<obra>/3-Produccion
    excel_path = _buscar_excel_armado(obra)

    # Si la planilla ya tiene columna TOTAL por fila, usamos exactamente esos valores.
    total_excel, procesado_excel, tiene_total_excel = _avance_desde_excel_armado(excel_path)
    if total_excel > 0 and tiene_total_excel:
        porcentaje_excel = round((procesado_excel / total_excel) * 100)
        if porcentaje_excel < 0:
            return 0
        if porcentaje_excel > 100:
            return 100
        return porcentaje_excel

    # Si no hay TOTAL en Excel, estimamos el procesado desde estados de BD.
    total_est, procesado_est = _avance_estimado_excel_sin_total(db, ot_id, excel_path)
    if total_est > 0:
        porcentaje_est = round((procesado_est / total_est) * 100)
        if porcentaje_est < 0:
            return 0
        if porcentaje_est > 100:
            return 100
        return porcentaje_est

    kg_por_pos = _kg_por_pos_desde_excel_armado(excel_path)

    # Fallback: metadata cargada en procesos
    if not kg_por_pos:
        piezas_meta = db.execute(
            """
            SELECT TRIM(COALESCE(posicion, '')) AS posicion,
                   MAX(COALESCE(cantidad, 0)) AS cantidad_pieza,
                   MAX(COALESCE(peso, 0)) AS peso_pieza
            FROM procesos
            WHERE ot_id = ?
              AND TRIM(COALESCE(posicion, '')) <> ''
            GROUP BY TRIM(COALESCE(posicion, ''))
            HAVING MAX(COALESCE(peso, 0)) > 0
            """,
            (ot_id,),
        ).fetchall()
        kg_por_pos = {}
        for pos, cantidad, peso in piezas_meta:
            kg = _to_float(cantidad, 0.0) * _to_float(peso, 0.0)
            if kg > 0:
                kg_por_pos[str(pos)] = kg

    if not kg_por_pos:
        return 0

    total_kg = 0.0
    avance_kg = 0.0

    for posicion, kg_pieza in kg_por_pos.items():
        kg_pieza = _to_float(kg_pieza, 0.0)
        if kg_pieza <= 0:
            continue

        total_kg += kg_pieza

        procesos_aprobados = set(obtener_procesos_completados(posicion, ot_id=ot_id))
        avance_pieza = 0.0
        if "ARMADO" in procesos_aprobados:
            avance_pieza += 70.0
        if "SOLDADURA" in procesos_aprobados:
            avance_pieza += 10.0
        if "PINTURA" in procesos_aprobados:
            avance_pieza += 15.0
        if "DESPACHO" in procesos_aprobados:
            avance_pieza += 5.0

        avance_kg += kg_pieza * (avance_pieza / 100.0)

    if total_kg <= 0:
        return 0

    porcentaje = round((avance_kg / total_kg) * 100)
    if porcentaje < 0:
        return 0
    if porcentaje > 100:
        return 100
    return porcentaje


def _persistir_avance_ot(db, ot_id, progreso_calculado, progreso_actual):
    actual = int(progreso_actual or 0)
    nuevo = int(progreso_calculado or 0)
    if actual == nuevo:
        return False
    db.execute(
        "UPDATE ordenes_trabajo SET estado_avance = ? WHERE id = ?",
        (nuevo, ot_id),
    )
    return True


@produccion_bp.route("/modulo/produccion")
def produccion():
    db = get_db()
    ots_en_curso = db.execute(
        """
        SELECT id, cliente, obra, titulo, tipo_estructura, fecha_entrega, estado, COALESCE(estado_avance, 0)
        FROM ordenes_trabajo
        WHERE estado != 'Finalizada' AND fecha_cierre IS NULL AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
        ORDER BY fecha_entrega ASC
        """
    ).fetchall()

    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * { box-sizing: border-box; }
    body { font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }
    h2 { color: #333; border-bottom: 3px solid #f093fb; padding-bottom: 10px; }
    .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
    .btn { display: inline-block; background: #667eea; color: white; padding: 10px 15px;
           text-decoration: none; border-radius: 5px; font-weight: bold; }
    .btn:hover { background: #5568d3; }
    table { width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
    th, td { padding: 12px; border-bottom: 1px solid #ddd; text-align: left; }
    th { background: #f093fb; color: white; font-weight: bold; }
    tr:hover { background: #f5f5f5; }
    .progress { width: 100%; height: 20px; background: #e0e0e0; border-radius: 10px; overflow: hidden; }
    .progress-bar { height: 100%; background: linear-gradient(90deg, #43e97b, #38f9d7); text-align: center;
                    color: white; font-size: 12px; line-height: 20px; }
    .sin-datos { text-align: center; padding: 30px; color: #999; }
    .nota { margin: 0 0 14px 0; color: #475569; font-size: 13px; }
    </style>
    </head>
    <body>
    <div class="header">
        <h2>🏭 Control de Producción</h2>
        <a href="/" class="btn">⬅️ Volver</a>
    </div>
    <p class="nota">% Avance automático por OT: KG de ARMADO y ponderación 70/10/15/5 por proceso aprobado.</p>
    """

    if len(ots_en_curso) == 0:
        html += "<div class='sin-datos'>✅ No hay órdenes en curso. ¡Todas finalizadas!</div>"
    else:
        html += """
        <table>
            <tr>
                <th>ID</th>
                <th>Cliente</th>
                <th>Obra</th>
                <th>Título</th>
                <th>Tipo de Estructura</th>
                <th>% Avance</th>
                <th>Fecha Entrega</th>
                <th>Estado</th>
            </tr>
        """

        hubo_cambios = False
        for ot_id, cliente, obra, titulo, tipo_estructura, fecha_entrega, estado, estado_avance_actual in ots_en_curso:
            progreso = calcular_avance_ot(db, ot_id)
            if _persistir_avance_ot(db, ot_id, progreso, estado_avance_actual):
                hubo_cambios = True
            html += f"""
            <tr>
                <td><b>{ot_id}</b></td>
                <td>{cliente or ''}</td>
                <td>{obra or ''}</td>
                <td>{titulo or ''}</td>
                <td>{tipo_estructura or ''}</td>
                <td>
                    <div class="progress">
                        <div class="progress-bar" style="width: {progreso}%">{progreso}%</div>
                    </div>
                </td>
                <td>{fecha_entrega or ''}</td>
                <td>{estado or ''}</td>
            </tr>
            """

        if hubo_cambios:
            db.commit()

        html += "</table>"

    html += """
    </body>
    </html>
    """
    return html
