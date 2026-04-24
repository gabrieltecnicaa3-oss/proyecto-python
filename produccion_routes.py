import os

from flask import Blueprint, request
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
    if "P/DESPACHO" in aprobados or "DESPACHO" in aprobados:
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
        if "P/DESPACHO" in procesos_aprobados or "DESPACHO" in procesos_aprobados:
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


def _desglose_ot(db, ot_id):
    posiciones_rows = db.execute(
        """
        SELECT DISTINCT TRIM(COALESCE(posicion, ''))
        FROM procesos
        WHERE ot_id = ?
          AND eliminado = 0
          AND TRIM(COALESCE(posicion, '')) <> ''
        """,
        (ot_id,),
    ).fetchall()

    posiciones = [str(r[0] or "").strip() for r in posiciones_rows if str(r[0] or "").strip()]
    total = len(posiciones)
    conteo = {
        "ARMADO": 0,
        "SOLDADURA": 0,
        "PINTURA": 0,
        "P/DESPACHO": 0,
    }
    if total == 0:
        return total, conteo

    for pos in posiciones:
        aprobados = set(obtener_procesos_completados(pos, ot_id=ot_id))
        for proceso in ("ARMADO", "SOLDADURA", "PINTURA"):
            if proceso in aprobados:
                conteo[proceso] += 1
        if "P/DESPACHO" in aprobados or "DESPACHO" in aprobados:
            conteo["P/DESPACHO"] += 1

    return total, conteo


@produccion_bp.route("/modulo/produccion")
def produccion():
    db = get_db()
    filtro_obra = (request.args.get("obra") or "").strip()
    filtro_ot_txt = (request.args.get("ot_id") or "").strip()
    filtro_ot_id = int(filtro_ot_txt) if filtro_ot_txt.isdigit() else None

    ots_en_curso = db.execute(
        """
        SELECT id, cliente, obra, titulo, tipo_estructura, fecha_entrega, estado, COALESCE(estado_avance, 0)
        FROM ordenes_trabajo
        WHERE estado != 'Finalizada' AND fecha_cierre IS NULL AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
        ORDER BY fecha_entrega ASC
        """
    ).fetchall()

    filas_ot = []
    hubo_cambios = False
    for ot_id, cliente, obra, titulo, tipo_estructura, fecha_entrega, estado, estado_avance_actual in ots_en_curso:
        progreso = calcular_avance_ot(db, ot_id)
        if _persistir_avance_ot(db, ot_id, progreso, estado_avance_actual):
            hubo_cambios = True
        total_piezas, conteo_procesos = _desglose_ot(db, ot_id)
        filas_ot.append({
            "ot_id": int(ot_id),
            "cliente": cliente or "",
            "obra": (obra or "").strip(),
            "titulo": titulo or "",
            "tipo_estructura": tipo_estructura or "",
            "fecha_entrega": fecha_entrega or "",
            "estado": estado or "",
            "progreso": int(progreso),
            "total_piezas": int(total_piezas),
            "conteo": conteo_procesos,
        })

    if hubo_cambios:
        db.commit()

    obras_disponibles = sorted({f["obra"] for f in filas_ot if f["obra"]})
    ots_disponibles = sorted(filas_ot, key=lambda x: x["ot_id"], reverse=True)

    filas_filtradas = []
    for fila in filas_ot:
        if filtro_obra and fila["obra"] != filtro_obra:
            continue
        if filtro_ot_id and fila["ot_id"] != filtro_ot_id:
            continue
        filas_filtradas.append(fila)

    resumen_obra = {}
    for fila in filas_filtradas:
        obra_key = fila["obra"] or "Sin obra"
        if obra_key not in resumen_obra:
            resumen_obra[obra_key] = {
                "ots": 0,
                "avance_sum": 0,
            }
        resumen_obra[obra_key]["ots"] += 1
        resumen_obra[obra_key]["avance_sum"] += int(fila["progreso"])

    opciones_obra = '<option value="">Todas las obras</option>'
    for obra in obras_disponibles:
        sel = "selected" if obra == filtro_obra else ""
        opciones_obra += f'<option value="{obra}" {sel}>{obra}</option>'

    opciones_ot = '<option value="">Todas las OT</option>'
    for fila in ots_disponibles:
        sel = "selected" if filtro_ot_id and int(fila["ot_id"]) == int(filtro_ot_id) else ""
        opciones_ot += f'<option value="{fila["ot_id"]}" {sel}>OT {fila["ot_id"]} - {fila["obra"] or "(sin obra)"}</option>'

    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * { box-sizing: border-box; }
    body { font-family: Arial; padding: 14px; background: #fff7ed; margin: 0; color: #431407; }
    h2 { color: #9a3412; border-bottom: 3px solid #f97316; padding-bottom: 10px; margin: 0; }
    .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 14px; gap: 10px; flex-wrap: wrap; }
    .btn { display: inline-block; background: #f97316; color: white; padding: 10px 15px;
           text-decoration: none; border-radius: 6px; font-weight: bold; }
    .btn:hover { background: #ea580c; }
    .panel { background: #fff; border: 1px solid #fed7aa; border-radius: 10px; padding: 12px; margin-bottom: 12px; }
    .filters { display: grid; grid-template-columns: repeat(3, minmax(180px, 1fr)); gap: 10px; align-items: end; }
    .filters label { font-size: 13px; font-weight: 700; color: #9a3412; display: block; margin-bottom: 4px; }
    .filters select { width: 100%; padding: 9px; border: 1px solid #fdba74; border-radius: 6px; background: #fffaf5; }
    .btn-filter { border: none; cursor: pointer; height: 40px; }
    .kpis { display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr)); gap: 10px; margin: 10px 0 0 0; }
    .kpi { background: #fff7ed; border: 1px solid #fdba74; border-radius: 8px; padding: 10px; }
    .kpi .t { color: #9a3412; font-size: 12px; }
    .kpi .v { color: #7c2d12; font-weight: 700; font-size: 24px; }
    .obra-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); gap: 10px; }
    .obra-card { background: #fffaf5; border: 1px solid #fed7aa; border-left: 4px solid #f97316; border-radius: 8px; padding: 10px; }
    .obra-card .nombre { font-weight: 700; color: #9a3412; }
    .obra-card .avance { font-size: 22px; font-weight: 700; color: #ea580c; }
    .table-wrap { width: 100%; overflow-x: auto; border-radius: 8px; }
    table { width: 100%; min-width: 1080px; border-collapse: collapse; background: white; box-shadow: 0 2px 8px rgba(0,0,0,0.08); }
    th, td { padding: 10px; border-bottom: 1px solid #fed7aa; text-align: left; }
    th { background: #f97316; color: white; font-weight: bold; }
    tr:hover { background: #fff7ed; }
    .progress { width: 100%; height: 20px; background: #ffedd5; border-radius: 10px; overflow: hidden; border: 1px solid #fdba74; }
    .progress-bar { height: 100%; background: linear-gradient(90deg, #f97316, #fb923c); text-align: center;
                    color: white; font-size: 12px; line-height: 20px; font-weight: 700; }
    .desglose { display: flex; gap: 6px; flex-wrap: wrap; }
    .chip { font-size: 11px; border-radius: 999px; padding: 3px 8px; background: #fff7ed; border: 1px solid #fdba74; color: #9a3412; }
    .sin-datos { text-align: center; padding: 30px; color: #9a3412; }
    .nota { margin: 0 0 12px 0; color: #7c2d12; font-size: 13px; }
    @media (max-width: 900px) {
        .filters { grid-template-columns: 1fr; }
        .header { flex-direction: column; align-items: stretch; }
        .header .btn { text-align: center; }
    }
    </style>
    </head>
    <body>
    <div class="header">
        <h2>🏭 Control de Producción</h2>
        <a href="/" class="btn">⬅️ Volver</a>
    </div>
    <p class="nota">% Avance automático por OT: KG de ARMADO y ponderación 70/10/15/5 por proceso aprobado.</p>
    """

    html += f"""
    <div class="panel">
        <form method="get" action="/modulo/produccion" class="filters">
            <div>
                <label>Filtrar por obra</label>
                <select name="obra">
                    {opciones_obra}
                </select>
            </div>
            <div>
                <label>Filtrar por OT</label>
                <select name="ot_id">
                    {opciones_ot}
                </select>
            </div>
            <button type="submit" class="btn btn-filter">Aplicar filtros</button>
        </form>
        <div class="kpis">
            <div class="kpi"><div class="t">OT visibles</div><div class="v">{len(filas_filtradas)}</div></div>
            <div class="kpi"><div class="t">Obras visibles</div><div class="v">{len(resumen_obra)}</div></div>
            <div class="kpi"><div class="t">Suma avance OT (visible)</div><div class="v">{sum([f['progreso'] for f in filas_filtradas])}%</div></div>
        </div>
    </div>
    """

    if resumen_obra:
        html += '<div class="panel"><h3 style="margin:0 0 10px 0;color:#9a3412;">Desglose por obra</h3><div class="obra-grid">'
        for obra_key in sorted(resumen_obra.keys()):
            data = resumen_obra[obra_key]
            html += f"""
            <div class="obra-card">
                <div class="nombre">{obra_key}</div>
                <div class="avance">{data['avance_sum']}%</div>
                <div style="font-size:12px;color:#7c2d12;">Avance obra = suma de avances de sus OT ({data['ots']} OT)</div>
            </div>
            """
        html += "</div></div>"

    if len(filas_filtradas) == 0:
        html += "<div class='sin-datos'>✅ No hay órdenes en curso. ¡Todas finalizadas!</div>"
    else:
        html += """
        <div class="table-wrap">
        <table>
            <tr>
                <th>ID</th>
                <th>Cliente</th>
                <th>Obra</th>
                <th>Título</th>
                <th>Tipo de Estructura</th>
                <th>% Avance</th>
                <th>Desglose por proceso</th>
                <th>Fecha Entrega</th>
                <th>Estado</th>
            </tr>
        """

        for fila in filas_filtradas:
            ot_id = fila["ot_id"]
            progreso = fila["progreso"]
            total_piezas = fila["total_piezas"]
            conteo = fila["conteo"]
            html += f"""
            <tr>
                <td><b>{ot_id}</b></td>
                <td>{fila['cliente']}</td>
                <td>{fila['obra']}</td>
                <td>{fila['titulo']}</td>
                <td>{fila['tipo_estructura']}</td>
                <td>
                    <div class="progress">
                        <div class="progress-bar" style="width: {progreso}%">{progreso}%</div>
                    </div>
                </td>
                <td>
                    <div class="desglose">
                        <span class="chip">Pzas {total_piezas}</span>
                        <span class="chip">A {conteo['ARMADO']}/{total_piezas}</span>
                        <span class="chip">S {conteo['SOLDADURA']}/{total_piezas}</span>
                        <span class="chip">P {conteo['PINTURA']}/{total_piezas}</span>
                        <span class="chip">PD {conteo['P/DESPACHO']}/{total_piezas}</span>
                    </div>
                </td>
                <td>{fila['fecha_entrega']}</td>
                <td>{fila['estado']}</td>
            </tr>
            """

        html += "</table></div>"

    html += """
    </body>
    </html>
    """
    return html
