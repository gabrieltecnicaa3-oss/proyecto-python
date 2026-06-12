import os
import time

from flask import Blueprint, request
from db_utils import get_db
from proceso_utils import obtener_procesos_completados, ORDEN_PROCESOS, _ot_no_requiere_pintura, _proceso_aprobado
from qr_utils import find_col, load_clean_excel

produccion_bp = Blueprint("produccion", __name__)
_APP_DIR = os.path.dirname(os.path.abspath(__file__))
_DATABOOKS_DIR = os.path.join(_APP_DIR, "Reportes Produccion")

# Cache de DataFrames de Excel: evita re-leer el mismo archivo en cada request.
# Clave: ruta absoluta del archivo. Valor: (mtime, DataFrame).
_excel_df_cache: dict = {}

# Cache de rutas de Excel por obra: evita os.walk en cada request.
# Clave: obra. Valor: (dir_mtime, excel_path, ts).
_excel_path_cache: dict = {}

# Cache de avance+desglose por OT: evita recalcular en ráfaga de requests.
# Clave: ot_id. Valor: (max_proc_id, avance, total_piezas, conteo, ts).
_avance_cache: dict = {}

_EXCEL_PATH_CACHE_TTL_SECONDS = 45
_AVANCE_CACHE_TTL_SECONDS = 8


def _get_df_cached(excel_path):
    """Devuelve el DataFrame del Excel usando cache basado en mtime."""
    if not excel_path or not os.path.isfile(excel_path):
        return None
    try:
        mtime = os.path.getmtime(excel_path)
        cached = _excel_df_cache.get(excel_path)
        if cached and cached[0] == mtime:
            return cached[1]
        df = load_clean_excel(excel_path)
        _excel_df_cache[excel_path] = (mtime, df)
        return df
    except Exception:
        return None


def _aprobados_de_filas(filas, orden_flujo=None):
    """Replica la logica de obtener_procesos_completados sin acceso a DB.

    filas: lista de tuplas (proceso, estado, re_inspeccion, reproceso).
    Retorna lista de procesos aprobados en orden, sin saltos.
    """
    flujo = list(orden_flujo or ORDEN_PROCESOS)
    aprobados = set()
    for proceso, estado, re_inspeccion, reproceso in filas:
        proc = (proceso or "").strip().upper()
        if proc == "P/DESPACHO":
            proc = "DESPACHO"
        if proc not in ORDEN_PROCESOS:
            continue
        if proc == "PINTURA":
            repro_u = str(reproceso or "").strip().upper()
            if "ETAPA:SUPERFICIE" in repro_u or "ETAPA:FONDO" in repro_u:
                continue
        if _proceso_aprobado(estado, re_inspeccion):
            aprobados.add(proc)
    completados = []
    for proc in flujo:
        if proc in aprobados:
            completados.append(proc)
        else:
            break
    return completados


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

    # Devolver desde cache si no vencio TTL y el directorio no fue modificado
    try:
        now = time.time()
        dir_mtime = os.path.getmtime(carpeta_obra)
        cached = _excel_path_cache.get(obra_txt)
        if cached and cached[0] == dir_mtime and (now - cached[2]) <= _EXCEL_PATH_CACHE_TTL_SECONDS:
            return cached[1]
    except Exception:
        dir_mtime = None
        now = time.time()

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
        if dir_mtime is not None:
            _excel_path_cache[obra_txt] = (dir_mtime, "", now)
        return ""

    candidatos.sort(key=lambda p: os.path.getmtime(p), reverse=True)
    result = candidatos[0]
    if dir_mtime is not None:
        _excel_path_cache[obra_txt] = (dir_mtime, result, now)
    return result


def _kg_por_pos_desde_excel_armado(excel_path):
    if not excel_path:
        return {}
    try:
        df = _get_df_cached(excel_path)
        if df is None:
            return {}
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
        df = _get_df_cached(excel_path)
        if df is None:
            return 0.0, 0.0, False
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


def _pesos_avance_por_ot(db, ot_id, obra=""):
    if _ot_no_requiere_pintura(db, obra=obra, ot_id=ot_id):
        return {
            "ARMADO": 70.0,
            "SOLDADURA": 10.0,
            "PINTURA": 0.0,
            "DESPACHO": 25.0,
        }
    return {
        "ARMADO": 70.0,
        "SOLDADURA": 10.0,
        "PINTURA": 15.0,
        "DESPACHO": 5.0,
    }


def _descripciones_por_pos_ot(db, ot_id):
    rows = db.execute(
        """
        SELECT TRIM(COALESCE(posicion,'')) AS posicion,
               TRIM(COALESCE(descripcion,'')) AS descripcion
        FROM procesos
        WHERE ot_id = ?
          AND TRIM(COALESCE(posicion,'')) <> ''
        ORDER BY id DESC
        """,
        (ot_id,),
    ).fetchall()
    desc_por_pos = {}
    for pos, desc in rows:
        pos_txt = str(pos or "").strip()
        if not pos_txt or pos_txt in desc_por_pos:
            continue
        desc_por_pos[pos_txt] = str(desc or "").strip()

    # Fallback: para posiciones sin descripcion en filas con ot_id, buscar en toda la tabla
    posiciones_sin_desc = [p for p, d in desc_por_pos.items() if not d]
    for pos_txt in posiciones_sin_desc:
        row_fb = db.execute(
            """
            SELECT TRIM(COALESCE(descripcion,''))
            FROM procesos
            WHERE TRIM(COALESCE(posicion,'')) = TRIM(?)
              AND TRIM(COALESCE(descripcion,'')) <> ''
            ORDER BY id DESC
            LIMIT 1
            """,
            (pos_txt,),
        ).fetchone()
        if row_fb and row_fb[0]:
            desc_por_pos[pos_txt] = str(row_fb[0]).strip()

    return desc_por_pos


def _es_descripcion_inserto(descripcion):
    return "INSERTO" in str(descripcion or "").strip().upper()


def _es_inserto(desc_pieza, pos=None):
    """Retorna True si la pieza es un inserto (por descripción o por código de posición)."""
    if _es_descripcion_inserto(desc_pieza):
        return True
    return str(pos or "").strip().upper().startswith("INS")


def _pesos_avance_por_pieza(desc_pieza, pesos_ot, pos=None):
    if _es_inserto(desc_pieza, pos=pos):
        return {
            "ARMADO": 90.0,
            "SOLDADURA": 0.0,
            "PINTURA": 0.0,
            "DESPACHO": 10.0,
        }
    return dict(pesos_ot)


def _avance_ratio_desde_aprobados(aprobados, pesos):
    ratio = 0.0
    if "ARMADO" in aprobados:
        ratio += pesos["ARMADO"] / 100.0
    if "SOLDADURA" in aprobados:
        ratio += pesos["SOLDADURA"] / 100.0
    if "PINTURA" in aprobados:
        ratio += pesos["PINTURA"] / 100.0
    if "P/DESPACHO" in aprobados or "DESPACHO" in aprobados:
        ratio += pesos["DESPACHO"] / 100.0
    return ratio


def _avance_estimado_excel_sin_total(db, ot_id, excel_path):
    if not excel_path:
        return 0.0, 0.0
    try:
        df = _get_df_cached(excel_path)
        if df is None:
            return 0.0, 0.0
        col_pos = find_col(df, "POS")
        col_cant = find_col(df, "CANT")
        col_peso = find_col(df, "PESO")
        if not col_pos or not col_cant or not col_peso:
            return 0.0, 0.0

        # Batch: una sola consulta para todos los procesos de la OT.
        all_proc_rows = db.execute(
            "SELECT TRIM(COALESCE(posicion,'')), proceso, estado, re_inspeccion, reproceso"
            " FROM procesos WHERE ot_id=? ORDER BY id",
            (ot_id,),
        ).fetchall()
        filas_por_pos: dict = {}
        for _pos, _proc, _est, _reinsp, _repro in all_proc_rows:
            if _pos:
                filas_por_pos.setdefault(_pos, []).append((_proc, _est, _reinsp, _repro))

        obra_row = db.execute(
            "SELECT TRIM(COALESCE(obra, '')) FROM ordenes_trabajo WHERE id = ?",
            (ot_id,),
        ).fetchone()
        obra = (obra_row[0] if obra_row else "") or ""
        pesos = _pesos_avance_por_ot(db, ot_id, obra)
        orden_flujo = ["ARMADO", "SOLDADURA", "DESPACHO"] if _ot_no_requiere_pintura(db, obra=obra, ot_id=ot_id) else list(ORDEN_PROCESOS)

        desc_por_pos = _descripciones_por_pos_ot(db, ot_id)
        ratio_por_base = {}
        for pos_real_txt, filas in filas_por_pos.items():
            base = _pos_base(pos_real_txt)
            desc_pos = desc_por_pos.get(pos_real_txt, "")
            # Usar flujo específico por pieza: INSERTO → solo ARMADO→DESPACHO
            if _es_inserto(desc_pos, pos=pos_real_txt):
                orden_flujo_pos = ["ARMADO", "DESPACHO"]
            else:
                orden_flujo_pos = orden_flujo
            aprobados = set(_aprobados_de_filas(filas, orden_flujo=orden_flujo_pos))
            pesos_pos = _pesos_avance_por_pieza(desc_pos, pesos, pos=pos_real_txt)
            ratio = _avance_ratio_desde_aprobados(aprobados, pesos_pos)
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
    pesos = _pesos_avance_por_ot(db, ot_id, obra)

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
        # Sin datos de peso: calcula avance ponderado por conteo de piezas
        posiciones_row = db.execute(
            "SELECT DISTINCT TRIM(COALESCE(posicion,'')) FROM procesos"
            " WHERE ot_id=? AND COALESCE(eliminado,0)=0 AND TRIM(COALESCE(posicion,''))<>''",
            (ot_id,),
        ).fetchall()
        posiciones = [r[0] for r in posiciones_row if r[0]]
        if not posiciones:
            return 0
        desc_por_pos = _descripciones_por_pos_ot(db, ot_id)
        avance_sum = 0.0
        for posicion in posiciones:
            procesos_aprobados = set(obtener_procesos_completados(posicion, ot_id=ot_id))
            pesos_pos = _pesos_avance_por_pieza(desc_por_pos.get(str(posicion), ""), pesos, pos=posicion)
            avance_pieza = 0.0
            if "ARMADO" in procesos_aprobados:
                avance_pieza += pesos_pos["ARMADO"]
            if "SOLDADURA" in procesos_aprobados:
                avance_pieza += pesos_pos["SOLDADURA"]
            if "PINTURA" in procesos_aprobados:
                avance_pieza += pesos_pos["PINTURA"]
            if "P/DESPACHO" in procesos_aprobados or "DESPACHO" in procesos_aprobados:
                avance_pieza += pesos_pos["DESPACHO"]
            avance_sum += avance_pieza / 100.0
        porcentaje = round((avance_sum / len(posiciones)) * 100)
        return max(0, min(100, porcentaje))

    total_kg = 0.0
    avance_kg = 0.0
    desc_por_pos = _descripciones_por_pos_ot(db, ot_id)

    for posicion, kg_pieza in kg_por_pos.items():
        kg_pieza = _to_float(kg_pieza, 0.0)
        if kg_pieza <= 0:
            continue

        total_kg += kg_pieza

        procesos_aprobados = set(obtener_procesos_completados(posicion, ot_id=ot_id))
        pesos_pos = _pesos_avance_por_pieza(desc_por_pos.get(str(posicion), ""), pesos, pos=posicion)
        avance_pieza = 0.0
        if "ARMADO" in procesos_aprobados:
            avance_pieza += pesos_pos["ARMADO"]
        if "SOLDADURA" in procesos_aprobados:
            avance_pieza += pesos_pos["SOLDADURA"]
        if "PINTURA" in procesos_aprobados:
            avance_pieza += pesos_pos["PINTURA"]
        if "P/DESPACHO" in procesos_aprobados or "DESPACHO" in procesos_aprobados:
            avance_pieza += pesos_pos["DESPACHO"]

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
    orden_flujo = ["ARMADO", "SOLDADURA", "DESPACHO"] if _ot_no_requiere_pintura(db, ot_id=ot_id) else list(ORDEN_PROCESOS)
    # Obtener posiciones validas (no eliminadas) — una sola consulta.
    pos_rows = db.execute(
        "SELECT DISTINCT TRIM(COALESCE(posicion,''))"
        " FROM procesos"
        " WHERE ot_id=? AND eliminado=0 AND TRIM(COALESCE(posicion,''))<>''",
        (ot_id,),
    ).fetchall()
    posiciones = [str(r[0] or "").strip() for r in pos_rows if str(r[0] or "").strip()]
    total = len(posiciones)
    conteo = {
        "ARMADO": 0,
        "SOLDADURA": 0,
        "PINTURA": 0,
        "P/DESPACHO": 0,
        "DESPACHADAS": 0,
    }
    if total == 0:
        return total, conteo

    # Batch: una sola consulta para todos los procesos de la OT.
    all_rows = db.execute(
        "SELECT TRIM(COALESCE(posicion,'')), proceso, estado, re_inspeccion, reproceso, COALESCE(estado_pieza,'')"
        " FROM procesos WHERE ot_id=? ORDER BY id",
        (ot_id,),
    ).fetchall()
    filas_por_pos: dict = {}
    despachadas_pos = set()
    for _pos, _proc, _est, _reinsp, _repro, _estado_pieza in all_rows:
        if _pos:
            filas_por_pos.setdefault(_pos, []).append((_proc, _est, _reinsp, _repro))
            proc_u = str(_proc or "").strip().upper()
            estado_pieza_u = str(_estado_pieza or "").strip().upper()
            if proc_u in ("DESPACHO", "P/DESPACHO") and estado_pieza_u == "DESPACHADO":
                despachadas_pos.add(_pos)

    for pos in posiciones:
        aprobados = set(_aprobados_de_filas(filas_por_pos.get(pos, []), orden_flujo=orden_flujo))
        for proceso in ("ARMADO", "SOLDADURA", "PINTURA"):
            if proceso in aprobados:
                conteo[proceso] += 1
        if "P/DESPACHO" in aprobados or "DESPACHO" in aprobados:
            conteo["P/DESPACHO"] += 1

    conteo["DESPACHADAS"] = len(despachadas_pos)

    return total, conteo


def _avance_y_desglose_ot(db, ot_id):
    """Calcula avance % y desglose de procesos en una sola pasada.

    Usa cache en memoria (_avance_cache) basado en MAX(procesos.id): si los
    registros de procesos no cambiaron desde la última llamada, devuelve el
    resultado cacheado sin tocar Excel ni re-procesar filas.
    """
    # 1. Revisar cache: evita recálculo de requests consecutivos
    try:
        now = time.time()
        max_row = db.execute(
            "SELECT MAX(id) FROM procesos WHERE ot_id=?", (ot_id,)
        ).fetchone()
        max_proc_id = max_row[0] if max_row else None
        cached = _avance_cache.get(ot_id)
        if (
            cached
            and cached[0] == max_proc_id
            and max_proc_id is not None
            and (now - cached[4]) <= _AVANCE_CACHE_TTL_SECONDS
        ):
            total_kg_c = cached[5] if len(cached) > 5 else 0.0
            avance_kg_c = cached[6] if len(cached) > 6 else 0.0
            return cached[1], cached[2], cached[3], total_kg_c, avance_kg_c
    except Exception:
        max_proc_id = None
        now = time.time()

    # 2. Obtener obra
    obra_row = db.execute(
        "SELECT TRIM(COALESCE(obra,'')) FROM ordenes_trabajo WHERE id=?", (ot_id,)
    ).fetchone()
    obra = (obra_row[0] if obra_row else "") or ""
    pesos = _pesos_avance_por_ot(db, ot_id, obra)
    desc_por_pos = _descripciones_por_pos_ot(db, ot_id)
    orden_flujo = ["ARMADO", "SOLDADURA", "DESPACHO"] if _ot_no_requiere_pintura(db, obra=obra, ot_id=ot_id) else list(ORDEN_PROCESOS)

    excel_path = _buscar_excel_armado(obra)

    # 3. Batch: todos los procesos de la OT en una sola query (incluye cantidad/peso para fallback)
    all_rows = db.execute(
        "SELECT TRIM(COALESCE(posicion,'')), proceso, estado, re_inspeccion, reproceso,"
        " COALESCE(eliminado,0), COALESCE(cantidad,0), COALESCE(peso,0), COALESCE(estado_pieza,'')"
        " FROM procesos WHERE ot_id=? ORDER BY id",
        (ot_id,),
    ).fetchall()

    filas_por_pos: dict = {}
    valid_positions: set = set()
    kg_por_pos_meta: dict = {}
    despachadas_pos: set = set()

    for _pos, _proc, _est, _reinsp, _repro, _elim, _cant, _peso, _estado_pieza in all_rows:
        if not _pos:
            continue
        filas_por_pos.setdefault(_pos, []).append((_proc, _est, _reinsp, _repro))
        if not _elim:
            valid_positions.add(_pos)
            kg = _to_float(_cant, 0.0) * _to_float(_peso, 0.0)
            if kg > 0:
                kg_por_pos_meta[_pos] = max(kg_por_pos_meta.get(_pos, 0.0), kg)
            proc_u = str(_proc or "").strip().upper()
            estado_pieza_u = str(_estado_pieza or "").strip().upper()
            if proc_u in ("DESPACHO", "P/DESPACHO") and estado_pieza_u == "DESPACHADO":
                despachadas_pos.add(_pos)

    # 4. Desglose (conteo por proceso y posiciones totales)
    total_piezas = len(valid_positions)
    conteo = {"ARMADO": 0, "SOLDADURA": 0, "PINTURA": 0, "P/DESPACHO": 0, "DESPACHADAS": 0}
    aprobados_por_pos: dict = {}
    for pos in valid_positions:
        # Usar flujo por pieza: INSERTO → solo ARMADO→DESPACHO
        flujo_pos = ["ARMADO", "DESPACHO"] if _es_inserto(desc_por_pos.get(pos, ""), pos=pos) else orden_flujo
        ap = set(_aprobados_de_filas(filas_por_pos.get(pos, []), orden_flujo=flujo_pos))
        aprobados_por_pos[pos] = ap
        for proc in ("ARMADO", "SOLDADURA", "PINTURA"):
            if proc in ap:
                conteo[proc] += 1
        if "P/DESPACHO" in ap or "DESPACHO" in ap:
            conteo["P/DESPACHO"] += 1
    conteo["DESPACHADAS"] = len(despachadas_pos)

    # 5. Avance — path 1: Excel con columna TOTAL exacta
    total_excel, procesado_excel, tiene_total_excel = _avance_desde_excel_armado(excel_path)
    if total_excel > 0 and tiene_total_excel:
        pct = max(0, min(100, round((procesado_excel / total_excel) * 100)))
        _avance_cache[ot_id] = (max_proc_id, pct, total_piezas, conteo, now, total_excel, procesado_excel)
        return pct, total_piezas, conteo, total_excel, procesado_excel

    # 6. Avance — path 2: Excel sin TOTAL + estados de BD (todos pre-cargados)
    if excel_path:
        df = _get_df_cached(excel_path)
        if df is not None:
            col_pos = find_col(df, "POS")
            col_cant = find_col(df, "CANT")
            col_peso = find_col(df, "PESO")
            if col_pos and col_cant and col_peso:
                ratio_por_base: dict = {}
                for pos_real, filas in filas_por_pos.items():
                    base = _pos_base(pos_real)
                    ap = aprobados_por_pos.get(pos_real) or set(_aprobados_de_filas(filas, orden_flujo=orden_flujo))
                    pesos_pos = _pesos_avance_por_pieza(desc_por_pos.get(pos_real, ""), pesos, pos=pos_real)
                    ratio = _avance_ratio_desde_aprobados(ap, pesos_pos)
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
                    total_kg += cant * peso
                    ratio = ratio_por_base.get(base, 0.0)
                    procesado_kg += peso * ratio

                if total_kg > 0:
                    pct = max(0, min(100, round((procesado_kg / total_kg) * 100)))
                    _avance_cache[ot_id] = (max_proc_id, pct, total_piezas, conteo, now, total_kg, procesado_kg)
                    return pct, total_piezas, conteo, total_kg, procesado_kg

    # 7. Avance — path 3: fallback con metadatos de cantidad/peso de procesos
    if not kg_por_pos_meta:
        # Sin datos de peso: calcula avance ponderado por conteo de piezas (igual peso para cada una)
        if not valid_positions:
            _avance_cache[ot_id] = (max_proc_id, 0, total_piezas, conteo, now, 0.0, 0.0)
            return 0, total_piezas, conteo, 0.0, 0.0
        avance_sum = 0.0
        for pos in valid_positions:
            ap = aprobados_por_pos.get(pos, set())
            if not ap:
                # Fallback: calcular aprobados para esta posición con flujo específico
                flujo_pos = ["ARMADO", "DESPACHO"] if _es_inserto(desc_por_pos.get(pos, ""), pos=pos) else orden_flujo
                permitir_saltos = flujo_pos == ["ARMADO", "DESPACHO"]
                filas_pos = filas_por_pos.get(pos, [])
                ap = set(_aprobados_de_filas(filas_pos, orden_flujo=flujo_pos, permitir_saltos=permitir_saltos))
            pesos_pos = _pesos_avance_por_pieza(desc_por_pos.get(pos, ""), pesos, pos=pos)
            avance_sum += _avance_ratio_desde_aprobados(ap, pesos_pos)
        pct = max(0, min(100, round((avance_sum / len(valid_positions)) * 100)))
        _avance_cache[ot_id] = (max_proc_id, pct, total_piezas, conteo, now, 0.0, 0.0)
        return pct, total_piezas, conteo, 0.0, 0.0

    total_kg = 0.0
    avance_kg = 0.0
    for posicion, kg_pieza in kg_por_pos_meta.items():
        kg_pieza = _to_float(kg_pieza, 0.0)
        if kg_pieza <= 0:
            continue
        total_kg += kg_pieza
        
        # Usar aprobados_por_pos cacheado o calcular con flujo específico por pieza
        ap = aprobados_por_pos.get(posicion)
        if ap is None:
            flujo_pos = ["ARMADO", "DESPACHO"] if _es_inserto(desc_por_pos.get(posicion, ""), pos=posicion) else orden_flujo
            permitir_saltos = flujo_pos == ["ARMADO", "DESPACHO"]
            filas_pos = filas_por_pos.get(posicion, [])
            ap = set(_aprobados_de_filas(filas_pos, orden_flujo=flujo_pos, permitir_saltos=permitir_saltos))
        
        pesos_pos = _pesos_avance_por_pieza(desc_por_pos.get(posicion, ""), pesos, pos=posicion)
        av = 0.0
        if "ARMADO" in ap:
            av += pesos_pos["ARMADO"]
        if "SOLDADURA" in ap:
            av += pesos_pos["SOLDADURA"]
        if "PINTURA" in ap:
            av += pesos_pos["PINTURA"]
        if "P/DESPACHO" in ap or "DESPACHO" in ap:
            av += pesos_pos["DESPACHO"]
        avance_kg += kg_pieza * (av / 100.0)

    if total_kg <= 0:
        _avance_cache[ot_id] = (max_proc_id, 0, total_piezas, conteo, now, 0.0, 0.0)
        return 0, total_piezas, conteo, 0.0, 0.0

    pct = max(0, min(100, round((avance_kg / total_kg) * 100)))
    _avance_cache[ot_id] = (max_proc_id, pct, total_piezas, conteo, now, total_kg, avance_kg)
    return pct, total_piezas, conteo, total_kg, avance_kg


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
        progreso, total_piezas, conteo_procesos, ot_total_kg, ot_avance_kg = _avance_y_desglose_ot(db, ot_id)
        if _persistir_avance_ot(db, ot_id, progreso, estado_avance_actual):
            hubo_cambios = True
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
            "total_kg": float(ot_total_kg or 0.0),
            "avance_kg": float(ot_avance_kg or 0.0),
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

    avance_visible_pct = 0
    avance_visible_kg_total = sum(max(0.0, float(f["total_kg"] or 0.0)) for f in filas_filtradas)
    if avance_visible_kg_total > 0:
        avance_visible_kg = sum(
            max(0.0, float(f["avance_kg"] or 0.0))
            for f in filas_filtradas
        )
        avance_visible_pct = round((avance_visible_kg / avance_visible_kg_total) * 100)
    elif filas_filtradas:
        avance_visible_pct = round(sum(float(f["progreso"] or 0) for f in filas_filtradas) / len(filas_filtradas))
    avance_visible_pct = max(0, min(100, avance_visible_pct))

    resumen_obra = {}
    for fila in filas_filtradas:
        obra_key = fila["obra"] or "Sin obra"
        if obra_key not in resumen_obra:
            resumen_obra[obra_key] = {
                "ots": 0,
                "total_kg": 0.0,
                "avance_kg": 0.0,
            }
        resumen_obra[obra_key]["ots"] += 1
        resumen_obra[obra_key]["total_kg"] += fila["total_kg"]
        resumen_obra[obra_key]["avance_kg"] += fila["avance_kg"]

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
            <div class="kpi"><div class="t">Avance OT visible</div><div class="v">{avance_visible_pct}%</div><div style="font-size:12px;color:#7c2d12;">Ponderado por KG</div></div>
        </div>
    </div>
    """

    if resumen_obra:
        html += '<div class="panel"><h3 style="margin:0 0 10px 0;color:#9a3412;">Desglose por obra</h3><div class="obra-grid">'
        for obra_key in sorted(resumen_obra.keys()):
            data = resumen_obra[obra_key]
            avance_obra = round(data['avance_kg'] / data['total_kg'] * 100) if data['total_kg'] > 0 else 0
            avance_obra = max(0, min(100, avance_obra))
            html += f"""
            <div class="obra-card">
                <div class="nombre">{obra_key}</div>
                <div class="avance">{avance_obra}%</div>
                <div style="font-size:12px;color:#7c2d12;">Avance ponderado por KG ({data['ots']} OT)</div>
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
                <th style="text-align:center;">Pzas</th>
                <th style="text-align:center;">Armado</th>
                <th style="text-align:center;">Soldadura</th>
                <th style="text-align:center;">Pintura</th>
                <th style="text-align:center;">P/Desp</th>
                <th style="text-align:center;">Despachadas</th>
                <th>Fecha Entrega</th>
                <th>Estado</th>
            </tr>
        """

        for fila in filas_filtradas:
            ot_id = fila["ot_id"]
            progreso = fila["progreso"]
            total_piezas = fila["total_piezas"]
            conteo = fila["conteo"]
            def _chip_proceso(count, total):
                if total == 0:
                    return '<span class="chip">-</span>'
                color = '#dcfce7' if count == total else ('#fff7ed' if count > 0 else '#f1f5f9')
                border = '#86efac' if count == total else ('#fdba74' if count > 0 else '#cbd5e1')
                text_color = '#166534' if count == total else ('#9a3412' if count > 0 else '#64748b')
                return f'<span class="chip" style="background:{color};border-color:{border};color:{text_color};">{count}/{total}</span>'

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
                <td style="text-align:center;"><span class="chip">{total_piezas}</span></td>
                <td style="text-align:center;">{_chip_proceso(conteo['ARMADO'], total_piezas)}</td>
                <td style="text-align:center;">{_chip_proceso(conteo['SOLDADURA'], total_piezas)}</td>
                <td style="text-align:center;">{_chip_proceso(conteo['PINTURA'], total_piezas)}</td>
                <td style="text-align:center;">{_chip_proceso(conteo['P/DESPACHO'], total_piezas)}</td>
                <td style="text-align:center;">{_chip_proceso(conteo['DESPACHADAS'], total_piezas)}</td>
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
