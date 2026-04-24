"""
proceso_utils.py
Lógica de estados de piezas, trazabilidad y validación de procesos de producción.
"""
import re
from db_utils import get_db

# Orden oficial de procesos de fabricación
ORDEN_PROCESOS = ["ARMADO", "SOLDADURA", "PINTURA", "DESPACHO"]


def _normalizar_etiqueta(value):
    return str(value or "").strip().upper()


def _ot_no_requiere_pintura(db, obra=None, ot_id=None):
    """Retorna True si la OT/obra está configurada como sin pintura."""
    row = None
    if ot_id is not None:
        row = db.execute(
            """
            SELECT COALESCE(esquema_pintura, '')
            FROM ordenes_trabajo
            WHERE id = ?
            LIMIT 1
            """,
            (ot_id,),
        ).fetchone()
    elif obra:
        row = db.execute(
            """
            SELECT COALESCE(esquema_pintura, '')
            FROM ordenes_trabajo
            WHERE TRIM(COALESCE(obra, '')) = TRIM(?)
              AND fecha_cierre IS NULL
              AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
            ORDER BY id DESC
            LIMIT 1
            """,
            (obra,),
        ).fetchone()

    esquema = _normalizar_etiqueta(row[0] if row else "")
    if not esquema:
        return False
    marcas_no_pintura = {
        "N/A",
        "NA",
        "NO APLICA",
        "NO APLICA",
        "SIN PINTURA",
        "NO REQUIERE PINTURA",
    }
    return esquema in marcas_no_pintura


def _extraer_ciclos_reinspeccion(reinspeccion_txt):
    ciclos = []
    if not reinspeccion_txt:
        return ciclos

    patron = re.compile(
        r"^(?:Ciclo:\s*(\d+)\s*\|\s*)?(?:Proceso:\s*([^|]+)\|\s*)?Fecha:\s*([^|]+)\|\s*(?:Operador|Operario):\s*([^|]+)\|\s*Estado:\s*([^|]+)(?:\|.*)?$",
        re.IGNORECASE,
    )

    lineas = [ln.strip() for ln in str(reinspeccion_txt).split("\n") if ln.strip()]
    for ln in lineas:
        m = patron.match(ln)
        if not m:
            continue
        motivo_m = re.search(r"\|\s*Motivo:\s*([^|]+)", ln, re.IGNORECASE)
        firma_m = re.search(r"\|\s*Firma:\s*([^|]+)", ln, re.IGNORECASE)
        responsable_m = re.search(r"\|\s*(?:Responsable|Inspector):\s*([^|]+)", ln, re.IGNORECASE)
        responsable_txt = (responsable_m.group(1).strip() if responsable_m else "")
        ciclos.append({
            "ciclo": int(m.group(1)) if m.group(1) else None,
            "proceso": (m.group(2) or "").strip().upper(),
            "fecha": (m.group(3) or "").strip(),
            "operario": (m.group(4) or "").strip(),
            "estado": (m.group(5) or "").strip().upper(),
            "motivo": (motivo_m.group(1).strip() if motivo_m else ""),
            "firma": (firma_m.group(1).strip() if firma_m else ""),
            "responsable": responsable_txt,
            "inspector": responsable_txt,
        })
    return ciclos


def _estado_control_aprueba(estado):
    estado_base = (estado or "").strip().upper()
    return estado_base in (
        "OK",
        "APROBADO",
        "OBS",
        "OBSERVACION",
        "OBSERVACIÓN",
        "OM",
        "OP MEJORA",
        "OPORTUNIDAD DE MEJORA",
    )


def _proceso_aprobado(estado, reinspeccion_txt):
    if _estado_control_aprueba(estado):
        return True
    ciclos = _extraer_ciclos_reinspeccion(reinspeccion_txt)
    if not ciclos:
        return False
    return _estado_control_aprueba(ciclos[-1].get("estado"))


def _estado_pieza_persistente(estado, reinspeccion_txt):
    estado_base = (estado or "").strip().upper()
    if _estado_control_aprueba(estado_base):
        return "APROBADA"
    if estado_base in ("NC", "NO CONFORME", "NO CONFORMIDAD"):
        ciclos = _extraer_ciclos_reinspeccion(reinspeccion_txt)
        if ciclos and _estado_control_aprueba(ciclos[-1].get("estado")):
            return "APROBADA"
        return "NO_APROBADA"
    return "PENDIENTE"


def _registrar_trazabilidad(db, proceso_id, posicion, obra, proceso, estado_control, estado_pieza, firma_digital, accion, re_inspeccion, tipo_evento):
    db.execute(
        """
        INSERT INTO trazabilidad_estados (
            proceso_id, posicion, obra, proceso, estado_control, estado_pieza, firma_digital, accion, re_inspeccion, tipo_evento
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            proceso_id,
            (posicion or "").strip(),
            (obra or "").strip() or None,
            (proceso or "").strip().upper(),
            (estado_control or "").strip().upper(),
            (estado_pieza or "").strip().upper(),
            (firma_digital or "").strip(),
            (accion or "").strip(),
            (re_inspeccion or "").strip(),
            (tipo_evento or "").strip().upper(),
        ),
    )


def _agregar_ciclo_reinspeccion(actual, proceso, fecha, operario, estado, motivo="", firma="", responsable=""):
    existentes = _extraer_ciclos_reinspeccion(actual)
    numero = len(existentes) + 1
    linea = (
        f"Ciclo: {numero} | Proceso: {(proceso or '').strip().upper()} | Fecha: {fecha} | "
        f"Operario: {operario} | Estado: {estado}"
    )
    if responsable:
        linea += f" | Responsable: {responsable}"
    if firma:
        linea += f" | Firma: {firma}"
    if motivo:
        linea += f" | Motivo: {motivo}"
    previo = (actual or "").strip()
    return f"{previo}\n{linea}" if previo else linea


def _obtener_timeline_pieza(db, pos, obra=None):
    if obra:
        rows = db.execute(
            """
            SELECT fecha_evento, proceso, estado_control, estado_pieza, firma_digital, accion, re_inspeccion, tipo_evento
            FROM trazabilidad_estados
            WHERE posicion=? AND COALESCE(obra, '') = COALESCE(?, '')
            ORDER BY datetime(fecha_evento) DESC, id DESC
            """,
            (pos, obra),
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT fecha_evento, proceso, estado_control, estado_pieza, firma_digital, accion, re_inspeccion, tipo_evento
            FROM trazabilidad_estados
            WHERE posicion=?
            ORDER BY datetime(fecha_evento) DESC, id DESC
            """,
            (pos,),
        ).fetchall()
    return rows


def obtener_procesos_completados(pos, obra=None, ot_id=None):
    """Retorna lista de procesos aprobados (OK efectivo) en orden, sin saltos."""
    db = get_db()
    if ot_id is not None:
        rows = db.execute(
            "SELECT proceso, estado, re_inspeccion, reproceso FROM procesos WHERE posicion=? AND ot_id=? ORDER BY id",
            (pos, ot_id)
        ).fetchall()
    elif obra:
        rows = db.execute(
            "SELECT proceso, estado, re_inspeccion, reproceso FROM procesos WHERE posicion=? AND obra=? ORDER BY id",
            (pos, obra)
        ).fetchall()
    else:
        rows = db.execute("SELECT proceso, estado, re_inspeccion, reproceso FROM procesos WHERE posicion=? ORDER BY id", (pos,)).fetchall()

    aprobados = set()
    for proceso, estado, reinspeccion, reproceso in rows:
        proc = (proceso or "").strip().upper()
        if proc not in ORDEN_PROCESOS:
            continue

        # En control de pintura por etapas, solo TERMINACION puede cerrar PINTURA.
        if proc == "PINTURA":
            repro_u = str(reproceso or "").strip().upper()
            if "ETAPA:SUPERFICIE" in repro_u or "ETAPA:FONDO" in repro_u:
                continue

        if _proceso_aprobado(estado, reinspeccion):
            aprobados.add(proc)

    completados = []
    for proc in ORDEN_PROCESOS:
        if proc in aprobados:
            completados.append(proc)
        else:
            break
    return completados


def pieza_completada(pos, obra=None, ot_id=None):
    """Retorna True si DESPACHO está aprobado (OK efectivo)."""
    return "DESPACHO" in obtener_procesos_completados(pos, obra, ot_id)


def validar_siguiente_proceso(pos, nuevo_proceso, obra=None, ot_id=None):
    """Valida que el proceso siga el orden correcto."""
    procesos_hechos = obtener_procesos_completados(pos, obra, ot_id)
    db = get_db()

    orden_flujo = list(ORDEN_PROCESOS)
    if _ot_no_requiere_pintura(db, obra=obra, ot_id=ot_id):
        orden_flujo = ["ARMADO", "SOLDADURA", "DESPACHO"]

    # Si el proceso ya existe, es una edición
    if nuevo_proceso in procesos_hechos:
        return True, "OK"

    # Obtener índice del nuevo proceso
    try:
        idx_nuevo = orden_flujo.index(nuevo_proceso)
    except ValueError:
        if nuevo_proceso == "PINTURA" and "PINTURA" not in orden_flujo:
            return False, "❌ Esta OT no requiere pintura; podés continuar con despacho"
        return False, "Proceso inválido"

    # El primer proceso debe ser ARMADO
    if len(procesos_hechos) == 0:
        if nuevo_proceso != "ARMADO":
            return False, "❌ El primer proceso debe ser ARMADO"
        return True, "OK"

    # Validar que siga el orden
    ultimo_proceso = procesos_hechos[-1]
    if ultimo_proceso not in orden_flujo:
        return False, "❌ Inconsistencia de flujo para esta OT"
    idx_ultimo = orden_flujo.index(ultimo_proceso)

    if idx_nuevo == idx_ultimo:
        return False, "❌ Este proceso ya fue completado, no se puede repetir"
    elif idx_nuevo != idx_ultimo + 1:
        return False, f"❌ El siguiente proceso debe ser {orden_flujo[idx_ultimo + 1]}"

    # Bloqueo adicional: no permitir avanzar si alguna etapa previa tiene NC abierta.
    if ot_id is not None:
        rows_prev = db.execute(
            """
            SELECT UPPER(TRIM(COALESCE(proceso, ''))) AS proceso,
                   UPPER(TRIM(COALESCE(estado, ''))) AS estado,
                   COALESCE(re_inspeccion, ''),
                   id
            FROM procesos
            WHERE posicion=? AND ot_id=?
            ORDER BY id DESC
            """,
            (pos, ot_id),
        ).fetchall()
    elif obra:
        rows_prev = db.execute(
            """
            SELECT UPPER(TRIM(COALESCE(proceso, ''))) AS proceso,
                   UPPER(TRIM(COALESCE(estado, ''))) AS estado,
                   COALESCE(re_inspeccion, ''),
                   id
            FROM procesos
            WHERE posicion=? AND COALESCE(obra, '')=COALESCE(?, '')
            ORDER BY id DESC
            """,
            (pos, obra),
        ).fetchall()
    else:
        rows_prev = db.execute(
            """
            SELECT UPPER(TRIM(COALESCE(proceso, ''))) AS proceso,
                   UPPER(TRIM(COALESCE(estado, ''))) AS estado,
                   COALESCE(re_inspeccion, ''),
                   id
            FROM procesos
            WHERE posicion=?
            ORDER BY id DESC
            """,
            (pos,),
        ).fetchall()

    latest_prev = {}
    for proc, estado, reinsp, row_id in rows_prev:
        if proc in ORDEN_PROCESOS and proc not in latest_prev:
            latest_prev[proc] = (estado, reinsp)

    estados_nc = {"NC", "NO CONFORME", "NO CONFORMIDAD"}
    for proc_prev in orden_flujo[:idx_nuevo]:
        dato = latest_prev.get(proc_prev)
        if not dato:
            continue
        estado_prev, reinsp_prev = dato
        if estado_prev in estados_nc and not _proceso_aprobado(estado_prev, reinsp_prev):
            return False, f"❌ No podés avanzar a {nuevo_proceso}: {proc_prev} tiene NC abierta sin cierre de re-inspección"

    return True, "OK"
