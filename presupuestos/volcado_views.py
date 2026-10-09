"""Volcado del previsto de un presupuesto adjudicado a las OT (campos base de
previsto de `economico_presupuesto`) e historial de volcados.

Vista previa sin escribir nada -> confirmación -> escritura + registro en
`volcados_previsto[_lineas]`. Todo el cálculo sale de `volcado_previsto.py`.
"""
import hashlib
import html as html_lib
import json
from decimal import Decimal

from flask import request, jsonify, session

from . import presupuestos_bp
from .routes import _db, _calcular_resultado_tarea
from .models import (
    obtener_presupuesto,
    listar_tareas,
    listar_reparto_tareas,
    listar_volcados,
    obtener_ultimo_volcado,
    aplicar_volcado_previsto,
)
from .reparto_views import _etiqueta_ot
from .views import _ESTILO_BASE
from .volcado_previsto import (
    CAMPOS_ECONOMICOS,
    armar_tarea_para_volcado,
    calcular_volcado_previsto,
    detectar_ediciones_manuales,
    verificar_cuadre_con_presupuesto,
)

ETIQUETA_CAMPO = {
    "mat_previsto": "Materiales",
    "pintura_previsto": "Pintura",
    "fletes_previsto": "Fletes",
    "subcontratos_previsto": "Subcontratos",
    "mo_previsto": "Mano de obra",
    "consumibles_previsto": "Consumibles",
    "ingenieria_previsto": "Ingeniería",
    "gastos_gen_previsto": "Gastos generales",
    "impuestos_previsto": "Impuestos",
    "beneficio_previsto": "Beneficio",
}
_CERO = Decimal("0.00")


def _dec(valor):
    return Decimal(str(valor if valor is not None else 0))


def _etiquetas_ot(db, ot_ids):
    """{ot_id: etiqueta} para las OT que existan."""
    etiquetas = {}
    for ot_id in ot_ids:
        fila = db.execute(
            "SELECT id, TRIM(COALESCE(obra, '')), TRIM(COALESCE(titulo, '')), fecha_cierre FROM ordenes_trabajo WHERE id = ?",
            (ot_id,),
        ).fetchone()
        if fila:
            etiquetas[ot_id] = _etiqueta_ot(fila[0], fila[1], fila[2], bool(fila[3]))
    return etiquetas


def _previsto_actual(db, ot_ids):
    """{ot_id: {campo: Decimal}} con lo que hoy tiene el módulo económico (0 si la OT no tiene fila)."""
    columnas = ", ".join(CAMPOS_ECONOMICOS)
    actual = {}
    for ot_id in ot_ids:
        fila = db.execute(f"SELECT {columnas} FROM economico_presupuesto WHERE ot_id = ?", (ot_id,)).fetchone()
        actual[ot_id] = {campo: _dec(fila[i] if fila else 0) for i, campo in enumerate(CAMPOS_ECONOMICOS)}
    return actual


def _construir_volcado(db, presupuesto_id):
    """Calcula todo lo que muestra la vista previa y lo que escribiría el volcado.
    Devuelve (publico, interno); lanza ValueError con un mensaje claro si no se puede."""
    from economico_routes import _ensure_schema  # asegura economico_presupuesto con todas sus columnas
    _ensure_schema(db)

    tareas_db = listar_tareas(db, presupuesto_id)
    reparto = listar_reparto_tareas(db, [t["id"] for t in tareas_db])

    entradas = []
    precio_venta_fab = precio_venta_mon = Decimal(0)
    for tarea in tareas_db:
        resultado = _calcular_resultado_tarea(db, tarea["id"])
        precio_venta_fab += _dec(resultado["fabricacion"]["cascada"]["precio_venta"])
        precio_venta_mon += _dec(resultado["montaje"]["cascada"]["precio_venta"])
        if resultado["fabricacion"]["resumen"]["costo_directo"] > 0:
            entradas.append(armar_tarea_para_volcado(resultado, tarea["id"], tarea["nombre"], reparto.get(tarea["id"], [])))

    volcado = calcular_volcado_previsto(entradas)  # ValueError si algún reparto no suma 100
    cuadre = verificar_cuadre_con_presupuesto(volcado, precio_venta_fab, len(entradas))
    por_ot = volcado["por_ot"]
    ot_ids = list(por_ot)

    ultimo = obtener_ultimo_volcado(db, presupuesto_id)
    ultimo_lineas = ultimo["lineas"] if ultimo else {}
    ot_ids_viejas = [o for o in ultimo_lineas if o not in por_ot]

    etiquetas = _etiquetas_ot(db, ot_ids + ot_ids_viejas)
    faltantes = [o for o in ot_ids if o not in etiquetas]
    if faltantes:
        raise ValueError(f"La OT {faltantes[0]} ya no existe. Corregí el reparto de la tarea.")

    actual = _previsto_actual(db, ot_ids)
    ediciones = detectar_ediciones_manuales(actual, ultimo_lineas, ot_ids)

    bloqueo = ""
    if not ot_ids:
        bloqueo = "Ninguna tarea tiene OT asignada: no hay nada para volcar."
    elif not cuadre["cuadra"]:
        bloqueo = "El control de cuadre no cierra: el volcado está bloqueado."

    ots_vista = []
    for ot_id in ot_ids:
        filas = []
        for campo in CAMPOS_ECONOMICOS:
            valor_actual, valor_nuevo = actual[ot_id][campo], por_ot[ot_id][campo]
            if valor_actual == 0 and valor_nuevo == 0:
                continue
            filas.append({
                "rubro": ETIQUETA_CAMPO[campo],
                "actual": float(valor_actual),
                "nuevo": float(valor_nuevo),
                "cambia": abs(valor_nuevo - valor_actual) > Decimal("0.005"),
            })
        ots_vista.append({
            "ot_id": ot_id,
            "etiqueta": etiquetas[ot_id],
            "filas": filas,
            "total_actual": float(sum(actual[ot_id].values(), _CERO)),
            "total_nuevo": float(sum(por_ot[ot_id].values(), _CERO)),
        })

    # La huella liga la confirmación a lo que el usuario vio en la vista previa.
    huella = hashlib.sha256(json.dumps({
        "nuevo": {str(o): {c: str(v) for c, v in por_ot[o].items()} for o in ot_ids},
        "actual": {str(o): {c: str(v) for c, v in actual[o].items()} for o in ot_ids},
    }, sort_keys=True).encode("utf-8")).hexdigest()

    publico = {
        "ots": ots_vista,
        "primer_volcado": ultimo is None,
        "ediciones_manuales": [
            {
                "ot_id": ot_id,
                "etiqueta": etiquetas[ot_id],
                "diferencias": [
                    {"rubro": ETIQUETA_CAMPO[d["campo"]], "actual": float(d["actual"]), "ultimo_volcado": float(d["ultimo_volcado"])}
                    for d in difs
                ],
            }
            for ot_id, difs in ediciones.items()
        ],
        "ots_sin_tareas_ahora": [{"ot_id": o, "etiqueta": etiquetas.get(o, f"OT {o}")} for o in ot_ids_viejas],
        "sin_ot": [{"nombre": f["nombre"], "total": float(f["total"])} for f in volcado["sin_ot"]],
        "cuadre": {k: (float(v) if isinstance(v, Decimal) else v) for k, v in cuadre.items()},
        "montaje_total": float(precio_venta_mon),
        "bloqueo": bloqueo,
        "puede_confirmar": not bloqueo,
        "huella": huella,
    }
    interno = {"por_ot": por_ot, "ediciones": ediciones, "etiquetas": etiquetas}
    return publico, interno


def _validar_adjudicado(db, presupuesto_id):
    """(presupuesto, None) o (None, respuesta_json_de_error)."""
    presupuesto = obtener_presupuesto(db, presupuesto_id)
    if not presupuesto:
        return None, (jsonify({"error": "Presupuesto no encontrado"}), 404)
    if presupuesto.get("estado") != "adjudicado":
        return None, (jsonify({"error": "El volcado solo está disponible para presupuestos adjudicados."}), 400)
    return presupuesto, None


@presupuestos_bp.route("/<int:presupuesto_id>/reparto/volcado/preview", methods=["GET"])
def vista_volcado_preview(presupuesto_id):
    db = _db()
    _, error = _validar_adjudicado(db, presupuesto_id)
    if error:
        return error
    try:
        publico, _ = _construir_volcado(db, presupuesto_id)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    return jsonify(publico)


@presupuestos_bp.route("/<int:presupuesto_id>/reparto/volcado", methods=["POST"])
def vista_volcado_confirmar(presupuesto_id):
    db = _db()
    _, error = _validar_adjudicado(db, presupuesto_id)
    if error:
        return error
    cuerpo = request.get_json(silent=True) or {}

    try:
        publico, interno = _construir_volcado(db, presupuesto_id)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    if not publico["puede_confirmar"]:
        return jsonify({"error": publico["bloqueo"]}), 400
    if cuerpo.get("huella") != publico["huella"]:
        return jsonify({"error": "Los datos cambiaron desde la vista previa. Volvé a abrirla para revisar."}), 409

    nota = str(cuerpo.get("nota") or "").strip()
    if interno["ediciones"]:
        pisadas = ", ".join(str(o) for o in interno["ediciones"])
        nota = (nota + " " if nota else "") + f"[Se pisaron valores editados a mano en las OT: {pisadas}]"
    usuario = session.get("nombre") or session.get("username") or str(session.get("user_id") or "")

    try:
        volcado_id = aplicar_volcado_previsto(db, presupuesto_id, usuario, nota or None, interno["por_ot"])
    except Exception as e:
        return jsonify({"error": f"No se pudo volcar: {e}"}), 500
    return jsonify({"ok": True, "volcado_id": volcado_id, "mensaje": f"Volcado #{volcado_id} registrado en {len(interno['por_ot'])} OT."})


# ─────────────────────────────────────────────────────────────────
# Historial
# ─────────────────────────────────────────────────────────────────

def _money(valor):
    return f"$ {float(valor or 0):,.2f}"


@presupuestos_bp.route("/<int:presupuesto_id>/reparto/historial", methods=["GET"])
def vista_historial_volcados(presupuesto_id):
    db = _db()
    presupuesto = obtener_presupuesto(db, presupuesto_id)
    if not presupuesto:
        return "<h3>❌ Presupuesto no encontrado</h3>", 404

    volcados = listar_volcados(db, presupuesto_id)
    etiquetas = _etiquetas_ot(db, {o for v in volcados for o in v["lineas"]})

    bloques = []
    for v in volcados:
        campos = [c for c in CAMPOS_ECONOMICOS if any(abs(l.get(c, 0)) > 0 for l in v["lineas"].values())]
        cabecera = "".join(f"<th style='text-align:right;'>{html_lib.escape(ETIQUETA_CAMPO[c])}</th>" for c in campos)
        filas = ""
        total_volcado = 0.0
        for ot_id, lineas in v["lineas"].items():
            total_ot = sum(lineas.values())
            total_volcado += total_ot
            celdas = "".join(f"<td style='text-align:right;'>{_money(lineas.get(c, 0))}</td>" for c in campos)
            filas += (
                f"<tr><td>{html_lib.escape(etiquetas.get(ot_id, f'OT {ot_id}'))}</td>{celdas}"
                f"<td style='text-align:right;'><b>{_money(total_ot)}</b></td></tr>"
            )
        nota = f"<div class='muted'>Nota: {html_lib.escape(v['nota'])}</div>" if v["nota"] else ""
        bloques.append(f"""
        <div class="card">
            <div style="display:flex;justify-content:space-between;gap:8px;flex-wrap:wrap;">
                <div><b>Volcado #{v['id']}</b> · {html_lib.escape(str(v['fecha'] or ''))} · {html_lib.escape(str(v['usuario'] or '-'))}</div>
                <div><b>{_money(total_volcado)}</b> en {len(v['lineas'])} OT</div>
            </div>
            {nota}
            <div style="overflow-x:auto;margin-top:8px;">
                <table><tr><th>OT</th>{cabecera}<th style='text-align:right;'>Total</th></tr>{filas}</table>
            </div>
        </div>""")

    cuerpo = "".join(bloques) or "<div class='card sin-datos'>Todavía no se hizo ningún volcado para este presupuesto.</div>"
    return f"""
    <html><head><meta name="viewport" content="width=device-width, initial-scale=1">
    <style>{_ESTILO_BASE}</style></head>
    <body><div class="wrap">
        <div class="top-bar">
            <h2>📜 Historial de volcados — Presupuesto #{presupuesto_id}</h2>
            <a href="/modulo/presupuestos/{presupuesto_id}/reparto" class="btn btn-secondary">⬅️ Volver al mapeo</a>
        </div>
        <div class="muted" style="margin-bottom:10px;">Cada volcado conserva lo que escribió en cada OT. El más nuevo está arriba.</div>
        {cuerpo}
    </div></body></html>
    """
