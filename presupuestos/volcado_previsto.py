"""Cálculo puro (sin Flask, sin DB) del previsto por OT a partir de los
resultados de `calculo_presupuesto.py`. No recalcula costos: solo mapea,
reparte y redondea lo que ya se calculó.

Solo se vuelca la sección FABRICACION; Montaje no entra nunca.
"""
from decimal import Decimal, ROUND_HALF_UP

from .calculo_presupuesto import RUBROS_FABRICACION

# Rubro de Presupuestos -> columna de `economico_presupuesto` (economico_routes.py).
CAMPO_ECONOMICO_POR_RUBRO = {
    "materiales": "mat_previsto",
    "bulones": "mat_previsto",
    "pintura": "pintura_previsto",
    "fletes": "fletes_previsto",
    "subcontratos": "subcontratos_previsto",
    "mano_obra": "mo_previsto",
    "consumibles": "consumibles_previsto",
    "ingenieria": "ingenieria_previsto",
}
CAMPO_GG = "gastos_gen_previsto"
CAMPO_BENEFICIO = "beneficio_previsto"
CAMPO_IMPUESTOS = "impuestos_previsto"

CAMPOS_ECONOMICOS = (
    "mat_previsto",
    "pintura_previsto",
    "fletes_previsto",
    "subcontratos_previsto",
    "mo_previsto",
    "consumibles_previsto",
    "ingenieria_previsto",
    CAMPO_GG,
    CAMPO_IMPUESTOS,
    CAMPO_BENEFICIO,
)

_CENTAVO = Decimal("0.01")
_CERO = Decimal("0.00")
_TOLERANCIA_PORCENTAJE = Decimal("0.000001")  # absorbe el ruido de float de 100/3 + 100/3 + 100/3
# Los componentes de cada tarea se redondean por separado; contra el precio de venta sin
# redondear pueden diferir unos centavos por tarea (nunca pesos).
TOLERANCIA_REDONDEO_POR_TAREA = Decimal("0.05")
TOLERANCIA_EDICION_MANUAL = Decimal("0.005")


def _dec(valor):
    return Decimal(str(valor if valor is not None else 0))


def _redondear(valor):
    return valor.quantize(_CENTAVO, rounding=ROUND_HALF_UP)


def _etiqueta(tarea):
    nombre = tarea.get("nombre")
    return f"{nombre!r}" if nombre else f"id {tarea.get('tarea_id')}"


def _montos_economicos(tarea):
    """{campo_economico: monto redondeado a 2 decimales} de la sección Fabricación."""
    acumulado = {campo: Decimal(0) for campo in CAMPOS_ECONOMICOS}
    for rubro, monto in (tarea.get("montos") or {}).items():
        campo = CAMPO_ECONOMICO_POR_RUBRO.get(rubro)
        if campo is None:
            raise ValueError(
                f"Tarea {_etiqueta(tarea)}: el rubro {rubro!r} no se vuelca. "
                f"Solo Fabricación ({', '.join(RUBROS_FABRICACION)})."
            )
        acumulado[campo] += _dec(monto)
    acumulado[CAMPO_GG] += _dec(tarea.get("gg"))
    acumulado[CAMPO_BENEFICIO] += _dec(tarea.get("beneficio"))
    acumulado[CAMPO_IMPUESTOS] += _dec(tarea.get("impuestos"))
    return {campo: _redondear(valor) for campo, valor in acumulado.items()}


def _reparto_normalizado(tarea):
    """[(ot_id, Decimal porcentaje)]; valida 0..100 y suma 100. [] = sin OT."""
    filas = []
    for fila in tarea.get("reparto") or []:
        ot_id, porcentaje = (fila["ot_id"], fila["porcentaje"]) if isinstance(fila, dict) else fila
        porcentaje = _dec(porcentaje)
        if porcentaje < 0 or porcentaje > 100:
            raise ValueError(
                f"Tarea {_etiqueta(tarea)}: el porcentaje {porcentaje} de la OT {ot_id} "
                "debe estar entre 0 y 100."
            )
        filas.append((ot_id, porcentaje))

    if filas:
        suma = sum((porcentaje for _, porcentaje in filas), Decimal(0))
        if abs(suma - 100) > _TOLERANCIA_PORCENTAJE:
            raise ValueError(
                f"Tarea {_etiqueta(tarea)}: los porcentajes del reparto suman {suma} y deben sumar 100."
            )
    return filas


def _repartir(monto, reparto):
    """Parte `monto` según los porcentajes; la diferencia de redondeo va a la OT
    de mayor porcentaje (la primera si hay empate), así la suma da `monto` exacto."""
    partes = [_redondear(monto * porcentaje / 100) for _, porcentaje in reparto]
    indice_mayor = max(range(len(reparto)), key=lambda i: reparto[i][1])
    partes[indice_mayor] += monto - sum(partes, _CERO)
    return partes


def calcular_volcado_previsto(tareas):
    """tareas: lista de dicts, una por tarea, con la sección FABRICACION ya calculada:

        {"tarea_id": 1, "nombre": "Cantoneras y rejillas",
         "montos": {"materiales": ..., "bulones": ..., "pintura": ..., "fletes": ...,
                    "subcontratos": ..., "mano_obra": ..., "consumibles": ..., "ingenieria": ...},
         "gg": ..., "beneficio": ..., "impuestos": ...,
         "reparto": [{"ot_id": 10, "porcentaje": 60}, ...]}   # [] / ausente = sin OT

    Devuelve (importes como Decimal con 2 decimales):
        por_ot: {ot_id: {campo de economico_presupuesto: monto}}
        sin_ot: [{"tarea_id", "nombre", "montos": {campo: monto}, "total"}]
        cuadre: {total_asignado, total_sin_ot, total_precio_venta, diferencia, cuadra}

    El precio de venta de una tarea es la suma de sus componentes ya redondeados
    a centavos (puede diferir en un centavo del precio de venta sin redondear).
    """
    por_ot = {}
    sin_ot = []
    total_precio_venta = _CERO

    for tarea in tareas:
        montos = _montos_economicos(tarea)
        reparto = _reparto_normalizado(tarea)
        total_tarea = sum(montos.values(), _CERO)
        total_precio_venta += total_tarea

        if not reparto:
            sin_ot.append({
                "tarea_id": tarea.get("tarea_id"),
                "nombre": tarea.get("nombre"),
                "montos": montos,
                "total": total_tarea,
            })
            continue

        for campo, monto in montos.items():
            for (ot_id, _), parte in zip(reparto, _repartir(monto, reparto)):
                destino = por_ot.setdefault(ot_id, {c: _CERO for c in CAMPOS_ECONOMICOS})
                destino[campo] += parte

    total_asignado = sum((sum(campos.values(), _CERO) for campos in por_ot.values()), _CERO)
    total_sin_ot = sum((fila["total"] for fila in sin_ot), _CERO)
    diferencia = total_precio_venta - (total_asignado + total_sin_ot)

    return {
        "por_ot": por_ot,
        "sin_ot": sin_ot,
        "cuadre": {
            "total_asignado": total_asignado,
            "total_sin_ot": total_sin_ot,
            "total_precio_venta": total_precio_venta,
            "diferencia": diferencia,
            "cuadra": diferencia == 0,
        },
    }

def armar_tarea_para_volcado(resultado_tarea, tarea_id, nombre, reparto):
    """Entrada de `calcular_volcado_previsto` a partir de lo que devuelve `calcular_tarea`
    (solo la sección Fabricación; no recalcula nada)."""
    fab = resultado_tarea["fabricacion"]
    montos = {}
    for item in fab["items"]:
        montos[item["rubro"]] = montos.get(item["rubro"], 0) + item["subtotal"]
    cascada = fab["cascada"]
    return {
        "tarea_id": tarea_id,
        "nombre": nombre,
        "montos": montos,
        "gg": cascada["gg"],
        "beneficio": cascada["beneficio"],
        "impuestos": cascada["impuestos"],
        "reparto": reparto,
    }


def verificar_cuadre_con_presupuesto(volcado, precio_venta_fabricacion, cantidad_tareas):
    """Asignado a OT + sin OT debe igualar el precio de venta Fabricación del presupuesto
    (dentro del redondeo por tarea). Si no, el volcado no debe hacerse."""
    interno = volcado["cuadre"]
    asignado_mas_sin_ot = interno["total_asignado"] + interno["total_sin_ot"]
    esperado = _dec(precio_venta_fabricacion)
    diferencia = esperado - asignado_mas_sin_ot
    tolerancia = TOLERANCIA_REDONDEO_POR_TAREA * max(cantidad_tareas, 1)
    return {
        "total_asignado": interno["total_asignado"],
        "total_sin_ot": interno["total_sin_ot"],
        "asignado_mas_sin_ot": asignado_mas_sin_ot,
        "precio_venta_fabricacion": esperado,
        "diferencia": diferencia,
        "tolerancia": tolerancia,
        "cuadra": bool(interno["cuadra"]) and abs(diferencia) <= tolerancia,
    }


def detectar_ediciones_manuales(actual_por_ot, ultimo_volcado_por_ot, ot_ids):
    """Compara lo que hay hoy en cada OT con lo que dejó el último volcado.

    {ot_id: [{"campo", "actual", "ultimo_volcado"}]} solo para las OT con diferencias.
    Las OT que no estaban en el último volcado (o si no hubo volcado) no se comparan."""
    ediciones = {}
    for ot_id in ot_ids:
        ultimo = ultimo_volcado_por_ot.get(ot_id)
        if ultimo is None:
            continue
        actual = actual_por_ot.get(ot_id) or {}
        diferencias = []
        for campo in CAMPOS_ECONOMICOS:
            valor_actual, valor_ultimo = _dec(actual.get(campo)), _dec(ultimo.get(campo))
            if abs(valor_actual - valor_ultimo) > TOLERANCIA_EDICION_MANUAL:
                diferencias.append({"campo": campo, "actual": valor_actual, "ultimo_volcado": valor_ultimo})
        if diferencias:
            ediciones[ot_id] = diferencias
    return ediciones
