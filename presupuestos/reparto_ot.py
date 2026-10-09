"""Helpers puros (sin Flask ni DB) del mapeo Tarea -> OT: validación del
reparto que llega de la pantalla y sugerencia de porcentajes por kg."""
from decimal import Decimal, InvalidOperation

_TOLERANCIA_SUMA = Decimal("0.000001")  # igual que volcado_previsto: absorbe ruido de float


def _decimal(valor):
    texto = str(valor).strip().replace(",", ".")
    try:
        numero = Decimal(texto)
    except InvalidOperation:
        return None
    return numero if numero.is_finite() else None


def validar_reparto_tarea(filas):
    """filas: [{"ot_id", "porcentaje"}] tal como las manda la pantalla.

    Devuelve (filas_limpias, error). `filas_limpias` = [{"ot_id": int, "porcentaje": float}].
    Las filas totalmente vacías se ignoran; sin filas = "Sin OT (nivel obra)" y es válido."""
    limpias = []
    for fila in filas or []:
        fila = fila or {}
        ot_txt = str(fila.get("ot_id") if fila.get("ot_id") is not None else "").strip()
        pct_txt = str(fila.get("porcentaje") if fila.get("porcentaje") is not None else "").strip()
        if ot_txt == "" and pct_txt == "":
            continue
        if ot_txt == "":
            return [], "Elegí una OT en cada fila (o quitá la fila vacía)."
        if not ot_txt.isdigit():
            return [], f"OT inválida: {ot_txt!r}."
        ot_id = int(ot_txt)
        if pct_txt == "":
            return [], f"Falta el porcentaje de la OT {ot_id}."
        porcentaje = _decimal(pct_txt)
        if porcentaje is None:
            return [], f"El porcentaje {pct_txt!r} de la OT {ot_id} no es un número."
        if porcentaje <= 0 or porcentaje > 100:
            return [], f"El porcentaje de la OT {ot_id} debe ser mayor a 0 y hasta 100."
        limpias.append((ot_id, porcentaje))

    repetidas = sorted({ot for ot, _ in limpias if sum(1 for o, _ in limpias if o == ot) > 1})
    if repetidas:
        return [], f"La OT {repetidas[0]} está repetida: usá una sola fila por OT."

    if limpias:
        suma = sum((p for _, p in limpias), Decimal(0))
        if abs(suma - 100) > _TOLERANCIA_SUMA:
            return [], f"Los porcentajes suman {suma:g}% y deben sumar 100%."

    return [{"ot_id": ot, "porcentaje": float(p)} for ot, p in limpias], None


def sugerir_porcentajes_por_kg(kg_por_ot):
    """{ot_id: kg} -> {ot_id: porcentaje} con 2 decimales que suman exactamente 100
    (resto por mayor fracción). None si hay menos de 2 OT o alguna no tiene kg."""
    if len(kg_por_ot) < 2 or any((kg or 0) <= 0 for kg in kg_por_ot.values()):
        return None

    total = sum(kg_por_ot.values())
    ots = list(kg_por_ot)
    crudos = [kg_por_ot[ot] / total * 10000 for ot in ots]  # centésimas de punto porcentual
    pisos = [int(c) for c in crudos]
    faltan = 10000 - sum(pisos)
    por_fraccion = sorted(range(len(ots)), key=lambda i: crudos[i] - pisos[i], reverse=True)
    for i in por_fraccion[:faltan]:
        pisos[i] += 1
    return {ot: pisos[i] / 100 for i, ot in enumerate(ots)}
