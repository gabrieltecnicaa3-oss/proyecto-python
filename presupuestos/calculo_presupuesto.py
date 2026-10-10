"""Motor de cálculo del módulo Presupuestos (sección 3 de
ESPECIFICACION_MODULO_PRESUPUESTOS_V1.md).

Reglas de diseño (no negociables):
- Sin Flask, sin base de datos. Solo dicts/números en, dicts/números afuera.
- Los porcentajes son SIEMPRE fracción (0.07 = 7%), nunca 7.
- El costo directo, GG, Beneficio e Impuestos se calculan con precisión
  continua (sin redondear pasos intermedios); el redondeo a pesos es
  responsabilidad de quien muestra el número (UI/reportes), no del motor.
"""


# ─────────────────────────────────────────────────────────────────
# Fabricación — cálculo por rubro (sección 3.1)
# ─────────────────────────────────────────────────────────────────

def calcular_materiales_perfil(datos, perfil, tipo_cambio_referencia=None):
    """datos: {perfil_id, cantidad, largo_mm, precio_unitario_kg, cant_barras?}
    perfil: {kg_m, m2_m} — ya resuelto desde el catálogo (articulos_sum), no se
    consulta la DB acá. precio_unitario_kg está en USD (ver sección 3.6): el
    subtotal en $ surge de multiplicarlo por el tipo de cambio de referencia.
    """
    cantidad = float(datos.get("cantidad") or 0)
    largo_mm = float(datos.get("largo_mm") or 0)
    precio_unitario_kg = float(datos.get("precio_unitario_kg") or 0)
    kg_m = float((perfil or {}).get("kg_m") or 0)
    m2_m = float((perfil or {}).get("m2_m") or 0)
    tipo_cambio = float(tipo_cambio_referencia or 0) or 1.0

    largo_m = largo_mm / 1000.0
    peso = cantidad * largo_m * kg_m
    m2 = cantidad * largo_m * m2_m
    subtotal = peso * precio_unitario_kg * tipo_cambio
    return {"peso": peso, "m2": m2, "subtotal": subtotal}


def calcular_placas(datos, peso_materiales_perfiles, tipo_cambio_referencia=None):
    """datos: {descripcion, porcentaje, precio_unitario_kg}. "Placas": peso =
    % del peso acumulado de las líneas tipo `perfil` cargadas ANTES de esta
    línea (orden de carga importa); precio_unitario_kg está en USD, subtotal
    en $ = peso * precio_unitario_kg * tipo de cambio de referencia."""
    porcentaje = float(datos.get("porcentaje") or 0)
    precio_unitario_kg = float(datos.get("precio_unitario_kg") or 0)
    tipo_cambio = float(tipo_cambio_referencia or 0) or 1.0
    peso = porcentaje * peso_materiales_perfiles
    subtotal = peso * precio_unitario_kg * tipo_cambio
    return {"peso": peso, "subtotal": subtotal}


def calcular_materiales_no_listado(datos, tipo_cambio_referencia=None):
    """datos: {descripcion, cantidad, unidad, precio_unitario}. Carga manual de
    materiales que no están en el catálogo de perfiles (sin peso/m2 asociado).
    precio_unitario está en USD: el subtotal en $ = cantidad * precio_unitario
    * tipo de cambio de referencia."""
    cantidad = float(datos.get("cantidad") or 0)
    precio_unitario = float(datos.get("precio_unitario") or 0)
    tipo_cambio = float(tipo_cambio_referencia or 0) or 1.0
    return cantidad * precio_unitario * tipo_cambio


def calcular_materiales_chapa(datos, perfil, tipo_cambio_referencia=None):
    """datos: {perfil_id, cantidad, largo_mm, precio_unitario_m2}. Variante de
    `calcular_materiales_perfil` para materiales de chapa/zinguería/grating:
    en vez de peso (kg), se factura por superficie (m2). m2 = cantidad *
    (largo_mm / 1000). precio_unitario_m2 está en USD: subtotal en $ = m2 *
    precio_unitario_m2 * tipo de cambio de referencia."""
    cantidad = float(datos.get("cantidad") or 0)
    largo_mm = float(datos.get("largo_mm") or 0)
    precio_unitario_m2 = float(datos.get("precio_unitario_m2") or 0)
    tipo_cambio = float(tipo_cambio_referencia or 0) or 1.0

    largo_m = largo_mm / 1000.0
    m2 = cantidad * largo_m
    subtotal = m2 * precio_unitario_m2 * tipo_cambio
    return {"m2": m2, "subtotal": subtotal}


def calcular_tornillos(datos, m2_total_chapas, tipo_cambio_referencia=None):
    """datos: {descripcion, precio_unitario}. Reemplaza a "Placas" en tareas de
    modo chapa: la cantidad de tornillos NO se carga a mano, surge de 4
    unidades por cada m2 total de chapa cargado en la sección (sin importar su
    posición, igual que Placas). precio_unitario está en USD/unidad: subtotal
    en $ = cantidad_tornillos * precio_unitario * tipo de cambio."""
    precio_unitario = float(datos.get("precio_unitario") or 0)
    tipo_cambio = float(tipo_cambio_referencia or 0) or 1.0
    cantidad = 4.0 * m2_total_chapas
    subtotal = cantidad * precio_unitario * tipo_cambio
    return {"cantidad": cantidad, "subtotal": subtotal}


def calcular_materiales_grating(datos, perfil, tipo_cambio_referencia=None):
    """datos: {perfil_id, cantidad, m2, precio_unitario_m2}. Variante de
    materiales para Grating: a diferencia de chapa (ancho fijo del catálogo x
    largo cargado), acá tanto `cantidad` como `m2` (m2 unitario de la plancha/
    pieza elegida) se cargan a mano, así que el total de m2 de la línea es
    cantidad * m2. `perfil.kg_m` se reutiliza como densidad kg/m2 del material
    elegido (catálogo articulos_sum, categoría Grating) para derivar el peso
    total. precio_unitario_m2 está en USD: subtotal en $ = total_m2 *
    precio_unitario_m2 * tipo de cambio de referencia."""
    cantidad = float(datos.get("cantidad") or 0)
    m2_unitario = float(datos.get("m2") or 0)
    precio_unitario_m2 = float(datos.get("precio_unitario_m2") or 0)
    kg_m2 = float((perfil or {}).get("kg_m") or 0)
    tipo_cambio = float(tipo_cambio_referencia or 0) or 1.0

    total_m2 = cantidad * m2_unitario
    total_kg = total_m2 * kg_m2
    subtotal = total_m2 * precio_unitario_m2 * tipo_cambio
    return {"m2": total_m2, "peso": total_kg, "subtotal": subtotal}


def calcular_fijaciones(datos, m2_total_grating, tipo_cambio_referencia=None):
    """datos: {descripcion, precio_unitario}. Reemplaza a "Placas"/"Tornillos"
    en tareas de Grating: la cantidad NO se carga a mano, surge de 4 unidades
    por cada m2 total de grating cargado en la sección (misma fórmula que
    `calcular_tornillos`). precio_unitario está en USD/unidad: subtotal en $ =
    cantidad_fijaciones * precio_unitario * tipo de cambio."""
    precio_unitario = float(datos.get("precio_unitario") or 0)
    tipo_cambio = float(tipo_cambio_referencia or 0) or 1.0
    cantidad = 4.0 * m2_total_grating
    subtotal = cantidad * precio_unitario * tipo_cambio
    return {"cantidad": cantidad, "subtotal": subtotal}


def calcular_bulones(datos, subtotal_materiales):
    """datos: {porcentaje}. Se aplica sobre el subtotal de materiales acumulado."""
    porcentaje = float(datos.get("porcentaje") or 0)
    return porcentaje * subtotal_materiales


def calcular_pintura(datos, m2_total_materiales, tipo_cambio_referencia=None):
    """datos: {esquema_id, precio_unitario_m2}. Se aplica sobre el m2 acumulado
    de las líneas materiales/perfil de la misma sección. precio_unitario_m2
    está en USD: subtotal en $ = precio_unitario_m2 * tipo de cambio * m2."""
    precio_unitario_m2 = float(datos.get("precio_unitario_m2") or 0)
    tipo_cambio = float(tipo_cambio_referencia or 0) or 1.0
    return precio_unitario_m2 * tipo_cambio * m2_total_materiales


def _cantidad_por_precio(datos):
    cantidad = float(datos.get("cantidad") or 0)
    precio_unitario = float(datos.get("precio_unitario") or 0)
    return cantidad * precio_unitario


def calcular_fletes(datos):
    """datos: {descripcion, cantidad, precio_unitario} (múltiples líneas)."""
    return _cantidad_por_precio(datos)


def calcular_subcontratos(datos):
    """datos: {descripcion, cantidad, precio_unitario}. Misma fórmula en
    Fabricación y Montaje."""
    return _cantidad_por_precio(datos)


def _operarios_dias_tarifa(datos):
    operarios = float(datos.get("operarios") or 0)
    dias = float(datos.get("dias") or 0)
    tarifa_dh = float(datos.get("tarifa_dh") or 0)
    return operarios * dias * tarifa_dh


def calcular_mano_obra(datos):
    """datos: {operarios, dias, tarifa_dh}. Misma fórmula en taller y obra."""
    return _operarios_dias_tarifa(datos)


def calcular_consumibles(datos):
    """datos: {operarios, dias, tarifa_dh}. Misma fórmula en taller y obra."""
    return _operarios_dias_tarifa(datos)


def calcular_ingenieria(datos, subtotal_materiales):
    """datos: {monto} (carga manual, no fórmula). El porcentaje es solo
    informativo: no interviene en ningún cálculo posterior."""
    monto = float(datos.get("monto") or 0)
    porcentaje_informativo = (monto / subtotal_materiales) if subtotal_materiales else 0.0
    return {"subtotal": monto, "porcentaje_informativo": porcentaje_informativo}


# ─────────────────────────────────────────────────────────────────
# Montaje — cálculo por rubro (sección 3.2)
# ─────────────────────────────────────────────────────────────────

def calcular_equipo(datos):
    """datos: {equipo_id, dias, tarifa_dia} (múltiples líneas, una por equipo)."""
    dias = float(datos.get("dias") or 0)
    tarifa_dia = float(datos.get("tarifa_dia") or 0)
    return dias * tarifa_dia


def _tarifa_dia_por_dias(datos):
    tarifa_dia = float(datos.get("tarifa_dia") or 0)
    dias = float(datos.get("dias") or 0)
    return tarifa_dia * dias


def calcular_ingeniero(datos):
    """datos: {tarifa_dia, dias}. Director de obra."""
    return _tarifa_dia_por_dias(datos)


def calcular_tecnico_hys(datos):
    """datos: {tarifa_dia, dias}."""
    return _tarifa_dia_por_dias(datos)


# ─────────────────────────────────────────────────────────────────
# Cálculo por sección (aplica el rubro correcto a cada item, en orden)
# ─────────────────────────────────────────────────────────────────

RUBROS_FABRICACION = (
    "materiales",
    "bulones",
    "pintura",
    "fletes",
    "subcontratos",
    "mano_obra",
    "consumibles",
    "ingenieria",
)

RUBROS_MONTAJE = (
    "mano_obra",
    "consumibles",
    "equipo",
    "subcontratos",
    "ingeniero",
    "tecnico_hys",
)


def calcular_seccion_fabricacion(items, perfiles_por_id=None, tipo_cambio_referencia=None):
    """items: lista ORDENADA de dicts {rubro, tipo_item (solo materiales), datos}.
    perfiles_por_id: {perfil_id: {kg_m, m2_m}}, ya resuelto (sin DB acá).
    tipo_cambio_referencia: precio de los materiales/pintura está en USD, se
    multiplica por este valor para obtener el subtotal en $ (sección 3.6).

    Bulones y los materiales porcentuales se calculan sobre los materiales de
    toda la sección, sin depender de la posición de sus líneas. El orden solo
    afecta el porcentaje informativo de ingeniería.

    Devuelve (items_resueltos, resumen). No muta la lista `items` original.
    """
    perfiles_por_id = perfiles_por_id or {}

    # Pre-pasada: peso total de materiales/perfil (para "Placas"), m2 total de
    # materiales/chapa (para "Tornillos"), m2 total de materiales/grating
    # (para "Fijaciones") y m2 total de materiales/perfil+chapa (para
    # "Pintura"), todas dependen del total de la sección y no de lo
    # acumulado hasta su posición (pintura puede crearse con un id menor que
    # las líneas de materiales, así que no puede depender del orden).
    peso_total_materiales_perfiles = 0.0
    m2_total_materiales_chapa = 0.0
    m2_total_materiales_grating = 0.0
    m2_total_materiales_pintura = 0.0
    for item in items:
        if item.get("rubro") == "materiales" and item.get("tipo_item") == "perfil":
            perfil = perfiles_por_id.get((item.get("datos") or {}).get("perfil_id"), {})
            calc_perfil = calcular_materiales_perfil(item.get("datos") or {}, perfil, tipo_cambio_referencia)
            peso_total_materiales_perfiles += calc_perfil["peso"]
            m2_total_materiales_pintura += calc_perfil["m2"]
        elif item.get("rubro") == "materiales" and item.get("tipo_item") == "chapa":
            perfil = perfiles_por_id.get((item.get("datos") or {}).get("perfil_id"), {})
            calc_chapa = calcular_materiales_chapa(item.get("datos") or {}, perfil, tipo_cambio_referencia)
            m2_total_materiales_chapa += calc_chapa["m2"]
            m2_total_materiales_pintura += calc_chapa["m2"]
        elif item.get("rubro") == "materiales" and item.get("tipo_item") == "grating":
            perfil = perfiles_por_id.get((item.get("datos") or {}).get("perfil_id"), {})
            m2_total_materiales_grating += calcular_materiales_grating(item.get("datos") or {}, perfil, tipo_cambio_referencia)["m2"]

    subtotal_total_materiales = 0.0
    for item in items:
        if item.get("rubro") != "materiales":
            continue
        datos = item.get("datos") or {}
        tipo_item = item.get("tipo_item")
        if tipo_item == "perfil":
            perfil = perfiles_por_id.get(datos.get("perfil_id"), {})
            subtotal_total_materiales += calcular_materiales_perfil(
                datos, perfil, tipo_cambio_referencia
            )["subtotal"]
        elif tipo_item == "porcentaje":
            subtotal_total_materiales += calcular_placas(
                datos, peso_total_materiales_perfiles, tipo_cambio_referencia
            )["subtotal"]
        elif tipo_item == "no_listado":
            subtotal_total_materiales += calcular_materiales_no_listado(
                datos, tipo_cambio_referencia
            )
        elif tipo_item == "chapa":
            perfil = perfiles_por_id.get(datos.get("perfil_id"), {})
            subtotal_total_materiales += calcular_materiales_chapa(
                datos, perfil, tipo_cambio_referencia
            )["subtotal"]
        elif tipo_item == "tornillos":
            subtotal_total_materiales += calcular_tornillos(
                datos, m2_total_materiales_chapa, tipo_cambio_referencia
            )["subtotal"]
        elif tipo_item == "grating":
            perfil = perfiles_por_id.get(datos.get("perfil_id"), {})
            subtotal_total_materiales += calcular_materiales_grating(
                datos, perfil, tipo_cambio_referencia
            )["subtotal"]
        elif tipo_item == "fijaciones":
            subtotal_total_materiales += calcular_fijaciones(
                datos, m2_total_materiales_grating, tipo_cambio_referencia
            )["subtotal"]

    items_resueltos = []
    acumulado_materiales_perfiles = 0.0
    acumulado_m2_materiales = 0.0
    acumulado_materiales_total = 0.0  # perfil + placas

    for item in items:
        rubro = item.get("rubro")
        tipo_item = item.get("tipo_item")
        datos = item.get("datos") or {}
        resultado = dict(item)

        if rubro == "materiales" and tipo_item == "perfil":
            perfil = perfiles_por_id.get(datos.get("perfil_id"), {})
            calc = calcular_materiales_perfil(datos, perfil, tipo_cambio_referencia)
            resultado["subtotal"] = calc["subtotal"]
            resultado["peso"] = calc["peso"]
            resultado["m2"] = calc["m2"]
            acumulado_materiales_perfiles += calc["subtotal"]
            acumulado_m2_materiales += calc["m2"]
            acumulado_materiales_total += calc["subtotal"]
        elif rubro == "materiales" and tipo_item == "porcentaje":
            calc = calcular_placas(datos, peso_total_materiales_perfiles, tipo_cambio_referencia)
            resultado["subtotal"] = calc["subtotal"]
            resultado["peso"] = calc["peso"]
            acumulado_materiales_total += calc["subtotal"]
        elif rubro == "materiales" and tipo_item == "no_listado":
            subtotal = calcular_materiales_no_listado(datos, tipo_cambio_referencia)
            resultado["subtotal"] = subtotal
            acumulado_materiales_total += subtotal
        elif rubro == "materiales" and tipo_item == "chapa":
            perfil = perfiles_por_id.get(datos.get("perfil_id"), {})
            calc = calcular_materiales_chapa(datos, perfil, tipo_cambio_referencia)
            resultado["subtotal"] = calc["subtotal"]
            resultado["m2"] = calc["m2"]
            acumulado_m2_materiales += calc["m2"]
            acumulado_materiales_total += calc["subtotal"]
        elif rubro == "materiales" and tipo_item == "tornillos":
            calc = calcular_tornillos(datos, m2_total_materiales_chapa, tipo_cambio_referencia)
            resultado["subtotal"] = calc["subtotal"]
            resultado["cantidad"] = calc["cantidad"]
            acumulado_materiales_total += calc["subtotal"]
        elif rubro == "materiales" and tipo_item == "grating":
            perfil = perfiles_por_id.get(datos.get("perfil_id"), {})
            calc = calcular_materiales_grating(datos, perfil, tipo_cambio_referencia)
            resultado["subtotal"] = calc["subtotal"]
            resultado["m2"] = calc["m2"]
            resultado["peso"] = calc["peso"]
            acumulado_m2_materiales += calc["m2"]
            acumulado_materiales_total += calc["subtotal"]
        elif rubro == "materiales" and tipo_item == "fijaciones":
            calc = calcular_fijaciones(datos, m2_total_materiales_grating, tipo_cambio_referencia)
            resultado["subtotal"] = calc["subtotal"]
            resultado["cantidad"] = calc["cantidad"]
            acumulado_materiales_total += calc["subtotal"]
        elif rubro == "bulones":
            resultado["subtotal"] = calcular_bulones(datos, subtotal_total_materiales)
        elif rubro == "pintura":
            resultado["subtotal"] = calcular_pintura(datos, m2_total_materiales_pintura, tipo_cambio_referencia)
            resultado["m2"] = m2_total_materiales_pintura
        elif rubro == "fletes":
            resultado["subtotal"] = calcular_fletes(datos)
        elif rubro == "subcontratos":
            resultado["subtotal"] = calcular_subcontratos(datos)
        elif rubro == "mano_obra":
            resultado["subtotal"] = calcular_mano_obra(datos)
        elif rubro == "consumibles":
            resultado["subtotal"] = calcular_consumibles(datos)
        elif rubro == "ingenieria":
            calc = calcular_ingenieria(datos, acumulado_materiales_total)
            resultado["subtotal"] = calc["subtotal"]
            resultado["porcentaje_informativo"] = calc["porcentaje_informativo"]
        else:
            raise ValueError(f"Rubro no válido para FABRICACION: {rubro!r}")

        items_resueltos.append(resultado)

    costo_directo = sum(it["subtotal"] for it in items_resueltos)
    return items_resueltos, {
        "costo_directo": costo_directo,
        "subtotal_materiales": acumulado_materiales_total,
        "m2_total_materiales": acumulado_m2_materiales,
    }


def calcular_seccion_montaje(items):
    """items: lista de dicts {rubro, datos}. Sin dependencias de orden entre sí."""
    items_resueltos = []

    for item in items:
        rubro = item.get("rubro")
        datos = item.get("datos") or {}
        resultado = dict(item)

        if rubro == "mano_obra":
            resultado["subtotal"] = calcular_mano_obra(datos)
        elif rubro == "consumibles":
            resultado["subtotal"] = calcular_consumibles(datos)
        elif rubro == "equipo":
            resultado["subtotal"] = calcular_equipo(datos)
        elif rubro == "subcontratos":
            resultado["subtotal"] = calcular_subcontratos(datos)
        elif rubro == "ingeniero":
            resultado["subtotal"] = calcular_ingeniero(datos)
        elif rubro == "tecnico_hys":
            resultado["subtotal"] = calcular_tecnico_hys(datos)
        else:
            raise ValueError(f"Rubro no válido para MONTAJE: {rubro!r}")

        items_resueltos.append(resultado)

    costo_directo = sum(it["subtotal"] for it in items_resueltos)
    return items_resueltos, {"costo_directo": costo_directo}


# ─────────────────────────────────────────────────────────────────
# Cascada final (sección 3.3) — GG → Beneficio → Impuestos, en cadena
# ─────────────────────────────────────────────────────────────────

def calcular_cascada(costo_directo, gg_pct, beneficio_pct, imp_pct):
    """Orden obligatorio: GG sobre costo_directo; Beneficio sobre
    (costo_directo + GG); Impuestos sobre (subtotal_1 + Beneficio).
    Cada % se aplica sobre el subtotal acumulado, nunca sobre el costo
    directo original."""
    gg = gg_pct * costo_directo
    subtotal_1 = costo_directo + gg
    beneficio = beneficio_pct * subtotal_1
    subtotal_2 = subtotal_1 + beneficio
    impuestos = imp_pct * subtotal_2
    precio_venta = subtotal_2 + impuestos
    return {
        "costo_directo": costo_directo,
        "gg": gg,
        "subtotal_1": subtotal_1,
        "beneficio": beneficio,
        "subtotal_2": subtotal_2,
        "impuestos": impuestos,
        "precio_venta": precio_venta,
    }


# ─────────────────────────────────────────────────────────────────
# Totales por tarea y por presupuesto (sección 3.4)
# ─────────────────────────────────────────────────────────────────

def calcular_tarea(fabricacion_items, fabricacion_pcts, montaje_items, montaje_pcts, perfiles_por_id=None, tipo_cambio_referencia=None):
    """fabricacion_pcts / montaje_pcts: {"gg_pct", "beneficio_pct", "imp_pct"}
    (uno por sección, tomados de tarea_secciones — cada sección tiene sus
    propios %, ver sección 3 de la especificación)."""
    items_fab, resumen_fab = calcular_seccion_fabricacion(fabricacion_items, perfiles_por_id, tipo_cambio_referencia)
    cascada_fab = calcular_cascada(
        resumen_fab["costo_directo"],
        fabricacion_pcts["gg_pct"],
        fabricacion_pcts["beneficio_pct"],
        fabricacion_pcts["imp_pct"],
    )

    items_mon, resumen_mon = calcular_seccion_montaje(montaje_items)
    cascada_mon = calcular_cascada(
        resumen_mon["costo_directo"],
        montaje_pcts["gg_pct"],
        montaje_pcts["beneficio_pct"],
        montaje_pcts["imp_pct"],
    )

    precio_venta_tarea = cascada_fab["precio_venta"] + cascada_mon["precio_venta"]

    return {
        "fabricacion": {"items": items_fab, "resumen": resumen_fab, "cascada": cascada_fab},
        "montaje": {"items": items_mon, "resumen": resumen_mon, "cascada": cascada_mon},
        "precio_venta_tarea": precio_venta_tarea,
    }


def calcular_presupuesto(tareas_resultados):
    """tareas_resultados: lista de dicts devueltos por `calcular_tarea`."""
    precio_venta_presupuesto = sum(t["precio_venta_tarea"] for t in tareas_resultados)
    return {"precio_venta_presupuesto": precio_venta_presupuesto}


# ─────────────────────────────────────────────────────────────────
# Reportes (sección 5) — cruzan TODAS las tareas de un presupuesto.
# Siguen tomando como entrada la lista de resultados de `calcular_tarea`,
# nunca la DB: la resolución de nombres (perfil/equipo) queda para la capa
# de rutas, acá solo se agregan montos y cantidades.
# ─────────────────────────────────────────────────────────────────

# Mapeo rubro -> categoría de costo (sección 5.1, hoja "Resumen" del Excel
# original). Nota: "Subcontratos" (FABRICACION y MONTAJE) no figura en la
# lista textual de la especificación; se agrega como categoría propia para
# no perder ese costo del total (a confirmar con el usuario si prefiere que
# se funda con otra fila).
CATEGORIAS_POR_RUBRO = {
    ("FABRICACION", "mano_obra"): "Mano de obra taller",
    ("FABRICACION", "consumibles"): "Consumibles taller",
    ("MONTAJE", "mano_obra"): "Mano de obra montaje",
    ("MONTAJE", "consumibles"): "Consumibles montaje",
    ("FABRICACION", "materiales"): "Materiales",
    ("FABRICACION", "bulones"): "Materiales",
    ("FABRICACION", "pintura"): "Pintura",
    ("FABRICACION", "fletes"): "Fletes",
    ("FABRICACION", "subcontratos"): "Subcontratos",
    ("MONTAJE", "subcontratos"): "Subcontratos",
    ("MONTAJE", "equipo"): "Equipos",
    ("FABRICACION", "ingenieria"): "Ingeniería",
    ("MONTAJE", "ingeniero"): "Director de obra",
    ("MONTAJE", "tecnico_hys"): "Técnico HyS",
}

ORDEN_CATEGORIAS_RESUMEN = (
    "Mano de obra taller",
    "Consumibles taller",
    "Mano de obra montaje",
    "Consumibles montaje",
    "Materiales",
    "Pintura",
    "Fletes",
    "Subcontratos",
    "Equipos",
    "Ingeniería",
    "Director de obra",
    "Técnico HyS",
    "GG Fab",
    "GG Mon",
    "Impuestos Fab",
    "Impuestos Mon",
    "Beneficio Fab",
    "Beneficio Mon",
)


def calcular_resumen_categorias(tareas_resultados):
    """Sección 5.1: cruza todas las tareas y suma los montos por categoría
    de costo. Devuelve {"categorias": [{nombre, monto, porcentaje}], "total"}."""
    montos = {cat: 0.0 for cat in ORDEN_CATEGORIAS_RESUMEN}

    for tarea in tareas_resultados:
        for item in tarea["fabricacion"]["items"]:
            categoria = CATEGORIAS_POR_RUBRO.get(("FABRICACION", item["rubro"]))
            if categoria:
                montos[categoria] += item["subtotal"]
        for item in tarea["montaje"]["items"]:
            categoria = CATEGORIAS_POR_RUBRO.get(("MONTAJE", item["rubro"]))
            if categoria:
                montos[categoria] += item["subtotal"]

        cascada_fab = tarea["fabricacion"]["cascada"]
        cascada_mon = tarea["montaje"]["cascada"]
        montos["GG Fab"] += cascada_fab["gg"]
        montos["GG Mon"] += cascada_mon["gg"]
        montos["Beneficio Fab"] += cascada_fab["beneficio"]
        montos["Beneficio Mon"] += cascada_mon["beneficio"]
        montos["Impuestos Fab"] += cascada_fab["impuestos"]
        montos["Impuestos Mon"] += cascada_mon["impuestos"]

    total = sum(montos.values())
    categorias = [
        {"nombre": cat, "monto": montos[cat], "porcentaje": (montos[cat] / total) if total else 0.0}
        for cat in ORDEN_CATEGORIAS_RESUMEN
    ]
    return {"categorias": categorias, "total": total}


# ─────────────────────────────────────────────────────────────────
# Sección 5.2/5.3 — Reportes Excel (explosión de insumos / previsión de
# fondos para Odoo). Mismo criterio que arriba: solo se reorganizan montos
# que `calcular_tarea` ya calculó (item["subtotal"] y cascada.gg/beneficio/
# impuestos), nunca se recalcula nada desde cero.
# ─────────────────────────────────────────────────────────────────

def calcular_totales_por_concepto(tareas_resultados):
    """Devuelve {(tipo_seccion, concepto): monto}, sumando TODAS las tareas
    recibidas (si se quiere el total de una sola tarea, pasar una lista de
    un elemento). `concepto` = mismo valor que `rubro` para items de costo,
    más "GG"/"BENEFICIO"/"IMPUESTOS" para los 3 componentes de la cascada
    (sección 2.1: mismo enum que usa `config_categorias_odoo.concepto`)."""
    totales = {}

    def _sumar(tipo_seccion, concepto, monto):
        clave = (tipo_seccion, concepto)
        totales[clave] = totales.get(clave, 0.0) + monto

    for tarea in tareas_resultados:
        for item in tarea["fabricacion"]["items"]:
            _sumar("FABRICACION", item["rubro"], item["subtotal"])
        for item in tarea["montaje"]["items"]:
            _sumar("MONTAJE", item["rubro"], item["subtotal"])

        cascada_fab = tarea["fabricacion"]["cascada"]
        cascada_mon = tarea["montaje"]["cascada"]
        _sumar("FABRICACION", "GG", cascada_fab["gg"])
        _sumar("FABRICACION", "BENEFICIO", cascada_fab["beneficio"])
        _sumar("FABRICACION", "IMPUESTOS", cascada_fab["impuestos"])
        _sumar("MONTAJE", "GG", cascada_mon["gg"])
        _sumar("MONTAJE", "BENEFICIO", cascada_mon["beneficio"])
        _sumar("MONTAJE", "IMPUESTOS", cascada_mon["impuestos"])

    return totales


# Filas del Reporte 1 (sección 5.2): (nombre de fila, conceptos que forman la
# columna Fabricación o None si la fila no aplica a esa sección, ídem Montaje).
# "Materiales" incluye bulones (mismo criterio que CATEGORIAS_POR_RUBRO/
# sección 5.1); "Subcontratos" se mantiene separado por sección (a diferencia
# de la sección 5.1, que lo funde en una sola categoría) porque acá hace
# falta mostrar el monto de Fabricación y el de Montaje en columnas distintas.
FILAS_EXPLOSION_INSUMOS = (
    ("Mano de obra", ("mano_obra",), ("mano_obra",)),
    ("Consumibles", ("consumibles",), ("consumibles",)),
    ("Materiales", ("materiales", "bulones"), None),
    ("Pintura", ("pintura",), None),
    ("Fletes", ("fletes",), None),
    ("Subcontratos", ("subcontratos",), ("subcontratos",)),
    ("Equipos", None, ("equipo",)),
    ("Ingeniería", ("ingenieria",), None),
    ("Director de obra", None, ("ingeniero",)),
    ("Técnico HyS", None, ("tecnico_hys",)),
    ("GG", ("GG",), ("GG",)),
    ("Impuestos", ("IMPUESTOS",), ("IMPUESTOS",)),
    ("Beneficio", ("BENEFICIO",), ("BENEFICIO",)),
)


def calcular_explosion_insumos(totales_por_concepto):
    """Arma las filas del Reporte 1 (sección 5.2) a partir de
    `calcular_totales_por_concepto`: una fila por categoría con su monto en
    Fabricación y/o Montaje. `None` = la categoría no aplica a esa sección
    (el Excel la muestra como "—"). Sirve tanto para la pestaña de una tarea
    (pasando el total de esa sola tarea) como para la pestaña "Resumen"
    (pasando el total de todas las tareas del presupuesto)."""
    filas = []
    total_fabricacion = total_montaje = 0.0
    for nombre, conceptos_fab, conceptos_mon in FILAS_EXPLOSION_INSUMOS:
        monto_fab = None
        if conceptos_fab is not None:
            monto_fab = sum(totales_por_concepto.get(("FABRICACION", c), 0.0) for c in conceptos_fab)
            total_fabricacion += monto_fab
        monto_mon = None
        if conceptos_mon is not None:
            monto_mon = sum(totales_por_concepto.get(("MONTAJE", c), 0.0) for c in conceptos_mon)
            total_montaje += monto_mon
        filas.append({"categoria": nombre, "fabricacion": monto_fab, "montaje": monto_mon})

    filas.append({"categoria": "TOTAL", "fabricacion": total_fabricacion, "montaje": total_montaje})
    return {
        "filas": filas,
        "total_fabricacion": total_fabricacion,
        "total_montaje": total_montaje,
        "total": total_fabricacion + total_montaje,
    }


def calcular_reporte_prevision_fondos(totales_por_concepto, mapeo_categorias_odoo):
    """Sección 5.3: reclasifica `calcular_totales_por_concepto` (de TODAS las
    tareas del presupuesto) a `categoria_odoo` y agrupa. `mapeo_categorias_odoo`
    es la lista de filas de la tabla `config_categorias_odoo` (sección 2.1),
    cada una `{"concepto", "tipo_seccion", "categoria_odoo", "factor"}` — se
    recibe como parámetro (seed cargado desde la DB por quien llama) para no
    hardcodear esos valores acá. `factor` (default 1) permite repartir un
    mismo concepto entre varias categorías (ej. mano_obra/MONTAJE: 0.897 a
    "Gastos e Inversión Laboral" y 0.103 a "Traslado y Movilidad de Personal").
    Los conceptos con `categoria_odoo` nulo (Beneficio Fab/Mon) quedan
    excluidos del resultado. `fabricacion`/`montaje` quedan en `None` (el
    Excel muestra "—") cuando ningún concepto de esa sección mapea a esa
    categoría — a diferencia de 0.0, que sí es un monto real (ej. un % en 0)."""
    montos = {}
    orden_categorias = []
    for fila in mapeo_categorias_odoo:
        categoria_odoo = fila.get("categoria_odoo")
        if not categoria_odoo:
            continue
        clave = (fila["tipo_seccion"], fila["concepto"])
        factor = float(fila.get("factor") if fila.get("factor") is not None else 1)
        monto = totales_por_concepto.get(clave, 0.0) * factor
        if categoria_odoo not in montos:
            montos[categoria_odoo] = {"FABRICACION": None, "MONTAJE": None}
            orden_categorias.append(categoria_odoo)
        tipo_seccion = fila["tipo_seccion"]
        montos[categoria_odoo][tipo_seccion] = (montos[categoria_odoo][tipo_seccion] or 0.0) + monto

    filas = [
        {
            "categoria": categoria,
            "fabricacion": montos[categoria]["FABRICACION"],
            "montaje": montos[categoria]["MONTAJE"],
        }
        for categoria in orden_categorias
    ]
    return {"filas": filas}


def calcular_peso_total_kg(tareas_resultados):
    """Suma el peso (kg) de todas las líneas materiales/perfil de FABRICACION
    de todas las tareas. Insumo del indicador $/kg (sección 3.5)."""
    return sum(
        item.get("peso") or 0.0
        for tarea in tareas_resultados
        for item in tarea["fabricacion"]["items"]
        if "peso" in item
    )


def calcular_costo_estructura(tareas_resultados):
    """Sección 3.5: costo_estructura = $materiales + $mano_obra_taller +
    $mano_obra_obra + $pintura (insumo del indicador $/kg, NO es el costo
    directo completo ni el precio de venta)."""
    total = 0.0
    for tarea in tareas_resultados:
        for item in tarea["fabricacion"]["items"]:
            if item["rubro"] in ("materiales", "pintura", "mano_obra"):
                total += item["subtotal"]
        for item in tarea["montaje"]["items"]:
            if item["rubro"] == "mano_obra":
                total += item["subtotal"]
    return total


def calcular_indicador_costo_kg(costo_estructura, peso_total_kg, tipo_cambio_referencia):
    """Sección 3.5: costo por kg = costo_estructura (materiales + mano de obra
    taller + mano de obra obra + pintura) / peso total de materiales (kg);
    costo por kg en USD = costo por kg / tipo de cambio de referencia."""
    costo_por_kg = (costo_estructura / peso_total_kg) if peso_total_kg else 0.0
    tipo_cambio = float(tipo_cambio_referencia or 0)
    costo_por_kg_usd = (costo_por_kg / tipo_cambio) if tipo_cambio else 0.0
    return {
        "peso_total_kg": peso_total_kg,
        "costo_estructura": costo_estructura,
        "costo_por_kg": costo_por_kg,
        "costo_por_kg_usd": costo_por_kg_usd,
    }


def calcular_indicador_mano_obra_consumibles(items_resueltos_fabricacion, tipo_cambio_referencia):
    """Indicador local del bloque combinado "Mano de obra + Consumibles" de
    FABRICACION:
    - usd_por_kg: (materiales SIN 'no_listado' + ingeniería + bulones + pintura +
      mano de obra + consumibles), convertido a USD, dividido el peso (kg) de
      esos materiales.
    - kg_por_hh: peso (kg) de esos materiales, dividido las horas-hombre
      (operarios * días * 10) de la línea de mano_obra de la sección.
    """
    costo_estructura = 0.0
    peso_materiales = 0.0
    operarios = 0.0
    dias = 0.0
    for item in items_resueltos_fabricacion:
        rubro = item.get("rubro")
        datos = item.get("datos") or {}
        if rubro == "materiales" and item.get("tipo_item") in ("perfil", "porcentaje"):
            costo_estructura += item.get("subtotal") or 0.0
            peso_materiales += item.get("peso") or 0.0
        elif rubro in ("ingenieria", "bulones", "pintura", "mano_obra", "consumibles"):
            costo_estructura += item.get("subtotal") or 0.0
        if rubro == "mano_obra":
            operarios = float(datos.get("operarios") or 0)
            dias = float(datos.get("dias") or 0)

    tipo_cambio = float(tipo_cambio_referencia or 0)
    costo_estructura_usd = (costo_estructura / tipo_cambio) if tipo_cambio else 0.0
    usd_por_kg = (costo_estructura_usd / peso_materiales) if peso_materiales else 0.0

    hh = operarios * dias * 10
    kg_por_hh = (peso_materiales / hh) if hh else 0.0

    return {"usd_por_kg": usd_por_kg, "kg_por_hh": kg_por_hh}


def calcular_kg_por_hh_presupuesto(tareas_resultados):
    """Indicador global KG/HH: peso total de materiales de FABRICACION (todas
    las tareas) dividido el total de horas-hombre (operarios * días * 10) de
    las líneas de mano_obra de FABRICACION de todas las tareas."""
    peso_materiales = 0.0
    hh_total = 0.0
    for tarea in tareas_resultados:
        for item in tarea["fabricacion"]["items"]:
            rubro = item.get("rubro")
            datos = item.get("datos") or {}
            if rubro == "materiales" and item.get("tipo_item") in ("perfil", "porcentaje"):
                peso_materiales += item.get("peso") or 0.0
            elif rubro == "mano_obra":
                operarios = float(datos.get("operarios") or 0)
                dias = float(datos.get("dias") or 0)
                hh_total += operarios * dias * 10
    return (peso_materiales / hh_total) if hh_total else 0.0


def calcular_m2_total_presupuesto(tareas_resultados):
    """Suma el m2 de todas las líneas materiales/perfil y materiales/chapa de
    FABRICACION de todas las tareas (insumo de los indicadores m2/día y
    USD/m2)."""
    return sum(
        item.get("m2") or 0.0
        for tarea in tareas_resultados
        for item in tarea["fabricacion"]["items"]
        if "m2" in item
    )


def calcular_m2_por_dia_montaje(tareas_resultados):
    """Indicador m2/día (montaje): m2 total de materiales de FABRICACION (todas
    las tareas) dividido el total de días de mano_obra de MONTAJE (todas las
    tareas)."""
    m2_total = calcular_m2_total_presupuesto(tareas_resultados)
    dias_total = 0.0
    for tarea in tareas_resultados:
        for item in tarea["montaje"]["items"]:
            if item.get("rubro") == "mano_obra":
                dias_total += float((item.get("datos") or {}).get("dias") or 0)
    return (m2_total / dias_total) if dias_total else 0.0


def calcular_usd_por_m2_total(precio_venta_presupuesto, m2_total, tipo_cambio_referencia):
    """Indicador USD/m2 (total): precio de venta total del presupuesto,
    convertido a USD, dividido el m2 total de materiales cargados (todas las
    tareas, perfil + chapa)."""
    tipo_cambio = float(tipo_cambio_referencia or 0)
    precio_venta_usd = (precio_venta_presupuesto / tipo_cambio) if tipo_cambio else 0.0
    return (precio_venta_usd / m2_total) if m2_total else 0.0


def calcular_indicador_kg_tarea(resultado_tarea, tipo_cambio_referencia):
    """Indicadores KG/HH y USD/kg de UNA sola tarea (modo perfil, ej.
    Estructura Metálica): kg_por_hh = kg totales de la tarea / (operarios *
    días * 10 de mano de obra de FABRICACION); costo_por_kg_usd = costo de
    estructura de la tarea (USD) / kg totales de la tarea."""
    peso_total_kg = calcular_peso_total_kg([resultado_tarea])
    costo_estructura = calcular_costo_estructura([resultado_tarea])
    indicador = calcular_indicador_costo_kg(costo_estructura, peso_total_kg, tipo_cambio_referencia)
    indicador["kg_por_hh"] = calcular_kg_por_hh_presupuesto([resultado_tarea])
    return indicador


def calcular_indicador_m2_tarea(resultado_tarea, tipo_cambio_referencia):
    """Indicadores m2/día y USD/m2 de UNA sola tarea (modo chapa/grating, ej.
    Chapeado): m2_por_dia_montaje = m2 totales de la tarea / días de mano de
    obra de MONTAJE de la tarea; usd_por_m2_total = costo directo de
    FABRICACION de la tarea (USD) / m2 totales de la tarea (NO usa el precio
    de venta, a diferencia del indicador global del presupuesto)."""
    m2_total = calcular_m2_total_presupuesto([resultado_tarea])
    m2_por_dia_montaje = calcular_m2_por_dia_montaje([resultado_tarea])
    costo_directo_fab = resultado_tarea["fabricacion"]["resumen"]["costo_directo"]
    tipo_cambio = float(tipo_cambio_referencia or 0)
    costo_directo_usd = (costo_directo_fab / tipo_cambio) if tipo_cambio else 0.0
    usd_por_m2_total = (costo_directo_usd / m2_total) if m2_total else 0.0
    return {"m2_total": m2_total, "m2_por_dia_montaje": m2_por_dia_montaje, "usd_por_m2_total": usd_por_m2_total}


def calcular_resumen_recursos(tareas_resultados):
    """Sección 5.3: resumen de recursos a obra, cruzando todas las tareas.
    Devuelve mano de obra (operarios-día), equipos necesarios (agrupados por
    equipo_id) y materiales a comprar (agrupados por perfil_id) — listo para
    exportar y, a futuro, precargar Compras al adjudicar."""
    operarios_dia_taller = 0.0
    operarios_dia_montaje = 0.0
    equipos_por_id = {}
    materiales_por_perfil = {}

    for tarea in tareas_resultados:
        for item in tarea["fabricacion"]["items"]:
            datos = item.get("datos") or {}
            if item["rubro"] == "mano_obra":
                operarios_dia_taller += float(datos.get("operarios") or 0) * float(datos.get("dias") or 0)
            if item["rubro"] == "materiales" and item.get("tipo_item") == "perfil":
                perfil_id = datos.get("perfil_id")
                entrada = materiales_por_perfil.setdefault(
                    perfil_id, {"perfil_id": perfil_id, "cantidad": 0.0, "peso_kg": 0.0}
                )
                entrada["cantidad"] += float(datos.get("cantidad") or 0)
                entrada["peso_kg"] += float(item.get("peso") or 0)

        for item in tarea["montaje"]["items"]:
            datos = item.get("datos") or {}
            if item["rubro"] == "mano_obra":
                operarios_dia_montaje += float(datos.get("operarios") or 0) * float(datos.get("dias") or 0)
            if item["rubro"] == "equipo":
                equipo_id = datos.get("equipo_id")
                entrada = equipos_por_id.setdefault(equipo_id, {"equipo_id": equipo_id, "dias": 0.0})
                entrada["dias"] += float(datos.get("dias") or 0)

    return {
        "mano_obra": {
            "operarios_dia_taller": operarios_dia_taller,
            "operarios_dia_montaje": operarios_dia_montaje,
        },
        "equipos": list(equipos_por_id.values()),
        "materiales": list(materiales_por_perfil.values()),
    }
