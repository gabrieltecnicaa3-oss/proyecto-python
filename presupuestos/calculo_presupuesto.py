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

def calcular_materiales_perfil(datos, perfil):
    """datos: {perfil_id, cantidad, largo_mm, precio_unitario_kg, cant_barras?}
    perfil: {kg_m, m2_m} — ya resuelto desde el catálogo (articulos_sum), no se
    consulta la DB acá.
    """
    cantidad = float(datos.get("cantidad") or 0)
    largo_mm = float(datos.get("largo_mm") or 0)
    precio_unitario_kg = float(datos.get("precio_unitario_kg") or 0)
    kg_m = float((perfil or {}).get("kg_m") or 0)
    m2_m = float((perfil or {}).get("m2_m") or 0)

    largo_m = largo_mm / 1000.0
    peso = cantidad * largo_m * kg_m
    m2 = cantidad * largo_m * m2_m
    subtotal = peso * precio_unitario_kg
    return {"peso": peso, "m2": m2, "subtotal": subtotal}


def calcular_placas(datos, peso_materiales_perfiles):
    """datos: {descripcion, porcentaje, precio_unitario_kg}. "Placas": peso =
    % del peso acumulado de las líneas tipo `perfil` cargadas ANTES de esta
    línea (orden de carga importa); subtotal = peso * precio_unitario_kg."""
    porcentaje = float(datos.get("porcentaje") or 0)
    precio_unitario_kg = float(datos.get("precio_unitario_kg") or 0)
    peso = porcentaje * peso_materiales_perfiles
    subtotal = peso * precio_unitario_kg
    return {"peso": peso, "subtotal": subtotal}


def calcular_materiales_no_listado(datos):
    """datos: {descripcion, cantidad, unidad, precio_unitario}. Carga manual de
    materiales que no están en el catálogo de perfiles (sin peso/m2 asociado)."""
    cantidad = float(datos.get("cantidad") or 0)
    precio_unitario = float(datos.get("precio_unitario") or 0)
    return cantidad * precio_unitario


def calcular_bulones(datos, subtotal_materiales):
    """datos: {porcentaje}. Se aplica sobre el subtotal de materiales acumulado."""
    porcentaje = float(datos.get("porcentaje") or 0)
    return porcentaje * subtotal_materiales


def calcular_pintura(datos, m2_total_materiales):
    """datos: {esquema_id, precio_unitario_m2}. Se aplica sobre el m2 acumulado
    de las líneas materiales/perfil de la misma sección."""
    precio_unitario_m2 = float(datos.get("precio_unitario_m2") or 0)
    return precio_unitario_m2 * m2_total_materiales


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


def calcular_seccion_fabricacion(items, perfiles_por_id=None):
    """items: lista ORDENADA de dicts {rubro, tipo_item (solo materiales), datos}.
    perfiles_por_id: {perfil_id: {kg_m, m2_m}}, ya resuelto (sin DB acá).

    El orden de `items` importa para `bulones` (usa el subtotal de materiales
    acumulado hasta ESE punto de la lista). "Placas" es la excepción: se
    calcula sobre el peso TOTAL de todas las líneas materiales/perfil de la
    sección (sin importar su posición), porque conceptualmente va al final:
    a más materiales cargados, más kg de placas corresponden.

    Devuelve (items_resueltos, resumen). No muta la lista `items` original.
    """
    perfiles_por_id = perfiles_por_id or {}

    # Pre-pasada: peso total de materiales/perfil (para "Placas", que depende
    # del total de la sección y no de lo acumulado hasta su posición).
    peso_total_materiales_perfiles = 0.0
    for item in items:
        if item.get("rubro") == "materiales" and item.get("tipo_item") == "perfil":
            perfil = perfiles_por_id.get((item.get("datos") or {}).get("perfil_id"), {})
            peso_total_materiales_perfiles += calcular_materiales_perfil(item.get("datos") or {}, perfil)["peso"]

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
            calc = calcular_materiales_perfil(datos, perfil)
            resultado["subtotal"] = calc["subtotal"]
            resultado["peso"] = calc["peso"]
            resultado["m2"] = calc["m2"]
            acumulado_materiales_perfiles += calc["subtotal"]
            acumulado_m2_materiales += calc["m2"]
            acumulado_materiales_total += calc["subtotal"]
        elif rubro == "materiales" and tipo_item == "porcentaje":
            calc = calcular_placas(datos, peso_total_materiales_perfiles)
            resultado["subtotal"] = calc["subtotal"]
            resultado["peso"] = calc["peso"]
            acumulado_materiales_total += calc["subtotal"]
        elif rubro == "materiales" and tipo_item == "no_listado":
            subtotal = calcular_materiales_no_listado(datos)
            resultado["subtotal"] = subtotal
            acumulado_materiales_total += subtotal
        elif rubro == "bulones":
            resultado["subtotal"] = calcular_bulones(datos, acumulado_materiales_total)
        elif rubro == "pintura":
            resultado["subtotal"] = calcular_pintura(datos, acumulado_m2_materiales)
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

def calcular_tarea(fabricacion_items, fabricacion_pcts, montaje_items, montaje_pcts, perfiles_por_id=None):
    """fabricacion_pcts / montaje_pcts: {"gg_pct", "beneficio_pct", "imp_pct"}
    (uno por sección, tomados de tarea_secciones — cada sección tiene sus
    propios %, ver sección 3 de la especificación)."""
    items_fab, resumen_fab = calcular_seccion_fabricacion(fabricacion_items, perfiles_por_id)
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
            if item["rubro"] in ("mano_obra", "consumibles"):
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
            if item["rubro"] in ("mano_obra", "consumibles"):
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
