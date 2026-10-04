"""Tests del motor de cálculo de Presupuestos (sin pytest, estilo del repo:
correr con `python -m presupuestos.test_calculo_presupuesto` desde la raíz).

Incluye el caso real de la sección 1 de ESPECIFICACION_MODULO_PRESUPUESTOS_V1.md
(tarea "Cantoneras y rejillas") como test de regresión del motor completo.
"""
import math

from presupuestos.calculo_presupuesto import (
    calcular_materiales_perfil,
    calcular_placas,
    calcular_materiales_no_listado,
    calcular_bulones,
    calcular_pintura,
    calcular_fletes,
    calcular_subcontratos,
    calcular_mano_obra,
    calcular_consumibles,
    calcular_ingenieria,
    calcular_equipo,
    calcular_ingeniero,
    calcular_tecnico_hys,
    calcular_seccion_fabricacion,
    calcular_seccion_montaje,
    calcular_cascada,
    calcular_tarea,
    calcular_presupuesto,
    calcular_resumen_categorias,
    calcular_peso_total_kg,
    calcular_indicador_costo_kg,
    calcular_costo_estructura,
    calcular_indicador_mano_obra_consumibles,
    calcular_resumen_recursos,
)


def _close(a, b, tol=1e-6):
    return math.isclose(a, b, abs_tol=tol)


def test_materiales_perfil():
    perfil = {"kg_m": 5.0, "m2_m": 0.3}
    datos = {"cantidad": 10, "largo_mm": 6000, "precio_unitario_kg": 2.0}
    r = calcular_materiales_perfil(datos, perfil)
    assert _close(r["peso"], 300.0), r
    assert _close(r["m2"], 18.0), r
    assert _close(r["subtotal"], 600.0), r


def test_calcular_placas():
    r = calcular_placas({"porcentaje": 0.05, "precio_unitario_kg": 2.0}, 300.0)
    assert _close(r["peso"], 15.0), r
    assert _close(r["subtotal"], 30.0), r


def test_calcular_materiales_no_listado():
    datos = {"descripcion": "Bulones especiales", "cantidad": 20, "unidad": "un", "precio_unitario": 15.0}
    assert _close(calcular_materiales_no_listado(datos), 300.0)


def test_calcular_indicador_mano_obra_consumibles():
    items_resueltos = [
        {"rubro": "materiales", "tipo_item": "perfil", "subtotal": 600.0, "peso": 300.0},
        {"rubro": "materiales", "tipo_item": "no_listado", "subtotal": 9999.0}, # no debe sumar
        {"rubro": "ingenieria", "subtotal": 100.0},
        {"rubro": "bulones", "subtotal": 50.0},
        {"rubro": "pintura", "subtotal": 250.0},
        {"rubro": "mano_obra", "datos": {"operarios": 2, "dias": 5}, "subtotal": 1000.0},
        {"rubro": "consumibles", "subtotal": 200.0},
    ]
    r = calcular_indicador_mano_obra_consumibles(items_resueltos, tipo_cambio_referencia=1000)
    # costo_estructura = 600 + 100 + 50 + 250 + 1000 + 200 = 2200; usd = 2200/1000 = 2.2; usd/kg = 2.2/300
    assert _close(r["usd_por_kg"], 2.2 / 300.0), r
    # hh = 2*5*10 = 100; kg/hh = 300/100 = 3.0
    assert _close(r["kg_por_hh"], 3.0), r


def test_bulones():
    assert _close(calcular_bulones({"porcentaje": 0.02}, 630.0), 12.6)


def test_pintura():
    assert _close(calcular_pintura({"precio_unitario_m2": 20.0}, 18.0), 360.0)


def test_fletes_y_subcontratos():
    datos = {"cantidad": 3, "precio_unitario": 100.0}
    assert _close(calcular_fletes(datos), 300.0)
    assert _close(calcular_subcontratos(datos), 300.0)


def test_mano_obra_y_consumibles():
    datos = {"operarios": 2, "dias": 5, "tarifa_dh": 1000.0}
    assert _close(calcular_mano_obra(datos), 10000.0)
    assert _close(calcular_consumibles(datos), 10000.0)


def test_ingenieria():
    r = calcular_ingenieria({"monto": 5000.0}, 1000.0)
    assert _close(r["subtotal"], 5000.0)
    assert _close(r["porcentaje_informativo"], 5.0)


def test_equipo_ingeniero_tecnico_hys():
    assert _close(calcular_equipo({"dias": 4, "tarifa_dia": 2000.0}), 8000.0)
    datos = {"tarifa_dia": 1500.0, "dias": 10}
    assert _close(calcular_ingeniero(datos), 15000.0)
    assert _close(calcular_tecnico_hys(datos), 15000.0)


def test_seccion_fabricacion_orden_de_acumuladores():
    # bulones y placas deben usar SOLO lo acumulado hasta su
    # posición en la lista, no el total final de la sección.
    items = [
        {"rubro": "materiales", "tipo_item": "perfil",
         "datos": {"perfil_id": 1, "cantidad": 10, "largo_mm": 6000, "precio_unitario_kg": 2.0}},
        {"rubro": "materiales", "tipo_item": "porcentaje",
         "datos": {"descripcion": "recortes", "porcentaje": 0.05, "precio_unitario_kg": 2.0}},
        {"rubro": "bulones", "datos": {"porcentaje": 0.02}},
        {"rubro": "pintura", "datos": {"esquema_id": 1, "precio_unitario_m2": 20.0}},
    ]
    perfiles = {1: {"kg_m": 5.0, "m2_m": 0.3}}
    items_resueltos, resumen = calcular_seccion_fabricacion(items, perfiles)

    assert _close(items_resueltos[0]["subtotal"], 600.0)          # perfil
    assert _close(items_resueltos[1]["subtotal"], 30.0)           # 0.05 * 300kg * 2.0
    assert _close(items_resueltos[2]["subtotal"], 12.6)           # 0.02 * (600+30)
    assert _close(items_resueltos[3]["subtotal"], 360.0)          # 20 * 18 m2
    assert _close(resumen["costo_directo"], 600 + 30 + 12.6 + 360)
    assert _close(resumen["m2_total_materiales"], 18.0)


def test_seccion_montaje_simple():
    items = [
        {"rubro": "mano_obra", "datos": {"operarios": 2, "dias": 5, "tarifa_dh": 1000.0}},
        {"rubro": "equipo", "datos": {"equipo_id": 1, "dias": 4, "tarifa_dia": 2000.0}},
    ]
    items_resueltos, resumen = calcular_seccion_montaje(items)
    assert _close(resumen["costo_directo"], 10000.0 + 8000.0)


def test_cascada_numeros_redondos():
    r = calcular_cascada(1000.0, gg_pct=0.10, beneficio_pct=0.20, imp_pct=0.05)
    assert _close(r["gg"], 100.0)
    assert _close(r["subtotal_1"], 1100.0)
    assert _close(r["beneficio"], 220.0)
    assert _close(r["subtotal_2"], 1320.0)
    assert _close(r["impuestos"], 66.0)
    assert _close(r["precio_venta"], 1386.0)


def test_caso_real_cantoneras_y_rejillas():
    """Sección 1 de la especificación. Se usan mano_obra como único item para
    fijar el costo directo exacto (792.672 y 4.279.818) y validar la cascada
    con los % reales (GG 7%, Beneficio 12%/25%, Impuestos 5%/3%).

    NOTA sobre la tolerancia: el documento fuente arrastra ~1 peso de ruido de
    redondeo propio (verificable a mano): 949.938 + 47.497 = 997.435, no
    997.434 como dice el texto; 4.579.405 + 1.144.851 = 5.724.256, no
    5.724.257. Por eso se compara con tolerancia de 1 peso por sección. El
    TOTAL DE LA TAREA sí cierra exacto (los errores de redondeo de FAB y MON
    se cancelan), y es la verificación más fuerte de que la cascada está bien
    implementada.
    """
    fabricacion_items = [
        {"rubro": "mano_obra", "datos": {"operarios": 1, "dias": 1, "tarifa_dh": 792672.0}},
    ]
    fabricacion_pcts = {"gg_pct": 0.07, "beneficio_pct": 0.12, "imp_pct": 0.05}

    montaje_items = [
        {"rubro": "mano_obra", "datos": {"operarios": 1, "dias": 1, "tarifa_dh": 4279818.0}},
    ]
    montaje_pcts = {"gg_pct": 0.07, "beneficio_pct": 0.25, "imp_pct": 0.03}

    resultado = calcular_tarea(fabricacion_items, fabricacion_pcts, montaje_items, montaje_pcts)

    costo_directo_fab = resultado["fabricacion"]["resumen"]["costo_directo"]
    costo_directo_mon = resultado["montaje"]["resumen"]["costo_directo"]
    assert _close(costo_directo_fab, 792672.0)
    assert _close(costo_directo_mon, 4279818.0)

    precio_venta_fab = resultado["fabricacion"]["cascada"]["precio_venta"]
    precio_venta_mon = resultado["montaje"]["cascada"]["precio_venta"]

    assert abs(round(precio_venta_fab) - 997434) <= 1, precio_venta_fab
    assert abs(round(precio_venta_mon) - 5895985) <= 1, precio_venta_mon

    # Verificación fuerte: el total de la tarea cierra EXACTO contra el Excel.
    assert round(resultado["precio_venta_tarea"]) == 6893419, resultado["precio_venta_tarea"]

    presupuesto = calcular_presupuesto([resultado])
    assert round(presupuesto["precio_venta_presupuesto"]) == 6893419


def test_resumen_categorias_cruza_tareas_y_cierra_el_total():
    fabricacion_items = [
        {"rubro": "mano_obra", "datos": {"operarios": 2, "dias": 5, "tarifa_dh": 1000}},
        {"rubro": "fletes", "datos": {"descripcion": "Flete a obra", "cantidad": 1, "precio_unitario": 500}},
    ]
    fabricacion_pcts = {"gg_pct": 0.10, "beneficio_pct": 0.10, "imp_pct": 0.05}
    montaje_items = [
        {"rubro": "equipo", "datos": {"equipo_id": 1, "dias": 3, "tarifa_dia": 200}},
    ]
    montaje_pcts = {"gg_pct": 0.05, "beneficio_pct": 0.08, "imp_pct": 0.03}

    resultado = calcular_tarea(fabricacion_items, fabricacion_pcts, montaje_items, montaje_pcts)
    resumen = calcular_resumen_categorias([resultado])

    por_nombre = {c["nombre"]: c["monto"] for c in resumen["categorias"]}
    assert _close(por_nombre["Mano de obra taller"], 10000.0)
    assert _close(por_nombre["Fletes"], 500.0)
    assert _close(por_nombre["Equipos"], 600.0)
    assert _close(por_nombre["GG Fab"], resultado["fabricacion"]["cascada"]["gg"])
    assert _close(por_nombre["GG Mon"], resultado["montaje"]["cascada"]["gg"])

    # El total del reporte por categorías debe cerrar exacto contra la cascada.
    assert _close(resumen["total"], resultado["precio_venta_tarea"])
    suma_porcentajes = sum(c["porcentaje"] for c in resumen["categorias"])
    assert _close(suma_porcentajes, 1.0)


def test_indicador_costo_kg():
    perfil = {"kg_m": 5.0, "m2_m": 0.3}
    fabricacion_items = [
        {"rubro": "materiales", "tipo_item": "perfil", "datos": {
            "perfil_id": 1, "cantidad": 10, "largo_mm": 6000, "precio_unitario_kg": 2.0,
        }},
    ]
    fabricacion_pcts = {"gg_pct": 0, "beneficio_pct": 0, "imp_pct": 0}
    montaje_items = []
    montaje_pcts = {"gg_pct": 0, "beneficio_pct": 0, "imp_pct": 0}

    resultado = calcular_tarea(
        fabricacion_items, fabricacion_pcts, montaje_items, montaje_pcts, perfiles_por_id={1: perfil}
    )
    peso_total = calcular_peso_total_kg([resultado])
    assert _close(peso_total, 300.0)

    costo_estructura = calcular_costo_estructura([resultado])
    assert _close(costo_estructura, 600.0)

    indicador = calcular_indicador_costo_kg(costo_estructura, peso_total, tipo_cambio_referencia=1000)
    assert _close(indicador["costo_por_kg"], 600.0 / 300.0)
    assert _close(indicador["costo_por_kg_usd"], indicador["costo_por_kg"] / 1000)

    # Sin peso ni tipo de cambio, no debe romper (división por cero evitada).
    indicador_vacio = calcular_indicador_costo_kg(1000.0, 0.0, None)
    assert indicador_vacio["costo_por_kg"] == 0.0
    assert indicador_vacio["costo_por_kg_usd"] == 0.0


def test_resumen_recursos_agrupa_por_perfil_y_equipo():
    perfil = {"kg_m": 5.0, "m2_m": 0.3}
    fabricacion_items = [
        {"rubro": "materiales", "tipo_item": "perfil", "datos": {
            "perfil_id": 1, "cantidad": 10, "largo_mm": 6000, "precio_unitario_kg": 2.0,
        }},
        {"rubro": "mano_obra", "datos": {"operarios": 2, "dias": 5, "tarifa_dh": 1000}},
    ]
    fabricacion_pcts = {"gg_pct": 0, "beneficio_pct": 0, "imp_pct": 0}
    montaje_items = [
        {"rubro": "equipo", "datos": {"equipo_id": 7, "dias": 3, "tarifa_dia": 200}},
    ]
    montaje_pcts = {"gg_pct": 0, "beneficio_pct": 0, "imp_pct": 0}

    resultado = calcular_tarea(
        fabricacion_items, fabricacion_pcts, montaje_items, montaje_pcts, perfiles_por_id={1: perfil}
    )
    # Dos tareas iguales para verificar que se agrupa (suma) por id, no que se pisa.
    recursos = calcular_resumen_recursos([resultado, resultado])

    assert _close(recursos["mano_obra"]["operarios_dia_taller"], 2 * (2 * 5))
    assert _close(recursos["mano_obra"]["operarios_dia_montaje"], 0.0)
    assert len(recursos["equipos"]) == 1
    assert _close(recursos["equipos"][0]["dias"], 2 * 3)
    assert len(recursos["materiales"]) == 1
    assert _close(recursos["materiales"][0]["cantidad"], 2 * 10)
    assert _close(recursos["materiales"][0]["peso_kg"], 2 * 300.0)


def _run_all():
    tests = [obj for name, obj in globals().items() if name.startswith("test_") and callable(obj)]
    ok = 0
    for t in tests:
        t()
        ok += 1
        print(f"[OK] {t.__name__}")
    print(f"\n{ok}/{len(tests)} tests OK")


if __name__ == "__main__":
    _run_all()
