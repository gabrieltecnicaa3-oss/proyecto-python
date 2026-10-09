"""Tests del volcado del previsto por OT (sin pytest). Correr con
`python -m presupuestos.test_volcado_previsto` desde la raíz."""
from decimal import Decimal
import sqlite3

from presupuestos.calculo_presupuesto import calcular_tarea
from presupuestos.volcado_previsto import (
    CAMPOS_ECONOMICOS,
    armar_tarea_para_volcado,
    calcular_volcado_previsto,
    detectar_ediciones_manuales,
    verificar_cuadre_con_presupuesto,
)

D = Decimal


def _tarea_desde_resultado(resultado, tarea_id, nombre, reparto):
    return armar_tarea_para_volcado(resultado, tarea_id, nombre, reparto)


def _cantoneras(reparto, tarea_id=1):
    """Tarea real de la especificación: Fabricación con costo directo $792.672."""
    resultado = calcular_tarea(
        [{"rubro": "mano_obra", "datos": {"operarios": 1, "dias": 1, "tarifa_dh": 792672.0}}],
        {"gg_pct": 0.07, "beneficio_pct": 0.12, "imp_pct": 0.05},
        [{"rubro": "mano_obra", "datos": {"operarios": 1, "dias": 1, "tarifa_dh": 4279818.0}}],
        {"gg_pct": 0.07, "beneficio_pct": 0.25, "imp_pct": 0.03},
    )
    return _tarea_desde_resultado(resultado, tarea_id, "Cantoneras y rejillas", reparto)


# Precio de venta Fabricación de Cantoneras con componentes a centavos:
# 792672.00 + 55487.04 + 101779.08 + 47496.91 (el Excel fuente dice 997.434, con ~1 peso de ruido).
PV_CANTONERAS = D("997435.03")


def _total_ot(campos):
    return sum(campos.values(), D("0.00"))


def _suma_por_campo(por_ot, campo):
    return sum((campos[campo] for campos in por_ot.values()), D("0.00"))


def _assert_cuadre_exacto(r, esperado_total):
    cuadre = r["cuadre"]
    assert cuadre["total_precio_venta"] == esperado_total, cuadre
    assert cuadre["total_asignado"] + cuadre["total_sin_ot"] == esperado_total, cuadre
    assert cuadre["diferencia"] == 0 and cuadre["cuadra"] is True, cuadre


def test_uno_a_uno_al_cien_por_ciento_y_nombres_del_modulo_economico():
    r = calcular_volcado_previsto([_cantoneras([{"ot_id": 10, "porcentaje": 100}])])

    assert list(r["por_ot"]) == [10] and r["sin_ot"] == []
    ot = r["por_ot"][10]
    assert set(ot) == set(CAMPOS_ECONOMICOS)
    assert ot["mo_previsto"] == D("792672.00")
    assert ot["gastos_gen_previsto"] == D("55487.04")
    assert ot["beneficio_previsto"] == D("101779.08")
    assert ot["impuestos_previsto"] == D("47496.91")
    assert ot["mat_previsto"] == D("0.00")
    assert _total_ot(ot) == PV_CANTONERAS
    _assert_cuadre_exacto(r, PV_CANTONERAS)


def test_montaje_no_se_vuelca():
    # El montaje de la tarea (costo 4.279.818) no entra: el total es solo Fabricación.
    r = calcular_volcado_previsto([_cantoneras([{"ot_id": 10, "porcentaje": 100}])])
    assert r["cuadre"]["total_precio_venta"] == PV_CANTONERAS
    assert r["por_ot"][10]["mo_previsto"] == D("792672.00")


def test_reparto_50_30_20_entre_tres_ot():
    reparto = [{"ot_id": 1, "porcentaje": 50}, {"ot_id": 2, "porcentaje": 30}, {"ot_id": 3, "porcentaje": 20}]
    r = calcular_volcado_previsto([_cantoneras(reparto)])

    assert r["por_ot"][1]["mo_previsto"] == D("396336.00")
    assert r["por_ot"][2]["mo_previsto"] == D("237801.60")
    assert r["por_ot"][3]["mo_previsto"] == D("158534.40")
    assert r["por_ot"][1]["gastos_gen_previsto"] == D("27743.52")
    assert r["por_ot"][2]["gastos_gen_previsto"] == D("16646.11")
    assert r["por_ot"][3]["gastos_gen_previsto"] == D("11097.41")
    # Cada rubro suma exacto a lo que tenía la tarea.
    assert _suma_por_campo(r["por_ot"], "gastos_gen_previsto") == D("55487.04")
    assert _suma_por_campo(r["por_ot"], "impuestos_previsto") == D("47496.91")
    _assert_cuadre_exacto(r, PV_CANTONERAS)


def test_reparto_un_tercio_cada_una_el_redondeo_cuadra():
    tercio = 100 / 3  # 33.333333333333336 como float
    reparto = [{"ot_id": 1, "porcentaje": tercio}, {"ot_id": 2, "porcentaje": tercio}, {"ot_id": 3, "porcentaje": tercio}]
    r = calcular_volcado_previsto([_cantoneras(reparto)])

    # Impuestos 47496.91 / 3 = 15832.3033...: sobra 1 centavo, va a la primera OT (empate de porcentaje).
    assert r["por_ot"][1]["impuestos_previsto"] == D("15832.31")
    assert r["por_ot"][2]["impuestos_previsto"] == D("15832.30")
    assert r["por_ot"][3]["impuestos_previsto"] == D("15832.30")
    for campo in CAMPOS_ECONOMICOS:
        esperado = {"mo_previsto": D("792672.00"), "gastos_gen_previsto": D("55487.04"),
                    "beneficio_previsto": D("101779.08"), "impuestos_previsto": D("47496.91")}.get(campo, D("0.00"))
        assert _suma_por_campo(r["por_ot"], campo) == esperado, campo
    _assert_cuadre_exacto(r, PV_CANTONERAS)


def test_la_diferencia_de_redondeo_va_a_la_ot_de_mayor_porcentaje():
    reparto = [{"ot_id": 1, "porcentaje": 20}, {"ot_id": 2, "porcentaje": 50}, {"ot_id": 3, "porcentaje": 30}]
    r = calcular_volcado_previsto([_cantoneras(reparto)])
    # GG: 20% -> 11097.408 (11097.41), 50% -> 27743.52, 30% -> 16646.112 (16646.11): suma exacta 55487.04.
    assert r["por_ot"][1]["gastos_gen_previsto"] == D("11097.41")
    assert _suma_por_campo(r["por_ot"], "gastos_gen_previsto") == D("55487.04")
    _assert_cuadre_exacto(r, PV_CANTONERAS)


def test_dos_tareas_a_la_misma_ot_se_suman_y_mapeo_materiales_bulones():
    otra = calcular_tarea(
        [
            {"rubro": "materiales", "tipo_item": "no_listado",
             "datos": {"descripcion": "Chapa especial", "cantidad": 10, "precio_unitario": 1000.0}},
            {"rubro": "bulones", "datos": {"porcentaje": 0.05}},
            {"rubro": "fletes", "datos": {"descripcion": "Flete", "cantidad": 1, "precio_unitario": 333.33}},
        ],
        {"gg_pct": 0.05, "beneficio_pct": 0.05, "imp_pct": 0.03},
        [], {"gg_pct": 0, "beneficio_pct": 0, "imp_pct": 0},
    )
    t_otra = _tarea_desde_resultado(otra, 2, "Otra tarea", [
        {"ot_id": 10, "porcentaje": 60}, {"ot_id": 11, "porcentaje": 40},
    ])
    t_cant = _cantoneras([{"ot_id": 10, "porcentaje": 100}])
    r = calcular_volcado_previsto([t_cant, t_otra])

    # Materiales + bulones suman en mat_previsto: 10000 + 5% = 10500 -> 60/40.
    assert r["por_ot"][10]["mat_previsto"] == D("6300.00")
    assert r["por_ot"][11]["mat_previsto"] == D("4200.00")
    assert r["por_ot"][10]["fletes_previsto"] == D("200.00")  # 333.33 * 60% = 199.998
    assert r["por_ot"][11]["fletes_previsto"] == D("133.33")
    # La OT 10 acumula las dos tareas.
    assert r["por_ot"][10]["mo_previsto"] == D("792672.00")
    total_tareas = _total_ot(r["por_ot"][10]) + _total_ot(r["por_ot"][11])
    assert total_tareas == r["cuadre"]["total_precio_venta"]
    _assert_cuadre_exacto(r, r["cuadre"]["total_precio_venta"])


def test_tarea_sin_ot_va_aparte_y_el_cuadre_incluye_ambas():
    asignada = _cantoneras([{"ot_id": 10, "porcentaje": 100}], tarea_id=1)
    libre = _cantoneras([], tarea_id=2)
    r = calcular_volcado_previsto([asignada, libre])

    assert list(r["por_ot"]) == [10]
    assert len(r["sin_ot"]) == 1
    fila = r["sin_ot"][0]
    assert fila["tarea_id"] == 2 and fila["nombre"] == "Cantoneras y rejillas"
    assert fila["montos"]["mo_previsto"] == D("792672.00")
    assert fila["total"] == PV_CANTONERAS
    assert r["cuadre"]["total_asignado"] == PV_CANTONERAS
    assert r["cuadre"]["total_sin_ot"] == PV_CANTONERAS
    _assert_cuadre_exacto(r, PV_CANTONERAS * 2)


def test_tarea_solo_sin_ot():
    r = calcular_volcado_previsto([_cantoneras([])])
    assert r["por_ot"] == {}
    _assert_cuadre_exacto(r, PV_CANTONERAS)


def test_errores_claros():
    def _falla(tarea, fragmento):
        try:
            calcular_volcado_previsto([tarea])
        except ValueError as exc:
            assert fragmento in str(exc), exc
        else:
            raise AssertionError(f"debía fallar con {fragmento!r}")

    _falla(_cantoneras([{"ot_id": 1, "porcentaje": 50}, {"ot_id": 2, "porcentaje": 49.99}]), "suman 99.99")
    _falla(_cantoneras([{"ot_id": 1, "porcentaje": 33.33}] * 3), "deben sumar 100")
    _falla(_cantoneras([{"ot_id": 1, "porcentaje": 120}, {"ot_id": 2, "porcentaje": -20}]), "entre 0 y 100")
    tarea_montaje = _cantoneras([])
    tarea_montaje["montos"]["equipo"] = 1000.0
    _falla(tarea_montaje, "'equipo'")


def test_cuadre_contra_el_precio_de_venta_del_presupuesto():
    # Precio de venta sin redondear del motor (997435.03104) vs lo asignado (997435.03).
    resultado = calcular_tarea(
        [{"rubro": "mano_obra", "datos": {"operarios": 1, "dias": 1, "tarifa_dh": 792672.0}}],
        {"gg_pct": 0.07, "beneficio_pct": 0.12, "imp_pct": 0.05}, [], {"gg_pct": 0, "beneficio_pct": 0, "imp_pct": 0},
    )
    pv = resultado["fabricacion"]["cascada"]["precio_venta"]
    v = calcular_volcado_previsto([_cantoneras([{"ot_id": 1, "porcentaje": 100}])])
    c = verificar_cuadre_con_presupuesto(v, pv, 1)
    assert c["cuadra"] is True and abs(c["diferencia"]) < D("0.01"), c

    # Una tarea omitida del volcado (aunque el volcado en sí cuadre) hace saltar el control.
    c = verificar_cuadre_con_presupuesto(v, pv + 5000, 1)
    assert c["cuadra"] is False and c["diferencia"] > 4999, c


def test_ediciones_manuales_solo_contra_el_ultimo_volcado():
    ultimo = {10: {c: 100.0 for c in CAMPOS_ECONOMICOS}, 11: {c: 0.0 for c in CAMPOS_ECONOMICOS}}
    actual = {
        10: {**{c: D("100") for c in CAMPOS_ECONOMICOS}, "mo_previsto": D("250.50")},  # editada a mano
        11: {c: D("0") for c in CAMPOS_ECONOMICOS},                                    # igual al volcado
        12: {c: D("999") for c in CAMPOS_ECONOMICOS},                                  # OT nueva, sin volcado previo
    }
    r = detectar_ediciones_manuales(actual, ultimo, [10, 11, 12])
    assert list(r) == [10]
    assert r[10] == [{"campo": "mo_previsto", "actual": D("250.50"), "ultimo_volcado": D("100.0")}]
    # Primer volcado: no hay nada que comparar.
    assert detectar_ediciones_manuales(actual, {}, [10, 11, 12]) == {}
    # Diferencias de fracciones de centavo no son ediciones.
    actual[10]["mo_previsto"] = D("100.004")
    assert detectar_ediciones_manuales(actual, ultimo, [10]) == {}


def test_aplicar_volcado_reemplaza_valores_y_conserva_historial():
    from economico_routes import _ensure_schema
    from presupuestos.models import (
        ensure_tablas_presupuestos, crear_presupuesto, aplicar_volcado_previsto,
        listar_volcados, obtener_ultimo_volcado,
    )
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE ordenes_trabajo (id INTEGER PRIMARY KEY, obra TEXT)")
    ensure_tablas_presupuestos(db)
    _ensure_schema(db)
    pid = crear_presupuesto(db, "Cliente")
    assert obtener_ultimo_volcado(db, pid) is None

    def _valores(base):
        return {c: D(str(base)) for c in CAMPOS_ECONOMICOS}

    # La OT 5 ya tenía fila en económico (carga manual previa); la OT 6 no.
    db.execute("INSERT INTO economico_presupuesto (ot_id, mat_previsto, mo_previsto) VALUES (5, 1, 2)")
    v1 = aplicar_volcado_previsto(db, pid, "ana", "primero", {5: _valores(10), 6: _valores(20)})
    fila = db.execute("SELECT mat_previsto, beneficio_previsto FROM economico_presupuesto WHERE ot_id=5").fetchone()
    assert fila == (10.0, 10.0)
    assert db.execute("SELECT COUNT(*) FROM economico_presupuesto").fetchone()[0] == 2  # sin duplicar la OT 5

    v2 = aplicar_volcado_previsto(db, pid, "ana", None, {5: _valores(30)})
    assert db.execute("SELECT mat_previsto FROM economico_presupuesto WHERE ot_id=5").fetchone()[0] == 30.0
    assert db.execute("SELECT mat_previsto FROM economico_presupuesto WHERE ot_id=6").fetchone()[0] == 20.0  # no tocada

    historial = listar_volcados(db, pid)
    assert [v["id"] for v in historial] == [v2, v1]  # más nuevo primero
    assert historial[1]["lineas"][5]["mat_previsto"] == 10.0  # el historial conserva el valor anterior
    assert historial[1]["nota"] == "primero" and historial[1]["usuario"] == "ana"
    assert len(historial[1]["lineas"][5]) == len(CAMPOS_ECONOMICOS)
    assert obtener_ultimo_volcado(db, pid)["id"] == v2


if __name__ == "__main__":
    pruebas = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    for nombre, funcion in pruebas:
        funcion()
        print(f"[OK] {nombre}")
    print(f"\n{len(pruebas)}/{len(pruebas)} tests OK")
