"""Tests del mapeo Tarea -> OT (sin pytest). Correr con
`python -m presupuestos.test_reparto_ot` desde la raíz."""
import sqlite3

from presupuestos.models import (
    ensure_tablas_presupuestos,
    listar_reparto_tareas,
    reemplazar_reparto_tareas,
    crear_presupuesto,
    crear_tarea,
    eliminar_tarea,
    eliminar_presupuesto,
)
from presupuestos.reparto_ot import validar_reparto_tarea, sugerir_porcentajes_por_kg


def _db():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE ordenes_trabajo (id INTEGER PRIMARY KEY, obra TEXT)")
    ensure_tablas_presupuestos(db)
    return db


def test_validar_ok_y_filas_vacias_se_ignoran():
    filas, error = validar_reparto_tarea([
        {"ot_id": "10", "porcentaje": "33.33"},
        {"ot_id": 11, "porcentaje": 33.33},
        {"ot_id": "12", "porcentaje": "33,34"},
        {"ot_id": "", "porcentaje": ""},
    ])
    assert error is None
    assert filas == [
        {"ot_id": 10, "porcentaje": 33.33},
        {"ot_id": 11, "porcentaje": 33.33},
        {"ot_id": 12, "porcentaje": 33.34},
    ]


def test_validar_sin_filas_es_sin_ot():
    assert validar_reparto_tarea([]) == ([], None)
    assert validar_reparto_tarea([{"ot_id": "", "porcentaje": ""}]) == ([], None)


def test_validar_errores():
    casos = [
        ([{"ot_id": "1", "porcentaje": "50"}, {"ot_id": "2", "porcentaje": "49.99"}], "suman 99.99%"),
        ([{"ot_id": "1", "porcentaje": "60"}, {"ot_id": "2", "porcentaje": "60"}], "suman 120%"),
        ([{"ot_id": "1", "porcentaje": "50"}, {"ot_id": "1", "porcentaje": "50"}], "repetida"),
        ([{"ot_id": "", "porcentaje": "100"}], "Elegí una OT"),
        ([{"ot_id": "1", "porcentaje": ""}], "Falta el porcentaje"),
        ([{"ot_id": "1", "porcentaje": "abc"}], "no es un número"),
        ([{"ot_id": "1", "porcentaje": "0"}], "mayor a 0"),
        ([{"ot_id": "1", "porcentaje": "101"}], "hasta 100"),
        ([{"ot_id": "x", "porcentaje": "100"}], "OT inválida"),
    ]
    for filas, fragmento in casos:
        limpias, error = validar_reparto_tarea(filas)
        assert limpias == [] and error and fragmento in error, (filas, error)


def test_sugerir_proporcional_y_suma_exacta_100():
    r = sugerir_porcentajes_por_kg({1: 500.0, 2: 300.0, 3: 200.0})
    assert r == {1: 50.0, 2: 30.0, 3: 20.0}

    r = sugerir_porcentajes_por_kg({1: 1.0, 2: 1.0, 3: 1.0})
    assert sorted(r.values()) == [33.33, 33.33, 33.34]
    assert round(sum(r.values()), 2) == 100.0

    r = sugerir_porcentajes_por_kg({1: 123.4, 2: 777.7, 3: 9.1, 4: 55.5})
    assert round(sum(r.values()), 2) == 100.0
    assert max(r, key=r.get) == 2


def test_sugerir_sin_kg_devuelve_none():
    assert sugerir_porcentajes_por_kg({1: 100.0, 2: 0.0}) is None
    assert sugerir_porcentajes_por_kg({1: 100.0}) is None
    assert sugerir_porcentajes_por_kg({}) is None


def test_guardar_leer_reemplazar_y_cascada():
    db = _db()
    pid = crear_presupuesto(db, "Cliente")
    t1, t2 = crear_tarea(db, pid, "T1"), crear_tarea(db, pid, "T2")

    reemplazar_reparto_tareas(db, {
        t1: [{"ot_id": 5, "porcentaje": 60.0}, {"ot_id": 6, "porcentaje": 40.0}],
        t2: [{"ot_id": 5, "porcentaje": 100.0}],
    })
    r = listar_reparto_tareas(db, [t1, t2])
    assert r[t1] == [{"ot_id": 5, "porcentaje": 60.0}, {"ot_id": 6, "porcentaje": 40.0}]
    assert r[t2] == [{"ot_id": 5, "porcentaje": 100.0}]  # la OT 5 recibe dos tareas

    reemplazar_reparto_tareas(db, {t1: []})  # vuelve a "sin OT"
    r = listar_reparto_tareas(db, [t1, t2])
    assert t1 not in r and t2 in r

    eliminar_tarea(db, t2)
    assert db.execute("SELECT COUNT(*) FROM tarea_ot_reparto").fetchone()[0] == 0

    t3 = crear_tarea(db, pid, "T3")
    reemplazar_reparto_tareas(db, {t3: [{"ot_id": 5, "porcentaje": 100.0}]})
    db.execute("INSERT INTO volcados_previsto (presupuesto_id, usuario) VALUES (?, 'x')", (pid,))
    db.execute("INSERT INTO volcados_previsto_lineas (volcado_id, ot_id, rubro, monto) VALUES (1, 5, 'mat_previsto', 10)")
    eliminar_presupuesto(db, pid)
    for tabla in ("tarea_ot_reparto", "volcados_previsto", "volcados_previsto_lineas", "tareas", "presupuestos"):
        assert db.execute(f"SELECT COUNT(*) FROM {tabla}").fetchone()[0] == 0, tabla


if __name__ == "__main__":
    pruebas = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    for nombre, funcion in pruebas:
        funcion()
        print(f"[OK] {nombre}")
    print(f"\n{len(pruebas)}/{len(pruebas)} tests OK")
