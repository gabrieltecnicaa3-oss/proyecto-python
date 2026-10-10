"""Tests de valores por defecto independientes por presupuesto."""
import sqlite3

from presupuestos.models import (
    actualizar_config,
    actualizar_presupuesto,
    copiar_presupuesto,
    crear_presupuesto,
    crear_secciones_tarea,
    crear_tarea,
    ensure_tablas_presupuestos,
    listar_secciones_tarea,
    obtener_config_presupuesto,
)


def _db():
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE ordenes_trabajo (id INTEGER PRIMARY KEY, obra TEXT)")
    ensure_tablas_presupuestos(db)
    return db


def test_config_default_se_copia_y_permanece_independiente_por_presupuesto():
    db = _db()
    config_inicial = {
        "gg_pct_default_fab": 0.07,
        "beneficio_pct_default_fab": 0.12,
        "imp_pct_default_fab": 0.05,
        "gg_pct_default_mon": 0.08,
        "beneficio_pct_default_mon": 0.15,
        "imp_pct_default_mon": 0.03,
        "tarifa_dh_taller_default": 1000,
        "tarifa_dh_obra_default": 2000,
        "tarifa_consumible_dh_taller_default": 300,
        "tarifa_consumible_dh_obra_default": 400,
    }
    actualizar_config(db, **config_inicial)
    primero = crear_presupuesto(db, "Cliente 1")

    config_nueva = {campo: valor * 2 for campo, valor in config_inicial.items()}
    actualizar_config(db, **config_nueva)
    segundo = crear_presupuesto(db, "Cliente 2")

    assert obtener_config_presupuesto(db, primero) == config_inicial
    assert obtener_config_presupuesto(db, segundo) == config_nueva

    tarea_id = crear_tarea(db, primero, "Nueva tarea")
    crear_secciones_tarea(db, tarea_id, obtener_config_presupuesto(db, primero))
    secciones = {seccion["tipo"]: seccion for seccion in listar_secciones_tarea(db, tarea_id)}
    assert secciones["FABRICACION"]["gg_pct"] == config_inicial["gg_pct_default_fab"]
    assert secciones["MONTAJE"]["gg_pct"] == config_inicial["gg_pct_default_mon"]

    grating_id = crear_tarea(db, primero, "Grating", tipo="Grating")
    crear_secciones_tarea(
        db, grating_id, obtener_config_presupuesto(db, primero), nombre_tarea="Grating"
    )
    grating_fab = next(
        seccion for seccion in listar_secciones_tarea(db, grating_id)
        if seccion["tipo"] == "FABRICACION"
    )
    assert grating_fab["gg_pct"] == config_inicial["gg_pct_default_fab"]

    copia = copiar_presupuesto(db, primero)
    assert obtener_config_presupuesto(db, copia) == config_inicial

    cambios_presupuesto = {campo: valor + 1 for campo, valor in config_inicial.items()}
    actualizar_presupuesto(db, primero, config_presupuesto=cambios_presupuesto)
    assert obtener_config_presupuesto(db, primero) == cambios_presupuesto
    assert obtener_config_presupuesto(db, segundo) == config_nueva
    assert obtener_config_presupuesto(db, copia) == config_inicial


def test_migracion_legacy_rellena_defaults_sin_pisar_presupuestos_existentes():
    db = sqlite3.connect(":memory:")
    db.execute("""
    CREATE TABLE presupuestos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente TEXT, planta TEXT, titulo TEXT, fecha DATE,
        estado TEXT, fecha_creacion DATETIME, fecha_adjudicacion DATETIME,
        ot_id INTEGER, tipo_cambio_referencia REAL, numero_presupuesto TEXT,
        copiado_de_id INTEGER, obra_referencia TEXT,
        odoo_analitica_fab_id INTEGER, odoo_analitica_mon_id INTEGER
    )
    """)
    db.execute("INSERT INTO presupuestos (cliente) VALUES ('Legacy previo')")
    ensure_tablas_presupuestos(db)
    db.execute("UPDATE config_presupuestos SET tarifa_dh_taller_default = 456")
    db.execute("INSERT INTO presupuestos (cliente) VALUES (?)", ("Legacy posterior",))
    db.commit()

    ensure_tablas_presupuestos(db)

    valores = db.execute(
        "SELECT tarifa_dh_taller_default FROM presupuestos ORDER BY id"
    ).fetchall()
    assert valores == [(0.0,), (456.0,)]


if __name__ == "__main__":
    pruebas = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    for nombre, funcion in pruebas:
        funcion()
        print(f"[OK] {nombre}")
    print(f"{len(pruebas)}/{len(pruebas)} tests OK")
