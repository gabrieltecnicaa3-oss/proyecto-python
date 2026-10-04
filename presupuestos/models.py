"""Modelo de datos del módulo Presupuestos (sin Flask, sin lógica de cálculo).

Fuente: ESPECIFICACION_MODULO_PRESUPUESTOS_V1.md, sección 2.

Convención del proyecto: no hay ORM ni Alembic. Cada módulo define su propia
`ensure_tablas_*(db)` idempotente (CREATE TABLE IF NOT EXISTS + ALTER TABLE
envueltos en try/except), igual que `_ensure_tables()` en suministros_routes.py
y `init_db()` en app2.py. Las funciones de este archivo son el equivalente a
"modelos": una fila = un dict, sin behavior extra.

Nota sobre `catalogo_perfiles`: la especificación indica reutilizar el
catálogo de perfiles del módulo de Compras. En este repo esa tabla se llama
`articulos_sum` (ver suministros_routes.py). `items_costo.perfil_id`
referencia lógicamente a `articulos_sum.id` (sin FK dura, ver comentario en
`ensure_tablas_presupuestos`).
"""
import json

from .constants import ESTADOS_PRESUPUESTO, TIPOS_SECCION


# ─────────────────────────────────────────────────────────────────
# Migración (idempotente)
# ─────────────────────────────────────────────────────────────────

def ensure_tablas_presupuestos(db):
    """Crea las tablas del módulo si no existen. Seguro de llamar en cada request."""

    db.execute("""
    CREATE TABLE IF NOT EXISTS presupuestos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cliente TEXT,
        planta TEXT,
        titulo TEXT,
        fecha DATE,
        obra_referencia TEXT,
        estado TEXT DEFAULT 'borrador',
        fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
        fecha_adjudicacion DATETIME,
        ot_id INTEGER,
        tipo_cambio_referencia REAL
    )
    """)
    # Migración para instalaciones existentes (creadas antes de agregar estas columnas).
    for _col, _def in (
        ("planta", "TEXT"),
        ("titulo", "TEXT"),
        ("fecha", "DATE"),
    ):
        try:
            db.execute(f"ALTER TABLE presupuestos ADD COLUMN {_col} {_def}")
        except Exception:
            pass

    db.execute("""
    CREATE TABLE IF NOT EXISTS tareas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        presupuesto_id INTEGER NOT NULL,
        nombre TEXT NOT NULL,
        orden INTEGER DEFAULT 0,
        FOREIGN KEY (presupuesto_id) REFERENCES presupuestos(id)
    )
    """)

    db.execute("""
    CREATE TABLE IF NOT EXISTS tarea_secciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tarea_id INTEGER NOT NULL,
        tipo TEXT NOT NULL,
        gg_pct REAL,
        beneficio_pct REAL,
        imp_pct REAL,
        FOREIGN KEY (tarea_id) REFERENCES tareas(id)
    )
    """)

    db.execute("""
    CREATE TABLE IF NOT EXISTS items_costo (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tarea_seccion_id INTEGER NOT NULL,
        rubro TEXT NOT NULL,
        tipo_item TEXT,
        perfil_id INTEGER,
        datos TEXT NOT NULL,
        subtotal REAL DEFAULT 0,
        FOREIGN KEY (tarea_seccion_id) REFERENCES tarea_secciones(id)
    )
    """)
    # perfil_id no lleva FOREIGN KEY dura: referencia lógica a articulos_sum(id)
    # (catálogo de Compras), que puede no existir todavía cuando se corre esta
    # migración de forma aislada.

    db.execute("""
    CREATE TABLE IF NOT EXISTS catalogo_equipos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        tarifa_dia_default REAL
    )
    """)

    db.execute("""
    CREATE TABLE IF NOT EXISTS catalogo_esquemas_pintura (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL,
        precio_unitario_m2_default REAL
    )
    """)

    db.execute("""
    CREATE TABLE IF NOT EXISTS config_presupuestos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        gg_pct_default_fab REAL DEFAULT 0,
        beneficio_pct_default_fab REAL DEFAULT 0,
        imp_pct_default_fab REAL DEFAULT 0,
        gg_pct_default_mon REAL DEFAULT 0,
        beneficio_pct_default_mon REAL DEFAULT 0,
        imp_pct_default_mon REAL DEFAULT 0,
        tarifa_dh_taller_default REAL DEFAULT 0,
        tarifa_dh_obra_default REAL DEFAULT 0,
        tarifa_consumible_dh_taller_default REAL DEFAULT 0,
        tarifa_consumible_dh_obra_default REAL DEFAULT 0
    )
    """)

    # Índices para los filtros/joins más frecuentes del futuro CRUD.
    db.execute("CREATE INDEX IF NOT EXISTS idx_tareas_presupuesto_id ON tareas(presupuesto_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_tarea_secciones_tarea_id ON tarea_secciones(tarea_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_items_costo_tarea_seccion_id ON items_costo(tarea_seccion_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_items_costo_perfil_id ON items_costo(perfil_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_presupuestos_ot_id ON presupuestos(ot_id)")

    db.commit()

    _asegurar_config_default(db)


def _asegurar_config_default(db):
    """Crea la fila única de config_presupuestos si la tabla está vacía."""
    row = db.execute("SELECT COUNT(1) FROM config_presupuestos").fetchone()
    total = int(row[0]) if row else 0
    if total == 0:
        db.execute(
            """
            INSERT INTO config_presupuestos (
                gg_pct_default_fab, beneficio_pct_default_fab, imp_pct_default_fab,
                gg_pct_default_mon, beneficio_pct_default_mon, imp_pct_default_mon,
                tarifa_dh_taller_default, tarifa_dh_obra_default,
                tarifa_consumible_dh_taller_default, tarifa_consumible_dh_obra_default
            )
            VALUES (0, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            """
        )
        db.commit()


# ─────────────────────────────────────────────────────────────────
# presupuestos
# ─────────────────────────────────────────────────────────────────

def crear_presupuesto(db, cliente, planta="", titulo="", fecha=None, tipo_cambio_referencia=None):
    """Header del presupuesto: cliente, planta, fecha y titulo (definidos por el usuario
    al crear "Nuevo presupuesto"). `obra_referencia` queda deprecado, se mantiene en el
    esquema solo por compatibilidad hacia atrás."""
    cursor = db.execute(
        """
        INSERT INTO presupuestos (cliente, planta, titulo, fecha, estado, tipo_cambio_referencia)
        VALUES (?, ?, ?, ?, 'borrador', ?)
        """,
        (cliente, planta, titulo, fecha, tipo_cambio_referencia),
    )
    db.commit()
    return cursor.lastrowid


def obtener_presupuesto(db, presupuesto_id):
    row = db.execute(
        """
        SELECT id, cliente, planta, titulo, fecha, estado, fecha_creacion,
               fecha_adjudicacion, ot_id, tipo_cambio_referencia
        FROM presupuestos WHERE id = ?
        """,
        (presupuesto_id,),
    ).fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "cliente": row[1],
        "planta": row[2],
        "titulo": row[3],
        "fecha": row[4],
        "estado": row[5],
        "fecha_creacion": row[6],
        "fecha_adjudicacion": row[7],
        "ot_id": row[8],
        "tipo_cambio_referencia": row[9],
    }


def listar_presupuestos(db):
    rows = db.execute(
        """
        SELECT id, cliente, planta, titulo, fecha, estado, fecha_creacion, ot_id
        FROM presupuestos
        ORDER BY id DESC
        """
    ).fetchall()
    return [
        {
            "id": r[0],
            "cliente": r[1],
            "planta": r[2],
            "titulo": r[3],
            "fecha": r[4],
            "estado": r[5],
            "fecha_creacion": r[6],
            "ot_id": r[7],
        }
        for r in rows
    ]


def actualizar_estado_presupuesto(db, presupuesto_id, estado, fecha_adjudicacion=None, ot_id=None):
    if estado not in ESTADOS_PRESUPUESTO:
        raise ValueError(f"Estado inválido: {estado!r}")
    db.execute(
        """
        UPDATE presupuestos
        SET estado = ?, fecha_adjudicacion = COALESCE(?, fecha_adjudicacion), ot_id = COALESCE(?, ot_id)
        WHERE id = ?
        """,
        (estado, fecha_adjudicacion, ot_id, presupuesto_id),
    )
    db.commit()


def actualizar_presupuesto(db, presupuesto_id, cliente=None, planta=None, titulo=None, fecha=None, tipo_cambio_referencia=None):
    """Actualiza los datos generales del header. Solo pisa los campos recibidos
    (None = no tocar), para permitir ediciones parciales desde el futuro form."""
    actual = obtener_presupuesto(db, presupuesto_id)
    if not actual:
        return
    db.execute(
        """
        UPDATE presupuestos
        SET cliente = ?, planta = ?, titulo = ?, fecha = ?, tipo_cambio_referencia = ?
        WHERE id = ?
        """,
        (
            cliente if cliente is not None else actual["cliente"],
            planta if planta is not None else actual["planta"],
            titulo if titulo is not None else actual["titulo"],
            fecha if fecha is not None else actual["fecha"],
            tipo_cambio_referencia if tipo_cambio_referencia is not None else actual["tipo_cambio_referencia"],
            presupuesto_id,
        ),
    )
    db.commit()


def eliminar_presupuesto(db, presupuesto_id):
    """Borrado en cascada: items_costo -> tarea_secciones -> tareas -> presupuesto
    (no hay FKs duras en SQLite/MySQL acá, así que la cascada se hace a mano)."""
    for tarea in listar_tareas(db, presupuesto_id):
        _eliminar_tarea_sin_commit(db, tarea["id"])
    db.execute("DELETE FROM presupuestos WHERE id = ?", (presupuesto_id,))
    db.commit()


# ─────────────────────────────────────────────────────────────────
# tareas
# ──────────────────────────────────────────────────────────────

def crear_tarea(db, presupuesto_id, nombre, orden=0):
    cursor = db.execute(
        "INSERT INTO tareas (presupuesto_id, nombre, orden) VALUES (?, ?, ?)",
        (presupuesto_id, nombre, orden),
    )
    db.commit()
    return cursor.lastrowid


def obtener_tarea(db, tarea_id):
    row = db.execute(
        "SELECT id, presupuesto_id, nombre, orden FROM tareas WHERE id = ?",
        (tarea_id,),
    ).fetchone()
    if not row:
        return None
    return {"id": row[0], "presupuesto_id": row[1], "nombre": row[2], "orden": row[3]}


def listar_tareas(db, presupuesto_id):
    rows = db.execute(
        "SELECT id, presupuesto_id, nombre, orden FROM tareas WHERE presupuesto_id = ? ORDER BY orden, id",
        (presupuesto_id,),
    ).fetchall()
    return [{"id": r[0], "presupuesto_id": r[1], "nombre": r[2], "orden": r[3]} for r in rows]


def _eliminar_tarea_sin_commit(db, tarea_id):
    """Borra secciones+items de una tarea y la tarea misma, sin commitear
    (uso interno para poder encadenar varias tareas en una sola transacción)."""
    for seccion in listar_secciones_tarea(db, tarea_id):
        db.execute("DELETE FROM items_costo WHERE tarea_seccion_id = ?", (seccion["id"],))
        db.execute("DELETE FROM tarea_secciones WHERE id = ?", (seccion["id"],))
    db.execute("DELETE FROM tareas WHERE id = ?", (tarea_id,))


def eliminar_tarea(db, tarea_id):
    """Borrado en cascada: items_costo -> tarea_secciones -> tarea."""
    _eliminar_tarea_sin_commit(db, tarea_id)
    db.commit()


# ─────────────────────────────────────────────────────────────────
# tarea_secciones — una tarea puede tener FABRICACION, MONTAJE o ambas
# (ej: "Estructura metálica" con fab+montaje; una tarea puramente de
# montaje no lleva sección de fabricación, y viceversa).
# ─────────────────────────────────────────────────────────────────

def crear_secciones_tarea(db, tarea_id, config_default, tipos=TIPOS_SECCION):
    """Crea las secciones indicadas en `tipos` (subconjunto de FABRICACION/MONTAJE)
    con los % default de config. Por default crea ambas.

    Sirve tanto para la creación inicial de la tarea (sin secciones todavía)
    como para agregar más adelante la sección faltante a una tarea existente
    (ej. "Chapeado" nace solo MONTAJE y después necesita también FABRICACION).
    Valida que no se dupliquen tipos ya existentes para la tarea (sección 2.1:
    una tarea no puede tener dos secciones del mismo tipo, pero sí puede tener
    solo una de las dos)."""
    existentes = {s["tipo"] for s in listar_secciones_tarea(db, tarea_id)}
    ids = {}
    for tipo in tipos:
        tipo = str(tipo or "").strip().upper()
        if tipo not in TIPOS_SECCION:
            raise ValueError(f"tipo debe ser uno de {TIPOS_SECCION}")
        if tipo in existentes:
            raise ValueError(f"La tarea ya tiene una sección {tipo}.")
        sufijo = "fab" if tipo == "FABRICACION" else "mon"
        cursor = db.execute(
            """
            INSERT INTO tarea_secciones (tarea_id, tipo, gg_pct, beneficio_pct, imp_pct)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                tarea_id,
                tipo,
                config_default.get(f"gg_pct_default_{sufijo}", 0),
                config_default.get(f"beneficio_pct_default_{sufijo}", 0),
                config_default.get(f"imp_pct_default_{sufijo}", 0),
            ),
        )
        ids[tipo] = cursor.lastrowid
        existentes.add(tipo)
    db.commit()
    return ids


def obtener_seccion(db, seccion_id):
    row = db.execute(
        """
        SELECT id, tarea_id, tipo, gg_pct, beneficio_pct, imp_pct
        FROM tarea_secciones WHERE id = ?
        """,
        (seccion_id,),
    ).fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "tarea_id": row[1],
        "tipo": row[2],
        "gg_pct": row[3],
        "beneficio_pct": row[4],
        "imp_pct": row[5],
    }


def listar_secciones_tarea(db, tarea_id):
    rows = db.execute(
        """
        SELECT id, tarea_id, tipo, gg_pct, beneficio_pct, imp_pct
        FROM tarea_secciones WHERE tarea_id = ? ORDER BY tipo
        """,
        (tarea_id,),
    ).fetchall()
    return [
        {
            "id": r[0],
            "tarea_id": r[1],
            "tipo": r[2],
            "gg_pct": r[3],
            "beneficio_pct": r[4],
            "imp_pct": r[5],
        }
        for r in rows
    ]


def actualizar_porcentajes_seccion(db, seccion_id, gg_pct, beneficio_pct, imp_pct):
    db.execute(
        "UPDATE tarea_secciones SET gg_pct = ?, beneficio_pct = ?, imp_pct = ? WHERE id = ?",
        (gg_pct, beneficio_pct, imp_pct, seccion_id),
    )
    db.commit()


def eliminar_seccion(db, seccion_id):
    """Borrado en cascada: items_costo -> tarea_seccion."""
    db.execute("DELETE FROM items_costo WHERE tarea_seccion_id = ?", (seccion_id,))
    db.execute("DELETE FROM tarea_secciones WHERE id = ?", (seccion_id,))
    db.commit()


# ─────────────────────────────────────────────────────────────────
# items_costo (datos se guarda como JSON; ver sección 3 de la especificación)
# ─────────────────────────────────────────────────────────────────

def crear_item_costo(db, tarea_seccion_id, rubro, datos, tipo_item=None, perfil_id=None, subtotal=0):
    cursor = db.execute(
        """
        INSERT INTO items_costo (tarea_seccion_id, rubro, tipo_item, perfil_id, datos, subtotal)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (tarea_seccion_id, rubro, tipo_item, perfil_id, json.dumps(datos, ensure_ascii=False), subtotal),
    )
    db.commit()
    return cursor.lastrowid


def _row_a_item_costo(row):
    return {
        "id": row[0],
        "tarea_seccion_id": row[1],
        "rubro": row[2],
        "tipo_item": row[3],
        "perfil_id": row[4],
        "datos": json.loads(row[5]) if row[5] else {},
        "subtotal": row[6],
    }


def obtener_item_costo(db, item_id):
    row = db.execute(
        """
        SELECT id, tarea_seccion_id, rubro, tipo_item, perfil_id, datos, subtotal
        FROM items_costo WHERE id = ?
        """,
        (item_id,),
    ).fetchone()
    return _row_a_item_costo(row) if row else None


def listar_items_costo(db, tarea_seccion_id):
    rows = db.execute(
        """
        SELECT id, tarea_seccion_id, rubro, tipo_item, perfil_id, datos, subtotal
        FROM items_costo WHERE tarea_seccion_id = ? ORDER BY id
        """,
        (tarea_seccion_id,),
    ).fetchall()
    return [_row_a_item_costo(r) for r in rows]


def actualizar_item_costo(db, item_id, datos=None, subtotal=None, perfil_id=None):
    item = obtener_item_costo(db, item_id)
    if not item:
        return
    nuevos_datos = datos if datos is not None else item["datos"]
    nuevo_subtotal = subtotal if subtotal is not None else item["subtotal"]
    nuevo_perfil_id = perfil_id if perfil_id is not None else item["perfil_id"]
    db.execute(
        "UPDATE items_costo SET datos = ?, subtotal = ?, perfil_id = ? WHERE id = ?",
        (json.dumps(nuevos_datos, ensure_ascii=False), nuevo_subtotal, nuevo_perfil_id, item_id),
    )
    db.commit()


def eliminar_item_costo(db, item_id):
    db.execute("DELETE FROM items_costo WHERE id = ?", (item_id,))
    db.commit()


# ─────────────────────────────────────────────────────────────────
# catalogo_equipos
# ─────────────────────────────────────────────────────────────────

def crear_equipo(db, nombre, tarifa_dia_default=0):
    cursor = db.execute(
        "INSERT INTO catalogo_equipos (nombre, tarifa_dia_default) VALUES (?, ?)",
        (nombre, tarifa_dia_default),
    )
    db.commit()
    return cursor.lastrowid


def listar_equipos(db):
    rows = db.execute("SELECT id, nombre, tarifa_dia_default FROM catalogo_equipos ORDER BY nombre").fetchall()
    return [{"id": r[0], "nombre": r[1], "tarifa_dia_default": r[2]} for r in rows]


def actualizar_equipo(db, equipo_id, nombre, tarifa_dia_default):
    db.execute(
        "UPDATE catalogo_equipos SET nombre = ?, tarifa_dia_default = ? WHERE id = ?",
        (nombre, tarifa_dia_default, equipo_id),
    )
    db.commit()


def eliminar_equipo(db, equipo_id):
    db.execute("DELETE FROM catalogo_equipos WHERE id = ?", (equipo_id,))
    db.commit()


# ─────────────────────────────────────────────────────────────────
# catalogo_esquemas_pintura
# ─────────────────────────────────────────────────────────────────

def crear_esquema_pintura(db, nombre, precio_unitario_m2_default=0):
    cursor = db.execute(
        "INSERT INTO catalogo_esquemas_pintura (nombre, precio_unitario_m2_default) VALUES (?, ?)",
        (nombre, precio_unitario_m2_default),
    )
    db.commit()
    return cursor.lastrowid


def listar_esquemas_pintura(db):
    rows = db.execute(
        "SELECT id, nombre, precio_unitario_m2_default FROM catalogo_esquemas_pintura ORDER BY nombre"
    ).fetchall()
    return [{"id": r[0], "nombre": r[1], "precio_unitario_m2_default": r[2]} for r in rows]


def actualizar_esquema_pintura(db, esquema_id, nombre, precio_unitario_m2_default):
    db.execute(
        "UPDATE catalogo_esquemas_pintura SET nombre = ?, precio_unitario_m2_default = ? WHERE id = ?",
        (nombre, precio_unitario_m2_default, esquema_id),
    )
    db.commit()


def eliminar_esquema_pintura(db, esquema_id):
    db.execute("DELETE FROM catalogo_esquemas_pintura WHERE id = ?", (esquema_id,))
    db.commit()


# ─────────────────────────────────────────────────────────────────
# config_presupuestos (fila única)
# ─────────────────────────────────────────────────────────────────

_CONFIG_CAMPOS = (
    "gg_pct_default_fab",
    "beneficio_pct_default_fab",
    "imp_pct_default_fab",
    "gg_pct_default_mon",
    "beneficio_pct_default_mon",
    "imp_pct_default_mon",
    "tarifa_dh_taller_default",
    "tarifa_dh_obra_default",
    "tarifa_consumible_dh_taller_default",
    "tarifa_consumible_dh_obra_default",
)


def obtener_config(db):
    _asegurar_config_default(db)
    row = db.execute(
        f"SELECT id, {', '.join(_CONFIG_CAMPOS)} FROM config_presupuestos ORDER BY id LIMIT 1"
    ).fetchone()
    if not row:
        return None
    return {"id": row[0], **{campo: row[i + 1] for i, campo in enumerate(_CONFIG_CAMPOS)}}


def actualizar_config(db, **campos):
    config_actual = obtener_config(db)
    valores = {campo: campos.get(campo, config_actual[campo]) for campo in _CONFIG_CAMPOS}
    set_clause = ", ".join(f"{campo} = ?" for campo in _CONFIG_CAMPOS)
    db.execute(
        f"UPDATE config_presupuestos SET {set_clause} WHERE id = ?",
        tuple(valores[campo] for campo in _CONFIG_CAMPOS) + (config_actual["id"],),
    )
    db.commit()
