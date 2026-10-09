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
from .volcado_previsto import CAMPOS_ECONOMICOS

# % default de FABRICACION fijos para ciertos tipos de tarea (no salen de
# config_presupuestos, que sigue en 0 para el resto de las tareas). Cada tipo
# de tarea puede tener su propio set de %.
PCTS_FABRICACION_POR_TIPO_TAREA = {
    "Chapeado": {"gg_pct": 0.07, "beneficio_pct": 0.05, "imp_pct": 0.03},
    "Grating": {"gg_pct": 0.05, "beneficio_pct": 0.05, "imp_pct": 0.03},
}


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
        numero_presupuesto TEXT,
        copiado_de_id INTEGER,
        estado TEXT DEFAULT 'borrador',
        fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
        fecha_adjudicacion DATETIME,
        ot_id INTEGER,
        tipo_cambio_referencia REAL,
        odoo_analitica_fab_id INTEGER,
        odoo_analitica_mon_id INTEGER
    )
    """)
    # Migración para instalaciones existentes (creadas antes de agregar estas columnas).
    for _col, _def in (
        ("planta", "TEXT"),
        ("titulo", "TEXT"),
        ("fecha", "DATE"),
        ("numero_presupuesto", "TEXT"),
        ("copiado_de_id", "INTEGER"),
        ("odoo_analitica_fab_id", "INTEGER"),
        ("odoo_analitica_mon_id", "INTEGER"),
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
        tipo TEXT,
        orden INTEGER DEFAULT 0,
        FOREIGN KEY (presupuesto_id) REFERENCES presupuestos(id)
    )
    """)
    try:
        db.execute("ALTER TABLE tareas ADD COLUMN tipo TEXT")
    except Exception:
        pass

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
        tarifa_dia_default REAL,
        producto_odoo TEXT
    )
    """)
    try:
        db.execute("ALTER TABLE catalogo_equipos ADD COLUMN producto_odoo TEXT")
        db.commit()
    except Exception:
        pass

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

    db.execute("""
    CREATE TABLE IF NOT EXISTS config_categorias_odoo (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        concepto TEXT NOT NULL,
        tipo_seccion TEXT NOT NULL,
        categoria_odoo TEXT,
        factor REAL DEFAULT 1
    )
    """)
    try:
        db.execute("ALTER TABLE config_categorias_odoo ADD COLUMN factor REAL DEFAULT 1")
        db.commit()
    except Exception:
        pass

    db.execute("""
    CREATE TABLE IF NOT EXISTS config_productos_odoo (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        seccion TEXT NOT NULL,
        concepto TEXT NOT NULL,
        tipo_item TEXT,
        familia TEXT,
        tarea TEXT,
        producto_odoo TEXT
    )
    """)
    try:
        db.execute("ALTER TABLE config_productos_odoo ADD COLUMN tarea TEXT")
        db.commit()
    except Exception:
        pass

    # Volcado del previsto a las OT. Las columnas FK son BIGINT (no INTEGER) para
    # que coincidan con los PK que MySQL crea para tareas/presupuestos/ordenes_trabajo.
    # Tarea sin OT = sin filas en tarea_ot_reparto. Sin UNIQUE(tarea_id, ot_id).
    db.execute("""
    CREATE TABLE IF NOT EXISTS tarea_ot_reparto (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tarea_id BIGINT NOT NULL,
        ot_id BIGINT NOT NULL,
        porcentaje REAL NOT NULL CHECK (porcentaje >= 0 AND porcentaje <= 100),
        FOREIGN KEY (tarea_id) REFERENCES tareas(id),
        FOREIGN KEY (ot_id) REFERENCES ordenes_trabajo(id)
    )
    """)

    db.execute("""
    CREATE TABLE IF NOT EXISTS volcados_previsto (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        presupuesto_id BIGINT NOT NULL,
        fecha DATETIME DEFAULT CURRENT_TIMESTAMP,
        usuario TEXT,
        nota TEXT,
        FOREIGN KEY (presupuesto_id) REFERENCES presupuestos(id)
    )
    """)

    # Lo escrito en cada OT en ese volcado: historial y base para detectar ediciones manuales.
    db.execute("""
    CREATE TABLE IF NOT EXISTS volcados_previsto_lineas (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        volcado_id BIGINT NOT NULL,
        ot_id BIGINT NOT NULL,
        rubro TEXT NOT NULL,
        monto REAL DEFAULT 0,
        FOREIGN KEY (volcado_id) REFERENCES volcados_previsto(id),
        FOREIGN KEY (ot_id) REFERENCES ordenes_trabajo(id)
    )
    """)

    # Índices para los filtros/joins más frecuentes del futuro CRUD.
    db.execute("CREATE INDEX IF NOT EXISTS idx_tareas_presupuesto_id ON tareas(presupuesto_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_tarea_secciones_tarea_id ON tarea_secciones(tarea_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_items_costo_tarea_seccion_id ON items_costo(tarea_seccion_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_items_costo_perfil_id ON items_costo(perfil_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_presupuestos_ot_id ON presupuestos(ot_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_tarea_ot_reparto_tarea_id ON tarea_ot_reparto(tarea_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_tarea_ot_reparto_ot_id ON tarea_ot_reparto(ot_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_volcados_previsto_presupuesto_id ON volcados_previsto(presupuesto_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_volcados_lineas_volcado_id ON volcados_previsto_lineas(volcado_id)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_volcados_lineas_ot_id ON volcados_previsto_lineas(ot_id)")

    db.commit()

    _asegurar_config_default(db)
    _asegurar_categorias_odoo_seed(db)
    _asegurar_split_traslado_movilidad(db)
    _asegurar_productos_odoo_seed(db)


# Seed de config_categorias_odoo (sección 2.1 de la especificación): mapeo
# concepto+tipo_seccion -> categoria_odoo que usa el Reporte 2 (sección 5.3).
# categoria_odoo = None excluye ese concepto del Reporte 2 (caso Beneficio).
# `factor` (default 1) permite que un mismo concepto se reparta entre varias
# categorías del Reporte 2 (caso mano_obra/MONTAJE, pedido del usuario: 89,7%
# a "Gastos e Inversión Laboral" y 10,3% a "Traslado y Movilidad de Personal").
_SEED_CATEGORIAS_ODOO = (
    ("mano_obra", "FABRICACION", "Gastos e Inversión Laboral", 1),
    ("mano_obra", "MONTAJE", "Gastos e Inversión Laboral", 0.897),
    ("mano_obra", "MONTAJE", "Traslado y Movilidad de Personal", 0.103),
    ("consumibles", "FABRICACION", "Consumibles de Obra y/o Taller", 1),
    ("consumibles", "MONTAJE", "Consumibles de Obra y/o Taller", 1),
    ("materiales", "FABRICACION", "Materiales de Estructuras Metálicas y Herrería", 1),
    ("bulones", "FABRICACION", "Materiales de Estructuras Metálicas y Herrería", 1),
    ("pintura", "FABRICACION", "Pintura y Accesorios", 1),
    ("fletes", "FABRICACION", "Fletes y Traslados de Cargas", 1),
    ("subcontratos", "FABRICACION", "Subcontratos - Estructuras Metálicas y Herrería", 1),
    ("subcontratos", "MONTAJE", "Subcontratos - Estructuras Metálicas y Herrería", 1),
    ("equipo", "MONTAJE", "Alquiler de Maquinaria, Grúas y Vehículos", 1),
    ("ingenieria", "FABRICACION", "Proyectos de Ingeniería y Arquitectura", 1),
    ("ingeniero", "MONTAJE", "Honorarios y Certificaciones de Obra", 1),
    ("tecnico_hys", "MONTAJE", "Honorarios y Certificaciones de Obra", 1),
    ("GG", "FABRICACION", "Gastos de Administración", 1),
    ("GG", "MONTAJE", "Gastos de Administración", 1),
    ("IMPUESTOS", "FABRICACION", "Impuestos", 1),
    ("IMPUESTOS", "MONTAJE", "Impuestos", 1),
    ("BENEFICIO", "FABRICACION", None, 1),
    ("BENEFICIO", "MONTAJE", None, 1),
)


def _asegurar_categorias_odoo_seed(db):
    """Carga el seed de config_categorias_odoo (sección 2.1) si la tabla
    está vacía. Editable luego desde la tabla directamente / futuro CRUD."""
    row = db.execute("SELECT COUNT(1) FROM config_categorias_odoo").fetchone()
    total = int(row[0]) if row else 0
    if total == 0:
        db.executemany(
            "INSERT INTO config_categorias_odoo (concepto, tipo_seccion, categoria_odoo, factor) VALUES (?, ?, ?, ?)",
            _SEED_CATEGORIAS_ODOO,
        )
        db.commit()


def _asegurar_split_traslado_movilidad(db):
    """Migración para DBs que ya tenían el seed viejo (mano_obra/MONTAJE al
    100% en "Gastos e Inversión Laboral"): ajusta esa fila a factor 0.897 y
    agrega la fila nueva "Traslado y Movilidad de Personal" (factor 0.103) si
    todavía no existe. Solo afecta MONTAJE; Fabricación queda intacta."""
    fila_existente = db.execute(
        """
        SELECT id FROM config_categorias_odoo
        WHERE concepto = 'mano_obra' AND tipo_seccion = 'MONTAJE'
          AND categoria_odoo = 'Traslado y Movilidad de Personal'
        LIMIT 1
        """
    ).fetchone()
    if fila_existente:
        return

    db.execute(
        """
        UPDATE config_categorias_odoo SET factor = 0.897
        WHERE concepto = 'mano_obra' AND tipo_seccion = 'MONTAJE'
          AND categoria_odoo = 'Gastos e Inversión Laboral'
        """
    )
    db.execute(
        """
        INSERT INTO config_categorias_odoo (concepto, tipo_seccion, categoria_odoo, factor)
        VALUES ('mano_obra', 'MONTAJE', 'Traslado y Movilidad de Personal', 0.103)
        """
    )
    db.commit()


def listar_categorias_odoo(db):
    """Lista config_categorias_odoo tal cual está en la DB (sección 2.1),
    para que el Reporte 2 (sección 5.3) reclasifique sin hardcodear valores."""
    _asegurar_categorias_odoo_seed(db)
    _asegurar_split_traslado_movilidad(db)
    rows = db.execute(
        "SELECT concepto, tipo_seccion, categoria_odoo, COALESCE(factor, 1) FROM config_categorias_odoo ORDER BY id"
    ).fetchall()
    return [
        {"concepto": r[0], "tipo_seccion": r[1], "categoria_odoo": r[2], "factor": r[3]}
        for r in rows
    ]


# Seed de config_productos_odoo (Reporte 3, pedido "día 0" de Odoo): mapeo
# (seccion, concepto, tipo_item, familia, tarea) -> producto_odoo. Los campos
# tipo_item/familia/tarea en None = comodín (aplican a cualquier valor); gana la
# regla más específica (tarea > familia > tipo_item). `tarea` es el tipo de la
# pestaña (Correas, Grating, Zinguería, Insertos): TODOS sus materiales van a un
# único producto. producto_odoo en None = sin producto (el reporte lo avisa).
# Los equipos (rubro "equipo") NO van acá: su producto vive en
# catalogo_equipos.producto_odoo.
_SEED_PRODUCTOS_ODOO = (
    ("FABRICACION", "materiales", "perfil", None, None, "Perfiles Metálicos"),
    ("FABRICACION", "materiales", "porcentaje", None, None, "Placas varias"),
    ("FABRICACION", "materiales", "chapa", None, None, "Chapas varias"),
    ("FABRICACION", "materiales", "tornillos", None, None, "Consumibles varios"),
    ("FABRICACION", "materiales", None, None, "Correas", "Correas varias"),
    ("FABRICACION", "materiales", None, None, "Grating", "Grating"),
    ("FABRICACION", "materiales", None, None, "Zinguería", "Zinguerias varias"),
    ("FABRICACION", "materiales", None, None, "Insertos", "Varillas Roscadas"),
    ("FABRICACION", "bulones", None, None, None, "Bulones"),
    ("FABRICACION", "pintura", None, None, None, "Pintura y Accesorios"),
    ("FABRICACION", "ingenieria", None, None, None, "Proyectos de Ingeniería (EEMM)"),
    ("FABRICACION", "subcontratos", None, None, None, "Subcontratos de Estructuras Metálicas y Herrería"),
    ("MONTAJE", "subcontratos", None, None, None, "Subcontratos de Estructuras Metálicas y Herrería"),
)

_INSERT_PRODUCTO_ODOO = (
    "INSERT INTO config_productos_odoo (seccion, concepto, tipo_item, familia, tarea, producto_odoo) VALUES (?, ?, ?, ?, ?, ?)"
)

# tipo_item que nunca existieron en el modelo (primer seed, reemplazados por la regla por pestaña).
_TIPO_ITEM_ODOO_OBSOLETOS = (
    "zingueria_ml", "zingueria_unidad", "zingueria_fijaciones", "grating_panel", "grating_escalon", "grating",
)


def _asegurar_productos_odoo_seed(db):
    """Carga el seed de config_productos_odoo si la tabla está vacía; si ya tenía el
    seed viejo (sin reglas por pestaña), lo migra una sola vez."""
    row = db.execute("SELECT COUNT(1) FROM config_productos_odoo").fetchone()
    total = int(row[0]) if row else 0
    if total == 0:
        db.executemany(_INSERT_PRODUCTO_ODOO, _SEED_PRODUCTOS_ODOO)
        db.commit()
        return

    ya_migrado = db.execute("SELECT 1 FROM config_productos_odoo WHERE tarea IS NOT NULL LIMIT 1").fetchone()
    if ya_migrado:
        return
    placeholders = ",".join("?" for _ in _TIPO_ITEM_ODOO_OBSOLETOS)
    db.execute(
        f"DELETE FROM config_productos_odoo WHERE concepto = 'materiales' AND tipo_item IN ({placeholders})",
        _TIPO_ITEM_ODOO_OBSOLETOS,
    )
    db.executemany(
        _INSERT_PRODUCTO_ODOO,
        [fila for fila in _SEED_PRODUCTOS_ODOO if fila[4] is not None or fila[2] == "tornillos"],
    )
    db.commit()


def listar_productos_odoo(db):
    """Lista config_productos_odoo tal cual está en la DB (Reporte 3)."""
    _asegurar_productos_odoo_seed(db)
    rows = db.execute(
        "SELECT seccion, concepto, tipo_item, familia, tarea, producto_odoo FROM config_productos_odoo ORDER BY id"
    ).fetchall()
    return [
        {"seccion": r[0], "concepto": r[1], "tipo_item": r[2], "familia": r[3], "tarea": r[4], "producto_odoo": r[5]}
        for r in rows
    ]


def actualizar_analitica_odoo(db, presupuesto_id, seccion, odoo_id):
    """Guarda el último ID de cuenta analítica de Odoo usado para el pedido de
    esa sección (FABRICACION o MONTAJE)."""
    columna = {"FABRICACION": "odoo_analitica_fab_id", "MONTAJE": "odoo_analitica_mon_id"}[seccion]
    db.execute(f"UPDATE presupuestos SET {columna} = ? WHERE id = ?", (odoo_id, presupuesto_id))
    db.commit()


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

def crear_presupuesto(db, cliente, planta="", titulo="", fecha=None, tipo_cambio_referencia=None, numero_presupuesto=None):
    """Header del presupuesto: cliente, planta, fecha, titulo y numero_presupuesto
    (definidos por el usuario al crear "Nuevo presupuesto", numero_presupuesto es de
    carga manual, sin formato fijo). `obra_referencia` queda deprecado, se mantiene en
    el esquema solo por compatibilidad hacia atrás."""
    cursor = db.execute(
        """
        INSERT INTO presupuestos (cliente, planta, titulo, fecha, numero_presupuesto, estado, tipo_cambio_referencia)
        VALUES (?, ?, ?, ?, ?, 'borrador', ?)
        """,
        (cliente, planta, titulo, fecha, numero_presupuesto, tipo_cambio_referencia),
    )
    db.commit()
    return cursor.lastrowid


def obtener_presupuesto(db, presupuesto_id):
    row = db.execute(
        """
        SELECT id, cliente, planta, titulo, fecha, estado, fecha_creacion,
               fecha_adjudicacion, ot_id, tipo_cambio_referencia, numero_presupuesto,
               copiado_de_id, obra_referencia, odoo_analitica_fab_id, odoo_analitica_mon_id
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
        "numero_presupuesto": row[10],
        "copiado_de_id": row[11],
        "obra_referencia": row[12],
        "odoo_analitica_fab_id": row[13],
        "odoo_analitica_mon_id": row[14],
    }


def listar_presupuestos(db):
    rows = db.execute(
        """
        SELECT p.id, p.cliente, p.planta, p.titulo, p.fecha, p.estado, p.fecha_creacion, p.ot_id,
               p.numero_presupuesto, p.copiado_de_id,
               COALESCE(orig.obra_referencia, orig.titulo, orig.cliente) AS copia_de_label
        FROM presupuestos p
        LEFT JOIN presupuestos orig ON orig.id = p.copiado_de_id
        ORDER BY p.id DESC
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
            "numero_presupuesto": r[8],
            "copiado_de_id": r[9],
            "copia_de_label": r[10],
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


def actualizar_presupuesto(db, presupuesto_id, cliente=None, planta=None, titulo=None, fecha=None, tipo_cambio_referencia=None, numero_presupuesto=None):
    """Actualiza los datos generales del header. Solo pisa los campos recibidos
    (None = no tocar), para permitir ediciones parciales desde el futuro form."""
    actual = obtener_presupuesto(db, presupuesto_id)
    if not actual:
        return
    db.execute(
        """
        UPDATE presupuestos
        SET cliente = ?, planta = ?, titulo = ?, fecha = ?, tipo_cambio_referencia = ?, numero_presupuesto = ?
        WHERE id = ?
        """,
        (
            cliente if cliente is not None else actual["cliente"],
            planta if planta is not None else actual["planta"],
            titulo if titulo is not None else actual["titulo"],
            fecha if fecha is not None else actual["fecha"],
            tipo_cambio_referencia if tipo_cambio_referencia is not None else actual["tipo_cambio_referencia"],
            numero_presupuesto if numero_presupuesto is not None else actual["numero_presupuesto"],
            presupuesto_id,
        ),
    )
    db.commit()


def eliminar_presupuesto(db, presupuesto_id):
    """Borrado en cascada: items_costo -> tarea_secciones -> tareas -> presupuesto
    (no hay FKs duras en SQLite/MySQL acá, así que la cascada se hace a mano)."""
    for tarea in listar_tareas(db, presupuesto_id):
        _eliminar_tarea_sin_commit(db, tarea["id"])
    # Historial de volcados (FK a presupuestos): líneas primero, después la cabecera.
    db.execute(
        "DELETE FROM volcados_previsto_lineas WHERE volcado_id IN (SELECT id FROM volcados_previsto WHERE presupuesto_id = ?)",
        (presupuesto_id,),
    )
    db.execute("DELETE FROM volcados_previsto WHERE presupuesto_id = ?", (presupuesto_id,))
    db.execute("DELETE FROM presupuestos WHERE id = ?", (presupuesto_id,))
    db.commit()


def copiar_presupuesto(db, presupuesto_id):
    """Duplica un presupuesto completo (tareas, secciones e items_costo) como un
    borrador nuevo, sin recalcular nada (los subtotales se recalculan solos al
    abrir la pantalla, vía el motor existente). Copia cliente y obra_referencia
    del original; el resto del header (número, planta, título, fecha, tipo de
    cambio) arranca en blanco para que el usuario lo complete en la copia.
    Devuelve el id del presupuesto nuevo, o None si `presupuesto_id` no existe."""
    original = db.execute(
        "SELECT cliente, obra_referencia FROM presupuestos WHERE id = ?",
        (presupuesto_id,),
    ).fetchone()
    if not original:
        return None
    cliente, obra_referencia = original[0], original[1]

    cursor = db.execute(
        """
        INSERT INTO presupuestos (cliente, obra_referencia, estado, copiado_de_id)
        VALUES (?, ?, 'borrador', ?)
        """,
        (cliente, obra_referencia, presupuesto_id),
    )
    nuevo_presupuesto_id = cursor.lastrowid

    for tarea in listar_tareas(db, presupuesto_id):
        nueva_tarea_id = crear_tarea(db, nuevo_presupuesto_id, tarea["nombre"], orden=tarea["orden"], tipo=tarea["tipo"])
        for seccion in listar_secciones_tarea(db, tarea["id"]):
            cursor_seccion = db.execute(
                "INSERT INTO tarea_secciones (tarea_id, tipo, gg_pct, beneficio_pct, imp_pct) VALUES (?, ?, ?, ?, ?)",
                (nueva_tarea_id, seccion["tipo"], seccion["gg_pct"], seccion["beneficio_pct"], seccion["imp_pct"]),
            )
            nueva_seccion_id = cursor_seccion.lastrowid
            for item in listar_items_costo(db, seccion["id"]):
                crear_item_costo(
                    db, nueva_seccion_id, item["rubro"], item["datos"],
                    tipo_item=item["tipo_item"], perfil_id=item["perfil_id"], subtotal=item["subtotal"],
                )

    db.commit()
    return nuevo_presupuesto_id


# ─────────────────────────────────────────────────────────────────
# tareas
# ──────────────────────────────────────────────────────────────

def crear_tarea(db, presupuesto_id, nombre, orden=0, tipo=None):
    cursor = db.execute(
        "INSERT INTO tareas (presupuesto_id, nombre, tipo, orden) VALUES (?, ?, ?, ?)",
        (presupuesto_id, nombre, tipo, orden),
    )
    db.commit()
    return cursor.lastrowid


def obtener_tarea(db, tarea_id):
    row = db.execute(
        "SELECT id, presupuesto_id, nombre, orden, tipo FROM tareas WHERE id = ?",
        (tarea_id,),
    ).fetchone()
    if not row:
        return None
    return {"id": row[0], "presupuesto_id": row[1], "nombre": row[2], "orden": row[3], "tipo": row[4]}


def listar_tareas(db, presupuesto_id):
    rows = db.execute(
        "SELECT id, presupuesto_id, nombre, orden, tipo FROM tareas WHERE presupuesto_id = ? ORDER BY orden, id",
        (presupuesto_id,),
    ).fetchall()
    return [{"id": r[0], "presupuesto_id": r[1], "nombre": r[2], "orden": r[3], "tipo": r[4]} for r in rows]


def _eliminar_tarea_sin_commit(db, tarea_id):
    """Borra secciones+items de una tarea y la tarea misma, sin commitear
    (uso interno para poder encadenar varias tareas en una sola transacción)."""
    for seccion in listar_secciones_tarea(db, tarea_id):
        db.execute("DELETE FROM items_costo WHERE tarea_seccion_id = ?", (seccion["id"],))
        db.execute("DELETE FROM tarea_secciones WHERE id = ?", (seccion["id"],))
    db.execute("DELETE FROM tarea_ot_reparto WHERE tarea_id = ?", (tarea_id,))
    db.execute("DELETE FROM tareas WHERE id = ?", (tarea_id,))


def eliminar_tarea(db, tarea_id):
    """Borrado en cascada: items_costo -> tarea_secciones -> tarea."""
    _eliminar_tarea_sin_commit(db, tarea_id)
    db.commit()


# ───────────────────────────────────────────────────────────────────
# reparto tarea -> OT (tarea sin filas = "Sin OT (nivel obra)")
# ───────────────────────────────────────────────────────────────────

def listar_reparto_tareas(db, tarea_ids):
    """{tarea_id: [{"ot_id", "porcentaje"}]} en orden de carga; las tareas sin filas no aparecen."""
    tarea_ids = list(tarea_ids)
    if not tarea_ids:
        return {}
    placeholders = ",".join("?" for _ in tarea_ids)
    rows = db.execute(
        f"SELECT tarea_id, ot_id, porcentaje FROM tarea_ot_reparto WHERE tarea_id IN ({placeholders}) ORDER BY id",
        tuple(tarea_ids),
    ).fetchall()
    reparto = {}
    for tarea_id, ot_id, porcentaje in rows:
        reparto.setdefault(tarea_id, []).append({"ot_id": ot_id, "porcentaje": float(porcentaje)})
    return reparto


def reemplazar_reparto_tareas(db, reparto_por_tarea):
    """reparto_por_tarea: {tarea_id: [{"ot_id", "porcentaje"}]}. Reemplaza el reparto de
    cada tarea indicada (lista vacía = sin OT) en una sola transacción."""
    try:
        for tarea_id, filas in reparto_por_tarea.items():
            db.execute("DELETE FROM tarea_ot_reparto WHERE tarea_id = ?", (tarea_id,))
            for fila in filas:
                db.execute(
                    "INSERT INTO tarea_ot_reparto (tarea_id, ot_id, porcentaje) VALUES (?, ?, ?)",
                    (tarea_id, fila["ot_id"], fila["porcentaje"]),
                )
        db.commit()
    except Exception:
        db.rollback()
        raise


# ───────────────────────────────────────────────────────────────────
# volcado del previsto a las OT (historial en volcados_previsto[_lineas])
# ───────────────────────────────────────────────────────────────────

def _lineas_de_volcado(db, volcado_id):
    rows = db.execute(
        "SELECT ot_id, rubro, monto FROM volcados_previsto_lineas WHERE volcado_id = ? ORDER BY id",
        (volcado_id,),
    ).fetchall()
    lineas = {}
    for ot_id, rubro, monto in rows:
        lineas.setdefault(ot_id, {})[rubro] = float(monto or 0)
    return lineas


def listar_volcados(db, presupuesto_id):
    """Volcados del presupuesto, el más nuevo primero: {id, fecha, usuario, nota, lineas: {ot_id: {campo: monto}}}."""
    rows = db.execute(
        "SELECT id, fecha, usuario, nota FROM volcados_previsto WHERE presupuesto_id = ? ORDER BY id DESC",
        (presupuesto_id,),
    ).fetchall()
    return [
        {"id": r[0], "fecha": r[1], "usuario": r[2], "nota": r[3], "lineas": _lineas_de_volcado(db, r[0])}
        for r in rows
    ]


def obtener_ultimo_volcado(db, presupuesto_id):
    row = db.execute(
        "SELECT id, fecha, usuario, nota FROM volcados_previsto WHERE presupuesto_id = ? ORDER BY id DESC LIMIT 1",
        (presupuesto_id,),
    ).fetchone()
    if not row:
        return None
    return {"id": row[0], "fecha": row[1], "usuario": row[2], "nota": row[3], "lineas": _lineas_de_volcado(db, row[0])}


def aplicar_volcado_previsto(db, presupuesto_id, usuario, nota, por_ot):
    """Escribe los campos base de previsto en `economico_presupuesto` (misma tabla y columnas
    que edita el módulo económico; el resto se recalcula allí como siempre) y registra el
    volcado, todo en una sola transacción. por_ot: {ot_id: {campo: Decimal}}.
    Devuelve el id del volcado."""
    try:
        cursor = db.execute(
            "INSERT INTO volcados_previsto (presupuesto_id, usuario, nota) VALUES (?, ?, ?)",
            (presupuesto_id, usuario, nota),
        )
        volcado_id = cursor.lastrowid

        asignaciones = ", ".join(f"{campo} = ?" for campo in CAMPOS_ECONOMICOS)
        columnas = ", ".join(CAMPOS_ECONOMICOS)
        marcas = ", ".join("?" for _ in CAMPOS_ECONOMICOS)
        for ot_id, valores in por_ot.items():
            montos = [float(valores[campo]) for campo in CAMPOS_ECONOMICOS]
            if db.execute("SELECT id FROM economico_presupuesto WHERE ot_id = ?", (ot_id,)).fetchone():
                db.execute(
                    f"UPDATE economico_presupuesto SET {asignaciones}, updated_at = CURRENT_TIMESTAMP WHERE ot_id = ?",
                    (*montos, ot_id),
                )
            else:
                db.execute(
                    f"INSERT INTO economico_presupuesto (ot_id, {columnas}) VALUES (?, {marcas})",
                    (ot_id, *montos),
                )
            for campo, monto in zip(CAMPOS_ECONOMICOS, montos):
                db.execute(
                    "INSERT INTO volcados_previsto_lineas (volcado_id, ot_id, rubro, monto) VALUES (?, ?, ?, ?)",
                    (volcado_id, ot_id, campo, monto),
                )
        db.commit()
        return volcado_id
    except Exception:
        db.rollback()
        raise


# ─────────────────────────────────────────────────────────────────
# tarea_secciones — una tarea puede tener FABRICACION, MONTAJE o ambas
# (ej: "Estructura metálica" con fab+montaje; una tarea puramente de
# montaje no lleva sección de fabricación, y viceversa).
# ─────────────────────────────────────────────────────────────────

def crear_secciones_tarea(db, tarea_id, config_default, tipos=TIPOS_SECCION, nombre_tarea=None):
    """Crea las secciones indicadas en `tipos` (subconjunto de FABRICACION/MONTAJE)
    con los % default de config. Por default crea ambas.

    Sirve tanto para la creación inicial de la tarea (sin secciones todavía)
    como para agregar más adelante la sección faltante a una tarea existente
    (ej. "Chapeado" nace solo MONTAJE y después necesita también FABRICACION).
    Valida que no se dupliquen tipos ya existentes para la tarea (sección 2.1:
    una tarea no puede tener dos secciones del mismo tipo, pero sí puede tener
    solo una de las dos).

    `nombre_tarea`: si la tarea tiene % fijos definidos en
    PCTS_FABRICACION_POR_TIPO_TAREA (ej. "Chapeado", "Grating"), la sección
    FABRICACION usa esos % en vez de los defaults de config_presupuestos
    (MONTAJE sigue usando config)."""
    existentes = {s["tipo"] for s in listar_secciones_tarea(db, tarea_id)}
    ids = {}
    for tipo in tipos:
        tipo = str(tipo or "").strip().upper()
        if tipo not in TIPOS_SECCION:
            raise ValueError(f"tipo debe ser uno de {TIPOS_SECCION}")
        if tipo in existentes:
            raise ValueError(f"La tarea ya tiene una sección {tipo}.")
        sufijo = "fab" if tipo == "FABRICACION" else "mon"
        pcts_fijos = PCTS_FABRICACION_POR_TIPO_TAREA.get(nombre_tarea)
        if tipo == "FABRICACION" and pcts_fijos:
            gg_pct = pcts_fijos["gg_pct"]
            beneficio_pct = pcts_fijos["beneficio_pct"]
            imp_pct = pcts_fijos["imp_pct"]
        else:
            gg_pct = config_default.get(f"gg_pct_default_{sufijo}", 0)
            beneficio_pct = config_default.get(f"beneficio_pct_default_{sufijo}", 0)
            imp_pct = config_default.get(f"imp_pct_default_{sufijo}", 0)
        cursor = db.execute(
            """
            INSERT INTO tarea_secciones (tarea_id, tipo, gg_pct, beneficio_pct, imp_pct)
            VALUES (?, ?, ?, ?, ?)
            """,
            (tarea_id, tipo, gg_pct, beneficio_pct, imp_pct),
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

def crear_equipo(db, nombre, tarifa_dia_default=0, producto_odoo=None):
    cursor = db.execute(
        "INSERT INTO catalogo_equipos (nombre, tarifa_dia_default, producto_odoo) VALUES (?, ?, ?)",
        (nombre, tarifa_dia_default, producto_odoo),
    )
    db.commit()
    return cursor.lastrowid


def listar_equipos(db):
    rows = db.execute("SELECT id, nombre, tarifa_dia_default, producto_odoo FROM catalogo_equipos ORDER BY nombre").fetchall()
    return [{"id": r[0], "nombre": r[1], "tarifa_dia_default": r[2], "producto_odoo": r[3]} for r in rows]


def actualizar_equipo(db, equipo_id, nombre, tarifa_dia_default, producto_odoo=None):
    db.execute(
        "UPDATE catalogo_equipos SET nombre = ?, tarifa_dia_default = ?, producto_odoo = ? WHERE id = ?",
        (nombre, tarifa_dia_default, producto_odoo, equipo_id),
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
