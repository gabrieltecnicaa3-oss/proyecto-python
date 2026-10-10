"""Sincroniza el catalogo local publicado sin reemplazar IDs existentes."""
import csv
import hashlib
import json
from pathlib import Path


CATALOGO_PATH = Path(__file__).with_name("catalogo_local_importar.csv")

LPN_PULGADAS_A_MM = {
    'LPN 1 1/4"x 1/8"': "LPN 32x3,2",
    'LPN 1 1/4"x 3/16"': "LPN 32x4,8",
    'LPN 1 1/2"x 1/8"': "LPN 38x3,2",
    'LPN 1 1/2"x 3/16"': "LPN 38x4,8",
    'LPN 1 1/2"x 1/4"': "LPN 38x6,4",
    'LPN 1 3/4"x 1/8"': "LPN 45x3,2",
    'LPN 1 3/4"x 3/16"': "LPN 45x4,8",
    'LPN 2"x 1/8"': "LPN 51x3,2",
    'LPN 2"x 3/16"': "LPN 51x4,8",
    'LPN 2"x 1/4"': "LPN 51x6,4",
    'LPN 2 1/2"x 3/16"': "LPN 64x4,8",
    'LPN 2 1/2"x 1/4"': "LPN 64x6,4",
    'LPN 2 1/2"x 5/16"': "LPN 64x7,9",
    'LPN 2 1/2"x 3/8"': "LPN 64x9,5",
    'LPN 3"x 1/4"': "LPN 76x6,4",
    'LPN 3"x 5/16"': "LPN 76x7,9",
    'LPN 3"x 3/8"': "LPN 76x9,5",
    'LPN 3 1/2"x 1/4"': "LPN 89x6,4",
    'LPN 3 1/2"x 5/16"': "LPN 89x7,9",
    'LPN 3 1/2"x 3/8"': "LPN 89x9,5",
    'LPN 4 1/32"x 1/4"': "LPN 102x6.4",
    'LPN 4 1/32"x 5/16"': "LPN 102x7,9",
    'LPN 4 1/32"x 3/8"': "LPN 102x9,5",
    'LPN 4 1/32"x 1/2"': "LPN 102x12,7",
}


def _columna_existe(db, tabla, columna):
    try:
        db.execute(f"SELECT {columna} FROM {tabla} LIMIT 0")
        return True
    except Exception as exc:
        mensaje = str(exc).lower()
        if any(error in mensaje for error in (
            "no such table", "doesn't exist", "no such column", "unknown column",
        )):
            return False
        raise


def _actualizar_referencias_articulos(db, cambios):
    for tabla in ("items_op", "items_oc"):
        if cambios and _columna_existe(db, tabla, "articulo_id"):
            for anterior, nuevo in cambios.items():
                db.execute(
                    f"UPDATE {tabla} SET articulo_id = ? WHERE articulo_id = ?",
                    (nuevo, anterior),
                )
    if cambios and _columna_existe(db, "items_costo", "perfil_id"):
        for item_id, perfil_id, datos_json in db.execute(
            "SELECT id, perfil_id, datos FROM items_costo"
        ).fetchall():
            datos = json.loads(datos_json)
            anterior = datos.get("perfil_id")
            if isinstance(anterior, str) and anterior.isdigit():
                anterior = int(anterior)
            if anterior in cambios:
                datos["perfil_id"] = cambios[anterior]
            if perfil_id in cambios or anterior in cambios:
                db.execute(
                    "UPDATE items_costo SET perfil_id = ?, datos = ? WHERE id = ?",
                    (cambios.get(perfil_id, perfil_id),
                     json.dumps(datos, ensure_ascii=False), item_id),
                )


def _eliminar_lpn_pulgadas(db):
    version = "lpn-pulgadas-a-mm-v1"
    if db.execute(
        "SELECT version FROM catalogo_materiales_versiones WHERE version = ?", (version,)
    ).fetchone():
        return
    articulos = {}
    for articulo_id, descripcion in db.execute(
        "SELECT id, descripcion FROM articulos_sum ORDER BY id"
    ).fetchall():
        articulos.setdefault(descripcion.strip().casefold(), []).append(articulo_id)
    cambios = {}
    for pulgadas, mm in LPN_PULGADAS_A_MM.items():
        ids_pulgadas = articulos.get(pulgadas.casefold(), [])
        if not ids_pulgadas:
            continue
        ids_mm = articulos.get(mm.casefold(), [])
        if not ids_mm:
            raise ValueError(f"No existe el equivalente en mm de {pulgadas}: {mm}")
        cambios.update({articulo_id: ids_mm[0] for articulo_id in ids_pulgadas})

    try:
        _actualizar_referencias_articulos(db, cambios)
        for anterior in cambios:
            db.execute("DELETE FROM articulos_sum WHERE id = ?", (anterior,))
        db.execute(
            "INSERT INTO catalogo_materiales_versiones (version) VALUES (?)", (version,)
        )
        db.commit()
    except Exception:
        db.rollback()
        raise


def _limpiar_categoria_tubos(db):
    version = "limpieza-tubos-circulares-v1"
    if db.execute(
        "SELECT version FROM catalogo_materiales_versiones WHERE version = ?", (version,)
    ).fetchone():
        return
    articulos = db.execute("SELECT id, categoria FROM articulos_sum").fetchall()
    # Comparar en Python evita que MySQL confunda la categoria antigua con
    # el nuevo nombre por su collation insensible a mayusculas.
    eliminar = {
        articulo_id: None for articulo_id, categoria in articulos
        if (categoria or "").strip() == "TUBO CIRCULAR"
    }
    renombrar = [
        articulo_id for articulo_id, categoria in articulos
        if (categoria or "").strip().casefold() == "tubos"
    ]
    try:
        _actualizar_referencias_articulos(db, eliminar)
        for articulo_id in eliminar:
            db.execute("DELETE FROM articulos_sum WHERE id = ?", (articulo_id,))
        for articulo_id in renombrar:
            db.execute(
                "UPDATE articulos_sum SET categoria = ? WHERE id = ?",
                ("Tubo circular", articulo_id),
            )
        db.execute(
            "INSERT INTO catalogo_materiales_versiones (version) VALUES (?)", (version,)
        )
        db.commit()
    except Exception:
        db.rollback()
        raise


def sincronizar_catalogo_materiales(db):
    contenido = CATALOGO_PATH.read_bytes()
    version = hashlib.sha256(contenido).hexdigest()
    db.execute("""
        CREATE TABLE IF NOT EXISTS catalogo_materiales_versiones (
            version VARCHAR(64) PRIMARY KEY,
            fecha DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    """)
    if db.execute(
        "SELECT version FROM catalogo_materiales_versiones WHERE version = ?", (version,)
    ).fetchone():
        _eliminar_lpn_pulgadas(db)
        _limpiar_categoria_tubos(db)
        return

    db.execute("""
        CREATE TABLE IF NOT EXISTS articulos_sum (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            codigo TEXT, descripcion TEXT NOT NULL,
            unidad TEXT DEFAULT 'u', categoria TEXT, activo INTEGER DEFAULT 1,
            kg_per_m REAL, m2_per_m REAL
        )
    """)
    for columna in ("kg_per_m", "m2_per_m"):
        try:
            db.execute(f"SELECT {columna} FROM articulos_sum LIMIT 0")
        except Exception as exc:
            mensaje = str(exc).lower()
            if "no such column" not in mensaje and "unknown column" not in mensaje:
                raise
            db.execute(f"ALTER TABLE articulos_sum ADD COLUMN {columna} REAL")

    filas = list(csv.DictReader(contenido.decode("utf-8-sig").splitlines(), delimiter=";"))
    existentes = {}
    for articulo_id, descripcion in db.execute("SELECT id, descripcion FROM articulos_sum").fetchall():
        existentes.setdefault(descripcion.strip().casefold(), []).append(articulo_id)

    try:
        for fila in filas:
            descripcion = fila["descripcion"].strip()
            categoria = fila["categoria"]
            if categoria.strip().casefold() == "tubos":
                categoria = "Tubo circular"
            kg_m = float(fila["kg_per_m"]) if fila["kg_per_m"] else None
            m2_m = float(fila["m2_per_m"]) if fila["m2_per_m"] else None
            ids = existentes.get(descripcion.casefold(), [])
            if ids:
                for articulo_id in ids:
                    db.execute("""
                        UPDATE articulos_sum
                        SET unidad = ?, categoria = ?,
                            kg_per_m = ?, m2_per_m = ?
                        WHERE id = ?
                    """, (fila["unidad"], categoria, kg_m, m2_m, articulo_id))
            else:
                cursor = db.execute("""
                    INSERT INTO articulos_sum (
                        codigo, descripcion, unidad, categoria, activo, kg_per_m, m2_per_m
                    ) VALUES (?, ?, ?, ?, 1, ?, ?)
                """, (fila["codigo"] or None, descripcion, fila["unidad"],
                      categoria, kg_m, m2_m))
                existentes[descripcion.casefold()] = [cursor.lastrowid]

        db.execute(
            "INSERT INTO catalogo_materiales_versiones (version) VALUES (?)", (version,)
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
    _eliminar_lpn_pulgadas(db)
    _limpiar_categoria_tubos(db)
