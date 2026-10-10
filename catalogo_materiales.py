"""Sincroniza el catalogo local publicado sin reemplazar IDs existentes."""
import csv
import hashlib
from pathlib import Path


CATALOGO_PATH = Path(__file__).with_name("catalogo_local_importar.csv")


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
                    """, (fila["unidad"], fila["categoria"], kg_m, m2_m, articulo_id))
            else:
                cursor = db.execute("""
                    INSERT INTO articulos_sum (
                        codigo, descripcion, unidad, categoria, activo, kg_per_m, m2_per_m
                    ) VALUES (?, ?, ?, ?, 1, ?, ?)
                """, (fila["codigo"] or None, descripcion, fila["unidad"],
                      fila["categoria"], kg_m, m2_m))
                existentes[descripcion.casefold()] = [cursor.lastrowid]

        db.execute(
            "INSERT INTO catalogo_materiales_versiones (version) VALUES (?)", (version,)
        )
        db.commit()
    except Exception:
        db.rollback()
        raise
