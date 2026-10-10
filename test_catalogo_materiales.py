"""Regresiones de la sincronizacion del catalogo publicado."""
import csv
import sqlite3
import unittest
from unittest.mock import patch

from catalogo_materiales import (
    CATALOGO_PATH, LPN_PULGADAS_A_MM, sincronizar_catalogo_materiales,
)


class CatalogoMaterialesTests(unittest.TestCase):
    def setUp(self):
        self.db = sqlite3.connect(":memory:")
        self.addCleanup(self.db.close)

    def test_catalogo_completo_y_superficies_de_perfiles(self):
        sincronizar_catalogo_materiales(self.db)
        self.assertEqual(self.db.execute("SELECT COUNT(*) FROM articulos_sum").fetchone()[0], 1155)
        self.assertEqual(self.db.execute(
            "SELECT COUNT(*) FROM articulos_sum WHERE categoria LIKE 'CH %'"
        ).fetchone()[0], 29)
        self.assertEqual(self.db.execute(
            "SELECT COUNT(*) FROM articulos_sum WHERE categoria LIKE 'GRA %'"
        ).fetchone()[0], 32)
        self.assertEqual(self.db.execute("""
            SELECT COUNT(*) FROM articulos_sum
            WHERE categoria NOT LIKE 'CH %' AND categoria NOT LIKE 'GRA %'
              AND COALESCE(m2_per_m, 0) <= 0
        """).fetchone()[0], 0)
        with CATALOGO_PATH.open(encoding="utf-8-sig", newline="") as archivo:
            for fila in csv.DictReader(archivo, delimiter=";"):
                valores = self.db.execute("""
                    SELECT unidad, categoria, kg_per_m, m2_per_m
                    FROM articulos_sum WHERE descripcion = ?
                """, (fila["descripcion"],)).fetchone()
                self.assertEqual(valores, (
                    fila["unidad"], fila["categoria"],
                    float(fila["kg_per_m"]) if fila["kg_per_m"] else None,
                    float(fila["m2_per_m"]) if fila["m2_per_m"] else None,
                ))

    def test_conserva_ids_codigos_referencias_y_articulos_adicionales(self):
        self.db.execute("""
            CREATE TABLE articulos_sum (
                id INTEGER PRIMARY KEY, codigo TEXT, descripcion TEXT,
                unidad TEXT, categoria TEXT, activo INTEGER
            )
        """)
        self.db.execute("""
            INSERT INTO articulos_sum VALUES
                (50, 'LPN-WEB', ' lpn 32x3,2 ', 'u', 'Anterior', 1),
                (51, 'LPN-DUP', 'LPN 32x3,2', 'u', 'Anterior', 0),
                (90, 'EXTRA', 'Material propio web', 'u', 'Especial', 1)
        """)
        self.db.execute("CREATE TABLE referencias (articulo_id INTEGER)")
        self.db.execute("INSERT INTO referencias VALUES (50)")
        self.db.commit()
        sincronizar_catalogo_materiales(self.db)
        self.assertEqual(self.db.execute(
            "SELECT codigo, activo, kg_per_m, m2_per_m FROM articulos_sum WHERE id=50"
        ).fetchone(), ("LPN-WEB", 1, 1.55, 0.128))
        self.assertEqual(self.db.execute(
            "SELECT codigo, activo, kg_per_m, m2_per_m FROM articulos_sum WHERE id=51"
        ).fetchone(), ("LPN-DUP", 0, 1.55, 0.128))
        self.assertEqual(self.db.execute(
            "SELECT COUNT(*) FROM referencias r JOIN articulos_sum a ON a.id=r.articulo_id"
        ).fetchone()[0], 1)
        self.assertEqual(self.db.execute(
            "SELECT descripcion FROM articulos_sum WHERE id=90"
        ).fetchone()[0], "Material propio web")

    def test_version_no_reaplica_ni_pisa_ediciones_posteriores(self):
        sincronizar_catalogo_materiales(self.db)
        self.db.execute("UPDATE articulos_sum SET m2_per_m=9 WHERE descripcion='LPN 32x3,2'")
        self.db.commit()
        sincronizar_catalogo_materiales(self.db)
        self.assertEqual(self.db.execute(
            "SELECT m2_per_m FROM articulos_sum WHERE descripcion='LPN 32x3,2'"
        ).fetchone()[0], 9)
        self.assertEqual(self.db.execute(
            "SELECT COUNT(*) FROM catalogo_materiales_versiones"
        ).fetchone()[0], 2)
        self.assertEqual(self.db.execute(
            "SELECT COUNT(*) FROM articulos_sum"
        ).fetchone()[0], 1155)

    def test_presupuestos_carga_catalogo_sin_abrir_suministros(self):
        from flask import Flask
        from presupuestos import presupuestos_bp
        from presupuestos import routes

        app = Flask(__name__)
        app.register_blueprint(presupuestos_bp)
        with patch.object(routes, "get_db", return_value=self.db):
            respuesta = app.test_client().get("/modulo/presupuestos/api/perfiles")
        self.assertEqual(respuesta.status_code, 200)
        perfiles = respuesta.get_json()["perfiles"]
        self.assertEqual(len(perfiles), 1155)
        self.assertEqual(sum(p["categoria"].startswith("CH ") for p in perfiles), 29)
        self.assertEqual(sum(p["categoria"].startswith("GRA ") for p in perfiles), 32)

    def test_fallo_revierte_datos_y_no_marca_version(self):
        sincronizar_catalogo_materiales(self.db)
        self.db.execute("DELETE FROM catalogo_materiales_versiones")
        self.db.execute("UPDATE articulos_sum SET m2_per_m=9")
        self.db.execute("""
            CREATE TRIGGER impedir_update BEFORE UPDATE ON articulos_sum
            WHEN NEW.descripcion = 'LPN 32x3,2'
            BEGIN SELECT RAISE(ABORT, 'fallo de prueba'); END
        """)
        self.db.commit()
        with self.assertRaisesRegex(sqlite3.IntegrityError, "fallo de prueba"):
            sincronizar_catalogo_materiales(self.db)
        self.assertEqual(self.db.execute(
            "SELECT COUNT(*) FROM catalogo_materiales_versiones"
        ).fetchone()[0], 0)
        self.assertEqual(self.db.execute(
            "SELECT COUNT(*) FROM articulos_sum WHERE m2_per_m=9"
        ).fetchone()[0], 1155)

    def test_elimina_lpn_pulgadas_y_reasigna_todas_las_referencias(self):
        import json
        from articulos_seed import ARTICULOS_SEED

        sincronizar_catalogo_materiales(self.db)
        self.db.execute("PRAGMA foreign_keys = ON")
        for tabla in ("items_op", "items_oc"):
            self.db.execute(f"""
                CREATE TABLE {tabla} (
                    articulo_id INTEGER REFERENCES articulos_sum(id)
                )
            """)
        self.db.execute("""
            CREATE TABLE items_costo (
                id INTEGER PRIMARY KEY,
                perfil_id INTEGER REFERENCES articulos_sum(id), datos TEXT
            )
        """)
        self.db.execute("""
            DELETE FROM catalogo_materiales_versiones
            WHERE version = 'lpn-pulgadas-a-mm-v1'
        """)
        anteriores = []
        for cod, desc, unidad, categoria, activo, kg in ARTICULOS_SEED:
            if desc not in LPN_PULGADAS_A_MM:
                continue
            anterior = self.db.execute("""
                INSERT INTO articulos_sum (descripcion, unidad, categoria, activo, kg_per_m)
                VALUES (?, ?, ?, ?, ?)
            """, (desc, unidad, categoria, activo, kg)).lastrowid
            nuevo, kg_mm = self.db.execute(
                "SELECT id, kg_per_m FROM articulos_sum WHERE descripcion = ?",
                (LPN_PULGADAS_A_MM[desc],),
            ).fetchone()
            self.assertEqual(kg, kg_mm)
            anteriores.append((anterior, nuevo))
            for tabla in ("items_op", "items_oc"):
                self.db.execute(f"INSERT INTO {tabla} VALUES (?)", (anterior,))
            self.db.execute(
                "INSERT INTO items_costo (perfil_id, datos) VALUES (?, ?)",
                (anterior, json.dumps({"perfil_id": str(anterior), "cantidad": 3})),
            )
        self.assertEqual(len(anteriores), 24)
        self.db.execute(
            "INSERT INTO items_costo (datos) VALUES (?)",
            (json.dumps({"perfil_id": anteriores[0][0]}),),
        )
        self.db.commit()
        sincronizar_catalogo_materiales(self.db)

        self.assertEqual(self.db.execute(
            "SELECT COUNT(*) FROM articulos_sum"
        ).fetchone()[0], 1155)
        for tabla in ("items_op", "items_oc"):
            self.assertEqual(
                [r[0] for r in self.db.execute(f"SELECT articulo_id FROM {tabla}")],
                [nuevo for _, nuevo in anteriores],
            )
        items = self.db.execute("SELECT perfil_id, datos FROM items_costo ORDER BY id").fetchall()
        for (perfil_id, datos), (_, nuevo) in zip(items, anteriores):
            self.assertEqual(perfil_id, nuevo)
            self.assertEqual(json.loads(datos), {"perfil_id": nuevo, "cantidad": 3})
        self.assertEqual(json.loads(items[-1][1])["perfil_id"], anteriores[0][1])
        self.assertEqual(self.db.execute("PRAGMA foreign_key_check").fetchall(), [])

    def test_eliminacion_lpn_fallida_revierte_referencias(self):
        sincronizar_catalogo_materiales(self.db)
        self.db.execute("""
            DELETE FROM catalogo_materiales_versiones
            WHERE version = 'lpn-pulgadas-a-mm-v1'
        """)
        anterior = self.db.execute("""
            INSERT INTO articulos_sum (descripcion) VALUES ('LPN 2"x 1/8"')
        """).lastrowid
        self.db.execute("CREATE TABLE items_op (articulo_id INTEGER)")
        self.db.execute("INSERT INTO items_op VALUES (?)", (anterior,))
        self.db.execute("""
            CREATE TRIGGER impedir_borrado BEFORE DELETE ON articulos_sum
            BEGIN SELECT RAISE(ABORT, 'no borrar'); END
        """)
        self.db.commit()
        with self.assertRaisesRegex(sqlite3.IntegrityError, "no borrar"):
            sincronizar_catalogo_materiales(self.db)
        self.assertEqual(self.db.execute(
            "SELECT articulo_id FROM items_op"
        ).fetchone()[0], anterior)
        self.assertIsNotNone(self.db.execute(
            "SELECT id FROM articulos_sum WHERE id = ?", (anterior,)
        ).fetchone())
        self.assertIsNone(self.db.execute("""
            SELECT version FROM catalogo_materiales_versiones
            WHERE version = 'lpn-pulgadas-a-mm-v1'
        """).fetchone())

    def test_eliminacion_lpn_admite_ordenes_compra_legacy_sin_articulo_id(self):
        sincronizar_catalogo_materiales(self.db)
        self.db.execute("""
            DELETE FROM catalogo_materiales_versiones
            WHERE version = 'lpn-pulgadas-a-mm-v1'
        """)
        self.db.execute("CREATE TABLE items_oc (id INTEGER PRIMARY KEY, descripcion TEXT)")
        self.db.execute("INSERT INTO items_oc VALUES (1, 'Compra historica')")
        self.db.execute("""
            INSERT INTO articulos_sum (descripcion) VALUES ('LPN 2"x 1/8"')
        """)
        self.db.commit()
        sincronizar_catalogo_materiales(self.db)
        self.assertEqual(self.db.execute(
            "SELECT * FROM items_oc"
        ).fetchall(), [(1, "Compra historica")])
        self.assertEqual(self.db.execute(
            "SELECT COUNT(*) FROM articulos_sum"
        ).fetchone()[0], 1155)


if __name__ == "__main__":
    unittest.main()
