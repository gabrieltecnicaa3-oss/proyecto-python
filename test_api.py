
import sqlite3
import json
import re

db = sqlite3.connect("database.db")

# Simular la llamada a la API
ot_id = 2  # La OT para GGO-001

ot = db.execute(
    "SELECT TRIM(COALESCE(obra, '')) FROM ordenes_trabajo WHERE id = ?",
    (ot_id,)
).fetchone()

if not ot:
    print("OT no encontrada")
else:
    obra_ot = (ot[0] or "").strip()
    print(f"Obra encontrada: '{obra_ot}'")
    
    # Ejecutar la misma consulta que la API
    piezas = db.execute("""
        SELECT p.id,
               p.posicion,
               COALESCE(p.obra, ''),
               COALESCE(p.cantidad, ''),
               COALESCE(p.perfil, ''),
               COALESCE(p.peso, ''),
               COALESCE(p.descripcion, '')
        FROM procesos p
        INNER JOIN (
            SELECT posicion, obra, MAX(id) AS ultimo_id
            FROM procesos
            WHERE posicion IS NOT NULL
              AND TRIM(COALESCE(obra, '')) = ?
              AND proceso = 'DESPACHO'
              AND UPPER(TRIM(COALESCE(estado, ''))) = 'OK'
            GROUP BY posicion, obra
        ) ult ON p.id = ult.ultimo_id
    """, (obra_ot,)).fetchall()

    print(f"Piezas encontradas: {len(piezas)}")
    for p in piezas:
        print(f"  ID: {p[0]}, Pos: {p[1]}, Obra: {p[2]}, Cant: {p[3]}, Descripción: {p[6]}")

db.close()
