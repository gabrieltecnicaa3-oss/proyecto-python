import sqlite3, os
db_path = os.path.join(os.path.abspath('.'), 'database.db')
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=== TODAS las OTs (activas y cerradas) ===")
rows = cur.execute("SELECT id, obra, titulo, fecha_cierre FROM ordenes_trabajo ORDER BY id").fetchall()
for r in rows:
    print(dict(r))

print("\n=== TODAS las posiciones con V5 en procesos (sin importar eliminado) ===")
rows = cur.execute("""
    SELECT p.id, p.ot_id, p.posicion, p.proceso, p.estado, p.eliminado,
           ot.obra, ot.titulo, ot.fecha_cierre
    FROM procesos p
    LEFT JOIN ordenes_trabajo ot ON ot.id = p.ot_id
    WHERE TRIM(COALESCE(p.posicion,'')) = 'V5'
    ORDER BY p.ot_id, p.id
""").fetchall()
for r in rows:
    print(dict(r))

print("\n=== OTs en tabla ordenes_trabajo con id entre 1 y 10 ===")
rows = cur.execute("SELECT id, obra, titulo, fecha_cierre FROM ordenes_trabajo WHERE id BETWEEN 1 AND 10 ORDER BY id").fetchall()
for r in rows:
    print(dict(r))

conn.close()
