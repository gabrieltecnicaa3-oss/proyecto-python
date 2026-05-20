import sqlite3, os
db_path = os.path.join(os.path.abspath('.'), 'database.db')
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=== TODAS las OTs con 'V5' en procesos ===")
rows = cur.execute("""
    SELECT p.id, p.ot_id, p.posicion, p.proceso, p.estado, p.eliminado,
           ot.obra, ot.titulo, ot.fecha_cierre
    FROM procesos p
    JOIN ordenes_trabajo ot ON ot.id = p.ot_id
    WHERE TRIM(COALESCE(p.posicion,'')) = 'V5'
    ORDER BY p.ot_id, p.id
""").fetchall()
for r in rows:
    print(dict(r))

print("\n=== OT con id=5 ===")
r = cur.execute("SELECT * FROM ordenes_trabajo WHERE id=5").fetchone()
print(dict(r) if r else "No existe OT id=5")

print("\n=== Todas las OTs (incluidas cerradas) con 'V5' ===")
rows = cur.execute("""
    SELECT DISTINCT ot.id, ot.obra, ot.titulo, ot.fecha_cierre
    FROM ordenes_trabajo ot
    JOIN procesos p ON p.ot_id = ot.id
    WHERE TRIM(COALESCE(p.posicion,'')) = 'V5'
    ORDER BY ot.id
""").fetchall()
for r in rows:
    print(dict(r))

conn.close()
