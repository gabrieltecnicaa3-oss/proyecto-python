import sqlite3, os
db_path = os.path.join(os.path.abspath('.'), 'database.db')
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=== OTs activas ===")
rows = cur.execute("SELECT id, obra, titulo, fecha_cierre FROM ordenes_trabajo WHERE fecha_cierre IS NULL ORDER BY id DESC LIMIT 20").fetchall()
for r in rows:
    print(dict(r))

print("\n=== Posiciones con 'V5' en procesos ===")
rows = cur.execute("SELECT id, ot_id, posicion, proceso, estado, eliminado FROM procesos WHERE TRIM(COALESCE(posicion,'')) LIKE '%V5%' ORDER BY ot_id, id").fetchall()
for r in rows:
    print(dict(r))

conn.close()
