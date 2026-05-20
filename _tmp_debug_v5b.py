import sqlite3, os
db_path = os.path.join(os.path.abspath('.'), 'database.db')
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row
cur = conn.cursor()

print("=== Todos los procesos de V5 en OT 16 ===")
rows = cur.execute("""
    SELECT id, ot_id, posicion, proceso, estado, eliminado, fecha, reproceso, estado_pieza, re_inspeccion
    FROM procesos
    WHERE ot_id=16 AND TRIM(COALESCE(posicion,''))='V5'
    ORDER BY id
""").fetchall()
for r in rows:
    d = dict(r)
    print(d)

print("\n=== Estado de SOLDADURA para todas las piezas de OT16 ===")
rows = cur.execute("""
    SELECT TRIM(COALESCE(posicion,'')) AS pos,
           UPPER(TRIM(COALESCE(proceso,''))) AS proc,
           UPPER(TRIM(COALESCE(estado,''))) AS est,
           MAX(id) as last_id
    FROM procesos
    WHERE ot_id=16
      AND UPPER(TRIM(COALESCE(proceso,''))) = 'SOLDADURA'
      AND eliminado = 0
      AND TRIM(COALESCE(posicion,'')) <> ''
    GROUP BY TRIM(COALESCE(posicion,''))
    ORDER BY pos
""").fetchall()
for r in rows:
    print(dict(r))

print("\n=== PINTURA records de OT 16 ===")
rows = cur.execute("""
    SELECT id, posicion, proceso, estado, reproceso, eliminado
    FROM procesos
    WHERE ot_id=16
      AND UPPER(TRIM(COALESCE(proceso,''))) IN ('PINTURA','PINTURA_FONDO')
      AND eliminado=0
    ORDER BY id
""").fetchall()
for r in rows:
    print(dict(r))

conn.close()
