import sqlite3

DB_PATH = r'C:\Users\usuar\OneDrive\Desktop\python\database.db'
con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# Ver duplicados exactos en TUBO CIRCULAR
cur.execute("""
    SELECT descripcion, COUNT(*) cnt, MIN(id) min_id, MAX(id) max_id
    FROM articulos_sum
    WHERE categoria='TUBO CIRCULAR'
    GROUP BY descripcion
    HAVING cnt > 1
    ORDER BY descripcion
    LIMIT 10
""")
dups = cur.fetchall()
print(f"Duplicados exactos TUBO CIRCULAR: {len(dups)}")
for r in dups[:5]:
    print(' ', r)

# Total vs unicos
cur.execute("SELECT COUNT(*) FROM articulos_sum WHERE categoria='TUBO CIRCULAR'")
total = cur.fetchone()[0]
cur.execute("SELECT COUNT(DISTINCT descripcion) FROM articulos_sum WHERE categoria='TUBO CIRCULAR'")
unicos = cur.fetchone()[0]
print(f"\nTotal: {total}  |  Únicos: {unicos}  |  Duplicados a borrar: {total - unicos}")

if dups:
    # Borrar duplicados conservando el id más alto (el importado recientemente)
    cur.execute("""
        DELETE FROM articulos_sum
        WHERE categoria='TUBO CIRCULAR'
        AND id NOT IN (
            SELECT MAX(id) FROM articulos_sum
            WHERE categoria='TUBO CIRCULAR'
            GROUP BY descripcion
        )
    """)
    print(f"Borrados: {cur.rowcount}")
    con.commit()
    cur.execute("SELECT COUNT(*) FROM articulos_sum WHERE categoria='TUBO CIRCULAR'")
    print(f"TUBO CIRCULAR final: {cur.fetchone()[0]}")

con.close()
print('OK')
