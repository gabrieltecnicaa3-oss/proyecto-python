import sqlite3

DB_PATH = r'C:\Users\usuar\OneDrive\Desktop\python\database.db'
con = sqlite3.connect(DB_PATH)
cur = con.cursor()

cur.execute("""
    SELECT descripcion, COUNT(*) as cnt
    FROM articulos_sum
    WHERE descripcion LIKE 'TUBO C DIAM%'
    GROUP BY descripcion
    HAVING cnt > 1
    ORDER BY descripcion
""")
dups = cur.fetchall()
print(f"Duplicados TUBO C DIAM: {len(dups)}")
for r in dups:
    print(' ', r)

if dups:
    # Mostrar los IDs duplicados
    cur.execute("""
        SELECT id, descripcion, categoria, unidad, kg_per_m
        FROM articulos_sum
        WHERE descripcion IN (
            SELECT descripcion FROM articulos_sum
            WHERE descripcion LIKE 'TUBO C DIAM%'
            GROUP BY descripcion HAVING COUNT(*) > 1
        )
        ORDER BY descripcion, id
        LIMIT 20
    """)
    print("\nDetalle (primeros 20):")
    for r in cur.fetchall():
        print(' ', r)

con.close()
