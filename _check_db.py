import sqlite3
con = sqlite3.connect(r'C:\Users\usuar\OneDrive\Desktop\python\database.db')
cur = con.cursor()

cur.execute("SELECT categoria, COUNT(*) FROM articulos_sum WHERE categoria IN ('RD','TUBO C') GROUP BY categoria")
print("Conteo por categoria:", cur.fetchall())

cur.execute("SELECT id, descripcion, unidad FROM articulos_sum WHERE categoria='TUBO C' ORDER BY descripcion LIMIT 6")
print("Primeros TUBO C:")
for r in cur.fetchall():
    print(' ', r)

# Detectar duplicados exactos
cur.execute("""
    SELECT descripcion, COUNT(*) as cnt
    FROM articulos_sum
    WHERE categoria IN ('RD','TUBO C')
    GROUP BY descripcion
    HAVING cnt > 1
    LIMIT 5
""")
dups = cur.fetchall()
print("Duplicados:", len(dups), "ejemplo:", dups[:2] if dups else 'ninguno')

con.close()
