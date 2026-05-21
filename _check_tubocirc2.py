import sqlite3

DB_PATH = r'C:\Users\usuar\OneDrive\Desktop\python\database.db'
con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# Ver algunos ejemplos de los IDs bajos (articulos viejos) vs IDs altos (importados)
cur.execute("""
    SELECT id, descripcion FROM articulos_sum
    WHERE categoria='TUBO CIRCULAR'
    ORDER BY id
    LIMIT 5
""")
print("5 mas antiguos (ids bajos):")
for r in cur.fetchall():
    print(f'  id={r[0]}  |{r[1]}|')

cur.execute("""
    SELECT id, descripcion FROM articulos_sum
    WHERE categoria='TUBO CIRCULAR'
    ORDER BY id DESC
    LIMIT 5
""")
print("\n5 mas nuevos (ids altos):")
for r in cur.fetchall():
    print(f'  id={r[0]}  |{r[1]}|')

# Ver si los viejos tienen formato distinto (ej: sin espacio entre DIAM y numero)
cur.execute("""
    SELECT id, descripcion FROM articulos_sum
    WHERE categoria='TUBO CIRCULAR'
      AND descripcion NOT LIKE 'TUBO C DIAM %'
    LIMIT 10
""")
distintos = cur.fetchall()
print(f"\nArticulos TUBO CIRCULAR sin formato 'TUBO C DIAM [espacio]': {len(distintos)}")
for r in distintos[:10]:
    print(f'  id={r[0]}  |{r[1]}|')

con.close()
