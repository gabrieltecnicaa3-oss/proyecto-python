import sqlite3

DB_PATH = r'C:\Users\usuar\OneDrive\Desktop\python\database.db'
con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# Mostrar articulos con doble espacio o espacio raro en la descripcion
cur.execute("""
    SELECT id, descripcion, categoria, unidad, kg_per_m
    FROM articulos_sum
    WHERE descripcion LIKE 'TUBO C DIAM %'
    ORDER BY descripcion
    LIMIT 10
""")
print("Con 'TUBO C DIAM  ' (doble espacio o espacio extra):")
for r in cur.fetchall():
    print(' ', r)

# Comparar normalizando espacios
cur.execute("""
    SELECT TRIM(REPLACE(descripcion,'  ',' ')) as norm, COUNT(*) cnt
    FROM articulos_sum
    WHERE categoria = 'TUBO CIRCULAR'
    GROUP BY norm
    HAVING cnt > 1
    ORDER BY norm
    LIMIT 10
""")
dups = cur.fetchall()
print(f"\nDuplicados al normalizar espacios: {len(dups)}")
for r in dups[:10]:
    print(' ', r)

# Ver ejemplos concretos de pares con/sin espacio
cur.execute("""
    SELECT a.id, a.descripcion, b.id, b.descripcion
    FROM articulos_sum a
    JOIN articulos_sum b ON TRIM(REPLACE(a.descripcion,'  ',' ')) = TRIM(REPLACE(b.descripcion,'  ',' '))
        AND a.id < b.id
    WHERE a.categoria = 'TUBO CIRCULAR'
    LIMIT 10
""")
pairs = cur.fetchall()
print(f"\nPares duplicados (ids distintos, descripcion equiv): {len(pairs)}")
for r in pairs:
    print(f'  id={r[0]} |{r[1]}|')
    print(f'  id={r[2]} |{r[3]}|')
    print()

con.close()
