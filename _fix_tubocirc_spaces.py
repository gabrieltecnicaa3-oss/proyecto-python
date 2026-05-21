import sqlite3

DB_PATH = r'C:\Users\usuar\OneDrive\Desktop\python\database.db'
con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# Contar los sin espacio
cur.execute("""
    SELECT COUNT(*) FROM articulos_sum
    WHERE categoria='TUBO CIRCULAR'
      AND descripcion NOT LIKE 'TUBO C DIAM %'
""")
sin_espacio = cur.fetchone()[0]
print(f"Sin espacio ('TUBO C DIAMxx'): {sin_espacio}")

# Normalizar: agregar espacio en 'TUBO C DIAM' + digito
import re
cur.execute("""
    SELECT id, descripcion FROM articulos_sum
    WHERE categoria='TUBO CIRCULAR'
      AND descripcion NOT LIKE 'TUBO C DIAM %'
""")
rows = cur.fetchall()
print(f"Normalizando {len(rows)} articulos...")

for rid, desc in rows:
    # 'TUBO C DIAM12,70 x 0,70' -> 'TUBO C DIAM 12,70 x 0,70'
    new_desc = re.sub(r'TUBO C DIAM(\d)', r'TUBO C DIAM \1', desc)
    cur.execute("UPDATE articulos_sum SET descripcion=?, codigo=? WHERE id=?", (new_desc, new_desc, rid))

print("Normalizado. Buscando duplicados exactos...")

# Borrar duplicados: conservar el de id mas alto (más reciente / con kg_per_m del Excel)
cur.execute("""
    SELECT descripcion, COUNT(*) cnt
    FROM articulos_sum
    WHERE categoria='TUBO CIRCULAR'
    GROUP BY descripcion
    HAVING cnt > 1
""")
dups = cur.fetchall()
print(f"Duplicados exactos ahora: {len(dups)}")

if dups:
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
print("OK")
