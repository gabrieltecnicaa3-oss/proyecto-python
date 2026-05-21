import sqlite3

DB_PATH = r'C:\Users\usuar\OneDrive\Desktop\python\database.db'

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# 1) Todas las unidades a 'm'
cur.execute("UPDATE articulos_sum SET unidad='m' WHERE categoria IN ('RD','TUBO C')")
print('unidad=m actualizados:', cur.rowcount)

# 2) Renombrar 'TUBO C Ø...' -> 'TUBO C DIAM...'
cur.execute("SELECT id, codigo, descripcion FROM articulos_sum WHERE descripcion LIKE 'TUBO C \u00d8%'")
rows = cur.fetchall()
print('Articulos con Ø:', len(rows))
for rid, cod, desc in rows:
    new_desc = desc.replace('TUBO C \u00d8', 'TUBO C DIAM')
    new_cod  = (cod or '').replace('TUBO C \u00d8', 'TUBO C DIAM')
    cur.execute('UPDATE articulos_sum SET descripcion=?, codigo=? WHERE id=?', (new_desc, new_cod, rid))

con.commit()

cur.execute("SELECT COUNT(*) FROM articulos_sum WHERE descripcion LIKE 'TUBO C DIAM%'")
print('TUBO C DIAM total:', cur.fetchone()[0])
cur.execute("SELECT COUNT(*) FROM articulos_sum WHERE descripcion LIKE 'TUBO C \u00d8%'")
print('TUBO C Ø restantes:', cur.fetchone()[0])

con.close()
print('OK')
