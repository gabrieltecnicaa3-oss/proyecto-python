import sqlite3

DB_PATH = r'C:\Users\usuar\OneDrive\Desktop\python\database.db'
con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# (prefijo_descripcion, nueva_categoria)
REGLAS = [
    ('IPE',   'PERFIL IPE'),
    ('IPN',   'PERFIL IPN'),
    ('IPB',   'PERFIL IPB'),
    ('HP',    'PERFIL W HP'),
    ('W ',    'PERFIL W'),
    ('UPN',   'PERFIL UPN'),
    ('PL',    'PLANCHUELA'),
    ('TUBO',  'TUBO RECTANGULAR'),   # TUBO C DIAM ya estan en TUBO CIRCULAR
]

for prefijo, nueva_cat in REGLAS:
    cur.execute(
        "UPDATE articulos_sum SET categoria=? WHERE descripcion LIKE ? AND categoria != ?",
        (nueva_cat, prefijo + '%', nueva_cat)
    )
    if cur.rowcount:
        print(f"  '{prefijo}%'  ->  {nueva_cat!r}  ({cur.rowcount} filas)")

con.commit()

print("\nCategorias finales:")
cur.execute("SELECT COALESCE(categoria,'(null)'), COUNT(*) FROM articulos_sum GROUP BY categoria ORDER BY categoria")
for r in cur.fetchall():
    print(f"  {r[0]:30s}  {r[1]}")

con.close()
print('OK')
