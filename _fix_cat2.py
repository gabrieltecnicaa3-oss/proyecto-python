import sqlite3

DB_PATH = r'C:\Users\usuar\OneDrive\Desktop\python\database.db'
con = sqlite3.connect(DB_PATH)
cur = con.cursor()

RENOMBRAR = {
    'TUBO':  'TUBO RECTANGULAR',
    'HP':    'PERFIL W HP',
    'IPB':   'PERFIL IPB',
    'IPE':   'PERFIL IPE',
    'IPN':   'PERFIL IPN',
    'PL':    'PLANCHUELA',
    'UPN':   'PERFIL UPN',
    'W':     'PERFIL W',
}

for viejo, nuevo in RENOMBRAR.items():
    cur.execute("UPDATE articulos_sum SET categoria=? WHERE categoria=?", (nuevo, viejo))
    print(f"  {viejo!r:8s} -> {nuevo!r:20s}  ({cur.rowcount} filas)")

con.commit()

print("\nCategorias finales:")
cur.execute("SELECT COALESCE(categoria,'(null)'), COUNT(*) FROM articulos_sum GROUP BY categoria ORDER BY categoria")
for r in cur.fetchall():
    print(' ', r)

con.close()
print('OK')
