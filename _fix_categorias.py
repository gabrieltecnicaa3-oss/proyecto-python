import sqlite3

DB_PATH = r'C:\Users\usuar\OneDrive\Desktop\python\database.db'
con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# Mapa de renombrado de categorias
RENOMBRAR = {
    'IPE':                  'PERFIL IPE',
    'IPN':                  'PERFIL IPN',
    'PL':                   'PLANCHUELA',
    'RD':                   'REDONDO',
    'TUBO':                 'TUBO RECTANGULAR',
    'TUBO C':               'TUBO CIRCULAR',
    'UPN':                  'PERFIL UPN',
    'TABLA DE PERFILES UPN':'PERFIL UPN',
    'W':                    'PERFIL W',
}

for viejo, nuevo in RENOMBRAR.items():
    cur.execute("UPDATE articulos_sum SET categoria=? WHERE categoria=?", (nuevo, viejo))
    if cur.rowcount:
        print(f"  {viejo!r:30s} -> {nuevo!r}  ({cur.rowcount} filas)")

# Eliminar articulos cuya descripcion empieza con Ø
cur.execute("SELECT id, descripcion, categoria FROM articulos_sum WHERE descripcion LIKE '\u00d8%'")
a_borrar = cur.fetchall()
print(f"\nEliminando {len(a_borrar)} articulos que empiezan con Ø:")
for row in a_borrar[:5]:
    print(' ', row)
if len(a_borrar) > 5:
    print(f'  ... y {len(a_borrar)-5} mas')

cur.execute("DELETE FROM articulos_sum WHERE descripcion LIKE '\u00d8%'")

con.commit()

# Resumen final
print("\nCategorias finales:")
cur.execute("SELECT COALESCE(categoria,'(null)'), COUNT(*) FROM articulos_sum GROUP BY categoria ORDER BY categoria")
for r in cur.fetchall():
    print(' ', r)

con.close()
print('\nOK')
