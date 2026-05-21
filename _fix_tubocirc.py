import sqlite3

DB_PATH = r'C:\Users\usuar\OneDrive\Desktop\python\database.db'
con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# Restaurar TUBO C DIAM -> TUBO CIRCULAR (fueron pisados por TUBO RECTANGULAR)
cur.execute(
    "UPDATE articulos_sum SET categoria='TUBO CIRCULAR' WHERE descripcion LIKE 'TUBO C DIAM%'"
)
print(f"TUBO CIRCULAR restaurados: {cur.rowcount}")

con.commit()

print("\nCategorias finales:")
cur.execute("SELECT COALESCE(categoria,'(null)'), COUNT(*) FROM articulos_sum GROUP BY categoria ORDER BY categoria")
for r in cur.fetchall():
    print(f"  {r[0]:30s}  {r[1]}")

con.close()
print('OK')
