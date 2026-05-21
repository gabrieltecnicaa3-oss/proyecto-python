import sqlite3

DB_PATH = r'C:\Users\usuar\OneDrive\Desktop\python\database.db'
con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# Ver categorias actuales
cur.execute("SELECT COALESCE(categoria,'(null)'), COUNT(*) FROM articulos_sum GROUP BY categoria ORDER BY categoria")
print("Categorias actuales:")
for r in cur.fetchall():
    print(' ', r)

# Articulos que empiezan con Ø
cur.execute("SELECT COUNT(*) FROM articulos_sum WHERE descripcion LIKE '\u00d8%'")
print("Con Ø al inicio:", cur.fetchone()[0])

con.close()
