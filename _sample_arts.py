import sqlite3

DB_PATH = r'C:\Users\usuar\OneDrive\Desktop\python\database.db'
con = sqlite3.connect(DB_PATH)
cur = con.cursor()

print("=== TUBO CIRCULAR (primeras 5) ===")
cur.execute("SELECT id, descripcion FROM articulos_sum WHERE categoria='TUBO CIRCULAR' ORDER BY id LIMIT 5")
for r in cur.fetchall(): print(f"  id={r[0]}  |{r[1]}|")

print("\n=== PERFILES LPN (primeras 10) ===")
cur.execute("SELECT id, descripcion FROM articulos_sum WHERE categoria='PERFILES LPN' ORDER BY id LIMIT 10")
for r in cur.fetchall(): print(f"  id={r[0]}  |{r[1]}|")

con.close()
