import sqlite3
con = sqlite3.connect(r'C:\Users\usuar\OneDrive\Desktop\python\database.db')
cur = con.cursor()
cur.execute("SELECT descripcion FROM articulos_sum WHERE categoria='PERFILES LPN' ORDER BY descripcion")
for r in cur.fetchall(): print(r[0])
con.close()
