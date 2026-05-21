import sqlite3
con = sqlite3.connect(r'C:\Users\usuar\OneDrive\Desktop\python\database.db')
cur = con.cursor()
cur.execute("SELECT COUNT(*) FROM proveedores")
antes = cur.fetchone()[0]
cur.execute("DELETE FROM proveedores")
con.commit()
cur.execute("SELECT COUNT(*) FROM proveedores")
despues = cur.fetchone()[0]
print(f"Proveedores borrados: {antes} → {despues}")
con.close()
print("OK — la app re-insertará la nueva lista al primer request")
