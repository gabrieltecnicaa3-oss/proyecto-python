import sqlite3

DB_PATH = r'C:\Users\usuar\OneDrive\Desktop\python\database.db'
con = sqlite3.connect(DB_PATH)
cur = con.cursor()

prefijos = ['IPE', 'IPN', 'IPB', 'UPN', 'HP', 'PL', 'TUBO', 'W ']
for p in prefijos:
    cur.execute(
        "SELECT COALESCE(categoria,'(null)'), COUNT(*) FROM articulos_sum "
        "WHERE descripcion LIKE ? GROUP BY categoria",
        (p + '%',)
    )
    rows = cur.fetchall()
    if rows:
        print(f"'{p}%': {rows}")
    else:
        print(f"'{p}%': sin resultados")

con.close()
