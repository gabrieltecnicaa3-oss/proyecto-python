import sqlite3

con = sqlite3.connect("database.db")
cur = con.cursor()
for (table,) in cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"):
    for col in cur.execute(f"PRAGMA table_info({table})").fetchall():
        cid, name, col_type, notnull, default_value, pk = col
        if name == "fecha_creacion":
            print(table, col)
