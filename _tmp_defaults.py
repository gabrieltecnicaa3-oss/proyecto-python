import sqlite3

con = sqlite3.connect("database.db")
cur = con.cursor()
for (table,) in cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"):
    for col in cur.execute(f"PRAGMA table_info({table})").fetchall():
        cid, name, col_type, notnull, default_value, pk = col
        if default_value is not None:
            print(table, name, col_type, default_value)
