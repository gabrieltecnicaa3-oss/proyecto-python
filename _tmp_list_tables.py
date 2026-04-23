import sqlite3

con = sqlite3.connect("database.db")
cur = con.cursor()
rows = cur.execute(
    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
).fetchall()
print([r[0] for r in rows])
