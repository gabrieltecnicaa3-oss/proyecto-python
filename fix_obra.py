import sqlite3

db = sqlite3.connect("database.db")

# Corregir la obra en ordenes_trabajo de GG0-001 a GGO-001
db.execute("UPDATE ordenes_trabajo SET obra = 'GGO-001' WHERE obra = 'GG0-001'")
db.commit()

# Verificar que se actualizó
result = db.execute("SELECT id, cliente, obra FROM ordenes_trabajo WHERE obra = 'GGO-001'").fetchone()
if result:
    print(f"✓ Obra corregida: ID={result[0]}, Cliente={result[1]}, Obra={result[2]}")
else:
    print("✗ Error: No se encontró el registro actualizado")

db.close()
