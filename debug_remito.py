import sqlite3

db = sqlite3.connect("database.db")
db.row_factory = sqlite3.Row

# Verificar la OT GGO-001
print("=== ORDENES_TRABAJO ===")
ot = db.execute("SELECT * FROM ordenes_trabajo WHERE obra = 'GGO-001' OR obra LIKE '%GGO%' LIMIT 5").fetchall()
if not ot:
    print("NO HAY ORDENES_TRABAJO PARA GGO-001")
    print("\n=== PRIMERAS 5 ORDENES_TRABAJO ===")
    ots_all = db.execute("SELECT id, cliente, obra FROM ordenes_trabajo LIMIT 5").fetchall()
    for row in ots_all:
        print(dict(row))
else:
    for row in ot:
        print(dict(row))

print("\n=== PROCESOS para GGO-001 ===")
procs = db.execute("""
    SELECT id, posicion, obra, proceso, estado, descripcion 
    FROM procesos 
    WHERE TRIM(COALESCE(obra, '')) = 'GGO-001'
    ORDER BY proceso, id
""").fetchall()

for row in procs:
    print(dict(row))

print("\n=== ÚLTIMOS PROCESOS DE DESPACHO PARA GGO-001 ===")
despachos = db.execute("""
    SELECT id, posicion, obra, proceso, estado
    FROM procesos
    WHERE TRIM(COALESCE(obra, '')) = 'GGO-001' AND proceso = 'DESPACHO'
    ORDER BY id DESC
    LIMIT 10
""").fetchall()

print(f"Total: {len(despachos)}")
for row in despachos:
    print(f"ID: {row[0]}, POS: {row[1]}, OBRA: '{row[2]}', PROCESO: {row[3]}, ESTADO: '{row[4]}'")

print("\n=== PROCESOS DE DESPACHO CON ESTADO OK PARA GGO-001 ===")
ok_despachos = db.execute("""
    SELECT id, posicion, obra, proceso, estado
    FROM procesos
    WHERE TRIM(COALESCE(obra, '')) = 'GGO-001' AND proceso = 'DESPACHO'
      AND UPPER(TRIM(COALESCE(estado, ''))) = 'OK'
    ORDER BY id DESC
    LIMIT 10
""").fetchall()

print(f"Total: {len(ok_despachos)}")
for row in ok_despachos:
    print(f"ID: {row[0]}, POS: {row[1]}, OBRA: '{row[2]}', PROCESO: {row[3]}, ESTADO: '{row[4]}'")

db.close()
