import sqlite3

db = sqlite3.connect("database.db")
db.row_factory = sqlite3.Row

# Buscar todos los registros V5
print("=== REGISTROS V5 ACTUALES ===\n")
v5_all = db.execute("""
    SELECT id, posicion, ot_id, obra, proceso, estado, eliminado
    FROM procesos
    WHERE UPPER(TRIM(posicion)) = 'V5'
""").fetchall()

for rec in v5_all:
    print(f"ID: {rec['id']}, OT: {rec['ot_id']}, Obra: {rec['obra']}, Proceso: {rec['proceso']}, Estado: {rec['estado']}, Eliminado: {rec['eliminado']}")

# Obtener OT17 (BUN-012)
print("\n\n=== OT17 (BUN-012) ===\n")
ot17 = db.execute("SELECT id, obra FROM ordenes_trabajo WHERE id = 17").fetchone()
if ot17:
    print(f"OT17: Obra: {ot17['obra']}, ID: {ot17['id']}")
    
    # Buscar todas las piezas de OT17
    piezas_ot17 = db.execute("""
        SELECT DISTINCT UPPER(TRIM(posicion)) as pos FROM procesos
        WHERE ot_id = 17
        ORDER BY pos
    """).fetchall()
    
    print(f"Piezas en OT17: {[p['pos'] for p in piezas_ot17]}")

# El problema: V5 no está en OT17
# Solución: Si V5 tiene registros sin ot_id pero con obra=BUN-012, asignarle ot_id=17

print("\n\n=== DIAGNÓSTICO ===\n")
print("V5 actual está en OT16 (GGO-001)")
print("Debería estar en OT17 (BUN-012)")
print("\nSolución: Actualizar registros V5 sin ot_id pero con obra=BUN-012 para que tengan ot_id=17")

# Buscar registros V5 sin ot_id pero con obra BUN-012
v5_sin_otid = db.execute("""
    SELECT id FROM procesos
    WHERE UPPER(TRIM(posicion)) = 'V5'
    AND ot_id IS NULL
    AND obra = 'BUN-012'
""").fetchall()

if v5_sin_otid:
    print(f"\nEncontrados {len(v5_sin_otid)} registros V5 sin ot_id pero obra=BUN-012")
    print("FIX: Actualizar estos registros para que tengan ot_id=17")
    
    for rec in v5_sin_otid:
        db.execute("UPDATE procesos SET ot_id = 17 WHERE id = ?", (rec['id'],))
    
    db.commit()
    print("✅ Actualización completada")
else:
    print("\n❌ No hay registros V5 sin ot_id pero con obra=BUN-012")
    print("El problema podría ser distinto en producción")

db.close()
