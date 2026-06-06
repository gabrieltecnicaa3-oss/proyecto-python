from db_utils import get_db

db = get_db()
print("=== POSICIONES EN OT 26 ===")
posiciones = db.execute("""
    SELECT DISTINCT TRIM(COALESCE(posicion, ''))
    FROM procesos
    WHERE ot_id = 26
    ORDER BY posicion
""").fetchall()

for (pos,) in posiciones:
    print(f"\nPosición: {pos}")
    procesos = db.execute("""
        SELECT proceso, estado
        FROM procesos
        WHERE ot_id = 26 AND TRIM(COALESCE(posicion, '')) = TRIM(?)
        ORDER BY id
    """, (pos,)).fetchall()
    for proc, estado in procesos:
        print(f"  {proc}: {estado}")

print("\n=== DESPACHO EN BD ===")
despachos = db.execute("""
    SELECT DISTINCT TRIM(COALESCE(posicion, ''))
    FROM procesos
    WHERE ot_id = 26 
      AND UPPER(TRIM(COALESCE(proceso, ''))) IN ('DESPACHO', 'P/DESPACHO')
    ORDER BY posicion
""").fetchall()
print(f"Piezas con DESPACHO/P/DESPACHO: {len(despachos)}")
for (pos,) in despachos:
    print(f"  {pos}")

print(f"\nTotal posiciones: {len(posiciones)}")
print(f"Con DESPACHO: {len(despachos)}")
print(f"Sin DESPACHO: {len(posiciones) - len(despachos)}")
