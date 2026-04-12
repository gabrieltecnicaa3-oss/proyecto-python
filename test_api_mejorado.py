import sqlite3
import json

db = sqlite3.connect("database.db")

# Verificar que hay piezas correctas
ot_id = 2  # GGO-001

ot = db.execute(
    "SELECT TRIM(COALESCE(obra, '')) FROM ordenes_trabajo WHERE id = ?",
    (ot_id,)
).fetchone()

if ot:
    obra_ot = (ot[0] or "").strip()
    print(f"✓ OT encontrada para: {obra_ot}")
    
    # Ejecutar la misma consulta que el API mejorado
    piezas = db.execute("""
        SELECT p_despacho.id,
               p_first.posicion,
               p_first.obra,
               COALESCE(p_first.cantidad, ''),
               COALESCE(p_first.perfil, ''),
               COALESCE(p_first.peso, ''),
               COALESCE(p_first.descripcion, '')
        FROM procesos p_despacho
        LEFT JOIN procesos p_first ON p_despacho.posicion = p_first.posicion 
                                   AND p_despacho.obra = p_first.obra
                                   AND p_first.id = (
                                       SELECT MIN(id) FROM procesos 
                                       WHERE posicion = p_despacho.posicion 
                                       AND obra = p_despacho.obra
                                   )
        WHERE TRIM(COALESCE(p_despacho.obra, '')) = ?
          AND p_despacho.proceso = 'DESPACHO'
          AND UPPER(TRIM(COALESCE(p_despacho.estado, ''))) = 'OK'
    """, (obra_ot,)).fetchall()

    print(f"✓ Piezas encontradas: {len(piezas)}")
    print("\nDatos de las piezas:")
    for p in piezas:
        print(f"  ID: {p[0]:<3} | Pos: {p[1]:<6} | Cant: {p[3]:<5} | Perfil: {p[4]:<10} | Peso: {p[5]:<8} | Desc: {p[6]}")

db.close()
