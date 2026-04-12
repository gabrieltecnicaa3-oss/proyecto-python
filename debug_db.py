#!/usr/bin/env python3
"""Debug: Verificar contenido de tabla procesos"""

import sqlite3

def debug_procesos():
    """Muestra el contenido actual de la tabla procesos"""
    db = sqlite3.connect("database.db")
    db.row_factory = sqlite3.Row
    
    # Obtener estructura de tabla
    cursor = db.execute("PRAGMA table_info(procesos)")
    columnas = [(row[1], row[2]) for row in cursor.fetchall()]
    
    print("=" * 80)
    print("ESTRUCTURA DE TABLA 'procesos'")
    print("=" * 80)
    for idx, (col, tipo) in enumerate(columnas):
        print(f"  [{idx}] {col:20} {tipo}")
    
    # Obtener datos
    cursor = db.execute("SELECT * FROM procesos ORDER BY id DESC LIMIT 10")
    rows = cursor.fetchall()
    
    print("\n" + "=" * 80)
    print("ÚLTIMOS 10 REGISTROS")
    print("=" * 80)
    
    for row in rows:
        print(f"\nID: {row[0]}")
        for idx, (col, _) in enumerate(columnas):
            val = row[idx]
            print(f"  [{idx}] {col:20} = {repr(val)}")
    
    print("\n" + "=" * 80)
    print("POSICIONES CON DATOS INICIALES (obra != NULL)")
    print("=" * 80)
    
    cursor = db.execute("""
        SELECT posicion, COUNT(*) as filas, GROUP_CONCAT(proceso) as procesos
        FROM procesos
        WHERE obra IS NOT NULL
        GROUP BY posicion
        ORDER BY posicion
    """)
    
    for row in cursor.fetchall():
        print(f"  POS: {row[0]:15} FILAS: {row[1]} PROCESOS: {row[2] or 'NINGUNO'}")
    
    db.close()

if __name__ == "__main__":
    debug_procesos()
