#!/usr/bin/env python3
"""Script para verificar contenido de tabla procesos"""

import sqlite3
import sys

def verificar_procesos():
    """Muestra contenido de tabla procesos"""
    try:
        db = sqlite3.connect("database.db")
        db.row_factory = sqlite3.Row
        
        # Query para la posición C14
        cursor = db.execute("""
            SELECT * FROM procesos WHERE posicion='C14' ORDER BY id
        """)
        
        rows = cursor.fetchall()
        
        print("=" * 100)
        print(f"BÚSQUEDA: Posición C14")
        print("=" * 100)
        
        if not rows:
            print("❌ NO HAY REGISTROS para C14")
        else:
            print(f"✅ Encontradas {len(rows)} fila(s) para C14\n")
            
            for i, row in enumerate(rows):
                print(f"Fila {i+1} (ID={row['id']}):")
                print(f"  posicion:   {row['posicion']}")
                print(f"  obra:       {row['obra']}")
                print(f"  cantidad:   {row['cantidad']}")
                print(f"  perfil:     {row['perfil']}")
                print(f"  peso:       {row['peso']}")
                print(f"  descripcion:{row['descripcion']}")
                print(f"  proceso:    {row['proceso']}")
                print(f"  fecha:      {row['fecha']}")
                print(f"  operario:   {row['operario']}")
                print(f"  estado:     {row['estado']}")
                print(f"  reproceso:  {row['reproceso']}")
                print()
        
        # Mostrar todas las posiciones con datos
        print("=" * 100)
        print("TODAS LAS POSICIONES CON DATOS (obra NOT NULL)")
        print("=" * 100)
        
        cursor = db.execute("""
            SELECT DISTINCT posicion, obra, cantidad, perfil 
            FROM procesos 
            WHERE obra IS NOT NULL 
            ORDER BY posicion
        """)
        
        todas = cursor.fetchall()
        if not todas:
            print("❌ No hay posiciones con datos de obra")
        else:
            print(f"✅ {len(todas)} posiciones con datos:\n")
            for row in todas:
                print(f"  {row['posicion']:10} | OBRA: {row['obra']:15} | CANT: {row['cantidad']} | PERFIL: {row['perfil']}")
        
        db.close()
        return len(rows) > 0
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    existe = verificar_procesos()
    sys.exit(0 if existe else 1)
