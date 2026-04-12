#!/usr/bin/env python3
"""Test: Verificar que generar_etiquetas_qr está guardando datos"""

import pandas as pd
import sqlite3
import os
import sys

# Agregar directorio al path para imports
sys.path.insert(0, r'c:\Users\usuar\OneDrive\Desktop\python')

def test_qr_generation():
    """Test de generación de QR"""
    
    # Crear Excel de prueba
    print("=" * 80)
    print("TEST: Generación de QR y guardado de datos")
    print("=" * 80)
    
    test_file = "test_excel.xlsx"
    
    # Crear DataFrame de prueba
    data = {
        'POS': ['C14', 'C15', 'A1'],
        'PLANO': ['PLANO-001', 'PLANO-002', 'PLANO-003'],
        'REV': ['A', 'B', 'A'],
        'OBRA': ['GG0-001', 'GG0-001', 'GG0-002'],
        'CANT': [1, 2, 5],
        'PERFIL': ['W200X26.6', 'HN100X100', 'W150X30'],
        'PESO': [10.5, 20.3, 30.1],
        'DESCRIP': ['Pieza 1', 'Pieza 2', 'Pieza 3']
    }
    
    df_test = pd.DataFrame(data)
    df_test.to_excel(test_file, index=False)
    print(f"\n✅ Excel de prueba creado: {test_file}")
    print(df_test.to_string())
    
    # Ahora intentar usar generar_etiquetas_qr
    print("\n" + "=" * 80)
    print("Ejecutando generar_etiquetas_qr()...")
    print("=" * 80)
    
    try:
        from app2 import generar_etiquetas_qr
        
        logo_path = r"C:\Users\usuar\OneDrive\Desktop\python\LOGO.png"
        if not os.path.exists(logo_path):
            print(f"⚠️ Logo no encontrado en {logo_path}, continuando sin validación de logo")
        
        # Ejecutar la función
        pdf_buffer = generar_etiquetas_qr(test_file, logo_path)
        print(f"\n✅ generar_etiquetas_qr() completada")
        print(f"   PDF size: {pdf_buffer.getbuffer().nbytes} bytes")
        
    except Exception as e:
        print(f"❌ Error en generar_etiquetas_qr(): {e}")
        import traceback
        traceback.print_exc()
    
    # Verificar datos en BD
    print("\n" + "=" * 80)
    print("Verificando datos en tabla procesos...")
    print("=" * 80)
    
    try:
        db = sqlite3.connect("database.db")
        db.row_factory = sqlite3.Row
        
        for pos in ['C14', 'C15', 'A1']:
            cursor = db.execute(f"SELECT * FROM procesos WHERE posicion='{pos}'")
            row = cursor.fetchone()
            
            if row:
                print(f"\n✅ {pos}:")
                print(f"   OBRA: {row['obra']}")
                print(f"   CANT: {row['cantidad']}")
                print(f"   PERFIL: {row['perfil']}")
            else:
                print(f"\n❌ {pos}: NO ENCONTRADO en BD")
        
        db.close()
        
    except Exception as e:
        print(f"❌ Error verificando BD: {e}")
    
    # Limpiar
    try:
        os.unlink(test_file)
        print(f"\n✅ Archivo de prueba eliminado")
    except:
        pass
    
    print("\n" + "=" * 80)
    print("FIN DEL TEST")
    print("=" * 80)

if __name__ == "__main__":
    test_qr_generation()
