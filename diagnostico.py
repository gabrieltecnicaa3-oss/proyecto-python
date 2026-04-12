#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Diagnosticar tabla procesos - ver qué datos hay cargados
"""
import sqlite3
import os

db_path = r'c:\Users\usuar\OneDrive\Desktop\python\database.db'

if os.path.exists(db_path):
    print("=" * 80)
    print("DIAGNÓSTICO: Tabla PROCESOS")
    print("=" * 80)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # 1. Verificar estructura
    print("\n1️⃣  ESTRUCTURA DE LA TABLA")
    print("-" * 80)
    cursor.execute("PRAGMA table_info(procesos)")
    columns = cursor.fetchall()
    for col in columns:
        print(f"  {col[1]:20} ({col[2]})")
    
    # 2. Contar registros
    print("\n2️⃣  CANTIDAD DE REGISTROS")
    print("-" * 80)
    cursor.execute("SELECT COUNT(*) FROM procesos")
    count = cursor.fetchone()[0]
    print(f"  Total: {count} registros")
    
    # 3. Ver primeros registros
    print("\n3️⃣  PRIMEROS 5 REGISTROS")
    print("-" * 80)
    cursor.execute("""
        SELECT id, posicion, obra, cantidad, perfil, peso, descripcion 
        FROM procesos 
        WHERE posicion IS NOT NULL 
        LIMIT 5
    """)
    rows = cursor.fetchall()
    if rows:
        for row in rows:
            print(f"\n  ID: {row[0]}")
            print(f"    - Posición: {row[1]}")
            print(f"    - Obra: {row[2]}")
            print(f"    - Cantidad: {row[3]}")
            print(f"    - Perfil: {row[4]}")
            print(f"    - Peso: {row[5]}")
            print(f"    - Descripción: {row[6]}")
    else:
        print("  ⚠️  No hay registros con posición")
    
    # 4. Verificar qué campos están vacíos
    print("\n4️⃣  CAMPOS VACÍOS")
    print("-" * 80)
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN posicion IS NULL THEN 1 END) as posicion_null,
            COUNT(CASE WHEN obra IS NULL THEN 1 END) as obra_null,
            COUNT(CASE WHEN cantidad IS NULL THEN 1 END) as cantidad_null,
            COUNT(CASE WHEN perfil IS NULL THEN 1 END) as perfil_null,
            COUNT(CASE WHEN peso IS NULL THEN 1 END) as peso_null,
            COUNT(CASE WHEN descripcion IS NULL THEN 1 END) as descripcion_null
        FROM procesos
    """)
    nulls = cursor.fetchone()
    print(f"  posicion NULL: {nulls[0]}/{count}")
    print(f"  obra NULL: {nulls[1]}/{count}")
    print(f"  cantidad NULL: {nulls[2]}/{count}")
    print(f"  perfil NULL: {nulls[3]}/{count}")
    print(f"  peso NULL: {nulls[4]}/{count}")
    print(f"  descripcion NULL: {nulls[5]}/{count}")
    
    conn.close()
    
    print("\n" + "=" * 80)
    print("INTERPRETACIÓN")
    print("=" * 80)
    if count == 0:
        print("❌ No hay registros en procesos")
        print("   → Necesitas cargar datos via Excel o importación")
    elif nulls[0] == count:
        print("❌ Todos los registros tienen posicion NULL")
        print("   → Necesitas cargar datos con posiciones")
    else:
        print("✅ Hay registros con posiciones")
        print("   → El problema está en el API endpoint o los nuevos campos")
else:
    print(f"❌ Database no encontrada: {db_path}")
