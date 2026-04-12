#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para migrar la base de datos y agregar columnas faltantes
"""

import sqlite3
import sys

try:
    print("Conectando a la base de datos...")
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()
    
    # Verificar estructura actual
    print("\nEstructura actual de 'ordenes_trabajo':")
    cursor.execute("PRAGMA table_info(ordenes_trabajo);")
    columns = cursor.fetchall()
    existing_columns = {col[1] for col in columns}
    
    for col in columns:
        print(f"  - {col[1]} ({col[2]})")
    
    # Agregar columna estado_avance si no existe
    if 'estado_avance' not in existing_columns:
        print("\n✓ Agregando columna 'estado_avance'...")
        cursor.execute("ALTER TABLE ordenes_trabajo ADD COLUMN estado_avance INTEGER DEFAULT 0;")
        conn.commit()
        print("  ✓ Columna agregada exitosamente")
    else:
        print("\n✓ La columna 'estado_avance' ya existe")
    
    # Agregar columna fecha_creacion si no existe
    if 'fecha_creacion' not in existing_columns:
        print("\n✓ Agregando columna 'fecha_creacion'...")
        cursor.execute("ALTER TABLE ordenes_trabajo ADD COLUMN fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP;")
        conn.commit()
        print("  ✓ Columna agregada exitosamente")
    else:
        print("\n✓ La columna 'fecha_creacion' ya existe")
    
    # Mostrar estructura final
    print("\nEstructura final de 'ordenes_trabajo':")
    cursor.execute("PRAGMA table_info(ordenes_trabajo);")
    columns = cursor.fetchall()
    for col in columns:
        print(f"  - {col[1]} ({col[2]})")
    
    conn.close()
    print("\n✓ Base de datos migrada exitosamente!")
    sys.exit(0)
    
except sqlite3.OperationalError as e:
    if "duplicate column" in str(e).lower():
        print(f"\n✓ La columna ya existe: {e}")
        sys.exit(0)
    else:
        print(f"\n✗ Error en la base de datos: {e}", file=sys.stderr)
        sys.exit(1)
        
except Exception as e:
    print(f"\n✗ Error inesperado: {e}", file=sys.stderr)
    sys.exit(1)
