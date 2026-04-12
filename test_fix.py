#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para probar que la migración funcione correctamente
"""

import sqlite3

print("Verificando la base de datos después de la migración...")
print("=" * 60)

conn = sqlite3.connect('database.db')
cursor = conn.cursor()

# Verificar las columnas
cursor.execute("PRAGMA table_info(ordenes_trabajo)")
columns = cursor.fetchall()

print("Columnas en 'ordenes_trabajo':")
column_names = []
for col_id, col_name, col_type, notnull, default, pk in columns:
    column_names.append(col_name)
    print(f"  {col_id:2} - {col_name:20} ({col_type:15}) Default: {default}")

print("\n" + "=" * 60)

# Verificar que existan las columnas necesarias
required_columns = ['id', 'cliente', 'obra', 'titulo', 'fecha_entrega', 'estado', 'estado_avance', 'fecha_creacion']

missing = [col for col in required_columns if col not in column_names]
if missing:
    print(f"✗ Columnas faltantes: {missing}")
else:
    print("✓ Todas las columnas requeridas existen")

print("\n" + "=" * 60)

# Probar inserción
try:
    cursor.execute("""
        INSERT INTO ordenes_trabajo (cliente, obra, titulo, fecha_entrega, estado, estado_avance)
        VALUES (?, ?, ?, ?, ?, ?)
    """, ("TestClient", "TestObra", "TestTitulo", "2026-04-10", "Pendiente", 0))
    conn.commit()
    print("✓ Prueba de inserción exitosa")
    
    # Recuperar el registro insertado
    cursor.execute("SELECT * FROM ordenes_trabajo ORDER BY id DESC LIMIT 1")
    row = cursor.fetchone()
    print(f"✓ Registro insertado: {row}")
    
except Exception as e:
    print(f"✗ Error en prueba de inserción: {e}")

conn.close()
print("\n¡Verificación completada!")
