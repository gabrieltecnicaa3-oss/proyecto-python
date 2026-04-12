#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para inspeccionar estructura de tabla procesos
"""
import sqlite3
import os

db_path = r'c:\Users\usuar\OneDrive\Desktop\python\database.db'

if os.path.exists(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Obtener estructura de la tabla
    cursor.execute("PRAGMA table_info(procesos)")
    columns = cursor.fetchall()
    
    print("=" * 60)
    print("ESTRUCTURA DE TABLA: procesos")
    print("=" * 60)
    
    if columns:
        print("\nColumnas actuales:")
        for col in columns:
            print(f"  - {col[1]} ({col[2]})")
        
        # Obtener un ejemplo de datos
        cursor.execute("SELECT * FROM procesos LIMIT 1")
        row = cursor.fetchone()
        if row:
            print("\nEjemplo de fila:")
            for i, col in enumerate(columns):
                print(f"  - {col[1]}: {row[i]}")
    else:
        print("\nTabla está vacía o no existe")
    
    conn.close()
else:
    print(f"Database no encontrada: {db_path}")
