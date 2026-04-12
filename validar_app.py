#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Script para validar app2.py antes de ejecutar
"""
import sys
import os

print("=" * 60)
print("VALIDACIÓN DE app2.py")
print("=" * 60)

# 1. Validar sintaxis
print("\n1️⃣  Validando sintaxis Python...")
try:
    import py_compile
    py_compile.compile(r'c:\Users\usuar\OneDrive\Desktop\python\app2.py', doraise=True)
    print("   ✅ Sintaxis válida")
except py_compile.PyCompileError as e:
    print(f"   ❌ Error de sintaxis: {e}")
    sys.exit(1)

# 2. Verificar imports
print("\n2️⃣  Validando imports...")
try:
    from flask import Flask
    from reportlab.platypus import SimpleDocTemplate
    import sqlite3
    import pandas as pd
    import qrcode
    print("   ✅ Todos los imports disponibles")
except ImportError as e:
    print(f"   ❌ Falta módulo: {e}")
    sys.exit(1)

# 3. Verificar directorios
print("\n3️⃣  Verificando directorios...")
base = r'c:\Users\usuar\OneDrive\Desktop\python'
for carpeta in ['remitos', 'qrs']:
    ruta = os.path.join(base, carpeta)
    if os.path.exists(ruta):
        print(f"   ✅ {carpeta}/ existe")
    else:
        print(f"   ⚠️  {carpeta}/ NO existe (se creará al ejecutar)")

# 4. Verificar database
print("\n4️⃣  Verificando database...")
db_path = os.path.join(base, 'database.db')
if os.path.exists(db_path):
    print(f"   ✅ database.db existe ({os.path.getsize(db_path)} bytes)")
else:
    print(f"   ⚠️  database.db NO existe (se creará al ejecutar)")

print("\n" + "=" * 60)
print("✅ VALIDACIÓN COMPLETADA - Listo para ejecutar")
print("=" * 60)
print("\nPróximo paso: Ejecuta run_app.bat")
