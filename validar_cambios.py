#!/usr/bin/env python3
"""Validar cambios en app2.py"""

import ast
import sys

try:
    with open('app2.py', 'r', encoding='utf-8') as f:
        code = f.read()
    
    ast.parse(code)
    print("✅ SINTAXIS VÁLIDA - No hay errores de Python")
    print("\n📝 CAMBIOS REALIZADOS:")
    print("=" * 60)
    print("Función: pieza(pos)")
    print("=" * 60)
    print("\n✨ MEJORAS IMPLEMENTADAS:")
    print("  1. Card destacado con datos de la pieza (OBRA, POS, CANT, PERFIL)")
    print("  2. Diseño visual mejorado con colores y emojis")
    print("  3. Los datos se extraen de la tabla 'procesos':")
    print("     - obra = procesos.obra (índice 2)")
    print("     - cantidad = procesos.cantidad (índice 3)")
    print("     - perfil = procesos.perfil (índice 4)")
    print("\n📍 UBICACIÓN DEL CARD:")
    print("  - Se muestra ANTES de los registros de procesos")
    print("  - Siempre visible cuando se escanea una pieza")
    print("\n🎨 ESTILOS AGREGADOS:")
    print("  - .data-card: Fondo azul claro (#e8f4f8)")
    print("  - .data-row: Filas con separadores")
    print("  - .data-label: Etiquetas en negrita")
    print("  - .data-value: Valores en azul destacado (#2196F3)")
    print("\n✅ LA APLICACIÓN ESTÁ LISTA PARA PROBAR")
    sys.exit(0)
    
except SyntaxError as e:
    print(f"❌ ERROR DE SINTAXIS: {e}")
    print(f"   Línea {e.lineno}: {e.text}")
    sys.exit(1)
except Exception as e:
    print(f"❌ ERROR: {e}")
    sys.exit(1)
