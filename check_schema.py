import sqlite3

conn = sqlite3.connect('database.db')
cursor = conn.cursor()

print("=" * 60)
print("Columnas en la tabla 'ordenes_trabajo':")
print("=" * 60)

try:
    cursor.execute("PRAGMA table_info(ordenes_trabajo);")
    columns = cursor.fetchall()
    for col in columns:
        col_id, col_name, col_type, notnull, default, pk = col
        print(f"{col_name:20} - {col_type:10}")
except Exception as e:
    print(f"Error: {e}")

print("\n" + "=" * 60)
print("Intentando adicionar columna 'estado_avance' si no existe...")
print("=" * 60)

try:
    cursor.execute("ALTER TABLE ordenes_trabajo ADD COLUMN estado_avance INTEGER DEFAULT 0;")
    conn.commit()
    print("✓ Columna 'estado_avance' agregada exitosamente")
except sqlite3.OperationalError as e:
    if "duplicate column name" in str(e):
        print("✓ La columna 'estado_avance' ya existe")
    else:
        print(f"Error: {e}")

conn.close()
print("\n¡Base de datos actualizada!")
