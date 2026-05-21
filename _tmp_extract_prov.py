import openpyxl, glob

path = r'C:\Users\usuar\OneDrive\Desktop\Info y Caracterizacion de Proveedores 2025-10-01.xlsx'
wb = openpyxl.load_workbook(path, data_only=True)
ws = wb.active

nombres = []
for i, row in enumerate(ws.iter_rows(values_only=True)):
    if i == 0:
        continue  # saltar encabezado
    nombre = row[1]  # columna "Proveedor"
    if nombre and str(nombre).strip():
        nombres.append(str(nombre).strip())

wb.close()

# Generar Python list
print(f"Total proveedores: {len(nombres)}")
print("\nPROVEEDORES_EXCEL = [")
for n in nombres:
    escaped = n.replace('"', '\\"')
    print(f'    "{escaped}",')
print("]")
