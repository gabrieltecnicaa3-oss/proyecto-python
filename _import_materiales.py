import openpyxl, sqlite3

EXCEL_PATH = r'C:\Users\usuar\OneDrive\Desktop\MATERIALES.xlsx'
DB_PATH    = r'C:\Users\usuar\OneDrive\Desktop\python\database.db'

# ── POST-IMPORT FIXES ──────────────────────────────────────────────────────────
if __name__ == '__main__':
    _fix = False
    import sys
    if '--fix' in sys.argv:
        _fix = True

def _run_fixes():
    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("UPDATE articulos_sum SET unidad='m' WHERE categoria IN ('RD','TUBO C')")
    print('unidad=m:', cur.rowcount)
    cur.execute("SELECT id, codigo, descripcion FROM articulos_sum WHERE descripcion LIKE 'TUBO C \u00d8%'")
    rows = cur.fetchall()
    print('Con Ø:', len(rows))
    for rid, cod, desc in rows:
        new_desc = desc.replace('TUBO C \u00d8', 'TUBO C DIAM')
        new_cod  = cod.replace('TUBO C \u00d8', 'TUBO C DIAM')
        cur.execute('UPDATE articulos_sum SET descripcion=?, codigo=? WHERE id=?', (new_desc, new_cod, rid))
    con.commit()
    cur.execute("SELECT COUNT(*) FROM articulos_sum WHERE descripcion LIKE 'TUBO C DIAM%'")
    print('TUBO C DIAM count:', cur.fetchone()[0])
    con.close()
    print('OK')

wb = openpyxl.load_workbook(EXCEL_PATH, read_only=True, data_only=True)
ws = wb['Tablas']
rows = list(ws.iter_rows(values_only=True))
wb.close()

tubo_c_rows = []
rd_rows     = []
in_rd_section = False

SECTION_HEADERS = {'PL', 'CA', 'AA', 'PE'}

for i, row in enumerate(rows):
    nombre = str(row[0] or '').strip()
    if nombre == 'RD':
        in_rd_section = True
        continue
    # Salir de sección RD solo si aparece otro encabezado de sección (distinto a "D ...")
    if in_rd_section and nombre in SECTION_HEADERS:
        in_rd_section = False
    if nombre.startswith('TUBO C') and row[6] is not None:
        tubo_c_rows.append((nombre, float(row[6])))
    elif in_rd_section and nombre.startswith('D ') and row[6] is not None:
        rd_rows.append(('RD ' + nombre, float(row[6])))

print(f'TUBO C: {len(tubo_c_rows)} filas')
print(f'RD:     {len(rd_rows)} filas')
if tubo_c_rows:
    print('  ej TUBO C:', tubo_c_rows[0])
if rd_rows:
    print('  ej RD:    ', rd_rows[0])

con = sqlite3.connect(DB_PATH)
cur = con.cursor()

cur.execute("DELETE FROM articulos_sum WHERE categoria IN ('RD','TUBO C')")
deleted = cur.rowcount
print(f'Borrados: {deleted} articulos previos')

for desc, kg in tubo_c_rows:
    cur.execute(
        "INSERT INTO articulos_sum (codigo,descripcion,unidad,categoria,activo,kg_per_m) VALUES (?,?,?,?,1,?)",
        (desc, desc, 'barra', 'TUBO C', kg)
    )

for desc, kg in rd_rows:
    cur.execute(
        "INSERT INTO articulos_sum (codigo,descripcion,unidad,categoria,activo,kg_per_m) VALUES (?,?,?,?,1,?)",
        (desc, desc, 'barra', 'RD', kg)
    )

con.commit()

cur.execute("SELECT categoria, COUNT(*) FROM articulos_sum WHERE categoria IN ('RD','TUBO C') GROUP BY categoria")
print('Verificacion:', cur.fetchall())
con.close()
print('OK')
