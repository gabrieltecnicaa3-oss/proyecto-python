import sqlite3
import re

DB_PATH = r'C:\Users\usuar\OneDrive\Desktop\python\database.db'

# ── Tabla de fracciones estándar (valor_decimal_pulgada → string) ──────────────
FRACS = [
    (0.0,        ""),
    (1/32,       "1/32"),
    (1/16,       "1/16"),
    (3/32,       "3/32"),
    (1/8,        "1/8"),
    (5/32,       "5/32"),
    (3/16,       "3/16"),
    (7/32,       "7/32"),
    (1/4,        "1/4"),
    (9/32,       "9/32"),
    (5/16,       "5/16"),
    (11/32,      "11/32"),
    (3/8,        "3/8"),
    (13/32,      "13/32"),
    (7/16,       "7/16"),
    (15/32,      "15/16"),
    (1/2,        "1/2"),
    (9/16,       "9/16"),
    (5/8,        "5/8"),
    (11/16,      "11/16"),
    (3/4,        "3/4"),
    (13/16,      "13/16"),
    (7/8,        "7/8"),
    (15/16,      "15/16"),
    (1.0,        "__CARRY__"),  # redondear al entero siguiente
]

TOLERANCIA_MM = 0.15      # tolerancia base para fracciones estándar
TOLERANCIA_LPN_MM = 1.5   # LPN usa tamaños nominales (ej: 32mm ≈ 1 1/4")
TOLERANCIA_TUBO_MM = 0.5  # TUBO C DIAM: cubre espesores como 0.89mm, 2.3mm, 4mm


def nearest_frac(frac_val):
    best_str, best_diff = "", float('inf')
    for val, s in FRACS:
        d = abs(frac_val - val)
        if d < best_diff:
            best_diff = d
            best_str = s
    return best_str, best_diff


def mm_to_inch_str(mm, tol=None):
    """Convierte mm a fracción de pulgada. Devuelve None si no hay fracción estándar."""
    if tol is None:
        tol = TOLERANCIA_MM
    val = mm / 25.4
    whole = int(val)
    frac_val = val - whole

    frac_str, diff = nearest_frac(frac_val)

    diff_mm = diff * 25.4
    if diff_mm > tol:
        return None  # No tiene equivalente limpio en pulgadas

    if frac_str == "__CARRY__":
        whole += 1
        frac_str = ""

    if not frac_str:
        return f'{whole}"'
    elif whole == 0:
        return f'{frac_str}"'
    else:
        return f'{whole} {frac_str}"'


def parse_f(s):
    # Formato europeo: '1.066,80' -> miles con punto, decimal con coma
    # Primero quitar el separador de miles (punto antes de 3 dígitos), luego coma→punto
    s = re.sub(r'\.(?=\d{3}(?:[,\.]|$))', '', s)  # quitar punto de miles
    return float(s.replace(',', '.'))


# ── Procesar TUBO CIRCULAR ─────────────────────────────────────────────────────
RE_TUBO = re.compile(r'^TUBO C DIAM\s+([\d,\.]+)\s+x\s+([\d,\.]+)$')

# ── Procesar PERFILES LPN ──────────────────────────────────────────────────────
RE_LPN = re.compile(r'^LPN\s+([\d,\.]+)x([\d,\.]+)$')


con = sqlite3.connect(DB_PATH)
cur = con.cursor()

# Limpiar artículos en pulgadas insertados anteriormente
cur.execute("DELETE FROM articulos_sum WHERE descripcion LIKE '%\"%'")
print(f"Limpieza previa: {cur.rowcount} artículos borrados")
con.commit()

# Obtener artículos existentes para evitar duplicados
cur.execute("SELECT descripcion FROM articulos_sum")
existing = {r[0] for r in cur.fetchall()}

to_insert = []
pending_descs = set()  # evita duplicados dentro del mismo lote

# TUBO CIRCULAR
cur.execute("""
    SELECT id, descripcion, unidad, categoria, kg_per_m
    FROM articulos_sum WHERE categoria='TUBO CIRCULAR'
""")
for rid, desc, unidad, cat, kg in cur.fetchall():
    m = RE_TUBO.match(desc)
    if not m:
        continue
    d1, d2 = parse_f(m.group(1)), parse_f(m.group(2))
    s1 = mm_to_inch_str(d1, tol=TOLERANCIA_TUBO_MM)
    s2 = mm_to_inch_str(d2, tol=TOLERANCIA_TUBO_MM)
    if s1 is None or s2 is None:
        print(f"  SIN EQUIV TUBO: {d1}x{d2}  ({d1/25.4:.3f}\" x {d2/25.4:.3f}\")")
        continue  # dimensión sin equivalente limpio en pulgadas
    new_desc = f'TUBO C DIAM {s1} x {s2}'
    if new_desc not in existing and new_desc not in pending_descs and new_desc != desc:
        to_insert.append((new_desc, unidad, cat, kg))
        pending_descs.add(new_desc)

# PERFILES LPN
cur.execute("""
    SELECT id, descripcion, unidad, categoria, kg_per_m
    FROM articulos_sum WHERE categoria='PERFILES LPN'
""")
for rid, desc, unidad, cat, kg in cur.fetchall():
    m = RE_LPN.match(desc)
    if not m:
        continue
    d1, d2 = parse_f(m.group(1)), parse_f(m.group(2))
    s1 = mm_to_inch_str(d1, tol=TOLERANCIA_LPN_MM)
    s2 = mm_to_inch_str(d2, tol=TOLERANCIA_LPN_MM)
    if s1 is None or s2 is None:
        print(f"  SIN EQUIV: LPN {d1}x{d2}  ({d1/25.4:.3f}\" x {d2/25.4:.3f}\")")
        continue  # dimensión sin equivalente limpio en pulgadas
    new_desc = f'LPN {s1}x {s2}'
    if new_desc not in existing and new_desc not in pending_descs and new_desc != desc:
        to_insert.append((new_desc, unidad, cat, kg))
        pending_descs.add(new_desc)

print(f"Nuevos artículos a insertar: {len(to_insert)}")
print("Ejemplos:")
for r in to_insert[:6]:
    print(f"  {r[0]}  kg/m={r[3]:.4f}")

# Insertar (codigo=NULL para que "Generar Codigos Auto" los procese)
cur.executemany(
    "INSERT INTO articulos_sum (descripcion,unidad,categoria,activo,kg_per_m) VALUES (?,?,?,1,?)",
    to_insert
)
con.commit()
print(f"\nInsertados: {cur.rowcount}")

# Verificar
cur.execute("SELECT categoria, COUNT(*) FROM articulos_sum WHERE categoria IN ('TUBO CIRCULAR','PERFILES LPN') GROUP BY categoria")
print("Totales por categoría:")
for r in cur.fetchall():
    print(f"  {r[0]}: {r[1]}")

con.close()
print("OK")
