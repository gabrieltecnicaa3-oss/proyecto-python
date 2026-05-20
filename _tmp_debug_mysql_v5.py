import pymysql

conn = pymysql.connect(
    host='127.0.0.1', port=3306,
    user='appuser', password='App1234!',
    database='gestion_produccion',
    cursorclass=pymysql.cursors.DictCursor
)
cur = conn.cursor()

print("=== TODAS las OTs (activas y cerradas) ===")
cur.execute("SELECT id, obra, titulo, fecha_cierre FROM ordenes_trabajo ORDER BY id")
for r in cur.fetchall():
    print(r)

print("\n=== Todas las posiciones 'V5' en procesos ===")
cur.execute("""
    SELECT p.id, p.ot_id, p.posicion, p.proceso, p.estado, p.eliminado,
           ot.obra, ot.titulo, ot.fecha_cierre
    FROM procesos p
    LEFT JOIN ordenes_trabajo ot ON ot.id = p.ot_id
    WHERE TRIM(COALESCE(p.posicion,'')) = 'V5'
    ORDER BY p.ot_id, p.id
""")
for r in cur.fetchall():
    print(r)

conn.close()
