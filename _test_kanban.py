import traceback
from db_utils import get_db
db = get_db()
try:
    r = db.execute("SELECT id, COALESCE(numero,''), COALESCE(obra,''), COALESCE(observaciones,''), estado, criticidad FROM ordenes_pedido ORDER BY id DESC").fetchall()
    print("ops ok:", len(r))
    r2 = db.execute("SELECT oc.id, COALESCE(oc.numero,''), oc.proveedor, oc.estado, oc.fecha, COALESCE(op.numero,''), COALESCE(op.obra,'') FROM ordenes_compra oc LEFT JOIN ordenes_pedido op ON op.id=oc.op_id ORDER BY oc.id DESC").fetchall()
    print("ocs ok:", len(r2))
    print("ALL OK")
except Exception:
    traceback.print_exc()
