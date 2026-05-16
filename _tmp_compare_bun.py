from datetime import date
import app2
from db_utils import get_db
from produccion_routes import _avance_y_desglose_ot, calcular_avance_ot

OBRA = 'BUN-012'

with app2.app.app_context():
    db = get_db()
    ots_prod = db.execute(
        "SELECT id FROM ordenes_trabajo WHERE estado != 'Finalizada' AND fecha_cierre IS NULL AND (es_mantenimiento IS NULL OR es_mantenimiento = 0) AND TRIM(COALESCE(obra,'')) = ? ORDER BY id",
        (OBRA,)
    ).fetchall()
    prod_ids = [int(r[0]) for r in ots_prod]

    total_kg_prod = 0.0
    avance_kg_prod = 0.0
    for oid in prod_ids:
        _, _, _, tkg, akg = _avance_y_desglose_ot(db, oid)
        total_kg_prod += float(tkg or 0)
        avance_kg_prod += float(akg or 0)
    pct_prod = round((avance_kg_prod / total_kg_prod) * 100) if total_kg_prod > 0 else 0

    fecha_desde = date.today().replace(day=1).isoformat()
    ots_estado_mes = db.execute(
        "WITH hs_ot AS (SELECT ot.id, COALESCE(ot.hs_previstas, 0) AS hs_previstas, COALESCE(SUM(CASE WHEN pt.fecha >= ? THEN pt.horas ELSE 0 END), 0) AS hs_cargadas FROM ordenes_trabajo ot LEFT JOIN partes_trabajo pt ON pt.ot_id = ot.id WHERE ot.fecha_cierre IS NULL AND LOWER(TRIM(COALESCE(ot.obra,''))) = LOWER(?) GROUP BY ot.id) SELECT id FROM hs_ot WHERE hs_previstas > 0 OR hs_cargadas > 0 ORDER BY id",
        (fecha_desde, OBRA)
    ).fetchall()
    estado_ids = [int(r[0]) for r in ots_estado_mes]

    kg_prev_by_ot = {}
    real_by_ot = {}
    for oid in estado_ids:
        _, _, _, tkg, _ = _avance_y_desglose_ot(db, oid)
        kg_prev_by_ot[oid] = float(tkg or 0)
        real_by_ot[oid] = float(calcular_avance_ot(db, oid))

    den = sum(max(0.0, kg_prev_by_ot[oid]) for oid in estado_ids)
    num = sum(max(0.0, kg_prev_by_ot[oid]) * real_by_ot[oid] for oid in estado_ids)
    pct_estado = round(num / den, 1) if den > 0 else 0.0

    print('prod_ids=', prod_ids)
    print('estado_ids=', estado_ids)
    print('pct_prod=', pct_prod)
    print('pct_estado=', pct_estado)
    print('solo_prod=', sorted(set(prod_ids)-set(estado_ids)))
    print('solo_estado=', sorted(set(estado_ids)-set(prod_ids)))
