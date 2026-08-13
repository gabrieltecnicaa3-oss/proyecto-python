"""Comprehensive fix: curva-s error handling, green line, monthly sums, title."""
content = open('economico_routes.py', encoding='utf-8').read()
changes = []

# ── Fix 1: Dashboard title ────────────────────────────────────────────────────
old = '📅 Egresos del período</span>'
new = '📅 Balance del período</span>'
n = content.count(old); changes.append(f'title fix: {n}'); content = content.replace(old, new, 1)

old2 = '"Egresos del período"'
new2 = '"Balance del período"'
n = content.count(old2); changes.append(f'title2 fix: {n}'); content = content.replace(old2, new2)

# ── Fix 2: dashboard period – HH-proportional distribution (sums to p_cd) ────
OLD_HHDASH = (
    '    # CD previsto mensual = (HH trabajadas en el mes / HH previstas OT) × p_cd de la OT\n'
    '    _pcd_map_de = {str(r[0]): float(r[1] or 0) for r in db.execute(\n'
    '        "SELECT ot_id, COALESCE(mat_previsto,0)+COALESCE(pintura_previsto,0)+COALESCE(mo_previsto,0)"\n'
    '        "+COALESCE(consumibles_previsto,0)+COALESCE(ingenieria_previsto,0) FROM economico_presupuesto"\n'
    '    ).fetchall()}\n'
    '    _hs_map_de = {str(r[0]): float(r[1] or 0) for r in db.execute(\n'
    '        "SELECT id, COALESCE(hs_previstas,0) FROM ordenes_trabajo"\n'
    '    ).fetchall()}\n'
    '    _ing_mes_de: dict = {}\n'
    '    for _r in _hh_rows:\n'
    '        _ot_k = str(_r[1] or ""); _hs_p = _hs_map_de.get(_ot_k, 0); _pcd_k = _pcd_map_de.get(_ot_k, 0)\n'
    '        if _hs_p > 0 and _pcd_k > 0:\n'
    '            _m_k = str(_r[0] or "")\n'
    '            _ing_mes_de[_m_k] = _ing_mes_de.get(_m_k, 0.0) + (float(_r[2] or 0) / _hs_p) * _pcd_k'
)
NEW_HHDASH = (
    '    # CD Previsto mensual: distribucion proporcional a HH → suma exactamente p_cd acumulado\n'
    '    _pcd_map_de = {str(r[0]): float(r[1] or 0) for r in db.execute(\n'
    '        "SELECT ot_id, COALESCE(mat_previsto,0)+COALESCE(pintura_previsto,0)+COALESCE(mo_previsto,0)"\n'
    '        "+COALESCE(consumibles_previsto,0)+COALESCE(ingenieria_previsto,0) FROM economico_presupuesto"\n'
    '    ).fetchall()}\n'
    '    _total_hh_ot_de: dict = {}\n'
    '    for _r in _hh_rows:\n'
    '        _ot_k = str(_r[1] or "")\n'
    '        _total_hh_ot_de[_ot_k] = _total_hh_ot_de.get(_ot_k, 0.0) + float(_r[2] or 0)\n'
    '    _ing_mes_de: dict = {}\n'
    '    for _r in _hh_rows:\n'
    '        _ot_k = str(_r[1] or ""); _pcd_k = _pcd_map_de.get(_ot_k, 0)\n'
    '        _tot_hh = _total_hh_ot_de.get(_ot_k, 0)\n'
    '        if _tot_hh > 0 and _pcd_k > 0:\n'
    '            _m_k = str(_r[0] or "")\n'
    '            _ing_mes_de[_m_k] = _ing_mes_de.get(_m_k, 0.0) + float(_r[2] or 0) / _tot_hh * _pcd_k'
)
n = content.count(OLD_HHDASH); changes.append(f'dashboard PV fix: {n}'); content = content.replace(OLD_HHDASH, NEW_HHDASH, 1)

# ── Fix 3: flujo-caja – replace broken _dist_pv_fc with HH-proportional ───────
# Remove the complex _dist_pv_fc block and replace with simpler proportional calc
import re
OLD_FC = content[content.find('    # CD Previsto mensual = linear distribution'):
                 content.find('    ing_mes: dict = {}\n    _all_ot_ids_fc')+200]

# Find the exact block
start_marker = '    # CD Previsto mensual = linear distribution of p_cd over the programmed project period'
end_marker   = "        ing_mes[_m_fc] = ing_mes.get(_m_fc, 0.0) + _v_fc"
start = content.find(start_marker)
end = content.find(end_marker)
if start != -1 and end != -1:
    end += len(end_marker)
    OLD_FC_BLOCK = content[start:end]
    NEW_FC_BLOCK = (
        '    # CD Previsto mensual: distribucion proporcional a HH → suma exactamente p_cd acumulado\n'
        '    _pcd_fc = {str(r[0]): float(r[1] or 0) for r in db.execute(\n'
        '        "SELECT ot_id, COALESCE(mat_previsto,0)+COALESCE(pintura_previsto,0)+COALESCE(mo_previsto,0)"\n'
        '        "+COALESCE(consumibles_previsto,0)+COALESCE(ingenieria_previsto,0) FROM economico_presupuesto"\n'
        '    ).fetchall()}\n'
        '    _total_hh_ot_fc: dict = {}\n'
        '    for r in hh_rows:\n'
        '        _ot_k_fc = str(r[1] or "")\n'
        '        _total_hh_ot_fc[_ot_k_fc] = _total_hh_ot_fc.get(_ot_k_fc, 0.0) + float(r[2] or 0)\n'
        '    ing_mes: dict = {}\n'
        '    for r in hh_rows:\n'
        '        _ot_k_fc = str(r[1] or ""); _pcd_v = _pcd_fc.get(_ot_k_fc, 0)\n'
        '        _tot_hh_fc = _total_hh_ot_fc.get(_ot_k_fc, 0)\n'
        '        if _tot_hh_fc > 0 and _pcd_v > 0:\n'
        '            _m_fc = str(r[0] or "")\n'
        '            ing_mes[_m_fc] = ing_mes.get(_m_fc, 0.0) + float(r[2] or 0) / _tot_hh_fc * _pcd_v'
    )
    changes.append(f'flujo-caja PV fix: replaced {len(OLD_FC_BLOCK)} chars')
    content = content.replace(OLD_FC_BLOCK, NEW_FC_BLOCK, 1)
else:
    changes.append('flujo-caja PV fix: block NOT FOUND')

# ── Fix 4: curva-s – wrap body in try-except, expose error without traceback ──
# Add import traceback and wrap the return statement
old_ret = '    return f"""<!DOCTYPE html><html lang="es"><head>\n<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">\n<title>{titulo}</title>'
new_ret = (
    '    except Exception as _cs_err:\n'
    '        import traceback as _tb\n'
    '        _tb.print_exc()\n'
    '        return (f"<h2>Error en Curva S</h2><pre style=\'background:#fee2e2;padding:16px;border-radius:8px;font-size:.85rem;\'>"\n'
    '                f"{_E(str(_cs_err))}</pre>"\n'
    '                f"<a href=\'/modulo/economico\'>\\u2190 Volver</a>"), 500\n\n'
    '    return f"""<!DOCTYPE html><html lang="es"><head>\n<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">\n<title>{titulo}</title>'
)
# Find the try: line added by _bac_cols setup and wrap everything after db call
try_start = '    # BAC = PV total; safe fallback omits fletes/subcontratos_previsto if migration is pending'
if try_start in content:
    content = content.replace(try_start, '    try:\n    ' + try_start.lstrip(), 1)
    changes.append('curva-s try block: added')
else:
    changes.append('curva-s try block: NOT FOUND')

n = content.count(old_ret); changes.append(f'curva-s except: {n}')
content = content.replace(old_ret, new_ret, 1)

for c in changes:
    print(c)

open('economico_routes.py', 'w', encoding='utf-8').write(content)
print('Done.')
