"""Reporte 3 — planilla de carga masiva de pedidos de compra "día 0" en Odoo.

No recalcula nada: parte de los items que ya devuelve `_calcular_resultado_tarea`
(subtotales del motor) y solo los agrupa por producto de Odoo según
`config_productos_odoo` (materiales/bulones/pintura/ingeniería/subcontratos) y
`catalogo_equipos.producto_odoo` (equipos de Montaje).

Se genera un archivo por sección (Fabricación / Montaje): un solo pedido por
archivo, con la convención de Odoo de dejar vacías las columnas de cabecera en
las líneas siguientes a la primera.
"""
import json
import re
import unicodedata
from datetime import date, datetime
from io import BytesIO

from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill

from .constants import (
    ODOO_VENDOR,
    ODOO_PREFIJO_FABRICACION,
    ODOO_PEDIDO_COLUMNAS,
    ODOO_RUBROS_POR_SECCION,
)
from .models import listar_tareas, listar_productos_odoo, listar_equipos
from .routes import _calcular_resultado_tarea

_FONT_HEADER = Font(bold=True, color="FFFFFF")
_FILL_HEADER = PatternFill("solid", fgColor="1F2937")

# Diferencia tolerada entre (total rubros - total archivo) y lo avisado, por
# redondeo a 2 decimales de cada línea.
_TOLERANCIA_REDONDEO = 0.05


def _norm(valor):
    sin_tildes = unicodedata.normalize("NFKD", str(valor or "")).encode("ascii", "ignore").decode("ascii")
    return sin_tildes.strip().casefold()


def _a_int(valor):
    try:
        return int(valor)
    except (TypeError, ValueError):
        return None


def _resolver_producto(reglas, rubro, tipo_item, familia, tarea=None):
    """Devuelve el producto_odoo de la regla más específica que aplica (tarea gana
    a familia, y ésta a tipo_item, y ésta a la comodín), o None si esa regla no
    tiene producto asignado o no hay ninguna regla."""
    mejor, mejor_score = None, -1
    for regla in reglas:
        if regla["concepto"] != rubro:
            continue
        if regla.get("tarea") and _norm(regla["tarea"]) != _norm(tarea):
            continue
        if regla["tipo_item"] and _norm(regla["tipo_item"]) != _norm(tipo_item):
            continue
        if regla["familia"] and _norm(regla["familia"]) != _norm(familia):
            continue
        score = (4 if regla.get("tarea") else 0) + (2 if regla["familia"] else 0) + (1 if regla["tipo_item"] else 0)
        if score > mejor_score:
            mejor, mejor_score = regla, score
    producto = str((mejor or {}).get("producto_odoo") or "").strip()
    return producto or None


def _etiqueta_sin_mapear(rubro, tipo_item, familia):
    etiqueta = rubro
    if tipo_item:
        etiqueta += f" / {tipo_item}"
    if familia:
        etiqueta += f" (familia {familia})"
    return etiqueta


def construir_pedido(items, reglas, familia_por_perfil_id, equipos_por_id, seccion):
    """Función pura: agrupa `items` (ya calculados, con `subtotal`) de UNA sección
    en líneas de pedido de Odoo.

    - Un artículo genérico por producto_odoo: Quantity 1, Unit Price = suma de
      subtotales (2 decimales).
    - Equipos (Montaje): una línea por (producto_odoo, tarifa_dia), Quantity =
      suma de días, Unit Price = tarifa_dia.
    - Todo concepto con importe > 0 y sin producto queda fuera y se informa en
      `sin_mapear` (nunca se omite en silencio).
    """
    rubros_incluidos = ODOO_RUBROS_POR_SECCION[seccion]
    por_producto = {}
    por_equipo = {}
    sin_mapear = {}
    total_rubros = 0.0

    for item in items:
        rubro = item.get("rubro")
        if rubro not in rubros_incluidos:
            continue
        importe = float(item.get("subtotal") or 0)
        total_rubros += importe
        datos = item.get("datos") or {}

        if rubro == "equipo":
            equipo = equipos_por_id.get(_a_int(datos.get("equipo_id"))) or {}
            producto = str(equipo.get("producto_odoo") or "").strip()
            if producto:
                clave = (producto, round(float(datos.get("tarifa_dia") or 0), 2))
                por_equipo[clave] = por_equipo.get(clave, 0.0) + float(datos.get("dias") or 0)
            elif importe > 0:
                etiqueta = f"equipo / {equipo.get('nombre') or '(sin equipo asociado)'}"
                sin_mapear[etiqueta] = sin_mapear.get(etiqueta, 0.0) + importe
            continue

        tipo_item = item.get("tipo_item")
        perfil_id = _a_int(datos.get("perfil_id") or item.get("perfil_id"))
        familia = familia_por_perfil_id.get(perfil_id) if perfil_id else None
        producto = _resolver_producto(reglas, rubro, tipo_item, familia, item.get("tarea"))
        if producto:
            por_producto[producto] = por_producto.get(producto, 0.0) + importe
        elif importe > 0:
            etiqueta = _etiqueta_sin_mapear(rubro, tipo_item, familia)
            sin_mapear[etiqueta] = sin_mapear.get(etiqueta, 0.0) + importe

    lineas = []
    for producto, importe in por_producto.items():
        precio = round(importe, 2)
        if precio > 0:
            lineas.append({"producto": producto, "cantidad": 1, "precio_unitario": precio})
    for (producto, tarifa), dias in por_equipo.items():
        cantidad = round(dias, 2)
        if cantidad > 0 and tarifa > 0:
            lineas.append({"producto": producto, "cantidad": cantidad, "precio_unitario": tarifa})

    avisos = [
        {"concepto": concepto, "importe": round(importe, 2)}
        for concepto, importe in sin_mapear.items()
        if round(importe, 2) > 0
    ]
    total_archivo = round(sum(l["cantidad"] * l["precio_unitario"] for l in lineas), 2)
    total_rubros = round(total_rubros, 2)
    diferencia = round(total_rubros - total_archivo, 2)
    suma_avisos = round(sum(a["importe"] for a in avisos), 2)

    return {
        "lineas": lineas,
        "sin_mapear": avisos,
        "total_archivo": total_archivo,
        "total_rubros": total_rubros,
        "diferencia": diferencia,
        "suma_sin_mapear": suma_avisos,
        "cuadra": abs(diferencia - suma_avisos) <= _TOLERANCIA_REDONDEO,
    }


def _order_deadline(fecha_adjudicacion, hoy=None):
    """"AAAA-MM-DD 12:00:00" (el 12:00 evita corrimientos de día por zona
    horaria). Sin fecha de adjudicación (o ilegible) usa la fecha de hoy."""
    dia = None
    if isinstance(fecha_adjudicacion, (date, datetime)):
        dia = fecha_adjudicacion
    elif fecha_adjudicacion:
        try:
            dia = datetime.strptime(str(fecha_adjudicacion).strip()[:10], "%Y-%m-%d")
        except ValueError:
            dia = None
    if dia is None:
        dia = hoy or date.today()
    return f"{dia.strftime('%Y-%m-%d')} 12:00:00"


def _order_reference(seccion, obra_referencia):
    obra = (obra_referencia or "").strip()
    if not obra:
        raise ValueError(
            "El presupuesto no tiene obra de referencia (ni título/cliente de respaldo) para armar el Order Reference."
        )
    return f"{ODOO_PREFIJO_FABRICACION}{obra}" if seccion == "FABRICACION" else obra


def _familias_por_perfil_id(db, items):
    ids = sorted({
        pid
        for it in items
        for pid in [_a_int((it.get("datos") or {}).get("perfil_id") or it.get("perfil_id"))]
        if pid
    })
    if not ids:
        return {}
    placeholders = ",".join("?" for _ in ids)
    try:
        rows = db.execute(
            f"SELECT id, COALESCE(categoria, '') FROM articulos_sum WHERE id IN ({placeholders})", tuple(ids)
        ).fetchall()
    except Exception:
        return {}
    return {r[0]: r[1] for r in rows}


def armar_pedido_odoo(db, presupuesto, seccion, obra_referencia):
    """Arma el pedido (líneas, avisos, totales, Order Reference y deadline) de
    una sección a partir de los subtotales del motor. Lanza ValueError si falta
    la obra para el Order Reference."""
    order_reference = _order_reference(seccion, obra_referencia)

    clave = "fabricacion" if seccion == "FABRICACION" else "montaje"
    tareas = listar_tareas(db, presupuesto["id"])
    # El tipo de la pestaña (Correas, Grating...) manda aunque la tarea se haya renombrado.
    items = [
        dict(item, tarea=(tarea.get("tipo") or tarea["nombre"]))
        for tarea in tareas
        for item in _calcular_resultado_tarea(db, tarea["id"])[clave]["items"]
    ]

    reglas = [r for r in listar_productos_odoo(db) if r["seccion"] == seccion]
    pedido = construir_pedido(
        items,
        reglas,
        _familias_por_perfil_id(db, items),
        {e["id"]: e for e in listar_equipos(db)},
        seccion,
    )
    pedido["order_reference"] = order_reference
    pedido["order_deadline"] = _order_deadline(presupuesto.get("fecha_adjudicacion"))
    return pedido


def generar_excel_pedido_odoo(pedido, analitica_id, vendor=ODOO_VENDOR):
    """Hoja única con los encabezados exactos de Odoo. La cabecera (referencia,
    proveedor, deadline, vendor reference) se repite en TODAS las filas: así Odoo
    crea un pedido por línea en lugar de agruparlas en uno solo.
    La distribución analítica va en TODAS las filas, como texto JSON."""
    distribucion = json.dumps({str(analitica_id): 100})

    wb = Workbook()
    ws = wb.active
    ws.title = "Pedido"
    ws.append(list(ODOO_PEDIDO_COLUMNAS))
    for celda in ws[1]:
        celda.font = _FONT_HEADER
        celda.fill = _FILL_HEADER

    for linea in pedido["lineas"]:
        cabecera = [pedido["order_reference"], vendor, pedido["order_deadline"], None, vendor]
        ws.append(cabecera + [
            linea["producto"],
            linea["cantidad"],
            linea["precio_unitario"],
            None,
            None,
            distribucion,
        ])
        ws.cell(row=ws.max_row, column=8).number_format = "0.00"

    for columna, ancho in zip("ABCDEFGHIJK", (30, 34, 22, 16, 34, 52, 20, 22, 18, 16, 36)):
        ws.column_dimensions[columna].width = ancho

    buffer = BytesIO()
    wb.save(buffer)
    buffer.seek(0)
    return buffer


_ENTERO_POSITIVO = re.compile(r"^[0-9]{1,12}$")


def validar_analitica_id(texto):
    """Entero > 0. Devuelve el int o lanza ValueError con un mensaje claro."""
    limpio = str(texto or "").strip()
    if not limpio:
        raise ValueError("Ingresá el ID de la cuenta analítica de Odoo.")
    if not _ENTERO_POSITIVO.match(limpio) or int(limpio) <= 0:
        raise ValueError("El ID de la cuenta analítica debe ser un número entero mayor a 0 (por ejemplo 932).")
    return int(limpio)


def nombre_archivo_pedido(order_reference):
    seguro = re.sub(r'[\\/:*?"<>|]', "_", order_reference)
    return f"odoo_pedido_{seguro}.xlsx"
