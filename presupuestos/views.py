"""Pantallas HTML del módulo Presupuestos (sin frameworks JS, estilo del resto
del MES: HTML server-rendered con fetch() a la API JSON para las partes
dinámicas — igual patrón que remito_routes.py `api_piezas_remito` + JS).

Fuente: ESPECIFICACION_MODULO_PRESUPUESTOS_V1.md, sección 4 (puntos 1 a 4) y
sección 5 (puntos 1 a 3):
  1) Listado de presupuestos.
  2) Datos generales de un presupuesto.
  3) Tareas -> Fabricación/Montaje -> tabla de items editable + panel de
     resultado (llama al endpoint de recálculo del paso 3, no reimplementa
     la fórmula acá).
  4) Resumen del presupuesto (todas las tareas, total FAB+MON, gran total).
  5) Reporte cruzado por categoría, indicador $/kg y USD/kg, y resumen de
     recursos a obra exportable (mano de obra, equipos, materiales).

Todavía sin pantalla de configuración ni botón de adjudicar (próximo paso).
"""
import csv
import json
from io import BytesIO, StringIO

from flask import request, redirect, send_file

from . import presupuestos_bp
from .routes import _db, _todos_los_resultados_tarea, _resumen_recursos_con_nombres
from .constants import ESTADOS_PRESUPUESTO, TIPOS_SECCION, RUBROS_POR_SECCION, TAREAS_ESTANDAR
from .calculo_presupuesto import calcular_presupuesto
from .models import (
    crear_presupuesto,
    obtener_presupuesto,
    listar_presupuestos,
    actualizar_presupuesto,
    actualizar_estado_presupuesto,
    eliminar_presupuesto,
    crear_tarea,
    listar_tareas,
    crear_secciones_tarea,
    eliminar_tarea,
    obtener_config,
)


# ─────────────────────────────────────────────────────────────────
# Estilo compartido (misma paleta "moderna" que /home, /pieza, /cargar)
# ─────────────────────────────────────────────────────────────────

_ESTILO_BASE = """
* { box-sizing: border-box; }
body {
    font-family: "Segoe UI", Tahoma, Arial, sans-serif;
    padding: 16px;
    margin: 0;
    background: radial-gradient(circle at 15% 0%, #f8fbff 0%, #eef3f7 55%, #e8edf3 100%);
    color: #0f172a;
}
.wrap { max-width: 1600px; margin: 0 auto; }
.top-bar { display: flex; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 12px; flex-wrap: wrap; }
h2 { margin: 0; color: #111827; }
.card {
    background: #ffffff; border: 1px solid #dbe4ee; border-radius: 14px;
    padding: 16px; margin-bottom: 14px; box-shadow: 0 8px 18px rgba(15,23,42,0.06);
}
.btn {
    display: inline-block; background: #4338ca; color: #fff; padding: 9px 14px;
    border-radius: 8px; text-decoration: none; font-weight: 700; font-size: 13px;
    border: none; cursor: pointer;
}
.btn:hover { background: #3730a3; }
.btn-secondary { background: #64748b; }
.btn-secondary:hover { background: #475569; }
.btn-danger { background: #dc2626; }
.btn-danger:hover { background: #b91c1c; }
.btn-sm { padding: 5px 9px; font-size: 12px; }
.btn-icon { padding: 2px 6px; font-size: 11px; line-height: 1.4; border-radius: 5px; }
table { width: 100%; border-collapse: collapse; background: #fff; }
th, td { padding: 9px 8px; border-bottom: 1px solid #e5e7eb; text-align: left; font-size: 13px; vertical-align: top; }
th { background: #eef2ff; color: #312e81; }
tr:hover { background: #f8fafc; }
label { display: block; font-size: 13px; font-weight: 700; color: #334155; margin-bottom: 4px; }
input, select, textarea {
    width: 100%; padding: 9px 10px; border: 1px solid #cbd5e1; border-radius: 8px;
    background: #fff; font-size: 14px; margin-bottom: 10px;
}
input[readonly] { background: #f1f5f9; color: #475569; cursor: not-allowed; }
.grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }
.grid3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 10px; }
.chip { display: inline-block; padding: 3px 9px; border-radius: 999px; font-size: 11px; font-weight: 700; border: 1px solid transparent; }
.chip-borrador { background: #e2e8f0; color: #475569; border-color: #cbd5e1; }
.chip-enviado { background: #ffedd5; color: #9a3412; border-color: #fdba74; }
.chip-adjudicado { background: #dcfce7; color: #166534; border-color: #86efac; }
.chip-perdido { background: #fee2e2; color: #b91c1c; border-color: #fecaca; }
.chip-fab { background: #e0e7ff; color: #3730a3; border-color: #c7d2fe; }
.chip-mon { background: #fce7f3; color: #9d174d; border-color: #fbcfe8; }
.tarea-card { border: 1px solid #e2e8f0; border-radius: 10px; padding: 12px; margin-bottom: 10px; background: #f8fafc; }
.seccion-box { background: #fff; border: 1px solid #e2e8f0; border-radius: 8px; padding: 10px; margin-top: 8px; }
.resultado-box { background: #f0fdf4; border: 1px solid #bbf7d0; border-radius: 8px; padding: 10px; margin-top: 8px; font-size: 13px; }
.resultado-box .fila { display: flex; justify-content: space-between; padding: 3px 0; }
.muted { color: #64748b; font-size: 12px; }
.sin-datos { text-align: center; padding: 24px; color: #64748b; }
.tarea-scroll-wrap { max-height: 78vh; overflow-y: auto; border: 1px solid #e2e8f0; border-radius: 10px; }
.panel-sticky {
    position: sticky; top: 0; z-index: 5; background: #fff; border-bottom: 2px solid #c7d2fe;
    padding: 10px 12px; box-shadow: 0 4px 10px rgba(15,23,42,0.06);
}
.panel-sticky .grid-paneles { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 8px; }
.mini-resultado { background: #f0fdf4; border: 1px solid #bbf7d0; border-radius: 8px; padding: 8px 10px; font-size: 12px; }
.mini-resultado.indicador { background: #eff6ff; border-color: #bfdbfe; }
.mini-resultado .fila { display: flex; justify-content: space-between; padding: 1px 0; }
.mini-resultado .fila.total { font-weight: 800; border-top: 1px solid #bbf7d0; margin-top: 3px; padding-top: 3px; }
.guardando-badge {
    display: none; font-size: 11px; font-weight: 700; color: #92400e; background: #fef3c7;
    border: 1px solid #fde68a; border-radius: 999px; padding: 2px 9px; margin-left: 8px;
}
.guardando-badge.activo { display: inline-block; }
.rubro-block { border: 1px solid #e2e8f0; border-radius: 6px; padding: 5px 8px; margin-top: 5px; background: #fff; }
.rubro-block h4 { margin: 0 0 3px 0; font-size: 11px; color: #4338ca; text-transform: capitalize; }
.rubro-block table { font-size: 12px; }
.rubro-block table th, .rubro-block table td { padding: 3px 6px; }
.rubro-block input, .rubro-block select { margin-bottom: 0; padding: 3px 6px; font-size: 12px; }
.seccion-wrap { padding: 6px 8px; border: 1px solid #dbe4ee; border-radius: 8px; margin-bottom: 6px; background: #fbfdff; }
.tabla-materiales th, .tabla-materiales td { padding: 2px 5px; font-size: 11px; white-space: nowrap; }
.tabla-materiales input, .tabla-materiales select { padding: 2px 5px; font-size: 11px; min-width: 76px; margin-bottom: 0; }
.tabs-tareas { display: flex; gap: 4px; flex-wrap: wrap; border-bottom: 2px solid #e2e8f0; margin-bottom: 10px; }
.tab-tarea {
    background: #eef2ff; color: #3730a3; border: 1px solid #c7d2fe; border-bottom: none;
    border-radius: 8px 8px 0 0; padding: 8px 14px; font-weight: 700; font-size: 13px; cursor: pointer;
}
.tab-tarea.activo { background: #4338ca; color: #fff; border-color: #4338ca; }
.tab-tarea:hover { background: #e0e7ff; }
.tab-tarea.activo:hover { background: #3730a3; }
"""


def _pct(valor):
    return f"{float(valor or 0) * 100:.2f}%"


def _fmt_money(valor):
    return f"$ {float(valor or 0):,.2f}"


def _badge_estado(estado):
    clases = {"borrador": "chip-borrador", "enviado": "chip-enviado", "adjudicado": "chip-adjudicado", "perdido": "chip-perdido"}
    return f'<span class="chip {clases.get(estado, "chip-borrador")}">{estado.upper()}</span>'


# ─────────────────────────────────────────────────────────────────
# 1) Listado de presupuestos
# ─────────────────────────────────────────────────────────────────

@presupuestos_bp.route("", methods=["GET"])
def vista_listado():
    db = _db()
    presupuestos = listar_presupuestos(db)

    filas = ""
    for p in presupuestos:
        total_presupuesto = calcular_presupuesto(_todos_los_resultados_tarea(db, p["id"]))["precio_venta_presupuesto"]
        filas += f"""
        <tr>
            <td><b>#{p['id']}</b></td>
            <td>{p['cliente'] or '-'}</td>
            <td>{p['planta'] or '-'}</td>
            <td>{_fmt_money(total_presupuesto)}</td>
            <td>{p['fecha_creacion'] or '-'}</td>
            <td>{_badge_estado(p['estado'])}</td>
            <td>
                <a class="btn btn-sm" href="/modulo/presupuestos/{p['id']}">Ver</a>
                <form method="post" action="/modulo/presupuestos/{p['id']}/eliminar" style="display:inline;" onsubmit="return confirm('¿Eliminar presupuesto #{p['id']}? Esta acción no se puede deshacer.');">
                    <button type="submit" class="btn btn-sm btn-danger">Eliminar</button>
                </form>
            </td>
        </tr>
        """

    tabla = f"""
    <table>
        <tr><th>ID</th><th>Cliente</th><th>Planta</th><th>Precio venta total</th><th>Creado</th><th>Estado</th><th>Acciones</th></tr>
        {filas}
    </table>
    """ if presupuestos else "<div class='sin-datos'>Todavía no hay presupuestos cargados.</div>"

    return f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>{_ESTILO_BASE}</style>
    </head>
    <body>
    <div class="wrap">
        <div class="top-bar">
            <h2>💲 Presupuestos</h2>
            <div>
                <a href="/" class="btn btn-secondary">⬅️ Volver</a>
                <a href="/modulo/presupuestos/configuracion" class="btn btn-secondary">⚙️ Configuración</a>
                <a href="/modulo/presupuestos/nuevo" class="btn">+ Nuevo presupuesto</a>
            </div>
        </div>
        <div class="card">{tabla}</div>
    </div>
    </body>
    </html>
    """


# ─────────────────────────────────────────────────────────────────
# 2) Datos generales: crear / editar
# ─────────────────────────────────────────────────────────────────

def _form_datos_generales(titulo_pagina, datos=None, boton_texto="Guardar"):
    datos = datos or {}
    return f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>{_ESTILO_BASE}</style>
    </head>
    <body>
    <div class="wrap">
        <div class="top-bar">
            <h2>💲 {titulo_pagina}</h2>
            <a href="/modulo/presupuestos" class="btn btn-secondary">⬅️ Volver</a>
        </div>
        <div class="card">
            <form method="post">
                <div class="grid2">
                    <div>
                        <label>Cliente *</label>
                        <input type="text" name="cliente" value="{datos.get('cliente') or ''}" required>
                    </div>
                    <div>
                        <label>Planta</label>
                        <input type="text" name="planta" value="{datos.get('planta') or ''}">
                    </div>
                </div>
                <div class="grid2">
                    <div>
                        <label>Título</label>
                        <input type="text" name="titulo" value="{datos.get('titulo') or ''}">
                    </div>
                    <div>
                        <label>Fecha</label>
                        <input type="date" name="fecha" value="{datos.get('fecha') or ''}">
                    </div>
                </div>
                <label>Tipo de cambio de referencia</label>
                <input type="number" step="0.0001" name="tipo_cambio_referencia" value="{datos.get('tipo_cambio_referencia') if datos.get('tipo_cambio_referencia') is not None else ''}">
                <button type="submit" class="btn">{boton_texto}</button>
            </form>
        </div>
    </div>
    </body>
    </html>
    """


def _crear_tareas_estandar(db, presupuesto_id):
    """Autocrea las tareas habituales (Fabricación + Montaje, en 0) de
    TAREAS_ESTANDAR al crear un presupuesto nuevo. Si usan o no cada una
    depende del proyecto; acá solo quedan predeterminadas y listas."""
    config = obtener_config(db)
    for orden, nombre in enumerate(TAREAS_ESTANDAR, start=1):
        tarea_id = crear_tarea(db, presupuesto_id, nombre, orden=orden)
        try:
            crear_secciones_tarea(db, tarea_id, config, tipos=("FABRICACION", "MONTAJE"))
        except ValueError:
            eliminar_tarea(db, tarea_id)


@presupuestos_bp.route("/nuevo", methods=["GET", "POST"])
def vista_crear_presupuesto():
    if request.method == "POST":
        db = _db()
        presupuesto_id = crear_presupuesto(
            db,
            cliente=(request.form.get("cliente") or "").strip(),
            planta=(request.form.get("planta") or "").strip(),
            titulo=(request.form.get("titulo") or "").strip(),
            fecha=(request.form.get("fecha") or "").strip() or None,
            tipo_cambio_referencia=request.form.get("tipo_cambio_referencia") or None,
        )
        _crear_tareas_estandar(db, presupuesto_id)
        return redirect(f"/modulo/presupuestos/{presupuesto_id}")

    return _form_datos_generales("Nuevo presupuesto", boton_texto="Crear presupuesto")


@presupuestos_bp.route("/<int:presupuesto_id>/editar", methods=["GET", "POST"])
def vista_editar_presupuesto(presupuesto_id):
    db = _db()
    presupuesto = obtener_presupuesto(db, presupuesto_id)
    if not presupuesto:
        return "<h3>❌ Presupuesto no encontrado</h3>", 404

    if request.method == "POST":
        actualizar_presupuesto(
            db,
            presupuesto_id,
            cliente=(request.form.get("cliente") or "").strip(),
            planta=(request.form.get("planta") or "").strip(),
            titulo=(request.form.get("titulo") or "").strip(),
            fecha=(request.form.get("fecha") or "").strip() or None,
            tipo_cambio_referencia=request.form.get("tipo_cambio_referencia") or None,
        )
        return redirect(f"/modulo/presupuestos/{presupuesto_id}")

    return _form_datos_generales(f"Editar presupuesto #{presupuesto_id}", datos=presupuesto)


@presupuestos_bp.route("/<int:presupuesto_id>/estado", methods=["POST"])
def vista_cambiar_estado(presupuesto_id):
    db = _db()
    estado = (request.form.get("estado") or "").strip().lower()
    if estado in ESTADOS_PRESUPUESTO:
        actualizar_estado_presupuesto(db, presupuesto_id, estado)
    return redirect(f"/modulo/presupuestos/{presupuesto_id}")


@presupuestos_bp.route("/<int:presupuesto_id>/eliminar", methods=["POST"])
def vista_eliminar_presupuesto(presupuesto_id):
    db = _db()
    eliminar_presupuesto(db, presupuesto_id)
    return redirect("/modulo/presupuestos")


# ─────────────────────────────────────────────────────────────────
# Tareas: crear / eliminar (formularios simples, server-side)
# ─────────────────────────────────────────────────────────────────

@presupuestos_bp.route("/<int:presupuesto_id>/tareas", methods=["POST"])
def vista_crear_tarea(presupuesto_id):
    db = _db()
    nombre = (request.form.get("nombre") or "").strip()
    tipos = tuple(t for t in ("FABRICACION", "MONTAJE") if request.form.get(f"tipo_{t.lower()}"))
    if nombre and tipos:
        orden = int(request.form.get("orden") or 0)
        tarea_id = crear_tarea(db, presupuesto_id, nombre, orden=orden)
        try:
            crear_secciones_tarea(db, tarea_id, obtener_config(db), tipos=tipos)
        except ValueError:
            eliminar_tarea(db, tarea_id)
    return redirect(f"/modulo/presupuestos/{presupuesto_id}")


@presupuestos_bp.route("/tareas/<int:tarea_id>/eliminar", methods=["POST"])
def vista_eliminar_tarea(tarea_id):
    db = _db()
    presupuesto_id = int(request.form.get("presupuesto_id") or 0)
    eliminar_tarea(db, tarea_id)
    return redirect(f"/modulo/presupuestos/{presupuesto_id}")


# ─────────────────────────────────────────────────────────────────
# 3) Detalle de presupuesto: tareas + Fabricación/Montaje + items + recálculo
#    (items e items/recálculo se manejan 100% por JS contra la API JSON del
#    paso 3 — acá no se reescribe ninguna fórmula de cálculo).
# ─────────────────────────────────────────────────────────────────

_CAMPOS_POR_RUBRO_JS = json.dumps({
    "materiales_perfil": [
        {"name": "descripcion", "label": "Descripción (elemento)", "type": "text"},
        {"name": "perfil_id", "label": "ID Perfil (Suministros)", "type": "number"},
        {"name": "cantidad", "label": "Cantidad", "type": "number"},
        {"name": "largo_mm", "label": "Largo (mm)", "type": "number"},
        {"name": "precio_unitario_kg", "label": "Precio unitario ($/kg)", "type": "number"},
    ],
    "materiales_porcentaje": [
        {"name": "descripcion", "label": "Descripción (elemento)", "type": "text"},
        {"name": "porcentaje", "label": "% sobre peso de materiales", "type": "number"},
        {"name": "precio_unitario_kg", "label": "Valor unitario ($/kg)", "type": "number"},
    ],
    "materiales_no_listado": [
        {"name": "descripcion", "label": "Descripción", "type": "text"},
        {"name": "cantidad", "label": "Cantidad", "type": "number"},
        {"name": "unidad", "label": "Unidad", "type": "text"},
        {"name": "precio_unitario", "label": "Precio unitario", "type": "number"},
    ],
    "bulones": [{"name": "porcentaje", "label": "% sobre materiales", "type": "number"}],
    "pintura": [
        {"name": "esquema_id", "label": "ID Esquema de pintura", "type": "number"},
        {"name": "precio_unitario_m2", "label": "Precio unitario ($/m2)", "type": "number"},
    ],
    "fletes": [
        {"name": "descripcion", "label": "Descripción", "type": "text"},
        {"name": "cantidad", "label": "Cantidad", "type": "number"},
        {"name": "precio_unitario", "label": "Precio unitario", "type": "number"},
    ],
    "subcontratos": [
        {"name": "descripcion", "label": "Descripción", "type": "text"},
        {"name": "cantidad", "label": "Cantidad", "type": "number"},
        {"name": "precio_unitario", "label": "Precio unitario", "type": "number"},
    ],
    "mano_obra": [
        {"name": "operarios", "label": "Operarios", "type": "number"},
        {"name": "dias", "label": "Días", "type": "number"},
        {"name": "tarifa_dh", "label": "UNITARIO", "type": "number"},
    ],
    "consumibles": [
        {"name": "operarios", "label": "Operarios", "type": "number"},
        {"name": "dias", "label": "Días", "type": "number"},
        {"name": "tarifa_dh", "label": "UNITARIO", "type": "number"},
    ],
    "ingenieria": [{"name": "monto", "label": "Monto", "type": "number"}],
    "equipo": [
        {"name": "equipo_id", "label": "ID Equipo (catálogo)", "type": "number"},
        {"name": "dias", "label": "Días", "type": "number"},
        {"name": "tarifa_dia", "label": "UNITARIO", "type": "number"},
    ],
    "ingeniero": [
        {"name": "tarifa_dia", "label": "UNITARIO", "type": "number"},
        {"name": "dias", "label": "Días", "type": "number"},
    ],
    "tecnico_hys": [
        {"name": "tarifa_dia", "label": "UNITARIO", "type": "number"},
        {"name": "dias", "label": "Días", "type": "number"},
    ],
}, ensure_ascii=False)

_RUBROS_POR_SECCION_JS = json.dumps(RUBROS_POR_SECCION, ensure_ascii=False)
_TIPOS_SECCION_JS = json.dumps(list(TIPOS_SECCION), ensure_ascii=False)


@presupuestos_bp.route("/<int:presupuesto_id>", methods=["GET"])
def vista_detalle_presupuesto(presupuesto_id):
    db = _db()
    presupuesto = obtener_presupuesto(db, presupuesto_id)
    if not presupuesto:
        return "<h3>❌ Presupuesto no encontrado</h3>", 404

    tareas = listar_tareas(db, presupuesto_id)

    opciones_estado = "".join(
        f'<option value="{e}" {"selected" if e == presupuesto["estado"] else ""}>{e.upper()}</option>'
        for e in ESTADOS_PRESUPUESTO
    )

    tabs_tareas_html = "".join(
        f'<button type="button" class="tab-tarea" id="tab-tarea-{t["id"]}" onclick="seleccionarTarea({t["id"]})">{t["nombre"]}</button>'
        for t in tareas
    )
    if not tareas:
        tabs_tareas_html = ""
    _tareas_info_js = json.dumps(
        {str(t["id"]): {"nombre": t["nombre"], "orden": t["orden"]} for t in tareas}, ensure_ascii=False
    )
    _tareas_ids_js = json.dumps([t["id"] for t in tareas])

    checkboxes_tipos = "".join(
        f'<label style="display:inline-flex;align-items:center;gap:6px;font-weight:400;margin-right:16px;">'
        f'<input type="checkbox" name="tipo_{tipo.lower()}" value="1" checked style="width:auto;margin:0;"> {tipo.title()}</label>'
        for tipo in TIPOS_SECCION
    )

    return f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>{_ESTILO_BASE}</style>
    </head>
    <body>
    <div class="wrap">
        <div class="top-bar">
            <h2>💲 Presupuesto #{presupuesto_id}</h2>
            <div>
                <a href="/modulo/presupuestos" class="btn btn-secondary">⬅️ Volver al listado</a>
                <a href="/modulo/presupuestos/{presupuesto_id}/editar" class="btn">✏️ Editar datos</a>
                <a href="/modulo/presupuestos/{presupuesto_id}/resumen" class="btn">📊 Resumen y reportes</a>
            </div>
        </div>

        <div class="card">
            <div class="grid3">
                <div><span class="muted">Cliente</span><br><b>{presupuesto['cliente'] or '-'}</b></div>
                <div><span class="muted">Planta</span><br><b>{presupuesto['planta'] or '-'}</b></div>
                <div><span class="muted">Título</span><br><b>{presupuesto['titulo'] or '-'}</b></div>
            </div>
            <div class="grid3" style="margin-top:8px;">
                <div><span class="muted">Fecha</span><br><b>{presupuesto['fecha'] or '-'}</b></div>
                <div><span class="muted">Tipo de cambio ref.</span><br><b>{presupuesto['tipo_cambio_referencia'] or '-'}</b></div>
                <div><span class="muted">Estado actual</span><br>{_badge_estado(presupuesto['estado'])}</div>
            </div>
            <form method="post" action="/modulo/presupuestos/{presupuesto_id}/estado" style="margin-top:12px;display:flex;gap:8px;align-items:center;">
                <select name="estado" style="max-width:220px;margin-bottom:0;">{opciones_estado}</select>
                <button type="submit" class="btn btn-sm">Cambiar estado</button>
            </form>
        </div>

        <div class="card">
            <div style="display:flex;justify-content:space-between;align-items:center;">
                <h3 style="margin:0;">Totales del presupuesto</h3>
                <button type="button" class="btn btn-sm" onclick="recalcularPresupuesto()">🔄 Recalcular todo</button>
            </div>
            <div id="totales-presupuesto" class="muted" style="margin-top:8px;">Sin calcular todavía.</div>
        </div>

        <div class="card">
            <h3 style="margin-top:0;">Tareas</h3>
            <div class="tabs-tareas">{tabs_tareas_html}</div>
            <div id="tarea-contenido" class="muted">{"Seleccioná una pestaña para ver el detalle de la tarea." if tareas else "Este presupuesto todavía no tiene tareas cargadas."}</div>
            <details style="margin-top:10px;">
                <summary style="cursor:pointer;font-weight:700;color:#4338ca;">+ Agregar tarea</summary>
                <form method="post" action="/modulo/presupuestos/{presupuesto_id}/tareas" style="margin-top:10px;" onsubmit="return validarSeccionesTarea(this);">
                    <label>Nombre de la tarea (ej: Estructura metálica, Chapeado, Correas, Zinguería...)</label>
                    <input type="text" name="nombre" required>
                    <label>Secciones que aplican</label>
                    <div style="margin-bottom:10px;">{checkboxes_tipos}</div>
                    <label>Orden</label>
                    <input type="number" name="orden" value="0">
                    <button type="submit" class="btn">Agregar tarea</button>
                </form>
            </details>
        </div>
    </div>

    <script>
    const PRESUPUESTO_ID = {presupuesto_id};
    const API = "/modulo/presupuestos/api";
    const CAMPOS_POR_RUBRO = {_CAMPOS_POR_RUBRO_JS};
    const RUBROS_POR_SECCION = {_RUBROS_POR_SECCION_JS};
    const TIPOS_SECCION = {_TIPOS_SECCION_JS};
    let CONFIG_DEFAULTS = {{}};
    let EQUIPOS = [];
    let ESQUEMAS = [];
    let PERFILES = [];
    const seccionTipoPorId = {{}};
    const ordenItemsPorSeccion = {{}};   // seccionId -> [itemId, itemId, ...] en el mismo orden que devuelve el recálculo
    const debounceTimers = {{}};         // clave -> timeoutId
    const AUTOSAVE_DEBOUNCE_MS = 650;
    const TAREAS_INFO = {_tareas_info_js};   // tareaId (string) -> {{nombre, orden}}
    const TAREAS_IDS = {_tareas_ids_js};     // [tareaId, ...] en orden
    let tareaActivaId = null;

    function seleccionarTarea(tareaId) {{
        tareaActivaId = tareaId;
        document.querySelectorAll(".tab-tarea").forEach(btn => btn.classList.remove("activo"));
        const tabBtn = document.getElementById(`tab-tarea-${{tareaId}}`);
        if (tabBtn) tabBtn.classList.add("activo");
        cargarTarea(tareaId);
    }}

    function validarSeccionesTarea(form) {{
        const marcados = form.querySelectorAll('input[type=checkbox][name^="tipo_"]:checked');
        if (marcados.length === 0) {{
            alert("Elegí al menos una sección (Fabricación y/o Montaje).");
            return false;
        }}
        return true;
    }}

    function cargarCatalogos() {{
        return Promise.all([
            fetch(`${{API}}/config`).then(r => r.json()).catch(() => ({{}})),
            fetch(`${{API}}/equipos`).then(r => r.json()).catch(() => ({{equipos: []}})),
            fetch(`${{API}}/esquemas-pintura`).then(r => r.json()).catch(() => ({{esquemas: []}})),
            fetch(`${{API}}/perfiles`).then(r => r.json()).catch(() => ({{perfiles: []}})),
        ]).then(([config, equipos, esquemas, perfiles]) => {{
            CONFIG_DEFAULTS = config || {{}};
            EQUIPOS = (equipos || {{}}).equipos || [];
            ESQUEMAS = (esquemas || {{}}).esquemas || [];
            PERFILES = (perfiles || {{}}).perfiles || [];
        }});
    }}
    cargarCatalogos().then(() => {{
        if (TAREAS_IDS.length) seleccionarTarea(TAREAS_IDS[0]);
    }});

    function fmtMoney(v) {{
        return "$ " + (Number(v) || 0).toLocaleString("es-AR", {{minimumFractionDigits: 2, maximumFractionDigits: 2}});
    }}
    function fmtNum(v, dec) {{
        return (Number(v) || 0).toLocaleString("es-AR", {{minimumFractionDigits: dec, maximumFractionDigits: dec}});
    }}

    function clavePorRubro(rubro, tipoItem) {{
        if (rubro === "materiales") {{
            if (tipoItem === "porcentaje") return "materiales_porcentaje";
            if (tipoItem === "no_listado") return "materiales_no_listado";
            return "materiales_perfil";
        }}
        return rubro;
    }}

    // ─────────────────────────────────────────────────────────────
    // Autosave: cada input dispara programarAutosaveItem (debounce) y
    // onblur fuerza el guardado inmediato. Nunca hay botón "Guardar".
    // ─────────────────────────────────────────────────────────────

    function mostrarGuardando(tareaId, activo) {{
        const badge = document.getElementById(`guardando-${{tareaId}}`);
        if (badge) badge.classList.toggle("activo", !!activo);
    }}

    function programarAutosaveItem(itemId, tareaId) {{
        const clave = `item-${{itemId}}`;
        clearTimeout(debounceTimers[clave]);
        mostrarGuardando(tareaId, true);
        debounceTimers[clave] = setTimeout(() => guardarItemAhora(itemId, tareaId), AUTOSAVE_DEBOUNCE_MS);
    }}

    function guardarItemInmediato(itemId, tareaId) {{
        const clave = `item-${{itemId}}`;
        clearTimeout(debounceTimers[clave]);
        guardarItemAhora(itemId, tareaId);
    }}

    function leerDatosDeItem(itemId) {{
        const fila = document.querySelector(`tr[data-item-id="${{itemId}}"]`);
        const rubro = fila ? fila.getAttribute("data-rubro") : null;
        const tipoItemSel = document.getElementById(`tipoitem-${{itemId}}`);
        const tipoItem = tipoItemSel ? tipoItemSel.value : (fila ? (fila.getAttribute("data-tipo-item") || null) : null);
        const clave = clavePorRubro(rubro, tipoItem || "perfil");
        const campos = CAMPOS_POR_RUBRO[clave] || [];
        const datos = {{}};
        campos.forEach(c => {{
            const el = document.getElementById(`campo-${{itemId}}-${{c.name}}`);
            if (!el) return;
            if (c.name === "porcentaje") {{
                datos[c.name] = (parseFloat(el.value || 0)) / 100;
            }} else {{
                datos[c.name] = c.type === "number" ? parseFloat(el.value || 0) : el.value;
            }}
        }});
        return {{ rubro, tipoItem: rubro === "materiales" ? tipoItem : null, datos }};
    }}

    function guardarItemAhora(itemId, tareaId) {{
        const {{ rubro, tipoItem, datos }} = leerDatosDeItem(itemId);
        if (!rubro) {{ mostrarGuardando(tareaId, false); return; }}
        fetch(`${{API}}/items/${{itemId}}`, {{
            method: "PUT", headers: {{"Content-Type": "application/json"}},
            body: JSON.stringify({{ rubro, datos, tipo_item: tipoItem }})
        }})
            .then(r => r.json().then(j => ({{ ok: r.ok, j }})))
            .then(({{ ok, j }}) => {{
                mostrarGuardando(tareaId, false);
                if (!ok) {{ alert(j.error || "No se pudo guardar el cambio."); return; }}
                refrescarCalculos(tareaId);
            }})
            .catch(err => {{ mostrarGuardando(tareaId, false); alert("Error de red: " + err); }});
    }}

    function agregarLinea(seccionId, tareaId, rubro, tipoItem, datosIniciales) {{
        fetch(`${{API}}/secciones/${{seccionId}}/items`, {{
            method: "POST", headers: {{"Content-Type": "application/json"}},
            body: JSON.stringify({{ rubro, datos: datosIniciales || {{}}, tipo_item: tipoItem || null }})
        }})
            .then(r => r.json().then(j => ({{ ok: r.ok, j }})))
            .then(({{ ok, j }}) => {{
                if (!ok) {{ alert(j.error || "No se pudo agregar la línea."); return; }}
                cargarTarea(tareaId);
            }})
            .catch(err => alert("Error de red: " + err));
    }}

    function eliminarLinea(itemId, tareaId) {{
        if (!confirm("¿Eliminar esta línea?")) return;
        fetch(`${{API}}/items/${{itemId}}`, {{ method: "DELETE" }})
            .then(r => r.json().then(j => ({{ ok: r.ok, j }})))
            .then(({{ ok, j }}) => {{
                if (!ok) {{ alert(j.error || "No se pudo eliminar."); return; }}
                cargarTarea(tareaId);
            }});
    }}

    function cambiarTipoItemMaterial(itemId, seccionId, tareaId, nuevoTipo) {{
        // Cambia perfil <-> porcentaje: re-renderiza toda la fila (columnas distintas) y guarda.
        const fila = document.querySelector(`tr[data-item-id="${{itemId}}"]`);
        if (!fila) return;
        const itemFalso = {{ id: itemId, rubro: "materiales", tipo_item: nuevoTipo, datos: {{}}, subtotal: 0 }};
        fila.outerHTML = filaMaterialHtml(seccionId, seccionTipoPorId[seccionId], tareaId, itemFalso);
        guardarItemInmediato(itemId, tareaId);
    }}


    function autocompletarTarifaEquipo(itemId, selectEl) {{
        const tarifaEl = document.getElementById(`campo-${{itemId}}-tarifa_dia`);
        if (!tarifaEl) return;
        const opt = selectEl.options[selectEl.selectedIndex];
        tarifaEl.value = opt ? (opt.getAttribute("data-tarifa") || 0) : 0;
    }}

    function autocompletarPrecioEsquema(itemId, selectEl) {{
        const precioEl = document.getElementById(`campo-${{itemId}}-precio_unitario_m2`);
        if (!precioEl) return;
        const opt = selectEl.options[selectEl.selectedIndex];
        precioEl.value = opt ? (opt.getAttribute("data-precio") || 0) : 0;
    }}

    // ─────────────────────────────────────────────────────────────
    // Render (planilla continua: todos los rubros de la sección visibles
    // a la vez, apilados, sin tabs ni pasos).
    // ─────────────────────────────────────────────────────────────

    function campoControlHtml(itemId, campo, valorActual, seccionTipo, rubro, tareaId) {{
        const val = (valorActual === undefined || valorActual === null) ? "" : valorActual;
        let evt = `oninput="programarAutosaveItem(${{itemId}}, ${{tareaId}})" onblur="guardarItemInmediato(${{itemId}}, ${{tareaId}})"`;
        if (rubro === "mano_obra" && (campo.name === "operarios" || campo.name === "dias")) {{
            evt = `oninput="programarAutosaveItem(${{itemId}}, ${{tareaId}}); sincronizarConsumibles(_seccionDeItem(${{itemId}}), ${{tareaId}})" onblur="guardarItemInmediato(${{itemId}}, ${{tareaId}}); sincronizarConsumibles(_seccionDeItem(${{itemId}}), ${{tareaId}})"`;
        }}
        if (campo.name === "perfil_id") {{
            const opciones = PERFILES.map(p => `<option value="${{p.id}}" ${{String(p.id) === String(val) ? "selected" : ""}}>${{p.label}}</option>`).join("");
            return `<select id="campo-${{itemId}}-${{campo.name}}" onchange="guardarItemInmediato(${{itemId}}, ${{tareaId}})">
                <option value="">-- Elegir --</option>
                ${{opciones}}
            </select>`;
        }}
        if (campo.name === "equipo_id") {{
            const opciones = EQUIPOS.map(e => `<option value="${{e.id}}" data-tarifa="${{e.tarifa_dia_default || 0}}" ${{String(e.id) === String(val) ? "selected" : ""}}>${{e.nombre}}</option>`).join("");
            return `<select id="campo-${{itemId}}-${{campo.name}}" onchange="autocompletarTarifaEquipo(${{itemId}}, this); guardarItemInmediato(${{itemId}}, ${{tareaId}});">
                <option value="">-- Elegir --</option>
                ${{opciones}}
            </select>`;
        }}
        if (campo.name === "esquema_id") {{
            const opciones = ESQUEMAS.map(e => `<option value="${{e.id}}" data-precio="${{e.precio_unitario_m2_default || 0}}" ${{String(e.id) === String(val) ? "selected" : ""}}>${{e.nombre}}</option>`).join("");
            return `<select id="campo-${{itemId}}-${{campo.name}}" onchange="autocompletarPrecioEsquema(${{itemId}}, this); guardarItemInmediato(${{itemId}}, ${{tareaId}});">
                <option value="">-- Elegir --</option>
                ${{opciones}}
            </select>`;
        }}
        if (campo.name === "porcentaje") {{
            const valorPct = (val === "" ? "" : (Number(val) || 0) * 100);
            return `<input type="number" step="any" id="campo-${{itemId}}-${{campo.name}}" value="${{valorPct}}" ${{evt}}>`;
        }}
        let valorInicial = val;
        let soloLectura = "";
        if (campo.name === "tarifa_dh") {{
            if (val === "") {{
                if (rubro === "mano_obra") {{
                    valorInicial = seccionTipo === "FABRICACION" ? (CONFIG_DEFAULTS.tarifa_dh_taller_default || 0) : (CONFIG_DEFAULTS.tarifa_dh_obra_default || 0);
                }} else if (rubro === "consumibles") {{
                    valorInicial = seccionTipo === "FABRICACION" ? (CONFIG_DEFAULTS.tarifa_consumible_dh_taller_default || 0) : (CONFIG_DEFAULTS.tarifa_consumible_dh_obra_default || 0);
                }}
            }}
            if (rubro === "consumibles") {{ soloLectura = `readonly title='Costo unitario prefijado (no editable)'`; }}
        }}
        if (rubro === "consumibles" && (campo.name === "operarios" || campo.name === "dias")) {{
            soloLectura = `readonly title='Se sincroniza automáticamente con Mano de obra'`;
        }}
        return `<input type="${{campo.type}}" step="any" id="campo-${{itemId}}-${{campo.name}}" value="${{valorInicial}}" ${{evt}} ${{soloLectura}}>`;
    }}

    function _seccionDeItem(itemId) {{
        const fila = document.querySelector(`tr[data-item-id="${{itemId}}"]`);
        return fila ? fila.getAttribute("data-seccion-id") : null;
    }}

    function sincronizarConsumibles(seccionId, tareaId) {{
        if (!seccionId) return;
        const filaMano = document.querySelector(`tr[data-seccion-id="${{seccionId}}"][data-rubro="mano_obra"]`);
        const filaCons = document.querySelector(`tr[data-seccion-id="${{seccionId}}"][data-rubro="consumibles"]`);
        if (!filaMano || !filaCons) return;
        const manoId = filaMano.getAttribute("data-item-id");
        const consId = filaCons.getAttribute("data-item-id");
        const opMano = document.getElementById(`campo-${{manoId}}-operarios`);
        const diasMano = document.getElementById(`campo-${{manoId}}-dias`);
        const opCons = document.getElementById(`campo-${{consId}}-operarios`);
        const diasCons = document.getElementById(`campo-${{consId}}-dias`);
        let cambio = false;
        if (opMano && opCons && opCons.value !== opMano.value) {{ opCons.value = opMano.value; cambio = true; }}
        if (diasMano && diasCons && diasCons.value !== diasMano.value) {{ diasCons.value = diasMano.value; cambio = true; }}
        if (cambio && consId) guardarItemInmediato(parseInt(consId), tareaId);
    }}

    function campoInputHtml(itemId, campo, valorActual, seccionTipo, rubro, tareaId) {{
        const control = campoControlHtml(itemId, campo, valorActual, seccionTipo, rubro, tareaId);
        const sufijo = campo.name === "porcentaje" ? `<span style="font-size:11px;color:#475569;">%</span>` : "";
        return `<div>
            <label style="font-size:11px;">${{campo.label}}</label>
            <div style="display:flex;align-items:center;gap:4px;">${{control}}${{sufijo}}</div>
        </div>`;
    }}

    function camposItemHtml(itemId, rubro, tipoItem, datos, seccionTipo, tareaId) {{
        const clave = clavePorRubro(rubro, tipoItem || "perfil");
        const campos = CAMPOS_POR_RUBRO[clave] || [];
        return `<div style="display:flex;gap:8px;flex-wrap:wrap;">${{campos.map(c => campoInputHtml(itemId, c, (datos || {{}})[c.name], seccionTipo, rubro, tareaId)).join("")}}</div>`;
    }}

    function filaItemHtml(seccionId, seccionTipo, tareaId, item) {{
        return `<tr data-item-id="${{item.id}}" data-rubro="${{item.rubro}}" data-tipo-item="${{item.tipo_item || ''}}" data-seccion-id="${{seccionId}}">
            <td style="width:70%;" data-celda-campos>${{camposItemHtml(item.id, item.rubro, item.tipo_item, item.datos, seccionTipo, tareaId)}}</td>
            <td style="width:15%;text-align:right;font-weight:700;"><span id="subtotal-item-${{item.id}}">${{fmtMoney(item.subtotal)}}</span></td>
            <td style="width:15%;">
                <button type="button" class="btn btn-icon btn-danger" onclick="eliminarLinea(${{item.id}}, ${{tareaId}})">✕</button>
            </td>
        </tr>`;
    }}

    function filaMaterialHtml(seccionId, seccionTipo, tareaId, item) {{
        const tipoItem = item.tipo_item || "perfil";
        const datos = item.datos || {{}};
        const descCtl = campoControlHtml(item.id, {{name: "descripcion", label: "Descripción", type: "text"}}, datos.descripcion, seccionTipo, "materiales", tareaId);
        let celdas;
        if (tipoItem === "porcentaje") {{
            celdas = `
                <td class="muted" style="text-align:center;">–</td>
                <td class="muted" style="text-align:center;">–</td>
                <td class="muted" style="text-align:center;">–</td>
                <td>${{campoControlHtml(item.id, {{name: "precio_unitario_kg", label: "$/kg", type: "number"}}, datos.precio_unitario_kg, seccionTipo, "materiales", tareaId)}}</td>
                <td style="display:flex;align-items:center;gap:4px;">${{campoControlHtml(item.id, {{name: "porcentaje", label: "%", type: "number"}}, datos.porcentaje, seccionTipo, "materiales", tareaId)}}<span style="font-size:11px;color:#475569;">%</span></td>`;
        }} else {{
            celdas = `
                <td>${{campoControlHtml(item.id, {{name: "perfil_id", label: "Perfil", type: "text"}}, datos.perfil_id, seccionTipo, "materiales", tareaId)}}</td>
                <td>${{campoControlHtml(item.id, {{name: "cantidad", label: "Cantidad", type: "number"}}, datos.cantidad, seccionTipo, "materiales", tareaId)}}</td>
                <td>${{campoControlHtml(item.id, {{name: "largo_mm", label: "Largo (mm)", type: "number"}}, datos.largo_mm, seccionTipo, "materiales", tareaId)}}</td>
                <td>${{campoControlHtml(item.id, {{name: "precio_unitario_kg", label: "$/kg", type: "number"}}, datos.precio_unitario_kg, seccionTipo, "materiales", tareaId)}}</td>
                <td class="muted" style="text-align:center;">–</td>`;
        }}
        return `<tr data-item-id="${{item.id}}" data-rubro="materiales" data-tipo-item="${{tipoItem}}" data-seccion-id="${{seccionId}}">
            <td>
                <select id="tipoitem-${{item.id}}" onchange="cambiarTipoItemMaterial(${{item.id}}, ${{seccionId}}, ${{tareaId}}, this.value)">
                    <option value="perfil" ${{tipoItem === "perfil" ? "selected" : ""}}>Perfil</option>
                    <option value="porcentaje" ${{tipoItem === "porcentaje" ? "selected" : ""}}>Placas</option>
                </select>
            </td>
            <td>${{descCtl}}</td>
            ${{celdas}}
            <td style="text-align:right;"><span id="kg-item-${{item.id}}">${{fmtNum(item.peso || 0, 1)}}</span></td>
            <td style="text-align:right;"><span id="m2-item-${{item.id}}">${{tipoItem === "perfil" ? fmtNum(item.m2 || 0, 2) : "–"}}</span></td>
            <td style="text-align:right;font-weight:700;"><span id="subtotal-item-${{item.id}}">${{fmtMoney(item.subtotal)}}</span></td>
            <td><button type="button" class="btn btn-icon btn-danger" onclick="eliminarLinea(${{item.id}}, ${{tareaId}})">✕</button></td>
        </tr>`;
    }}

    function filaMaterialNoListadoHtml(seccionId, seccionTipo, tareaId, item) {{
        const datos = item.datos || {{}};
        return `<tr data-item-id="${{item.id}}" data-rubro="materiales" data-tipo-item="no_listado" data-seccion-id="${{seccionId}}">
            <td>${{campoControlHtml(item.id, {{name: "descripcion", label: "Descripción", type: "text"}}, datos.descripcion, seccionTipo, "materiales", tareaId)}}</td>
            <td>${{campoControlHtml(item.id, {{name: "cantidad", label: "Cantidad", type: "number"}}, datos.cantidad, seccionTipo, "materiales", tareaId)}}</td>
            <td>${{campoControlHtml(item.id, {{name: "unidad", label: "Unidad", type: "text"}}, datos.unidad, seccionTipo, "materiales", tareaId)}}</td>
            <td>${{campoControlHtml(item.id, {{name: "precio_unitario", label: "Precio unitario", type: "number"}}, datos.precio_unitario, seccionTipo, "materiales", tareaId)}}</td>
            <td style="text-align:right;font-weight:700;"><span id="subtotal-item-${{item.id}}">${{fmtMoney(item.subtotal)}}</span></td>
            <td><button type="button" class="btn btn-icon btn-danger" onclick="eliminarLinea(${{item.id}}, ${{tareaId}})">✕</button></td>
        </tr>`;
    }}

    function rubroBlockHtml(seccionId, seccionTipo, tareaId, rubro, items) {{
        ordenItemsPorSeccion[seccionId] = ordenItemsPorSeccion[seccionId] || {{}};
        ordenItemsPorSeccion[seccionId][rubro] = items.map(it => it.id);

        if (rubro === "materiales") {{
            const perfilItems = items.filter(it => (it.tipo_item || "perfil") === "perfil");
            const placasItems = items.filter(it => it.tipo_item === "porcentaje");
            const noListadoItems = items.filter(it => it.tipo_item === "no_listado");
            // Placas siempre se muestra al final de la tabla (depende del total de la sección)
            // y ya viene autocreada (una única línea, sin botón "Agregar").
            const filas = perfilItems.concat(placasItems).map(it => filaMaterialHtml(seccionId, seccionTipo, tareaId, it)).join("");
            const filasNoListado = noListadoItems.map(it => filaMaterialNoListadoHtml(seccionId, seccionTipo, tareaId, it)).join("");
            return `<div class="rubro-block">
                <h4>materiales</h4>
                <div style="overflow-x:auto;">
                <table class="tabla-materiales">
                    <thead><tr>
                        <th>Tipo</th><th>Descripción</th><th>Perfil</th><th>Cantidad</th><th>Largo (mm)</th><th>$/kg</th>
                        <th>% Placas</th><th>Total (kg)</th><th>m2</th><th>Subtotal</th><th></th>
                    </tr></thead>
                    <tbody>${{filas || '<tr><td colspan="11" class="muted">Sin líneas todavía.</td></tr>'}}</tbody>
                </table>
                </div>
                <div style="margin-top:6px;">
                    <button type="button" class="btn btn-sm btn-secondary" onclick="agregarLinea(${{seccionId}}, ${{tareaId}}, 'materiales', 'perfil')">+ Perfil</button>
                </div>
                <div style="margin-top:6px;padding-top:5px;border-top:1px dashed #cbd5e1;">
                    <h4 style="font-size:12px;">materiales no listados</h4>
                    <div style="overflow-x:auto;">
                    <table class="tabla-materiales">
                        <thead><tr>
                            <th>Descripción</th><th>Cantidad</th><th>Unidad</th><th>Precio unitario</th><th>Precio total</th><th></th>
                        </tr></thead>
                        <tbody>${{filasNoListado || '<tr><td colspan="6" class="muted">Sin líneas todavía.</td></tr>'}}</tbody>
                    </table>
                    </div>
                    <div style="margin-top:6px;">
                        <button type="button" class="btn btn-sm btn-secondary" onclick="agregarLinea(${{seccionId}}, ${{tareaId}}, 'materiales', 'no_listado')">+ Agregar material no listado</button>
                    </div>
                </div>
            </div>`;
        }}

        const filas = items.map(it => filaItemHtml(seccionId, seccionTipo, tareaId, it)).join("");
        const RUBROS_UNA_LINEA = ["bulones", "consumibles", "pintura", "mano_obra", "ingenieria", "fletes", "ingeniero", "tecnico_hys"];
        let botonesAgregar;
        if (RUBROS_UNA_LINEA.includes(rubro)) {{
            // Estos rubros se autocrean (ver cargarTarea): nunca hace falta un botón "Agregar".
            botonesAgregar = "";
        }} else {{
            botonesAgregar = `<button type="button" class="btn btn-sm btn-secondary" onclick="agregarLinea(${{seccionId}}, ${{tareaId}}, '${{rubro}}', null)">+ Agregar línea</button>`;
        }}
        return `<div class="rubro-block">
            <h4>${{rubro.replace(/_/g, " ")}}</h4>
            <table>
                ${{filas || '<tr><td class="muted">Sin líneas todavía.</td></tr>'}}
            </table>
            <div style="margin-top:6px;">${{botonesAgregar}}</div>
        </div>`;
    }}

    function bloqueManoObraConsumiblesHtml(seccionId, seccionTipo, tareaId, itemsManoObra, itemsConsumibles) {{
        ordenItemsPorSeccion[seccionId] = ordenItemsPorSeccion[seccionId] || {{}};
        ordenItemsPorSeccion[seccionId]["mano_obra"] = itemsManoObra.map(it => it.id);
        ordenItemsPorSeccion[seccionId]["consumibles"] = itemsConsumibles.map(it => it.id);
        const filasManoObra = itemsManoObra.map(it => filaItemHtml(seccionId, seccionTipo, tareaId, it)).join("");
        const filasConsumibles = itemsConsumibles.map(it => filaItemHtml(seccionId, seccionTipo, tareaId, it)).join("");
        return `<div class="rubro-block">
            <div style="display:flex;gap:10px;flex-wrap:wrap;align-items:stretch;">
                <div style="flex:1 1 260px;min-width:220px;">
                    <h4>mano de obra</h4>
                    <table>${{filasManoObra || '<tr><td class="muted">Sin líneas todavía.</td></tr>'}}</table>
                </div>
                <div style="flex:1 1 260px;min-width:220px;">
                    <h4>consumibles</h4>
                    <table>${{filasConsumibles || '<tr><td class="muted">Sin líneas todavía.</td></tr>'}}</table>
                </div>
                <div style="flex:0 0 120px;display:flex;flex-direction:column;gap:4px;justify-content:center;">
                    <div class="mini-resultado indicador" style="padding:5px 8px;">
                        <b style="font-size:11px;">USD/KG</b>
                        <div id="indic-usdkg-${{seccionId}}" style="font-weight:700;">–</div>
                    </div>
                    <div class="mini-resultado indicador" style="padding:5px 8px;">
                        <b style="font-size:11px;">KG/HH</b>
                        <div id="indic-kghh-${{seccionId}}" style="font-weight:700;">–</div>
                    </div>
                </div>
            </div>
        </div>`;
    }}

    function renderSeccionCompleta(tareaId, seccion) {{
        seccionTipoPorId[seccion.id] = seccion.tipo;
        const chipClase = seccion.tipo === "FABRICACION" ? "chip-fab" : "chip-mon";
        const itemsPorRubro = {{}};
        (seccion.items || []).forEach(it => {{
            (itemsPorRubro[it.rubro] = itemsPorRubro[it.rubro] || []).push(it);
        }});
        let bloquesRubros;
        if (seccion.tipo === "FABRICACION") {{
            // Orden fijo pedido (Excel de referencia): materiales, ingeniería, bulones,
            // pintura, [mano de obra + consumibles combinados con indicadores], subcontratos, fletes.
            bloquesRubros = ["materiales", "ingenieria", "bulones", "pintura"]
                .map(rubro => rubroBlockHtml(seccion.id, seccion.tipo, tareaId, rubro, itemsPorRubro[rubro] || []))
                .join("");
            bloquesRubros += bloqueManoObraConsumiblesHtml(
                seccion.id, seccion.tipo, tareaId, itemsPorRubro["mano_obra"] || [], itemsPorRubro["consumibles"] || []
            );
            bloquesRubros += ["subcontratos", "fletes"]
                .map(rubro => rubroBlockHtml(seccion.id, seccion.tipo, tareaId, rubro, itemsPorRubro[rubro] || []))
                .join("");
        }} else {{
            const rubrosDeLaSeccion = RUBROS_POR_SECCION[seccion.tipo] || [];
            bloquesRubros = rubrosDeLaSeccion
                .map(rubro => rubroBlockHtml(seccion.id, seccion.tipo, tareaId, rubro, itemsPorRubro[rubro] || []))
                .join("");
        }}

        return `<div class="seccion-wrap" id="seccion-wrap-${{seccion.id}}">
            <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:6px;">
                <span class="chip ${{chipClase}}">${{seccion.tipo}}</span>
                <div style="display:flex;gap:6px;align-items:center;flex-wrap:wrap;">
                    <label style="margin:0;font-size:11px;">GG</label>
                    <div style="display:flex;align-items:center;gap:2px;">
                        <input type="number" step="any" id="pct-gg-${{seccion.id}}" value="${{(seccion.gg_pct || 0) * 100}}" style="width:70px;margin:0;"
                            oninput="programarAutosavePct(${{seccion.id}}, ${{tareaId}})" onblur="guardarPctInmediato(${{seccion.id}}, ${{tareaId}})">
                        <span style="font-size:11px;color:#475569;">%</span>
                    </div>
                    <label style="margin:0;font-size:11px;">Benef.</label>
                    <div style="display:flex;align-items:center;gap:2px;">
                        <input type="number" step="any" id="pct-ben-${{seccion.id}}" value="${{(seccion.beneficio_pct || 0) * 100}}" style="width:70px;margin:0;"
                            oninput="programarAutosavePct(${{seccion.id}}, ${{tareaId}})" onblur="guardarPctInmediato(${{seccion.id}}, ${{tareaId}})">
                        <span style="font-size:11px;color:#475569;">%</span>
                    </div>
                    <label style="margin:0;font-size:11px;">Imp.</label>
                    <div style="display:flex;align-items:center;gap:2px;">
                        <input type="number" step="any" id="pct-imp-${{seccion.id}}" value="${{(seccion.imp_pct || 0) * 100}}" style="width:70px;margin:0;"
                            oninput="programarAutosavePct(${{seccion.id}}, ${{tareaId}})" onblur="guardarPctInmediato(${{seccion.id}}, ${{tareaId}})">
                        <span style="font-size:11px;color:#475569;">%</span>
                    </div>
                </div>
            </div>
            ${{bloquesRubros}}
        </div>`;
    }}

    function programarAutosavePct(seccionId, tareaId) {{
        const clave = `pct-${{seccionId}}`;
        clearTimeout(debounceTimers[clave]);
        mostrarGuardando(tareaId, true);
        debounceTimers[clave] = setTimeout(() => guardarPctAhora(seccionId, tareaId), AUTOSAVE_DEBOUNCE_MS);
    }}

    function guardarPctInmediato(seccionId, tareaId) {{
        clearTimeout(debounceTimers[`pct-${{seccionId}}`]);
        guardarPctAhora(seccionId, tareaId);
    }}

    function guardarPctAhora(seccionId, tareaId) {{
        const gg = parseFloat(document.getElementById(`pct-gg-${{seccionId}}`).value || 0) / 100;
        const ben = parseFloat(document.getElementById(`pct-ben-${{seccionId}}`).value || 0) / 100;
        const imp = parseFloat(document.getElementById(`pct-imp-${{seccionId}}`).value || 0) / 100;
        fetch(`${{API}}/secciones/${{seccionId}}`, {{
            method: "PUT", headers: {{"Content-Type": "application/json"}},
            body: JSON.stringify({{ gg_pct: gg, beneficio_pct: ben, imp_pct: imp }})
        }})
            .then(r => r.json().then(j => ({{ ok: r.ok, j }})))
            .then(({{ ok, j }}) => {{
                mostrarGuardando(tareaId, false);
                if (!ok) {{ alert(j.error || "No se pudo guardar."); return; }}
                refrescarCalculos(tareaId);
            }})
            .catch(err => {{ mostrarGuardando(tareaId, false); alert("Error de red: " + err); }});
    }}

    function panelStickyHtml(tareaId, tipos) {{
        let bloques = "";
        if (tipos.includes("FABRICACION")) {{
            bloques += `<div class="mini-resultado" id="mini-fab-${{tareaId}}"><b>Fabricación</b><div class="muted">Sin calcular.</div></div>`;
        }}
        if (tipos.includes("MONTAJE")) {{
            bloques += `<div class="mini-resultado" id="mini-mon-${{tareaId}}"><b>Montaje</b><div class="muted">Sin calcular.</div></div>`;
        }}
        bloques += `<div class="mini-resultado" id="mini-tarea-${{tareaId}}" style="background:#eef2ff;border-color:#c7d2fe;"><b>Total tarea</b><div class="muted">Sin calcular.</div></div>`;
        bloques += `<div class="mini-resultado" id="mini-presupuesto-${{tareaId}}" style="background:#fff7ed;border-color:#fdba74;"><b>Total presupuesto</b><div class="muted">Sin calcular.</div></div>`;
        bloques += `<div class="mini-resultado indicador" id="mini-indicador-${{tareaId}}"><b>$/kg y USD/kg</b><div class="muted">Sin calcular.</div></div>`;
        return `<div class="panel-sticky">
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
                <b style="font-size:13px;">📊 Resultado en vivo</b>
                <span class="guardando-badge" id="guardando-${{tareaId}}">💾 Guardando…</span>
            </div>
            <div class="grid-paneles">${{bloques}}</div>
        </div>`;
    }}

    function miniCascadaHtml(titulo, cascada) {{
        if (!cascada) return `<b>${{titulo}}</b><div class="muted">Sin datos.</div>`;
        return `<b>${{titulo}}</b>
            <div class="fila"><span>Costo directo</span><span>${{fmtMoney(cascada.costo_directo)}}</span></div>
            <div class="fila"><span>GG</span><span>${{fmtMoney(cascada.gg)}}</span></div>
            <div class="fila"><span>Beneficio</span><span>${{fmtMoney(cascada.beneficio)}}</span></div>
            <div class="fila"><span>Impuestos</span><span>${{fmtMoney(cascada.impuestos)}}</span></div>
            <div class="fila total"><span>Precio venta</span><span>${{fmtMoney(cascada.precio_venta)}}</span></div>`;
    }}

    function actualizarSubtotalesItems(seccionId, itemsResultado) {{
        const ordenPorRubro = ordenItemsPorSeccion[seccionId] || {{}};
        const consumido = {{}};
        (itemsResultado || []).forEach(itemRes => {{
            const rubro = itemRes.rubro;
            const orden = ordenPorRubro[rubro] || [];
            const idx = consumido[rubro] || 0;
            const itemId = orden[idx];
            consumido[rubro] = idx + 1;
            if (itemId === undefined) return;
            const span = document.getElementById(`subtotal-item-${{itemId}}`);
            if (span) span.textContent = fmtMoney(itemRes.subtotal);
            const kgSpan = document.getElementById(`kg-item-${{itemId}}`);
            if (kgSpan && itemRes.peso !== undefined) kgSpan.textContent = fmtNum(itemRes.peso, 1);
            const m2Span = document.getElementById(`m2-item-${{itemId}}`);
            if (m2Span && itemRes.m2 !== undefined) m2Span.textContent = fmtNum(itemRes.m2, 2);
        }});
    }}

    function refrescarCalculos(tareaId) {{
        fetch(`${{API}}/tareas/${{tareaId}}/recalcular`)
            .then(r => r.json())
            .then(({{ resultado, error }}) => {{
                if (error) return;
                document.querySelectorAll(`[id^="seccion-wrap-"]`).forEach(el => {{
                    const seccionId = el.id.replace("seccion-wrap-", "");
                    const tipo = seccionTipoPorId[seccionId];
                    const itemsResultado = tipo === "FABRICACION" ? resultado.fabricacion.items : resultado.montaje.items;
                    actualizarSubtotalesItems(seccionId, itemsResultado);
                    if (tipo === "FABRICACION" && resultado.fabricacion.indicador_mano_obra) {{
                        const ind = resultado.fabricacion.indicador_mano_obra;
                        const usdEl = document.getElementById(`indic-usdkg-${{seccionId}}`);
                        if (usdEl) usdEl.textContent = fmtNum(ind.usd_por_kg, 3);
                        const kgEl = document.getElementById(`indic-kghh-${{seccionId}}`);
                        if (kgEl) kgEl.textContent = fmtNum(ind.kg_por_hh, 2);
                    }}
                }});
                const miniFab = document.getElementById(`mini-fab-${{tareaId}}`);
                if (miniFab) miniFab.innerHTML = miniCascadaHtml("Fabricación", resultado.fabricacion.cascada);
                const miniMon = document.getElementById(`mini-mon-${{tareaId}}`);
                if (miniMon) miniMon.innerHTML = miniCascadaHtml("Montaje", resultado.montaje.cascada);
                const miniTarea = document.getElementById(`mini-tarea-${{tareaId}}`);
                if (miniTarea) miniTarea.innerHTML = `<b>Total tarea</b><div class="fila total"><span>Precio venta</span><span>${{fmtMoney(resultado.precio_venta_tarea)}}</span></div>`;
            }});
        fetch(`${{API}}/presupuestos/${{PRESUPUESTO_ID}}/recalcular`)
            .then(r => r.json())
            .then(({{ totales, error }}) => {{
                if (error) return;
                const miniPres = document.getElementById(`mini-presupuesto-${{tareaId}}`);
                if (miniPres) miniPres.innerHTML = `<b>Total presupuesto</b><div class="fila total"><span>Precio venta</span><span>${{fmtMoney(totales.precio_venta_presupuesto)}}</span></div>`;
                const cont = document.getElementById("totales-presupuesto");
                if (cont) cont.innerHTML = `<div class="resultado-box"><div class="fila" style="font-weight:800;"><span>TOTAL PRESUPUESTO</span><span>${{fmtMoney(totales.precio_venta_presupuesto)}}</span></div></div>`;
            }});
        fetch(`${{API}}/presupuestos/${{PRESUPUESTO_ID}}/indicador-kg`)
            .then(r => r.json())
            .then((datos) => {{
                if (datos.error) return;
                const miniInd = document.getElementById(`mini-indicador-${{tareaId}}`);
                if (!miniInd) return;
                const sinTipoCambio = !datos.tipo_cambio_referencia ? '<div class="muted">Sin tipo de cambio ref.</div>' : "";
                miniInd.innerHTML = `<b>$/kg y USD/kg</b>
                    <div class="fila"><span>Peso total</span><span>${{fmtNum(datos.peso_total_kg, 1)}} kg</span></div>
                    <div class="fila"><span>$/kg</span><span>${{fmtMoney(datos.costo_por_kg)}}</span></div>
                    <div class="fila"><span>USD/kg</span><span>${{fmtNum(datos.costo_por_kg_usd, 4)}}</span></div>
                    ${{sinTipoCambio}}`;
            }});
    }}

    function agregarSeccionFaltante(tareaId, tipo) {{
        fetch(`${{API}}/tareas/${{tareaId}}/secciones`, {{
            method: "POST", headers: {{"Content-Type": "application/json"}},
            body: JSON.stringify({{ tipo }})
        }})
            .then(r => r.json().then(j => ({{ ok: r.ok, j }})))
            .then(({{ ok, j }}) => {{
                if (!ok) {{ alert(j.error || "No se pudo agregar la sección."); return; }}
                cargarTarea(tareaId);
            }})
            .catch(err => alert("Error de red: " + err));
    }}

    function cargarTarea(tareaId) {{
        const cont = document.getElementById("tarea-contenido");
        const wrapPrevio = document.querySelector(".tarea-scroll-wrap");
        const mismaTarea = cont.dataset.tareaActual === String(tareaId);
        const scrollPrevio = (mismaTarea && wrapPrevio) ? wrapPrevio.scrollTop : 0;
        cont.innerHTML = "Cargando...";
        fetch(`${{API}}/tareas/${{tareaId}}`)
            .then(r => r.json())
            .then(({{ tarea, error }}) => {{
                if (error) {{ cont.innerHTML = `<span style="color:#dc2626;">${{error}}</span>`; return; }}

                // Autocrear la línea única de los rubros "de una sola línea" (sin botón
                // "Agregar línea": el cuadro vacío para completar aparece directo).
                const RUBROS_AUTO_UNA_LINEA = {{
                    FABRICACION: ["bulones", "pintura", "fletes", "mano_obra", "consumibles", "ingenieria"],
                    MONTAJE: ["mano_obra", "consumibles", "ingeniero", "tecnico_hys"],
                }};
                const DATOS_DEFAULT_POR_RUBRO = {{ bulones: {{"porcentaje": 0.05}} }};
                const creaciones = [];
                (tarea.secciones || []).forEach(s => {{
                    (RUBROS_AUTO_UNA_LINEA[s.tipo] || []).forEach(rubro => {{
                        const yaExiste = (s.items || []).some(it => it.rubro === rubro);
                        if (!yaExiste) {{
                            creaciones.push(fetch(`${{API}}/secciones/${{s.id}}/items`, {{
                                method: "POST", headers: {{"Content-Type": "application/json"}},
                                body: JSON.stringify({{ rubro, datos: DATOS_DEFAULT_POR_RUBRO[rubro] || {{}}, tipo_item: null }})
                            }}));
                        }}
                    }});
                    if (s.tipo === "FABRICACION") {{
                        const hayPlacas = (s.items || []).some(it => it.rubro === "materiales" && it.tipo_item === "porcentaje");
                        if (!hayPlacas) {{
                            creaciones.push(fetch(`${{API}}/secciones/${{s.id}}/items`, {{
                                method: "POST", headers: {{"Content-Type": "application/json"}},
                                body: JSON.stringify({{ rubro: "materiales", datos: {{"porcentaje": 0.15}}, tipo_item: "porcentaje" }})
                            }}));
                        }}
                    }}
                }});
                if (creaciones.length > 0) {{
                    Promise.all(creaciones).then(() => cargarTarea(tareaId));
                    return;
                }}

                const tipos = (tarea.secciones || []).map(s => s.tipo);
                let secciones = (tarea.secciones || []).map(s => renderSeccionCompleta(tareaId, s)).join("");
                const faltante = TIPOS_SECCION.find(t => !tipos.includes(t));
                if (faltante) {{
                    secciones += `<button type="button" class="btn btn-sm btn-secondary" style="margin-top:8px;" onclick="agregarSeccionFaltante(${{tareaId}}, '${{faltante}}')">+ Agregar sección ${{faltante === 'FABRICACION' ? 'Fabricación' : 'Montaje'}}</button>`;
                }}
                const info = TAREAS_INFO[String(tareaId)] || {{}};
                const cabecera = `<div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px;margin-bottom:6px;">
                    <b style="font-size:15px;">${{info.nombre || ("Tarea " + tareaId)}}</b>
                    <form method="post" action="/modulo/presupuestos/tareas/${{tareaId}}/eliminar" onsubmit="return confirm('¿Eliminar esta tarea y todos sus items?');">
                        <input type="hidden" name="presupuesto_id" value="${{PRESUPUESTO_ID}}">
                        <button type="submit" class="btn btn-sm btn-danger">Eliminar tarea</button>
                    </form>
                </div>`;
                cont.innerHTML = `<div class="tarea-scroll-wrap">
                    ${{cabecera}}
                    ${{panelStickyHtml(tareaId, tipos)}}
                    <div style="padding:6px;">${{secciones}}</div>
                </div>`;
                cont.dataset.tareaActual = String(tareaId);
                const wrapNuevo = document.querySelector(".tarea-scroll-wrap");
                if (wrapNuevo) wrapNuevo.scrollTop = scrollPrevio;
                (tarea.secciones || []).forEach(s => sincronizarConsumibles(s.id, tareaId));
                refrescarCalculos(tareaId);
            }})
            .catch(err => {{ cont.innerHTML = `<span style="color:#dc2626;">Error de red: ${{err}}</span>`; }});
    }}

    function recalcularPresupuesto() {{
        const cont = document.getElementById("totales-presupuesto");
        cont.innerHTML = "Calculando...";
        fetch(`${{API}}/presupuestos/${{PRESUPUESTO_ID}}/recalcular`)
            .then(r => r.json())
            .then(({{ tareas, totales, error }}) => {{
                if (error) {{ cont.innerHTML = `<span style="color:#dc2626;">${{error}}</span>`; return; }}
                let filas = (tareas || []).map(t => `<div class="fila"><span>${{t.nombre}}</span><span>${{fmtMoney(t.resultado.precio_venta_tarea)}}</span></div>`).join("");
                cont.innerHTML = `<div class="resultado-box">${{filas}}<div class="fila" style="font-weight:800;border-top:1px solid #bbf7d0;padding-top:4px;"><span>TOTAL PRESUPUESTO</span><span>${{fmtMoney(totales.precio_venta_presupuesto)}}</span></div></div>`;
            }});
    }}
    </script>
    </body>
    </html>
    """


# ─────────────────────────────────────────────────────────────────
# 4) Resumen del presupuesto (sección 4.4) y reportes (sección 5.1 a 5.3)
#    Todo el cálculo se pide vía fetch() a la API JSON del paso 3/4 — acá
#    solo se renderiza. El export de recursos sí recalcula server-side
#    (misma función de agregación) para generar el CSV.
# ─────────────────────────────────────────────────────────────────

@presupuestos_bp.route("/<int:presupuesto_id>/resumen", methods=["GET"])
def vista_resumen_presupuesto(presupuesto_id):
    db = _db()
    presupuesto = obtener_presupuesto(db, presupuesto_id)
    if not presupuesto:
        return "<h3>❌ Presupuesto no encontrado</h3>", 404

    return f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>{_ESTILO_BASE}</style>
    </head>
    <body>
    <div class="wrap">
        <div class="top-bar">
            <h2>📊 Resumen y reportes — Presupuesto #{presupuesto_id}</h2>
            <div>
                <a href="/modulo/presupuestos/{presupuesto_id}" class="btn btn-secondary">⬅️ Volver al presupuesto</a>
                <a href="/modulo/presupuestos/{presupuesto_id}/resumen/export.csv" class="btn">⬇ Exportar recursos (CSV)</a>
            </div>
        </div>

        <div class="card">
            <h3 style="margin-top:0;">Resumen por tarea (Fabricación + Montaje)</h3>
            <div id="resumen-tareas" class="muted">Cargando...</div>
        </div>

        <div class="card">
            <h3 style="margin-top:0;">Reporte cruzado por categoría</h3>
            <div class="muted" style="margin-bottom:8px;">Cruza todas las tareas del presupuesto (igual a la hoja "Resumen" del Excel original).</div>
            <div id="reporte-categorias" class="muted">Cargando...</div>
        </div>

        <div class="card">
            <h3 style="margin-top:0;">Indicador de costo</h3>
            <div id="indicador-kg" class="muted">Cargando...</div>
        </div>

        <div class="card">
            <h3 style="margin-top:0;">Resumen de recursos a obra</h3>
            <div id="resumen-recursos" class="muted">Cargando...</div>
        </div>
    </div>

    <script>
    const PRESUPUESTO_ID = {presupuesto_id};
    const API = "/modulo/presupuestos/api";

    function fmtMoney(v) {{
        return "$ " + (Number(v) || 0).toLocaleString("es-AR", {{minimumFractionDigits: 2, maximumFractionDigits: 2}});
    }}
    function fmtPct(v) {{
        return ((Number(v) || 0) * 100).toFixed(2) + "%";
    }}
    function fmtNum(v, dec) {{
        return (Number(v) || 0).toLocaleString("es-AR", {{minimumFractionDigits: dec, maximumFractionDigits: dec}});
    }}

    function cargarResumenTareas() {{
        const cont = document.getElementById("resumen-tareas");
        fetch(`${{API}}/presupuestos/${{PRESUPUESTO_ID}}/resumen`)
            .then(r => r.json())
            .then(({{ tareas, total_fabricacion, total_montaje, total_presupuesto, error }}) => {{
                if (error) {{ cont.innerHTML = `<span style="color:#dc2626;">${{error}}</span>`; return; }}
                if (!tareas || !tareas.length) {{ cont.innerHTML = "<div class='sin-datos'>Este presupuesto todavía no tiene tareas.</div>"; return; }}
                let filas = tareas.map(t => `<tr>
                    <td>${{t.nombre}}</td>
                    <td>${{fmtMoney(t.precio_venta_fabricacion)}}</td>
                    <td>${{fmtMoney(t.precio_venta_montaje)}}</td>
                    <td><b>${{fmtMoney(t.precio_venta_tarea)}}</b></td>
                </tr>`).join("");
                cont.innerHTML = `<table>
                    <tr><th>Tarea</th><th>Fabricación</th><th>Montaje</th><th>Total tarea</th></tr>
                    ${{filas}}
                    <tr style="font-weight:800;background:#eef2ff;">
                        <td>GRAN TOTAL</td>
                        <td>${{fmtMoney(total_fabricacion)}}</td>
                        <td>${{fmtMoney(total_montaje)}}</td>
                        <td>${{fmtMoney(total_presupuesto)}}</td>
                    </tr>
                </table>`;
            }});
    }}

    function cargarReporteCategorias() {{
        const cont = document.getElementById("reporte-categorias");
        fetch(`${{API}}/presupuestos/${{PRESUPUESTO_ID}}/reporte-categorias`)
            .then(r => r.json())
            .then(({{ categorias, total, error }}) => {{
                if (error) {{ cont.innerHTML = `<span style="color:#dc2626;">${{error}}</span>`; return; }}
                let filas = (categorias || []).map(c => `<tr>
                    <td>${{c.nombre}}</td>
                    <td>${{fmtMoney(c.monto)}}</td>
                    <td>${{fmtPct(c.porcentaje)}}</td>
                </tr>`).join("");
                cont.innerHTML = `<table>
                    <tr><th>Categoría</th><th>Monto</th><th>% del total</th></tr>
                    ${{filas}}
                    <tr style="font-weight:800;background:#eef2ff;"><td>TOTAL GENERAL</td><td>${{fmtMoney(total)}}</td><td>100.00%</td></tr>
                </table>`;
            }});
    }}

    function cargarIndicadorKg() {{
        const cont = document.getElementById("indicador-kg");
        fetch(`${{API}}/presupuestos/${{PRESUPUESTO_ID}}/indicador-kg`)
            .then(r => r.json())
            .then(({{ peso_total_kg, costo_por_kg, costo_por_kg_usd, tipo_cambio_referencia, error }}) => {{
                if (error) {{ cont.innerHTML = `<span style="color:#dc2626;">${{error}}</span>`; return; }}
                const sinTipoCambio = !tipo_cambio_referencia ? '<div class="muted">⚠️ El presupuesto no tiene tipo de cambio de referencia cargado: el USD/kg no se puede calcular.</div>' : '';
                cont.innerHTML = `<div class="resultado-box">
                    <div class="fila"><span>Peso total de materiales</span><span>${{fmtNum(peso_total_kg, 2)}} kg</span></div>
                    <div class="fila"><span>Costo por kg ($/kg)</span><span>${{fmtMoney(costo_por_kg)}}</span></div>
                    <div class="fila"><span>Costo por kg (USD/kg)</span><span>${{fmtNum(costo_por_kg_usd, 4)}}</span></div>
                </div>${{sinTipoCambio}}`;
            }});
    }}

    function cargarResumenRecursos() {{
        const cont = document.getElementById("resumen-recursos");
        fetch(`${{API}}/presupuestos/${{PRESUPUESTO_ID}}/resumen-recursos`)
            .then(r => r.json())
            .then(({{ mano_obra, equipos, materiales, error }}) => {{
                if (error) {{ cont.innerHTML = `<span style="color:#dc2626;">${{error}}</span>`; return; }}
                let html = `<div class="resultado-box">
                    <div class="fila"><span>Mano de obra — Taller (operarios-día)</span><span>${{fmtNum(mano_obra.operarios_dia_taller, 1)}}</span></div>
                    <div class="fila"><span>Mano de obra — Montaje (operarios-día)</span><span>${{fmtNum(mano_obra.operarios_dia_montaje, 1)}}</span></div>
                </div>`;

                html += "<h4 style='margin:12px 0 6px 0;'>Equipos necesarios</h4>";
                if (equipos && equipos.length) {{
                    html += "<table><tr><th>Equipo</th><th>Días totales</th></tr>";
                    html += equipos.map(e => `<tr><td>${{e.nombre}}</td><td>${{fmtNum(e.dias, 1)}}</td></tr>`).join("");
                    html += "</table>";
                }} else {{
                    html += "<div class='sin-datos'>Sin equipos cargados en Montaje.</div>";
                }}

                html += "<h4 style='margin:12px 0 6px 0;'>Materiales a comprar</h4>";
                if (materiales && materiales.length) {{
                    html += "<table><tr><th>Perfil</th><th>Cantidad</th><th>Peso total (kg)</th></tr>";
                    html += materiales.map(m => `<tr><td>${{m.nombre}}</td><td>${{fmtNum(m.cantidad, 2)}}</td><td>${{fmtNum(m.peso_kg, 2)}}</td></tr>`).join("");
                    html += "</table>";
                }} else {{
                    html += "<div class='sin-datos'>Sin líneas de materiales/perfil cargadas en Fabricación.</div>";
                }}

                cont.innerHTML = html;
            }});
    }}

    cargarResumenTareas();
    cargarReporteCategorias();
    cargarIndicadorKg();
    cargarResumenRecursos();
    </script>
    </body>
    </html>
    """


@presupuestos_bp.route("/<int:presupuesto_id>/resumen/export.csv", methods=["GET"])
def vista_exportar_resumen_recursos_csv(presupuesto_id):
    db = _db()
    presupuesto = obtener_presupuesto(db, presupuesto_id)
    if not presupuesto:
        return "<h3>❌ Presupuesto no encontrado</h3>", 404

    resumen = _resumen_recursos_con_nombres(db, presupuesto_id)

    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow(["RESUMEN DE RECURSOS A OBRA"])
    writer.writerow(["Presupuesto", f"#{presupuesto_id}"])
    writer.writerow(["Cliente", presupuesto.get("cliente") or ""])
    writer.writerow(["Planta", presupuesto.get("planta") or ""])
    writer.writerow([])

    writer.writerow(["MANO DE OBRA", "Operarios-día"])
    writer.writerow(["Taller (Fabricación)", resumen["mano_obra"]["operarios_dia_taller"]])
    writer.writerow(["Montaje (Obra)", resumen["mano_obra"]["operarios_dia_montaje"]])
    writer.writerow([])

    writer.writerow(["EQUIPOS NECESARIOS", "Días totales"])
    for equipo in resumen["equipos"]:
        writer.writerow([equipo["nombre"], equipo["dias"]])
    writer.writerow([])

    writer.writerow(["MATERIALES A COMPRAR", "Cantidad", "Peso total (kg)"])
    for material in resumen["materiales"]:
        writer.writerow([material["nombre"], material["cantidad"], round(material["peso_kg"], 2)])

    csv_bytes = BytesIO(buffer.getvalue().encode("utf-8-sig"))
    nombre_archivo = f"resumen_recursos_presupuesto_{presupuesto_id}.csv"
    return send_file(csv_bytes, mimetype="text/csv", as_attachment=True, download_name=nombre_archivo)
