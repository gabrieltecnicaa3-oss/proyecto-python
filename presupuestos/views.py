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

from flask import request, redirect, send_file, jsonify

from . import presupuestos_bp
from .routes import _db, _todos_los_resultados_tarea, _resumen_recursos_con_nombres
from .constants import ESTADOS_PRESUPUESTO, TIPOS_SECCION, RUBROS_POR_SECCION, TAREAS_ESTANDAR, TAREAS_MODO_CHAPA, TAREAS_MODO_GRATING, MATERIALES_TEMPLATE_POR_TAREA, TAREAS_SELECCIONABLES
from .calculo_presupuesto import calcular_presupuesto
from .reportes_excel import generar_reporte_explosion_insumos, generar_reporte_prevision_fondos
from .reportes_odoo_pedido import (
    armar_pedido_odoo,
    generar_excel_pedido_odoo,
    validar_analitica_id,
    nombre_archivo_pedido,
)
from .models import (
    crear_presupuesto,
    obtener_presupuesto,
    listar_presupuestos,
    actualizar_presupuesto,
    actualizar_estado_presupuesto,
    eliminar_presupuesto,
    copiar_presupuesto,
    crear_tarea,
    listar_tareas,
    crear_secciones_tarea,
    eliminar_tarea,
    obtener_config,
    obtener_config_presupuesto,
    CONFIG_CAMPOS_PRESUPUESTO,
    actualizar_analitica_odoo,
)


def _obtener_obra_referencia(db, presupuesto, presupuesto_id):
    """`presupuestos.obra_referencia` (sección 5.3) existe en la tabla pero
    ningún CRUD de models.py la expone todavía (campo histórico, sin pantalla
    de carga) — se lee directo acá. Si está vacía, se usa el título o el
    cliente del presupuesto como respaldo para que el Reporte 2 no quede con
    el nombre de obra en blanco."""
    row = db.execute("SELECT obra_referencia FROM presupuestos WHERE id = ?", (presupuesto_id,)).fetchone()
    obra_referencia = (row[0] if row else None) or ""
    obra_referencia = obra_referencia.strip()
    if not obra_referencia:
        obra_referencia = (presupuesto.get("titulo") or presupuesto.get("cliente") or "").strip()
    return obra_referencia


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
.tarea-scroll-wrap { border: 1px solid #e2e8f0; border-radius: 10px; }
.panel-sticky {
    position: sticky; top: 0; z-index: 5; background: #fff; border-bottom: 2px solid #c7d2fe;
    padding: 10px 12px; box-shadow: 0 4px 10px rgba(15,23,42,0.06);
}
.panel-sticky .grid-paneles { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 8px; }
.mini-resultado { background: #f0fdf4; border: 1px solid #bbf7d0; border-radius: 8px; padding: 8px 10px; font-size: 12px; }
.mini-resultado.indicador { background: #eff6ff; border-color: #bfdbfe; }
.mini-resultado.mini-fab { background: #eafbf0; border-color: #c3e6c9; }
.mini-resultado.mini-mon { background: #fdf0e3; border-color: #f5d6ad; }
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
.seccion-wrap-fabricacion { background: #eaf7ec; border-color: #c3e6c9; }
.seccion-wrap-montaje { background: #fdf0e3; border-color: #f5d6ad; }
.tabla-materiales th, .tabla-materiales td { padding: 2px 5px; font-size: 11px; white-space: nowrap; }
.tabla-materiales input, .tabla-materiales select { padding: 2px 5px; font-size: 11px; min-width: 76px; margin-bottom: 0; }
.tabs-tareas { display: flex; gap: 4px; flex-wrap: wrap; border-bottom: 2px solid #e2e8f0; margin-bottom: 10px; }
.oculto { display: none; }
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
        copia_badge = (
            f'<br><span class="muted" style="font-size:11px;">📋 copia de {p["copia_de_label"] or ("#" + str(p["copiado_de_id"]))}</span>'
            if p.get("copiado_de_id") else ""
        )
        filas += f"""
        <tr>
            <td><b>#{p['id']}</b>{copia_badge}</td>
            <td>{p['numero_presupuesto'] or '-'}</td>
            <td>{p['cliente'] or '-'}</td>
            <td>{p['planta'] or '-'}</td>
            <td>{_fmt_money(total_presupuesto)}</td>
            <td>{p['fecha_creacion'] or '-'}</td>
            <td>{_badge_estado(p['estado'])}</td>
            <td>
                <a class="btn btn-sm" href="/modulo/presupuestos/{p['id']}">Ver</a>
                <form method="post" action="/modulo/presupuestos/{p['id']}/copiar" style="display:inline;">
                    <button type="submit" class="btn btn-sm btn-secondary">Copiar</button>
                </form>
                <form method="post" action="/modulo/presupuestos/{p['id']}/eliminar" style="display:inline;" onsubmit="return confirm('¿Eliminar presupuesto #{p['id']}? Esta acción no se puede deshacer.');">
                    <button type="submit" class="btn btn-sm btn-danger">Eliminar</button>
                </form>
            </td>
        </tr>
        """

    tabla = f"""
    <table>
        <tr><th>ID</th><th>N° presupuesto</th><th>Cliente</th><th>Planta</th><th>Precio venta total</th><th>Creado</th><th>Estado</th><th>Acciones</th></tr>
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

def _config_presupuesto_desde_form(form):
    config = {}
    for campo in CONFIG_CAMPOS_PRESUPUESTO:
        valor = float(form.get(campo) or 0)
        config[campo] = valor / 100 if "_pct_" in campo else valor
    return config


def _campo_config_presupuesto(campo, valor):
    es_porcentaje = "_pct_" in campo
    etiquetas = {
        "gg_pct_default_fab": "Gastos generales",
        "beneficio_pct_default_fab": "Beneficio",
        "imp_pct_default_fab": "Impuestos",
        "gg_pct_default_mon": "Gastos generales",
        "beneficio_pct_default_mon": "Beneficio",
        "imp_pct_default_mon": "Impuestos",
        "tarifa_dh_taller_default": "Mano de obra de fabricación ($/día-hombre)",
        "tarifa_dh_obra_default": "Mano de obra de montaje ($/día-hombre)",
        "tarifa_consumible_dh_taller_default": "Consumibles de fabricación ($/día-hombre)",
        "tarifa_consumible_dh_obra_default": "Consumibles de montaje ($/día-hombre)",
    }
    valor_mostrado = float(valor or 0) * (100 if es_porcentaje else 1)
    return f"""
    <div>
        <label>{etiquetas[campo]}{" (%)" if es_porcentaje else ""}</label>
        <input type="number" step="any" name="{campo}" value="{valor_mostrado:.2f}">
    </div>
    """


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
                        <label>N° de presupuesto</label>
                        <input type="text" name="numero_presupuesto" value="{datos.get('numero_presupuesto') or ''}">
                    </div>
                </div>
                <div class="grid2">
                    <div>
                        <label>Planta</label>
                        <input type="text" name="planta" value="{datos.get('planta') or ''}">
                    </div>
                    <div>
                        <label>Título</label>
                        <input type="text" name="titulo" value="{datos.get('titulo') or ''}">
                    </div>
                </div>
                <div class="grid2">
                    <div>
                        <label>Fecha</label>
                        <input type="date" name="fecha" value="{datos.get('fecha') or ''}">
                    </div>
                    <div></div>
                </div>
                <label>Tipo de cambio de referencia</label>
                <input type="number" step="0.0001" name="tipo_cambio_referencia" value="{datos.get('tipo_cambio_referencia') if datos.get('tipo_cambio_referencia') is not None else ''}">
                <h3>Valores por defecto de este presupuesto</h3>
                <p class="muted">Los porcentajes se aplican a las secciones nuevas; las tarifas se usan en las líneas sin tarifa guardada. Los cambios no modifican otros presupuestos ni las secciones ya creadas.</p>
                <h4>Fabricación</h4>
                <div class="grid3">
                    {_campo_config_presupuesto('gg_pct_default_fab', datos.get('gg_pct_default_fab'))}
                    {_campo_config_presupuesto('beneficio_pct_default_fab', datos.get('beneficio_pct_default_fab'))}
                    {_campo_config_presupuesto('imp_pct_default_fab', datos.get('imp_pct_default_fab'))}
                </div>
                <h4>Montaje</h4>
                <div class="grid3">
                    {_campo_config_presupuesto('gg_pct_default_mon', datos.get('gg_pct_default_mon'))}
                    {_campo_config_presupuesto('beneficio_pct_default_mon', datos.get('beneficio_pct_default_mon'))}
                    {_campo_config_presupuesto('imp_pct_default_mon', datos.get('imp_pct_default_mon'))}
                </div>
                <h4>Tarifas de mano de obra y consumibles</h4>
                <div class="grid2">
                    {_campo_config_presupuesto('tarifa_dh_taller_default', datos.get('tarifa_dh_taller_default'))}
                    {_campo_config_presupuesto('tarifa_dh_obra_default', datos.get('tarifa_dh_obra_default'))}
                    {_campo_config_presupuesto('tarifa_consumible_dh_taller_default', datos.get('tarifa_consumible_dh_taller_default'))}
                    {_campo_config_presupuesto('tarifa_consumible_dh_obra_default', datos.get('tarifa_consumible_dh_obra_default'))}
                </div>
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
    config = obtener_config_presupuesto(db, presupuesto_id)
    for orden, nombre in enumerate(TAREAS_ESTANDAR, start=1):
        tarea_id = crear_tarea(db, presupuesto_id, nombre, orden=orden, tipo=nombre)
        try:
            crear_secciones_tarea(db, tarea_id, config, tipos=("FABRICACION", "MONTAJE"), nombre_tarea=nombre)
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
            numero_presupuesto=(request.form.get("numero_presupuesto") or "").strip() or None,
            config_presupuesto=_config_presupuesto_desde_form(request.form),
        )
        _crear_tareas_estandar(db, presupuesto_id)
        return redirect(f"/modulo/presupuestos/{presupuesto_id}")

    return _form_datos_generales(
        "Nuevo presupuesto", datos=obtener_config(_db()), boton_texto="Crear presupuesto"
    )


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
            numero_presupuesto=(request.form.get("numero_presupuesto") or "").strip() or None,
            config_presupuesto=_config_presupuesto_desde_form(request.form),
        )
        return redirect(f"/modulo/presupuestos/{presupuesto_id}")

    return _form_datos_generales(f"Editar presupuesto #{presupuesto_id}", datos=presupuesto)


@presupuestos_bp.route("/<int:presupuesto_id>/copiar", methods=["POST"])
def vista_copiar_presupuesto(presupuesto_id):
    """Duplica tareas/secciones/items del presupuesto (ver copiar_presupuesto en
    models.py) y manda directo a la pantalla de edición del borrador nuevo,
    para completar los datos generales que arrancan en blanco."""
    db = _db()
    nuevo_id = copiar_presupuesto(db, presupuesto_id)
    if nuevo_id is None:
        return "<h3>❌ Presupuesto no encontrado</h3>", 404
    return redirect(f"/modulo/presupuestos/{nuevo_id}/editar")


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
    tipo_tarea = (request.form.get("tipo") or "").strip()
    nombre = (request.form.get("nombre") or "").strip() or tipo_tarea
    tipos = tuple(t for t in ("FABRICACION", "MONTAJE") if request.form.get(f"tipo_{t.lower()}"))
    if tipo_tarea and nombre and tipos:
        orden = int(request.form.get("orden") or 0)
        tarea_id = crear_tarea(db, presupuesto_id, nombre, orden=orden, tipo=tipo_tarea)
        try:
            crear_secciones_tarea(
                db, tarea_id, obtener_config_presupuesto(db, presupuesto_id),
                tipos=tipos, nombre_tarea=tipo_tarea,
            )
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
        {"name": "precio_unitario_kg", "label": "Precio unitario (USD/KG)", "type": "number"},
    ],
    "materiales_porcentaje": [
        {"name": "descripcion", "label": "Descripción (elemento)", "type": "text"},
        {"name": "porcentaje", "label": "% sobre peso de materiales", "type": "number"},
        {"name": "precio_unitario_kg", "label": "Valor unitario (USD/KG)", "type": "number"},
    ],
    "materiales_no_listado": [
        {"name": "descripcion", "label": "Descripción", "type": "text"},
        {"name": "cantidad", "label": "Cantidad", "type": "number"},
        {"name": "unidad", "label": "Unidad", "type": "text"},
        {"name": "precio_unitario", "label": "Precio unitario (USD)", "type": "number"},
    ],
    "materiales_chapa": [
        {"name": "descripcion", "label": "Descripción", "type": "text"},
        {"name": "perfil_id", "label": "Tipo", "type": "number"},
        {"name": "cantidad", "label": "Cantidad", "type": "number"},
        {"name": "largo_mm", "label": "Largo (mm)", "type": "number"},
        {"name": "precio_unitario_m2", "label": "USD/M2", "type": "number"},
    ],
    "materiales_tornillos": [
        {"name": "descripcion", "label": "Descripción", "type": "text"},
        {"name": "precio_unitario", "label": "USD/UNIDAD", "type": "number"},
    ],
    "materiales_grating": [
        {"name": "descripcion", "label": "Descripción", "type": "text"},
        {"name": "perfil_id", "label": "Tipo", "type": "number"},
        {"name": "cantidad", "label": "Cantidad", "type": "number"},
        {"name": "m2", "label": "M2", "type": "number"},
        {"name": "precio_unitario_m2", "label": "USD/M2", "type": "number"},
    ],
    "materiales_fijaciones": [
        {"name": "descripcion", "label": "Descripción", "type": "text"},
        {"name": "precio_unitario", "label": "USD/UNIDAD", "type": "number"},
    ],
    "bulones": [{"name": "porcentaje", "label": "% sobre materiales", "type": "number"}],
    "pintura": [
        {"name": "esquema_id", "label": "ID Esquema de pintura", "type": "number"},
        {"name": "precio_unitario_m2", "label": "Precio unitario (USD/M2)", "type": "number"},
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
_TAREAS_MODO_CHAPA_JS = json.dumps(list(TAREAS_MODO_CHAPA), ensure_ascii=False)
_TAREAS_MODO_GRATING_JS = json.dumps(list(TAREAS_MODO_GRATING), ensure_ascii=False)
_MATERIALES_TEMPLATE_POR_TAREA_JS = json.dumps(
    {k: list(v) for k, v in MATERIALES_TEMPLATE_POR_TAREA.items()}, ensure_ascii=False
)


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
        {str(t["id"]): {"nombre": t["nombre"], "orden": t["orden"], "tipo": t.get("tipo") or t["nombre"]} for t in tareas}, ensure_ascii=False
    )
    _tareas_ids_js = json.dumps([t["id"] for t in tareas])

    checkboxes_tipos = "".join(
        f'<label style="display:inline-flex;align-items:center;gap:6px;font-weight:400;margin-right:16px;">'
        f'<input type="checkbox" name="tipo_{tipo.lower()}" value="1" checked style="width:auto;margin:0;"> {tipo.title()}</label>'
        for tipo in TIPOS_SECCION
    )
    opciones_tareas_seleccionables = "".join(
        f'<option value="{nombre}">{nombre}</option>' for nombre in TAREAS_SELECCIONABLES
    )

    return f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/tom-select@2.3.1/dist/css/tom-select.css">
    <script src="https://cdn.jsdelivr.net/npm/tom-select@2.3.1/dist/js/tom-select.complete.min.js"></script>
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
                <form method="post" action="/modulo/presupuestos/{presupuesto_id}/copiar" style="display:inline;">
                    <button type="submit" class="btn btn-secondary">📋 Copiar presupuesto</button>
                </form>
            </div>
        </div>

        <div class="card">
            <div class="grid3">
                <div><span class="muted">N° de presupuesto</span><br><b>{presupuesto['numero_presupuesto'] or '-'}</b></div>
                <div><span class="muted">Cliente</span><br><b>{presupuesto['cliente'] or '-'}</b></div>
                <div><span class="muted">Planta</span><br><b>{presupuesto['planta'] or '-'}</b></div>
            </div>
            <div class="grid3" style="margin-top:8px;">
                <div><span class="muted">Título</span><br><b>{presupuesto['titulo'] or '-'}</b></div>
                <div><span class="muted">Fecha</span><br><b>{presupuesto['fecha'] or '-'}</b></div>
                <div><span class="muted">Tipo de cambio ref.</span><br><b>{presupuesto['tipo_cambio_referencia'] or '-'}</b></div>
            </div>
            <div class="grid3" style="margin-top:8px;">
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
            <div class="tabs-tareas" style="align-items:flex-start;flex-wrap:wrap;">
                {tabs_tareas_html}
                <button type="button" class="btn btn-sm" title="Agregar tarea" style="margin-left:2px;" onclick="document.getElementById('form-agregar-tarea').classList.toggle('oculto');">+</button>
                <div id="form-agregar-tarea" class="oculto" style="flex-basis:100%;margin-top:10px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:8px;padding:10px;">
                    <form method="post" action="/modulo/presupuestos/{presupuesto_id}/tareas" onsubmit="return validarSeccionesTarea(this);">
                        <div class="grid2">
                            <div>
                                <label>Tipo de tarea</label>
                                <select name="tipo" required>
                                    <option value="">-- Seleccionar --</option>
                                    {opciones_tareas_seleccionables}
                                </select>
                            </div>
                            <div>
                                <label>Nombre (manual)</label>
                                <input type="text" name="nombre" placeholder="Ej: Escalera Oeste">
                            </div>
                        </div>
                        <label>Secciones que aplican</label>
                        <div style="margin-bottom:10px;">{checkboxes_tipos}</div>
                        <label>Orden</label>
                        <input type="number" name="orden" value="0">
                        <button type="submit" class="btn">Agregar tarea</button>
                    </form>
                </div>
            </div>
            <div id="tarea-contenido" class="muted">{"Seleccioná una pestaña para ver el detalle de la tarea." if tareas else "Este presupuesto todavía no tiene tareas cargadas."}</div>
        </div>

        <div class="card">
            <h3 style="margin-top:0;">RESUMEN</h3>
            <div class="muted" style="margin-bottom:8px;">REPORTE POR CATEGORIA</div>
            <div id="resumen-tareas-presupuesto" class="muted">Cargando...</div>
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
    let PERFIL_POR_ID = {{}};
    let FAMILIAS_PERFIL = [];
    const seccionTipoPorId = {{}};
    const ordenItemsPorSeccion = {{}};   // seccionId -> [itemId, itemId, ...] en el mismo orden que devuelve el recálculo
    const debounceTimers = {{}};         // clave -> timeoutId
    const AUTOSAVE_DEBOUNCE_MS = 650;
    const TAREAS_INFO = {_tareas_info_js};   // tareaId (string) -> {{nombre, orden}}
    const TAREAS_IDS = {_tareas_ids_js};     // [tareaId, ...] en orden
    const TAREAS_MODO_CHAPA = {_TAREAS_MODO_CHAPA_JS};   // nombres de tarea que usan materiales por m2 (chapa/tornillos)
    const TAREAS_MODO_GRATING = {_TAREAS_MODO_GRATING_JS};   // nombres de tarea que usan materiales Grating (m2 + kg/m2 automatico/fijaciones)
    const MATERIALES_TEMPLATE_POR_TAREA = {_MATERIALES_TEMPLATE_POR_TAREA_JS};   // nombre de tarea -> lineas de perfil preseleccionadas
    let tareaActivaId = null;

    function modoMaterialesTarea(tareaId) {{
        const info = TAREAS_INFO[String(tareaId)] || {{}};
        const tipo = info.tipo || info.nombre || "";
        if (TAREAS_MODO_CHAPA.includes(tipo)) return "chapa";
        if (TAREAS_MODO_GRATING.includes(tipo)) return "grating";
        return "perfil";
    }}

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
            fetch(`${{API}}/presupuestos/${{PRESUPUESTO_ID}}/config`).then(r => r.json()),
            fetch(`${{API}}/equipos`).then(r => r.json()).catch(() => ({{equipos: []}})),
            fetch(`${{API}}/esquemas-pintura`).then(r => r.json()).catch(() => ({{esquemas: []}})),
            fetch(`${{API}}/perfiles`).then(r => r.json()).catch(() => ({{perfiles: []}})),
        ]).then(([config, equipos, esquemas, perfiles]) => {{
            CONFIG_DEFAULTS = config || {{}};
            EQUIPOS = (equipos || {{}}).equipos || [];
            ESQUEMAS = (esquemas || {{}}).esquemas || [];
            PERFILES = (perfiles || {{}}).perfiles || [];
            PERFIL_POR_ID = {{}};
            const familiasSet = new Set();
            PERFILES.forEach(p => {{
                PERFIL_POR_ID[p.id] = p;
                if (p.categoria) familiasSet.add(p.categoria);
            }});
            FAMILIAS_PERFIL = Array.from(familiasSet).sort();
        }});
    }}
    cargarCatalogos().then(() => {{
        if (TAREAS_IDS.length) seleccionarTarea(TAREAS_IDS[0]);
    }});

    function fmtMoney(v) {{
        return "$ " + (Number(v) || 0).toLocaleString("es-AR", {{minimumFractionDigits: 2, maximumFractionDigits: 2}});
    }}
    function fmtPct(v) {{
        return ((Number(v) || 0) * 100).toFixed(2) + "%";
    }}
    function fmtNum(v, dec) {{
        return (Number(v) || 0).toLocaleString("es-AR", {{minimumFractionDigits: dec, maximumFractionDigits: dec}});
    }}

    function cargarResumenTareasPresupuesto() {{
        const cont = document.getElementById("resumen-tareas-presupuesto");
        if (!cont) return;
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
    cargarResumenTareasPresupuesto();


    function clavePorRubro(rubro, tipoItem) {{
        if (rubro === "materiales") {{
            if (tipoItem === "porcentaje") return "materiales_porcentaje";
            if (tipoItem === "no_listado") return "materiales_no_listado";
            if (tipoItem === "chapa") return "materiales_chapa";
            if (tipoItem === "tornillos") return "materiales_tornillos";
            if (tipoItem === "grating") return "materiales_grating";
            if (tipoItem === "fijaciones") return "materiales_fijaciones";
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
        const tipoItem = fila ? (fila.getAttribute("data-tipo-item") || null) : null;
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
        let evt = `oninput="programarAutosaveItem(${{itemId}}, ${{tareaId}})"`;
        if (rubro === "mano_obra" && (campo.name === "operarios" || campo.name === "dias")) {{
            evt = `oninput="programarAutosaveItem(${{itemId}}, ${{tareaId}}); sincronizarConsumibles(_seccionDeItem(${{itemId}}), ${{tareaId}})"`;
        }}
        if (campo.name === "perfil_id") {{
            const familiaActual = (val && PERFIL_POR_ID[val]) ? (PERFIL_POR_ID[val].categoria || "") : "";
            const modoFamilias = modoMaterialesTarea(tareaId);
            const familiasDisponibles = familiasParaModo(modoFamilias);
            const opcionesFamilia = familiasDisponibles.map(f => `<option value="${{f}}" ${{f === familiaActual ? "selected" : ""}}>${{f}}</option>`).join("");
            return `<div style="display:flex;gap:4px;flex-wrap:nowrap;align-items:center;">
                <select id="familia-${{itemId}}" style="width:150px;min-width:150px;flex:0 0 auto;" onchange="actualizarOpcionesPerfil(${{itemId}}, ${{tareaId}})">
                    <option value="">Todas las familias</option>
                    ${{opcionesFamilia}}
                </select>
                <select id="campo-${{itemId}}-perfil_id" data-valor-inicial="${{val || ''}}" style="width:280px;min-width:280px;flex:0 0 auto;" onchange="guardarItemInmediato(${{itemId}}, ${{tareaId}}); actualizarKgm2Grating(${{itemId}})"></select>
            </div>`;
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
                const claveTarifa = rubro === "consumibles"
                    ? "tarifa_consumible_dh"
                    : "tarifa_dh";
                const lugarTarifa = seccionTipo === "FABRICACION" ? "taller" : "obra";
                valorInicial = CONFIG_DEFAULTS[`${{claveTarifa}}_${{lugarTarifa}}_default`] || 0;
            }}
            if (rubro === "consumibles") {{ soloLectura = `readonly title='Costo unitario prefijado (no editable)'`; }}
        }}
        if (rubro === "consumibles" && (campo.name === "operarios" || campo.name === "dias")) {{
            soloLectura = `readonly title='Se sincroniza automáticamente con Mano de obra'`;
        }}
        let anchoEstilo = "";
        if (campo.name === "descripcion") anchoEstilo = `style="width:230px;min-width:230px;"`;
        if (campo.name === "porcentaje") anchoEstilo = `style="width:46px;min-width:46px;"`;
        return `<input type="${{campo.type}}" step="any" id="campo-${{itemId}}-${{campo.name}}" value="${{valorInicial}}" ${{anchoEstilo}} ${{evt}} ${{soloLectura}}>`;
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
        if (cambio && consId) programarAutosaveItem(parseInt(consId), tareaId);
    }}

    // ─────────────────────────────────────────────────────────────
    // Combobox de perfil con autocompletado (Tom Select) + filtro por
    // familia (IPN/UPN/W/...). El <select id="campo-{{id}}-perfil_id">
    // sigue guardando el mismo valor de siempre (perfil_id); Tom Select
    // solo mejora el componente visual, no cambia el dato ni la fórmula.
    // ─────────────────────────────────────────────────────────────

    function esFamiliaChapa(nombre) {{
        return (nombre || "").toUpperCase().startsWith("CH ");
    }}

    function esFamiliaGrating(nombre) {{
        return (nombre || "").toUpperCase().startsWith("GRA ");
    }}

    function familiasParaModo(modo) {{
        if (modo === "chapa") return FAMILIAS_PERFIL.filter(f => esFamiliaChapa(f));
        if (modo === "grating") return FAMILIAS_PERFIL.filter(f => esFamiliaGrating(f));
        return FAMILIAS_PERFIL.filter(f => !esFamiliaChapa(f) && !esFamiliaGrating(f));
    }}

    function perfilesParaModo(modo) {{
        if (modo === "chapa") return PERFILES.filter(p => esFamiliaChapa(p.categoria));
        if (modo === "grating") return PERFILES.filter(p => esFamiliaGrating(p.categoria));
        return PERFILES.filter(p => !esFamiliaChapa(p.categoria) && !esFamiliaGrating(p.categoria));
    }}

    function inicializarCombosPerfil(contenedor) {{
        const raiz = contenedor || document;
        raiz.querySelectorAll('select[id^="campo-"][id$="-perfil_id"]').forEach(sel => {{
            if (sel.tomselect) return; // ya inicializado
            const itemId = sel.id.replace("campo-", "").replace("-perfil_id", "");
            const valorInicial = sel.getAttribute("data-valor-inicial") || "";
            const familiaSel = document.getElementById(`familia-${{itemId}}`);
            const familia = familiaSel ? familiaSel.value : "";
            const modo = modoMaterialesTarea(tareaActivaId);
            const opciones = perfilesParaModo(modo).filter(p => !familia || p.categoria === familia);
            const ts = new TomSelect(sel, {{
                options: opciones,
                valueField: "id",
                labelField: "label",
                searchField: ["label"],
                maxOptions: 200,
                placeholder: "-- Elegir perfil --",
                score: function (search) {{
                    const s = search.toLowerCase();
                    return function (item) {{
                        return item.label.toLowerCase().includes(s) ? 1 : 0;
                    }};
                }},
            }});
            if (valorInicial) ts.setValue(String(valorInicial), true);
            actualizarKgm2Grating(itemId);
        }});
    }}

    function actualizarKgm2Grating(itemId) {{
        const kgm2Span = document.getElementById(`kgm2-item-${{itemId}}`);
        if (!kgm2Span) return;
        const sel = document.getElementById(`campo-${{itemId}}-perfil_id`);
        const val = sel ? sel.value : null;
        const perfil = (val && PERFIL_POR_ID[val]) ? PERFIL_POR_ID[val] : null;
        kgm2Span.textContent = fmtNum(perfil ? (perfil.kg_m || 0) : 0, 2);
    }}

    function actualizarOpcionesPerfil(itemId, tareaId) {{
        const sel = document.getElementById(`campo-${{itemId}}-perfil_id`);
        if (!sel || !sel.tomselect) return;
        const ts = sel.tomselect;
        const familiaSel = document.getElementById(`familia-${{itemId}}`);
        const familia = familiaSel ? familiaSel.value : "";
        const modo = modoMaterialesTarea(tareaId);
        const opciones = perfilesParaModo(modo).filter(p => !familia || p.categoria === familia);
        ts.clear(true);
        ts.clearOptions();
        ts.addOptions(opciones);
        ts.refreshOptions(false);
        guardarItemInmediato(itemId, tareaId);
        actualizarKgm2Grating(itemId);
    }}

    function campoInputHtml(itemId, campo, valorActual, seccionTipo, rubro, tareaId) {{
        const control = campoControlHtml(itemId, campo, valorActual, seccionTipo, rubro, tareaId);
        const sufijo = campo.name === "porcentaje" ? `<span style="font-size:11px;color:#475569;">%</span>` : "";
        return `<div>
            <label style="font-size:11px;">${{campo.label}}</label>
            <div style="display:flex;align-items:center;gap:4px;">${{control}}${{sufijo}}</div>
        </div>`;
    }}

    function camposItemHtml(itemId, rubro, tipoItem, datos, seccionTipo, tareaId, extraHtml) {{
        const clave = clavePorRubro(rubro, tipoItem || "perfil");
        const campos = CAMPOS_POR_RUBRO[clave] || [];
        return `<div style="display:flex;gap:8px;flex-wrap:wrap;align-items:flex-end;">${{campos.map(c => campoInputHtml(itemId, c, (datos || {{}})[c.name], seccionTipo, rubro, tareaId)).join("")}}${{extraHtml || ""}}</div>`;
    }}

    function filaItemHtml(seccionId, seccionTipo, tareaId, item) {{
        const extraPintura = item.rubro === "pintura"
            ? `<div><label style="font-size:11px;color:#475569;display:block;">M2 totales</label><span id="m2-item-${{item.id}}" style="font-weight:700;">${{fmtNum(item.m2 || 0, 2)}}</span></div>`
            : "";
        return `<tr data-item-id="${{item.id}}" data-rubro="${{item.rubro}}" data-tipo-item="${{item.tipo_item || ''}}" data-seccion-id="${{seccionId}}">
            <td style="width:70%;" data-celda-campos>${{camposItemHtml(item.id, item.rubro, item.tipo_item, item.datos, seccionTipo, tareaId, extraPintura)}}</td>
            <td style="width:15%;text-align:right;font-weight:700;"><span id="subtotal-item-${{item.id}}">${{fmtMoney(item.subtotal)}}</span></td>
            <td style="width:15%;">
                <button type="button" class="btn btn-icon btn-danger" onclick="eliminarLinea(${{item.id}}, ${{tareaId}})">✕</button>
            </td>
        </tr>`;
    }}

    function filaMaterialHtml(seccionId, seccionTipo, tareaId, item) {{
        const tipoItem = item.tipo_item || "perfil";
        const datos = item.datos || {{}};
        const descripcionInicial = (tipoItem === "porcentaje" && !datos.descripcion) ? "Placas" : datos.descripcion;
        const descCtl = campoControlHtml(item.id, {{name: "descripcion", label: "Descripción", type: "text"}}, descripcionInicial, seccionTipo, "materiales", tareaId);
        let celdas;
        if (tipoItem === "porcentaje") {{
            celdas = `
                <td class="muted" style="text-align:center;">–</td>
                <td class="muted" style="text-align:center;">–</td>
                <td class="muted" style="text-align:center;">–</td>
                <td>${{campoControlHtml(item.id, {{name: "precio_unitario_kg", label: "USD/KG", type: "number"}}, datos.precio_unitario_kg, seccionTipo, "materiales", tareaId)}}</td>
                <td style="display:flex;align-items:center;gap:4px;">${{campoControlHtml(item.id, {{name: "porcentaje", label: "%", type: "number"}}, datos.porcentaje, seccionTipo, "materiales", tareaId)}}<span style="font-size:11px;color:#475569;">%</span></td>`;
        }} else {{
            celdas = `
                <td>${{campoControlHtml(item.id, {{name: "perfil_id", label: "Perfil", type: "text"}}, datos.perfil_id, seccionTipo, "materiales", tareaId)}}</td>
                <td>${{campoControlHtml(item.id, {{name: "cantidad", label: "Cantidad", type: "number"}}, datos.cantidad, seccionTipo, "materiales", tareaId)}}</td>
                <td>${{campoControlHtml(item.id, {{name: "largo_mm", label: "Largo (mm)", type: "number"}}, datos.largo_mm, seccionTipo, "materiales", tareaId)}}</td>
                <td>${{campoControlHtml(item.id, {{name: "precio_unitario_kg", label: "USD/KG", type: "number"}}, datos.precio_unitario_kg, seccionTipo, "materiales", tareaId)}}</td>
                <td class="muted" style="text-align:center;">–</td>`;
        }}
        return `<tr data-item-id="${{item.id}}" data-rubro="materiales" data-tipo-item="${{tipoItem}}" data-seccion-id="${{seccionId}}">
            <td>${{descCtl}}</td>
            ${{celdas}}
            <td style="text-align:right;"><span id="kg-item-${{item.id}}">${{fmtNum(item.peso || 0, 1)}}</span></td>
            <td style="text-align:right;"><span id="m2-item-${{item.id}}">${{tipoItem === "perfil" ? fmtNum(item.m2 || 0, 2) : "–"}}</span></td>
            <td style="text-align:right;font-weight:700;"><span id="subtotal-item-${{item.id}}">${{fmtMoney(item.subtotal)}}</span></td>
            <td><button type="button" class="btn btn-icon btn-danger" onclick="eliminarLinea(${{item.id}}, ${{tareaId}})">✕</button></td>
        </tr>`;
    }}

    function filaMaterialChapaHtml(seccionId, seccionTipo, tareaId, item) {{
        const tipoItem = item.tipo_item || "chapa";
        const datos = item.datos || {{}};
        if (tipoItem === "tornillos") {{
            const descCtl = campoControlHtml(item.id, {{name: "descripcion", label: "Descripción", type: "text"}}, datos.descripcion || "Tornillos", seccionTipo, "materiales", tareaId);
            return `<tr data-item-id="${{item.id}}" data-rubro="materiales" data-tipo-item="tornillos" data-seccion-id="${{seccionId}}">
                <td>${{descCtl}}</td>
                <td class="muted" style="text-align:center;">–</td>
                <td style="text-align:right;" title="4 unidades por m2 de chapa (automático)"><span id="cant-item-${{item.id}}">${{fmtNum(item.cantidad || 0, 0)}}</span></td>
                <td class="muted" style="text-align:center;">–</td>
                <td>${{campoControlHtml(item.id, {{name: "precio_unitario", label: "USD/UNIDAD", type: "number"}}, datos.precio_unitario, seccionTipo, "materiales", tareaId)}}</td>
                <td class="muted" style="text-align:center;">–</td>
                <td style="text-align:right;font-weight:700;"><span id="subtotal-item-${{item.id}}">${{fmtMoney(item.subtotal)}}</span></td>
                <td><button type="button" class="btn btn-icon btn-danger" onclick="eliminarLinea(${{item.id}}, ${{tareaId}})">✕</button></td>
            </tr>`;
        }}
        const descCtl = campoControlHtml(item.id, {{name: "descripcion", label: "Descripción", type: "text"}}, datos.descripcion, seccionTipo, "materiales", tareaId);
        return `<tr data-item-id="${{item.id}}" data-rubro="materiales" data-tipo-item="chapa" data-seccion-id="${{seccionId}}">
            <td>${{descCtl}}</td>
            <td>${{campoControlHtml(item.id, {{name: "perfil_id", label: "Tipo", type: "text"}}, datos.perfil_id, seccionTipo, "materiales", tareaId)}}</td>
            <td>${{campoControlHtml(item.id, {{name: "cantidad", label: "Cantidad", type: "number"}}, datos.cantidad, seccionTipo, "materiales", tareaId)}}</td>
            <td>${{campoControlHtml(item.id, {{name: "largo_mm", label: "Largo (mm)", type: "number"}}, datos.largo_mm, seccionTipo, "materiales", tareaId)}}</td>
            <td>${{campoControlHtml(item.id, {{name: "precio_unitario_m2", label: "USD/M2", type: "number"}}, datos.precio_unitario_m2, seccionTipo, "materiales", tareaId)}}</td>
            <td style="text-align:right;"><span id="m2-item-${{item.id}}">${{fmtNum(item.m2 || 0, 2)}}</span></td>
            <td style="text-align:right;font-weight:700;"><span id="subtotal-item-${{item.id}}">${{fmtMoney(item.subtotal)}}</span></td>
            <td><button type="button" class="btn btn-icon btn-danger" onclick="eliminarLinea(${{item.id}}, ${{tareaId}})">✕</button></td>
        </tr>`;
    }}

    function filaMaterialGratingHtml(seccionId, seccionTipo, tareaId, item) {{
        const tipoItem = item.tipo_item || "grating";
        const datos = item.datos || {{}};
        if (tipoItem === "fijaciones") {{
            const descCtl = campoControlHtml(item.id, {{name: "descripcion", label: "Descripción", type: "text"}}, datos.descripcion || "Fijaciones", seccionTipo, "materiales", tareaId);
            return `<tr data-item-id="${{item.id}}" data-rubro="materiales" data-tipo-item="fijaciones" data-seccion-id="${{seccionId}}">
                <td>${{descCtl}}</td>
                <td class="muted" style="text-align:center;">–</td>
                <td style="text-align:right;" title="4 unidades por m2 de grating (automático)"><span id="cant-item-${{item.id}}">${{fmtNum(item.cantidad || 0, 0)}}</span></td>
                <td class="muted" style="text-align:center;">–</td>
                <td class="muted" style="text-align:center;">–</td>
                <td>${{campoControlHtml(item.id, {{name: "precio_unitario", label: "USD/UNIDAD", type: "number"}}, datos.precio_unitario, seccionTipo, "materiales", tareaId)}}</td>
                <td class="muted" style="text-align:center;">–</td>
                <td class="muted" style="text-align:center;">–</td>
                <td style="text-align:right;font-weight:700;"><span id="subtotal-item-${{item.id}}">${{fmtMoney(item.subtotal)}}</span></td>
                <td><button type="button" class="btn btn-icon btn-danger" onclick="eliminarLinea(${{item.id}}, ${{tareaId}})">✕</button></td>
            </tr>`;
        }}
        const descCtl = campoControlHtml(item.id, {{name: "descripcion", label: "Descripción", type: "text"}}, datos.descripcion, seccionTipo, "materiales", tareaId);
        const perfilGrating = (datos.perfil_id && PERFIL_POR_ID[datos.perfil_id]) ? PERFIL_POR_ID[datos.perfil_id] : null;
        const kgM2Inicial = perfilGrating ? (perfilGrating.kg_m || 0) : 0;
        return `<tr data-item-id="${{item.id}}" data-rubro="materiales" data-tipo-item="grating" data-seccion-id="${{seccionId}}">
            <td>${{descCtl}}</td>
            <td>${{campoControlHtml(item.id, {{name: "perfil_id", label: "Tipo", type: "text"}}, datos.perfil_id, seccionTipo, "materiales", tareaId)}}</td>
            <td>${{campoControlHtml(item.id, {{name: "cantidad", label: "Cantidad", type: "number"}}, datos.cantidad, seccionTipo, "materiales", tareaId)}}</td>
            <td>${{campoControlHtml(item.id, {{name: "m2", label: "M2", type: "number"}}, datos.m2, seccionTipo, "materiales", tareaId)}}</td>
            <td style="text-align:right;" title="Automático, según el Tipo elegido"><span id="kgm2-item-${{item.id}}">${{fmtNum(kgM2Inicial, 2)}}</span></td>
            <td>${{campoControlHtml(item.id, {{name: "precio_unitario_m2", label: "USD/M2", type: "number"}}, datos.precio_unitario_m2, seccionTipo, "materiales", tareaId)}}</td>
            <td style="text-align:right;"><span id="m2-item-${{item.id}}">${{fmtNum(item.m2 || 0, 2)}}</span></td>
            <td style="text-align:right;"><span id="kg-item-${{item.id}}">${{fmtNum(item.peso || 0, 1)}}</span></td>
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
            <td>${{campoControlHtml(item.id, {{name: "precio_unitario", label: "Precio unitario (USD)", type: "number"}}, datos.precio_unitario, seccionTipo, "materiales", tareaId)}}</td>
            <td style="text-align:right;font-weight:700;"><span id="subtotal-item-${{item.id}}">${{fmtMoney(item.subtotal)}}</span></td>
            <td><button type="button" class="btn btn-icon btn-danger" onclick="eliminarLinea(${{item.id}}, ${{tareaId}})">✕</button></td>
        </tr>`;
    }}

    function rubroBlockHtml(seccionId, seccionTipo, tareaId, rubro, items) {{
        ordenItemsPorSeccion[seccionId] = ordenItemsPorSeccion[seccionId] || {{}};
        ordenItemsPorSeccion[seccionId][rubro] = items.map(it => it.id);

        if (rubro === "materiales") {{
            const modo = modoMaterialesTarea(tareaId);
            const noListadoItems = items.filter(it => it.tipo_item === "no_listado");
            const filasNoListado = noListadoItems.map(it => filaMaterialNoListadoHtml(seccionId, seccionTipo, tareaId, it)).join("");
            const botonAgregarNoListado = `<div style="margin-top:6px;">
                    <button type="button" class="btn btn-sm btn-secondary" onclick="agregarLinea(${{seccionId}}, ${{tareaId}}, 'materiales', 'no_listado')">+ Agregar material no listado</button>
                </div>`;
            const totSubtotalNoListado = noListadoItems.reduce((acc, it) => acc + (it.subtotal || 0), 0);
            const tablaNoListadoHtml = `<div style="margin-top:6px;padding-top:5px;border-top:1px dashed #cbd5e1;">
                    <h4 style="font-size:12px;">materiales no listados</h4>
                    <div style="overflow-x:auto;">
                    <table class="tabla-materiales">
                        <thead><tr>
                            <th>Descripción</th><th>Cantidad</th><th>Unidad</th><th>Precio unitario (USD)</th><th>Precio total</th><th></th>
                        </tr></thead>
                        <tbody>${{filasNoListado || '<tr><td colspan="6" class="muted">Sin líneas todavía.</td></tr>'}}</tbody>
                        <tfoot><tr style="font-weight:700;background:#f8fafc;">
                            <td colspan="4" style="text-align:right;">Subtotal:</td>
                            <td style="text-align:right;"><span id="tot-subtotal-nolistado-${{seccionId}}">${{fmtMoney(totSubtotalNoListado)}}</span></td>
                            <td></td>
                        </tr></tfoot>
                    </table>
                    </div>
                    ${{botonAgregarNoListado}}
                </div>`;

            if (modo === "chapa") {{
                const chapaItems = items.filter(it => (it.tipo_item || "chapa") === "chapa");
                const tornillosItems = items.filter(it => it.tipo_item === "tornillos");
                // Tornillos siempre al final (depende del m2 total de la sección) y ya
                // viene autocreado (una única línea, sin botón "Agregar").
                const filas = chapaItems.concat(tornillosItems).map(it => filaMaterialChapaHtml(seccionId, seccionTipo, tareaId, it)).join("");
                const totalesChapa = chapaItems.concat(tornillosItems);
                const totM2Chapa = totalesChapa.reduce((acc, it) => acc + (it.m2 || 0), 0);
                const totSubtotalChapa = totalesChapa.reduce((acc, it) => acc + (it.subtotal || 0), 0);
                return `<div class="rubro-block">
                <h4>materiales</h4>
                <div style="overflow-x:auto;">
                <table class="tabla-materiales">
                    <thead><tr>
                        <th>Descripción</th><th>Tipo</th><th>Cantidad</th><th>Largo (mm)</th><th>USD/M2</th>
                        <th>Total m2</th><th>Subtotal</th><th></th>
                    </tr></thead>
                    <tbody>${{filas || '<tr><td colspan="8" class="muted">Sin líneas todavía.</td></tr>'}}</tbody>
                    <tfoot><tr style="font-weight:700;background:#f8fafc;">
                        <td colspan="5" style="text-align:right;">Subtotales:</td>
                        <td style="text-align:right;"><span id="tot-m2-chapa-${{seccionId}}">${{fmtNum(totM2Chapa, 2)}}</span></td>
                        <td style="text-align:right;"><span id="tot-subtotal-chapa-${{seccionId}}">${{fmtMoney(totSubtotalChapa)}}</span></td>
                        <td></td>
                    </tr></tfoot>
                </table>
                </div>
                <div style="margin-top:6px;">
                    <button type="button" class="btn btn-sm btn-secondary" onclick="agregarLinea(${{seccionId}}, ${{tareaId}}, 'materiales', 'chapa')">+ Chapa</button>
                </div>
                ${{tablaNoListadoHtml}}
            </div>`;
            }}

            if (modo === "grating") {{
                const gratingItems = items.filter(it => (it.tipo_item || "grating") === "grating");
                const fijacionesItems = items.filter(it => it.tipo_item === "fijaciones");
                // Fijaciones siempre al final (depende del m2 total de la sección) y ya
                // viene autocreada (una única línea, sin botón "Agregar").
                const filas = gratingItems.concat(fijacionesItems).map(it => filaMaterialGratingHtml(seccionId, seccionTipo, tareaId, it)).join("");
                return `<div class="rubro-block">
                <h4>materiales</h4>
                <div style="overflow-x:auto;">
                <table class="tabla-materiales">
                    <thead><tr>
                        <th>Descripción</th><th>Tipo</th><th>Cantidad</th><th>M2</th><th>KG/M2</th><th>USD/M2</th>
                        <th>Total m2</th><th>Total kg</th><th>Subtotal</th><th></th>
                    </tr></thead>
                    <tbody>${{filas || '<tr><td colspan="10" class="muted">Sin líneas todavía.</td></tr>'}}</tbody>
                </table>
                </div>
                <div style="margin-top:6px;">
                    <button type="button" class="btn btn-sm btn-secondary" onclick="agregarLinea(${{seccionId}}, ${{tareaId}}, 'materiales', 'grating')">+ Grating</button>
                </div>
                ${{tablaNoListadoHtml}}
            </div>`;
            }}

            const perfilItems = items.filter(it => (it.tipo_item || "perfil") === "perfil");
            const placasItems = items.filter(it => it.tipo_item === "porcentaje");
            // Placas siempre se muestra al final de la tabla (depende del total de la sección)
            // y ya viene autocreada (una única línea, sin botón "Agregar").
            const filas = perfilItems.concat(placasItems).map(it => filaMaterialHtml(seccionId, seccionTipo, tareaId, it)).join("");
            const totalesMateriales = perfilItems.concat(placasItems);
            const totKgMateriales = totalesMateriales.reduce((acc, it) => acc + (it.peso || 0), 0);
            const totM2Materiales = totalesMateriales.reduce((acc, it) => acc + (it.m2 || 0), 0);
            const totSubtotalMateriales = totalesMateriales.reduce((acc, it) => acc + (it.subtotal || 0), 0);
            return `<div class="rubro-block">
                <h4>materiales</h4>
                <div style="overflow-x:auto;">
                <table class="tabla-materiales">
                    <thead><tr>
                        <th>Descripción</th><th>Perfil</th><th>Cantidad</th><th>Largo (mm)</th><th>USD/KG</th>
                        <th>% Placas</th><th>Total (kg)</th><th>Total m2</th><th>Subtotal</th><th></th>
                    </tr></thead>
                    <tbody>${{filas || '<tr><td colspan="10" class="muted">Sin líneas todavía.</td></tr>'}}</tbody>
                    <tfoot><tr style="font-weight:700;background:#f8fafc;">
                        <td colspan="6" style="text-align:right;">Subtotales:</td>
                        <td style="text-align:right;"><span id="tot-kg-materiales-${{seccionId}}">${{fmtNum(totKgMateriales, 1)}}</span></td>
                        <td style="text-align:right;"><span id="tot-m2-materiales-${{seccionId}}">${{fmtNum(totM2Materiales, 2)}}</span></td>
                        <td style="text-align:right;"><span id="tot-subtotal-materiales-${{seccionId}}">${{fmtMoney(totSubtotalMateriales)}}</span></td>
                        <td></td>
                    </tr></tfoot>
                </table>
                </div>
                <div style="margin-top:6px;">
                    <button type="button" class="btn btn-sm btn-secondary" onclick="agregarLinea(${{seccionId}}, ${{tareaId}}, 'materiales', 'perfil')">+ Perfil</button>
                </div>
                ${{tablaNoListadoHtml}}
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
            // Tareas en modo "chapa" (ver TAREAS_MODO_CHAPA) no usan ingeniería ni bulones.
            // Tareas en modo "grating" (ver TAREAS_MODO_GRATING) no usan bulones ni pintura (va galvanizado), pero sí ingeniería.
            const modo = modoMaterialesTarea(tareaId);
            const primerBloque = modo === "chapa"
                ? ["materiales", "pintura"]
                : modo === "grating"
                    ? ["materiales", "ingenieria"]
                    : ["materiales", "ingenieria", "bulones", "pintura"];
            bloquesRubros = primerBloque
                .map(rubro => rubroBlockHtml(seccion.id, seccion.tipo, tareaId, rubro, itemsPorRubro[rubro] || []))
                .join("");
            bloquesRubros += ["mano_obra", "consumibles", "subcontratos", "fletes"]
                .map(rubro => rubroBlockHtml(seccion.id, seccion.tipo, tareaId, rubro, itemsPorRubro[rubro] || []))
                .join("");
        }} else {{
            const rubrosDeLaSeccion = RUBROS_POR_SECCION[seccion.tipo] || [];
            bloquesRubros = rubrosDeLaSeccion
                .map(rubro => rubroBlockHtml(seccion.id, seccion.tipo, tareaId, rubro, itemsPorRubro[rubro] || []))
                .join("");
        }}

        return `<div class="seccion-wrap seccion-wrap-${{seccion.tipo.toLowerCase()}}" id="seccion-wrap-${{seccion.id}}">
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
            bloques += `<div class="mini-resultado mini-fab" id="mini-fab-${{tareaId}}"><b>Fabricación</b><div class="muted">Sin calcular.</div></div>`;
        }}
        if (tipos.includes("MONTAJE")) {{
            bloques += `<div class="mini-resultado mini-mon" id="mini-mon-${{tareaId}}"><b>Montaje</b><div class="muted">Sin calcular.</div></div>`;
        }}
        const nombreTarea = (TAREAS_INFO[String(tareaId)] || {{}}).nombre || ("Tarea " + tareaId);
        bloques += `<div class="mini-resultado" id="mini-tarea-${{tareaId}}" style="background:#eef2ff;border-color:#c7d2fe;"><b>Total ${{nombreTarea}}</b><div class="muted">Sin calcular.</div></div>`;
        bloques += `<div class="mini-resultado indicador" id="mini-indicador-${{tareaId}}"><b>Indicadores</b><div class="muted">Sin calcular.</div></div>`;
        return `<div class="panel-sticky">
            <div style="display:flex;align-items:center;gap:8px;margin-bottom:6px;">
                <b style="font-size:13px;">📊 Resumen</b>
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
        const totales = {{
            materialesKg: 0, materialesM2: 0, materialesSubtotal: 0,
            noListadoSubtotal: 0, chapaM2: 0, chapaSubtotal: 0,
        }};
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
            const kgm2Span = document.getElementById(`kgm2-item-${{itemId}}`);
            if (kgm2Span && itemRes.peso !== undefined && itemRes.m2) kgm2Span.textContent = fmtNum(itemRes.peso / itemRes.m2, 2);
            const m2Span = document.getElementById(`m2-item-${{itemId}}`);
            if (m2Span && itemRes.m2 !== undefined) m2Span.textContent = fmtNum(itemRes.m2, 2);
            const cantSpan = document.getElementById(`cant-item-${{itemId}}`);
            if (cantSpan && itemRes.cantidad !== undefined) cantSpan.textContent = fmtNum(itemRes.cantidad, 0);

            if (rubro === "materiales" && itemRes.tipo_item === "no_listado") {{
                totales.noListadoSubtotal += itemRes.subtotal || 0;
            }} else if (rubro === "materiales" && (itemRes.tipo_item === "chapa" || itemRes.tipo_item === "tornillos")) {{
                totales.chapaM2 += itemRes.m2 || 0;
                totales.chapaSubtotal += itemRes.subtotal || 0;
            }} else if (rubro === "materiales" && (itemRes.tipo_item === "perfil" || itemRes.tipo_item === "porcentaje" || !itemRes.tipo_item)) {{
                totales.materialesKg += itemRes.peso || 0;
                totales.materialesM2 += itemRes.m2 || 0;
                totales.materialesSubtotal += itemRes.subtotal || 0;
            }}
        }});

        const setTxt = (id, texto) => {{ const el = document.getElementById(id); if (el) el.textContent = texto; }};
        setTxt(`tot-kg-materiales-${{seccionId}}`, fmtNum(totales.materialesKg, 1));
        setTxt(`tot-m2-materiales-${{seccionId}}`, fmtNum(totales.materialesM2, 2));
        setTxt(`tot-subtotal-materiales-${{seccionId}}`, fmtMoney(totales.materialesSubtotal));
        setTxt(`tot-subtotal-nolistado-${{seccionId}}`, fmtMoney(totales.noListadoSubtotal));
        setTxt(`tot-m2-chapa-${{seccionId}}`, fmtNum(totales.chapaM2, 2));
        setTxt(`tot-subtotal-chapa-${{seccionId}}`, fmtMoney(totales.chapaSubtotal));
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
                }});
                const miniFab = document.getElementById(`mini-fab-${{tareaId}}`);
                if (miniFab) miniFab.innerHTML = miniCascadaHtml("Fabricación", resultado.fabricacion.cascada);
                const miniMon = document.getElementById(`mini-mon-${{tareaId}}`);
                if (miniMon) miniMon.innerHTML = miniCascadaHtml("Montaje", resultado.montaje.cascada);
                const miniTarea = document.getElementById(`mini-tarea-${{tareaId}}`);
                const nombreTarea = (TAREAS_INFO[String(tareaId)] || {{}}).nombre || ("Tarea " + tareaId);
                const cf = resultado.fabricacion.cascada, cm = resultado.montaje.cascada;
                const cascadaTarea = {{
                    costo_directo: (cf.costo_directo || 0) + (cm.costo_directo || 0),
                    gg: (cf.gg || 0) + (cm.gg || 0),
                    beneficio: (cf.beneficio || 0) + (cm.beneficio || 0),
                    impuestos: (cf.impuestos || 0) + (cm.impuestos || 0),
                    precio_venta: resultado.precio_venta_tarea,
                }};
                if (miniTarea) miniTarea.innerHTML = miniCascadaHtml("Total " + nombreTarea, cascadaTarea);
            }});
        fetch(`${{API}}/presupuestos/${{PRESUPUESTO_ID}}/recalcular`)
            .then(r => r.json())
            .then(({{ totales, error }}) => {{
                if (error) return;
                const cont = document.getElementById("totales-presupuesto");
                if (cont) cont.innerHTML = `<div class="resultado-box"><div class="fila" style="font-weight:800;"><span>TOTAL PRESUPUESTO</span><span>${{fmtMoney(totales.precio_venta_presupuesto)}}</span></div></div>`;
                cargarResumenTareasPresupuesto();
            }});
        fetch(`${{API}}/tareas/${{tareaId}}/indicador`)
            .then(r => r.json())
            .then((datos) => {{
                if (datos.error) return;
                const miniInd = document.getElementById(`mini-indicador-${{tareaId}}`);
                if (!miniInd) return;
                const sinTipoCambio = !datos.tipo_cambio_referencia ? '<div class="muted">Sin tipo de cambio ref.</div>' : "";
                const modoTareaInd = modoMaterialesTarea(tareaId);
                const filasInd = modoTareaInd === "chapa"
                    ? `<div class="fila"><span>m2/día (montaje)</span><span>${{fmtNum(datos.m2_por_dia_montaje, 2)}}</span></div>
                       <div class="fila"><span>USD/m2 (total)</span><span>${{fmtNum(datos.usd_por_m2_total, 2)}}</span></div>`
                    : `<div class="fila"><span>KG/HH</span><span>${{fmtNum(datos.kg_por_hh, 2)}}</span></div>
                       <div class="fila"><span>USD/kg</span><span>${{fmtNum(datos.costo_por_kg_usd, 4)}}</span></div>`;
                miniInd.innerHTML = `<b>Indicadores</b>
                    ${{filasInd}}
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
        const mismaTarea = cont.dataset.tareaActual === String(tareaId);
        const scrollYPrevio = window.scrollY;
        if (!mismaTarea) cont.innerHTML = "Cargando...";
        fetch(`${{API}}/tareas/${{tareaId}}`)
            .then(r => r.json())
            .then(({{ tarea, error }}) => {{
                if (error) {{ cont.innerHTML = `<span style="color:#dc2626;">${{error}}</span>`; return; }}

                // Autocrear la línea única de los rubros "de una sola línea" (sin botón
                // "Agregar línea": el cuadro vacío para completar aparece directo).
                // Las tareas en modo "chapa" (ver TAREAS_MODO_CHAPA) no usan ingeniería ni bulones.
                const modoTarea = modoMaterialesTarea(tareaId);
                const RUBROS_AUTO_UNA_LINEA = {{
                    FABRICACION: modoTarea === "chapa"
                        ? ["pintura", "fletes", "mano_obra", "consumibles"]
                        : modoTarea === "grating"
                            ? ["fletes", "mano_obra", "consumibles", "ingenieria"]
                            : ["bulones", "pintura", "fletes", "mano_obra", "consumibles", "ingenieria"],
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
                    if (s.tipo === "FABRICACION" && modoTarea === "chapa") {{
                        const hayTornillos = (s.items || []).some(it => it.rubro === "materiales" && it.tipo_item === "tornillos");
                        if (!hayTornillos) {{
                            creaciones.push(fetch(`${{API}}/secciones/${{s.id}}/items`, {{
                                method: "POST", headers: {{"Content-Type": "application/json"}},
                                body: JSON.stringify({{ rubro: "materiales", datos: {{"descripcion": "Tornillos"}}, tipo_item: "tornillos" }})
                            }}));
                        }}
                    }} else if (s.tipo === "FABRICACION" && modoTarea === "grating") {{
                        const hayFijaciones = (s.items || []).some(it => it.rubro === "materiales" && it.tipo_item === "fijaciones");
                        if (!hayFijaciones) {{
                            creaciones.push(fetch(`${{API}}/secciones/${{s.id}}/items`, {{
                                method: "POST", headers: {{"Content-Type": "application/json"}},
                                body: JSON.stringify({{ rubro: "materiales", datos: {{"descripcion": "Fijaciones"}}, tipo_item: "fijaciones" }})
                            }}));
                        }}
                    }} else if (s.tipo === "FABRICACION") {{
                        const hayPlacas = (s.items || []).some(it => it.rubro === "materiales" && it.tipo_item === "porcentaje");
                        if (!hayPlacas) {{
                            creaciones.push(fetch(`${{API}}/secciones/${{s.id}}/items`, {{
                                method: "POST", headers: {{"Content-Type": "application/json"}},
                                body: JSON.stringify({{ rubro: "materiales", datos: {{"porcentaje": 0.15, "descripcion": "Placas"}}, tipo_item: "porcentaje" }})
                            }}));
                        }}
                        const infoTarea = TAREAS_INFO[String(tareaId)] || {{}};
                        const tipoTarea = infoTarea.tipo || infoTarea.nombre || "";
                        const plantilla = MATERIALES_TEMPLATE_POR_TAREA[tipoTarea];
                        const hayPerfiles = (s.items || []).some(it => it.rubro === "materiales" && it.tipo_item === "perfil");
                        if (plantilla && !hayPerfiles) {{
                            plantilla.forEach(linea => {{
                                const perfil = PERFILES.find(p => (p.label || "").trim().toLowerCase() === linea.catalogo_descripcion.trim().toLowerCase());
                                if (!perfil) return;
                                creaciones.push(fetch(`${{API}}/secciones/${{s.id}}/items`, {{
                                    method: "POST", headers: {{"Content-Type": "application/json"}},
                                    body: JSON.stringify({{
                                        rubro: "materiales",
                                        tipo_item: "perfil",
                                        datos: {{
                                            descripcion: linea.descripcion,
                                            perfil_id: perfil.id,
                                            cantidad: linea.cantidad || 0,
                                            largo_mm: linea.largo_mm || 0,
                                            precio_unitario_kg: linea.precio_unitario_kg,
                                        }},
                                    }})
                                }}));
                            }});
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
                window.scrollTo(window.scrollX, scrollYPrevio);
                inicializarCombosPerfil(cont);
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

# Reporte 3 (pedido "día 0" de Odoo): botones + diálogo del Resumen. Son strings
# planos (no f-strings) para no tener que escapar las llaves del JS.
_ODOO_PEDIDO_BOTONES_HTML = """
                <button type="button" class="btn" onclick="abrirPedidoOdoo('fab')">⬇ Odoo: pedido Fabricación</button>
                <button type="button" class="btn" onclick="abrirPedidoOdoo('mon')">⬇ Odoo: pedido Montaje</button>
"""

_ODOO_PEDIDO_DIALOGO_HTML = """
<dialog id="dlg-odoo" style="border:1px solid #cbd5e1;border-radius:12px;padding:16px;max-width:560px;width:92%;">
    <h3 id="odoo-titulo" style="margin:0 0 8px 0;">Pedido para Odoo</h3>
    <div id="odoo-estado" class="muted">Cargando...</div>
    <div id="odoo-cuerpo" style="display:none;">
        <label id="odoo-label" for="odoo-analitica" style="font-weight:700;display:block;margin-top:4px;"></label>
        <input id="odoo-analitica" type="text" inputmode="numeric" autocomplete="off" style="width:100%;padding:8px;margin-top:4px;">
        <div id="odoo-avisos" style="margin-top:10px;"></div>
        <div id="odoo-totales" style="margin-top:10px;"></div>
    </div>
    <div id="odoo-error" style="color:#b91c1c;font-weight:600;margin-top:8px;"></div>
    <div style="margin-top:12px;display:flex;gap:8px;justify-content:flex-end;">
        <button type="button" class="btn btn-secondary" onclick="cerrarPedidoOdoo()">Cancelar</button>
        <button type="button" class="btn" id="odoo-descargar" onclick="descargarPedidoOdoo()" disabled>Descargar</button>
    </div>
</dialog>
"""

_ODOO_PEDIDO_JS = """
const ODOO_URL = "/modulo/presupuestos/__PRESUPUESTO_ID__/resumen/odoo-pedido/";
let odooClave = null;
let odooNombreArchivo = "pedido_odoo.xlsx";

function odooFmt(v) {
    return "$ " + (Number(v) || 0).toLocaleString("es-AR", {minimumFractionDigits: 2, maximumFractionDigits: 2});
}
function odooEsc(t) {
    return String(t).replace(/[&<>"']/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]));
}
function odooEl(id) { return document.getElementById(id); }

function abrirPedidoOdoo(clave) {
    odooClave = clave;
    odooEl("odoo-titulo").textContent = clave === "fab" ? "Pedido de Fabricación para Odoo" : "Pedido de Montaje para Odoo";
    odooEl("odoo-estado").textContent = "Cargando...";
    odooEl("odoo-cuerpo").style.display = "none";
    odooEl("odoo-error").textContent = "";
    odooEl("odoo-descargar").disabled = true;
    odooEl("dlg-odoo").showModal();

    fetch(ODOO_URL + clave + "/preview")
        .then(r => r.json().then(d => ({ok: r.ok, d: d})))
        .then(({ok, d}) => {
            odooEl("odoo-estado").textContent = "";
            if (!ok) { odooEl("odoo-error").textContent = d.error || "No se pudo preparar el pedido."; return; }

            odooNombreArchivo = d.nombre_archivo;
            odooEl("odoo-label").textContent = d.label_analitica;
            odooEl("odoo-analitica").value = d.analitica_id || "";

            let avisos = "";
            if (d.sin_mapear.length) {
                avisos = "<div style='background:#fef3c7;border:1px solid #fcd34d;border-radius:8px;padding:8px;'>" +
                    "<b>Quedan fuera del archivo (sin producto de Odoo asignado):</b><ul style='margin:6px 0 0 18px;padding:0;'>" +
                    d.sin_mapear.map(a => "<li>" + odooEsc(a.concepto) + ": " + odooFmt(a.importe) + "</li>").join("") +
                    "</ul></div>";
            }
            odooEl("odoo-avisos").innerHTML = avisos;

            odooEl("odoo-totales").innerHTML =
                "<div><b>Total del archivo:</b> " + odooFmt(d.total_archivo) + "</div>" +
                "<div><b>Total de los rubros incluidos en el presupuesto:</b> " + odooFmt(d.total_rubros) + "</div>" +
                "<div class='muted'>Diferencia: " + odooFmt(d.diferencia) + " (suma de lo avisado: " + odooFmt(d.suma_sin_mapear) + ")</div>" +
                (d.cuadra ? "" : "<div style='color:#b91c1c;font-weight:600;'>La diferencia no coincide con lo avisado: revisar antes de importar.</div>");

            odooEl("odoo-cuerpo").style.display = "block";
            if (!d.lineas.length) {
                odooEl("odoo-error").textContent = "No hay líneas para exportar: ningún concepto tiene producto de Odoo asignado.";
                return;
            }
            odooEl("odoo-descargar").disabled = false;
        })
        .catch(() => {
            odooEl("odoo-estado").textContent = "";
            odooEl("odoo-error").textContent = "No se pudo preparar el pedido.";
        });
}

function cerrarPedidoOdoo() { odooEl("dlg-odoo").close(); }

function descargarPedidoOdoo() {
    const valor = odooEl("odoo-analitica").value.trim();
    odooEl("odoo-error").textContent = "";
    if (!valor) { odooEl("odoo-error").textContent = "Ingresá el ID de la cuenta analítica de Odoo."; return; }
    if (!/^[0-9]+$/.test(valor) || Number(valor) <= 0) {
        odooEl("odoo-error").textContent = "El ID de la cuenta analítica debe ser un número entero mayor a 0 (por ejemplo 932).";
        return;
    }

    const datos = new FormData();
    datos.append("analitica_id", valor);
    fetch(ODOO_URL + odooClave + "/descargar", {method: "POST", body: datos})
        .then(async r => {
            if (!r.ok) {
                let msg = "No se pudo generar el archivo.";
                try { msg = (await r.json()).error || msg; } catch (e) {}
                odooEl("odoo-error").textContent = msg;
                return;
            }
            const blob = await r.blob();
            const enlace = document.createElement("a");
            enlace.href = URL.createObjectURL(blob);
            enlace.download = odooNombreArchivo;
            document.body.appendChild(enlace);
            enlace.click();
            enlace.remove();
            setTimeout(() => URL.revokeObjectURL(enlace.href), 2000);
            cerrarPedidoOdoo();
        })
        .catch(() => { odooEl("odoo-error").textContent = "No se pudo generar el archivo."; });
}
"""


@presupuestos_bp.route("/<int:presupuesto_id>/resumen", methods=["GET"])
def vista_resumen_presupuesto(presupuesto_id):
    db = _db()
    presupuesto = obtener_presupuesto(db, presupuesto_id)
    if not presupuesto:
        return "<h3>❌ Presupuesto no encontrado</h3>", 404

    botones_odoo = _ODOO_PEDIDO_BOTONES_HTML if presupuesto.get("estado") == "adjudicado" else ""
    boton_reparto = (
        f'<a href="/modulo/presupuestos/{presupuesto_id}/reparto" class="btn">🧩 Mapeo Tarea → OT</a>'
        if presupuesto.get("estado") == "adjudicado" else ""
    )
    dialogo_odoo = (
        _ODOO_PEDIDO_DIALOGO_HTML + "<script>" + _ODOO_PEDIDO_JS.replace("__PRESUPUESTO_ID__", str(presupuesto_id)) + "</script>"
        if botones_odoo else ""
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
            <h2>📊 Resumen y reportes — Presupuesto #{presupuesto_id}</h2>
            <div>
                <a href="/modulo/presupuestos/{presupuesto_id}" class="btn btn-secondary">⬅️ Volver al presupuesto</a>
                <a href="/modulo/presupuestos/{presupuesto_id}/resumen/export.csv" class="btn">⬇ Exportar recursos (CSV)</a>
                <a href="/modulo/presupuestos/{presupuesto_id}/resumen/reporte-explosion-insumos.xlsx" class="btn">⬇ Explosión de insumos (Excel)</a>
                <a href="/modulo/presupuestos/{presupuesto_id}/resumen/reporte-prevision-fondos.xlsx" class="btn">⬇ Previsión de fondos (Excel)</a>
                {botones_odoo}
                {boton_reparto}
            </div>
        </div>
{dialogo_odoo}

        <div class="card">
            <h3 style="margin-top:0;">Resumen por tarea (Fabricación + Montaje)</h3>
            <div id="resumen-tareas" class="muted">Cargando...</div>
        </div>

        <div class="card">
            <h3 style="margin-top:0;">Reporte cruzado por categoría</h3>
            <div class="muted" style="margin-bottom:8px;">REPORTE POR CATEGORIA</div>
            <div id="reporte-categorias" class="muted">Cargando...</div>
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


_XLSX_MIMETYPE = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"


@presupuestos_bp.route("/<int:presupuesto_id>/resumen/reporte-explosion-insumos.xlsx", methods=["GET"])
def vista_reporte_explosion_insumos_xlsx(presupuesto_id):
    """Sección 5.2 — Reporte 1: una pestaña por tarea + pestaña "Resumen"."""
    db = _db()
    presupuesto = obtener_presupuesto(db, presupuesto_id)
    if not presupuesto:
        return "<h3>❌ Presupuesto no encontrado</h3>", 404

    buffer = generar_reporte_explosion_insumos(db, presupuesto_id)
    nombre_archivo = f"explosion_insumos_presupuesto_{presupuesto_id}.xlsx"
    return send_file(buffer, mimetype=_XLSX_MIMETYPE, as_attachment=True, download_name=nombre_archivo)


@presupuestos_bp.route("/<int:presupuesto_id>/resumen/reporte-prevision-fondos.xlsx", methods=["GET"])
def vista_reporte_prevision_fondos_xlsx(presupuesto_id):
    """Sección 5.3 — Reporte 2: previsión de fondos para Odoo."""
    db = _db()
    presupuesto = obtener_presupuesto(db, presupuesto_id)
    if not presupuesto:
        return "<h3>❌ Presupuesto no encontrado</h3>", 404

    obra_referencia = _obtener_obra_referencia(db, presupuesto, presupuesto_id)
    buffer = generar_reporte_prevision_fondos(db, presupuesto_id, obra_referencia)
    nombre_archivo = f"prevision_fondos_presupuesto_{presupuesto_id}.xlsx"
    return send_file(buffer, mimetype=_XLSX_MIMETYPE, as_attachment=True, download_name=nombre_archivo)


# ─────────────────────────────────────────────────────────────────
# Reporte 3 — pedido "día 0" para carga masiva en Odoo (un archivo por sección)
# ─────────────────────────────────────────────────────────────────

_ODOO_SECCIONES = {"fab": "FABRICACION", "mon": "MONTAJE"}


def _contexto_pedido_odoo(presupuesto_id, clave):
    """Devuelve ((db, presupuesto, seccion), None) o (None, respuesta_de_error)."""
    db = _db()
    presupuesto = obtener_presupuesto(db, presupuesto_id)
    if not presupuesto:
        return None, (jsonify({"error": "Presupuesto no encontrado"}), 404)
    seccion = _ODOO_SECCIONES.get(clave)
    if not seccion:
        return None, (jsonify({"error": "Sección inválida (usar fab o mon)"}), 404)
    if presupuesto.get("estado") != "adjudicado":
        return None, (jsonify({"error": "El pedido de Odoo solo se genera para presupuestos adjudicados."}), 400)
    return (db, presupuesto, seccion), None


@presupuestos_bp.route("/<int:presupuesto_id>/resumen/odoo-pedido/<clave>/preview", methods=["GET"])
def vista_odoo_pedido_preview(presupuesto_id, clave):
    """Avisos (conceptos sin producto de Odoo) y totales antes de descargar."""
    contexto, error = _contexto_pedido_odoo(presupuesto_id, clave)
    if error:
        return error
    db, presupuesto, seccion = contexto

    obra_referencia = _obtener_obra_referencia(db, presupuesto, presupuesto_id)
    try:
        pedido = armar_pedido_odoo(db, presupuesto, seccion, obra_referencia)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    campo_analitica = "odoo_analitica_fab_id" if seccion == "FABRICACION" else "odoo_analitica_mon_id"
    return jsonify({
        "order_reference": pedido["order_reference"],
        "label_analitica": f"ID de la cuenta analítica en Odoo para {pedido['order_reference']}",
        "analitica_id": presupuesto.get(campo_analitica),
        "nombre_archivo": nombre_archivo_pedido(pedido["order_reference"]),
        "lineas": pedido["lineas"],
        "sin_mapear": pedido["sin_mapear"],
        "total_archivo": pedido["total_archivo"],
        "total_rubros": pedido["total_rubros"],
        "diferencia": pedido["diferencia"],
        "suma_sin_mapear": pedido["suma_sin_mapear"],
        "cuadra": pedido["cuadra"],
    })


@presupuestos_bp.route("/<int:presupuesto_id>/resumen/odoo-pedido/<clave>/descargar", methods=["POST"])
def vista_odoo_pedido_descargar(presupuesto_id, clave):
    """Valida el ID analítico, genera el .xlsx y recién ahí lo guarda en el presupuesto."""
    contexto, error = _contexto_pedido_odoo(presupuesto_id, clave)
    if error:
        return error
    db, presupuesto, seccion = contexto

    try:
        analitica_id = validar_analitica_id(request.form.get("analitica_id"))
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    obra_referencia = _obtener_obra_referencia(db, presupuesto, presupuesto_id)
    try:
        pedido = armar_pedido_odoo(db, presupuesto, seccion, obra_referencia)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    if not pedido["lineas"]:
        return jsonify({"error": "No hay líneas para exportar: ningún concepto tiene producto de Odoo asignado."}), 400

    buffer = generar_excel_pedido_odoo(pedido, analitica_id)
    actualizar_analitica_odoo(db, presupuesto_id, seccion, analitica_id)
    return send_file(
        buffer,
        mimetype=_XLSX_MIMETYPE,
        as_attachment=True,
        download_name=nombre_archivo_pedido(pedido["order_reference"]),
    )
