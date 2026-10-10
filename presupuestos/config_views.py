"""Pantalla de Configuración del módulo Presupuestos (sección 4 punto 5 de
ESPECIFICACION_MODULO_PRESUPUESTOS_V1.md).

Pantalla SEPARADA de la carga de presupuestos (paso 4 / views.py): edita
`config_presupuestos` (tarifas y % por defecto), y el ABM de
`catalogo_equipos` y `catalogo_esquemas_pintura` (sección 2.1). No se mezcla
con las pantallas de listado/carga de presupuestos.
"""
from flask import request, redirect
import html as html_lib

from . import presupuestos_bp
from .routes import _db
from .models import (
    obtener_config,
    actualizar_config,
    crear_equipo,
    listar_equipos,
    actualizar_equipo,
    eliminar_equipo,
    crear_esquema_pintura,
    listar_esquemas_pintura,
    actualizar_esquema_pintura,
    eliminar_esquema_pintura,
)
from .views import _ESTILO_BASE
from .constants import ODOO_PRODUCTOS_EQUIPOS_SUGERIDOS


def _num(valor):
    try:
        return float(valor)
    except (TypeError, ValueError):
        return 0.0


def _texto_o_none(valor):
    return (valor or "").strip() or None


# ─────────────────────────────────────────────────────────────────
# Configuración global (config_presupuestos)
# ─────────────────────────────────────────────────────────────────

@presupuestos_bp.route("/configuracion", methods=["GET", "POST"])
def vista_configuracion():
    db = _db()

    if request.method == "POST" and request.form.get("_form") == "config":
        actualizar_config(
            db,
            gg_pct_default_fab=_num(request.form.get("gg_pct_default_fab")) / 100,
            beneficio_pct_default_fab=_num(request.form.get("beneficio_pct_default_fab")) / 100,
            imp_pct_default_fab=_num(request.form.get("imp_pct_default_fab")) / 100,
            gg_pct_default_mon=_num(request.form.get("gg_pct_default_mon")) / 100,
            beneficio_pct_default_mon=_num(request.form.get("beneficio_pct_default_mon")) / 100,
            imp_pct_default_mon=_num(request.form.get("imp_pct_default_mon")) / 100,
            tarifa_dh_taller_default=_num(request.form.get("tarifa_dh_taller_default")),
            tarifa_dh_obra_default=_num(request.form.get("tarifa_dh_obra_default")),
            tarifa_consumible_dh_taller_default=_num(request.form.get("tarifa_consumible_dh_taller_default")),
            tarifa_consumible_dh_obra_default=_num(request.form.get("tarifa_consumible_dh_obra_default")),
        )
        return redirect("/modulo/presupuestos/configuracion")

    config = obtener_config(db)
    equipos = listar_equipos(db)
    esquemas = listar_esquemas_pintura(db)

    return _pagina_configuracion(config, equipos, esquemas)


def _campo_pct(nombre, valor):
    return f"""
    <div>
        <label>{nombre.replace('_', ' ')}</label>
        <input type="number" step="0.01" name="{nombre}" value="{float(valor or 0) * 100:.2f}">
    </div>
    """


def _campo_monto(nombre, valor):
    return f"""
    <div>
        <label>{nombre.replace('_', ' ')}</label>
        <input type="number" step="0.01" name="{nombre}" value="{float(valor or 0):.2f}">
    </div>
    """


def _pagina_configuracion(config, equipos, esquemas):
    forms_equipos = "".join(
        f'<form id="equipo-form-{e["id"]}" method="post" action="/modulo/presupuestos/configuracion/equipos/{e["id"]}/editar"></form>'
        for e in equipos
    )
    filas_equipos = "".join(
        f"""
        <tr>
            <td><input form="equipo-form-{e['id']}" type="text" name="nombre" value="{e['nombre']}" required></td>
            <td><input form="equipo-form-{e['id']}" type="number" step="0.01" name="tarifa_dia_default" value="{e['tarifa_dia_default'] or 0}"></td>
            <td><input form="equipo-form-{e['id']}" type="text" name="producto_odoo" list="productos-odoo-equipos" value="{html_lib.escape(e['producto_odoo'] or '', quote=True)}" placeholder="(sin asignar)"></td>
            <td style="white-space:nowrap;">
                <button form="equipo-form-{e['id']}" type="submit" class="btn btn-sm">Guardar</button>
                <form method="post" action="/modulo/presupuestos/configuracion/equipos/{e['id']}/eliminar" style="display:inline;" onsubmit="return confirm('¿Eliminar equipo {e['nombre']}?');">
                    <button type="submit" class="btn btn-sm btn-danger">Eliminar</button>
                </form>
            </td>
        </tr>
        """
        for e in equipos
    ) or "<tr><td colspan='4' class='sin-datos'>Sin equipos cargados.</td></tr>"

    opciones_productos_equipos = "".join(
        f'<option value="{html_lib.escape(p, quote=True)}"></option>' for p in ODOO_PRODUCTOS_EQUIPOS_SUGERIDOS
    )

    forms_esquemas = "".join(
        f'<form id="esquema-form-{e["id"]}" method="post" action="/modulo/presupuestos/configuracion/esquemas/{e["id"]}/editar"></form>'
        for e in esquemas
    )
    filas_esquemas = "".join(
        f"""
        <tr>
            <td><input form="esquema-form-{e['id']}" type="text" name="nombre" value="{e['nombre']}" required></td>
            <td><input form="esquema-form-{e['id']}" type="number" step="0.01" name="precio_unitario_m2_default" value="{e['precio_unitario_m2_default'] or 0}"></td>
            <td style="white-space:nowrap;">
                <button form="esquema-form-{e['id']}" type="submit" class="btn btn-sm">Guardar</button>
                <form method="post" action="/modulo/presupuestos/configuracion/esquemas/{e['id']}/eliminar" style="display:inline;" onsubmit="return confirm('¿Eliminar esquema {e['nombre']}?');">
                    <button type="submit" class="btn btn-sm btn-danger">Eliminar</button>
                </form>
            </td>
        </tr>
        """
        for e in esquemas
    ) or "<tr><td colspan='3' class='sin-datos'>Sin esquemas de pintura cargados.</td></tr>"


    return f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>{_ESTILO_BASE}</style>
    </head>
    <body>
    <div class="wrap">
        <div class="top-bar">
            <h2>⚙️ Configuración de Presupuestos</h2>
            <a href="/modulo/presupuestos" class="btn btn-secondary">⬅️ Volver</a>
        </div>

        <div class="card">
            <h3>Tarifas y porcentajes por defecto</h3>
            <p class="muted">Se precargan en los presupuestos nuevos. Cada presupuesto conserva sus propios valores, así que los cambios posteriores no modifican los anteriores.</p>
            <form method="post">
                <input type="hidden" name="_form" value="config">
                <h4>Fabricación</h4>
                <div class="grid3">
                    {_campo_pct('gg_pct_default_fab', config['gg_pct_default_fab'])}
                    {_campo_pct('beneficio_pct_default_fab', config['beneficio_pct_default_fab'])}
                    {_campo_pct('imp_pct_default_fab', config['imp_pct_default_fab'])}
                </div>
                <h4>Montaje</h4>
                <div class="grid3">
                    {_campo_pct('gg_pct_default_mon', config['gg_pct_default_mon'])}
                    {_campo_pct('beneficio_pct_default_mon', config['beneficio_pct_default_mon'])}
                    {_campo_pct('imp_pct_default_mon', config['imp_pct_default_mon'])}
                </div>
                <h4>Tarifas de mano de obra y consumibles ($/día-hombre)</h4>
                <div class="grid2">
                    {_campo_monto('tarifa_dh_taller_default', config['tarifa_dh_taller_default'])}
                    {_campo_monto('tarifa_dh_obra_default', config['tarifa_dh_obra_default'])}
                </div>
                <div class="grid2">
                    {_campo_monto('tarifa_consumible_dh_taller_default', config['tarifa_consumible_dh_taller_default'])}
                    {_campo_monto('tarifa_consumible_dh_obra_default', config['tarifa_consumible_dh_obra_default'])}
                </div>
                <button type="submit" class="btn">Guardar configuración</button>
            </form>
        </div>

        <div class="card">
            <h3>Catálogo de equipos (Montaje)</h3>
            {forms_equipos}
            <datalist id="productos-odoo-equipos">{opciones_productos_equipos}</datalist>
            <table>
                <tr><th>Nombre</th><th>Tarifa día ($)</th><th>Producto en Odoo (pedido día 0)</th><th>Acciones</th></tr>
                {filas_equipos}
            </table>
            <form method="post" action="/modulo/presupuestos/configuracion/equipos" style="margin-top:12px;">
                <div class="grid2">
                    <div>
                        <label>Nombre nuevo equipo</label>
                        <input type="text" name="nombre" required>
                    </div>
                    <div>
                        <label>Tarifa día ($)</label>
                        <input type="number" step="0.01" name="tarifa_dia_default" value="0">
                    </div>
                </div>
                <div>
                    <label>Producto en Odoo (opcional)</label>
                    <input type="text" name="producto_odoo" list="productos-odoo-equipos">
                </div>
                <button type="submit" class="btn">+ Agregar equipo</button>
            </form>
        </div>

        <div class="card">
            <h3>Catálogo de esquemas de pintura</h3>
            {forms_esquemas}
            <table>
                <tr><th>Nombre</th><th>Precio por m² ($)</th><th>Acciones</th></tr>
                {filas_esquemas}
            </table>
            <form method="post" action="/modulo/presupuestos/configuracion/esquemas" style="margin-top:12px;">
                <div class="grid2">
                    <div>
                        <label>Nombre nuevo esquema</label>
                        <input type="text" name="nombre" required>
                    </div>
                    <div>
                        <label>Precio por m² ($)</label>
                        <input type="number" step="0.01" name="precio_unitario_m2_default" value="0">
                    </div>
                </div>
                <button type="submit" class="btn">+ Agregar esquema</button>
            </form>
        </div>
    </div>
    </body>
    </html>
    """


# ─────────────────────────────────────────────────────────────────
# ABM catalogo_equipos
# ─────────────────────────────────────────────────────────────────

@presupuestos_bp.route("/configuracion/equipos", methods=["POST"])
def vista_crear_equipo():
    db = _db()
    nombre = (request.form.get("nombre") or "").strip()
    if nombre:
        crear_equipo(
            db,
            nombre=nombre,
            tarifa_dia_default=_num(request.form.get("tarifa_dia_default")),
            producto_odoo=_texto_o_none(request.form.get("producto_odoo")),
        )
    return redirect("/modulo/presupuestos/configuracion")


@presupuestos_bp.route("/configuracion/equipos/<int:equipo_id>/editar", methods=["POST"])
def vista_editar_equipo(equipo_id):
    db = _db()
    nombre = (request.form.get("nombre") or "").strip()
    if nombre:
        actualizar_equipo(
            db,
            equipo_id,
            nombre=nombre,
            tarifa_dia_default=_num(request.form.get("tarifa_dia_default")),
            producto_odoo=_texto_o_none(request.form.get("producto_odoo")),
        )
    return redirect("/modulo/presupuestos/configuracion")


@presupuestos_bp.route("/configuracion/equipos/<int:equipo_id>/eliminar", methods=["POST"])
def vista_eliminar_equipo(equipo_id):
    db = _db()
    eliminar_equipo(db, equipo_id)
    return redirect("/modulo/presupuestos/configuracion")


# ─────────────────────────────────────────────────────────────────
# ABM catalogo_esquemas_pintura
# ─────────────────────────────────────────────────────────────────

@presupuestos_bp.route("/configuracion/esquemas", methods=["POST"])
def vista_crear_esquema():
    db = _db()
    nombre = (request.form.get("nombre") or "").strip()
    if nombre:
        crear_esquema_pintura(
            db, nombre=nombre, precio_unitario_m2_default=_num(request.form.get("precio_unitario_m2_default"))
        )
    return redirect("/modulo/presupuestos/configuracion")


@presupuestos_bp.route("/configuracion/esquemas/<int:esquema_id>/editar", methods=["POST"])
def vista_editar_esquema(esquema_id):
    db = _db()
    nombre = (request.form.get("nombre") or "").strip()
    if nombre:
        actualizar_esquema_pintura(
            db, esquema_id, nombre=nombre, precio_unitario_m2_default=_num(request.form.get("precio_unitario_m2_default"))
        )
    return redirect("/modulo/presupuestos/configuracion")


@presupuestos_bp.route("/configuracion/esquemas/<int:esquema_id>/eliminar", methods=["POST"])
def vista_eliminar_esquema(esquema_id):
    db = _db()
    eliminar_esquema_pintura(db, esquema_id)
    return redirect("/modulo/presupuestos/configuracion")
