"""Rutas (API JSON) del módulo Presupuestos.

Fuente: ESPECIFICACION_MODULO_PRESUPUESTOS_V1.md, sección 2.1 (endpoints) y
sección 2.2 (rubros válidos por sección). Estilo de rutas/respuestas alineado
con el resto del proyecto (ver ejemplos en calidad_routes.py y
remito_routes.py: jsonify({"error": ...}), <status>).

Todavía sin pantallas: solo backend. La lógica de cálculo vive en
calculo_presupuesto.py y acá solo se invoca, nunca se reescribe.
"""
from flask import request, jsonify

from db_utils import get_db
from catalogo_materiales import sincronizar_catalogo_materiales

from . import presupuestos_bp
from .constants import ESTADOS_PRESUPUESTO, TIPOS_SECCION, rubro_valido
from .calculo_presupuesto import (
    calcular_tarea,
    calcular_presupuesto,
    calcular_resumen_categorias,
    calcular_peso_total_kg,
    calcular_indicador_costo_kg,
    calcular_costo_estructura,
    calcular_indicador_mano_obra_consumibles,
    calcular_kg_por_hh_presupuesto,
    calcular_m2_total_presupuesto,
    calcular_m2_por_dia_montaje,
    calcular_usd_por_m2_total,
    calcular_indicador_kg_tarea,
    calcular_indicador_m2_tarea,
    calcular_resumen_recursos,
)
from .models import (
    ensure_tablas_presupuestos,
    crear_presupuesto,
    obtener_presupuesto,
    listar_presupuestos,
    actualizar_presupuesto,
    obtener_config_presupuesto,
    CONFIG_CAMPOS_PRESUPUESTO,
    actualizar_estado_presupuesto,
    eliminar_presupuesto,
    crear_tarea,
    obtener_tarea,
    listar_tareas,
    eliminar_tarea,
    crear_secciones_tarea,
    obtener_seccion,
    listar_secciones_tarea,
    actualizar_porcentajes_seccion,
    eliminar_seccion,
    crear_item_costo,
    obtener_item_costo,
    listar_items_costo,
    actualizar_item_costo,
    eliminar_item_costo,
    obtener_config,
    listar_equipos,
    listar_esquemas_pintura,
)


def _db():
    """get_db() + migración idempotente, igual que `_ensure_tables` en el resto del proyecto."""
    db = get_db()
    ensure_tablas_presupuestos(db)
    sincronizar_catalogo_materiales(db)
    return db


def _body():
    return request.get_json(silent=True) or {}


def _obtener_perfiles_por_id(db, perfil_ids):
    """Resuelve {perfil_id: {kg_m, m2_m}} desde articulos_sum (Compras).
    Sin FK dura entre módulos: si la tabla no existe todavía, devuelve {}."""
    ids = sorted({int(pid) for pid in perfil_ids if pid})
    if not ids:
        return {}
    placeholders = ",".join("?" for _ in ids)
    try:
        rows = db.execute(
            f"""
            SELECT id, COALESCE(kg_per_m, 0), COALESCE(m2_per_m, 0)
            FROM articulos_sum WHERE id IN ({placeholders})
            """,
            tuple(ids),
        ).fetchall()
    except Exception:
        return {}
    return {r[0]: {"kg_m": r[1], "m2_m": r[2]} for r in rows}


def _items_para_motor(items_db):
    """Convierte items_costo (fila de DB) al formato que espera calculo_presupuesto."""
    return [{"rubro": it["rubro"], "tipo_item": it["tipo_item"], "datos": it["datos"]} for it in items_db]


# ─────────────────────────────────────────────────────────────────
# Catálogos para precarga en la pantalla de carga de items (sección 2.1):
# config_presupuestos, catalogo_equipos, catalogo_esquemas_pintura y
# articulos_sum (perfiles de Compras) para el combo de materiales/perfil.
# ─────────────────────────────────────────────────────────────────

@presupuestos_bp.route("/api/config", methods=["GET"])
def api_obtener_config():
    try:
        db = _db()
        return jsonify(obtener_config(db)), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/presupuestos/<int:presupuesto_id>/config", methods=["GET"])
def api_obtener_config_presupuesto(presupuesto_id):
    try:
        db = _db()
        config = obtener_config_presupuesto(db, presupuesto_id)
        if config is None:
            return jsonify({"error": "Presupuesto no encontrado"}), 404
        return jsonify(config), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/equipos", methods=["GET"])
def api_listar_equipos():
    try:
        db = _db()
        return jsonify({"equipos": listar_equipos(db)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/esquemas-pintura", methods=["GET"])
def api_listar_esquemas_pintura():
    try:
        db = _db()
        return jsonify({"esquemas": listar_esquemas_pintura(db)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/perfiles", methods=["GET"])
def api_listar_perfiles():
    """Lista del catalogo compartido con Compras para el combo de materiales."""
    try:
        db = _db()
        rows = db.execute(
            """
            SELECT id, COALESCE(descripcion, ''), COALESCE(categoria, ''), COALESCE(kg_per_m, 0)
            FROM articulos_sum
            WHERE COALESCE(activo, 1) = 1
            ORDER BY COALESCE(descripcion, '')
            """
        ).fetchall()
        perfiles = [
            {
                "id": r[0],
                "label": r[1] or f"Perfil #{r[0]}",
                "categoria": r[2] or "",
                "kg_m": r[3] or 0,
            }
            for r in rows
        ]
        return jsonify({"perfiles": perfiles}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────
# presupuestos
# ─────────────────────────────────────────────────────────────────

@presupuestos_bp.route("/api/presupuestos", methods=["GET"])
def api_listar_presupuestos():
    try:
        db = _db()
        return jsonify({"presupuestos": listar_presupuestos(db)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/presupuestos", methods=["POST"])
def api_crear_presupuesto():
    try:
        data = _body()
        cliente = str(data.get("cliente") or "").strip()
        if not cliente:
            return jsonify({"error": "El cliente es obligatorio."}), 400

        db = _db()
        presupuesto_id = crear_presupuesto(
            db,
            cliente=cliente,
            planta=str(data.get("planta") or "").strip(),
            titulo=str(data.get("titulo") or "").strip(),
            fecha=data.get("fecha"),
            tipo_cambio_referencia=data.get("tipo_cambio_referencia"),
            config_presupuesto={
                campo: data[campo] for campo in CONFIG_CAMPOS_PRESUPUESTO if campo in data
            },
        )
        return jsonify({"presupuesto": obtener_presupuesto(db, presupuesto_id)}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/presupuestos/<int:presupuesto_id>", methods=["GET"])
def api_obtener_presupuesto(presupuesto_id):
    try:
        db = _db()
        presupuesto = obtener_presupuesto(db, presupuesto_id)
        if not presupuesto:
            return jsonify({"error": "Presupuesto no encontrado"}), 404

        tareas = []
        for tarea in listar_tareas(db, presupuesto_id):
            tarea_out = dict(tarea)
            tarea_out["secciones"] = listar_secciones_tarea(db, tarea["id"])
            tareas.append(tarea_out)

        presupuesto_out = dict(presupuesto)
        presupuesto_out["tareas"] = tareas
        return jsonify({"presupuesto": presupuesto_out}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/presupuestos/<int:presupuesto_id>", methods=["PUT"])
def api_actualizar_presupuesto(presupuesto_id):
    try:
        db = _db()
        if not obtener_presupuesto(db, presupuesto_id):
            return jsonify({"error": "Presupuesto no encontrado"}), 404

        data = _body()
        actualizar_presupuesto(
            db,
            presupuesto_id,
            cliente=data.get("cliente"),
            planta=data.get("planta"),
            titulo=data.get("titulo"),
            fecha=data.get("fecha"),
            tipo_cambio_referencia=data.get("tipo_cambio_referencia"),
            numero_presupuesto=data.get("numero_presupuesto"),
            config_presupuesto={
                campo: data[campo] for campo in CONFIG_CAMPOS_PRESUPUESTO if campo in data
            },
        )
        return jsonify({"presupuesto": obtener_presupuesto(db, presupuesto_id)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/presupuestos/<int:presupuesto_id>/estado", methods=["POST"])
def api_cambiar_estado_presupuesto(presupuesto_id):
    try:
        db = _db()
        if not obtener_presupuesto(db, presupuesto_id):
            return jsonify({"error": "Presupuesto no encontrado"}), 404

        data = _body()
        estado = str(data.get("estado") or "").strip().lower()
        if estado not in ESTADOS_PRESUPUESTO:
            return jsonify({"error": f"Estado inválido. Debe ser uno de: {', '.join(ESTADOS_PRESUPUESTO)}"}), 400

        actualizar_estado_presupuesto(
            db,
            presupuesto_id,
            estado,
            fecha_adjudicacion=data.get("fecha_adjudicacion"),
            ot_id=data.get("ot_id"),
        )
        return jsonify({"presupuesto": obtener_presupuesto(db, presupuesto_id)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/presupuestos/<int:presupuesto_id>", methods=["DELETE"])
def api_eliminar_presupuesto(presupuesto_id):
    try:
        db = _db()
        if not obtener_presupuesto(db, presupuesto_id):
            return jsonify({"error": "Presupuesto no encontrado"}), 404
        eliminar_presupuesto(db, presupuesto_id)
        return jsonify({"ok": True}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────
# tareas
# ─────────────────────────────────────────────────────────────────

@presupuestos_bp.route("/api/presupuestos/<int:presupuesto_id>/tareas", methods=["GET"])
def api_listar_tareas(presupuesto_id):
    try:
        db = _db()
        if not obtener_presupuesto(db, presupuesto_id):
            return jsonify({"error": "Presupuesto no encontrado"}), 404

        tareas = []
        for tarea in listar_tareas(db, presupuesto_id):
            tarea_out = dict(tarea)
            tarea_out["secciones"] = listar_secciones_tarea(db, tarea["id"])
            tareas.append(tarea_out)
        return jsonify({"tareas": tareas}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/presupuestos/<int:presupuesto_id>/tareas", methods=["POST"])
def api_crear_tarea(presupuesto_id):
    try:
        db = _db()
        if not obtener_presupuesto(db, presupuesto_id):
            return jsonify({"error": "Presupuesto no encontrado"}), 404

        data = _body()
        nombre = str(data.get("nombre") or "").strip()
        if not nombre:
            return jsonify({"error": "El nombre de la tarea es obligatorio."}), 400
        tipo_tarea = str(data.get("tipo") or "").strip() or nombre

        tipos_in = data.get("tipos")
        tipos = list(TIPOS_SECCION) if tipos_in is None else tipos_in
        tipos = [str(t).strip().upper() for t in tipos]
        if not tipos or any(t not in TIPOS_SECCION for t in tipos):
            return jsonify({"error": f"tipos debe ser un subconjunto no vacío de {TIPOS_SECCION}"}), 400

        orden = int(data.get("orden") or 0)
        tarea_id = crear_tarea(db, presupuesto_id, nombre, orden=orden, tipo=tipo_tarea)
        try:
            crear_secciones_tarea(
                db, tarea_id, obtener_config_presupuesto(db, presupuesto_id),
                tipos=tuple(tipos), nombre_tarea=tipo_tarea,
            )
        except ValueError as ve:
            eliminar_tarea(db, tarea_id)
            return jsonify({"error": str(ve)}), 400

        tarea_out = dict(obtener_tarea(db, tarea_id))
        tarea_out["secciones"] = listar_secciones_tarea(db, tarea_id)
        return jsonify({"tarea": tarea_out}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/tareas/<int:tarea_id>", methods=["GET"])
def api_obtener_tarea(tarea_id):
    try:
        db = _db()
        tarea = obtener_tarea(db, tarea_id)
        if not tarea:
            return jsonify({"error": "Tarea no encontrada"}), 404

        tarea_out = dict(tarea)
        secciones = []
        for seccion in listar_secciones_tarea(db, tarea_id):
            seccion_out = dict(seccion)
            seccion_out["items"] = listar_items_costo(db, seccion["id"])
            secciones.append(seccion_out)
        tarea_out["secciones"] = secciones
        return jsonify({"tarea": tarea_out}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/tareas/<int:tarea_id>", methods=["PUT"])
def api_actualizar_tarea(tarea_id):
    try:
        db = _db()
        tarea = obtener_tarea(db, tarea_id)
        if not tarea:
            return jsonify({"error": "Tarea no encontrada"}), 404

        data = _body()
        nombre = str(data.get("nombre") or tarea["nombre"]).strip()
        orden = int(data.get("orden") if data.get("orden") is not None else tarea["orden"])
        db.execute("UPDATE tareas SET nombre = ?, orden = ? WHERE id = ?", (nombre, orden, tarea_id))
        db.commit()
        return jsonify({"tarea": obtener_tarea(db, tarea_id)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/tareas/<int:tarea_id>", methods=["DELETE"])
def api_eliminar_tarea(tarea_id):
    try:
        db = _db()
        if not obtener_tarea(db, tarea_id):
            return jsonify({"error": "Tarea no encontrada"}), 404
        eliminar_tarea(db, tarea_id)
        return jsonify({"ok": True}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/tareas/<int:tarea_id>/secciones", methods=["POST"])
def api_agregar_seccion_tarea(tarea_id):
    """Agrega la sección faltante (FABRICACION o MONTAJE) a una tarea ya
    existente (sección 2.1: se puede completar más adelante si hace falta)."""
    try:
        db = _db()
        tarea = obtener_tarea(db, tarea_id)
        if not tarea:
            return jsonify({"error": "Tarea no encontrada"}), 404

        data = _body()
        tipo = str(data.get("tipo") or "").strip().upper()
        if tipo not in TIPOS_SECCION:
            return jsonify({"error": f"tipo debe ser uno de {TIPOS_SECCION}"}), 400

        try:
            crear_secciones_tarea(
                db, tarea_id, obtener_config_presupuesto(db, tarea["presupuesto_id"]),
                tipos=(tipo,), nombre_tarea=(tarea.get("tipo") or tarea["nombre"]),
            )
        except ValueError as ve:
            return jsonify({"error": str(ve)}), 400

        tarea_out = dict(obtener_tarea(db, tarea_id))
        tarea_out["secciones"] = listar_secciones_tarea(db, tarea_id)
        return jsonify({"tarea": tarea_out}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────
# tarea_secciones (solo edición de % — la creación va atada a crear_tarea)
# ─────────────────────────────────────────────────────────────────

@presupuestos_bp.route("/api/secciones/<int:seccion_id>", methods=["PUT"])
def api_actualizar_seccion(seccion_id):
    try:
        db = _db()
        seccion = obtener_seccion(db, seccion_id)
        if not seccion:
            return jsonify({"error": "Sección no encontrada"}), 404

        data = _body()
        gg_pct = float(data.get("gg_pct") if data.get("gg_pct") is not None else seccion["gg_pct"] or 0)
        beneficio_pct = float(data.get("beneficio_pct") if data.get("beneficio_pct") is not None else seccion["beneficio_pct"] or 0)
        imp_pct = float(data.get("imp_pct") if data.get("imp_pct") is not None else seccion["imp_pct"] or 0)
        actualizar_porcentajes_seccion(db, seccion_id, gg_pct, beneficio_pct, imp_pct)
        return jsonify({"seccion": obtener_seccion(db, seccion_id)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/secciones/<int:seccion_id>", methods=["DELETE"])
def api_eliminar_seccion(seccion_id):
    try:
        db = _db()
        if not obtener_seccion(db, seccion_id):
            return jsonify({"error": "Sección no encontrada"}), 404
        eliminar_seccion(db, seccion_id)
        return jsonify({"ok": True}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────
# items_costo
# ─────────────────────────────────────────────────────────────────

@presupuestos_bp.route("/api/secciones/<int:seccion_id>/items", methods=["GET"])
def api_listar_items(seccion_id):
    try:
        db = _db()
        if not obtener_seccion(db, seccion_id):
            return jsonify({"error": "Sección no encontrada"}), 404
        return jsonify({"items": listar_items_costo(db, seccion_id)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/secciones/<int:seccion_id>/items", methods=["POST"])
def api_crear_item(seccion_id):
    try:
        db = _db()
        seccion = obtener_seccion(db, seccion_id)
        if not seccion:
            return jsonify({"error": "Sección no encontrada"}), 404

        data = _body()
        rubro = str(data.get("rubro") or "").strip()
        if not rubro_valido(seccion["tipo"], rubro):
            return jsonify({"error": f"Rubro {rubro!r} no válido para sección {seccion['tipo']}."}), 400

        datos = data.get("datos")
        if not isinstance(datos, dict):
            return jsonify({"error": "datos debe ser un objeto JSON."}), 400

        tipo_item = data.get("tipo_item")
        perfil_id = data.get("perfil_id")
        if perfil_id is None and rubro == "materiales" and tipo_item in ("perfil", "chapa", "grating"):
            # La columna indexada perfil_id es para resolver kg_m/m2_m desde
            # Suministros; si no vino explícita, se deriva de datos.perfil_id
            # (que es lo que realmente usa el motor de cálculo).
            perfil_id = datos.get("perfil_id")

        item_id = crear_item_costo(
            db,
            seccion_id,
            rubro,
            datos,
            tipo_item=tipo_item,
            perfil_id=perfil_id,
            subtotal=data.get("subtotal") or 0,
        )
        return jsonify({"item": obtener_item_costo(db, item_id)}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/items/<int:item_id>", methods=["PUT"])
def api_actualizar_item(item_id):
    try:
        db = _db()
        item = obtener_item_costo(db, item_id)
        if not item:
            return jsonify({"error": "Item no encontrado"}), 404

        data = _body()
        if "rubro" in data or "datos" in data:
            seccion = obtener_seccion(db, item["tarea_seccion_id"])
            rubro_nuevo = str(data.get("rubro") or item["rubro"]).strip()
            if not rubro_valido(seccion["tipo"], rubro_nuevo):
                return jsonify({"error": f"Rubro {rubro_nuevo!r} no válido para sección {seccion['tipo']}."}), 400
            if rubro_nuevo != item["rubro"]:
                db.execute("UPDATE items_costo SET rubro = ? WHERE id = ?", (rubro_nuevo, item_id))
                db.commit()

        datos = data.get("datos") if isinstance(data.get("datos"), dict) else None
        perfil_id = data.get("perfil_id")
        rubro_actual = data.get("rubro") or item["rubro"]
        tipo_item_actual = data.get("tipo_item") or item["tipo_item"]
        if perfil_id is None and datos is not None and rubro_actual == "materiales" and tipo_item_actual in ("perfil", "chapa", "grating"):
            perfil_id = datos.get("perfil_id")

        actualizar_item_costo(
            db,
            item_id,
            datos=datos,
            subtotal=data.get("subtotal"),
            perfil_id=perfil_id,
        )
        return jsonify({"item": obtener_item_costo(db, item_id)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/items/<int:item_id>", methods=["DELETE"])
def api_eliminar_item(item_id):
    try:
        db = _db()
        if not obtener_item_costo(db, item_id):
            return jsonify({"error": "Item no encontrado"}), 404
        eliminar_item_costo(db, item_id)
        return jsonify({"ok": True}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────
# recálculo (usa calculo_presupuesto.py — acá no se reescribe la fórmula)
# ─────────────────────────────────────────────────────────────────

def _calcular_resultado_tarea(db, tarea_id):
    """Arma fabricacion_items/montaje_items/pcts desde la DB y llama a calcular_tarea."""
    tarea = obtener_tarea(db, tarea_id)
    presupuesto = obtener_presupuesto(db, tarea["presupuesto_id"]) if tarea else None
    tipo_cambio_referencia = presupuesto["tipo_cambio_referencia"] if presupuesto else None

    secciones = listar_secciones_tarea(db, tarea_id)
    secciones_por_tipo = {s["tipo"]: s for s in secciones}

    fabricacion_items, fabricacion_pcts = [], {"gg_pct": 0, "beneficio_pct": 0, "imp_pct": 0}
    montaje_items, montaje_pcts = [], {"gg_pct": 0, "beneficio_pct": 0, "imp_pct": 0}
    perfil_ids = []

    if "FABRICACION" in secciones_por_tipo:
        seccion = secciones_por_tipo["FABRICACION"]
        items_db = listar_items_costo(db, seccion["id"])
        # Fallback a datos.perfil_id por si el item quedó sin la columna
        # indexada completada (ver fix en api_crear_item/api_actualizar_item).
        perfil_ids += [
            it["perfil_id"] or (it["datos"] or {}).get("perfil_id")
            for it in items_db
            if it["perfil_id"] or (it["datos"] or {}).get("perfil_id")
        ]
        fabricacion_items = _items_para_motor(items_db)
        fabricacion_pcts = {
            "gg_pct": seccion["gg_pct"] or 0,
            "beneficio_pct": seccion["beneficio_pct"] or 0,
            "imp_pct": seccion["imp_pct"] or 0,
        }

    if "MONTAJE" in secciones_por_tipo:
        seccion = secciones_por_tipo["MONTAJE"]
        montaje_items = _items_para_motor(listar_items_costo(db, seccion["id"]))
        montaje_pcts = {
            "gg_pct": seccion["gg_pct"] or 0,
            "beneficio_pct": seccion["beneficio_pct"] or 0,
            "imp_pct": seccion["imp_pct"] or 0,
        }

    perfiles_por_id = _obtener_perfiles_por_id(db, perfil_ids)
    resultado = calcular_tarea(
        fabricacion_items, fabricacion_pcts, montaje_items, montaje_pcts, perfiles_por_id,
        tipo_cambio_referencia=tipo_cambio_referencia,
    )
    resultado["fabricacion"]["indicador_mano_obra"] = calcular_indicador_mano_obra_consumibles(
        resultado["fabricacion"]["items"], tipo_cambio_referencia
    )
    return resultado


@presupuestos_bp.route("/api/tareas/<int:tarea_id>/recalcular", methods=["GET"])
def api_recalcular_tarea(tarea_id):
    try:
        db = _db()
        if not obtener_tarea(db, tarea_id):
            return jsonify({"error": "Tarea no encontrada"}), 404
        return jsonify({"resultado": _calcular_resultado_tarea(db, tarea_id)}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/tareas/<int:tarea_id>/indicador", methods=["GET"])
def api_indicador_tarea(tarea_id):
    """Indicadores propios de la tarea (no cruzan con el resto del presupuesto):
    KG/HH + USD/kg (modo perfil) y m2/día + USD/m2 (modo chapa/grating)."""
    try:
        db = _db()
        tarea = obtener_tarea(db, tarea_id)
        if not tarea:
            return jsonify({"error": "Tarea no encontrada"}), 404
        presupuesto = obtener_presupuesto(db, tarea["presupuesto_id"])
        tipo_cambio_referencia = presupuesto["tipo_cambio_referencia"] if presupuesto else None

        resultado = _calcular_resultado_tarea(db, tarea_id)
        indicador = calcular_indicador_kg_tarea(resultado, tipo_cambio_referencia)
        indicador_m2 = calcular_indicador_m2_tarea(resultado, tipo_cambio_referencia)
        indicador["m2_por_dia_montaje"] = indicador_m2["m2_por_dia_montaje"]
        indicador["usd_por_m2_total"] = indicador_m2["usd_por_m2_total"]
        indicador["tipo_cambio_referencia"] = tipo_cambio_referencia
        return jsonify(indicador), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/presupuestos/<int:presupuesto_id>/recalcular", methods=["GET"])
def api_recalcular_presupuesto(presupuesto_id):
    try:
        db = _db()
        if not obtener_presupuesto(db, presupuesto_id):
            return jsonify({"error": "Presupuesto no encontrado"}), 404

        resultados_por_tarea = []
        for tarea in listar_tareas(db, presupuesto_id):
            resultado = _calcular_resultado_tarea(db, tarea["id"])
            resultados_por_tarea.append({
                "tarea_id": tarea["id"],
                "nombre": tarea["nombre"],
                "resultado": resultado,
            })

        totales = calcular_presupuesto([r["resultado"] for r in resultados_por_tarea])
        return jsonify({"tareas": resultados_por_tarea, "totales": totales}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─────────────────────────────────────────────────────────────────
# reportes (sección 5) — cruzan todas las tareas del presupuesto.
# La lógica de agregación vive en calculo_presupuesto.py; acá solo se arma
# el input desde la DB y se resuelven nombres de catálogos para mostrar.
# ─────────────────────────────────────────────────────────────────

def _todos_los_resultados_tarea(db, presupuesto_id):
    return [_calcular_resultado_tarea(db, t["id"]) for t in listar_tareas(db, presupuesto_id)]


def _resumen_recursos_con_nombres(db, presupuesto_id):
    resultados = _todos_los_resultados_tarea(db, presupuesto_id)
    resumen = calcular_resumen_recursos(resultados)

    perfil_ids = [m["perfil_id"] for m in resumen["materiales"] if m["perfil_id"]]
    nombres_perfil = {}
    if perfil_ids:
        placeholders = ",".join("?" for _ in perfil_ids)
        try:
            rows = db.execute(
                f"SELECT id, COALESCE(descripcion, ''), COALESCE(codigo, '') FROM articulos_sum WHERE id IN ({placeholders})",
                tuple(perfil_ids),
            ).fetchall()
            nombres_perfil = {r[0]: (r[1] or r[2]) for r in rows}
        except Exception:
            nombres_perfil = {}
    for m in resumen["materiales"]:
        m["nombre"] = nombres_perfil.get(m["perfil_id"]) or (
            f"Perfil #{m['perfil_id']}" if m["perfil_id"] else "(sin perfil asociado)"
        )

    equipo_ids = [e["equipo_id"] for e in resumen["equipos"] if e["equipo_id"]]
    nombres_equipo = {}
    if equipo_ids:
        placeholders = ",".join("?" for _ in equipo_ids)
        try:
            rows = db.execute(
                f"SELECT id, nombre FROM catalogo_equipos WHERE id IN ({placeholders})",
                tuple(equipo_ids),
            ).fetchall()
            nombres_equipo = {r[0]: r[1] for r in rows}
        except Exception:
            nombres_equipo = {}
    for e in resumen["equipos"]:
        e["nombre"] = nombres_equipo.get(e["equipo_id"]) or (
            f"Equipo #{e['equipo_id']}" if e["equipo_id"] else "(sin equipo asociado)"
        )

    return resumen


@presupuestos_bp.route("/api/presupuestos/<int:presupuesto_id>/resumen", methods=["GET"])
def api_resumen_presupuesto(presupuesto_id):
    """Sección 4.4: todas las tareas, total FAB + MON por tarea y gran total."""
    try:
        db = _db()
        if not obtener_presupuesto(db, presupuesto_id):
            return jsonify({"error": "Presupuesto no encontrado"}), 404

        tareas_out = []
        total_fab = total_mon = 0.0
        for tarea in listar_tareas(db, presupuesto_id):
            resultado = _calcular_resultado_tarea(db, tarea["id"])
            precio_fab = resultado["fabricacion"]["cascada"]["precio_venta"]
            precio_mon = resultado["montaje"]["cascada"]["precio_venta"]
            total_fab += precio_fab
            total_mon += precio_mon
            tareas_out.append({
                "tarea_id": tarea["id"],
                "nombre": tarea["nombre"],
                "precio_venta_fabricacion": precio_fab,
                "precio_venta_montaje": precio_mon,
                "precio_venta_tarea": resultado["precio_venta_tarea"],
            })

        return jsonify({
            "tareas": tareas_out,
            "total_fabricacion": total_fab,
            "total_montaje": total_mon,
            "total_presupuesto": total_fab + total_mon,
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/presupuestos/<int:presupuesto_id>/reporte-categorias", methods=["GET"])
def api_reporte_categorias(presupuesto_id):
    """Sección 5.1: reporte cruzado por categoría (hoja "Resumen" del Excel)."""
    try:
        db = _db()
        if not obtener_presupuesto(db, presupuesto_id):
            return jsonify({"error": "Presupuesto no encontrado"}), 404
        resultados = _todos_los_resultados_tarea(db, presupuesto_id)
        return jsonify(calcular_resumen_categorias(resultados)), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/presupuestos/<int:presupuesto_id>/indicador-kg", methods=["GET"])
def api_indicador_kg(presupuesto_id):
    """Sección 3.5: indicador de costo en $/kg y USD/kg."""
    try:
        db = _db()
        presupuesto = obtener_presupuesto(db, presupuesto_id)
        if not presupuesto:
            return jsonify({"error": "Presupuesto no encontrado"}), 404

        resultados = _todos_los_resultados_tarea(db, presupuesto_id)
        totales = calcular_presupuesto(resultados)
        peso_total_kg = calcular_peso_total_kg(resultados)
        costo_estructura = calcular_costo_estructura(resultados)
        indicador = calcular_indicador_costo_kg(
            costo_estructura, peso_total_kg, presupuesto["tipo_cambio_referencia"]
        )
        indicador["kg_por_hh"] = calcular_kg_por_hh_presupuesto(resultados)
        m2_total = calcular_m2_total_presupuesto(resultados)
        indicador["m2_por_dia_montaje"] = calcular_m2_por_dia_montaje(resultados)
        indicador["usd_por_m2_total"] = calcular_usd_por_m2_total(
            totales["precio_venta_presupuesto"], m2_total, presupuesto["tipo_cambio_referencia"]
        )
        indicador["precio_venta_presupuesto"] = totales["precio_venta_presupuesto"]
        indicador["tipo_cambio_referencia"] = presupuesto["tipo_cambio_referencia"]
        return jsonify(indicador), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@presupuestos_bp.route("/api/presupuestos/<int:presupuesto_id>/resumen-recursos", methods=["GET"])
def api_resumen_recursos(presupuesto_id):
    """Sección 5.3: resumen de recursos a obra (mano de obra, equipos, materiales)."""
    try:
        db = _db()
        if not obtener_presupuesto(db, presupuesto_id):
            return jsonify({"error": "Presupuesto no encontrado"}), 404
        return jsonify(_resumen_recursos_con_nombres(db, presupuesto_id)), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
