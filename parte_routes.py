import os
import json
import html as html_lib
from io import BytesIO
from datetime import datetime
from urllib.parse import quote
from flask import Blueprint, redirect, request, send_file
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Image, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from db_utils import (
    get_db,
    is_integrity_error,
    _resolver_imagen_firma_empleado as _db_resolver_imagen_firma_empleado,
)

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
_FIRMAS_EMPLEADOS_DIR = os.path.join(_APP_DIR, "Firmas empleados")


def _resolver_imagen_firma_empleado(nombre, firma_electronica):
    return _db_resolver_imagen_firma_empleado(nombre, firma_electronica, _FIRMAS_EMPLEADOS_DIR)


parte_bp = Blueprint("parte", __name__)


PUESTOS_SUPERVISOR = [
    "Of tecnica",
    "Jefe de Taller",
    "Coord Estructuras",
    "Resp. calidad",
    "Mantenimiento",
    "Encargado Pintura",
]

PUESTOS_OPERARIO = [
    "Of. Soldador",
    "Of. Armador",
    "Medio Of.",
    "Ayudante",
    "Of. Pintor",
]


def _normalizar_tipo_puesto(valor):
    tipo = str(valor or "").strip().lower()
    if tipo == "operario":
        return "operario"
    return "supervisor"


def _inferir_tipo_puesto_legacy(puesto_txt):
    txt = str(puesto_txt or "").strip().lower()
    if any(k in txt for k in ["operario", "soldador", "armador", "medio", "ayudante", "pintor"]):
        return "operario"
    return "supervisor"


def _extraer_nombre_apellido_desde_full(nombre_full):
    txt = str(nombre_full or "").strip()
    if not txt:
        return "", ""
    partes = txt.split()
    if len(partes) <= 1:
        return txt, ""
    return partes[0], " ".join(partes[1:])


def _opciones_detalle_html(tipo_actual, detalle_actual):
    detalle_actual = str(detalle_actual or "").strip()
    opciones = PUESTOS_OPERARIO if tipo_actual == "operario" else PUESTOS_SUPERVISOR
    html_opts = '<option value="">-- Seleccionar puesto --</option>'
    for item in opciones:
        selected = "selected" if detalle_actual == item else ""
        html_opts += f'<option value="{html_lib.escape(item)}" {selected}>{html_lib.escape(item)}</option>'
    return html_opts


def _nombre_mostrable(nombre_full, nombre_base="", apellido=""):
    n_base = str(nombre_base or "").strip()
    ape = str(apellido or "").strip()
    if not n_base:
        n_base, ape_guess = _extraer_nombre_apellido_desde_full(nombre_full)
        if not ape:
            ape = ape_guess
    if ape and n_base:
        return f"{ape}, {n_base}"
    return str(nombre_full or "").strip()


@parte_bp.route("/modulo/parte", methods=["GET", "POST"])
def parte_semanal():
    db = get_db()

    if request.method == "POST":
        accion = (request.form.get("accion") or "").strip()

        if accion == "guardar_empleado":
            nombre_base = (request.form.get("empleado_nombre") or "").strip()
            apellido_emp = (request.form.get("empleado_apellido") or "").strip()
            if not apellido_emp and " " in nombre_base:
                n_guess, a_guess = _extraer_nombre_apellido_desde_full(nombre_base)
                nombre_base, apellido_emp = n_guess, a_guess
            tipo_puesto = _normalizar_tipo_puesto(request.form.get("empleado_tipo_puesto"))
            puesto_detalle = (request.form.get("empleado_puesto_detalle") or request.form.get("empleado_puesto") or "").strip()
            firma_ingresada = (request.form.get("empleado_firma") or "").strip()
            firma_emp = "0" if tipo_puesto == "operario" else firma_ingresada
            nombre_emp = " ".join([v for v in [nombre_base, apellido_emp] if v]).strip()
            puesto_emp = puesto_detalle

            if not nombre_base or not apellido_emp or not puesto_detalle:
                return redirect("/modulo/parte?mensaje=" + quote("⚠️ Completá nombre, apellido y puesto"))
            if tipo_puesto == "supervisor" and not firma_emp:
                return redirect("/modulo/parte?mensaje=" + quote("⚠️ Para supervisor la firma electrónica es obligatoria"))

            firma_imagen_rel = ""
            if firma_emp and firma_emp != "0":
                firma_imagen_rel = _resolver_imagen_firma_empleado(nombre_emp, firma_emp)

            existe = db.execute(
                "SELECT id FROM empleados_parte WHERE LOWER(TRIM(nombre)) = LOWER(TRIM(?))",
                (nombre_emp,)
            ).fetchone()

            if existe:
                db.execute(
                    """
                    UPDATE empleados_parte
                    SET nombre=?, nombre_base=?, apellido=?, puesto=?, puesto_tipo=?, puesto_detalle=?, firma_electronica=?, firma_imagen_path=?, fecha_actualizacion=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (nombre_emp, nombre_base, apellido_emp, puesto_emp, tipo_puesto, puesto_detalle, firma_emp, firma_imagen_rel, existe[0])
                )
                mensaje = "✅ Empleado actualizado"
            else:
                try:
                    db.execute(
                        """
                        INSERT INTO empleados_parte (nombre, nombre_base, apellido, puesto, puesto_tipo, puesto_detalle, firma_electronica, firma_imagen_path)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (nombre_emp, nombre_base, apellido_emp, puesto_emp, tipo_puesto, puesto_detalle, firma_emp, firma_imagen_rel)
                    )
                except Exception as exc:
                    if not is_integrity_error(exc):
                        raise
                    return redirect("/modulo/parte?mensaje=" + quote("⚠️ Ya existe un empleado con ese nombre"))
                mensaje = "✅ Empleado cargado"

            db.commit()
            return redirect("/modulo/parte?mensaje=" + quote(mensaje))

        if accion == "editar_empleado":
            empleado_id = (request.form.get("empleado_id") or "").strip()
            nombre_base = (request.form.get("empleado_nombre") or "").strip()
            apellido_emp = (request.form.get("empleado_apellido") or "").strip()
            if not apellido_emp and " " in nombre_base:
                n_guess, a_guess = _extraer_nombre_apellido_desde_full(nombre_base)
                nombre_base, apellido_emp = n_guess, a_guess
            tipo_puesto = _normalizar_tipo_puesto(request.form.get("empleado_tipo_puesto"))
            puesto_detalle = (request.form.get("empleado_puesto_detalle") or request.form.get("empleado_puesto") or "").strip()
            firma_ingresada = (request.form.get("empleado_firma") or "").strip()
            firma_emp = "0" if tipo_puesto == "operario" else firma_ingresada
            nombre_emp = " ".join([v for v in [nombre_base, apellido_emp] if v]).strip()
            puesto_emp = puesto_detalle

            if not empleado_id.isdigit():
                return redirect("/modulo/parte?mensaje=" + quote("⚠️ Empleado inválido"))
            if not nombre_base or not apellido_emp or not puesto_detalle:
                return redirect("/modulo/parte?mensaje=" + quote("⚠️ Completá nombre, apellido y puesto"))
            if tipo_puesto == "supervisor" and not firma_emp:
                return redirect("/modulo/parte?mensaje=" + quote("⚠️ Para supervisor la firma electrónica es obligatoria"))

            firma_imagen_rel = ""
            if firma_emp and firma_emp != "0":
                firma_imagen_rel = _resolver_imagen_firma_empleado(nombre_emp, firma_emp)

            try:
                db.execute(
                    """
                    UPDATE empleados_parte
                    SET nombre=?, nombre_base=?, apellido=?, puesto=?, puesto_tipo=?, puesto_detalle=?, firma_electronica=?, firma_imagen_path=?, fecha_actualizacion=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (nombre_emp, nombre_base, apellido_emp, puesto_emp, tipo_puesto, puesto_detalle, firma_emp, firma_imagen_rel, int(empleado_id))
                )
            except Exception as exc:
                if not is_integrity_error(exc):
                    raise
                return redirect("/modulo/parte?mensaje=" + quote("⚠️ Ya existe un empleado con ese nombre"))

            db.commit()
            return redirect("/modulo/parte?mensaje=" + quote("✅ Empleado editado"))

        if accion == "eliminar_empleado":
            empleado_id = (request.form.get("empleado_id") or "").strip()
            if not empleado_id.isdigit():
                return redirect("/modulo/parte?mensaje=" + quote("⚠️ Empleado inválido"))

            db.execute("DELETE FROM empleados_parte WHERE id=?", (int(empleado_id),))
            db.commit()
            return redirect("/modulo/parte?mensaje=" + quote("✅ Empleado eliminado"))

        semana_inicio = request.form.get("semana_inicio")
        empleados_json = request.form.get("empleados_json", "[]")

        if not semana_inicio:
            return "Falta fecha de inicio", 400

        empleados = json.loads(empleados_json)

        empleados_map = {}
        for nombre, firma_digital, firma_imagen_path in db.execute(
            "SELECT nombre, firma_electronica, firma_imagen_path FROM empleados_parte"
        ).fetchall():
            clave = str(nombre or "").strip().lower()
            if clave:
                empleados_map[clave] = {
                    "firma_digital": str(firma_digital or "").strip(),
                    "firma_imagen_path": str(firma_imagen_path or "").strip(),
                }

        for emp in empleados:
            nombre_emp = str(emp.get('nombre') or '').strip()
            firma_data = empleados_map.get(nombre_emp.lower(), {})
            horas_total = sum([float(emp.get(dia, 0) or 0) for dia in ['lun', 'mar', 'mie', 'jue', 'vie', 'sab']])
            db.execute("""
                INSERT INTO partes_trabajo (fecha, operario, ot_id, horas, firma_digital, firma_imagen_path, actividad)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                semana_inicio,
                nombre_emp,
                emp.get('ot_id'),
                horas_total,
                firma_data.get("firma_digital", ""),
                firma_data.get("firma_imagen_path", ""),
                f"Semana del {semana_inicio}" + (f" | Proceso: {emp.get('proceso')}" if emp.get('proceso') else "")
            ))

        db.commit()
        return redirect("/modulo/parte")

    # GET
    ots = db.execute(
        "SELECT id, obra, titulo FROM ordenes_trabajo WHERE estado != 'Finalizada' AND fecha_cierre IS NULL ORDER BY id DESC"
    ).fetchall()

    empleados_catalogo = db.execute(
        """
        SELECT id,
               COALESCE(nombre, ''),
               COALESCE(nombre_base, ''),
               COALESCE(apellido, ''),
               COALESCE(puesto_tipo, ''),
               COALESCE(puesto_detalle, ''),
               COALESCE(puesto, ''),
               COALESCE(firma_electronica, ''),
               COALESCE(firma_imagen_path, '')
        FROM empleados_parte
        ORDER BY LOWER(TRIM(COALESCE(apellido, ''))) COLLATE NOCASE ASC,
                 LOWER(TRIM(COALESCE(nombre_base, nombre, ''))) COLLATE NOCASE ASC
        """
    ).fetchall()

    operarios_catalogo = db.execute(
        """
        SELECT nombre
        FROM empleados_parte
        WHERE LOWER(TRIM(COALESCE(puesto_tipo, ''))) = 'operario'
           OR LOWER(TRIM(COALESCE(puesto, ''))) LIKE '%operario%'
           OR LOWER(TRIM(COALESCE(puesto, ''))) LIKE '%soldador%'
           OR LOWER(TRIM(COALESCE(puesto, ''))) LIKE '%armador%'
           OR LOWER(TRIM(COALESCE(puesto, ''))) LIKE '%ayudante%'
           OR LOWER(TRIM(COALESCE(puesto, ''))) LIKE '%pintor%'
        ORDER BY LOWER(TRIM(COALESCE(nombre, ''))) COLLATE NOCASE ASC
        """
    ).fetchall()

    mensaje = (request.args.get("mensaje") or "").strip()
    mensaje_html = ""
    if mensaje:
        clase = "flash-error" if ("⚠️" in mensaje or "❌" in mensaje) else "flash-ok"
        mensaje_html = f'<div class="flash {clase}">{html_lib.escape(mensaje)}</div>'

    operarios_options = ""
    for (nombre_operario,) in operarios_catalogo:
        nombre_txt = str(nombre_operario or "").strip()
        if nombre_txt:
            operarios_options += f'<option value="{html_lib.escape(nombre_txt)}">{html_lib.escape(nombre_txt)}</option>'

    empleados_listado = ""
    for empleado_id, nombre_full, nombre_base, apellido, puesto_tipo, puesto_detalle, puesto_legacy, firma, firma_imagen_path in empleados_catalogo:
        nombre_txt = html_lib.escape(_nombre_mostrable(nombre_full, nombre_base, apellido))
        puesto_val = str(puesto_detalle or "").strip() or str(puesto_legacy or "").strip()
        puesto_txt = html_lib.escape(puesto_val)
        firma_val = str(firma or "").strip()
        if _normalizar_tipo_puesto(puesto_tipo) == "operario":
            firma_val = "0"
        firma_txt = html_lib.escape(firma_val)
        empleados_listado += f"""
            <tr>
                <td>
                    <input type="text" name="empleado_nombre" value="{nombre_txt}" form="edit-emp-{empleado_id}" required>
                </td>
                <td>
                    <input type="text" name="empleado_puesto" value="{puesto_txt}" form="edit-emp-{empleado_id}" required>
                </td>
                <td>
                    <input type="text" name="empleado_firma" value="{firma_txt}" form="edit-emp-{empleado_id}" required>
                </td>
                <td style="white-space: nowrap; min-width: 220px;">
                    <form id="edit-emp-{empleado_id}" method="post" style="display:inline; margin:0; padding:0; background:transparent;">
                        <input type="hidden" name="accion" value="editar_empleado">
                        <input type="hidden" name="empleado_id" value="{empleado_id}">
                        <button type="submit" class="btn-mini">💾 Editar</button>
                    </form>
                    <form method="post" style="display:inline; margin:0; padding:0; background:transparent;" onsubmit="return confirm('¿Eliminar empleado?');">
                        <input type="hidden" name="accion" value="eliminar_empleado">
                        <input type="hidden" name="empleado_id" value="{empleado_id}">
                        <button type="submit" class="btn-mini btn-mini-del">🗑 Eliminar</button>
                    </form>
                </td>
            </tr>
        """

    if not empleados_listado:
        empleados_listado = "<tr><td colspan='4' style='text-align:center;color:#6b7280;'>No hay empleados cargados</td></tr>"

    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * { box-sizing: border-box; }
    body { font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }
    h2 { color: #333; border-bottom: 3px solid #667eea; padding-bottom: 10px; }
    .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
    .header-actions { display: flex; gap: 10px; align-items: center; }
    .btn { background: #f97316; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; }
    .btn:hover { background: #ea580c; }
    form { background: white; padding: 20px; border-radius: 5px; margin: 20px 0; }
    .form-group { margin-bottom: 20px; }
    label { display: block; font-weight: bold; margin-bottom: 5px; }
    input[type="date"], input[type="text"], select { width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 4px; }
    table { width: 100%; border-collapse: collapse; background: white; margin-top: 15px; }
    th, td { padding: 10px; border: 1px solid #ddd; text-align: center; }
    th { background: #f97316; color: white; font-weight: bold; }
    td { background: white; }
    input[type="number"] { width: 100%; padding: 5px; border: 1px solid #ccc; border-radius: 3px; }
    .btn-add { background: #f97316; padding: 8px 12px; cursor: pointer; margin-top: 10px; }
    .btn-add:hover { background: #ea580c; }
    button { width: 100%; padding: 12px; background: #f97316; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; }
    button:hover { background: #ea580c; }
    .btn-delete { background: #fa709a; color: white; padding: 5px 10px; border: none; cursor: pointer; border-radius: 3px; }
    .btn-x {
        background: #fa709a;
        color: #fff;
        border: none;
        font-size: 16px;
        padding: 5px 10px;
        border-radius: 4px;
        cursor: pointer;
        vertical-align: middle;
        transition: background 0.12s;
        margin-left: 2px;
        font-weight: bold;
        width: auto;
        height: 32px;
        display: inline-block;
        line-height: 1;
    }
    .btn-x:hover {
        background: #dc2626;
        color: #fff;
    }
    .total { font-weight: bold; background: #e8f5e9; }
    .flash { padding: 10px 12px; border-radius: 6px; margin-bottom: 14px; font-weight: bold; }
    .flash-ok { background: #e8f5e9; color: #1b5e20; border: 1px solid #a5d6a7; }
    .flash-error { background: #fff3e0; color: #8a4b00; border: 1px solid #ffcc80; }
    @media (max-width: 900px) { .header { flex-direction: column; align-items: flex-start; } .header-actions { width: 100%; margin-top: 10px; } }
    </style>
    </head>
    <body>
    <div class="header">
        <h2>⏱ Parte Semanal - Empleados</h2>
        <div class="header-actions">
            <a href="/modulo/parte/carga-empleados" class="btn btn-carga">👥 Carga de empleados</a>
            <a href="/modulo/parte/reportes" class="btn btn-reportes">📊 Ver reportes</a>
            <a href="/" class="btn">⬅️ Volver</a>
        </div>
    </div>
    """ + mensaje_html + """
    
    <form method="post" id="parte-form">
        <input type="hidden" name="accion" value="guardar_parte">
        <div class="form-group">
            <label>Semana iniciando:</label>
            <input type="date" name="semana_inicio" id="semana_inicio" required>
        </div>
        
        <h3>📋 Planilla de Horas (Lunes a Sábado)</h3>
        <table id="planilla-table">
            <tr>
                <th>Empleado</th>
                <th>OT Asignada</th>
                <th>Proceso</th>
                <th>Lun</th>
                <th>Mar</th>
                <th>Mié</th>
                <th>Jue</th>
                <th>Vie</th>
                <th>Sáb</th>
                <th>Total</th>
                <th>Acciones</th>
            </tr>
            <tr id="template-row" style="display: none;">
                <td>
                    <select class="empleado-input" required>
                        <option value="">Seleccionar operario...</option>
    """ + operarios_options + """
                    </select>
                </td>
                <td>
                    <select class="ot-input" required>
                        <option value="">Seleccionar...</option>
    """

    for ot in ots:
        obra_ot = str(ot[1] or '').strip()
        titulo_ot = str(ot[2] or '').strip()
        etiqueta_ot = f"{ot[0]} - {obra_ot} - {titulo_ot}" if titulo_ot else f"{ot[0]} - {obra_ot}"
        html += f'<option value="{ot[0]}">{etiqueta_ot}</option>'

    html += """
                    </select>
                </td>
                <td>
                    <select class="proceso-input">
                        <option value="">-- Proceso --</option>
                        <option value="Armado">Armado</option>
                        <option value="Soldadura">Soldadura</option>
                        <option value="Pintura">Pintura</option>
                        <option value="Mantenimiento">Mantenimiento</option>
                    </select>
                </td>
                <td><input type="number" class="horas lun" min="0" max="24" step="0.5" value="0"></td>
                <td><input type="number" class="horas mar" min="0" max="24" step="0.5" value="0"></td>
                <td><input type="number" class="horas mie" min="0" max="24" step="0.5" value="0"></td>
                <td><input type="number" class="horas jue" min="0" max="24" step="0.5" value="0"></td>
                <td><input type="number" class="horas vie" min="0" max="24" step="0.5" value="0"></td>
                <td><input type="number" class="horas sab" min="0" max="24" step="0.5" value="0"></td>
                <td class="total">0</td>
                <td style="white-space:nowrap;">
                    <button type="button" class="btn-add" style="background:#38bdf8;width:auto;padding:5px 10px;font-size:12px;margin-top:4px;margin-right:4px;" onclick="agregarFilaDesde(this);">+</button>
                    <button type="button" class="btn-x" title="Eliminar" onclick="this.closest('tr').remove(); actualizarTotales();">&#10006;</button>
                </td>
            </tr>
            <tr id="resumen-dias" style="background:#e8f5e9; font-weight:bold;">
                <td colspan="3"><b>Sumatoria HS por día</b></td>
                <td id="sum-lun">0.0</td>
                <td id="sum-mar">0.0</td>
                <td id="sum-mie">0.0</td>
                <td id="sum-jue">0.0</td>
                <td id="sum-vie">0.0</td>
                <td id="sum-sab">0.0</td>
                <td id="sum-total">0.0</td>
                <td>—</td>
            </tr>
        </table>
        
        <button type="button" class="btn-add" onclick="agregarEmpleado()">➕ Agregar Empleado</button>
        
        <input type="hidden" name="empleados_json" id="empleados_json">
        <button type="submit" onclick="guardarParte()">💾 Guardar Parte Semanal</button>
    </form>
    
    <script>
    function agregarFilaDesde(btn) {
        const sourceRow = btn.closest('tr');
        const template = document.getElementById('template-row');
        const newRow = template.cloneNode(true);
        newRow.id = '';
        newRow.style.display = '';
        const srcEmp = sourceRow.querySelector('.empleado-input');
        const srcOt = sourceRow.querySelector('.ot-input');
        const srcProc = sourceRow.querySelector('.proceso-input');
        if (srcEmp) newRow.querySelector('.empleado-input').value = srcEmp.value;
        if (srcOt) newRow.querySelector('.ot-input').value = srcOt.value;
        if (srcProc) newRow.querySelector('.proceso-input').value = srcProc.value;
        sourceRow.parentNode.insertBefore(newRow, sourceRow.nextSibling);
        newRow.querySelectorAll('.horas').forEach(input => {
            input.addEventListener('change', actualizarTotales);
        });
        bindAcciones(newRow);
        actualizarTotales();
    }

    function bindAcciones(row) {
        const delBtn = row.querySelector('.btn-delete');
        if (delBtn) delBtn.onclick = function() { this.closest('tr').remove(); actualizarTotales(); };
        const addBtn = row.querySelector('.btn-add');
        if (addBtn) addBtn.onclick = function() { agregarFilaDesde(this); };
    }

    function agregarFila() {
        // Duplica la última fila visible (copiando empleado, OT y proceso)
        const rows = Array.from(document.querySelectorAll('#planilla-table tr')).filter(
            r => r.id !== 'template-row' && r.id !== 'resumen-dias' && r.style.display !== 'none' && r.querySelectorAll('th').length === 0
        );
        const lastRow = rows[rows.length - 1];
        const template = document.getElementById('template-row');
        const newRow = template.cloneNode(true);
        newRow.id = '';
        newRow.style.display = '';
        if (lastRow) {
            const lastEmp = lastRow.querySelector('.empleado-input');
            const lastOt = lastRow.querySelector('.ot-input');
            const lastProc = lastRow.querySelector('.proceso-input');
            if (lastEmp) newRow.querySelector('.empleado-input').value = lastEmp.value;
            if (lastOt) newRow.querySelector('.ot-input').value = lastOt.value;
            if (lastProc) newRow.querySelector('.proceso-input').value = lastProc.value;
        }
        const resumen = document.getElementById('resumen-dias');
        const parent = resumen ? resumen.parentNode : document.getElementById('planilla-table');
        parent.insertBefore(newRow, resumen);
        newRow.querySelectorAll('.horas').forEach(input => {
            input.addEventListener('change', actualizarTotales);
        });
        newRow.querySelector('.btn-delete').onclick = function() {
            this.closest('tr').remove();
            actualizarTotales();
        };
        bindAcciones(newRow);
    }

    function agregarEmpleado() {
        const template = document.getElementById('template-row');
        const newRow = template.cloneNode(true);
        newRow.id = '';
        newRow.style.display = '';

        const resumen = document.getElementById('resumen-dias');
        const parent = resumen ? resumen.parentNode : document.getElementById('planilla-table');
        parent.insertBefore(newRow, resumen);
        
        newRow.querySelectorAll('.horas').forEach(input => {
            input.addEventListener('change', actualizarTotales);
        });
        
        newRow.querySelector('.btn-delete').onclick = function() {
            this.closest('tr').remove();
            actualizarTotales();
        };
        bindAcciones(newRow);
    }
    
    function actualizarTotales() {
        const rows = document.querySelectorAll('#planilla-table tr');
        const sum = { lun: 0, mar: 0, mie: 0, jue: 0, vie: 0, sab: 0 };

        function valorHora(row, clase) {
            const el = row.querySelector(clase);
            return el ? (parseFloat(el.value) || 0) : 0;
        }

        rows.forEach((row, idx) => {
            if (idx === 0 || row.id === 'template-row' || row.id === 'resumen-dias') return;

            const horas = row.querySelectorAll('.horas');
            let total = 0;
            horas.forEach(h => total += parseFloat(h.value) || 0);

            const elTotal = row.querySelector('.total');
            if (elTotal) {
                elTotal.textContent = total.toFixed(1);
            }

            sum.lun += valorHora(row, '.lun');
            sum.mar += valorHora(row, '.mar');
            sum.mie += valorHora(row, '.mie');
            sum.jue += valorHora(row, '.jue');
            sum.vie += valorHora(row, '.vie');
            sum.sab += valorHora(row, '.sab');
        });

        document.getElementById('sum-lun').textContent = sum.lun.toFixed(1);
        document.getElementById('sum-mar').textContent = sum.mar.toFixed(1);
        document.getElementById('sum-mie').textContent = sum.mie.toFixed(1);
        document.getElementById('sum-jue').textContent = sum.jue.toFixed(1);
        document.getElementById('sum-vie').textContent = sum.vie.toFixed(1);
        document.getElementById('sum-sab').textContent = sum.sab.toFixed(1);
        document.getElementById('sum-total').textContent = (sum.lun + sum.mar + sum.mie + sum.jue + sum.vie + sum.sab).toFixed(1);
    }
    
    function guardarParte() {
        const semana = document.getElementById('semana_inicio').value;
        if (!semana) {
            alert('❌ Selecciona la fecha de inicio');
            return;
        }
        
        const rows = document.querySelectorAll('#planilla-table tr');
        const empleados = [];
        
        rows.forEach((row, idx) => {
            if (idx === 0 || row.id === 'template-row' || row.id === 'resumen-dias') return;

            const empleadoSelect = row.querySelector('.empleado-input');
            if (!empleadoSelect || empleadoSelect.value.trim() === '') return;
            
            empleados.push({
                nombre: empleadoSelect.value,
                ot_id: row.querySelector('.ot-input').value,
                proceso: (row.querySelector('.proceso-input') ? row.querySelector('.proceso-input').value : ''),
                lun: row.querySelector('.lun').value,
                mar: row.querySelector('.mar').value,
                mie: row.querySelector('.mie').value,
                jue: row.querySelector('.jue').value,
                vie: row.querySelector('.vie').value,
                sab: row.querySelector('.sab').value
            });
        });
        
        if (empleados.length === 0) {
            alert('❌ Agrega al menos un empleado');
            return;
        }
        
        document.getElementById('empleados_json').value = JSON.stringify(empleados);
        document.getElementById('parte-form').submit();
    }
    
    function inicializarFilasIniciales() {
        const yaHayFilas = document.querySelectorAll('#planilla-table tr:not(#template-row):not(#resumen-dias)').length > 1;
        if (yaHayFilas) return;

        const filasIniciales = 6;
        for (let i = 0; i < filasIniciales; i++) {
            agregarEmpleado();
        }

        actualizarTotales();
    }

    inicializarFilasIniciales();
    document.addEventListener('DOMContentLoaded', inicializarFilasIniciales);
    </script>
    </body>
    </html>
    """
    return html


@parte_bp.route("/modulo/parte/carga-empleados", methods=["GET", "POST"])
def parte_carga_empleados():
    db = get_db()

    # Compatibilidad/migración liviana de columnas nuevas
    try:
        cols = {r[1] for r in db.execute("PRAGMA table_info(empleados_parte)").fetchall()}
        if "nombre_base" not in cols:
            db.execute("ALTER TABLE empleados_parte ADD COLUMN nombre_base TEXT")
        if "apellido" not in cols:
            db.execute("ALTER TABLE empleados_parte ADD COLUMN apellido TEXT")
        if "puesto_tipo" not in cols:
            db.execute("ALTER TABLE empleados_parte ADD COLUMN puesto_tipo TEXT")
        if "puesto_detalle" not in cols:
            db.execute("ALTER TABLE empleados_parte ADD COLUMN puesto_detalle TEXT")

        rows_fix = db.execute(
            """
            SELECT id, COALESCE(nombre, ''), COALESCE(nombre_base, ''), COALESCE(apellido, ''), COALESCE(puesto_tipo, ''), COALESCE(puesto, '')
            FROM empleados_parte
            """
        ).fetchall()
        for emp_id, nombre_full, nombre_base, apellido, puesto_tipo, puesto in rows_fix:
            nombre_base_fix = str(nombre_base or "").strip()
            apellido_fix = str(apellido or "").strip()
            if not nombre_base_fix:
                n_guess, a_guess = _extraer_nombre_apellido_desde_full(nombre_full)
                nombre_base_fix = n_guess
                if not apellido_fix:
                    apellido_fix = a_guess

            tipo_fix = _normalizar_tipo_puesto(puesto_tipo) if str(puesto_tipo or "").strip() else _inferir_tipo_puesto_legacy(puesto)

            db.execute(
                """
                UPDATE empleados_parte
                SET nombre_base = COALESCE(NULLIF(nombre_base, ''), ?),
                    apellido = COALESCE(NULLIF(apellido, ''), ?),
                    puesto_tipo = COALESCE(NULLIF(puesto_tipo, ''), ?)
                WHERE id = ?
                """,
                (nombre_base_fix, apellido_fix, tipo_fix, int(emp_id)),
            )
        db.commit()
    except Exception:
        pass

    if request.method == "POST":
        accion = (request.form.get("accion") or "").strip()

        if accion == "guardar_empleado":
            nombre_base = (request.form.get("empleado_nombre") or "").strip()
            apellido_emp = (request.form.get("empleado_apellido") or "").strip()
            tipo_puesto = _normalizar_tipo_puesto(request.form.get("empleado_tipo_puesto"))
            puesto_detalle = (request.form.get("empleado_puesto_detalle") or "").strip()
            firma_ingresada = (request.form.get("empleado_firma") or "").strip()
            firma_emp = "0" if tipo_puesto == "operario" else firma_ingresada
            nombre_emp = " ".join([v for v in [nombre_base, apellido_emp] if v]).strip()
            puesto_emp = puesto_detalle

            if not nombre_base or not apellido_emp or not puesto_detalle:
                return redirect("/modulo/parte/carga-empleados?mensaje=" + quote("⚠️ Completá nombre, apellido y puesto"))
            if tipo_puesto == "supervisor" and not firma_emp:
                return redirect("/modulo/parte/carga-empleados?mensaje=" + quote("⚠️ Para supervisor la firma electrónica es obligatoria"))

            firma_imagen_rel = ""
            if firma_emp and firma_emp != "0":
                firma_imagen_rel = _resolver_imagen_firma_empleado(nombre_emp, firma_emp)

            existe = db.execute(
                "SELECT id FROM empleados_parte WHERE LOWER(TRIM(nombre)) = LOWER(TRIM(?))",
                (nombre_emp,)
            ).fetchone()

            if existe:
                db.execute(
                    """
                    UPDATE empleados_parte
                    SET nombre=?, nombre_base=?, apellido=?, puesto=?, puesto_tipo=?, puesto_detalle=?, firma_electronica=?, firma_imagen_path=?, fecha_actualizacion=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (nombre_emp, nombre_base, apellido_emp, puesto_emp, tipo_puesto, puesto_detalle, firma_emp, firma_imagen_rel, existe[0])
                )
                mensaje = "✅ Empleado actualizado"
            else:
                try:
                    db.execute(
                        """
                        INSERT INTO empleados_parte (nombre, nombre_base, apellido, puesto, puesto_tipo, puesto_detalle, firma_electronica, firma_imagen_path)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (nombre_emp, nombre_base, apellido_emp, puesto_emp, tipo_puesto, puesto_detalle, firma_emp, firma_imagen_rel)
                    )
                except Exception as exc:
                    if not is_integrity_error(exc):
                        raise
                    return redirect("/modulo/parte/carga-empleados?mensaje=" + quote("⚠️ Ya existe un empleado con ese nombre"))
                mensaje = "✅ Empleado cargado"

            db.commit()
            return redirect("/modulo/parte/carga-empleados?mensaje=" + quote(mensaje))

        if accion == "editar_empleado":
            empleado_id = (request.form.get("empleado_id") or "").strip()
            nombre_base = (request.form.get("empleado_nombre") or "").strip()
            apellido_emp = (request.form.get("empleado_apellido") or "").strip()
            tipo_puesto = _normalizar_tipo_puesto(request.form.get("empleado_tipo_puesto"))
            puesto_detalle = (request.form.get("empleado_puesto_detalle") or "").strip()
            firma_ingresada = (request.form.get("empleado_firma") or "").strip()
            firma_emp = "0" if tipo_puesto == "operario" else firma_ingresada
            nombre_emp = " ".join([v for v in [nombre_base, apellido_emp] if v]).strip()
            puesto_emp = puesto_detalle

            if not empleado_id.isdigit():
                return redirect("/modulo/parte/carga-empleados?mensaje=" + quote("⚠️ Empleado inválido"))
            if not nombre_base or not apellido_emp or not puesto_detalle:
                return redirect("/modulo/parte/carga-empleados?mensaje=" + quote("⚠️ Completá nombre, apellido y puesto"))
            if tipo_puesto == "supervisor" and not firma_emp:
                return redirect("/modulo/parte/carga-empleados?mensaje=" + quote("⚠️ Para supervisor la firma electrónica es obligatoria"))

            firma_imagen_rel = ""
            if firma_emp and firma_emp != "0":
                firma_imagen_rel = _resolver_imagen_firma_empleado(nombre_emp, firma_emp)

            try:
                db.execute(
                    """
                    UPDATE empleados_parte
                    SET nombre=?, nombre_base=?, apellido=?, puesto=?, puesto_tipo=?, puesto_detalle=?, firma_electronica=?, firma_imagen_path=?, fecha_actualizacion=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (nombre_emp, nombre_base, apellido_emp, puesto_emp, tipo_puesto, puesto_detalle, firma_emp, firma_imagen_rel, int(empleado_id))
                )
            except Exception as exc:
                if not is_integrity_error(exc):
                    raise
                return redirect("/modulo/parte/carga-empleados?mensaje=" + quote("⚠️ Ya existe un empleado con ese nombre"))

            db.commit()
            return redirect("/modulo/parte/carga-empleados?mensaje=" + quote("✅ Empleado editado"))

        if accion == "eliminar_empleado":
            empleado_id = (request.form.get("empleado_id") or "").strip()
            if not empleado_id.isdigit():
                return redirect("/modulo/parte/carga-empleados?mensaje=" + quote("⚠️ Empleado inválido"))

            db.execute("DELETE FROM empleados_parte WHERE id=?", (int(empleado_id),))
            db.commit()
            return redirect("/modulo/parte/carga-empleados?mensaje=" + quote("✅ Empleado eliminado"))

    # GET
    edit_id_txt = (request.args.get("edit_id") or "").strip()
    edit_id = int(edit_id_txt) if edit_id_txt.isdigit() else None

    empleados_catalogo = db.execute(
        """
        SELECT id,
               COALESCE(nombre, ''),
               COALESCE(nombre_base, ''),
               COALESCE(apellido, ''),
               COALESCE(puesto_tipo, ''),
               COALESCE(puesto_detalle, ''),
               COALESCE(puesto, ''),
               COALESCE(firma_electronica, ''),
               COALESCE(firma_imagen_path, '')
        FROM empleados_parte
        ORDER BY LOWER(TRIM(COALESCE(apellido, ''))) COLLATE NOCASE ASC,
                 LOWER(TRIM(COALESCE(nombre_base, nombre, ''))) COLLATE NOCASE ASC
        """
    ).fetchall()

    mensaje = (request.args.get("mensaje") or "").strip()
    mensaje_html = ""
    if mensaje:
        clase = "flash-error" if ("⚠️" in mensaje or "❌" in mensaje) else "flash-ok"
        mensaje_html = f'<div class="flash {clase}">{html_lib.escape(mensaje)}</div>'

    form_accion = "guardar_empleado"
    form_titulo = "Agregar nuevo empleado"
    form_btn = "💾 Guardar Empleado"
    form_empleado_id = ""
    form_nombre = ""
    form_apellido = ""
    form_tipo = "supervisor"
    form_puesto_detalle = ""
    form_firma = ""

    if edit_id is not None:
        edit_row = db.execute(
            """
            SELECT id,
                   COALESCE(nombre, ''),
                   COALESCE(nombre_base, ''),
                   COALESCE(apellido, ''),
                   COALESCE(puesto_tipo, ''),
                   COALESCE(puesto_detalle, ''),
                   COALESCE(puesto, ''),
                   COALESCE(firma_electronica, '')
            FROM empleados_parte
            WHERE id = ?
            LIMIT 1
            """,
            (edit_id,),
        ).fetchone()
        if edit_row:
            _, nombre_full_e, nombre_base_e, apellido_e, tipo_e, detalle_e, puesto_legacy_e, firma_e = edit_row
            nombre_base_e = str(nombre_base_e or "").strip()
            apellido_e = str(apellido_e or "").strip()
            if not nombre_base_e:
                n_guess, a_guess = _extraer_nombre_apellido_desde_full(nombre_full_e)
                nombre_base_e = n_guess
                if not apellido_e:
                    apellido_e = a_guess

            tipo_e = _normalizar_tipo_puesto(tipo_e) if str(tipo_e or "").strip() else _inferir_tipo_puesto_legacy(puesto_legacy_e)
            detalle_e = str(detalle_e or "").strip() or str(puesto_legacy_e or "").strip()
            firma_e = "0" if tipo_e == "operario" else str(firma_e or "").strip()

            form_accion = "editar_empleado"
            form_titulo = "Editar empleado"
            form_btn = "💾 Guardar cambios"
            form_empleado_id = str(edit_id)
            form_nombre = nombre_base_e
            form_apellido = apellido_e
            form_tipo = tipo_e
            form_puesto_detalle = detalle_e
            form_firma = firma_e

    empleados_listado = ""
    for empleado_id, nombre_full, nombre_base, apellido, puesto_tipo, puesto_detalle, puesto_legacy, firma, firma_imagen_path in empleados_catalogo:
        nombre_base_raw = str(nombre_base or "").strip()
        apellido_raw = str(apellido or "").strip()
        if not nombre_base_raw:
            n_guess, a_guess = _extraer_nombre_apellido_desde_full(nombre_full)
            nombre_base_raw = n_guess
            if not apellido_raw:
                apellido_raw = a_guess

        tipo_raw = _normalizar_tipo_puesto(puesto_tipo) if str(puesto_tipo or "").strip() else _inferir_tipo_puesto_legacy(puesto_legacy)
        detalle_raw = str(puesto_detalle or "").strip() or str(puesto_legacy or "").strip()
        firma_raw = str(firma or "").strip()
        if tipo_raw == "operario":
            firma_raw = "0"

        nombre_txt = html_lib.escape(nombre_base_raw)
        apellido_txt = html_lib.escape(apellido_raw)
        nombre_mostrable_txt = html_lib.escape(_nombre_mostrable(nombre_full, nombre_base_raw, apellido_raw))
        tipo_txt = html_lib.escape("Operario" if tipo_raw == "operario" else "Supervisor")
        detalle_txt = html_lib.escape(detalle_raw or "-")
        firma_txt = html_lib.escape(firma_raw)
        empleados_listado += f"""
            <tr>
                <td>{nombre_txt}</td>
                <td>{apellido_txt}</td>
                <td>{tipo_txt}</td>
                <td>{detalle_txt}</td>
                <td>{firma_txt}</td>
                <td style="white-space: nowrap; min-width: 220px;">
                    <a href="/modulo/parte/carga-empleados?edit_id={empleado_id}" class="btn-mini" style="text-decoration:none;display:inline-block;">✏️ Editar</a>
                    <form method="post" style="display:inline; margin:0; padding:0; background:transparent;" onsubmit="return confirm('¿Eliminar empleado?');">
                        <input type="hidden" name="accion" value="eliminar_empleado">
                        <input type="hidden" name="empleado_id" value="{empleado_id}">
                        <button type="submit" class="btn-mini btn-mini-del">🗑 Eliminar</button>
                    </form>
                </td>
            </tr>
        """

    if not empleados_listado:
        empleados_listado = "<tr><td colspan='6' style='text-align:center;color:#6b7280;'>No hay empleados cargados</td></tr>"

    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * { box-sizing: border-box; }
    body { font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }
    h2 { color: #333; border-bottom: 3px solid #f97316; padding-bottom: 10px; }
    .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
    .header-actions { display: flex; gap: 10px; align-items: center; }
    .btn { background: #f97316; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; }
    form { background: white; padding: 20px; border-radius: 5px; margin: 20px 0; }
    .form-group { margin-bottom: 20px; }
    label { display: block; font-weight: bold; margin-bottom: 5px; }
    input[type="date"], input[type="text"], select { width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 4px; }
    table { width: 100%; border-collapse: collapse; background: white; margin-top: 15px; }
    th, td { padding: 10px; border: 1px solid #ddd; text-align: center; }
    th { background: #f97316; color: white; font-weight: bold; }
    td { background: white; text-align: left; }
    button { width: 100%; padding: 12px; background: #f97316; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; }
    button:hover { background: #ea580c; }
    .grid-5 { display: grid; grid-template-columns: repeat(5, 1fr); gap: 10px; }
    .btn-mini { width: auto; padding: 8px 10px; font-size: 12px; margin-right: 6px; margin-top: 4px; }
    .btn-mini-del { background: #ef5350; }
    .btn-mini-del:hover { background: #d84343; }
    .flash { padding: 10px 12px; border-radius: 6px; margin-bottom: 14px; font-weight: bold; }
    .flash-ok { background: #e8f5e9; color: #1b5e20; border: 1px solid #a5d6a7; }
    .flash-error { background: #fff3e0; color: #8a4b00; border: 1px solid #ffcc80; }
    @media (max-width: 900px) { .header { flex-direction: column; align-items: flex-start; } .header-actions { width: 100%; margin-top: 10px; } .grid-5 { grid-template-columns: 1fr; } }
    </style>
    </head>
    <body>
    <div class="header">
        <h2>👥 Carga de Empleados</h2>
        <div class="header-actions">
            <a href="/modulo/parte" class="btn">⏱ Volver a Parte</a>
            <a href="/" class="btn">⬅️ Volver</a>
        </div>
    </div>
    """ + mensaje_html + """
    
    <form method="post" id="empleados-form">
        <input type="hidden" name="accion" value="__FORM_ACCION__">
        __FORM_EMPLEADO_ID__
        <h3>__FORM_TITULO__</h3>
        <div class="grid-5">
            <div>
                <label>Nombre</label>
                <input type="text" name="empleado_nombre" id="empleado_nombre" placeholder="Nombre" value="__FORM_NOMBRE__" required>
            </div>
            <div>
                <label>Apellido</label>
                <input type="text" name="empleado_apellido" id="empleado_apellido" placeholder="Apellido" value="__FORM_APELLIDO__" required>
            </div>
            <div>
                <label>Tipo</label>
                <select name="empleado_tipo_puesto" id="empleado_tipo_puesto" onchange="actualizarCamposNuevo()" required>
                    <option value="supervisor" __FORM_TIPO_SUP__>Supervisor</option>
                    <option value="operario" __FORM_TIPO_OPE__>Operario</option>
                </select>
            </div>
            <div>
                <label>Puesto</label>
                <select name="empleado_puesto_detalle" id="empleado_puesto_detalle" required>
                    <option value="">-- Seleccionar puesto --</option>
                </select>
            </div>
            <div>
                <label>Firma electrónica</label>
                <input type="text" name="empleado_firma" id="empleado_firma" placeholder="Código o nombre de firma" value="__FORM_FIRMA__" required>
            </div>
        </div>
        <button type="submit">__FORM_BOTON__</button>
        __FORM_CANCELAR__
    </form>

    <div style="display:flex;justify-content:space-between;align-items:center;gap:10px;flex-wrap:wrap;margin-top:16px;">
        <h3 style="margin:0;">📋 Empleados registrados</h3>
    </div>
    <table>
        <tr>
            <th>Nombre (A-Z)</th>
            <th>Apellido (A-Z)</th>
            <th>Tipo</th>
            <th>Puesto</th>
            <th>Firma electrónica</th>
            <th>Acciones</th>
        </tr>
    """ + empleados_listado + """
    </table>

    <script>
    const PUESTOS_SUPERVISOR = ["Of tecnica", "Jefe de Taller", "Coord Estructuras", "Resp. calidad", "Mantenimiento", "Encargado Pintura"];
    const PUESTOS_OPERARIO = ["Of. Soldador", "Of. Armador", "Medio Of.", "Ayudante", "Of. Pintor"];

    function setOpcionesDetalle(selectEl, tipo, valorActual) {
        const arr = (tipo === 'operario') ? PUESTOS_OPERARIO : PUESTOS_SUPERVISOR;
        selectEl.innerHTML = '<option value="">-- Seleccionar puesto --</option>';
        arr.forEach(item => {
            const opt = document.createElement('option');
            opt.value = item;
            opt.textContent = item;
            if ((valorActual || '').trim() === item) opt.selected = true;
            selectEl.appendChild(opt);
        });
    }

    function actualizarCamposNuevo() {
        const tipo = document.getElementById('empleado_tipo_puesto').value;
        const detalle = document.getElementById('empleado_puesto_detalle');
        const firma = document.getElementById('empleado_firma');
        setOpcionesDetalle(detalle, tipo, '__FORM_PUESTO_DETALLE__' || detalle.value);
        if (tipo === 'operario') {
            firma.value = '0';
            firma.readOnly = true;
            firma.style.background = '#f1f5f9';
        } else {
            if ((firma.value || '').trim() === '0') firma.value = '';
            firma.readOnly = false;
            firma.style.background = '#fff';
        }
    }

    document.addEventListener('DOMContentLoaded', function() {
        actualizarCamposNuevo();
    });
    </script>
    
    </body>
    </html>
    """
    html = html.replace("__FORM_ACCION__", html_lib.escape(form_accion))
    html = html.replace("__FORM_EMPLEADO_ID__", f'<input type="hidden" name="empleado_id" value="{html_lib.escape(form_empleado_id)}">' if form_empleado_id else "")
    html = html.replace("__FORM_TITULO__", html_lib.escape(form_titulo))
    html = html.replace("__FORM_NOMBRE__", html_lib.escape(form_nombre))
    html = html.replace("__FORM_APELLIDO__", html_lib.escape(form_apellido))
    html = html.replace("__FORM_TIPO_SUP__", "selected" if form_tipo == "supervisor" else "")
    html = html.replace("__FORM_TIPO_OPE__", "selected" if form_tipo == "operario" else "")
    html = html.replace("__FORM_PUESTO_DETALLE__", html_lib.escape(form_puesto_detalle))
    html = html.replace("__FORM_FIRMA__", html_lib.escape(form_firma))
    html = html.replace("__FORM_BOTON__", html_lib.escape(form_btn))
    html = html.replace("__FORM_CANCELAR__", '<a href="/modulo/parte/carga-empleados" class="btn" style="display:inline-block;margin-top:10px;background:#9ca3af;">Cancelar edición</a>' if form_empleado_id else "")
    return html


@parte_bp.route("/modulo/parte/reportes")
def parte_semanal_reportes():
    db = get_db()

    filtro_obra = request.args.get("obra", "").strip()
    filtro_empleado = request.args.get("empleado", "").strip()
    filtro_semana = request.args.get("semana", "").strip()
    filtro_mes = request.args.get("mes", "").strip()

    obras = db.execute("""
        SELECT DISTINCT TRIM(ot.obra) AS obra
        FROM partes_trabajo pt
        LEFT JOIN ordenes_trabajo ot ON ot.id = pt.ot_id
        WHERE ot.obra IS NOT NULL AND TRIM(ot.obra) <> ''
        ORDER BY obra ASC
    """).fetchall()
    empleados = db.execute("""
        SELECT DISTINCT TRIM(operario) AS operario
        FROM partes_trabajo
        WHERE operario IS NOT NULL AND TRIM(operario) <> ''
        ORDER BY operario ASC
    """).fetchall()
    empleados_catalogo = db.execute("""
        SELECT TRIM(COALESCE(nombre, '')),
               TRIM(COALESCE(nombre_base, '')),
               TRIM(COALESCE(apellido, ''))
        FROM empleados_parte
        WHERE TRIM(COALESCE(nombre, '')) <> ''
    """).fetchall()
    empleados_display_map = {}
    for nombre_full, nombre_base, apellido in empleados_catalogo:
        clave = str(nombre_full or "").strip().lower()
        if clave:
            empleados_display_map[clave] = _nombre_mostrable(nombre_full, nombre_base, apellido)

    condiciones = []
    params = []
    if filtro_obra:
        condiciones.append("TRIM(COALESCE(ot.obra, '')) = ?")
        params.append(filtro_obra)
    if filtro_empleado:
        condiciones.append("LOWER(TRIM(COALESCE(pt.operario, ''))) = ?")
        params.append(filtro_empleado.lower())
    if filtro_semana:
        condiciones.append("pt.fecha = ?")
        params.append(filtro_semana)
    if filtro_mes:
        condiciones.append("substr(pt.fecha, 1, 7) = ?")
        params.append(filtro_mes)

    mes_label = "-"
    if filtro_mes:
        try:
            anio, mes_num = filtro_mes.split("-")
            meses_nombres = [
                "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
                "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
            ]
            mes_label = f"{meses_nombres[int(mes_num) - 1]} {anio}"
        except Exception:
            mes_label = filtro_mes

    where_sql = f"WHERE {' AND '.join(condiciones)}" if condiciones else ""

    reportes = db.execute(f"""
        SELECT pt.id,
               pt.fecha,
               pt.operario,
               TRIM(COALESCE(ot.obra, '')) AS obra,
               pt.ot_id,
               TRIM(COALESCE(ot.titulo, '')) AS ot_titulo,
               TRIM(COALESCE(pt.actividad, '')) AS actividad,
               COALESCE(pt.horas, 0) AS horas
        FROM partes_trabajo pt
        LEFT JOIN ordenes_trabajo ot ON ot.id = pt.ot_id
        {where_sql}
        ORDER BY pt.fecha DESC, pt.operario ASC
    """, params).fetchall()

    total_horas = sum(float(r[7] or 0) for r in reportes)

    opciones_obras = '<option value="">Todas las obras</option>'
    for obra in obras:
        obra_val = str(obra[0] or '').strip()
        selected = 'selected' if obra_val == filtro_obra else ''
        opciones_obras += f'<option value="{obra_val}" {selected}>{obra_val}</option>'

    opciones_empleados = '<option value="">Todos los empleados</option>'
    empleados_items = []
    for empleado in empleados:
        empleado_val = str(empleado[0] or '').strip()
        if not empleado_val:
            continue
        empleado_label = empleados_display_map.get(empleado_val.lower(), empleado_val)
        empleados_items.append((empleado_val, empleado_label))
    empleados_items.sort(key=lambda x: x[1].lower())
    for empleado_val, empleado_label in empleados_items:
        selected = 'selected' if empleado_val == filtro_empleado else ''
        opciones_empleados += f'<option value="{html_lib.escape(empleado_val)}" {selected}>{html_lib.escape(empleado_label)}</option>'

    semanas = db.execute("""
        SELECT DISTINCT fecha
        FROM partes_trabajo
        WHERE fecha IS NOT NULL AND TRIM(fecha) <> ''
        ORDER BY fecha DESC
    """).fetchall()
    opciones_semanas = '<option value="">Todas las semanas</option>'
    for semana in semanas:
        semana_val = str(semana[0] or '').strip()
        selected = 'selected' if semana_val == filtro_semana else ''
        opciones_semanas += f'<option value="{semana_val}" {selected}>{semana_val}</option>'

    meses = db.execute("""
        SELECT DISTINCT substr(fecha, 1, 7) AS mes
        FROM partes_trabajo
        WHERE fecha IS NOT NULL AND TRIM(fecha) <> ''
        ORDER BY mes DESC
    """).fetchall()
    opciones_meses = '<option value="">Todos los meses</option>'
    for mes in meses:
        mes_val = str(mes[0] or '').strip()
        if mes_val:
            año, mes_num = mes_val.split('-')
            mes_nombre = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'][int(mes_num) - 1]
            selected = 'selected' if mes_val == filtro_mes else ''
            opciones_meses += f'<option value="{mes_val}" {selected}>{mes_nombre} {año}</option>'

    reportes_por_semana = {}
    for rep in reportes:
        fecha = rep[1] or ''
        if fecha not in reportes_por_semana:
            reportes_por_semana[fecha] = {}

        operario = rep[2] or ''
        if operario not in reportes_por_semana[fecha]:
            reportes_por_semana[fecha][operario] = []

        reportes_por_semana[fecha][operario].append(rep)

    filas = ""
    for fecha in sorted(reportes_por_semana.keys(), reverse=True):
        reps_semana = reportes_por_semana[fecha]
        total_empleados = len(reps_semana)

        horas_semana = sum(float(r[7] or 0) for row_list in reps_semana.values() for r in row_list)
        registros_semana = sum(len(row_list) for row_list in reps_semana.values())

        filas += f"""
        <tr style="background: #eef8fd; font-weight: bold;">
            <td colspan="8" style="background: #a8d8ea; color: #1f4e5f; padding: 12px; font-size: 16px;">
                📅 Semana del {fecha} | {total_empleados} empleados | {registros_semana} registros | {horas_semana:.1f} HS
            </td>
        </tr>
        """

        for operario in sorted(reps_semana.keys()):
            rows_emp = reps_semana[operario]
            operario_label = empleados_display_map.get(str(operario or "").strip().lower(), str(operario or "").strip())
            for idx, rep in enumerate(rows_emp):
                parte_id = rep[0]
                ot_id = rep[4] or '-'
                ot_titulo = rep[5] or '---'
                actividad = rep[6] or '---'
                obra = rep[3] or '---'
                horas = float(rep[7] or 0)

                horas_cell = f"<b>{horas:.1f}</b>" if idx == len(rows_emp) - 1 else f"{horas:.1f}"

                filas += f"""
        <tr>
            <td><b>{html_lib.escape(operario_label)}</b></td>
            <td>{obra}</td>
            <td><b>{ot_id}</b></td>
            <td>{ot_titulo}</td>
            <td>{actividad}</td>
            <td style="text-align: center;">1</td>
            <td style="text-align: right;">{horas_cell}</td>
            <td style="text-align: center;"><a href="/modulo/parte/reportes/eliminar/{parte_id}" style="color: #d32f2f; text-decoration: none; font-weight: bold; cursor: pointer;" onclick="return confirm('¿Estás seguro de que deseas eliminar este registro?');">✕</a></td>
        </tr>
        """

    if not filas:
        filas = "<tr><td colspan='8' style='text-align:center; color:#777;'>No hay partes guardados para ese filtro</td></tr>"

    html = f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * {{ box-sizing: border-box; }}
    body {{ font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }}
    .header {{ display: flex; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 20px; }}
    .header-left {{ display: flex; align-items: center; gap: 12px; flex: 1; }}
    h2 {{ color: #333; border-bottom: 3px solid #f97316; padding-bottom: 10px; margin: 0; }}
    .header-btns {{ display: flex; gap: 8px; }}
    .btn {{ background: #f97316; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; }}
    .btn:hover {{ background: #ea580c; }}
    .btn-pdf {{ background: #ff9800; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; border: none; cursor: pointer; font-weight: bold; }}
    .btn-pdf:hover {{ background: #f57c00; }}
    .filters {{ background: white; padding: 18px; border-radius: 6px; margin-bottom: 16px; box-shadow: 0 2px 5px rgba(0,0,0,0.08); }}
    .filters form {{ display: grid; grid-template-columns: 1fr 1fr 1fr 1fr auto auto; gap: 10px; align-items: end; }}
    label {{ display: block; font-weight: bold; margin-bottom: 5px; }}
    select {{ width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 4px; }}
    button {{ padding: 10px 16px; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; background: #43a047; color: white; }}
    button:hover {{ background: #2e7d32; }}
    .btn-clear {{ background: #9e9e9e; }}
    .btn-clear:hover {{ background: #757575; }}
    .summary {{ background: #e8f5e9; border-left: 5px solid #43a047; padding: 14px; border-radius: 5px; margin-bottom: 16px; color: #1b5e20; font-size: 16px; }}
    table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 5px rgba(0,0,0,0.08); font-size: 13px; }}
    th, td {{ padding: 10px; border-bottom: 1px solid #ddd; text-align: left; }}
    th {{ background: #43a047; color: white; font-weight: bold; font-size: 12px; }}
    tr:nth-child(even) td {{ background: #fafafa; }}
    @media (max-width: 900px) {{
        .filters form {{ grid-template-columns: 1fr; }}
        .header {{ flex-direction: column; align-items: stretch; }}
    }}
    </style>
    </head>
    <body>
    <div class="header">
        <div class="header-left">
            <h2>📊 Reportes de Parte Semanal - Empleados</h2>
        </div>
        <div class="header-btns">
            <form method="POST" action="/modulo/parte/reportes/pdf" style="display: inline;">
                <input type="hidden" name="obra" value="{filtro_obra}">
                <input type="hidden" name="empleado" value="{filtro_empleado}">
                <input type="hidden" name="semana" value="{filtro_semana}">
                <input type="hidden" name="mes" value="{filtro_mes}">
                <button type="submit" class="btn-pdf">📄 Descargar PDF</button>
            </form>
            <a href="/modulo/parte" class="btn">⬅️ Volver</a>
        </div>
    </div>

    <div class="filters">
        <form method="get">
            <div>
                <label>Obra</label>
                <select name="obra">
                    {opciones_obras}
                </select>
            </div>
            <div>
                <label>Empleado</label>
                <select name="empleado">
                    {opciones_empleados}
                </select>
            </div>
            <div>
                <label>Semana</label>
                <select name="semana">
                    {opciones_semanas}
                </select>
            </div>
            <div>
                <label>Mes</label>
                <select name="mes">
                    {opciones_meses}
                </select>
            </div>
            <button type="submit">Filtrar</button>
            <a href="/modulo/parte/reportes" class="btn btn-clear">Limpiar</a>
        </form>
    </div>

    <div class="summary">
        Horas consumidas: <b>{total_horas:.1f}</b> | Registros encontrados: <b>{len(reportes)}</b>
    </div>

    <table>
        <tr>
            <th style="width: 15%;">Operario</th>
            <th style="width: 12%;">Obra</th>
            <th style="width: 6%;">OT</th>
            <th style="width: 30%;">Descripción OT</th>
            <th style="width: 20%;">Actividad</th>
            <th style="width: 6%;">Reg.</th>
            <th style="width: 8%;">HS</th>
            <th style="width: 3%;">Acción</th>
        </tr>
        {filas}
    </table>
    </body>
    </html>
    """
    return html


@parte_bp.route("/modulo/parte/reportes/eliminar/<int:parte_id>")
def parte_semanal_reporte_eliminar(parte_id):
    db = get_db()
    db.execute("DELETE FROM partes_trabajo WHERE id=?", (parte_id,))
    db.commit()
    return redirect("/modulo/parte/reportes")


@parte_bp.route("/modulo/parte/reportes/pdf", methods=["POST"])
def parte_semanal_reportes_pdf():
    db = get_db()

    filtro_obra = request.form.get("obra", "").strip()
    filtro_empleado = request.form.get("empleado", "").strip()
    filtro_semana = request.form.get("semana", "").strip()
    filtro_mes = request.form.get("mes", "").strip()

    condiciones = []
    params = []
    if filtro_obra:
        condiciones.append("TRIM(COALESCE(ot.obra, '')) = ?")
        params.append(filtro_obra)
    if filtro_empleado:
        condiciones.append("LOWER(TRIM(COALESCE(pt.operario, ''))) = ?")
        params.append(filtro_empleado.lower())
    if filtro_semana:
        condiciones.append("pt.fecha = ?")
        params.append(filtro_semana)
    if filtro_mes:
        condiciones.append("substr(pt.fecha, 1, 7) = ?")
        params.append(filtro_mes)

    mes_label = "-"
    if filtro_mes:
        try:
            anio, mes_num = filtro_mes.split("-")
            meses_nombres = [
                "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
                "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
            ]
            mes_label = f"{meses_nombres[int(mes_num) - 1]} {anio}"
        except Exception:
            mes_label = filtro_mes

    where_sql = f"WHERE {' AND '.join(condiciones)}" if condiciones else ""

    reportes = db.execute(f"""
        SELECT pt.fecha,
               pt.operario,
               TRIM(COALESCE(ot.obra, '')) AS obra,
               pt.ot_id,
               TRIM(COALESCE(ot.titulo, '')) AS ot_titulo,
               TRIM(COALESCE(pt.actividad, '')) AS actividad,
               COALESCE(pt.horas, 0) AS horas
        FROM partes_trabajo pt
        LEFT JOIN ordenes_trabajo ot ON ot.id = pt.ot_id
        {where_sql}
        ORDER BY pt.fecha DESC, pt.operario ASC
    """, params).fetchall()

    reportes_por_semana = {}
    for rep in reportes:
        fecha = rep[0] or ''
        operario = rep[1] or ''
        reportes_por_semana.setdefault(fecha, {}).setdefault(operario, []).append(rep)

    total_horas = sum(float(r[6] or 0) for r in reportes)
    total_registros = len(reportes)

    pdf_buffer = BytesIO()
    doc = SimpleDocTemplate(
        pdf_buffer,
        pagesize=landscape(letter),
        leftMargin=18,
        rightMargin=18,
        topMargin=18,
        bottomMargin=18,
    )

    styles = getSampleStyleSheet()
    story = []

    title_style = ParagraphStyle(
        'ReporteTitle',
        parent=styles['Heading1'],
        fontSize=16,
        leading=20,
        textColor=colors.HexColor('#1f2937'),
        spaceAfter=4,
    )
    subtitle_style = ParagraphStyle(
        'ReporteSubTitle',
        parent=styles['Normal'],
        fontSize=10,
        leading=12,
        textColor=colors.HexColor('#4b5563'),
    )
    cell_style = ParagraphStyle(
        'CellWrap',
        parent=styles['Normal'],
        fontSize=8,
        leading=10,
    )

    logo_path = os.path.join(_APP_DIR, "LOGO.png")
    logo_flow = ""
    if os.path.exists(logo_path):
        logo_flow = Image(logo_path, width=40 * mm, height=22 * mm)

    header_right = Paragraph(
        (
            "<b>REPORTE DE PARTE MENSUAL</b><br/><font size='10'>REGISTRO DE HORAS - EMPLEADOS</font>"
            if filtro_mes else
            "<b>REPORTE DE PARTE SEMANAL</b><br/><font size='10'>REGISTRO DE HORAS - EMPLEADOS</font>"
        ),
        title_style,
    )
    header_table = Table([[logo_flow, header_right]], colWidths=[50 * mm, 215 * mm])
    header_table.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('ALIGN', (0, 0), (0, 0), 'LEFT'),
        ('ALIGN', (1, 0), (1, 0), 'LEFT'),
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
    ]))
    story.append(header_table)
    story.append(Spacer(1, 8))

    info_row1 = [
        Paragraph(f"<b>Semana:</b> {filtro_semana or '-'}", subtitle_style),
        Paragraph(f"<b>Obra:</b> {filtro_obra or '-'}", subtitle_style),
        Paragraph(f"<b>Empleado:</b> {filtro_empleado or '-'}", subtitle_style),
    ]
    info_row2 = [
        Paragraph(f"<b>Mes:</b> {mes_label}", subtitle_style),
        Paragraph(f"<b>Total HS:</b> {total_horas:.1f}", subtitle_style),
        Paragraph(f"<b>Registros:</b> {total_registros}", subtitle_style),
    ]
    info_table = Table([info_row1, info_row2], colWidths=[75 * mm, 75 * mm, 75 * mm])
    info_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#f8fafc')),
        ('BOX', (0, 0), (-1, -1), 0.6, colors.HexColor('#cbd5e1')),
        ('INNERGRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#e2e8f0')),
        ('LEFTPADDING', (0, 0), (-1, -1), 8),
        ('RIGHTPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ]))
    story.append(info_table)
    story.append(Spacer(1, 10))

    table_data = [['OPERARIO', 'OBRA', 'OT', 'DESCRIPCION OT', 'ACTIVIDAD', 'REG.', 'HS']]
    filas_semana = []
    for fecha in sorted(reportes_por_semana.keys(), reverse=True):
        reps_semana = reportes_por_semana[fecha]

        total_empleados = len(reps_semana)
        horas_semana = sum(float(r[6] or 0) for rows in reps_semana.values() for r in rows)
        registros_semana = sum(len(rows) for rows in reps_semana.values())
        filas_semana.append(len(table_data))
        table_data.append([
            Paragraph(
                f"<b>SEMANA DEL {fecha} | {total_empleados} empleados | {registros_semana} registros | {horas_semana:.1f} HS</b>",
                cell_style,
            ),
            '', '', '', '', '', ''
        ])

        for operario in sorted(reps_semana.keys()):
            for rep in reps_semana[operario]:
                table_data.append([
                    Paragraph(str(operario or '---'), cell_style),
                    Paragraph(str(rep[2] or '---'), cell_style),
                    Paragraph(str(rep[3] or '-'), cell_style),
                    Paragraph(str(rep[4] or '---'), cell_style),
                    Paragraph(str(rep[5] or '---'), cell_style),
                    Paragraph('1', cell_style),
                    Paragraph(f"{float(rep[6] or 0):.1f}", cell_style),
                ])

    table = Table(
        table_data,
        colWidths=[32 * mm, 30 * mm, 14 * mm, 58 * mm, 82 * mm, 12 * mm, 12 * mm],
        repeatRows=1,
    )
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#ff9800')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('ALIGN', (2, 0), (2, -1), 'CENTER'),
        ('ALIGN', (5, 0), (-1, -1), 'RIGHT'),
        ('BOX', (0, 0), (-1, -1), 0.5, colors.HexColor('#ffcc99')),
        ('INNERGRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#ffe0b2')),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fff3e0')]),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ('LEFTPADDING', (0, 0), (-1, -1), 4),
        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ]))

    if filas_semana:
        week_style_cmds = []
        for fila in filas_semana:
            week_style_cmds.extend([
                ('SPAN', (0, fila), (6, fila)),
                ('BACKGROUND', (0, fila), (6, fila), colors.HexColor('#f7d7b4')),
                ('TEXTCOLOR', (0, fila), (6, fila), colors.HexColor('#7a4b12')),
                ('FONTNAME', (0, fila), (6, fila), 'Helvetica-Bold'),
                ('FONTSIZE', (0, fila), (6, fila), 9),
                ('ALIGN', (0, fila), (6, fila), 'LEFT'),
                ('TOPPADDING', (0, fila), (6, fila), 6),
                ('BOTTOMPADDING', (0, fila), (6, fila), 6),
            ])
        table.setStyle(TableStyle(week_style_cmds))
    story.append(table)
    story.append(Spacer(1, 12))

    footer_style = ParagraphStyle(
        'Footer',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#9ca3af'),
        alignment=0,
    )
    story.append(Paragraph(f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}", footer_style))

    doc.build(story)
    pdf_buffer.seek(0)

    filename = f"Parte_Semanal_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    return send_file(
        pdf_buffer,
        mimetype='application/pdf',
        as_attachment=True,
        download_name=filename,
    )
