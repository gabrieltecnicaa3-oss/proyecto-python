"""
ot_routes.py
Blueprint Flask con todas las rutas del módulo Órdenes de Trabajo.
"""
import html as html_lib
import os
from datetime import datetime
import uuid

from flask import Blueprint, redirect, request
from db_utils import get_db, _asegurar_estructura_databook_si_valida as _db_asegurar_estructura_databook_si_valida
from qr_utils import load_clean_excel, find_col, clean_xls

# ---------------------------------------------------------------------------
# Constantes de rutas (deben coincidir con las definidas en app2.py)
# ---------------------------------------------------------------------------
_APP_DIR = os.path.dirname(os.path.abspath(__file__))
_DATABOOKS_DIR = os.path.join(_APP_DIR, "Reportes Produccion")
_DATABOOK_SECCIONES = {
    "calidad_recepcion": os.path.join("1-Calidad (Data Book)", "1.1-Recepcion de material"),
    "calidad_corte_perfiles": os.path.join("1-Calidad (Data Book)", "1.2-Corte perfiles"),
    "calidad_armado_soldadura": os.path.join("1-Calidad (Data Book)", "1.3-Armado y soldadura"),
    "calidad_pintura": os.path.join("1-Calidad (Data Book)", "1.4-Pintura"),
    "calidad_despacho": os.path.join("1-Calidad (Data Book)", "1.5-Despacho"),
    "remitos": "2-Remitos de despacho",
}


def _asegurar_databook(obra, ot_id=None):
    return _db_asegurar_estructura_databook_si_valida(obra, _DATABOOKS_DIR, _DATABOOK_SECCIONES, ot_id=ot_id)


def _dir_tmp_import_ot():
    ruta = os.path.join(_APP_DIR, "_tmp_ot_uploads")
    os.makedirs(ruta, exist_ok=True)
    return ruta


def _guardar_ficha_tmp(file_storage):
    if not file_storage or not file_storage.filename:
        return ""
    ext = os.path.splitext(file_storage.filename)[1].lower()
    if ext not in (".xls", ".xlsx"):
        return ""
    nombre_tmp = f"ficha_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}_{uuid.uuid4().hex}{ext}"
    path_tmp = os.path.join(_dir_tmp_import_ot(), nombre_tmp)
    file_storage.save(path_tmp)
    return path_tmp


def _path_tmp_valido(path_tmp):
    if not path_tmp:
        return False
    try:
        path_abs = os.path.abspath(path_tmp)
        base_abs = os.path.abspath(_dir_tmp_import_ot())
        return path_abs.startswith(base_abs) and os.path.isfile(path_abs)
    except Exception:
        return False


def _cargar_piezas_excel_a_ot(db, excel_path, obra, ot_id):
    """Importa piezas del Excel Armado y las vincula a la OT recién creada."""
    try:
        df = load_clean_excel(excel_path)
        if df is None:
            return 0

        col_pos = find_col(df, "POS")
        col_cant = find_col(df, "CANT")
        col_perfil = find_col(df, "PERFIL")
        col_peso = find_col(df, "PESO")
        col_desc = find_col(df, "DESCRIP")
        if not col_desc:
            col_desc = find_col(df, "DESC")

        if not col_pos:
            return 0

        inserted = 0
        prefijos_expandibles = ("V", "C", "PU", "INS")
        prefijos_duplicar_igual = ("A", "T", "G", "BA")

        def _expandir_posiciones(pos_base, cantidad_txt):
            pos_txt = str(pos_base or "").strip()
            if not pos_txt:
                return []

            cant = 1
            try:
                cant = int(float(str(cantidad_txt or "1").replace(",", ".")))
            except Exception:
                cant = 1
            if cant < 1:
                cant = 1

            pos_u = pos_txt.upper()
            es_expandible = any(pos_u.startswith(p) for p in prefijos_expandibles)
            es_duplicar_igual = any(pos_u.startswith(p) for p in prefijos_duplicar_igual)
            es_excluido_ti_to = pos_u.startswith("TI") or pos_u.startswith("TO")

            if es_expandible and cant > 1:
                return [f"{pos_txt}-{n}" for n in range(1, cant + 1)]
            if es_duplicar_igual and not es_excluido_ti_to and cant > 1:
                return [pos_txt for _ in range(cant)]
            return [pos_txt]

        for _, row in df.iterrows():
            pos = clean_xls(row.get(col_pos, ""))
            if not pos:
                continue

            cant_txt = clean_xls(row.get(col_cant, "")) if col_cant else ""
            perfil = clean_xls(row.get(col_perfil, "")) if col_perfil else ""
            peso_txt = clean_xls(row.get(col_peso, "")) if col_peso else ""
            desc = clean_xls(row.get(col_desc, "")) if col_desc else ""

            try:
                cantidad = float(str(cant_txt).replace(",", ".")) if cant_txt else None
            except Exception:
                cantidad = None

            try:
                peso = float(str(peso_txt).replace(",", ".")) if peso_txt else None
            except Exception:
                peso = None

            for pos_expandida in _expandir_posiciones(pos, cant_txt):
                existing = db.execute(
                    """
                    SELECT id FROM procesos
                    WHERE TRIM(COALESCE(posicion, '')) = TRIM(?)
                      AND COALESCE(ot_id, -1) = COALESCE(?, -1)
                      AND eliminado = 0
                    LIMIT 1
                    """,
                    (pos_expandida, ot_id),
                ).fetchone()

                if existing:
                    db.execute(
                        """
                        UPDATE procesos
                        SET obra = COALESCE(NULLIF(obra, ''), ?),
                            cantidad = COALESCE(cantidad, ?),
                            perfil = COALESCE(NULLIF(perfil, ''), ?),
                            peso = COALESCE(peso, ?),
                            descripcion = COALESCE(NULLIF(descripcion, ''), ?),
                            ot_id = COALESCE(ot_id, ?)
                        WHERE id = ?
                        """,
                        (obra, cantidad, perfil or None, peso, desc or None, ot_id, existing[0]),
                    )
                else:
                    db.execute(
                        """
                        INSERT INTO procesos
                        (posicion, obra, cantidad, perfil, peso, descripcion, ot_id, escaneado_qr, eliminado)
                        VALUES (?, ?, ?, ?, ?, ?, ?, 0, 0)
                        """,
                        (pos_expandida, obra, cantidad, perfil or None, peso, desc or None, ot_id),
                    )
                    inserted += 1

        return inserted
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Blueprint
# ---------------------------------------------------------------------------
ot_bp = Blueprint("ot", __name__)


@ot_bp.route("/modulo/ot")
def ot_lista():
    db = get_db()
    filtro_obra = (request.args.get("obra") or "").strip()
    ots_all = db.execute("SELECT * FROM ordenes_trabajo WHERE fecha_cierre IS NULL AND (es_mantenimiento IS NULL OR es_mantenimiento = 0) ORDER BY id DESC").fetchall()
    obras_disponibles = sorted({str(o[2] or "").strip() for o in ots_all if (o[2] or "").strip()})
    ots = [o for o in ots_all if not filtro_obra or str(o[2] or "").strip() == filtro_obra]

    obras_options = '<option value="">-- Todas las obras --</option>'
    for ob in obras_disponibles:
        sel = 'selected' if ob == filtro_obra else ''
        obras_options += f'<option value="{html_lib.escape(ob)}" {sel}>{html_lib.escape(ob)}</option>'

    html = f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {{ font-family: Arial; padding: 15px; background: #f4f4f4; }}
    h2 {{ color: #333; border-bottom: 3px solid #667eea; padding-bottom: 10px; }}
    .btn {{ display: inline-block; background: #667eea; color: white; padding: 10px 15px; 
           text-decoration: none; border-radius: 5px; margin: 10px 0; }}
    .btn:hover {{ background: #5568d3; }}
    .btn-nuevo {{ background: #43e97b; }}
    .btn-nuevo:hover {{ background: #2cc96e; }}
    table {{ width: 100%; border-collapse: collapse; background: white; margin-top: 20px; 
            box-shadow: 0 2px 5px rgba(0,0,0,0.1); }}
    th, td {{ padding: 12px; border-bottom: 1px solid #ddd; text-align: left; }}
    th {{ background: #667eea; color: white; }}
    tr:hover {{ background: #f5f5f5; }}
    .estado-pendiente {{ background: #ffe5e5; }}
    .estado-proceso {{ background: #fff9e5; }}
    .estado-finalizada {{ background: #e5ffe5; }}
    .sin-datos {{ text-align: center; padding: 30px; color: #999; }}
    .header {{ display: flex; justify-content: space-between; align-items: center; }}
    .header a {{ margin-right: 10px; }}
    .filtro-obra {{ background:white; padding:12px 16px; border-radius:6px; margin-bottom:14px; box-shadow:0 1px 4px rgba(0,0,0,0.08); display:flex; align-items:center; gap:10px; flex-wrap:wrap; }}
    .filtro-obra label {{ font-weight:bold; color:#374151; }}
    .filtro-obra select {{ padding:8px 10px; border:1px solid #d1d5db; border-radius:5px; font-size:14px; }}
    </style>
    </head>
    <body>
    <div class="header">
        <div>
            <h2>📋 Órdenes de Trabajo</h2>
            <a href="/" class="btn">⬅️ Volver al Inicio</a>
        </div>
        <a href="/modulo/ot/nueva" class="btn btn-nuevo">➕ Nueva OT</a>
    </div>
    <div class="filtro-obra">
        <label>🏗️ Filtrar por obra:</label>
        <select id="filtro-obra-sel" onchange="window.location.href='/modulo/ot?obra='+encodeURIComponent(this.value)">
            {obras_options}
        </select>
    </div>
    """

    if len(ots) == 0:
        html += "<div class='sin-datos'>⚠️ No hay órdenes de trabajo registradas</div>"
    else:
        html += """
        <details open style="margin-bottom:16px;">
        <summary style="cursor:pointer;font-weight:bold;font-size:15px;padding:8px 0;color:#667eea;">
            🏗️ Ver por Obra
        </summary>
        <div style="margin-top:10px;">
        """
        obras_dict = {}
        for ot in ots:
            obra_key = str(ot[2] or "Sin obra").strip()
            obras_dict.setdefault(obra_key, []).append(ot)
        for obra_key, ots_obra in sorted(obras_dict.items()):
            html += f"""
            <div style="background:#f0f4ff;border-left:4px solid #667eea;padding:8px 12px;margin-bottom:6px;border-radius:4px;">
                <b>📁 {html_lib.escape(obra_key)}</b>
                &nbsp;&nbsp;
                {'&nbsp;'.join(
                    f'<a href="/modulo/ot/editar/{o[0]}" style="background:#667eea;color:white;padding:3px 9px;border-radius:4px;font-size:12px;text-decoration:none;">OT-{o[0]}: {html_lib.escape(str(o[3] or ""))}</a>'
                    for o in ots_obra
                )}
            </div>
            """
        html += "</div></details>"
        html += """
        <table>
            <tr>
                <th>ID</th>
                <th>Cliente</th>
                <th>Obra</th>
                <th>Título</th>
                <th>Tipo</th>
                <th>Fecha Entrega</th>
                <th>Estado</th>
                <th>Creación</th>
                <th>Acciones</th>
            </tr>
        """
        for ot in ots:
            estado_class = f"estado-{ot[5].lower().replace(' ', '')}"
            html += f"""
            <tr class="{estado_class}">
                <td><b>{ot[0]}</b></td>
                <td>{ot[1]}</td>
                <td>{ot[2]}</td>
                <td>{ot[3]}</td>
                <td>{ot[9] or '---'}</td>
                <td>{ot[4]}</td>
                <td>{ot[5]}</td>
                <td>{ot[6]}</td>
                <td>
                    <a href="/modulo/ot/editar/{ot[0]}" class="btn" style="background: #4facfe;">Editar</a>
                    <a href="/modulo/ot/eliminar/{ot[0]}" class="btn" style="background: #fa709a;" onclick="return confirm('¿Eliminar?')">Eliminar</a>
                    <form method="post" action="/modulo/ot/cerrar/{ot[0]}" style="display:inline;">
                        <button type="submit" class="btn" style="background:#fbbf24;color:#000;" onclick="return confirm('¿Cerrar esta OT? Se moverá a Historial y se quitarán sus piezas del estado de piezas por proceso.')">Cerrar OT</button>
                    </form>
                </td>
            </tr>
            """
        html += "</table>"

    html += """
    </body>
    </html>
    """
    return html


@ot_bp.route("/modulo/ot/nueva", methods=["GET", "POST"])
def ot_nueva():
    # Paso 1: Selección de modo
    modo = request.args.get("modo")
    if not modo:
        html = """
        <html><head><meta name='viewport' content='width=device-width, initial-scale=1'>
        <style>
        body { font-family: Arial; padding: 15px; background: #f4f4f4; }
        .card { background: white; border-radius: 8px; box-shadow: 0 2px 8px #0001; padding: 30px; margin: 30px auto; max-width: 420px; text-align: center; }
        .btn { display: block; width: 100%; margin: 18px 0; padding: 18px; font-size: 1.2em; border-radius: 8px; border: none; font-weight: bold; cursor: pointer; }
        .btn-manual { background: #43e97b; color: white; }
        .btn-excel { background: #667eea; color: white; }
        .btn:hover { opacity: 0.92; }
        </style></head><body>
        <div class='card'>
        <h2>📋 Nueva Orden de Trabajo</h2>
        <form method='get'>
            <button class='btn btn-manual' name='modo' value='manual' type='submit'>1️⃣ Carga MANUAL</button>
            <button class='btn btn-excel' name='modo' value='excel' type='submit'>2️⃣ Importar FICHA (Excel)</button>
        </form>
        <a href='/modulo/ot' style='color:#667eea;text-decoration:none;'>⬅️ Volver</a>
        </div></body></html>
        """
        return html

    # Paso 2: Carga manual
    if modo == "manual":
        if request.method == "POST":
            try:
                obra = (request.form.get("obra") or "").strip()
                db = get_db()
                cursor_ot = db.execute("""
                INSERT INTO ordenes_trabajo (cliente, obra, titulo, fecha_entrega, estado, estado_avance, hs_previstas, tipo_estructura, esquema_pintura, espesor_total_requerido)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    request.form["cliente"],
                    obra,
                    request.form["titulo"],
                    request.form["fecha_entrega"],
                    request.form["estado"],
                    0,
                    request.form.get("hs_previstas") or 0,
                    request.form.get("tipo_estructura") or "",
                    request.form.get("esquema_pintura") or "",
                    request.form.get("espesor_total_requerido") or ""
                ))
                db.commit()
                ot_id_nuevo = cursor_ot.lastrowid
                _asegurar_databook(obra, ot_id=ot_id_nuevo)
                if ot_id_nuevo:
                    db.execute(
                        "UPDATE procesos SET ot_id = ? WHERE TRIM(COALESCE(obra,'')) = ? AND ot_id IS NULL",
                        (ot_id_nuevo, obra)
                    )
                    db.commit()
                # Guardar Excel adjunto si fue enviado (opcional)
                excel_file = request.files.get("excel_armado")
                if excel_file and excel_file.filename:
                    import werkzeug.utils
                    filename = werkzeug.utils.secure_filename(excel_file.filename)
                    ext = os.path.splitext(filename)[1].lower()
                    if ext in (".xls", ".xlsx") and obra:
                        carpeta_ot = _asegurar_databook(obra, ot_id=ot_id_nuevo)
                        if carpeta_ot:
                            excel_path = os.path.join(carpeta_ot, filename)
                            excel_file.save(excel_path)
                            _cargar_piezas_excel_a_ot(db, excel_path, obra, ot_id_nuevo)
                            db.commit()
                return redirect("/modulo/ot")
            except Exception as e:
                error_msg = f"❌ Error al guardar OT: {str(e)}"
                return f"""
                <html>
                <head><meta name='viewport' content='width=device-width, initial-scale=1'>
                <style>
                body {{ font-family: Arial; padding: 20px; background: #f4f4f4; }}
                .error-box {{ background: #fee2e2; border: 2px solid #dc2626; border-radius: 8px; padding: 20px; max-width: 600px; margin: 20px auto; }}
                .error-title {{ color: #dc2626; font-weight: bold; font-size: 18px; margin-bottom: 10px; }}
                .error-text {{ color: #7f1d1d; font-family: monospace; background: #fef2f2; padding: 10px; border-radius: 4px; overflow-x: auto; }}
                a {{ display: inline-block; margin-top: 15px; padding: 10px 15px; background: #667eea; color: white; text-decoration: none; border-radius: 4px; }}
                </style></head>
                <body>
                <div class='error-box'>
                <div class='error-title'>{error_msg}</div>
                <div class='error-text'>{html_lib.escape(str(e))}</div>
                <a href='/modulo/ot/nueva?modo=manual'>⬅️ Volver a Carga Manual</a>
                </div>
                </body>
                </html>
                """

        html = """
        <html>
        <head>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <style>
        body { font-family: Arial; padding: 15px; background: #f4f4f4; }
        h2 { color: #333; border-bottom: 3px solid #667eea; padding-bottom: 10px; }
        form { background: white; padding: 20px; border-radius: 5px; max-width: 600px; }
        input, select { width: 100%; padding: 10px; margin: 8px 0; border: 1px solid #ddd; 
                        border-radius: 4px; box-sizing: border-box; }
        label { display: block; margin-top: 15px; font-weight: bold; }
        button { width: 100%; padding: 12px; background: #43e97b; color: white; 
                 border: none; border-radius: 4px; font-weight: bold; cursor: pointer; margin-top: 20px; }
        button:hover { background: #2cc96e; }
        .btn-cancel { background: #999; margin-top: 10px; }
        .btn-cancel:hover { background: #777; }
        </style>
        </head>
        <body>
        <h2>📋 Nueva Orden de Trabajo (Carga Manual)</h2>
        <form method="post" enctype="multipart/form-data">
            <label>Cliente:</label>
            <input type="text" name="cliente" required>
            <label>Obra:</label>
            <input type="text" name="obra" required>
            <label>Título OT:</label>
            <input type="text" name="titulo" required>
            <label>Fecha de Entrega:</label>
            <input type="date" name="fecha_entrega" required>
            <label>Estado:</label>
            <select name="estado" required>
                <option value="Pendiente">Pendiente</option>
                <option value="En proceso">En proceso</option>
                <option value="Finalizada">Finalizada</option>
            </select>
            <label>Hs Previstas:</label>
            <input type="number" name="hs_previstas" min="0" step="0.5" placeholder="0">
            <div style="margin-top:6px; background:#fef3c7; border:1px solid #fcd34d; border-radius:4px; padding:8px; color:#92400e; font-size:12px; line-height:1.3;">
                <b>Nota:</b> Si es subcontrato, completar con <b>0 hs</b>
            </div>
            <label>Tipo de Estructura:</label>
            <select name="tipo_estructura" required>
                <option value="">Seleccionar tipo...</option>
                <option value="TIPO I">TIPO I</option>
                <option value="TIPO II">TIPO II</option>
                <option value="TIPO III">TIPO III</option>
            </select>
            <div style="margin-top:10px; background:#fff7ed; border:1px solid #fed7aa; border-radius:6px; padding:10px; color:#7c2d12; font-size:13px; line-height:1.35;">
                <b>TIPO I:</b> Trabajos de herreria menores. En este grupo entraran trabajos como tapas camaras, barandas individuales, portones, elementos auxiliares para montaje, plataformas, etc.<br><br>
                <b>TIPO II:</b> Son elementos metalicos, estructurales, de complejidad tal que lo conforman varias partes y requieren de una ingenieria de detalle completa.<br><br>
                <b>TIPO III:</b> Son elementos metalicos de fabricacion en serie en donde la ingenieria, fabricacion y controles de calidad no aplica en los tipos 1 y 2. Todo elemento se encuadra en tipo 3 cuando supera las diez unidades.
            </div>
            <label>Esquema de pintura:</label>
            <input type="text" name="esquema_pintura" placeholder="Ej: Epoxi bicapa" maxlength="100">
            <label>Espesor total requerido (μm):</label>
            <input type="text" name="espesor_total_requerido" placeholder="Ej: 120" maxlength="20">
            <label>📎 Excel Armado <span style="font-weight:normal; color:#888;">(opcional)</span>:</label>
            <input type="file" name="excel_armado" accept=".xls,.xlsx" style="padding:6px;">
            <div style="font-size:12px; color:#666; margin-top:4px;">Se guardará en la carpeta Producción del DataBook de la obra.</div>
            <button type="submit">💾 Crear OT</button>
            <a href="/modulo/ot" class="btn-cancel" style="text-align: center; text-decoration: none; color: white; display: block; padding: 12px; border-radius: 4px;">Cancelar</a>
        </form>
        </body>
        </html>
        """
        return html

    # Paso 3: Importar ficha Excel
    if modo == "excel":
        import os
        import pandas as pd
        import unicodedata
        # Paso 1: Subir Excel y mostrar campos
        if request.method == "POST" and "ficha_excel" in request.files:
            file = request.files.get("ficha_excel")
            if not file or not file.filename.lower().endswith((".xlsx", ".xls")):
                return "<h3>❌ Archivo inválido. Debe ser un Excel .xlsx/.xls</h3><a href='/modulo/ot/nueva?modo=excel'>Volver</a>"
            try:
                path_tmp_ficha = _guardar_ficha_tmp(file)
                if not _path_tmp_valido(path_tmp_ficha):
                    return "<h3>❌ No se pudo guardar temporalmente la ficha Excel.</h3><a href='/modulo/ot/nueva?modo=excel'>Volver</a>"

                df = pd.read_excel(path_tmp_ficha)
                # Definir campos y variantes aceptadas
                campos = [
                    "Cliente", "Obra", "Título", "Fecha de Entrega", "Estado", "Hs Previstas", "Tipo de Estructura", "Esquema de pintura", "Espesor total requerido"
                ]
                # Diccionario de variantes aceptadas (sin tildes, mayúsculas, minúsculas, etc.)
                variantes = {
                    "cliente": "Cliente",
                    "obra": "Obra",
                    "titulo": "Título",
                    "título": "Título",
                    "titulo ot": "Título",
                    "fecha de entrega": "Fecha de Entrega",
                    "estado": "Estado",
                    "hs previstas": "Hs Previstas",
                    "horas previstas": "Hs Previstas",
                    "tipo de estructura": "Tipo de Estructura",
                    "esquema de pintura": "Esquema de pintura",
                    "esquema pintura": "Esquema de pintura",
                    "espesor total requerido": "Espesor total requerido",
                    "espesor requerido": "Espesor total requerido"
                }
                import re
                def normalizar(s):
                    s = str(s or "").strip().lower()
                    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
                    s = re.sub(r':\s*$', '', s)  # Quitar dos puntos finales
                    s = re.sub(r'\(.*?\)', '', s)  # Quitar texto entre paréntesis
                    s = s.replace('  ', ' ').strip()
                    return s
                datos = {k: '' for k in campos}
                for i in range(len(df)):
                    clave_raw = str(df.iloc[i,0]).strip()
                    valor = str(df.iloc[i,1]).strip() if df.shape[1] > 1 else ''
                    clave_norm = normalizar(clave_raw)
                    campo = variantes.get(clave_norm, None)
                    if not campo:
                        # Buscar coincidencia directa con los campos normalizados
                        for c in campos:
                            if normalizar(c) == clave_norm:
                                campo = c
                                break
                    if campo:
                        datos[campo] = valor
                # Mapear a variables
                cliente = datos['Cliente']
                obra = datos['Obra']
                titulo = datos['Título']
                fecha_entrega = datos['Fecha de Entrega']
                
                # DEBUG: Ver qué está llegando
                print(f"\n[DEBUG] === INFORMACIÓN DEL EXCEL ===")
                print(f"[DEBUG] Diccionario de datos: {datos}")
                print(f"[DEBUG] fecha_entrega RAW = {repr(fecha_entrega)}, tipo = {type(fecha_entrega)}")
                
                import re
                import math
                import pandas as pd
                from datetime import datetime, timedelta
                
                # Convertir fecha_entrega a formato yyyy-mm-dd válido para input type="date"
                def convertir_fecha_a_iso(valor):
                    if not valor:
                        return ''
                    
                    # Detectar NaN de pandas/numpy
                    try:
                        if pd.isna(valor):
                            return ''
                    except:
                        pass
                    
                    if isinstance(valor, float) and math.isnan(valor):
                        return ''
                    
                    # Si es datetime
                    if hasattr(valor, 'strftime'):
                        try:
                            return valor.strftime('%Y-%m-%d')
                        except:
                            return ''
                    
                    # Si es número (Excel serial date)
                    if isinstance(valor, (int, float)) and not isinstance(valor, bool):
                        try:
                            # Excel base: 1899-12-30 (en Python datetime)
                            base = datetime(1899, 12, 30)
                            fecha = base + timedelta(days=float(valor))
                            return fecha.strftime('%Y-%m-%d')
                        except:
                            return ''
                    
                    # Si es string
                    valor_str = str(valor).strip()
                    if not valor_str or valor_str.lower() in ('nan', 'none', 'null', 'nat', ''):
                        return ''
                    
                    # Si tiene timestamp (YYYY-MM-DD HH:MM:SS), extraer solo la fecha
                    if ' ' in valor_str and len(valor_str.split()[0]) == 10:
                        valor_str = valor_str.split()[0]  # Tomar solo la parte de fecha
                    
                    # Reemplazar separadores comunes
                    valor_str = valor_str.replace('.', '/').replace('-', '/').replace(' ', '')
                    
                    # Intentar coincidencias directas
                    # yyyy/mm/dd
                    if re.match(r'^\d{4}/\d{2}/\d{2}$', valor_str):
                        partes = valor_str.split('/')
                        y, m, d = partes
                        try:
                            # Validar que sea una fecha válida
                            datetime(int(y), int(m), int(d))
                            return f"{y}-{m}-{d}"
                        except:
                            return ''
                    
                    # dd/mm/yyyy o d/m/yyyy
                    if re.match(r'^\d{1,2}/\d{1,2}/\d{4}$', valor_str):
                        partes = valor_str.split('/')
                        if len(partes) == 3:
                            d, m, y = partes
                            d = d.zfill(2)
                            m = m.zfill(2)
                            try:
                                # Validar que sea una fecha válida
                                datetime(int(y), int(m), int(d))
                                return f"{y}-{m}-{d}"
                            except:
                                return ''
                    
                    # Intentar parsear con datetime.strptime
                    formatos = ['%d/%m/%Y', '%d-%m-%Y', '%Y/%m/%d', '%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y']
                    for fmt in formatos:
                        try:
                            fecha_obj = datetime.strptime(valor_str, fmt)
                            return fecha_obj.strftime('%Y-%m-%d')
                        except:
                            pass
                    
                    # Si nada funcionó, retornar vacío
                    return ''
                
                fecha_entrega = convertir_fecha_a_iso(fecha_entrega)
                print(f"[DEBUG] fecha_entrega CONVERTIDA = {repr(fecha_entrega)}")
                print(f"[DEBUG] === FIN DEBUG ===\n")
                estado = datos['Estado'] or 'Pendiente'
                hs_previstas = datos['Hs Previstas'] or '0'
                tipo_estructura = datos['Tipo de Estructura']
                esquema_pintura = datos['Esquema de pintura']
                espesor_total_requerido = datos['Espesor total requerido']
                # Mostrar formulario editable con los datos
                html = f"""
                <html><head><meta name='viewport' content='width=device-width, initial-scale=1'>
                <style>
                body {{ font-family: Arial; padding: 15px; background: #f4f4f4; }}
                .card {{ background: white; border-radius: 8px; box-shadow: 0 2px 8px #0001; padding: 30px; margin: 30px auto; max-width: 520px; }}
                input, select {{ width: 100%; padding: 10px; margin: 8px 0; border: 1px solid #ddd; border-radius: 4px; box-sizing: border-box; }}
                label {{ display: block; margin-top: 15px; font-weight: bold; }}
                button {{ width: 100%; padding: 14px; background: #43e97b; color: white; border: none; border-radius: 8px; font-weight: bold; font-size: 1.1em; cursor: pointer; margin-top: 20px; }}
                button:hover {{ background: #2cc96e; }}
                .btn-cancel {{ background: #999; margin-top: 10px; }}
                .btn-cancel:hover {{ background: #777; }}
                </style></head><body>
                <div class='card'>
                <h2>📥 Revisar y Crear OT</h2>
                <form method='post' enctype='multipart/form-data'>
                    <input type='hidden' name='from_excel' value='1'>
                    <input type='hidden' name='ficha_excel_tmp_path' value='{html_lib.escape(path_tmp_ficha)}'>
                    <label>Cliente:</label>
                    <input type='text' name='cliente' value='{cliente}' required>
                    <label>Obra:</label>
                    <input type='text' name='obra' value='{obra}' required>
                    <label>Título OT:</label>
                    <input type='text' name='titulo' value='{titulo}' required>
                    <label>Fecha de Entrega:</label>
                    <input type='date' name='fecha_entrega' value='{fecha_entrega}' required>
                    <label>Estado:</label>
                    <select name='estado' required>
                        <option value='Pendiente' {'selected' if estado=='Pendiente' else ''}>Pendiente</option>
                        <option value='En proceso' {'selected' if estado=='En proceso' else ''}>En proceso</option>
                        <option value='Finalizada' {'selected' if estado=='Finalizada' else ''}>Finalizada</option>
                    </select>
                    <label>Hs Previstas:</label>
                    <input type='number' name='hs_previstas' min='0' step='0.5' value='{hs_previstas}'>
                    <div style="margin-top:6px; background:#fef3c7; border:1px solid #fcd34d; border-radius:4px; padding:8px; color:#92400e; font-size:12px; line-height:1.3;">
                        <b>Nota:</b> Si es subcontrato, completar con <b>0 hs</b>
                    </div>
                    <label>Tipo de Estructura:</label>
                    <select name='tipo_estructura' required>
                        <option value=''>Seleccionar tipo...</option>
                        <option value='TIPO I' {'selected' if tipo_estructura=='TIPO I' else ''}>TIPO I</option>
                        <option value='TIPO II' {'selected' if tipo_estructura=='TIPO II' else ''}>TIPO II</option>
                        <option value='TIPO III' {'selected' if tipo_estructura=='TIPO III' else ''}>TIPO III</option>
                    </select>
                    <label>Esquema de pintura:</label>
                    <input type='text' name='esquema_pintura' value='{esquema_pintura}' maxlength='100'>
                    <label>Espesor total requerido (μm):</label>
                    <input type='text' name='espesor_total_requerido' value='{espesor_total_requerido}' maxlength='20'>
                    <label>📎 Excel Armado <span style='font-weight:normal; color:#888;'>(opcional)</span>:</label>
                    <input type='file' name='excel_armado' accept='.xls,.xlsx' style='padding:6px;'>
                    <div style='font-size:12px; color:#666; margin-top:4px;'>Se guardará en la carpeta Producción del DataBook de la obra.</div>
                    <button type='submit'>💾 Crear OT</button>
                    <a href='/modulo/ot/nueva?modo=excel' class='btn-cancel' style='text-align: center; text-decoration: none; color: white; display: block; padding: 12px; border-radius: 4px;'>Cancelar</a>
                </form>
                </div></body></html>
                """
                return html
            except Exception as e:
                return f"<h3>❌ Error procesando el Excel: {str(e)}</h3><a href='/modulo/ot/nueva?modo=excel'>Volver</a>"

        # Paso 2: Confirmar y crear OT
        if request.method == "POST" and request.form.get("from_excel") == "1":
            try:
                cliente = request.form.get('cliente', '')
                obra = request.form.get('obra', '')
                titulo = request.form.get('titulo', '')
                fecha_entrega = request.form.get('fecha_entrega', '')
                estado = request.form.get('estado', 'Pendiente')
                hs_previstas = request.form.get('hs_previstas', '0')
                tipo_estructura = request.form.get('tipo_estructura', '')
                esquema_pintura = request.form.get('esquema_pintura', '')
                espesor_total_requerido = request.form.get('espesor_total_requerido', '')
                db = get_db()
                cursor_ot = db.execute("""
                    INSERT INTO ordenes_trabajo (cliente, obra, titulo, fecha_entrega, estado, estado_avance, hs_previstas, tipo_estructura, esquema_pintura, espesor_total_requerido)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    cliente,
                    obra,
                    titulo,
                    fecha_entrega,
                    estado,
                    0,
                    hs_previstas,
                    tipo_estructura,
                    esquema_pintura,
                    espesor_total_requerido
                ))
                db.commit()
                ot_id_nuevo = cursor_ot.lastrowid
                _asegurar_databook(obra, ot_id=ot_id_nuevo)
                if ot_id_nuevo:
                    db.execute(
                        "UPDATE procesos SET ot_id = ? WHERE TRIM(COALESCE(obra,'')) = ? AND ot_id IS NULL",
                        (ot_id_nuevo, obra)
                    )
                    db.commit()

                # Guardar Excel armado adjunto si fue enviado (opcional).
                # Si no se adjunta, NO se importan piezas automáticamente.
                excel_file = request.files.get("excel_armado")
                ficha_tmp_path = (request.form.get("ficha_excel_tmp_path") or "").strip()
                excel_origen_path = ""
                if excel_file and excel_file.filename:
                    import werkzeug.utils
                    filename = werkzeug.utils.secure_filename(excel_file.filename)
                    ext = os.path.splitext(filename)[1].lower()
                    if ext in (".xls", ".xlsx") and obra:
                        carpeta_ot = _asegurar_databook(obra, ot_id=ot_id_nuevo)
                        if carpeta_ot:
                            excel_path = os.path.join(carpeta_ot, filename)
                            excel_file.save(excel_path)
                            excel_origen_path = excel_path

                if excel_origen_path:
                    _cargar_piezas_excel_a_ot(db, excel_origen_path, obra, ot_id_nuevo)
                    db.commit()

                if _path_tmp_valido(ficha_tmp_path):
                    try:
                        os.remove(ficha_tmp_path)
                    except Exception:
                        pass
                return redirect("/modulo/ot")
            except Exception as e:
                return f"<h3>❌ Error al crear la OT: {str(e)}</h3><a href='/modulo/ot/nueva?modo=excel'>Volver</a>"

        # Pantalla inicial: subir archivo
        html = """
        <html><head><meta name='viewport' content='width=device-width, initial-scale=1'>
        <style>
        body { font-family: Arial; padding: 15px; background: #f4f4f4; }
        .card { background: white; border-radius: 8px; box-shadow: 0 2px 8px #0001; padding: 30px; margin: 30px auto; max-width: 420px; text-align: center; }
        input[type=file] { margin: 18px 0; }
        button { background: #667eea; color: white; border: none; border-radius: 8px; padding: 14px 0; width: 100%; font-size: 1.1em; font-weight: bold; cursor: pointer; }
        button:hover { opacity: 0.92; }
        </style></head><body>
        <div class='card'>
        <h2>📥 Importar FICHA de OT (Excel)</h2>
        <form method='post' enctype='multipart/form-data'>
            <input type='file' name='ficha_excel' accept='.xlsx,.xls' required><br>
            <button type='submit'>Importar y Revisar</button>
        </form>
        <a href='/modulo/ot/nueva'>⬅️ Volver</a>
        </div></body></html>
        """
        return html
    return html


@ot_bp.route("/modulo/ot/editar/<int:ot_id>", methods=["GET", "POST"])
def ot_editar(ot_id):
    db = get_db()
    ot = db.execute("SELECT * FROM ordenes_trabajo WHERE id=?", (ot_id,)).fetchone()

    if not ot:
        return "<h3>❌ Orden no encontrada</h3>"

    if request.method == "POST":
        db.execute("""
        UPDATE ordenes_trabajo 
        SET cliente=?, obra=?, titulo=?, fecha_entrega=?, estado=?, hs_previstas=?, tipo_estructura=?, esquema_pintura=?, espesor_total_requerido=?
        WHERE id=?
        """, (
            request.form["cliente"],
            request.form["obra"],
            request.form["titulo"],
            request.form["fecha_entrega"],
            request.form["estado"],
            request.form.get("hs_previstas") or 0,
            request.form.get("tipo_estructura") or "",
            request.form.get("esquema_pintura") or "",
            request.form.get("espesor_total_requerido") or "",
            ot_id
        ))
        db.commit()
        return redirect("/modulo/ot")

    html = f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {{ font-family: Arial; padding: 15px; background: #f4f4f4; }}
    h2 {{ color: #333; border-bottom: 3px solid #667eea; padding-bottom: 10px; }}
    form {{ background: white; padding: 20px; border-radius: 5px; max-width: 600px; }}
    input, select {{ width: 100%; padding: 10px; margin: 8px 0; border: 1px solid #ddd; 
                    border-radius: 4px; box-sizing: border-box; }}
    label {{ display: block; margin-top: 15px; font-weight: bold; }}
    button {{ width: 100%; padding: 12px; background: #43e97b; color: white; 
             border: none; border-radius: 4px; font-weight: bold; cursor: pointer; margin-top: 20px; }}
    button:hover {{ background: #2cc96e; }}
    </style>
    </head>
    <body>
    <h2>✏️ Editar Orden de Trabajo</h2>
    <form method="post">
        <label>Cliente:</label>
        <input type="text" name="cliente" value="{ot[1]}" required>
        
        <label>Obra:</label>
        <input type="text" name="obra" value="{ot[2]}" required>
        
        <label>Título OT:</label>
        <input type="text" name="titulo" value="{ot[3]}" required>
        
        <label>Fecha de Entrega:</label>
        <input type="date" name="fecha_entrega" value="{ot[4]}" required>
        
        <label>Estado:</label>
        <select name="estado" required>
            <option value="Pendiente" {"selected" if ot[5] == "Pendiente" else ""}>Pendiente</option>
            <option value="En proceso" {"selected" if ot[5] == "En proceso" else ""}>En proceso</option>
            <option value="Finalizada" {"selected" if ot[5] == "Finalizada" else ""}>Finalizada</option>
        </select>
        
        <label>Hs Previstas:</label>
        <input type="number" name="hs_previstas" min="0" step="0.5" value="{ot[8] or 0}">
        <div style="margin-top:6px; background:#fef3c7; border:1px solid #fcd34d; border-radius:4px; padding:8px; color:#92400e; font-size:12px; line-height:1.3;">
            <b>Nota:</b> Si es subcontrato, completar con <b>0 hs</b>
        </div>

        <label>Tipo de Estructura:</label>
        <select name="tipo_estructura" required>
            <option value="TIPO I" {"selected" if (len(ot) > 9 and ot[9] == "TIPO I") else ""}>TIPO I</option>
            <option value="TIPO II" {"selected" if (len(ot) > 9 and ot[9] == "TIPO II") else ""}>TIPO II</option>
            <option value="TIPO III" {"selected" if (len(ot) > 9 and ot[9] == "TIPO III") else ""}>TIPO III</option>
        </select>

        <div style="margin-top:10px; background:#fff7ed; border:1px solid #fed7aa; border-radius:6px; padding:10px; color:#7c2d12; font-size:13px; line-height:1.35;">
            <b>TIPO I:</b> Trabajos de herreria menores. En este grupo entraran trabajos como tapas camaras, barandas individuales, portones, elementos auxiliares para montaje, plataformas, etc.<br><br>
            <b>TIPO II:</b> Son elementos metalicos, estructurales, de complejidad tal que lo conforman varias partes y requieren de una ingenieria de detalle completa.<br><br>
            <b>TIPO III:</b> Son elementos metalicos de fabricacion en serie en donde la ingenieria, fabricacion y controles de calidad no aplica en los tipos 1 y 2. Todo elemento se encuadra en tipo 3 cuando supera las diez unidades.
        </div>
        
        <div style="margin-top:15px; background:#f3e8e8; border:1px solid #e5a3a3; border-radius:6px; padding:10px;">
            <b>Estado de Cierre:</b>
            <p style="margin:8px 0; font-size:13px;">
            {"🔒 <b>CERRADA</b> el " + ot[10][:16] if (len(ot) > 10 and ot[10]) else "✅ ACTIVA"}
            </p>
            {"<button type='submit' formaction='/modulo/ot/reabrir/" + str(ot[0]) + "' formmethod='post' style='width:auto; background:#e5a3a3; padding:8px 12px; border:none; border-radius:4px; cursor:pointer; font-weight:bold; margin-top:0;'>🔓 Reabrir OT</button>" if (len(ot) > 10 and ot[10]) else "<button type='submit' formaction='/modulo/ot/cerrar/" + str(ot[0]) + "' formmethod='post' style='width:auto; background:#667eea; padding:8px 12px; border:none; border-radius:4px; cursor:pointer; font-weight:bold; margin-top:0;' onclick='return confirm(\"¿Cerrar esta OT? Se ocultarán todas sus piezas y procesos.\");'>🔒 Cerrar OT</button>"}
        </div>
        
        <label>Esquema de pintura:</label>
        <input type="text" name="esquema_pintura" value="{ot[11] if len(ot) > 11 and ot[11] else ''}" maxlength="100" placeholder="Ej: Epoxi bicapa">

        <label>Espesor total requerido (μm):</label>
        <input type="text" name="espesor_total_requerido" value="{ot[12] if len(ot) > 12 and ot[12] else ''}" maxlength="20" placeholder="Ej: 120">

        <button type="submit">💾 Actualizar OT</button>
    </form>
    </body>
    </html>
    """
    return html


@ot_bp.route("/modulo/ot/eliminar/<int:ot_id>")
def ot_eliminar(ot_id):
    db = get_db()
    # Obtener la obra asociada a la OT
    ot = db.execute("SELECT obra FROM ordenes_trabajo WHERE id=?", (ot_id,)).fetchone()
    if ot and ot[0]:
        db.execute("DELETE FROM procesos WHERE obra=?", (ot[0],))
    db.execute("DELETE FROM ordenes_trabajo WHERE id=?", (ot_id,))
    db.commit()
    return redirect("/modulo/ot")


@ot_bp.route("/modulo/ot/cerrar/<int:ot_id>", methods=["POST"])
def cerrar_ot(ot_id):
    db = get_db()
    ot = db.execute("SELECT obra FROM ordenes_trabajo WHERE id=?", (ot_id,)).fetchone()
    if not ot:
        return redirect("/modulo/ot?mensaje=OT no encontrada")
    obra = ot[0]
    db.execute("UPDATE ordenes_trabajo SET fecha_cierre=CURRENT_TIMESTAMP, estado='Finalizada' WHERE id=?", (ot_id,))
    if obra:
        db.execute("DELETE FROM procesos WHERE obra=?", (obra,))
    db.commit()
    return redirect("/modulo/ot?mensaje=OT cerrada y movida a Historial")


@ot_bp.route("/modulo/ot/reabrir/<int:ot_id>", methods=["POST"])
def ot_reabrir(ot_id):
    db = get_db()
    db.execute(
        "UPDATE ordenes_trabajo SET fecha_cierre = NULL, estado='Activa' WHERE id=?",
        (ot_id,)
    )
    db.commit()
    return redirect("/modulo/ot?mensaje=OT reabierta")


@ot_bp.route("/modulo/historial")
def historial_ots():
    db = get_db()
    ots_cerradas = db.execute("""
        SELECT * FROM ordenes_trabajo 
        WHERE fecha_cierre IS NOT NULL AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
        ORDER BY fecha_cierre DESC
    """).fetchall()

    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body { font-family: Arial; padding: 15px; background: #f4f4f4; }
    h2 { color: #333; border-bottom: 3px solid #667eea; padding-bottom: 10px; }
    .btn { display: inline-block; background: #667eea; color: white; padding: 10px 15px; 
           text-decoration: none; border-radius: 5px; margin: 10px 0; }
    .btn:hover { background: #5568d3; }
    table { width: 100%; border-collapse: collapse; background: white; margin-top: 20px; 
            box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
    th, td { padding: 12px; border-bottom: 1px solid #ddd; text-align: left; }
    th { background: #667eea; color: white; }
    tr:hover { background: #f5f5f5; }
    .sin-datos { text-align: center; padding: 30px; color: #999; }
    .header { display: flex; justify-content: space-between; align-items: center; }
    .btn-reabrir { background: #e5a3a3; }
    .btn-reabrir:hover { background: #d48e8e; }
    </style>
    </head>
    <body>
    <div class="header">
        <div>
            <h2>📋 Historial de OTs - Órdenes Cerradas</h2>
            <a href="/" class="btn">⬅️ Volver al Inicio</a>
        </div>
        <a href="/modulo/ot" class="btn">📌 OTs Activas</a>
    </div>
    """

    if len(ots_cerradas) == 0:
        html += "<div class='sin-datos'>⚠️ No hay órdenes de trabajo cerradas</div>"
    else:
        html += """
        <details open style="margin-bottom:16px;">
        <summary style="cursor:pointer;font-weight:bold;font-size:15px;padding:8px 0;color:#667eea;">
            📁 Ver por Obra
        </summary>
        <div style="margin-top:10px;">
        """
        obras_dict = {}
        for ot in ots_cerradas:
            obra_key = str(ot[2] or "Sin obra").strip()
            obras_dict.setdefault(obra_key, []).append(ot)
        for obra_key, ots_obra in sorted(obras_dict.items()):
            html += f"""
            <div style="background:#f0f4ff;border-left:4px solid #667eea;padding:8px 12px;margin-bottom:6px;border-radius:4px;">
                <b>📁 {html_lib.escape(obra_key)}</b>
                &nbsp;&nbsp;
                {'&nbsp;'.join(
                    f'<a href="/modulo/ot/editar/{o[0]}" style="background:#667eea;color:white;padding:3px 9px;border-radius:4px;font-size:12px;text-decoration:none;">OT-{o[0]}: {html_lib.escape(str(o[3] or ""))}</a>'
                    for o in ots_obra
                )}
            </div>
            """
        html += "</div></details>"
        html += """
        <table>
            <tr>
                <th>ID</th>
                <th>Cliente</th>
                <th>Obra</th>
                <th>Título</th>
                <th>Tipo</th>
                <th>Fecha Entrega</th>
                <th>Estado</th>
                <th>Cierre</th>
                <th>Acciones</th>
            </tr>
        """
        for ot in ots_cerradas:
            cierre_txt = (ot[10][:16] if (len(ot) > 10 and ot[10]) else "-")
            html += f"""
            <tr style="background:#f0f0f0;">
                <td><b>{ot[0]}</b></td>
                <td>{ot[1]}</td>
                <td>{ot[2]}</td>
                <td>{ot[3]}</td>
                <td>{ot[9] or '---'}</td>
                <td>{ot[4]}</td>
                <td>{ot[5]}</td>
                <td><b>🔒 {cierre_txt}</b></td>
                <td>
                    <a href="/modulo/ot/editar/{ot[0]}" class="btn" style="background: #4facfe;">Ver</a>
                    <form method="post" action="/modulo/ot/reabrir/{ot[0]}" style="display:inline;">
                        <button type="submit" class="btn btn-reabrir">🔓 Reabrir</button>
                    </form>
                </td>
            </tr>
            """
        html += "</table>"

    html += """
    </body>
    </html>
    """
    return html
