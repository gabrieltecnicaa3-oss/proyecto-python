import os
import json
import html as html_lib
from io import BytesIO
from urllib.parse import parse_qs, quote, urlencode

from flask import Blueprint, jsonify, redirect, request, send_file, session
from reportlab.lib import colors
from reportlab.lib.pagesizes import landscape, letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.platypus import Image, Paragraph, SimpleDocTemplate, Spacer, Table, TableStyle

from db_utils import (
    get_db,
    _guardar_pdf_databook as _db_guardar_pdf_databook,
    _obtener_ots_para_obra,
    _obtener_ot_id_pieza,
    _completar_metadatos_por_obra_pos,
    _format_cantidad_1_decimal,
    _obtener_responsables_control as _db_obtener_responsables_control,
    _ruta_firma_responsable as _db_ruta_firma_responsable,
    _obtener_operarios_disponibles as _db_obtener_operarios_disponibles,
)
from proceso_utils import (
    _extraer_ciclos_reinspeccion,
    _estado_pieza_persistente,
    _registrar_trazabilidad,
    _agregar_ciclo_reinspeccion,
    pieza_completada,
    validar_siguiente_proceso,
)

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
_FIRMAS_EMPLEADOS_DIR = os.path.join(_APP_DIR, "Firmas empleados")
_DATABOOKS_DIR = os.path.join(_APP_DIR, "Reportes Produccion")
_FIRMA_OK_AUTOMATICA = "GABRIEL IBARRA"
_INSPECTOR_FIRMAS = {
    "Leandro Abella": "LEANDRO ABELLA",
    "Gabriel Ibarra": _FIRMA_OK_AUTOMATICA,
    "Daniel Hereñu": "DANIEL HEREÑU",
}
_DATABOOK_SECCIONES = {
    "calidad_recepcion": os.path.join("1-Calidad (Data Book)", "1.1-Recepcion de material"),
    "calidad_corte_perfiles": os.path.join("1-Calidad (Data Book)", "1.2-Corte perfiles"),
    "calidad_armado_soldadura": os.path.join("1-Calidad (Data Book)", "1.3-Armado y soldadura"),
    "calidad_pintura": os.path.join("1-Calidad (Data Book)", "1.4-Pintura"),
    "calidad_despacho": os.path.join("1-Calidad (Data Book)", "1.5-Despacho"),
    "remitos": "2-Remitos de despacho",
}


def _guardar_pdf_databook(obra, seccion_key, filename, pdf_bytes, ot_id=None):
    return _db_guardar_pdf_databook(obra, seccion_key, filename, pdf_bytes, _DATABOOKS_DIR, _DATABOOK_SECCIONES, ot_id=ot_id)


def _obtener_responsables_control(db):
    return _db_obtener_responsables_control(db, _FIRMAS_EMPLEADOS_DIR, _INSPECTOR_FIRMAS)


def _ruta_firma_responsable(responsables_control, responsable):
    return _db_ruta_firma_responsable(responsables_control, responsable, _FIRMAS_EMPLEADOS_DIR)


def _obtener_operarios_disponibles(db):
    return _db_obtener_operarios_disponibles(db)


calidad_bp = Blueprint("calidad", __name__)


def construir_redirect_desde_qr(qr_data):
    """Normaliza el contenido del QR y arma una URL valida a /pieza/<pos>."""
    if not qr_data:
        return None

    texto = qr_data.strip()
    if not texto:
        return None

    pos = ""
    query_string = ""

    if "/pieza/" in texto:
        fragmento = texto.split("/pieza/", 1)[1]
        if "?" in fragmento:
            pos, query_string = fragmento.split("?", 1)
        else:
            pos = fragmento
    else:
        if "?" in texto:
            pos, query_string = texto.split("?", 1)
        else:
            pos = texto

    pos = pos.strip().strip("/")
    if not pos:
        return None

    permitidos = ["obra", "cant", "perfil", "peso", "desc"]
    params = {}
    if query_string:
        parsed = parse_qs(query_string, keep_blank_values=False)
        for key in permitidos:
            values = parsed.get(key)
            if values and str(values[0]).strip():
                params[key] = str(values[0]).strip()

    obra_qr = str(params.get("obra", "")).strip()
    if obra_qr:
        db = get_db()
        ot_id_existente = _obtener_ot_id_pieza(db, pos, obra_qr)
        if ot_id_existente:
            params["ot_id"] = str(ot_id_existente)
        else:
            ots_obra = _obtener_ots_para_obra(db, obra_qr)
            if len(ots_obra) == 1:
                params["ot_id"] = str(ots_obra[0][0])
            elif len(ots_obra) > 1:
                params_sel = {"pos": pos, **params}
                return f"/qr/seleccionar-ot?{urlencode(params_sel)}"

    url_base = f"/pieza/{quote(pos)}"
    if params:
        return f"{url_base}?{urlencode(params)}"
    return url_base

CONTROL_DESPACHO_ITEMS = [
    "Dar aviso al cliente para inspeccionar el producto antes de su envío",
    "Se dio aviso y coordinó con obra el arribo del pedido?",
    "Se coordinó con obra forma de descarga (Pala con uñas, hidro, etc)",
    "Cuenta con etiqueta de identificación con buena legibilidad y en lugar correcto",
    "Confección de remitos para ingreso a planta (Por triplicado)",
    "Control de embalaje: protección de aristas y zonas comprometidas",
    "El conjunto está en buenas condiciones de terminación superficial (Sin golpes, marcas)",
    "El conjunto está en buenas condiciones de pintura",
    "Se enviaron los elementos de fijación necesarios para el montaje",
    "Se envió pintura necesaria para los retoques",
]



# ======================
# SUB-MÁ“DULO RECEPCIÁ“N DE MATERIALES
# ======================
CONTROL_RECEPCION_ITEMS = [
    {
        "n": 1,
        "tipo": "Ubicacion del material",
        "detalle": "Se cuenta con suficiente lugar de acopio?",
        "frecuencia": "Siempre",
        "criterio": "100%",
    },
    {
        "n": 2,
        "tipo": "Documentacion",
        "detalle": "Coincide la solicitud de compra con el remito que trae el proveedor?",
        "frecuencia": "Siempre",
        "criterio": "100%",
    },
    {
        "n": 3,
        "tipo": "Documentacion",
        "detalle": "Traen en fisico, o se recibio por mail el certificado de calidad?",
        "frecuencia": "Siempre",
        "criterio": "Si no llego en fisico, hacer el reclamo posterior",
    },
    {
        "n": 4,
        "tipo": "Control visual",
        "detalle": "Material o materia prima correctamente empaquetado, embalado e identificado?",
        "frecuencia": "Siempre",
        "criterio": "En caso de no aprobar, dar aviso al coord. de EEMM",
    },
    {
        "n": 5,
        "tipo": "Control visual cuantitativo de materia prima",
        "detalle": "Controlar cantidad de paquetes",
        "frecuencia": "Siempre",
        "criterio": "100%",
    },
    {
        "n": 6,
        "tipo": "Control visual cuantitativo de materia prima",
        "detalle": "Controlar cantidad de barras",
        "frecuencia": "1 cada 3 paquetes",
        "criterio": "100%",
    },
    {
        "n": 7,
        "tipo": "Control visual cualitativo de materia prima",
        "detalle": "Exentas de defectos superficiales: deformaciones, alabeos, golpes, pliegues, fisuras, cascara excesiva, escamas u otras discontinuidades",
        "frecuencia": "Siempre",
        "criterio": "100%",
    },
    {
        "n": 8,
        "tipo": "Control visual de pintura / consumibles",
        "detalle": "Verificar fecha de caducidad",
        "frecuencia": "Siempre",
        "criterio": "100%",
    },
    {
        "n": 9,
        "tipo": "Control visual de otros",
        "detalle": "Estado general. Consultar a coordinar EEMM por controles particulares",
        "frecuencia": "Siempre",
        "criterio": "100%",
    },
    {
        "n": 10,
        "tipo": "Producto tercerizado",
        "detalle": "Analizar el producto en el formulario 7-9.2 Inspeccion y ensayos en produccion",
        "frecuencia": "Siempre",
        "criterio": "100%",
    },
]


@calidad_bp.route("/modulo/calidad/recepcion", methods=["GET", "POST"])
def calidad_recepcion():
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Image, Paragraph, Spacer
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import cm
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from io import BytesIO
    import os

    db = get_db()
    responsables_control = _obtener_responsables_control(db)
    firmas_responsables = {k: v.get("firma", "") for k, v in responsables_control.items()}
    imagenes_responsables = {k: v.get("firma_url", "") for k, v in responsables_control.items()}
    opciones_responsables = "".join(
        f'<option value="{html_lib.escape(nombre)}">{html_lib.escape(nombre)}</option>'
        for nombre in sorted(responsables_control.keys(), key=lambda x: x.lower())
    )

    if request.method == "POST":
        obra = (request.form.get("obra") or "").strip()
        proveedor = (request.form.get("proveedor") or "").strip()
        remito_asociado = (request.form.get("remito_asociado") or "").strip()
        responsable = (request.form.get("responsable") or "").strip()
        firma_form = (request.form.get("firma_digital") or "").strip()
        fecha = (request.form.get("fecha") or "").strip()
        ot_id_txt = (request.form.get("ot_id") or "").strip()

        if not all([obra, proveedor, remito_asociado, responsable, fecha, ot_id_txt]):
            return "Faltan datos requeridos", 400

        if not ot_id_txt.isdigit():
            return "Seleccioná una OT válida", 400

        ot_id_doc = int(ot_id_txt)
        ot_valida = db.execute(
            """
            SELECT id FROM ordenes_trabajo
            WHERE id = ?
              AND TRIM(COALESCE(obra, '')) = TRIM(COALESCE(?, ''))
              AND fecha_cierre IS NULL
              AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
            LIMIT 1
            """,
            (ot_id_doc, obra),
        ).fetchone()
        if not ot_valida:
            return "La OT seleccionada no corresponde a la obra", 400

        if responsable not in firmas_responsables:
            return "Seleccioná un responsable válido", 400

        firma_digital = firmas_responsables.get(responsable, "")
        if not firma_digital or firma_form != firma_digital:
            return "La firma es obligatoria y se completa automáticamente al seleccionar responsable", 400

        firma_path_responsable = _ruta_firma_responsable(responsables_control, responsable)

        detalle_items = []
        for item in CONTROL_RECEPCION_ITEMS:
            idx = item["n"]
            estado = (request.form.get(f"estado_{idx}") or "").strip().upper()
            observacion = (request.form.get(f"observacion_{idx}") or "").strip()
            if estado not in ("CONFORME", "NO CONFORME", "NO APLICA"):
                return f"Falta completar el estado del item {idx}", 400
            detalle_items.append({
                "n": idx,
                "tipo": item["tipo"],
                "detalle": item["detalle"],
                "frecuencia": item["frecuencia"],
                "criterio": item["criterio"],
                "estado": estado,
                "observacion": observacion,
            })

        pdf_buffer = BytesIO()
        doc = SimpleDocTemplate(
            pdf_buffer,
            pagesize=letter,
            topMargin=0.3*cm,
            bottomMargin=0.6*cm,
            leftMargin=0.5*cm,
            rightMargin=0.5*cm
        )

        elements = []
        styles = getSampleStyleSheet()
        base_style = ParagraphStyle('RecepBase', parent=styles['Normal'], fontSize=7.5, leading=9, textColor=colors.HexColor('#333333'))
        head_style = ParagraphStyle('RecepHead', parent=styles['Normal'], fontSize=7.5, leading=9, alignment=1, fontName='Helvetica-Bold', textColor=colors.white)

        encabezado_path = None
        posibles_encabezados = [
            os.path.join(_APP_DIR, "ENCABEZADO_RECEPCION.png"),
            os.path.join(_APP_DIR, "ENCABEZADO_RECEPCION.jpg"),
            os.path.join(_APP_DIR, "ENCABEZADO_RECEPCION.jpeg"),
            os.path.join(_APP_DIR, "ENCABEZADO_RECEPCION", "ENCABEZADO_RECEPCION.png"),
            os.path.join(_APP_DIR, "ENCABEZADO_RECEPCION", "encabezado_recepcion.png"),
            os.path.join(_APP_DIR, "ENCABEZADO_RECEPCION", "ENCABEZADO_RECEPCION.jpg"),
        ]
        for candidato in posibles_encabezados:
            if os.path.exists(candidato):
                encabezado_path = candidato
                break

        if encabezado_path:
            encabezado_img = Image(encabezado_path)
            max_width = 19.8 * cm
            max_height = 3.2 * cm
            if encabezado_img.drawWidth > max_width:
                escala = max_width / float(encabezado_img.drawWidth)
                encabezado_img.drawWidth *= escala
                encabezado_img.drawHeight *= escala
            if encabezado_img.drawHeight > max_height:
                escala_h = max_height / float(encabezado_img.drawHeight)
                encabezado_img.drawWidth *= escala_h
                encabezado_img.drawHeight *= escala_h
            elements.append(encabezado_img)
        else:
            elements.append(Paragraph("<b>CONTROL DE RECEPCION</b>", ParagraphStyle('RH1', parent=styles['Heading2'], alignment=1)))

        elements.append(Spacer(1, 0.2*cm))

        data_info = Table([
            [Paragraph(f"<b>Obra:</b> {obra}", base_style), Paragraph(f"<b>Proveedor:</b> {proveedor}", base_style)],
            [Paragraph(f"<b>Remito asociado:</b> {remito_asociado}", base_style), Paragraph(f"<b>Responsable:</b> {responsable}", base_style)],
            [Paragraph(f"<b>Fecha:</b> {fecha}", base_style), Paragraph("", base_style)],
        ], colWidths=[9.9*cm, 9.9*cm])
        data_info.setStyle(TableStyle([
            ('BOX', (0, 0), (-1, -1), 0.6, colors.HexColor('#fed7aa')),
            ('INNERGRID', (0, 0), (-1, -1), 0.35, colors.HexColor('#fed7aa')),
            ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#fffaf5')),
            ('LEFTPADDING', (0, 0), (-1, -1), 5),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        elements.append(data_info)
        elements.append(Spacer(1, 0.25*cm))

        table_data = [[
            Paragraph("<b>Tipo de Control</b>", head_style),
            Paragraph("<b>Frecuencia</b>", head_style),
            Paragraph("<b>Criterio de Aceptacion</b>", head_style),
            Paragraph("<b>Aprueba?</b>", head_style),
            Paragraph("<b>Observacion</b>", head_style),
        ]]

        for item in detalle_items:
            tipo_text = f"<b>{item['n']}- {item['tipo']}:</b><br/>{item['detalle']}"
            table_data.append([
                Paragraph(tipo_text, base_style),
                Paragraph(item['frecuencia'], base_style),
                Paragraph(item['criterio'], base_style),
                Paragraph(f"<b>{item['estado']}</b>", ParagraphStyle('EstadoRecep', parent=base_style, alignment=1, fontName='Helvetica-Bold')),
                Paragraph(item['observacion'] or "", base_style),
            ])

        control_table = Table(table_data, colWidths=[8.9*cm, 2.0*cm, 3.9*cm, 2.9*cm, 2.1*cm], repeatRows=1)
        control_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f97316')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('GRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#cbd5e1')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fff7ed')]),
        ]))
        elements.append(control_table)

        elements.append(Spacer(1, 0.35*cm))
        firma_cell_content = []
        if firma_path_responsable:
            firma_img = Image(firma_path_responsable)
            max_w = 5.4 * cm
            max_h = 1.8 * cm
            escala = min(max_w / float(firma_img.drawWidth), max_h / float(firma_img.drawHeight), 1.0)
            firma_img.drawWidth = firma_img.drawWidth * escala
            firma_img.drawHeight = firma_img.drawHeight * escala
            firma_cell_content.append(firma_img)
        firma_cell_content.append(Paragraph(f"<b>{html_lib.escape(responsable)}</b>", ParagraphStyle('FirmaRespNombre', parent=styles['Normal'], alignment=1, fontSize=9, textColor=colors.HexColor('#111827'))))

        firma_table = Table([
            ["", firma_cell_content, ""],
            ["", Paragraph("<b>Firma Responsable</b>", ParagraphStyle('FirmaResp', parent=styles['Normal'], alignment=1, fontSize=9, textColor=colors.HexColor('#333333'))), ""],
        ], colWidths=[6.2*cm, 7.4*cm, 6.2*cm])
        firma_table.setStyle(TableStyle([
            ('LINEABOVE', (1, 1), (1, 1), 1, colors.HexColor('#333333')),
            ('ALIGN', (1, 0), (1, 0), 'CENTER'),
            ('VALIGN', (1, 0), (1, 0), 'BOTTOM'),
            ('TOPPADDING', (1, 1), (1, 1), 8),
            ('BOTTOMPADDING', (1, 1), (1, 1), 0),
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
            ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ]))
        elements.append(firma_table)

        doc.build(elements)
        pdf_buffer.seek(0)

        filename = f"Recepcion_OT_{ot_id_doc}_{obra}_{fecha}.pdf".replace(" ", "_").replace("/", "-")
        _guardar_pdf_databook(obra, "calidad_recepcion", filename, pdf_buffer.getvalue(), ot_id=ot_id_doc)
        pdf_buffer.seek(0)
        return send_file(
            pdf_buffer,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=filename
        )

    obras = db.execute("""
        SELECT DISTINCT obra
        FROM ordenes_trabajo
                WHERE fecha_cierre IS NULL
                    AND obra IS NOT NULL AND TRIM(obra) <> ''
                    AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
        ORDER BY obra ASC
    """).fetchall()
    ots_activas = db.execute(
        """
        SELECT id, TRIM(COALESCE(obra, '')) AS obra, TRIM(COALESCE(titulo, '')) AS titulo
        FROM ordenes_trabajo
        WHERE fecha_cierre IS NULL
          AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
        ORDER BY id DESC
        """
    ).fetchall()
    mapa_obra_ots = {}
    for ot_id_r, obra_r, titulo_r in ots_activas:
        if not obra_r:
            continue
        mapa_obra_ots.setdefault(obra_r, []).append({
            "id": int(ot_id_r),
            "titulo": titulo_r or "",
        })
    
    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta charset="UTF-8">
    <meta charset="UTF-8">
    <style>
    body { font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }
    h2 { color: #333; border-bottom: 3px solid #fa709a; padding-bottom: 10px; }
    .btn { background: #667eea; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; }
    .top-actions { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
    .btn-remito { background: #f97316; }
    .btn-remito:hover { background: #ea580c; }
    form { background: white; padding: 20px; border-radius: 5px; max-width: 1100px; margin: 20px 0; }
    input, select, textarea { width: 100%; padding: 10px; margin: 8px 0; border: 1px solid #ddd; border-radius: 4px; box-sizing: border-box; }
    label { display: block; font-weight: bold; margin-top: 15px; }
    button { width: 100%; padding: 12px; background: #ff9800; color: white; border: none; border-radius: 4px; cursor: pointer; margin-top: 20px; font-weight: bold; font-size: 14px; }
    button:hover { background: #fb8c00; }
    .items-table { width: 100%; margin-top: 15px; box-shadow: 0 2px 5px rgba(0,0,0,0.08); }
    .items-table th { background: #fb7185; }
    .items-table td textarea { min-height: 44px; margin: 0; }
    </style>
    </head>
    <body>
    <div class="top-actions">
        <a href="/modulo/calidad" class="btn">⬅️ Volver</a>
        <a href="/modulo/remito" class="btn btn-remito">🚚 Ir a Remitos</a>
    </div>
    <h2>📋 Control Recepción de Materiales</h2>
    <p style="background:#e3f2fd; color:#0d47a1; padding:10px; border-radius:5px;"><b>Estados:</b> Conforme &nbsp; | &nbsp; No conforme &nbsp; | &nbsp; No aplica</p>
    
    <form method="post">
        <label>Obra:</label>
        <select name="obra" required>
            <option value="">Seleccionar obra...</option>
    """
    
    for obra in obras:
        html += f'<option value="{obra[0]}">{obra[0]}</option>'
    
    html += """
        </select>

        <label>OT:</label>
        <select name="ot_id" id="ot_id_recepcion" required>
            <option value="">Seleccionar OT...</option>
        </select>

        <label>Proveedor:</label>
        <input type="text" name="proveedor" placeholder="Nombre del proveedor" required>

        <label>Remito asociado:</label>
        <input type="text" name="remito_asociado" placeholder="Ej: REM-000123" required>

        <label>Responsable:</label>
        <select name="responsable" id="responsable_select" required>
            <option value="">-- Seleccionar responsable --</option>
            {opciones_responsables}
        </select>

        <label>Firma digital:</label>
        <input type="text" id="firma_digital_input" name="firma_digital" placeholder="Se completa automaticamente al seleccionar responsable" readonly required>
        <img id="firma_preview" src="" alt="Firma Responsable" style="display:none; margin-top:8px; max-width:280px; border:1px solid #ddd; border-radius:6px; background:white; padding:6px;" onerror="this.style.display='none';">

        <table class="items-table">
            <tr>
                <th style="width: 80px;">N°</th>
                <th>Control</th>
                <th style="width: 220px;">Estado</th>
                <th>Observación</th>
            </tr>
    """

    for item in CONTROL_RECEPCION_ITEMS:
        index = item["n"]
        item_label = f"<b>{html_lib.escape(item['tipo'])}</b><br>{html_lib.escape(item['detalle'])}"
        html += f"""
            <tr>
                <td><b>{index}</b></td>
                <td>{item_label}</td>
                <td>
                    <select name="estado_{index}" required>
                        <option value="">Seleccionar...</option>
                        <option value="CONFORME">Conforme</option>
                        <option value="NO CONFORME">No conforme</option>
                        <option value="NO APLICA">No aplica</option>
                    </select>
                </td>
                <td>
                    <textarea name="observacion_{index}" rows="2" placeholder="Observación del item {index}..."></textarea>
                </td>
            </tr>
        """

    html += """
        </table>
        
        <label>Fecha de Recepción:</label>
        <input type="date" name="fecha" required>

        <button type="submit">📄 Generar PDF Recepción</button>
    </form>
    <script>
    (function() {
        const responsableSel = document.getElementById('responsable_select');
        const firmaInput = document.getElementById('firma_digital_input');
        const firmaPreview = document.getElementById('firma_preview');
        const firmasResponsables = {json.dumps(firmas_responsables, ensure_ascii=False)};
        const imagenesResponsables = {json.dumps(imagenes_responsables, ensure_ascii=False)};
        if (!responsableSel || !firmaInput) return;

        function syncResponsable() {
            const responsable = responsableSel.value || '';
            const firma = firmasResponsables[responsable] || '';
            const firmaUrl = imagenesResponsables[responsable] || '';
            firmaInput.value = firma;
            firmaInput.readOnly = true;
            if (firmaPreview) {
                if (firmaUrl) {
                    firmaPreview.src = firmaUrl;
                    firmaPreview.style.display = 'block';
                } else {
                    firmaPreview.style.display = 'none';
                }
            }
        }

        responsableSel.addEventListener('change', syncResponsable);
        syncResponsable();
    })();
    </script>
    </body>
    </html>
    """
    html = html.replace("{opciones_responsables}", opciones_responsables)
    html = html.replace("{json.dumps(firmas_responsables, ensure_ascii=False)}", json.dumps(firmas_responsables, ensure_ascii=False))
    html = html.replace("{json.dumps(imagenes_responsables, ensure_ascii=False)}", json.dumps(imagenes_responsables, ensure_ascii=False))
    return html

# ======================
# SUB-MÁ“DULO CONTROL DE DESPACHO
# ======================
@calidad_bp.route("/modulo/calidad/despacho", methods=["GET", "POST"])
def calidad_despacho():
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Image, Paragraph, PageBreak, Spacer
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import mm, cm
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from io import BytesIO
    import os
    
    db = get_db()
    responsables_control = _obtener_responsables_control(db)
    firmas_responsables = {k: v.get("firma", "") for k, v in responsables_control.items()}
    imagenes_responsables = {k: v.get("firma_url", "") for k, v in responsables_control.items()}
    opciones_responsables = "".join(
        f'<option value="{html_lib.escape(nombre)}">{html_lib.escape(nombre)}</option>'
        for nombre in sorted(responsables_control.keys(), key=lambda x: x.lower())
    )
    
    if request.method == "POST":
        obra = (request.form.get("obra") or "").strip()
        responsable = (request.form.get("responsable") or "").strip()
        firma_form = (request.form.get("firma_digital") or "").strip()
        remito_asociado = (request.form.get("remito_asociado") or "").strip()
        fecha = request.form.get("fecha")
        ot_id_txt = (request.form.get("ot_id") or "").strip()
        detalle_items = []
        conteo = {"CONFORME": 0, "NO CONFORME": 0, "NO APLICA": 0}
        
        if not all([obra, responsable, remito_asociado, fecha, ot_id_txt]):
            return "Faltan datos requeridos", 400

        if not ot_id_txt.isdigit():
            return "Seleccioná una OT válida", 400

        ot_id_doc = int(ot_id_txt)
        ot_valida = db.execute(
            """
            SELECT id FROM ordenes_trabajo
            WHERE id = ?
              AND TRIM(COALESCE(obra, '')) = TRIM(COALESCE(?, ''))
              AND fecha_cierre IS NULL
              AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
            LIMIT 1
            """,
            (ot_id_doc, obra),
        ).fetchone()
        if not ot_valida:
            return "La OT seleccionada no corresponde a la obra", 400

        if responsable not in firmas_responsables:
            return "Seleccioná un responsable válido", 400

        firma_digital = firmas_responsables.get(responsable, "")
        if not firma_digital or firma_form != firma_digital:
            return "La firma es obligatoria y se completa automáticamente al seleccionar responsable", 400

        firma_path_responsable = _ruta_firma_responsable(responsables_control, responsable)

        for index, item_label in enumerate(CONTROL_DESPACHO_ITEMS, start=1):
            estado = (request.form.get(f"estado_{index}") or "").strip().upper()
            observacion = (request.form.get(f"observacion_{index}") or "").strip()
            if estado not in ("CONFORME", "NO CONFORME", "NO APLICA"):
                return f"Falta completar el estado del {item_label}", 400
            conteo[estado] += 1
            detalle_items.append({
                "item": index,
                "label": item_label,
                "estado": estado,
                "observacion": observacion,
            })

        if conteo["NO CONFORME"] > 0:
            resultado_general = "NO CONFORME"
        elif conteo["CONFORME"] > 0:
            resultado_general = "CONFORME"
        else:
            resultado_general = "NO APLICA"

        # Crear PDF directamente
        pdf_buffer = BytesIO()
        doc = SimpleDocTemplate(
            pdf_buffer,
            pagesize=letter,
            topMargin=0.3*cm,
            bottomMargin=0.5*cm,
            leftMargin=0.5*cm,
            rightMargin=0.5*cm
        )
        
        elements = []
        styles = getSampleStyleSheet()
        
        # Estilos personalizados
        title_style = ParagraphStyle(
            'TitleStyle',
            parent=styles['Heading1'],
            fontSize=18,
            textColor=colors.HexColor('#000000'),
            alignment=1,
            spaceAfter=0,
            fontName='Helvetica-Bold'
        )
        
        cell_style = ParagraphStyle(
            'CellStyle',
            parent=styles['Normal'],
            fontSize=8.5,
            textColor=colors.HexColor('#333333'),
            alignment=0,
            leading=12
        )
        
        header_cell_style = ParagraphStyle(
            'HeaderCellStyle',
            parent=styles['Normal'],
            fontSize=7,
            textColor=colors.HexColor('#1f2937'),
            alignment=1,
            leading=9,
            fontName='Helvetica-Bold'
        )

        head_style = ParagraphStyle(
            'HeadDesp',
            parent=styles['Normal'],
            fontSize=7.2,
            leading=8.2,
            alignment=1,
            fontName='Helvetica-Bold',
            textColor=colors.white,
        )
        
        # ====== ENCABEZADO DESDE IMAGEN (sin modificar diseño) ======
        encabezado_path = None
        posibles_encabezados = [
            "encabezado_despacho.png",
            "ENCABEZADO_DESPACHO.png",
            "encabezado_despacho.jpg",
            "ENCABEZADO_DESPACHO.jpg",
            "encabezado_despacho.jpeg",
            "ENCABEZADO_DESPACHO.jpeg",
        ]
        for nombre_archivo in posibles_encabezados:
            candidato = os.path.join(_APP_DIR, nombre_archivo)
            if os.path.exists(candidato):
                encabezado_path = candidato
                break

        if encabezado_path:
            encabezado_img = Image(encabezado_path)
            max_width = 19.8 * cm
            max_height = 3.2 * cm
            if encabezado_img.drawWidth > max_width:
                escala = max_width / float(encabezado_img.drawWidth)
                encabezado_img.drawWidth = encabezado_img.drawWidth * escala
                encabezado_img.drawHeight = encabezado_img.drawHeight * escala
            if encabezado_img.drawHeight > max_height:
                escala_h = max_height / float(encabezado_img.drawHeight)
                encabezado_img.drawWidth = encabezado_img.drawWidth * escala_h
                encabezado_img.drawHeight = encabezado_img.drawHeight * escala_h
            elements.append(encabezado_img)
        else:
            # Fallback: mantener encabezado armado en tabla si la imagen aÁºn no existe en disco.
            logo_path = os.path.join(_APP_DIR, "LOGO.png")
            logo_width = 2.5*cm
            logo_height = 2*cm

            logo_cell = ""
            if os.path.exists(logo_path):
                try:
                    logo_cell = Image(logo_path, width=logo_width, height=logo_height)
                except Exception:
                    logo_cell = Paragraph("A3", header_cell_style)
            else:
                logo_cell = Paragraph("A3", header_cell_style)

            title_cell = Paragraph("CONTROL FINAL DE DESPACHO", title_style)
            codigo_cell = Paragraph("<b>Código<br/>7-9.5</b>", header_cell_style)

            header_table_data = [[logo_cell, title_cell, codigo_cell]]
            header_table = Table(header_table_data, colWidths=[2.8*cm, 11*cm, 2.5*cm])
            header_table.setStyle(TableStyle([
                ('ALIGN', (0, 0), (0, 0), 'CENTER'),
                ('ALIGN', (1, 0), (1, 0), 'CENTER'),
                ('ALIGN', (2, 0), (2, 0), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('LEFTPADDING', (0, 0), (-1, -1), 3),
                ('RIGHTPADDING', (0, 0), (-1, -1), 3),
                ('TOPPADDING', (0, 0), (-1, -1), 2),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ]))
            elements.append(header_table)

            # Segunda fila: Revisó, Aprobó, Fecha, Revisión, Página
            info_data = [[
                Paragraph("<b>Revisó:<br/>MF</b>", header_cell_style),
                Paragraph("<b>Aprobó:<br/>GI</b>", header_cell_style),
                Paragraph("<b>Fecha:<br/>10/12/2025</b>", header_cell_style),
                Paragraph("<b>Revisión:<br/>01</b>", header_cell_style),
                Paragraph("<b>Página 1 de 1</b>", header_cell_style),
            ]]
            info_table = Table(info_data, colWidths=[2.3*cm, 2.3*cm, 2.6*cm, 2.3*cm, 2.9*cm])
            info_table.setStyle(TableStyle([
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('LEFTPADDING', (0, 0), (-1, -1), 2),
                ('RIGHTPADDING', (0, 0), (-1, -1), 2),
                ('TOPPADDING', (0, 0), (-1, -1), 2),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 0), (-1, -1), 7),
                ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
            ]))
            elements.append(info_table)
        elements.append(Spacer(1, 0.28*cm))

        # Datos básicos mejor distribuidos debajo del encabezado
        info_style = ParagraphStyle(
            'InfoDespacho',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.HexColor('#1f2937'),
            leading=11
        )
        info_data = [
            [Paragraph(f"<b>OBRA:</b> {obra}", info_style), Paragraph(f"<b>Responsable:</b> {responsable}", info_style)],
            [Paragraph(f"<b>Remito asociado:</b> {remito_asociado}", info_style), Paragraph(f"<b>Fecha:</b> {fecha}", info_style)],
        ]
        info_table = Table(info_data, colWidths=[8.2*cm, 8.3*cm])
        info_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#fffaf5')),
            ('BOX', (0, 0), (-1, -1), 0.6, colors.HexColor('#fed7aa')),
            ('INNERGRID', (0, 0), (-1, -1), 0.35, colors.HexColor('#fed7aa')),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        elements.append(info_table)
        elements.append(Spacer(1, 0.35*cm))
        
        # Tabla de control con 10 items
        table_data = [
            [Paragraph("<b>N°</b>", head_style), 
             Paragraph("<b>CONTROL DE DESPACHO</b>", head_style), 
             Paragraph("<b>VERIFICA</b>", head_style), 
             Paragraph("<b>OBSERVACIÁ“N</b>", head_style)]
        ]
        
        for item in detalle_items:
            item_num = item.get("item", "")
            label = item.get("label", "")
            estado = item.get("estado", "")
            
            observacion = item.get("observacion", "")
            
            table_data.append([
                Paragraph(f"<b>{item_num}</b>", cell_style),
                Paragraph(label, cell_style),
                Paragraph(f"<b>{estado}</b>", ParagraphStyle('EstadoDespacho', parent=cell_style, alignment=1, fontName='Helvetica-Bold')),
                Paragraph(observacion or "", cell_style)
            ])
        
        # Crear tabla
        control_table = Table(table_data, colWidths=[0.7*cm, 8.8*cm, 2.9*cm, 4*cm])
        control_table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f97316')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('GRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#cbd5e1')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fff7ed')]),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
        ]))
        elements.append(control_table)
        elements.append(Spacer(1, 0.3*cm))
        
        # Firma fija al pie para liberar espacio Áºtil en la hoja
        def draw_footer_signature(canvas, doc_obj):
            x_left = doc_obj.leftMargin
            x_right = doc_obj.pagesize[0] - doc_obj.rightMargin
            x_center = (x_left + x_right) / 2

            y_line = 2.2 * cm
            y_text = 1.65 * cm
            y_img = 2.55 * cm

            canvas.saveState()
            canvas.setStrokeColor(colors.HexColor('#333333'))
            canvas.setLineWidth(1)
            canvas.line(x_center - 42 * mm, y_line, x_center + 42 * mm, y_line)

            if firma_path_responsable and os.path.isfile(firma_path_responsable):
                try:
                    canvas.drawImage(
                        firma_path_responsable,
                        x_center - (52 * mm),
                        y_img,
                        width=104 * mm,
                        height=20 * mm,
                        preserveAspectRatio=True,
                        mask='auto'
                    )
                except Exception:
                    pass

            canvas.setFont('Helvetica-Bold', 9)
            canvas.setFillColor(colors.HexColor('#333333'))
            canvas.drawCentredString(x_center, y_text, f'Responsable: {responsable}')
            canvas.restoreState()
        
        # Construir PDF
        doc.build(elements, onFirstPage=draw_footer_signature, onLaterPages=draw_footer_signature)
        pdf_buffer.seek(0)
        
        # Generar nombre de archivo
        filename = f"Despacho_OT_{ot_id_doc}_{obra}_{fecha}.pdf"
        filename = filename.replace(" ", "_").replace("/", "-")

        _guardar_pdf_databook(obra, "calidad_despacho", filename, pdf_buffer.getvalue(), ot_id=ot_id_doc)
        pdf_buffer.seek(0)
        
        return send_file(
            pdf_buffer,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=filename
        )
    
    obras = db.execute("""
        SELECT DISTINCT obra
        FROM ordenes_trabajo
                WHERE fecha_cierre IS NULL
                    AND obra IS NOT NULL AND TRIM(obra) <> ''
        ORDER BY obra ASC
    """).fetchall()
    ots_activas = db.execute(
        """
        SELECT id, TRIM(COALESCE(obra, '')) AS obra, TRIM(COALESCE(titulo, '')) AS titulo
        FROM ordenes_trabajo
        WHERE fecha_cierre IS NULL
          AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
          AND TRIM(COALESCE(obra, '')) <> ''
        ORDER BY id DESC
        """
    ).fetchall()
    mapa_obra_ots = {}
    for ot_id_r, obra_r, titulo_r in ots_activas:
        mapa_obra_ots.setdefault(str(obra_r), []).append({
            "id": int(ot_id_r),
            "titulo": str(titulo_r or "").strip(),
        })
    
    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta charset="UTF-8">
    <meta charset="UTF-8">
    <style>
    body { font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }
    h2 { color: #333; border-bottom: 3px solid #fa709a; padding-bottom: 10px; }
    .btn { background: #667eea; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; }
    .top-actions { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
    .btn-remito { background: #f97316; }
    .btn-remito:hover { background: #ea580c; }
    form { background: white; padding: 20px; border-radius: 5px; max-width: 1100px; margin: 20px 0; }
    input, select, textarea { width: 100%; padding: 10px; margin: 8px 0; border: 1px solid #ddd; border-radius: 4px; box-sizing: border-box; }
    label { display: block; font-weight: bold; margin-top: 15px; }
    button { width: 100%; padding: 12px; background: #ff9800; color: white; border: none; border-radius: 4px; cursor: pointer; margin-top: 20px; font-weight: bold; font-size: 14px; }
    button:hover { background: #fb8c00; }
    .items-table { width: 100%; margin-top: 15px; box-shadow: 0 2px 5px rgba(0,0,0,0.08); }
    .items-table th { background: #fb7185; }
    .items-table td textarea { min-height: 44px; margin: 0; }
    </style>
    </head>
    <body>
    <div class="top-actions">
        <a href="/modulo/calidad" class="btn">⬅️ Volver</a>
        <a href="/modulo/remito" class="btn btn-remito">🚚 Ir a Remitos</a>
    </div>
    <h2>📦¦ Control Despacho</h2>
    
    <form method="post">
        <label>Obra:</label>
        <select name="obra" id="obra_despacho" onchange="cargarOTsDespacho()" required>
            <option value="">Seleccionar obra...</option>
    """
    
    for obra in obras:
        html += f'<option value="{obra[0]}">{obra[0]}</option>'
    
    html += """
        </select>

        <label>OT:</label>
        <select name="ot_id" id="ot_id_despacho" required>
            <option value="">Seleccionar OT...</option>
        </select>

        <label>Remito asociado:</label>
        <input type="text" name="remito_asociado" placeholder="Ej: R-000123" required>
        
        <label>Responsable:</label>
        <select name="responsable" id="responsable_select" required>
            <option value="">-- Seleccionar responsable --</option>
            {opciones_responsables}
        </select>

        <label>Firma digital:</label>
        <input type="text" id="firma_digital_input" name="firma_digital" placeholder="Se completa automaticamente al seleccionar responsable" readonly required>
        <img id="firma_preview" src="" alt="Firma Responsable" style="display:none; margin-top:8px; max-width:280px; border:1px solid #ddd; border-radius:6px; background:white; padding:6px;" onerror="this.style.display='none';">

        <table class="items-table">
            <tr>
                <th style="width: 80px;">N°</th>
                <th>Control</th>
                <th style="width: 220px;">Estado</th>
                <th>Observación</th>
            </tr>
    """

    for index, item_label in enumerate(CONTROL_DESPACHO_ITEMS, start=1):
        html += f"""
            <tr>
                <td><b>{index}</b></td>
                <td>{item_label}</td>
                <td>
                    <select name="estado_{index}" required>
                        <option value="">Seleccionar...</option>
                        <option value="CONFORME">Conforme</option>
                        <option value="NO CONFORME">No conforme</option>
                        <option value="NO APLICA">No aplica</option>
                    </select>
                </td>
                <td>
                    <textarea name="observacion_{index}" rows="2" placeholder="Observación del item {index}..."></textarea>
                </td>
            </tr>
        """

    html += """
        </table>
        
        <label>Fecha de Despacho:</label>
        <input type="date" name="fecha" required>
        
        <button type="submit">	📄 Generar PDF Despacho</button>
    </form>
    <script>
    (function() {
        const obraSel = document.getElementById('obra_despacho');
        const otSel = document.getElementById('ot_id_despacho');
        const responsableSel = document.getElementById('responsable_select');
        const firmaInput = document.getElementById('firma_digital_input');
        const firmaPreview = document.getElementById('firma_preview');
        const mapaObraOts = {json.dumps(mapa_obra_ots, ensure_ascii=False)};
        const firmasResponsables = {json.dumps(firmas_responsables, ensure_ascii=False)};
        const imagenesResponsables = {json.dumps(imagenes_responsables, ensure_ascii=False)};
        if (!responsableSel || !firmaInput) return;

        function cargarOTsDespacho() {
            if (!obraSel || !otSel) return;
            const obra = obraSel.value || '';
            const lista = mapaObraOts[obra] || [];
            otSel.innerHTML = '<option value="">Seleccionar OT...</option>';
            for (const ot of lista) {
                const opt = document.createElement('option');
                opt.value = String(ot.id || '');
                const titulo = String(ot.titulo || '').trim();
                opt.textContent = titulo ? `OT ${ot.id} - ${titulo}` : `OT ${ot.id}`;
                otSel.appendChild(opt);
            }
        }

        function syncResponsable() {
            const responsable = responsableSel.value || '';
            const firma = firmasResponsables[responsable] || '';
            const firmaUrl = imagenesResponsables[responsable] || '';
            firmaInput.value = firma;
            firmaInput.readOnly = true;
            if (firmaPreview) {
                if (firmaUrl) {
                    firmaPreview.src = firmaUrl;
                    firmaPreview.style.display = 'block';
                } else {
                    firmaPreview.style.display = 'none';
                }
            }
        }

        window.cargarOTsDespacho = cargarOTsDespacho;
        if (obraSel) obraSel.addEventListener('change', cargarOTsDespacho);
        cargarOTsDespacho();
        responsableSel.addEventListener('change', syncResponsable);
        syncResponsable();
    })();
    </script>
    </body>
    </html>
    """
    html = html.replace("{opciones_responsables}", opciones_responsables)
    html = html.replace("{json.dumps(firmas_responsables, ensure_ascii=False)}", json.dumps(firmas_responsables, ensure_ascii=False))
    html = html.replace("{json.dumps(imagenes_responsables, ensure_ascii=False)}", json.dumps(imagenes_responsables, ensure_ascii=False))
    html = html.replace("{json.dumps(mapa_obra_ots, ensure_ascii=False)}", json.dumps(mapa_obra_ots, ensure_ascii=False))
    return html

# ======================
@calidad_bp.route("/modulo/calidad/escaneo", methods=["GET"])
def calidad_escaneo():
    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta charset="UTF-8">
    <style>
    * { box-sizing: border-box; }
    body { font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }
    h2 { color: #333; border-bottom: 3px solid #4facfe; padding-bottom: 10px; }
    .info { background: #e3f2fd; padding: 15px; border-radius: 5px; margin-bottom: 15px; color: #0d47a1; }
    .btn { display: inline-block; background: #4facfe; color: white; padding: 12px 20px;
           text-decoration: none; border-radius: 5px; margin-top: 10px; border: none; cursor: pointer; font-size: 16px; }
    .btn:hover { background: #2a7aad; }
    .btn-secondary { background: #43e97b; }
    .btn-secondary:hover { background: #2cc96e; }
    .btn-danger { background: #f44336; }
    .btn-danger:hover { background: #da190b; }

    #qr-reader { width: 100%; max-width: 500px; margin: 20px 0; }
    #qr-reader > * { max-width: 100% !important; }

    .scanner-container { display: flex; flex-direction: column; max-width: 500px; }
    .button-group { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 15px; }
    .button-group > * { flex: 1; min-width: 150px; }

    #manual-input-section {
        background: white; padding: 20px; border-radius: 5px; margin-top: 15px; display: none;
    }
    #manual-input-section.show { display: block; }

    input[type="text"] { width: 100%; padding: 15px; margin: 10px 0; border: 2px solid #4facfe;
            border-radius: 4px; font-size: 18px; }
    input[type="text"]:focus { outline: none; border-color: #2a7aad; box-shadow: 0 0 5px #4facfe; }

    .error-msg { background: #ffcccc; color: red; padding: 12px; border-radius: 5px; margin-bottom: 15px; display: none; }
    .error-msg.show { display: block; }
    .success-msg { background: #ccffcc; color: green; padding: 12px; border-radius: 5px; margin-bottom: 15px; display: none; }
    .success-msg.show { display: block; }

    .submodulos-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 20px; margin: 30px 0; }
    .submodulo-btn { display: flex; flex-direction: column; align-items: center; justify-content: center; 
                     background: white; border: 2px solid #4facfe; border-radius: 10px; padding: 30px 20px; 
                     text-decoration: none; color: #333; transition: all 0.3s ease; cursor: pointer; }
    .submodulo-btn:hover { background: #4facfe; color: white; transform: translateY(-5px); box-shadow: 0 5px 15px rgba(79, 172, 254, 0.4); }
    .submodulo-btn .icono { font-size: 48px; margin-bottom: 15px; }
    .submodulo-btn .texto { font-size: 18px; font-weight: bold; text-align: center; }
    .btn-danger { background: #f44336; }
    .btn-danger:hover { background: #da190b; }

    #qr-reader { width: 100%; max-width: 500px; margin: 20px 0; }
    #qr-reader > * { max-width: 100% !important; }

    .scanner-container { display: flex; flex-direction: column; max-width: 500px; }
    .button-group { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 15px; }
    .button-group > * { flex: 1; min-width: 150px; }

    #manual-input-section {
        background: white; padding: 20px; border-radius: 5px; margin-top: 15px; display: none;
    }
    #manual-input-section.show { display: block; }

    input[type="text"] { width: 100%; padding: 15px; margin: 10px 0; border: 2px solid #4facfe;
            border-radius: 4px; font-size: 18px; }
    input[type="text"]:focus { outline: none; border-color: #2a7aad; box-shadow: 0 0 5px #4facfe; }

    .error-msg { background: #ffcccc; color: red; padding: 12px; border-radius: 5px; margin-bottom: 15px; display: none; }
    .error-msg.show { display: block; }
    .success-msg { background: #ccffcc; color: green; padding: 12px; border-radius: 5px; margin-bottom: 15px; display: none; }
    .success-msg.show { display: block; }
    </style>
    </head>
    <body>
    <h2>📱 Control Producción - Escaneo QR</h2>
    <div class="info"><strong>Seleccioná el sub módulo a utilizar:</strong></div>

    <div class="submodulos-grid">
        <a class="submodulo-btn submodulo-escaneo" href="/modulo/calidad/escaneo/qr">
            <span class="icono">📱</span>
            <span class="texto">ESCANEO QR</span>
        </a>
        <a class="submodulo-btn submodulo-armado" href="/modulo/calidad/escaneo/form-armado-soldadura">
            <span class="icono">🧩</span>
            <span class="texto">FORM ARMADO<br>Y SOLDADURA</span>
        </a>
        <a class="submodulo-btn submodulo-pintura" href="/modulo/calidad/escaneo/control-pintura">
            <span class="icono">🎨</span>
            <span class="texto">FORM<br>PINTURA</span>
        </a>
    </div>

    <div style="margin-top: 20px;">
        <a href="/modulo/calidad" class="btn">⬅️ Volver</a>
    </div>
    </body>
    </html>
    """
    return html


@calidad_bp.route("/modulo/calidad/escaneo/qr", methods=["GET", "POST"])
def calidad_escaneo_qr():
    if request.method == "POST":
        qr_data = request.form.get("qr_code", "").strip()

        if not qr_data:
            return redirect("/modulo/calidad/escaneo/qr")

        redirect_url = construir_redirect_desde_qr(qr_data)
        if not redirect_url:
            return redirect("/modulo/calidad/escaneo/qr")

        return redirect(redirect_url)

    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta charset="UTF-8">
    <script src="https://unpkg.com/html5-qrcode"></script>
    <style>
    * { box-sizing: border-box; }
    body { font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }
    h2 { color: #333; border-bottom: 3px solid #4facfe; padding-bottom: 10px; }
    .info { background: #e3f2fd; padding: 15px; border-radius: 5px; margin-bottom: 15px; color: #0d47a1; }
    .btn { display: inline-block; background: #4facfe; color: white; padding: 12px 20px;
           text-decoration: none; border-radius: 5px; margin-top: 10px; border: none; cursor: pointer; font-size: 16px; }
    .btn:hover { background: #2a7aad; }
    .btn-secondary { background: #43e97b; }
    .btn-secondary:hover { background: #2cc96e; }
    .btn-danger { background: #f44336; }
    .btn-danger:hover { background: #da190b; }

    #qr-reader { width: 100%; max-width: 500px; margin: 20px 0; }
    #qr-reader > * { max-width: 100% !important; }

    .scanner-container { display: flex; flex-direction: column; max-width: 500px; }
    .button-group { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 15px; }
    .button-group > * { flex: 1; min-width: 150px; }

    #manual-input-section {
        background: white; padding: 20px; border-radius: 5px; margin-top: 15px; display: none;
    }
    #manual-input-section.show { display: block; }

    input[type="text"] { width: 100%; padding: 15px; margin: 10px 0; border: 2px solid #4facfe;
            border-radius: 4px; font-size: 18px; }
    input[type="text"]:focus { outline: none; border-color: #2a7aad; box-shadow: 0 0 5px #4facfe; }

    .error-msg { background: #ffcccc; color: red; padding: 12px; border-radius: 5px; margin-bottom: 15px; display: none; }
    .error-msg.show { display: block; }
    .success-msg { background: #ccffcc; color: green; padding: 12px; border-radius: 5px; margin-bottom: 15px; display: none; }
    .success-msg.show { display: block; }
    </style>
    </head>
    <body>
    <h2>📱 ESCANEO QR</h2>

    <div class="info">
        <strong>📌 Instrucciones:</strong><br>
        1. Presiona "Iniciar Escaneo" para abrir la cámara<br>
        2. Apunta al código QR impreso en la pieza<br>
        3. El código se capturará automáticamente<br>
        4. Se abrirá la página de control de la pieza
    </div>

    <div class="error-msg" id="error-msg"></div>
    <div class="success-msg" id="success-msg"></div>

    <div class="scanner-container">
        <div id="qr-reader" style="display: none;"></div>

        <div class="button-group">
            <button class="btn btn-secondary" id="start-btn" onclick="startQRScan()">📷 Iniciar Escaneo</button>
            <button class="btn btn-danger" id="stop-btn" onclick="stopQRScan()" style="display: none;">⏹️ Detener</button>
            <button class="btn" onclick="toggleManualInput()">🔨 Escaneo Manual</button>
        </div>
    </div>

    <div id="manual-input-section">
        <form method="post">
            <label><strong>Ingresa el QR manualmente:</strong></label>
            <input type="text" name="qr_code" placeholder="Escanea o pega el QR aquí..." autofocus autocomplete="off">
            <button type="submit" class="btn btn-secondary">✓ Procesar QR</button>
        </form>
    </div>

    <div style="margin-top: 20px;">
        <a href="/modulo/calidad/escaneo" class="btn">⬅️ Volver a Sub Módulos</a>
    </div>

    <script>
    let html5QrcodeScanner = null;
    let isScanning = false;

    function startQRScan() {
        if (isScanning) return;

        const qrReaderDiv = document.getElementById('qr-reader');
        const startBtn = document.getElementById('start-btn');
        const stopBtn = document.getElementById('stop-btn');
        const errorMsg = document.getElementById('error-msg');

        errorMsg.classList.remove('show');
        errorMsg.textContent = '';

        qrReaderDiv.style.display = 'block';
        startBtn.style.display = 'none';
        stopBtn.style.display = 'inline-block';
        isScanning = true;

        html5QrcodeScanner = new Html5Qrcode("qr-reader");

        Html5Qrcode.getCameras().then(devices => {
            if (devices && devices.length) {
                const cameraId = devices[0].id;
                html5QrcodeScanner.start(
                    cameraId,
                    { fps: 10, qrbox: 250 },
                    onQRCodeScanned,
                    onQRCodeError
                );
            }
        }).catch(err => {
            showError('Error al acceder a la cámara: ' + err);
            stopQRScan();
        });
    }

    function stopQRScan() {
        if (html5QrcodeScanner && isScanning) {
            html5QrcodeScanner.stop().then(() => {
                document.getElementById('qr-reader').style.display = 'none';
                document.getElementById('start-btn').style.display = 'inline-block';
                document.getElementById('stop-btn').style.display = 'none';
                isScanning = false;
            });
        }
    }

    function onQRCodeScanned(decodedText, decodedResult) {
        if (!isScanning) return;

        showSuccess('QR detectado: ' + decodedText);
        stopQRScan();
        processQR(decodedText);
    }

    function onQRCodeError(error) {
        // Silenciar errores de escaneo constantes
    }

    function processQR(qrData) {
        fetch('/procesar-qr', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ qr_code: qrData })
        })
        .then(response => response.json())
        .then(data => {
            if (data.redirect) {
                window.location.href = data.redirect;
            } else if (data.error) {
                showError(data.error);
            }
        })
        .catch(err => showError('Error al procesar QR: ' + err));
    }

    function toggleManualInput() {
        const section = document.getElementById('manual-input-section');
        section.classList.toggle('show');
        if (section.classList.contains('show')) {
            section.querySelector('input').focus();
        }
    }

    function showError(message) {
        const errorMsg = document.getElementById('error-msg');
        errorMsg.textContent = 'âŒ ' + message;
        errorMsg.classList.add('show');
    }

    function showSuccess(message) {
        const successMsg = document.getElementById('success-msg');
        successMsg.textContent = '✅ ' + message;
        successMsg.classList.add('show');
        setTimeout(() => successMsg.classList.remove('show'), 3000);
    }
    </script>
    </body>
    </html>
    """
    return html


def _render_form_produccion_manual(titulo, procesos_permitidos):
    obra_qs = request.args.get("obra", "").strip()
    db = get_db()
    responsables_control = _obtener_responsables_control(db)
    operarios_disponibles = _obtener_operarios_disponibles(db)
    firmas_responsables = {k: v.get("firma", "") for k, v in responsables_control.items()}
    imagenes_responsables = {k: v.get("firma_url", "") for k, v in responsables_control.items()}
    opciones_responsables = "".join(
        f'<option value="{html_lib.escape(nombre)}">{html_lib.escape(nombre)}</option>'
        for nombre in sorted(responsables_control.keys(), key=lambda x: x.lower())
    )
    opciones_operarios = "".join(
        f'<option value="{html_lib.escape(nombre)}">{html_lib.escape(nombre)}</option>'
        for nombre in operarios_disponibles
    )

    if request.method == "POST":
        pos = (request.form.get("posicion") or "").strip()
        obra = (request.form.get("obra") or "").strip()
        proceso = (request.form.get("proceso") or "").strip().upper()
        fecha = (request.form.get("fecha") or "").strip()
        operario = (request.form.get("operario") or "").strip()
        estado = (request.form.get("estado") or "").strip().upper()
        accion = (request.form.get("accion") or request.form.get("reproceso") or "").strip()
        responsable = (request.form.get("responsable") or "").strip()
        re_fecha = (request.form.get("reinspeccion_fecha") or "").strip()
        re_operador = (request.form.get("reinspeccion_operador") or "").strip()
        re_estado = (request.form.get("reinspeccion_estado") or "").strip().upper()
        re_motivo = (request.form.get("reinspeccion_motivo") or "").strip()
        re_responsable = (request.form.get("reinspeccion_responsable") or "").strip()
        re_firma_form = (request.form.get("reinspeccion_firma") or "").strip()
        firma_form = (request.form.get("firma_digital") or "").strip()
        firma_digital = firmas_responsables.get(responsable, "")

        if responsable not in firmas_responsables:
            return "Seleccioná un responsable válido", 400

        if not firma_digital or firma_form != firma_digital:
            return "La firma es obligatoria en cada escaneo", 400

        if any([re_fecha, re_operador, re_estado, re_motivo, re_responsable, re_firma_form]):
            return "La Re-inspeccion se registra solo desde el botón Re-inspeccion", 400

        re_inspeccion = ""
        estado_pieza = _estado_pieza_persistente(estado, re_inspeccion)
        firma_evento = firma_digital
        firma_reinspeccion = ""
        if all([re_fecha, re_operador, re_estado, re_responsable, re_firma_form]):
            if re_responsable not in firmas_responsables:
                return "Seleccioná un responsable válido para la Re-inspeccion", 400
            firma_reinspeccion = firmas_responsables.get(re_responsable, "")
            if not firma_reinspeccion or re_firma_form != firma_reinspeccion:
                return "La firma es obligatoria para registrar la Re-inspeccion", 400
            re_inspeccion = _agregar_ciclo_reinspeccion(
                "",
                proceso,
                re_fecha,
                re_operador,
                re_estado,
                re_motivo,
                firma_reinspeccion,
                re_responsable,
            )
            estado_pieza = _estado_pieza_persistente(estado, re_inspeccion)
            firma_evento = firma_reinspeccion or firma_digital

        if not all([pos, proceso, fecha, operario, estado]):
            return "Faltan datos requeridos", 400

        if proceso not in procesos_permitidos:
            return "Proceso no permitido para este formulario", 400

        if estado not in ("OK", "NC", "OBS", "OM"):
            return "Estado invalido", 400

        if pieza_completada(pos, obra if obra else None):
            return "La pieza ya esta completada y no admite nuevos procesos", 400

        es_valido, mensaje = validar_siguiente_proceso(pos, proceso, obra if obra else None)
        if not es_valido:
            return mensaje, 400

        existe_proceso = db.execute(
            """
            SELECT 1 FROM procesos
            WHERE posicion=?
              AND COALESCE(obra, '') = COALESCE(?, '')
              AND UPPER(TRIM(COALESCE(proceso, ''))) = ?
            LIMIT 1
            """,
            (pos, obra or "", proceso.upper())
        ).fetchone()
        if existe_proceso:
            return f"El proceso {proceso} ya está cargado para esta pieza", 400

        cursor = db.execute(
            """
            INSERT INTO procesos (posicion, obra, proceso, fecha, operario, estado, reproceso, re_inspeccion, firma_digital, estado_pieza, escaneado_qr)
            VALUES (?,?,?,?,?,?,?,?,?,?,1)
            """,
            (pos, obra or None, proceso, fecha, operario, estado, accion, re_inspeccion, firma_digital, estado_pieza)
        )
        _registrar_trazabilidad(
            db,
            cursor.lastrowid,
            pos,
            obra,
            proceso,
            estado,
            estado_pieza,
            firma_evento,
            accion,
            re_inspeccion,
            "ALTA_CONTROL_MANUAL",
        )
        db.commit()

        redirect_url = f"/pieza/{quote(pos)}"
        if obra:
            redirect_url += f"?obra={quote(obra)}"
        return redirect(redirect_url)

    opciones_proceso = ""
    for proc in procesos_permitidos:
        opciones_proceso += f'<option value="{proc}">{proc}</option>'

    html = f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta charset="UTF-8">
    <style>
    body {{ font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }}
    h2 {{ color: #333; border-bottom: 3px solid #4facfe; padding-bottom: 10px; }}
    form {{ background: white; padding: 18px; border-radius: 8px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); max-width: 700px; }}
    label {{ display:block; font-weight:bold; margin-top: 12px; }}
    input, select {{ width: 100%; padding: 10px; margin-top: 6px; border: 1px solid #d1d5db; border-radius: 6px; }}
    button {{ margin-top: 16px; width: 100%; padding: 12px; border: none; border-radius: 6px; font-weight: bold; color: white; background: #16a34a; cursor: pointer; }}
    .btn-volver {{ display:inline-block; margin-top: 12px; text-decoration:none; background:#667eea; color:white; padding:10px 15px; border-radius:6px; }}
    .info {{ background:#e3f2fd; color:#1e3a8a; padding:12px; border-radius:6px; margin-bottom:12px; }}
    </style>
    </head>
    <body>
    <h2>{titulo}</h2>
    <div class="info">Completá el formulario manual para registrar el proceso de producción.</div>
    <form method="post">
        <label>Posición de pieza</label>
        <input type="text" name="posicion" required>

        <label>Obra (opcional)</label>
        <input type="text" name="obra" value="{obra_qs}">

        <label>Proceso</label>
        <select name="proceso" required>
            {opciones_proceso}
        </select>

        <label>Fecha</label>
        <input type="date" name="fecha" required>

        <label>Operario</label>
        <select name="operario" required>
            <option value="">-- Seleccionar operario --</option>
            {opciones_operarios}
        </select>

        <label>Estado</label>
        <select name="estado" required>
            <option value="OK">OK (APROBADO)</option>
            <option value="NC">NC (No conforme)</option>
            <option value="OBS">OBS (Observacion)</option>
            <option value="OM">OM (Oportunidad de mejora)</option>
        </select>

        <label>Accion</label>
        <input type="text" name="accion" placeholder="Detalle de accion (si aplica)">

        <label>Responsable</label>
        <select name="responsable" id="responsable_select" required>
            <option value="">-- Seleccionar responsable --</option>
            {opciones_responsables}
        </select>

        <label>Firma (digital)</label>
        <input type="text" id="firma_digital_input" name="firma_digital" placeholder="Se completa automaticamente al seleccionar responsable" readonly>
        <img id="firma_ok_preview" src="" alt="Firma" style="display:none; margin-top:8px; max-width:280px; border:1px solid #ddd; border-radius:6px; background:white; padding:6px;" onerror="this.style.display='none';">

        <div id="reinspeccion_block" style="margin-top:12px; background:#fff7ed; border:1px solid #fdba74; border-radius:6px; padding:10px;">
            <b>Re-inspeccion (solo desde botón Re-inspeccion)</b><br>
            <div class="form-group">
                <label>Fecha:</label>
                <input type="date" id="reinspeccion_fecha" name="reinspeccion_fecha">
            </div>
            <div class="form-group">
                <label>Operario:</label>
                <select id="reinspeccion_operador" name="reinspeccion_operador">
                    <option value="">-- Seleccionar operario --</option>
                    {opciones_operarios}
                </select>
            </div>
            <div class="form-group">
                <label>Responsable:</label>
                <select id="reinspeccion_responsable" name="reinspeccion_responsable">
                    <option value="">-- Seleccionar responsable --</option>
                    {opciones_responsables}
                </select>
            </div>
            <div class="form-group">
                <label>Firma re-inspeccion:</label>
                <input type="text" id="reinspeccion_firma" name="reinspeccion_firma" placeholder="Se completa automaticamente al seleccionar responsable" readonly>
                <img id="reinspeccion_firma_ok_preview" src="" alt="Firma Re-inspeccion" style="display:none; margin-top:8px; max-width:280px; border:1px solid #ddd; border-radius:6px; background:white; padding:6px;" onerror="this.style.display='none';">
            </div>
            <div class="form-group">
                <label>Estado:</label>
                <select id="reinspeccion_estado" name="reinspeccion_estado">
                    <option value="">-- Seleccionar --</option>
                    <option value="OK">OK (APROBADO)</option>
                    <option value="NC">NC (No conforme)</option>
                    <option value="OBS">OBS (Observacion)</option>
                    <option value="OM">OM (Oportunidad de mejora)</option>
                </select>
            </div>
            <div class="form-group">
                <label>Motivo (si corresponde):</label>
                <input type="text" id="reinspeccion_motivo" name="reinspeccion_motivo" placeholder="Motivo del resultado de re-inspeccion">
            </div>
        </div>

        <button type="submit">ðŸ’¾ Guardar Proceso</button>
    </form>

    <a class="btn-volver" href="/modulo/calidad/escaneo">⬅️ Volver a Escaneo QR</a>
    <script>
    (function() {{
        const sel = document.querySelector('select[name="estado"]');
        const responsableSel = document.getElementById('responsable_select');
        const firmaInput = document.getElementById('firma_digital_input');
        const firmaPreview = document.getElementById('firma_ok_preview');
        const reinspBlock = document.getElementById('reinspeccion_block');
        const reinspFields = [
            document.getElementById('reinspeccion_fecha'),
            document.getElementById('reinspeccion_operador'),
            document.getElementById('reinspeccion_responsable'),
            document.getElementById('reinspeccion_firma'),
            document.getElementById('reinspeccion_estado'),
            document.getElementById('reinspeccion_motivo'),
        ].filter(Boolean);
        const reinspResponsableSel = document.getElementById('reinspeccion_responsable');
        const reinspFirmaInput = document.getElementById('reinspeccion_firma');
        const reinspFirmaPreview = document.getElementById('reinspeccion_firma_ok_preview');
        const firmasResponsables = {json.dumps(firmas_responsables, ensure_ascii=False)};
        const imagenesResponsables = {json.dumps(imagenes_responsables, ensure_ascii=False)};
        if (!sel || !firmaInput || !responsableSel) return;

        function setReinspeccionActiva(activa) {{
            if (reinspBlock) reinspBlock.style.opacity = activa ? '1' : '0.55';
            reinspFields.forEach((el) => {{
                el.disabled = !activa;
                if (!activa) el.value = '';
            }});
            if (!activa && reinspFirmaPreview) reinspFirmaPreview.style.display = 'none';
        }}

        function syncResponsable() {{
            const responsable = responsableSel.value || '';
            const firma = firmasResponsables[responsable] || '';
            const firmaUrl = imagenesResponsables[responsable] || '';
            firmaInput.value = firma;
            firmaInput.readOnly = true;
            if (firmaPreview) {{
                if (firmaUrl) {{
                    firmaPreview.src = firmaUrl;
                    firmaPreview.style.display = 'block';
                }} else {{
                    firmaPreview.style.display = 'none';
                }}
            }}
        }}

        function syncReinspeccionResponsable() {{
            if (!reinspResponsableSel || !reinspFirmaInput) return;
            const responsable = reinspResponsableSel.value || '';
            const firma = firmasResponsables[responsable] || '';
            const firmaUrl = imagenesResponsables[responsable] || '';
            reinspFirmaInput.value = firma;
            reinspFirmaInput.readOnly = true;
            if (reinspFirmaPreview) {{
                if (firmaUrl) {{
                    reinspFirmaPreview.src = firmaUrl;
                    reinspFirmaPreview.style.display = 'block';
                }} else {{
                    reinspFirmaPreview.style.display = 'none';
                }}
            }}
        }}

        function syncFormulario() {{
            setReinspeccionActiva(false);
            syncResponsable();
            syncReinspeccionResponsable();
        }}

        responsableSel.addEventListener('change', syncResponsable);
        if (reinspResponsableSel) reinspResponsableSel.addEventListener('change', syncReinspeccionResponsable);
        sel.addEventListener('change', syncFormulario);
        syncResponsable();
        syncReinspeccionResponsable();
        syncFormulario();
    }})();
    </script>
    </body>
    </html>
    """
    return html


@calidad_bp.route("/modulo/calidad/escaneo/form-armado-soldadura", methods=["GET", "POST"])
def calidad_escaneo_form_armado_soldadura():
    from datetime import date
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

    db = get_db()
    es_obra = str(session.get("user_role") or "").strip().lower() == "obra"
    responsables_control = _obtener_responsables_control(db)
    responsable_por_firma = {
        str(data.get("firma", "")).strip().lower(): nombre
        for nombre, data in responsables_control.items()
        if str(data.get("firma", "")).strip()
    }
    imagen_por_firma = {
        str(data.get("firma", "")).strip().lower(): str(data.get("firma_url", "")).strip()
        for nombre, data in responsables_control.items()
        if str(data.get("firma", "")).strip() and str(data.get("firma_url", "")).strip()
    }

    def _pdf_firma_cell_as(firma_txt):
        """Devuelve Image de ReportLab si existe archivo, sino Paragraph."""
        if not firma_txt:
            return Paragraph("-", base_style)
        url = imagen_por_firma.get(firma_txt.lower(), "")
        if url and "/firma-supervisor/" in url:
            from urllib.parse import unquote as _uq
            archivo = _uq(url.rsplit("/", 1)[-1])
            ruta = os.path.join(_FIRMAS_EMPLEADOS_DIR, archivo)
            if os.path.isfile(ruta):
                try:
                    img = Image(ruta)
                    max_w, max_h = 2.0 * cm, 0.9 * cm
                    escala = min(max_w / float(img.drawWidth), max_h / float(img.drawHeight), 1.0)
                    img.drawWidth *= escala
                    img.drawHeight *= escala
                    return img
                except Exception:
                    pass
        return Paragraph(firma_txt or "-", base_style)

    def obtener_avance_ot(ot_id_sel, obra_sel):
        if not ot_id_sel:
            return []

        # Asegura metadatos completos para las piezas de la obra de la OT seleccionada.
        if obra_sel:
            _completar_metadatos_por_obra_pos(db, obra_sel)

        # Mapa base de posiciones de la OT (incluye piezas aun sin ARMADO/SOLDADURA)
        meta_rows = db.execute(
            """
            SELECT posicion, cantidad, perfil
            FROM procesos
            WHERE ot_id = ?
              AND eliminado = 0
            ORDER BY id DESC
            """,
            (ot_id_sel,)
        ).fetchall()
        meta_por_pos = {}
        for pos_meta, cantidad_meta, perfil_meta in meta_rows:
            pos_meta_key = (pos_meta or "").strip()
            if not pos_meta_key:
                continue
            if pos_meta_key not in meta_por_pos:
                meta_por_pos[pos_meta_key] = {
                    "cantidad": "",
                    "perfil": "",
                }
            if not meta_por_pos[pos_meta_key]["cantidad"] and cantidad_meta not in (None, ""):
                meta_por_pos[pos_meta_key]["cantidad"] = str(cantidad_meta)
            if not meta_por_pos[pos_meta_key]["perfil"] and perfil_meta not in (None, ""):
                meta_por_pos[pos_meta_key]["perfil"] = str(perfil_meta)

        avance = {}
        for pos_key, meta in meta_por_pos.items():
            avance[pos_key] = {
                "posicion": pos_key,
                "cantidad": meta.get("cantidad", ""),
                "perfil": meta.get("perfil", ""),
                "armado": "",
                "armado_fecha": "",
                "armado_responsable": "",
                "armado_firma_digital": "",
                "armado_firma_url": "",
                "armado_reinspeccion": "",
                "soldadura": "",
                "soldadura_fecha": "",
                "soldadura_responsable": "",
                "soldadura_firma_digital": "",
                "soldadura_firma_url": "",
                "soldadura_reinspeccion": "",
            }

        rows = db.execute(
            """
            SELECT posicion, proceso, estado, fecha, firma_digital, re_inspeccion, id, cantidad, perfil
            FROM procesos
            WHERE ot_id = ?
              AND eliminado = 0
              AND UPPER(TRIM(proceso)) IN ('ARMADO', 'SOLDADURA')
            ORDER BY id DESC
            """,
            (ot_id_sel,)
        ).fetchall()

        for pos, proceso, estado, fecha_reg, firma_digital, re_inspeccion, _, cantidad, perfil in rows:
            pos_key = (pos or "").strip()
            if not pos_key:
                continue

            proc_key = (proceso or "").strip().upper()
            if pos_key not in avance:
                avance[pos_key] = {
                    "posicion": pos_key,
                    "cantidad": "",
                    "perfil": "",
                    "armado": "",
                    "armado_fecha": "",
                    "armado_responsable": "",
                    "armado_firma_digital": "",
                    "armado_firma_url": "",
                    "armado_reinspeccion": "",
                    "soldadura": "",
                    "soldadura_fecha": "",
                    "soldadura_responsable": "",
                    "soldadura_firma_digital": "",
                    "soldadura_firma_url": "",
                    "soldadura_reinspeccion": "",
                }

            if not avance[pos_key]["cantidad"] and cantidad not in (None, ""):
                avance[pos_key]["cantidad"] = str(cantidad)
            if not avance[pos_key]["perfil"] and perfil not in (None, ""):
                avance[pos_key]["perfil"] = str(perfil)

            # Fallback: completar desde cualquier fila de la misma posicion/obra (QR/import).
            meta = meta_por_pos.get(pos_key, {})
            if not avance[pos_key]["cantidad"] and meta.get("cantidad"):
                avance[pos_key]["cantidad"] = meta.get("cantidad", "")
            if not avance[pos_key]["perfil"] and meta.get("perfil"):
                avance[pos_key]["perfil"] = meta.get("perfil", "")
            if proc_key == "ARMADO" and not avance[pos_key]["armado"]:
                avance[pos_key]["armado"] = (estado or "").strip().upper()
                avance[pos_key]["armado_fecha"] = (fecha_reg or "").strip()
                firma_txt = (firma_digital or "").strip()
                avance[pos_key]["armado_firma_digital"] = firma_txt
                avance[pos_key]["armado_responsable"] = responsable_por_firma.get(firma_txt.lower(), "-") if firma_txt else "-"
                avance[pos_key]["armado_firma_url"] = imagen_por_firma.get(firma_txt.lower(), "") if firma_txt else ""
                avance[pos_key]["armado_reinspeccion"] = (re_inspeccion or "").strip()
            elif proc_key == "SOLDADURA" and not avance[pos_key]["soldadura"]:
                avance[pos_key]["soldadura"] = (estado or "").strip().upper()
                avance[pos_key]["soldadura_fecha"] = (fecha_reg or "").strip()
                firma_txt = (firma_digital or "").strip()
                avance[pos_key]["soldadura_firma_digital"] = firma_txt
                avance[pos_key]["soldadura_responsable"] = responsable_por_firma.get(firma_txt.lower(), "-") if firma_txt else "-"
                avance[pos_key]["soldadura_firma_url"] = imagen_por_firma.get(firma_txt.lower(), "") if firma_txt else ""
                avance[pos_key]["soldadura_reinspeccion"] = (re_inspeccion or "").strip()

        return sorted(avance.values(), key=lambda x: x["posicion"])

    ots_db = db.execute(
        """
                SELECT id,
                             TRIM(COALESCE(obra, '')) AS obra,
                             TRIM(COALESCE(titulo, '')) AS titulo
        FROM ordenes_trabajo
        WHERE fecha_cierre IS NULL
          AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
          AND TRIM(COALESCE(obra, '')) <> ''
        ORDER BY id DESC
        """
    ).fetchall()
    ots = [{"id": int(r[0]), "obra": str(r[1] or "").strip(), "titulo": str(r[2] or "").strip()} for r in ots_db]

    ot_id_txt = (request.values.get("ot_id") or "").strip()
    ot_id = int(ot_id_txt) if ot_id_txt.isdigit() else None
    ot_sel = next((o for o in ots if o["id"] == ot_id), None)
    obra = ot_sel["obra"] if ot_sel else ""
    titulo_ot = ot_sel["titulo"] if ot_sel else ""
    rows_avance = obtener_avance_ot(ot_id, obra)
    mensaje = (request.args.get("mensaje") or "").strip()

    if request.method == "POST" and es_obra:
        return redirect("/modulo/calidad/escaneo/form-armado-soldadura?mensaje=" + quote("Solo visualizacion para usuario obra"))

    if request.method == "POST" and (request.form.get("accion") or "").strip().lower() == "pdf":
        ot_post_txt = (request.form.get("ot_id") or "").strip()
        ot_post = int(ot_post_txt) if ot_post_txt.isdigit() else None
        ot_post_data = next((o for o in ots if o["id"] == ot_post), None)
        if not ot_post_data:
            return redirect("/modulo/calidad/escaneo/form-armado-soldadura?mensaje=" + quote("⚠️ Seleccioná una OT"))

        ot_id = ot_post
        obra = ot_post_data["obra"]
        titulo_ot = ot_post_data.get("titulo", "")
        rows_avance = obtener_avance_ot(ot_id, obra)
        ot_label = f"OT {ot_id} - {obra}" + (f" - {titulo_ot}" if titulo_ot else "")

        pdf_buffer = BytesIO()
        doc = SimpleDocTemplate(
            pdf_buffer,
            pagesize=letter,
            topMargin=0.4*cm,
            bottomMargin=0.6*cm,
            leftMargin=0.5*cm,
            rightMargin=0.5*cm
        )

        elements = []
        styles = getSampleStyleSheet()
        base_style = ParagraphStyle('BaseAS', parent=styles['Normal'], fontSize=7.4, leading=8.6, textColor=colors.HexColor('#333333'))
        head_style = ParagraphStyle('HeadAS', parent=styles['Normal'], fontSize=7.2, leading=8.2, alignment=1, fontName='Helvetica-Bold', textColor=colors.white)

        encabezado_path = None
        candidatos = [
            os.path.join(_APP_DIR, "ENCABEZADO_ARMADO Y SOLDADURA.png"),
            os.path.join(_APP_DIR, "ENCABEZADO_ARMADO Y SOLDADURA.jpg"),
            os.path.join(_APP_DIR, "ENCABEZADO_ARMADO Y SOLDADURA.jpeg"),
            os.path.join(_APP_DIR, "ENCABEZADO_ARMADO_SOLDADURA.png"),
            os.path.join(_APP_DIR, "ENCABEZADO_ARMADO_SOLDADURA.jpg"),
            os.path.join(_APP_DIR, "ENCABEZADO_ARMADO_SOLDADURA.jpeg"),
            os.path.join(_APP_DIR, "encabezado_armado_soldadura.png"),
        ]
        for c in candidatos:
            if os.path.exists(c):
                encabezado_path = c
                break

        if encabezado_path:
            head_img = Image(encabezado_path)
            max_width = 19.8 * cm
            max_height = 3.2 * cm
            if head_img.drawWidth > max_width:
                ratio = max_width / float(head_img.drawWidth)
                head_img.drawWidth *= ratio
                head_img.drawHeight *= ratio
            if head_img.drawHeight > max_height:
                ratio_h = max_height / float(head_img.drawHeight)
                head_img.drawWidth *= ratio_h
                head_img.drawHeight *= ratio_h
            elements.append(head_img)
        else:
            logo_path = os.path.join(_APP_DIR, "LOGO.png")
            logo_cell = Image(logo_path, width=2.8*cm, height=2.2*cm) if os.path.exists(logo_path) else Paragraph("A3", base_style)
            title_cell = Paragraph("<b>CONTROL DE ARMADO Y SOLDADURA</b>", ParagraphStyle('TAS', parent=styles['Heading2'], alignment=1, textColor=colors.HexColor('#111827')))
            code_cell = Paragraph("<b>Código<br/>7-9.3</b>", ParagraphStyle('CAS', parent=base_style, alignment=1, fontName='Helvetica-Bold'))
            header = Table([[logo_cell, title_cell, code_cell]], colWidths=[4.8*cm, 12.0*cm, 3.0*cm])
            header.setStyle(TableStyle([
                ('GRID', (0,0), (-1,-1), 0.8, colors.HexColor('#111827')),
                ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
                ('ALIGN', (0,0), (-1,-1), 'CENTER'),
                ('TOPPADDING', (0,0), (-1,-1), 4),
                ('BOTTOMPADDING', (0,0), (-1,-1), 4),
            ]))
            elements.append(header)

        elements.append(Spacer(1, 0.2*cm))

        info = Table([
            [Paragraph(f"<b>OT:</b> {html_lib.escape(ot_label)}", base_style), Paragraph(f"<b>Fecha:</b> {date.today().isoformat()}", base_style)],
            [Paragraph(f"<b>Obra:</b> {obra}", base_style), Paragraph("", base_style)],
        ], colWidths=[9.9*cm, 9.9*cm])
        info.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#fff7ed')),
            ('BOX', (0,0), (-1,-1), 0.6, colors.HexColor('#fdba74')),
            ('INNERGRID', (0,0), (-1,-1), 0.35, colors.HexColor('#fed7aa')),
            ('LEFTPADDING', (0,0), (-1,-1), 6),
            ('RIGHTPADDING', (0,0), (-1,-1), 6),
            ('TOPPADDING', (0,0), (-1,-1), 5),
            ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ]))
        elements.append(info)
        elements.append(Spacer(1, 0.25*cm))
        elements.append(Paragraph("<b>Estado de Armado y Soldadura</b>", ParagraphStyle('SecAS', parent=styles['Normal'], fontSize=9.0, textColor=colors.HexColor('#9a3412'))))
        elements.append(Spacer(1, 0.08*cm))

        table_data = [[
            Paragraph("<b>Posición</b>", head_style),
            Paragraph("<b>Cantidad</b>", head_style),
            Paragraph("<b>Perfil</b>", head_style),
            Paragraph("<b>Estado Armado</b>", head_style),
            Paragraph("<b>Fecha Armado</b>", head_style),
            Paragraph("<b>Responsable Armado</b>", head_style),
            Paragraph("<b>Firma digital Armado</b>", head_style),
            Paragraph("<b>Estado Soldadura</b>", head_style),
            Paragraph("<b>Fecha Soldadura</b>", head_style),
            Paragraph("<b>Responsable Soldadura</b>", head_style),
            Paragraph("<b>Firma digital Soldadura</b>", head_style),
        ]]
        for r in rows_avance:
            table_data.append([
                Paragraph(r["posicion"], base_style),
                Paragraph(_format_cantidad_1_decimal(r["cantidad"]), base_style),
                Paragraph(r["perfil"] or "-", base_style),
                Paragraph(r["armado"] or "PENDIENTE", base_style),
                Paragraph(r["armado_fecha"] or "-", base_style),
                Paragraph(r["armado_responsable"] or "-", base_style),
                _pdf_firma_cell_as(r["armado_firma_digital"]),
                Paragraph(r["soldadura"] or "PENDIENTE", base_style),
                Paragraph(r["soldadura_fecha"] or "-", base_style),
                Paragraph(r["soldadura_responsable"] or "-", base_style),
                _pdf_firma_cell_as(r["soldadura_firma_digital"]),
            ])

        # Total ancho: 19.8cm (igual que área Áºtil del PDF)
        t = Table(table_data, colWidths=[1.5*cm, 1.1*cm, 2.3*cm, 1.45*cm, 1.75*cm, 1.9*cm, 2.35*cm, 1.45*cm, 1.75*cm, 1.9*cm, 2.35*cm], repeatRows=1)
        t.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#f97316')),
            ('TEXTCOLOR', (0,0), (-1,0), colors.white),
            ('GRID', (0,0), (-1,-1), 0.4, colors.HexColor('#cbd5e1')),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#fff7ed')]),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('LEFTPADDING', (0,0), (-1,-1), 4),
            ('RIGHTPADDING', (0,0), (-1,-1), 4),
            ('TOPPADDING', (0,0), (-1,-1), 4),
            ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ]))
        elements.append(t)
        elements.append(Spacer(1, 0.3*cm))

        reinsp_head = ParagraphStyle('ReinspHeadAS', parent=styles['Normal'], fontSize=7.0, leading=8.0, alignment=1, fontName='Helvetica-Bold', textColor=colors.white)
        reinsp_sub_head = ParagraphStyle('ReinspSubHeadAS', parent=styles['Normal'], fontSize=6.7, leading=7.8, alignment=1, fontName='Helvetica-Bold', textColor=colors.HexColor('#7c2d12'))
        reinsp_rows = [[
            Paragraph("<b>Posición</b>", reinsp_head),
            Paragraph("<b>📩 ARMADO</b>", reinsp_head), "", "", "", "",
            Paragraph("<b>⚡ SOLDADURA</b>", reinsp_head), "", "", "", "",
        ], [
            Paragraph("", reinsp_sub_head),
            Paragraph("<b>Estado</b>", reinsp_sub_head),
            Paragraph("<b>Fecha</b>", reinsp_sub_head),
            Paragraph("<b>Acción correctiva</b>", reinsp_sub_head),
            Paragraph("<b>Responsable</b>", reinsp_sub_head),
            Paragraph("<b>Firma</b>", reinsp_sub_head),
            Paragraph("<b>Estado</b>", reinsp_sub_head),
            Paragraph("<b>Fecha</b>", reinsp_sub_head),
            Paragraph("<b>Acción correctiva</b>", reinsp_sub_head),
            Paragraph("<b>Responsable</b>", reinsp_sub_head),
            Paragraph("<b>Firma</b>", reinsp_sub_head),
        ]]

        for r in rows_avance:
            estado_arm = (r.get("armado") or "").strip().upper()
            estado_sold = (r.get("soldadura") or "").strip().upper()
            es_nc_arm = estado_arm in ("NC", "NO CONFORME", "NO CONFORMIDAD")
            es_nc_sold = estado_sold in ("NC", "NO CONFORME", "NO CONFORMIDAD")
            if not (es_nc_arm or es_nc_sold):
                continue

            ciclos_arm = _extraer_ciclos_reinspeccion(r.get("armado_reinspeccion") or "") if es_nc_arm else []
            ciclos_sold = _extraer_ciclos_reinspeccion(r.get("soldadura_reinspeccion") or "") if es_nc_sold else []
            ult_arm = ciclos_arm[-1] if ciclos_arm else {}
            ult_sold = ciclos_sold[-1] if ciclos_sold else {}

            reinsp_rows.append([
                Paragraph(r.get("posicion") or "-", base_style),
                Paragraph((ult_arm.get("estado") or "-").strip().upper() if es_nc_arm else "-", base_style),
                Paragraph((ult_arm.get("fecha") or "-").strip() if es_nc_arm else "-", base_style),
                Paragraph((ult_arm.get("motivo") or "-").strip() if es_nc_arm else "-", base_style),
                Paragraph((ult_arm.get("responsable") or "-").strip() if es_nc_arm else "-", base_style),
                _pdf_firma_cell_as((ult_arm.get("firma") or "").strip()) if es_nc_arm else Paragraph("-", base_style),
                Paragraph((ult_sold.get("estado") or "-").strip().upper() if es_nc_sold else "-", base_style),
                Paragraph((ult_sold.get("fecha") or "-").strip() if es_nc_sold else "-", base_style),
                Paragraph((ult_sold.get("motivo") or "-").strip() if es_nc_sold else "-", base_style),
                Paragraph((ult_sold.get("responsable") or "-").strip() if es_nc_sold else "-", base_style),
                _pdf_firma_cell_as((ult_sold.get("firma") or "").strip()) if es_nc_sold else Paragraph("-", base_style),
            ])

        if len(reinsp_rows) > 2:
            elements.append(Paragraph("<b>Re-inspección (solo piezas NC)</b>", ParagraphStyle('ReinspTitleAS', parent=styles['Normal'], fontSize=9, textColor=colors.HexColor('#9a3412'))))
            elements.append(Spacer(1, 0.1*cm))
            # Total ancho: 19.8cm (igual a tabla principal)
            rt = Table(reinsp_rows, colWidths=[1.5*cm, 1.2*cm, 1.55*cm, 2.6*cm, 1.55*cm, 2.25*cm, 1.2*cm, 1.55*cm, 2.6*cm, 1.55*cm, 2.25*cm], repeatRows=2)
            rt.setStyle(TableStyle([
                ('SPAN', (1,0), (5,0)),
                ('SPAN', (6,0), (10,0)),
                ('SPAN', (0,0), (0,1)),
                ('BACKGROUND', (0,0), (10,0), colors.HexColor('#ea580c')),
                ('BACKGROUND', (1,1), (5,1), colors.HexColor('#ffedd5')),
                ('BACKGROUND', (6,1), (10,1), colors.HexColor('#fff7ed')),
                ('TEXTCOLOR', (0,0), (10,0), colors.white),
                ('GRID', (0,0), (10,-1), 0.35, colors.HexColor('#cbd5e1')),
                ('ROWBACKGROUNDS', (0,2), (10,-1), [colors.white, colors.HexColor('#fff7ed')]),
                ('VALIGN', (0,0), (10,-1), 'MIDDLE'),
                ('ALIGN', (0,0), (10,1), 'CENTER'),
                ('LEFTPADDING', (0,0), (10,-1), 3),
                ('RIGHTPADDING', (0,0), (10,-1), 3),
                ('TOPPADDING', (0,0), (10,-1), 3),
                ('BOTTOMPADDING', (0,0), (10,-1), 3),
            ]))
            elements.append(rt)

        doc.build(elements)
        pdf_buffer.seek(0)
        filename = f"Control_Armado_Soldadura_OT_{ot_id}_{obra}_{date.today().isoformat()}.pdf".replace(" ", "_")
        _guardar_pdf_databook(obra, "calidad_armado_soldadura", filename, pdf_buffer.getvalue(), ot_id=ot_id)
        pdf_buffer.seek(0)
        return send_file(pdf_buffer, mimetype='application/pdf', as_attachment=True, download_name=filename)

    # Búsqueda por pieza (solo en vista HTML)
    busqueda_pieza_as = (request.args.get("busqueda_pieza") or "").strip()
    if busqueda_pieza_as:
        rows_avance = [r for r in rows_avance if busqueda_pieza_as.lower() in r["posicion"].lower()]

    total = len(rows_avance)
    comp_armado = len([r for r in rows_avance if r["armado"]])
    comp_sold = len([r for r in rows_avance if r["soldadura"]])

    # Paginación
    POR_PAGINA_AS = 20
    page_as_txt = (request.args.get("page") or "1").strip()
    page_as = int(page_as_txt) if page_as_txt.isdigit() else 1
    total_paginas_as = max(1, (total + POR_PAGINA_AS - 1) // POR_PAGINA_AS)
    page_as = max(1, min(page_as, total_paginas_as))
    inicio_as = (page_as - 1) * POR_PAGINA_AS
    fin_as = min(inicio_as + POR_PAGINA_AS, total)
    rows_avance_pagina = rows_avance[inicio_as:fin_as]

    def _pag_as_url(p):
        params = []
        if ot_id:
            params.append(f"ot_id={ot_id}")
        if busqueda_pieza_as:
            params.append(f"busqueda_pieza={quote(busqueda_pieza_as)}")
        params.append(f"page={p}")
        return "/modulo/calidad/escaneo/form-armado-soldadura?" + "&".join(params)

    paginacion_as_html = ""
    if total_paginas_as > 1:
        paginacion_as_html = '<div style="display:flex;justify-content:center;gap:5px;flex-wrap:wrap;padding:10px 0;">'
        paginacion_as_html += f'<a href="{_pag_as_url(page_as-1)}" style="padding:6px 10px;border:1px solid #ddd;border-radius:4px;text-decoration:none;color:#333;">&#8249; Ant.</a>' if page_as > 1 else '<span style="padding:6px 10px;border:1px solid #ddd;border-radius:4px;color:#ccc;">&#8249; Ant.</span>'
        for _p in range(max(1, page_as - 2), min(total_paginas_as + 1, page_as + 3)):
            if _p == page_as:
                paginacion_as_html += f'<span style="padding:6px 10px;border:1px solid #f97316;border-radius:4px;background:#f97316;color:white;font-weight:bold;">{_p}</span>'
            else:
                paginacion_as_html += f'<a href="{_pag_as_url(_p)}" style="padding:6px 10px;border:1px solid #ddd;border-radius:4px;text-decoration:none;color:#333;">{_p}</a>'
        paginacion_as_html += f'<a href="{_pag_as_url(page_as+1)}" style="padding:6px 10px;border:1px solid #ddd;border-radius:4px;text-decoration:none;color:#333;">Sig. &#8250;</a>' if page_as < total_paginas_as else '<span style="padding:6px 10px;border:1px solid #ddd;border-radius:4px;color:#ccc;">Sig. &#8250;</span>'
        paginacion_as_html += '</div>'

    opciones_ot = '<option value="">-- Seleccionar OT --</option>'
    for o in ots:
        sel = 'selected' if ot_id is not None and int(ot_id) == int(o["id"]) else ''
        ot_desc = f'OT {o["id"]} - {o["obra"] or "(sin obra)"}' + (f' - {o["titulo"]}' if o.get("titulo") else '')
        opciones_ot += f'<option value="{o["id"]}" data-obra="{html_lib.escape(o["obra"])}" {sel}>{html_lib.escape(ot_desc)}</option>'

    rows_html = ""
    for r in rows_avance_pagina:
        armado_txt = r["armado"] if r["armado"] else "PENDIENTE"
        sold_txt = r["soldadura"] if r["soldadura"] else "PENDIENTE"
        armado_class = "ok" if r["armado"] else "pend"
        sold_class = "ok" if r["soldadura"] else "pend"
        rows_html += f"""
        <tr>
            <td><b>{r['posicion']}</b></td>
            <td class="td-meta">{_format_cantidad_1_decimal(r['cantidad'])}</td>
            <td class="td-meta">{r['perfil'] or '-'}</td>
            <td class="td-a">
                <div class="{armado_class}">{armado_txt}</div>
            </td>
            <td class="td-a">{r['armado_fecha'] or '-'}</td>
            <td class="td-a">{r['armado_responsable'] or '-'}</td>
            <td class="td-a">{'<img src="' + r['armado_firma_url'] + '" style="max-height:32px;max-width:95px;vertical-align:middle;border:1px solid #e5e7eb;border-radius:4px;background:#fff;padding:2px;">' if r.get('armado_firma_url') else (r['armado_firma_digital'] or '-')}</td>
            <td class="td-s">
                <div class="{sold_class}">{sold_txt}</div>
            </td>
            <td class="td-s">{r['soldadura_fecha'] or '-'}</td>
            <td class="td-s">{r['soldadura_responsable'] or '-'}</td>
            <td class="td-s">{'<img src="' + r['soldadura_firma_url'] + '" style="max-height:32px;max-width:95px;vertical-align:middle;border:1px solid #e5e7eb;border-radius:4px;background:#fff;padding:2px;">' if r.get('soldadura_firma_url') else (r['soldadura_firma_digital'] or '-')}</td>
        </tr>
        """

    def _firma_html_reinsp(firma_txt):
        firma_val = (firma_txt or "").strip()
        if not firma_val:
            return "-"
        firma_url = imagen_por_firma.get(firma_val.lower(), "")
        if firma_url:
            return f'<img src="{firma_url}" style="max-height:30px;max-width:90px;vertical-align:middle;border:1px solid #e5e7eb;border-radius:4px;background:#fff;padding:2px;">'
        return html_lib.escape(firma_val)

    reinspeccion_rows_html = ""
    for r in rows_avance_pagina:
        estado_arm = (r.get("armado") or "").strip().upper()
        estado_sold = (r.get("soldadura") or "").strip().upper()
        es_nc_arm = estado_arm in ("NC", "NO CONFORME", "NO CONFORMIDAD")
        es_nc_sold = estado_sold in ("NC", "NO CONFORME", "NO CONFORMIDAD")
        if not (es_nc_arm or es_nc_sold):
            continue

        ciclos_arm = _extraer_ciclos_reinspeccion(r.get("armado_reinspeccion") or "") if es_nc_arm else []
        ciclos_sold = _extraer_ciclos_reinspeccion(r.get("soldadura_reinspeccion") or "") if es_nc_sold else []
        ult_arm = ciclos_arm[-1] if ciclos_arm else {}
        ult_sold = ciclos_sold[-1] if ciclos_sold else {}

        arm_estado = html_lib.escape((ult_arm.get("estado") or "-").strip().upper()) if es_nc_arm else "-"
        arm_fecha = html_lib.escape((ult_arm.get("fecha") or "-").strip()) if es_nc_arm else "-"
        arm_accion = html_lib.escape((ult_arm.get("motivo") or "-").strip()) if es_nc_arm else "-"
        arm_resp = html_lib.escape((ult_arm.get("responsable") or "-").strip()) if es_nc_arm else "-"
        arm_firma = _firma_html_reinsp((ult_arm.get("firma") or "").strip()) if es_nc_arm else "-"

        sold_estado = html_lib.escape((ult_sold.get("estado") or "-").strip().upper()) if es_nc_sold else "-"
        sold_fecha = html_lib.escape((ult_sold.get("fecha") or "-").strip()) if es_nc_sold else "-"
        sold_accion = html_lib.escape((ult_sold.get("motivo") or "-").strip()) if es_nc_sold else "-"
        sold_resp = html_lib.escape((ult_sold.get("responsable") or "-").strip()) if es_nc_sold else "-"
        sold_firma = _firma_html_reinsp((ult_sold.get("firma") or "").strip()) if es_nc_sold else "-"

        reinspeccion_rows_html += f"""
        <tr>
            <td><b>{html_lib.escape(r.get('posicion') or '-')}</b></td>
            <td>{arm_estado}</td>
            <td>{arm_fecha}</td>
            <td>{arm_accion}</td>
            <td>{arm_resp}</td>
            <td>{arm_firma}</td>
            <td>{sold_estado}</td>
            <td>{sold_fecha}</td>
            <td>{sold_accion}</td>
            <td>{sold_resp}</td>
            <td>{sold_firma}</td>
        </tr>
        """

    if not reinspeccion_rows_html:
        reinspeccion_rows_html = "<tr><td colspan='11' style='text-align:center;color:#6b7280;'>No hay piezas NC con datos de re-inspección</td></tr>"
    
    if not rows_html:
        rows_html = "<tr><td colspan='11' style='text-align:center;color:#6b7280;'>No hay registros para la obra seleccionada</td></tr>"
    
    acciones_as_html = ""
    if es_obra:
        acciones_as_html = """
        <div class="actions">
            <a href="/modulo/calidad/escaneo" class="btn btn-blue">⬅️ Volver a Sub Módulos</a>
        </div>
        <p style="margin-top:10px;background:#fff7ed;color:#9a3412;padding:10px;border-radius:6px;border:1px solid #fdba74;"><b>Modo solo visualizacion:</b> podes usar filtros y consultar datos, sin generar PDF.</p>
        """
    else:
        acciones_as_html = f"""
        <form method="post" action="/modulo/calidad/escaneo/form-armado-soldadura">
            <input type="hidden" name="accion" value="pdf">
            <input type="hidden" name="ot_id" id="ot-hidden-as" value="{ot_id or ''}">
            <div class="actions">
                <button type="submit" class="btn" {'disabled style="opacity:0.6;cursor:not-allowed;"' if not ot_id else ''}>📄 Generar PDF (todas las piezas)</button>
                <a href="/modulo/calidad/escaneo" class="btn btn-blue">⬅️ Volver a Sub Módulos</a>
            </div>
        </form>
        """

    html = f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta charset="UTF-8">
    <meta charset="UTF-8">
    <style>
    body {{ font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }}
    h2 {{ color: #9a3412; border-bottom: 3px solid #f97316; padding-bottom: 10px; }}
    .box {{ background:white; border-radius:8px; padding:14px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); margin-bottom: 14px; }}
    .filtro {{ display:grid; grid-template-columns: 1fr auto; gap:10px; align-items:end; }}
    .filtro select, .filtro input {{ width:100%; padding:10px; border:1px solid #d1d5db; border-radius:6px; }}
    .btn {{ background:#f97316; color:white; border:none; padding:10px 14px; border-radius:6px; font-weight:bold; cursor:pointer; text-decoration:none; display:inline-block; }}
    .btn:hover {{ background:#ea580c; }}
    .btn-blue {{ background:#2563eb; }}
    .btn-blue:hover {{ background:#1d4ed8; }}
    .kpis {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap:10px; margin-top: 10px; }}
    .kpi {{ background:#fff7ed; border:1px solid #fed7aa; border-radius:8px; padding:10px; }}
    .kpi .t {{ font-size:12px; color:#9a3412; }}
    .kpi .v {{ font-size:22px; color:#7c2d12; font-weight:bold; }}
    table {{ width:100%; border-collapse: collapse; background:white; table-layout: fixed; }}
    th, td {{ border-bottom:1px solid #e5e7eb; padding:9px; text-align:center; font-size:13px; }}
    th {{ background:#f97316; color:white; }}
    th.th-pos {{ background:#c2410c; }}
    th.th-armado {{ background:#ea580c; letter-spacing:0.5px; }}
    th.th-soldadura {{ background:#f97316; letter-spacing:0.5px; }}
    th.th-sub {{ font-size:11px; font-weight:600; }}
    th.th-meta {{ background:#c2410c; }}
    th.th-sub-a {{ background:#ffedd5; color:#7c2d12; }}
    th.th-sub-s {{ background:#fff7ed; color:#9a3412; }}
    td.td-meta {{ background:#fffaf5; }}
    td.td-a {{ background:#fff7ed; }}
    td.td-s {{ background:#ffedd5; }}
    td:first-child {{ text-align:left; }}
    .ok {{ color:#166534; font-weight:bold; }}
    .pend {{ color:#9ca3af; font-weight:bold; }}
    .actions {{ display:flex; gap:10px; flex-wrap:wrap; margin-top:12px; }}
    .reinsp-box {{ margin-top:16px; border:1px solid #fdba74; border-radius:8px; background:#fff7ed; padding:10px; }}
    .reinsp-title {{ margin:0 0 8px 0; color:#9a3412; font-size:15px; }}
    .reinsp-table th {{ background:#ea580c; color:white; font-size:12px; }}
    .reinsp-table td {{ font-size:12px; }}
    .reinsp-table .reinsp-group-arm {{ background:#ea580c; }}
    .reinsp-table .reinsp-group-sold {{ background:#f97316; }}
    .reinsp-table .reinsp-sub-arm {{ background:#ffedd5; color:#7c2d12; }}
    .reinsp-table .reinsp-sub-sold {{ background:#fff7ed; color:#9a3412; }}
    </style>
    </head>
    <body>
    <h2>🧩 Control de Armado y Soldadura</h2>

    <div class="box">
        {'<p style="background:#fff7ed;color:#9a3412;padding:10px;border-radius:6px;border:1px solid #fdba74;margin-top:0;"><b>' + html_lib.escape(mensaje) + '</b></p>' if mensaje else ''}
        <form method="get" action="/modulo/calidad/escaneo/form-armado-soldadura" id="form-filtro-as">
            <div class="filtro">
                <div>
                    <label><b>Filtrar por OT</b></label>
                    <select name="ot_id" id="ot-select-as" onchange="document.getElementById('form-filtro-as').submit();">
                        {opciones_ot}
                    </select>
                </div>
                <div>
                    <label><b>Obra (autocompletada)</b></label>
                    <input type="text" id="obra-visible-as" value="{html_lib.escape(obra)}" readonly>
                </div>
            </div>
            <div style="display:flex;gap:10px;margin-top:10px;align-items:flex-end;">
                <div style="flex:1;">
                    <label><b>Buscar por Posición</b></label>
                    <input type="text" name="busqueda_pieza" value="{html_lib.escape(busqueda_pieza_as)}" placeholder="🔍 Buscar posición...">
                </div>
                <button type="submit" class="btn" style="white-space:nowrap;">🔎 Buscar</button>
                {'<a href="/modulo/calidad/escaneo/form-armado-soldadura' + (f"?ot_id={ot_id}" if ot_id else "") + '" class="btn" style="background:#6b7280;white-space:nowrap;">&#10005; Limpiar</a>' if busqueda_pieza_as else ""}
            </div>
        </form>

        <div class="kpis">
            <div class="kpi"><div class="t">Total piezas</div><div class="v">{total}</div></div>
            <div class="kpi"><div class="t">Armado cargado</div><div class="v">{comp_armado}</div></div>
            <div class="kpi"><div class="t">Soldadura cargada</div><div class="v">{comp_sold}</div></div>
            <div class="kpi"><div class="t">Página {page_as} / {total_paginas_as}</div><div class="v" style="font-size:14px;">{inicio_as+1 if total>0 else 0}–{fin_as} de {total}</div></div>
        </div>
    </div>

    <div class="box">
        <table>
            <tr>
                <th class="th-pos" rowspan="2">Posición</th>
                <th class="th-meta" rowspan="2">Cantidad</th>
                <th class="th-meta" rowspan="2">Perfil</th>
                <th class="th-armado" colspan="4">📩 ARMADO</th>
                <th class="th-soldadura" colspan="4">⚡ SOLDADURA</th>
            </tr>
            <tr>
                <th class="th-sub th-sub-a">Estado</th>
                <th class="th-sub th-sub-a">Fecha</th>
                <th class="th-sub th-sub-a">Responsable</th>
                <th class="th-sub th-sub-a">Firma</th>
                <th class="th-sub th-sub-s">Estado</th>
                <th class="th-sub th-sub-s">Fecha</th>
                <th class="th-sub th-sub-s">Responsable</th>
                <th class="th-sub th-sub-s">Firma</th>
            </tr>
            {rows_html}
        </table>

        <div class="reinsp-box">
            <h3 class="reinsp-title">Re-inspección (solo piezas NC)</h3>
            <table class="reinsp-table">
                <tr>
                    <th rowspan="2">Posición</th>
                    <th class="reinsp-group-arm" colspan="5">📩 ARMADO</th>
                    <th class="reinsp-group-sold" colspan="5">⚡ SOLDADURA</th>
                </tr>
                <tr>
                    <th class="reinsp-sub-arm">Estado</th>
                    <th class="reinsp-sub-arm">Fecha</th>
                    <th class="reinsp-sub-arm">Acción correctiva</th>
                    <th class="reinsp-sub-arm">Responsable</th>
                    <th class="reinsp-sub-arm">Firma</th>
                    <th class="reinsp-sub-sold">Estado</th>
                    <th class="reinsp-sub-sold">Fecha</th>
                    <th class="reinsp-sub-sold">Acción correctiva</th>
                    <th class="reinsp-sub-sold">Responsable</th>
                    <th class="reinsp-sub-sold">Firma</th>
                </tr>
                {reinspeccion_rows_html}
            </table>
        </div>

        {paginacion_as_html}
        {acciones_as_html}
    </div>
    <script>
    (function() {{
        const otSel = document.getElementById('ot-select-as');
        const obraVisible = document.getElementById('obra-visible-as');
        const otHidden = document.getElementById('ot-hidden-as');
        if (!otSel) return;

        function syncOtMeta() {{
            const opt = otSel.options[otSel.selectedIndex];
            const obra = opt ? (opt.getAttribute('data-obra') || '') : '';
            if (obraVisible) obraVisible.value = obra;
            if (otHidden) otHidden.value = otSel.value || '';
        }}

        otSel.addEventListener('change', syncOtMeta);
        syncOtMeta();
    }})();
    </script>
    </body>
    </html>
    """
    return html


@calidad_bp.route("/modulo/calidad/escaneo/form-pintura", methods=["GET", "POST"])
def calidad_escaneo_form_pintura():
    from datetime import date
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
    from reportlab.lib.pagesizes import landscape, letter
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

    db = get_db()
    es_obra = str(session.get("user_role") or "").strip().lower() == "obra"
    responsables_control = _obtener_responsables_control(db)
    firmas_responsables = {k: v.get("firma", "") for k, v in responsables_control.items()}

    def _to_float(val):
        txt = str(val or "").strip().replace(",", ".")
        if not txt:
            return 0.0
        try:
            return float(txt)
        except Exception:
            return 0.0

    def _obtener_piezas_obra(obra_sel):
        if not obra_sel:
            return []

        _completar_metadatos_por_obra_pos(db, obra_sel)

        rows = db.execute(
            """
            SELECT posicion, cantidad, perfil
            FROM procesos
            WHERE TRIM(COALESCE(obra, '')) = TRIM(?)
                AND TRIM(COALESCE(posicion, '')) <> ''
                AND eliminado = 0
                AND NOT EXISTS (
                    SELECT 1 FROM procesos p2
                    WHERE TRIM(COALESCE(p2.obra, '')) = TRIM(COALESCE(procesos.obra, ''))
                        AND TRIM(COALESCE(p2.posicion, '')) = TRIM(COALESCE(procesos.posicion, ''))
                        AND UPPER(TRIM(p2.proceso)) = 'PINTURA'
                        AND p2.eliminado = 0
                )
                AND EXISTS (
                    SELECT 1 FROM ordenes_trabajo ot
                    WHERE TRIM(COALESCE(ot.obra, '')) = TRIM(COALESCE(procesos.obra, ''))
                        AND ot.fecha_cierre IS NULL
                        AND (ot.es_mantenimiento IS NULL OR ot.es_mantenimiento = 0)
                )
            ORDER BY id DESC
            """,
            (obra_sel,)
        ).fetchall()

        piezas_map = {}
        for pos, cantidad, perfil in rows:
            key = (pos or "").strip()
            if not key or key in piezas_map:
                continue
            piezas_map[key] = {
                "pieza": key,
                "cantidad": _format_cantidad_1_decimal(cantidad),
                "descripcion": str(perfil or "").strip(),
            }

        return sorted(piezas_map.values(), key=lambda x: x["pieza"])

    obras_db = db.execute(
        """
        SELECT DISTINCT TRIM(obra) AS obra
        FROM procesos
        WHERE obra IS NOT NULL AND TRIM(obra) <> ''
          AND EXISTS (
                SELECT 1 FROM ordenes_trabajo ot
                WHERE TRIM(COALESCE(ot.obra, '')) = TRIM(COALESCE(procesos.obra, ''))
                  AND ot.fecha_cierre IS NULL
                  AND (ot.es_mantenimiento IS NULL OR ot.es_mantenimiento = 0)
          )
        ORDER BY obra ASC
        """
    ).fetchall()
    obras = [r[0] for r in obras_db]

    obra = (request.values.get("obra") or "").strip()
    piezas = _obtener_piezas_obra(obra)
    
    # Obtener OT, esquema de pintura y espesor requerido
    ot_id_pintura = None
    ot_titulo_pintura = ""
    esquema_pintura = ""
    espesor_total_requerido = ""
    if obra:
        ot_data = db.execute(
            """
            SELECT id, COALESCE(titulo, ''), COALESCE(esquema_pintura, ''), COALESCE(espesor_total_requerido, '')
            FROM ordenes_trabajo
            WHERE TRIM(COALESCE(obra,'')) = ?
              AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
              AND fecha_cierre IS NULL
            ORDER BY id DESC
            LIMIT 1
            """,
            (obra,)
        ).fetchone()
        if ot_data:
            ot_id_pintura = int(ot_data[0]) if ot_data[0] is not None else None
            ot_titulo_pintura = str(ot_data[1] or "")
            esquema_pintura = ot_data[2] or ""
            espesor_total_requerido = ot_data[3] or ""

    if request.method == "POST" and es_obra:
        return redirect("/modulo/calidad/escaneo/form-pintura?obra=" + quote(obra) + "&mensaje=" + quote("Solo visualizacion para usuario obra"))

    if request.method == "POST" and (request.form.get("accion") or "").strip().lower() == "pdf":
        obra = (request.form.get("obra") or "").strip()
        if not obra:
            return "Selecciona una obra", 400

        # Obtener OT, esquema y espesor requerido
        ot_id_pintura = None
        ot_titulo_pintura = ""
        esquema_pintura = ""
        espesor_total_requerido = ""
        ot_data = db.execute(
            """
            SELECT id, COALESCE(titulo, ''), COALESCE(esquema_pintura, ''), COALESCE(espesor_total_requerido, '')
            FROM ordenes_trabajo
            WHERE TRIM(COALESCE(obra,'')) = ?
              AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
              AND fecha_cierre IS NULL
            ORDER BY id DESC
            LIMIT 1
            """,
            (obra,)
        ).fetchone()
        if ot_data:
            ot_id_pintura = int(ot_data[0]) if ot_data[0] is not None else None
            ot_titulo_pintura = str(ot_data[1] or "")
            esquema_pintura = ot_data[2] or ""
            espesor_total_requerido = ot_data[3] or ""

        piezas_form = request.form.getlist("pieza[]")
        cantidades_form = request.form.getlist("cantidad[]")
        desc_form = request.form.getlist("descripcion[]")
        sup_estado_form = request.form.getlist("sup_estado[]")
        sup_resp_form = request.form.getlist("sup_responsable[]")
        sup_firma_form = request.form.getlist("sup_firma[]")
        fondo_espesor_form = request.form.getlist("fondo_espesor[]")
        fondo_fecha_form = request.form.getlist("fondo_fecha[]")
        fondo_resp_form = request.form.getlist("fondo_responsable[]")
        fondo_firma_form = request.form.getlist("fondo_firma[]")
        term_espesor_form = request.form.getlist("term_espesor[]")
        term_fecha_form = request.form.getlist("term_fecha[]")
        term_resp_form = request.form.getlist("term_responsable[]")
        term_firma_form = request.form.getlist("term_firma[]")

        filas_pintura = []
        total_filas = len(piezas_form)
        for i in range(total_filas):
            pieza = (piezas_form[i] if i < len(piezas_form) else "").strip()
            if not pieza:
                continue
            cantidad = (cantidades_form[i] if i < len(cantidades_form) else "").strip()
            descripcion = (desc_form[i] if i < len(desc_form) else "").strip()
            sup_estado = (sup_estado_form[i] if i < len(sup_estado_form) else "").strip().upper()
            sup_resp = (sup_resp_form[i] if i < len(sup_resp_form) else "").strip()
            sup_resp_nombre = (sup_firma_form[i] if i < len(sup_firma_form) else "").strip()  # Ahora es el nombre
            sup_firma = responsables_control.get(sup_resp_nombre, {}).get("firma", "") if sup_resp_nombre else ""
            fondo_espesor = _to_float(fondo_espesor_form[i] if i < len(fondo_espesor_form) else "")
            fondo_fecha = (fondo_fecha_form[i] if i < len(fondo_fecha_form) else "").strip()
            fondo_resp_nombre = (fondo_firma_form[i] if i < len(fondo_firma_form) else "").strip()
            fondo_firma = responsables_control.get(fondo_resp_nombre, {}).get("firma", "") if fondo_resp_nombre else ""
            term_espesor = _to_float(term_espesor_form[i] if i < len(term_espesor_form) else "")
            term_fecha = (term_fecha_form[i] if i < len(term_fecha_form) else "").strip()
            term_resp_nombre = (term_firma_form[i] if i < len(term_firma_form) else "").strip()
            term_firma = responsables_control.get(term_resp_nombre, {}).get("firma", "") if term_resp_nombre else ""
            filas_pintura.append({
                "pieza": pieza,
                "cantidad": cantidad,
                "descripcion": descripcion,
                "sup_estado": sup_estado,
                "sup_resp": sup_resp,
                "sup_firma": sup_firma,
                "fondo_espesor": fondo_espesor,
                "fondo_fecha": fondo_fecha,
                "fondo_resp": fondo_resp_nombre,
                "fondo_firma": fondo_firma,
                "term_espesor": term_espesor,
                "term_fecha": term_fecha,
                "term_resp": term_resp_nombre,
                "term_firma": term_firma,
            })

        # Eliminado bloque de temperatura y humedad
        mediciones = []

        pdf_buffer = BytesIO()
        doc = SimpleDocTemplate(
            pdf_buffer,
            pagesize=landscape(letter),
            topMargin=0.5 * cm,
            bottomMargin=0.6 * cm,
            leftMargin=0.6 * cm,
            rightMargin=0.6 * cm,
        )

        styles = getSampleStyleSheet()
        base_style = ParagraphStyle('BaseP', parent=styles['Normal'], fontSize=7.2, leading=8.4, textColor=colors.HexColor('#1f2937'))
        head_style = ParagraphStyle('HeadP', parent=styles['Normal'], fontSize=7.1, leading=8.2, alignment=1, fontName='Helvetica-Bold', textColor=colors.white)
        title_style = ParagraphStyle('TitleP', parent=styles['Heading2'], fontSize=14, textColor=colors.HexColor('#111827'), alignment=0)

        def _encabezado_pintura_path():
            candidatos = [
                os.path.join(_APP_DIR, "ENCABEZAO_PINTURA.png"),
                os.path.join(_APP_DIR, "ENCABEZAO_PINTURA.jpg"),
                os.path.join(_APP_DIR, "ENCABEZAO_PINTURA.jpeg"),
                os.path.join(_APP_DIR, "ENCABEZADO_PINTURA.png"),
                os.path.join(_APP_DIR, "ENCABEZADO_PINTURA.jpg"),
                os.path.join(_APP_DIR, "ENCABEZADO_PINTURA.jpeg"),
            ]
            for c in candidatos:
                if os.path.exists(c):
                    return c
            return None

        def _firma_pdf_flowable(responsable_nombre):
            ruta = _ruta_firma_responsable(responsables_control, responsable_nombre)
            if not ruta:
                return Paragraph("-", base_style)
            try:
                img = RLImage(ruta)
                img.drawWidth = 1.9 * cm
                img.drawHeight = 0.55 * cm
                return img
            except Exception:
                return Paragraph("-", base_style)

        elements = []
        encabezado_pintura = _encabezado_pintura_path()
        if encabezado_pintura:
            try:
                encabezado_img = RLImage(encabezado_pintura)
                max_width = 26.0 * cm
                if encabezado_img.drawWidth > max_width:
                    escala = max_width / float(encabezado_img.drawWidth)
                    encabezado_img.drawWidth *= escala
                    encabezado_img.drawHeight *= escala
                elements.append(encabezado_img)
            except Exception:
                elements.append(Paragraph("<b>CONTROL DE PINTURA</b>", title_style))
        else:
            elements.append(Paragraph("<b>CONTROL DE PINTURA</b>", title_style))
        elements.append(Spacer(1, 0.2 * cm))
        ot_desc_pintura = f"OT {ot_id_pintura} - {obra}" + (f" - {ot_titulo_pintura}" if ot_titulo_pintura else "") if ot_id_pintura else "-"
        info = Table([
            [Paragraph(f"<b>OT:</b> {html_lib.escape(ot_desc_pintura)}", base_style), Paragraph(f"<b>Fecha reporte:</b> {date.today().isoformat()}", base_style)],
            [Paragraph(f"<b>Obra:</b> {obra}", base_style), Paragraph("", base_style)],
        ], colWidths=[13.4 * cm, 13.4 * cm])
        elements.append(info)
        elements.append(Spacer(1, 0.25*cm))
        
        # Sección de Datos de Entrada
        elementos_entrada = [
            [
                Paragraph("<b>Esquema de Pintura</b>", head_style), 
                Paragraph(esquema_pintura or "-", base_style),
                Paragraph("<b>Espesor Total Requerido (μm)</b>", head_style),
                Paragraph(espesor_total_requerido or "-", base_style),
            ]
        ]
        entrada_table = Table(elementos_entrada, colWidths=[4.5*cm, 4.5*cm, 5.0*cm, 4.5*cm])
        entrada_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, 0), colors.HexColor('#e8f4f8')),
            ('BACKGROUND', (2, 0), (2, 0), colors.HexColor('#e8f4f8')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#0c4a6e')),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#cbd5e1')),
            ('LEFTPADDING', (0, 0), (-1, -1), 5),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        elements.append(entrada_table)
        elements.append(Spacer(1, 0.2*cm))
        
        # Sección de Tabla de Control
        elements.append(Paragraph("<b>1) Estado de Superficie y Control Pintura</b>", ParagraphStyle('Sec2', parent=styles['Normal'], fontSize=10, textColor=colors.HexColor('#9a3412'))))
        elements.append(Spacer(1, 0.08*cm))
        pie_table_data = [
            [
                Paragraph("<b>PIEZA</b>", head_style),
                Paragraph("<b>CANT.</b>", head_style),
                Paragraph("<b>DESCRIPCION</b>", head_style),
                Paragraph("<b>CONTROL SUPERFICIE</b>", head_style),
                "",
                Paragraph("<b>FONDO DE IMPRIMACION</b>", head_style),
                "",
                "",
                Paragraph("<b>TERMINACION</b>", head_style),
                "",
                "",
                Paragraph("<b>RESUMEN DE PINTURA</b>", head_style),
                "",
                "",
            ],
            [
                "",
                "",
                "",
                Paragraph("<b>ESTADO</b>", head_style),
                Paragraph("<b>RESP. Y FIRMA</b>", head_style),
                Paragraph("<b>ESP. PROM.</b>", head_style),
                Paragraph("<b>FECHA</b>", head_style),
                Paragraph("<b>RESP. Y FIRMA</b>", head_style),
                Paragraph("<b>ESP. PROM.</b>", head_style),
                Paragraph("<b>FECHA</b>", head_style),
                Paragraph("<b>RESP. Y FIRMA</b>", head_style),
                Paragraph("<b>ESP. TOTAL</b>", head_style),
                Paragraph("<b>ESP. REQ.</b>", head_style),
                Paragraph("<b>ESTADO</b>", head_style),
            ],
        ]

        if filas_pintura:
            for r in filas_pintura:
                fondo_esp = r.get('fondo_espesor', 0) or 0
                term_esp = r.get('term_espesor', 0) or 0
                esp_total = fondo_esp + term_esp
                esp_req = float(espesor_total_requerido or 0)
                estado = "APROBADO" if esp_total >= esp_req and esp_total > 0 else ("NO CONFORME" if esp_total > 0 else "-")
                
                pie_table_data.append([
                    Paragraph(r["pieza"], base_style),
                    Paragraph(r["cantidad"] or "-", base_style),
                    Paragraph(r["descripcion"] or "-", base_style),
                    Paragraph(r["sup_estado"] or "-", base_style),
                    _firma_pdf_flowable(r.get("sup_resp") or ""),
                    Paragraph(f"{fondo_esp:.1f}" if fondo_esp else "-", base_style),
                    Paragraph(r.get("fondo_fecha", "") or "-", base_style),
                    _firma_pdf_flowable(r.get("fondo_resp") or ""),
                    Paragraph(f"{term_esp:.1f}" if term_esp else "-", base_style),
                    Paragraph(r.get("term_fecha", "") or "-", base_style),
                    _firma_pdf_flowable(r.get("term_resp") or ""),
                    Paragraph(f"{esp_total:.1f}" if esp_total > 0 else "-", base_style),
                    Paragraph(f"{esp_req:.1f}" if esp_req > 0 else "-", base_style),
                    Paragraph(estado, base_style),
                ])
        else:
            pie_table_data.append([Paragraph("-", base_style)] + [Paragraph("-", base_style) for _ in range(13)])

        pie_table = Table(
            pie_table_data,
            colWidths=[1.6*cm, 0.9*cm, 2.7*cm, 1.4*cm, 1.7*cm, 1.1*cm, 1.3*cm, 1.7*cm, 1.1*cm, 1.3*cm, 1.7*cm, 1.1*cm, 1.1*cm, 1.1*cm],
            repeatRows=2,
        )
        pie_table.setStyle(TableStyle([
            ('SPAN', (0, 0), (0, 1)),
            ('SPAN', (1, 0), (1, 1)),
            ('SPAN', (2, 0), (2, 1)),
            ('SPAN', (3, 0), (4, 0)),
            ('SPAN', (5, 0), (7, 0)),
            ('SPAN', (8, 0), (10, 0)),
            ('SPAN', (11, 0), (13, 0)),
            ('BACKGROUND', (0, 0), (2, 1), colors.HexColor('#f97316')),
            ('BACKGROUND', (3, 0), (4, 1), colors.HexColor('#ea580c')),
            ('BACKGROUND', (5, 0), (7, 1), colors.HexColor('#f97316')),
            ('BACKGROUND', (8, 0), (10, 1), colors.HexColor('#ea580c')),
            ('BACKGROUND', (11, 0), (13, 1), colors.HexColor('#f97316')),
            ('TEXTCOLOR', (0, 0), (-1, 1), colors.white),
            ('GRID', (0, 0), (-1, -1), 0.35, colors.HexColor('#cbd5e1')),
            ('ROWBACKGROUNDS', (0, 2), (-1, -1), [colors.white, colors.HexColor('#fff7ed')]),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('LEFTPADDING', (0, 0), (-1, -1), 3),
            ('RIGHTPADDING', (0, 0), (-1, -1), 3),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))

        elements.append(Paragraph("<b>2) Estado de Superficie y Control Pintura</b>", ParagraphStyle('Sec2', parent=styles['Normal'], fontSize=9, textColor=colors.HexColor('#9a3412'))))
        elements.append(Spacer(1, 0.08 * cm))
        elements.append(pie_table)

        doc.build(elements)
        pdf_buffer.seek(0)
        
        # Guardar registro en BD
        mediciones_json = json.dumps(mediciones)
        piezas_json = json.dumps(filas_pintura)
        cursor = db.execute(
            """INSERT INTO control_pintura 
               (obra, mediciones, piezas, estado, usuario_creacion, usuario_modificacion)
               VALUES (?, ?, ?, 'activo', 'usuario', 'usuario')""",
            (obra, mediciones_json, piezas_json)
        )
        db.commit()
        control_id = cursor.lastrowid
        
        if ot_id_pintura:
            filename = f"Control_Pintura_OT_{ot_id_pintura}_{obra}_{date.today().isoformat()}.pdf".replace(" ", "_")
        else:
            filename = f"Control_Pintura_{obra}_{date.today().isoformat()}.pdf".replace(" ", "_")
        _guardar_pdf_databook(obra, "calidad_pintura", filename, pdf_buffer.getvalue(), ot_id=ot_id_pintura)
        pdf_buffer.seek(0)
        return send_file(pdf_buffer, mimetype='application/pdf', as_attachment=True, download_name=filename)

    opciones_obras = '<option value="">-- Seleccionar obra --</option>'
    for o in obras:
        sel = 'selected' if o == obra else ''
        opciones_obras += f'<option value="{o}" {sel}>{o}</option>'

    opciones_responsables = '<option value="">Seleccionar...</option>' + "".join(
        f'<option value="{html_lib.escape(nombre)}">{html_lib.escape(nombre)}</option>'
        for nombre in sorted(responsables_control.keys(), key=lambda x: x.lower())
    )

    mediciones_html = ""
    for i in range(1, 5):
        mediciones_html += f"""
        <tr>
            <td><b>Mano {i}</b></td>
            <td><input type="date" name="med_fecha[]"></td>
            <td><input type="time" name="med_hora[]"></td>
            <td>
                <input type="number" step="0.1" name="med_temp[]"
                placeholder="°C">
            </td>
            <td><input type="number" step="0.1" name="med_humedad[]"
                placeholder="%"></td>
        </tr>
        """

    piezas_rows_html = ""
    for idx, p in enumerate(piezas, start=1):
        piezas_rows_html += f"""
        <tr class="pieza-row" data-idx="{idx}">
            <td><b>{html_lib.escape(p['pieza'])}</b><input type="hidden" name="pieza[]"
                value="{html_lib.escape(p['pieza'])}"></td>
            <td>{int(float(p['cantidad']) if p['cantidad'] else 0)}<input type="hidden"
                name="cantidad[]"
                value="{html_lib.escape(p['cantidad'])}"></td>
            <td>{html_lib.escape(p['descripcion']) if p['descripcion'] else '-'}<input type="hidden"
                name="descripcion[]"
                value="{html_lib.escape(p['descripcion'])}"></td>

            <td>
                <select name="sup_estado[]"
                    class="sup-estado"
                    data-idx="{idx}"
                    required>
                    <option value="">Seleccionar...</option>
                    <option value="APROBADA">Aprobado</option>
                    <option value="NO_APROBADA">No aprobado</option>
                    <option value="NO APLICA">No aplica</option>
                </select>
            </td>
            <td>
                <select name="sup_responsable[]"
                    class="sup-resp"
                    data-idx="{idx}"
                    required>
                    {opciones_responsables}
                </select>
                <input type="text"
                    id="sup-firma-{idx}"
                    readonly
                    placeholder="Automática">
                <input type="hidden"
                    name="sup_firma[]"
                    id="sup-firma-path-{idx}">
            </td>

            <td><input type="number"
                step="0.1"
                name="fondo_espesor[]"
                class="fondo-espesor"
                data-idx="{idx}"></td>
            <td><input type="date"
                name="fondo_fecha[]"
                class="fondo-fecha"
                data-idx="{idx}"></td>
            <td>
                <select name="fondo_responsable[]"
                    class="fondo-resp"
                    data-idx="{idx}">
                    {opciones_responsables}
                </select>
                <input type="text"
                    id="fondo-firma-{idx}"
                    readonly
                    placeholder="Automática">
                <input type="hidden"
                    name="fondo_firma[]"
                    id="fondo-firma-path-{idx}">
            </td>

            <td><input type="number"
                step="0.1"
                name="term_espesor[]"
                class="term-espesor"
                data-idx="{idx}"></td>
            <td><input type="date"
                name="term_fecha[]"
                class="term-fecha"
                data-idx="{idx}"></td>
            <td>
                <select name="term_responsable[]"
                    class="term-resp"
                    data-idx="{idx}">
                    {opciones_responsables}
                </select>
                <input type="text"
                    id="term-firma-{idx}"
                    readonly
                    placeholder="Automática">
                <input type="hidden"
                    name="term_firma[]"
                    id="term-firma-path-{idx}">
            </td>

            <td><input type="number"
                step="0.1"
                id="espesor-total-{idx}"
                class="espesor-total"
                data-idx="{idx}"
                readonly
                placeholder="Auto"></td>
            <td><input type="number"
                step="0.1"
                id="espesor-req-{idx}"
                class="espesor-req"
                data-idx="{idx}"
                readonly
                value="{espesor_total_requerido}"></td>
            <td><input type="text"
                id="estado-pintura-{idx}"
                class="estado-pintura"
                data-idx="{idx}"
                readonly
                placeholder="-"></td>
        </tr>
        """

    if not piezas_rows_html:
        piezas_rows_html = "<tr><td colspan='15' style='text-align:center;color:#6b7280;'>Seleccioná una obra para cargar piezas</td></tr>"

    mensaje = (request.args.get("mensaje") or "").strip()

    acciones_pintura_html = ""
    if es_obra:
        acciones_pintura_html = """
            <div class="actions">
                <a href="/modulo/calidad/escaneo" class="btn btn-blue">⬅️ Volver a Sub Módulos</a>
            </div>
            <p style="margin-top:10px;background:#fff7ed;color:#9a3412;padding:10px;border-radius:6px;border:1px solid #fdba74;"><b>Modo solo visualizacion:</b> podes usar filtros y consultar datos, sin generar PDF.</p>
        """
    else:
        acciones_pintura_html = """
            <div class="actions">
                <button type="submit" class="btn">📄 Generar PDF Pintura</button>
                <a href="/modulo/calidad/escaneo/controles-pintura" class="btn btn-blue">ð📋 Ver Controles Anteriores</a>
                <a href="/modulo/calidad/escaneo" class="btn btn-blue">⬅️ Volver a Sub Módulos</a>
            </div>
        """

    html = f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <meta charset="UTF-8">
    <meta charset="UTF-8">
    <style>
    body {{ font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }}
    h2 {{ color: #9a3412; border-bottom: 3px solid #f97316; padding-bottom: 10px; margin-top: 0; }}
    .box {{ background:white; border-radius:8px; padding:14px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); margin-bottom: 14px; }}
    .filtro {{ display:grid; grid-template-columns: 1fr auto; gap:10px; align-items:end; }}
    .filtro select, .filtro input {{ width:100%; padding:10px; border:1px solid #d1d5db; border-radius:6px; }}
    .btn {{ background:#f97316; color:white; border:none; padding:8px 12px; border-radius:6px; font-weight:bold; cursor:pointer; text-decoration:none; display:inline-block; font-size:12px; }}
    .btn:hover {{ background:#ea580c; }}
    .btn-blue {{ background:#2563eb; }}
    .btn-blue:hover {{ background:#1d4ed8; }}
    .btn-edit {{ background:#059669; }}
    .btn-edit:hover {{ background:#047857; }}
    .btn-pdf {{ background:#7c3aed; }}
    .btn-pdf:hover {{ background:#6d28d9; }}
    table {{ width:100%; border-collapse: collapse; background:white; }}
    th, td {{ border-bottom:1px solid #e5e7eb; padding:10px; text-align:left; font-size:13px; }}
    th {{ background:#f97316; color:white; font-weight:bold; }}
    .th-med {{ background:#0ea5e9; }}
    .th-sup {{ background:#ea580c; }}
    .th-pint {{ background:#f97316; }}
    td:first-child {{ text-align:left; }}
    input, select {{ width:100%; padding:7px; border:1px solid #d1d5db; border-radius:6px; box-sizing:border-box; font-size:12px; }}
    .actions {{ display:flex; gap:10px; flex-wrap:wrap; margin-top:12px; }}
    .estado-ok {{ color:#166534; font-weight:bold; background:#dcfce7; }}
    .estado-nc {{ color:#991b1b; font-weight:bold; background:#fee2e2; }}
    .firma-img {{ display:block; margin:auto; }}
    .pieza-row.deshabilitada input, .pieza-row.deshabilitada select {{ opacity:0.5; cursor:not-allowed; }}
    </style>
    </head>
    <body>
    <h2>🎨 Control de Pintura</h2>

    {'<p style="background:#fff7ed;color:#9a3412;padding:10px;border-radius:6px;border:1px solid #fdba74;margin-top:0;"><b>' + html_lib.escape(mensaje) + '</b></p>' if mensaje else ''}

    <div class="box">
        <form method="get" action="/modulo/calidad/escaneo/form-pintura">
            <div class="filtro">
                <div>
                    <label><b>Filtrar por obra</b></label>
                    <select name="obra" required>
                        {opciones_obras}
                    </select>
                </div>
                <button type="submit" class="btn">Aplicar filtro</button>
            </div>
        </form>
        
        <div style="margin-top:15px; padding:10px; background:#f0f0f0; border-radius:5px; display:{'block' if obra else 'none'};">
            <div style="margin-bottom:10px;">
                <label><b>Esquema de Pintura:</b></label>
                <input type="text" id="esquema-pintura" readonly value="{html_lib.escape(esquema_pintura)}" style="width:100%; padding:8px; margin-top:5px; background:#fff; border:1px solid #ddd; border-radius:3px;">
            </div>
            <div>
                <label><b>Espesor Total Requerido (μm):</b></label>
                <input type="text" id="espesor-requerido" readonly value="{html_lib.escape(espesor_total_requerido)}" style="width:100%; padding:8px; margin-top:5px; background:#fff; border:1px solid #ddd; border-radius:3px;">
            </div>
        </div>
    </div>

    <form method="post" action="/modulo/calidad/escaneo/form-pintura">
        <input type="hidden" name="accion" value="pdf">
        <input type="hidden" name="obra" value="{html_lib.escape(obra)}">

        <!-- Eliminada sección de Temperatura y Humedad del formulario -->

            <h3 style="margin-top:0;color:#9a3412;">Estado de Superficie y Control Pintura</h3>
            <table>
                <tr>
                    <th rowspan="2" style="width:8%;">Pieza</th>
                    <th rowspan="2" style="width:5%;">Cant.</th>
                    <th rowspan="2" style="width:10%;">Descripción</th>
                    <th class="th-sup" colspan="2">Control Superficie</th>
                    <th class="th-pint" colspan="3">Fondo de Imprimación</th>
                    <th class="th-pint" colspan="3">Terminación</th>
                    <th class="th-pint" colspan="3">Resumen de Pintura</th>
                </tr>
                <tr>
                    <th class="th-sup">Estado</th>
                    <th class="th-sup">Resp. y Firma</th>
                    <th class="th-pint">Esp. Prom.</th>
                    <th class="th-pint">Fecha</th>
                    <th class="th-pint">Resp. y Firma</th>
                    <th class="th-pint">Esp. Prom.</th>
                    <th class="th-pint">Fecha</th>
                    <th class="th-pint">Resp. y Firma</th>
                    <th class="th-pint">Esp. Total</th>
                    <th class="th-pint">Esp. Req.</th>
                    <th class="th-pint">Estado</th>
                </tr>
                {piezas_rows_html}
            </table>

            {acciones_pintura_html}
        </div>
    </form>

    <script>
    (function() {{
        const firmasResponsables = {json.dumps(firmas_responsables, ensure_ascii=False)};

        function updateFirma(selectEl, displayId, pathId) {{
            const responsable = selectEl.value || '';
            const firma = firmasResponsables[responsable] || '';
            const display = document.getElementById(displayId);
            const path = document.getElementById(pathId);
            if (display) display.value = firma || '';
            if (path) path.value = responsable;
        }}

        function toggleRowDisabled(idx, isDisabled) {{
            const row = document.querySelector('.pieza-row[data-idx="' + idx + '"]');
            if (!row) return;
            const inputs = row.querySelectorAll('input, select');
            inputs.forEach(inp => {{
                if (inp.classList.contains('sup-estado')) return; // no deshabilitar el selector
                if (isDisabled) {{
                    inp.disabled = true;
                    inp.style.opacity = '0.5';
                }} else {{
                    if (!inp.hasAttribute('disabled-when-noapl')) {{
                        inp.disabled = false;
                        inp.style.opacity = '1';
                    }}
                }}
            }});
            row.classList.toggle('deshabilitada', isDisabled);
        }}

        document.querySelectorAll('.sup-resp').forEach(sel => {{
            sel.addEventListener('change', () => updateFirma(sel, 'sup-firma-' + sel.dataset.idx, 'sup-firma-path-' + sel.dataset.idx));
        }});
        document.querySelectorAll('.fondo-resp').forEach(sel => {{
            sel.addEventListener('change', () => updateFirma(sel, 'fondo-firma-' + sel.dataset.idx, 'fondo-firma-path-' + sel.dataset.idx));
        }});
        document.querySelectorAll('.term-resp').forEach(sel => {{
            sel.addEventListener('change', () => updateFirma(sel, 'term-firma-' + sel.dataset.idx, 'term-firma-path-' + sel.dataset.idx));
        }});

        document.querySelectorAll('.sup-estado').forEach(sel => {{
            sel.addEventListener('change', () => {{
                const isNoAplica = sel.value === 'NO APLICA';
                toggleRowDisabled(sel.dataset.idx, isNoAplica);
            }});
        }});

        function toFloat(v) {{
            const n = parseFloat(String(v || '').replace(',', '.'));
            return isNaN(n) ? 0 : n;
        }}

        function calcularEspesorYEstado(idx) {{
            const fondoInput = document.querySelector('.fondo-espesor[data-idx="' + idx + '"]');
            const termInput = document.querySelector('.term-espesor[data-idx="' + idx + '"]');
            const totalInput = document.getElementById('espesor-total-' + idx);
            const reqInput = document.getElementById('espesor-req-' + idx);
            const estadoInput = document.getElementById('estado-pintura-' + idx);

            if (!fondoInput || !termInput || !totalInput || !reqInput || !estadoInput) return;

            const fondo = toFloat(fondoInput.value);
            const term = toFloat(termInput.value);
            const total = fondo + term;
            const requerido = toFloat(reqInput.value);

            totalInput.value = total > 0 ? total.toFixed(1) : '';
            
            if (total > 0 && requerido > 0) {{
                estadoInput.value = total >= requerido ? 'APROBADO' : 'NO CONFORME';
                estadoInput.style.color = total >= requerido ? '#16a34a' : '#dc2626';
            }} else {{
                estadoInput.value = '';
                estadoInput.style.color = '#000';
            }}
        }}

        // Agregar event listeners a los inputs de espesor
        document.querySelectorAll('.fondo-espesor, .term-espesor').forEach(input => {{
            input.addEventListener('change', () => {{
                calcularEspesorYEstado(input.dataset.idx);
            }});
            input.addEventListener('input', () => {{
                calcularEspesorYEstado(input.dataset.idx);
            }});
        }});

        // Calcular al cargar la página
        document.querySelectorAll('.pieza-row').forEach(row => {{
            const idx = row.dataset.idx;
            calcularEspesorYEstado(idx);
        }});
    }})();
    </script>
    </body>
    </html>
    """
    return html

# ======================
# RUTA LISTAR CONTROLES DE PINTURA
# ======================
@calidad_bp.route("/modulo/calidad/escaneo/controles-pintura", methods=["GET"])
def listar_controles_pintura():
    db = get_db()

    obra_filtro = (request.args.get("obra") or "").strip()
    busqueda_pieza_cp = (request.args.get("busqueda_pieza") or "").strip()
    page_cp_txt = (request.args.get("page") or "1").strip()
    page_cp = int(page_cp_txt) if page_cp_txt.isdigit() else 1
    POR_PAGINA_CP = 20

    base_where = "estado='activo'"
    params_count = []
    if obra_filtro:
        base_where += " AND TRIM(COALESCE(obra, '')) = TRIM(?)"
        params_count.append(obra_filtro)
    if busqueda_pieza_cp:
        base_where += " AND LOWER(COALESCE(piezas,'')) LIKE LOWER(?)"
        params_count.append(f"%{busqueda_pieza_cp}%")

    total_controles = db.execute(f"SELECT COUNT(*) FROM control_pintura WHERE {base_where}", params_count).fetchone()[0]
    total_paginas_cp = max(1, (total_controles + POR_PAGINA_CP - 1) // POR_PAGINA_CP)
    page_cp = max(1, min(page_cp, total_paginas_cp))
    offset_cp = (page_cp - 1) * POR_PAGINA_CP

    params = list(params_count) + [POR_PAGINA_CP, offset_cp]
    query = f"SELECT id, obra, fecha_creacion, fecha_modificacion FROM control_pintura WHERE {base_where} ORDER BY fecha_creacion DESC LIMIT ? OFFSET ?"

    controles = db.execute(query, params).fetchall()

    # Paginación HTML
    def _pag_cp_url(p):
        parts = []
        if obra_filtro:
            parts.append(f"obra={quote(obra_filtro)}")
        if busqueda_pieza_cp:
            parts.append(f"busqueda_pieza={quote(busqueda_pieza_cp)}")
        parts.append(f"page={p}")
        return "/modulo/calidad/escaneo/controles-pintura?" + "&".join(parts)

    paginacion_cp_html = ""
    if total_paginas_cp > 1:
        paginacion_cp_html = '<div style="display:flex;justify-content:center;gap:5px;flex-wrap:wrap;padding:10px 0;">'
        paginacion_cp_html += f'<a href="{_pag_cp_url(page_cp-1)}" style="padding:6px 10px;border:1px solid #ddd;border-radius:4px;text-decoration:none;color:#333;">&#8249; Ant.</a>' if page_cp > 1 else '<span style="padding:6px 10px;border:1px solid #ddd;border-radius:4px;color:#ccc;">&#8249; Ant.</span>'
        for _p in range(max(1, page_cp - 2), min(total_paginas_cp + 1, page_cp + 3)):
            if _p == page_cp:
                paginacion_cp_html += f'<span style="padding:6px 10px;border:1px solid #f97316;border-radius:4px;background:#f97316;color:white;font-weight:bold;">{_p}</span>'
            else:
                paginacion_cp_html += f'<a href="{_pag_cp_url(_p)}" style="padding:6px 10px;border:1px solid #ddd;border-radius:4px;text-decoration:none;color:#333;">{_p}</a>'
        paginacion_cp_html += f'<a href="{_pag_cp_url(page_cp+1)}" style="padding:6px 10px;border:1px solid #ddd;border-radius:4px;text-decoration:none;color:#333;">Sig. &#8250;</a>' if page_cp < total_paginas_cp else '<span style="padding:6px 10px;border:1px solid #ddd;border-radius:4px;color:#ccc;">Sig. &#8250;</span>'
        paginacion_cp_html += '</div>'
    
    # Obtener lista de obras Áºnicas
    obras_list = db.execute(
        "SELECT DISTINCT obra FROM control_pintura WHERE estado='activo' AND obra<>'' ORDER BY obra"
    ).fetchall()
    obras = [o[0] for o in obras_list]
    
    opciones_obras = '<option value="">-- Todas las obras --</option>'
    for o in obras:
        sel = 'selected' if o == obra_filtro else ''
        opciones_obras += f'<option value="{o}" {sel}>{o}</option>'
    
    filas_html = ""
    for ctrl in controles:
        ctrl_id, obra, fecha_creacion, fecha_mod = ctrl
        fmt_fecha = fecha_creacion.split(" ")[0] if fecha_creacion else "-"
        btn_editar = f'<a href="/modulo/calidad/escaneo/editar-control-pintura/{ctrl_id}" class="btn btn-edit">✏️ Editar</a>'
        btn_pdf = f'<a href="/modulo/calidad/escaneo/generar-pdf-control/{ctrl_id}" class="btn btn-pdf">📄 PDF</a>'
        filas_html += f"""
        <tr>
            <td>{ctrl_id}</td>
            <td>{html_lib.escape(obra)}</td>
            <td>{fmt_fecha}</td>
            <td style="text-align:center;">{btn_editar} {btn_pdf}</td>
        </tr>
        """
    
    if not filas_html:
        filas_html = "<tr><td colspan='4' style='text-align:center;color:#6b7280;'>No hay controles registrados</td></tr>"
    
    html = f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {{ font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }}
    h2 {{ color: #9a3412; border-bottom: 3px solid #f97316; padding-bottom: 10px; margin-top: 0; }}
    .box {{ background:white; border-radius:8px; padding:14px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); margin-bottom: 14px; }}
    .filtro {{ display:grid; grid-template-columns: 1fr 1fr auto; gap:10px; align-items:end; }}
    .filtro select, .filtro input {{ width:100%; padding:10px; border:1px solid #d1d5db; border-radius:6px; }}
    .btn {{ background:#f97316; color:white; border:none; padding:8px 12px; border-radius:6px; font-weight:bold; cursor:pointer; text-decoration:none; display:inline-block; font-size:12px; }}
    .btn:hover {{ background:#ea580c; }}
    .btn-blue {{ background:#2563eb; }}
    .btn-blue:hover {{ background:#1d4ed8; }}
    .btn-edit {{ background:#059669; }}
    .btn-edit:hover {{ background:#047857; }}
    .btn-pdf {{ background:#7c3aed; }}
    .btn-pdf:hover {{ background:#6d28d9; }}
    table {{ width:100%; border-collapse: collapse; background:white; }}
    th, td {{ border-bottom:1px solid #e5e7eb; padding:10px; text-align:left; font-size:13px; }}
    th {{ background:#f97316; color:white; font-weight:bold; }}
    .actions {{ display:flex; gap:10px; flex-wrap:wrap; margin-top:12px; }}
    .info-pag {{ text-align:center; color:#6b7280; font-size:12px; margin-bottom:6px; }}
    </style>
    </head>
    <body>
    <h2>📋 Controles de Pintura Anteriores</h2>

    <div class="box">
        <form method="get" action="/modulo/calidad/escaneo/controles-pintura">
            <div class="filtro">
                <div>
                    <label><b>Filtrar por obra</b></label>
                    <select name="obra">
                        {opciones_obras}
                    </select>
                </div>
                <div>
                    <label><b>Buscar por pieza</b></label>
                    <input type="text" name="busqueda_pieza" value="{html_lib.escape(busqueda_pieza_cp)}" placeholder="🔍 Nombre de pieza...">
                </div>
                <button type="submit" class="btn">🔍 Filtrar</button>
            </div>
        </form>
    </div>

    <div class="box">
    <p class="info-pag">Mostrando {min(offset_cp+1, total_controles) if total_controles>0 else 0}–{min(offset_cp+POR_PAGINA_CP, total_controles)} de {total_controles} controles</p>
        <table>
            <thead>
                <tr>
                    <th style="width:10%;">ID</th>
                    <th style="width:40%;">Obra</th>
                    <th style="width:25%;">Fecha Creación</th>
                    <th style="width:25%;">Acciones</th>
                </tr>
            </thead>
            <tbody>
                {filas_html}
            </tbody>
        </table>
        {paginacion_cp_html}
    </div>

    <div class="actions">
        <a href="/modulo/calidad/escaneo/control-pintura" class="btn btn-blue">➕ Nuevo Control</a>
        <a href="/modulo/calidad/escaneo" class="btn btn-blue">⬅️ Volver a Sub Módulos</a>
    </div>
    </body>
    </html>
    """
    return html

# ======================
# RUTA GENERAR PDF DESDE CONTROL GUARDADO
# ======================
@calidad_bp.route("/modulo/calidad/escaneo/generar-pdf-control/<int:control_id>", methods=["GET"])
def generar_pdf_control(control_id):
    from datetime import date
    db = get_db()
    ctrl_row = db.execute(
        "SELECT id, obra, mediciones, piezas FROM control_pintura WHERE id=? AND estado IN ('activo','en_progreso','completado')",
        (control_id,),
    ).fetchone()
    if not ctrl_row: return "Control no encontrado", 404
    ctrl_id, obra, mediciones_json, piezas_json = ctrl_row
    mediciones = json.loads(mediciones_json) if mediciones_json else {}
    if not isinstance(mediciones, dict):
        mediciones = {}
    filas_pintura = json.loads(piezas_json) if piezas_json else []
    if not isinstance(filas_pintura, list):
        filas_pintura = []
    operario_control = str(mediciones.get("operario") or "-")
    reinspecciones = mediciones.get("reinspeccion") if isinstance(mediciones.get("reinspeccion"), list) else []

    def _to_float(value):
        txt = str(value or "").strip().replace(",", ".")
        if not txt or txt == "-":
            return 0.0
        try:
            return float(txt)
        except Exception:
            return 0.0

    def _format_entero(value):
        num = _to_float(value)
        if num <= 0:
            return "-"
        return str(int(round(num)))
    
    # Obtener espesor requerido y esquema de la OT
    espesor_total_requerido = ""
    esquema_pintura = ""
    ot_data = db.execute(
        "SELECT espesor_total_requerido, esquema_pintura FROM ordenes_trabajo WHERE TRIM(COALESCE(obra,'')) = ? AND (es_mantenimiento IS NULL OR es_mantenimiento = 0) LIMIT 1",
        (obra,)
    ).fetchone()
    if ot_data:
        espesor_total_requerido = ot_data[0] or ""
        esquema_pintura = ot_data[1] or ""
    
    responsables_control = _obtener_responsables_control(db)
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
    from reportlab.lib.pagesizes import letter
    pdf_buffer = BytesIO()
    doc = SimpleDocTemplate(pdf_buffer, pagesize=letter, topMargin=0.4*cm, bottomMargin=0.6*cm, leftMargin=0.5*cm, rightMargin=0.5*cm)
    styles = getSampleStyleSheet()
    base_style = ParagraphStyle('BaseP', parent=styles['Normal'], fontSize=7.2, leading=8.4, textColor=colors.HexColor('#333333'))
    head_style = ParagraphStyle('HeadP', parent=styles['Normal'], fontSize=7.0, leading=8.0, alignment=1, fontName='Helvetica-Bold', textColor=colors.white)

    def _encabezado_pintura_path():
        candidatos = [
            os.path.join(_APP_DIR, "ENCABEZAO_PINTURA.png"),
            os.path.join(_APP_DIR, "ENCABEZAO_PINTURA.jpg"),
            os.path.join(_APP_DIR, "ENCABEZAO_PINTURA.jpeg"),
            os.path.join(_APP_DIR, "ENCABEZADO_PINTURA.png"),
            os.path.join(_APP_DIR, "ENCABEZADO_PINTURA.jpg"),
            os.path.join(_APP_DIR, "ENCABEZADO_PINTURA.jpeg"),
        ]
        for c in candidatos:
            if os.path.exists(c):
                return c
        return None

    def _firma_pdf_flowable(responsable_nombre):
        ruta = _ruta_firma_responsable(responsables_control, responsable_nombre)
        if not ruta:
            return Paragraph("-", base_style)
        try:
            img = RLImage(ruta)
            img.drawWidth = 1.5 * cm
            img.drawHeight = 0.5 * cm
            return img
        except Exception:
            return Paragraph("-", base_style)

    elements = []
    encabezado_pintura = _encabezado_pintura_path()
    if encabezado_pintura:
        try:
            encabezado_img = RLImage(encabezado_pintura)
            max_width = 19.8 * cm
            max_height = 3.2 * cm
            if encabezado_img.drawWidth > max_width:
                escala = max_width / float(encabezado_img.drawWidth)
                encabezado_img.drawWidth *= escala
                encabezado_img.drawHeight *= escala
            if encabezado_img.drawHeight > max_height:
                escala_h = max_height / float(encabezado_img.drawHeight)
                encabezado_img.drawWidth *= escala_h
                encabezado_img.drawHeight *= escala_h
            elements.append(encabezado_img)
        except Exception:
            elements.append(Paragraph("<b>CONTROL DE PINTURA</b>", ParagraphStyle('TitleP', parent=styles['Heading2'], fontSize=12, textColor=colors.HexColor('#111827'), alignment=0)))
    else:
        elements.append(Paragraph("<b>CONTROL DE PINTURA</b>", ParagraphStyle('TitleP', parent=styles['Heading2'], fontSize=12, textColor=colors.HexColor('#111827'), alignment=0)))
    elements.append(Spacer(1, 0.2*cm))

    info = Table([
        [Paragraph(f"<b>Obra:</b> {obra}", base_style), Paragraph(f"<b>Fecha reporte:</b> {date.today().isoformat()}", base_style)],
        [Paragraph(f"<b>Operario:</b> {html_lib.escape(operario_control)}", base_style), Paragraph(f"<b>Esquema de pintura:</b> {html_lib.escape(esquema_pintura or '-')}", base_style)],
        [Paragraph(f"<b>Espesor requerido (\u03bcm):</b> {html_lib.escape(str(espesor_total_requerido or '-'))}", base_style), Paragraph("", base_style)],
    ], colWidths=[9.9*cm, 9.9*cm])
    info.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#fff7ed')),
        ('BOX', (0, 0), (-1, -1), 0.6, colors.HexColor('#fdba74')),
        ('INNERGRID', (0, 0), (-1, -1), 0.35, colors.HexColor('#fed7aa')),
        ('LEFTPADDING', (0, 0), (-1, -1), 6),
        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
    ]))
    elements.append(info)
    elements.append(Spacer(1, 0.2*cm))
    
    # Sección de Tabla de Control
    elements.append(Paragraph("<b>Estado de Superficie y Control Pintura</b>", ParagraphStyle('Sec2', parent=styles['Normal'], fontSize=9.0, textColor=colors.HexColor('#9a3412'))))
    elements.append(Spacer(1, 0.08*cm))
    pie_table_data = [
        [Paragraph("<b>Pieza</b>", head_style), Paragraph("<b>Cant.</b>", head_style), Paragraph("<b>Descripción</b>", head_style), Paragraph("<b>Control Superficie</b>", head_style), "", Paragraph("<b>Fondo de Imprimación</b>", head_style), "", "", Paragraph("<b>Terminación</b>", head_style), "", "", Paragraph("<b>Resumen de Pintura</b>", head_style), ""],
        ["", "", "", Paragraph("<b>Estado</b>", head_style), Paragraph("<b>Resp. y Firma</b>", head_style), Paragraph("<b>Esp. Prom.</b>", head_style), Paragraph("<b>Fecha</b>", head_style), Paragraph("<b>Resp. y Firma</b>", head_style), Paragraph("<b>Esp. Prom.</b>", head_style), Paragraph("<b>Fecha</b>", head_style), Paragraph("<b>Resp. y Firma</b>", head_style), Paragraph("<b>Esp. Total</b>", head_style), Paragraph("<b>Estado</b>", head_style)],
    ]
    if filas_pintura:
        for r in filas_pintura:
            fondo_esp = _to_float(r.get('fondo_espesor'))
            term_esp = _to_float(r.get('term_espesor'))
            esp_total = _to_float(r.get('esp_total')) if str(r.get('esp_total') or '').strip() not in {'', '-'} else (fondo_esp + term_esp)
            esp_req = _to_float(r.get('esp_req')) or _to_float(espesor_total_requerido)
            estado_row = str(r.get('estado_pintura') or '').upper()
            if 'APROBADA' in estado_row:
                estado = 'OK'
            elif 'NO CONFORME' in estado_row or 'NO APROBADA' in estado_row:
                estado = 'NO CONFORME'
            else:
                estado = "OK" if esp_total >= esp_req and esp_total > 0 else ("NO CONFORME" if esp_total > 0 else "-")
            sup_estado_raw = str(r.get("sup_estado") or "-").strip().upper()
            sup_estado_display = "OK" if sup_estado_raw in ("CONFORME", "OK", "APROBADO") else ("NO CONFORME" if sup_estado_raw in ("NO CONFORME", "NC") else sup_estado_raw)
            
            pie_table_data.append([
                Paragraph(str(r.get("pieza") or "-"), base_style),
                Paragraph(str(r.get("cantidad") or "-"), base_style),
                Paragraph(str(r.get("descripcion") or "-"), base_style),
                Paragraph(sup_estado_display, base_style),
                _firma_pdf_flowable(r.get("sup_resp") or ""),
                Paragraph(f"{fondo_esp:.1f}" if fondo_esp else "-", base_style),
                Paragraph(str(r.get("fondo_fecha", "") or "-"), base_style),
                _firma_pdf_flowable(r.get("fondo_resp") or ""),
                Paragraph(f"{term_esp:.1f}" if term_esp else "-", base_style),
                Paragraph(str(r.get("term_fecha", "") or "-"), base_style),
                _firma_pdf_flowable(r.get("term_resp") or ""),
                Paragraph(f"{esp_total:.1f}" if esp_total > 0 else "-", base_style),
                Paragraph(estado, base_style),
            ])
    else:
        pie_table_data.append([Paragraph("-", base_style)] + [Paragraph("-", base_style) for _ in range(12)])
    pie_table = Table(
        pie_table_data,
        colWidths=[1.75*cm, 0.9*cm, 2.1*cm, 1.25*cm, 1.9*cm, 0.95*cm, 1.55*cm, 1.9*cm, 0.95*cm, 1.55*cm, 1.9*cm, 1.1*cm, 1.85*cm],
        repeatRows=2,
    )
    pie_table.setStyle(TableStyle([
        ('SPAN', (0, 0), (0, 1)),
        ('SPAN', (1, 0), (1, 1)),
        ('SPAN', (2, 0), (2, 1)),
        ('SPAN', (3, 0), (4, 0)),
        ('SPAN', (5, 0), (7, 0)),
        ('SPAN', (8, 0), (10, 0)),
        ('SPAN', (11, 0), (12, 0)),
        ('BACKGROUND', (0, 0), (2, 1), colors.HexColor('#f97316')),
        ('BACKGROUND', (3, 0), (4, 1), colors.HexColor('#fb923c')),
        ('BACKGROUND', (5, 0), (7, 1), colors.HexColor('#f97316')),
        ('BACKGROUND', (8, 0), (10, 1), colors.HexColor('#fb923c')),
        ('BACKGROUND', (11, 0), (12, 1), colors.HexColor('#f97316')),
        ('TEXTCOLOR', (0, 0), (-1, 1), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#cbd5e1')),
        ('ROWBACKGROUNDS', (0, 2), (-1, -1), [colors.white, colors.HexColor('#fff7ed')]),
        ('LEFTPADDING', (0, 0), (-1, -1), 3),
        ('RIGHTPADDING', (0, 0), (-1, -1), 3),
        ('TOPPADDING', (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ('ALIGN', (2, 2), (2, -1), 'LEFT'),
    ]))
    elements.append(pie_table)

    # Sección Re-inspección
    elements.append(Spacer(1, 0.18*cm))
    elements.append(Paragraph("<b>Re-inspección (solo piezas NC)</b>", ParagraphStyle('Sec3', parent=styles['Normal'], fontSize=9.0, textColor=colors.HexColor('#9a3412'))))
    elements.append(Spacer(1, 0.08*cm))
    ri_table_data = [
        [
            Paragraph("<b>Posición</b>", head_style),
            Paragraph("<b>Proceso</b>", head_style),
            Paragraph("<b>Motivo</b>", head_style),
            Paragraph("<b>Fecha</b>", head_style),
            Paragraph("<b>Acción correctiva</b>", head_style),
            Paragraph("<b>Responsable</b>", head_style),
            Paragraph("<b>Firma</b>", head_style),
        ]
    ]
    if reinspecciones:
        for ri in reinspecciones:
            ri_table_data.append(
                [
                    Paragraph(str((ri or {}).get("posicion") or "-"), base_style),
                    Paragraph(str((ri or {}).get("proceso") or "-"), base_style),
                    Paragraph(str((ri or {}).get("motivo") or "-"), base_style),
                    Paragraph(str((ri or {}).get("fecha") or "-"), base_style),
                    Paragraph(str((ri or {}).get("accion_correctiva") or "-"), base_style),
                    Paragraph(str((ri or {}).get("responsable") or "-"), base_style),
                    _firma_pdf_flowable(str((ri or {}).get("responsable") or "")),
                ]
            )
    else:
        ri_table_data.append([Paragraph("-", base_style) for _ in range(7)])

    ri_table = Table(ri_table_data, colWidths=[2.0*cm, 2.2*cm, 2.2*cm, 2.0*cm, 6.2*cm, 2.8*cm, 2.4*cm], repeatRows=1)
    ri_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f97316')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#cbd5e1')),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fff7ed')]),
        ('LEFTPADDING', (0, 0), (-1, -1), 3),
        ('RIGHTPADDING', (0, 0), (-1, -1), 3),
        ('TOPPADDING', (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ('ALIGN', (4, 1), (4, -1), 'LEFT'),
    ]))
    elements.append(ri_table)

    doc.build(elements)
    pdf_buffer.seek(0)
    filename = f"Control_Pintura_{obra}_ID{control_id}_{date.today().isoformat()}.pdf".replace(" ", "_")
    return send_file(pdf_buffer, mimetype='application/pdf', as_attachment=True, download_name=filename)

# ======================
# RUTA EDITAR CONTROL DE PINTURA
# ======================
@calidad_bp.route("/modulo/calidad/escaneo/editar-control-pintura/<int:control_id>", methods=["GET", "POST"])
def editar_control_pintura(control_id):
    from datetime import date
    db = get_db()
    ctrl_row = db.execute("SELECT id, obra, mediciones, piezas FROM control_pintura WHERE id=? AND estado='activo'", (control_id,)).fetchone()
    if not ctrl_row: return "Control no encontrado", 404
    ctrl_id, obra, mediciones_json, piezas_json = ctrl_row
    mediciones = json.loads(mediciones_json) if mediciones_json else []
    filas_pintura = json.loads(piezas_json) if piezas_json else []
    
    # Obtener espesor requerido y esquema de la OT
    espesor_total_requerido = ""
    esquema_pintura = ""
    ot_data = db.execute(
        "SELECT espesor_total_requerido, esquema_pintura FROM ordenes_trabajo WHERE TRIM(COALESCE(obra,'')) = ? AND (es_mantenimiento IS NULL OR es_mantenimiento = 0) LIMIT 1",
        (obra,)
    ).fetchone()
    if ot_data:
        espesor_total_requerido = ot_data[0] or ""
        esquema_pintura = ot_data[1] or ""
    
    responsables_control = _obtener_responsables_control(db)
    firmas_responsables = {k: v.get("firma", "") for k, v in responsables_control.items()}
    if request.method == "POST" and (request.form.get("accion") or "").strip().lower() == "pdf":
        def _to_float(val):
            txt = str(val or "").strip().replace(",", ".")
            if not txt: return 0.0
            try: return float(txt)
            except: return 0.0
        piezas_form, cantidades_form, desc_form, sup_estado_form, sup_resp_form, sup_firma_form = request.form.getlist("pieza[]"), request.form.getlist("cantidad[]"), request.form.getlist("descripcion[]"), request.form.getlist("sup_estado[]"), request.form.getlist("sup_responsable[]"), request.form.getlist("sup_firma[]")
        fondo_espesor_form, fondo_fecha_form, fondo_resp_form, fondo_firma_form = request.form.getlist("fondo_espesor[]"), request.form.getlist("fondo_fecha[]"), request.form.getlist("fondo_responsable[]"), request.form.getlist("fondo_firma[]")
        term_espesor_form, term_fecha_form, term_resp_form, term_firma_form = request.form.getlist("term_espesor[]"), request.form.getlist("term_fecha[]"), request.form.getlist("term_responsable[]"), request.form.getlist("term_firma[]")
        filas_pintura_nuevas = []
        for i in range(len(piezas_form)):
            pieza = (piezas_form[i] if i < len(piezas_form) else "").strip()
            if not pieza: continue
            sup_resp_nombre = (sup_resp_form[i] if i < len(sup_resp_form) else "").strip()
            sup_firma = responsables_control.get(sup_resp_nombre, {}).get("firma", "") if sup_resp_nombre else ""
            fondo_resp_nombre = (fondo_resp_form[i] if i < len(fondo_resp_form) else "").strip()
            fondo_firma = responsables_control.get(fondo_resp_nombre, {}).get("firma", "") if fondo_resp_nombre else ""
            term_resp_nombre = (term_resp_form[i] if i < len(term_resp_form) else "").strip()
            term_firma = responsables_control.get(term_resp_nombre, {}).get("firma", "") if term_resp_nombre else ""
            filas_pintura_nuevas.append({"pieza": pieza, "cantidad": (cantidades_form[i] if i < len(cantidades_form) else "").strip(), "descripcion": (desc_form[i] if i < len(desc_form) else "").strip(), "sup_estado": (sup_estado_form[i] if i < len(sup_estado_form) else "").strip().upper(), "sup_resp": sup_resp_nombre, "sup_firma": sup_firma, "fondo_espesor": _to_float(fondo_espesor_form[i] if i < len(fondo_espesor_form) else ""), "fondo_fecha": (fondo_fecha_form[i] if i < len(fondo_fecha_form) else "").strip(), "fondo_resp": fondo_resp_nombre, "fondo_firma": fondo_firma, "term_espesor": _to_float(term_espesor_form[i] if i < len(term_espesor_form) else ""), "term_fecha": (term_fecha_form[i] if i < len(term_fecha_form) else "").strip(), "term_resp": term_resp_nombre, "term_firma": term_firma})
        med_fechas, med_horas, med_temps, med_humedades = request.form.getlist("med_fecha[]"), request.form.getlist("med_hora[]"), request.form.getlist("med_temp[]"), request.form.getlist("med_humedad[]")
        mediciones_nuevas = []
        for i in range(max(len(med_fechas), len(med_horas), len(med_temps), len(med_humedades))):
            fecha_m = (med_fechas[i] if i < len(med_fechas) else "").strip()
            if not any([(med_fechas[i] if i < len(med_fechas) else "").strip(), (med_horas[i] if i < len(med_horas) else "").strip(), (med_temps[i] if i < len(med_temps) else "").strip(), (med_humedades[i] if i < len(med_humedades) else "").strip()]): continue
            mediciones_nuevas.append({"mano": str(i+1), "fecha": fecha_m, "hora": (med_horas[i] if i < len(med_horas) else "").strip(), "temp": (med_temps[i] if i < len(med_temps) else "").strip(), "humedad": (med_humedades[i] if i < len(med_humedades) else "").strip()})
        db.execute("UPDATE control_pintura SET mediciones=?, piezas=?, fecha_modificacion=CURRENT_TIMESTAMP WHERE id=?", (json.dumps(mediciones_nuevas), json.dumps(filas_pintura_nuevas), control_id))
        db.commit()
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
        from reportlab.lib.pagesizes import landscape, letter
        pdf_buffer = BytesIO()
        doc = SimpleDocTemplate(pdf_buffer, pagesize=landscape(letter), topMargin=0.5*cm, bottomMargin=0.6*cm, leftMargin=0.6*cm, rightMargin=0.6*cm)
        styles = getSampleStyleSheet()
        base_style = ParagraphStyle('BaseP', parent=styles['Normal'], fontSize=7.2, leading=8.4, textColor=colors.HexColor('#1f2937'))
        head_style = ParagraphStyle('HeadP', parent=styles['Normal'], fontSize=7.1, leading=8.2, alignment=1, fontName='Helvetica-Bold', textColor=colors.white)
        title_style = ParagraphStyle('TitleP', parent=styles['Heading2'], fontSize=14, textColor=colors.HexColor('#111827'), alignment=0)

        def _encabezado_pintura_path():
            candidatos = [
                os.path.join(_APP_DIR, "ENCABEZAO_PINTURA.png"),
                os.path.join(_APP_DIR, "ENCABEZAO_PINTURA.jpg"),
                os.path.join(_APP_DIR, "ENCABEZAO_PINTURA.jpeg"),
                os.path.join(_APP_DIR, "ENCABEZADO_PINTURA.png"),
                os.path.join(_APP_DIR, "ENCABEZADO_PINTURA.jpg"),
                os.path.join(_APP_DIR, "ENCABEZADO_PINTURA.jpeg"),
            ]
            for c in candidatos:
                if os.path.exists(c):
                    return c
            return None

        def _firma_pdf_flowable(responsable_nombre):
            ruta = _ruta_firma_responsable(responsables_control, responsable_nombre)
            if not ruta:
                return Paragraph("-", base_style)
            try:
                img = RLImage(ruta)
                img.drawWidth = 1.9 * cm
                img.drawHeight = 0.55 * cm
                return img
            except Exception:
                return Paragraph("-", base_style)

        elements = []
        encabezado_pintura = _encabezado_pintura_path()
        if encabezado_pintura:
            try:
                encabezado_img = RLImage(encabezado_pintura)
                max_width = 26.0 * cm
                if encabezado_img.drawWidth > max_width:
                    escala = max_width / float(encabezado_img.drawWidth)
                    encabezado_img.drawWidth *= escala
                    encabezado_img.drawHeight *= escala
                elements.append(encabezado_img)
            except Exception:
                elements.append(Paragraph("<b>CONTROL DE PINTURA (EDITADO)</b>", title_style))
        else:
            elements.append(Paragraph("<b>CONTROL DE PINTURA (EDITADO)</b>", title_style))
        elements.append(Spacer(1, 0.2*cm))
        info = Table([[Paragraph(f"<b>Obra:</b> {obra}", base_style), Paragraph(f"<b>Fecha:</b> {date.today().isoformat()}", base_style)]])
        elements.append(info)
        elements.append(Spacer(1, 0.25*cm))
        
        # Sección de Datos de Entrada
        elementos_entrada = [
            [
                Paragraph("<b>Esquema de Pintura</b>", head_style), 
                Paragraph(esquema_pintura or "-", base_style),
                Paragraph("<b>Espesor Total Requerido (μm)</b>", head_style),
                Paragraph(espesor_total_requerido or "-", base_style),
            ]
        ]
        entrada_table = Table(elementos_entrada, colWidths=[4.5*cm, 4.5*cm, 5.0*cm, 4.5*cm])
        entrada_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (0, 0), colors.HexColor('#e8f4f8')),
            ('BACKGROUND', (2, 0), (2, 0), colors.HexColor('#e8f4f8')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.HexColor('#0c4a6e')),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#cbd5e1')),
            ('LEFTPADDING', (0, 0), (-1, -1), 5),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        elements.append(entrada_table)
        elements.append(Spacer(1, 0.2*cm))
        
        # Sección de Tabla de Control
        elements.append(Paragraph("<b>1) Estado de Superficie y Control Pintura</b>", ParagraphStyle('Sec2', parent=styles['Normal'], fontSize=10, textColor=colors.HexColor('#9a3412'))))
        elements.append(Spacer(1, 0.08*cm))
        pie_table_data = [
            [Paragraph("<b>Pieza</b>", head_style), Paragraph("<b>Cant.</b>", head_style), Paragraph("<b>Descripción</b>", head_style), Paragraph("<b>Control Superficie</b>", head_style), "", Paragraph("<b>Fondo de Imprimación</b>", head_style), "", "", Paragraph("<b>Terminación</b>", head_style), "", "", Paragraph("<b>Resumen de Pintura</b>", head_style), "", ""],
            ["", "", "", Paragraph("<b>Estado</b>", head_style), Paragraph("<b>Resp. y Firma</b>", head_style), Paragraph("<b>Esp. Prom.</b>", head_style), Paragraph("<b>Fecha</b>", head_style), Paragraph("<b>Resp. y Firma</b>", head_style), Paragraph("<b>Esp. Prom.</b>", head_style), Paragraph("<b>Fecha</b>", head_style), Paragraph("<b>Resp. y Firma</b>", head_style), Paragraph("<b>Esp. Total</b>", head_style), Paragraph("<b>Esp. Req.</b>", head_style), Paragraph("<b>Estado</b>", head_style)],
        ]
        for r in filas_pintura_nuevas:
            fondo_esp = r.get('fondo_espesor', 0) or 0
            term_esp = r.get('term_espesor', 0) or 0
            esp_total = fondo_esp + term_esp
            esp_req = float(espesor_total_requerido or 0)
            estado = "APROBADO" if esp_total >= esp_req and esp_total > 0 else ("NO CONFORME" if esp_total > 0 else "-")
            
            pie_table_data.append([
                Paragraph(r["pieza"], base_style),
                Paragraph(r["cantidad"] or "-", base_style),
                Paragraph(r["descripcion"] or "-", base_style),
                Paragraph(r["sup_estado"] or "-", base_style),
                _firma_pdf_flowable(r.get("sup_resp") or ""),
                Paragraph(f"{fondo_esp:.1f}" if fondo_esp else "-", base_style),
                Paragraph(r.get("fondo_fecha", "") or "-", base_style),
                _firma_pdf_flowable(r.get("fondo_resp") or ""),
                Paragraph(f"{term_esp:.1f}" if term_esp else "-", base_style),
                Paragraph(r.get("term_fecha", "") or "-", base_style),
                _firma_pdf_flowable(r.get("term_resp") or ""),
                Paragraph(f"{esp_total:.1f}" if esp_total > 0 else "-", base_style),
                Paragraph(f"{esp_req:.1f}" if esp_req > 0 else "-", base_style),
                Paragraph(estado, base_style),
            ])
        pie_table = Table(pie_table_data, colWidths=[1.5*cm]*14)
        pie_table.setStyle(TableStyle([
            ('SPAN', (0, 0), (0, 1)),
            ('SPAN', (1, 0), (1, 1)),
            ('SPAN', (2, 0), (2, 1)),
            ('SPAN', (3, 0), (4, 0)),
            ('SPAN', (5, 0), (7, 0)),
            ('SPAN', (8, 0), (10, 0)),
            ('SPAN', (11, 0), (13, 0)),
            ('BACKGROUND', (0, 0), (2, 1), colors.HexColor('#f97316')),
            ('BACKGROUND', (3, 0), (4, 1), colors.HexColor('#ea580c')),
            ('BACKGROUND', (5, 0), (7, 1), colors.HexColor('#f97316')),
            ('BACKGROUND', (8, 0), (10, 1), colors.HexColor('#ea580c')),
            ('BACKGROUND', (11, 0), (13, 1), colors.HexColor('#f97316')),
            ('TEXTCOLOR', (0, 0), (-1, 1), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#d1d5db')),
            ('ROWBACKGROUNDS', (0, 2), (-1, -1), [colors.white, colors.HexColor('#fff7ed')]),
            ('LEFTPADDING', (0, 0), (-1, -1), 2),
            ('RIGHTPADDING', (0, 0), (-1, -1), 2),
            ('TOPPADDING', (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ]))
        elements.append(pie_table)
        doc.build(elements)
        pdf_buffer.seek(0)
        filename = f"Control_Pintura_{obra}_ID{control_id}_EDITADO_{date.today().isoformat()}.pdf".replace(" ", "_")
        return send_file(pdf_buffer, mimetype='application/pdf', as_attachment=True, download_name=filename)
    opciones_resp = '<option value="">Sel...</option>' + "".join(f'<option value="{html_lib.escape(k)}">{html_lib.escape(k)}</option>' for k in sorted(responsables_control.keys()))
    med_html = ""
    for i in range(1, 5):
        med = mediciones[i-1] if i-1 < len(mediciones) else {}
        med_html += f'<tr><td>M{i}</td><td><input type="date" name="med_fecha[]" value="{med.get("fecha", "")}"></td><td><input type="time" name="med_hora[]" value="{med.get("hora", "")}"></td><td><input type="number" step="0.1" name="med_temp[]" value="{med.get("temp", "")}" placeholder="°C"></td><td><input type="number" step="0.1" name="med_humedad[]" value="{med.get("humedad", "")}" placeholder="%"></td></tr>'
    piezas_html = ""
    for idx, p in enumerate(filas_pintura, 1):
        piezas_html += f'<tr><td>{html_lib.escape(p.get("pieza", ""))}<input type="hidden" name="pieza[]" value="{html_lib.escape(p.get("pieza", ""))}"></td><td>{html_lib.escape(p.get("cantidad", ""))}<input type="hidden" name="cantidad[]" value="{html_lib.escape(p.get("cantidad", ""))}"></td><td><input type="hidden" name="descripcion[]" value="{html_lib.escape(p.get("descripcion", ""))}">{html_lib.escape(p.get("descripcion", ""))}</td><td><select name="sup_estado[]"><option>Sel</option><option {"selected" if p.get("sup_estado") == "CONFORME" else ""}>OK</option><option {"selected" if p.get("sup_estado") == "NO CONFORME" else ""}>NO</option></select></td><td><select name="sup_responsable[]" class="sr" data-i="{idx}">{opciones_resp}</select></td><td><input type="text" name="sup_firma[]" id="sf{idx}" value="{html_lib.escape(p.get("sup_resp", ""))}" readonly></td><td><input type="number" step="0.01" name="mano1[]" value="{p.get("mano1", 0)}" class="m1" data-i="{idx}"></td><td><input type="number" step="0.01" name="mano2[]" value="{p.get("mano2", 0)}" class="m2" data-i="{idx}"></td><td><input type="number" step="0.01" name="mano3[]" value="{p.get("mano3", 0)}" class="m3" data-i="{idx}"></td><td><input type="number" step="0.01" name="mano4[]" value="{p.get("mano4", 0)}" class="m4" data-i="{idx}"></td><td><input type="number" step="0.01" name="espesor_solicitado[]" value="{p.get("espesor", 0)}" class="esp" data-i="{idx}"></td><td><input type="text" id="ef{idx}" value="{p.get("estado_final", "")}" readonly></td><td><select name="pintura_responsable[]" class="pr" data-i="{idx}">{opciones_resp}</select></td><td><input type="text" name="pintura_firma[]" id="pf{idx}" value="{html_lib.escape(p.get("pint_resp", ""))}" readonly></td></tr>'
    return f'<html><head><style>body{{font-family:Arial;padding:10px;}}table{{width:100%;border-collapse:collapse;}}th,td{{border:1px solid #ddd;padding:5px;font-size:10px;}}th{{background:#f97316;color:white;}}input,select{{width:100%;box-sizing:border-box;padding:4px;}}button{{background:#f97316;color:white;border:none;padding:6px 10px;border-radius:4px;cursor:pointer;}}</style></head><body><h2>✏️ Editar Control ID {control_id}</h2><form method="post"><input type="hidden" name="accion" value="pdf"><table><tr><th>M</th><th>Fecha</th><th>Hora</th><th>T°C</th><th>%H</th></tr>{med_html}</table><table><tr><th colspan="6">Pieza</th><th colspan="8">Pintura</th></tr><tr><th>Pieza</th><th>Cant</th><th>Desc</th><th>Est</th><th>Resp</th><th>Firma</th><th>M1</th><th>M2</th><th>M3</th><th>M4</th><th>Esp</th><th>EF</th><th>Resp</th><th>Firma</th></tr>{piezas_html}</table><br><button>Guardar PDF</button> <a href="/modulo/calidad/escaneo/controles-pintura" style="padding:6px 10px;background:#2563eb;color:white;text-decoration:none;border-radius:4px;">Volver</a></form><script>const f={json.dumps(firmas_responsables)};function uf(s,id){{document.getElementById(id).value=s.value||"";}}document.querySelectorAll(".sr").forEach(s=>s.addEventListener("change",()=>uf(s,"sf"+s.dataset.i)));document.querySelectorAll(".pr").forEach(s=>s.addEventListener("change",()=>uf(s,"pf"+s.dataset.i)));function ue(i){{const m4=parseFloat(document.querySelector(".m4[data-i=\'"+i+"\']").value)||0;const e=parseFloat(document.querySelector(".esp[data-i=\'"+i+"\']").value)||0;document.getElementById("ef"+i).value=m4>e?"OK":"NO";}}document.querySelectorAll(".m4,.esp").forEach(x=>x.addEventListener("input",()=>ue(x.dataset.i)));</script></body></html>'

# ======================
# NUEVA RUTA: CONTROL DE PINTURA CON FLUJO DE 5 PASOS
# ======================
@calidad_bp.route("/modulo/calidad/escaneo/control-pintura", methods=["GET", "POST"])
def control_pintura_nuevo():
    from datetime import date, datetime

    # MODO CARGA: Formulario simple para operarios
    if request.method == "GET":
        db = get_db()
        responsables_control = _obtener_responsables_control(db)
        responsables_list = sorted(responsables_control.keys())
        operarios_disponibles = _obtener_operarios_disponibles(db)
        operarios_list = sorted(set(operarios_disponibles))

        # Obtener OTs activas con piezas disponibles para control pintura.
        ot_rows = db.execute(
            """
            SELECT
                ot.id,
                TRIM(COALESCE(ot.obra, '')) AS obra,
                COALESCE(ot.titulo, '') AS titulo,
                COALESCE(ot.esquema_pintura, '') AS esquema,
                COALESCE(ot.espesor_total_requerido, '') AS espesor
            FROM ordenes_trabajo ot
            WHERE ot.fecha_cierre IS NULL
              AND (ot.es_mantenimiento IS NULL OR ot.es_mantenimiento = 0)
              AND TRIM(COALESCE(ot.obra, '')) <> ''
              AND EXISTS (
                    SELECT 1
                    FROM procesos p
                    WHERE TRIM(COALESCE(p.obra, '')) = TRIM(COALESCE(ot.obra, ''))
                      AND TRIM(COALESCE(p.posicion, '')) <> ''
                      AND COALESCE(p.eliminado, 0) = 0
              )
            ORDER BY ot.id DESC
            """
        ).fetchall()
        if not ot_rows:
            ot_rows = db.execute(
                """
                SELECT
                    ot.id,
                    TRIM(COALESCE(ot.obra, '')) AS obra,
                    COALESCE(ot.titulo, '') AS titulo,
                    COALESCE(ot.esquema_pintura, '') AS esquema,
                    COALESCE(ot.espesor_total_requerido, '') AS espesor
                FROM ordenes_trabajo ot
                WHERE ot.fecha_cierre IS NULL
                  AND (ot.es_mantenimiento IS NULL OR ot.es_mantenimiento = 0)
                  AND TRIM(COALESCE(ot.obra, '')) <> ''
                ORDER BY ot.id DESC
                """
            ).fetchall()

        ot_meta = {}
        for ot_id, obra_ot, titulo_ot, esquema_ot, espesor_ot in ot_rows:
            ot_meta[str(ot_id)] = {
                "obra": str(obra_ot or "").strip(),
                "titulo": str(titulo_ot or "").strip(),
                "esquema": str(esquema_ot or "").strip(),
                "espesor": str(espesor_ot or "").strip(),
            }

        ot_id_sel_txt = (request.args.get("ot_id") or "").strip()
        ot_id_sel = int(ot_id_sel_txt) if ot_id_sel_txt.isdigit() else None
        obra_sel = ""
        esquema_sel = ""
        espesor_sel = ""

        if ot_id_sel_txt and ot_id_sel_txt in ot_meta:
            obra_sel = ot_meta[ot_id_sel_txt]["obra"]
            esquema_sel = ot_meta[ot_id_sel_txt]["esquema"]
            espesor_sel = ot_meta[ot_id_sel_txt]["espesor"]

        # Obtener piezas para la OT/obra seleccionada y etapa actual
        etapa_sel = (request.args.get("etapa") or "").strip().upper()
        if etapa_sel not in ("SUPERFICIE", "FONDO", "TERMINACION"):
            etapa_sel = ""

        piezas_list = []
        piezas_all = []
        historial_por_pieza = {}
        nc_records_preview = []
        piezas_preview_list = []
        if obra_sel and ot_id_sel is not None:
            piezas_rows = db.execute(
                """
                SELECT DISTINCT TRIM(COALESCE(posicion, '')) AS posicion
                FROM procesos
                WHERE ot_id = ?
                    AND TRIM(COALESCE(posicion, '')) <> ''
                    AND eliminado = 0
                ORDER BY posicion ASC
                """,
                (ot_id_sel,)
            ).fetchall()
            piezas_all = [r[0] for r in piezas_rows]

            hist_rows = db.execute(
                """
                SELECT id, TRIM(COALESCE(posicion, '')) AS posicion,
                       UPPER(TRIM(COALESCE(proceso, ''))) AS proceso,
                       UPPER(TRIM(COALESCE(estado, ''))) AS estado,
                       COALESCE(reproceso, '') AS reproceso,
                       COALESCE(fecha, '') AS fecha,
                       COALESCE(estado_pieza, '') AS estado_pieza,
                       COALESCE(re_inspeccion, '') AS re_inspeccion
                FROM procesos
                                WHERE ot_id = ?
                  AND eliminado = 0
                  AND UPPER(TRIM(COALESCE(proceso, ''))) IN ('PINTURA', 'PINTURA_FONDO')
                ORDER BY id DESC
                """,
                                (ot_id_sel,),
            ).fetchall()

            ultimo_por_pieza_etapa = {}
            for _, pos_h, proc_h, estado_h, repro_h, fecha_h, estado_pieza_h, re_insp_h in hist_rows:
                if not pos_h:
                    continue
                repro_u = (repro_h or "").upper()
                if "ETAPA:SUPERFICIE" in repro_u:
                    etapa_h = "SUPERFICIE"
                elif "ETAPA:FONDO" in repro_u:
                    etapa_h = "FONDO"
                elif "ETAPA:TERMINACION" in repro_u:
                    etapa_h = "TERMINACION"
                elif proc_h == "PINTURA_FONDO":
                    etapa_h = "FONDO"
                else:
                    continue  # skip records without explicit ETAPA tag (legacy data)

                key = (pos_h, etapa_h)
                if key in ultimo_por_pieza_etapa:
                    continue

                estado_txt = (estado_h or "-").strip().upper()
                estado_pieza_txt = (estado_pieza_h or "").strip().upper()
                # Resolver estado efectivo considerando ciclos de re-inspección
                if estado_txt == "NC" and re_insp_h:
                    ciclos_h = _extraer_ciclos_reinspeccion(re_insp_h)
                    if ciclos_h:
                        ultimo_ciclo_estado = ciclos_h[-1].get("estado", "").strip().upper()
                        if ultimo_ciclo_estado in ("OK", "APROBADO", "CONFORME", "OBS", "OM"):
                            estado_txt = ultimo_ciclo_estado
                # Normalizar etiqueta de display
                if estado_txt in ("OK", "APROBADO", "CONFORME"):
                    disp = f"OK ({fecha_h or '-'})"
                elif estado_txt == "NC":
                    disp = f"NO CONFORME ({fecha_h or '-'})"
                elif estado_txt in ("OBS", "OM"):
                    disp = f"{estado_txt} ({fecha_h or '-'})"
                else:
                    disp = f"{estado_txt or '-'} ({fecha_h or '-'})"
                if estado_pieza_txt == "RE-INSPECCION" and estado_txt == "NC":
                    disp = f"RE-INSPECCION ({fecha_h or '-'})"

                ultimo_por_pieza_etapa[key] = {
                    "estado": estado_txt,
                    "estado_pieza": estado_pieza_txt,
                    "display": disp,
                }

            for p in piezas_all:
                historial_por_pieza[p] = {
                    "SUPERFICIE": "-",
                    "FONDO": "-",
                    "TERMINACION": "-",
                }

            for (p, e), val in ultimo_por_pieza_etapa.items():
                if p not in historial_por_pieza:
                    historial_por_pieza[p] = {
                        "SUPERFICIE": "-",
                        "FONDO": "-",
                        "TERMINACION": "-",
                    }
                historial_por_pieza[p][e] = val["display"]

            # --- Recolectar NC records para sección de re-inspección en preview ---
            for _, pos_h2, proc_h2, estado_h2, repro_h2, fecha_h2, _sp2, _ri2 in hist_rows:
                if not pos_h2:
                    continue
                est_h2 = (estado_h2 or "").strip().upper()
                if est_h2 not in ("NC", "NO CONFORME", "NO CONFORMIDAD"):
                    continue
                repro_u2 = (repro_h2 or "").upper()
                if "ETAPA:SUPERFICIE" in repro_u2:
                    etapa_h2 = "SUPERFICIE"
                elif "ETAPA:FONDO" in repro_u2:
                    etapa_h2 = "FONDO"
                elif "ETAPA:TERMINACION" in repro_u2:
                    etapa_h2 = "TERMINACION"
                elif (proc_h2 or "") == "PINTURA_FONDO":
                    etapa_h2 = "FONDO"
                else:
                    continue
                repro_raw2 = (repro_h2 or "").strip()
                for pfx2 in ("ETAPA:TERMINACION", "ETAPA:FONDO", "ETAPA:SUPERFICIE"):
                    if repro_raw2.upper().startswith(pfx2):
                        repro_raw2 = repro_raw2[len(pfx2):].lstrip(" |").strip()
                        break
                motivo_nc2 = repro_raw2.replace("Motivo NC:", "").strip(" |") or "-"
                final_info2 = ultimo_por_pieza_etapa.get((pos_h2, etapa_h2), {})
                est_final2 = final_info2.get("estado", "NC").strip().upper()
                est_final_label2 = "OK" if est_final2 in ("OK", "APROBADO", "CONFORME", "OBS", "OM") else "NO CONFORME"
                nc_records_preview.append({
                    "posicion": pos_h2,
                    "etapa": {"SUPERFICIE": "Superficie", "FONDO": "Fondo", "TERMINACION": "Terminación"}.get(etapa_h2, etapa_h2),
                    "motivo": motivo_nc2,
                    "fecha": fecha_h2 or "-",
                    "estado_final": est_final_label2,
                })

            # --- Preview enriquecida: datos completos por pieza/etapa ---
            def _esp_str_prev(txt, lbl):
                import re as _re_prev
                m = _re_prev.search(rf"{lbl}:\s*([0-9]+(?:[.,][0-9]+)?)", txt or "", flags=_re_prev.IGNORECASE)
                return str(int(round(float(m.group(1).replace(",", "."))))) if m else "-"

            pv_rows = db.execute(
                """
                SELECT TRIM(COALESCE(posicion,'')) AS pos,
                       UPPER(TRIM(COALESCE(proceso,''))) AS proc,
                       UPPER(TRIM(COALESCE(estado,''))) AS est,
                       COALESCE(reproceso,'') AS rep,
                       COALESCE(fecha,'') AS fec,
                       COALESCE(firma_digital,'') AS firma,
                       COALESCE(cantidad,0) AS cant,
                       COALESCE(perfil,'') AS perfil,
                       COALESCE(descripcion,'') AS desc_txt
                FROM procesos
                                WHERE ot_id = ?
                  AND eliminado = 0
                  AND UPPER(TRIM(COALESCE(proceso,''))) IN ('PINTURA','PINTURA_FONDO')
                ORDER BY id DESC
                """,
                                (ot_id_sel,),
            ).fetchall()

            meta_rows = db.execute(
                """
                SELECT TRIM(COALESCE(posicion,'')) AS pos,
                       COALESCE(cantidad, 0) AS cant,
                       COALESCE(perfil, '') AS perfil,
                       COALESCE(descripcion, '') AS desc_txt
                FROM procesos
                WHERE ot_id = ?
                  AND eliminado = 0
                  AND TRIM(COALESCE(posicion,'')) <> ''
                ORDER BY id DESC
                """,
                (ot_id_sel,),
            ).fetchall()

            meta_por_pieza = {}
            for m_pos, m_cant, m_perfil, m_desc in meta_rows:
                key_m = str(m_pos or "").strip()
                if not key_m or key_m in meta_por_pieza:
                    continue
                try:
                    cant_num_m = float(str(m_cant or 0).replace(",", "."))
                    cant_disp_m = str(int(round(cant_num_m))) if cant_num_m > 0 else "-"
                except Exception:
                    cant_disp_m = "-"
                meta_por_pieza[key_m] = {
                    "cant": cant_disp_m,
                    "desc": (str(m_perfil or "").strip() or str(m_desc or "").strip() or "-"),
                }

            pv_seen = {}
            piezas_preview = {}
            for pos_base in piezas_all:
                key_b = str(pos_base or "").strip()
                if not key_b:
                    continue
                base_meta = meta_por_pieza.get(key_b, {})
                piezas_preview[key_b] = {
                    "pieza": key_b,
                    "cant": base_meta.get("cant") or "-",
                    "desc": base_meta.get("desc") or "-",
                    "superficie": {"estado": "-", "fecha": "-", "resp": "-"},
                    "fondo": {"estado": "-", "fecha": "-", "resp": "-", "esp": "-"},
                    "terminacion": {"estado": "-", "fecha": "-", "resp": "-", "esp": "-"},
                }
            for pos_v, proc_v, est_v, rep_v, fec_v, firma_v, cant_v, perfil_v, desc_v in pv_rows:
                if not pos_v:
                    continue
                rep_u_v = (rep_v or "").upper()
                if "ETAPA:SUPERFICIE" in rep_u_v:
                    etapa_v = "SUPERFICIE"
                elif "ETAPA:FONDO" in rep_u_v:
                    etapa_v = "FONDO"
                elif "ETAPA:TERMINACION" in rep_u_v:
                    etapa_v = "TERMINACION"
                elif proc_v == "PINTURA_FONDO":
                    etapa_v = "FONDO"
                else:
                    continue
                key_v = (pos_v, etapa_v)
                if key_v in pv_seen:
                    continue
                pv_seen[key_v] = True
                if pos_v not in piezas_preview:
                    try:
                        cant_num_v = float(str(cant_v or 0).replace(",", "."))
                        cant_d_v = str(int(round(cant_num_v))) if cant_num_v > 0 else "-"
                    except Exception:
                        cant_d_v = "-"
                    piezas_preview[pos_v] = {
                        "pieza": pos_v,
                        "cant": cant_d_v,
                        "desc": (str(perfil_v or "").strip() or str(desc_v or "").strip() or "-"),
                        "superficie": {"estado": "-", "fecha": "-", "resp": "-"},
                        "fondo": {"estado": "-", "fecha": "-", "resp": "-", "esp": "-"},
                        "terminacion": {"estado": "-", "fecha": "-", "resp": "-", "esp": "-"},
                    }
                final_v = ultimo_por_pieza_etapa.get(key_v, {})
                est_final_v = final_v.get("estado", est_v).strip().upper()
                if est_final_v in ("OK", "APROBADO", "CONFORME", "OBS", "OM"):
                    est_label_v = "OK"
                elif est_final_v in ("NC", "NO CONFORME", "NO CONFORMIDAD"):
                    est_label_v = "NO CONFORME"
                else:
                    est_label_v = est_final_v or "-"
                p_v = piezas_preview[pos_v]
                if etapa_v == "SUPERFICIE" and p_v["superficie"]["estado"] == "-":
                    p_v["superficie"] = {"estado": est_label_v, "fecha": fec_v or "-", "resp": firma_v or "-"}
                elif etapa_v == "FONDO" and p_v["fondo"]["estado"] == "-":
                    p_v["fondo"] = {"estado": est_label_v, "fecha": fec_v or "-", "resp": firma_v or "-", "esp": _esp_str_prev(rep_v, "Espesor fondo")}
                elif etapa_v == "TERMINACION" and p_v["terminacion"]["estado"] == "-":
                    p_v["terminacion"] = {"estado": est_label_v, "fecha": fec_v or "-", "resp": firma_v or "-", "esp": _esp_str_prev(rep_v, "Espesor terminacion")}
            for pos_v in piezas_preview:
                p_v = piezas_preview[pos_v]
                try:
                    fe = float(str(p_v["fondo"].get("esp") or "0").replace("-", "0") or "0")
                    te = float(str(p_v["terminacion"].get("esp") or "0").replace("-", "0") or "0")
                    esp_tot_v = fe + te
                    p_v["esp_total"] = str(int(round(esp_tot_v))) if esp_tot_v > 0 else "-"
                except Exception:
                    p_v["esp_total"] = "-"
                ests_v = [p_v["superficie"]["estado"], p_v["fondo"]["estado"], p_v["terminacion"]["estado"]]
                if any(e == "NO CONFORME" for e in ests_v if e != "-"):
                    p_v["estado_resumen"] = "NO CONFORME"
                elif any(e != "-" for e in ests_v):
                    p_v["estado_resumen"] = "OK"
                else:
                    p_v["estado_resumen"] = "-"
            piezas_preview_list = [piezas_preview[k] for k in sorted(piezas_preview.keys())]
            # --- fin preview enriquecida ---

            # Obtener estado de SOLDADURA para cada pieza
            soldadura_rows = db.execute(
                """
                SELECT DISTINCT TRIM(COALESCE(posicion, '')) AS posicion,
                       UPPER(TRIM(COALESCE(estado, ''))) AS estado,
                       COALESCE(re_inspeccion, '') AS re_inspeccion
                FROM procesos
                WHERE ot_id = ?
                  AND UPPER(TRIM(COALESCE(proceso, ''))) = 'SOLDADURA'
                  AND eliminado = 0
                ORDER BY id DESC
                """,
                (ot_id_sel,)
            ).fetchall()
            
            # Mapear último estado de soldadura por pieza
            ultimo_soldadura = {}
            for pos_s, est_s, re_insp_s in soldadura_rows:
                if pos_s not in ultimo_soldadura:
                    # Resolver estado considerando re-inspecciones
                    est_final_s = est_s
                    if est_s == "NC" and re_insp_s:
                        ciclos_s = _extraer_ciclos_reinspeccion(re_insp_s)
                        if ciclos_s:
                            ultimo_ciclo_s = ciclos_s[-1].get("estado", "").strip().upper()
                            if ultimo_ciclo_s in ("OK", "APROBADO", "CONFORME", "OBS", "OM"):
                                est_final_s = ultimo_ciclo_s
                    ultimo_soldadura[pos_s] = est_final_s

            # Filtrar piezas: solo las que estén OK en SOLDADURA
            piezas_filtradas = []
            for p in piezas_all:
                est_sold = (ultimo_soldadura.get(p) or "").upper()
                if est_sold in ("OK", "APROBADO", "CONFORME", "OBS", "OM"):
                    piezas_filtradas.append(p)
            
            if etapa_sel:
                aprobadas = []
                pendientes = []
                for p in piezas_filtradas:
                    info = ultimo_por_pieza_etapa.get((p, etapa_sel), {})
                    estado = (info.get("estado") or "").upper()
                    estado_pieza = (info.get("estado_pieza") or "").upper()
                    if estado in ("OK", "OBS", "OM") and estado_pieza != "RE-INSPECCION":
                        continue
                    if estado == "NC" or estado_pieza == "RE-INSPECCION":
                        pendientes.append(p)
                    else:
                        aprobadas.append(p)
                piezas_list = sorted(pendientes) + sorted(aprobadas)
            else:
                piezas_list = sorted(piezas_filtradas)
        
        opciones_ot = ""
        for ot_id, obra_ot, titulo_ot, esquema_ot, espesor_ot in ot_rows:
            ot_id_txt = str(ot_id)
            selected = "selected" if ot_id_txt == ot_id_sel_txt else ""
            obra_attr = html_lib.escape(str(obra_ot or "").strip())
            esquema_attr = html_lib.escape(str(esquema_ot or "").strip())
            espesor_attr = html_lib.escape(str(espesor_ot or "").strip())
            titulo_txt = html_lib.escape(str(titulo_ot or "").strip())
            label_ot = f"OT {ot_id_txt} - {obra_attr}"
            if titulo_txt:
                label_ot += f" - {titulo_txt}"
            opciones_ot += (
                f'<option value="{ot_id_txt}" data-obra="{obra_attr}" data-esquema="{esquema_attr}" '
                f'data-espesor="{espesor_attr}" {selected}>{label_ot}</option>'
            )
        opciones_responsables = "".join(f'<option value="{r}">{r}</option>' for r in responsables_list)
        opciones_operarios = "".join(f'<option value="{o}">{o}</option>' for o in operarios_list)
        
        # Paginación de piezas para checkboxes (10 por página)
        piezas_per_page = 10
        total_piezas_list = len(piezas_list)
        total_pages_piezas = max(1, (total_piezas_list + piezas_per_page - 1) // piezas_per_page)
        page_piezas = 1  # Página inicial
        
        # Dividir piezas en páginas
        piezas_pages = {}
        for i in range(0, total_piezas_list, piezas_per_page):
            page_num = (i // piezas_per_page) + 1
            piezas_pages[page_num] = piezas_list[i:i + piezas_per_page]
        
        # Generar checkboxes simples (primera página)
        piezas_checkboxes_superficie = ""
        piezas_checkboxes_fondo = ""
        piezas_checkboxes_terminacion = ""
        
        if piezas_pages.get(1):
            for p in piezas_pages[1]:
                checkbox_html = f'<label style="display: block; margin: 8px 0;"><input type="checkbox" name="piezas_superficie" value="{p}"> {p}</label>'
                piezas_checkboxes_superficie += checkbox_html
                piezas_checkboxes_fondo += f'<label style="display: block; margin: 8px 0;"><input type="checkbox" name="piezas_fondo" value="{p}"> {p}</label>'
                piezas_checkboxes_terminacion += f'<label style="display: block; margin: 8px 0;"><input type="checkbox" name="piezas_terminacion" value="{p}"> {p}</label>'
        
        # Generar HTML de paginación (igual para todas las etapas)
        pagination_html = ""
        if total_pages_piezas > 1:
            pagination_html = '<div style="display:flex;justify-content:center;gap:6px;flex-wrap:wrap;margin-top:12px;border-top:1px solid #ddd;padding-top:8px;font-size:12px;">'
            pagination_html += '<button type="button" id="btn-prev-piezas" style="padding:6px 10px;border:1px solid #ddd;border-radius:4px;background:#fff;color:#333;cursor:pointer;" onclick="cambiarPaginaPiezas(-1); return false;">← Anterior</button>'
            pagination_html += '<div style="display:flex;gap:4px;">'
            for page_num in range(1, total_pages_piezas + 1):
                pagination_html += f'<button type="button" onclick="irAPaginaPiezas({page_num}); return false;" style="padding:6px 10px;border:1px solid #ddd;border-radius:4px;background:#fff;color:#333;cursor:pointer;" class="btn-page-piezas" data-page="{page_num}">{page_num}</button>'
            pagination_html += '</div>'
            pagination_html += '<button type="button" id="btn-next-piezas" style="padding:6px 10px;border:1px solid #ddd;border-radius:4px;background:#fff;color:#333;cursor:pointer;" onclick="cambiarPaginaPiezas(1); return false;">Siguiente →</button>'
            pagination_html += '<span style="color:#666;margin-left:10px;">Página <span id="current-page-piezas">1</span> / ' + str(total_pages_piezas) + '</span>'
            pagination_html += '</div>'
        
        html_carga = f"""
        <html>
        <head>
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Control Pintura</title>
        <style>
        * {{ box-sizing: border-box; margin: 0; padding: 0; }}
        body {{ font-family: 'Segoe UI', Arial, sans-serif; background: linear-gradient(160deg, #fff7ed 0%, #ffedd5 45%, #ffe4cc 100%); padding: 20px; color: #7c2d12; }}
        .container {{ max-width: 920px; margin: 0 auto; background: #fffaf5; border-radius: 14px; box-shadow: 0 12px 28px rgba(194, 65, 12, 0.12); border: 1px solid #fed7aa; padding: 30px; }}
        h1 {{ color: #7c2d12; margin-bottom: 30px; border-bottom: 3px solid #f97316; padding-bottom: 15px; }}
        .form-group {{ margin-bottom: 20px; }}
        label {{ display: block; font-weight: bold; color: #7c2d12; margin-bottom: 6px; }}
        input[type="text"], input[type="date"], select {{ width: 100%; padding: 10px; border: 1px solid #fdba74; border-radius: 8px; font-size: 14px; background: #fff; }}
        input[readonly] {{ background: #fff7ed; color: #7c2d12; }}
        input[type="text"]:focus, input[type="date"]:focus, select:focus {{ outline: none; border-color: #f97316; box-shadow: 0 0 0 3px rgba(251, 146, 60, 0.2); }}
        .row {{ display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }}
        .row.full {{ grid-template-columns: 1fr; }}
        .etapa-container {{ display: none; background: #fff7ed; padding: 20px; border-radius: 10px; border-left: 4px solid #f97316; margin-top: 20px; border: 1px solid #fed7aa; }}
        .etapa-container.show {{ display: block; }}
        .piezas-grid {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; max-height: 300px; overflow-y: auto; border: 1px solid #fdba74; padding: 15px; border-radius: 8px; background: #fff; }}
        .estado-grupo {{ display: flex; gap: 15px; align-items: center; margin-top: 15px; }}
        .estado-grupo label {{ margin-bottom: 0; }}
        input[type="radio"] {{ margin-right: 5px; }}
        input[type="number"] {{ width: 100%; padding: 10px; border: 1px solid #fdba74; border-radius: 8px; font-size: 14px; }}
        .btn-group {{ display: flex; gap: 10px; margin-top: 30px; justify-content: flex-end; }}
        button {{ padding: 12px 24px; border: none; border-radius: 8px; font-size: 14px; font-weight: bold; cursor: pointer; transition: all 0.2s; }}
        button.btn-submit {{ background: #10b981; color: white; }}
        button.btn-submit:hover {{ background: #059669; }}
        button.btn-cancel {{ background: #6b7280; color: white; }}
        button.btn-cancel:hover {{ background: #4b5563; }}
        .kpi-row {{ display: grid; grid-template-columns: repeat(4, minmax(120px, 1fr)); gap: 10px; margin-bottom: 14px; }}
        .kpi-card {{ background: #fff; border: 1px solid #fdba74; border-radius: 10px; padding: 10px; box-shadow: 0 4px 10px rgba(194, 65, 12, 0.08); }}
        .kpi-card .t {{ font-size: 12px; color: #9a3412; margin-bottom: 4px; font-weight: 600; }}
        .kpi-card .v {{ font-size: 24px; color: #7c2d12; font-weight: 800; }}
        </style>
        <script>
        function mostrarEtapa() {{
            const etapa = document.getElementById('etapa').value;
            document.getElementById('etapa-superficie').classList.remove('show');
            document.getElementById('etapa-fondo').classList.remove('show');
            document.getElementById('etapa-terminacion').classList.remove('show');
            
            if (etapa === 'SUPERFICIE') document.getElementById('etapa-superficie').classList.add('show');
            else if (etapa === 'FONDO') document.getElementById('etapa-fondo').classList.add('show');
            else if (etapa === 'TERMINACION') document.getElementById('etapa-terminacion').classList.add('show');
            toggleMotivoNC();
        }}

        function toggleMotivoNC() {{
            const estado = (document.getElementById('estado_control')?.value || '').toUpperCase();
            const group = document.getElementById('motivo_nc_group');
            if (!group) return;
            group.style.display = estado === 'NC' ? 'block' : 'none';
        }}

        // Variables globales para paginación de piezas
        const piezasPageData = {json.dumps(piezas_pages, ensure_ascii=False)};
        const totalPagesPiezas = {total_pages_piezas};
        let currentPagePiezas = 1;

        function cambiarPaginaPiezas(dir) {{
            const newPage = currentPagePiezas + dir;
            if (newPage >= 1 && newPage <= totalPagesPiezas) {{
                irAPaginaPiezas(newPage);
            }}
        }}

        function irAPaginaPiezas(pageNum) {{
            currentPagePiezas = pageNum;
            const piezasData = piezasPageData[pageNum] || [];
            
            // Actualizar checkboxes para todas las etapas
            ['superficie', 'fondo', 'terminacion'].forEach(etapa => {{
                const container = document.getElementById('piezas-container-' + etapa);
                if (!container) return;

                // Generar HTML de checkboxes para la página
                let html = '';
                piezasData.forEach(p => {{
                    html += '<label style="display: block; margin: 8px 0;"><input type="checkbox" name="piezas_' + etapa + '" value="' + p + '"> ' + p + '</label>';
                }});

                if (!html) {{
                    html = '<p style="grid-column: 1/-1; text-align: center; color: #999;">No hay piezas en esta página</p>';
                }}

                container.innerHTML = html;
            }});

            // Actualizar estado de botones de paginación
            const prevBtn = document.getElementById('btn-prev-piezas');
            const nextBtn = document.getElementById('btn-next-piezas');
            const currentPageSpan = document.getElementById('current-page-piezas');
            
            if (prevBtn) prevBtn.disabled = pageNum === 1;
            if (nextBtn) nextBtn.disabled = pageNum === totalPagesPiezas;
            if (currentPageSpan) currentPageSpan.textContent = pageNum;

            // Resaltar botón de página actual
            document.querySelectorAll('.btn-page-piezas').forEach(btn => {{
                const btnPage = parseInt(btn.getAttribute('data-page') || '1', 10);
                if (btnPage === pageNum) {{
                    btn.style.background = '#f97316';
                    btn.style.color = '#fff';
                    btn.style.fontWeight = 'bold';
                }} else {{
                    btn.style.background = '#fff';
                    btn.style.color = '#333';
                    btn.style.fontWeight = 'normal';
                }}
            }});
        }}
        
        function syncOtMeta() {{
            const sel = document.getElementById('ot_id');
            const obraInput = document.getElementById('obra');
            const obraView = document.getElementById('obra_view');
            const esquema = document.getElementById('esquema_pintura');
            const espesor = document.getElementById('espesor_final');
            const opt = sel && sel.selectedIndex >= 0 ? sel.options[sel.selectedIndex] : null;

            const obra = opt ? (opt.getAttribute('data-obra') || '') : '';
            const esquemaTxt = opt ? (opt.getAttribute('data-esquema') || '') : '';
            const espesorTxt = opt ? (opt.getAttribute('data-espesor') || '') : '';

            if (obraInput) obraInput.value = obra;
            if (obraView) obraView.value = obra;
            if (esquema) esquema.value = esquemaTxt;
            if (espesor) espesor.value = espesorTxt;
            actualizarEstadoBotonPdf();
        }}

        function actualizarEstadoBotonPdf() {{
            const btn = document.getElementById('btn-pdf');
            const otId = document.getElementById('ot_id')?.value || '';
            if (!btn) return;
            btn.disabled = !otId;
            btn.style.opacity = otId ? '1' : '0.55';
            btn.style.cursor = otId ? 'pointer' : 'not-allowed';
            btn.title = otId ? '' : 'Selecciona una OT para habilitar el PDF';
        }}

        function cargarPiezas() {{
            const otId = document.getElementById('ot_id').value;
            const etapa = document.getElementById('etapa').value;
            if (!otId) return;
            let url = '?ot_id=' + encodeURIComponent(otId);
            if (etapa) url += '&etapa=' + encodeURIComponent(etapa);
            window.location.href = url;
        }}

        function onEtapaChange() {{
            mostrarEtapa();
            const otId = document.getElementById('ot_id').value;
            if (otId) cargarPiezas();
        }}
        
        document.addEventListener('DOMContentLoaded', function() {{
            syncOtMeta();
            mostrarEtapa();
            toggleMotivoNC();
            actualizarEstadoBotonPdf();
            // Inicializar paginación de piezas
            if (totalPagesPiezas > 0) {{
                irAPaginaPiezas(1);
            }}
        }});
        </script>
        </head>
        <body>
        <div class="container">
            <h1>🎨 CONTROL PINTURA</h1>
            <form method="post">
                <input type="hidden" name="accion" value="carga">
                
                <div class="row">
                    <div class="form-group">
                        <label for="ot_id">OT *</label>
                        <select id="ot_id" name="ot_id" required onchange="syncOtMeta(); cargarPiezas();">
                            <option value="">-- Seleccionar OT --</option>
                            {opciones_ot}
                        </select>
                    </div>
                    <div class="form-group">
                        <label for="fecha">Fecha *</label>
                        <input type="date" id="fecha" name="fecha" value="{date.today().isoformat()}" required>
                    </div>
                </div>

                <div class="row">
                    <div class="form-group">
                        <label for="obra_view">Obra *</label>
                        <input type="text" id="obra_view" value="{html_lib.escape(obra_sel)}" readonly>
                        <input type="hidden" id="obra" name="obra" value="{html_lib.escape(obra_sel)}">
                    </div>
                    <div class="form-group">
                        <label for="esquema_pintura">Esquema de pintura</label>
                        <input type="text" id="esquema_pintura" value="{html_lib.escape(esquema_sel)}" readonly>
                    </div>
                </div>

                <div class="row full">
                    <div class="form-group">
                        <label for="espesor_final">Espesor final requerido (μm)</label>
                        <input type="text" id="espesor_final" value="{html_lib.escape(espesor_sel)}" readonly>
                    </div>
                </div>
                
                <div class="row full">
                    <div class="form-group">
                        <label for="etapa">Etapa a Controlar *</label>
                        <select id="etapa" name="etapa" required onchange="onEtapaChange()">
                            <option value="">-- Seleccionar Etapa --</option>
                            <option value="SUPERFICIE" {"selected" if etapa_sel == "SUPERFICIE" else ""}>🔍 Superficie</option>
                            <option value="FONDO" {"selected" if etapa_sel == "FONDO" else ""}>🎨 Fondo / Imprimación</option>
                            <option value="TERMINACION" {"selected" if etapa_sel == "TERMINACION" else ""}>✅ Terminación</option>
                        </select>
                    </div>
                </div>

                <div class="row">
                    <div class="form-group">
                        <label for="responsable">Responsable Control *</label>
                        <select id="responsable" name="responsable" required>
                            <option value="">-- Seleccionar --</option>
                            {opciones_responsables}
                        </select>
                    </div>
                    <div class="form-group">
                        <label for="operario">Operario *</label>
                        <select id="operario" name="operario" required>
                            <option value="">-- Seleccionar --</option>
                            {opciones_operarios}
                        </select>
                    </div>
                </div>

                <div class="row">
                    <div class="form-group">
                        <label for="estado_control">Estado *</label>
                        <select id="estado_control" name="estado_control" onchange="toggleMotivoNC(); actualizarPreview();">
                            <option value="OK" selected>OK</option>
                            <option value="NC">NO CONFORME</option>
                            <option value="OBS">OBS</option>
                            <option value="OM">OP MEJORA</option>
                        </select>
                    </div>
                    <div class="form-group" id="motivo_nc_group" style="display:none;">
                        <label for="motivo_nc">Motivo NC *</label>
                        <input type="text" id="motivo_nc" name="motivo_nc" placeholder="Detalle de no conformidad">
                    </div>
                </div>
                
                <div id="etapa-superficie" class="etapa-container">
                    <h3>A) Si elige SUPERFICIE</h3>
                    <div class="piezas-grid">
                        <div id="piezas-container-superficie">
                            {piezas_checkboxes_superficie if piezas_list else '<p style="text-align: center; color: #999;">No hay piezas disponibles (debe estar OK en Soldadura)</p>'}
                        </div>
                    </div>
                    {pagination_html}
                    <div style="margin-top: 10px; color: #666; font-size: 13px;">El estado general (OK/NC/OBS/OM) se define arriba y aplica a esta carga.</div>
                </div>
                
                <div id="etapa-fondo" class="etapa-container">
                    <h3>B) Si elige FONDO / IMPRIMACIÓN</h3>
                    <div class="piezas-grid">
                        <div id="piezas-container-fondo">
                            {piezas_checkboxes_fondo if piezas_list else '<p style="text-align: center; color: #999;">No hay piezas disponibles (debe estar OK en Soldadura)</p>'}
                        </div>
                    </div>
                    {pagination_html}
                    <div class="form-group">
                        <label for="fondo_espesor">Espesor Promedio (μm)</label>
                        <input type="number" id="fondo_espesor" name="fondo_espesor" step="0.1" placeholder="Ej: 120.5">
                    </div>
                </div>
                
                <div id="etapa-terminacion" class="etapa-container">
                    <h3>C) Si elige TERMINACIÓN</h3>
                    <div class="piezas-grid">
                        <div id="piezas-container-terminacion">
                            {piezas_checkboxes_terminacion if piezas_list else '<p style="text-align: center; color: #999;">No hay piezas disponibles (debe estar OK en Soldadura)</p>'}
                        </div>
                    </div>
                    {pagination_html}
                    <div class="form-group">
                        <label for="term_espesor">Espesor Final (μm)</label>
                        <input type="number" id="term_espesor" name="term_espesor" step="0.1" placeholder="Ej: 300.5">
                    </div>
                </div>
                
                <!-- PREVISUALIZACIÓN DEL PDF -->
                <div style="margin-top: 40px; padding-top: 30px; border-top: 3px solid #f97316;">
                    <h2 style="color: #333; margin-bottom: 15px;">📄 Previsualización del Registro</h2>
                    <div class="kpi-row">
                        <div class="kpi-card"><div class="t">Total piezas OT</div><div class="v" id="kpi-total-piezas">0</div></div>
                        <div class="kpi-card"><div class="t">Controladas superficie</div><div class="v" id="kpi-superficie">0</div></div>
                        <div class="kpi-card"><div class="t">Controladas fondo</div><div class="v" id="kpi-fondo">0</div></div>
                        <div class="kpi-card"><div class="t">Controladas terminación</div><div class="v" id="kpi-terminacion">0</div></div>
                    </div>
                    <div id="preview-table" style="background: #fff; border: 1px solid #ddd; border-radius: 8px; padding: 15px; overflow-x: auto;">
                        <table style="width: 100%; border-collapse: collapse; font-size: 12px; min-width: 700px;">
                            <thead>
                                <tr style="background: #f97316; color: white;">
                                    <th rowspan="2" style="padding: 6px 8px; text-align: left; border: 1px solid #ddd;">Posición</th>
                                    <th colspan="2" style="padding: 6px 8px; text-align: center; border: 1px solid #ddd;">Control Superficie</th>
                                    <th colspan="4" style="padding: 6px 8px; text-align: center; border: 1px solid #ddd;">Fondo de Imprimación</th>
                                    <th colspan="4" style="padding: 6px 8px; text-align: center; border: 1px solid #ddd;">Terminación</th>
                                    <th rowspan="2" style="padding: 6px 8px; text-align: center; border: 1px solid #ddd;">Esp. Total</th>
                                    <th rowspan="2" style="padding: 6px 8px; text-align: center; border: 1px solid #ddd;">Estado</th>
                                </tr>
                                <tr style="background: #fb923c; color: white;">
                                    <th style="padding: 5px; border: 1px solid #ddd; font-size: 11px;">Estado</th>
                                    <th style="padding: 5px; border: 1px solid #ddd; font-size: 11px;">Resp.</th>
                                    <th style="padding: 5px; border: 1px solid #ddd; font-size: 11px;">Esp. (μm)</th>
                                    <th style="padding: 5px; border: 1px solid #ddd; font-size: 11px;">Estado</th>
                                    <th style="padding: 5px; border: 1px solid #ddd; font-size: 11px;">Fecha</th>
                                    <th style="padding: 5px; border: 1px solid #ddd; font-size: 11px;">Resp.</th>
                                    <th style="padding: 5px; border: 1px solid #ddd; font-size: 11px;">Esp. (μm)</th>
                                    <th style="padding: 5px; border: 1px solid #ddd; font-size: 11px;">Estado</th>
                                    <th style="padding: 5px; border: 1px solid #ddd; font-size: 11px;">Fecha</th>
                                    <th style="padding: 5px; border: 1px solid #ddd; font-size: 11px;">Resp.</th>
                                </tr>
                            </thead>
                            <tbody id="preview-body">
                                <tr><td colspan="13" style="text-align: center; padding: 20px; color: #999;">Selecciona OT para ver histórico acumulado</td></tr>
                            </tbody>
                        </table>
                        <div id="preview-pagination" style="display:flex;justify-content:center;gap:6px;flex-wrap:wrap;margin-top:12px;"></div>
                    </div>

                    <!-- SECCIÓN RE-INSPECCIONES -->
                    <div style="margin-top: 24px;">
                        <h3 style="color: #9a3412; margin-bottom: 8px;">🔄 Re-inspecciones (piezas NC)</h3>
                        <div id="reinspeccion-section">
                            <p style="color: #666; font-style: italic; padding: 10px;">Sin re-inspecciones registradas.</p>
                        </div>
                    </div>
                </div>
                
                <div class="btn-group" style="flex-wrap: wrap; justify-content: space-between; margin-top: 30px;">
                    <div style="display: flex; gap: 10px; flex-wrap: wrap;">
                        <button type="button" class="btn-cancel" onclick="window.history.back()">Cancelar</button>
                        <a href="/modulo/calidad/escaneo" style="display: inline-block; padding: 12px 24px; background: #6b7280; color: white; border: none; border-radius: 5px; font-weight: bold; text-decoration: none; cursor: pointer;">⬅️ Volver a Submódulos</a>
                    </div>
                    <div style="display: flex; gap: 10px;">
                        <button type="button" class="btn-cancel" id="btn-pdf" onclick="generarPDF()" style="background: #2563eb;">📄 Generar PDF</button>
                        <button type="submit" class="btn-submit">💾 Guardar Carga</button>
                    </div>
                </div>
            </form>
        </div>
        
        <script>
        const historialPorPiezaBase = {json.dumps(historial_por_pieza, ensure_ascii=False)};
        const piezasPreviewBase = {json.dumps(piezas_preview_list, ensure_ascii=False)};
        const reinspeccionesBase = {json.dumps(nc_records_preview, ensure_ascii=False)};

        function actualizarIndicadores(piezasRender) {{
            const total = (piezasRender || []).length;
            let sup = 0;
            let fon = 0;
            let ter = 0;
            (piezasRender || []).forEach(p => {{
                const s = ((p.superficie || {{}}).estado || '-').toUpperCase();
                const f = ((p.fondo || {{}}).estado || '-').toUpperCase();
                const t = ((p.terminacion || {{}}).estado || '-').toUpperCase();
                if (s && s !== '-') sup += 1;
                if (f && f !== '-') fon += 1;
                if (t && t !== '-') ter += 1;
            }});
            const setTxt = (id, val) => {{
                const el = document.getElementById(id);
                if (el) el.textContent = String(val);
            }};
            setTxt('kpi-total-piezas', total);
            setTxt('kpi-superficie', sup);
            setTxt('kpi-fondo', fon);
            setTxt('kpi-terminacion', ter);
        }}

        let previewPage = 1;
        const PREVIEW_PAGE_SIZE = 20;

        function _renderPreviewPagination(totalItems, totalPages) {{
            const pag = document.getElementById('preview-pagination');
            if (!pag) return;
            if (!totalItems || totalPages <= 1) {{
                pag.innerHTML = '';
                return;
            }}

            const btnStyle = 'padding:6px 10px;border:1px solid #ddd;border-radius:4px;background:#fff;color:#333;cursor:pointer;font-size:12px;';
            const btnActStyle = 'padding:6px 10px;border:1px solid #f97316;border-radius:4px;background:#f97316;color:#fff;font-weight:bold;font-size:12px;';

            let html = '';
            html += previewPage > 1
                ? `<button type="button" data-preview-page="${{previewPage - 1}}" style="${{btnStyle}}">&#8249; Ant.</button>`
                : `<span style="${{btnStyle}}opacity:0.5;cursor:not-allowed;">&#8249; Ant.</span>`;

            const ini = Math.max(1, previewPage - 2);
            const fin = Math.min(totalPages, previewPage + 2);
            for (let p = ini; p <= fin; p++) {{
                if (p === previewPage) html += `<span style="${{btnActStyle}}">${{p}}</span>`;
                else html += `<button type="button" data-preview-page="${{p}}" style="${{btnStyle}}">${{p}}</button>`;
            }}

            html += previewPage < totalPages
                ? `<button type="button" data-preview-page="${{previewPage + 1}}" style="${{btnStyle}}">Sig. &#8250;</button>`
                : `<span style="${{btnStyle}}opacity:0.5;cursor:not-allowed;">Sig. &#8250;</span>`;

            html += `<span style="padding:6px 10px;color:#6b7280;font-size:12px;">Página ${{previewPage}} / ${{totalPages}}</span>`;
            pag.innerHTML = html;

            pag.querySelectorAll('[data-preview-page]').forEach(btn => {{
                btn.addEventListener('click', () => {{
                    const p = parseInt(btn.getAttribute('data-preview-page') || '1', 10);
                    if (!Number.isNaN(p)) {{
                        previewPage = p;
                        actualizarPreview();
                    }}
                }});
            }});
        }}

        function actualizarPreview() {{
            const etapa = document.getElementById('etapa').value;
            const estado = (document.getElementById('estado_control')?.value || '').toUpperCase();
            const motivoNc = (document.getElementById('motivo_nc')?.value || '').trim();
            const fondoEsp = (document.getElementById('fondo_espesor')?.value || '').trim();
            const termEsp = (document.getElementById('term_espesor')?.value || '').trim();
            const tbody = document.getElementById('preview-body');
            const fecha = document.getElementById('fecha')?.value || '-';
            const responsable = document.getElementById('responsable')?.value || '-';

            // Deep clone piezasPreviewBase y construir mapa
            const piezasMap = {{}};
            (piezasPreviewBase || []).forEach(p => {{
                piezasMap[p.pieza] = JSON.parse(JSON.stringify(p));
            }});

            const estadoLabel = () => {{
                if (estado === 'NC') return 'NO CONFORME';
                if (estado === 'OBS') return 'OBS';
                if (estado === 'OM') return 'OM';
                return 'OK';
            }};

            const ensurePieza = (p) => {{
                if (!piezasMap[p]) {{
                    piezasMap[p] = {{ pieza: p,
                        superficie: {{estado:'-',fecha:'-',resp:'-'}},
                        fondo: {{estado:'-',fecha:'-',resp:'-',esp:'-'}},
                        terminacion: {{estado:'-',fecha:'-',resp:'-',esp:'-'}},
                        esp_total: '-', estado_resumen: '-' }};
                }}
            }};

            if (etapa === 'SUPERFICIE') {{
                Array.from(document.querySelectorAll('input[name="piezas_superficie"]:checked')).forEach(el => {{
                    ensurePieza(el.value);
                    piezasMap[el.value].superficie = {{ estado: estadoLabel(), fecha: fecha, resp: responsable }};
                }});
            }} else if (etapa === 'FONDO') {{
                Array.from(document.querySelectorAll('input[name="piezas_fondo"]:checked')).forEach(el => {{
                    ensurePieza(el.value);
                    piezasMap[el.value].fondo = {{ estado: estadoLabel(), fecha: fecha, resp: responsable, esp: fondoEsp || '-' }};
                }});
            }} else if (etapa === 'TERMINACION') {{
                Array.from(document.querySelectorAll('input[name="piezas_terminacion"]:checked')).forEach(el => {{
                    ensurePieza(el.value);
                    piezasMap[el.value].terminacion = {{ estado: estadoLabel(), fecha: fecha, resp: responsable, esp: termEsp || '-' }};
                }});
            }}

            const piezasRender = Object.values(piezasMap).sort((a, b) => a.pieza.localeCompare(b.pieza, undefined, {{numeric: true}}));
            actualizarIndicadores(piezasRender);

            if (piezasRender.length === 0) {{
                tbody.innerHTML = '<tr><td colspan="13" style="text-align:center;padding:20px;color:#999;">Selecciona OT para ver histórico acumulado</td></tr>';
                _renderPreviewPagination(0, 1);
                return;
            }}

            const totalItems = piezasRender.length;
            const totalPages = Math.max(1, Math.ceil(totalItems / PREVIEW_PAGE_SIZE));
            if (previewPage > totalPages) previewPage = totalPages;
            if (previewPage < 1) previewPage = 1;

            const startIdx = (previewPage - 1) * PREVIEW_PAGE_SIZE;
            const endIdx = Math.min(startIdx + PREVIEW_PAGE_SIZE, totalItems);
            const piezasPagina = piezasRender.slice(startIdx, endIdx);

            const tdS = 'padding:5px 6px;border:1px solid #ddd;text-align:center;font-size:11px;vertical-align:top;';
            tbody.innerHTML = piezasPagina.map((p, i) => {{
                const bg = i % 2 === 0 ? 'white' : '#fff7ed';
                const sup = p.superficie || {{}};
                const fon = p.fondo || {{}};
                const ter = p.terminacion || {{}};
                const fe = parseFloat(String(fon.esp || '0').replace('-','0')) || 0;
                const te = parseFloat(String(ter.esp || '0').replace('-','0')) || 0;
                const espTot = (fe + te) > 0 ? `${{Math.round(fe + te)}} μm` : '-';
                const ests = [sup.estado, fon.estado, ter.estado].filter(e => e && e !== '-');
                let estadoRes = p.estado_resumen || '-';
                if (ests.some(e => e === 'NO CONFORME')) estadoRes = 'NO CONFORME';
                else if (ests.length > 0) estadoRes = 'OK';
                const ec = estadoRes === 'OK' ? '#15803d' : estadoRes === 'NO CONFORME' ? '#b91c1c' : '#374151';
                const cc = (e) => e === 'OK' ? '#15803d' : e === 'NO CONFORME' ? '#b91c1c' : '#374151';
                return `<tr style="background:${{bg}};">
                    <td style="${{tdS}}text-align:left;font-weight:700;">${{p.pieza}}</td>
                    <td style="${{tdS}}color:${{cc(sup.estado)}};font-weight:bold;">${{sup.estado || '-'}}</td>
                    <td style="${{tdS}}">${{sup.resp || '-'}}</td>
                    <td style="${{tdS}}">${{fon.esp && fon.esp !== '-' ? fon.esp + ' μm' : '-'}}</td>
                    <td style="${{tdS}}color:${{cc(fon.estado)}};font-weight:bold;">${{fon.estado || '-'}}</td>
                    <td style="${{tdS}}">${{fon.fecha || '-'}}</td>
                    <td style="${{tdS}}">${{fon.resp || '-'}}</td>
                    <td style="${{tdS}}">${{ter.esp && ter.esp !== '-' ? ter.esp + ' μm' : '-'}}</td>
                    <td style="${{tdS}}color:${{cc(ter.estado)}};font-weight:bold;">${{ter.estado || '-'}}</td>
                    <td style="${{tdS}}">${{ter.fecha || '-'}}</td>
                    <td style="${{tdS}}">${{ter.resp || '-'}}</td>
                    <td style="${{tdS}}">${{espTot}}</td>
                    <td style="${{tdS}}font-weight:bold;color:${{ec}};">${{estadoRes}}</td>
                </tr>`;
            }}).join('');

            _renderPreviewPagination(totalItems, totalPages);
        }}

        function renderReinspeccion() {{
            const ncs = reinspeccionesBase || [];
            const container = document.getElementById('reinspeccion-section');
            if (!container) return;
            if (ncs.length === 0) {{
                container.innerHTML = '<p style="color:#666;font-style:italic;padding:10px;">Sin re-inspecciones registradas.</p>';
                return;
            }}
            const rows = ncs.map((nc, i) => `
                <tr style="background:${{i % 2 === 0 ? 'white' : '#fff7ed'}};">
                    <td style="padding:6px 8px;border:1px solid #ddd;font-weight:bold;">${{nc.posicion}}</td>
                    <td style="padding:6px 8px;border:1px solid #ddd;">${{nc.etapa}}</td>
                    <td style="padding:6px 8px;border:1px solid #ddd;">${{nc.motivo}}</td>
                    <td style="padding:6px 8px;border:1px solid #ddd;">${{nc.fecha}}</td>
                    <td style="padding:6px 8px;border:1px solid #ddd;font-weight:bold;color:${{nc.estado_final === 'OK' ? '#15803d' : '#b91c1c'}}">${{nc.estado_final}}</td>
                </tr>`).join('');
            container.innerHTML = `<table style="width:100%;border-collapse:collapse;font-size:13px;margin-top:8px;">
                <thead><tr style="background:#c2410c;color:white;">
                    <th style="padding:8px;border:1px solid #ddd;text-align:left;">Posición</th>
                    <th style="padding:8px;border:1px solid #ddd;text-align:center;">Proceso</th>
                    <th style="padding:8px;border:1px solid #ddd;text-align:center;">Motivo NC</th>
                    <th style="padding:8px;border:1px solid #ddd;text-align:center;">Fecha</th>
                    <th style="padding:8px;border:1px solid #ddd;text-align:center;">Estado Final</th>
                </tr></thead>
                <tbody>${{rows}}</tbody>
            </table>`;
        }}

        function generarPDF() {{
            const otId = document.getElementById('ot_id')?.value || '';
            const obra = document.getElementById('obra').value;
            if (!otId || !obra) {{
                alert('Selecciona una OT primero');
                return;
            }}
            window.open(`/modulo/calidad/escaneo/generar-pdf-pintura?ot_id=${{encodeURIComponent(otId)}}&obra=${{encodeURIComponent(obra)}}`, '_blank');
        }}
        
        document.querySelectorAll('input[type="checkbox"], input[type="radio"], input[type="number"], input[type="text"], select').forEach(el => {{
            el.addEventListener('change', actualizarPreview);
            el.addEventListener('input', actualizarPreview);
        }});
        document.getElementById('estado_control')?.addEventListener('change', toggleMotivoNC);
        actualizarPreview();
        renderReinspeccion();
        </script>
        </body>
        </html>
        """
        return html_carga
    
    # Procesar POST del MODO CARGA
    if request.method == "POST":
        accion = (request.form.get("accion") or "").strip()
        if accion == "carga":
            db = get_db()
            ot_id_txt = (request.form.get("ot_id") or "").strip()
            obra = (request.form.get("obra") or "").strip()
            fecha = (request.form.get("fecha") or "").strip()
            responsable = (request.form.get("responsable") or "").strip()
            operario = (request.form.get("operario") or "").strip()
            etapa = (request.form.get("etapa") or "").strip()

            ot_id = None
            if ot_id_txt.isdigit():
                row_ot = db.execute(
                    """
                    SELECT id, TRIM(COALESCE(obra, ''))
                    FROM ordenes_trabajo
                    WHERE id = ?
                      AND fecha_cierre IS NULL
                      AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
                    LIMIT 1
                    """,
                    (int(ot_id_txt),),
                ).fetchone()
                if row_ot:
                    ot_id = int(row_ot[0])
                    obra = str(row_ot[1] or "").strip()

            if not obra:
                return "Debes seleccionar una OT válida", 400

            estado_sel = (request.form.get("estado_control") or "OK").strip().upper()
            motivo_nc = (request.form.get("motivo_nc") or "").strip()

            if estado_sel not in ("OK", "NC", "OBS", "OM"):
                estado_sel = "OK"

            if estado_sel == "NC" and not motivo_nc:
                return "Debes indicar el motivo cuando el estado es NO CONFORME", 400

            estado_pieza = "RE-INSPECCION" if estado_sel == "NC" else "APROBADA"
            re_inspeccion_txt = ""
            if estado_sel == "NC":
                re_inspeccion_txt = _agregar_ciclo_reinspeccion(
                    "",
                    "PINTURA",
                    fecha,
                    operario,
                    "NC",
                    motivo_nc,
                    responsable,
                    responsable,
                )
            
            if etapa == "SUPERFICIE":
                piezas = request.form.getlist("piezas_superficie")
                if not piezas:
                    return "Debes seleccionar al menos una pieza", 400
                for p in piezas:
                    detalle_sup = "ETAPA:SUPERFICIE"
                    if estado_sel == "NC" and motivo_nc:
                        detalle_sup += f" | Motivo NC: {motivo_nc}"
                    db.execute(
                        "INSERT INTO procesos (posicion, obra, ot_id, proceso, fecha, operario, estado, reproceso, re_inspeccion, firma_digital, estado_pieza, eliminado) VALUES (?,?,?,?,?,?,?,?,?,?,?,0)",
                        (p, obra, ot_id, "PINTURA", fecha, operario, estado_sel, detalle_sup, re_inspeccion_txt, responsable, estado_pieza),
                    )
                db.commit()
            elif etapa == "FONDO":
                piezas = request.form.getlist("piezas_fondo")
                espesor = (request.form.get("fondo_espesor") or "").strip()
                if not piezas:
                    return "Debes seleccionar al menos una pieza", 400
                for p in piezas:
                    detalle_fondo = "ETAPA:FONDO"
                    if espesor:
                        detalle_fondo += f" | Espesor fondo: {espesor}μm"
                    if estado_sel == "NC" and motivo_nc:
                        detalle_fondo = (detalle_fondo + " | " if detalle_fondo else "") + f"Motivo NC: {motivo_nc}"
                    db.execute(
                        "INSERT INTO procesos (posicion, obra, ot_id, proceso, fecha, operario, estado, reproceso, re_inspeccion, firma_digital, estado_pieza, eliminado) VALUES (?,?,?,?,?,?,?,?,?,?,?,0)",
                        (p, obra, ot_id, "PINTURA_FONDO", fecha, operario, estado_sel, detalle_fondo, re_inspeccion_txt, responsable, estado_pieza),
                    )
                db.commit()
            elif etapa == "TERMINACION":
                piezas = request.form.getlist("piezas_terminacion")
                espesor = (request.form.get("term_espesor") or "").strip()
                if not piezas:
                    return "Debes seleccionar al menos una pieza", 400
                for p in piezas:
                    detalle_term = "ETAPA:TERMINACION"
                    if espesor:
                        detalle_term += f" | Espesor terminacion: {espesor}μm"
                    if estado_sel == "NC" and motivo_nc:
                        detalle_term = (detalle_term + " | " if detalle_term else "") + f"Motivo NC: {motivo_nc}"
                    db.execute(
                        "INSERT INTO procesos (posicion, obra, ot_id, proceso, fecha, operario, estado, reproceso, re_inspeccion, firma_digital, estado_pieza, eliminado) VALUES (?,?,?,?,?,?,?,?,?,?,?,0)",
                        (p, obra, ot_id, "PINTURA", fecha, operario, estado_sel, detalle_term, re_inspeccion_txt, responsable, estado_pieza),
                    )
                db.commit()

            if ot_id is not None:
                return redirect(f"/modulo/calidad/escaneo/control-pintura?ot_id={ot_id}&etapa={quote(etapa)}")
            return redirect(f"/modulo/calidad/escaneo/control-pintura?obra={quote(obra)}&etapa={quote(etapa)}")

    db = get_db()
    responsables_control = _obtener_responsables_control(db)
    responsables_list = sorted(responsables_control.keys())
    firmas_responsables = {k: (v.get("firma") or "") for k, v in responsables_control.items()}
    operarios_disponibles = _obtener_operarios_disponibles(db)
    operarios_list = sorted(set(operarios_disponibles))

    def _to_float(value):
        txt = str(value or "").strip().replace(",", ".")
        if not txt:
            return 0.0
        try:
            return float(txt)
        except Exception:
            return 0.0

    def _obtener_obras():
        rows = db.execute(
            """
            SELECT DISTINCT TRIM(obra) AS obra
            FROM procesos
            WHERE obra IS NOT NULL AND TRIM(obra) <> ''
              AND EXISTS (
                    SELECT 1 FROM ordenes_trabajo ot
                    WHERE TRIM(COALESCE(ot.obra, '')) = TRIM(COALESCE(procesos.obra, ''))
                      AND ot.fecha_cierre IS NULL
                      AND (ot.es_mantenimiento IS NULL OR ot.es_mantenimiento = 0)
              )
            ORDER BY obra ASC
            """
        ).fetchall()
        return [r[0] for r in rows]

    def _obtener_datos_obra(obra_sel):
        if not obra_sel:
            return "", ""
        row = db.execute(
            """
            SELECT esquema_pintura, espesor_total_requerido
            FROM ordenes_trabajo
            WHERE TRIM(COALESCE(obra,'')) = ?
              AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
            LIMIT 1
            """,
            (obra_sel,),
        ).fetchone()
        if not row:
            return "", ""
        return (row[0] or "", str(row[1] or ""))

    def _obtener_piezas_obra(obra_sel):
        if not obra_sel:
            return []

        _completar_metadatos_por_obra_pos(db, obra_sel)
        rows = db.execute(
            """
            SELECT id, posicion, cantidad, perfil
            FROM procesos p
            WHERE TRIM(COALESCE(p.obra, '')) = TRIM(?)
                AND TRIM(COALESCE(p.posicion, '')) <> ''
                AND p.eliminado = 0
                AND EXISTS (
                    SELECT 1
                    FROM procesos ps
                    WHERE TRIM(COALESCE(ps.obra, '')) = TRIM(COALESCE(p.obra, ''))
                        AND TRIM(COALESCE(ps.posicion, '')) = TRIM(COALESCE(p.posicion, ''))
                        AND UPPER(TRIM(COALESCE(ps.proceso, ''))) LIKE '%SOLDAD%'
                        AND ps.eliminado = 0
                )
            ORDER BY id DESC
            """,
            (obra_sel,),
        ).fetchall()

        piezas_map = {}
        for _, pos, cantidad, perfil in rows:
            clave = str(pos or "").strip()
            if not clave or clave in piezas_map:
                continue

            sold_row = db.execute(
                """
                SELECT COALESCE(estado, ''), COALESCE(re_inspeccion, '')
                FROM procesos
                WHERE TRIM(COALESCE(obra, '')) = TRIM(?)
                    AND TRIM(COALESCE(posicion, '')) = TRIM(?)
                    AND UPPER(TRIM(COALESCE(proceso, ''))) LIKE '%SOLDAD%'
                    AND eliminado = 0
                ORDER BY id DESC
                LIMIT 1
                """,
                (obra_sel, clave),
            ).fetchone()
            if not sold_row:
                continue

            estado_sold, reinsp_sold = sold_row
            estado_pieza_sold = _estado_pieza_persistente(estado_sold, reinsp_sold)
            if estado_pieza_sold != "APROBADA":
                continue

            piezas_map[clave] = {
                "pieza": clave,
                "cantidad": _format_cantidad_1_decimal(cantidad),
                "descripcion": str(perfil or "").strip(),
            }

        return sorted(piezas_map.values(), key=lambda x: x["pieza"])

    def _cargar_control_obra(obra_sel):
        if not obra_sel:
            return None, {}

        row = db.execute(
            """
            SELECT id, mediciones
            FROM control_pintura
            WHERE TRIM(COALESCE(obra,'')) = TRIM(?)
              AND estado IN ('en_progreso', 'completado')
            ORDER BY id DESC
            LIMIT 1
            """,
            (obra_sel,),
        ).fetchone()

        if not row:
            return None, {}

        control_id, med_json = row
        try:
            data = json.loads(med_json) if med_json else {}
        except Exception:
            data = {}

        if not isinstance(data, dict):
            data = {}

        if "historial" not in data or not isinstance(data.get("historial"), list):
            data["historial"] = []
        if "estado_actual" not in data or not isinstance(data.get("estado_actual"), dict):
            data["estado_actual"] = {}

        return control_id, data

    def _snapshot_piezas(estado_actual, piezas_catalogo=None):
        meta = {}
        for p in (piezas_catalogo or []):
            key = str((p or {}).get("pieza") or "").strip()
            if key:
                meta[key] = p or {}

        rows = []
        for pieza in sorted((estado_actual or {}).keys()):
            st = estado_actual.get(pieza) or {}
            superficie = st.get("superficie") or {}
            fondo = st.get("fondo") or {}
            terminacion = st.get("terminacion") or {}
            info = meta.get(pieza) or {}
            rows.append(
                {
                    "pieza": pieza,
                    "cantidad": info.get("cantidad") or "-",
                    "descripcion": info.get("descripcion") or "-",
                    "sup_estado": superficie.get("estado") or "-",
                    "sup_fecha": superficie.get("fecha") or "-",
                    "sup_resp": superficie.get("responsable") or "-",
                    "fondo_espesor": fondo.get("espesor") or "-",
                    "fondo_fecha": fondo.get("fecha") or "-",
                    "fondo_resp": fondo.get("responsable") or "-",
                    "term_espesor": terminacion.get("espesor") or "-",
                    "term_fecha": terminacion.get("fecha") or "-",
                    "term_resp": terminacion.get("responsable") or "-",
                    "esp_total": terminacion.get("espesor_total") or "-",
                    "esp_req": terminacion.get("espesor_requerido") or "-",
                    "estado_pintura": st.get("estado_pintura") or "-",
                }
            )
        return rows

    def _upsert_estado_pintura_en_procesos(pieza, obra_sel, fecha_control, responsable, operario, aprobado):
        estado_control = "OK" if aprobado else "NC"
        estado_pieza = "APROBADA" if aprobado else "NO_APROBADA"
        firma = (responsables_control.get(responsable) or {}).get("firma", "")
        operario_txt = str(operario or "").strip() or str(responsable or "").strip()
        ot_id = _obtener_ot_id_pieza(db, pieza, obra_sel)
        if ot_id is None:
            row_ot = db.execute(
                """
                SELECT id
                FROM ordenes_trabajo
                WHERE TRIM(COALESCE(obra,'')) = TRIM(?)
                  AND fecha_cierre IS NULL
                  AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
                ORDER BY id DESC
                LIMIT 1
                """,
                (obra_sel or "",),
            ).fetchone()
            if row_ot:
                ot_id = row_ot[0]

        row = db.execute(
            """
            SELECT id
            FROM procesos
            WHERE posicion=?
              AND COALESCE(obra,'') = COALESCE(?, '')
              AND UPPER(TRIM(COALESCE(proceso,''))) = 'PINTURA'
            ORDER BY id DESC
            LIMIT 1
            """,
            (pieza, obra_sel),
        ).fetchone()

        if row:
            proc_id = row[0]
            db.execute(
                """
                UPDATE procesos
                SET fecha=?, operario=?, estado=?, firma_digital=?, estado_pieza=?,
                    reproceso=COALESCE(reproceso,''), re_inspeccion=COALESCE(re_inspeccion,''),
                    ot_id=COALESCE(?, ot_id)
                WHERE id=?
                """,
                (fecha_control, operario_txt, estado_control, firma, estado_pieza, ot_id, proc_id),
            )
            _registrar_trazabilidad(
                db,
                proc_id,
                pieza,
                obra_sel,
                "PINTURA",
                estado_control,
                estado_pieza,
                firma,
                "AUTO_PINTURA",
                "",
                "ACTUALIZACION_CONTROL",
            )
            return

        cursor = db.execute(
            """
            INSERT INTO procesos (posicion, obra, ot_id, proceso, fecha, operario, estado, reproceso, re_inspeccion, firma_digital, estado_pieza, escaneado_qr)
            VALUES (?, ?, ?, 'PINTURA', ?, ?, ?, '', '', ?, ?, 1)
            """,
            (pieza, obra_sel or None, ot_id, fecha_control, operario_txt, estado_control, firma, estado_pieza),
        )
        _registrar_trazabilidad(
            db,
            cursor.lastrowid,
            pieza,
            obra_sel,
            "PINTURA",
            estado_control,
            estado_pieza,
            firma,
            "AUTO_PINTURA",
            "",
            "ALTA_CONTROL",
        )

    def _sincronizar_pintura_desde_estado_actual(obra_sel, estado_dict, operario_default=""):
        cambios = 0
        for pieza, st in (estado_dict or {}).items():
            if not isinstance(st, dict):
                continue
            term = st.get("terminacion") or {}
            if not term:
                continue

            responsable = str(term.get("responsable") or "").strip()
            if responsable not in responsables_control:
                continue

            fecha_control = str(term.get("fecha") or "").strip()
            if not fecha_control:
                continue

            operario = str(term.get("operario") or operario_default or responsable).strip()

            aprobado = bool(st.get("pintura_aprobada")) or ("APROBADA" in str(st.get("estado_pintura") or "").upper())
            _upsert_estado_pintura_en_procesos(str(pieza or "").strip(), obra_sel, fecha_control, responsable, operario, aprobado)
            cambios += 1

        if cambios:
            db.commit()

    def _registrar_reinspecciones_en_procesos(obra_sel, operario_sel, filas_reinspeccion):
        for ri in (filas_reinspeccion or []):
            pieza = str((ri or {}).get("posicion") or "").strip()
            if not pieza:
                continue

            proceso_ri = str((ri or {}).get("proceso") or "").strip()
            motivo_ri = str((ri or {}).get("motivo") or "").strip().upper()
            fecha_ri = str((ri or {}).get("fecha") or "").strip()
            accion_ri = str((ri or {}).get("accion_correctiva") or "").strip()
            responsable_ri = str((ri or {}).get("responsable") or "").strip()
            firma_ri = str((ri or {}).get("firma") or "").strip() or firmas_responsables.get(responsable_ri, "")

            estado_ciclo = "NC"
            if motivo_ri == "OBS":
                estado_ciclo = "OBS"
            elif motivo_ri == "OP MEJORA":
                estado_ciclo = "OP MEJORA"

            row = db.execute(
                """
                SELECT id, COALESCE(estado, ''), COALESCE(re_inspeccion, ''), COALESCE(firma_digital, '')
                FROM procesos
                WHERE TRIM(COALESCE(obra, '')) = TRIM(?)
                  AND TRIM(COALESCE(posicion, '')) = TRIM(?)
                  AND UPPER(TRIM(COALESCE(proceso, ''))) = 'PINTURA'
                ORDER BY id DESC
                LIMIT 1
                """,
                (obra_sel, pieza),
            ).fetchone()

            if not row:
                # Si aún no existe PINTURA para la pieza, crear un registro base en NC.
                _upsert_estado_pintura_en_procesos(pieza, obra_sel, fecha_ri, responsable_ri, operario_sel, False)
                row = db.execute(
                    """
                    SELECT id, COALESCE(estado, ''), COALESCE(re_inspeccion, ''), COALESCE(firma_digital, '')
                    FROM procesos
                    WHERE TRIM(COALESCE(obra, '')) = TRIM(?)
                      AND TRIM(COALESCE(posicion, '')) = TRIM(?)
                      AND UPPER(TRIM(COALESCE(proceso, ''))) = 'PINTURA'
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (obra_sel, pieza),
                ).fetchone()
                if not row:
                    continue

            proc_id, estado_base, re_prev, firma_prev = row
            if motivo_ri == "NC NO CONFORME":
                estado_base = "NC"

            proceso_pfx = f"[{proceso_ri}] " if proceso_ri else ""
            motivo_linea = f"{proceso_pfx}{motivo_ri}. Accion correctiva: {accion_ri}" if accion_ri else f"{proceso_pfx}{motivo_ri}"
            ciclos_previos = _extraer_ciclos_reinspeccion(re_prev)
            if ciclos_previos:
                ult = ciclos_previos[-1]
                if (
                    str(ult.get("fecha") or "").strip() == fecha_ri
                    and str(ult.get("operario") or "").strip() == operario_sel
                    and str(ult.get("estado") or "").strip().upper() == estado_ciclo
                    and str(ult.get("responsable") or "").strip() == responsable_ri
                    and str(ult.get("motivo") or "").strip() == motivo_linea
                ):
                    continue
            re_nuevo = _agregar_ciclo_reinspeccion(
                re_prev,
                "PINTURA",
                fecha_ri,
                operario_sel,
                estado_ciclo,
                motivo=motivo_linea,
                firma=firma_ri,
                responsable=responsable_ri,
            )
            estado_pieza_nuevo = _estado_pieza_persistente(estado_base, re_nuevo)

            db.execute(
                """
                UPDATE procesos
                SET fecha=?, operario=?, estado=?, re_inspeccion=?, firma_digital=?, estado_pieza=?
                WHERE id=?
                """,
                (fecha_ri, operario_sel, estado_base, re_nuevo, firma_ri or firma_prev, estado_pieza_nuevo, proc_id),
            )
            _registrar_trazabilidad(
                db,
                proc_id,
                pieza,
                obra_sel,
                "PINTURA",
                estado_base,
                estado_pieza_nuevo,
                firma_ri or firma_prev,
                "REINSPECCION_PINTURA",
                re_nuevo,
                "REINSPECCION",
            )

    def _normalizar_reinspecciones(reinspeccion_data, piezas_validas_set):
        reinspeccion_clean = []
        if not isinstance(reinspeccion_data, list):
            return reinspeccion_clean, None

        _procesos_validos = {"SUPERFICIE", "FONDO", "TERMINACION"}
        for item in reinspeccion_data:
            if not isinstance(item, dict):
                continue
            pos = str(item.get("posicion") or "").strip()
            proceso_ri = str(item.get("proceso") or "").strip().upper()
            motivo_ri = str(item.get("motivo") or "").strip().upper()
            fecha_ri = str(item.get("fecha") or "").strip()
            accion_ri = str(item.get("accion_correctiva") or "").strip()
            resp_ri = str(item.get("responsable") or "").strip()
            firma_ri = str(item.get("firma") or "").strip()

            if not any([pos, proceso_ri, motivo_ri, fecha_ri, accion_ri, resp_ri, firma_ri]):
                continue
            if not all([pos, proceso_ri, motivo_ri, fecha_ri, accion_ri, resp_ri]):
                return None, "Completá todos los campos en Re-inspección (posición, proceso, motivo, fecha, acción correctiva y responsable)."
            if pos not in piezas_validas_set:
                return None, f"La pieza {pos} no pertenece al listado habilitado para esta obra."
            if resp_ri not in responsables_control:
                return None, "Responsable inválido en Re-inspección."
            if motivo_ri not in {"NC NO CONFORME", "OBS", "OP MEJORA"}:
                return None, "Motivo inválido en Re-inspección."
            if proceso_ri not in _procesos_validos:
                return None, f"Proceso inválido en Re-inspección. Debe ser Superficie, Fondo o Terminación."

            reinspeccion_clean.append(
                {
                    "posicion": pos,
                    "proceso": proceso_ri,
                    "motivo": motivo_ri,
                    "fecha": fecha_ri,
                    "accion_correctiva": accion_ri,
                    "responsable": resp_ri,
                    "firma": firma_ri or firmas_responsables.get(resp_ri, ""),
                }
            )
        return reinspeccion_clean, None

    if request.method == "POST":
        accion = (request.form.get("accion") or "").strip().lower()

        if accion not in {"guardar_etapa", "guardar_reinspeccion"}:
            return jsonify({"error": "Acción no soportada"}), 400

        obra = (request.form.get("obra") or "").strip()
        fecha = (request.form.get("fecha") or "").strip()
        etapa = (request.form.get("etapa") or "").strip().lower()
        responsable = (request.form.get("responsable") or "").strip()
        operario = (request.form.get("operario") or "").strip()
        esquema_pintura = (request.form.get("esquema_pintura") or "").strip()
        espesor_requerido_txt = (request.form.get("espesor_requerido") or "").strip()
        espesor_requerido_val = _to_float(espesor_requerido_txt)

        try:
            reinspeccion_data = json.loads(request.form.get("reinspeccion_json") or "[]")
        except Exception:
            reinspeccion_data = []

        if not all([obra, fecha, operario]):
            return jsonify({"error": "Completa obra, fecha y operario."}), 400

        if accion == "guardar_etapa" and not all([etapa, responsable]):
            return jsonify({"error": "Completa etapa y responsable para guardar control."}), 400

        if accion == "guardar_etapa" and etapa not in {"superficie", "fondo", "terminacion"}:
            return jsonify({"error": "Etapa inválida."}), 400

        if accion == "guardar_etapa" and responsable not in responsables_control:
            return jsonify({"error": "Responsable inválido."}), 400

        if operario not in operarios_list:
            return jsonify({"error": "Operario inválido."}), 400

        try:
            piezas_data = json.loads(request.form.get("piezas_json") or "[]")
        except Exception:
            piezas_data = []

        if accion == "guardar_etapa" and (not isinstance(piezas_data, list) or not piezas_data):
            return jsonify({"error": "Selecciona piezas para guardar el control."}), 400

        piezas_obra = _obtener_piezas_obra(obra)
        piezas_validas_set = {str(p.get("pieza") or "").strip() for p in piezas_obra}
        reinspeccion_clean, reins_err = _normalizar_reinspecciones(reinspeccion_data, piezas_validas_set)
        if reins_err:
            return jsonify({"error": reins_err}), 400

        control_id, data = _cargar_control_obra(obra)
        if not data:
            data = {
                "obra": obra,
                "esquema_pintura": esquema_pintura,
                "espesor_requerido": espesor_requerido_txt,
                "historial": [],
                "estado_actual": {},
            }

        data["obra"] = obra
        data["esquema_pintura"] = esquema_pintura
        data["espesor_requerido"] = espesor_requerido_txt
        data["operario"] = operario
        data["reinspeccion"] = reinspeccion_clean

        estado_actual = data.setdefault("estado_actual", {})

        if accion == "guardar_reinspeccion":
            if not reinspeccion_clean:
                return jsonify({"error": "Carga al menos una fila de re-inspección para guardar."}), 400

            _registrar_reinspecciones_en_procesos(obra, operario, reinspeccion_clean)

            estado_db = "completado" if bool(control_id and db.execute("SELECT 1 FROM control_pintura WHERE id=? AND estado='completado'", (control_id,)).fetchone()) else "en_progreso"
            piezas_snapshot = _snapshot_piezas(estado_actual, piezas_obra)
            if control_id:
                db.execute(
                    """
                    UPDATE control_pintura
                    SET mediciones=?, piezas=?, estado=?, fecha_modificacion=CURRENT_TIMESTAMP, usuario_modificacion='usuario'
                    WHERE id=?
                    """,
                    (json.dumps(data), json.dumps(piezas_snapshot), estado_db, control_id),
                )
            else:
                cursor = db.execute(
                    """
                    INSERT INTO control_pintura (obra, mediciones, piezas, estado, usuario_creacion, usuario_modificacion)
                    VALUES (?, ?, ?, ?, 'usuario', 'usuario')
                    """,
                    (obra, json.dumps(data), json.dumps(piezas_snapshot), estado_db),
                )
                control_id = cursor.lastrowid

            db.commit()
            return jsonify(
                {
                    "success": True,
                    "control_id": control_id,
                    "estado": estado_db,
                    "mensaje": f"Re-inspección guardada ({len(reinspeccion_clean)} fila/s).",
                    "redirect_url": f"/modulo/calidad/escaneo/control-pintura?obra={quote(obra)}",
                }
            )

        evento_piezas = []

        for item in piezas_data:
            pieza = str((item or {}).get("pieza") or "").strip()
            if not pieza:
                continue

            st = estado_actual.setdefault(pieza, {})

            if etapa == "superficie":
                estado_sup = str((item or {}).get("estado") or "").strip().upper()
                if estado_sup not in {"CONFORME", "NO CONFORME"}:
                    return jsonify({"error": f"Superficie inválida para {pieza}."}), 400

                st["superficie"] = {
                    "estado": estado_sup,
                    "ok": estado_sup == "CONFORME",
                    "fecha": str((item or {}).get("fecha_control") or fecha).strip() or fecha,
                    "responsable": responsable,
                    "operario": operario,
                }
                if estado_sup != "CONFORME":
                    st.pop("fondo", None)
                    st.pop("terminacion", None)
                    st["estado_pintura"] = "PENDIENTE SUPERFICIE"

                evento_piezas.append({"pieza": pieza, "estado": estado_sup, "fecha_control": st["superficie"]["fecha"]})

            elif etapa == "fondo":
                sup = st.get("superficie") or {}
                if not sup.get("ok"):
                    return jsonify({"error": f"No se puede cargar fondo en {pieza} sin superficie CONFORME."}), 400

                espesor_fondo = _to_float((item or {}).get("espesor"))
                if espesor_fondo <= 0:
                    return jsonify({"error": f"Espesor de fondo inválido para {pieza}."}), 400

                fecha_control = str((item or {}).get("fecha_control") or fecha).strip() or fecha
                st["fondo"] = {
                    "espesor": f"{espesor_fondo:.1f}",
                    "ok": True,
                    "fecha": fecha_control,
                    "responsable": responsable,
                    "operario": operario,
                }
                st.pop("terminacion", None)
                st["estado_pintura"] = "PENDIENTE TERMINACION"

                evento_piezas.append({"pieza": pieza, "espesor": f"{espesor_fondo:.1f}", "fecha_control": fecha_control})

            else:
                fondo = st.get("fondo") or {}
                if not fondo.get("ok"):
                    return jsonify({"error": f"No se puede cargar terminación en {pieza} sin fondo OK."}), 400

                espesor_term = _to_float((item or {}).get("espesor"))
                if espesor_term <= 0:
                    return jsonify({"error": f"Espesor de terminación inválido para {pieza}."}), 400

                fecha_control = str((item or {}).get("fecha_control") or fecha).strip() or fecha
                esp_fondo = _to_float(fondo.get("espesor"))
                esp_total = esp_fondo + espesor_term
                aprobado = esp_total >= espesor_requerido_val and espesor_requerido_val > 0

                st["terminacion"] = {
                    "espesor": f"{espesor_term:.1f}",
                    "fecha": fecha_control,
                    "responsable": responsable,
                    "operario": operario,
                    "espesor_total": f"{esp_total:.1f}",
                    "espesor_requerido": f"{espesor_requerido_val:.1f}" if espesor_requerido_val > 0 else "-",
                }
                st["pintura_aprobada"] = bool(aprobado)
                st["estado_pintura"] = "PINTURA APROBADA" if aprobado else "PINTURA NO CONFORME"

                _upsert_estado_pintura_en_procesos(pieza, obra, fecha_control, responsable, operario, aprobado)

                evento_piezas.append(
                    {
                        "pieza": pieza,
                        "espesor": f"{espesor_term:.1f}",
                        "fecha_control": fecha_control,
                        "espesor_total": f"{esp_total:.1f}",
                        "estado_pintura": st["estado_pintura"],
                    }
                )

        if not evento_piezas:
            return jsonify({"error": "No hay piezas válidas para registrar."}), 400

        data.setdefault("historial", []).append(
            {
                "fecha_registro": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "fecha_control": fecha,
                "etapa": etapa,
                "responsable": responsable,
                "piezas": evento_piezas,
            }
        )

        keys_obra = [p["pieza"] for p in piezas_obra]
        completo = True if keys_obra else False
        for pieza in keys_obra:
            st = estado_actual.get(pieza) or {}
            if not (st.get("superficie") and st.get("fondo") and st.get("terminacion") and st.get("pintura_aprobada")):
                completo = False
                break

        estado_db = "completado" if completo else "en_progreso"
        piezas_snapshot = _snapshot_piezas(estado_actual, piezas_obra)

        if control_id:
            db.execute(
                """
                UPDATE control_pintura
                SET mediciones=?, piezas=?, estado=?, fecha_modificacion=CURRENT_TIMESTAMP, usuario_modificacion='usuario'
                WHERE id=?
                """,
                (json.dumps(data), json.dumps(piezas_snapshot), estado_db, control_id),
            )
        else:
            cursor = db.execute(
                """
                INSERT INTO control_pintura (obra, mediciones, piezas, estado, usuario_creacion, usuario_modificacion)
                VALUES (?, ?, ?, ?, 'usuario', 'usuario')
                """,
                (obra, json.dumps(data), json.dumps(piezas_snapshot), estado_db),
            )
            control_id = cursor.lastrowid

        _registrar_reinspecciones_en_procesos(obra, operario, reinspeccion_clean)

        db.commit()

        return jsonify(
            {
                "success": True,
                "control_id": control_id,
                "estado": estado_db,
                "mensaje": f"Control guardado: {etapa.upper()} ({len(evento_piezas)} pieza/s).",
                "redirect_url": f"/modulo/calidad/escaneo/control-pintura?obra={quote(obra)}",
            }
        )

    obras = _obtener_obras()
    obra = (request.args.get("obra") or "").strip()
    esquema, espesor = _obtener_datos_obra(obra)
    piezas = _obtener_piezas_obra(obra)
    control_id, data_control = _cargar_control_obra(obra)
    estado_actual = data_control.get("estado_actual") if isinstance(data_control, dict) else {}
    operario_actual = data_control.get("operario") if isinstance(data_control, dict) else ""
    reinspeccion_actual = data_control.get("reinspeccion") if isinstance(data_control, dict) else []
    if obra and estado_actual:
        _sincronizar_pintura_desde_estado_actual(obra, estado_actual, operario_actual)

    opciones_obras = '<option value="">-- Seleccionar obra --</option>'
    for o in obras:
        sel = "selected" if o == obra else ""
        opciones_obras += f'<option value="{html_lib.escape(o)}" {sel}>{html_lib.escape(o)}</option>'

    opciones_etapa = """
        <option value="">-- Seleccionar etapa --</option>
        <option value="superficie">Control de superficie</option>
        <option value="fondo">Fondo imprimacion</option>
        <option value="terminacion">Terminacion</option>
    """

    opciones_responsables = '<option value="">-- Seleccionar responsable --</option>'
    for resp in responsables_list:
        opciones_responsables += f'<option value="{html_lib.escape(resp)}">{html_lib.escape(resp)}</option>'

    opciones_operarios = '<option value="">-- Seleccionar operario --</option>'
    for op in operarios_list:
        sel = "selected" if op == operario_actual else ""
        opciones_operarios += f'<option value="{html_lib.escape(op)}" {sel}>{html_lib.escape(op)}</option>'

    def _firma_html(responsable):
        nombre = str(responsable or "").strip()
        if not nombre:
            return "-"
        info = responsables_control.get(nombre) or {}
        firma_url = str(info.get("firma_url") or "").strip()
        if not firma_url:
            return html_lib.escape(nombre)
        return f'<img src="{html_lib.escape(firma_url)}" alt="firma" style="max-width:100px;max-height:24px;display:block;">'

    piezas_rows = ""
    for p in piezas:
        pieza = p["pieza"]
        st = (estado_actual or {}).get(pieza) or {}
        sup = st.get("superficie") or {}
        fondo = st.get("fondo") or {}
        term = st.get("terminacion") or {}
        estado_p = st.get("estado_pintura") or "-"
        estado_final = "APROBADO" if st.get("pintura_aprobada") else ("NO APROBADO" if term.get("espesor") else "-")
        estado_cls = "ok" if estado_final == "APROBADO" else ("nc" if estado_final == "NO APROBADO" else "")

        sup_resp = sup.get("responsable") or "-"
        fondo_resp = fondo.get("responsable") or "-"
        term_resp = term.get("responsable") or "-"

        piezas_rows += f"""
        <tr data-pieza="{html_lib.escape(pieza)}">
            <td><input type="checkbox" class="pieza-check" value="{html_lib.escape(pieza)}"></td>
            <td>{html_lib.escape(pieza)}</td>
            <td>{html_lib.escape(str(p.get('cantidad') or '-'))}</td>
            <td>{html_lib.escape(p.get('descripcion') or '-')}</td>
            <td>{html_lib.escape(sup.get('estado') or '-')}</td>
            <td>{html_lib.escape(str(sup.get('fecha') or '-'))}</td>
            <td>{html_lib.escape(str(sup_resp))}</td>
            <td>{_firma_html(sup_resp)}</td>
            <td>{html_lib.escape(str(fondo.get('espesor') or '-'))}</td>
            <td>{html_lib.escape(str(fondo.get('fecha') or '-'))}</td>
            <td>{_firma_html(fondo_resp)}</td>
            <td>{html_lib.escape(str(term.get('espesor') or '-'))}</td>
            <td>{html_lib.escape(str(term.get('fecha') or '-'))}</td>
            <td>{_firma_html(term_resp)}</td>
            <td>{html_lib.escape(str(term.get('espesor_total') or '-'))}</td>
            <td class="estado {estado_cls}">{html_lib.escape(estado_final)}</td>
        </tr>
        """

    piezas_nc = []
    for p in piezas:
        pieza = p["pieza"]
        st = (estado_actual or {}).get(pieza) or {}
        term = st.get("terminacion") or {}
        if term and not st.get("pintura_aprobada"):
            piezas_nc.append(pieza)
    piezas_reinsp = piezas_nc if piezas_nc else [p["pieza"] for p in piezas]
    options_reinsp_pieza = '<option value="">Seleccionar pieza...</option>' + "".join(
        f'<option value="{html_lib.escape(px)}">{html_lib.escape(px)}</option>' for px in piezas_reinsp
    )
    options_reinsp_proceso = (
        '<option value="">Seleccionar...</option>'
        '<option value="SUPERFICIE">Superficie</option>'
        '<option value="FONDO">Fondo</option>'
        '<option value="TERMINACION">Terminación</option>'
    )
    options_reinsp_motivo = (
        '<option value="">Seleccionar...</option>'
        '<option value="NC NO CONFORME">NC No conforme</option>'
        '<option value="OBS">OBS</option>'
        '<option value="OP MEJORA">OP Mejora</option>'
    )

    options_reinsp_responsable = '<option value="">Seleccionar...</option>' + "".join(
        f'<option value="{html_lib.escape(r)}">{html_lib.escape(r)}</option>' for r in responsables_list
    )
    reinspeccion_rows = ""
    for ri in (reinspeccion_actual or []):
        ri_pos_val = str((ri or {}).get("posicion") or "").strip()
        ri_proceso_val = str((ri or {}).get("proceso") or "").strip().upper()
        ri_motivo_val = str((ri or {}).get("motivo") or "").strip().upper()
        ri_fecha = html_lib.escape(str((ri or {}).get("fecha") or ""))
        ri_accion = html_lib.escape(str((ri or {}).get("accion_correctiva") or ""))
        ri_resp = str((ri or {}).get("responsable") or "").strip()
        ri_firma = html_lib.escape(str((ri or {}).get("firma") or ""))
        opts_pieza = '<option value="">Seleccionar pieza...</option>'
        piezas_row = list(piezas_reinsp)
        if ri_pos_val and ri_pos_val not in piezas_row:
            piezas_row.append(ri_pos_val)
        for px in piezas_row:
            sel = "selected" if px == ri_pos_val else ""
            opts_pieza += f'<option value="{html_lib.escape(px)}" {sel}>{html_lib.escape(px)}</option>'
        opts_proceso = '<option value="">Seleccionar...</option>'
        for pv, pl in [("SUPERFICIE", "Superficie"), ("FONDO", "Fondo"), ("TERMINACION", "Terminación")]:
            sel = "selected" if pv == ri_proceso_val else ""
            opts_proceso += f'<option value="{pv}" {sel}>{pl}</option>'
        opts_motivo = '<option value="">Seleccionar...</option>'
        for mv, lbl in [("NC NO CONFORME", "NC No conforme"), ("OBS", "OBS"), ("OP MEJORA", "OP Mejora")]:
            sel = "selected" if mv == ri_motivo_val else ""
            opts_motivo += f'<option value="{mv}" {sel}>{lbl}</option>'
        opts_sel = '<option value="">Seleccionar...</option>'
        for r in responsables_list:
            sel = "selected" if r == ri_resp else ""
            opts_sel += f'<option value="{html_lib.escape(r)}" {sel}>{html_lib.escape(r)}</option>'
        reinspeccion_rows += f"""
        <tr class="ri-row">
            <td><select class="ri-pos">{opts_pieza}</select></td>
            <td><select class="ri-proceso">{opts_proceso}</select></td>
            <td><select class="ri-motivo">{opts_motivo}</select></td>
            <td><input type="date" class="ri-fecha" value="{ri_fecha}"></td>
            <td><input type="text" class="ri-accion" value="{ri_accion}" placeholder="Acción correctiva"></td>
            <td><select class="ri-resp">{opts_sel}</select></td>
            <td><input type="text" class="ri-firma" value="{ri_firma}" readonly></td>
            <td><button type="button" class="btn btn-sm" style="background:#dc2626;" onclick="quitarRiRow(this)">🗑️ Quitar</button></td>
        </tr>
        """

    html = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Control de Pintura</title>
        <style>
            body {{ font-family: Arial, sans-serif; padding: 15px; background: #f4f4f4; margin: 0; }}
            .wrap {{ max-width: 1150px; margin: 0 auto; }}
            h2 {{ color: #9a3412; border-bottom: 3px solid #f97316; padding-bottom: 8px; margin: 0 0 16px 0; }}
            .card {{ background: #fff; border-radius: 8px; padding: 16px; box-shadow: 0 2px 6px rgba(0,0,0,.08); margin-bottom: 14px; }}
            .sec-title {{ margin: 0 0 12px 0; color: #7c2d12; font-size: 15px; font-weight: bold; border-left: 4px solid #f97316; padding-left: 8px; }}
            .grid2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 10px; }}
            .full {{ grid-column: 1 / -1; }}
            .field label {{ display: block; font-weight: bold; margin-bottom: 4px; color: #374151; font-size: 13px; }}
            .field input, .field select {{ width: 100%; padding: 9px 10px; border: 1px solid #d1d5db; border-radius: 6px; font-size: 13px; box-sizing: border-box; }}
            .dato {{ background: #fff7ed; border: 1px solid #fed7aa; border-radius: 6px; padding: 9px 10px; color: #9a3412; font-weight: bold; font-size: 13px; }}
            .hint {{ margin: 8px 0 0 0; color: #6b7280; font-size: 12px; font-style: italic; }}
            .btn {{ background: #f97316; color: #fff; border: none; padding: 9px 14px; border-radius: 6px; font-weight: bold; cursor: pointer; font-size: 13px; text-decoration: none; display: inline-block; }}
            .btn:hover {{ background: #ea580c; }}
            .btn:disabled {{ background: #cbd5e1; cursor: not-allowed; }}
            .btn-blue {{ background: #2563eb; }}
            .btn-blue:hover {{ background: #1d4ed8; }}
            .btn-green {{ background: #16a34a; }}
            .btn-green:hover {{ background: #15803d; }}
            .btn-sm {{ padding: 5px 9px; font-size: 12px; }}
            table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
            th, td {{ border-bottom: 1px solid #e5e7eb; padding: 7px 6px; text-align: center; }}
            th {{ background: #f97316; color: #fff; }}
            th.th-meta {{ background: #c2410c; }}
            th.th-sup {{ background: #ea580c; }}
            th.th-fondo {{ background: #f97316; }}
            th.th-term {{ background: #c2410c; }}
            th.th-res {{ background: #ea580c; }}
            th.th-sub {{ font-size: 11px; font-weight: 600; }}
            th.th-sub-sup {{ background: #ffedd5; color: #7c2d12; font-size: 11px; }}
            th.th-sub-fondo {{ background: #fff7ed; color: #9a3412; font-size: 11px; }}
            th.th-sub-term {{ background: #ffedd5; color: #7c2d12; font-size: 11px; }}
            th.th-sub-res {{ background: #fff7ed; color: #9a3412; font-size: 11px; }}
            td:first-child {{ text-align: left; }}
            .estado-ok, .ok {{ color: #166534; font-weight: bold; }}
            .estado-nc, .nc {{ color: #991b1b; font-weight: bold; }}
            .actions {{ display: flex; gap: 8px; flex-wrap: wrap; margin-top: 12px; align-items: center; }}
            .msg {{ margin-top: 8px; font-weight: bold; min-height: 18px; }}
            .msg-ok {{ color: #166534; }}
            .msg-err {{ color: #b91c1c; }}
            .reinsp-card {{ border: 2px solid #fdba74; background: #fffaf5; }}
            .reinsp-card .sec-title {{ color: #9a3412; }}
            .ri-table th {{ background: #ea580c; font-size: 11px; padding: 6px 5px; }}
            .ri-table td {{ font-size: 12px; padding: 5px 4px; }}
            .ri-table input, .ri-table select {{ padding: 4px 5px; font-size: 12px; width: 100%; box-sizing: border-box; border: 1px solid #d1d5db; border-radius: 4px; }}
            .pagination-btn {{ background:#2563eb;color:#fff;border:none;padding:5px 10px;border-radius:5px;cursor:pointer;font-size:12px; }}
            .pagination-btn:disabled {{ background:#cbd5e1;cursor:not-allowed; }}
            .ri-actions {{ display: flex; gap: 8px; align-items: center; margin-top: 8px; flex-wrap: wrap; }}
        </style>
    </head>
    <body>
        <div class="wrap">
            <h2>🎨 Control de Pintura</h2>
            <form id="formControl" method="post">
                <input type="hidden" name="accion" value="guardar_etapa">
                <input type="hidden" name="esquema_pintura" id="esquemaInput" value="{html_lib.escape(esquema)}">
                <input type="hidden" name="espesor_requerido" id="espesorInput" value="{html_lib.escape(str(espesor))}">
                <input type="hidden" name="piezas_json" id="piezasJson" value="[]">
                <input type="hidden" name="reinspeccion_json" id="reinspeccionJson" value="[]">

                <!-- ─── Configuración del control ─── -->
                <div class="card">
                    <h3 class="sec-title">⚙️ Configuración del control</h3>
                    <div class="grid2">
                        <div class="field full">
                            <label>🏗️ Obra</label>
                            <select id="obra" name="obra" required>
                                {opciones_obras}
                            </select>
                        </div>
                        <div class="field">
                            <label>📅 Fecha</label>
                            <input type="date" id="fecha" name="fecha" value="{date.today().isoformat()}" required>
                        </div>
                        <div class="field">
                            <label>🔧 Etapa a registrar</label>
                            <select id="etapa" name="etapa" required>
                                {opciones_etapa}
                            </select>
                        </div>
                        <div class="field">
                            <label>🎨 Esquema de pintura</label>
                            <div class="dato" id="esquemaView">{html_lib.escape(esquema or '-')}</div>
                        </div>
                        <div class="field">
                            <label>📏 Espesor total requerido (μm)</label>
                            <div class="dato" id="espesorView">{html_lib.escape(str(espesor or '-'))}</div>
                        </div>
                        <div class="field">
                            <label>👤 Responsable del control</label>
                            <select id="responsable" name="responsable" required>
                                {opciones_responsables}
                            </select>
                        </div>
                        <div class="field">
                            <label>👷 Operario</label>
                            <select id="operario" name="operario" required>
                                {opciones_operarios}
                            </select>
                        </div>
                    </div>
                    <p class="hint">🔄 Secuencia obligatoria por pieza: Superficie → Fondo → Terminación. No se habilita una etapa si la anterior no está aprobada.</p>
                </div>

                <!-- ─── Piezas ─── -->
                <div class="card">
                    <h3 class="sec-title">🔩 Piezas con soldadura aprobada</h3>
                    <div style="overflow-x:auto;">
                        <table id="tablaPiezas">
                            <thead>
                                <tr>
                                    <th rowspan="2">✔</th>
                                    <th class="th-meta" rowspan="2">Pieza</th>
                                    <th class="th-meta" rowspan="2">Cant.</th>
                                    <th class="th-meta" rowspan="2">Descripción</th>
                                    <th class="th-sup" colspan="4">🔲 Control de superficie</th>
                                    <th class="th-fondo" colspan="3">🎨 Fondo imprimación</th>
                                    <th class="th-term" colspan="3">✅ Terminación</th>
                                    <th class="th-res" colspan="2">📊 Resumen</th>
                                </tr>
                                <tr>
                                    <th class="th-sub th-sub-sup">Estado</th>
                                    <th class="th-sub th-sub-sup">Fecha</th>
                                    <th class="th-sub th-sub-sup">Responsable</th>
                                    <th class="th-sub th-sub-sup">Firma</th>
                                    <th class="th-sub th-sub-fondo">Esp. (μm)</th>
                                    <th class="th-sub th-sub-fondo">Fecha</th>
                                    <th class="th-sub th-sub-fondo">Firma</th>
                                    <th class="th-sub th-sub-term">Esp. (μm)</th>
                                    <th class="th-sub th-sub-term">Fecha</th>
                                    <th class="th-sub th-sub-term">Firma</th>
                                    <th class="th-sub th-sub-res">Esp. total</th>
                                    <th class="th-sub th-sub-res">Estado</th>
                                </tr>
                            </thead>
                            <tbody id="piezasTbody">
                                {piezas_rows if piezas_rows else '<tr><td colspan="16" style="text-align:center;color:#6b7280;padding:16px;">No hay piezas con soldadura aprobada para esta obra</td></tr>'}
                            </tbody>
                        </table>
                    </div>
                    <div id="paginacion" style="display:flex;align-items:center;gap:8px;margin-top:8px;flex-wrap:wrap;">
                        <button type="button" class="btn btn-sm btn-blue" id="paginaAnterior">◄ Anterior</button>
                        <span id="paginaInfo" style="font-size:13px;color:#374151;"></span>
                        <button type="button" class="btn btn-sm btn-blue" id="paginaSiguiente">Siguiente ►</button>
                        <span style="font-size:12px;color:#6b7280;margin-left:6px;">Filas por página:</span>
                        <select id="filasPorPagina" style="padding:4px 6px;font-size:12px;border:1px solid #d1d5db;border-radius:4px;width:auto;">
                            <option value="10">10</option>
                            <option value="20" selected>20</option>
                            <option value="50">50</option>
                            <option value="9999">Todas</option>
                        </select>
                    </div>
                    <div id="camposEtapa" style="margin-top:12px;"></div>
                    <div style="margin-top:10px;">
                        <button class="btn" type="button" id="btnGuardarControl">💾 Guardar control</button>
                    </div>
                </div>

                <!-- ─── Re-inspección ─── -->
                <div class="card reinsp-card">
                    <h3 class="sec-title">🔍 Re-inspección (piezas NC)</h3>
                    <p class="hint" style="margin:0 0 10px 0;">Registrá las re-inspecciones para piezas con estado no conforme. Indicá pieza, proceso afectado, motivo y completá los datos.</p>
                    <div style="overflow-x:auto;">
                        <table class="ri-table" id="tablaReinspeccion">
                            <thead>
                                <tr>
                                    <th>Posición</th>
                                    <th>Proceso</th>
                                    <th>Motivo</th>
                                    <th>Fecha</th>
                                    <th>Acción correctiva</th>
                                    <th>Responsable</th>
                                    <th>Firma</th>
                                    <th></th>
                                </tr>
                            </thead>
                            <tbody id="reinsBody">
                                {reinspeccion_rows if reinspeccion_rows else '<tr class="ri-row"><td><select class="ri-pos">'+options_reinsp_pieza+'</select></td><td><select class="ri-proceso">'+options_reinsp_proceso+'</select></td><td><select class="ri-motivo">'+options_reinsp_motivo+'</select></td><td><input type="date" class="ri-fecha"></td><td><input type="text" class="ri-accion" placeholder="Acción correctiva"></td><td><select class="ri-resp">'+options_reinsp_responsable+'</select></td><td><input type="text" class="ri-firma" readonly></td><td><button type="button" class="btn btn-sm" style="background:#dc2626;" onclick="quitarRiRow(this)">🗑️ Quitar</button></td></tr>'}
                            </tbody>
                        </table>
                    </div>
                    <div class="ri-actions">
                        <button type="button" class="btn btn-blue btn-sm" id="btnAddRI">➕ Agregar fila</button>
                        <button class="btn btn-green" type="button" id="btnGuardarRI">💾 Guardar re-inspección</button>
                    </div>
                </div>

                <!-- ─── Botones de navegación ─── -->
                <div class="card" style="padding:12px 16px;">
                    <div class="actions">
                        {f'<a class="btn btn-blue" href="/modulo/calidad/escaneo/generar-pdf-control/{control_id}">📄 Generar PDF</a>' if control_id else '<button class="btn btn-blue" type="button" disabled>📄 Generar PDF</button>'}
                        <a class="btn btn-blue" href="/modulo/calidad/escaneo/controles-pintura">📋 Ver controles</a>
                        <a class="btn btn-blue" href="/home">🏠 Estado de piezas</a>
                        <a class="btn btn-blue" href="/modulo/calidad/escaneo">⬅️ Volver</a>
                    </div>
                </div>

                <div id="msg" class="msg"></div>
            </form>
        </div>

        <script>
            const estadoActual = {json.dumps(estado_actual or {})};
            const firmasResponsables = {json.dumps(firmas_responsables)};
            const opcionesReinspResponsable = {json.dumps(options_reinsp_responsable)};
            const opcionesReinspPieza = {json.dumps(options_reinsp_pieza)};
            const opcionesReinspMotivo = {json.dumps(options_reinsp_motivo)};
            const opcionesReinspProceso = {json.dumps(options_reinsp_proceso)};

            function setMsg(text, ok) {{
                const node = document.getElementById('msg');
                node.className = ok ? 'msg msg-ok' : 'msg msg-err';
                node.textContent = text || '';
            }}

            function puedeSeleccionar(etapa, pieza) {{
                const st = estadoActual[pieza] || {{}};
                if (etapa === 'superficie') {{
                    return !(st.superficie && st.superficie.estado);
                }}
                if (etapa === 'fondo') {{
                    return !!(st.superficie && st.superficie.ok) && !(st.fondo && st.fondo.ok);
                }}
                if (etapa === 'terminacion') {{
                    return !!(st.fondo && st.fondo.ok) && !(st.terminacion && st.terminacion.espesor);
                }}
                return false;
            }}

            function refrescarSeleccionPorEtapa() {{
                const etapa = document.getElementById('etapa').value;
                const checks = document.querySelectorAll('.pieza-check');
                checks.forEach(chk => {{
                    const ok = puedeSeleccionar(etapa, chk.value);
                    chk.disabled = !ok;
                    if (!ok) chk.checked = false;
                }});
                renderCamposEtapa();
            }}

            function renderCamposEtapa() {{
                const etapa = document.getElementById('etapa').value;
                const cont = document.getElementById('camposEtapa');
                const sel = Array.from(document.querySelectorAll('.pieza-check:checked')).map(x => x.value);

                if (!etapa) {{ cont.innerHTML = ''; return; }}
                if (!sel.length) {{
                    cont.innerHTML = '<div class="hint">Seleccioná al menos una pieza habilitada para la etapa elegida.</div>';
                    return;
                }}

                let html = '<div style="overflow-x:auto;margin-top:4px;"><table><thead><tr><th>Pieza</th>';
                if (etapa === 'superficie') {{
                    html += '<th>Estado superficie</th><th>Fecha control</th>';
                }} else {{
                    html += '<th>Espesor (μm)</th><th>Fecha control</th>';
                }}
                html += '</tr></thead><tbody>';
                sel.forEach((pieza) => {{
                    html += '<tr><td><b>' + pieza + '</b></td>';
                    if (etapa === 'superficie') {{
                        html += '<td><select data-k="estado" data-p="' + pieza + '">';
                        html += '<option value="">Seleccionar...</option>';
                        html += '<option value="APROBADA">✅ APROBADO</option>';
                        html += '<option value="NO_APROBADA">❌ NO APROBADO</option>';
                        html += '</select></td>';
                        html += '<td><input data-k="fecha_control" data-p="' + pieza + '" type="date" value="' + (document.getElementById('fecha').value || '') + '"></td>';
                    }} else {{
                        html += '<td><input data-k="espesor" data-p="' + pieza + '" type="number" step="0.1" min="0" placeholder="0.0" style="width:90px;"></td>';
                        html += '<td><input data-k="fecha_control" data-p="' + pieza + '" type="date" value="' + (document.getElementById('fecha').value || '') + '"></td>';
                    }}
                    html += '</tr>';
                }});
                html += '</tbody></table></div>';
                cont.innerHTML = html;
            }}

            document.getElementById('obra').addEventListener('change', () => {{
                const obra = document.getElementById('obra').value;
                if (!obra) return;
                window.location.href = '/modulo/calidad/escaneo/control-pintura?obra=' + encodeURIComponent(obra);
            }});

            document.getElementById('etapa').addEventListener('change', refrescarSeleccionPorEtapa);
            document.querySelectorAll('.pieza-check').forEach(c => c.addEventListener('change', renderCamposEtapa));

            // ─── Paginación ───
            let paginaActual = 1;
            function getFilasPorPagina() {{
                return parseInt(document.getElementById('filasPorPagina').value) || 20;
            }}
            function actualizarPaginacion() {{
                const filas = Array.from(document.querySelectorAll('#piezasTbody tr'));
                const pp = getFilasPorPagina();
                const total = filas.length;
                const totalPags = pp >= 9990 ? 1 : Math.max(1, Math.ceil(total / pp));
                if (paginaActual > totalPags) paginaActual = totalPags;
                const desde = pp >= 9990 ? 0 : (paginaActual - 1) * pp;
                const hasta = pp >= 9990 ? total : Math.min(desde + pp, total);
                filas.forEach((tr, i) => {{
                    tr.style.display = (i >= desde && i < hasta) ? '' : 'none';
                }});
                document.getElementById('paginaInfo').textContent =
                    total === 0 ? '' : `Página ${{paginaActual}} de ${{totalPags}} (${{total}} piezas)`;
                document.getElementById('paginaAnterior').disabled = paginaActual <= 1;
                document.getElementById('paginaSiguiente').disabled = paginaActual >= totalPags;
            }}
            document.getElementById('paginaAnterior').addEventListener('click', () => {{ paginaActual--; actualizarPaginacion(); }});
            document.getElementById('paginaSiguiente').addEventListener('click', () => {{ paginaActual++; actualizarPaginacion(); }});
            document.getElementById('filasPorPagina').addEventListener('change', () => {{ paginaActual = 1; actualizarPaginacion(); }});
            actualizarPaginacion();


            function enviarFormulario(accionGuardado) {{
                const obra = document.getElementById('obra').value;
                const etapa = document.getElementById('etapa').value;
                const fecha = document.getElementById('fecha').value;
                const responsable = document.getElementById('responsable').value;
                const operario = document.getElementById('operario').value;
                const piezasSel = Array.from(document.querySelectorAll('.pieza-check:checked')).map(x => x.value);

                if (!obra || !fecha || !operario) {{
                    setMsg('⚠️ Completá obra, fecha y operario.', false);
                    return;
                }}
                if (accionGuardado === 'guardar_etapa') {{
                    if (!etapa || !responsable) {{
                        setMsg('⚠️ Completá etapa y responsable para guardar control.', false);
                        return;
                    }}
                    if (!piezasSel.length) {{
                        setMsg('⚠️ Seleccioná piezas habilitadas para registrar.', false);
                        return;
                    }}
                }}

                const piezas = [];
                for (const pieza of piezasSel) {{
                    const data = {{ pieza }};
                    if (etapa === 'superficie') {{
                        const node = document.querySelector('[data-k="estado"][data-p="' + pieza + '"]');
                        const nFec = document.querySelector('[data-k="fecha_control"][data-p="' + pieza + '"]');
                        const estado = (node && node.value) ? node.value : '';
                        const fCtrl = nFec ? nFec.value : '';
                        if (!estado || !fCtrl) {{
                            setMsg('⚠️ Completá estado y fecha para todas las piezas.', false);
                            return;
                        }}
                        data.estado = estado;
                        data.fecha_control = fCtrl;
                    }} else {{
                        const nEsp = document.querySelector('[data-k="espesor"][data-p="' + pieza + '"]');
                        const nFec = document.querySelector('[data-k="fecha_control"][data-p="' + pieza + '"]');
                        const esp = nEsp ? nEsp.value : '';
                        const fCtrl = nFec ? nFec.value : '';
                        if (!esp || !fCtrl) {{
                            setMsg('⚠️ Completá espesor y fecha para todas las piezas.', false);
                            return;
                        }}
                        data.espesor = esp;
                        data.fecha_control = fCtrl;
                    }}
                    piezas.push(data);
                }}

                const fd = new FormData();
                fd.append('accion', accionGuardado);
                fd.append('obra', obra);
                fd.append('fecha', fecha);
                fd.append('etapa', etapa);
                fd.append('responsable', responsable);
                fd.append('operario', operario);
                fd.append('esquema_pintura', document.getElementById('esquemaInput').value || '');
                fd.append('espesor_requerido', document.getElementById('espesorInput').value || '');
                fd.append('piezas_json', JSON.stringify(accionGuardado === 'guardar_etapa' ? piezas : []));

                const riRows = [];
                const nodos = Array.from(document.querySelectorAll('#reinsBody .ri-row'));
                for (const row of nodos) {{
                    const item = {{
                        posicion: (row.querySelector('.ri-pos')?.value || '').trim(),
                        proceso: (row.querySelector('.ri-proceso')?.value || '').trim(),
                        motivo: (row.querySelector('.ri-motivo')?.value || '').trim(),
                        fecha: (row.querySelector('.ri-fecha')?.value || '').trim(),
                        accion_correctiva: (row.querySelector('.ri-accion')?.value || '').trim(),
                        responsable: (row.querySelector('.ri-resp')?.value || '').trim(),
                        firma: (row.querySelector('.ri-firma')?.value || '').trim(),
                    }};
                    const tieneAlgo = Object.values(item).some(v => !!v);
                    if (!tieneAlgo) continue;
                    if (!item.posicion || !item.proceso || !item.motivo || !item.fecha || !item.accion_correctiva || !item.responsable) {{
                        setMsg('⚠️ Completá todos los campos de cada fila de Re-inspección cargada.', false);
                        return;
                    }}
                    riRows.push(item);
                }}
                fd.append('reinspeccion_json', JSON.stringify(riRows));

                fetch('/modulo/calidad/escaneo/control-pintura', {{ method: 'POST', body: fd }})
                    .then(r => r.json())
                    .then(data => {{
                        if (!data.success) {{
                            setMsg('❌ ' + (data.error || 'No se pudo guardar el control.'), false);
                            return;
                        }}
                        setMsg('✅ ' + data.mensaje, true);
                        if (data.redirect_url) {{
                            setTimeout(() => {{ window.location.href = data.redirect_url; }}, 700);
                        }}
                    }})
                    .catch(() => setMsg('❌ Error enviando datos al servidor.', false));
            }}

            document.getElementById('btnGuardarControl').addEventListener('click', () => enviarFormulario('guardar_etapa'));
            document.getElementById('btnGuardarRI').addEventListener('click', () => enviarFormulario('guardar_reinspeccion'));

            if (document.getElementById('etapa').value) {{
                refrescarSeleccionPorEtapa();
            }}

            function quitarRiRow(btn) {{
                const row = btn.closest('.ri-row');
                const body = document.getElementById('reinsBody');
                if (body.querySelectorAll('.ri-row').length === 1) {{
                    row.querySelectorAll('input').forEach(i => i.value = '');
                    row.querySelectorAll('select').forEach(s => s.value = '');
                    return;
                }}
                row.remove();
            }}

            function bindReinspRow(row) {{
                const sel = row.querySelector('.ri-resp');
                const firma = row.querySelector('.ri-firma');
                if (sel && firma) {{
                    const sync = () => {{ firma.value = firmasResponsables[sel.value] || ''; }};
                    sel.addEventListener('change', sync);
                    sync();
                }}
            }}

            document.querySelectorAll('#reinsBody .ri-row').forEach(bindReinspRow);
            document.getElementById('btnAddRI').addEventListener('click', () => {{
                const body = document.getElementById('reinsBody');
                const tr = document.createElement('tr');
                tr.className = 'ri-row';
                tr.innerHTML =
                    '<td><select class="ri-pos">' + opcionesReinspPieza + '</select></td>' +
                    '<td><select class="ri-proceso">' + opcionesReinspProceso + '</select></td>' +
                    '<td><select class="ri-motivo">' + opcionesReinspMotivo + '</select></td>' +
                    '<td><input type="date" class="ri-fecha"></td>' +
                    '<td><input type="text" class="ri-accion" placeholder="Acción correctiva"></td>' +
                    '<td><select class="ri-resp">' + opcionesReinspResponsable + '</select></td>' +
                    '<td><input type="text" class="ri-firma" readonly></td>' +
                    '<td><button type="button" class="btn btn-sm" style="background:#dc2626;" onclick="quitarRiRow(this)">🗑️ Quitar</button></td>';
                body.appendChild(tr);
                bindReinspRow(tr);
            }});
        </script>
    </body>
    </html>
    """

    return html

# Ruta para generar PDF de registros de pintura por obra
@calidad_bp.route("/modulo/calidad/escaneo/generar-pdf-pintura", methods=["GET"])
def generar_pdf_pintura():
    from datetime import date
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from urllib.parse import unquote
    import re

    db = get_db()
    ot_id_txt = (request.args.get("ot_id") or "").strip()
    ot_id = int(ot_id_txt) if ot_id_txt.isdigit() else None
    if ot_id is None:
        return "Debes filtrar una OT para generar el PDF", 400

    row_ot = db.execute(
        """
        SELECT id,
               TRIM(COALESCE(obra, '')) AS obra,
             TRIM(COALESCE(titulo, '')) AS titulo,
               COALESCE(esquema_pintura, '') AS esquema,
               COALESCE(espesor_total_requerido, '') AS espesor
        FROM ordenes_trabajo
        WHERE id = ?
        LIMIT 1
        """,
        (ot_id,),
    ).fetchone()
    if not row_ot:
        return "OT no encontrada", 404

    obra = str(row_ot[1] or "").strip()
    if not obra:
        return "La OT seleccionada no tiene obra asociada", 400

    responsables_control = _obtener_responsables_control(db)
    responsable_por_firma = {
        str(data.get("firma", "")).strip().lower(): nombre
        for nombre, data in responsables_control.items()
        if str(data.get("firma", "")).strip()
    }
    imagen_por_firma = {
        str(data.get("firma", "")).strip().lower(): str(data.get("firma_url", "")).strip()
        for nombre, data in responsables_control.items()
        if str(data.get("firma", "")).strip() and str(data.get("firma_url", "")).strip()
    }
    # También indexar por nombre: firma_digital en procesos guarda el nombre de la persona
    for _nombre_idx, _data_idx in responsables_control.items():
        _url_idx = str(_data_idx.get("firma_url", "")).strip()
        if _url_idx and _nombre_idx.strip().lower() not in imagen_por_firma:
            imagen_por_firma[_nombre_idx.strip().lower()] = _url_idx

        rows = db.execute(
            """
            SELECT id,
                   TRIM(COALESCE(posicion, '')) AS posicion,
                   COALESCE(cantidad, 0),
                   COALESCE(perfil, ''),
                   COALESCE(descripcion, ''),
                   UPPER(TRIM(COALESCE(proceso, ''))) AS proceso,
                   UPPER(TRIM(COALESCE(estado, ''))) AS estado,
                   COALESCE(fecha, ''),
                   COALESCE(reproceso, ''),
                   COALESCE(re_inspeccion, ''),
                   COALESCE(firma_digital, ''),
                   COALESCE(estado_pieza, '')
            FROM procesos
            WHERE ot_id = ?
              AND eliminado = 0
              AND UPPER(TRIM(COALESCE(proceso, ''))) IN ('PINTURA', 'PINTURA_FONDO')
            ORDER BY posicion ASC, id DESC
            """,
            (ot_id,),
        ).fetchall()

        espesor_total_requerido = str(row_ot[4] or "")
        esquema_pintura = str(row_ot[3] or "")

    def _to_float(value):
        txt = str(value or "").strip().replace(",", ".")
        if not txt or txt == "-":
            return 0.0
        try:
            return float(txt)
        except Exception:
            return 0.0

    def _format_entero(value):
        num = _to_float(value)
        if num <= 0:
            return "-"
        return str(int(round(num)))

    def _etapa_de_row(proceso_u, reproceso_txt):
        repro_u = (reproceso_txt or "").upper()
        if "ETAPA:SUPERFICIE" in repro_u:
            return "SUPERFICIE"
        if "ETAPA:FONDO" in repro_u:
            return "FONDO"
        if "ETAPA:TERMINACION" in repro_u:
            return "TERMINACION"
        if proceso_u == "PINTURA_FONDO":
            return "FONDO"
        return None

    def _extraer_espesor(texto, etiqueta):
        patron = rf"{etiqueta}:\s*([0-9]+(?:[\.,][0-9]+)?)"
        m = re.search(patron, texto or "", flags=re.IGNORECASE)
        return _to_float(m.group(1)) if m else 0.0

    def _detalle_limpio(reproceso_txt):
        txt = re.sub(r'^ETAPA:(SUPERFICIE|FONDO|TERMINACION)\s*\|?\s*', '', str(reproceso_txt or '').strip(), flags=re.IGNORECASE)
        return txt.strip() or "-"

    def _estado_resumen_principal(estado_u):
        estado_u = str(estado_u or "").strip().upper()
        if estado_u in ("NC", "NO CONFORME", "NO CONFORMIDAD"):
            return "NO CONFORME"
        if estado_u in ("OK", "APROBADO", "OBS", "OM", "CONFORME"):
            return "OK"
        if estado_u in ("OBS", "OM"):
            return estado_u
        return estado_u or "-"

    def _resolver_resultado_etapa(estado_u, fecha_txt, firma_txt, responsable_nombre, ciclos):
        estado_base = _estado_resumen_principal(estado_u)
        resultado = {
            "estado": estado_base,
            "fecha": fecha_txt or "-",
            "firma": firma_txt or "",
            "resp": responsable_nombre or "-",
        }
        if estado_base != "NO CONFORME":
            return resultado
        if not ciclos:
            return resultado

        ultimo = ciclos[-1] or {}
        estado_final = str(ultimo.get("estado") or "").strip().upper()
        if estado_final in ("OK", "APROBADO", "OBS", "OM"):
            resultado["estado"] = "OK"
            resultado["fecha"] = ultimo.get("fecha") or resultado["fecha"]
            resultado["firma"] = ultimo.get("firma") or resultado["firma"]
            resultado["resp"] = ultimo.get("responsable") or ultimo.get("inspector") or resultado["resp"]
            return resultado
        if estado_final in ("NC", "NO CONFORME", "NO CONFORMIDAD"):
            resultado["estado"] = "NO CONFORME"
        else:
            resultado["estado"] = "RE-INSPECCIÓN"
        resultado["fecha"] = ultimo.get("fecha") or resultado["fecha"]
        resultado["firma"] = ultimo.get("firma") or resultado["firma"]
        resultado["resp"] = ultimo.get("responsable") or ultimo.get("inspector") or resultado["resp"]
        return resultado

    def _firma_pdf(firma_txt, responsable_txt=""):
        firma_val = str(firma_txt or "").strip()
        responsable_val = str(responsable_txt or "").strip() or responsable_por_firma.get(firma_val.lower(), firma_val) or "-"
        if not firma_val:
            return Paragraph(html_lib.escape(responsable_val), base_style)
        url = imagen_por_firma.get(firma_val.lower(), "")
        if url and "/firma-supervisor/" in url:
            archivo = unquote(url.rsplit("/", 1)[-1])
            ruta = os.path.join(_FIRMAS_EMPLEADOS_DIR, archivo)
            if os.path.isfile(ruta):
                try:
                    img = RLImage(ruta)
                    img.drawWidth = 1.45 * cm
                    img.drawHeight = 0.5 * cm
                    firma_box = Table(
                        [[img], [Paragraph(html_lib.escape(responsable_val), base_style)]],
                        colWidths=[1.7 * cm],
                    )
                    firma_box.setStyle(TableStyle([
                        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                        ('LEFTPADDING', (0, 0), (-1, -1), 0),
                        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
                        ('TOPPADDING', (0, 0), (-1, -1), 0),
                        ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
                    ]))
                    return firma_box
                except Exception:
                    pass
        return Paragraph(html_lib.escape(responsable_val), base_style)

    def _encabezado_pintura_path():
        candidatos = [
            os.path.join(_APP_DIR, "ENCABEZADO_PINTURA.png"),
            os.path.join(_APP_DIR, "ENCABEZADO_PINTURA.jpg"),
            os.path.join(_APP_DIR, "ENCABEZADO_PINTURA.jpeg"),
            os.path.join(_APP_DIR, "ENCABEZAO_PINTURA.png"),
            os.path.join(_APP_DIR, "ENCABEZAO_PINTURA.jpg"),
            os.path.join(_APP_DIR, "ENCABEZAO_PINTURA.jpeg"),
        ]
        for candidato in candidatos:
            if os.path.exists(candidato):
                return candidato
        return None

    piezas = {}
    reinspection_rows = []
    operario_control = "-"
    fecha_reporte = date.today().isoformat()
    ultimo_id = -1

    for row in rows:
        row_id, posicion, cantidad, perfil, descripcion, proceso_u, estado_u, fecha_txt, reproceso_txt, re_inspeccion_txt, firma_txt, estado_pieza_txt = row
        if not posicion:
            continue
        etapa = _etapa_de_row(proceso_u, reproceso_txt)
        if not etapa:
            continue

        if row_id > ultimo_id:
            ultimo_id = row_id
            fecha_reporte = fecha_txt or fecha_reporte
            try:
                oper = db.execute("SELECT operario FROM procesos WHERE id=?", (row_id,)).fetchone()
                operario_control = (oper[0] if oper else "") or operario_control
            except Exception:
                pass

        if posicion not in piezas:
            piezas[posicion] = {
                "pieza": posicion,
                "cantidad": _format_entero(cantidad),
                "descripcion": (perfil or descripcion or "-").strip() or "-",
                "superficie": {"estado": "-", "fecha": "-", "resp": "", "firma": ""},
                "fondo": {"esp": 0.0, "fecha": "-", "resp": "", "firma": "", "estado": "-"},
                "terminacion": {"esp": 0.0, "fecha": "-", "resp": "", "firma": "", "estado": "-"},
                "estado_resumen": "-",
            }

        responsable_nombre = responsable_por_firma.get(str(firma_txt or "").strip().lower(), str(firma_txt or "").strip())
        pieza = piezas[posicion]
        ciclos = _extraer_ciclos_reinspeccion(re_inspeccion_txt or "")
        resultado_etapa = _resolver_resultado_etapa(estado_u, fecha_txt, firma_txt, responsable_nombre, ciclos)

        if etapa == "SUPERFICIE" and pieza["superficie"]["estado"] == "-":
            pieza["superficie"] = {
                "estado": resultado_etapa["estado"],
                "fecha": resultado_etapa["fecha"],
                "resp": resultado_etapa["resp"],
                "firma": resultado_etapa["firma"],
            }
            pieza["estado_resumen"] = resultado_etapa["estado"]
        elif etapa == "FONDO" and pieza["fondo"]["fecha"] == "-":
            pieza["fondo"] = {
                "esp": _extraer_espesor(reproceso_txt, "Espesor fondo"),
                "fecha": resultado_etapa["fecha"],
                "resp": resultado_etapa["resp"],
                "firma": resultado_etapa["firma"],
                "estado": resultado_etapa["estado"],
            }
            if pieza["estado_resumen"] == "-":
                pieza["estado_resumen"] = resultado_etapa["estado"]
        elif etapa == "TERMINACION" and pieza["terminacion"]["fecha"] == "-":
            pieza["terminacion"] = {
                "esp": _extraer_espesor(reproceso_txt, "Espesor terminacion"),
                "fecha": resultado_etapa["fecha"],
                "resp": resultado_etapa["resp"],
                "firma": resultado_etapa["firma"],
                "estado": resultado_etapa["estado"],
            }
            pieza["estado_resumen"] = resultado_etapa["estado"]

        if estado_u == "NC":
            motivo_base = _detalle_limpio(reproceso_txt)
            motivo_base = motivo_base.replace("Motivo NC:", "").strip(" |") or "-"
            if ciclos:
                for ciclo in ciclos:
                    reinspection_rows.append({
                        "posicion": posicion,
                        "proceso": {"SUPERFICIE": "Superficie", "FONDO": "Fondo", "TERMINACION": "Terminación"}.get(etapa, etapa),
                        "motivo": motivo_base,
                        "fecha": ciclo.get("fecha") or "-",
                        "accion_correctiva": ciclo.get("motivo") or "-",
                        "responsable": ciclo.get("responsable") or ciclo.get("inspector") or responsable_nombre or "-",
                        "firma": ciclo.get("firma") or firma_txt or "",
                    })
            else:
                reinspection_rows.append({
                    "posicion": posicion,
                    "proceso": {"SUPERFICIE": "Superficie", "FONDO": "Fondo", "TERMINACION": "Terminación"}.get(etapa, etapa),
                    "motivo": motivo_base,
                    "fecha": fecha_txt or "-",
                    "accion_correctiva": "-",
                    "responsable": responsable_nombre or "-",
                    "firma": firma_txt or "",
                })

    filas_pintura = []
    for posicion in sorted(piezas.keys()):
        pieza = piezas[posicion]
        fondo_esp = pieza["fondo"]["esp"]
        term_esp = pieza["terminacion"]["esp"]
        esp_total = fondo_esp + term_esp
        estados_etapa = [
            pieza["superficie"].get("estado") or "-",
            pieza["fondo"].get("estado") or "-",
            pieza["terminacion"].get("estado") or "-",
        ]
        estados_controlados = [e for e in estados_etapa if e != "-"]
        if any(e in ("NO CONFORME", "RE-INSPECCIÓN") for e in estados_controlados):
            estado_resumen = "NO CONFORME"
        elif estados_controlados:
            if esp_total > 0 and espesor_total_requerido:
                estado_resumen = "OK" if esp_total >= _to_float(espesor_total_requerido) else "NO CONFORME"
            else:
                estado_resumen = "OK"
        else:
            estado_resumen = "-"
        filas_pintura.append({
            "pieza": pieza["pieza"],
            "cantidad": pieza["cantidad"],
            "descripcion": pieza["descripcion"],
            "sup_estado": pieza["superficie"]["estado"],
            "sup_fecha": pieza["superficie"]["fecha"],
            "sup_resp": pieza["superficie"]["resp"],
            "sup_firma": pieza["superficie"]["firma"],
            "fondo_espesor": fondo_esp,
            "fondo_fecha": pieza["fondo"]["fecha"],
            "fondo_resp": pieza["fondo"]["resp"],
            "fondo_firma": pieza["fondo"]["firma"],
            "term_espesor": term_esp,
            "term_fecha": pieza["terminacion"]["fecha"],
            "term_resp": pieza["terminacion"]["resp"],
            "term_firma": pieza["terminacion"]["firma"],
            "esp_total": esp_total,
            "estado_pintura": estado_resumen,
        })

    pdf_buffer = BytesIO()
    doc = SimpleDocTemplate(pdf_buffer, pagesize=letter, topMargin=0.4*cm, bottomMargin=0.6*cm, leftMargin=0.5*cm, rightMargin=0.5*cm)
    styles = getSampleStyleSheet()
    base_style = ParagraphStyle('BasePaint', parent=styles['Normal'], fontSize=7.2, leading=8.4, textColor=colors.HexColor('#333333'))
    head_style = ParagraphStyle('HeadPaint', parent=styles['Normal'], fontSize=7.0, leading=8.0, alignment=1, fontName='Helvetica-Bold', textColor=colors.white)
    subhead_style = ParagraphStyle('SubHeadPaint', parent=styles['Normal'], fontSize=6.7, leading=7.8, alignment=1, fontName='Helvetica-Bold', textColor=colors.white)

    elements = []
    encabezado = _encabezado_pintura_path()
    if encabezado:
        try:
            header_img = RLImage(encabezado)
            max_width = 19.8 * cm
            max_height = 3.2 * cm
            if header_img.drawWidth > max_width:
                ratio = max_width / float(header_img.drawWidth)
                header_img.drawWidth *= ratio
                header_img.drawHeight *= ratio
            if header_img.drawHeight > max_height:
                ratio_h = max_height / float(header_img.drawHeight)
                header_img.drawWidth *= ratio_h
                header_img.drawHeight *= ratio_h
            elements.append(header_img)
        except Exception:
            elements.append(Paragraph("<b>CONTROL DE PINTURA</b>", ParagraphStyle('TitlePint', parent=styles['Heading2'], alignment=1, textColor=colors.HexColor('#111827'))))
    else:
        elements.append(Paragraph("<b>CONTROL DE PINTURA</b>", ParagraphStyle('TitlePint', parent=styles['Heading2'], alignment=1, textColor=colors.HexColor('#111827'))))
    elements.append(Spacer(1, 0.2*cm))

    info = Table([
        [Paragraph(f"<b>OT:</b> {html_lib.escape(f'OT {ot_id} - {obra}' + (f' - {str(row_ot[2] or "")}' if str(row_ot[2] or '').strip() else ''))}", base_style), Paragraph(f"<b>Fecha reporte:</b> {html_lib.escape(fecha_reporte or date.today().isoformat())}", base_style)],
        [Paragraph(f"<b>Obra:</b> {html_lib.escape(obra)}", base_style), Paragraph(f"<b>Esquema de pintura:</b> {html_lib.escape(str(esquema_pintura or '-'))}", base_style)],
        [Paragraph(f"<b>Espesor requerido (μm):</b> {html_lib.escape(str(espesor_total_requerido or '-'))}", base_style), Paragraph("", base_style)],
    ], colWidths=[9.9*cm, 9.9*cm])
    info.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#fff7ed')),
        ('BOX', (0, 0), (-1, -1), 0.6, colors.HexColor('#fdba74')),
        ('INNERGRID', (0, 0), (-1, -1), 0.35, colors.HexColor('#fed7aa')),
        ('LEFTPADDING', (0, 0), (-1, -1), 6),
        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
    ]))
    elements.append(info)
    elements.append(Spacer(1, 0.18*cm))

    elements.append(Paragraph("<b>Estado de Superficie y Control Pintura</b>", ParagraphStyle('SecPaint', parent=styles['Normal'], fontSize=9.0, textColor=colors.HexColor('#9a3412'))))
    elements.append(Spacer(1, 0.08*cm))

    pie_table_data = [
        [Paragraph("<b>Pieza</b>", head_style), Paragraph("<b>Control Superficie</b>", head_style), "", Paragraph("<b>Fondo de Imprimación</b>", head_style), "", "", "", Paragraph("<b>Terminación</b>", head_style), "", "", "", Paragraph("<b>Resumen de Pintura</b>", head_style), ""],
        ["", Paragraph("<b>Estado</b>", subhead_style), Paragraph("<b>Resp. y Firma</b>", subhead_style), Paragraph("<b>Esp.</b>", subhead_style), Paragraph("<b>Estado</b>", subhead_style), Paragraph("<b>Fecha</b>", subhead_style), Paragraph("<b>Resp. y Firma</b>", subhead_style), Paragraph("<b>Esp.</b>", subhead_style), Paragraph("<b>Estado</b>", subhead_style), Paragraph("<b>Fecha</b>", subhead_style), Paragraph("<b>Resp. y Firma</b>", subhead_style), Paragraph("<b>Esp. Total</b>", subhead_style), Paragraph("<b>Estado</b>", subhead_style)],
    ]

    if filas_pintura:
        for fila in filas_pintura:
            pie_table_data.append([
                Paragraph(html_lib.escape(str(fila.get("pieza") or "-")), base_style),
                Paragraph(html_lib.escape(str(fila.get("sup_estado") or "-")), base_style),
                _firma_pdf(fila.get("sup_firma") or "", fila.get("sup_resp") or ""),
                Paragraph(_format_entero(fila.get('fondo_espesor')), base_style),
                Paragraph(html_lib.escape(str((piezas.get(str(fila.get("pieza") or ""), {}) or {}).get("fondo", {}).get("estado") or "-")), base_style),
                Paragraph(html_lib.escape(str(fila.get("fondo_fecha") or "-")), base_style),
                _firma_pdf(fila.get("fondo_firma") or "", fila.get("fondo_resp") or ""),
                Paragraph(_format_entero(fila.get('term_espesor')), base_style),
                Paragraph(html_lib.escape(str((piezas.get(str(fila.get("pieza") or ""), {}) or {}).get("terminacion", {}).get("estado") or "-")), base_style),
                Paragraph(html_lib.escape(str(fila.get("term_fecha") or "-")), base_style),
                _firma_pdf(fila.get("term_firma") or "", fila.get("term_resp") or ""),
                Paragraph(_format_entero(fila.get('esp_total')), base_style),
                Paragraph(html_lib.escape(str(fila.get("estado_pintura") or "-")), base_style),
            ])
    else:
        pie_table_data.append([Paragraph("-", base_style) for _ in range(13)])

    pie_table = Table(pie_table_data, colWidths=[2.0*cm, 1.25*cm, 2.1*cm, 1.0*cm, 1.25*cm, 1.25*cm, 2.1*cm, 1.0*cm, 1.25*cm, 1.25*cm, 2.1*cm, 1.2*cm, 1.55*cm], repeatRows=2)
    pie_table.setStyle(TableStyle([
        ('SPAN', (0, 0), (0, 1)),
        ('SPAN', (1, 0), (2, 0)),
        ('SPAN', (3, 0), (6, 0)),
        ('SPAN', (7, 0), (10, 0)),
        ('SPAN', (11, 0), (12, 0)),
        ('BACKGROUND', (0, 0), (0, 1), colors.HexColor('#f97316')),
        ('BACKGROUND', (1, 0), (2, 1), colors.HexColor('#fb923c')),
        ('BACKGROUND', (3, 0), (6, 1), colors.HexColor('#f97316')),
        ('BACKGROUND', (7, 0), (10, 1), colors.HexColor('#fb923c')),
        ('BACKGROUND', (11, 0), (12, 1), colors.HexColor('#f97316')),
        ('TEXTCOLOR', (0, 0), (-1, 1), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#cbd5e1')),
        ('ROWBACKGROUNDS', (0, 2), (-1, -1), [colors.white, colors.HexColor('#fff7ed')]),
        ('LEFTPADDING', (0, 0), (-1, -1), 3),
        ('RIGHTPADDING', (0, 0), (-1, -1), 3),
        ('TOPPADDING', (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
    ]))
    elements.append(pie_table)

    elements.append(Spacer(1, 0.18*cm))
    elements.append(Paragraph("<b>Re-inspección (solo piezas NC)</b>", ParagraphStyle('SecPaintRI', parent=styles['Normal'], fontSize=9.0, textColor=colors.HexColor('#9a3412'))))
    elements.append(Spacer(1, 0.08*cm))

    ri_table_data = [[
        Paragraph("<b>Posición</b>", head_style),
        Paragraph("<b>Proceso</b>", head_style),
        Paragraph("<b>Motivo</b>", head_style),
        Paragraph("<b>Fecha</b>", head_style),
        Paragraph("<b>Acción correctiva</b>", head_style),
        Paragraph("<b>Responsable</b>", head_style),
        Paragraph("<b>Firma</b>", head_style),
    ]]
    if reinspection_rows:
        for ri in reinspection_rows:
            ri_table_data.append([
                Paragraph(html_lib.escape(str(ri.get("posicion") or "-")), base_style),
                Paragraph(html_lib.escape(str(ri.get("proceso") or "-")), base_style),
                Paragraph(html_lib.escape(str(ri.get("motivo") or "-")), base_style),
                Paragraph(html_lib.escape(str(ri.get("fecha") or "-")), base_style),
                Paragraph(html_lib.escape(str(ri.get("accion_correctiva") or "-")), base_style),
                Paragraph(html_lib.escape(str(ri.get("responsable") or "-")), base_style),
                _firma_pdf(ri.get("firma") or "", ri.get("responsable") or ""),
            ])
    else:
        ri_table_data.append([Paragraph("-", base_style) for _ in range(7)])

    ri_table = Table(ri_table_data, colWidths=[1.9*cm, 2.2*cm, 2.2*cm, 1.9*cm, 6.1*cm, 2.7*cm, 2.3*cm], repeatRows=1)
    ri_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f97316')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#cbd5e1')),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fff7ed')]),
        ('LEFTPADDING', (0, 0), (-1, -1), 3),
        ('RIGHTPADDING', (0, 0), (-1, -1), 3),
        ('TOPPADDING', (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ('ALIGN', (4, 1), (4, -1), 'LEFT'),
    ]))
    elements.append(ri_table)

    doc.build(elements)
    pdf_buffer.seek(0)
    filename = f"Control_Pintura_OT_{ot_id}_{obra}_{date.today().isoformat()}.pdf".replace(" ", "_")
    _guardar_pdf_databook(obra, "calidad_pintura", filename, pdf_buffer.getvalue(), ot_id=ot_id)
    pdf_buffer.seek(0)
    return send_file(pdf_buffer, mimetype='application/pdf', as_attachment=True, download_name=filename)

# Endpoint AJAX para obtener datos de obra
@calidad_bp.route("/modulo/calidad/escaneo/control-pintura/api", methods=["GET"])
def control_pintura_api():
    db = get_db()
    obra = (request.args.get("obra") or "").strip()

    if not obra:
        return jsonify({"error": "Obra requerida"}), 400

    row = db.execute(
        """
        SELECT esquema_pintura, espesor_total_requerido
        FROM ordenes_trabajo
        WHERE TRIM(COALESCE(obra,'')) = ?
          AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
        LIMIT 1
        """,
        (obra,),
    ).fetchone()

    ctrl = db.execute(
        """
        SELECT id, mediciones
        FROM control_pintura
        WHERE TRIM(COALESCE(obra,'')) = TRIM(?)
          AND estado IN ('en_progreso', 'completado')
        ORDER BY id DESC
        LIMIT 1
        """,
        (obra,),
    ).fetchone()

    estado_actual = {}
    historial_count = 0
    control_id = None

    if ctrl:
        control_id, med_json = ctrl
        try:
            med = json.loads(med_json) if med_json else {}
        except Exception:
            med = {}
        if isinstance(med, dict):
            estado_actual = med.get("estado_actual") or {}
            historial_count = len(med.get("historial") or [])

    return jsonify(
        {
            "esquema": (row[0] if row else "") or "",
            "espesor": str((row[1] if row else "") or ""),
            "control_id": control_id,
            "estado_actual": estado_actual,
            "historial_count": historial_count,
        }
    )

# ======================
# RUTA: RESUMEN DE CONTROL DE PINTURA
# ======================
@calidad_bp.route("/modulo/calidad/escaneo/resumen-control/<int:control_id>", methods=["GET"])
def resumen_control_pintura(control_id):
    """Mostrar resumen de control completado con tabla final"""
    db = get_db()
    responsables_control = _obtener_responsables_control(db)
    
    ctrl_row = db.execute(
        "SELECT id, obra, mediciones, piezas FROM control_pintura WHERE id = ? AND estado IN ('en_progreso', 'completado')",
        (control_id,)
    ).fetchone()
    
    if not ctrl_row:
        return "Control no encontrado", 404
    
    ctrl_id, obra, med_json, piezas_json = ctrl_row
    datos = json.loads(med_json) if med_json else {}
    etapas = datos.get("etapas", {})
    
    # Verificar si está completo
    etapas_completas = list(etapas.keys())
    esta_completo = len(etapas_completas) == 3 and set(etapas_completas) == {"superficie", "fondo", "terminacion"}
    
    esquema = datos.get("esquema_pintura", "-")
    espesor_requerido = datos.get("espesor_requerido", "-")
    
    # Construir tabla consolidada
    filas_tabla = []
    
    if etapas_completas:
        piezas_dict = {}
        
        # Procesar superficie
        if "superficie" in etapas:
            for p in etapas["superficie"]:
                pieza = p.get("pieza", "")
                if pieza not in piezas_dict:
                    piezas_dict[pieza] = {
                        "pieza": pieza,
                        "sup_estado": p.get("estado", "-"),
                        "fondo_espesor": "-",
                        "fondo_fecha": "-",
                        "term_espesor": "-",
                        "term_fecha": "-",
                    }
                else:
                    piezas_dict[pieza]["sup_estado"] = p.get("estado", "-")
        
        # Procesar fondo
        if "fondo" in etapas:
            for p in etapas["fondo"]:
                pieza = p.get("pieza", "")
                if pieza not in piezas_dict:
                    piezas_dict[pieza] = {
                        "pieza": pieza,
                        "sup_estado": "-",
                        "fondo_espesor": p.get("espesor", "-"),
                        "fondo_fecha": p.get("fecha", "-"),
                        "term_espesor": "-",
                        "term_fecha": "-",
                    }
                else:
                    piezas_dict[pieza]["fondo_espesor"] = p.get("espesor", "-")
                    piezas_dict[pieza]["fondo_fecha"] = p.get("fecha", "-")
        
        # Procesar terminación
        if "terminacion" in etapas:
            for p in etapas["terminacion"]:
                pieza = p.get("pieza", "")
                if pieza not in piezas_dict:
                    piezas_dict[pieza] = {
                        "pieza": pieza,
                        "sup_estado": "-",
                        "fondo_espesor": "-",
                        "fondo_fecha": "-",
                        "term_espesor": p.get("espesor", "-"),
                        "term_fecha": p.get("fecha", "-"),
                    }
                else:
                    piezas_dict[pieza]["term_espesor"] = p.get("espesor", "-")
                    piezas_dict[pieza]["term_fecha"] = p.get("fecha", "-")
        
        # Calcular resumen
        for pieza_key in sorted(piezas_dict.keys()):
            row = piezas_dict[pieza_key]
            
            # Calcular espesor total
            try:
                fondo_esp = float(str(row["fondo_espesor"] or "0").replace(",", "."))
            except:
                fondo_esp = 0
            
            try:
                term_esp = float(str(row["term_espesor"] or "0").replace(",", "."))
            except:
                term_esp = 0
            
            esp_total = fondo_esp + term_esp
            
            # Calcular estado
            try:
                esp_req = float(str(espesor_requerido or "0").replace(",", "."))
            except:
                esp_req = 0
            
            estado = "APROBADO" if (esp_total >= esp_req and esp_total > 0) else ("NO CONFORME" if esp_total > 0 else "-")
            
            row["esp_total"] = f"{esp_total:.1f}" if esp_total > 0 else "-"
            row["esp_requerido"] = f"{esp_req:.1f}" if esp_req > 0 else "-"
            row["estado"] = estado
            
            filas_tabla.append(row)
    
    # Generar HTML de tabla
    filas_html = ""
    for row in filas_tabla:
        estado_style = 'style="color: #16a34a; font-weight: bold;"' if row["estado"] == "APROBADO" else 'style="color: #dc2626; font-weight: bold;"' if row["estado"] != "-" else ''
        filas_html += f"""
        <tr>
            <td>{row['pieza']}</td>
            <td>{row['sup_estado']}</td>
            <td>{row['fondo_espesor']}</td>
            <td>{row['fondo_fecha']}</td>
            <td>{row['term_espesor']}</td>
            <td>{row['term_fecha']}</td>
            <td>{row['esp_total']}</td>
            <td>{row['esp_requerido']}</td>
            <td {estado_style}>{row['estado']}</td>
        </tr>
        """
    
    # Estados de las etapas
    etapa_surface = "✓ COMPLETADA" if "superficie" in etapas_completas else "⏳ Pendiente"
    etapa_fondo = "✓ COMPLETADA" if "fondo" in etapas_completas else "⏳ Pendiente"
    etapa_term = "✓ COMPLETADA" if "terminacion" in etapas_completas else "⏳ Pendiente"
    
    html = f"""
    <html>
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>Resumen Control de Pintura</title>
        <style>
            body {{ font-family: Arial, sans-serif; padding: 20px; background: #f5f5f5; margin: 0; }}
            .container {{ max-width: 1100px; margin: 0 auto; }}
            .header {{ background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); margin-bottom: 20px; }}
            h1 {{ color: #9a3412; margin: 0; border-bottom: 3px solid #f97316; padding-bottom: 10px; }}
            
            .info-grid {{ display: grid; grid-template-columns: 1fr 1fr 1fr 1fr; gap: 15px; margin: 20px 0; }}
            .info-box {{ background: white; padding: 15px; border-radius: 6px; border-left: 4px solid #f97316; }}
            .info-box label {{ font-weight: bold; color: #666; font-size: 12px; }}
            .info-box .value {{ font-size: 16px; color: #333; font-weight: bold; margin-top: 5px; }}
            
            .etapas {{ display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 15px; margin: 20px 0; }}
            .etapa {{ background: white; padding: 15px; border-radius: 6px; text-align: center; }}
            .etapa.completada {{ background: #dcfce7; border: 2px solid #16a34a; }}
            .etapa.pendiente {{ background: #fef3c7; border: 2px solid #f59e0b; }}
            .etapa .nombre {{ font-weight: bold; font-size: 14px; margin-bottom: 10px; }}
            .etapa .estado {{ font-size: 12px; }}
            
            table {{ width: 100%; border-collapse: collapse; background: white; margin-top: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
            th {{ background: #f97316; color: white; padding: 12px; text-align: left; font-weight: bold; font-size: 13px; }}
            td {{ padding: 10px 12px; border-bottom: 1px solid #e5e7eb; }}
            tr:hover {{ background: #f9f9f9; }}
            
            .buttons {{ display: flex; gap: 10px; margin-top: 20px; }}
            button, a {{ padding: 10px 20px; border: none; border-radius: 6px; font-weight: bold; cursor: pointer; text-decoration: none; display: inline-block; }}
            .btn-primary {{ background: #f97316; color: white; }}
            .btn-primary:hover {{ background: #ea580c; }}
            .btn-secondary {{ background: #e5e7eb; color: #333; }}
            .btn-secondary:hover {{ background: #d1d5db; }}
            
            .status-badge {{ display: inline-block; padding: 5px 10px; border-radius: 4px; font-size: 12px; font-weight: bold; }}
            .status-ok {{ background: #dcfce7; color: #166534; }}
            .status-nc {{ background: #fee2e2; color: #991b1b; }}
            .status-pending {{ background: #fef3c7; color: #92400e; }}
        </style>
    </head>
    <body>
        <div class="container">
            <div class="header">
                <h1>🎨 Resumen de Control de Pintura - ID {control_id}</h1>
            </div>
            
            <div class="info-grid">
                <div class="info-box">
                    <label>Obra</label>
                    <div class="value">{obra}</div>
                </div>
                <div class="info-box">
                    <label>Esquema de Pintura</label>
                    <div class="value">{esquema}</div>
                </div>
                <div class="info-box">
                    <label>Espesor Requerido (μm)</label>
                    <div class="value">{espesor_requerido}</div>
                </div>
                <div class="info-box">
                    <label>Estado</label>
                    <div class="value">
                        <span class="status-badge {'status-ok' if esta_completo else 'status-pending'}">
                            {'COMPLETADO' if esta_completo else 'EN PROGRESO'}
                        </span>
                    </div>
                </div>
            </div>
            
            <h3>Estado de Etapas</h3>
            <div class="etapas">
                <div class="etapa {'completada' if 'superficie' in etapas_completas else 'pendiente'}">
                    <div class="nombre">1. Control de Superficie</div>
                    <div class="estado">{etapa_surface}</div>
                </div>
                <div class="etapa {'completada' if 'fondo' in etapas_completas else 'pendiente'}">
                    <div class="nombre">2. Fondo de Imprimación</div>
                    <div class="estado">{etapa_fondo}</div>
                </div>
                <div class="etapa {'completada' if 'terminacion' in etapas_completas else 'pendiente'}">
                    <div class="nombre">3. Terminación</div>
                    <div class="estado">{etapa_term}</div>
                </div>
            </div>
            
            <h3>Tabla de Resultados</h3>
            <table>
                <thead>
                    <tr>
                        <th>Pieza</th>
                        <th>Control Superficie</th>
                        <th>Fondo - Espesor (μm)</th>
                        <th>Fondo - Fecha</th>
                        <th>Terminación - Espesor (μm)</th>
                        <th>Terminación - Fecha</th>
                        <th>Espesor Total (μm)</th>
                        <th>Espesor Req. (μm)</th>
                        <th>Estado</th>
                    </tr>
                </thead>
                <tbody>
                    {filas_html if filas_html else '<tr><td colspan="9" style="text-align: center; color: #999;">Sin datos</td></tr>'}
                </tbody>
            </table>
            
            <div class="buttons">
                <button class="btn-primary" onclick="window.print()">🖨️ Imprimir</button>
                <a href="/modulo/calidad/escaneo/control-pintura?obra={urllib.parse.quote(obra)}" class="btn-secondary">← Volver a Formulario</a>
                <a href="/modulo/calidad/escaneo/controles-pintura" class="btn-secondary">📋 Listar Controles</a>
            </div>
        </div>
    </body>
    </html>
    """
    
    return html

# ======================
# ENDPOINT JSON PARA PROCESAR QR
# ======================
@calidad_bp.route("/procesar-qr", methods=["POST"])
def procesar_qr():
    try:
        data = request.get_json()
        qr_data = data.get("qr_code", "").strip()
        
        if not qr_data:
            return jsonify({"error": "QR vacío"}), 400

        redirect_url = construir_redirect_desde_qr(qr_data)
        if not redirect_url:
            return jsonify({"error": "Formato de QR inválido"}), 400

        return jsonify({"redirect": redirect_url}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ======================
# MÁ“DULO 3 - PARTE SEMANAL (Placeholder)
# ======================
