import os
import re
import json
import html as html_lib
from io import BytesIO
from datetime import datetime
from urllib.parse import quote
from flask import Blueprint, redirect, request, send_file, session
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Image, Paragraph, Spacer
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4, landscape, letter
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from db_utils import get_db, _guardar_pdf_databook as _db_guardar_pdf_databook
from flask import jsonify

_APP_DIR = os.path.dirname(os.path.abspath(__file__))
_REMITOS_DIR = os.path.join(_APP_DIR, "remitos")
_DATABOOKS_DIR = os.path.join(_APP_DIR, "Reportes Produccion")
_DATABOOK_SECCIONES = {
    "calidad_recepcion": os.path.join("1-Calidad (Data Book)", "1.1-Recepcion de material"),
    "calidad_corte_perfiles": os.path.join("1-Calidad (Data Book)", "1.2-Corte perfiles"),
    "calidad_armado_soldadura": os.path.join("1-Calidad (Data Book)", "1.3-Armado y soldadura"),
    "calidad_pintura": os.path.join("1-Calidad (Data Book)", "1.4-Pintura"),
    "calidad_despacho": os.path.join("1-Calidad (Data Book)", "1.5-Despacho"),
    "remitos": "2-Remitos de despacho",
}

os.makedirs(_REMITOS_DIR, exist_ok=True)


def _guardar_pdf_databook(obra, seccion_key, filename, pdf_bytes, ot_id=None):
    return _db_guardar_pdf_databook(obra, seccion_key, filename, pdf_bytes, _DATABOOKS_DIR, _DATABOOK_SECCIONES, ot_id=ot_id)


remito_bp = Blueprint("remito", __name__)


@remito_bp.route("/modulo/remito", methods=["GET", "POST"])
def remitos():
    db = get_db()
    role_actual = str(session.get("user_role") or "").strip().lower()
    es_obra = role_actual == "obra"

    if request.method == "POST":
        ot_id = request.form.get("ot_id")
        fecha_remito = request.form.get("fecha_remito")
        transporte = request.form.get("transporte", "")
        piezas_ids = request.form.getlist("piezas")
        manual_articulos = request.form.getlist("manual_articulo[]")
        manual_cantidades_raw = request.form.getlist("manual_cantidad[]")
        manual_observaciones_list = request.form.getlist("manual_observaciones[]")

        manual_items = []
        total_manual_rows = max(len(manual_articulos), len(manual_cantidades_raw), len(manual_observaciones_list))
        for i in range(total_manual_rows):
            articulo = (manual_articulos[i] if i < len(manual_articulos) else "").strip()
            cantidad_raw = (manual_cantidades_raw[i] if i < len(manual_cantidades_raw) else "").strip()
            observaciones = (manual_observaciones_list[i] if i < len(manual_observaciones_list) else "").strip()

            if not articulo:
                continue

            try:
                cantidad = int(float(cantidad_raw)) if cantidad_raw else 1
            except Exception:
                cantidad = 1

            if cantidad < 1:
                cantidad = 1

            manual_items.append({
                "articulo": articulo,
                "cantidad": cantidad,
                "observaciones": observaciones
            })

        if not manual_items:
            manual_articulo_simple = (request.form.get("manual_articulo", "") or "").strip()
            manual_cantidad_simple_raw = (request.form.get("manual_cantidad", "") or "").strip()
            manual_observaciones_simple = (request.form.get("manual_observaciones", "") or "").strip()
            if manual_articulo_simple:
                try:
                    manual_cantidad_simple = int(float(manual_cantidad_simple_raw)) if manual_cantidad_simple_raw else 1
                except Exception:
                    manual_cantidad_simple = 1
                if manual_cantidad_simple < 1:
                    manual_cantidad_simple = 1
                manual_items.append({
                    "articulo": manual_articulo_simple,
                    "cantidad": manual_cantidad_simple,
                    "observaciones": manual_observaciones_simple
                })

        if not ot_id or not fecha_remito:
            return "Faltan datos requeridos", 400
        if not piezas_ids and not manual_items:
            return "Debe seleccionar al menos una pieza o cargar un articulo manual", 400

        ot = db.execute("SELECT cliente, obra FROM ordenes_trabajo WHERE id = ?", (ot_id,)).fetchone()

        if not ot:
            return "OT no encontrada", 404

        try:
            next_remito = db.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM remitos").fetchone()[0]
            remito_code = f"R-{int(next_remito):06d}"

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            pdf_base = f"remito_{remito_code}_{timestamp}.pdf"
            pdf_filename = os.path.join(_REMITOS_DIR, pdf_base)

            doc = SimpleDocTemplate(
                pdf_filename,
                pagesize=A4,
                leftMargin=18,
                rightMargin=18,
                topMargin=18,
                bottomMargin=18
            )
            story = []
            styles = getSampleStyleSheet()

            title_style = ParagraphStyle(
                'RemitoTitle',
                parent=styles['Heading1'],
                fontSize=18,
                leading=22,
                textColor=colors.HexColor('#1f2937'),
                spaceAfter=4
            )
            subtitle_style = ParagraphStyle(
                'RemitoSubTitle',
                parent=styles['Normal'],
                fontSize=11,
                leading=14,
                textColor=colors.HexColor('#4b5563')
            )
            cell_style = ParagraphStyle(
                'CellWrap',
                parent=styles['Normal'],
                fontSize=8,
                leading=10
            )

            logo_path = os.path.join(_APP_DIR, "LOGO.png")
            logo_flow = ""
            if os.path.exists(logo_path):
                logo_flow = Image(logo_path, width=40*mm, height=22*mm)

            header_right = Paragraph(
                f"<b>REMITO DE ENTREGA</b><br/><font size='11'>Remito N.&deg; {remito_code}</font>",
                ParagraphStyle('HeaderCenter', parent=title_style, alignment=1)
            )
            header_table = Table([[logo_flow, header_right]], colWidths=[13*mm, 161*mm])
            header_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('ALIGN', (0, 0), (0, 0), 'LEFT'),
                ('ALIGN', (1, 0), (1, 0), 'CENTER'),
                ('LEFTPADDING', (0, 0), (-1, -1), 0),
                ('RIGHTPADDING', (0, 0), (-1, -1), 0),
                ('TOPPADDING', (0, 0), (-1, -1), 0),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ]))
            story.append(header_table)

            total_items = len(piezas_ids) + len(manual_items)
            info_rows = [
                [
                    Paragraph(f"<b>Cliente:</b> {ot[0]}", subtitle_style),
                    Paragraph(f"<b>OT:</b> {ot_id}", subtitle_style),
                    Paragraph(f"<b>Fecha:</b> {fecha_remito}", subtitle_style)
                ],
                [
                    Paragraph(f"<b>Obra:</b> {ot[1]}", subtitle_style),
                    Paragraph(f"<b>Transporte:</b> {transporte or '-'}", subtitle_style),
                    Paragraph(f"<b>Cant. de Items:</b> {total_items}", subtitle_style)
                ]
            ]
            info_table = Table(info_rows, colWidths=[65*mm, 58*mm, 51*mm])
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

            table_data = [['POS.', 'PERFIL', 'PESO', 'TOTAL', 'ENVIADO', 'DESCRIPCION', 'OBSERVACIONES']]
            total_enviado_sum = 0

            for pieza_id in piezas_ids:
                pieza = db.execute("""
                    SELECT p_despacho.id,
                           p_first.posicion,
                           p_first.obra,
                           COALESCE(p_first.cantidad, ''),
                           COALESCE(p_first.perfil, ''),
                           COALESCE(p_first.peso, ''),
                           COALESCE(p_first.descripcion, '')
                    FROM procesos p_despacho
                    LEFT JOIN procesos p_first ON p_despacho.posicion = p_first.posicion
                                               AND p_despacho.obra = p_first.obra
                                               AND p_first.id = (
                                                   SELECT MIN(id) FROM procesos
                                                   WHERE posicion = p_despacho.posicion
                                                     AND obra = p_despacho.obra
                                               )
                    WHERE p_despacho.id = ?
                """, (pieza_id,)).fetchone()

                if pieza:
                    posicion = str(pieza[1]) if pieza[1] else ''
                    cantidad_total = int(float(pieza[3]) if pieza[3] else 0)
                    perfil = str(pieza[4]) if pieza[4] else ''
                    peso = str(pieza[5]) if pieza[5] else ''
                    descripcion = str(pieza[6]) if pieza[6] else ''
                    observaciones = request.form.get(f"obs_{pieza_id}", "")

                    cantidad_enviada = request.form.get(f"cant_{pieza_id}", str(cantidad_total))
                    try:
                        cantidad_enviada = int(cantidad_enviada)
                    except Exception:
                        cantidad_enviada = cantidad_total

                    if cantidad_enviada < 0:
                        cantidad_enviada = 0
                    if cantidad_enviada > cantidad_total:
                        cantidad_enviada = cantidad_total

                    total_enviado_sum += cantidad_enviada
                    enviado_display = f"{cantidad_enviada} de {cantidad_total}"

                    table_data.append([
                        Paragraph(posicion, cell_style),
                        Paragraph(perfil, cell_style),
                        Paragraph(peso, cell_style),
                        Paragraph(str(cantidad_total), cell_style),
                        Paragraph(enviado_display, cell_style),
                        Paragraph(descripcion, cell_style),
                        Paragraph(observaciones or '-', cell_style)
                    ])

            for idx, item in enumerate(manual_items, start=1):
                manual_articulo = item["articulo"]
                manual_cantidad = item["cantidad"]
                manual_observaciones = item["observaciones"]
                total_enviado_sum += manual_cantidad
                table_data.append([
                    Paragraph(f"MAN {idx}", cell_style),
                    Paragraph("-", cell_style),
                    Paragraph("-", cell_style),
                    Paragraph(str(manual_cantidad), cell_style),
                    Paragraph(f"{manual_cantidad} de {manual_cantidad}", cell_style),
                    Paragraph(manual_articulo, cell_style),
                    Paragraph(manual_observaciones or '-', cell_style)
                ])

            table = Table(
                table_data,
                colWidths=[14*mm, 22*mm, 12*mm, 11*mm, 17*mm, 47*mm, 51*mm],
                repeatRows=1
            )
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f97316')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 9),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('ALIGN', (0, 0), (0, -1), 'CENTER'),
                ('ALIGN', (2, 0), (4, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('GRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#cbd5e1')),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fff7ed')]),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('LEFTPADDING', (0, 0), (-1, -1), 5),
                ('RIGHTPADDING', (0, 0), (-1, -1), 5),
            ]))
            story.append(table)
            story.append(Spacer(1, 10))

            resumen = Table(
                [[
                    Paragraph(f"<b>Total enviado:</b> {total_enviado_sum} unidades", subtitle_style),
                    Paragraph(f"<b>Remito:</b> {remito_code}", subtitle_style)
                ]],
                colWidths=[116*mm, 58*mm]
            )
            resumen.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#f8fafc')),
                ('BOX', (0, 0), (-1, -1), 0.5, colors.HexColor('#cbd5e1')),
                ('LEFTPADDING', (0, 0), (-1, -1), 8),
                ('RIGHTPADDING', (0, 0), (-1, -1), 8),
                ('TOPPADDING', (0, 0), (-1, -1), 5),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ]))
            story.append(resumen)
            story.append(Spacer(1, 18))

            firma_table = Table(
                [[
                    Paragraph("<b>Responsable de Entrega</b><br/><br/>____________________________", subtitle_style),
                    Paragraph("<b>Recibido Por</b><br/><br/>____________________________", subtitle_style)
                ]],
                colWidths=[87*mm, 87*mm]
            )
            firma_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('LEFTPADDING', (0, 0), (-1, -1), 0),
                ('RIGHTPADDING', (0, 0), (-1, -1), 0),
                ('TOPPADDING', (0, 0), (-1, -1), 0),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
            ]))
            story.append(firma_table)

            doc.build(story)

            try:
                with open(pdf_filename, "rb") as f_pdf:
                    _guardar_pdf_databook(ot[1], "remitos", pdf_base, f_pdf.read(), ot_id=int(ot_id))
            except Exception:
                pass

            material_entregado_value = ','.join(piezas_ids)
            for item in manual_items:
                manual_tag = f"MANUAL:{item['articulo']}"
                material_entregado_value = f"{material_entregado_value},{manual_tag}" if material_entregado_value else manual_tag

            db.execute("""
                INSERT INTO remitos (cliente, ot_id, material_entregado, cantidad, fecha, pdf_path)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (ot[0], ot_id, material_entregado_value, total_enviado_sum, fecha_remito, pdf_base))

            # Al generar el remito PDF, marcar las piezas incluidas como despachadas.
            remito_meta = f"REMITO:{remito_code}|FECHA_DESPACHO:{fecha_remito}"
            for pieza_id in piezas_ids:
                row_proc = db.execute(
                    """
                    SELECT COALESCE(reproceso, '')
                    FROM procesos
                    WHERE id = ?
                    LIMIT 1
                    """,
                    (pieza_id,),
                ).fetchone()
                reproceso_prev = str(row_proc[0] or "").strip() if row_proc else ""
                if remito_meta in reproceso_prev:
                    reproceso_nuevo = reproceso_prev
                elif reproceso_prev:
                    reproceso_nuevo = f"{reproceso_prev} | {remito_meta}"
                else:
                    reproceso_nuevo = remito_meta

                db.execute(
                    """
                    UPDATE procesos
                    SET estado_pieza = 'DESPACHADO',
                        reproceso = ?
                    WHERE id = ?
                    """,
                    (reproceso_nuevo, pieza_id),
                )

            db.commit()

            return redirect(f"/descargar-remito/{pdf_base}")
        except Exception as e:
            return f"Error generando PDF: {str(e)}", 500

    # GET
    ots = db.execute("SELECT id, cliente, obra, TRIM(COALESCE(titulo, '')) AS titulo FROM ordenes_trabajo WHERE estado != 'Finalizada' AND fecha_cierre IS NULL AND ((es_mantenimiento IS NULL OR es_mantenimiento = 0) OR id = 2) ORDER BY id DESC").fetchall()
    obras_disponibles = sorted({str(ot[2] or "").strip() for ot in ots if str(ot[2] or "").strip()})
    ots_por_obra = {}
    for ot in ots:
        obra_txt = str(ot[2] or "").strip()
        if not obra_txt:
            continue
        ots_por_obra.setdefault(obra_txt, []).append({
            "id": int(ot[0]),
            "cliente": str(ot[1] or ""),
            "obra": obra_txt,
            "titulo": str(ot[3] or ""),
        })
    ots_por_obra_json = json.dumps(ots_por_obra, ensure_ascii=False).replace("</", "<\\/")

    # Paginación de remitos
    POR_PAGINA_REM = 20
    page_rem_txt = (request.args.get("page") or "1").strip()
    page_rem = int(page_rem_txt) if page_rem_txt.isdigit() else 1
    total_remitos = db.execute("SELECT COUNT(*) FROM remitos").fetchone()[0]
    total_paginas_rem = max(1, (total_remitos + POR_PAGINA_REM - 1) // POR_PAGINA_REM)
    page_rem = max(1, min(page_rem, total_paginas_rem))
    offset_rem = (page_rem - 1) * POR_PAGINA_REM
    remitos_list = db.execute("SELECT * FROM remitos ORDER BY fecha_creacion DESC LIMIT ? OFFSET ?", (POR_PAGINA_REM, offset_rem)).fetchall()

    def _pag_rem_url(p):
        return f"/modulo/remito?page={p}"

    paginacion_rem_html = ""
    if total_paginas_rem > 1:
        paginacion_rem_html = '<div style="display:flex;justify-content:center;gap:5px;flex-wrap:wrap;padding:10px 0;">'
        paginacion_rem_html += f'<a href="{_pag_rem_url(page_rem-1)}" style="padding:6px 10px;border:1px solid #ddd;border-radius:4px;text-decoration:none;color:#333;">&#8249; Ant.</a>' if page_rem > 1 else '<span style="padding:6px 10px;border:1px solid #ddd;border-radius:4px;color:#ccc;">&#8249; Ant.</span>'
        for _p in range(max(1, page_rem - 2), min(total_paginas_rem + 1, page_rem + 3)):
            if _p == page_rem:
                paginacion_rem_html += f'<span style="padding:6px 10px;border:1px solid #667eea;border-radius:4px;background:#667eea;color:white;font-weight:bold;">{_p}</span>'
            else:
                paginacion_rem_html += f'<a href="{_pag_rem_url(_p)}" style="padding:6px 10px;border:1px solid #ddd;border-radius:4px;text-decoration:none;color:#333;">{_p}</a>'
        paginacion_rem_html += f'<a href="{_pag_rem_url(page_rem+1)}" style="padding:6px 10px;border:1px solid #ddd;border-radius:4px;text-decoration:none;color:#333;">Sig. &#8250;</a>' if page_rem < total_paginas_rem else '<span style="padding:6px 10px;border:1px solid #ddd;border-radius:4px;color:#ccc;">Sig. &#8250;</span>'
        paginacion_rem_html += '</div>'

    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * { box-sizing: border-box; }
    body { font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }
    h2 { color: #333; border-bottom: 3px solid #f093fb; padding-bottom: 10px; }
    .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
    .header-actions { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
    .btn { background: #f97316; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; }
    .btn:hover { background: #ea580c; }
    .btn-despacho { background: #f97316; }
    .btn-despacho:hover { background: #ea580c; }
    form { background: white; padding: 20px; border-radius: 5px; margin: 20px 0; max-width: 1200px; }
    .form-group { margin-bottom: 15px; }
    label { display: block; font-weight: bold; margin-bottom: 5px; }
    input[type="text"], input[type="date"], select { width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 4px; }
    button { width: 100%; padding: 12px; background: #f97316; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; font-size: 16px; }
    button:hover { background: #ea580c; }
    .piezas-table-wrapper { margin: 20px 0; overflow-x: auto; }
    .piezas-table { width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    .piezas-table th { background: #f97316; color: white; padding: 12px; text-align: left; font-weight: bold; border-bottom: 2px solid #ea580c; }
    .piezas-table td { padding: 12px; border-bottom: 1px solid #e0e0e0; }
    .piezas-table tr:hover { background: #f9f9f9; }
    .piezas-table input[type="checkbox"] { margin-right: 8px; cursor: pointer; width: 18px; height: 18px; }
    .pieza-row { background: white; }
    .pieza-row textarea { width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 3px; font-family: Arial; font-size: 12px; resize: vertical; min-height: 40px; }
    .cantidad-cell, .perfil-cell, .peso-cell { text-align: center; font-size: 13px; }
    .descripcion-cell { max-width: 150px; }
    .cantidad-input { width: 80px; padding: 6px; border: 1px solid #ddd; border-radius: 3px; text-align: center; font-size: 13px; }
    .cantidad-info { font-size: 12px; color: #666; font-weight: bold; }
    .manual-item-box { background: #fff7ed; border: 1px solid #fed7aa; border-radius: 6px; padding: 12px; margin-top: 8px; }
    .manual-item-grid { display: grid; grid-template-columns: 2fr 1fr; gap: 10px; }
    .manual-item-grid textarea { width: 100%; min-height: 70px; padding: 8px; border: 1px solid #ddd; border-radius: 4px; resize: vertical; }
    .manual-item-row { border: 1px dashed #fdba74; border-radius: 6px; padding: 10px; margin-bottom: 10px; background: #fffbeb; }
    .manual-item-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 8px; }
    .manual-item-title { font-size: 13px; font-weight: bold; color: #9a3412; margin-bottom: 8px; }
    .btn-manual { border: none; border-radius: 4px; color: white; padding: 7px 10px; cursor: pointer; font-size: 12px; }
    .btn-add-manual { background: #16a34a; }
    .btn-remove-manual { background: #dc2626; }
    table { width: 100%; border-collapse: collapse; background: white; margin-top: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    th, td { padding: 10px; border-bottom: 1px solid #ddd; text-align: left; }
    th { background: #f093fb; color: white; font-weight: bold; }
    tr:hover { background: #f5f5f5; }
    .btn-small { display: inline-block; padding: 5px 10px; font-size: 12px; width: auto; }
    .btn-download { background: #4facfe; text-decoration: none; border-radius: 3px; color: white; }
    .btn-delete { background: #ef4444; text-decoration: none; border-radius: 3px; color: white; margin-left: 6px; }
    .error { color: #d32f2f; margin: 10px 0; padding: 10px; background: #ffebee; border-radius: 3px; }
    .success { color: #388e3c; margin: 10px 0; padding: 10px; background: #e8f5e9; border-radius: 3px; }
    </style>
    </head>
    <body>
    <div class="header">
        <h2>📦 Generador de Remitos</h2>
        <div class="header-actions">
            __BTN_DESPACHO__
            <a href="/" class="btn">⬅️ Volver</a>
        </div>
    </div>

    __FORM_REMITO__
    """

    form_remito_html = """
    <form method="post" id="remito-form">
        <div class="form-group">
            <label>Obra:</label>
            <select id="obra-select" onchange="cargarOTs()" required>
                <option value="">Seleccionar obra...</option>
    """

    for obra in obras_disponibles:
        form_remito_html += f'<option value="{html_lib.escape(obra)}">{html_lib.escape(obra)}</option>'

    form_remito_html += """
            </select>
        </div>

        <div class="form-group">
            <label>Orden de Trabajo:</label>
            <select name="ot_id" id="ot-select" onchange="cargarPiezas()" required disabled>
                <option value="">Seleccionar OT...</option>
            </select>
        </div>
        
        <div class="form-group">
            <label>Fecha de Remito:</label>
            <input type="date" name="fecha_remito" required>
        </div>
        
        <div class="form-group">
            <label>Transporte:</label>
            <input type="text" name="transporte" placeholder="Ej: Empresa XYZ, Auto particular, etc.">
        </div>

        <div class="form-group">
            <label><b>✓ Piezas Aprobadas en Despacho:</b></label>
            <div class="piezas-table-wrapper" id="piezas-container">
                <p style="color: #999; padding: 20px;">Selecciona una OT primero...</p>
            </div>
        </div>

        <div class="form-group">
            <label><b>➕ Carga Manual (pieza u otro articulo):</b></label>
            <div class="manual-item-box">
                <div id="manual-items-container">
                    <div class="manual-item-row">
                        <div class="manual-item-title">Articulo manual #1</div>
                        <div class="manual-item-grid">
                            <div>
                                <label>Articulo / Descripcion:</label>
                                <input type="text" name="manual_articulo[]" placeholder="Ej: Buloneria, placa adicional, insumo, etc.">
                            </div>
                            <div>
                                <label>Cantidad:</label>
                                <input type="number" name="manual_cantidad[]" min="1" value="1" placeholder="1">
                            </div>
                        </div>
                        <div style="margin-top: 10px;">
                            <label>Observaciones del articulo manual:</label>
                            <textarea name="manual_observaciones[]" placeholder="Detalle adicional del articulo cargado manualmente..."></textarea>
                        </div>
                    </div>
                </div>
                <div class="manual-item-actions">
                    <button type="button" class="btn-manual btn-add-manual" onclick="agregarArticuloManual()">+ Agregar otro articulo</button>
                </div>
            </div>
        </div>
        
        <button type="submit">📄 Generar Remito PDF</button>
    </form>
    """

    if es_obra:
        form_remito_html = """
        <div class="success" style="margin: 14px 0 8px 0;">
            Modo solo lectura: podés ver remitos generados y descargar sus PDF.
        </div>
        """

    html = html.replace(
        "__BTN_DESPACHO__",
        "" if es_obra else '<a href="/modulo/calidad/despacho" class="btn btn-despacho">📦 Ir a Control Despacho (Formulario)</a>'
    )
    html = html.replace("__FORM_REMITO__", form_remito_html)

    html += f"""
    <h2>Remitos Generados <span style="font-size:14px;color:#666;font-weight:normal;">({total_remitos} total, p&aacute;g {page_rem}/{total_paginas_rem})</span></h2>
    <table>
        <tr>
            <th>N° Remito</th>
            <th>OT</th>
            <th>Cliente</th>
            <th>Cantidad</th>
            <th>Fecha</th>
            <th>Acciones</th>
        </tr>
    """

    for remito in remitos_list:
        pdf_name = os.path.basename(remito[6] or "")
        remito_code = f"R-{int(remito[0]):06d}"
        if pdf_name.startswith("remito_R-"):
            partes = pdf_name.split("_")
            if len(partes) >= 2:
                remito_code = partes[1]

        acciones_html = f'<a href="/descargar-remito/{quote(pdf_name)}" class="btn btn-small btn-download">📥 Descargar</a>'
        if not es_obra:
            acciones_html += f' <a href="/eliminar-remito/{remito[0]}" class="btn btn-small btn-delete" onclick="return confirm(\'¿Eliminar este remito? Esta acción no se puede deshacer.\');">🗑 Eliminar</a>'

        html += f"""
        <tr>
            <td><b>{remito_code}</b></td>
            <td><b>{remito[2]}</b></td>
            <td>{remito[1]}</td>
            <td>{int(remito[4])}</td>
            <td>{remito[5]}</td>
            <td>
                {acciones_html}
            </td>
        </tr>
        """

    html += f"""
    </table>
    {paginacion_rem_html}
    """

    html += """
    <script id="ots-por-obra-json" type="application/json">__OTS_POR_OBRA_JSON__</script>

    <script>
    const otsPorObra = JSON.parse(document.getElementById('ots-por-obra-json').textContent || '{}');

    function cargarOTs() {
        const obraSel = document.getElementById('obra-select').value;
        const otSelect = document.getElementById('ot-select');

        otSelect.innerHTML = '<option value="">Seleccionar OT...</option>';
        document.getElementById('piezas-container').innerHTML = '<p style="color: #999; padding: 20px;">Selecciona una OT...</p>';

        if (!obraSel || !otsPorObra[obraSel] || !Array.isArray(otsPorObra[obraSel]) || otsPorObra[obraSel].length === 0) {
            otSelect.disabled = true;
            return;
        }

        otsPorObra[obraSel].forEach(ot => {
            const opt = document.createElement('option');
            opt.value = String(ot.id);
            opt.textContent = `OT ${ot.id} - ${ot.obra}${ot.titulo ? ' - ' + ot.titulo : ''}`;
            otSelect.appendChild(opt);
        });
        otSelect.disabled = false;
    }

    function renumerarArticulosManual() {
        const rows = document.querySelectorAll('#manual-items-container .manual-item-row');
        rows.forEach((row, index) => {
            let title = row.querySelector('.manual-item-title');
            if (!title) {
                title = document.createElement('div');
                title.className = 'manual-item-title';
                row.insertBefore(title, row.firstChild);
            }
            title.textContent = `Articulo manual #${index + 1}`;
        });
    }

    function agregarArticuloManual() {
        const container = document.getElementById('manual-items-container');
        const row = document.createElement('div');
        row.className = 'manual-item-row';
        row.innerHTML = `
            <div class="manual-item-title"></div>
            <div class="manual-item-grid">
                <div>
                    <label>Articulo / Descripcion:</label>
                    <input type="text" name="manual_articulo[]" placeholder="Ej: Buloneria, placa adicional, insumo, etc.">
                </div>
                <div>
                    <label>Cantidad:</label>
                    <input type="number" name="manual_cantidad[]" min="1" value="1" placeholder="1">
                </div>
            </div>
            <div style="margin-top: 10px;">
                <label>Observaciones del articulo manual:</label>
                <textarea name="manual_observaciones[]" placeholder="Detalle adicional del articulo cargado manualmente..."></textarea>
            </div>
            <div class="manual-item-actions">
                <button type="button" class="btn-manual btn-remove-manual" onclick="this.closest('.manual-item-row').remove(); renumerarArticulosManual();">- Quitar</button>
            </div>
        `;
        container.appendChild(row);
        renumerarArticulosManual();
    }
    
    function cargarPiezas() {
        const otId = document.getElementById('ot-select').value;

        if (!otId) {
            document.getElementById('piezas-container').innerHTML = '<p style="color: #999; padding: 20px;">Selecciona una OT...</p>';
            return;
        }
        
        fetch(`/api/piezas-remito/${otId}`)
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    console.error('Error del servidor:', data.error);
                    document.getElementById('piezas-container').innerHTML = `<p class="error">Error: ${data.error}</p>`;
                } else if (data.piezas && data.piezas.length > 0) {
                    let html = '<table class="piezas-table"><thead><tr>';
                    html += '<th style="width: 40px;">✓</th>';
                    html += '<th style="width: 100px;">Posición</th>';
                    html += '<th style="width: 140px;">Total</th>';
                    html += '<th style="width: 100px;">A Enviar</th>';
                    html += '<th style="width: 120px;">Perfil</th>';
                    html += '<th style="width: 100px;">Peso</th>';
                    html += '<th style="width: 200px;">Descripción</th>';
                    html += '<th style="width: 250px;">Observaciones</th>';
                    html += '</tr></thead><tbody>';
                    
                    data.piezas.forEach(pieza => {
                        const cantidadTotal = parseInt(parseFloat(pieza.cantidad) || 0);
                        html += `<tr class="pieza-row">
                            <td><input type="checkbox" name="piezas" value="${pieza.id}" checked></td>
                            <td><strong>${pieza.posicion}</strong></td>
                            <td class="cantidad-info">${cantidadTotal} unidades</td>
                            <td><input type="number" name="cant_${pieza.id}" class="cantidad-input" value="${cantidadTotal}" min="0" max="${cantidadTotal}" placeholder="0"></td>
                            <td class="perfil-cell">${pieza.perfil}</td>
                            <td class="peso-cell">${pieza.peso}</td>
                            <td class="descripcion-cell">${pieza.descripcion}</td>
                            <td><textarea name="obs_${pieza.id}" placeholder="Observaciones..."></textarea></td>
                        </tr>`;
                    });
                    
                    html += '</tbody></table>';
                    document.getElementById('piezas-container').innerHTML = html;
                } else {
                    document.getElementById('piezas-container').innerHTML = '<p class="error">No hay piezas disponibles</p>';
                }
            })
            .catch(err => {
                console.error('Error cargando piezas:', err);
                document.getElementById('piezas-container').innerHTML = `<p class="error">Error: ${err.message}</p>`;
            });
    }

    renumerarArticulosManual();
    </script>
    </body>
    </html>
    """
    html = html.replace("__OTS_POR_OBRA_JSON__", ots_por_obra_json)
    return html


@remito_bp.route("/api/piezas-remito/<int:ot_id>", methods=["GET"])
def api_piezas_remito(ot_id):
    try:
        db = get_db()
        ot = db.execute(
            "SELECT TRIM(COALESCE(obra, '')) FROM ordenes_trabajo WHERE id = ?",
            (ot_id,)
        ).fetchone()

        if not ot:
            return jsonify({"error": "OT no encontrada", "piezas": []}), 404

        obra_ot = (ot[0] or "").strip()
        if not obra_ot:
            return jsonify({"piezas": []}), 200

        piezas_por_ot = db.execute("""
            SELECT p_despacho.id,
                   p_first.posicion,
                   p_first.obra,
                   COALESCE(p_first.cantidad, ''),
                   COALESCE(p_first.perfil, ''),
                   COALESCE(p_first.peso, ''),
                   COALESCE(p_first.descripcion, '')
            FROM procesos p_despacho
            LEFT JOIN procesos p_first ON p_despacho.posicion = p_first.posicion 
                                       AND p_despacho.obra = p_first.obra
                                       AND p_first.id = (
                                           SELECT MIN(id) FROM procesos 
                                           WHERE posicion = p_despacho.posicion 
                                           AND obra = p_despacho.obra
                                       )
            WHERE p_despacho.ot_id = ?
              AND p_despacho.proceso = 'P/DESPACHO'
              AND UPPER(TRIM(COALESCE(p_despacho.estado, ''))) = 'OK'
              AND UPPER(TRIM(COALESCE(p_despacho.estado_pieza, ''))) != 'DESPACHADO'
        """, (ot_id,)).fetchall()

        piezas_por_obra = db.execute("""
            SELECT p_despacho.id,
                   p_first.posicion,
                   p_first.obra,
                   COALESCE(p_first.cantidad, ''),
                   COALESCE(p_first.perfil, ''),
                   COALESCE(p_first.peso, ''),
                   COALESCE(p_first.descripcion, '')
            FROM procesos p_despacho
            LEFT JOIN procesos p_first ON p_despacho.posicion = p_first.posicion
                                       AND p_despacho.obra = p_first.obra
                                       AND p_first.id = (
                                           SELECT MIN(id) FROM procesos
                                           WHERE posicion = p_despacho.posicion
                                           AND obra = p_despacho.obra
                                       )
            WHERE TRIM(COALESCE(p_despacho.obra, '')) = ?
              AND (p_despacho.ot_id IS NULL OR p_despacho.ot_id = ?)
              AND p_despacho.proceso = 'P/DESPACHO'
              AND UPPER(TRIM(COALESCE(p_despacho.estado, ''))) = 'OK'
              AND UPPER(TRIM(COALESCE(p_despacho.estado_pieza, ''))) != 'DESPACHADO'
        """, (obra_ot, ot_id)).fetchall()

        ids_vistos = {p[0] for p in piezas_por_ot}
        piezas = list(piezas_por_ot) + [p for p in piezas_por_obra if p[0] not in ids_vistos]

        def natural_key_posicion(valor):
            texto = str(valor or "").strip().upper()
            return [int(part) if part.isdigit() else part for part in re.split(r"(\d+)", texto)]

        piezas = sorted(piezas, key=lambda fila: natural_key_posicion(fila[1]))

        piezas_list = []
        for p in piezas:
            pieza_id = p[0]
            posicion = str(p[1]) if p[1] else ''
            obra = str(p[2]) if p[2] else ''
            cantidad = str(p[3]) if p[3] else ''
            perfil = str(p[4]) if p[4] else ''
            peso = str(p[5]) if p[5] else ''
            descripcion = str(p[6]) if p[6] else ''

            piezas_list.append({
                "id": pieza_id,
                "posicion": posicion,
                "obra": obra,
                "cantidad": cantidad,
                "perfil": perfil,
                "peso": peso,
                "descripcion": descripcion,
                "nombre": f"{posicion} - {descripcion}".strip()
            })

        return jsonify({"piezas": piezas_list}), 200
    except Exception as e:
        return jsonify({"error": str(e), "piezas": []}), 500


@remito_bp.route("/eliminar-remito/<int:remito_id>", methods=["GET"])
def eliminar_remito(remito_id):
    try:
        db = get_db()
        row = db.execute("SELECT pdf_path FROM remitos WHERE id = ?", (remito_id,)).fetchone()
        if not row:
            return "Remito no encontrado", 404

        pdf_stored = row[0] or ""
        pdf_name = os.path.basename(pdf_stored)
        pdf_full_path = os.path.join(_REMITOS_DIR, pdf_name)

        db.execute("DELETE FROM remitos WHERE id = ?", (remito_id,))
        db.commit()

        if pdf_name and os.path.exists(pdf_full_path):
            os.remove(pdf_full_path)

        return redirect("/modulo/remito")
    except Exception as e:
        return f"Error eliminando remito: {str(e)}", 500


@remito_bp.route("/descargar-remito/<filename>")
def descargar_remito(filename):
    try:
        filepath = os.path.join(_REMITOS_DIR, filename)
        if not os.path.exists(filepath):
            return "Remito no encontrado", 404
        return send_file(filepath, as_attachment=True, download_name=filename)
    except Exception as e:
        return f"Error descargando remito: {str(e)}", 500
