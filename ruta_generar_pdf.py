# Insertara antes de "# ======================" comentario (RUTA GENERAR PDF DESDE CONTROL)
@app.route("/modulo/calidad/escaneo/generar-pdf-control/<int:control_id>", methods=["GET"])
def generar_pdf_control(control_id):
    from datetime import date
    
    db = get_db()
    
    # Obtener control
    ctrl_row = db.execute(
        "SELECT id, obra, mediciones, piezas FROM control_pintura WHERE id=? AND estado='activo'",
        (control_id,)
    ).fetchone()
    
    if not ctrl_row:
        return "Control no encontrado", 404
    
    ctrl_id, obra, mediciones_json, piezas_json = ctrl_row
    mediciones = json.loads(mediciones_json) if mediciones_json else []
    filas_pintura = json.loads(piezas_json) if piezas_json else []
    
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
    from reportlab.lib.pagesizes import landscape, letter
    
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
    title_style = ParagraphStyle('TitleP', parent=styles['Heading2'], fontSize=14, textColor=colors.HexColor('#111827'))
    
    elements = []
    elements.append(Paragraph("<b>CONTROL DE PINTURA</b>", title_style))
    elements.append(Spacer(1, 0.2 * cm))
    
    info = Table([
        [Paragraph(f"<b>Obra:</b> {obra}", base_style), 
         Paragraph(f"<b>Fecha reporte:</b> {date.today().isoformat()}", base_style)],
    ], colWidths=[13.4 * cm, 13.4 * cm])
    
    elements.append(info)
    elements.append(Spacer(1, 0.2 * cm))
    
    # Tabla Temperatura y Humedad
    elements.append(Paragraph("<b>1) Temperatura y Humedad</b>", ParagraphStyle('Sec1', parent=styles['Normal'], fontSize=9, textColor=colors.HexColor('#0c4a6e'))))
    elements.append(Spacer(1, 0.08 * cm))
    
    med_table_data = [
        [Paragraph(f"<b>Mano</b>", head_style),
         Paragraph(f"<b>Fecha</b>", head_style),
         Paragraph(f"<b>Hora</b>", head_style),
         Paragraph(f"<b>Temperatura (°C)</b>", head_style),
         Paragraph(f"<b>Humedad (%)</b>", head_style)]
    ]
    for m in mediciones:
        med_table_data.append([
            Paragraph(f"Mano {m['mano']}", base_style),
            Paragraph(m['fecha'] or "-", base_style),
            Paragraph(m['hora'] or "-", base_style),
            Paragraph(m['temp'] or "-", base_style),
            Paragraph(m['humedad'] or "-", base_style),
        ])
    
    med_table = Table(med_table_data, colWidths=[2.6 * cm, 2.8 * cm, 2.8 * cm, 3.5 * cm, 2.5 * cm])
    med_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0ea5e9')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 7.5),
        ('FONTSIZE', (0, 1), (-1, -1), 6.8),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#d1d5db')),
        ('LEFTPADDING', (0, 0), (-1, -1), 3),
        ('RIGHTPADDING', (0, 0), (-1, -1), 3),
        ('TOPPADDING', (0, 0), (-1, -1), 3),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
    ]))
    
    elements.append(med_table)
    elements.append(Spacer(1, 0.15 * cm))
    
    # Tabla Piezas y Pintura
    elements.append(Paragraph("<b>2) Estado de Superficie y Manos de Pintura</b>", ParagraphStyle('Sec2', parent=styles['Normal'], fontSize=9, textColor=colors.HexColor('#9a3412'))))
    elements.append(Spacer(1, 0.08 * cm))
    
    pie_table_data = [
        [Paragraph("<b>Pieza</b>", head_style),
         Paragraph("<b>Cant.</b>", head_style),
         Paragraph("<b>Descripción</b>", head_style),
         Paragraph("<b>Estado</b>", head_style),
         Paragraph("<b>Responsable</b>", head_style),
         Paragraph("<b>Firma</b>", head_style),
         Paragraph("<b>Mano 1</b>", head_style),
         Paragraph("<b>Mano 2</b>", head_style),
         Paragraph("<b>Mano 3</b>", head_style),
         Paragraph("<b>Mano 4</b>", head_style),
         Paragraph("<b>Espesor Solic.</b>", head_style),
         Paragraph("<b>Est. Final</b>", head_style),
         Paragraph("<b>Responsable</b>", head_style),
         Paragraph("<b>Firma</b>", head_style)]
    ]
    
    if filas_pintura:
        for r in filas_pintura:
            pie_table_data.append([
                Paragraph(r["pieza"], base_style),
                Paragraph(r["cantidad"] or "-", base_style),
                Paragraph(r["descripcion"] or "-", base_style),
                Paragraph(r["sup_estado"] or "-", base_style),
                Paragraph(r["sup_resp"] or "-", base_style),
                Paragraph(r["sup_firma"] or "-", base_style),
                Paragraph(f"{r['mano1']:.2f}", base_style),
                Paragraph(f"{r['mano2']:.2f}", base_style),
                Paragraph(f"{r['mano3']:.2f}", base_style),
                Paragraph(f"{r['mano4']:.2f}", base_style),
                Paragraph(f"{r['espesor']:.2f}", base_style),
                Paragraph(r["estado_final"], base_style),
                Paragraph(r["pint_resp"] or "-", base_style),
                Paragraph(r["pint_firma"] or "-", base_style),
            ])
    
    pie_table = Table(pie_table_data, colWidths=[1.5*cm]*14)
    pie_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f97316')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 6.8),
        ('FONTSIZE', (0, 1), (-1, -1), 6.5),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#d1d5db')),
        ('LEFTPADDING', (0, 0), (-1, -1), 2),
        ('RIGHTPADDING', (0, 0), (-1, -1), 2),
        ('TOPPADDING', (0, 0), (-1, -1), 2),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
    ]))
    
    elements.append(pie_table)
    
    doc.build(elements)
    pdf_buffer.seek(0)
    filename = f"Control_Pintura_{obra}_ID{control_id}_{date.today().isoformat()}.pdf".replace(" ", "_")
    pdf_buffer.seek(0)
    return send_file(pdf_buffer, mimetype='application/pdf', as_attachment=True, download_name=filename)
