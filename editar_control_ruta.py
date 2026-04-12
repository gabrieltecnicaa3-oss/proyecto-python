@app.route("/modulo/calidad/escaneo/editar-control-pintura/<int:control_id>", methods=["GET", "POST"])
def editar_control_pintura(control_id):
    from datetime import date
    db = get_db()
    ctrl_row = db.execute("SELECT id, obra, mediciones, piezas FROM control_pintura WHERE id=? AND estado='activo'", (control_id,)).fetchone()
    if not ctrl_row: return "Control no encontrado", 404
    ctrl_id, obra, mediciones_json, piezas_json = ctrl_row
    mediciones = json.loads(mediciones_json) if mediciones_json else []
    filas_pintura = json.loads(piezas_json) if piezas_json else []
    responsables_control = _obtener_responsables_control(db)
    firmas_responsables = {k: v.get("firma", "") for k, v in responsables_control.items()}
    
    if request.method == "POST":
        accion = (request.form.get("accion") or "").strip().lower()
        if accion == "pdf":
            def _to_float(val):
                txt = str(val or "").strip().replace(",", ".")
                if not txt: return 0.0
                try: return float(txt)
                except: return 0.0
            
            piezas_form = request.form.getlist("pieza[]")
            cantidades_form = request.form.getlist("cantidad[]")
            desc_form = request.form.getlist("descripcion[]")
            sup_estado_form = request.form.getlist("sup_estado[]")
            sup_resp_form = request.form.getlist("sup_responsable[]")
            sup_firma_form = request.form.getlist("sup_firma[]")
            mano1_form = request.form.getlist("mano1[]")
            mano2_form = request.form.getlist("mano2[]")
            mano3_form = request.form.getlist("mano3[]")
            mano4_form = request.form.getlist("mano4[]")
            espesor_form = request.form.getlist("espesor_solicitado[]")
            pint_resp_form = request.form.getlist("pintura_responsable[]")
            pint_firma_form = request.form.getlist("pintura_firma[]")
            
            filas_pintura_nuevas = []
            for i in range(len(piezas_form)):
                pieza = (piezas_form[i] if i < len(piezas_form) else "").strip()
                if not pieza: continue
                filas_pintura_nuevas.append({
                    "pieza": pieza,
                    "cantidad": (cantidades_form[i] if i < len(cantidades_form) else "").strip(),
                    "descripcion": (desc_form[i] if i < len(desc_form) else "").strip(),
                    "sup_estado": (sup_estado_form[i] if i < len(sup_estado_form) else "").strip().upper(),
                    "sup_resp": (sup_resp_form[i] if i < len(sup_resp_form) else "").strip(),
                    "sup_firma": (sup_firma_form[i] if i < len(sup_firma_form) else "").strip(),
                    "mano1": _to_float(mano1_form[i] if i < len(mano1_form) else ""),
                    "mano2": _to_float(mano2_form[i] if i < len(mano2_form) else ""),
                    "mano3": _to_float(mano3_form[i] if i < len(mano3_form) else ""),
                    "mano4": _to_float(mano4_form[i] if i < len(mano4_form) else ""),
                    "espesor": _to_float(espesor_form[i] if i < len(espesor_form) else ""),
                    "estado_final": "OK" if _to_float(mano4_form[i] if i < len(mano4_form) else "") > _to_float(espesor_form[i] if i < len(espesor_form) else "") else "NO CONFORME",
                    "pint_resp": (pint_resp_form[i] if i < len(pint_resp_form) else "").strip(),
                    "pint_firma": (pint_firma_form[i] if i < len(pint_firma_form) else "").strip(),
                })
            
            med_fechas = request.form.getlist("med_fecha[]")
            med_horas = request.form.getlist("med_hora[]")
            med_temps = request.form.getlist("med_temp[]")
            med_humedades = request.form.getlist("med_humedad[]")
            
            mediciones_nuevas = []
            for i in range(max(len(med_fechas), len(med_horas), len(med_temps), len(med_humedades))):
                fecha_m = (med_fechas[i] if i < len(med_fechas) else "").strip()
                hora_m = (med_horas[i] if i < len(med_horas) else "").strip()
                temp_m = (med_temps[i] if i < len(med_temps) else "").strip()
                hum_m = (med_humedades[i] if i < len(med_humedades) else "").strip()
                if not (fecha_m or hora_m or temp_m or hum_m): continue
                mediciones_nuevas.append({"mano": str(i+1), "fecha": fecha_m, "hora": hora_m, "temp": temp_m, "humedad": hum_m})
            
            db.execute("UPDATE control_pintura SET mediciones=?, piezas=?, fecha_modificacion=CURRENT_TIMESTAMP WHERE id=?", (json.dumps(mediciones_nuevas), json.dumps(filas_pintura_nuevas), control_id))
            db.commit()
            
            from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
            from reportlab.lib.pagesizes import landscape, letter
            pdf_buffer = BytesIO()
            doc = SimpleDocTemplate(pdf_buffer, pagesize=landscape(letter), topMargin=0.5*cm, bottomMargin=0.6*cm, leftMargin=0.6*cm, rightMargin=0.6*cm)
            styles = getSampleStyleSheet()
            base_style = ParagraphStyle('BaseP', parent=styles['Normal'], fontSize=7.2, leading=8.4, textColor=colors.HexColor('#1f2937'))
            head_style = ParagraphStyle('HeadP', parent=styles['Normal'], fontSize=7.1, leading=8.2, alignment=1, fontName='Helvetica-Bold', textColor=colors.white)
            title_style = ParagraphStyle('TitleP', parent=styles['Heading2'], fontSize=14, textColor=colors.HexColor('#111827'))
            elements = [Paragraph("<b>CONTROL DE PINTURA (EDITADO)</b>", title_style), Spacer(1, 0.2*cm)]
            info = Table([[Paragraph(f"<b>Obra:</b> {obra}", base_style), Paragraph(f"<b>Fecha reporte:</b> {date.today().isoformat()}", base_style)]], colWidths=[13.4*cm, 13.4*cm])
            elements.append(info)
            elements.append(Spacer(1, 0.2*cm))
            elements.append(Paragraph("<b>1) Temperatura y Humedad</b>", ParagraphStyle('Sec1', parent=styles['Normal'], fontSize=9, textColor=colors.HexColor('#0c4a6e'))))
            elements.append(Spacer(1, 0.08*cm))
            med_table_data = [[Paragraph(f"<b>Mano</b>", head_style), Paragraph(f"<b>Fecha</b>", head_style), Paragraph(f"<b>Hora</b>", head_style), Paragraph(f"<b>Temperatura (°C)</b>", head_style), Paragraph(f"<b>Humedad (%)</b>", head_style)]]
            for m in mediciones_nuevas: med_table_data.append([Paragraph(f"Mano {m['mano']}", base_style), Paragraph(m['fecha'] or "-", base_style), Paragraph(m['hora'] or "-", base_style), Paragraph(m['temp'] or "-", base_style), Paragraph(m['humedad'] or "-", base_style)])
            med_table = Table(med_table_data, colWidths=[2.6*cm, 2.8*cm, 2.8*cm, 3.5*cm, 2.5*cm])
            med_table.setStyle(TableStyle([('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0ea5e9')), ('TEXTCOLOR', (0, 0), (-1, 0), colors.white), ('ALIGN', (0, 0), (-1, -1), 'CENTER'), ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'), ('FONTSIZE', (0, 0), (-1, 0), 7.5), ('FONTSIZE', (0, 1), (-1, -1), 6.8), ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#d1d5db')), ('LEFTPADDING', (0, 0), (-1, -1), 3), ('RIGHTPADDING', (0, 0), (-1, -1), 3), ('TOPPADDING', (0, 0), (-1, -1), 3), ('BOTTOMPADDING', (0, 0), (-1, -1), 3)]))
            elements.append(med_table)
            elements.append(Spacer(1, 0.15*cm))
            elements.append(Paragraph("<b>2) Estado de Superficie y Manos de Pintura</b>", ParagraphStyle('Sec2', parent=styles['Normal'], fontSize=9, textColor=colors.HexColor('#9a3412'))))
            elements.append(Spacer(1, 0.08*cm))
            pie_table_data = [[Paragraph("<b>Pieza</b>", head_style), Paragraph("<b>Cant.</b>", head_style), Paragraph("<b>Descripción</b>", head_style), Paragraph("<b>Estado</b>", head_style), Paragraph("<b>Responsable</b>", head_style), Paragraph("<b>Firma</b>", head_style), Paragraph("<b>Mano 1</b>", head_style), Paragraph("<b>Mano 2</b>", head_style), Paragraph("<b>Mano 3</b>", head_style), Paragraph("<b>Mano 4</b>", head_style), Paragraph("<b>Espesor Solic.</b>", head_style), Paragraph("<b>Est. Final</b>", head_style), Paragraph("<b>Responsable</b>", head_style), Paragraph("<b>Firma</b>", head_style)]]
            for r in filas_pintura_nuevas: pie_table_data.append([Paragraph(r["pieza"], base_style), Paragraph(r["cantidad"] or "-", base_style), Paragraph(r["descripcion"] or "-", base_style), Paragraph(r["sup_estado"] or "-", base_style), Paragraph(r["sup_resp"] or "-", base_style), Paragraph(r["sup_firma"] or "-", base_style), Paragraph(f"{r['mano1']:.2f}", base_style), Paragraph(f"{r['mano2']:.2f}", base_style), Paragraph(f"{r['mano3']:.2f}", base_style), Paragraph(f"{r['mano4']:.2f}", base_style), Paragraph(f"{r['espesor']:.2f}", base_style), Paragraph(r["estado_final"], base_style), Paragraph(r["pint_resp"] or "-", base_style), Paragraph(r["pint_firma"] or "-", base_style)])
            pie_table = Table(pie_table_data, colWidths=[1.5*cm]*14)
            pie_table.setStyle(TableStyle([('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f97316')), ('TEXTCOLOR', (0, 0), (-1, 0), colors.white), ('ALIGN', (0, 0), (-1, -1), 'CENTER'), ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'), ('FONTSIZE', (0, 0), (-1, 0), 6.8), ('FONTSIZE', (0, 1), (-1, -1), 6.5), ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#d1d5db')), ('LEFTPADDING', (0, 0), (-1, -1), 2), ('RIGHTPADDING', (0, 0), (-1, -1), 2), ('TOPPADDING', (0, 0), (-1, -1), 2), ('BOTTOMPADDING', (0, 0), (-1, -1), 2)]))
            elements.append(pie_table)
            doc.build(elements)
            pdf_buffer.seek(0)
            filename = f"Control_Pintura_{obra}_ID{control_id}_EDITADO_{date.today().isoformat()}.pdf".replace(" ", "_")
            return send_file(pdf_buffer, mimetype='application/pdf', as_attachment=True, download_name=filename)
    
    opciones_responsables = '<option value="">Seleccionar...</option>' + "".join(f'<option value="{html_lib.escape(nombre)}">{html_lib.escape(nombre)}</option>' for nombre in sorted(responsables_control.keys()))
    
    mediciones_html = ""
    for i in range(1, 5):
        med = mediciones[i-1] if i-1 < len(mediciones) else {}
        mediciones_html += f"""<tr>
            <td><b>Mano {i}</b></td>
            <td><input type="date" name="med_fecha[]" value="{med.get('fecha', '')}"></td>
            <td><input type="time" name="med_hora[]" value="{med.get('hora', '')}"></td>
            <td><input type="number" step="0.1" name="med_temp[]" value="{med.get('temp', '')}" placeholder="°C"></td>
            <td><input type="number" step="0.1" name="med_humedad[]" value="{med.get('humedad', '')}" placeholder="%"></td>
        </tr>"""
    
    piezas_rows_html = ""
    for idx, p in enumerate(filas_pintura, 1):
        piezas_rows_html += f"""<tr>
            <td><b>{html_lib.escape(p.get('pieza', ''))}</b><input type="hidden" name="pieza[]" value="{html_lib.escape(p.get('pieza', ''))}"></td>
            <td>{html_lib.escape(p.get('cantidad', ''))}<input type="hidden" name="cantidad[]" value="{html_lib.escape(p.get('cantidad', ''))}"></td>
            <td>{html_lib.escape(p.get('descripcion', ''))}<input type="hidden" name="descripcion[]" value="{html_lib.escape(p.get('descripcion', ''))}"></td>
            <td><select name="sup_estado[]"><option value="">Sel...</option><option value="CONFORME" {'selected' if p.get('sup_estado') == 'CONFORME' else ''}>Conforme</option><option value="NO CONFORME" {'selected' if p.get('sup_estado') == 'NO CONFORME' else ''}>No Conforme</option><option value="NO APLICA" {'selected' if p.get('sup_estado') == 'NO APLICA' else ''}>No Aplica</option></select></td>
            <td><select name="sup_responsable[]" class="sup-resp" data-idx="{idx}">{opciones_responsables}</select></td>
            <td><input type="text" name="sup_firma[]" id="sup-firma-{idx}" value="{html_lib.escape(p.get('sup_firma', ''))}" readonly></td>
            <td><input type="number" step="0.01" name="mano1[]" class="mano1" data-idx="{idx}" value="{p.get('mano1', 0)}"></td>
            <td><input type="number" step="0.01" name="mano2[]" class="mano2" data-idx="{idx}" value="{p.get('mano2', 0)}"></td>
            <td><input type="number" step="0.01" name="mano3[]" class="mano3" data-idx="{idx}" value="{p.get('mano3', 0)}"></td>
            <td><input type="number" step="0.01" name="mano4[]" class="mano4" data-idx="{idx}" value="{p.get('mano4', 0)}"></td>
            <td><input type="number" step="0.01" name="espesor_solicitado[]" class="espesor" data-idx="{idx}" value="{p.get('espesor', 0)}"></td>
            <td><input type="text" class="estado-final" id="estado-final-{idx}" value="{p.get('estado_final', '')}" readonly></td>
            <td><select name="pintura_responsable[]" class="pint-resp" data-idx="{idx}">{opciones_responsables}</select></td>
            <td><input type="text" name="pintura_firma[]" id="pint-firma-{idx}" value="{html_lib.escape(p.get('pint_firma', ''))}" readonly></td>
        </tr>"""
    
    html = f"""
    <html><head><meta name="viewport" content="width=device-width, initial-scale=1"><style>
    body{{font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0;}}
    h2{{color: #9a3412; border-bottom: 3px solid #f97316; padding-bottom: 10px;}}
    table{{width:100%; border-collapse: collapse; background:white;}}
    th, td{{border-bottom:1px solid #e5e7eb; padding:8px; font-size:11px;}}
    th{{background:#f97316; color:white;}}
    input, select{{width:100%; padding:6px; border:1px solid #d1d5db; border-radius:4px; box-sizing:border-box; font-size:11px;}}
    .btn{{background:#f97316; color:white; border:none; padding:8px 12px; border-radius:4px; font-weight:bold; cursor:pointer;}}
    .btn-blue{{background:#2563eb;}}
    </style></head><body>
    <h2>✏️ Editar Control de Pintura (ID: {control_id})</h2>
    <form method="post">
        <input type="hidden" name="accion" value="pdf">
        <table style="margin-bottom:10px;">
            <tr><th style="width:12%; background:#0ea5e9;">Mano</th><th style="background:#0ea5e9;">Fecha</th><th style="background:#0ea5e9;">Hora</th><th style="background:#0ea5e9;">Temp (°C)</th><th style="background:#0ea5e9;">Humedad (%)</th></tr>
            {mediciones_html}
        </table>
        <table>
            <tr><th colspan="3">Pieza</th><th colspan="3">Control Superficie</th><th colspan="8">Control Pintura</th></tr>
            <tr><th>Pieza</th><th>Cant.</th><th>Desc</th><th>Estado</th><th>Resp</th><th>Firma</th><th>M1</th><th>M2</th><th>M3</th><th>M4</th><th>Espesor</th><th>Est.Final</th><th>Resp</th><th>Firma</th></tr>
            {piezas_rows_html}
        </table>
        <br>
        <button type="submit" class="btn">📄 Generar PDF Actualizado</button>
        <a href="/modulo/calidad/escaneo/controles-pintura" class="btn btn-blue">⬅️ Volver</a>
    </form>
    <script>
    const firmas = {json.dumps(firmas_responsables, ensure_ascii=False)};
    function updateFirma(sel, id){{ document.getElementById(id).value = firmas[sel.value] || ''; }}
    document.querySelectorAll('.sup-resp').forEach(s=> s.addEventListener('change', ()=>updateFirma(s, 'sup-firma-'+s.dataset.idx)));
    document.querySelectorAll('.pint-resp').forEach(s=> s.addEventListener('change', ()=>updateFirma(s, 'pint-firma-'+s.dataset.idx)));
    function updateEstado(idx){{
        const m4 = parseFloat(document.querySelector('.mano4[data-idx="'+idx+'"]').value) || 0;
        const esp = parseFloat(document.querySelector('.espesor[data-idx="'+idx+'"]').value) || 0;
        const el = document.getElementById('estado-final-'+idx);
        el.value = m4 > esp ? 'OK' : 'NO CONFORME';
    }}
    document.querySelectorAll('.mano4, .espesor').forEach(i=>i.addEventListener('input', ()=>updateEstado(i.dataset.idx)));
    </script>
    </body></html>
    """
    return html
