# NUEVA RUTA: LISTAR CONTROLES DE PINTURA
@app.route("/modulo/calidad/escaneo/controles-pintura", methods=["GET"])
def listar_controles_pintura():
    db = get_db()
    
    obra_filtro = (request.args.get("obra") or "").strip()
    
    query = "SELECT id, obra, fecha_creacion, fecha_modificacion FROM control_pintura WHERE estado='activo'"
    params = []
    
    if obra_filtro:
        query += " AND TRIM(COALESCE(obra, '')) = TRIM(?)"
        params.append(obra_filtro)
    
    query += " ORDER BY fecha_creacion DESC LIMIT 100"
    
    controles = db.execute(query, params).fetchall()
    
    # Obtener lista de obras únicas
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
    .actions {{ display:flex; gap:10px; flex-wrap:wrap; margin-top:12px; }}
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
                <button type="submit" class="btn">🔍 Filtrar</button>
            </div>
        </form>
    </div>

    <div class="box">
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
    </div>

    <div class="actions">
        <a href="/modulo/calidad/escaneo/form-pintura" class="btn btn-blue">➕ Nuevo Control</a>
        <a href="/modulo/calidad/escaneo" class="btn btn-blue">⬅️ Volver a Sub Módulos</a>
    </div>
    </body>
    </html>
    """
    return html
