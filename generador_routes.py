import os
import tempfile
from io import BytesIO
from urllib.parse import quote, urlencode

# Lazy import: import pandas as pd (se importa dentro de funciones que lo necesitan)
import qrcode
from flask import Blueprint, request, send_file
from reportlab.lib import colors
from reportlab.lib.pagesizes import A3
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    Image,
    KeepInFrame,
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from db_utils import (
    get_db,
    _asegurar_estructura_databook_si_valida as _db_asegurar_estructura_databook_si_valida,
)
from qr_utils import clean_xls as _clean_xls
from qr_utils import find_col, load_clean_excel, upsert_piezas_desde_excel

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


def _asegurar_estructura_databook_si_valida(obra):
    return _db_asegurar_estructura_databook_si_valida(obra, _DATABOOKS_DIR, _DATABOOK_SECCIONES)


generador_bp = Blueprint("generador", __name__)


# ======================
# FUNCIONES GENERADOR QR
# ======================
def generar_etiquetas_qr(excel_file, logo_path, cargar_bd_excel=False):
    """Genera PDF con etiquetas A3 y QR codes"""
    import pandas as pd
    try:
        df = load_clean_excel(excel_file)

        col_pos = find_col(df, "POS")
        plano_col = find_col(df, "PLANO")
        rev_col = find_col(df, "REV")
        obra_col = find_col(df, "OBRA")
        cant_col = find_col(df, "CANT")
        perfil_col = find_col(df, "PERFIL")
        peso_col = find_col(df, "PESO")
        desc_col = find_col(df, "DESCRIP")

        print("\n[DEBUG] Columnas encontradas:")
        print(f"  POS: {col_pos}")
        print(f"  OBRA: {obra_col}")
        print(f"  CANT: {cant_col}")
        print(f"  PERFIL: {perfil_col}")
        print(f"  Filas a procesar: {len(df)}")

        if cargar_bd_excel:
            db = get_db()
            saved_count = upsert_piezas_desde_excel(
                db,
                df,
                col_pos,
                obra_col,
                cant_col,
                perfil_col,
                peso_col,
                desc_col,
                asegurar_databook_si_valida=_asegurar_estructura_databook_si_valida,
            )
            print(f"[DEBUG] Modo anterior activo: {saved_count} fila(s) sincronizadas desde Excel")
        else:
            print("[DEBUG] Carga de BD desde Excel desactivada: solo se registra al escanear QR")

        styles = getSampleStyleSheet()
        label_style = ParagraphStyle(
            "LabelStyle",
            parent=styles["Normal"],
            fontSize=10.5,
            leading=11.5,
            alignment=1,
        )

        qr_temp_dir = tempfile.mkdtemp()

        cols = 6
        rows_per_page = 5
        prefijos_expandibles = ["V", "C", "PU", "INS"]
        prefijos_duplicar_igual = ["A", "T"]
        rows_expandidas = []

        for _, row in df.iterrows():
            pos = str(row.get(col_pos, "")).strip()
            pos_upper = pos.upper()
            cant_str = str(row.get(cant_col, "0")).split(".")[0]

            try:
                cant = int(cant_str) if cant_str else 1
            except Exception:
                cant = 1

            es_expandible = any(pos_upper.startswith(prefijo) for prefijo in prefijos_expandibles)
            es_duplicar_igual = any(pos_upper.startswith(prefijo) for prefijo in prefijos_duplicar_igual)
            es_excluido_ti_to = pos_upper.startswith("TI") or pos_upper.startswith("TO")

            if es_expandible and cant > 1:
                for num in range(1, cant + 1):
                    row_copia = row.copy()
                    nuevo_pos = f"{pos}-{num}"
                    row_copia[col_pos] = nuevo_pos
                    rows_expandidas.append(row_copia)
            elif es_duplicar_igual and not es_excluido_ti_to and cant > 1:
                for _ in range(cant):
                    rows_expandidas.append(row.copy())
            else:
                rows_expandidas.append(row)

        df_expandido = pd.DataFrame(rows_expandidas).reset_index(drop=True)

        total_items = len(df_expandido)
        items_per_page = cols * rows_per_page
        num_pages = (total_items + items_per_page - 1) // items_per_page

        pdf_buffer = BytesIO()
        doc = SimpleDocTemplate(
            pdf_buffer,
            pagesize=A3,
            topMargin=2 * mm,
            bottomMargin=2 * mm,
            leftMargin=2 * mm,
            rightMargin=2 * mm,
        )

        elements = []
        i = 0

        for page in range(num_pages):
            data = []
            for _ in range(rows_per_page):
                row_data = []
                for _ in range(cols):
                    if i < len(df_expandido):
                        row = df_expandido.iloc[i]
                        pos = _clean_xls(row.get(col_pos, ""))
                        plano = _clean_xls(row.get(plano_col, ""))
                        rev = _clean_xls(row.get(rev_col, ""))
                        obra = _clean_xls(row.get(obra_col, ""))
                        cant = _clean_xls(row.get(cant_col, ""))
                        perfil = _clean_xls(row.get(perfil_col, ""))
                        peso = _clean_xls(row.get(peso_col, ""))
                        desc = _clean_xls(row.get(desc_col, ""))

                        qr_params = {}
                        if obra:
                            qr_params["obra"] = obra
                        if cant:
                            qr_params["cant"] = cant
                        if perfil:
                            qr_params["perfil"] = perfil
                        if peso:
                            qr_params["peso"] = peso

                        qr_text = f"https://web-production-5edf5c.up.railway.app/pieza/{quote(pos)}"
                        if qr_params:
                            qr_text += f"?{urlencode(qr_params)}"
                        qr_path = f"{qr_temp_dir}/qr_{i}.png"

                        qr = qrcode.QRCode(
                            version=None,
                            error_correction=qrcode.constants.ERROR_CORRECT_M,
                            box_size=10,
                            border=1,
                        )
                        qr.add_data(qr_text)
                        qr.make(fit=True)
                        img = qr.make_image(fill_color="black", back_color="white")
                        img.save(qr_path)

                        desc_corta = (desc[:55] + "...") if desc and len(desc) > 55 else desc

                        text = f"""
                        <font size="12"><b>OBRA:</b> {obra}</font><br/>
                        <font size="12"><b>POS:</b> {pos}</font><br/>
                        <font size="12"><b>CANT:</b> {cant}</font><br/><br/>
                        <font size="9"><b>PERFIL:</b> {perfil}</font><br/>
                        <font size="9"><b>PESO:</b> {peso}</font><br/>
                        <font size="8">{desc_corta}</font>
                        """

                        separador = Spacer(1, 2.0 * mm)

                        content = [
                            Spacer(1, 1.8 * mm),
                            Image(logo_path, width=20 * mm, height=16 * mm),
                            Paragraph(text, label_style),
                            separador,
                            Image(qr_path, width=30 * mm, height=30 * mm),
                        ]

                        content_fit = KeepInFrame(43 * mm, 78 * mm, content, mode="shrink", hAlign="CENTER", vAlign="TOP")

                        row_data.append(content_fit)
                        i += 1
                    else:
                        row_data.append("")

                data.append(row_data)

            has_content = any(any(cell != "" for cell in row) for row in data)
            if has_content:
                table = Table(data, colWidths=45 * mm, rowHeights=80 * mm)
                table.setStyle(TableStyle([
                    ("GRID", (0, 0), (-1, -1), 0.5, colors.black),
                    ("ALIGN", (0, 0), (-1, -1), "CENTER"),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#FFFFFF")),
                    ("LEFTPADDING", (0, 0), (-1, -1), 2),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 2),
                    ("TOPPADDING", (0, 0), (-1, -1), 0),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 0),
                ]))

                elements.append(table)

                if page < num_pages - 1:
                    elements.append(PageBreak())

        doc.build(elements)
        pdf_buffer.seek(0)

        import shutil

        shutil.rmtree(qr_temp_dir, ignore_errors=True)

        return pdf_buffer

    except Exception as e:
        raise Exception(f"Error generando QR: {str(e)}")


def _buscar_excels_en_produccion(obra):
    """Retorna lista de excels dentro de carpetas OT de la obra."""
    carpeta_obra = os.path.join(_DATABOOKS_DIR, obra)
    if not os.path.isdir(carpeta_obra):
        return []

    encontrados = []
    for carpeta_ot in sorted(os.listdir(carpeta_obra)):
        ruta_ot = os.path.join(carpeta_obra, carpeta_ot)
        if not os.path.isdir(ruta_ot):
            continue
        if not carpeta_ot.upper().startswith("OT "):
            continue
        for raiz, _, archivos in os.walk(ruta_ot):
            for nombre in sorted(archivos):
                if nombre.lower().endswith((".xls", ".xlsx")) and "armado" in nombre.lower():
                    encontrados.append((nombre, os.path.join(raiz, nombre)))
    return encontrados


@generador_bp.route("/modulo/generador", methods=["GET", "POST"])
def generador_qr_main():
    import html as html_lib
    from io import BytesIO

    db = get_db()
    error_html = ""

    # Obtener todas las OTs activas (sin filtro de filesystem)
    ots_all = db.execute(
        "SELECT id, obra, titulo FROM ordenes_trabajo WHERE fecha_cierre IS NULL ORDER BY id DESC"
    ).fetchall()

    opciones_ot = '<option value="">-- Seleccionar OT --</option>'
    for ot in ots_all:
        label = f"OT {ot[0]} | {ot[1] or ''} - {ot[2] or ''}"
        opciones_ot += f'<option value="{ot[0]}">{html_lib.escape(label)}</option>'

    # Procesar POST: se requiere OT y archivo Excel subido
    if request.method == "POST":
        ot_id_txt = (request.form.get("ot_id") or "").strip()
        excel_file = request.files.get("excel_file")

        if not ot_id_txt or not ot_id_txt.isdigit():
            error_html = '<div style="background:#fee2e2;border:1px solid #fca5a5;color:#b91c1c;padding:12px;border-radius:6px;margin-bottom:14px;">❌ Seleccioná una OT válida.</div>'
        elif not excel_file or excel_file.filename == "":
            error_html = '<div style="background:#fee2e2;border:1px solid #fca5a5;color:#b91c1c;padding:12px;border-radius:6px;margin-bottom:14px;">❌ Adjuntá el archivo Excel con las piezas.</div>'
        elif not excel_file.filename.lower().endswith((".xls", ".xlsx")):
            error_html = '<div style="background:#fee2e2;border:1px solid #fca5a5;color:#b91c1c;padding:12px;border-radius:6px;margin-bottom:14px;">❌ El archivo debe ser .xls o .xlsx.</div>'
        else:
            ot_id = int(ot_id_txt)
            nombre_obra = ""
            for ot in ots_all:
                if ot[0] == ot_id:
                    nombre_obra = ot[1] or ""
                    break
            try:
                import tempfile, os as _os
                suffix = ".xlsx" if excel_file.filename.lower().endswith(".xlsx") else ".xls"
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                    tmp_path = tmp.name
                    excel_file.save(tmp_path)
                try:
                    logo_path = _os.path.join(_APP_DIR, "LOGO.png")
                    pdf_buffer = generar_etiquetas_qr(tmp_path, logo_path)
                finally:
                    try:
                        _os.unlink(tmp_path)
                    except Exception:
                        pass
                nombre_archivo_obra = nombre_obra.replace(' ', '_').replace('/', '_').replace('\\', '_') if nombre_obra else "SIN_NOMBRE"
                pdf_filename = f"ETIQUETAS_A3_{nombre_archivo_obra}.pdf"
                return send_file(
                    pdf_buffer,
                    mimetype="application/pdf",
                    as_attachment=True,
                    download_name=pdf_filename,
                )
            except Exception as e:
                error_html = f'<div style="background:#fee2e2;border:1px solid #fca5a5;color:#b91c1c;padding:12px;border-radius:6px;margin-bottom:14px;">❌ Error al generar PDF: {html_lib.escape(str(e))}</div>'

    html = f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{ font-family: 'Segoe UI', sans-serif; padding: 20px; background: #f4f4f4; }}
    .container {{ max-width: 600px; margin: 0 auto; }}
    h1 {{ color: #333; margin-bottom: 18px; }}
    .card {{ background: white; padding: 22px; border-radius: 10px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); margin-bottom: 16px; }}
    label {{ display: block; margin-bottom: 8px; font-weight: bold; color: #333; }}
    select, input[type=file] {{ width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 6px; font-size: 14px; margin-bottom: 14px; }}
    .btn {{ display: block; width: 100%; padding: 12px; background: #43e97b; color: white;
            border: none; border-radius: 6px; font-weight: bold; font-size: 15px; cursor: pointer; margin-top: 14px; text-align:center; text-decoration:none; }}
    .btn:hover {{ background: #2cc96e; }}
    .btn-sec {{ background: #667eea; margin-top: 10px; }}
    .btn-sec:hover {{ background: #5568d3; }}
    .info {{ background: #eff6ff; border-left: 4px solid #3b82f6; padding: 12px; border-radius: 6px; font-size: 13px; color: #1e40af; margin-bottom: 16px; }}
    </style>
    </head>
    <body>
    <div class="container">
        <h1>🏷️ Generador de Etiquetas QR A3</h1>
        {error_html}
        <div class="info">Seleccioná la OT y subí el archivo Excel (.xls / .xlsx) con las piezas para generar las etiquetas.</div>
        <div class="card">
            <form method="post" enctype="multipart/form-data">
                <label>Orden de Trabajo:</label>
                <select name="ot_id" required>
                    {opciones_ot}
                </select>
                <label>Archivo Excel con piezas:</label>
                <input type="file" name="excel_file" accept=".xls,.xlsx" required>
                <button type="submit" class="btn">🏷️ Generar Etiquetas PDF</button>
            </form>
        </div>
        <a href="/" class="btn btn-sec">← Volver al Inicio</a>
    </div>
    </body>
    </html>
    """
    return html


# Ruta y función de descarga de plantilla eliminadas
