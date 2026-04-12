import pandas as pd
import qrcode
import os
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Image, Paragraph, PageBreak
from reportlab.lib import colors
from reportlab.lib.pagesizes import A3, landscape
from reportlab.lib.units import mm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
import tkinter as tk
from tkinter import filedialog, messagebox

# =========================
# LIMPIEZA AUTOMATICA EXCEL
# =========================
def load_clean_excel(path):
    raw = pd.read_excel(path, header=None)

    for i in range(10):
        row = raw.iloc[i].fillna("").astype(str).str.upper()

        if any("POS" in str(x) for x in row):
            df = pd.read_excel(path, header=i)
            df.columns = [str(c).strip().upper() for c in df.columns]
            return df

    return pd.read_excel(path)

# =========================
# BUSCAR COLUMNA
# =========================
def find_col(df, keyword):
    for c in df.columns:
        if keyword in c:
            return c
    return None

# =========================
# GENERAR ETIQUETAS
# =========================
def generar():
    try:
        excel1 = filedialog.askopenfilename(title="Seleccionar Excel ARMADOS")
        excel2 = filedialog.askopenfilename(title="Seleccionar Excel PRODUCCIÓN")
        logo = r"C:\Users\usuar\OneDrive\Desktop\python\LOGO.png"

        df1 = load_clean_excel(excel1)
        df2 = load_clean_excel(excel2)

        col_pos1 = find_col(df1, "POS")
        col_pos2 = find_col(df2, "POS")

        df = pd.merge(df1, df2, left_on=col_pos1, right_on=col_pos2, how="left")

        plano_col = find_col(df, "PLANO")
        rev_col = find_col(df, "REV")
        obra_col = find_col(df, "OBRA")
        cant_col = find_col(df, "CANT")
        perfil_col = find_col(df, "PERFIL")
        peso_col = find_col(df, "PESO")
        desc_col = find_col(df, "DESCRIP")

        styles = getSampleStyleSheet()
        # Crear estilo compacto para las etiquetas
        label_style = ParagraphStyle(
            'LabelStyle',
            parent=styles['Normal'],
            fontSize=6,
            leading=7,
            alignment=1  # CENTER
        )

        # Usar ruta absoluta para el directorio de QR
        qr_dir = r"C:\Users\usuar\OneDrive\Desktop\python\qrs"
        os.makedirs(qr_dir, exist_ok=True)

        cols = 10
        rows_per_page = 4
        
        # Expandir filas según cantidad para códigos especiales
        prefijos_expandibles = ["V", "C", "PU", "INS"]
        rows_expandidas = []
        
        for idx, row in df.iterrows():
            pos = str(row.get(col_pos1, "")).strip()
            cant_str = str(row.get(cant_col, "0")).split(".")[0]
            
            try:
                cant = int(cant_str) if cant_str else 1
            except:
                cant = 1
            
            # Verificar si pos comienza con algún prefijo expandible
            es_expandible = any(pos.startswith(prefijo) for prefijo in prefijos_expandibles)
            
            if es_expandible and cant > 1:
                # Generar una fila por cada pieza
                for num in range(1, cant + 1):
                    row_copia = row.copy()
                    # Crear nuevo código como V1-5, V2-5, etc.
                    nuevo_pos = f"{pos}-{num}"
                    row_copia[col_pos1] = nuevo_pos
                    rows_expandidas.append(row_copia)
            else:
                rows_expandidas.append(row)
        
        # Crear nuevo DataFrame con las filas expandidas
        df_expandido = pd.DataFrame(rows_expandidas).reset_index(drop=True)
        
        # Calcular número de páginas necesarias
        total_items = len(df_expandido)
        items_per_page = cols * rows_per_page
        num_pages = (total_items + items_per_page - 1) // items_per_page
        
        # Crear documento con márgenes pequeños en landscape
        doc = SimpleDocTemplate(
            r"C:\Users\usuar\OneDrive\Desktop\python\ETIQUETAS_A3.pdf", 
            pagesize=landscape(A3),
            topMargin=2*mm,
            bottomMargin=2*mm,
            leftMargin=2*mm,
            rightMargin=2*mm
        )
        
        elements = []
        i = 0
        
        for page in range(num_pages):
            data = []
            for r in range(rows_per_page):
                row_data = []
                for c in range(cols):
                    if i < len(df_expandido):
                        row = df_expandido.iloc[i]

                        pos = str(row.get(col_pos1, ""))
                        plano = str(row.get(plano_col, ""))
                        rev = str(row.get(rev_col, ""))
                        obra = str(row.get(obra_col, ""))
                        cant = str(row.get(cant_col, ""))
                        perfil = str(row.get(perfil_col, ""))
                        peso = str(row.get(peso_col, ""))
                        desc = str(row.get(desc_col, ""))

                        # QR híbrido - usar la posición completa (incluyendo numeración para expandibles)
                        qr_text = f"http://192.168.0.134:5000/pieza/{pos}"

                        qr_path = f"{qr_dir}/qr_{i}.png"
                        qr = qrcode.QRCode(
                            version=1,
                            error_correction=qrcode.constants.ERROR_CORRECT_H,
                            box_size=10,
                            border=2,
                        )
                        qr.add_data(qr_text)
                        qr.make(fit=True)
                        img = qr.make_image(fill_color="black", back_color="white")
                        img.save(qr_path)

                        text = f"""
                        <b>OBRA:</b> {obra}<br/>
                        <b>POS:</b> {pos}<br/>
                        <b>CANT:</b> {cant}<br/><br/>
                        <b>PERFIL:</b> {perfil}<br/>
                        <b>PESO:</b> {peso}<br/><br/>

                        {desc}
                        """

                        content = [
                            Image(logo, width=20*mm, height=16*mm),
                            Paragraph(text, label_style),
                            Image(qr_path, width=30*mm, height=30*mm)
                        ]

                        row_data.append(content)
                        i += 1
                    else:
                        row_data.append("")

                data.append(row_data)

            # Crear tabla para esta página solo si tiene datos
            has_content = any(any(cell != "" for cell in row) for row in data)
            if has_content:
                table = Table(data, colWidths=40*mm, rowHeights=70*mm)
                table.setStyle(TableStyle([
                    ('GRID', (0,0), (-1,-1), 0.5, colors.black),
                    ('ALIGN', (0,0), (-1,-1), 'CENTER'),
                    ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
                    ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#FFFFFF')),
                    ('LEFTPADDING', (0,0), (-1,-1), 2),
                    ('RIGHTPADDING', (0,0), (-1,-1), 2),
                    ('TOPPADDING', (0,0), (-1,-1), 3),
                    ('BOTTOMPADDING', (0,0), (-1,-1), 3)
                ]))
                
                elements.append(table)
                
                # Agregar salto de página si no es la última página con contenido
                if page < num_pages - 1:
                    elements.append(PageBreak())

        doc.build(elements)

        messagebox.showinfo("OK", "PDF generado correctamente")
    

    except Exception as e:
        messagebox.showerror("Error", str(e))


# =========================
# INTERFAZ
# =========================
root = tk.Tk()
root.title("Generador de Etiquetas A3")

root.geometry("300x150")

btn = tk.Button(root, text="GENERAR ETIQUETAS", command=generar, height=2, width=25)
btn.pack(pady=30)

root.mainloop()