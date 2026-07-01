from datetime import datetime
from io import BytesIO
import html as html_lib
import json
import os

from flask import Blueprint, request, redirect, send_file, session
from werkzeug.utils import secure_filename

from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image, PageBreak, HRFlowable
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, A4
from reportlab.lib.units import mm, cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
from reportlab.graphics.shapes import Drawing, Circle, String

from db_utils import get_db

AUDITORIA_FOTOS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "auditorias_fotos")
if not os.path.exists(AUDITORIA_FOTOS_DIR):
    os.makedirs(AUDITORIA_FOTOS_DIR)


auditoria_obra_bp = Blueprint("auditoria-obra", __name__, url_prefix="/modulo/auditoria-obra")


CATEGORIAS_OBSERVACION = [
    ("Montaje", "Alineacion, plomos, nivelacion, secuencia de montaje"),
    ("Soldadura", "Preparacion de juntas, WPS, calidad visual, limpieza"),
    ("Buloneria", "Torque, tipo de pernos, arandelas, identificacion"),
    ("Pintura", "Danios, retoques, espesor, proteccion de superficies"),
    ("Seguridad", "EPP, lineas de vida, senializacion, izajes"),
    ("Orden y Limpieza", "Acopio, circulacion, residuos"),
    ("Equipos", "Gruas, eslingas, grilletes, herramientas"),
    ("Documentacion", "Planos, procedimientos, registros, certificados"),
]

EVALUACION_ASPECTOS = [
  ("calidad_montaje", "Calidad de montaje"),
  ("soldaduras", "Soldaduras"),
  ("pernos", "Pernos"),
  ("seguridad", "Seguridad"),
  ("orden_limpieza", "Orden y limpieza"),
  ("documentacion", "Documentación"),
]

EVALUACION_OPCIONES = ["", "Muy Buena", "Buena", "Conforme", "Mejorable", "Crítica", "N/A (NO APLICA)"]


def _es_admin_session():
    return str(session.get("user_role") or "").strip().lower() == "administrador"


def _e(value):
    return html_lib.escape(str(value or ""))


def _parse_json_list(value):
  if isinstance(value, list):
    return value
  txt = str(value or "").strip()
  if not txt:
    return []
  try:
    parsed = json.loads(txt)
    return parsed if isinstance(parsed, list) else [txt]
  except Exception:
    return [line.strip("-• \t") for line in txt.splitlines() if line.strip()]


def _parse_text_items_from_form(form, field_name):
  values = []
  for item in form.getlist(field_name):
    txt = str(item or "").strip()
    if txt:
      values.append(txt)
  return values


def _parse_acciones_from_form(form):
  acciones = form.getlist("accion_pendiente[]")
  responsables = form.getlist("responsable_pendiente[]")
  fechas = form.getlist("fecha_compromiso[]")
  max_len = max(len(acciones), len(responsables), len(fechas))
  rows = []
  for i in range(max_len):
    accion = (acciones[i] if i < len(acciones) else "").strip()
    responsable = (responsables[i] if i < len(responsables) else "").strip()
    fecha = (fechas[i] if i < len(fechas) else "").strip()
    if not any([accion, responsable, fecha]):
      continue
    rows.append({"accion": accion, "responsable": responsable, "fecha_compromiso": fecha})
  return rows


def _ensure_schema(db):
  # SQLite first; fallback to MySQL-compatible DDL when needed.
  try:
    db.execute(
      """
      CREATE TABLE IF NOT EXISTS auditorias_obra (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        ot_id INTEGER,
        cliente TEXT,
        obra TEXT,
        proyecto TEXT,
        fecha_auditoria DATE,
        resumen TEXT,
        aspectos_positivos TEXT,
        acciones_pendientes TEXT,
        observaciones_json TEXT,
        evaluacion_json TEXT,
        realizado_por TEXT,
        creado_por TEXT,
        fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (ot_id) REFERENCES ordenes_trabajo(id)
      )
      """
    )
  except Exception:
    db.execute(
      """
      CREATE TABLE IF NOT EXISTS auditorias_obra (
        id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        ot_id BIGINT,
        cliente TEXT,
        obra TEXT,
        proyecto TEXT,
        fecha_auditoria DATE,
        resumen TEXT,
        aspectos_positivos TEXT,
        acciones_pendientes TEXT,
        observaciones_json LONGTEXT,
        evaluacion_json LONGTEXT,
        realizado_por TEXT,
        creado_por TEXT,
        fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP
      )
      """
    )

  try:
    db.execute("CREATE INDEX IF NOT EXISTS idx_auditorias_obra_fecha ON auditorias_obra(fecha_auditoria)")
  except Exception:
    try:
      db.execute("CREATE INDEX idx_auditorias_obra_fecha ON auditorias_obra(fecha_auditoria)")
    except Exception:
      pass

  try:
    db.execute("CREATE INDEX IF NOT EXISTS idx_auditorias_obra_ot ON auditorias_obra(ot_id)")
  except Exception:
    try:
      db.execute("CREATE INDEX idx_auditorias_obra_ot ON auditorias_obra(ot_id)")
    except Exception:
      pass

  try:
    db.execute("ALTER TABLE auditorias_obra ADD COLUMN evaluacion_json TEXT")
  except Exception:
    pass
  try:
    db.execute("ALTER TABLE auditorias_obra ADD COLUMN realizado_por TEXT")
  except Exception:
    pass
  db.commit()


def _build_pdf_bytes(auditoria, observaciones, evaluaciones=None):
    """Genera PDF de auditoría de obra con estilo del reporte de producción."""
    from reportlab.platypus import KeepTogether
    import locale

    # ─── Colores del reporte ───────────────────────────────────────────────
    NARANJA     = colors.HexColor("#f97316")
    NARANJA_OSC = colors.HexColor("#ea580c")
    AZUL_OSC    = colors.HexColor("#1f3864")
    GRIS_LABEL  = colors.HexColor("#f1f5f9")
    GRIS_BG     = colors.HexColor("#f8fafc")
    VERDE       = colors.HexColor("#166534")
    ROJO        = colors.HexColor("#b91c1c")
    TEXTO_DARK  = colors.HexColor("#111827")
    TEXTO_MED   = colors.HexColor("#374151")
    TEXTO_LIGHT = colors.HexColor("#6b7280")
    BORDE       = colors.HexColor("#e5e7eb")
    VERDE_SEMAFORO = colors.HexColor("#34d399")
    AMBAR_SEMAFORO = colors.HexColor("#fbbf24")
    ROJO_SEMAFORO = colors.HexColor("#ef4444")
    GRIS_SEMAFORO = colors.HexColor("#9ca3af")

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=A4,
        topMargin=12*mm,
        bottomMargin=14*mm,
        leftMargin=14*mm,
        rightMargin=14*mm,
        title="Informe de Auditoría de Obra",
    )

    styles = getSampleStyleSheet()
    st_normal   = ParagraphStyle("normal_a",   fontName="Helvetica",       fontSize=9,  textColor=TEXTO_DARK, leading=13)
    st_small    = ParagraphStyle("small_a",    fontName="Helvetica",       fontSize=8,  textColor=TEXTO_MED,  leading=11)
    st_label    = ParagraphStyle("label_a",    fontName="Helvetica-Bold",  fontSize=7,  textColor=TEXTO_LIGHT, leading=10, spaceAfter=1)
    st_value    = ParagraphStyle("value_a",    fontName="Helvetica-Bold",  fontSize=10, textColor=TEXTO_DARK,  leading=13)
    st_h_sec    = ParagraphStyle("hsec_a",     fontName="Helvetica-Bold",  fontSize=10, textColor=NARANJA,     leading=14, spaceBefore=8, spaceAfter=3)
    st_obs_num  = ParagraphStyle("obsnum_a",   fontName="Helvetica-Bold",  fontSize=9,  textColor=AZUL_OSC,    leading=13, spaceBefore=6)
    st_white    = ParagraphStyle("white_a",    fontName="Helvetica-Bold",  fontSize=9,  textColor=colors.white, leading=12)
    st_white_sm = ParagraphStyle("whitesm_a",  fontName="Helvetica",       fontSize=8,  textColor=colors.white, leading=11)
    st_title    = ParagraphStyle("title_a",    fontName="Helvetica-Bold",  fontSize=14, textColor=colors.white, leading=18, alignment=TA_CENTER)
    st_footer   = ParagraphStyle("footer_a",   fontName="Helvetica",       fontSize=7,  textColor=TEXTO_LIGHT, leading=9)

    def _date_fmt(s):
        try:
            return datetime.strptime(s, "%Y-%m-%d").strftime("%d/%m/%Y")
        except Exception:
            return s or ""

    fecha_fmt = _date_fmt(auditoria.get("fecha_auditoria", ""))
    obra      = auditoria.get("obra", "") or ""
    cliente   = auditoria.get("cliente", "") or ""
    proyecto  = auditoria.get("proyecto", "") or ""
    realizado_por = auditoria.get("realizado_por", "") or ""

    def _iso_badge(top, code, stroke_color):
      d = Drawing(13 * mm, 13 * mm)
      cx = 6.5 * mm
      cy = 6.5 * mm
      r = 5.8 * mm
      d.add(Circle(cx, cy, r, strokeColor=stroke_color, strokeWidth=1.2, fillColor=colors.white))
      d.add(String(cx, cy + 1.2 * mm, str(top), textAnchor="middle", fontName="Helvetica", fontSize=4.6))
      d.add(String(cx, cy - 2.5 * mm, str(code), textAnchor="middle", fontName="Helvetica-Bold", fontSize=6.2))
      return d

    def _eval_color(valor):
      v = str(valor or "").strip().lower()
      if v.startswith("n/a") or "no aplica" in v:
        return GRIS_SEMAFORO, "#9ca3af"
      if "mejorable" in v:
        return AMBAR_SEMAFORO, "#fbbf24"
      if "cr" in v:
        return ROJO_SEMAFORO, "#ef4444"
      if v in ("muy buena", "buena", "conforme"):
        return VERDE_SEMAFORO, "#34d399"
      return GRIS_SEMAFORO, "#9ca3af"

    story = []
    page_w = A4[0] - 28*mm  # usable width

    # ── ENCABEZADO (estilo reporte semanal) ───────────────────────────────
    hdr_top = Table([[" "]], colWidths=[page_w], rowHeights=[1.2 * mm])
    hdr_top.setStyle(TableStyle([
      ("BACKGROUND", (0, 0), (-1, -1), NARANJA_OSC),
      ("BOX", (0, 0), (-1, -1), 0, colors.white),
    ]))
    story.append(hdr_top)

    header_title = ParagraphStyle(
      "hdr_title_a",
      fontName="Helvetica-Bold",
      fontSize=12,
      textColor=colors.HexColor("#1f2937"),
      leading=15,
    )
    header_sub = ParagraphStyle(
      "hdr_sub_a",
      fontName="Helvetica",
      fontSize=9,
      textColor=colors.HexColor("#374151"),
      leading=12,
    )
    header_chip = ParagraphStyle(
      "hdr_chip_a",
      fontName="Helvetica-Bold",
      fontSize=8,
      textColor=colors.HexColor("#b45309"),
      alignment=TA_RIGHT,
      leading=11,
    )
    header_date = ParagraphStyle(
      "hdr_date_a",
      fontName="Helvetica",
      fontSize=9,
      textColor=colors.HexColor("#374151"),
      alignment=TA_RIGHT,
      leading=12,
    )

    logo_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "LOGO.png")
    if os.path.isfile(logo_path):
      logo_cell = Image(logo_path, width=16 * mm, height=11 * mm)
    else:
      logo_cell = Paragraph("A3", ParagraphStyle("hdr_logo_fallback", fontName="Helvetica-Bold", fontSize=11, textColor=NARANJA_OSC))

    titulo_txt = "INFORME DE AUDITORIA DE OBRA"
    subtitulo_txt = f"Obra: {obra or '-'}"
    fecha_header = fecha_fmt or datetime.now().strftime("%d/%m/%Y")

    hdr_data = [[
      logo_cell,
      Paragraph(f"{_e(titulo_txt)}<br/>{_e(subtitulo_txt)}", header_title),
      Paragraph(
        '<font backcolor="#fff7ed" color="#b45309">  INFORME INTERNO  </font><br/>' + _e(fecha_header),
        header_date,
      ),
    ]]
    hdr_tbl = Table(hdr_data, colWidths=[22 * mm, page_w - 62 * mm, 40 * mm], rowHeights=[15 * mm])
    hdr_tbl.setStyle(TableStyle([
      ("BACKGROUND", (0, 0), (-1, -1), colors.HexColor("#f3f4f6")),
      ("LINEBELOW", (0, 0), (-1, -1), 1, colors.HexColor("#cbd5e1")),
      ("BOX", (0, 0), (-1, -1), 0.5, colors.HexColor("#e5e7eb")),
      ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
      ("ALIGN", (2, 0), (2, 0), "RIGHT"),
      ("LEFTPADDING", (0, 0), (-1, -1), 6),
      ("RIGHTPADDING", (0, 0), (-1, -1), 6),
      ("TOPPADDING", (0, 0), (-1, -1), 5),
      ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
    ]))
    story.append(hdr_tbl)
    story.append(Spacer(1, 3 * mm))

    # ── FICHA DE DATOS ────────────────────────────────────────────────────
    def _ficha_cell(label, value):
        return [
            Paragraph(label, st_label),
            Paragraph(str(value or "—"), st_value),
        ]

    ficha_data = [
        [_ficha_cell("OBRA / PROYECTO", obra),
         _ficha_cell("CLIENTE", cliente),
         _ficha_cell("PROYECTO", proyecto)],
        [_ficha_cell("FECHA AUDITORÍA", fecha_fmt),
          _ficha_cell("REALIZADO POR", realizado_por),
         _ficha_cell("ESTADO", "Emitido")],
    ]
    col3 = page_w / 3
    ficha_tbl = Table(ficha_data, colWidths=[col3, col3, col3])
    ficha_tbl.setStyle(TableStyle([
        ("BACKGROUND",  (0,0), (-1,-1), GRIS_LABEL),
        ("BOX",         (0,0), (-1,-1), 0.5, BORDE),
        ("INNERGRID",   (0,0), (-1,-1), 0.5, BORDE),
        ("LEFTPADDING", (0,0), (-1,-1), 6),
        ("RIGHTPADDING",(0,0), (-1,-1), 6),
        ("TOPPADDING",  (0,0), (-1,-1), 5),
        ("BOTTOMPADDING",(0,0),(-1,-1), 5),
        ("VALIGN",      (0,0), (-1,-1), "TOP"),
    ]))
    story.append(ficha_tbl)
    story.append(Spacer(1, 4*mm))

    def _section_title(num, txt):
        story.append(Paragraph(f"{num}. {txt.upper()}", st_h_sec))
        story.append(HRFlowable(width="100%", thickness=1, color=NARANJA, spaceAfter=3))

    def _body_para(txt):
        story.append(Paragraph(str(txt or "—").replace("\n", "<br/>"), st_normal))
        story.append(Spacer(1, 2*mm))

    # ── 1. RESUMEN ────────────────────────────────────────────────────────
    _section_title(1, "RESUMEN")
    _body_para(auditoria.get("resumen", ""))

    # ── 2. TABLA DE OBSERVACIONES ─────────────────────────────────────────
    _section_title(2, "TABLA DE OBSERVACIONES")

    t_hdrs = ["N°", "Punto Analizado", "Observación", "Estado", "Informado", "Categoría"]
    t_cw   = [8*mm, 30*mm, page_w - 8*mm - 30*mm - 22*mm - 16*mm - 28*mm, 22*mm, 16*mm, 28*mm]
    t_rows = [[ Paragraph(h, st_white) for h in t_hdrs ]]
    if observaciones:
        for idx, obs in enumerate(observaciones, start=1):
            est = str(obs.get("estado", "") or "")
            est_color = ROJO if "orregir" in est else (VERDE if "onforme" in est else TEXTO_DARK)
            est_style = ParagraphStyle("ests", fontName="Helvetica-Bold", fontSize=8, textColor=est_color, leading=11, alignment=TA_CENTER)
            t_rows.append([
                Paragraph(str(idx), ParagraphStyle("nc", fontName="Helvetica", fontSize=8, alignment=TA_CENTER, leading=11)),
                Paragraph(_e(obs.get("punto", "")), st_small),
                Paragraph(_e(obs.get("observacion", "")), st_small),
                Paragraph(_e(est), est_style),
                Paragraph("Sí" if obs.get("informado") else "No", ParagraphStyle("inf", fontName="Helvetica", fontSize=8, alignment=TA_CENTER, leading=11)),
                Paragraph(_e(obs.get("categoria", "")), st_small),
            ])
    else:
        t_rows.append([Paragraph("Sin observaciones.", st_small)] + [""] * 5)

    t_obs = Table(t_rows, colWidths=t_cw, repeatRows=1)
    t_obs.setStyle(TableStyle([
        ("BACKGROUND",   (0,0), (-1,0),  AZUL_OSC),
        ("BACKGROUND",   (0,1), (-1,-1), colors.white),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [colors.white, GRIS_BG]),
        ("BOX",          (0,0), (-1,-1), 0.5, BORDE),
        ("INNERGRID",    (0,0), (-1,-1), 0.3, BORDE),
        ("LEFTPADDING",  (0,0), (-1,-1), 4),
        ("RIGHTPADDING", (0,0), (-1,-1), 4),
        ("TOPPADDING",   (0,0), (-1,-1), 4),
        ("BOTTOMPADDING",(0,0), (-1,-1), 4),
        ("VALIGN",       (0,0), (-1,-1), "TOP"),
    ]))
    story.append(t_obs)
    story.append(Spacer(1, 4*mm))

    # ── 3. DETALLE DE OBSERVACIONES ───────────────────────────────────────
    _section_title(3, "DETALLE DE OBSERVACIONES")
    if observaciones:
        for idx, obs in enumerate(observaciones, start=1):
            block = []
            block.append(Paragraph(f"Observación {idx}: {_e(obs.get('punto', ''))}", st_obs_num))
            det_data = [
                [Paragraph("Estado",     st_label), Paragraph(_e(obs.get("estado",     "")), st_normal)],
                [Paragraph("Comentario", st_label), Paragraph(_e(obs.get("comentario", "")), st_normal)],
            ]
            det_cw = [25*mm, page_w - 25*mm]
            det_tbl = Table(det_data, colWidths=det_cw)
            det_tbl.setStyle(TableStyle([
                ("BACKGROUND",  (0,0), (0,-1), GRIS_LABEL),
                ("BOX",         (0,0), (-1,-1), 0.4, BORDE),
                ("INNERGRID",   (0,0), (-1,-1), 0.3, BORDE),
                ("LEFTPADDING", (0,0), (-1,-1), 5),
                ("TOPPADDING",  (0,0), (-1,-1), 3),
                ("BOTTOMPADDING",(0,0),(-1,-1), 3),
                ("VALIGN",      (0,0), (-1,-1), "TOP"),
            ]))
            block.append(det_tbl)

            # ── Foto adjunta ──────────────────────────────────────────────
            fotos = []
            if isinstance(obs.get("fotos"), list):
              fotos.extend([fp for fp in obs.get("fotos") if fp])
            if not fotos and obs.get("foto_path"):
              fotos.append(obs.get("foto_path"))

            img_flowables = []
            for foto_path in fotos[:3]:
              if foto_path and os.path.isfile(foto_path):
                try:
                  img = Image(foto_path)
                  img_w = min((page_w - 8 * mm) / 3, 75 * mm)
                  factor = img_w / float(img.imageWidth or 1)
                  img.drawWidth = img_w
                  img.drawHeight = (img.imageHeight or 1) * factor
                  img_flowables.append(img)
                except Exception:
                  pass

            if img_flowables:
              while len(img_flowables) < 3:
                img_flowables.append(Paragraph("", st_small))
              imgs_tbl = Table([img_flowables], colWidths=[(page_w - 8 * mm) / 3] * 3)
              imgs_tbl.setStyle(TableStyle([
                ("LEFTPADDING", (0, 0), (-1, -1), 2),
                ("RIGHTPADDING", (0, 0), (-1, -1), 2),
                ("TOPPADDING", (0, 0), (-1, -1), 2),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 2),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
              ]))
              block.append(Spacer(1, 2 * mm))
              block.append(imgs_tbl)

            block.append(Spacer(1, 3*mm))
            story.append(KeepTogether(block))
    else:
        _body_para("Sin observaciones cargadas.")

    # ── 4. ASPECTOS POSITIVOS ─────────────────────────────────────────────
    _section_title(4, "ASPECTOS POSITIVOS")
    aspectos_items = _parse_json_list(auditoria.get("aspectos_positivos", ""))
    if aspectos_items:
      aspectos_rows = [[Paragraph("Item", st_white)]]
      for item in aspectos_items:
        aspectos_rows.append([Paragraph(_e(item), st_normal)])
      aspectos_tbl = Table(aspectos_rows, colWidths=[page_w])
      aspectos_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), AZUL_OSC),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, GRIS_BG]),
        ("BOX", (0, 0), (-1, -1), 0.5, BORDE),
        ("INNERGRID", (0, 0), (-1, -1), 0.3, BORDE),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
      ]))
      story.append(aspectos_tbl)
      story.append(Spacer(1, 3*mm))
    else:
      _body_para("Sin aspectos positivos cargados.")

    # ── 5. ACCIONES PENDIENTES ────────────────────────────────────────────
    _section_title(5, "ACCIONES PENDIENTES")
    acciones_items = _parse_json_list(auditoria.get("acciones_pendientes", ""))
    if acciones_items and isinstance(acciones_items[0], dict):
      acciones_rows = [[Paragraph("Acción", st_white), Paragraph("Responsable", st_white), Paragraph("Fecha compromiso", st_white)]]
      for item in acciones_items:
        acciones_rows.append([
          Paragraph(_e(item.get("accion", "")), st_small),
          Paragraph(_e(item.get("responsable", "")), st_small),
          Paragraph(_e(item.get("fecha_compromiso", "")), st_small),
        ])
      acciones_tbl = Table(acciones_rows, colWidths=[page_w * 0.5, page_w * 0.25, page_w * 0.25], repeatRows=1)
      acciones_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), AZUL_OSC),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, GRIS_BG]),
        ("BOX", (0, 0), (-1, -1), 0.5, BORDE),
        ("INNERGRID", (0, 0), (-1, -1), 0.3, BORDE),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
      ]))
      story.append(acciones_tbl)
      story.append(Spacer(1, 3*mm))
    elif acciones_items:
      acciones_rows = [[Paragraph("Acción", st_white), Paragraph("Responsable", st_white), Paragraph("Fecha compromiso", st_white)]]
      for item in acciones_items:
        acciones_rows.append([
          Paragraph(_e(item), st_small),
          Paragraph("", st_small),
          Paragraph("", st_small),
        ])
      acciones_tbl = Table(acciones_rows, colWidths=[page_w * 0.5, page_w * 0.25, page_w * 0.25], repeatRows=1)
      acciones_tbl.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), AZUL_OSC),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, GRIS_BG]),
        ("BOX", (0, 0), (-1, -1), 0.5, BORDE),
        ("INNERGRID", (0, 0), (-1, -1), 0.3, BORDE),
        ("LEFTPADDING", (0, 0), (-1, -1), 5),
        ("RIGHTPADDING", (0, 0), (-1, -1), 5),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
      ]))
      story.append(acciones_tbl)
      story.append(Spacer(1, 3*mm))
    else:
      _body_para("Sin acciones pendientes cargadas.")

    # ── 6. EVALUACION GENERAL ────────────────────────────────────────────
    _section_title(6, "EVALUACION GENERAL")
    eval_rows_map = {}
    if isinstance(evaluaciones, list):
      for item in evaluaciones:
        asp = str(item.get("aspecto") or "").strip()
        if asp:
          eval_rows_map[asp] = str(item.get("valor") or "").strip()

    eval_table_rows = [[Paragraph("Aspecto", st_white), Paragraph("Evaluación", st_white)]]
    for _, label in EVALUACION_ASPECTOS:
      val = eval_rows_map.get(label, "")
      _, hex_color = _eval_color(val)
      eval_text = f'<font color="{hex_color}">&#9679;</font> {_e(val or "Sin evaluar")}'
      eval_table_rows.append([
        Paragraph(_e(label), st_normal),
        Paragraph(eval_text, ParagraphStyle("eval_v", fontName="Helvetica-Bold", fontSize=9, textColor=TEXTO_DARK, leading=12)),
      ])

    eval_tbl = Table(eval_table_rows, colWidths=[page_w * 0.62, page_w * 0.38], repeatRows=1)
    eval_tbl.setStyle(TableStyle([
      ("BACKGROUND", (0, 0), (-1, 0), AZUL_OSC),
      ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, GRIS_BG]),
      ("BOX", (0, 0), (-1, -1), 0.5, BORDE),
      ("INNERGRID", (0, 0), (-1, -1), 0.3, BORDE),
      ("LEFTPADDING", (0, 0), (-1, -1), 5),
      ("RIGHTPADDING", (0, 0), (-1, -1), 5),
      ("TOPPADDING", (0, 0), (-1, -1), 4),
      ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
      ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
    ]))
    story.append(eval_tbl)
    story.append(Spacer(1, 6 * mm))

    # ── FIRMAS ───────────────────────────────────────────────────────────
    firma_data = [
      [Paragraph("Realizado por", st_label), Paragraph("Firma Coordinador de EEMM", st_label)],
      [Paragraph(_e(realizado_por or " "), st_normal), Paragraph(" ", st_normal)],
      [Paragraph("_______________________________", st_small), Paragraph("_______________________________", st_small)],
    ]
    firma_tbl = Table(firma_data, colWidths=[page_w / 2, page_w / 2])
    firma_tbl.setStyle(TableStyle([
      ("LEFTPADDING", (0, 0), (-1, -1), 4),
      ("RIGHTPADDING", (0, 0), (-1, -1), 4),
      ("TOPPADDING", (0, 0), (-1, -1), 3),
      ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
      ("VALIGN", (0, 0), (-1, -1), "BOTTOM"),
    ]))
    story.append(firma_tbl)

    # ── FOOTER con numero de pagina ───────────────────────────────────────
    def _add_footer(canvas, doc):
        canvas.saveState()
        canvas.setFont("Helvetica", 7)
        canvas.setFillColor(TEXTO_LIGHT)
        footer_y = 8*mm
        canvas.drawString(14*mm, footer_y, f"{obra} · Auditoría {fecha_fmt}")
        canvas.drawRightString(A4[0] - 14*mm, footer_y, f"Página {doc.page}")
        canvas.setStrokeColor(BORDE)
        canvas.setLineWidth(0.5)
        canvas.line(14*mm, footer_y + 3.5*mm, A4[0] - 14*mm, footer_y + 3.5*mm)
        canvas.restoreState()

    doc.build(story, onFirstPage=_add_footer, onLaterPages=_add_footer)
    buf.seek(0)
    return buf


def _save_foto(file_obj, auditoria_id, obs_idx, slot_idx):
    """Guarda el archivo subido y retorna la ruta absoluta."""
    if not file_obj or not file_obj.filename:
        return ""
    ext = os.path.splitext(secure_filename(file_obj.filename))[1].lower()
    if ext not in (".jpg", ".jpeg", ".png", ".gif", ".webp"):
        return ""
    filename = f"audit_{auditoria_id:04d}_obs{obs_idx:02d}_f{slot_idx}{ext}"
    path = os.path.join(AUDITORIA_FOTOS_DIR, filename)
    file_obj.save(path)
    return path


def _parse_observaciones_from_form_with_photos(form, files, auditoria_id):
    """Parsea observaciones y guarda fotos subidas."""
    puntos      = form.getlist("obs_punto[]")
    observaciones = form.getlist("obs_texto[]")
    estados     = form.getlist("obs_estado[]")
    informados  = form.getlist("obs_informado[]")
    categorias  = form.getlist("obs_categoria[]")
    temas       = form.getlist("obs_tema[]")
    comentarios = form.getlist("obs_comentario[]")
    fotos_1_files = files.getlist("obs_foto1[]")
    fotos_2_files = files.getlist("obs_foto2[]")
    fotos_3_files = files.getlist("obs_foto3[]")

    max_len = max(len(puntos), len(observaciones), len(estados), len(informados),
                  len(categorias), len(temas), len(comentarios))
    rows = []
    for i in range(max_len):
        punto    = (puntos[i]       if i < len(puntos)       else "").strip()
        obs      = (observaciones[i] if i < len(observaciones) else "").strip()
        estado   = (estados[i]      if i < len(estados)      else "").strip()
        informado_txt = (informados[i] if i < len(informados) else "").strip().lower()
        categoria = (categorias[i]  if i < len(categorias)   else "").strip()
        tema     = (temas[i]        if i < len(temas)        else "").strip()
        comentario = (comentarios[i] if i < len(comentarios) else "").strip()

        if not any([punto, obs, estado, categoria, tema, comentario]):
            continue

        fotos_paths = []
        if i < len(fotos_1_files):
          fp1 = _save_foto(fotos_1_files[i], auditoria_id, i + 1, 1)
          if fp1:
            fotos_paths.append(fp1)
        if i < len(fotos_2_files):
          fp2 = _save_foto(fotos_2_files[i], auditoria_id, i + 1, 2)
          if fp2:
            fotos_paths.append(fp2)
        if i < len(fotos_3_files):
          fp3 = _save_foto(fotos_3_files[i], auditoria_id, i + 1, 3)
          if fp3:
            fotos_paths.append(fp3)

        rows.append({
            "punto": punto,
            "observacion": obs,
            "estado": estado,
            "informado": informado_txt in ("si", "sí", "1", "true", "x"),
            "categoria": categoria,
            "tema": tema,
            "comentario": comentario,
            "fotos": fotos_paths,
            "foto_path": fotos_paths[0] if fotos_paths else "",
        })
    return rows


def _parse_evaluacion_from_form(form):
    rows = []
    for key, label in EVALUACION_ASPECTOS:
        rows.append(
            {
                "aspecto": label,
                "valor": (form.get(f"eval_{key}") or "").strip(),
            }
        )
    return rows


# Mantener _parse_observaciones_from_form para compatibilidad (sin fotos)
def _build_docx_bytes(*a, **kw):
    raise NotImplementedError("Use _build_pdf_bytes")  # no longer used

    def _set_cell_bg(cell, hex_color):
        tc = cell._tc
        tcPr = tc.get_or_add_tcPr()
        shd = OxmlElement('w:shd')
        shd.set(qn('w:val'), 'clear')
        shd.set(qn('w:color'), 'auto')
        shd.set(qn('w:fill'), hex_color)
        tcPr.append(shd)

    def _cell_text(cell, text, bold=False, size=10, color=None, align=WD_ALIGN_PARAGRAPH.LEFT):
        cell.text = ""
        p = cell.paragraphs[0]
        p.alignment = align
        run = p.add_run(str(text or ""))
        run.bold = bold
        run.font.size = Pt(size)
        run.font.name = "Calibri"
        if color:
            run.font.color.rgb = RGBColor(*color)

    doc = Document()

    # ── Margenes ─────────────────────────────────────────────────────────
    for section in doc.sections:
        section.top_margin = Inches(0.6)
        section.bottom_margin = Inches(0.6)
        section.left_margin = Inches(0.8)
        section.right_margin = Inches(0.8)

    normal = doc.styles["Normal"]
    normal.font.name = "Calibri"
    normal.font.size = Pt(10)

    # ── ENCABEZADO ───────────────────────────────────────────────────────
    hdr_table = doc.add_table(rows=1, cols=3)
    hdr_table.style = "Table Grid"

    # Celda izquierda: empresa
    c_left = hdr_table.cell(0, 0)
    _set_cell_bg(c_left, "1F3864")
    _cell_text(c_left, "A3 SERVICIOS\nCONSTRUCTIVOS", bold=True, size=11, color=(255, 255, 255), align=WD_ALIGN_PARAGRAPH.CENTER)
    c_left.width = Inches(1.4)

    # Celda central: titulo
    c_center = hdr_table.cell(0, 1)
    _set_cell_bg(c_center, "1F3864")
    c_center.text = ""
    p_title = c_center.paragraphs[0]
    p_title.alignment = WD_ALIGN_PARAGRAPH.CENTER
    r1 = p_title.add_run("INFORME DE AUDITORÍA DE OBRA")
    r1.bold = True
    r1.font.size = Pt(14)
    r1.font.name = "Calibri"
    r1.font.color.rgb = RGBColor(255, 255, 255)

    # Celda derecha: fecha
    c_right = hdr_table.cell(0, 2)
    _set_cell_bg(c_right, "1F3864")
    from datetime import datetime as _dt
    fecha_fmt = ""
    try:
        fd = auditoria.get("fecha_auditoria", "") or ""
        if fd:
            fecha_fmt = _dt.strptime(fd, "%Y-%m-%d").strftime("%d de %B de %Y")
    except Exception:
        fecha_fmt = auditoria.get("fecha_auditoria", "")
    _cell_text(c_right, fecha_fmt, size=10, color=(255, 255, 255), align=WD_ALIGN_PARAGRAPH.RIGHT)
    c_right.width = Inches(1.6)

    doc.add_paragraph("")

    # ── FICHA DE DATOS ───────────────────────────────────────────────────
    ficha = doc.add_table(rows=2, cols=6)
    ficha.style = "Table Grid"

    labels_row1 = ["OBRA / PROYECTO", auditoria.get("obra", ""),
                   "CLIENTE", auditoria.get("cliente", ""),
                   "PROYECTO", auditoria.get("proyecto", "")]
    labels_row2 = ["FECHA AUDITORÍA", fecha_fmt,
                   "REALIZADO POR", "",
                   "ESTADO", "Emitido"]

    for col_i, txt in enumerate(labels_row1):
        cell = ficha.cell(0, col_i)
        is_label = col_i % 2 == 0
        _set_cell_bg(cell, "D6E4F0" if is_label else "FFFFFF")
        _cell_text(cell, txt, bold=is_label, size=9)

    for col_i, txt in enumerate(labels_row2):
        cell = ficha.cell(1, col_i)
        is_label = col_i % 2 == 0
        _set_cell_bg(cell, "D6E4F0" if is_label else "FFFFFF")
        _cell_text(cell, txt, bold=is_label, size=9)

    doc.add_paragraph("")

    # ── FUNCION helpers ──────────────────────────────────────────────────
    def _add_section_title(texto, numero):
        p = doc.add_paragraph()
        p.paragraph_format.space_before = Pt(6)
        run = p.add_run(f"{numero}. {texto.upper()}")
        run.bold = True
        run.font.size = Pt(11)
        run.font.name = "Calibri"
        run.font.color.rgb = RGBColor(31, 56, 100)

    # ── 1. RESUMEN ───────────────────────────────────────────────────────
    _add_section_title("RESUMEN", 1)
    doc.add_paragraph(auditoria.get("resumen", "") or "—")

    # ── 2. TABLA DE OBSERVACIONES ────────────────────────────────────────
    _add_section_title("TABLA DE OBSERVACIONES", 2)

    t2 = doc.add_table(rows=1, cols=6)
    t2.style = "Table Grid"
    hdrs = ["N°", "Punto Analizado", "Observación", "Estado", "Informado", "Categoría"]
    widths = [Inches(0.3), Inches(1.2), Inches(2.2), Inches(0.9), Inches(0.7), Inches(1.0)]
    for i, (h, w) in enumerate(zip(hdrs, widths)):
        cell = t2.cell(0, i)
        cell.width = w
        _set_cell_bg(cell, "1F3864")
        _cell_text(cell, h, bold=True, size=9, color=(255, 255, 255), align=WD_ALIGN_PARAGRAPH.CENTER)

    if observaciones:
        for idx, obs in enumerate(observaciones, start=1):
            r = t2.add_row().cells
            _cell_text(r[0], idx, size=9, align=WD_ALIGN_PARAGRAPH.CENTER)
            _cell_text(r[1], obs.get("punto", ""), size=9)
            _cell_text(r[2], obs.get("observacion", ""), size=9)
            estado_val = str(obs.get("estado", "") or "")
            color_est = (200, 0, 0) if "orregir" in estado_val else (0, 120, 0) if "onforme" in estado_val else None
            _cell_text(r[3], estado_val, bold=True, size=9, color=color_est, align=WD_ALIGN_PARAGRAPH.CENTER)
            _cell_text(r[4], "Sí" if obs.get("informado") else "No", size=9, align=WD_ALIGN_PARAGRAPH.CENTER)
            _cell_text(r[5], obs.get("categoria", ""), size=9)
    else:
        r = t2.add_row().cells
        for cell in r:
            cell.text = ""

    # ── 3. DETALLE DE OBSERVACIONES ──────────────────────────────────────
    _add_section_title("DETALLE DE OBSERVACIONES", 3)
    if observaciones:
        for idx, obs in enumerate(observaciones, start=1):
            p_num = doc.add_paragraph()
            run_num = p_num.add_run(f"Observación {idx}: {obs.get('punto', '')}")
            run_num.bold = True
            run_num.font.name = "Calibri"
            run_num.font.size = Pt(10)

            detail_tbl = doc.add_table(rows=4, cols=2)
            detail_tbl.style = "Table Grid"
            rows_data = [
                ("Estado", obs.get("estado", "")),
                ("Tema", obs.get("tema", "")),
                ("Comentario", obs.get("comentario", "")),
                ("Foto de referencia", obs.get("foto", "")),
            ]
            for ri, (lbl, val) in enumerate(rows_data):
                _set_cell_bg(detail_tbl.cell(ri, 0), "D6E4F0")
                _cell_text(detail_tbl.cell(ri, 0), lbl, bold=True, size=9)
                _cell_text(detail_tbl.cell(ri, 1), val, size=9)
            doc.add_paragraph("")
    else:
        doc.add_paragraph("Sin observaciones cargadas.")

    # ── 4. ASPECTOS POSITIVOS ────────────────────────────────────────────
    _add_section_title("ASPECTOS POSITIVOS", 4)
    doc.add_paragraph(auditoria.get("aspectos_positivos", "") or "—")

    # ── 5. ACCIONES PENDIENTES ───────────────────────────────────────────
    _add_section_title("ACCIONES PENDIENTES", 5)
    doc.add_paragraph(auditoria.get("acciones_pendientes", "") or "—")

    output = BytesIO()
    doc.save(output)
    output.seek(0)
    return output


def _obtener_ots_activas(db):
    return db.execute(
        """
        SELECT id, COALESCE(cliente,''), COALESCE(obra,''), COALESCE(titulo,''), COALESCE(fecha_entrega,'')
        FROM ordenes_trabajo
        WHERE fecha_cierre IS NULL
          AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
          AND TRIM(COALESCE(obra, '')) != ''
        ORDER BY COALESCE(fecha_entrega, '9999-12-31') ASC, id ASC
        """
    ).fetchall()


def _obtener_obras_activas(db):
    """Devuelve lista de (obra, cliente, proyecto) unicas de OTs activas."""
    rows = db.execute(
        """
        SELECT TRIM(COALESCE(obra,'')), TRIM(COALESCE(cliente,'')), TRIM(COALESCE(titulo,''))
        FROM ordenes_trabajo
        WHERE fecha_cierre IS NULL
          AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
          AND TRIM(COALESCE(obra, '')) != ''
        ORDER BY TRIM(obra) ASC
        """
    ).fetchall()
    seen = {}
    for obra, cliente, titulo in rows:
        if obra not in seen:
            seen[obra] = (cliente, titulo)
    return [(obra, v[0], v[1]) for obra, v in seen.items()]


def _parse_observaciones_from_form(form):
    puntos = form.getlist("obs_punto[]")
    observaciones = form.getlist("obs_texto[]")
    estados = form.getlist("obs_estado[]")
    informados = form.getlist("obs_informado[]")
    categorias = form.getlist("obs_categoria[]")
    temas = form.getlist("obs_tema[]")
    comentarios = form.getlist("obs_comentario[]")
    fotos = form.getlist("obs_foto[]")

    max_len = max(
        len(puntos),
        len(observaciones),
        len(estados),
        len(informados),
        len(categorias),
        len(temas),
        len(comentarios),
        len(fotos),
    )

    rows = []
    for i in range(max_len):
        punto = (puntos[i] if i < len(puntos) else "").strip()
        obs = (observaciones[i] if i < len(observaciones) else "").strip()
        estado = (estados[i] if i < len(estados) else "").strip()
        informado_txt = (informados[i] if i < len(informados) else "").strip().lower()
        categoria = (categorias[i] if i < len(categorias) else "").strip()
        tema = (temas[i] if i < len(temas) else "").strip()
        comentario = (comentarios[i] if i < len(comentarios) else "").strip()
        foto = (fotos[i] if i < len(fotos) else "").strip()

        if not any([punto, obs, estado, categoria, tema, comentario, foto]):
            continue

        rows.append(
            {
                "punto": punto,
                "observacion": obs,
                "estado": estado,
                "informado": informado_txt in ("si", "sí", "1", "true", "x"),
                "categoria": categoria,
                "tema": tema,
                "comentario": comentario,
                "foto": foto,
            }
        )

    return rows


@auditoria_obra_bp.route("", methods=["GET", "POST"])
def modulo_auditoria_obra():
    if not _es_admin_session():
        return "<h3>Sin permiso para acceder al modulo Auditoria de Obra.</h3>", 403

    db = get_db()
    _ensure_schema(db)

    mensaje = ""
    error = ""

    if request.method == "POST":
        accion = (request.form.get("accion") or "guardar").strip().lower()

        ot_id_txt = (request.form.get("ot_id") or "").strip()
        cliente = (request.form.get("cliente") or "").strip()
        obra = (request.form.get("obra") or "").strip()
        proyecto = (request.form.get("proyecto") or "").strip()
        fecha_auditoria = (request.form.get("fecha_auditoria") or "").strip()
        realizado_por = (request.form.get("realizado_por") or "").strip()
        resumen = (request.form.get("resumen") or "").strip()
        aspectos_positivos = _parse_text_items_from_form(request.form, "aspecto_positivo[]")
        acciones_pendientes = _parse_acciones_from_form(request.form)
        evaluacion_rows = _parse_evaluacion_from_form(request.form)

        if not fecha_auditoria:
            fecha_auditoria = datetime.now().strftime("%Y-%m-%d")

        if not obra:
            error = "Seleccioná una obra antes de guardar."
        elif not resumen:
            error = "La seccion RESUMEN es obligatoria."
        else:
            ot_id = int(ot_id_txt) if ot_id_txt.isdigit() else None
            creado_por = str(session.get("username") or session.get("nombre") or "admin")
            if not realizado_por:
                realizado_por = str(session.get("nombre") or session.get("username") or "")

            # Insertar primero para obtener el ID (necesario para guardar fotos)
            cur = db.execute(
                """
                INSERT INTO auditorias_obra (
                    ot_id, cliente, obra, proyecto, fecha_auditoria,
                    resumen, aspectos_positivos, acciones_pendientes,
                    observaciones_json, evaluacion_json, realizado_por, creado_por
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (ot_id, cliente, obra, proyecto, fecha_auditoria,
                 resumen, json.dumps(aspectos_positivos, ensure_ascii=False), json.dumps(acciones_pendientes, ensure_ascii=False), "[]", json.dumps(evaluacion_rows, ensure_ascii=False), realizado_por, creado_por),
            )
            db.commit()
            auditoria_id = int(cur.lastrowid)

            # Parsear observaciones + guardar fotos ahora que tenemos el ID
            observaciones_rows = _parse_observaciones_from_form_with_photos(
                request.form, request.files, auditoria_id
            )
            db.execute(
                "UPDATE auditorias_obra SET observaciones_json = ? WHERE id = ?",
                (json.dumps(observaciones_rows, ensure_ascii=False), auditoria_id),
            )
            db.commit()

            if accion == "guardar_pdf":
                auditoria_dict = {
                    "cliente": cliente, "obra": obra, "proyecto": proyecto,
                    "fecha_auditoria": fecha_auditoria, "resumen": resumen,
                  "aspectos_positivos": aspectos_positivos,
                  "acciones_pendientes": acciones_pendientes,
                    "realizado_por": realizado_por,
                    "creado_por": creado_por,
                }
                pdf_buf = _build_pdf_bytes(auditoria_dict, observaciones_rows, evaluacion_rows)
                nombre = f"auditoria_obra_{auditoria_id:04d}_{datetime.now().strftime('%Y%m%d')}.pdf"
                return send_file(pdf_buf, as_attachment=True, download_name=nombre, mimetype="application/pdf")

            mensaje = f"Auditoria guardada con ID {auditoria_id}."

    obras = _obtener_obras_activas(db)
    auditorias = db.execute(
        """
        SELECT id, COALESCE(fecha_auditoria,''), COALESCE(cliente,''), COALESCE(obra,''), COALESCE(proyecto,''), COALESCE(creado_por,'')
        FROM auditorias_obra
        ORDER BY id DESC
        LIMIT 20
        """
    ).fetchall()

    opts_html = '<option value="">Seleccionar obra...</option>'
    for obra_opt, cliente_opt, titulo_opt in obras:
        opts_html += (
            f'<option value="{_e(obra_opt)}" '
            f'data-cliente="{_e(cliente_opt)}" '
            f'data-proyecto="{_e(titulo_opt)}">'
            f'{_e(obra_opt)}</option>'
        )

    categorias_options_html = "".join(
        f'<option value="{_e(cat)}">{_e(cat)}</option>' for cat, _ in CATEGORIAS_OBSERVACION
    )

    cat_ayuda_html = "".join(
        f"<tr><td>{_e(cat)}</td><td>{_e(ej)}</td></tr>" for cat, ej in CATEGORIAS_OBSERVACION
    )

    auditorias_html = ""
    for aid, fecha, cliente, obra, proyecto, creado_por in auditorias:
        auditorias_html += (
            "<tr>"
            f"<td>{int(aid)}</td>"
            f"<td>{_e(fecha)}</td>"
            f"<td>{_e(cliente)}</td>"
            f"<td>{_e(obra)}</td>"
            f"<td>{_e(proyecto)}</td>"
            f"<td>{_e(creado_por)}</td>"
            f"<td><a href=\"/modulo/auditoria-obra/pdf/{int(aid)}\" class=\"btn-link\" style=\"color:#dc2626;\">⬇ PDF</a></td>"
            "</tr>"
        )

    msg_html = f'<div class="ok">{_e(mensaje)}</div>' if mensaje else ""
    err_html = f'<div class="err">{_e(error)}</div>' if error else ""

    today = datetime.now().strftime("%Y-%m-%d")
    realizado_por_default = str(session.get("nombre") or session.get("username") or "")

    evaluacion_options_html = "".join(
      f'<option value="{_e(v)}">{_e(v or "-")}</option>' for v in EVALUACION_OPCIONES
    )
    evaluacion_rows_html = ""
    for key, label in EVALUACION_ASPECTOS:
      evaluacion_rows_html += (
        "<tr>"
        f"<td>{_e(label)}</td>"
        f"<td><select name=\"eval_{_e(key)}\">{evaluacion_options_html}</select></td>"
        "</tr>"
      )

    return f"""
    <html>
    <head>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <title>Auditoria de Obra</title>
      <style>
        * {{ box-sizing: border-box; }}
        body {{ margin:0; font-family: Arial, sans-serif; background:#f3f4f6; color:#111827; padding:16px; }}
        .wrap {{ max-width: 1250px; margin: 0 auto; }}
        .card {{ background:#fff; border:1px solid #e5e7eb; border-radius:10px; padding:14px; margin-bottom:12px; }}
        .report-header {{
          background:#f3f4f6;
          border-top:4px solid #ea580c;
          border-bottom:3px solid #cbd5e1;
          border-left:1px solid #e5e7eb;
          border-right:1px solid #e5e7eb;
          border-radius:0;
          padding:14px 16px;
          margin-bottom:12px;
          display:grid;
          grid-template-columns: 70px 1fr auto;
          gap:12px;
          align-items:center;
        }}
        .report-header img {{ width:58px; height:auto; display:block; }}
        .report-title {{ font-size:34px; margin:0 0 4px 0; font-weight:800; color:#1f2937; text-transform:uppercase; letter-spacing:0.3px; }}
        .report-subtitle {{ font-size:18px; color:#374151; margin:0; }}
        .report-right {{ text-align:right; }}
        .report-chip {{
          display:inline-block;
          font-size:12px;
          font-weight:800;
          color:#b45309;
          border:1px solid #fdba74;
          background:#fff7ed;
          border-radius:999px;
          padding:6px 12px;
          margin-bottom:6px;
        }}
        .report-date {{ font-size:15px; color:#374151; font-weight:700; }}
        h1 {{ margin:0 0 10px 0; font-size:22px; }}
        h2 {{ margin:0 0 10px 0; font-size:17px; color:#1f2937; }}
        .muted {{ color:#6b7280; font-size:13px; }}
        .grid4 {{ display:grid; grid-template-columns: repeat(4, minmax(180px, 1fr)); gap:10px; }}
        .grid3 {{ display:grid; grid-template-columns: repeat(3, minmax(180px, 1fr)); gap:10px; }}
        label {{ display:block; font-size:12px; color:#374151; margin-bottom:4px; font-weight:700; }}
        input, select, textarea {{ width:100%; padding:9px 10px; border:1px solid #d1d5db; border-radius:8px; font:inherit; }}
        textarea {{ min-height:90px; resize:vertical; }}
        table {{ width:100%; border-collapse: collapse; }}
        th, td {{ border:1px solid #e5e7eb; padding:8px; vertical-align:top; text-align:left; }}
        th {{ background:#f9fafb; }}
        .actions {{ display:flex; gap:10px; flex-wrap:wrap; }}
        button {{ border:0; border-radius:8px; padding:10px 12px; cursor:pointer; font-weight:700; }}
        .btn-main {{ background:#0f766e; color:#fff; }}
        .btn-secondary {{ background:#1d4ed8; color:#fff; }}
        .btn-light {{ background:#e5e7eb; color:#111827; text-decoration:none; display:inline-block; }}
        .btn-link {{ text-decoration:none; color:#1d4ed8; font-weight:700; }}
        .ok {{ background:#dcfce7; border:1px solid #86efac; color:#166534; border-radius:8px; padding:10px; margin-bottom:10px; }}
        .err {{ background:#fee2e2; border:1px solid #fecaca; color:#991b1b; border-radius:8px; padding:10px; margin-bottom:10px; }}
        .obs-row textarea {{ min-height:65px; }}
        @media (max-width: 980px) {{
          .report-header {{ grid-template-columns: 1fr; text-align:center; }}
          .report-header img {{ margin:0 auto; }}
          .report-right {{ text-align:center; }}
          .report-title {{ font-size:24px; }}
          .report-subtitle {{ font-size:15px; }}
          .grid4, .grid3 {{ grid-template-columns: 1fr; }}
          body {{ padding:10px; }}
        }}
      </style>
    </head>
    <body>
      <div class="wrap">
        <a href="/" class="btn-light" style="margin-bottom:10px;">Volver al panel</a>
        <div class="report-header">
          <div>
            <img src="/logo-a3" alt="A3">
          </div>
          <div>
            <p class="report-title">Informe de Auditoria de Obra</p>
            <p class="report-subtitle">Carga y seguimiento de hallazgos en campo</p>
          </div>
          <div class="report-right">
            <span class="report-chip">INFORME INTERNO</span>
            <div class="report-date">{_e(datetime.now().strftime("%d %b %Y"))}</div>
          </div>
        </div>
        <div class="card">
          <h1>Auditoria de Obra</h1>
          <p class="muted">Módulo exclusivo para administrador. Carga la auditoría, adjunta fotos por observación y descargá directamente en PDF.</p>
          {msg_html}
          {err_html}

          <form method="post" enctype="multipart/form-data">
            <input type="hidden" name="accion" id="accion" value="guardar">

            <div class="card" style="border-style:dashed;">
              <h2>Encabezado</h2>
              <div class="grid4">
                <div>
                  <label>Fecha de auditoría</label>
                  <input type="date" name="fecha_auditoria" value="{_e(today)}" required>
                </div>
                <div>
                  <label>Obra</label>
                  <select name="obra" id="obra_select" required onchange="_syncObra(this)">{opts_html}</select>
                </div>
                <div>
                  <label>Cliente</label>
                  <input type="text" name="cliente" id="cliente" readonly style="background:#f9fafb;">
                </div>
                <div>
                  <label>Proyecto</label>
                  <input type="text" name="proyecto" id="proyecto" placeholder="Completar manualmente">
                </div>
                <div>
                  <label>Realizado por</label>
                  <input type="text" name="realizado_por" value="{_e(realizado_por_default)}" placeholder="Nombre y apellido" required>
                </div>
              </div>
              <input type="hidden" name="ot_id" value="">
            </div>

            <div class="card">
              <h2>Seccion 1: RESUMEN</h2>
              <textarea name="resumen" placeholder="Texto general de la visita, alcance y principales desvíos detectados." required></textarea>
            </div>

            <div class="card">
              <h2>Seccion 2 y 3: Tabla de observaciones + detalle OBS</h2>
              <p class="muted">Cada fila genera una observacion de tabla y su bloque en la seccion OBSERVACIONES.</p>
              <div style="overflow-x:auto;">
                <table id="tabla_obs">
                  <thead>
                    <tr>
                      <th style="width:150px;">Punto Analizado</th>
                      <th style="width:240px;">Observacion</th>
                      <th style="width:120px;">Estado</th>
                      <th style="width:120px;">Informado en Obra</th>
                      <th style="width:150px;">Categoria</th>
                      <th style="width:230px;">Comentario</th>
                      <th style="width:140px;">Foto 1</th>
                      <th style="width:140px;">Foto 2</th>
                      <th style="width:140px;">Foto 3</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr class="obs-row">
                      <td><input name="obs_punto[]" placeholder="Ej: Soldaduras"></td>
                      <td><textarea name="obs_texto[]" placeholder="Detalle observado"></textarea></td>
                      <td>
                        <select name="obs_estado[]">
                          <option value="">-</option>
                          <option value="Conforme">Conforme</option>
                          <option value="Corregir">Corregir</option>
                        </select>
                      </td>
                      <td>
                        <select name="obs_informado[]">
                          <option value="Si">Si</option>
                          <option value="No">No</option>
                        </select>
                      </td>
                      <td>
                        <select name="obs_categoria[]">
                          <option value="">-</option>
                          {categorias_options_html}
                        </select>
                      </td>
                      <td><textarea name="obs_comentario[]" placeholder="Comentario"></textarea></td>
                      <td><input type="file" name="obs_foto1[]" accept="image/*" style="font-size:11px;padding:4px;"></td>
                      <td><input type="file" name="obs_foto2[]" accept="image/*" style="font-size:11px;padding:4px;"></td>
                      <td><input type="file" name="obs_foto3[]" accept="image/*" style="font-size:11px;padding:4px;"></td>
                    </tr>
                  </tbody>
                </table>
              </div>
              <div style="margin-top:10px;">
                <button type="button" class="btn-light" id="btn_add_obs">Agregar observacion</button>
              </div>
            </div>

            <div class="card">
              <h2>Seccion 4: ASPECTOS POSITIVOS</h2>
              <p class="muted">Completá los puntos positivos. Luego se mostrarán como items en el PDF.</p>
              <div style="overflow-x:auto;">
                <table id="tabla_aspectos_positivos">
                  <thead>
                    <tr><th>Item</th></tr>
                  </thead>
                  <tbody>
                    <tr class="ap-row"><td><input name="aspecto_positivo[]" placeholder="Buen orden general de la obra"></td></tr>
                    <tr><td><input name="aspecto_positivo[]" placeholder="Documentación completa y disponible"></td></tr>
                    <tr><td><input name="aspecto_positivo[]" placeholder="Personal con EPP correcto"></td></tr>
                    <tr><td><input name="aspecto_positivo[]" placeholder=""></td></tr>
                  </tbody>
                </table>
              </div>
              <div style="margin-top:10px;">
                <button type="button" class="btn-light" id="btn_add_aspecto">Sumar item</button>
              </div>
            </div>

            <div class="card">
              <h2>Seccion 5: ACCIONES PENDIENTES</h2>
              <p class="muted">Completa acción, responsable y fecha de compromiso.</p>
              <div style="overflow-x:auto;">
                <table id="tabla_acciones_pendientes">
                  <thead>
                    <tr>
                      <th style="width:50%;">Acción</th>
                      <th style="width:25%;">Responsable</th>
                      <th style="width:25%;">Fecha compromiso</th>
                    </tr>
                  </thead>
                  <tbody>
                    <tr class="accion-row">
                      <td><input name="accion_pendiente[]" placeholder="Completar ajuste de pernos"></td>
                      <td><input name="responsable_pendiente[]" placeholder="Juan Pérez"></td>
                      <td><input type="date" name="fecha_compromiso[]"></td>
                    </tr>
                    <tr>
                      <td><input name="accion_pendiente[]" placeholder="Revisar limpieza de frente"></td>
                      <td><input name="responsable_pendiente[]" placeholder=""></td>
                      <td><input type="date" name="fecha_compromiso[]"></td>
                    </tr>
                    <tr>
                      <td><input name="accion_pendiente[]" placeholder=""></td>
                      <td><input name="responsable_pendiente[]" placeholder=""></td>
                      <td><input type="date" name="fecha_compromiso[]"></td>
                    </tr>
                  </tbody>
                </table>
              </div>
              <div style="margin-top:10px;">
                <button type="button" class="btn-light" id="btn_add_accion">Sumar item</button>
              </div>
            </div>

            <div class="card">
              <h2>Seccion 6: EVALUACION GENERAL (semaforo)</h2>
              <div style="overflow-x:auto;">
                <table>
                  <thead>
                    <tr>
                      <th>Aspecto</th>
                      <th>Evaluación</th>
                    </tr>
                  </thead>
                  <tbody>
                    {evaluacion_rows_html}
                  </tbody>
                </table>
              </div>
            </div>

            <div class="actions">
              <button class="btn-main" type="submit" onclick="document.getElementById('accion').value='guardar';">Guardar informe</button>
              <button class="btn-secondary" type="submit" onclick="document.getElementById('accion').value='guardar_pdf';" style="background:#dc2626;">⬇ Guardar y descargar PDF</button>
            </div>
          </form>
        </div>

        <div class="card">
          <h2>Ultimos informes</h2>
          <div style="overflow-x:auto;">
            <table>
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Fecha</th>
                  <th>Cliente</th>
                  <th>Obra</th>
                  <th>Proyecto</th>
                  <th>Creado por</th>
                  <th>PDF</th>
                </tr>
              </thead>
              <tbody>{auditorias_html or '<tr><td colspan="7">Sin auditorias cargadas.</td></tr>'}</tbody>
            </table>
          </div>
        </div>
      </div>

      <script>
        function _syncObra(sel) {{
          const opt = sel.options[sel.selectedIndex];
          if (!opt) return;
          document.getElementById('cliente').value = opt.getAttribute('data-cliente') || '';
        }}
        (function() {{
          // Auto-sync on load if only one obra
          const sel = document.getElementById('obra_select');
          if (sel && sel.options.length === 2) {{
            sel.selectedIndex = 1;
            _syncObra(sel);
          }}

          const addBtn = document.getElementById("btn_add_obs");
          const tbody = document.querySelector("#tabla_obs tbody");
          addBtn.addEventListener("click", function() {{
            const base = tbody.querySelector("tr.obs-row");
            if (!base) return;
            const tr = base.cloneNode(true);
            tr.querySelectorAll("input, textarea").forEach(function(el) {{ el.value = ""; }});
            tr.querySelectorAll("select").forEach(function(el) {{ el.selectedIndex = 0; }});
            tbody.appendChild(tr);
          }});

          const btnAspecto = document.getElementById("btn_add_aspecto");
          const tbodyAspecto = document.querySelector("#tabla_aspectos_positivos tbody");
          btnAspecto.addEventListener("click", function() {{
            const base = tbodyAspecto.querySelector("tr.ap-row") || tbodyAspecto.querySelector("tr");
            if (!base) return;
            const tr = base.cloneNode(true);
            tr.classList.add("ap-row");
            tr.querySelectorAll("input").forEach(function(el) {{ el.value = ""; }});
            tbodyAspecto.appendChild(tr);
          }});

          const btnAccion = document.getElementById("btn_add_accion");
          const tbodyAccion = document.querySelector("#tabla_acciones_pendientes tbody");
          btnAccion.addEventListener("click", function() {{
            const base = tbodyAccion.querySelector("tr.accion-row") || tbodyAccion.querySelector("tr");
            if (!base) return;
            const tr = base.cloneNode(true);
            tr.classList.add("accion-row");
            tr.querySelectorAll("input").forEach(function(el) {{ el.value = ""; }});
            tbodyAccion.appendChild(tr);
          }});
        }})();
      </script>
    </body>
    </html>
    """


@auditoria_obra_bp.route("/pdf/<int:auditoria_id>", methods=["GET"])
def descargar_pdf_auditoria(auditoria_id):
    if not _es_admin_session():
        return "<h3>Sin permiso para descargar este informe.</h3>", 403

    db = get_db()
    _ensure_schema(db)

    row = db.execute(
        """
        SELECT id, COALESCE(cliente,''), COALESCE(obra,''), COALESCE(proyecto,''),
               COALESCE(fecha_auditoria,''), COALESCE(resumen,''),
               COALESCE(aspectos_positivos,''), COALESCE(acciones_pendientes,''),
           COALESCE(observaciones_json,'[]'), COALESCE(evaluacion_json,'[]'),
           COALESCE(realizado_por,''), COALESCE(creado_por,'')
        FROM auditorias_obra
        WHERE id = ?
        LIMIT 1
        """,
        (auditoria_id,),
    ).fetchone()

    if not row:
        return "Auditoria no encontrada", 404

    aid, cliente, obra, proyecto, fecha_auditoria, resumen, aspectos_positivos, acciones_pendientes, observaciones_json, evaluacion_json, realizado_por, creado_por = row

    try:
        observaciones = json.loads(observaciones_json or "[]")
        if not isinstance(observaciones, list):
            observaciones = []
    except Exception:
        observaciones = []

    try:
      evaluaciones = json.loads(evaluacion_json or "[]")
      if not isinstance(evaluaciones, list):
        evaluaciones = []
    except Exception:
      evaluaciones = []

    auditoria = {
        "cliente": cliente, "obra": obra, "proyecto": proyecto,
        "fecha_auditoria": fecha_auditoria, "resumen": resumen,
        "aspectos_positivos": aspectos_positivos,
        "acciones_pendientes": acciones_pendientes,
      "realizado_por": realizado_por,
        "creado_por": creado_por,
    }
    pdf_buf = _build_pdf_bytes(auditoria, observaciones, evaluaciones)
    nombre = f"auditoria_obra_{int(aid):04d}_{datetime.now().strftime('%Y%m%d')}.pdf"
    return send_file(pdf_buf, as_attachment=True, download_name=nombre, mimetype="application/pdf")
