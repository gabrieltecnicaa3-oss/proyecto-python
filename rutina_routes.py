from flask import Blueprint, redirect, request, session
import html as html_lib

from db_utils import get_db

rutina_bp = Blueprint("rutina", __name__)

PERIODOS = ("DIARIO", "SEMANAL", "MENSUAL")
ESTADOS = ("pendiente", "en_curso", "completada")

DEFAULT_ITEMS = {
    "DIARIO": [
        "Revisar OTs en rojo y amarillo del tablero",
        "Identificar bloqueo principal por OT critica",
        "Asignar responsable y compromiso del dia",
        "Registrar accion en observaciones de programacion",
    ],
    "SEMANAL": [
        "Comparar plan vs real por OT y por tipo",
        "Validar desvio semanal y tendencia",
        "Reasignar recursos a OTs de mayor impacto",
        "Definir 3 acciones para la semana siguiente",
    ],
    "MENSUAL": [
        "Revisar causas repetidas de desvio",
        "Ajustar horas previstas y umbrales",
        "Definir mejoras de proceso y fecha",
        "Comunicar cambios de criterio a supervisores",
    ],
}


def _seed_items_if_needed(db):
    for periodo in PERIODOS:
        row = db.execute(
            """
            SELECT COUNT(1)
            FROM rutina_control_items
            WHERE UPPER(TRIM(COALESCE(periodo, ''))) = ?
              AND COALESCE(activo, 1) = 1
            """,
            (periodo,),
        ).fetchone()
        total = int(row[0] or 0) if row else 0
        if total > 0:
            continue
        for idx, titulo in enumerate(DEFAULT_ITEMS.get(periodo, []), start=1):
            db.execute(
                """
                INSERT INTO rutina_control_items
                (periodo, titulo, detalle, responsable, estado, fecha_objetivo, orden, activo, actualizado_por)
                VALUES (?, ?, ?, ?, 'pendiente', NULL, ?, 1, ?)
                """,
                (periodo, titulo, "", "", idx, "sistema"),
            )
    db.commit()


def _cargar_items(db):
    rows = db.execute(
        """
        SELECT id,
               UPPER(TRIM(COALESCE(periodo, ''))),
               COALESCE(titulo, ''),
               COALESCE(detalle, ''),
               COALESCE(responsable, ''),
               LOWER(TRIM(COALESCE(estado, 'pendiente'))),
               COALESCE(fecha_objetivo, ''),
               COALESCE(orden, 0),
               COALESCE(actualizado_por, ''),
               COALESCE(fecha_actualizacion, '')
        FROM rutina_control_items
        WHERE COALESCE(activo, 1) = 1
        ORDER BY
            CASE UPPER(TRIM(COALESCE(periodo, '')))
                WHEN 'DIARIO' THEN 1
                WHEN 'SEMANAL' THEN 2
                WHEN 'MENSUAL' THEN 3
                ELSE 9
            END,
            COALESCE(orden, 0),
            id
        """
    ).fetchall()
    data = {"DIARIO": [], "SEMANAL": [], "MENSUAL": []}
    for row in rows:
        periodo = str(row[1] or "").strip().upper()
        if periodo not in data:
            continue
        data[periodo].append(
            {
                "id": int(row[0]),
                "titulo": str(row[2] or ""),
                "detalle": str(row[3] or ""),
                "responsable": str(row[4] or ""),
                "estado": str(row[5] or "pendiente"),
                "fecha_objetivo": str(row[6] or ""),
                "actualizado_por": str(row[8] or ""),
                "fecha_actualizacion": str(row[9] or ""),
            }
        )
    return data


def _resumen_periodo(items):
    total = len(items)
    pendientes = sum(1 for i in items if i.get("estado") == "pendiente")
    en_curso = sum(1 for i in items if i.get("estado") == "en_curso")
    completas = sum(1 for i in items if i.get("estado") == "completada")
    return {
        "total": total,
        "pendientes": pendientes,
        "en_curso": en_curso,
        "completadas": completas,
    }


@rutina_bp.route("/modulo/rutina-control", methods=["GET", "POST"])
def rutina_control():
    db = get_db()
    _seed_items_if_needed(db)

    user_name = str(session.get("nombre") or session.get("username") or "admin").strip() or "admin"

    if request.method == "POST":
        accion = str(request.form.get("accion") or "").strip().lower()

        if accion == "agregar":
            periodo = str(request.form.get("periodo") or "").strip().upper()
            titulo = str(request.form.get("titulo") or "").strip()
            detalle = str(request.form.get("detalle") or "").strip()
            responsable = str(request.form.get("responsable") or "").strip()
            fecha_objetivo = str(request.form.get("fecha_objetivo") or "").strip()

            if periodo in PERIODOS and titulo:
                row_orden = db.execute(
                    """
                    SELECT COALESCE(MAX(orden), 0)
                    FROM rutina_control_items
                    WHERE UPPER(TRIM(COALESCE(periodo, ''))) = ?
                      AND COALESCE(activo, 1) = 1
                    """,
                    (periodo,),
                ).fetchone()
                nuevo_orden = int(row_orden[0] or 0) + 1
                db.execute(
                    """
                    INSERT INTO rutina_control_items
                    (periodo, titulo, detalle, responsable, estado, fecha_objetivo, orden, activo, actualizado_por, fecha_actualizacion)
                    VALUES (?, ?, ?, ?, 'pendiente', ?, ?, 1, ?, CURRENT_TIMESTAMP)
                    """,
                    (periodo, titulo, detalle, responsable, fecha_objetivo or None, nuevo_orden, user_name),
                )
                db.commit()

        elif accion == "guardar":
            item_id = str(request.form.get("item_id") or "").strip()
            estado = str(request.form.get("estado") or "pendiente").strip().lower()
            responsable = str(request.form.get("responsable") or "").strip()
            fecha_objetivo = str(request.form.get("fecha_objetivo") or "").strip()
            detalle = str(request.form.get("detalle") or "").strip()
            titulo = str(request.form.get("titulo") or "").strip()

            if item_id.isdigit() and estado in ESTADOS and titulo:
                db.execute(
                    """
                    UPDATE rutina_control_items
                    SET titulo = ?,
                        detalle = ?,
                        responsable = ?,
                        estado = ?,
                        fecha_objetivo = ?,
                        actualizado_por = ?,
                        fecha_actualizacion = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (titulo, detalle, responsable, estado, fecha_objetivo or None, user_name, int(item_id)),
                )
                db.commit()

        elif accion == "archivar":
            item_id = str(request.form.get("item_id") or "").strip()
            if item_id.isdigit():
                db.execute(
                    """
                    UPDATE rutina_control_items
                    SET activo = 0,
                        actualizado_por = ?,
                        fecha_actualizacion = CURRENT_TIMESTAMP
                    WHERE id = ?
                    """,
                    (user_name, int(item_id)),
                )
                db.commit()

        return redirect("/modulo/rutina-control")

    data = _cargar_items(db)
    resumen = {k: _resumen_periodo(v) for k, v in data.items()}

    def color_chip(estado):
        if estado == "completada":
            return "chip-ok", "Completada"
        if estado == "en_curso":
            return "chip-warn", "En curso"
        return "chip-neutral", "Pendiente"

    def render_periodo(periodo, subtitulo):
        tarjetas = []
        for item in data.get(periodo, []):
            estado = item.get("estado", "pendiente")
            estado_cls, estado_txt = color_chip(estado)
            tarjetas.append(
                f"""
                <form method="post" class="item-card">
                  <input type="hidden" name="accion" value="guardar">
                  <input type="hidden" name="item_id" value="{item['id']}">
                  <div class="item-top">
                    <span class="chip {estado_cls}">{estado_txt}</span>
                    <span class="meta-mini">Actualizo: {html_lib.escape(item.get('actualizado_por') or '-')}</span>
                  </div>
                  <label class="lb">Titulo</label>
                  <input class="inp" name="titulo" value="{html_lib.escape(item.get('titulo') or '')}" required>
                  <label class="lb">Detalle</label>
                  <textarea class="inp" name="detalle" rows="2">{html_lib.escape(item.get('detalle') or '')}</textarea>
                  <div class="row-2">
                    <div>
                      <label class="lb">Responsable</label>
                      <input class="inp" name="responsable" value="{html_lib.escape(item.get('responsable') or '')}" placeholder="Nombre responsable">
                    </div>
                    <div>
                      <label class="lb">Fecha objetivo</label>
                      <input class="inp" type="date" name="fecha_objetivo" value="{html_lib.escape(item.get('fecha_objetivo') or '')}">
                    </div>
                  </div>
                  <label class="lb">Estado</label>
                  <select class="inp" name="estado">
                    <option value="pendiente" {"selected" if estado == "pendiente" else ""}>Pendiente</option>
                    <option value="en_curso" {"selected" if estado == "en_curso" else ""}>En curso</option>
                    <option value="completada" {"selected" if estado == "completada" else ""}>Completada</option>
                  </select>
                  <div class="actions-row">
                    <button class="btn btn-save" type="submit">Guardar</button>
                    <button class="btn btn-archive" type="submit" name="accion" value="archivar" onclick="return confirm('Archivar item?');">Archivar</button>
                  </div>
                </form>
                """
            )

        r = resumen.get(periodo, {})
        return f"""
        <section class="periodo-card">
          <div class="periodo-head">
            <div>
              <h2>{periodo}</h2>
              <p>{subtitulo}</p>
            </div>
            <div class="kpis">
              <span>Total: <b>{int(r.get('total', 0))}</b></span>
              <span>Pend: <b>{int(r.get('pendientes', 0))}</b></span>
              <span>Curso: <b>{int(r.get('en_curso', 0))}</b></span>
              <span>Comp: <b>{int(r.get('completadas', 0))}</b></span>
            </div>
          </div>
          <div class="items-grid">
            {''.join(tarjetas) if tarjetas else '<div class="empty">Sin items activos en este periodo.</div>'}
          </div>
        </section>
        """

    return f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * {{ box-sizing: border-box; }}
    body {{
        margin: 0;
        font-family: "Segoe UI", Tahoma, Arial, sans-serif;
        background:
            radial-gradient(circle at 15% 0%, #d1fae5 0%, rgba(209,250,229,0) 45%),
            radial-gradient(circle at 100% 10%, #a7f3d0 0%, rgba(167,243,208,0) 38%),
            linear-gradient(145deg, #f0fdfa 0%, #eff6ff 42%, #f8fafc 100%);
        color: #0f172a;
        padding: 16px;
    }}
    .wrap {{ max-width: 1200px; margin: 0 auto; }}
    .hero {{
        background: linear-gradient(130deg, #0f766e 0%, #0d9488 60%, #2dd4bf 100%);
        color: #fff;
        border-radius: 16px;
        padding: 16px;
        box-shadow: 0 12px 26px rgba(15, 118, 110, 0.22);
        margin-bottom: 12px;
    }}
    .hero h1 {{ margin: 0 0 6px 0; font-size: 1.7rem; }}
    .hero p {{ margin: 0; opacity: 0.95; }}
    .top-actions {{ display: flex; gap: 8px; flex-wrap: wrap; margin: 10px 0 12px 0; }}
    .btn {{
        border: 0;
        border-radius: 10px;
        padding: 9px 12px;
        font-weight: 700;
        cursor: pointer;
        text-decoration: none;
        display: inline-block;
    }}
    .btn-home {{ background: #0f766e; color: #fff; }}
    .btn-soft {{ background: #ecfeff; color: #0f766e; border: 1px solid #99f6e4; }}
    .new-card {{
        background: #fff;
        border: 1px solid #dbeafe;
        border-radius: 14px;
        padding: 12px;
        box-shadow: 0 8px 18px rgba(15,23,42,0.06);
        margin-bottom: 12px;
    }}
    .new-grid {{ display: grid; grid-template-columns: 180px 1fr 1fr 180px; gap: 8px; }}
    .inp {{ width: 100%; border: 1px solid #cbd5e1; border-radius: 8px; padding: 8px 10px; font: inherit; }}
    .periodo-card {{
        background: #ffffff;
        border: 1px solid #dbeafe;
        border-radius: 14px;
        padding: 12px;
        margin-bottom: 12px;
        box-shadow: 0 8px 18px rgba(15,23,42,0.06);
    }}
    .periodo-head {{ display: flex; justify-content: space-between; gap: 8px; flex-wrap: wrap; margin-bottom: 8px; }}
    .periodo-head h2 {{ margin: 0; }}
    .periodo-head p {{ margin: 2px 0 0 0; color: #475569; font-size: 0.9rem; }}
    .kpis {{ display: flex; gap: 10px; flex-wrap: wrap; font-size: 0.85rem; color: #334155; }}
    .items-grid {{ display: grid; gap: 8px; grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    .item-card {{ border: 1px solid #e2e8f0; border-radius: 12px; padding: 10px; background: #f8fafc; }}
    .item-top {{ display: flex; justify-content: space-between; gap: 6px; align-items: center; margin-bottom: 6px; }}
    .chip {{ display: inline-block; border-radius: 999px; font-size: 0.75rem; font-weight: 700; padding: 4px 10px; }}
    .chip-ok {{ background: #dcfce7; color: #166534; }}
    .chip-warn {{ background: #ffedd5; color: #9a3412; }}
    .chip-neutral {{ background: #e2e8f0; color: #334155; }}
    .meta-mini {{ font-size: 0.75rem; color: #64748b; }}
    .lb {{ display: block; margin: 6px 0 4px 0; font-size: 0.78rem; color: #475569; font-weight: 700; }}
    .row-2 {{ display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }}
    .actions-row {{ margin-top: 8px; display: flex; gap: 8px; }}
    .btn-save {{ background: #0f766e; color: #fff; }}
    .btn-archive {{ background: #dc2626; color: #fff; }}
    .empty {{ color: #64748b; font-size: 0.9rem; padding: 10px; }}
    @media (max-width: 980px) {{
        .new-grid {{ grid-template-columns: 1fr; }}
        .items-grid {{ grid-template-columns: 1fr; }}
        .row-2 {{ grid-template-columns: 1fr; }}
    }}
    </style>
    </head>
    <body>
      <div class="wrap">
        <div class="hero">
          <h1>Rutina de Control de Avance</h1>
          <p>Checklist operativa persistente: responsable, estado y fecha por cada accion.</p>
        </div>

        <div class="top-actions">
          <a class="btn btn-home" href="/">Volver al panel</a>
          <a class="btn btn-soft" href="/modulo/programacion">Ir a Programacion</a>
        </div>

        <form method="post" class="new-card">
          <input type="hidden" name="accion" value="agregar">
          <div class="new-grid">
            <select class="inp" name="periodo" required>
              <option value="DIARIO">DIARIO</option>
              <option value="SEMANAL">SEMANAL</option>
              <option value="MENSUAL">MENSUAL</option>
            </select>
            <input class="inp" name="titulo" placeholder="Nueva accion" required>
            <input class="inp" name="responsable" placeholder="Responsable">
            <input class="inp" type="date" name="fecha_objetivo">
          </div>
          <div style="margin-top:8px; display:grid; grid-template-columns: 1fr auto; gap:8px;">
            <input class="inp" name="detalle" placeholder="Detalle de la accion">
            <button class="btn btn-save" type="submit">Agregar item</button>
          </div>
        </form>

        {render_periodo("DIARIO", "Ritual de inicio de turno (10-15 min)")}
        {render_periodo("SEMANAL", "Control de cumplimiento (viernes)")}
        {render_periodo("MENSUAL", "Revision de capacidad y mejora")}
      </div>
    </body>
    </html>
    """
