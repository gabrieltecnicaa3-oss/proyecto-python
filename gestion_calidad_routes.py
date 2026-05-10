from datetime import date, timedelta
import html as html_lib
from urllib.parse import quote

from flask import Blueprint, redirect, request

from db_utils import get_db


gestion_calidad_bp = Blueprint("gestion_calidad", __name__)
_gestion_calidad_schema_ready = False


def _asegurar_schema_gestion_calidad():
    global _gestion_calidad_schema_ready
    if _gestion_calidad_schema_ready:
        return

    db = get_db()

    try:
        db.execute(
            """
            CREATE TABLE IF NOT EXISTS hallazgos_calidad (
                id INTEGER PRIMARY KEY,
                fecha_hallazgo DATE,
                proceso TEXT,
                tipo_hallazgo TEXT,
                estado_tratamiento TEXT,
                accion_inmediata TEXT,
                acciones_correctivas TEXT,
                fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
    except Exception:
        pass

    # Columnas usadas por este modulo.
    for _sql in [
        "ALTER TABLE hallazgos_calidad ADD COLUMN requiere_causa_raiz INTEGER DEFAULT 0",
        "ALTER TABLE hallazgos_calidad ADD COLUMN porque_1 TEXT",
        "ALTER TABLE hallazgos_calidad ADD COLUMN porque_2 TEXT",
        "ALTER TABLE hallazgos_calidad ADD COLUMN porque_3 TEXT",
        "ALTER TABLE hallazgos_calidad ADD COLUMN porque_4 TEXT",
        "ALTER TABLE hallazgos_calidad ADD COLUMN porque_5 TEXT",
        "ALTER TABLE hallazgos_calidad ADD COLUMN clasificacion_causa TEXT",
        "ALTER TABLE hallazgos_calidad ADD COLUMN genero_retrabajo INTEGER DEFAULT 0",
        "ALTER TABLE hallazgos_calidad ADD COLUMN retrabajo_hs REAL DEFAULT 0",
        "ALTER TABLE hallazgos_calidad ADD COLUMN retrabajo_proceso_afectado TEXT",
        "ALTER TABLE hallazgos_calidad ADD COLUMN retrabajo_impacto TEXT",
        "ALTER TABLE hallazgos_calidad ADD COLUMN desperdicio_kg REAL DEFAULT 0",
        "ALTER TABLE hallazgos_calidad ADD COLUMN impacto_entrega_dias REAL DEFAULT 0",
        "ALTER TABLE hallazgos_calidad ADD COLUMN costo_hallazgo REAL DEFAULT 0",
        "ALTER TABLE procesos ADD COLUMN eliminado INTEGER DEFAULT 0",
    ]:
        try:
            db.execute(_sql)
        except Exception:
            pass

    try:
        db.commit()
    except Exception:
        pass

    _gestion_calidad_schema_ready = True


@gestion_calidad_bp.before_request
def _gestion_calidad_before_request_schema():
    _asegurar_schema_gestion_calidad()


@gestion_calidad_bp.route("/modulo/calidad")
def calidad():
    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * { box-sizing: border-box; }
    body { font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }
    h2 { color: #333; border-bottom: 3px solid #4facfe; padding-bottom: 10px; }
    .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
    .btn-home { background: #667eea; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; }
    .btn-home:hover { background: #5568d3; }
    .cards-container { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-top: 30px; }
    .card { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 5px rgba(0,0,0,0.1);
            text-align: center; text-decoration: none; color: inherit; transition: all 0.3s; }
    .card:hover { transform: translateY(-5px); box-shadow: 0 5px 15px rgba(0,0,0,0.2); }
    .card-icon { font-size: 48px; margin-bottom: 10px; }
    .card h3 { color: #333; margin: 10px 0; }
    .card p { color: #666; font-size: 14px; }
    .card.recepcion { border-top: 4px solid #4facfe; }
    .card.escaneo { border-top: 4px solid #43e97b; }
    .card.despacho { border-top: 4px solid #fa709a; }
    </style>
    </head>
    <body>
    <div class="header">
        <h2>🧪 Módulo de Calidad</h2>
        <a href="/" class="btn-home">⬅️ Volver</a>
    </div>

    <p style="background: #e3f2fd; padding: 15px; border-radius: 5px; color: #0d47a1;">
        <strong>📋 Descripción:</strong> Gestión integral de calidad con 3 sub-módulos:
        Control de Recepción, Escaneo de Producción y Control de Despacho.
    </p>

    <div class="cards-container">
        <a href="/modulo/calidad/recepcion" class="card recepcion">
            <div class="card-icon">📋</div>
            <h3>Control Recepción</h3>
            <p>Registrar materiales y componentes recibidos en almacén</p>
        </a>

        <a href="/modulo/calidad/escaneo" class="card escaneo">
            <div class="card-icon">📱</div>
            <h3>Control Produccion - Escaneo QR</h3>
            <p>Escanear códigos QR de piezas durante producción</p>
        </a>

        <a href="/modulo/calidad/despacho" class="card despacho">
            <div class="card-icon">📦</div>
            <h3>Control Despacho</h3>
            <p>Verificar piezas completadas listas para enviar</p>
        </a>
    </div>
    </body>
    </html>
    """
    return html


@gestion_calidad_bp.route("/modulo/gestion-calidad", methods=["GET", "POST"])
def gestion_calidad_dashboard():
    db = get_db()
    if request.method == "POST":
        def _to_float(value):
            txt = str(value or "").strip().replace(",", ".")
            if not txt:
                return 0.0
            try:
                return float(txt)
            except Exception:
                return 0.0

        periodo_post = (request.form.get("periodo") or "mensual").strip().lower()
        if periodo_post not in ("mensual", "trimestral", "semestral"):
            periodo_post = "mensual"

        fecha_hallazgo = (request.form.get("fecha_hallazgo") or date.today().isoformat()).strip()
        proceso_h = (request.form.get("proceso_h") or "").strip().upper()
        tipo_hallazgo = (request.form.get("tipo_hallazgo") or "").strip().upper()
        estado_tratamiento = (request.form.get("estado_tratamiento") or "").strip().upper()
        accion_inmediata = (request.form.get("accion_inmediata") or "").strip()
        acciones_correctivas = (request.form.get("acciones_correctivas") or "").strip()

        requiere_causa_raiz = 1 if (request.form.get("requiere_causa_raiz") or "") == "1" else 0
        porque_1 = (request.form.get("porque_1") or "").strip()
        porque_2 = (request.form.get("porque_2") or "").strip()
        porque_3 = (request.form.get("porque_3") or "").strip()
        porque_4 = (request.form.get("porque_4") or "").strip()
        porque_5 = (request.form.get("porque_5") or "").strip()
        clasificacion_causa = (request.form.get("clasificacion_causa") or "").strip().upper()

        genero_retrabajo = 1 if (request.form.get("genero_retrabajo") or "") == "1" else 0
        retrabajo_hs = _to_float(request.form.get("retrabajo_hs"))
        retrabajo_proceso_afectado = (request.form.get("retrabajo_proceso_afectado") or "").strip().upper()
        retrabajo_impacto = (request.form.get("retrabajo_impacto") or "").strip()
        desperdicio_kg = _to_float(request.form.get("desperdicio_kg"))
        impacto_entrega_dias = _to_float(request.form.get("impacto_entrega_dias"))
        costo_hallazgo = _to_float(request.form.get("costo_hallazgo"))

        procesos_validos = {"ARMADO", "SOLDADURA", "PINTURA", "DESPACHO"}
        tipos_validos = {"NC", "OBS", "OM"}
        estados_validos = {"ABIERTO", "EN PROCESO", "CERRADA"}
        clasificaciones_validas = {
            "MANO_DE_OBRA",
            "INGENIERIA",
            "MATERIAL",
            "METODO",
            "MAQUINA",
            "COMPRAS",
            "PAGOS_PROVEEDORES",
        }
        procesos_retrabajo_validos = {"ARMADO", "SOLDADURA", "PINTURA", "DESPACHO", "COMPRAS", "PAGOS_PROVEEDORES"}

        if proceso_h not in procesos_validos or tipo_hallazgo not in tipos_validos or estado_tratamiento not in estados_validos:
            return redirect("/modulo/gestion-calidad?periodo=" + quote(periodo_post) + "&mensaje=" + quote("⚠️ Revisá los datos del hallazgo"))
        if not accion_inmediata or not acciones_correctivas:
            return redirect("/modulo/gestion-calidad?periodo=" + quote(periodo_post) + "&mensaje=" + quote("⚠️ Completá acción inmediata y acciones correctivas"))

        if requiere_causa_raiz:
            if (not porque_1 or not porque_2 or not porque_3 or not porque_4 or not porque_5 or clasificacion_causa not in clasificaciones_validas):
                return redirect("/modulo/gestion-calidad?periodo=" + quote(periodo_post) + "&mensaje=" + quote("⚠️ Si requiere causa raíz, completá los 5 por qué y la clasificación"))
        else:
            porque_1 = ""
            porque_2 = ""
            porque_3 = ""
            porque_4 = ""
            porque_5 = ""
            clasificacion_causa = ""

        if genero_retrabajo:
            if retrabajo_hs <= 0 or not retrabajo_impacto:
                return redirect("/modulo/gestion-calidad?periodo=" + quote(periodo_post) + "&mensaje=" + quote("⚠️ Si hubo retrabajo, indicá hs e impacto"))
        else:
            retrabajo_hs = 0
            retrabajo_proceso_afectado = ""
            retrabajo_impacto = ""
            desperdicio_kg = 0
            impacto_entrega_dias = 0
            costo_hallazgo = 0

        db.execute(
            """
            INSERT INTO hallazgos_calidad (
                fecha_hallazgo,
                proceso,
                tipo_hallazgo,
                estado_tratamiento,
                accion_inmediata,
                acciones_correctivas,
                requiere_causa_raiz,
                porque_1,
                porque_2,
                porque_3,
                porque_4,
                porque_5,
                clasificacion_causa,
                genero_retrabajo,
                retrabajo_hs,
                retrabajo_proceso_afectado,
                retrabajo_impacto,
                desperdicio_kg,
                impacto_entrega_dias,
                costo_hallazgo
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                fecha_hallazgo,
                proceso_h,
                tipo_hallazgo,
                estado_tratamiento,
                accion_inmediata,
                acciones_correctivas,
                requiere_causa_raiz,
                porque_1,
                porque_2,
                porque_3,
                porque_4,
                porque_5,
                clasificacion_causa,
                genero_retrabajo,
                retrabajo_hs,
                retrabajo_proceso_afectado,
                retrabajo_impacto,
                desperdicio_kg,
                impacto_entrega_dias,
                costo_hallazgo,
            )
        )
        db.commit()
        return redirect("/modulo/gestion-calidad?periodo=" + quote(periodo_post) + "&mensaje=" + quote("✅ Tratamiento de hallazgo guardado"))

    periodo = (request.args.get("periodo") or "mensual").strip().lower()
    mensaje = (request.args.get("mensaje") or "").strip()
    if periodo not in ("mensual", "trimestral", "semestral"):
        periodo = "mensual"

    hoy = date.today()
    dias_periodo = {
        "mensual": 30,
        "trimestral": 90,
        "semestral": 180,
    }
    fecha_desde = hoy - timedelta(days=dias_periodo[periodo])
    fecha_hasta = hoy

    procesos_base = ["ARMADO", "SOLDADURA", "PINTURA", "DESPACHO"]
    estados_nc = {"NC", "NO CONFORME", "NO CONFORMIDAD"}
    estados_obs = {"OBS", "OBSERVACION", "OBSERVACIÓN"}
    estados_om = {"OM", "OP MEJORA", "OPORTUNIDAD DE MEJORA"}

    metricas = {
        p: {"nc": 0, "obs": 0, "om": 0, "total": 0}
        for p in procesos_base
    }

    rows = db.execute(
                """
                SELECT
                        UPPER(TRIM(proceso)) AS proceso,
                        UPPER(TRIM(estado)) AS estado,
                        COUNT(*) AS total
                FROM procesos
                WHERE fecha IS NOT NULL
                    AND TRIM(fecha) <> ''
                    AND fecha >= ?
                    AND fecha <= ?
                    AND UPPER(TRIM(proceso)) IN ('ARMADO','SOLDADURA','PINTURA','DESPACHO')
                    AND estado IS NOT NULL
                    AND TRIM(estado) <> ''
                    AND eliminado = 0
                GROUP BY UPPER(TRIM(proceso)), UPPER(TRIM(estado))
                """,
                (fecha_desde.isoformat(), fecha_hasta.isoformat())
        ).fetchall()

    for proceso, estado, total in rows:
        if proceso not in metricas:
            continue
        cantidad = int(total or 0)
        metricas[proceso]["total"] += cantidad
        if estado in estados_nc:
            metricas[proceso]["nc"] += cantidad
        elif estado in estados_obs:
            metricas[proceso]["obs"] += cantidad
        elif estado in estados_om:
            metricas[proceso]["om"] += cantidad

    total_registros = sum(m["total"] for m in metricas.values())
    total_nc = sum(m["nc"] for m in metricas.values())
    total_obs = sum(m["obs"] for m in metricas.values())
    total_om = sum(m["om"] for m in metricas.values())
    total_hallazgos = total_nc + total_obs + total_om
    porcentaje_hallazgos = (total_hallazgos / total_registros * 100) if total_registros else 0

    proceso_critico = "-"
    critico_valor = -1
    for proceso in procesos_base:
        hallazgos = metricas[proceso]["nc"] + metricas[proceso]["obs"] + metricas[proceso]["om"]
        if hallazgos > critico_valor:
            critico_valor = hallazgos
            proceso_critico = proceso

    resumen_retrabajo = db.execute(
        """
        SELECT
            COUNT(*) AS total_retrabajos,
            COALESCE(SUM(COALESCE(retrabajo_hs, 0)), 0),
            COALESCE(SUM(COALESCE(desperdicio_kg, 0)), 0),
            COALESCE(SUM(COALESCE(impacto_entrega_dias, 0)), 0),
            COALESCE(SUM(COALESCE(costo_hallazgo, 0)), 0)
        FROM hallazgos_calidad
        WHERE fecha_hallazgo IS NOT NULL
          AND TRIM(fecha_hallazgo) <> ''
          AND fecha_hallazgo >= ?
          AND fecha_hallazgo <= ?
          AND COALESCE(genero_retrabajo, 0) = 1
        """,
        (fecha_desde.isoformat(), fecha_hasta.isoformat())
    ).fetchone()

    total_retrabajos = int((resumen_retrabajo[0] if resumen_retrabajo else 0) or 0)
    total_hs_retrabajo = float((resumen_retrabajo[1] if resumen_retrabajo else 0) or 0)
    total_desperdicio_kg = float((resumen_retrabajo[2] if resumen_retrabajo else 0) or 0)
    total_impacto_dias = float((resumen_retrabajo[3] if resumen_retrabajo else 0) or 0)
    total_costo_hallazgos = float((resumen_retrabajo[4] if resumen_retrabajo else 0) or 0)

    filas_causa = db.execute(
        """
        SELECT UPPER(TRIM(COALESCE(clasificacion_causa, ''))) AS causa, COUNT(*)
        FROM hallazgos_calidad
        WHERE fecha_hallazgo IS NOT NULL
          AND TRIM(fecha_hallazgo) <> ''
          AND fecha_hallazgo >= ?
          AND fecha_hallazgo <= ?
          AND COALESCE(requiere_causa_raiz, 0) = 1
          AND TRIM(COALESCE(clasificacion_causa, '')) <> ''
        GROUP BY UPPER(TRIM(COALESCE(clasificacion_causa, '')))
        ORDER BY COUNT(*) DESC
        """,
        (fecha_desde.isoformat(), fecha_hasta.isoformat())
    ).fetchall()

    mapa_causa_legible = {
        "MANO_DE_OBRA": "Mano de obra",
        "INGENIERIA": "Ingeniería",
        "MATERIAL": "Material",
        "METODO": "Método",
        "MAQUINA": "Máquina",
        "COMPRAS": "Compras",
        "PAGOS_PROVEEDORES": "Pagos proveedores",
    }
    total_causa_raiz = sum(int(r[1] or 0) for r in filas_causa)
    causas_rows_html = ""
    for causa, cantidad in filas_causa:
        causa_txt = mapa_causa_legible.get(str(causa or "").strip().upper(), str(causa or "-").strip() or "-")
        causas_rows_html += f"""
        <tr>
            <td>{html_lib.escape(causa_txt)}</td>
            <td><b>{int(cantidad or 0)}</b></td>
        </tr>
        """

    if not causas_rows_html:
        causas_rows_html = """
        <tr>
            <td colspan="2" style="text-align:center; color:#6b7280;">Sin causas clasificadas para el período</td>
        </tr>
        """

    filas_html = ""
    barras_html = ""
    for proceso in procesos_base:
        nc = metricas[proceso]["nc"]
        obs = metricas[proceso]["obs"]
        om = metricas[proceso]["om"]
        hallazgos = nc + obs + om
        total_proceso = metricas[proceso]["total"]
        tasa = (hallazgos / total_proceso * 100) if total_proceso else 0

        filas_html += f"""
        <tr>
            <td><b>{proceso}</b></td>
            <td>{nc}</td>
            <td>{obs}</td>
            <td>{om}</td>
            <td><b>{hallazgos}</b></td>
            <td>{tasa:.1f}%</td>
        </tr>
        """

        ancho_barra = min(100, hallazgos * 10)
        barras_html += f"""
        <div class="bar-row">
            <div class="bar-label">{proceso}</div>
            <div class="bar-track">
                <div class="bar-fill" style="width:{ancho_barra}%">{hallazgos}</div>
            </div>
        </div>
        """

    tratamientos = db.execute(
        """
                SELECT
                        fecha_hallazgo,
                        proceso,
                        tipo_hallazgo,
                        estado_tratamiento,
                        accion_inmediata,
                        acciones_correctivas,
                        COALESCE(requiere_causa_raiz, 0),
                        COALESCE(clasificacion_causa, ''),
                        COALESCE(genero_retrabajo, 0),
                        COALESCE(retrabajo_hs, 0),
                        COALESCE(desperdicio_kg, 0),
                        COALESCE(impacto_entrega_dias, 0),
                        COALESCE(costo_hallazgo, 0)
        FROM hallazgos_calidad
        WHERE fecha_hallazgo IS NOT NULL
          AND TRIM(fecha_hallazgo) <> ''
          AND fecha_hallazgo >= ?
          AND fecha_hallazgo <= ?
        ORDER BY fecha_hallazgo DESC, id DESC
        LIMIT 200
        """,
        (fecha_desde.isoformat(), fecha_hasta.isoformat())
    ).fetchall()

    tratamientos_rows_html = ""
    for fh, proc, tipo, est, acc_i, acc_c, req_cr, clasif, gen_rtb, hs_rtb, kg_rtb, dias_rtb, costo_rtb in tratamientos:
        tipo_class = "tipo-obs"
        if tipo == "NC":
            tipo_class = "tipo-nc"
        elif tipo == "OM":
            tipo_class = "tipo-om"

        est_class = "estado-abierto"
        if est == "EN PROCESO":
            est_class = "estado-proceso"
        elif est == "CERRADA":
            est_class = "estado-cerrada"

        causa_txt = "NO"
        if int(req_cr or 0) == 1:
            clasif_key = str(clasif or "").strip().upper()
            clasif_desc = mapa_causa_legible.get(clasif_key, clasif_key.replace("_", " "))
            causa_txt = f"SI ({clasif_desc})"

        retrabajo_txt = "NO"
        if int(gen_rtb or 0) == 1:
            retrabajo_txt = (
                f"SI | HH: {float(hs_rtb or 0):.1f} | KG: {float(kg_rtb or 0):.1f} | "
                f"Días: {float(dias_rtb or 0):.1f} | Costo: {float(costo_rtb or 0):.2f}"
            )

        tratamientos_rows_html += f"""
        <tr>
            <td>{fh}</td>
            <td><b>{html_lib.escape(str(proc or ''))}</b></td>
            <td><span class="badge {tipo_class}">{tipo}</span></td>
            <td><span class="badge {est_class}">{est}</span></td>
            <td>{html_lib.escape(causa_txt)}</td>
            <td style="text-align:left;">{html_lib.escape(retrabajo_txt)}</td>
            <td style="text-align:left;">{html_lib.escape(str(acc_i or ''))}</td>
            <td style="text-align:left;">{html_lib.escape(str(acc_c or ''))}</td>
        </tr>
        """

    if not tratamientos_rows_html:
        tratamientos_rows_html = """
        <tr>
            <td colspan="8" style="text-align:center; color:#6b7280;">Sin tratamientos cargados para el período seleccionado</td>
        </tr>
        """

    selected_mensual = "selected" if periodo == "mensual" else ""
    selected_trimestral = "selected" if periodo == "trimestral" else ""
    selected_semestral = "selected" if periodo == "semestral" else ""

    html = f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * {{ box-sizing: border-box; }}
    body {{ font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }}
    .header {{ display: flex; justify-content: space-between; align-items: center; gap: 10px; flex-wrap: wrap; }}
    h2 {{ color: #14532d; border-bottom: 3px solid #22c55e; padding-bottom: 10px; margin: 0; }}
    .btn-home {{ background: #667eea; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; }}
    .btn-home:hover {{ background: #5568d3; }}

    .filtro {{ background: white; padding: 15px; border-radius: 8px; margin: 15px 0; box-shadow: 0 2px 6px rgba(0,0,0,0.08); }}
    .filtro form {{ display: flex; flex-wrap: wrap; gap: 10px; align-items: center; }}
    .filtro select {{ padding: 10px; border: 1px solid #ddd; border-radius: 6px; min-width: 210px; }}
    .filtro button {{ background: #16a34a; color: white; border: none; padding: 10px 14px; border-radius: 6px; font-weight: bold; cursor: pointer; }}

    .resumen {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; margin-bottom: 15px; }}
    .kpi {{ background: white; border-radius: 8px; padding: 14px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); border-left: 4px solid #22c55e; }}
    .kpi .t {{ color: #166534; font-size: 12px; margin-bottom: 4px; }}
    .kpi .v {{ color: #14532d; font-size: 24px; font-weight: bold; }}

    .layout {{ display: grid; grid-template-columns: 1.2fr 1fr; gap: 15px; }}
    .card {{ background: white; border-radius: 8px; padding: 14px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); }}
    .card h3 {{ margin-top: 0; color: #166534; }}

    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ border-bottom: 1px solid #e5e7eb; padding: 9px; text-align: center; font-size: 13px; }}
    th {{ background: #ecfdf3; color: #166534; }}
    td:first-child {{ text-align: left; }}

    .bar-row {{ display: grid; grid-template-columns: 110px 1fr; gap: 8px; align-items: center; margin-bottom: 10px; }}
    .bar-label {{ font-weight: bold; color: #14532d; font-size: 12px; }}
    .bar-track {{ background: #dcfce7; border-radius: 999px; overflow: hidden; height: 20px; }}
    .bar-fill {{ background: linear-gradient(90deg, #22c55e, #16a34a); color: white; font-weight: bold; font-size: 12px; line-height: 20px; padding-right: 8px; text-align: right; min-width: 26px; }}

    .rango {{ font-size: 12px; color: #4b5563; margin-top: 2px; }}
    .msg-ok {{ background:#ecfdf3; color:#166534; border:1px solid #86efac; padding:10px; border-radius:8px; margin-bottom:10px; }}

    .tratamiento-form {{ background:white; border-radius:8px; padding:14px; margin-top:16px; box-shadow:0 2px 6px rgba(0,0,0,0.08); }}
    .tratamiento-form h3 {{ margin-top:0; color:#166534; }}
    .trat-grid {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap:10px; }}
    .tratamiento-form input, .tratamiento-form select, .tratamiento-form textarea {{ width:100%; padding:10px; border:1px solid #d1d5db; border-radius:6px; }}
    .tratamiento-form textarea {{ min-height:74px; resize:vertical; }}
    .tratamiento-form button {{ margin-top:10px; background:#16a34a; color:white; border:none; padding:10px 14px; border-radius:6px; font-weight:bold; cursor:pointer; }}
    .toggle-line {{ margin: 10px 0 4px 0; padding: 8px 10px; background: #ecfdf3; border: 1px solid #bbf7d0; border-radius: 8px; }}
    .toggle-line label {{ display: flex; align-items: center; gap: 8px; font-weight: bold; color: #166534; }}
    .subbloque {{ display:none; margin-top:10px; padding:10px; border:1px dashed #86efac; border-radius:8px; background:#f0fdf4; }}
    .hint-mini {{ color:#166534; font-size:12px; margin:2px 0 8px 0; }}

    .badge {{ display:inline-block; padding:3px 8px; border-radius:999px; font-weight:bold; font-size:11px; }}
    .tipo-nc {{ background:#fee2e2; color:#991b1b; }}
    .tipo-obs {{ background:#ffedd5; color:#9a3412; }}
    .tipo-om {{ background:#fef9c3; color:#854d0e; }}
    .estado-abierto {{ background:#fee2e2; color:#b91c1c; }}
    .estado-proceso {{ background:#ffedd5; color:#9a3412; }}
    .estado-cerrada {{ background:#dcfce7; color:#166534; }}

    @media (max-width: 980px) {{
        .layout {{ grid-template-columns: 1fr; }}
    }}
    </style>
    </head>
    <body>
    <div class="header">
        <h2>✅ Gestión de Calidad</h2>
        <a href="/" class="btn-home">⬅️ Volver</a>
    </div>

    {f'<div class="msg-ok">{html_lib.escape(mensaje)}</div>' if mensaje else ''}

    <div class="filtro">
        <form method="get" action="/modulo/gestion-calidad">
            <label><b>Periodo:</b></label>
            <select name="periodo">
                <option value="mensual" {selected_mensual}>Mensual (últimos 30 días)</option>
                <option value="trimestral" {selected_trimestral}>Trimestral (últimos 90 días)</option>
                <option value="semestral" {selected_semestral}>Semestral (últimos 180 días)</option>
            </select>
            <button type="submit">Generar indicadores</button>
            <div class="rango">Rango analizado: {fecha_desde.isoformat()} a {fecha_hasta.isoformat()}</div>
        </form>
    </div>

    <div class="resumen">
        <div class="kpi"><div class="t">No Conformes</div><div class="v">{total_nc}</div></div>
        <div class="kpi"><div class="t">OBS</div><div class="v">{total_obs}</div></div>
        <div class="kpi"><div class="t">Oportunidades de Mejora</div><div class="v">{total_om}</div></div>
        <div class="kpi"><div class="t">Total Hallazgos</div><div class="v">{total_hallazgos}</div></div>
        <div class="kpi"><div class="t">% Hallazgos / Registros</div><div class="v">{porcentaje_hallazgos:.1f}%</div></div>
        <div class="kpi"><div class="t">Proceso con más hallazgos</div><div class="v" style="font-size:18px;">{proceso_critico}</div></div>
        <div class="kpi"><div class="t">Con análisis causa raíz</div><div class="v">{total_causa_raiz}</div></div>
        <div class="kpi"><div class="t">Hallazgos con retrabajo</div><div class="v">{total_retrabajos}</div></div>
        <div class="kpi"><div class="t">HH retrabajo</div><div class="v">{total_hs_retrabajo:.1f}</div></div>
        <div class="kpi"><div class="t">Desperdicio material (kg)</div><div class="v">{total_desperdicio_kg:.1f}</div></div>
        <div class="kpi"><div class="t">Impacto entrega (días)</div><div class="v">{total_impacto_dias:.1f}</div></div>
        <div class="kpi"><div class="t">Costo total hallazgos</div><div class="v">{total_costo_hallazgos:.2f}</div></div>
    </div>

    <div class="layout">
        <div class="card">
            <h3>Detalle por Proceso</h3>
            <table>
                <tr>
                    <th>Proceso</th>
                    <th>No Conformes</th>
                    <th>OBS</th>
                    <th>Op. Mejora</th>
                    <th>Total Hallazgos</th>
                    <th>Tasa Hallazgos</th>
                </tr>
                {filas_html}
            </table>
        </div>
        <div class="card">
            <h3>Indicador Visual de Hallazgos</h3>
            {barras_html}
        </div>
        <div class="card">
            <h3>Dashboard de causa raíz</h3>
            <table>
                <tr>
                    <th>Tipo causa</th>
                    <th>Cantidad</th>
                </tr>
                {causas_rows_html}
            </table>
        </div>
    </div>

    <div class="tratamiento-form">
        <h3>Tratamiento de Hallazgos (NC / OBS / OM)</h3>
        <form method="post" action="/modulo/gestion-calidad">
            <input type="hidden" name="periodo" value="{periodo}">
            <div class="trat-grid">
                <div>
                    <label><b>Fecha</b></label>
                    <input type="date" name="fecha_hallazgo" value="{hoy.isoformat()}" required>
                </div>
                <div>
                    <label><b>Proceso</b></label>
                    <select name="proceso_h" required>
                        <option value="ARMADO">ARMADO</option>
                        <option value="SOLDADURA">SOLDADURA</option>
                        <option value="PINTURA">PINTURA</option>
                        <option value="DESPACHO">DESPACHO</option>
                    </select>
                </div>
                <div>
                    <label><b>Tipo de hallazgo</b></label>
                    <select name="tipo_hallazgo" required>
                        <option value="NC">NC (No conforme)</option>
                        <option value="OBS">OBS (Observacion)</option>
                        <option value="OM">OM (Oportunidad de mejora)</option>
                    </select>
                </div>
                <div>
                    <label><b>Estado</b></label>
                    <select name="estado_tratamiento" required>
                        <option value="ABIERTO">ABIERTO</option>
                        <option value="EN PROCESO">EN PROCESO</option>
                        <option value="CERRADA">CERRADA</option>
                    </select>
                </div>
            </div>
            <div class="trat-grid">
                <div>
                    <label><b>Acción inmediata</b></label>
                    <textarea name="accion_inmediata" required placeholder="Describir acción inmediata"></textarea>
                </div>
                <div>
                    <label><b>Acciones correctivas</b></label>
                    <textarea name="acciones_correctivas" required placeholder="Describir acciones correctivas"></textarea>
                </div>
            </div>

            <div class="toggle-line">
                <label>
                    <input type="checkbox" id="chk_causa_raiz" name="requiere_causa_raiz" value="1">
                    1-¿Requiere análisis causa raíz? (SI/NO)
                </label>
            </div>
            <div id="bloque_causa_raiz" class="subbloque">
                <div class="hint-mini">Si marcás SI, completá los 5 por qué y clasificá la causa.</div>
                <div class="trat-grid">
                    <div>
                        <label><b>¿Por qué ocurrió (1)</b></label>
                        <input type="text" name="porque_1" id="porque_1" placeholder="Primer por qué">
                    </div>
                    <div>
                        <label><b>¿Por qué? (2)</b></label>
                        <input type="text" name="porque_2" id="porque_2" placeholder="Segundo por qué">
                    </div>
                    <div>
                        <label><b>¿Por qué? (3)</b></label>
                        <input type="text" name="porque_3" id="porque_3" placeholder="Tercer por qué">
                    </div>
                    <div>
                        <label><b>¿Por qué? (4)</b></label>
                        <input type="text" name="porque_4" id="porque_4" placeholder="Cuarto por qué">
                    </div>
                    <div>
                        <label><b>¿Causa raiz (5)</b></label>
                        <input type="text" name="porque_5" id="porque_5" placeholder="Causa raíz">
                    </div>
                    <div>
                        <label><b>Clasificación de causa</b></label>
                        <select name="clasificacion_causa" id="clasificacion_causa">
                            <option value="">-- Seleccionar --</option>
                            <option value="MANO_DE_OBRA">Mano de obra</option>
                            <option value="INGENIERIA">Ingeniería</option>
                            <option value="MATERIAL">Material</option>
                            <option value="METODO">Método</option>
                            <option value="MAQUINA">Máquina</option>
                            <option value="COMPRAS">Compras</option>
                            <option value="PAGOS_PROVEEDORES">Pagos proveedores</option>
                        </select>
                    </div>
                </div>
            </div>

            <div style="margin-top: 15px; padding: 14px; background: linear-gradient(135deg, #f0fdf4 0%, #dcfce7 100%); border: 2px solid #22c55e; border-radius: 8px;">
                <h4 style="margin: 0 0 12px 0; color: #166534;">📊 Resumen Impactos (Período actual)</h4>
                <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 10px;">
                    <div style="background: #fff; padding: 12px; border-radius: 6px; border-left: 4px solid #ef4444; text-align: center;">
                        <div style="font-size: 11px; color: #666;">No Conformes</div>
                        <div style="font-size: 22px; font-weight: bold; color: #991b1b;">{total_nc}</div>
                    </div>
                    <div style="background: #fff; padding: 12px; border-radius: 6px; border-left: 4px solid #f59e0b; text-align: center;">
                        <div style="font-size: 11px; color: #666;">HH Retrabajo</div>
                        <div style="font-size: 22px; font-weight: bold; color: #92400e;">{total_hs_retrabajo:.1f}</div>
                    </div>
                    <div style="background: #fff; padding: 12px; border-radius: 6px; border-left: 4px solid #8b5cf6; text-align: center;">
                        <div style="font-size: 11px; color: #666;">Costo Total</div>
                        <div style="font-size: 22px; font-weight: bold; color: #6d28d9;">${total_costo_hallazgos:.2f}</div>
                    </div>
                    <div style="background: #fff; padding: 12px; border-radius: 6px; border-left: 4px solid #06b6d4; text-align: center;">
                        <div style="font-size: 11px; color: #666;">Atraso (días)</div>
                        <div style="font-size: 22px; font-weight: bold; color: #164e63;">{total_impacto_dias:.1f}</div>
                    </div>
                </div>
            </div>

            <div class="toggle-line">
                <label>
                    <input type="checkbox" id="chk_retrabajo" name="genero_retrabajo" value="1">
                    2- ¿Generó retrabajo? (SI/NO)
                </label>
            </div>
            <div id="bloque_retrabajo" class="subbloque">
                <div class="hint-mini">Si marcás SI, definí HH, impacto, desperdicio, atraso y costo.</div>
                <div class="trat-grid">
                    <div>
                        <label><b>HH retrabajo</b></label>
                        <input type="number" step="0.1" min="0" name="retrabajo_hs" id="retrabajo_hs" placeholder="Ej: 10">
                    </div>
                    <div>
                        <label><b>Desperdicio material (kg)</b></label>
                        <input type="number" step="0.1" min="0" name="desperdicio_kg" placeholder="Ej: 150">
                    </div>
                    <div>
                        <label><b>Impacto entrega (días)</b></label>
                        <input type="number" step="0.1" min="0" name="impacto_entrega_dias" placeholder="Ej: 5">
                    </div>
                    <div>
                        <label><b>Costo del hallazgo</b></label>
                        <input type="number" step="0.01" min="0" name="costo_hallazgo" placeholder="Costo estimado/real">
                    </div>
                    <input type="hidden" name="retrabajo_proceso_afectado" value="">
                    <div style="grid-column: 1 / -1;">
                        <label><b>Impacto del retrabajo (NC ↔ HH ↔ Costos ↔ Atrasos)</b></label>
                        <textarea name="retrabajo_impacto" id="retrabajo_impacto" placeholder="Describir el impacto sobre producción, costo y entrega"></textarea>
                    </div>
                </div>
            </div>
            <button type="submit">Guardar tratamiento</button>
        </form>
    </div>

    <div class="card" style="margin-top:15px;">
        <h3>Seguimiento de Tratamientos</h3>
        <table>
            <tr>
                <th>Fecha</th>
                <th>Proceso</th>
                <th>Hallazgo</th>
                <th>Estado</th>
                <th>Causa raíz</th>
                <th>Retrabajo / Costo</th>
                <th>Acción inmediata</th>
                <th>Acciones correctivas</th>
            </tr>
            {tratamientos_rows_html}
        </table>
    </div>
    <script>
    (function () {{
        const chkCausa = document.getElementById('chk_causa_raiz');
        const bloqueCausa = document.getElementById('bloque_causa_raiz');
        const chkRetrabajo = document.getElementById('chk_retrabajo');
        const bloqueRetrabajo = document.getElementById('bloque_retrabajo');

        const causaFields = [
            document.getElementById('porque_1'),
            document.getElementById('porque_2'),
            document.getElementById('porque_3'),
            document.getElementById('porque_4'),
            document.getElementById('porque_5'),
            document.getElementById('clasificacion_causa')
        ];

        const retrabajoReq = [
            document.getElementById('retrabajo_hs'),
            document.getElementById('retrabajo_impacto')
        ];

        function toggleCausa() {{
            const on = !!(chkCausa && chkCausa.checked);
            if (bloqueCausa) bloqueCausa.style.display = on ? 'block' : 'none';
            causaFields.forEach(function (el) {{
                if (!el) return;
                el.required = on;
            }});
        }}

        function toggleRetrabajo() {{
            const on = !!(chkRetrabajo && chkRetrabajo.checked);
            if (bloqueRetrabajo) bloqueRetrabajo.style.display = on ? 'block' : 'none';
            retrabajoReq.forEach(function (el) {{
                if (!el) return;
                el.required = on;
            }});
        }}

        if (chkCausa) chkCausa.addEventListener('change', toggleCausa);
        if (chkRetrabajo) chkRetrabajo.addEventListener('change', toggleRetrabajo);
        toggleCausa();
        toggleRetrabajo();
    }})();
    </script>
    </body>
    </html>
    """
    return html
