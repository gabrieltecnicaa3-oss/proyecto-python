from datetime import date, timedelta
from urllib.parse import quote

from flask import Blueprint, redirect, request

from db_utils import get_db


gestion_calidad_bp = Blueprint("gestion_calidad", __name__)


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
        periodo_post = (request.form.get("periodo") or "mensual").strip().lower()
        if periodo_post not in ("mensual", "trimestral", "semestral"):
            periodo_post = "mensual"

        fecha_hallazgo = (request.form.get("fecha_hallazgo") or date.today().isoformat()).strip()
        proceso_h = (request.form.get("proceso_h") or "").strip().upper()
        tipo_hallazgo = (request.form.get("tipo_hallazgo") or "").strip().upper()
        estado_tratamiento = (request.form.get("estado_tratamiento") or "").strip().upper()
        accion_inmediata = (request.form.get("accion_inmediata") or "").strip()
        acciones_correctivas = (request.form.get("acciones_correctivas") or "").strip()

        procesos_validos = {"ARMADO", "SOLDADURA", "PINTURA", "DESPACHO"}
        tipos_validos = {"NC", "OBS", "OM"}
        estados_validos = {"ABIERTO", "EN PROCESO", "CERRADA"}

        if proceso_h not in procesos_validos or tipo_hallazgo not in tipos_validos or estado_tratamiento not in estados_validos:
            return redirect("/modulo/gestion-calidad?periodo=" + quote(periodo_post) + "&mensaje=" + quote("⚠️ Revisá los datos del hallazgo"))
        if not accion_inmediata or not acciones_correctivas:
            return redirect("/modulo/gestion-calidad?periodo=" + quote(periodo_post) + "&mensaje=" + quote("⚠️ Completá acción inmediata y acciones correctivas"))

        db.execute(
            """
            INSERT INTO hallazgos_calidad (
                fecha_hallazgo, proceso, tipo_hallazgo, estado_tratamiento, accion_inmediata, acciones_correctivas
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (fecha_hallazgo, proceso_h, tipo_hallazgo, estado_tratamiento, accion_inmediata, acciones_correctivas)
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
        SELECT fecha_hallazgo, proceso, tipo_hallazgo, estado_tratamiento, accion_inmediata, acciones_correctivas
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
    for fh, proc, tipo, est, acc_i, acc_c in tratamientos:
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

        tratamientos_rows_html += f"""
        <tr>
            <td>{fh}</td>
            <td><b>{proc}</b></td>
            <td><span class="badge {tipo_class}">{tipo}</span></td>
            <td><span class="badge {est_class}">{est}</span></td>
            <td style="text-align:left;">{acc_i}</td>
            <td style="text-align:left;">{acc_c}</td>
        </tr>
        """

    if not tratamientos_rows_html:
        tratamientos_rows_html = """
        <tr>
            <td colspan="6" style="text-align:center; color:#6b7280;">Sin tratamientos cargados para el período seleccionado</td>
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

    {f'<div class="msg-ok">{mensaje}</div>' if mensaje else ''}

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
                <th>Acción inmediata</th>
                <th>Acciones correctivas</th>
            </tr>
            {tratamientos_rows_html}
        </table>
    </div>
    </body>
    </html>
    """
    return html
