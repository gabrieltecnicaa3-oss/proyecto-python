"""
Dashboard de Análisis Estratégico de Producción
Predice fechas, calcula ruta crítica, detecta cuellos de botella y valida probabilidad de cumplimiento.
Incluye gráficos con tendencias y filtros por obra.
"""

from flask import Blueprint, request, session, redirect
from db_utils import get_db
from datetime import datetime, timedelta
import random
import html
import json


analisis_estrategico_bp = Blueprint("analisis-estrategico", __name__, url_prefix="/modulo/analisis-estrategico")


ORDEN_PROCESOS = [
    "RECEPCION",
    "CORTE PERFILES",
    "ARMADO Y SOLDADURA",
    "PINTURA",
    "DESPACHO",
]


def _calcular_duracion_proceso(db, ot_id, proceso):
    """Calcula duración real de un proceso en una OT."""
    rows = db.execute(
        """
        SELECT MIN(fecha) as inicio, MAX(fecha) as fin
        FROM procesos
        WHERE ot_id = ? AND proceso = ? AND estado = 'Aprobado'
        """,
        (ot_id, proceso),
    ).fetchone()
    
    if not rows or not rows[0] or not rows[1]:
        return 0
    
    try:
        inicio = datetime.fromisoformat(str(rows[0]).replace("Z", "+00:00"))
        fin = datetime.fromisoformat(str(rows[1]).replace("Z", "+00:00"))
        return max(0, (fin - inicio).days)
    except Exception:
        return 0


def _calcular_productividad_proceso(db, ot_id, proceso):
    """% de piezas aprobadas en primer intento (sin reproceso)."""
    total = db.execute(
        "SELECT COUNT(1) FROM procesos WHERE ot_id = ? AND proceso = ?",
        (ot_id, proceso),
    ).fetchone()
    
    if not total or total[0] == 0:
        return 100
    
    aprobadas = db.execute(
        "SELECT COUNT(1) FROM procesos WHERE ot_id = ? AND proceso = ? AND reproceso IS NULL",
        (ot_id, proceso),
    ).fetchone()
    
    return int((aprobadas[0] / total[0]) * 100) if total[0] > 0 else 100


def _calcular_ruta_critica(db, obra_filtro='TODAS'):
    """Identifica procesos que no tienen holgura."""
    # Excluye OTs con avance 100% (piezas ya en p/despacho aunque OT no cerrada formalmente)
    rows = db.execute(
        """
        SELECT id, titulo, fecha_entrega, estado, obra
        FROM ordenes_trabajo
        WHERE estado != 'Finalizada'
          AND fecha_cierre IS NULL
          AND COALESCE(estado_avance, 0) < 100
        ORDER BY fecha_entrega ASC
        """
    ).fetchall()
    ots = [
        r for r in rows
        if obra_filtro in ('TODAS', None, '') or str(r[4] or '').strip() == obra_filtro
    ]
    
    critica = []
    hoy = datetime.now()
    
    for ot_id, titulo, fecha_entrega_str, estado, obra in ots:
        try:
            if fecha_entrega_str:
                fecha_entrega = datetime.strptime(str(fecha_entrega_str), "%Y-%m-%d")
                dias_restantes = max(0, (fecha_entrega - hoy).days)
            else:
                dias_restantes = 9999
        except Exception:
            dias_restantes = 9999
        
        duracion_total = 0
        para_procesos = []
        for proceso in ORDEN_PROCESOS:
            duracion = _calcular_duracion_proceso(db, ot_id, proceso)
            productividad = _calcular_productividad_proceso(db, ot_id, proceso)
            if duracion > 0:
                duracion_total += duracion
            para_procesos.append((proceso, duracion, productividad))
        
        if dias_restantes <= 7 or dias_restantes <= duracion_total:
            critica.append({
                "ot_id": ot_id,
                "titulo": titulo,
                "fecha_entrega": fecha_entrega_str,
                "dias_restantes": dias_restantes,
                "duracion_estimada": max(1, duracion_total),
                "procesos": para_procesos,
                "holgura": dias_restantes - duracion_total,
                "obra": obra,
            })
    
    return sorted(critica, key=lambda x: x["holgura"])


def _simular_probabilidad_cumplimiento(db, ot_id, fecha_entrega_str, simulaciones=200):
    """Simula fechas de finalización con variabilidad."""
    try:
        fecha_entrega = datetime.strptime(str(fecha_entrega_str), "%Y-%m-%d")
    except Exception:
        return 50
    
    hoy = datetime.now()
    dias_hasta_entrega = (fecha_entrega - hoy).days
    if dias_hasta_entrega < 0:
        return 0
    
    duraciones = {}
    total_dias_estimado = 0
    for proceso in ORDEN_PROCESOS:
        duraciones[proceso] = _calcular_duracion_proceso(db, ot_id, proceso) or 2
        total_dias_estimado += duraciones[proceso]
    
    if total_dias_estimado == 0:
        total_dias_estimado = 10
    
    cumplimientos = 0
    for _ in range(simulaciones):
        total_sim = 0
        for proceso, dias in duraciones.items():
            variabilidad = random.uniform(0.6, 1.4)
            total_sim += dias * variabilidad
        
        fecha_fin_simulada = hoy + timedelta(days=total_sim)
        if fecha_fin_simulada <= fecha_entrega:
            cumplimientos += 1
    
    return int((cumplimientos / simulaciones) * 100)


def _detectar_cuello_botella(db, obra_filtro='TODAS'):
    """Identifica proceso que está retrasando más OTs."""
    cuello = {}
    for proceso in ORDEN_PROCESOS:
        if obra_filtro and obra_filtro != 'TODAS':
            ots_retrasadas = db.execute(
                """
                SELECT COUNT(DISTINCT p.ot_id)
                FROM procesos p
                JOIN ordenes_trabajo ot ON ot.id = p.ot_id
                WHERE p.proceso = ? AND p.estado != 'Aprobado'
                  AND ot.fecha_cierre IS NULL
                  AND ot.obra = ?
                """,
                (proceso, obra_filtro),
            ).fetchone()
        else:
            ots_retrasadas = db.execute(
                """
                SELECT COUNT(DISTINCT ot_id)
                FROM procesos p
                WHERE p.proceso = ? AND p.estado != 'Aprobado'
                  AND EXISTS (SELECT 1 FROM ordenes_trabajo ot WHERE ot.id = p.ot_id AND ot.fecha_cierre IS NULL)
                """,
                (proceso,),
            ).fetchone()
        cuello[proceso] = ots_retrasadas[0] if ots_retrasadas else 0

    return sorted(cuello.items(), key=lambda x: x[1], reverse=True)


def _calcular_tendencia_productividad(db, granularidad='semanal', obra_filtro='TODAS'):
    """Calcula tendencia de productividad agrupada por granularidad (diaria/semanal/mensual)."""
    if obra_filtro and obra_filtro != 'TODAS':
        datos = db.execute(
            """
            SELECT DATE(p.fecha) as dia,
                   COUNT(1) as total,
                   SUM(CASE WHEN p.estado = 'Aprobado' THEN 1 ELSE 0 END) as aprobadas
            FROM procesos p
            JOIN ordenes_trabajo ot ON ot.id = p.ot_id
            WHERE p.fecha IS NOT NULL AND p.eliminado != 1 AND ot.obra = ?
            GROUP BY DATE(p.fecha)
            ORDER BY dia ASC
            LIMIT 365
            """,
            (obra_filtro,),
        ).fetchall()
    else:
        datos = db.execute(
            """
            SELECT DATE(fecha) as dia,
                   COUNT(1) as total,
                   SUM(CASE WHEN estado = 'Aprobado' THEN 1 ELSE 0 END) as aprobadas
            FROM procesos
            WHERE fecha IS NOT NULL AND eliminado != 1
            GROUP BY DATE(fecha)
            ORDER BY dia ASC
            LIMIT 365
            """
        ).fetchall()

    diarios = [(str(row[0]), int(row[1] or 0), int(row[2] or 0)) for row in datos]

    if granularidad == 'diaria':
        return diarios[-60:]
    elif granularidad == 'mensual':
        meses: dict = {}
        for dia, total, aprobadas in diarios:
            key = dia[:7]
            if key not in meses:
                meses[key] = [0, 0]
            meses[key][0] += total
            meses[key][1] += aprobadas
        return [(k, v[0], v[1]) for k, v in sorted(meses.items())]
    else:  # semanal (default)
        semanas: dict = {}
        for dia, total, aprobadas in diarios:
            try:
                dt = datetime.strptime(dia, "%Y-%m-%d")
                year, week, _ = dt.isocalendar()
                key = f"{year}-S{week:02d}"
            except Exception:
                key = dia
            if key not in semanas:
                semanas[key] = [0, 0]
            semanas[key][0] += total
            semanas[key][1] += aprobadas
        return [(k, v[0], v[1]) for k, v in sorted(semanas.items())]


def _obtener_obras(db):
    """Obtiene lista de todas las obras con OTs abiertas."""
    obras = db.execute(
        """
        SELECT DISTINCT obra
        FROM ordenes_trabajo
        WHERE obra IS NOT NULL AND fecha_cierre IS NULL
        ORDER BY obra
        """
    ).fetchall()
    
    return [row[0] for row in obras] if obras else []


def _calcular_velocidad_promedio(db, obra_filtro='TODAS'):
    """Calcula velocidad promedio de piezas procesadas por día."""
    fecha_limite = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
    if obra_filtro and obra_filtro != 'TODAS':
        procesos_completados = db.execute(
            """
            SELECT COUNT(1)
            FROM procesos p
            JOIN ordenes_trabajo ot ON ot.id = p.ot_id
            WHERE p.estado IN ('Aprobado', 'Reproceso') AND p.fecha > ? AND ot.obra = ?
            """,
            (fecha_limite, obra_filtro),
        ).fetchone()
    else:
        procesos_completados = db.execute(
            """
            SELECT COUNT(1)
            FROM procesos
            WHERE estado IN ('Aprobado', 'Reproceso') AND fecha > ?
            """,
            (fecha_limite,),
        ).fetchone()

    total = (procesos_completados[0] or 0)
    if total == 0:
        ots_activas = db.execute("SELECT COUNT(1) FROM ordenes_trabajo WHERE fecha_cierre IS NULL").fetchone()
        return (ots_activas[0] or 0) // 2

    return int(total / 30) if total > 0 else 0


def _calcular_kg_hh(db, obra_filtro='TODAS'):
    """Calcula indicador KG/HH: kg fabricados aprobados en ARMADO dividido horas reales del parte semanal."""
    ok_estados = ('OK', 'APROBADO', 'OBS', 'OBSERVACION', 'OM', 'OP MEJORA', 'OPORTUNIDAD DE MEJORA')
    ok_ph = ",".join("?" * len(ok_estados))
    if obra_filtro and obra_filtro != 'TODAS':
        kg_row = db.execute(
            f"""
            SELECT SUM(COALESCE(p.cantidad, 1) * COALESCE(p.peso, 0))
            FROM procesos p
            JOIN ordenes_trabajo ot ON ot.id = p.ot_id
            WHERE p.proceso = 'ARMADO'
              AND UPPER(TRIM(COALESCE(p.estado, ''))) IN ({ok_ph})
              AND p.eliminado = 0
              AND ot.obra = ?
            """,
            list(ok_estados) + [obra_filtro],
        ).fetchone()
        hh_row = db.execute(
            """
            SELECT SUM(COALESCE(pt.horas, 0))
            FROM partes_trabajo pt
            JOIN ordenes_trabajo ot ON ot.id = pt.ot_id
            WHERE ot.obra = ?
            """,
            (obra_filtro,),
        ).fetchone()
    else:
        kg_row = db.execute(
            f"""
            SELECT SUM(COALESCE(cantidad, 1) * COALESCE(peso, 0))
            FROM procesos
            WHERE proceso = 'ARMADO'
              AND UPPER(TRIM(COALESCE(estado, ''))) IN ({ok_ph})
              AND eliminado = 0
            """,
            list(ok_estados),
        ).fetchone()
        hh_row = db.execute(
            "SELECT SUM(COALESCE(horas, 0)) FROM partes_trabajo"
        ).fetchone()
    kg = float(kg_row[0] or 0) if kg_row else 0.0
    hh = float(hh_row[0] or 0) if hh_row else 0.0
    ratio = round(kg / hh, 2) if hh > 0 else 0.0
    return kg, hh, ratio


@analisis_estrategico_bp.route("/")
def dashboard_estrategico():
    # Solo admin puede ver este módulo
    if str(session.get("user_role") or "").strip().lower() != "administrador":
        return redirect("/")
    
    db = get_db()
    
    # Obtener obra del filtro (por defecto TODAS)
    obra_filtro = request.args.get("obra", "TODAS")
    granularidad = request.args.get("granularidad", "semanal")
    if granularidad not in ("diaria", "semanal", "mensual"):
        granularidad = "semanal"
    obras = _obtener_obras(db)

    # Calcular KPIs (todos filtrados por obra)
    ruta_critica = _calcular_ruta_critica(db, obra_filtro=obra_filtro)
    cuello_botella = _detectar_cuello_botella(db, obra_filtro=obra_filtro)
    velocidad = _calcular_velocidad_promedio(db, obra_filtro=obra_filtro)
    tendencia = _calcular_tendencia_productividad(db, granularidad=granularidad, obra_filtro=obra_filtro)
    kg_total, hh_total, kg_hh_ratio = _calcular_kg_hh(db, obra_filtro=obra_filtro)

    # Proyección general
    if obra_filtro and obra_filtro != 'TODAS':
        ots_abiertas = db.execute(
            "SELECT COUNT(1) FROM ordenes_trabajo WHERE estado != 'Finalizada' AND fecha_cierre IS NULL AND obra = ?",
            (obra_filtro,),
        ).fetchone()
    else:
        ots_abiertas = db.execute(
            "SELECT COUNT(1) FROM ordenes_trabajo WHERE estado != 'Finalizada' AND fecha_cierre IS NULL"
        ).fetchone()
    num_ots = ots_abiertas[0] if ots_abiertas else 0

    # Calcular probabilidades + tabla (una sola pasada — evita doble Monte Carlo)
    hoy_dt = datetime.now()
    if obra_filtro and obra_filtro != 'TODAS':
        ots_list = db.execute(
            """SELECT id, titulo, fecha_entrega, obra FROM ordenes_trabajo
               WHERE fecha_cierre IS NULL AND estado != 'Finalizada' AND fecha_entrega IS NOT NULL
                 AND COALESCE(estado_avance, 0) < 100 AND obra = ?
               ORDER BY fecha_entrega ASC""",
            (obra_filtro,),
        ).fetchall()
    else:
        ots_list = db.execute(
            """SELECT id, titulo, fecha_entrega, obra FROM ordenes_trabajo
               WHERE fecha_cierre IS NULL AND estado != 'Finalizada' AND fecha_entrega IS NOT NULL
                 AND COALESCE(estado_avance, 0) < 100
               ORDER BY fecha_entrega ASC"""
        ).fetchall()

    probabilidades = []
    fechas_proyectadas = []
    filas_prob = ""

    for ot in ots_list:
        ot_id_p, titulo_p, fecha_entrega_str_p, obra_p = ot
        prob = _simular_probabilidad_cumplimiento(db, ot_id_p, fecha_entrega_str_p)
        probabilidades.append(prob)
        try:
            fe = datetime.strptime(str(fecha_entrega_str_p), "%Y-%m-%d")
            dias_rest_p = max(0, (fe - hoy_dt).days)
            duracion_p = sum(_calcular_duracion_proceso(db, ot_id_p, p) or 2 for p in ORDEN_PROCESOS)
            fecha_proy_p = hoy_dt + timedelta(days=max(1, duracion_p))
            dias_atraso_p = (fecha_proy_p - fe).days
            if dias_atraso_p > 0:
                fechas_proyectadas.append(dias_atraso_p)
        except Exception:
            dias_rest_p = "-"

        if prob >= 70:
            pc = "#16a34a"; pbg = "#dcfce7"; pst = "✅ Buena"
        elif prob >= 50:
            pc = "#92400e"; pbg = "#fef3c7"; pst = "⚠️ Riesgo"
        else:
            pc = "#991b1b"; pbg = "#fee2e2"; pst = "🔴 Crítica"

        pbar = f'<div style="background:#e2e8f0;border-radius:4px;height:8px;margin-top:4px"><div style="background:{pc};height:8px;border-radius:4px;width:{prob}%"></div></div>'
        filas_prob += f"<tr><td>{html.escape(str(titulo_p or '-'))}</td><td>{html.escape(str(obra_p or '-'))}</td><td>{html.escape(str(fecha_entrega_str_p or '-'))}</td><td>{dias_rest_p}</td><td style='min-width:130px'><b style='color:{pc}'>{prob}%</b>{pbar}</td><td><span style='background:{pbg};color:{pc};padding:2px 8px;border-radius:12px;font-size:11px;font-weight:700'>{pst}</span></td></tr>"

    probabilidad_prom = sum(probabilidades) // len(probabilidades) if probabilidades else 50
    atraso_promedio = sum(fechas_proyectadas) // len(fechas_proyectadas) if fechas_proyectadas else 0

    # Procesos críticos
    criticos_str = ", ".join(p[0] for p in cuello_botella[:3])

    # Preparar datos para gráficos
    dias_datos = [x[0] for x in tendencia]
    piezas_datos = [x[1] for x in tendencia]
    aprobadas_datos = [x[2] for x in tendencia]

    # Calcular línea de tendencia (promedio móvil)
    ventana = 3 if granularidad == 'semanal' else (4 if granularidad == 'mensual' else 7)
    linea_tendencia = []
    for i in range(len(piezas_datos)):
        inicio = max(0, i - (ventana // 2))
        fin = min(len(piezas_datos), i + (ventana // 2) + 1)
        promedio = sum(piezas_datos[inicio:fin]) / len(piezas_datos[inicio:fin]) if piezas_datos[inicio:fin] else 0
        linea_tendencia.append(round(promedio, 1))

    # --- Proyección a 2 períodos ---
    def _next_labels(last_label, gran, n=2):
        result = []
        if gran == 'semanal':
            try:
                year, week = int(last_label[:4]), int(last_label[6:])
                for i in range(1, n + 1):
                    w, y = week + i, year
                    max_w = datetime(y, 12, 28).isocalendar()[1]
                    if w > max_w:
                        w -= max_w
                        y += 1
                    result.append(f"{y}-S{w:02d}")
            except Exception:
                result = [f"Proy.{i}" for i in range(1, n + 1)]
        elif gran == 'mensual':
            try:
                year, month = int(last_label[:4]), int(last_label[5:7])
                for i in range(1, n + 1):
                    month += 1
                    if month > 12:
                        month, year = 1, year + 1
                    result.append(f"{year}-{month:02d}")
            except Exception:
                result = [f"Proy.{i}" for i in range(1, n + 1)]
        else:
            try:
                last_dt = datetime.strptime(last_label, "%Y-%m-%d")
                result = [(last_dt + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(1, n + 1)]
            except Exception:
                result = [f"Proy.{i}" for i in range(1, n + 1)]
        return result

    # Valor proyectado = promedio de últimos períodos disponibles
    ultimos = [v for v in linea_tendencia[-4:] if v is not None] if linea_tendencia else []
    valor_proyectado = round(sum(ultimos) / len(ultimos), 1) if ultimos else 0

    # Conectar proyección desde el último valor real
    last_real = linea_tendencia[-1] if linea_tendencia else None
    proj_labels = _next_labels(dias_datos[-1], granularidad) if dias_datos else ["Proy.1", "Proy.2"]

    # Nones para histórico excepto el último punto que conecta la línea
    proyeccion_datos = [None] * (len(piezas_datos) - 1) + [last_real, valor_proyectado, valor_proyectado]

    # Recomendaciones antes de extender las listas
    tendencia_positiva = len(linea_tendencia) > 3 and linea_tendencia[-1] > linea_tendencia[max(0, len(linea_tendencia) - 4)]

    # Extender listas con períodos proyectados
    dias_datos = dias_datos + proj_labels
    piezas_datos = piezas_datos + [None, None]
    aprobadas_datos = aprobadas_datos + [None, None]
    linea_tendencia = linea_tendencia + [None, None]

    titulo_grafico = {"diaria": "Productividad Diaria", "semanal": "Productividad Semanal", "mensual": "Productividad Mensual"}.get(granularidad, "Productividad Semanal")

    # Datos para gráfico de cuello de botella
    procesos_cuello = [p[0] for p in cuello_botella]
    cant_cuello = [p[1] for p in cuello_botella]
    colores_cuello = ["#ef4444" if cant > 0 else "#10b981" for cant in cant_cuello]
    
    # Filas de tabla crítica
    filas_critica = ""
    for item in ruta_critica[:5]:
        holgura_color = "#fee2e2" if item["holgura"] <= 0 else "#fef3c7" if item["holgura"] <= 3 else "#dcfce7"
        holgura_txt_color = "#991b1b" if item["holgura"] <= 0 else "#92400e" if item["holgura"] <= 3 else "#166534"
        
        filas_critica += f"""
        <tr>
          <td>{html.escape(str(item['titulo'] or '-'))}</td>
          <td>{html.escape(str(item['obra'] or '-'))}</td>
          <td>{html.escape(str(item['fecha_entrega'] or '-'))}</td>
          <td>{item['dias_restantes']}</td>
          <td>{item['duracion_estimada']}</td>
          <td style="background:{holgura_color}; color:{holgura_txt_color}; font-weight:700;">{item['holgura']}</td>
        </tr>
        """
    
    filas_cuello = ""
    for proceso, cant in cuello_botella:
        filas_cuello += f"<tr><td>{html.escape(proceso)}</td><td>{cant}</td></tr>"
    
    return f"""
    <html>
    <head>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.js"></script>
      <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); margin: 0; padding: 20px; color: #0f172a; }}
        .container {{ max-width: 1600px; margin: 0 auto; }}
        h1 {{ color: #fff; margin-bottom: 8px; }}
        .top {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 16px; gap: 12px; flex-wrap: wrap; }}
        .top-left {{ display: flex; gap: 8px; }}
        .top a {{ display: inline-block; padding: 9px 12px; background: #fff; color: #667eea; text-decoration: none; border-radius: 8px; font-weight: 700; font-size: 13px; }}
        .filtro-obra {{ display: inline-flex; gap: 8px; align-items: center; }}
        .filtro-obra label {{ color: #fff; font-weight: 600; font-size: 13px; }}
        .filtro-obra select {{ padding: 8px 12px; border-radius: 8px; border: 1px solid #ccc; background: #fff; font-weight: 600; cursor: pointer; font-size: 12px; }}
        .kpis {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; margin-bottom: 20px; }}
        .kpi-card {{ background: #fff; border-radius: 12px; padding: 16px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1); }}
        .kpi-label {{ font-size: 11px; color: #64748b; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }}
        .kpi-value {{ font-size: 28px; font-weight: 800; margin-top: 8px; }}
        .kpi-value.good {{ color: #16a34a; }}
        .kpi-value.warning {{ color: #ea580c; }}
        .kpi-value.critical {{ color: #dc2626; }}
        .card {{ background: #fff; border-radius: 12px; padding: 16px; margin-bottom: 16px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1); }}
        .card h3 {{ margin: 0 0 12px 0; color: #1e293b; font-size: 16px; }}
        .charts-row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(450px, 1fr)); gap: 16px; margin-bottom: 16px; }}
        .chart-container {{ background: #fff; border-radius: 12px; padding: 16px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1); position: relative; min-height: 350px; }}
        .chart-container h3 {{ margin: 0 0 20px 0; padding-top: 0; }}
        canvas {{ max-height: 300px !important; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
        th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #e2e8f0; }}
        th {{ background: #f1f5f9; font-weight: 600; }}
        .critical-list {{ list-style: none; padding: 0; margin: 0; }}
        .critical-list li {{ padding: 8px; border-left: 4px solid #dc2626; background: #fee2e2; margin-bottom: 8px; border-radius: 4px; font-size: 13px; }}
      </style>
    </head>
    <body>
      <div class="container">
        <h1>📊 Dashboard de Análisis Estratégico</h1>
        <div class="top">
          <div class="top-left">
            <a href="/modulo/tablero-ejecutivo">Tablero Ejecutivo</a>
            <a href="/">Panel Principal</a>
          </div>
          <form class="filtro-obra" method="get" style="gap:12px;flex-wrap:wrap;">
            <div style="display:flex;align-items:center;gap:6px;">
              <label>Obra:</label>
              <select name="obra">
                <option value="TODAS" {'selected' if obra_filtro == 'TODAS' else ''}>TODAS</option>
                {''.join(f'<option value="{o}" {"selected" if obra_filtro == o else ""}>{html.escape(o)}</option>' for o in obras)}
              </select>
            </div>
            <div style="display:flex;align-items:center;gap:6px;">
              <label>Vista:</label>
              <select name="granularidad">
                <option value="diaria" {'selected' if granularidad == 'diaria' else ''}>Diaria</option>
                <option value="semanal" {'selected' if granularidad == 'semanal' else ''}>Semanal</option>
                <option value="mensual" {'selected' if granularidad == 'mensual' else ''}>Mensual</option>
              </select>
            </div>
            <button type="submit" style="padding:8px 14px;border-radius:8px;border:none;background:#fff;color:#667eea;font-weight:700;cursor:pointer;">Aplicar</button>
          </form>
        </div>
        
        <div class="kpis">
          <div class="kpi-card">
            <div class="kpi-label">Probabilidad de cumplir plazo</div>
            <div class="kpi-value {'good' if probabilidad_prom >= 70 else 'warning' if probabilidad_prom >= 50 else 'critical'}">{probabilidad_prom}%</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">Atraso promedio proyectado</div>
            <div class="kpi-value {'good' if atraso_promedio <= 0 else 'warning' if atraso_promedio <= 5 else 'critical'}">+{atraso_promedio} <span style="font-size: 14px;">días</span></div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">OTs abiertas</div>
            <div class="kpi-value">{num_ots}</div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">Velocidad promedio</div>
            <div class="kpi-value">{velocidad} <span style="font-size: 14px;">pcs/día</span></div>
          </div>
          <div class="kpi-card">
            <div class="kpi-label">KG / HH Real</div>
            <div class="kpi-value">{kg_hh_ratio}</div>
            <div style="font-size:11px;color:#64748b;margin-top:4px;">{round(kg_total, 0):.0f} kg · {round(hh_total, 1)} HH parte semanal</div>
          </div>
        </div>

        <div class="charts-row">
          <div class="chart-container">
            <h3>📈 {titulo_grafico}</h3>
            <canvas id="trendChart"></canvas>
            <p style="font-size:11px;color:#64748b;margin:8px 0 0 0">🔵 Registros &nbsp;|&nbsp; 🟢 Aprobados &nbsp;|&nbsp; 🔴 Tendencia &nbsp;|&nbsp; 🟠 Proyección 2 períodos</p>
          </div>
          <div class="chart-container">
            <h3>⚙️ Cuellos de Botella</h3>
            <canvas id="cuellosChart"></canvas>
          </div>
        </div>

        <div class="card">
          <h3>🎯 Probabilidad de Cumplimiento por OT</h3>
          <table>
            <tr><th>OT</th><th>Obra</th><th>Fecha Entrega</th><th>Días Restantes</th><th>Probabilidad (Monte Carlo)</th><th>Estado</th></tr>
            {filas_prob if filas_prob else '<tr><td colspan="6" style="color:#64748b">Sin OTs con fecha de entrega definida.</td></tr>'}
          </table>
        </div>

        <div class="card">
          <h3>🚨 Ruta Crítica (Sin holgura real)</h3>
          <table>
            <tr><th>OT</th><th>Obra</th><th>Entrega</th><th>Días Restantes</th><th>Duración Est.</th><th>Holgura</th></tr>
            {filas_critica if filas_critica else '<tr><td colspan="6">Sin OTs en ruta crítica.</td></tr>'}
          </table>
        </div>

        <div class="card">
          <h3>⚙️ Cuello de botella (Procesos que limitan)</h3>
          <table>
            <tr><th>Proceso</th><th>OTs Retrasadas</th></tr>
            {filas_cuello}
          </table>
        </div>

        <div class="card">
          <h3>💡 Recomendaciones estratégicas</h3>
          <ul class="critical-list">
            {'<li>⚠️ <b>Probabilidad baja:</b> Necesita intervención inmediata. Evalúa acelerar, cambiar secuencia o reasignar recursos.</li>' if probabilidad_prom < 50 else '<li>✓ <b>Probabilidad razonable:</b> Mantén monitoreo.</li>' if probabilidad_prom >= 70 else '<li>⚠️ <b>Probabilidad media:</b> Realiza seguimiento diario.</li>'}
            {'<li>🔴 <b>Cuello crítico:</b> ' + criticos_str + '. El patrocinador debe acelerar estos procesos.</li>' if criticos_str else '<li>✓ Carga balanceada entre procesos.</li>'}
            {'<li>📉 <b>Velocidad baja:</b> ' + str(velocidad) + ' pcs/día. Investiga causas de ralentización.</li>' if velocidad < 5 else '<li>✓ Velocidad dentro del rango esperado.</li>'}
            {'<li>🟢 <b>Tendencia positiva:</b> Productividad mejora. Continúa con el ritmo actual.</li>' if tendencia_positiva else '<li>🔴 <b>Tendencia negativa:</b> Productividad decrece. Acción urgente.</li>'}
          </ul>
        </div>
      </div>

      <script>
        // Gráfico de productividad diaria (barras + línea de tendencia)
        const ctx1 = document.getElementById('trendChart')?.getContext('2d');
        if (ctx1) {{
          new Chart(ctx1, {{
            type: 'bar',
            data: {{
              labels: {json.dumps(dias_datos)},
              datasets: [
                {{
                  type: 'bar',
                  label: 'Registros del día',
                  data: {json.dumps(piezas_datos)},
                  backgroundColor: 'rgba(59,130,246,0.45)',
                  borderColor: '#3b82f6',
                  borderWidth: 1,
                  order: 2,
                }},
                {{
                  type: 'bar',
                  label: 'Aprobados',
                  data: {json.dumps(aprobadas_datos)},
                  backgroundColor: 'rgba(16,185,129,0.55)',
                  borderColor: '#10b981',
                  borderWidth: 1,
                  order: 3,
                }},
                {{
                  type: 'line',
                  label: 'Tendencia media',
                  data: {json.dumps(linea_tendencia)},
                  borderColor: '#ef4444',
                  borderWidth: 2,
                  borderDash: [6, 4],
                  fill: false,
                  pointRadius: 0,
                  tension: 0.4,
                  order: 1,
                  spanGaps: false,
                }},
                {{
                  type: 'line',
                  label: 'Proyección (2 per.)',
                  data: {json.dumps(proyeccion_datos)},
                  borderColor: '#f97316',
                  borderWidth: 3,
                  borderDash: [4, 4],
                  fill: false,
                  pointRadius: 4,
                  pointBackgroundColor: '#f97316',
                  tension: 0.3,
                  order: 0,
                  spanGaps: true,
                }}
              ]
            }},
            options: {{
              responsive: true,
              maintainAspectRatio: false,
              plugins: {{
                legend: {{ position: 'bottom', labels: {{ boxWidth: 12, padding: 10, font: {{ size: 11 }} }} }},
                tooltip: {{
                  callbacks: {{
                    title: (items) => 'Fecha: ' + items[0].label,
                    label: (item) => item.dataset.label + ': ' + item.parsed.y + ' procesos',
                  }}
                }}
              }},
              scales: {{
                y: {{ beginAtZero: true, title: {{ display: true, text: 'Procesos' }} }},
              }}
            }}
          }});
        }}

        // Gráfico de cuellos de botella
        const ctx2 = document.getElementById('cuellosChart')?.getContext('2d');
        if (ctx2) {{
          new Chart(ctx2, {{
            type: 'bar',
            data: {{
              labels: {json.dumps(procesos_cuello)},
              datasets: [{{
                label: 'OTs retrasadas',
                data: {json.dumps(cant_cuello)},
                backgroundColor: {json.dumps(colores_cuello)},
              }}]
            }},
            options: {{
              indexAxis: 'y',
              responsive: true,
              maintainAspectRatio: false,
              plugins: {{ legend: {{ display: false }} }},
              scales: {{ x: {{ beginAtZero: true }} }}
            }}
          }});
        }}


      </script>
    </body>
    </html>
    """
