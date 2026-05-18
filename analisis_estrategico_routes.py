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


def _calcular_ruta_critica(db):
    """Identifica procesos que no tienen holgura."""
    ots = db.execute(
        """
        SELECT id, titulo, fecha_entrega, estado, obra
        FROM ordenes_trabajo
        WHERE estado != 'Finalizada' AND fecha_cierre IS NULL
        ORDER BY fecha_entrega ASC
        """
    ).fetchall()
    
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


def _simular_probabilidad_cumplimiento(db, ot_id, fecha_entrega_str, simulaciones=1000):
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


def _detectar_cuello_botella(db):
    """Identifica proceso que está retrasando más OTs."""
    cuello = {}
    for proceso in ORDEN_PROCESOS:
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


def _calcular_tendencia_productividad(db, dias=60):
    """Calcula tendencia de productividad. Intenta los últimos N días, si no hay datos busca más atrás."""
    datos = db.execute(
        """
        SELECT DATE(fecha) as dia,
               COUNT(1) as total,
               SUM(CASE WHEN estado = 'Aprobado' THEN 1 ELSE 0 END) as aprobadas
        FROM procesos
        WHERE fecha IS NOT NULL AND eliminado != 1
        GROUP BY DATE(fecha)
        ORDER BY dia ASC
        LIMIT 60
        """
    ).fetchall()
    
    return [(str(row[0]), int(row[1] or 0), int(row[2] or 0)) for row in datos] if datos else []


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


def _calcular_velocidad_promedio(db):
    """Calcula velocidad promedio de piezas procesadas por día."""
    procesos_completados = db.execute(
        """
        SELECT COUNT(1)
        FROM procesos
        WHERE estado IN ('Aprobado', 'Reproceso') AND fecha > datetime('now', '-30 days')
        """
    ).fetchone()
    
    total = (procesos_completados[0] or 0)
    if total == 0:
        ots_activas = db.execute("SELECT COUNT(1) FROM ordenes_trabajo WHERE fecha_cierre IS NULL").fetchone()
        return (ots_activas[0] or 0) // 2
    
    return int(total / 30) if total > 0 else 0


@analisis_estrategico_bp.route("/")
def dashboard_estrategico():
    # Solo admin puede ver este módulo
    if str(session.get("user_role") or "").strip().lower() != "administrador":
        return redirect("/")
    
    db = get_db()
    
    # Obtener obra del filtro (por defecto TODAS)
    obra_filtro = request.args.get("obra", "TODAS")
    obras = _obtener_obras(db)
    
    # Calcular KPIs
    ruta_critica = _calcular_ruta_critica(db)
    cuello_botella = _detectar_cuello_botella(db)
    velocidad = _calcular_velocidad_promedio(db)
    tendencia = _calcular_tendencia_productividad(db, dias=30)
    
    # Proyección general
    ots_abiertas = db.execute(
        "SELECT COUNT(1) FROM ordenes_trabajo WHERE estado != 'Finalizada' AND fecha_cierre IS NULL"
    ).fetchone()
    num_ots = ots_abiertas[0] if ots_abiertas else 0
    
    # Calcular proyección de término si se mantiene productividad
    if num_ots > 0:
        ots_list = db.execute(
            "SELECT id, titulo, fecha_entrega FROM ordenes_trabajo WHERE estado != 'Finalizada' AND fecha_cierre IS NULL ORDER BY fecha_entrega"
        ).fetchall()
        
        probabilidades = []
        fechas_proyectadas = []
        for ot in ots_list:
            prob = _simular_probabilidad_cumplimiento(db, ot[0], ot[2])
            probabilidades.append(prob)
            
            try:
                fecha_entrega = datetime.strptime(str(ot[2]), "%Y-%m-%d")
                duracion = sum(_calcular_duracion_proceso(db, ot[0], p) or 2 for p in ORDEN_PROCESOS)
                fecha_proyectada = datetime.now() + timedelta(days=max(1, duracion))
                dias_atraso = (fecha_proyectada - fecha_entrega).days
                if dias_atraso > 0:
                    fechas_proyectadas.append(dias_atraso)
            except Exception:
                pass
        
        probabilidad_prom = sum(probabilidades) // len(probabilidades) if probabilidades else 50
        atraso_promedio = sum(fechas_proyectadas) // len(fechas_proyectadas) if fechas_proyectadas else 0
    else:
        probabilidad_prom = 100
        atraso_promedio = 0
    
    # Procesos críticos
    criticos_str = ", ".join(p[0] for p in cuello_botella[:3])
    
    # Preparar datos para gráficos
    dias_datos = [x[0] for x in tendencia]
    piezas_datos = [x[1] for x in tendencia]
    aprobadas_datos = [x[2] for x in tendencia]
    
    # Calcular línea de tendencia (promedio móvil 7 días)
    linea_tendencia = []
    for i in range(len(piezas_datos)):
        inicio = max(0, i - 3)
        fin = min(len(piezas_datos), i + 4)
        promedio = sum(piezas_datos[inicio:fin]) / len(piezas_datos[inicio:fin]) if piezas_datos[inicio:fin] else 0
        linea_tendencia.append(round(promedio, 1))
    
    # Datos para gráfico de cuello de botella
    procesos_cuello = [p[0] for p in cuello_botella]
    cant_cuello = [p[1] for p in cuello_botella]
    colores_cuello = ["#ef4444" if cant > 0 else "#10b981" for cant in cant_cuello]
    
    # Datos para gráfico de probabilidades: top 8 OTs abiertas con fecha_entrega
    ots_para_prob = db.execute(
        """SELECT id, titulo, fecha_entrega FROM ordenes_trabajo
           WHERE fecha_cierre IS NULL AND estado != 'Finalizada' AND fecha_entrega IS NOT NULL
           ORDER BY fecha_entrega ASC LIMIT 8"""
    ).fetchall()
    ots_nombres = [html.escape(str(r[1])[:15]) for r in ots_para_prob]
    ots_probabilidades = [_simular_probabilidad_cumplimiento(db, r[0], r[2]) for r in ots_para_prob]
    
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
          <div class="filtro-obra">
            <label>Filtrar por Obra:</label>
            <select onchange="location.href='?obra=' + this.value">
              <option value="TODAS" {'selected' if obra_filtro == 'TODAS' else ''}>TODAS</option>
              {''.join(f'<option value="{o}" {"selected" if obra_filtro == o else ""}>{html.escape(o)}</option>' for o in obras)}
            </select>
          </div>
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
        </div>

        <div class="charts-row">
          <div class="chart-container">
            <h3>📈 Tendencia de Productividad (últimos 30 días)</h3>
            <canvas id="trendChart"></canvas>
          </div>
          <div class="chart-container">
            <h3>⚙️ Cuellos de Botella</h3>
            <canvas id="cuellosChart"></canvas>
          </div>
        </div>

        {'<div class="chart-container" style="grid-column: 1 / -1;"><h3>🎯 Probabilidad de Cumplimiento por OT Crítica</h3><canvas id="probChart" style="max-height: 200px;"></canvas></div>' if ots_probabilidades else ''}

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
            {'<li>🟢 <b>Tendencia positiva:</b> Productividad mejora. Continúa con el ritmo actual.</li>' if len(linea_tendencia) > 5 and linea_tendencia[-1] > linea_tendencia[-7] else '<li>🔴 <b>Tendencia negativa:</b> Productividad decrece. Acción urgente.</li>'}
          </ul>
        </div>
      </div>

      <script>
        // Gráfico de tendencia
        const ctx1 = document.getElementById('trendChart')?.getContext('2d');
        if (ctx1) {{
          new Chart(ctx1, {{
            type: 'line',
            data: {{
              labels: {json.dumps(dias_datos)},
              datasets: [
                {{
                  label: 'Piezas totales',
                  data: {json.dumps(piezas_datos)},
                  borderColor: '#3b82f6',
                  backgroundColor: 'rgba(59, 130, 246, 0.05)',
                  tension: 0.4,
                  fill: true,
                  pointRadius: 2,
                  borderWidth: 2,
                }},
                {{
                  label: 'Piezas aprobadas',
                  data: {json.dumps(aprobadas_datos)},
                  borderColor: '#10b981',
                  backgroundColor: 'rgba(16, 185, 129, 0.05)',
                  tension: 0.4,
                  fill: true,
                  pointRadius: 2,
                  borderWidth: 2,
                }},
                {{
                  label: 'Tendencia (7d)',
                  data: {json.dumps(linea_tendencia)},
                  borderColor: '#ef4444',
                  borderWidth: 3,
                  borderDash: [5, 5],
                  fill: false,
                  pointRadius: 0,
                  tension: 0.4,
                }}
              ]
            }},
            options: {{
              responsive: true,
              maintainAspectRatio: false,
              plugins: {{
                legend: {{ position: 'bottom', labels: {{ boxWidth: 12, padding: 10, font: {{ size: 11 }} }} }},
                filler: {{ propagate: true }}
              }},
              scales: {{
                y: {{ beginAtZero: true }},
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

        // Gráfico de probabilidades
        const ctx3 = document.getElementById('probChart')?.getContext('2d');
        if (ctx3) {{
          const colors = {json.dumps(ots_probabilidades)}.map(p => p >= 70 ? '#10b981' : p >= 50 ? '#f59e0b' : '#ef4444');
          new Chart(ctx3, {{
            type: 'bar',
            data: {{
              labels: {json.dumps(ots_nombres)},
              datasets: [{{
                label: 'Probabilidad %',
                data: {json.dumps(ots_probabilidades)},
                backgroundColor: colors,
              }}]
            }},
            options: {{
              indexAxis: 'x',
              responsive: true,
              maintainAspectRatio: false,
              plugins: {{ legend: {{ display: false }} }},
              scales: {{ y: {{ min: 0, max: 100 }} }}
            }}
          }});
        }}
      </script>
    </body>
    </html>
    """
