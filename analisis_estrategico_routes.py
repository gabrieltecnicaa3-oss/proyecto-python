"""
Dashboard de Análisis Estratégico de Producción
Predice fechas, calcula ruta crítica, detecta cuellos de botella y valida probabilidad de cumplimiento.
"""

from flask import Blueprint, request
from db_utils import get_db
from datetime import datetime, timedelta
import random
import html


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


def _extraer_ot_id_desde_titulo(titulo):
    """Intenta extraer número de OT del título (ej: 'OT 10' -> 10)."""
    if not titulo:
        return None
    partes = str(titulo).upper().split()
    for i, p in enumerate(partes):
        if p == "OT" and i + 1 < len(partes):
            try:
                return int(partes[i + 1])
            except Exception:
                pass
    return None


def _calcular_ruta_critica(db):
    """
    Identifica procesos que no tienen holgura.
    Retorna lista de (ot_id, titulo, proceso, duracion_dias, pct_aprobacion).
    """
    ots = db.execute(
        """
        SELECT id, titulo, fecha_entrega, estado
        FROM ordenes_trabajo
        WHERE estado != 'Finalizada' AND fecha_cierre IS NULL
        ORDER BY fecha_entrega ASC
        """
    ).fetchall()
    
    critica = []
    hoy = datetime.now()
    
    for ot_id, titulo, fecha_entrega_str, estado in ots:
        try:
            if fecha_entrega_str:
                fecha_entrega = datetime.strptime(str(fecha_entrega_str), "%Y-%m-%d")
                dias_restantes = max(0, (fecha_entrega - hoy).days)
            else:
                dias_restantes = 9999
        except Exception:
            dias_restantes = 9999
        
        # Sumar duración de procesos pendientes
        duracion_total = 0
        para_procesos = []
        for proceso in ORDEN_PROCESOS:
            duracion = _calcular_duracion_proceso(db, ot_id, proceso)
            productividad = _calcular_productividad_proceso(db, ot_id, proceso)
            if duracion > 0:
                duracion_total += duracion
            para_procesos.append((proceso, duracion, productividad))
        
        # Si la OT está próxima a la fecha o sin holgura, es crítica
        if dias_restantes <= 7 or dias_restantes <= duracion_total:
            critica.append({
                "ot_id": ot_id,
                "titulo": titulo,
                "fecha_entrega": fecha_entrega_str,
                "dias_restantes": dias_restantes,
                "duracion_estimada": max(1, duracion_total),  # Mínimo 1 día
                "procesos": para_procesos,
                "holgura": dias_restantes - duracion_total,
            })
    
    return sorted(critica, key=lambda x: x["holgura"])


def _simular_probabilidad_cumplimiento(db, ot_id, fecha_entrega_str, simulaciones=1000):
    """
    Simula fechas de finalización con variabilidad.
    Retorna % de probabilidad de cumplir plazo.
    """
    try:
        fecha_entrega = datetime.strptime(str(fecha_entrega_str), "%Y-%m-%d")
    except Exception:
        return 50
    
    hoy = datetime.now()
    dias_hasta_entrega = (fecha_entrega - hoy).days
    if dias_hasta_entrega < 0:
        return 0
    
    # Duraciones base de procesos
    duraciones = {}
    total_dias_estimado = 0
    for proceso in ORDEN_PROCESOS:
        duraciones[proceso] = _calcular_duracion_proceso(db, ot_id, proceso) or 2  # Default 2 días si no hay data
        total_dias_estimado += duraciones[proceso]
    
    # Si la estimación es demasiado conservadora, ajustar
    if total_dias_estimado == 0:
        total_dias_estimado = 10  # Default 10 días si nada hay
    
    cumplimientos = 0
    for _ in range(simulaciones):
        # Simular con variabilidad ±40% (más realista para incertidumbre)
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


def _calcular_velocidad_promedio(db):
    """Calcula velocidad promedio de piezas procesadas (aprobadas o no) por día."""
    # Contar todas las piezas procesadas en últimos 30 días (más realista que solo aprobadas)
    procesos_completados = db.execute(
        """
        SELECT COUNT(1)
        FROM procesos
        WHERE estado IN ('Aprobado', 'Reproceso') AND fecha > datetime('now', '-30 days')
        """
    ).fetchone()
    
    total = (procesos_completados[0] or 0)
    if total == 0:
        # Si no hay procesos recientes, estimar por OTs abiertas
        ots_activas = db.execute("SELECT COUNT(1) FROM ordenes_trabajo WHERE fecha_cierre IS NULL").fetchone()
        return (ots_activas[0] or 0) // 2  # Estimación: 2 piezas por OT
    
    return int(total / 30) if total > 0 else 0


@analisis_estrategico_bp.route("/")
def dashboard_estrategico():
    db = get_db()
    
    # Calcular KPIs
    ruta_critica = _calcular_ruta_critica(db)
    cuello_botella = _detectar_cuello_botella(db)
    velocidad = _calcular_velocidad_promedio(db)
    
    # Proyección general
    ots_abiertas = db.execute(
        "SELECT COUNT(1) FROM ordenes_trabajo WHERE fecha_cierre IS NULL"
    ).fetchone()
    num_ots = ots_abiertas[0] if ots_abiertas else 0
    
    # Calcular proyección de término si se mantiene productividad
    if num_ots > 0:
        ots_list = db.execute(
            "SELECT id, titulo, fecha_entrega FROM ordenes_trabajo WHERE fecha_cierre IS NULL ORDER BY fecha_entrega"
        ).fetchall()
        
        probabilidades = []
        fechas_proyectadas = []
        for ot in ots_list:
            prob = _simular_probabilidad_cumplimiento(db, ot[0], ot[2])
            probabilidades.append(prob)
            
            # Estimar fecha proyectada
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
    
    # HTML
    filas_critica = ""
    for item in ruta_critica[:5]:
        holgura_color = "#fee2e2" if item["holgura"] <= 0 else "#fef3c7" if item["holgura"] <= 3 else "#dcfce7"
        holgura_txt_color = "#991b1b" if item["holgura"] <= 0 else "#92400e" if item["holgura"] <= 3 else "#166534"
        
        filas_critica += f"""
        <tr>
          <td>{html.escape(str(item['titulo'] or '-'))}</td>
          <td>{html.escape(str(item['fecha_entrega'] or '-'))}</td>
          <td>{item['dias_restantes']}</td>
          <td>{item['duracion_estimada']}</td>
          <td style="background:{holgura_color}; color:{holgura_txt_color}; font-weight:700;">{item['holgura']}</td>
        </tr>
        """
    
    filas_cuello = ""
    for proceso, cant in cuello_botella:
        filas_cuello += f"<tr><td>{html.escape(proceso)}</td><td>{cant} OTs</td></tr>"
    
    return f"""
    <html>
    <head>
      <meta name="viewport" content="width=device-width, initial-scale=1">
      <style>
        body {{ font-family: 'Segoe UI', Arial, sans-serif; background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); margin: 0; padding: 20px; color: #0f172a; }}
        .container {{ max-width: 1400px; margin: 0 auto; }}
        h1 {{ color: #fff; margin-bottom: 8px; }}
        .top a {{ display: inline-block; margin-right: 8px; margin-bottom: 16px; padding: 9px 12px; background: #fff; color: #667eea; text-decoration: none; border-radius: 8px; font-weight: 700; }}
        .kpis {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin-bottom: 20px; }}
        .kpi-card {{ background: #fff; border-radius: 12px; padding: 16px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1); }}
        .kpi-label {{ font-size: 12px; color: #64748b; font-weight: 600; text-transform: uppercase; letter-spacing: 0.5px; }}
        .kpi-value {{ font-size: 28px; font-weight: 800; margin-top: 8px; }}
        .kpi-value.good {{ color: #16a34a; }}
        .kpi-value.warning {{ color: #ea580c; }}
        .kpi-value.critical {{ color: #dc2626; }}
        .card {{ background: #fff; border-radius: 12px; padding: 16px; margin-bottom: 16px; box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1); }}
        .card h3 {{ margin: 0 0 12px 0; color: #1e293b; }}
        table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
        th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #e2e8f0; }}
        th {{ background: #f1f5f9; font-weight: 600; }}
        .critical-list {{ list-style: none; padding: 0; margin: 0; }}
        .critical-list li {{ padding: 8px; border-left: 4px solid #dc2626; background: #fee2e2; margin-bottom: 8px; border-radius: 4px; }}
      </style>
    </head>
    <body>
      <div class="container">
        <h1>📊 Dashboard de Análisis Estratégico</h1>
        <div class="top">
          <a href="/modulo/tablero-ejecutivo">Tablero Ejecutivo</a>
          <a href="/">Panel Principal</a>
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

        <div class="card">
          <h3>🚨 Ruta Crítica (Holgura crítica ≤ 0 días)</h3>
          <table>
            <tr><th>OT</th><th>Entrega</th><th>Días restantes</th><th>Duración estimada</th><th>Holgura</th></tr>
            {filas_critica if filas_critica else '<tr><td colspan="5">Sin OTs en ruta crítica.</td></tr>'}
          </table>
        </div>

        <div class="card">
          <h3>⚙️ Cuello de botella (Procesos que frenan)</h3>
          <table>
            <tr><th>Proceso</th><th>OTs retrasadas</th></tr>
            {filas_cuello}
          </table>
        </div>

        <div class="card">
          <h3>💡 Recomendaciones estratégicas</h3>
          <ul class="critical-list">
            {'<li>⚠️ <b>Probabilidad baja de cumplimiento:</b> Evaluar aceleración, cambio de secuencia o recursos.</li>' if probabilidad_prom < 50 else '<li>✓ Probabilidad razonable: monitorear tendencias.</li>' if probabilidad_prom >= 70 else '<li>⚠️ Probabilidad media: realizar seguimiento diario.</li>'}
            {'<li>🔴 <b>Cuello de botella crítico:</b> ' + criticos_str + '. Considerar pasar personal o máquinas.</li>' if criticos_str else '<li>✓ Carga balanceada en procesos.</li>'}
            {'<li>📉 Velocidad baja: ' + str(velocidad) + ' piezas/día. Identificar causas de ralentización.</li>' if velocidad < 5 else '<li>✓ Velocidad en rango esperado.</li>'}
          </ul>
        </div>
      </div>
    </body>
    </html>
    """
