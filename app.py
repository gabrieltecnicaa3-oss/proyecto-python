from flask import Flask, request, redirect
import sqlite3

app = Flask(__name__)

# ======================
# DB
# ======================
def get_db():
    return sqlite3.connect("database.db")

def init_db():
    db = get_db()
    db.execute("""
    CREATE TABLE IF NOT EXISTS procesos (
        id INTEGER PRIMARY KEY,
        posicion TEXT,
        proceso TEXT,
        fecha TEXT,
        operario TEXT,
        estado TEXT,
        reproceso TEXT
    )
    """)
    
    db.execute("""
    CREATE TABLE IF NOT EXISTS ordenes_trabajo (
        id INTEGER PRIMARY KEY,
        cliente TEXT,
        obra TEXT,
        titulo TEXT,
        fecha_entrega TEXT,
        estado TEXT,
        fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    db.execute("""
    CREATE TABLE IF NOT EXISTS recepcion_materiales (
        id INTEGER PRIMARY KEY,
        ot_id INTEGER,
        material TEXT,
        proveedor TEXT,
        estado TEXT,
        observaciones TEXT,
        foto TEXT,
        fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (ot_id) REFERENCES ordenes_trabajo(id)
    )
    """)
    
    db.execute("""
    CREATE TABLE IF NOT EXISTS control_proceso (
        id INTEGER PRIMARY KEY,
        ot_id INTEGER,
        operacion TEXT,
        estado TEXT,
        observaciones TEXT,
        hora DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (ot_id) REFERENCES ordenes_trabajo(id)
    )
    """)
    
    db.execute("""
    CREATE TABLE IF NOT EXISTS control_despacho (
        id INTEGER PRIMARY KEY,
        ot_id INTEGER,
        fecha TEXT,
        responsable TEXT,
        conforme TEXT,
        observaciones TEXT,
        fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (ot_id) REFERENCES ordenes_trabajo(id)
    )
    """)
    
    db.execute("""
    CREATE TABLE IF NOT EXISTS partes_trabajo (
        id INTEGER PRIMARY KEY,
        fecha TEXT,
        operario TEXT,
        ot_id INTEGER,
        horas REAL,
        actividad TEXT,
        FOREIGN KEY (ot_id) REFERENCES ordenes_trabajo(id)
    )
    """)
    
    db.execute("""
    CREATE TABLE IF NOT EXISTS remitos (
        id INTEGER PRIMARY KEY,
        cliente TEXT,
        ot_id INTEGER,
        material_entregado TEXT,
        cantidad REAL,
        fecha TEXT,
        pdf_path TEXT,
        fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (ot_id) REFERENCES ordenes_trabajo(id)
    )
    """)
    
    db.commit()

init_db()

# ======================
# ORDEN DE PROCESOS
# ======================
ORDEN_PROCESOS = ["ARMADO", "SOLDADURA", "PINTURA", "DESPACHO"]

def obtener_procesos_completados(pos):
    """Retorna lista de procesos completados en orden"""
    db = get_db()
    rows = db.execute("SELECT proceso FROM procesos WHERE posicion=? ORDER BY id", (pos,)).fetchall()
    return [r[0] for r in rows]

def pieza_completada(pos):
    """Retorna True si el DESPACHO fue completado"""
    db = get_db()
    row = db.execute("SELECT COUNT(*) FROM procesos WHERE posicion=? AND proceso='DESPACHO'", (pos,)).fetchone()
    return row[0] > 0

def validar_siguiente_proceso(pos, nuevo_proceso):
    """Valida que el proceso siga el orden correcto"""
    procesos_hechos = obtener_procesos_completados(pos)
    
    # Si el proceso ya existe, es una edición
    if nuevo_proceso in procesos_hechos:
        return True, "OK"
    
    # Obtener índice del nuevo proceso
    try:
        idx_nuevo = ORDEN_PROCESOS.index(nuevo_proceso)
    except ValueError:
        return False, "Proceso inválido"
    
    # El primer proceso debe ser ARMADO
    if len(procesos_hechos) == 0:
        if nuevo_proceso != "ARMADO":
            return False, "❌ El primer proceso debe ser ARMADO"
        return True, "OK"
    
    # Validar que siga el orden
    ultimo_proceso = procesos_hechos[-1]
    idx_ultimo = ORDEN_PROCESOS.index(ultimo_proceso)
    
    if idx_nuevo == idx_ultimo:
        return False, "❌ Este proceso ya fue completado, no se puede repetir"
    elif idx_nuevo != idx_ultimo + 1:
        return False, f"❌ El siguiente proceso debe ser {ORDEN_PROCESOS[idx_ultimo + 1]}"
    
    return True, "OK"

# ======================
# DASHBOARD - INICIO
# ======================
@app.route("/")
def dashboard():
    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * {
        margin: 0;
        padding: 0;
        box-sizing: border-box;
    }
    body {
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        min-height: 100vh;
        padding: 20px;
    }
    .container {
        max-width: 1200px;
        margin: 0 auto;
    }
    .header {
        text-align: center;
        color: white;
        margin-bottom: 40px;
        padding-top: 20px;
    }
    .header h1 {
        font-size: 2.5em;
        margin-bottom: 10px;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
    }
    .header p {
        font-size: 1.1em;
        opacity: 0.9;
    }
    .modules-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
        gap: 20px;
        margin-bottom: 20px;
    }
    .module-card {
        background: white;
        border-radius: 12px;
        padding: 25px;
        box-shadow: 0 8px 16px rgba(0,0,0,0.1);
        transition: transform 0.3s ease, box-shadow 0.3s ease;
        cursor: pointer;
        text-decoration: none;
        color: #333;
    }
    .module-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 12px 24px rgba(0,0,0,0.15);
    }
    .module-icon {
        font-size: 3em;
        margin-bottom: 15px;
        display: block;
    }
    .module-card h3 {
        font-size: 1.3em;
        margin-bottom: 8px;
        color: #667eea;
    }
    .module-card p {
        font-size: 0.9em;
        color: #666;
        line-height: 1.4;
    }
    .module-card.ot {
        border-left: 5px solid #667eea;
    }
    .module-card.produccion {
        border-left: 5px solid #f093fb;
    }
    .module-card.calidad {
        border-left: 5px solid #4facfe;
    }
    .module-card.parte {
        border-left: 5px solid #43e97b;
    }
    .module-card.remito {
        border-left: 5px solid #fa709a;
    }
    .module-card.estado {
        border-left: 5px solid #30cfd0;
    }
    .footer {
        text-align: center;
        color: white;
        padding: 20px;
        font-size: 0.9em;
    }
    .legacy-link {
        display: inline-block;
        background: rgba(255,255,255,0.2);
        color: white;
        padding: 10px 15px;
        border-radius: 5px;
        text-decoration: none;
        margin-top: 10px;
        transition: background 0.3s;
    }
    .legacy-link:hover {
        background: rgba(255,255,255,0.3);
    }
    </style>
    </head>
    <body>
    <div class="container">
        <div class="header">
            <h1>🏭 Sistema de Gestión de Producción</h1>
            <p>Control integral de órdenes de trabajo, calidad y producción</p>
        </div>
        
        <div class="modules-grid">
            <a href="/modulo/ot" class="module-card ot">
                <span class="module-icon">📋</span>
                <h3>Órdenes de Trabajo</h3>
                <p>Crear y gestionar órdenes de trabajo, seguimiento de estado y entregas</p>
            </a>
            
            <a href="/modulo/produccion" class="module-card produccion">
                <span class="module-icon">🏭</span>
                <h3>Producción</h3>
                <p>Control de procesos y seguimiento de producción en planta</p>
            </a>
            
            <a href="/modulo/calidad" class="module-card calidad">
                <span class="module-icon">🧪</span>
                <h3>Calidad</h3>
                <p>Recepción de materiales, escaneo QR y control de despacho</p>
            </a>
            
            <a href="/modulo/parte" class="module-card parte">
                <span class="module-icon">⏱</span>
                <h3>Parte Semanal</h3>
                <p>Registro de horas de trabajo y actividades por operario</p>
            </a>
            
            <a href="/modulo/remito" class="module-card remito">
                <span class="module-icon">🚚</span>
                <h3>Remitos</h3>
                <p>Generación de remitos y documentos de entrega</p>
            </a>
            
            <a href="/modulo/estado" class="module-card estado">
                <span class="module-icon">📊</span>
                <h3>Estado de Producción</h3>
                <p>Tablero de control, indicadores y avance de órdenes</p>
            </a>
        </div>
        
        <div style="text-align: center;">
            <a href="/home" class="legacy-link">📈 Sistema Anterior - Picado por Posición</a>
        </div>
        
        <div class="footer">
            <p>© 2026 Sistema de Gestión de Producción</p>
        </div>
    </div>
    </body>
    </html>
    """
    return html

# ======================
# HOME - VER TODAS LAS TUPLAS
# ======================
@app.route("/home")
@app.route("/home/<int:page>")
def home(page=1):
    db = get_db()
    
    # Obtener parámetro de búsqueda
    busqueda = request.args.get('search', '').strip()
    
    all_rows = db.execute("SELECT * FROM procesos ORDER BY posicion ASC").fetchall()
    
    # Agrupar por posición para obtener piezas únicas
    piezas = {}
    for r in all_rows:
        pos = r[1]
        if pos not in piezas:
            piezas[pos] = []
        piezas[pos].append(r)
    
    posiciones_unicas = sorted(piezas.keys())
    
    # Filtrar por búsqueda
    if busqueda:
        posiciones_unicas = [pos for pos in posiciones_unicas if busqueda.lower() in pos.lower()]
    
    # Paginación de 10 piezas por página
    piezas_por_pagina = 10
    total_piezas = len(posiciones_unicas)
    total_paginas = (total_piezas + piezas_por_pagina - 1) // piezas_por_pagina
    
    # Validar página
    if page < 1:
        page = 1
    if page > total_paginas and total_paginas > 0:
        page = total_paginas
    
    # Calcular índices para obtener las piezas de la página actual
    inicio = (page - 1) * piezas_por_pagina
    fin = inicio + piezas_por_pagina
    posiciones_pagina = posiciones_unicas[inicio:fin]

    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {
        font-family: Arial;
        padding: 15px;
        background: #f4f4f4;
    }
    h2 {
        color: #333;
        border-bottom: 3px solid orange;
        padding-bottom: 10px;
    }
    .buscador-box {
        background: white;
        padding: 15px;
        border-radius: 5px;
        margin-bottom: 15px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
    }
    .buscador-box input {
        width: 100%;
        padding: 10px;
        border: 1px solid #ddd;
        border-radius: 4px;
        font-size: 14px;
        box-sizing: border-box;
    }
    .buscador-box button {
        background: orange;
        color: white;
        border: none;
        padding: 10px 20px;
        border-radius: 4px;
        font-weight: bold;
        cursor: pointer;
        margin-top: 10px;
        width: 100%;
    }
    .buscador-box button:hover {
        background: darkorange;
    }
    .info-busqueda {
        font-size: 12px;
        color: #666;
        margin-top: 8px;
    }
    table {
        width: 100%;
        border-collapse: collapse;
        background: white;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        margin-bottom: 20px;
    }
    th {
        background: orange;
        color: white;
        padding: 12px;
        text-align: center;
        font-weight: bold;
        font-size: 14px;
    }
    td {
        padding: 12px;
        border-bottom: 1px solid #e0e0e0;
        text-align: center;
    }
    td:first-child {
        text-align: left;
        font-weight: bold;
    }
    tr:nth-child(even) {
        background: #f5f5f5;
    }
    tr:nth-child(odd) {
        background: #e8e8e8;
    }
    tr:hover {
        background: #d0d0d0;
    }
    .completado {
        color: green;
        font-weight: bold;
    }
    .incompleto {
        color: #999;
    }
    .btn-ver {
        display: inline-block;
        background: blue;
        color: white;
        padding: 6px 10px;
        border-radius: 4px;
        text-decoration: none;
        font-weight: bold;
        font-size: 11px;
    }
    .btn-ver:hover {
        background: darkblue;
    }
    .sin-registros {
        background: white;
        padding: 20px;
        border-radius: 5px;
        text-align: center;
        color: #666;
    }
    .paginacion {
        text-align: center;
        margin-top: 20px;
        display: flex;
        justify-content: center;
        gap: 5px;
        flex-wrap: wrap;
        align-items: center;
    }
    .paginacion a, .paginacion span {
        padding: 8px 12px;
        border: 1px solid #ddd;
        border-radius: 4px;
        text-decoration: none;
        color: #333;
        display: inline-block;
    }
    .paginacion a:hover {
        background: #ddd;
    }
    .paginacion .activa {
        background: orange;
        color: white;
        border-color: orange;
    }
    .paginacion .deshabilitada {
        color: #ccc;
        cursor: not-allowed;
    }
    .info-paginacion {
        text-align: center;
        color: #666;
        margin-bottom: 15px;
        font-size: 12px;
    }
    </style>
    </head>

    <body>
    <h2>📊 Panel de Control - Estado de piezas por proceso</h2>
    
    <div class="buscador-box">
        <form method="get" action="/home">
            <input type="text" name="search" placeholder="🔍 Buscar por posición..." value="{busqueda}">
            <button type="submit">Buscar</button>
            <div class="info-busqueda">
                Ordenado por posición | 
                <a href="/home" style="color: blue; text-decoration: none;">Limpiar búsqueda</a>
            </div>
        </form>
    </div>
    """

    if total_piezas == 0:
        html += "<div class='sin-registros'>⚠️ No hay registros encontrados</div>"
    else:
        html += f"<div class='info-paginacion'>Mostrando {inicio + 1}-{min(fin, total_piezas)} de {total_piezas} piezas</div>"
        html += """
        <table>
            <tr>
                <th>Posición</th>
                <th>Armado</th>
                <th>Soldadura</th>
                <th>Pintura</th>
                <th>Despacho</th>
                <th>Acciones</th>
            </tr>
        """
        for pos in posiciones_pagina:
            procesos_hechos = obtener_procesos_completados(pos)
            
            # Crear celdas para cada proceso
            celdas = []
            for proceso in ORDEN_PROCESOS:
                if proceso in procesos_hechos:
                    celdas.append(f'<td><span class="completado">✅ {proceso}</span></td>')
                else:
                    celdas.append(f'<td><span class="incompleto">⊘</span></td>')
            
            html += f"""
            <tr>
                <td><b>{pos}</b></td>
                {celdas[0]}
                {celdas[1]}
                {celdas[2]}
                {celdas[3]}
                <td><a class="btn-ver" href="/pieza/{pos}">Ver Pieza</a></td>
            </tr>
            """
        html += "</table>"
        
        # Generar paginación
        html += "<div class='paginacion'>"
        
        # Botón anterior
        if page > 1:
            url_anterior = f'/home/{page - 1}' + (f'?search={busqueda}' if busqueda else '')
            html += f'<a href="{url_anterior}">← Anterior</a>'
        else:
            html += '<span class="deshabilitada">← Anterior</span>'
        
        # Números de página
        inicio_rango = max(1, page - 2)
        fin_rango = min(total_paginas, page + 2)
        
        if inicio_rango > 1:
            url_primera = '/home/1' + (f'?search={busqueda}' if busqueda else '')
            html += f'<a href="{url_primera}">1</a>'
            if inicio_rango > 2:
                html += '<span>...</span>'
        
        for p in range(inicio_rango, fin_rango + 1):
            if p == page:
                html += f'<span class="activa">{p}</span>'
            else:
                url_pagina = f'/home/{p}' + (f'?search={busqueda}' if busqueda else '')
                html += f'<a href="{url_pagina}">{p}</a>'
        
        if fin_rango < total_paginas:
            if fin_rango < total_paginas - 1:
                html += '<span>...</span>'
            url_ultima = f'/home/{total_paginas}' + (f'?search={busqueda}' if busqueda else '')
            html += f'<a href="{url_ultima}">{total_paginas}</a>'
        
        # Botón siguiente
        if page < total_paginas:
            url_siguiente = f'/home/{page + 1}' + (f'?search={busqueda}' if busqueda else '')
            html += f'<a href="{url_siguiente}">Siguiente →</a>'
        else:
            html += '<span class="deshabilitada">Siguiente →</span>'
        
        html += "</div>"

    html += """
    </body>
    </html>
    """

    return html

# ======================
# VER PIEZA (MEJORADO)
# ======================
@app.route("/pieza/<pos>")
def pieza(pos):
    db = get_db()
    rows = db.execute("SELECT * FROM procesos WHERE posicion=?", (pos,)).fetchall()
    es_completada = pieza_completada(pos)

    html = f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {{
        font-family: Arial;
        padding: 15px;
        background: #f4f4f4;
    }}
    .card {{
        background: white;
        padding: 15px;
        border-radius: 10px;
        margin-bottom: 10px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
        display: flex;
        justify-content: space-between;
        align-items: center;
    }}
    .card-info {{
        flex: 1;
    }}
    .ok {{ color: green; font-weight: bold; }}
    .no {{ color: red; font-weight: bold; }}
    .btn {{
        display: inline-block;
        text-align: center;
        background: orange;
        color: white;
        padding: 10px 15px;
        border-radius: 5px;
        text-decoration: none;
        font-weight: bold;
        margin-top: 10px;
        font-size: 12px;
    }}
    .btn-add {{
        display: block;
        width: 100%;
    }}
    .bloqueado {{
        background: #ccc;
        color: #666;
        cursor: not-allowed;
    }}
    .warning {{
        background: #ffcccc;
        color: red;
        padding: 12px;
        border-radius: 5px;
        margin-bottom: 15px;
        font-weight: bold;
    }}
    .completado {{
        background: #ccffcc;
        color: green;
        padding: 12px;
        border-radius: 5px;
        margin-bottom: 15px;
        font-weight: bold;
    }}
    </style>
    </head>

    <body>
    <h2>📦 Pieza {pos}</h2>
    """

    if es_completada:
        html += "<div class='completado'>✅ PIEZA COMPLETADA - No se puede editar</div>"
    
    if len(rows) == 0:
        html += "<div class='card'><b>⚠ SIN REGISTROS TODAVÍA</b></div>"
    else:
        for r in rows:
            estado_class = "ok" if r[5] == "OK" else "no"
            btn_editar = ""
            
            if not es_completada:
                btn_editar = f'<a class="btn" href="/editar/{r[0]}">✏️ Editar</a>'

            html += f"""
            <div class="card">
                <div class="card-info">
                    <b>{r[2]}</b><br>
                    📅 {r[3]}<br>
                    👷 {r[4]}<br>
                    Estado: <span class="{estado_class}">{r[5]}</span><br>
                    🔧 {r[6]}
                </div>
                <div>{btn_editar}</div>
            </div>
            """

    btn_agregar = "btn-add bloqueado" if es_completada else "btn-add"
    btn_texto = "🔒 PIEZA COMPLETADA" if es_completada else "➕ CARGAR CONTROL"
    btn_href = "#" if es_completada else f"/cargar/{pos}"
    
    html += f"""
    <div style="display: flex; gap: 10px; margin-top: 10px;">
        <a class="btn {btn_agregar}" href="{btn_href}" style="flex: 1;">{btn_texto}</a>
        <a class="btn" href="/home" style="flex: 1; background: #4CAF50;">📊 Ver Reporte de Piezas</a>
    </div>
    </body>
    </html>
    """

    return html

# ======================
# FORMULARIO CARGAR
# ======================
@app.route("/cargar/<pos>", methods=["GET","POST"])
def cargar(pos):
    if pieza_completada(pos):
        return f"""
        <html>
        <head>
        <style>
        body {{ font-family: Arial; padding: 15px; }}
        .error {{ background: #ffcccc; color: red; padding: 15px; border-radius: 5px; }}
        .btn {{ display: inline-block; background: orange; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; margin-top: 10px; }}
        </style>
        </head>
        <body>
        <div class="error">🔒 <b>PIEZA COMPLETADA</b><br>No se puede agregar más procesos</div>
        <a class="btn" href="/pieza/{pos}">⬅️ Volver</a>
        </body>
        </html>
        """
    
    if request.method == "POST":
        nuevo_proceso = request.form["proceso"]
        es_valido, mensaje = validar_siguiente_proceso(pos, nuevo_proceso)
        
        if not es_valido:
            return f"""
            <html>
            <head>
            <style>
            body {{ font-family: Arial; padding: 15px; }}
            .error {{ background: #ffcccc; color: red; padding: 15px; border-radius: 5px; }}
            .btn {{ display: inline-block; background: orange; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; margin-top: 10px; }}
            </style>
            </head>
            <body>
            <div class="error"><b>{mensaje}</b></div>
            <a class="btn" href="/pieza/{pos}">⬅️ Intentar de nuevo</a>
            </body>
            </html>
            """
        
        db = get_db()
        db.execute("""
        INSERT INTO procesos (posicion, proceso, fecha, operario, estado, reproceso)
        VALUES (?,?,?,?,?,?)
        """, (
            pos,
            nuevo_proceso,
            request.form["fecha"],
            request.form["operario"],
            request.form["estado"],
            request.form["reproceso"]
        ))
        db.commit()

        return f"""
        <html>
        <head>
        <style>
        body {{ font-family: Arial; padding: 15px; }}
        .success {{ background: #ccffcc; color: green; padding: 15px; border-radius: 5px; }}
        .btn {{ display: inline-block; background: orange; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; margin-top: 10px; }}
        </style>
        </head>
        <body>
        <div class="success">✅ <b>Guardado correctamente</b></div>
        <a class="btn" href="/pieza/{pos}">⬅️ Volver</a>
        </body>
        </html>
        """
    
    procesos_hechos = obtener_procesos_completados(pos)
    
    # Mostrar qué procesos se pueden hacer
    siguiente_proceso = None
    if len(procesos_hechos) == 0:
        siguiente_proceso = "ARMADO"
    elif len(procesos_hechos) < len(ORDEN_PROCESOS):
        idx = ORDEN_PROCESOS.index(procesos_hechos[-1])
        siguiente_proceso = ORDEN_PROCESOS[idx + 1]
    
    # Generar opciones de proceso
    opciones = ""
    for proc in ORDEN_PROCESOS:
        if proc not in procesos_hechos:
            selected = "selected" if proc == siguiente_proceso else ""
            opciones += f'<option {selected}>{proc}</option>'
    
    info_orden = "<div style='background:#fff3cd; padding:10px; border-radius:5px; margin-bottom:15px;'>"
    if procesos_hechos:
        info_orden += f"✅ Completados: {', '.join(procesos_hechos)}<br>"
    if siguiente_proceso:
        info_orden += f"⏭️ Siguiente: <b>{siguiente_proceso}</b>"
    info_orden += "</div>"

    return f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {{ font-family: Arial; padding: 15px; background: #f4f4f4; }}
    input, select {{
        width: 100%;
        padding: 10px;
        margin: 8px 0;
        box-sizing: border-box;
    }}
    button {{
        width: 100%;
        padding: 12px;
        background: green;
        color: white;
        border: none;
        border-radius: 8px;
        font-weight: bold;
    }}
    .info {{ background: #fff3cd; padding: 10px; border-radius: 5px; margin-bottom: 15px; }}
    </style>
    </head>

    <body>
    <h2>🛠 Cargar control - {pos}</h2>
    {info_orden}

    <form method="post">
        Proceso:
        <select name="proceso">
            {opciones}
        </select>

        Fecha:
        <input type="date" name="fecha" required>

        Operario:
        <input type="text" name="operario" required>

        Estado:
        <select name="estado">
            <option>OK</option>
            <option>NO APLICA</option>
        </select>

        Reproceso:
        <input type="text" name="reproceso" placeholder="Dejar en blanco si no aplica">

        <button type="submit">💾 Guardar</button>
    </form>
    </body>
    </html>
    """


# ======================
# EDITAR REGISTRO
# ======================
@app.route("/editar/<int:row_id>", methods=["GET","POST"])
def editar(row_id):
    db = get_db()
    row = db.execute("SELECT * FROM procesos WHERE id=?", (row_id,)).fetchone()
    
    if not row:
        return "<h3>❌ Registro no encontrado</h3>"
    
    pos = row[1]
    
    # Validar que la pieza no esté completada
    if pieza_completada(pos):
        return f"""
        <html>
        <head>
        <style>
        body {{ font-family: Arial; padding: 15px; }}
        .error {{ background: #ffcccc; color: red; padding: 15px; border-radius: 5px; }}
        .btn {{ display: inline-block; background: orange; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; margin-top: 10px; }}
        </style>
        </head>
        <body>
        <div class="error">🔒 <b>PIEZA COMPLETADA</b><br>No se puede editar registros</div>
        <a class="btn" href="/pieza/{pos}">⬅️ Volver</a>
        </body>
        </html>
        """
    
    if request.method == "POST":
        db.execute("""
        UPDATE procesos 
        SET fecha=?, operario=?, estado=?, reproceso=?
        WHERE id=?
        """, (
            request.form["fecha"],
            request.form["operario"],
            request.form["estado"],
            request.form["reproceso"],
            row_id
        ))
        db.commit()

        return f"""
        <html>
        <head>
        <style>
        body {{ font-family: Arial; padding: 15px; }}
        .success {{ background: #ccffcc; color: green; padding: 15px; border-radius: 5px; }}
        .btn {{ display: inline-block; background: orange; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; margin-top: 10px; }}
        </style>
        </head>
        <body>
        <div class="success">✅ <b>Actualizado correctamente</b></div>
        <a class="btn" href="/pieza/{pos}">⬅️ Volver</a>
        </body>
        </html>
        """

    return f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {{ font-family: Arial; padding: 15px; background: #f4f4f4; }}
    input, select {{
        width: 100%;
        padding: 10px;
        margin: 8px 0;
        box-sizing: border-box;
    }}
    button {{
        width: 100%;
        padding: 12px;
        background: green;
        color: white;
        border: none;
        border-radius: 8px;
        font-weight: bold;
    }}
    .info {{ background: #e3f2fd; padding: 10px; border-radius: 5px; margin-bottom: 15px; }}
    </style>
    </head>

    <body>
    <h2>✏️ Editar - {row[2]}</h2>
    <div class="info">Pieza: <b>{pos}</b></div>

    <form method="post">
        Fecha:
        <input type="date" name="fecha" value="{row[3]}" required>

        Operario:
        <input type="text" name="operario" value="{row[4]}" required>

        Estado:
        <select name="estado">
            <option {"selected" if row[5] == "OK" else ""}>OK</option>
            <option {"selected" if row[5] == "NO APLICA" else ""}>NO APLICA</option>
        </select>

        Reproceso:
        <input type="text" name="reproceso" value="{row[6]}">

        <button type="submit">💾 Guardar cambios</button>
    </form>
    </body>
    </html>
    """

# ======================
# MÓDULO 1 - ÓRDENES DE TRABAJO
# ======================
@app.route("/modulo/ot")
def ot_lista():
    db = get_db()
    ots = db.execute("SELECT * FROM ordenes_trabajo ORDER BY id DESC").fetchall()
    
    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body { font-family: Arial; padding: 15px; background: #f4f4f4; }
    h2 { color: #333; border-bottom: 3px solid #667eea; padding-bottom: 10px; }
    .btn { display: inline-block; background: #667eea; color: white; padding: 10px 15px; 
           text-decoration: none; border-radius: 5px; margin: 10px 0; }
    .btn:hover { background: #5568d3; }
    .btn-nuevo { background: #43e97b; }
    .btn-nuevo:hover { background: #2cc96e; }
    table { width: 100%; border-collapse: collapse; background: white; margin-top: 20px; 
            box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
    th, td { padding: 12px; border-bottom: 1px solid #ddd; text-align: left; }
    th { background: #667eea; color: white; }
    tr:hover { background: #f5f5f5; }
    .estado-pendiente { background: #ffe5e5; }
    .estado-proceso { background: #fff9e5; }
    .estado-finalizada { background: #e5ffe5; }
    .sin-datos { text-align: center; padding: 30px; color: #999; }
    .header { display: flex; justify-content: space-between; align-items: center; }
    .header a { margin-right: 10px; }
    </style>
    </head>
    <body>
    <div class="header">
        <div>
            <h2>📋 Órdenes de Trabajo</h2>
            <a href="/" class="btn">⬅️ Volver al Inicio</a>
        </div>
        <a href="/modulo/ot/nueva" class="btn btn-nuevo">➕ Nueva OT</a>
    </div>
    """
    
    if len(ots) == 0:
        html += "<div class='sin-datos'>⚠️ No hay órdenes de trabajo registradas</div>"
    else:
        html += """
        <table>
            <tr>
                <th>ID</th>
                <th>Cliente</th>
                <th>Obra</th>
                <th>Título</th>
                <th>Fecha Entrega</th>
                <th>Estado</th>
                <th>Creación</th>
                <th>Acciones</th>
            </tr>
        """
        for ot in ots:
            estado_class = f"estado-{ot[5].lower().replace(' ', '')}"
            html += f"""
            <tr class="{estado_class}">
                <td><b>{ot[0]}</b></td>
                <td>{ot[1]}</td>
                <td>{ot[2]}</td>
                <td>{ot[3]}</td>
                <td>{ot[4]}</td>
                <td>{ot[5]}</td>
                <td>{ot[6]}</td>
                <td>
                    <a href="/modulo/ot/editar/{ot[0]}" class="btn" style="background: #4facfe;">Editar</a>
                    <a href="/modulo/ot/eliminar/{ot[0]}" class="btn" style="background: #fa709a;" onclick="return confirm('¿Eliminar?')">Eliminar</a>
                </td>
            </tr>
            """
        html += "</table>"
    
    html += """
    </body>
    </html>
    """
    return html

@app.route("/modulo/ot/nueva", methods=["GET", "POST"])
def ot_nueva():
    if request.method == "POST":
        db = get_db()
        db.execute("""
        INSERT INTO ordenes_trabajo (cliente, obra, titulo, fecha_entrega, estado)
        VALUES (?, ?, ?, ?, ?)
        """, (
            request.form["cliente"],
            request.form["obra"],
            request.form["titulo"],
            request.form["fecha_entrega"],
            request.form["estado"]
        ))
        db.commit()
        return redirect("/modulo/ot")
    
    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body { font-family: Arial; padding: 15px; background: #f4f4f4; }
    h2 { color: #333; border-bottom: 3px solid #667eea; padding-bottom: 10px; }
    form { background: white; padding: 20px; border-radius: 5px; max-width: 600px; }
    input, select { width: 100%; padding: 10px; margin: 8px 0; border: 1px solid #ddd; 
                    border-radius: 4px; box-sizing: border-box; }
    label { display: block; margin-top: 15px; font-weight: bold; }
    button { width: 100%; padding: 12px; background: #43e97b; color: white; 
             border: none; border-radius: 4px; font-weight: bold; cursor: pointer; margin-top: 20px; }
    button:hover { background: #2cc96e; }
    .btn-cancel { background: #999; margin-top: 10px; }
    .btn-cancel:hover { background: #777; }
    </style>
    </head>
    <body>
    <h2>📋 Nueva Orden de Trabajo</h2>
    <form method="post">
        <label>Cliente:</label>
        <input type="text" name="cliente" required>
        
        <label>Obra:</label>
        <input type="text" name="obra" required>
        
        <label>Título OT:</label>
        <input type="text" name="titulo" required>
        
        <label>Fecha de Entrega:</label>
        <input type="date" name="fecha_entrega" required>
        
        <label>Estado:</label>
        <select name="estado" required>
            <option value="Pendiente">Pendiente</option>
            <option value="En proceso">En proceso</option>
            <option value="Finalizada">Finalizada</option>
        </select>
        
        <button type="submit">💾 Crear OT</button>
        <a href="/modulo/ot" class="btn-cancel" style="text-align: center; text-decoration: none; color: white; display: block;
           padding: 12px; border-radius: 4px;">Cancelar</a>
    </form>
    </body>
    </html>
    """
    return html

@app.route("/modulo/ot/editar/<int:ot_id>", methods=["GET", "POST"])
def ot_editar(ot_id):
    db = get_db()
    ot = db.execute("SELECT * FROM ordenes_trabajo WHERE id=?", (ot_id,)).fetchone()
    
    if not ot:
        return "<h3>❌ Orden no encontrada</h3>"
    
    if request.method == "POST":
        db.execute("""
        UPDATE ordenes_trabajo 
        SET cliente=?, obra=?, titulo=?, fecha_entrega=?, estado=?
        WHERE id=?
        """, (
            request.form["cliente"],
            request.form["obra"],
            request.form["titulo"],
            request.form["fecha_entrega"],
            request.form["estado"],
            ot_id
        ))
        db.commit()
        return redirect("/modulo/ot")
    
    html = f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {{ font-family: Arial; padding: 15px; background: #f4f4f4; }}
    h2 {{ color: #333; border-bottom: 3px solid #667eea; padding-bottom: 10px; }}
    form {{ background: white; padding: 20px; border-radius: 5px; max-width: 600px; }}
    input, select {{ width: 100%; padding: 10px; margin: 8px 0; border: 1px solid #ddd; 
                    border-radius: 4px; box-sizing: border-box; }}
    label {{ display: block; margin-top: 15px; font-weight: bold; }}
    button {{ width: 100%; padding: 12px; background: #43e97b; color: white; 
             border: none; border-radius: 4px; font-weight: bold; cursor: pointer; margin-top: 20px; }}
    button:hover {{ background: #2cc96e; }}
    </style>
    </head>
    <body>
    <h2>✏️ Editar Orden de Trabajo</h2>
    <form method="post">
        <label>Cliente:</label>
        <input type="text" name="cliente" value="{ot[1]}" required>
        
        <label>Obra:</label>
        <input type="text" name="obra" value="{ot[2]}" required>
        
        <label>Título OT:</label>
        <input type="text" name="titulo" value="{ot[3]}" required>
        
        <label>Fecha de Entrega:</label>
        <input type="date" name="fecha_entrega" value="{ot[4]}" required>
        
        <label>Estado:</label>
        <select name="estado" required>
            <option value="Pendiente" {"selected" if ot[5] == "Pendiente" else ""}>Pendiente</option>
            <option value="En proceso" {"selected" if ot[5] == "En proceso" else ""}>En proceso</option>
            <option value="Finalizada" {"selected" if ot[5] == "Finalizada" else ""}>Finalizada</option>
        </select>
        
        <button type="submit">💾 Actualizar OT</button>
    </form>
    </body>
    </html>
    """
    return html

@app.route("/modulo/ot/eliminar/<int:ot_id>")
def ot_eliminar(ot_id):
    db = get_db()
    db.execute("DELETE FROM ordenes_trabajo WHERE id=?", (ot_id,))
    db.commit()
    return redirect("/modulo/ot")

# ======================
# MÓDULO 2 - CALIDAD (Placeholder)
# ======================
@app.route("/modulo/calidad")
def calidad():
    html = """
    <html><head><style>
    body { font-family: Arial; padding: 20px; background: #f4f4f4; }
    h2 { color: #333; }
    .btn { display: inline-block; background: #667eea; color: white; padding: 10px 15px; 
           text-decoration: none; border-radius: 5px; }
    </style></head><body>
    <a href="/" class="btn">⬅️ Volver</a>
    <h2>🧪 Módulo Calidad (En desarrollo)</h2>
    <p>SubMódulos:</p>
    <ul>
        <li>📌 Recepción de Materiales</li>
        <li>📱 Control en Proceso (Escaneo QR)</li>
        <li>📦 Control de Despacho</li>
    </ul>
    </body></html>
    """
    return html

# ======================
# MÓDULO 3 - PARTE SEMANAL (Placeholder)
# ======================
@app.route("/modulo/parte")
def parte_semanal():
    html = """
    <html><head><style>
    body { font-family: Arial; padding: 20px; background: #f4f4f4; }
    h2 { color: #333; }
    .btn { display: inline-block; background: #667eea; color: white; padding: 10px 15px; 
           text-decoration: none; border-radius: 5px; }
    </style></head><body>
    <a href="/" class="btn">⬅️ Volver</a>
    <h2>⏱ Parte Semanal (En desarrollo)</h2>
    <p>Registro de horas por operario y actividad</p>
    </body></html>
    """
    return html

# ======================
# MÓDULO 4 - REMITOS (Placeholder)
# ======================
@app.route("/modulo/remito")
def remitos():
    html = """
    <html><head><style>
    body { font-family: Arial; padding: 20px; background: #f4f4f4; }
    h2 { color: #333; }
    .btn { display: inline-block; background: #667eea; color: white; padding: 10px 15px; 
           text-decoration: none; border-radius: 5px; }
    </style></head><body>
    <a href="/" class="btn">⬅️ Volver</a>
    <h2>🚚 Remitos (En desarrollo)</h2>
    <p>Generación de remitos y documentos de entrega</p>
    </body></html>
    """
    return html

# ======================
# MÓDULO 5 - ESTADO DE PRODUCCIÓN (Placeholder)
# ======================
@app.route("/modulo/estado")
def estado_produccion():
    html = """
    <html><head><style>
    body { font-family: Arial; padding: 20px; background: #f4f4f4; }
    h2 { color: #333; }
    .btn { display: inline-block; background: #667eea; color: white; padding: 10px 15px; 
           text-decoration: none; border-radius: 5px; }
    </style></head><body>
    <a href="/" class="btn">⬅️ Volver</a>
    <h2>📊 Estado de Producción (En desarrollo)</h2>
    <p>Tablero de control e indicadores de producción</p>
    </body></html>
    """
    return html

# ======================
# MÓDULO 6 - PRODUCCIÓN (Versión anterior)
# ======================
@app.route("/modulo/produccion")
def produccion():
    html = """
    <html><head><style>
    body { font-family: Arial; padding: 20px; background: #f4f4f4; }
    h2 { color: #333; }
    .btn { display: inline-block; background: #667eea; color: white; padding: 10px 15px; 
           text-decoration: none; border-radius: 5px; }
    </style></head><body>
    <a href="/" class="btn">⬅️ Volver</a>
    <h2>🏭 Producción - Sistema de Picado</h2>
    <a href="/home" class="btn" style="background: #f093fb;">Ir al Sistema de Picado por Posición</a>
    </body></html>
    """
    return html

# ======================
app.run(host="0.0.0.0", port=5000, debug=True)
