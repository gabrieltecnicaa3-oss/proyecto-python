from flask import Flask, request, redirect, send_file, jsonify, session
from proceso_utils import (
    ORDEN_PROCESOS,
    _extraer_ciclos_reinspeccion,
    _estado_control_aprueba,
    _proceso_aprobado,
    _estado_pieza_persistente,
    _registrar_trazabilidad,
    _agregar_ciclo_reinspeccion,
    _obtener_timeline_pieza,
    obtener_procesos_completados,
    pieza_completada,
    validar_siguiente_proceso,
)
from ot_routes import ot_bp
from db_utils import (
    get_db,
    _resolver_ot_id_para_obra,
    _normalizar_nombre_carpeta,
    _normalizar_nombre_archivo,
    _asegurar_estructura_databook as _db_asegurar_estructura_databook,
    _asegurar_estructura_databook_si_valida as _db_asegurar_estructura_databook_si_valida,
    _obtener_ots_para_obra,
    _obtener_ot_id_pieza,
    _guardar_pdf_databook as _db_guardar_pdf_databook,
    _completar_metadatos_por_obra_pos,
    _normalizar_texto_busqueda,
    _format_cantidad_1_decimal,
    _resolver_imagen_firma_empleado as _db_resolver_imagen_firma_empleado,
    _url_firma_desde_path as _db_url_firma_desde_path,
    _obtener_responsables_control as _db_obtener_responsables_control,
    _ruta_firma_responsable as _db_ruta_firma_responsable,
    _obtener_operarios_disponibles as _db_obtener_operarios_disponibles,
)
from qr_utils import (
    load_clean_excel,
    find_col,
    clean_xls as _clean_xls,
    obtener_firma_ok_path as _qr_obtener_firma_ok_path,
    upsert_piezas_desde_excel,
)
import csv
import html as html_lib
import json
import sqlite3
# import pandas as pd  # Importación lazy: se importa en generar_etiquetas_qr() cuando sea necesario
import qrcode
import os
import re
import tempfile
import unicodedata
from urllib.parse import quote, urlencode, parse_qs, unquote
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Image, Paragraph, PageBreak, Spacer, KeepInFrame
from reportlab.lib import colors
from reportlab.lib.pagesizes import A3, landscape, letter
from reportlab.lib.units import mm, cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from io import BytesIO, StringIO
from datetime import timedelta
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.secret_key = os.environ.get("APP_SECRET_KEY", "dev-secret-change-me")
app.permanent_session_lifetime = timedelta(days=30)
app.register_blueprint(ot_bp)

ROLE_ADMIN = "administrador"
ROLE_SUPERVISOR = "supervisor"
ROLE_OBRA = "obra"
ALLOWED_ROLES = {ROLE_ADMIN, ROLE_SUPERVISOR, ROLE_OBRA}

PUBLIC_PATHS = {
    "/login",
    "/logout",
    "/logo-a3",
    "/firma-ok",
}

OBRA_RESTRICTED_PREFIXES = (
    "/cargar",
    "/editar",
    "/proceso/eliminar",
    "/home/eliminar",
    "/admin/usuarios",
    "/modulo/ot/nueva",
    "/modulo/ot/editar",
    "/modulo/ot/eliminar",
    "/modulo/ot/cerrar",
    "/modulo/ot/reabrir",
    "/modulo/parte",
    "/eliminar-remito",
    "/modulo/generador",
    "/modulo/calidad/recepcion",
    "/modulo/calidad/despacho",
    "/modulo/calidad/escaneo/controles-pintura",
    "/modulo/calidad/escaneo/generar-pdf-control",
    "/modulo/calidad/escaneo/generar-pdf-pintura",
    "/modulo/calidad/escaneo/editar-control-pintura",
    "/modulo/calidad/escaneo/formulario-control-pintura",
)

OBRA_ALLOWED_POST_PREFIXES = (
    "/modulo/calidad/escaneo/qr",
    "/procesar-qr",
    "/qr/seleccionar-ot",
)


def _session_user_role():
    return str(session.get("user_role") or "").strip().lower()


def _is_logged_in():
    return bool(session.get("user_id"))


def _is_admin_session():
    return _session_user_role() == ROLE_ADMIN


def _is_obra_session():
    return _session_user_role() == ROLE_OBRA


def _rol_puede_acceder(role, path, method):
    if role in (ROLE_ADMIN, ROLE_SUPERVISOR):
        return True

    if role != ROLE_OBRA:
        return False

    p = str(path or "").lower()

    metodo = str(method or "").upper()
    if metodo not in ("GET", "HEAD"):
        if not any(p.startswith(prefix) for prefix in OBRA_ALLOWED_POST_PREFIXES):
            return False

    if any(p.startswith(prefix) for prefix in OBRA_RESTRICTED_PREFIXES):
        return False
    if p.startswith("/descargar-remito/"):
        return True
    if "export" in p or "pdf" in p or "imprimir" in p:
        return False

    return True


def _respuesta_sin_permiso():
    return """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body { font-family: Arial; background:#f4f4f4; padding: 16px; }
    .box { max-width: 560px; margin: 0 auto; background: #fff; border-radius: 10px; padding: 16px; border: 1px solid #e5e7eb; }
    .warn { background:#fee2e2; color:#991b1b; padding: 12px; border-radius: 8px; }
    a { display:inline-block; margin-top:10px; background:#2563eb; color:#fff; text-decoration:none; padding:10px 14px; border-radius:8px; }
    </style>
    </head>
    <body>
      <div class="box">
        <div class="warn"><b>Sin permisos para esta acción.</b></div>
        <a href="/">Volver al inicio</a>
      </div>
    </body>
    </html>
    """, 403


def _crear_admin_por_defecto_si_no_hay(db):
    row = db.execute("SELECT COUNT(1) FROM usuarios").fetchone()
    total = int(row[0]) if row else 0
    if total == 0:
        db.execute(
            """
            INSERT INTO usuarios (username, password_hash, nombre, rol, activo)
            VALUES (?, ?, ?, ?, 1)
            """,
            (
                "admin",
                generate_password_hash("admin123"),
                "Administrador",
                ROLE_ADMIN,
            ),
        )


USUARIOS_INICIALES = [
    ("admin_GI", "adminGI", "Gabriel Ibarra", ROLE_ADMIN, 1),
    ("sup_LA", "temp1234", "Leandro Abella", ROLE_SUPERVISOR, 1),
    ("sup_FT", "temp1234", "Franco Tizone", ROLE_SUPERVISOR, 1),
    ("sup_DH", "temp1234", "Daniel Hereñu", ROLE_SUPERVISOR, 1),
    ("sup_AP", "temp1234", "Agustin Pascual", ROLE_SUPERVISOR, 1),
    ("obra_MF", "temp1234", "Martin Flores", ROLE_OBRA, 1),
    ("obra_JP", "temp1234", "Jose Pugno", ROLE_OBRA, 1),
    ("obra_LC", "temp1234", "Lucas Chiabrando", ROLE_OBRA, 1),
    ("obra_MA", "temp1234", "Maxi Audet", ROLE_OBRA, 1),
    ("obra_ES", "temp1234", "Esteban Severino", ROLE_OBRA, 1),
    ("obra_DL", "temp1234", "Diego Lombardi", ROLE_OBRA, 1),
    ("obra_RO", "temp1234", "Rocio Oberti", ROLE_OBRA, 1),
    ("obra_DM", "temp1234", "Diego Mendez", ROLE_OBRA, 1),
    ("obra_JC", "temp1234", "Javier Castiglione", ROLE_OBRA, 1),
    ("obra_FS", "temp1234", "Franco Sanchez", ROLE_OBRA, 1),
    ("obra_AD", "temp1234", "Adrian Dominguez", ROLE_OBRA, 1),
    ("obra_EL", "temp1234", "Emanuel Lizalde", ROLE_OBRA, 1),
    ("obra_FR", "temp1234", "Facundo Rodriguez", ROLE_OBRA, 1),
    ("obra_DE", "temp1234", "Diego Estalle", ROLE_OBRA, 1),
]


def _normalizar_rol_usuario(rol):
    r = str(rol or "").strip().lower()
    if r in ALLOWED_ROLES:
        return r
    if r in ("admin", "administrador"):
        return ROLE_ADMIN
    if r in ("supervisor",):
        return ROLE_SUPERVISOR
    if r in ("obra",):
        return ROLE_OBRA
    return ""


def _cargar_usuarios_iniciales(db):
    for username, password, nombre, rol, activo in USUARIOS_INICIALES:
        ya_existe = db.execute(
            """
            SELECT id FROM usuarios
            WHERE LOWER(TRIM(username)) = LOWER(TRIM(?))
            LIMIT 1
            """,
            (username,),
        ).fetchone()
        if ya_existe:
            continue

        rol_norm = _normalizar_rol_usuario(rol)
        if not rol_norm:
            continue

        db.execute(
            """
            INSERT INTO usuarios (username, password_hash, nombre, rol, activo)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                username.strip(),
                generate_password_hash(str(password or "")),
                str(nombre or "").strip(),
                rol_norm,
                1 if int(activo or 0) == 1 else 0,
            ),
        )

FIRMA_OK_AUTOMATICA = "GABRIEL IBARRA"
FIRMA_OK_CANDIDATOS = [
    "FIRMA GABI.png",
    "FIRMA_OK.png",
    "FIRMA_OK.jpg",
    "FIRMA_GABRIEL_IBARRA.png",
    "FIRMA_GABRIEL_IBARRA.jpg",
]
INSPECTOR_FIRMAS = {
    "Leandro Abella": "LEANDRO ABELLA",
    "Gabriel Ibarra": FIRMA_OK_AUTOMATICA,
    "Daniel Hereñu": "DANIEL HEREÑU",
}

# ======================
# RUTAS ABSOLUTAS
# ======================
APP_DIR = os.path.dirname(os.path.abspath(__file__))
REMITOS_DIR = os.path.join(APP_DIR, "remitos")
QRS_DIR = os.path.join(APP_DIR, "qrs")
FIRMAS_EMPLEADOS_DIR = os.path.join(APP_DIR, "Firmas empleados")
DATABOOKS_DIR = os.path.join(APP_DIR, "Reportes Produccion")

# ======================
# CREAR CARPETAS NECESARIAS
# ======================
for carpeta in [REMITOS_DIR, QRS_DIR, FIRMAS_EMPLEADOS_DIR, DATABOOKS_DIR]:
    if not os.path.exists(carpeta):
        os.makedirs(carpeta)


DATABOOK_SECCIONES = {
    "calidad_recepcion": os.path.join("1-Calidad (Data Book)", "1.1-Recepcion de material"),
    "calidad_corte_perfiles": os.path.join("1-Calidad (Data Book)", "1.2-Corte perfiles"),
    "calidad_armado_soldadura": os.path.join("1-Calidad (Data Book)", "1.3-Armado y soldadura"),
    "calidad_pintura": os.path.join("1-Calidad (Data Book)", "1.4-Pintura"),
    "calidad_despacho": os.path.join("1-Calidad (Data Book)", "1.5-Despacho"),
    "remitos": "2-Remitos de despacho",
}


def _asegurar_estructura_databook(obra, ot_id=None):
    return _db_asegurar_estructura_databook(obra, DATABOOKS_DIR, DATABOOK_SECCIONES, ot_id=ot_id)


def _asegurar_estructura_databook_si_valida(obra, ot_id=None):
    return _db_asegurar_estructura_databook_si_valida(obra, DATABOOKS_DIR, DATABOOK_SECCIONES, ot_id=ot_id)


def _guardar_pdf_databook(obra, seccion_key, filename, pdf_bytes, ot_id=None):
    return _db_guardar_pdf_databook(obra, seccion_key, filename, pdf_bytes, DATABOOKS_DIR, DATABOOK_SECCIONES, ot_id=ot_id)


def _resolver_imagen_firma_empleado(nombre, firma_electronica):
    return _db_resolver_imagen_firma_empleado(nombre, firma_electronica, FIRMAS_EMPLEADOS_DIR)


def _url_firma_desde_path(firma_imagen_path):
    return _db_url_firma_desde_path(firma_imagen_path, FIRMAS_EMPLEADOS_DIR)


def _obtener_responsables_control(db):
    return _db_obtener_responsables_control(db, FIRMAS_EMPLEADOS_DIR, INSPECTOR_FIRMAS)


def _ruta_firma_responsable(responsables_control, responsable):
    return _db_ruta_firma_responsable(responsables_control, responsable, FIRMAS_EMPLEADOS_DIR)


def _obtener_operarios_disponibles(db):
    return _db_obtener_operarios_disponibles(db)


def obtener_firma_ok_path():
    return _qr_obtener_firma_ok_path(APP_DIR, FIRMA_OK_CANDIDATOS)

def init_db():
    db = get_db()
    db.execute("""
    CREATE TABLE IF NOT EXISTS procesos (
        id INTEGER PRIMARY KEY,
        posicion TEXT,
        obra TEXT,
        cantidad REAL,
        perfil TEXT,
        peso REAL,
        descripcion TEXT,
        proceso TEXT,
        fecha TEXT,
        operario TEXT,
        estado TEXT,
        reproceso TEXT,
        re_inspeccion TEXT,
        firma_digital TEXT,
        estado_pieza TEXT,
        escaneado_qr INTEGER DEFAULT 0
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
        estado_avance INTEGER DEFAULT 0,
        fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
        hs_previstas REAL DEFAULT 0,
        tipo_estructura TEXT,
        esquema_pintura TEXT,
        espesor_total_requerido TEXT,
        fecha_cierre DATETIME
    )
    """)

    # Migración automática de columnas nuevas
    try:
        cursor = db.execute("PRAGMA table_info(ordenes_trabajo)")
        ot_columns = {row[1] for row in cursor.fetchall()}
        if 'esquema_pintura' not in ot_columns:
            db.execute("ALTER TABLE ordenes_trabajo ADD COLUMN esquema_pintura TEXT")
            db.commit()
        if 'espesor_total_requerido' not in ot_columns:
            db.execute("ALTER TABLE ordenes_trabajo ADD COLUMN espesor_total_requerido TEXT")
            db.commit()
    except Exception:
        pass
    
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
        posicion TEXT,
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
        obra TEXT,
        fecha TEXT,
        responsable TEXT,
        conforme TEXT,
        observaciones TEXT,
        detalle_control TEXT,
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
        firma_digital TEXT,
        firma_imagen_path TEXT,
        actividad TEXT,
        FOREIGN KEY (ot_id) REFERENCES ordenes_trabajo(id)
    )
    """)

    db.execute("""
    CREATE TABLE IF NOT EXISTS empleados_parte (
        id INTEGER PRIMARY KEY,
        nombre TEXT UNIQUE,
        puesto TEXT,
        firma_electronica TEXT,
        firma_imagen_path TEXT,
        fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
        fecha_actualizacion DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)

    db.execute("""
    CREATE TABLE IF NOT EXISTS usuarios (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        nombre TEXT,
        rol TEXT NOT NULL,
        activo INTEGER DEFAULT 1,
        ultimo_login DATETIME,
        creado_en DATETIME DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    db.execute("""
    CREATE TABLE IF NOT EXISTS procesos (
        id INTEGER PRIMARY KEY,
        posicion TEXT,
        obra TEXT,
        proceso TEXT,
        fecha TEXT,
        operario TEXT,
        estado TEXT,
        reproceso TEXT,
        re_inspeccion TEXT,
        firma_digital TEXT,
        estado_pieza TEXT,
        escaneado_qr INTEGER DEFAULT 0,
        ot_id INTEGER,
        eliminado INTEGER DEFAULT 0
    )
    """)

    # Migración automática del campo eliminado
    try:
        cursor = db.execute("PRAGMA table_info(procesos)")
        columns = {row[1] for row in cursor.fetchall()}
        if 'eliminado' not in columns:
            db.execute("ALTER TABLE procesos ADD COLUMN eliminado INTEGER DEFAULT 0")
            db.commit()
    except Exception:
        pass

    db.execute("""
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
    """)

    db.execute("""
    CREATE TABLE IF NOT EXISTS trazabilidad_estados (
        id INTEGER PRIMARY KEY,
        fecha_evento DATETIME DEFAULT CURRENT_TIMESTAMP,
        proceso_id INTEGER,
        posicion TEXT,
        obra TEXT,
        proceso TEXT,
        estado_control TEXT,
        estado_pieza TEXT,
        firma_digital TEXT,
        accion TEXT,
        re_inspeccion TEXT,
        tipo_evento TEXT
    )
    """)

    db.execute("""
    CREATE TABLE IF NOT EXISTS control_pintura (
        id INTEGER PRIMARY KEY,
        obra TEXT,
        fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
        fecha_modificacion DATETIME DEFAULT CURRENT_TIMESTAMP,
        mediciones TEXT,
        piezas TEXT,
        estado TEXT DEFAULT 'activo',
        usuario_creacion TEXT,
        usuario_modificacion TEXT
    )
    """)
    
    db.commit()
    
    # Migración: Agregar columnas faltantes y limpiar datos incorrectos
    try:
        # Verificar columnas en ordenes_trabajo
        cursor = db.execute("PRAGMA table_info(ordenes_trabajo)")
        ot_columns = {row[1] for row in cursor.fetchall()}
        
        if 'estado_avance' not in ot_columns:
            try:
                db.execute("ALTER TABLE ordenes_trabajo ADD COLUMN estado_avance INTEGER DEFAULT 0")
                db.commit()
            except Exception:
                pass
        
        if 'fecha_creacion' not in ot_columns:
            try:
                db.execute("ALTER TABLE ordenes_trabajo ADD COLUMN fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP")
                db.commit()
            except Exception:
                pass

        if 'hs_previstas' not in ot_columns:
            try:
                db.execute("ALTER TABLE ordenes_trabajo ADD COLUMN hs_previstas REAL DEFAULT 0")
                db.commit()
            except Exception:
                pass

        if 'fecha_cierre' not in ot_columns:
            try:
                db.execute("ALTER TABLE ordenes_trabajo ADD COLUMN fecha_cierre DATETIME")
                db.commit()
            except Exception:
                pass

        if 'tipo_estructura' not in ot_columns:
            try:
                db.execute("ALTER TABLE ordenes_trabajo ADD COLUMN tipo_estructura TEXT")
                db.commit()
            except Exception:
                pass
        
        if 'es_mantenimiento' not in ot_columns:
            try:
                db.execute("ALTER TABLE ordenes_trabajo ADD COLUMN es_mantenimiento INTEGER DEFAULT 0")
                db.commit()
            except Exception:
                pass
        
        # Verificar columnas en procesos
        cursor = db.execute("PRAGMA table_info(procesos)")
        proc_columns = {row[1] for row in cursor.fetchall()}
        
        if 'ot_id' not in proc_columns:
            try:
                db.execute("ALTER TABLE procesos ADD COLUMN ot_id INTEGER")
                db.commit()
            except Exception:
                pass
        
        # Agregar columnas que podrían faltar en procesos
        for col_name, col_type in [('obra', 'TEXT'), ('cantidad', 'REAL'), ('perfil', 'TEXT'), ('peso', 'REAL'), ('descripcion', 'TEXT')]:
            if col_name not in proc_columns:
                try:
                    db.execute(f"ALTER TABLE procesos ADD COLUMN {col_name} {col_type}")
                    db.commit()
                except Exception:
                    pass

        if 'escaneado_qr' not in proc_columns:
            try:
                db.execute("ALTER TABLE procesos ADD COLUMN escaneado_qr INTEGER DEFAULT 0")
                db.commit()
            except Exception:
                pass

        if 're_inspeccion' not in proc_columns:
            try:
                db.execute("ALTER TABLE procesos ADD COLUMN re_inspeccion TEXT")
                db.commit()
            except Exception:
                pass

        if 'firma_digital' not in proc_columns:
            try:
                db.execute("ALTER TABLE procesos ADD COLUMN firma_digital TEXT")
                db.commit()
            except Exception:
                pass

        if 'estado_pieza' not in proc_columns:
            try:
                db.execute("ALTER TABLE procesos ADD COLUMN estado_pieza TEXT")
                db.commit()
            except Exception:
                pass

        cursor = db.execute("PRAGMA table_info(control_despacho)")
        despacho_columns = {row[1] for row in cursor.fetchall()}

        if 'obra' not in despacho_columns:
            try:
                db.execute("ALTER TABLE control_despacho ADD COLUMN obra TEXT")
                db.commit()
            except Exception:
                pass

        if 'detalle_control' not in despacho_columns:
            try:
                db.execute("ALTER TABLE control_despacho ADD COLUMN detalle_control TEXT")
                db.commit()
            except Exception:
                pass

        cursor = db.execute("PRAGMA table_info(partes_trabajo)")
        partes_columns = {row[1] for row in cursor.fetchall()}

        if 'firma_digital' not in partes_columns:
            try:
                db.execute("ALTER TABLE partes_trabajo ADD COLUMN firma_digital TEXT")
                db.commit()
            except Exception:
                pass

        if 'firma_imagen_path' not in partes_columns:
            try:
                db.execute("ALTER TABLE partes_trabajo ADD COLUMN firma_imagen_path TEXT")
                db.commit()
            except Exception:
                pass

        cursor = db.execute("PRAGMA table_info(empleados_parte)")
        empleados_parte_columns = {row[1] for row in cursor.fetchall()}

        if 'firma_imagen_path' not in empleados_parte_columns:
            try:
                db.execute("ALTER TABLE empleados_parte ADD COLUMN firma_imagen_path TEXT")
                db.commit()
            except Exception:
                pass

        cursor = db.execute("PRAGMA table_info(usuarios)")
        usuarios_columns = {row[1] for row in cursor.fetchall()}

        if 'nombre' not in usuarios_columns:
            try:
                db.execute("ALTER TABLE usuarios ADD COLUMN nombre TEXT")
                db.commit()
            except Exception:
                pass

        if 'rol' not in usuarios_columns:
            try:
                db.execute(f"ALTER TABLE usuarios ADD COLUMN rol TEXT DEFAULT '{ROLE_OBRA}'")
                db.commit()
            except Exception:
                pass

        if 'activo' not in usuarios_columns:
            try:
                db.execute("ALTER TABLE usuarios ADD COLUMN activo INTEGER DEFAULT 1")
                db.commit()
            except Exception:
                pass

        if 'ultimo_login' not in usuarios_columns:
            try:
                db.execute("ALTER TABLE usuarios ADD COLUMN ultimo_login DATETIME")
                db.commit()
            except Exception:
                pass

        if 'creado_en' not in usuarios_columns:
            try:
                db.execute("ALTER TABLE usuarios ADD COLUMN creado_en DATETIME DEFAULT CURRENT_TIMESTAMP")
                db.commit()
            except Exception:
                pass
        
        # Limpiar datos incorrectos: Si obra contiene nombres de procesos, borrar
        try:
            procesos_invalidos = ("ARMADO", "SOLDADURA", "PINTURA", "DESPACHO")
            for proc in procesos_invalidos:
                db.execute("UPDATE procesos SET obra = NULL WHERE obra = ?", (proc,))
            db.commit()
        except Exception:
            pass
        
        # Intentar vincular posiciones con órdenes de trabajo por nombre de obra
        try:
            # Obtener todas las posiciones sin obra asignada
            posiciones_sin_obra = db.execute("""
                SELECT DISTINCT posicion FROM procesos 
                    WHERE posicion IS NOT NULL AND (obra IS NULL OR obra = '')  
                ORDER BY posicion
            """).fetchall()
            
            for pos_tuple in posiciones_sin_obra:
                pos = pos_tuple[0]
                # Intentar asignar la obra más reciente a esta posición (la de la última OT disponible)
                ot = db.execute("""
                    SELECT obra FROM ordenes_trabajo 
                    WHERE estado != 'Finalizada' AND fecha_cierre IS NULL
                    ORDER BY fecha_creacion DESC 
                    LIMIT 1
                """).fetchone()
                
                if ot and ot[0]:
                    try:
                        db.execute(
                            "UPDATE procesos SET obra = ? WHERE posicion = ? AND (obra IS NULL OR obra = '')",
                            (ot[0], pos)
                        )
                    except Exception:
                        pass
            
            db.commit()
        except Exception:
            pass
        
        # Crear OTs de mantenimiento fijas si no existen
        try:
            ots_mantenimiento = [
                (1, "ADM TALLER EEEMM", "Administración de Taller EEEMM"),
                (2, "ADM OBRAS", "Administración de Obras"),
            ]
            
            for ot_id, obra, titulo in ots_mantenimiento:
                existe = db.execute(
                    "SELECT id FROM ordenes_trabajo WHERE id = ?",
                    (ot_id,)
                ).fetchone()
                
                if not existe:
                    db.execute(
                        """INSERT INTO ordenes_trabajo 
                           (id, cliente, obra, titulo, estado, es_mantenimiento) 
                           VALUES (?, ?, ?, ?, ?, 1)""",
                        (ot_id, "MANTENIMIENTO", obra, titulo, "Activa")
                    )
            
            db.commit()
        except Exception:
            pass
            
    except Exception:
        pass  # Si hay un error en la migración, continuamos normalmente

    # Asegurar estructura de carpetas para todas las obras con OTs activas
    try:
        obras_activas = db.execute("""
            SELECT DISTINCT TRIM(obra) FROM ordenes_trabajo
            WHERE obra IS NOT NULL AND TRIM(obra) != ''
              AND fecha_cierre IS NULL
              AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
        """).fetchall()
        for (obra_row,) in obras_activas:
            _asegurar_estructura_databook(obra_row)
    except Exception:
        pass

    # Semilla inicial para poder iniciar sesión en entornos nuevos.
    try:
        _crear_admin_por_defecto_si_no_hay(db)
        _cargar_usuarios_iniciales(db)
        db.commit()
    except Exception:
        pass

# Inicializar BD de forma diferida (lazy) en la primera solicitud
_db_initialized = False

def _lazy_init_db():
    global _db_initialized
    if not _db_initialized:
        init_db()
        _db_initialized = True


def _auth_guard():
    path = str(request.path or "")

    if path.startswith("/static/"):
        return None
    if path in PUBLIC_PATHS or path.startswith("/firma-supervisor/"):
        return None

    if not _is_logged_in():
        next_qs = quote(path)
        return redirect(f"/login?next={next_qs}")

    role = _session_user_role()
    if not _rol_puede_acceder(role, path, request.method):
        return _respuesta_sin_permiso()

    return None


@app.before_request
def antes_de_solicitud():
    _lazy_init_db()
    guard = _auth_guard()
    if guard is not None:
        return guard


@app.route("/login", methods=["GET", "POST"])
def login():
    if _is_logged_in():
        return redirect("/")

    error = ""
    next_url = (request.args.get("next") or "/").strip() or "/"

    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        next_url = (request.form.get("next") or "/").strip() or "/"

        db = get_db()
        row = db.execute(
            """
            SELECT id, username, password_hash, COALESCE(nombre, ''), COALESCE(rol, ''), COALESCE(activo, 1)
            FROM usuarios
            WHERE LOWER(TRIM(username)) = LOWER(TRIM(?))
            LIMIT 1
            """,
            (username,),
        ).fetchone()

        if not row:
            error = "Usuario o contraseña inválidos"
        elif int(row[5] or 0) != 1:
            error = "Usuario inactivo"
        elif not check_password_hash(str(row[2] or ""), password):
            error = "Usuario o contraseña inválidos"
        else:
            role = str(row[4] or "").strip().lower()
            if role not in ALLOWED_ROLES:
                error = "Rol de usuario inválido"
            else:
                session.permanent = True
                session["user_id"] = int(row[0])
                session["username"] = str(row[1])
                session["nombre"] = str(row[3] or "")
                session["user_role"] = role
                db.execute("UPDATE usuarios SET ultimo_login = CURRENT_TIMESTAMP WHERE id = ?", (int(row[0]),))
                db.commit()
                if not next_url.startswith("/"):
                    next_url = "/"
                return redirect(next_url)

    return f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * {{ box-sizing: border-box; }}
    body {{
        margin: 0;
        min-height: 100vh;
        display: grid;
        place-items: center;
        padding: 16px;
        font-family: "Segoe UI", Tahoma, Arial, sans-serif;
        background: radial-gradient(circle at 15% 0%, #f8fbff 0%, #eef3f7 55%, #e8edf3 100%);
    }}
    .card {{
        width: 100%;
        max-width: 420px;
        background: #ffffff;
        border: 1px solid #dbe4ee;
        border-radius: 14px;
        padding: 18px;
        box-shadow: 0 8px 18px rgba(15,23,42,0.08);
    }}
    h2 {{ margin: 0 0 10px 0; color: #0f172a; }}
    p {{ margin: 0 0 14px 0; color: #475569; }}
    label {{ display:block; font-size:13px; font-weight:700; color:#334155; margin-bottom:6px; }}
    input {{ width:100%; padding:10px 12px; border:1px solid #cbd5e1; border-radius:8px; margin-bottom:12px; }}
    button {{ width:100%; padding:12px; background:#0f766e; color:#fff; border:none; border-radius:10px; font-weight:800; cursor:pointer; }}
    button:hover {{ background:#0d6660; }}
    .err {{ background:#fee2e2; color:#991b1b; border:1px solid #fecaca; border-radius:8px; padding:10px; margin-bottom:12px; }}
    .hint {{ margin-top:10px; font-size:12px; color:#64748b; }}
    </style>
    </head>
    <body>
      <form method="post" class="card">
        <h2>Iniciar sesión</h2>
        <p>Acceso al sistema de gestión</p>
        {'<div class="err">' + html_lib.escape(error) + '</div>' if error else ''}
        <input type="hidden" name="next" value="{html_lib.escape(next_url)}">
        <label for="username">Usuario</label>
        <input id="username" name="username" autocomplete="username" required>
        <label for="password">Contraseña</label>
        <input id="password" type="password" name="password" autocomplete="current-password" required>
        <button type="submit">Entrar</button>
        <div class="hint">Usuario inicial: admin / admin123</div>
      </form>
    </body>
    </html>
    """


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/login")


@app.route("/admin/usuarios", methods=["GET", "POST"])
def admin_usuarios():
    if not _is_admin_session():
        return _respuesta_sin_permiso()

    db = get_db()
    mensaje = ""
    error = ""

    if request.method == "POST":
        accion = (request.form.get("accion") or "").strip().lower()

        if accion == "crear":
            username = (request.form.get("username") or "").strip()
            password = request.form.get("password") or ""
            nombre = (request.form.get("nombre") or "").strip()
            rol = _normalizar_rol_usuario(request.form.get("rol") or "")
            activo = 1 if (request.form.get("activo") or "1") == "1" else 0

            if not username:
                error = "El username es obligatorio"
            elif any(ch.isspace() for ch in username):
                error = "El username no puede tener espacios"
            elif len(password) < 4:
                error = "La contraseña debe tener al menos 4 caracteres"
            elif rol not in ALLOWED_ROLES:
                error = "Rol inválido"
            else:
                existe = db.execute(
                    """
                    SELECT id FROM usuarios
                    WHERE LOWER(TRIM(username)) = LOWER(TRIM(?))
                    LIMIT 1
                    """,
                    (username,),
                ).fetchone()
                if existe:
                    error = "Ya existe un usuario con ese username"
                else:
                    db.execute(
                        """
                        INSERT INTO usuarios (username, password_hash, nombre, rol, activo)
                        VALUES (?, ?, ?, ?, ?)
                        """,
                        (username, generate_password_hash(password), nombre, rol, activo),
                    )
                    db.commit()
                    mensaje = f"Usuario creado: {username}"

        elif accion == "toggle":
            user_id_txt = (request.form.get("user_id") or "").strip()
            if not user_id_txt.isdigit():
                error = "Usuario inválido"
            else:
                user_id = int(user_id_txt)
                row = db.execute("SELECT activo, username FROM usuarios WHERE id = ?", (user_id,)).fetchone()
                if not row:
                    error = "Usuario no encontrado"
                else:
                    nuevo_activo = 0 if int(row[0] or 0) == 1 else 1
                    db.execute("UPDATE usuarios SET activo = ? WHERE id = ?", (nuevo_activo, user_id))
                    db.commit()
                    estado_txt = "activado" if nuevo_activo == 1 else "desactivado"
                    mensaje = f"Usuario {row[1]} {estado_txt}"

        elif accion == "reset_password":
            user_id_txt = (request.form.get("user_id") or "").strip()
            nueva_password = request.form.get("nueva_password") or ""
            if not user_id_txt.isdigit():
                error = "Usuario inválido"
            elif len(nueva_password) < 4:
                error = "La nueva contraseña debe tener al menos 4 caracteres"
            else:
                user_id = int(user_id_txt)
                row = db.execute("SELECT username FROM usuarios WHERE id = ?", (user_id,)).fetchone()
                if not row:
                    error = "Usuario no encontrado"
                else:
                    db.execute(
                        "UPDATE usuarios SET password_hash = ? WHERE id = ?",
                        (generate_password_hash(nueva_password), user_id),
                    )
                    db.commit()
                    mensaje = f"Contraseña actualizada para {row[0]}"

        elif accion == "eliminar":
            user_id_txt = (request.form.get("user_id") or "").strip()
            if not user_id_txt.isdigit():
                error = "Usuario inválido"
            else:
                user_id = int(user_id_txt)
                if int(session.get("user_id") or 0) == user_id:
                    error = "No podés eliminar tu propio usuario en sesión"
                else:
                    row = db.execute("SELECT username FROM usuarios WHERE id = ?", (user_id,)).fetchone()
                    if not row:
                        error = "Usuario no encontrado"
                    else:
                        db.execute("DELETE FROM usuarios WHERE id = ?", (user_id,))
                        db.commit()
                        mensaje = f"Usuario eliminado: {row[0]}"

    rows = db.execute(
        """
        SELECT id, username, COALESCE(nombre, ''), COALESCE(rol, ''), COALESCE(activo, 1), COALESCE(ultimo_login, '')
        FROM usuarios
        ORDER BY
            CASE LOWER(TRIM(COALESCE(rol, '')))
                WHEN 'administrador' THEN 0
                WHEN 'supervisor' THEN 1
                ELSE 2
            END,
            LOWER(TRIM(COALESCE(username, '')))
        """
    ).fetchall()

    filas_html = ""
    for user_id, username, nombre, rol, activo, ultimo_login in rows:
        activo_txt = "ACTIVO" if int(activo or 0) == 1 else "INACTIVO"
        activo_bg = "#dcfce7" if int(activo or 0) == 1 else "#fee2e2"
        activo_color = "#166534" if int(activo or 0) == 1 else "#991b1b"
        toggle_label = "Desactivar" if int(activo or 0) == 1 else "Activar"
        filas_html += f"""
        <tr>
            <td>{int(user_id)}</td>
            <td>{html_lib.escape(str(username or ''))}</td>
            <td>{html_lib.escape(str(nombre or ''))}</td>
            <td>{html_lib.escape(str(rol or ''))}</td>
            <td><span style=\"padding:4px 8px;border-radius:999px;background:{activo_bg};color:{activo_color};font-weight:700;\">{activo_txt}</span></td>
            <td>{html_lib.escape(str(ultimo_login or '-'))}</td>
            <td>
                <form method=\"post\" style=\"display:inline-block; margin:2px;\">
                    <input type=\"hidden\" name=\"accion\" value=\"toggle\">
                    <input type=\"hidden\" name=\"user_id\" value=\"{int(user_id)}\">
                    <button type=\"submit\">{toggle_label}</button>
                </form>
                <form method=\"post\" style=\"display:inline-block; margin:2px;\" onsubmit=\"return confirm('¿Eliminar usuario? Esta acción no se puede deshacer.');\">
                    <input type=\"hidden\" name=\"accion\" value=\"eliminar\">
                    <input type=\"hidden\" name=\"user_id\" value=\"{int(user_id)}\">
                    <button type=\"submit\" style=\"background:#dc2626;color:#fff;border:0;\">Eliminar</button>
                </form>
                <form method=\"post\" style=\"display:inline-block; margin:2px;\">
                    <input type=\"hidden\" name=\"accion\" value=\"reset_password\">
                    <input type=\"hidden\" name=\"user_id\" value=\"{int(user_id)}\">
                    <input type=\"password\" name=\"nueva_password\" placeholder=\"Nueva pass\" required style=\"width:120px;\">
                    <button type=\"submit\">Cambiar pass</button>
                </form>
            </td>
        </tr>
        """

    msg_html = f'<div class="ok">{html_lib.escape(mensaje)}</div>' if mensaje else ''
    err_html = f'<div class="err">{html_lib.escape(error)}</div>' if error else ''

    return f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {{ font-family: Arial, sans-serif; background:#f3f4f6; margin:0; padding:16px; color:#111827; }}
    .wrap {{ max-width: 1200px; margin:0 auto; }}
    .card {{ background:#fff; border:1px solid #e5e7eb; border-radius:10px; padding:14px; margin-bottom:12px; }}
    h2 {{ margin: 0 0 10px 0; }}
    .ok {{ background:#dcfce7; border:1px solid #86efac; color:#166534; padding:10px; border-radius:8px; margin-bottom:10px; }}
    .err {{ background:#fee2e2; border:1px solid #fecaca; color:#991b1b; padding:10px; border-radius:8px; margin-bottom:10px; }}
    .grid {{ display:grid; grid-template-columns: repeat(5, minmax(120px, 1fr)); gap:8px; }}
    input, select, button {{ padding:8px 10px; border:1px solid #d1d5db; border-radius:6px; }}
    button {{ cursor:pointer; }}
    table {{ width:100%; border-collapse: collapse; background:#fff; }}
    th, td {{ border-bottom:1px solid #e5e7eb; text-align:left; padding:10px; vertical-align: top; }}
    th {{ background:#f8fafc; }}
    .back {{ display:inline-block; margin-bottom:10px; text-decoration:none; background:#2563eb; color:#fff; padding:8px 12px; border-radius:8px; }}
    @media (max-width: 980px) {{ .grid {{ grid-template-columns: 1fr; }} }}
    </style>
    </head>
    <body>
      <div class="wrap">
        <a href="/" class="back">Volver al panel</a>
        <div class="card">
          <h2>Gestion de usuarios</h2>
          {msg_html}
          {err_html}
          <form method="post" class="grid">
            <input type="hidden" name="accion" value="crear">
            <input name="username" placeholder="username" required>
            <input type="password" name="password" placeholder="password inicial" required>
            <input name="nombre" placeholder="nombre completo" required>
            <select name="rol" required>
                <option value="administrador">administrador</option>
                <option value="supervisor">supervisor</option>
                <option value="obra" selected>obra</option>
            </select>
            <select name="activo" required>
                <option value="1" selected>activo</option>
                <option value="0">inactivo</option>
            </select>
            <button type="submit" style="grid-column: 1 / -1; background:#0f766e; color:#fff; border:0;">Crear usuario</button>
          </form>
        </div>

        <div class="card" style="overflow-x:auto;">
          <table>
            <tr>
              <th>ID</th>
              <th>Username</th>
              <th>Nombre</th>
              <th>Rol</th>
              <th>Estado</th>
              <th>Ultimo login</th>
              <th>Acciones</th>
            </tr>
            {filas_html}
          </table>
        </div>
      </div>
    </body>
    </html>
    """

# ======================
# FUNCIONES GENERADOR QR
# ======================
def generar_etiquetas_qr(excel_file, logo_path, cargar_bd_excel=False):
    """Genera PDF con etiquetas A3 y QR codes"""
    import pandas as pd  # Importación lazy de pandas
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
        
        print(f"\n[DEBUG] Columnas encontradas:")
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
            # Modo recomendado: solo QR
            print("[DEBUG] Carga de BD desde Excel desactivada: solo se registra al escanear QR")
        
        styles = getSampleStyleSheet()
        label_style = ParagraphStyle(
            'LabelStyle',
            parent=styles['Normal'],
            fontSize=10.5,
            leading=11.5,
            alignment=1
        )
        
        # Directorio temporal para QR
        qr_temp_dir = tempfile.mkdtemp()
        
        cols = 6
        rows_per_page = 5
        prefijos_expandibles = ["V", "C", "PU", "INS"]
        prefijos_duplicar_igual = ["A", "T", "G", "BA", "ES"]
        rows_expandidas = []
        
        # Expandir filas según cantidad
        for idx, row in df.iterrows():
            # 1. Limpieza agresiva de la posición
            val_pos = row.get(col_pos, "")
            if pd.isna(val_pos): continue # Salta si está vacío
            
            pos = str(val_pos).strip()
            pos_upper = pos.upper()
            
            # 2. Limpieza de la cantidad (aseguramos que sea número)
            val_cant = row.get(cant_col, 1)
            try:
                # Si viene como 2.0 lo pasa a 2, si es "2" lo pasa a 2
                cant = int(float(str(val_cant).replace(',', '.'))) if val_cant else 1
            except:
                cant = 1
            
            # 3. Verificación de prefijos (usando la variable limpia)
            es_expandible = any(pos_upper.startswith(p) for p in prefijos_expandibles)
            # Forzamos que si empieza con G, entre sí o sí
            es_duplicar_igual = any(pos_upper.startswith(p) for p in prefijos_duplicar_igual) or pos_upper.startswith("G")
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
                        pos   = _clean_xls(row.get(col_pos, ""))
                        plano = _clean_xls(row.get(plano_col, ""))
                        rev   = _clean_xls(row.get(rev_col, ""))
                        obra  = _clean_xls(row.get(obra_col, ""))
                        cant  = _clean_xls(row.get(cant_col, ""))
                        perfil = _clean_xls(row.get(perfil_col, ""))
                        peso  = _clean_xls(row.get(peso_col, ""))
                        desc  = _clean_xls(row.get(desc_col, ""))
                        
                        # Incluir info completa pero manteniendo densidad controlada
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

                        separador = Spacer(1, 2.0*mm)

                        content = [
                            Spacer(1, 1.8*mm),
                            Image(logo_path, width=20*mm, height=16*mm),
                            Paragraph(text, label_style),
                            separador,
                            Image(qr_path, width=30*mm, height=30*mm)
                        ]

                        # Ajusta automáticamente el contenido para evitar superposición en etiquetas con texto largo.
                        # vAlign='TOP' evita espacio extra arriba del logo y abajo del QR.
                        content_fit = KeepInFrame(43*mm, 78*mm, content, mode='shrink', hAlign='CENTER', vAlign='TOP')
                        
                        row_data.append(content_fit)
                        i += 1
                    else:
                        row_data.append("")
                
                data.append(row_data)
            
            has_content = any(any(cell != "" for cell in row) for row in data)
            if has_content:
                table = Table(data, colWidths=45*mm, rowHeights=80*mm)
                table.setStyle(TableStyle([
                    ('GRID', (0,0), (-1,-1), 0.5, colors.black),
                    ('ALIGN', (0,0), (-1,-1), 'CENTER'),
                    ('VALIGN', (0,0), (-1,-1), 'TOP'),
                    ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#FFFFFF')),
                    ('LEFTPADDING', (0,0), (-1,-1), 2),
                    ('RIGHTPADDING', (0,0), (-1,-1), 2),
                    ('TOPPADDING', (0,0), (-1,-1), 0),
                    ('BOTTOMPADDING', (0,0), (-1,-1), 0)
                ]))
                
                elements.append(table)
                
                if page < num_pages - 1:
                    elements.append(PageBreak())
        
        doc.build(elements)
        pdf_buffer.seek(0)
        
        # Limpiar directorio temporal
        import shutil
        shutil.rmtree(qr_temp_dir, ignore_errors=True)
        
        return pdf_buffer
    
    except Exception as e:
        raise Exception(f"Error generando QR: {str(e)}")

# (ORDEN_PROCESOS y funciones de trazabilidad/proceso movidas a proceso_utils.py)


def construir_redirect_desde_qr(qr_data):
    """Normaliza el contenido del QR y arma una URL válida a /pieza/<pos>."""
    if not qr_data:
        return None

    texto = qr_data.strip()
    if not texto:
        return None

    pos = ""
    query_string = ""

    if "/pieza/" in texto:
        fragmento = texto.split("/pieza/", 1)[1]
        if "?" in fragmento:
            pos, query_string = fragmento.split("?", 1)
        else:
            pos = fragmento
    else:
        if "?" in texto:
            pos, query_string = texto.split("?", 1)
        else:
            pos = texto

    pos = pos.strip().strip("/")
    if not pos:
        return None

    permitidos = ["obra", "cant", "perfil", "peso", "desc"]
    params = {}
    if query_string:
        parsed = parse_qs(query_string, keep_blank_values=False)
        for key in permitidos:
            values = parsed.get(key)
            if values and str(values[0]).strip():
                params[key] = str(values[0]).strip()

    obra_qr = str(params.get("obra", "")).strip()
    if obra_qr:
        db = get_db()
        ot_id_existente = _obtener_ot_id_pieza(db, pos, obra_qr)
        if ot_id_existente:
            params["ot_id"] = str(ot_id_existente)
        else:
            ots_obra = _obtener_ots_para_obra(db, obra_qr)
            if len(ots_obra) == 1:
                params["ot_id"] = str(ots_obra[0][0])
            elif len(ots_obra) > 1:
                params_sel = {"pos": pos, **params}
                return f"/qr/seleccionar-ot?{urlencode(params_sel)}"

    url_base = f"/pieza/{quote(pos)}"
    if params:
        return f"{url_base}?{urlencode(params)}"
    return url_base

@app.route("/qr/seleccionar-ot", methods=["GET", "POST"])
def qr_seleccionar_ot():
    pos = (request.values.get("pos") or "").strip()
    obra = (request.values.get("obra") or "").strip()
    cant = (request.values.get("cant") or "").strip()
    perfil = (request.values.get("perfil") or "").strip()
    peso = (request.values.get("peso") or "").strip()
    desc = (request.values.get("desc") or "").strip()

    if not pos or not obra:
        return redirect("/modulo/calidad/escaneo/qr")

    db = get_db()
    ots_obra = _obtener_ots_para_obra(db, obra)

    def _redir_pieza(ot_id_sel=None):
        params = {"obra": obra}
        if cant:
            params["cant"] = cant
        if perfil:
            params["perfil"] = perfil
        if peso:
            params["peso"] = peso
        if desc:
            params["desc"] = desc
        if ot_id_sel:
            params["ot_id"] = str(ot_id_sel)
        return redirect(f"/pieza/{quote(pos)}?{urlencode(params)}")

    if len(ots_obra) == 0:
        return _redir_pieza()
    if len(ots_obra) == 1:
        return _redir_pieza(ots_obra[0][0])

    if request.method == "POST":
        ot_id_txt = (request.form.get("ot_id") or "").strip()
        if not ot_id_txt.isdigit() or int(ot_id_txt) not in {r[0] for r in ots_obra}:
            return "OT inválida para la obra seleccionada", 400
        return _redir_pieza(int(ot_id_txt))

    opciones = "".join(
        f'<option value="{ot_id}">OT {ot_id} - {html_lib.escape(str(titulo or ""))}</option>'
        for ot_id, titulo in ots_obra
    )
    return f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {{ font-family: Arial; padding: 15px; background: #f4f4f4; }}
    .box {{ max-width: 520px; background: white; padding: 18px; border-radius: 8px; }}
    h2 {{ margin-top: 0; color: #0f172a; }}
    select, button {{ width: 100%; padding: 10px; margin-top: 10px; }}
    button {{ background: #2563eb; color: white; border: none; border-radius: 6px; font-weight: bold; }}
    .meta {{ background: #f1f5f9; padding: 8px; border-radius: 6px; font-size: 14px; }}
    </style>
    </head>
    <body>
      <div class="box">
        <h2>Seleccionar OT para la pieza</h2>
        <div class="meta"><b>Posición:</b> {html_lib.escape(pos)}<br><b>Obra:</b> {html_lib.escape(obra)}</div>
        <form method="post">
          <input type="hidden" name="pos" value="{html_lib.escape(pos)}">
          <input type="hidden" name="obra" value="{html_lib.escape(obra)}">
          <input type="hidden" name="cant" value="{html_lib.escape(cant)}">
          <input type="hidden" name="perfil" value="{html_lib.escape(perfil)}">
          <input type="hidden" name="peso" value="{html_lib.escape(peso)}">
          <input type="hidden" name="desc" value="{html_lib.escape(desc)}">
          <select name="ot_id" required>
            <option value="">-- Seleccionar OT --</option>
            {opciones}
          </select>
          <button type="submit">Continuar</button>
        </form>
      </div>
    </body>
    </html>
    """

@app.route("/logo-a3")
def logo_a3():
    logo_path = os.path.join(APP_DIR, "LOGO.png")
    if os.path.exists(logo_path):
        return send_file(logo_path)
    return "Logo no encontrado", 404


@app.route("/firma-ok")
def firma_ok():
    firma_path = obtener_firma_ok_path()
    if firma_path:
        return send_file(firma_path)
    return "Firma no encontrada", 404


@app.route("/firma-supervisor/<nombre_archivo>")
def firma_supervisor(nombre_archivo):
    archivo = os.path.basename(str(nombre_archivo or "").strip())
    if not archivo:
        return "Firma no encontrada", 404
    firma_path = os.path.join(FIRMAS_EMPLEADOS_DIR, archivo)
    if os.path.isfile(firma_path):
        return send_file(firma_path)
    return "Firma no encontrada", 404

# ======================
# DASHBOARD - INICIO
# ======================
@app.route("/")
def dashboard():
    role_actual = _session_user_role()
    container_max_width = "1200px"

    modulos = [
        {
            "href": "/modulo/ot",
            "css": "ot",
            "icon": "📋",
            "titulo": "Órdenes de Trabajo",
            "desc": "Crear y gestionar órdenes de trabajo, seguimiento de estado y entregas",
        },
        {
            "href": "/modulo/produccion",
            "css": "produccion",
            "icon": "🏭",
            "titulo": "Producción",
            "desc": "Control de procesos y seguimiento de producción en planta",
        },
        {
            "href": "/modulo/calidad",
            "css": "calidad",
            "icon": "🧪",
            "titulo": "Calidad",
            "desc": "Recepción de materiales, escaneo QR y control de despacho",
        },
        {
            "href": "/modulo/parte",
            "css": "parte",
            "icon": "⏱",
            "titulo": "Parte Semanal - Empleados",
            "desc": "Registro de empleados, horas de trabajo y actividades por operario",
        },
        {
            "href": "/modulo/remito",
            "css": "remito",
            "icon": "🚚",
            "titulo": "Remitos",
            "desc": "Generación de remitos y documentos de entrega",
        },
        {
            "href": "/modulo/estado",
            "css": "estado",
            "icon": "📊",
            "titulo": "Estado de Producción",
            "desc": "Tablero de control, indicadores y avance de órdenes",
        },
        {
            "href": "/home",
            "css": "piezas",
            "icon": "📈",
            "titulo": "Estado de Piezas por Proceso",
            "desc": "Seguimiento por pieza, filtros por obra y avance de procesos escaneados",
        },
        {
            "href": "/modulo/generador",
            "css": "generador",
            "icon": "🏷️",
            "titulo": "Generador de Etiquetas QR",
            "desc": "Genera etiquetas A3 con códigos QR desde archivos Excel",
        },
        {
            "href": "/modulo/gestion-calidad",
            "css": "gestioncalidad",
            "icon": "✅",
            "titulo": "Gestión de Calidad",
            "desc": "Dashboard de no conformes, observaciones y oportunidades de mejora por proceso",
        },
        {
            "href": "/modulo/historial",
            "css": "historial",
            "icon": "📚",
            "titulo": "Historial de OTs",
            "desc": "Órdenes de trabajo cerradas - Archivo de OTs finalizadas",
        },
    ]

    cards_html = "".join(
        f'''
            <a href="{m["href"]}" class="module-card {m["css"]}">
                <span class="module-icon">{m["icon"]}</span>
                <h3>{html_lib.escape(m["titulo"])}</h3>
                <p>{html_lib.escape(m["desc"])}</p>
            </a>
        '''
        for m in modulos
    )

    top_actions = '''
                <a href="/modulo/calidad/escaneo/qr" style="display: inline-block; padding: 5px 14px; font-size: 0.95em; background: #f4f4f4; color: #7c2d12; border: 1px solid #fdba74; border-radius: 6px; text-decoration: none; box-shadow: 0 1px 3px rgba(154,52,18,0.07); transition: background 0.2s; margin-left: 6px;">
                    <span style="font-size:1.1em; vertical-align:middle; margin-right:6px;">📱</span>Escanear Pieza
                </a>
                <a href="/logout" style="display: inline-block; padding: 5px 14px; font-size: 0.95em; background: #334155; color: #ffffff; border: 1px solid #334155; border-radius: 6px; text-decoration: none; box-shadow: 0 1px 3px rgba(51,65,85,0.24); transition: background 0.2s; margin-left: 6px;">
                    🚪 Cerrar sesión
                </a>
    '''
    if _is_admin_session():
        top_actions = (
            '''
                <a href="/admin/usuarios" style="display: inline-block; padding: 5px 14px; font-size: 0.95em; background: #0f766e; color: #ffffff; border: 1px solid #0f766e; border-radius: 6px; text-decoration: none; box-shadow: 0 1px 3px rgba(15,118,110,0.24); transition: background 0.2s; margin-left: 6px;">
                    👥 Gestión de Usuarios
                </a>
            '''
            + top_actions
        )

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
        background:
            radial-gradient(circle at 15% 20%, #ffd8a8 0%, rgba(255,216,168,0) 38%),
            radial-gradient(circle at 90% 15%, #ffb86b 0%, rgba(255,184,107,0) 34%),
            linear-gradient(140deg, #fff4e6 0%, #ffe4c7 42%, #ffd0a8 100%);
        min-height: 100vh;
        padding: 20px;
    }
    .container {
        max-width: __CONTAINER_MAX_WIDTH__;
        margin: 0 auto;
    }
    .header {
        margin-bottom: 28px;
        padding: 18px;
        border-radius: 18px;
        background: linear-gradient(110deg, rgba(255,255,255,0.9), rgba(255,247,237,0.88));
        border: 1px solid #fdba74;
        box-shadow: 0 12px 28px rgba(154, 52, 18, 0.14);
    }
    .header-inner {
        display: grid;
        grid-template-columns: 240px 1fr;
        gap: 18px;
        align-items: center;
    }
    .logo-card {
        background: #fff;
        border: 1px solid #fed7aa;
        border-radius: 14px;
        padding: 10px;
        display: flex;
        justify-content: center;
        align-items: center;
    }
    .logo-card img {
        width: 100%;
        max-width: 210px;
        height: auto;
        display: block;
    }
    .header-copy {
        color: #9a3412;
    }
    .header-chip {
        display: inline-block;
        background: #f97316;
        color: #fff;
        font-weight: bold;
        border-radius: 999px;
        padding: 6px 12px;
        margin-bottom: 10px;
        font-size: 0.85em;
    }
    .header h1 {
        font-size: 2.2em;
        margin-bottom: 8px;
        color: #7c2d12;
    }
    .header p {
        font-size: 1.05em;
        color: #9a3412;
    }
    .modules-grid {
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
        gap: 20px;
        margin-bottom: 20px;
    }
    .module-card {
        background: #ffffff;
        border-radius: 12px;
        padding: 25px;
        box-shadow: 0 8px 16px rgba(124,45,18,0.11);
        transition: transform 0.3s ease, box-shadow 0.3s ease;
        cursor: pointer;
        text-decoration: none;
        color: #333;
        border: 1px solid #ffedd5;
    }
    .module-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 15px 30px rgba(194,65,12,0.22);
    }
    .module-icon {
        font-size: 3em;
        margin-bottom: 15px;
        display: block;
    }
    .module-card h3 {
        font-size: 1.3em;
        margin-bottom: 8px;
        color: #9a3412;
    }
    .module-card p {
        font-size: 0.9em;
        color: #7c2d12;
        line-height: 1.4;
    }
    .module-card.ot {
        border-left: 5px solid #f97316;
    }
    .module-card.produccion {
        border-left: 5px solid #fb923c;
    }
    .module-card.calidad {
        border-left: 5px solid #ea580c;
    }
    .module-card.parte {
        border-left: 5px solid #fdba74;
    }
    .module-card.remito {
        border-left: 5px solid #f97316;
    }
    .module-card.estado {
        border-left: 5px solid #c2410c;
    }
    .module-card.piezas {
        border-left: 5px solid #fb923c;
    }
    .module-card.generador {
        border-left: 5px solid #f59e0b;
    }
    .module-card.gestioncalidad {
        border-left: 5px solid #16a34a;
    }
    .footer {
        text-align: center;
        color: #9a3412;
        padding: 20px;
        font-size: 0.9em;
    }
    @media (max-width: 820px) {
        .header-inner {
            grid-template-columns: 1fr;
            text-align: center;
        }
        .logo-card {
            max-width: 260px;
            margin: 0 auto;
        }
    }
    @media (max-width: 500px) {
        body { padding: 10px; }
        .modules-grid { grid-template-columns: 1fr; gap: 12px; }
        .header h1 { font-size: 1.5em; }
        .module-card { padding: 18px; }
        .module-icon { font-size: 2.2em; margin-bottom: 10px; }
        .module-card h3 { font-size: 1.1em; }
        div[style*="text-align: right"] { text-align: center !important; }
    }
    </style>
    </head>
    <body>
    <div class="container">
        <div class="header">
            <div class="header-inner">
                <div class="logo-card">
                    <img src="/logo-a3" alt="Logo A3 Servicios Constructivos">
                </div>
                <div class="header-copy">
                    <span class="header-chip">Panel Principal</span>
                    <h1>🏭 Sistema de Gestión de Producción</h1>
                    <p>Control integral de órdenes de trabajo, calidad y producción</p>
                </div>
            </div>
        </div>
        
        <div style="margin-bottom: 20px;">
            <div style="margin-top: 8px; margin-bottom: 8px; text-align: right;">
                __TOP_ACTIONS__
            </div>
        </div>
        
        <div class="modules-grid">
            __CARDS_HTML__
        </div>
        
        <div class="footer">
            <p>© 2026 Sistema de Gestión de Producción</p>
        </div>
    </div>
    
    </body>
    </html>
    """
    html = html.replace("__CONTAINER_MAX_WIDTH__", container_max_width)
    html = html.replace("__TOP_ACTIONS__", top_actions)
    html = html.replace("__CARDS_HTML__", cards_html)
    return html

# ======================
# HOME - VER TODAS LAS TUPLAS
# ======================
@app.route("/home")
@app.route("/home/<int:page>")
def home(page=1):
    db = get_db()
    es_obra = _is_obra_session()
    responsables_control = _obtener_responsables_control(db)
    responsable_por_firma = {
        str(data.get("firma", "")).strip().lower(): nombre
        for nombre, data in responsables_control.items()
        if str(data.get("firma", "")).strip()
    }
    
    # Obtener parámetros de búsqueda
    busqueda_ot_txt = request.args.get('ot_id', '').strip()
    busqueda_ot = int(busqueda_ot_txt) if busqueda_ot_txt.isdigit() else None
    busqueda_pieza = request.args.get('pieza', '').strip()
    mensaje = request.args.get('mensaje', '').strip()
    # OTs activas para filtro principal
    ots_activas = db.execute(
        """
        SELECT id, TRIM(COALESCE(obra, '')), TRIM(COALESCE(titulo, ''))
        FROM ordenes_trabajo
        WHERE fecha_cierre IS NULL
          AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
        ORDER BY id DESC
        """
    ).fetchall()

    if busqueda_ot is None and ots_activas:
        busqueda_ot = int(ots_activas[0][0])

    # Obtener piezas de la OT seleccionada (solo piezas vinculadas explícitamente a esa OT)
    if busqueda_ot is not None:
        all_rows = db.execute(
            """
            SELECT p.id,
                   TRIM(COALESCE(p.posicion, '')) AS posicion,
                   TRIM(COALESCE(p.obra, '')) AS obra,
                   COALESCE(p.ot_id, 0) AS ot_id
            FROM procesos p
            WHERE p.eliminado = 0
              AND TRIM(COALESCE(p.posicion, '')) <> ''
              AND COALESCE(p.ot_id, -1) = COALESCE(?, -1)
            ORDER BY posicion ASC
            """,
            (busqueda_ot,),
        ).fetchall()
    else:
        all_rows = []

    # Agrupar por posición + obra + OT para permitir códigos repetidos
    piezas = {}
    for r in all_rows:
        pos = str(r[1] or '').strip()
        if not pos:
            continue
        obra = str(r[2] or '').strip()
        ot_id_row = int(r[3] or 0)
        key = (pos, obra, ot_id_row)
        if key not in piezas:
            piezas[key] = r

    piezas_unicas = sorted(piezas.keys(), key=lambda x: (x[0], x[1]))

    # Filtrar por pieza/posición (segundo filtro)
    if busqueda_pieza:
        piezas_unicas = [k for k in piezas_unicas if busqueda_pieza.lower() in k[0].lower()]
    
    # Paginación de 10 piezas por página
    piezas_por_pagina = 10
    total_piezas = len(piezas_unicas)
    total_paginas = (total_piezas + piezas_por_pagina - 1) // piezas_por_pagina
    
    # Validar página
    if page < 1:
        page = 1
    if page > total_paginas and total_paginas > 0:
        page = total_paginas
    
    # Calcular índices para obtener las piezas de la página actual
    inicio = (page - 1) * piezas_por_pagina
    fin = inicio + piezas_por_pagina
    piezas_pagina = piezas_unicas[inicio:fin]

    # Generar opciones del dropdown con las OTs activas
    ots_options = '<option value="">-- Seleccionar OT --</option>'
    for ot_id_opt, obra_opt, titulo_opt in ots_activas:
        selected = 'selected' if busqueda_ot is not None and int(ot_id_opt) == int(busqueda_ot) else ''
        etiqueta = f"OT {int(ot_id_opt)} - {obra_opt}" + (f" - {titulo_opt}" if titulo_opt else "")
        ots_options += f'<option value="{int(ot_id_opt)}" {selected}>{html_lib.escape(etiqueta)}</option>'

    def _ot_no_requiere_pintura_panel(pos_sel, obra_sel, ot_id_sel=None):
        row_esq = db.execute(
            """
            SELECT COALESCE(ot.esquema_pintura, '')
            FROM procesos p
            LEFT JOIN ordenes_trabajo ot ON ot.id = p.ot_id
            WHERE TRIM(COALESCE(p.posicion, '')) = TRIM(?)
              AND TRIM(COALESCE(p.obra, '')) = TRIM(COALESCE(?, ''))
              AND COALESCE(p.ot_id, -1) = COALESCE(?, -1)
              AND p.eliminado = 0
            ORDER BY p.id DESC
            LIMIT 1
            """,
            (pos_sel, obra_sel or '', ot_id_sel),
        ).fetchone()

        esquema = str((row_esq[0] if row_esq else '') or '').strip().upper()
        if not esquema and obra_sel:
            row_ot = db.execute(
                """
                SELECT COALESCE(esquema_pintura, '')
                FROM ordenes_trabajo
                WHERE TRIM(COALESCE(obra, '')) = TRIM(COALESCE(?, ''))
                  AND fecha_cierre IS NULL
                  AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
                ORDER BY id DESC
                LIMIT 1
                """,
                (obra_sel,),
            ).fetchone()
            esquema = str((row_ot[0] if row_ot else '') or '').strip().upper()

        return esquema in {"N/A", "NA", "NO APLICA", "SIN PINTURA", "NO REQUIERE PINTURA"}

    def obtener_resumen_panel_pieza(pos_sel, obra_sel, ot_id_sel=None):
        def _etapa_pintura(reproceso_txt, proceso_u):
            ru = (reproceso_txt or '').upper()
            if 'ETAPA:SUPERFICIE' in ru:
                return 'SUPERFICIE'
            if 'ETAPA:FONDO' in ru:
                return 'FONDO'
            if 'ETAPA:TERMINACION' in ru:
                return 'TERMINACION'
            if (proceso_u or '').upper() == 'PINTURA_FONDO':
                return 'FONDO'
            return None

        def _resolver_estado_pintura(estado_u, fecha_txt, firma_txt, re_inspeccion_txt):
            estado_base = (estado_u or '').strip().upper()
            fecha_final = (fecha_txt or '').strip() or '-'
            firma_final = (firma_txt or '').strip()
            ciclos = _extraer_ciclos_reinspeccion(re_inspeccion_txt or '')

            if estado_base in ('NC', 'NO CONFORME', 'NO CONFORMIDAD'):
                if ciclos:
                    ultimo = ciclos[-1] or {}
                    estado_ultimo = str(ultimo.get('estado') or '').strip().upper()
                    fecha_final = str(ultimo.get('fecha') or '').strip() or fecha_final
                    firma_final = str(ultimo.get('firma') or '').strip() or firma_final
                    if _estado_control_aprueba(estado_ultimo):
                        return {'estado': 'OK', 'fecha': fecha_final, 'firma': firma_final, 'ciclos': len(ciclos)}
                    if estado_ultimo in ('NC', 'NO CONFORME', 'NO CONFORMIDAD'):
                        return {'estado': 'NO CONFORME', 'fecha': fecha_final, 'firma': firma_final, 'ciclos': len(ciclos)}
                    return {'estado': 'RE-INSPECCIÓN', 'fecha': fecha_final, 'firma': firma_final, 'ciclos': len(ciclos)}
                return {'estado': 'NO CONFORME', 'fecha': fecha_final, 'firma': firma_final, 'ciclos': 0}

            if _estado_control_aprueba(estado_base):
                return {'estado': 'OK', 'fecha': fecha_final, 'firma': firma_final, 'ciclos': len(ciclos)}

            return {'estado': estado_base or '-', 'fecha': fecha_final, 'firma': firma_final, 'ciclos': len(ciclos)}

        rows = db.execute(
            """
            SELECT UPPER(TRIM(proceso)), UPPER(TRIM(COALESCE(estado, ''))), COALESCE(re_inspeccion, ''), COALESCE(firma_digital, ''), COALESCE(fecha, ''), COALESCE(estado_pieza, '')
            FROM procesos
            WHERE posicion=?
              AND COALESCE(obra, '') = COALESCE(?, '')
                            AND COALESCE(ot_id, -1) = COALESCE(?, -1)
              AND eliminado=0
              AND UPPER(TRIM(COALESCE(proceso, ''))) IN ('ARMADO','SOLDADURA','DESPACHO')
                    AND (
                        (ot_id IS NOT NULL AND EXISTS (
                            SELECT 1 FROM ordenes_trabajo ot
                            WHERE ot.id = procesos.ot_id AND ot.fecha_cierre IS NULL AND (ot.es_mantenimiento IS NULL OR ot.es_mantenimiento = 0)
                        ))
                        OR
                        (ot_id IS NULL AND EXISTS (
                            SELECT 1 FROM ordenes_trabajo ot
                            WHERE TRIM(COALESCE(ot.obra, '')) = TRIM(COALESCE(procesos.obra, ''))
                                AND ot.fecha_cierre IS NULL AND (ot.es_mantenimiento IS NULL OR ot.es_mantenimiento = 0)
                        ))
                    )
            ORDER BY id DESC
            """,
            (pos_sel, obra_sel or '', ot_id_sel)
        ).fetchall()

        latest = {}
        firmas_faltantes = 0
        nc_total = 0
        nc_pendientes = 0
        nc_cerradas = 0
        ciclos_total = 0

        for proceso, estado, reinspeccion, firma, fecha_reg, estado_pieza in rows:
            if not firma.strip():
                firmas_faltantes += 1
            if proceso not in latest:
                latest[proceso] = {
                    'estado': estado,
                    'reinspeccion': reinspeccion,
                    'firma': firma,
                    'fecha': fecha_reg,
                    'estado_pieza': (estado_pieza or '').strip().upper(),
                }

            if estado in ('NC', 'NO CONFORME', 'NO CONFORMIDAD'):
                nc_total += 1
                ciclos = _extraer_ciclos_reinspeccion(reinspeccion)
                ciclos_total += len(ciclos)
                if ciclos and _estado_control_aprueba(ciclos[-1].get('estado')):
                    nc_cerradas += 1
                else:
                    nc_pendientes += 1

        paint_rows = db.execute(
            """
            SELECT UPPER(TRIM(COALESCE(proceso, ''))),
                   UPPER(TRIM(COALESCE(estado, ''))),
                   COALESCE(re_inspeccion, ''),
                   COALESCE(firma_digital, ''),
                   COALESCE(fecha, ''),
                   COALESCE(reproceso, '')
            FROM procesos
            WHERE posicion=?
              AND COALESCE(obra, '') = COALESCE(?, '')
                            AND COALESCE(ot_id, -1) = COALESCE(?, -1)
              AND eliminado=0
              AND UPPER(TRIM(COALESCE(proceso, ''))) IN ('PINTURA','PINTURA_FONDO')
              AND (
                    (ot_id IS NOT NULL AND EXISTS (
                        SELECT 1 FROM ordenes_trabajo ot
                        WHERE ot.id = procesos.ot_id AND ot.fecha_cierre IS NULL AND (ot.es_mantenimiento IS NULL OR ot.es_mantenimiento = 0)
                    ))
                    OR
                    (ot_id IS NULL AND EXISTS (
                        SELECT 1 FROM ordenes_trabajo ot
                        WHERE TRIM(COALESCE(ot.obra, '')) = TRIM(COALESCE(procesos.obra, ''))
                            AND ot.fecha_cierre IS NULL AND (ot.es_mantenimiento IS NULL OR ot.es_mantenimiento = 0)
                    ))
              )
                        ORDER BY id DESC
            """,
            (pos_sel, obra_sel or '', ot_id_sel)
        ).fetchall()

        etapas = {'SUPERFICIE': None, 'FONDO': None, 'TERMINACION': None}
        for proc_u, est_u, reins_u, firma_u, fecha_u, repro_u in paint_rows:
            etapa = _etapa_pintura(repro_u, proc_u)
            if not etapa or etapas.get(etapa) is not None:
                continue
            res = _resolver_estado_pintura(est_u, fecha_u, firma_u, reins_u)
            etapas[etapa] = res
            ciclos_total += int(res.get('ciclos') or 0)

        estados_pint = [
            (etapas['SUPERFICIE'] or {}).get('estado') or '-',
            (etapas['FONDO'] or {}).get('estado') or '-',
            (etapas['TERMINACION'] or {}).get('estado') or '-',
        ]
        estados_pint_ctl = [e for e in estados_pint if e != '-']
        estado_pintura = '-'
        if any(e in ('NO CONFORME', 'RE-INSPECCIÓN') for e in estados_pint_ctl):
            estado_pintura = 'NO CONFORME'
        elif estados_pint_ctl:
            estado_pintura = 'OK'

        if estado_pintura != '-':
            fecha_candidata = '-'
            firma_candidata = ''
            for etapa_k in ('TERMINACION', 'FONDO', 'SUPERFICIE'):
                d = etapas.get(etapa_k)
                if d and d.get('fecha') and d.get('fecha') != '-':
                    fecha_candidata = d.get('fecha')
                    firma_candidata = d.get('firma') or ''
                    break
            latest['PINTURA'] = {
                'estado': estado_pintura,
                'reinspeccion': '',
                'firma': firma_candidata,
                'fecha': fecha_candidata,
                'estado_pieza': 'APROBADA' if estado_pintura == 'OK' else 'NO_APROBADA',
                'ciclos_count': sum(int((etapas.get(k) or {}).get('ciclos') or 0) for k in ('SUPERFICIE', 'FONDO', 'TERMINACION')),
            }

        estado_general = 'SIN_CONTROL'
        if any(v.get('estado_pieza') == 'NO_APROBADA' for v in latest.values()):
            estado_general = 'NO_APROBADA'
        elif latest:
            estado_general = 'APROBADA'

        return latest, {
            'estado_general': estado_general,
            'firmas_faltantes': firmas_faltantes,
            'nc_total': nc_total,
            'nc_pendientes': nc_pendientes,
            'nc_cerradas': nc_cerradas,
            'ciclos_total': ciclos_total,
        }

    panel_cache = {}
    piezas_aprobadas = 0
    piezas_no_aprobadas = 0
    piezas_con_reinspeccion = 0
    for pos_key, obra_key, ot_key in piezas_unicas:
        latest_proc, stats_proc = obtener_resumen_panel_pieza(pos_key, obra_key, ot_key)
        panel_cache[(pos_key, obra_key, ot_key)] = (latest_proc, stats_proc)
        if stats_proc['estado_general'] == 'APROBADA':
            piezas_aprobadas += 1
        elif stats_proc['estado_general'] == 'NO_APROBADA':
            piezas_no_aprobadas += 1
        if stats_proc['ciclos_total'] > 0:
            piezas_con_reinspeccion += 1

    mensaje_html = f'<div class="mensaje-ok">{html_lib.escape(mensaje)}</div>' if mensaje else ''

    html = f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {{
        font-family: "Segoe UI", Tahoma, Arial, sans-serif;
        padding: 16px;
        background: radial-gradient(circle at 15% 0%, #f8fbff 0%, #eef3f7 55%, #e8edf3 100%);
    }}
    h2 {{
        color: #111827;
        border-bottom: 3px solid #0ea5a3;
        padding-bottom: 10px;
        margin: 0;
    }}
    .top-bar {{
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 12px;
        margin-bottom: 10px;
    }}
    .btn-volver {{
        display: inline-block;
        background: #2563eb;
        color: white;
        padding: 10px 14px;
        border-radius: 8px;
        text-decoration: none;
        font-weight: bold;
        white-space: nowrap;
    }}
    .btn-volver:hover {{
        background: #1d4ed8;
    }}
    .buscador-box {{
        background: #ffffff;
        padding: 15px;
        border-radius: 10px;
        margin-bottom: 15px;
        border: 1px solid #dbe4ee;
        box-shadow: 0 6px 14px rgba(15,23,42,0.06);
    }}
    .buscador-box form {{
        display: grid;
        gap: 10px;
    }}
    .filtro-grupo {{
        display: grid;
        grid-template-columns: 1fr 1fr;
        gap: 10px;
    }}
    .filtro-grupo select,
    .filtro-grupo input {{
        padding: 10px;
        border: 1px solid #ddd;
        border-radius: 4px;
        font-size: 14px;
        box-sizing: border-box;
    }}
    .buscador-box button {{
        background: #0f766e;
        color: white;
        border: none;
        padding: 10px 20px;
        border-radius: 8px;
        font-weight: bold;
        cursor: pointer;
        margin-top: 10px;
        width: 100%;
    }}
    .buscador-box button:hover {{
        background: #0d6660;
    }}
    .btn-eliminar-obra {{
        background: #d32f2f !important;
        margin-top: 8px;
        width: auto !important;
        justify-self: start;
        padding: 8px 14px !important;
        font-size: 13px;
    }}
    .btn-eliminar-obra:hover {{
        background: #b71c1c !important;
    }}
    .info-busqueda {{
        font-size: 12px;
        color: #666;
        margin-top: 8px;
    }}
    .mensaje-ok {{
        background: #e8f5e9;
        border: 1px solid #c8e6c9;
        color: #2e7d32;
        padding: 10px;
        border-radius: 4px;
        margin-bottom: 12px;
    }}
    .summary-grid {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
        gap: 10px;
        margin-bottom: 14px;
    }}
    .summary-card {{
        background: white;
        border: 1px solid #dbe4ee;
        border-radius: 10px;
        padding: 12px;
        box-shadow: 0 6px 14px rgba(15,23,42,0.05);
    }}
    .summary-card .t {{
        font-size: 12px;
        color: #475569;
        margin-bottom: 4px;
    }}
    .summary-card .v {{
        font-size: 24px;
        font-weight: 800;
        color: #0f172a;
    }}
    table {{
        width: 100%;
        border-collapse: collapse;
        background: white;
        border: 1px solid #dbe4ee;
        border-radius: 10px;
        overflow: hidden;
        box-shadow: 0 6px 14px rgba(15,23,42,0.06);
        margin-bottom: 20px;
    }}
    th {{
        background: #0f766e;
        color: white;
        padding: 14px 10px;
        text-align: center;
        font-weight: bold;
        font-size: 13px;
        letter-spacing: 0.2px;
    }}
    td {{
        padding: 14px 10px;
        border-bottom: 1px solid #e0e0e0;
        text-align: center;
        vertical-align: middle;
    }}
    td:first-child {{
        text-align: left;
        font-weight: bold;
        font-size: 16px;
    }}
    td.obra-col {{
        text-align: left;
        font-weight: bold;
        color: #333;
    }}
    tr:nth-child(even) {{ background: #f8fafc; }}
    tr:nth-child(odd) {{ background: #ffffff; }}
    tr:hover {{ background: #eef6ff; }}
    .completado {{
        color: #15803d;
        font-weight: bold;
    }}
    .incompleto {{
        color: #94a3b8;
    }}
    .btn-ver {{
        display: inline-block;
        background: #1d4ed8;
        color: white;
        padding: 6px 10px;
        border-radius: 6px;
        text-decoration: none;
        font-weight: bold;
        font-size: 11px;
    }}
    .btn-ver:hover {{
        background: #1e40af;
    }}
    .acciones-col {{
        white-space: nowrap;
    }}
    .btn-eliminar-pieza {{
        display: inline-block;
        background: #dc2626;
        color: white;
        padding: 6px 10px;
        border-radius: 6px;
        text-decoration: none;
        font-weight: bold;
        font-size: 11px;
        border: none;
        cursor: pointer;
        margin-left: 6px;
    }}
    .btn-eliminar-pieza:hover {{
        background: #b91c1c;
    }}
    .sin-registros {{
        background: white;
        padding: 20px;
        border-radius: 8px;
        text-align: center;
        color: #666;
    }}
    .chip {{
        display: inline-block;
        padding: 4px 10px;
        border-radius: 999px;
        font-size: 11px;
        font-weight: 700;
        border: 1px solid transparent;
    }}
    .chip-ok {{ background: #dcfce7; color: #166534; border-color: #86efac; }}
    .chip-warn {{ background: #ffedd5; color: #9a3412; border-color: #fdba74; }}
    .chip-nc {{ background: #fee2e2; color: #b91c1c; border-color: #fecaca; }}
    .chip-neutral {{ background: #e2e8f0; color: #475569; border-color: #cbd5e1; }}
    .audit-text {{
        margin-top: 6px;
        font-size: 10px;
        color: #475569;
        line-height: 1.35;
    }}
    .stage-box {{
        display: grid;
        gap: 5px;
        justify-items: center;
        min-width: 104px;
        position: relative;
        cursor: default;
    }}
    .stage-main {{
        font-size: 12px;
        font-weight: 700;
        line-height: 1.2;
        color: #334155;
    }}
    .stage-sub {{
        font-size: 10px;
        color: #64748b;
        line-height: 1.25;
    }}
    .table-note {{
        font-size: 11px;
        color: #64748b;
        margin: -4px 0 10px 0;
    }}
    .stage-tooltip {{
        position: absolute;
        left: 50%;
        top: calc(100% + 8px);
        transform: translateX(-50%);
        width: 220px;
        background: #0f172a;
        color: #f8fafc;
        border-radius: 10px;
        padding: 10px 12px;
        font-size: 11px;
        line-height: 1.4;
        text-align: left;
        box-shadow: 0 10px 24px rgba(15,23,42,0.24);
        opacity: 0;
        visibility: hidden;
        pointer-events: none;
        transition: opacity 0.16s ease, visibility 0.16s ease;
        z-index: 20;
    }}
    .stage-tooltip::before {{
        content: "";
        position: absolute;
        left: 50%;
        top: -6px;
        transform: translateX(-50%) rotate(45deg);
        width: 12px;
        height: 12px;
        background: #0f172a;
    }}
    .stage-box:hover .stage-tooltip {{
        opacity: 1;
        visibility: visible;
    }}
    .stage-tooltip b {{
        color: #bfdbfe;
    }}
    .paginacion {{
        text-align: center;
        margin-top: 20px;
        display: flex;
        justify-content: center;
        gap: 5px;
        flex-wrap: wrap;
        align-items: center;
    }}
    .paginacion a, .paginacion span {{
        padding: 8px 12px;
        border: 1px solid #ddd;
        border-radius: 4px;
        text-decoration: none;
        color: #333;
        display: inline-block;
    }}
    .paginacion a:hover {{
        background: #ddd;
    }}
    .paginacion .activa {{
        background: orange;
        color: white;
        border-color: orange;
    }}
    .paginacion .deshabilitada {{
        color: #ccc;
        cursor: not-allowed;
    }}
    .info-paginacion {{
        text-align: center;
        color: #666;
        margin-bottom: 15px;
        font-size: 12px;
    }}
    </style>
    </head>

    <body>
    <div class="top-bar">
        <h2>📊 Panel de Control - Estado de piezas por proceso</h2>
        <a href="/" class="btn-volver">⬅️ Volver</a>
    </div>
    
    <div class="buscador-box">
        {mensaje_html}
        <form method="get" action="/home" id="form-home">
            <div class="filtro-grupo">
                <select name="ot_id" id="ot-select-home" onchange="document.getElementById('form-home').submit();">
                    {ots_options}
                </select>
            </div>
            <div class="filtro-grupo" style="margin-top:8px;">
                <input type="text" name="pieza" placeholder="🔍 Buscar por Posición..." value="{busqueda_pieza}">
            </div>
            <button type="submit">🔎 Buscar</button>
            {'' if es_obra else '''
            <button
                type="submit"
                class="btn-eliminar-obra"
                formaction="/home/eliminar-ot"
                formmethod="post"
                onclick="return confirm('¿Seguro que querés eliminar TODAS las piezas de la OT seleccionada? Esta acción no se puede deshacer.')"
            >🗑️ Eliminar piezas de la OT seleccionada</button>
            '''}
            <div class="info-busqueda">
                <a href="/home" style="color: blue; text-decoration: none;">Limpiar todos los filtros</a>
            </div>
        </form>
    </div>

    <div class="summary-grid">
        <div class="summary-card"><div class="t">Piezas filtradas</div><div class="v">{total_piezas}</div></div>
        <div class="summary-card"><div class="t">Piezas aprobadas</div><div class="v">{piezas_aprobadas}</div></div>
        <div class="summary-card"><div class="t">Piezas no aprobadas</div><div class="v">{piezas_no_aprobadas}</div></div>
        <div class="summary-card"><div class="t">Con re-inspección</div><div class="v">{piezas_con_reinspeccion}</div></div>
    </div>
    """

    if total_piezas == 0:
        html += "<div class='sin-registros'>⚠️ No hay registros encontrados</div>"
    else:
        html += f"<div class='info-paginacion'>Mostrando {inicio + 1}-{min(fin, total_piezas)} de {total_piezas} piezas</div>"
        html += "<div class='table-note'>La tabla muestra un resumen por etapa. Pasá el cursor sobre cada estado para ver el detalle.</div>"
        html += """
        <table>
            <tr>
                <th>Posición</th>
                <th>Obra</th>
                <th>Armado</th>
                <th>Soldadura</th>
                <th>Pintura</th>
                <th>Despacho</th>
                <th>Re-inspección ISO</th>
                <th>Acciones</th>
            </tr>
        """
        for pos, obra_key, ot_key in piezas_pagina:
            latest_proc, stats_proc = panel_cache.get((pos, obra_key, ot_key), ({}, {}))
            pintura_no_aplica = _ot_no_requiere_pintura_panel(pos, obra_key, ot_key)
            
            # Obtener la obra (índice 8 — obra fue agregada con ALTER TABLE al final)
            obra_raw = str(obra_key or '').strip()
            
            # Si la obra contiene nombres de procesos o está vacía, intenta obtenerla desde OT
            procesos_invalidos = ("ARMADO", "SOLDADURA", "PINTURA", "DESPACHO")
            if obra_raw in procesos_invalidos or obra_raw == '' or obra_raw == 'None':
                # Intentar obtener la obra desde ordenes_trabajo
                ot_obra = db.execute("""
                    SELECT obra FROM ordenes_trabajo 
                    WHERE estado != 'Finalizada' AND fecha_cierre IS NULL
                    ORDER BY fecha_creacion DESC 
                    LIMIT 1
                """).fetchone()
                
                if ot_obra and ot_obra[0]:
                    obra = ot_obra[0]
                else:
                    obra = '---'
            else:
                obra = obra_raw

            obra_link = obra_key if obra_key else (obra if obra != '---' else '')

            if stats_proc.get('nc_total', 0) == 0:
                resumen_iso = '<span class="chip chip-ok">SIN NC</span><div class="audit-text">Trazabilidad al día</div>'
            elif stats_proc.get('nc_pendientes', 0) > 0:
                resumen_iso = f'<span class="chip chip-nc">NC PENDIENTE</span><div class="audit-text">Pendientes: {stats_proc.get("nc_pendientes", 0)}</div>'
            else:
                resumen_iso = f'<span class="chip chip-warn">NC CERRADA</span><div class="audit-text">Ciclos: {stats_proc.get("ciclos_total", 0)}</div>'
            
            # Crear celdas para cada proceso
            celdas = []
            for proceso in ORDEN_PROCESOS:
                dato = latest_proc.get(proceso)
                if proceso == 'PINTURA' and pintura_no_aplica and not dato:
                    tooltip_html = "<b>Estado:</b> No aplica<br><b>Detalle:</b> OT configurada sin requerimiento de pintura"
                    celdas.append(f'<td><div class="stage-box"><span class="chip chip-ok">N/A</span><div class="stage-sub">No requiere</div><div class="stage-tooltip">{tooltip_html}</div></div></td>')
                    continue
                if not dato:
                    tooltip_html = "<b>Estado:</b> Sin control<br><b>Detalle:</b> No hay registros para esta etapa"
                    celdas.append(f'<td><div class="stage-box"><span class="chip chip-neutral">SIN CONTROL</span><div class="stage-tooltip">{tooltip_html}</div></div></td>')
                else:
                    estado_proc = (dato.get('estado') or '').strip().upper()
                    reinspeccion_proc = dato.get('reinspeccion') or ''
                    firma_proc = (dato.get('firma') or '').strip()
                    responsable_proc = responsable_por_firma.get(str(firma_proc).strip().lower(), '-') if firma_proc else '-'
                    fecha_proc = (dato.get('fecha') or '').strip() or '-'
                    ciclos_proc = _extraer_ciclos_reinspeccion(reinspeccion_proc)
                    ciclos_count = len(ciclos_proc)
                    if proceso == 'PINTURA':
                        ciclos_count = int(dato.get('ciclos_count') or 0)
                    estado_pieza_proc = (dato.get('estado_pieza') or '').strip().upper() or '-'

                    # Obtener última fecha aprobada (considerando ciclos de re-inspección)
                    fecha_aprobada = '-'
                    if _estado_control_aprueba(estado_proc):
                        fecha_aprobada = fecha_proc
                    elif ciclos_proc:
                        for ciclo in reversed(ciclos_proc):
                            if isinstance(ciclo, dict) and _estado_control_aprueba(ciclo.get('estado')):
                                fecha_aprobada = (ciclo.get('fecha') or '-').strip() or '-'
                                break
                        if fecha_aprobada == '-':
                            fecha_aprobada = fecha_proc

                    if proceso == 'PINTURA' and estado_proc == 'OK':
                        badge = '<span class="chip chip-ok">OK</span>'
                        hallazgo = 'OK'
                        detalle = fecha_aprobada
                    elif proceso == 'PINTURA' and estado_proc in ('NO CONFORME', 'RE-INSPECCIÓN'):
                        badge = '<span class="chip chip-nc">NO CONFORME</span>'
                        hallazgo = 'No conforme'
                        detalle = fecha_proc
                    elif _estado_control_aprueba(estado_proc):
                        badge = '<span class="chip chip-ok">OK</span>'
                        hallazgo = 'OK' if estado_proc in ('OK', 'APROBADO') else f'Hallazgo {estado_proc}'
                        detalle = fecha_aprobada
                    elif estado_proc in ('NC', 'NO CONFORME', 'NO CONFORMIDAD'):
                        if ciclos_proc and isinstance(ciclos_proc[-1], dict) and _estado_control_aprueba(ciclos_proc[-1].get('estado')):
                            badge = '<span class="chip chip-ok">OK</span>'
                            hallazgo = 'NC cerrada'
                            detalle = fecha_aprobada
                        elif ciclos_proc:
                            badge = '<span class="chip chip-warn">RE-INSPECCION</span>'
                            hallazgo = 'En curso'
                            detalle = fecha_aprobada
                        else:
                            badge = '<span class="chip chip-nc">NO CONFORME</span>'
                            hallazgo = 'NC abierta'
                            detalle = fecha_proc
                    else:
                        badge = '<span class="chip chip-neutral">PENDIENTE</span>'
                        hallazgo = 'Sin cierre'
                        detalle = fecha_proc

                    firma_txt = 'OK' if firma_proc else 'Falta firma'
                    ultimo_ciclo_txt = '-'
                    if ciclos_proc and isinstance(ciclos_proc[-1], dict):
                        ultimo = ciclos_proc[-1]
                        ultimo_ciclo_txt = f"{(ultimo.get('estado') or '-').upper()} | {(ultimo.get('fecha') or '-')}"
                    tooltip_partes = [
                        f"<b>Estado control:</b> {html_lib.escape(estado_proc or '-')}",
                        f"<b>Estado pieza:</b> {html_lib.escape(estado_pieza_proc)}",
                        f"<b>Fecha:</b> {html_lib.escape(fecha_proc)}",
                        f"<b>Responsable:</b> {html_lib.escape(responsable_proc)}",
                        f"<b>Re-inspecciones:</b> {ciclos_count}",
                        f"<b>Último ciclo:</b> {html_lib.escape(ultimo_ciclo_txt)}",
                    ]
                    tooltip_html = "<br>".join(tooltip_partes)
                    celdas.append(f'<td><div class="stage-box">{badge}<div class="stage-sub">{detalle}</div><div class="stage-tooltip">{tooltip_html}</div></div></td>')
            
            accion_eliminar_html = ""
            if not es_obra:
                accion_eliminar_html = f'''
                    <form method="post" action="/home/eliminar-pieza" style="display:inline;">
                        <input type="hidden" name="posicion" value="{pos}">
                        <input type="hidden" name="obra" value="{obra_link}">
                        <input type="hidden" name="ot_id" value="{ot_key}">
                        <button
                            type="submit"
                            class="btn-eliminar-pieza"
                            onclick="return confirm('¿Seguro que querés eliminar esta pieza? Esta acción no se puede deshacer.')"
                        >Eliminar</button>
                    </form>
                '''

            html += f"""
            <tr>
                <td><b>{pos}</b></td>
                <td class="obra-col">{obra}</td>
                {celdas[0]}
                {celdas[1]}
                {celdas[2]}
                {celdas[3]}
                <td>{resumen_iso}</td>
                <td class="acciones-col">
                    <a class="btn-ver" href="/pieza/{quote(pos)}?obra={quote(obra_link)}&ot_id={ot_key}">Ver Pieza</a>
                    {accion_eliminar_html}
                </td>
            </tr>
            """
        html += "</table>"
        
        # Generar paginación
        html += "<div class='paginacion'>"
        
        # Botón anterior
        params = []
        if busqueda_ot is not None:
            params.append(f"ot_id={busqueda_ot}")
        if busqueda_pieza:
            params.append(f"pieza={busqueda_pieza}")
        query_str = "&".join(params)
        query_param = f"?{query_str}" if query_str else ""
        
        if page > 1:
            url_anterior = f'/home/{page - 1}{query_param}'
            html += f'<a href="{url_anterior}">← Anterior</a>'
        else:
            html += '<span class="deshabilitada">← Anterior</span>'
        
        # Números de página
        inicio_rango = max(1, page - 2)
        fin_rango = min(total_paginas, page + 2)
        
        if inicio_rango > 1:
            url_primera = f'/home/1{query_param}'
            html += f'<a href="{url_primera}">1</a>'
            if inicio_rango > 2:
                html += '<span>...</span>'
        
        for p in range(inicio_rango, fin_rango + 1):
            if p == page:
                html += f'<span class="activa">{p}</span>'
            else:
                url_pagina = f'/home/{p}{query_param}'
                html += f'<a href="{url_pagina}">{p}</a>'
        
        if fin_rango < total_paginas:
            if fin_rango < total_paginas - 1:
                html += '<span>...</span>'
            url_ultima = f'/home/{total_paginas}{query_param}'
            html += f'<a href="{url_ultima}">{total_paginas}</a>'
        
        # Botón siguiente
        if page < total_paginas:
            url_siguiente = f'/home/{page + 1}{query_param}'
            html += f'<a href="{url_siguiente}">Siguiente →</a>'
        else:
            html += '<span class="deshabilitada">Siguiente →</span>'
        
        html += "</div>"

    html += """
    </body>
    </html>
    """

    return html

@app.route("/home/eliminar-ot", methods=["POST"])
def eliminar_piezas_por_ot():
    if _is_obra_session():
        return _respuesta_sin_permiso()

    ot_id_txt = request.form.get("ot_id", "").strip()
    if not ot_id_txt.isdigit():
        return redirect("/home?mensaje=" + quote("⚠️ Seleccioná una OT antes de eliminar"))

    ot_id = int(ot_id_txt)
    db = get_db()
    cursor = db.execute("UPDATE procesos SET eliminado=1 WHERE ot_id = ?", (ot_id,))
    eliminadas = cursor.rowcount if cursor.rowcount is not None else 0
    db.commit()

    mensaje = f"✅ Se eliminaron {eliminadas} registro(s) de la OT {ot_id}"
    return redirect(f"/home?ot_id={ot_id}&mensaje=" + quote(mensaje))

@app.route("/home/eliminar-pieza", methods=["POST"])
def eliminar_pieza_individual():
    if _is_obra_session():
        return _respuesta_sin_permiso()

    posicion = request.form.get("posicion", "").strip()
    obra = request.form.get("obra", "").strip()
    ot_id_txt = request.form.get("ot_id", "").strip()
    ot_id = int(ot_id_txt) if ot_id_txt.isdigit() else None

    if not posicion:
        return redirect("/home?mensaje=" + quote("⚠️ Falta la posición de la pieza a eliminar"))

    db = get_db()
    if obra and ot_id is not None:
        cursor = db.execute(
            "UPDATE procesos SET eliminado=1 WHERE posicion = ? AND obra = ? AND ot_id = ?",
            (posicion, obra, ot_id)
        )
    elif obra:
        cursor = db.execute(
            "UPDATE procesos SET eliminado=1 WHERE posicion = ? AND obra = ?",
            (posicion, obra)
        )
    else:
        cursor = db.execute(
            "UPDATE procesos SET eliminado=1 WHERE posicion = ? AND (obra IS NULL OR TRIM(obra) = '')",
            (posicion,)
        )

    eliminadas = cursor.rowcount if cursor.rowcount is not None else 0
    db.commit()

    if obra and ot_id is not None:
        mensaje = f"✅ Pieza eliminada: {posicion} ({obra}) OT {ot_id} - {eliminadas} registro(s)"
    elif obra:
        mensaje = f"✅ Pieza eliminada: {posicion} ({obra}) - {eliminadas} registro(s)"
    else:
        mensaje = f"✅ Pieza eliminada: {posicion} - {eliminadas} registro(s)"
    destino = f"/home?mensaje={quote(mensaje)}"
    if ot_id is not None:
        destino = f"/home?ot_id={ot_id}&mensaje={quote(mensaje)}"
    return redirect(destino)

# ======================
# VER PIEZA (MEJORADO)
# ======================
@app.route("/pieza/<pos>")
def pieza(pos):
    db = get_db()
    es_obra = _is_obra_session()

    def _clean_qr(v):
        s = (v or '').strip()
        return '' if s.lower() in ('nan', 'none', 'nat', 'null') else s

    qr_obra     = _clean_qr(request.args.get('obra', ''))
    qr_ot_id_txt = _clean_qr(request.args.get('ot_id', ''))
    qr_ot_id = int(qr_ot_id_txt) if qr_ot_id_txt.isdigit() else None
    qr_cantidad = _clean_qr(request.args.get('cant', ''))
    qr_perfil   = _clean_qr(request.args.get('perfil', ''))
    qr_peso     = _clean_qr(request.args.get('peso', ''))
    qr_desc     = _clean_qr(request.args.get('desc', ''))

    # Si viene obra por QR/query, todo se resuelve por POS+OBRA.
    if qr_obra:
        datos_iniciales = db.execute("""
            SELECT * FROM procesos 
            WHERE posicion=? AND obra=?
            LIMIT 1
        """, (pos, qr_obra)).fetchone()

        todas_filas = db.execute("""
            SELECT * FROM procesos 
            WHERE posicion=? AND obra=?
            ORDER BY id
        """, (pos, qr_obra)).fetchall()
    else:
        # Compatibilidad con URLs antiguas sin obra
        datos_iniciales = db.execute("""
            SELECT * FROM procesos 
            WHERE posicion=? AND obra IS NOT NULL 
            LIMIT 1
        """, (pos,)).fetchone()

        todas_filas = db.execute("""
            SELECT * FROM procesos 
            WHERE posicion=? 
            ORDER BY id
        """, (pos,)).fetchall()

    # Si vienen datos desde el QR y faltan en BD, los guardamos para próximos accesos.
    try:
        if qr_obra or qr_cantidad or qr_perfil or qr_peso or qr_desc:
            cantidad_num = None
            peso_num = None
            if qr_cantidad:
                try:
                    cantidad_num = float(str(qr_cantidad).replace(',', '.'))
                except Exception:
                    cantidad_num = None
            if qr_peso:
                try:
                    peso_num = float(str(qr_peso).replace(',', '.'))
                except Exception:
                    peso_num = None

            if qr_obra:
                existe = db.execute(
                    "SELECT id FROM procesos WHERE posicion=? AND obra=? LIMIT 1",
                    (pos, qr_obra)
                ).fetchone()
            else:
                existe = db.execute("SELECT id FROM procesos WHERE posicion=? LIMIT 1", (pos,)).fetchone()
            if existe:
                if qr_obra:
                    db.execute("""
                        UPDATE procesos
                        SET obra = COALESCE(NULLIF(obra, ''), ?),
                            cantidad = COALESCE(cantidad, ?),
                            perfil = COALESCE(NULLIF(perfil, ''), ?),
                            peso = COALESCE(peso, ?),
                            descripcion = COALESCE(NULLIF(descripcion, ''), ?),
                            escaneado_qr = 1
                        WHERE posicion=? AND obra=?
                    """, (qr_obra or None, cantidad_num, qr_perfil or None, peso_num, qr_desc or None, pos, qr_obra))
                    ot_id_aplicar = qr_ot_id or _resolver_ot_id_para_obra(db, qr_obra)
                    if ot_id_aplicar:
                        db.execute(
                            "UPDATE procesos SET ot_id = ? WHERE posicion=? AND obra=? AND ot_id IS NULL",
                            (ot_id_aplicar, pos, qr_obra)
                        )
                else:
                    db.execute("""
                        UPDATE procesos
                        SET obra = COALESCE(NULLIF(obra, ''), ?),
                            cantidad = COALESCE(cantidad, ?),
                            perfil = COALESCE(NULLIF(perfil, ''), ?),
                            peso = COALESCE(peso, ?),
                            descripcion = COALESCE(NULLIF(descripcion, ''), ?),
                            escaneado_qr = 1
                        WHERE posicion=?
                    """, (qr_obra or None, cantidad_num, qr_perfil or None, peso_num, qr_desc or None, pos))
            else:
                db.execute("""
                    INSERT INTO procesos (posicion, obra, cantidad, perfil, peso, descripcion, escaneado_qr)
                    VALUES (?, ?, ?, ?, ?, ?, 1)
                """, (pos, qr_obra or None, cantidad_num, qr_perfil or None, peso_num, qr_desc or None))
                if qr_obra:
                    ot_id_aplicar = qr_ot_id or _resolver_ot_id_para_obra(db, qr_obra)
                    if ot_id_aplicar:
                        db.execute(
                            "UPDATE procesos SET ot_id = ? WHERE posicion=? AND obra=? AND ot_id IS NULL",
                            (ot_id_aplicar, pos, qr_obra)
                        )
            db.commit()

            if qr_obra:
                datos_iniciales = db.execute("""
                    SELECT * FROM procesos
                    WHERE posicion=? AND obra=? AND eliminado=0
                    LIMIT 1
                """, (pos, qr_obra)).fetchone()
                todas_filas = db.execute("""
                    SELECT * FROM procesos
                    WHERE posicion=? AND obra=? AND eliminado=0
                    ORDER BY id
                """, (pos, qr_obra)).fetchall()
            else:
                datos_iniciales = db.execute("""
                    SELECT * FROM procesos
                    WHERE posicion=? AND obra IS NOT NULL AND eliminado=0
                    LIMIT 1
                """, (pos,)).fetchone()
                todas_filas = db.execute("""
                    SELECT * FROM procesos
                    WHERE posicion=? AND eliminado=0
                    ORDER BY id
                """, (pos,)).fetchall()
    except Exception:
        pass

    # Consolidar metadatos QR para esta pieza/obra y reflejarlos en todas sus etapas.
    obra_scope = qr_obra or (
        str(datos_iniciales[8]).strip() if datos_iniciales and len(datos_iniciales) > 8 and datos_iniciales[8] else ""
    )
    if obra_scope:
        _completar_metadatos_por_obra_pos(db, obra_scope, pos)
        datos_iniciales = db.execute(
            """
            SELECT * FROM procesos
            WHERE posicion=? AND obra=?
            LIMIT 1
            """,
            (pos, obra_scope),
        ).fetchone()
        todas_filas = db.execute(
            """
            SELECT * FROM procesos
            WHERE posicion=? AND obra=?
            ORDER BY id
            """,
            (pos, obra_scope),
        ).fetchall()

    obra_scope_btn = qr_obra or (str(datos_iniciales[2]).strip() if datos_iniciales and len(datos_iniciales) > 2 and datos_iniciales[2] else "")
    ot_scope_btn = qr_ot_id or _obtener_ot_id_pieza(db, pos, obra_scope_btn)
    es_completada = pieza_completada(pos, qr_obra if qr_obra else None, ot_scope_btn)

    html = f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {{
        font-family: "Segoe UI", Tahoma, Arial, sans-serif;
        padding: 16px;
        background: radial-gradient(circle at 12% 0%, #f8fbff 0%, #eef3f7 55%, #e8edf3 100%);
        color: #0f172a;
    }}
    h2 {{
        margin: 0 0 14px 0;
        font-size: 28px;
        letter-spacing: 0.2px;
    }}
    .card {{
        background: #ffffff;
        padding: 16px;
        border-radius: 14px;
        margin-bottom: 12px;
        border: 1px solid #dbe4ee;
        box-shadow: 0 6px 18px rgba(15,23,42,0.06);
        display: flex;
        justify-content: space-between;
        align-items: flex-start;
        gap: 14px;
    }}
    .card-info {{
        flex: 1;
    }}
    .card-actions {{
        min-width: 150px;
    }}
    .process-title {{
        font-size: 22px;
        font-weight: 800;
        color: #111827;
        margin-bottom: 6px;
        letter-spacing: 0.3px;
    }}
    .meta-line {{
        font-size: 16px;
        margin-top: 3px;
        color: #0f172a;
    }}
    .kv-line {{
        margin-top: 4px;
        font-size: 16px;
        color: #0f172a;
    }}
    .kv-label {{
        font-weight: 700;
        color: #1f2937;
    }}
    .reins-title {{
        margin-top: 10px;
        margin-bottom: 4px;
        font-size: 15px;
        font-weight: 800;
        letter-spacing: 0.5px;
        color: #0f172a;
    }}
    .reins-content-empty {{
        color: #64748b;
        font-style: italic;
    }}
    .ciclo-card {{
        margin-top: 8px;
        padding: 10px;
        border: 1px solid #d8e2ec;
        border-radius: 10px;
        background: linear-gradient(180deg, #ffffff 0%, #f8fbff 100%);
    }}
    .ciclo-title {{
        font-size: 16px;
        font-weight: 800;
        margin-bottom: 4px;
    }}
    .data-card {{
        background: linear-gradient(120deg, #ecfeff 0%, #e0f2fe 100%);
        padding: 16px;
        border-radius: 14px;
        margin-bottom: 15px;
        border: 1px solid #bae6fd;
        border-left: 6px solid #0284c7;
    }}
    .data-row {{
        display: flex;
        justify-content: space-between;
        padding: 9px 0;
        border-bottom: 1px dashed #93c5fd;
    }}
    .data-row:last-child {{
        border-bottom: none;
    }}
    .data-label {{
        font-weight: 700;
        color: #0f172a;
    }}
    .data-value {{
        color: #0c4a6e;
        font-weight: 800;
    }}
    .estado-ok {{ color: #15803d; font-weight: bold; }}
    .estado-nc {{ color: #dc2626; font-weight: bold; }}
    .estado-obs {{ color: #ea580c; font-weight: bold; }}
    .estado-om {{ color: #ca8a04; font-weight: bold; }}
    .flujo-badge {{
        display: inline-block;
        margin-top: 4px;
        padding: 5px 12px;
        border-radius: 999px;
        font-size: 12px;
        font-weight: bold;
        border: 1px solid transparent;
    }}
    .flujo-bloqueado {{ background: #fee2e2; color: #b91c1c; border-color: #fecaca; }}
    .flujo-liberado {{ background: #dcfce7; color: #166534; border-color: #86efac; }}
    .flujo-curso {{ background: #ffedd5; color: #9a3412; border-color: #fdba74; }}
    .btn {{
        display: inline-block;
        text-align: center;
        background: #f97316;
        color: white;
        padding: 10px 14px;
        border-radius: 9px;
        text-decoration: none;
        font-weight: 700;
        margin-top: 0;
        font-size: 13px;
        border: none;
        box-shadow: 0 4px 10px rgba(0,0,0,0.08);
        transition: transform 0.12s ease, filter 0.12s ease;
    }}
    .btn:hover {{
        transform: translateY(-1px);
        filter: brightness(0.98);
    }}
    .btn-add {{
        display: block;
        width: 100%;
    }}
    .bloqueado {{
        background: #94a3b8;
        color: #e2e8f0;
        cursor: not-allowed;
    }}
    .warning {{
        background: #fee2e2;
        color: #991b1b;
        padding: 12px;
        border-radius: 8px;
        margin-bottom: 15px;
        font-weight: bold;
    }}
    .completado {{
        background: #dcfce7;
        color: #166534;
        padding: 12px;
        border-radius: 8px;
        border: 1px solid #86efac;
        margin-bottom: 15px;
        font-weight: bold;
    }}
    .footer-actions {{
        display: flex;
        gap: 10px;
        margin-top: 10px;
        flex-wrap: wrap;
    }}
    .footer-actions .btn {{
        flex: 1;
        min-width: 180px;
    }}
    @media (max-width: 980px) {{
        .card {{
            flex-direction: column;
            align-items: stretch;
        }}
        .card-actions {{
            min-width: auto;
        }}
    }}
    </style>
    </head>

    <body>
    <h2>📦 Pieza {html_lib.escape(str(pos))}</h2>
    """

    if es_completada:
        html += "<div class='completado'>✅ PIEZA COMPLETADA - No se puede editar</div>"
    
    if datos_iniciales:
        # índices reales: obra=8, cantidad=9, perfil=10 (ALTER TABLE los agregó al final)
        obra = str(datos_iniciales[8]).strip() if datos_iniciales[8] else "---"
        cantidad = _format_cantidad_1_decimal(datos_iniciales[9]) if datos_iniciales[9] is not None else "---"
        perfil = str(datos_iniciales[10]).strip() if datos_iniciales[10] else "---"
    else:
        obra = qr_obra if qr_obra else "---"
        cantidad = _format_cantidad_1_decimal(qr_cantidad) if qr_cantidad else "---"
        perfil = qr_perfil if qr_perfil else "---"

    html += f"""
    <div class="data-card">
        <div class="data-row">
            <span class="data-label">🏢 OBRA:</span>
            <span class="data-value">{obra}</span>
        </div>
        <div class="data-row">
            <span class="data-label">📍 POS:</span>
            <span class="data-value">{pos}</span>
        </div>
        <div class="data-row">
            <span class="data-label">📊 CANT:</span>
            <span class="data-value">{cantidad}</span>
        </div>
        <div class="data-row">
            <span class="data-label">🔩 PERFIL:</span>
            <span class="data-value">{perfil}</span>
        </div>
    </div>
    """

    responsables_control = _obtener_responsables_control(db)
    responsable_por_firma = {
        str(data.get("firma", "")).strip().lower(): nombre
        for nombre, data in responsables_control.items()
        if str(data.get("firma", "")).strip()
    }

    def _estado_badge(estado_u):
        estado_u = str(estado_u or "").strip().upper()
        if estado_u in ("NC", "NO CONFORME", "NO CONFORMIDAD"):
            return '<span class="flujo-badge flujo-bloqueado">NO CONFORME</span>'
        elif estado_u in ("OK", "APROBADO", "OBS", "OM"):
            return '<span class="flujo-badge flujo-liberado">OK</span>'
        elif estado_u == "RE-INSPECCIÓN":
            return '<span class="flujo-badge flujo-curso">RE-INSPECCION</span>'
        else:
            return '<span class="flujo-badge flujo-curso">PENDIENTE</span>'

    def _estado_cls(estado_u):
        if estado_u in ("OK", "APROBADO"):
            return "estado-ok"
        elif estado_u in ("NC", "NO CONFORME", "NO CONFORMIDAD"):
            return "estado-nc"
        elif estado_u in ("OBS", "OBSERVACION", "OBSERVACIÓN"):
            return "estado-obs"
        elif estado_u in ("OM", "OP MEJORA", "OPORTUNIDAD DE MEJORA"):
            return "estado-om"
        return ""

    def _detalle_pintura(reproceso_txt):
        """Strips ETAPA:X prefix and returns the rest as detail."""
        import re as _re
        txt = (reproceso_txt or "").strip()
        txt = _re.sub(r'^ETAPA:(SUPERFICIE|FONDO|TERMINACION)\s*\|?\s*', '', txt, flags=_re.IGNORECASE).strip()
        return txt if txt else "-"

    def _etapa_desde_reproceso(reproceso_txt, proceso_upper):
        ru = (reproceso_txt or "").upper()
        if "ETAPA:SUPERFICIE" in ru:
            return "SUPERFICIE"
        elif "ETAPA:FONDO" in ru:
            return "FONDO"
        elif "ETAPA:TERMINACION" in ru:
            return "TERMINACION"
        elif proceso_upper == "PINTURA_FONDO":
            return "FONDO"
        return None  # unknown/legacy

    def _resolver_estado_pintura(estado_u, fecha_txt, responsable_txt, re_inspeccion_txt):
        estado_base = str(estado_u or "").strip().upper()
        fecha_final = fecha_txt or "-"
        responsable_final = responsable_txt or "-"
        ciclos = _extraer_ciclos_reinspeccion(re_inspeccion_txt or "")

        if estado_base in ("NC", "NO CONFORME", "NO CONFORMIDAD"):
            if ciclos:
                ultimo = ciclos[-1] or {}
                estado_ultimo = str(ultimo.get("estado") or "").strip().upper()
                fecha_final = ultimo.get("fecha") or fecha_final
                responsable_final = ultimo.get("responsable") or ultimo.get("inspector") or responsable_final
                if _estado_control_aprueba(estado_ultimo):
                    return {"estado": "OK", "fecha": fecha_final, "responsable": responsable_final}
                if estado_ultimo in ("NC", "NO CONFORME", "NO CONFORMIDAD"):
                    return {"estado": "NO CONFORME", "fecha": fecha_final, "responsable": responsable_final}
                return {"estado": "RE-INSPECCIÓN", "fecha": fecha_final, "responsable": responsable_final}
            return {"estado": "NO CONFORME", "fecha": fecha_final, "responsable": responsable_final}

        if _estado_control_aprueba(estado_base):
            return {"estado": "OK", "fecha": fecha_final, "responsable": responsable_final}

        return {"estado": estado_base or "-", "fecha": fecha_final, "responsable": responsable_final}

    # Pre-group PINTURA records by etapa (preserving id order = oldest first)
    pintura_groups = {}  # etapa -> list of rows
    pintura_row_ids = set()
    for r in todas_filas:
        if r[2] and str(r[2]).strip().upper() in ("PINTURA", "PINTURA_FONDO"):
            extr = db.execute("SELECT reproceso FROM procesos WHERE id=?", (r[0],)).fetchone()
            repro = extr[0] if extr else ""
            etapa_r = _etapa_desde_reproceso(repro, str(r[2]).strip().upper())
            if etapa_r is None:
                continue  # skip legacy records without ETAPA tag
            if etapa_r not in pintura_groups:
                pintura_groups[etapa_r] = []
            pintura_groups[etapa_r].append((r, repro))
            pintura_row_ids.add(r[0])

    if len(todas_filas) == 0:
        html += "<div class='card'><b>⚠ SIN REGISTROS TODAVÍA</b></div>"
    else:
        seen_pintura_etapas = set()
        # Mostrar los registros de procesos
        for r in todas_filas:
            # Solo mostrar si tiene proceso definido
            # índices reales: proceso=2, fecha=3, operario=4, estado=5, reproceso=6
            if not r[2]:
                continue

            proc_u = str(r[2]).strip().upper()

            # ── PINTURA: render grouped by etapa ──────────────────────────────
            if proc_u in ("PINTURA", "PINTURA_FONDO"):
                extr0 = db.execute("SELECT reproceso FROM procesos WHERE id=?", (r[0],)).fetchone()
                repro0 = extr0[0] if extr0 else ""
                etapa_r = _etapa_desde_reproceso(repro0, proc_u)
                if etapa_r is None or etapa_r in seen_pintura_etapas:
                    continue
                seen_pintura_etapas.add(etapa_r)

                etapa_label = {"SUPERFICIE": "SUPERFICIE", "FONDO": "FONDO / IMPRIMACIÓN", "TERMINACION": "TERMINACIÓN"}.get(etapa_r, etapa_r)
                group = pintura_groups.get(etapa_r, [])
                if not group:
                    continue

                main_r, main_repro = group[0]
                main_extras = db.execute("SELECT reproceso, re_inspeccion, firma_digital FROM procesos WHERE id=?", (main_r[0],)).fetchone()
                main_reinspeccion = str(main_extras[1] or "").strip() if main_extras else ""
                main_firma = str(main_extras[2] or "").strip() if main_extras else ""
                main_responsable = responsable_por_firma.get(main_firma.strip().lower(), main_firma or "-")
                main_estado = str(main_r[5] or "").strip().upper()
                main_detalle = _detalle_pintura(main_repro)
                estado_resuelto = _resolver_estado_pintura(main_estado, main_r[3] or "-", main_responsable, main_reinspeccion)

                last_r, _ = group[-1]
                grupo_badge = _estado_badge(estado_resuelto.get("estado"))

                # Build re-inspection ciclos from rows 2..N
                ciclos_html = ""
                if len(group) > 1:
                    ciclos_items = []
                    for num_ciclo, (ci_r, ci_repro) in enumerate(group[1:], start=1):
                        ci_extras = db.execute("SELECT reproceso, re_inspeccion, firma_digital FROM procesos WHERE id=?", (ci_r[0],)).fetchone()
                        ci_firma = str(ci_extras[2] or "").strip() if ci_extras else ""
                        ci_responsable = responsable_por_firma.get(ci_firma.strip().lower(), ci_firma or "-")
                        ci_estado = str(ci_r[5] or "").strip().upper()
                        ci_detalle = _detalle_pintura(ci_repro)
                        ci_badge = _estado_badge(ci_estado)
                        ciclos_items.append(f"""
                            <div class="ciclo-card">
                                <div class="ciclo-title">Ciclo {num_ciclo}</div>
                                {ci_badge}<br>
                                Fecha: {ci_r[3] or '-'}<br>
                                Operario: {ci_r[4] or '-'}<br>
                                Estado: <span class="{_estado_cls(ci_estado)}">{ci_r[5] or '-'}</span><br>
                                Motivo: {ci_detalle}<br>
                                Responsable: {ci_responsable}
                            </div>""")
                    ciclos_html = "".join(ciclos_items)
                else:
                    ciclos_html = '<div class="reins-content-empty">Sin ciclos registrados</div>'

                # Delete button for the last record in the group
                acciones = ""
                if not es_completada and not es_obra:
                    acciones += f'''
                    <form method="post" action="/proceso/eliminar/{last_r[0]}" style="margin-top:6px;" onsubmit="return confirm('¿Eliminar el último registro de {etapa_label}?');">
                        <button type="submit" class="btn" style="background:#dc2626; width:100%;">🗑 Eliminar</button>
                    </form>'''

                html += f"""
                <div class="card">
                    <div class="card-info">
                        <div class="process-title">🎨 PINTURA - {etapa_label}</div>
                        {grupo_badge}<br>
                        <div class="meta-line"><span class="kv-label">Fecha:</span> {estado_resuelto.get("fecha") or '-'}</div>
                        <div class="meta-line"><span class="kv-label">Operario:</span> {main_r[4] or '-'}</div>
                        <div class="kv-line"><span class="kv-label">Estado:</span> <span class="{_estado_cls(estado_resuelto.get('estado') or '')}">{estado_resuelto.get("estado") or '-'}</span></div>
                        <div class="kv-line"><span class="kv-label">Motivo:</span> {main_detalle}</div>
                        <div class="kv-line"><span class="kv-label">Responsable:</span> {estado_resuelto.get("responsable") or '-'}</div>
                        <div class="reins-title">RE-INSPECCION</div>
                        {ciclos_html}
                    </div>
                    <div class="card-actions">{acciones}</div>
                </div>
                """
                continue

            # ── Non-PINTURA records: render normally ───────────────────────────
            extras = db.execute(
                "SELECT reproceso, re_inspeccion, firma_digital FROM procesos WHERE id=?",
                (r[0],)
            ).fetchone()
            accion_txt = str(extras[0]).strip() if extras and extras[0] else ""
            re_inspeccion_txt = str(extras[1]).strip() if extras and extras[1] else ""
            firma_txt = str(extras[2]).strip() if extras and extras[2] else ""
            estado_valor = str(r[5] or "").strip().upper()
            estado_class = _estado_cls(estado_valor)

            ciclos_reinspeccion = _extraer_ciclos_reinspeccion(re_inspeccion_txt)
            ultimo_ciclo_estado = (ciclos_reinspeccion[-1].get("estado") or "").upper() if ciclos_reinspeccion else ""
            historico_control_html = ""

            if ciclos_reinspeccion:
                items_reinspeccion = []
                for c in ciclos_reinspeccion:
                    numero = c.get("ciclo") or "-"
                    fecha_c = c.get("fecha") or "-"
                    operario_c = c.get("operario") or "-"
                    responsable_c = c.get("responsable") or c.get("inspector") or ""
                    estado_c = c.get("estado") or "-"
                    estado_c_upper = estado_c.upper()
                    firma_c = c.get("firma") or "-"
                    responsable_c = responsable_c or responsable_por_firma.get(str(firma_c).strip().lower(), "-")
                    motivo_c = c.get("motivo") or "-"
                    if _estado_control_aprueba(estado_c_upper):
                        ciclo_badge = '<span class="flujo-badge flujo-liberado">OK</span>'
                    elif estado_c_upper == "NC":
                        ciclo_badge = '<span class="flujo-badge flujo-bloqueado">NO CONFORME</span>'
                    else:
                        ciclo_badge = '<span class="flujo-badge flujo-curso">RE-INSPECCION EN CURSO</span>'

                    items_reinspeccion.append(
                        f"""
                        <div class=\"ciclo-card\">
                            <div class=\"ciclo-title\">Ciclo {numero}</div>
                            {ciclo_badge}<br>
                            Fecha: {fecha_c}<br>
                            Operario: {operario_c}<br>
                            Estado: {estado_c}<br>
                            Motivo: {motivo_c}<br>
                            Responsable: {responsable_c}
                        </div>
                        """
                    )
                reinspeccion_html = "".join(items_reinspeccion)
            else:
                reinspeccion_html = '<div class="reins-content-empty">Sin ciclos registrados</div>'

            if estado_valor == "NC":
                flujo_estado_html = '<span class="flujo-badge flujo-bloqueado">NO CONFORME</span>'
            elif _estado_control_aprueba(estado_valor):
                flujo_estado_html = '<span class="flujo-badge flujo-liberado">OK</span>'
            elif ciclos_reinspeccion:
                flujo_estado_html = '<span class="flujo-badge flujo-curso">RE-INSPECCION EN CURSO</span>'
            else:
                flujo_estado_html = '<span class="flujo-badge flujo-curso">PENDIENTE DE RESOLUCION</span>'

            reinsp_aprobada = _estado_control_aprueba(ultimo_ciclo_estado)
            acciones = ""
            if not es_completada and not es_obra:
                if estado_valor in ("NC", "NO CONFORME", "NO CONFORMIDAD") and not reinsp_aprobada:
                    acciones += f'<a class="btn" href="/editar/{r[0]}?mode=reinspeccion" style="background:#ea580c; display:block; margin-bottom:6px;">🔁 Re-inspeccion</a>'
                acciones += f'''
                <form method="post" action="/proceso/eliminar/{r[0]}" style="margin-top:6px;" onsubmit="return confirm('¿Eliminar este proceso? Esta acción no se puede deshacer.');">
                    <button type="submit" class="btn" style="background:#dc2626; width:100%;">🗑 Eliminar</button>
                </form>
                '''

            responsable_txt = responsable_por_firma.get(str(firma_txt).strip().lower(), "")
            html += f"""
            <div class="card">
                <div class="card-info">
                    <div class="process-title">{r[2]}</div>
                    {flujo_estado_html}<br>
                    <div class="meta-line"><span class="kv-label">Fecha:</span> {r[3]}</div>
                    <div class="meta-line"><span class="kv-label">Operario:</span> {r[4]}</div>
                    <div class="kv-line"><span class="kv-label">Estado:</span> <span class="{estado_class}">{r[5]}</span></div>
                    <div class="kv-line"><span class="kv-label">Motivo:</span> {accion_txt if accion_txt else '-'}</div>
                    <div class="kv-line"><span class="kv-label">Responsable:</span> {responsable_txt if responsable_txt else '-'}</div>
                    {historico_control_html}
                    <div class="reins-title">RE-INSPECCION</div>
                    {reinspeccion_html}
                </div>
                <div class="card-actions">{acciones}</div>
            </div>
            """

    btn_agregar = "btn-add bloqueado" if es_completada else "btn-add"
    btn_texto = "🔒 PIEZA COMPLETADA" if es_completada else "➕ CARGAR CONTROL"
    obra_url = obra if obra != '---' else (qr_obra or "")
    ot_qs = f"&ot_id={ot_scope_btn}" if ot_scope_btn is not None else ""
    # Si soldadura está aprobada, "Cargar control" redirige al formulario de pintura
    if not es_completada and obra_url:
        sol_rows = db.execute(
            "SELECT estado, re_inspeccion FROM procesos WHERE posicion=? AND obra=? AND UPPER(TRIM(COALESCE(proceso,'')))='SOLDADURA' ORDER BY id",
            (pos, obra_url)
        ).fetchall()
        soldadura_aprobada = any(_proceso_aprobado(r[0], r[1]) for r in sol_rows) if sol_rows else False
    else:
        soldadura_aprobada = False
    if es_completada:
        btn_href = "#"
    elif soldadura_aprobada:
        btn_href = f"/modulo/calidad/escaneo/control-pintura?obra={quote(obra_url)}"
    else:
        btn_href = f"/cargar/{quote(pos)}?obra={quote(obra_url)}{ot_qs}"
    historial_href = f"/pieza/{quote(pos)}/historial?obra={quote(obra_url)}" if obra_url else f"/pieza/{quote(pos)}/historial"
    export_href = f"/pieza/{quote(pos)}/historial/export.csv?obra={quote(obra_url)}" if obra_url else f"/pieza/{quote(pos)}/historial/export.csv"

    acciones_footer = [
        f'<a class="btn" href="{historial_href}" style="background: #0ea5e9;">🕒 Historial ISO</a>',
        f'<a class="btn" href="/home" style="background: #16a34a;">📊 Ver Reporte de Piezas</a>',
    ]
    if not es_obra:
        acciones_footer.insert(0, f'<a class="btn {btn_agregar}" href="{btn_href}">{btn_texto}</a>')
        acciones_footer.insert(2, f'<a class="btn" href="{export_href}" style="background: #2563eb;">⬇ Exportar CSV</a>')

    html += f"""
    <div class="footer-actions">
        {' '.join(acciones_footer)}
    </div>
    </body>
    </html>
    """

    return html


@app.route("/pieza/<pos>/historial")
def historial_pieza(pos):
    obra_qs = (request.args.get("obra") or "").strip()
    db = get_db()
    eventos = _obtener_timeline_pieza(db, pos, obra_qs if obra_qs else None)

    filas_html = ""
    cards_html = ""
    for fecha_evento, proceso, estado_control, estado_pieza, firma_digital, accion, re_inspeccion, tipo_evento in eventos:
        fecha_txt = str(fecha_evento or "-")
        proc_txt = str(proceso or "-")
        control_txt = str(estado_control or "-")
        pieza_txt = str(estado_pieza or "-")
        firma_txt = str(firma_digital or "-")
        accion_txt = str(accion or "-")
        reins_txt = str(re_inspeccion or "-")
        tipo_txt = str(tipo_evento or "-")

        filas_html += f"""
        <tr>
            <td>{fecha_txt}</td>
            <td>{tipo_txt}</td>
            <td>{proc_txt}</td>
            <td>{control_txt}</td>
            <td>{pieza_txt}</td>
            <td>{firma_txt}</td>
            <td>{accion_txt}</td>
        </tr>
        """

        cards_html += f"""
        <div class="timeline-card">
            <div><b>{fecha_txt}</b> · {tipo_txt}</div>
            <div>Proceso: <b>{proc_txt}</b></div>
            <div>Estado control: {control_txt}</div>
            <div>Estado pieza: {pieza_txt}</div>
            <div>Firma: {firma_txt}</div>
            <div>Accion: {accion_txt}</div>
            <div>Re-inspeccion: {reins_txt}</div>
        </div>
        """

    if not filas_html:
        filas_html = "<tr><td colspan='7' style='text-align:center; color:#6b7280;'>Sin eventos de trazabilidad para esta pieza</td></tr>"
    if not cards_html:
        cards_html = "<div class='timeline-card'>Sin eventos de trazabilidad para esta pieza</div>"

    obra_url = obra_qs
    volver_href = f"/pieza/{quote(pos)}?obra={quote(obra_url)}" if obra_url else f"/pieza/{quote(pos)}"
    export_href = f"/pieza/{quote(pos)}/historial/export.csv?obra={quote(obra_url)}" if obra_url else f"/pieza/{quote(pos)}/historial/export.csv"

    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {{ font-family: Arial; background:#f4f4f4; margin:0; padding:15px; }}
    h2 {{ margin-top:0; }}
    .actions {{ display:flex; gap:10px; flex-wrap:wrap; margin-bottom:12px; }}
    .btn {{ display:inline-block; padding:10px 14px; border-radius:6px; color:#fff; text-decoration:none; font-weight:bold; }}
    .btn-back {{ background:#f59e0b; }}
    .btn-csv {{ background:#2563eb; }}
    .card {{ background:#fff; border-radius:8px; padding:12px; box-shadow:0 2px 6px rgba(0,0,0,.08); margin-bottom:12px; }}
    table {{ width:100%; border-collapse:collapse; }}
    th, td {{ padding:8px; border-bottom:1px solid #e5e7eb; text-align:left; font-size:13px; }}
    th {{ background:#ecfeff; color:#0f172a; }}
    .timeline {{ display:grid; gap:8px; }}
    .timeline-card {{ background:#ffffff; border-left:4px solid #0ea5e9; border-radius:6px; padding:10px; box-shadow:0 1px 3px rgba(0,0,0,.06); }}
    </style>
    </head>
    <body>
    <h2>🧾 Historial ISO - Pieza {pos}</h2>
    <div class="actions">
        <a class="btn btn-back" href="{volver_href}">⬅ Volver a pieza</a>
        <a class="btn btn-csv" href="{export_href}">⬇ Exportar CSV</a>
    </div>

    <div class="card">
        <h3 style="margin-top:0;">Linea de tiempo</h3>
        <div class="timeline">{cards_html}</div>
    </div>

    <div class="card">
        <h3 style="margin-top:0;">Tabla de auditoria</h3>
        <table>
            <tr>
                <th>Fecha evento</th>
                <th>Tipo evento</th>
                <th>Proceso</th>
                <th>Estado control</th>
                <th>Estado pieza</th>
                <th>Firma</th>
                <th>Accion</th>
            </tr>
            {filas_html}
        </table>
    </div>
    </body>
    </html>
    """
    return html


@app.route("/pieza/<pos>/historial/export.csv")
def export_historial_pieza_csv(pos):
    obra_qs = (request.args.get("obra") or "").strip()
    db = get_db()
    eventos = _obtener_timeline_pieza(db, pos, obra_qs if obra_qs else None)

    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow([
        "fecha_evento",
        "tipo_evento",
        "posicion",
        "obra",
        "proceso",
        "estado_control",
        "estado_pieza",
        "firma_digital",
        "accion",
        "re_inspeccion",
    ])

    for fecha_evento, proceso, estado_control, estado_pieza, firma_digital, accion, re_inspeccion, tipo_evento in eventos:
        writer.writerow([
            fecha_evento or "",
            tipo_evento or "",
            pos,
            obra_qs,
            proceso or "",
            estado_control or "",
            estado_pieza or "",
            firma_digital or "",
            accion or "",
            re_inspeccion or "",
        ])

    csv_bytes = BytesIO(buffer.getvalue().encode("utf-8-sig"))
    nombre_obra = (obra_qs or "sin_obra").replace(" ", "_")
    nombre_archivo = f"historial_iso_{pos}_{nombre_obra}.csv"
    return send_file(csv_bytes, mimetype="text/csv", as_attachment=True, download_name=nombre_archivo)

# ======================
# FORMULARIO CARGAR
# ======================
@app.route("/cargar/<pos>", methods=["GET","POST"])
def cargar(pos):
    obra_qs = request.args.get("obra", "").strip()
    ot_id_qs = (request.args.get("ot_id") or "").strip()
    pieza_url = f"/pieza/{quote(pos)}?obra={quote(obra_qs)}" if obra_qs else f"/pieza/{quote(pos)}"

    db = get_db()
    ot_id_resuelto = None
    if ot_id_qs.isdigit():
        ot_id_resuelto = int(ot_id_qs)
    elif obra_qs:
        ot_id_resuelto = _obtener_ot_id_pieza(db, pos, obra_qs)
        if ot_id_resuelto is None:
            ot_id_resuelto = _resolver_ot_id_para_obra(db, obra_qs)

    if obra_qs and ot_id_resuelto is None:
        ots_obra_ini = _obtener_ots_para_obra(db, obra_qs)
        if len(ots_obra_ini) > 1:
            return redirect(f"/qr/seleccionar-ot?pos={quote(pos)}&obra={quote(obra_qs)}")

    if pieza_completada(pos, obra_qs if obra_qs else None, ot_id_resuelto):
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
        <a class="btn" href="{pieza_url}">⬅️ Volver</a>
        </body>
        </html>
        """
    
    responsables_control = _obtener_responsables_control(db)
    firmas_responsables = {k: v.get("firma", "") for k, v in responsables_control.items()}
    imagenes_responsables = {k: v.get("firma_url", "") for k, v in responsables_control.items()}
    opciones_responsables = "".join(
        f'<option value="{html_lib.escape(nombre)}">{html_lib.escape(nombre)}</option>'
        for nombre in sorted(responsables_control.keys(), key=lambda x: x.lower())
    )

    # OTs visibles: primero priorizar las de la obra de la pieza; si no hay, usar las vinculadas a la pieza.
    if obra_qs:
        ot_rows = db.execute(
            """
            SELECT id,
                   TRIM(COALESCE(obra, '')) AS obra,
                   TRIM(COALESCE(esquema_pintura, '')) AS esquema,
                   TRIM(COALESCE(espesor_total_requerido, '')) AS espesor
            FROM ordenes_trabajo
            WHERE fecha_cierre IS NULL
              AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
              AND TRIM(COALESCE(obra, '')) = TRIM(?)
            ORDER BY id DESC
            """,
            (obra_qs,)
        ).fetchall()
    else:
        ot_rows = db.execute(
            """
            SELECT DISTINCT ot.id,
                   TRIM(COALESCE(ot.obra, '')) AS obra,
                   TRIM(COALESCE(ot.esquema_pintura, '')) AS esquema,
                   TRIM(COALESCE(ot.espesor_total_requerido, '')) AS espesor
            FROM ordenes_trabajo ot
            JOIN procesos p
                ON p.posicion = ?
                AND (
                    (p.ot_id IS NOT NULL AND p.ot_id = ot.id)
                    OR
                    (p.ot_id IS NULL AND TRIM(COALESCE(p.obra, '')) = TRIM(COALESCE(ot.obra, '')))
                )
            WHERE ot.fecha_cierre IS NULL
              AND (ot.es_mantenimiento IS NULL OR ot.es_mantenimiento = 0)
            ORDER BY ot.id DESC
            """,
            (pos,)
        ).fetchall()

    if not ot_rows:
        ot_rows = db.execute(
            """
            SELECT id,
                   TRIM(COALESCE(obra, '')) AS obra,
                   TRIM(COALESCE(esquema_pintura, '')) AS esquema,
                   TRIM(COALESCE(espesor_total_requerido, '')) AS espesor
            FROM ordenes_trabajo
            WHERE fecha_cierre IS NULL
              AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
            ORDER BY id DESC
            """
        ).fetchall()

    if request.method == "POST":
        ot_id_txt = (request.form.get("ot_id") or "").strip()
        if not ot_id_txt.isdigit():
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
            <div class="error"><b>Seleccioná una OT válida.</b></div>
            <a class="btn" href="{pieza_url}">⬅️ Volver</a>
            </body>
            </html>
            """

        ot_id_actual = int(ot_id_txt)
        ot_row = db.execute(
            """
            SELECT TRIM(COALESCE(obra, '')),
                   TRIM(COALESCE(esquema_pintura, '')),
                   TRIM(COALESCE(espesor_total_requerido, ''))
            FROM ordenes_trabajo
            WHERE id = ?
              AND fecha_cierre IS NULL
              AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
            LIMIT 1
            """,
            (ot_id_actual,)
        ).fetchone()
        if not ot_row:
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
            <div class="error"><b>La OT seleccionada no está activa.</b></div>
            <a class="btn" href="{pieza_url}">⬅️ Volver</a>
            </body>
            </html>
            """

        obra_post = str(ot_row[0] or "").strip()
        pieza_url = f"/pieza/{quote(pos)}?obra={quote(obra_post)}" if obra_post else f"/pieza/{quote(pos)}"
        nuevo_proceso = request.form["proceso"]
        estado_val = (request.form.get("estado") or "").strip().upper()

        es_valido, mensaje = validar_siguiente_proceso(pos, nuevo_proceso, obra_post if obra_post else None, ot_id_actual)
        
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
            <a class="btn" href="{pieza_url}">⬅️ Intentar de nuevo</a>
            </body>
            </html>
            """
        
        accion = (request.form.get("accion") or request.form.get("reproceso") or "").strip()
        responsable = (request.form.get("responsable") or "").strip()
        re_fecha = (request.form.get("reinspeccion_fecha") or "").strip()
        re_operador = (request.form.get("reinspeccion_operador") or "").strip()
        re_estado = (request.form.get("reinspeccion_estado") or "").strip().upper()
        re_motivo = (request.form.get("reinspeccion_motivo") or "").strip()
        re_responsable = (request.form.get("reinspeccion_responsable") or "").strip()
        re_firma_form = (request.form.get("reinspeccion_firma") or "").strip()

        if responsable not in firmas_responsables:
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
            <div class="error"><b>Seleccioná un responsable válido.</b></div>
            <a class="btn" href="{pieza_url}">⬅️ Volver</a>
            </body>
            </html>
            """

        existe_proceso = db.execute(
            """
            SELECT 1 FROM procesos
            WHERE posicion=?
                            AND COALESCE(obra, '') = COALESCE(?, '')
                            AND COALESCE(ot_id, -1) = COALESCE(?, -1)
              AND UPPER(TRIM(COALESCE(proceso, ''))) = ?
            LIMIT 1
            """,
                        (pos, obra_post or "", ot_id_actual, nuevo_proceso.upper())
        ).fetchone()
        if existe_proceso:
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
            <div class="error"><b>❌ El proceso {nuevo_proceso} ya está cargado para esta pieza.</b></div>
            <a class="btn" href="{pieza_url}">⬅️ Volver</a>
            </body>
            </html>
            """

        if any([re_fecha, re_operador, re_estado, re_motivo, re_responsable, re_firma_form]):
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
            <div class="error"><b>La Re-inspeccion se registra solo desde el botón Re-inspeccion.</b></div>
            <a class="btn" href="{pieza_url}">⬅️ Volver</a>
            </body>
            </html>
            """

        firma_form = (request.form.get("firma_digital") or "").strip()
        firma_digital = firmas_responsables.get(responsable, "")

        if not firma_digital or firma_form != firma_digital:
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
            <div class="error"><b>La firma es obligatoria en cada escaneo.</b></div>
            <a class="btn" href="{pieza_url}">⬅️ Volver</a>
            </body>
            </html>
            """

        re_inspeccion = ""
        firma_reinspeccion = ""
        if all([re_fecha, re_operador, re_estado]):
            if re_responsable not in firmas_responsables:
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
                <div class="error"><b>Seleccioná un responsable válido para la Re-inspeccion.</b></div>
                <a class="btn" href="{pieza_url}">⬅️ Volver</a>
                </body>
                </html>
                """
            firma_reinspeccion = firmas_responsables.get(re_responsable, "")
            if not firma_reinspeccion or re_firma_form != firma_reinspeccion:
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
                <div class="error"><b>La firma es obligatoria para registrar la Re-inspeccion.</b></div>
                <a class="btn" href="{pieza_url}">⬅️ Volver</a>
                </body>
                </html>
                """
            re_inspeccion = _agregar_ciclo_reinspeccion(
                "",
                nuevo_proceso,
                re_fecha,
                re_operador,
                re_estado,
                re_motivo,
                firma_reinspeccion,
                re_responsable,
            )
        estado_pieza = _estado_pieza_persistente(estado_val, re_inspeccion)
        firma_evento = firma_reinspeccion or firma_digital
        
        # Insertar NUEVO registro para cada proceso registrado
        # Esto mantiene histórico de procesos
        cursor = db.execute("""
        INSERT INTO procesos (posicion, obra, ot_id, proceso, fecha, operario, estado, reproceso, re_inspeccion, firma_digital, estado_pieza, escaneado_qr)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,1)
        """, (
            pos,
            obra_post or None,
            ot_id_actual,
            nuevo_proceso,
            request.form["fecha"],
            request.form["operario"],
            request.form["estado"],
            accion,
            re_inspeccion,
            firma_digital,
            estado_pieza,
        ))
        _registrar_trazabilidad(
            db,
            cursor.lastrowid,
            pos,
            obra_post,
            nuevo_proceso,
            estado_val,
            estado_pieza,
            firma_evento,
            accion,
            re_inspeccion,
            "ALTA_CONTROL",
        )
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
        <a class="btn" href="{pieza_url}">⬅️ Volver</a>
        </body>
        </html>
        """

    ot_id_existente = ot_id_resuelto or _obtener_ot_id_pieza(db, pos, obra_qs)
    if ot_id_existente is None and len(ot_rows) == 1:
        ot_id_existente = int(ot_rows[0][0])

    obra_ot_actual = obra_qs
    if ot_id_existente is not None:
        for _otid, _obra, _esq, _esp in ot_rows:
            if int(_otid) == int(ot_id_existente):
                obra_ot_actual = _obra or obra_qs
                break

    procesos_hechos = obtener_procesos_completados(pos, obra_ot_actual if obra_ot_actual else None, ot_id_existente)
    operarios_disponibles = _obtener_operarios_disponibles(db)
    opciones_operarios = "".join(
        f'<option value="{html_lib.escape(nombre)}">{html_lib.escape(nombre)}</option>'
        for nombre in operarios_disponibles
    )
    opciones_ot = "".join(
        f'<option value="{int(ot_id)}" data-obra="{html_lib.escape(obra)}" {"selected" if ot_id_existente is not None and int(ot_id_existente) == int(ot_id) else ""}>OT {int(ot_id)} - {html_lib.escape(obra or "(sin obra)")}</option>'
        for ot_id, obra, _esquema, _espesor in ot_rows
    )
    
    # Mostrar qué procesos se pueden hacer
    siguiente_proceso = None
    if len(procesos_hechos) == 0:
        siguiente_proceso = "ARMADO"
    elif len(procesos_hechos) < len(ORDEN_PROCESOS):
        try:
            idx = ORDEN_PROCESOS.index(procesos_hechos[-1])
            siguiente_proceso = ORDEN_PROCESOS[idx + 1]
        except (ValueError, IndexError):
            # Si hay error, forzar al primer proceso
            siguiente_proceso = "ARMADO"
            procesos_hechos = []
    
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
    if ot_id_existente:
        info_orden += f"<br>🧾 OT seleccionada: <b>{ot_id_existente}</b>"
    if obra_ot_actual:
        info_orden += f"<br>🏢 Obra: <b>{html_lib.escape(obra_ot_actual)}</b>"
    info_orden += "</div>"
    ot_hidden = str(ot_id_existente or "")

    return f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * {{ box-sizing: border-box; }}
    body {{
        font-family: "Segoe UI", Tahoma, Arial, sans-serif;
        padding: 16px;
        margin: 0;
        background:
            radial-gradient(circle at 15% 0%, #f8fbff 0%, #eef3f7 55%, #e8edf3 100%);
        color: #0f172a;
    }}
    .wrap {{ max-width: 920px; margin: 0 auto; }}
    .header-card {{
        background: linear-gradient(120deg, #ffffff, #f8fafc);
        border: 1px solid #dbe4ee;
        border-radius: 14px;
        padding: 16px;
        margin-bottom: 12px;
        box-shadow: 0 8px 18px rgba(15,23,42,0.06);
    }}
    h2 {{ margin: 0 0 8px 0; color: #0f172a; }}
    .info-orden {{
        background: #fffbeb;
        border: 1px solid #fde68a;
        color: #92400e;
        padding: 10px;
        border-radius: 8px;
        font-size: 14px;
        line-height: 1.4;
    }}
    .form-card {{
        background: #ffffff;
        border: 1px solid #dbe4ee;
        border-radius: 14px;
        padding: 16px;
        box-shadow: 0 8px 18px rgba(15,23,42,0.06);
    }}
    .grid {{
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 12px;
    }}
    .field {{ display: flex; flex-direction: column; gap: 6px; }}
    .field.full {{ grid-column: 1 / -1; }}
    label {{ font-size: 13px; font-weight: 700; color: #334155; }}
    input, select {{
        width: 100%;
        padding: 10px 12px;
        border: 1px solid #cbd5e1;
        border-radius: 8px;
        background: #fff;
        font-size: 14px;
    }}
    input[readonly] {{ background: #f8fafc; color: #475569; }}
    #estado_select {{ font-weight: 700; }}
    .firma-preview {{
        display: none;
        margin-top: 6px;
        max-width: 280px;
        border: 1px solid #d1d5db;
        border-radius: 6px;
        background: #fff;
        padding: 6px;
    }}
    .reinspeccion-block {{
        margin-top: 14px;
        background: #fff7ed;
        border: 1px solid #fdba74;
        border-radius: 10px;
        padding: 12px;
    }}
    .reinspeccion-title {{ margin: 0 0 10px 0; color: #9a3412; font-size: 15px; }}
    .btn-save {{
        width: 100%;
        padding: 12px;
        margin-top: 14px;
        background: #0f766e;
        color: #fff;
        border: none;
        border-radius: 10px;
        font-weight: 800;
        letter-spacing: 0.2px;
        cursor: pointer;
    }}
    .btn-save:hover {{ background: #0d6660; }}
    @media (max-width: 760px) {{
        .grid {{ grid-template-columns: 1fr; }}
    }}
    </style>
    </head>

    <body>
    <div class="wrap">
        <div class="header-card">
            <h2>🛠 Cargar control - {pos}</h2>
            <div class="info-orden">{info_orden}</div>
        </div>

        <form method="post" class="form-card">
            <input type="hidden" name="obra" id="obra_hidden" value="{html_lib.escape(obra_ot_actual or obra_qs)}">

            <div class="grid">
                <div class="field">
                    <label for="ot_id_select">OT</label>
                    <select name="ot_id" id="ot_id_select" required>
                        <option value="">-- Seleccionar OT --</option>
                        {opciones_ot}
                    </select>
                </div>

                <div class="field">
                    <label for="obra_visible">Obra</label>
                    <input type="text" id="obra_visible" value="{html_lib.escape(obra_ot_actual or obra_qs)}" readonly>
                </div>

                <div class="field">
                    <label for="proceso_select">Proceso</label>
                    <select id="proceso_select" name="proceso">
                        {opciones}
                    </select>
                </div>

                <div class="field">
                    <label for="fecha_control">Fecha</label>
                    <input type="date" id="fecha_control" name="fecha" required>
                </div>

                <div class="field">
                    <label for="operario_select">Operario</label>
                    <select id="operario_select" name="operario" required>
                        <option value="">-- Seleccionar operario --</option>
                        {opciones_operarios}
                    </select>
                </div>

                <div class="field">
                    <label for="estado_select">Estado</label>
                    <select name="estado" id="estado_select">
                        <option value="OK" style="color:#15803d;">OK (APROBADO)</option>
                        <option value="NC" style="color:#dc2626;">NC (No conforme)</option>
                        <option value="OBS" style="color:#ea580c;">OBS (Observacion)</option>
                        <option value="OM" style="color:#ca8a04;">OM (Oportunidad de mejora)</option>
                    </select>
                </div>

                <div class="field full">
                    <label for="accion_input">Accion</label>
                    <input type="text" id="accion_input" name="accion" placeholder="Dejar en blanco si no aplica">
                </div>

                <div class="field">
                    <label for="responsable_select">Responsable</label>
                    <select name="responsable" id="responsable_select" required>
                        <option value="">-- Seleccionar responsable --</option>
                        {opciones_responsables}
                    </select>
                </div>

                <div class="field">
                    <label for="firma_digital_input">Firma (digital)</label>
                    <input type="text" id="firma_digital_input" name="firma_digital" placeholder="Se completa automaticamente al seleccionar responsable" readonly>
                    <img id="firma_ok_preview" class="firma-preview" src="" alt="Firma" onerror="this.style.display='none';">
                </div>
            </div>

            <div id="reinspeccion_block" class="reinspeccion-block">
                <h3 class="reinspeccion-title">Re-inspeccion (solo desde botón Re-inspeccion)</h3>
                <div class="grid">
                    <div class="field">
                        <label for="reinspeccion_fecha">Fecha</label>
                        <input type="date" id="reinspeccion_fecha" name="reinspeccion_fecha">
                    </div>
                    <div class="field">
                        <label for="reinspeccion_operador">Operario</label>
                        <select id="reinspeccion_operador" name="reinspeccion_operador">
                            <option value="">-- Seleccionar operario --</option>
                            {opciones_operarios}
                        </select>
                    </div>
                    <div class="field">
                        <label for="reinspeccion_responsable">Responsable</label>
                        <select id="reinspeccion_responsable" name="reinspeccion_responsable">
                            <option value="">-- Seleccionar responsable --</option>
                            {opciones_responsables}
                        </select>
                    </div>
                    <div class="field">
                        <label for="reinspeccion_firma">Firma re-inspeccion</label>
                        <input type="text" id="reinspeccion_firma" name="reinspeccion_firma" placeholder="Se completa automaticamente al seleccionar responsable" readonly>
                        <img id="reinspeccion_firma_ok_preview" class="firma-preview" src="" alt="Firma Re-inspeccion" onerror="this.style.display='none';">
                    </div>
                    <div class="field">
                        <label for="reinspeccion_estado">Estado</label>
                        <select id="reinspeccion_estado" name="reinspeccion_estado">
                            <option value="">-- Seleccionar --</option>
                            <option value="OK">OK (APROBADO)</option>
                            <option value="NC">NC (No conforme)</option>
                            <option value="OBS">OBS (Observacion)</option>
                            <option value="OM">OM (Oportunidad de mejora)</option>
                        </select>
                    </div>
                    <div class="field full">
                        <label for="reinspeccion_motivo">Motivo (si corresponde)</label>
                        <input type="text" id="reinspeccion_motivo" name="reinspeccion_motivo" placeholder="Motivo del resultado de re-inspeccion">
                    </div>
                </div>
            </div>

            <button type="submit" class="btn-save">💾 Guardar</button>
        </form>
    </div>
    <script>
    (function() {{
        const sel = document.getElementById('estado_select');
        const cargarForm = document.querySelector('form[method="post"]');
        const otSel = document.getElementById('ot_id_select');
        const obraHidden = document.getElementById('obra_hidden');
        const obraVisible = document.getElementById('obra_visible');
        const responsableSel = document.getElementById('responsable_select');
        const firmaInput = document.getElementById('firma_digital_input');
        const firmaPreview = document.getElementById('firma_ok_preview');
        const reinspBlock = document.getElementById('reinspeccion_block');
        const reinspFields = [
            document.getElementById('reinspeccion_fecha'),
            document.getElementById('reinspeccion_operador'),
            document.getElementById('reinspeccion_responsable'),
            document.getElementById('reinspeccion_firma'),
            document.getElementById('reinspeccion_estado'),
            document.getElementById('reinspeccion_motivo'),
        ].filter(Boolean);
        const reinspResponsableSel = document.getElementById('reinspeccion_responsable');
        const reinspFirmaInput = document.getElementById('reinspeccion_firma');
        const reinspFirmaPreview = document.getElementById('reinspeccion_firma_ok_preview');
        const firmasResponsables = {json.dumps(firmas_responsables, ensure_ascii=False)};
        const imagenesResponsables = {json.dumps(imagenes_responsables, ensure_ascii=False)};
        if (!sel || !responsableSel || !firmaInput) return;

        function syncOtMeta() {{
            if (!otSel) return;
            const opt = otSel.options[otSel.selectedIndex];
            const obra = opt ? (opt.getAttribute('data-obra') || '') : '';
            if (obraHidden) obraHidden.value = obra;
            if (obraVisible) obraVisible.value = obra;
        }}

        function setReinspeccionActiva(activa) {{
            if (reinspBlock) reinspBlock.style.opacity = activa ? '1' : '0.55';
            reinspFields.forEach((el) => {{
                el.disabled = !activa;
                if (!activa) el.value = '';
            }});
            if (!activa && reinspFirmaPreview) reinspFirmaPreview.style.display = 'none';
        }}

        function syncResponsable() {{
            const responsable = responsableSel.value || '';
            const firma = firmasResponsables[responsable] || '';
            const firmaUrl = imagenesResponsables[responsable] || '';
            firmaInput.value = firma;
            firmaInput.readOnly = true;
            if (firmaPreview) {{
                if (firmaUrl) {{
                    firmaPreview.src = firmaUrl;
                    firmaPreview.style.display = 'block';
                }} else {{
                    firmaPreview.style.display = 'none';
                }}
            }}
        }}

        function syncReinspeccionResponsable() {{
            if (!reinspResponsableSel || !reinspFirmaInput) return;
            const responsable = reinspResponsableSel.value || '';
            const firma = firmasResponsables[responsable] || '';
            const firmaUrl = imagenesResponsables[responsable] || '';
            reinspFirmaInput.value = firma;
            reinspFirmaInput.readOnly = true;
            if (reinspFirmaPreview) {{
                if (firmaUrl) {{
                    reinspFirmaPreview.src = firmaUrl;
                    reinspFirmaPreview.style.display = 'block';
                }} else {{
                    reinspFirmaPreview.style.display = 'none';
                }}
            }}
        }}

        function pintarEstado() {{
            const v = (sel.value || '').toUpperCase();
            sel.style.backgroundColor = '#ffffff';
            if (v === 'OK') sel.style.color = '#15803d';
            else if (v === 'NC') sel.style.color = '#dc2626';
            else if (v === 'OBS') sel.style.color = '#ea580c';
            else if (v === 'OM') sel.style.color = '#ca8a04';
            else sel.style.color = '#111827';

            setReinspeccionActiva(false);
            syncResponsable();
        }}
        responsableSel.addEventListener('change', syncResponsable);
        if (otSel) otSel.addEventListener('change', () => {{
            syncOtMeta();
            const opt = otSel.options[otSel.selectedIndex];
            const obra = opt ? (opt.getAttribute('data-obra') || '') : '';
            const params = new URLSearchParams();
            if (obra) params.set('obra', obra);
            if (otSel.value) params.set('ot_id', otSel.value);
            window.location.href = `/cargar/{quote(pos)}?${{params.toString()}}`;
        }});
        if (reinspResponsableSel) reinspResponsableSel.addEventListener('change', syncReinspeccionResponsable);
        sel.addEventListener('change', pintarEstado);
        syncOtMeta();
        syncResponsable();
        syncReinspeccionResponsable();
        pintarEstado();
    }})();
    </script>
    </body>
    </html>
    """


# ======================
# EDITAR REGISTRO
# ======================
@app.route("/editar/<int:row_id>", methods=["GET","POST"])
def editar(row_id):
    db = get_db()
    row = db.execute("SELECT id, posicion, obra FROM procesos WHERE id=?", (row_id,)).fetchone()
    
    if not row:
        return "<h3>❌ Registro no encontrado</h3>"
    
    pos = row[1]
    obra = str(row[2]).strip() if row[2] else ""
    pieza_url = f"/pieza/{quote(pos)}?obra={quote(obra)}" if obra else f"/pieza/{quote(pos)}"
    mode = request.args.get("mode", "").strip().lower()
    solo_reinspeccion = (mode == "reinspeccion")

    row_det = db.execute(
        "SELECT proceso, fecha, operario, estado, reproceso, re_inspeccion, firma_digital FROM procesos WHERE id=?",
        (row_id,)
    ).fetchone()
    if not row_det:
        return "<h3>❌ Registro no encontrado</h3>"

    responsables_control = _obtener_responsables_control(db)
    firmas_responsables = {k: v.get("firma", "") for k, v in responsables_control.items()}
    imagenes_responsables = {k: v.get("firma_url", "") for k, v in responsables_control.items()}
    opciones_responsables = "".join(
        f'<option value="{html_lib.escape(nombre)}">{html_lib.escape(nombre)}</option>'
        for nombre in sorted(responsables_control.keys(), key=lambda x: x.lower())
    )
    operarios_disponibles = _obtener_operarios_disponibles(db)
    opciones_operarios = "".join(
        f'<option value="{html_lib.escape(nombre)}">{html_lib.escape(nombre)}</option>'
        for nombre in operarios_disponibles
    )


    # Validar que la pieza no esté completada
    if pieza_completada(pos, obra if obra else None):
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
        <a class="btn" href="{pieza_url}">⬅️ Volver</a>
        </body>
        </html>
        """
    
    if request.method == "POST":
        estado_val = (request.form.get("estado") or "").strip().upper()
        accion = (request.form.get("accion") or request.form.get("reproceso") or "").strip()
        re_fecha = (request.form.get("reinspeccion_fecha") or "").strip()
        re_operador = (request.form.get("reinspeccion_operador") or "").strip()
        re_estado = (request.form.get("reinspeccion_estado") or "").strip().upper()
        re_motivo = (request.form.get("reinspeccion_motivo") or "").strip()
        re_responsable = (request.form.get("reinspeccion_responsable") or "").strip()
        re_firma_form = (request.form.get("reinspeccion_firma") or "").strip()

        if any([re_fecha, re_operador, re_estado, re_motivo, re_responsable, re_firma_form]) and not solo_reinspeccion:
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
            <div class="error"><b>La Re-inspeccion solo se puede completar desde el botón Re-inspeccion.</b></div>
            <a class="btn" href="{pieza_url}">⬅️ Volver</a>
            </body>
            </html>
            """

        if any([re_fecha, re_operador, re_estado, re_motivo, re_responsable, re_firma_form]) and not all([re_fecha, re_operador, re_estado, re_responsable, re_firma_form]):
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
            <div class="error"><b>Completá Fecha, Operario, Responsable, Firma y Estado de Re-inspeccion.</b></div>
            <a class="btn" href="{pieza_url}">⬅️ Volver</a>
            </body>
            </html>
            """

        if any([re_fecha, re_operador, re_estado, re_motivo]) and estado_val != "NC":
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
            <div class="error"><b>La Re-inspeccion solo se habilita cuando el estado es NC.</b></div>
            <a class="btn" href="{pieza_url}">⬅️ Volver</a>
            </body>
            </html>
            """

        firma_form = (request.form.get("firma_digital") or "").strip()
        firma_digital = FIRMA_OK_AUTOMATICA if estado_val == "OK" else firma_form
        if not firma_digital:
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
            <div class="error"><b>La firma es obligatoria en cada escaneo.</b></div>
            <a class="btn" href="{pieza_url}">⬅️ Volver</a>
            </body>
            </html>
            """

        re_inspeccion = str(row_det[5] or "").strip()
        firma_reinspeccion = ""
        if all([re_fecha, re_operador, re_estado, re_responsable, re_firma_form]):
            if re_responsable not in firmas_responsables:
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
                <div class="error"><b>Seleccioná un responsable válido para la Re-inspeccion.</b></div>
                <a class="btn" href="{pieza_url}">⬅️ Volver</a>
                </body>
                </html>
                """
            firma_reinspeccion = firmas_responsables.get(re_responsable, "")
            if not firma_reinspeccion or re_firma_form != firma_reinspeccion:
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
                <div class="error"><b>La firma es obligatoria para registrar la Re-inspeccion.</b></div>
                <a class="btn" href="{pieza_url}">⬅️ Volver</a>
                </body>
                </html>
                """
            re_inspeccion = _agregar_ciclo_reinspeccion(
                re_inspeccion,
                row_det[0],
                re_fecha,
                re_operador,
                re_estado,
                re_motivo,
                firma_reinspeccion,
                re_responsable,
            )
        estado_pieza = _estado_pieza_persistente(estado_val, re_inspeccion)
        firma_evento = firma_reinspeccion or firma_digital
        db.execute("""
        UPDATE procesos 
        SET fecha=?, operario=?, estado=?, reproceso=?, re_inspeccion=?, firma_digital=?, estado_pieza=?
        WHERE id=?
        """, (
            request.form["fecha"],
            request.form["operario"],
            request.form["estado"],
            accion,
            re_inspeccion,
            firma_digital,
            estado_pieza,
            row_id
        ))
        _registrar_trazabilidad(
            db,
            row_id,
            pos,
            obra,
            row_det[0],
            estado_val,
            estado_pieza,
            firma_evento,
            accion,
            re_inspeccion,
            "EDICION_CONTROL",
        )
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
        <a class="btn" href="{pieza_url}">⬅️ Volver</a>
        </body>
        </html>
        """

    estado_actual = str(row_det[3]).strip().upper() if row_det[3] else ""
    mapa_estado = {
        "APROBADO": "OK",
        "NO CONFORME": "NC",
        "OBS": "OBS",
        "OP MEJORA": "OM",
        "OK": "OK",
        "NC": "NC",
        "OM": "OM",
    }
    estado_actual_normalizado = mapa_estado.get(estado_actual, estado_actual)
    estados_nuevos = [
        ("OK", "OK (APROBADO)"),
        ("NC", "NC (No conforme)"),
        ("OBS", "OBS (Observacion)"),
        ("OM", "OM (Oportunidad de mejora)"),
    ]
    colores_estado = {
        "OK": "#15803d",
        "NC": "#dc2626",
        "OBS": "#ea580c",
        "OM": "#ca8a04",
    }
    opciones_estado = ""
    for est_valor, est_label in estados_nuevos:
        selected = "selected" if estado_actual_normalizado == est_valor else ""
        color = colores_estado.get(est_valor, "#111827")
        opciones_estado += f'<option value="{est_valor}" style="color:{color};" {selected}>{est_label}</option>'
    if estado_actual and estado_actual_normalizado not in [e[0] for e in estados_nuevos] and estado_actual != "OK":
        opciones_estado += f'<option value="{estado_actual}" selected>{estado_actual} (actual)</option>'

    firma_val = str(row_det[6]).strip() if row_det[6] else ""

    # En modo re-inspeccion los campos principales van readonly/disabled
    lock = "readonly" if solo_reinspeccion else ""
    lock_sel = "disabled" if solo_reinspeccion else ""
    titulo_form = "🔁 Re-inspeccion" if solo_reinspeccion else "✏️ Editar"
    aviso_modo = ""
    if solo_reinspeccion:
        aviso_modo = """<div class="aviso-modo">⚠️ <b>Modo Re-inspeccion:</b> Solo podés completar los campos de re-inspeccion. El resto está bloqueado.</div>"""

    reinspeccion_section_html = ""
    if solo_reinspeccion:
        reinspeccion_section_html = f"""
        <div id="reinspeccion_block" class="reinspeccion-block">
            <h3 class="reinspeccion-title">Re-inspeccion</h3>
            <div class="grid">
            <div class="field">
                <label for="reinspeccion_fecha">Fecha</label>
                <input type="date" id="reinspeccion_fecha" name="reinspeccion_fecha" value="">
            </div>
            <div class="field">
                <label for="reinspeccion_operador">Operario</label>
                <select id="reinspeccion_operador" name="reinspeccion_operador">
                    <option value="" selected>-- Seleccionar operario --</option>
                    {opciones_operarios}
                </select>
            </div>
            <div class="field">
                <label for="reinspeccion_responsable">Responsable</label>
                <select id="reinspeccion_responsable" name="reinspeccion_responsable">
                    <option value="" selected>-- Seleccionar responsable --</option>
                    {opciones_responsables}
                </select>
            </div>
            <div class="field">
                <label for="reinspeccion_firma">Firma re-inspeccion</label>
                <input type="text" id="reinspeccion_firma" name="reinspeccion_firma" value="" placeholder="Se completa automaticamente al seleccionar responsable" readonly>
                <img id="reinspeccion_firma_ok_preview" class="firma-preview" src="" alt="Firma Re-inspeccion" onerror="this.style.display='none';">
            </div>
            <div class="field">
                <label for="reinspeccion_estado">Estado</label>
                <select id="reinspeccion_estado" name="reinspeccion_estado">
                    <option value="" selected>-- Seleccionar --</option>
                    <option value="OK">OK (APROBADO)</option>
                    <option value="NC">NC (No conforme)</option>
                    <option value="OBS">OBS (Observacion)</option>
                    <option value="OM">OM (Oportunidad de mejora)</option>
                </select>
            </div>
            <div class="field full">
                <label for="reinspeccion_motivo">Motivo (si corresponde)</label>
                <input type="text" id="reinspeccion_motivo" name="reinspeccion_motivo" placeholder="Motivo del resultado de re-inspeccion">
            </div>
            </div>
        </div>
        """

    return f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * {{ box-sizing: border-box; }}
    body {{
        font-family: "Segoe UI", Tahoma, Arial, sans-serif;
        padding: 16px;
        margin: 0;
        background:
            radial-gradient(circle at 15% 0%, #f8fbff 0%, #eef3f7 55%, #e8edf3 100%);
        color: #0f172a;
    }}
    .wrap {{ max-width: 920px; margin: 0 auto; }}
    .header-card {{
        background: linear-gradient(120deg, #ffffff, #f8fafc);
        border: 1px solid #dbe4ee;
        border-radius: 14px;
        padding: 16px;
        margin-bottom: 12px;
        box-shadow: 0 8px 18px rgba(15,23,42,0.06);
    }}
    h2 {{ margin: 0 0 8px 0; color: #0f172a; }}
    .info-pieza {{
        background: #ecfeff;
        border: 1px solid #bae6fd;
        color: #0c4a6e;
        padding: 10px;
        border-radius: 8px;
        font-size: 14px;
    }}
    .aviso-modo {{
        background: #fff7ed;
        border: 1px solid #fdba74;
        border-radius: 8px;
        padding: 10px;
        margin-top: 10px;
        color: #9a3412;
    }}
    .form-card {{
        background: #ffffff;
        border: 1px solid #dbe4ee;
        border-radius: 14px;
        padding: 16px;
        box-shadow: 0 8px 18px rgba(15,23,42,0.06);
    }}
    .grid {{
        display: grid;
        grid-template-columns: repeat(2, minmax(0, 1fr));
        gap: 12px;
    }}
    .field {{ display: flex; flex-direction: column; gap: 6px; }}
    .field.full {{ grid-column: 1 / -1; }}
    label {{ font-size: 13px; font-weight: 700; color: #334155; }}
    input, select {{
        width: 100%;
        padding: 10px 12px;
        border: 1px solid #cbd5e1;
        border-radius: 8px;
        background: #fff;
        font-size: 14px;
    }}
    input[readonly], select[disabled] {{
        background: #f3f4f6;
        color: #6b7280;
        border: 1px solid #d1d5db;
        cursor: not-allowed;
    }}
    #estado_select {{ font-weight: 700; }}
    .firma-preview {{
        display: none;
        margin-top: 6px;
        max-width: 280px;
        border: 1px solid #d1d5db;
        border-radius: 6px;
        background: #fff;
        padding: 6px;
    }}
    .reinspeccion-block {{
        margin-top: 14px;
        background: #fff7ed;
        border: 1px solid #fdba74;
        border-radius: 10px;
        padding: 12px;
    }}
    .reinspeccion-title {{ margin: 0 0 10px 0; color: #9a3412; font-size: 15px; }}
    .btn-save {{
        width: 100%;
        padding: 12px;
        margin-top: 14px;
        background: #0f766e;
        color: #fff;
        border: none;
        border-radius: 10px;
        font-weight: 800;
        letter-spacing: 0.2px;
        cursor: pointer;
    }}
    .btn-save:hover {{ background: #0d6660; }}
    .btn-back {{
        display: block;
        margin-top: 10px;
        text-align: center;
        padding: 10px;
        border-radius: 10px;
        text-decoration: none;
        background: #2563eb;
        color: white;
        font-weight: 700;
    }}
    .btn-back:hover {{ background: #1d4ed8; }}
    @media (max-width: 760px) {{
        .grid {{ grid-template-columns: 1fr; }}
    }}
    </style>
    </head>

    <body>
    <div class="wrap">
        <div class="header-card">
            <h2>{titulo_form} - {row_det[0]}</h2>
            <div class="info-pieza">Pieza: <b>{pos}</b></div>
            {aviso_modo}
        </div>

        <form method="post" class="form-card">
            <div class="grid">
                <div class="field">
                    <label for="fecha_editar">Fecha</label>
                    <input type="date" id="fecha_editar" name="fecha" value="{row_det[1]}" required {lock}>
                </div>
                <div class="field">
                    <label for="operario_editar">Operario</label>
                    <input type="text" id="operario_editar" name="operario" value="{row_det[2]}" required {lock}>
                </div>
                <div class="field">
                    <label for="estado_select">Estado</label>
                    <select name="estado" id="estado_select" {lock_sel}>
                        {opciones_estado}
                    </select>
                    {'<input type="hidden" name="estado" value="' + (row_det[3] or '') + '">' if solo_reinspeccion else ''}
                </div>
                <div class="field">
                    <label for="accion_editar">Accion</label>
                    <input type="text" id="accion_editar" name="accion" value="{row_det[4] if row_det[4] else ''}" {lock}>
                </div>
                <div class="field full">
                    <label for="firma_digital_input">Firma (digital)</label>
                    <input type="text" id="firma_digital_input" name="firma_digital" value="{firma_val}" placeholder="Se completa automaticamente cuando el estado es OK" {lock}>
                    <img id="firma_ok_preview" class="firma-preview" src="/firma-ok" alt="Firma OK" onerror="this.style.display='none';">
                </div>
            </div>

            {reinspeccion_section_html}

            <button type="submit" class="btn-save">💾 {'Guardar Re-inspeccion' if solo_reinspeccion else 'Guardar cambios'}</button>
            <a href="{pieza_url}" class="btn-back">⬅️ Volver a estado de pieza</a>
        </form>
    </div>
    <script>
    (function() {{
        const soloReinsp = {'true' if solo_reinspeccion else 'false'};
        const sel = document.getElementById('estado_select');
        const firmaInput = document.getElementById('firma_digital_input');
        const firmaPreview = document.getElementById('firma_ok_preview');
        const reinspBlock = document.getElementById('reinspeccion_block');
        const reinspFields = [
            document.getElementById('reinspeccion_fecha'),
            document.getElementById('reinspeccion_operador'),
            document.getElementById('reinspeccion_responsable'),
            document.getElementById('reinspeccion_firma'),
            document.getElementById('reinspeccion_estado'),
            document.getElementById('reinspeccion_motivo'),
        ].filter(Boolean);
        const reinspResponsableSel = document.getElementById('reinspeccion_responsable');
        const reinspFirmaInput = document.getElementById('reinspeccion_firma');
        const reinspFirmaPreview = document.getElementById('reinspeccion_firma_ok_preview');
        const firmasResponsables = {json.dumps(firmas_responsables, ensure_ascii=False)};
        const imagenesResponsables = {json.dumps(imagenes_responsables, ensure_ascii=False)};
        const firmaAuto = '{FIRMA_OK_AUTOMATICA}';

        function setReinspeccionActiva(activa) {{
            if (reinspBlock) reinspBlock.style.opacity = activa ? '1' : '0.55';
            reinspFields.forEach((el) => {{
                el.disabled = !activa;
                if (!activa) el.value = '';
            }});
            if (!activa && reinspFirmaPreview) reinspFirmaPreview.style.display = 'none';
        }}

        function syncReinspeccionResponsable() {{
            if (!reinspResponsableSel || !reinspFirmaInput) return;
            const responsable = reinspResponsableSel.value || '';
            const firma = firmasResponsables[responsable] || '';
            const firmaUrl = imagenesResponsables[responsable] || '';
            reinspFirmaInput.value = firma;
            reinspFirmaInput.readOnly = true;
            if (reinspFirmaPreview) {{
                if (firmaUrl) {{
                    reinspFirmaPreview.src = firmaUrl;
                    reinspFirmaPreview.style.display = 'block';
                }} else {{
                    reinspFirmaPreview.style.display = 'none';
                }}
            }}
        }}

        // En modo reinspeccion: siempre activo y firma se controla por el select de reinspeccion
        if (soloReinsp) {{
            setReinspeccionActiva(true);
            if (reinspResponsableSel) reinspResponsableSel.addEventListener('change', syncReinspeccionResponsable);
            syncReinspeccionResponsable();
            return;
        }}

        function pintarEstado() {{
            if (!sel) return;
            const v = (sel.value || '').toUpperCase();
            sel.style.backgroundColor = '#ffffff';
            if (v === 'OK') sel.style.color = '#15803d';
            else if (v === 'NC') sel.style.color = '#dc2626';
            else if (v === 'OBS') sel.style.color = '#ea580c';
            else if (v === 'OM') sel.style.color = '#ca8a04';
            else sel.style.color = '#111827';

            setReinspeccionActiva(v === 'NC');

            if (firmaInput) {{
                if (v === 'OK') {{
                    firmaInput.value = firmaAuto;
                    firmaInput.readOnly = true;
                    if (firmaPreview) firmaPreview.style.display = 'block';
                }} else {{
                    if (firmaInput.value === firmaAuto) firmaInput.value = '';
                    firmaInput.readOnly = false;
                    if (firmaPreview) firmaPreview.style.display = 'none';
                }}
            }}
        }}
        if (sel) sel.addEventListener('change', pintarEstado);
        pintarEstado();
    }})();
    </script>
    </body>
    </html>
    """


@app.route("/proceso/eliminar/<int:row_id>", methods=["POST"])
def eliminar_proceso(row_id):
    if _is_obra_session():
        return _respuesta_sin_permiso()

    db = get_db()
    row = db.execute("SELECT posicion, obra FROM procesos WHERE id=?", (row_id,)).fetchone()
    if not row:
        return "<h3>❌ Proceso no encontrado</h3>"

    pos = str(row[0] or "").strip()
    obra = str(row[1] or "").strip()
    pieza_url = f"/pieza/{quote(pos)}?obra={quote(obra)}" if obra else f"/pieza/{quote(pos)}"

    db.execute("DELETE FROM procesos WHERE id=?", (row_id,))
    db.commit()
    return redirect(pieza_url)

# ======================
# MÓDULO 1 - ÓRDENES DE TRABAJO  →  movido a ot_routes.py (Blueprint registrado en app)
# ======================
# MÓDULOS EXTRAIDOS A BLUEPRINTS
# ======================
from gestion_calidad_routes import gestion_calidad_bp
from calidad_routes import calidad_bp
from parte_routes import parte_bp
from remito_routes import remito_bp
from estado_routes import estado_bp
from produccion_routes import produccion_bp
from generador_routes import generador_bp

app.register_blueprint(gestion_calidad_bp)
app.register_blueprint(calidad_bp)
app.register_blueprint(parte_bp)
app.register_blueprint(remito_bp)
app.register_blueprint(estado_bp)
app.register_blueprint(produccion_bp)
app.register_blueprint(generador_bp)


# ====================== BÚSQUEDA GLOBAL ======================
@app.route("/api/buscar")
def api_buscar():
    q = (request.args.get("q") or "").strip().upper()
    if len(q) < 2:
        return jsonify({"resultados": []})
    
    db = get_db()
    resultados = {
        "ots": [],
        "piezas": [],
        "operarios": []
    }
    
    # Buscar en OTs
    ots = db.execute("""
        SELECT id, obra, titulo, estado 
        FROM ordenes_trabajo 
        WHERE UPPER(id) LIKE ? OR UPPER(obra) LIKE ? OR UPPER(titulo) LIKE ?
        LIMIT 10
    """, (f"%{q}%", f"%{q}%", f"%{q}%")).fetchall()
    
    for ot in ots:
        resultados["ots"].append({
            "id": ot[0],
            "obra": ot[1],
            "titulo": ot[2],
            "estado": ot[3],
            "url": f"/modulo/ot?id={ot[0]}"
        })
    
    # Buscar en piezas (procesos)
    piezas = db.execute("""
        SELECT DISTINCT posicion, obra, ot_id
        FROM procesos
        WHERE UPPER(posicion) LIKE ? OR UPPER(obra) LIKE ?
        ORDER BY fecha DESC
        LIMIT 10
    """, (f"%{q}%", f"%{q}%")).fetchall()
    
    for pieza in piezas:
        resultados["piezas"].append({
            "posicion": pieza[0],
            "obra": pieza[1],
            "ot_id": pieza[2],
            "url": f"/modulo/ot?id={pieza[2]}"
        })
    
    # Buscar operarios
    operarios = db.execute("""
        SELECT DISTINCT operario
        FROM procesos
        WHERE UPPER(operario) LIKE ?
        LIMIT 10
    """, (f"%{q}%",)).fetchall()
    
    for op in operarios:
        if op[0]:
            resultados["operarios"].append({
                "nombre": op[0]
            })
    
    return jsonify(resultados)


# ====================== DIAGNÓSTICO DRIVE ======================
@app.route("/drive/status")
def drive_status():
    if not _is_admin_session():
        return jsonify({"error": "no autorizado"}), 403
    import os as _os
    try:
        import drive_utils as _du
        # Forzar reintento de inicialización para diagnóstico
        _du._drive_service = None
        _du._drive_init_attempted = False
        _du._drive_last_error = None
        svc = _du._get_drive_service()
        disponible = svc is not None
        folder_id = _os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "")
        creds_json_len = len(_os.environ.get("GOOGLE_CREDENTIALS_JSON", ""))
        oauth_client_id_len = len(_os.environ.get("GOOGLE_OAUTH_CLIENT_ID", ""))
        oauth_secret_len = len(_os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", ""))
        oauth_token_len = len(_os.environ.get("GOOGLE_OAUTH_REFRESH_TOKEN", ""))
        msg = "Drive OK" if disponible else "Drive NO disponible"
        if disponible and folder_id:
            try:
                meta = svc.files().get(fileId=folder_id, fields="id,name").execute()
                folder_name = meta.get("name", "?")
                msg = f"Drive OK - Carpeta: {folder_name}"
            except Exception as e:
                msg = f"Drive conectado pero error accediendo carpeta: {e}"
        return jsonify({
            "status": msg,
            "disponible": disponible,
            "error_detalle": _du._drive_last_error,
            "upload_error_detalle": getattr(_du, "_drive_last_upload_error", None),
            "upload_last_ok": getattr(_du, "_drive_last_upload_ok", None),
            "upload_trace": getattr(_du, "_drive_last_upload_trace", None),
            "folder_id_set": bool(folder_id),
            "folder_id_value": folder_id[:12] + "..." if len(folder_id) > 12 else folder_id,
            "service_account_json_len": creds_json_len,
            "oauth_client_id_len": oauth_client_id_len,
            "oauth_client_secret_len": oauth_secret_len,
            "oauth_refresh_token_len": oauth_token_len,
        })
    except Exception as e:
        return jsonify({"status": f"Error: {e}", "disponible": False})


@app.route("/drive/test-upload")
def drive_test_upload():
    if not _is_admin_session():
        return jsonify({"error": "no autorizado"}), 403
    try:
        import drive_utils as _du
        # PDF mínimo válido de prueba
        test_pdf = (
            b"%PDF-1.4\n1 0 obj<</Type/Catalog/Pages 2 0 R>>endobj\n"
            b"2 0 obj<</Type/Pages/Kids[3 0 R]/Count 1>>endobj\n"
            b"3 0 obj<</Type/Page/MediaBox[0 0 3 3]>>endobj\n"
            b"xref\n0 4\n0000000000 65535 f\n"
            b"trailer<</Size 4/Root 1 0 R>>\nstartxref\n0\n%%EOF"
        )
        link = _du.subir_pdf_a_drive(
            test_pdf,
            "test-upload.pdf",
            "_TEST_",
            "_test_seccion_",
            ot_subfolder=None,
        )
        if link:
            return jsonify({
                "ok": True,
                "link": link,
                "trace": getattr(_du, "_drive_last_upload_trace", None),
                "upload": getattr(_du, "_drive_last_upload_ok", None),
            })
        else:
            return jsonify({
                "ok": False,
                "error": getattr(_du, "_drive_last_upload_error", "sin detalle"),
                "trace": getattr(_du, "_drive_last_upload_trace", None),
            })
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ======================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)