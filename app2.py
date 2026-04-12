from flask import Flask, request, redirect, send_file, jsonify
import csv
import html as html_lib
import json
import sqlite3
import pandas as pd
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

app = Flask(__name__)

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
DATABOOKS_DIR = os.path.join(APP_DIR, "DataBooks")

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
    "produccion": "3-Produccion",
    "gestion_calidad": "4-Gestion de calidad",
}


def _normalizar_nombre_carpeta(nombre):
    txt = str(nombre or "").strip() or "SIN_OBRA"
    txt = re.sub(r'[<>:"/\\|?*]+', "-", txt)
    txt = re.sub(r"\s+", " ", txt).strip().rstrip(".")
    return txt or "SIN_OBRA"


def _normalizar_nombre_archivo(nombre):
    txt = str(nombre or "").strip() or "documento.pdf"
    txt = txt.replace("/", "-").replace("\\", "-")
    txt = re.sub(r'[<>:"|?*]+', "-", txt)
    txt = re.sub(r"\s+", "_", txt).strip("._")
    return txt or "documento.pdf"


def _asegurar_estructura_databook(obra):
    obra_dir = os.path.join(DATABOOKS_DIR, _normalizar_nombre_carpeta(obra))
    for seccion_rel in DATABOOK_SECCIONES.values():
        os.makedirs(os.path.join(obra_dir, seccion_rel), exist_ok=True)
    return obra_dir


def _asegurar_estructura_databook_si_valida(obra):
    obra_txt = str(obra or "").strip()
    if not obra_txt or obra_txt == "---":
        return ""
    return _asegurar_estructura_databook(obra_txt)


def _resolver_ot_id_para_obra(db, obra):
    """Si la obra tiene exactamente 1 OT, devuelve su id. Sino devuelve None."""
    obra_txt = str(obra or "").strip()
    if not obra_txt:
        return None
    rows = db.execute(
        "SELECT id FROM ordenes_trabajo WHERE TRIM(COALESCE(obra,'')) = ? ORDER BY id",
        (obra_txt,)
    ).fetchall()
    return rows[0][0] if len(rows) == 1 else None


def _obtener_ots_para_obra(db, obra):
    """Devuelve lista de (id, titulo) de OTs para una obra (orden ascendente)."""
    obra_txt = str(obra or "").strip()
    if not obra_txt:
        return []
    return db.execute(
        "SELECT id, titulo FROM ordenes_trabajo WHERE TRIM(COALESCE(obra,'')) = ? ORDER BY id",
        (obra_txt,)
    ).fetchall()


def _obtener_ot_id_pieza(db, pos, obra):
    """Devuelve el ot_id unico de una pieza si ya fue asignado previamente."""
    obra_txt = str(obra or "").strip()
    if not obra_txt:
        return None
    rows = db.execute(
        "SELECT DISTINCT ot_id FROM procesos WHERE posicion = ? AND TRIM(COALESCE(obra,'')) = ? AND ot_id IS NOT NULL",
        (pos, obra_txt)
    ).fetchall()
    return rows[0][0] if len(rows) == 1 else None


def _guardar_pdf_databook(obra, seccion_key, filename, pdf_bytes):
    if not pdf_bytes:
        return ""

    obra_dir = _asegurar_estructura_databook(obra)
    seccion_rel = DATABOOK_SECCIONES.get(seccion_key, "")
    destino_dir = os.path.join(obra_dir, seccion_rel) if seccion_rel else obra_dir
    os.makedirs(destino_dir, exist_ok=True)

    safe_filename = _normalizar_nombre_archivo(filename)
    if not safe_filename.lower().endswith(".pdf"):
        safe_filename += ".pdf"

    destino_path = os.path.join(destino_dir, safe_filename)
    base, ext = os.path.splitext(destino_path)
    correlativo = 2
    while os.path.exists(destino_path):
        destino_path = f"{base}_{correlativo}{ext}"
        correlativo += 1

    with open(destino_path, "wb") as f:
        f.write(pdf_bytes)

    return destino_path

# ======================
# DB
# ======================
def get_db():
    return sqlite3.connect("database.db")


def _completar_metadatos_por_obra_pos(db, obra=None, posicion=None):
    """Completa cantidad/perfil/peso vacíos usando datos de la misma obra+posición.

    Regla: para cada par (obra, posicion), toma el primer valor no vacío (id DESC)
    y lo propaga únicamente a filas que lo tengan vacío.
    """
    filtros = []
    params = []
    obra_txt = str(obra or "").strip()
    pos_txt = str(posicion or "").strip()

    if obra_txt:
        filtros.append("COALESCE(obra, '') = COALESCE(?, '')")
        params.append(obra_txt)
    if pos_txt:
        filtros.append("TRIM(COALESCE(posicion, '')) = ?")
        params.append(pos_txt)

    where_clause = f"WHERE {' AND '.join(filtros)}" if filtros else ""

    rows = db.execute(
        f"""
        SELECT id,
               TRIM(COALESCE(posicion, '')) AS posicion,
               TRIM(COALESCE(obra, '')) AS obra,
               cantidad,
               perfil,
               peso
        FROM procesos
        {where_clause}
        ORDER BY id DESC
        """,
        tuple(params),
    ).fetchall()

    if not rows:
        return 0

    meta = {}
    for row_id, pos, obr, cantidad, perfil, peso in rows:
        if not pos:
            continue
        key = (obr, pos)
        if key not in meta:
            meta[key] = {
                "cantidad": None,
                "perfil": "",
                "peso": None,
            }

        if meta[key]["cantidad"] is None and cantidad is not None:
            meta[key]["cantidad"] = cantidad
        perfil_txt = str(perfil or "").strip()
        if not meta[key]["perfil"] and perfil_txt:
            meta[key]["perfil"] = perfil_txt
        if meta[key]["peso"] is None and peso is not None:
            meta[key]["peso"] = peso

    updates = 0
    for row_id, pos, obr, cantidad, perfil, peso in rows:
        if not pos:
            continue
        key = (obr, pos)
        m = meta.get(key)
        if not m:
            continue

        perfil_txt = str(perfil or "").strip()

        new_cantidad = cantidad if cantidad is not None else m["cantidad"]
        new_perfil = perfil_txt if perfil_txt else (m["perfil"] or None)
        new_peso = peso if peso is not None else m["peso"]

        if (
            new_cantidad != cantidad
            or new_perfil != (perfil_txt if perfil_txt else None)
            or new_peso != peso
        ):
            db.execute(
                """
                UPDATE procesos
                SET cantidad = ?, perfil = ?, peso = ?
                WHERE id = ?
                """,
                (new_cantidad, new_perfil, new_peso, row_id),
            )
            updates += 1

    if updates:
        db.commit()
    return updates


def _normalizar_texto_busqueda(texto):
    txt = unicodedata.normalize("NFKD", str(texto or ""))
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
    txt = re.sub(r"[^a-zA-Z0-9]+", " ", txt).strip().lower()
    return txt


def _format_cantidad_1_decimal(valor):
    txt = str(valor if valor is not None else "").strip()
    if not txt:
        return "-"
    try:
        num = float(txt.replace(",", "."))
        return f"{num:.1f}"
    except Exception:
        return txt


def _resolver_imagen_firma_empleado(nombre, firma_electronica):
    candidatos = []
    try:
        for nombre_archivo in os.listdir(FIRMAS_EMPLEADOS_DIR):
            ruta_archivo = os.path.join(FIRMAS_EMPLEADOS_DIR, nombre_archivo)
            ext = os.path.splitext(nombre_archivo)[1].lower()
            if os.path.isfile(ruta_archivo) and ext in {".png", ".jpg", ".jpeg", ".webp"}:
                candidatos.append(nombre_archivo)
    except Exception:
        return ""

    if not candidatos:
        return ""

    firma_raw = str(firma_electronica or "").strip()
    firma_norm = _normalizar_texto_busqueda(firma_raw)
    nombre_norm = _normalizar_texto_busqueda(nombre)

    # Regla principal: codigo de firma (ej: 001) debe matchear nombre de archivo (ej: 001-Lea.png)
    codigo_m = re.search(r"\d+", firma_raw)
    codigo = codigo_m.group(0).zfill(3) if codigo_m else ""
    if codigo:
        for archivo in sorted(candidatos, key=lambda x: x.lower()):
            base = os.path.splitext(archivo)[0].lower().strip()
            if base.startswith(codigo + "-") or base == codigo:
                return os.path.join("Firmas empleados", archivo)

    tokens_firma = [t for t in firma_norm.split() if len(t) >= 3]
    tokens_nombre = [t for t in nombre_norm.split() if len(t) >= 3]
    tokens_objetivo = list(dict.fromkeys(tokens_firma + tokens_nombre))

    mejor_archivo = ""
    mejor_puntaje = -1
    for archivo in candidatos:
        base_norm = _normalizar_texto_busqueda(os.path.splitext(archivo)[0])
        puntaje = 0
        for tok in tokens_objetivo:
            if tok in base_norm:
                puntaje += 1
        if firma_norm and firma_norm in base_norm:
            puntaje += 4
        if nombre_norm and nombre_norm in base_norm:
            puntaje += 2
        if puntaje > mejor_puntaje:
            mejor_puntaje = puntaje
            mejor_archivo = archivo

    if mejor_puntaje <= 0:
        return ""

    return os.path.join("Firmas empleados", mejor_archivo)


def _url_firma_desde_path(firma_imagen_path):
    nombre_archivo = os.path.basename(str(firma_imagen_path or "").strip())
    if not nombre_archivo:
        return ""
    ruta_abs = os.path.join(FIRMAS_EMPLEADOS_DIR, nombre_archivo)
    if not os.path.isfile(ruta_abs):
        return ""
    return f"/firma-supervisor/{quote(nombre_archivo)}"


def _obtener_responsables_control(db):
    responsables = {}
    rows = db.execute(
        """
        SELECT nombre, firma_electronica, firma_imagen_path
        FROM empleados_parte
        WHERE LOWER(TRIM(COALESCE(puesto, ''))) LIKE '%supervisor%'
        ORDER BY nombre
        """
    ).fetchall()

    for nombre, firma, firma_imagen_path in rows:
        nombre_txt = str(nombre or "").strip()
        firma_txt = str(firma or "").strip()
        if not nombre_txt or not firma_txt:
            continue

        firma_path = str(firma_imagen_path or "").strip() or _resolver_imagen_firma_empleado(nombre_txt, firma_txt)
        responsables[nombre_txt] = {
            "firma": firma_txt,
            "firma_url": _url_firma_desde_path(firma_path),
        }

    if responsables:
        return responsables

    # Fallback para no romper formularios si aún no hay supervisores cargados en empleados_parte.
    for nombre_txt, firma_txt in INSPECTOR_FIRMAS.items():
        firma_path = _resolver_imagen_firma_empleado(nombre_txt, firma_txt)
        responsables[nombre_txt] = {
            "firma": firma_txt,
            "firma_url": _url_firma_desde_path(firma_path),
        }
    return responsables


def _ruta_firma_responsable(responsables_control, responsable):
    info = responsables_control.get(str(responsable or "").strip()) or {}
    firma_url = str(info.get("firma_url") or "").strip()
    archivo = ""
    if "/firma-supervisor/" in firma_url:
        archivo = unquote(firma_url.rsplit("/", 1)[-1])
    if not archivo:
        firma_rel = _resolver_imagen_firma_empleado(responsable, info.get("firma", ""))
        archivo = os.path.basename(str(firma_rel or "").strip())
    if not archivo:
        return ""
    ruta = os.path.join(FIRMAS_EMPLEADOS_DIR, archivo)
    return ruta if os.path.isfile(ruta) else ""


def _obtener_operarios_disponibles(db):
    rows = db.execute(
        """
        SELECT DISTINCT TRIM(nombre) AS nombre
        FROM empleados_parte
        WHERE LOWER(TRIM(COALESCE(puesto, ''))) LIKE '%operario%'
          AND TRIM(COALESCE(nombre, '')) <> ''
        ORDER BY nombre
        """
    ).fetchall()

    operarios = [str(r[0]).strip() for r in rows if r and str(r[0]).strip()]
    if operarios:
        return operarios

    # Fallback de compatibilidad para instalaciones con datos previos.
    rows = db.execute(
        """
        SELECT DISTINCT TRIM(operario) AS operario
        FROM procesos
        WHERE TRIM(COALESCE(operario, '')) <> ''
        ORDER BY operario
        """
    ).fetchall()
    return [str(r[0]).strip() for r in rows if r and str(r[0]).strip()]

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
        fecha_cierre DATETIME
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
    
    db.execute("""
    CREATE TABLE IF NOT EXISTS avance_produccion (
        id INTEGER PRIMARY KEY,
        ot_id INTEGER NOT NULL,
        fecha DATE,
        porcentaje INTEGER DEFAULT 0,
        observaciones TEXT,
        fecha_creacion DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (ot_id) REFERENCES ordenes_trabajo(id)
    )
    """)

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
            
    except Exception:
        pass  # Si hay un error en la migración, continuamos normalmente

init_db()

# ======================
# FUNCIONES GENERADOR QR
# ======================
def load_clean_excel(path):
    """Carga Excel y detecta automáticamente el encabezado"""
    raw = pd.read_excel(path, header=None)
    for i in range(10):
        row = raw.iloc[i].fillna("").astype(str).str.upper()
        if any("POS" in str(x) for x in row):
            df = pd.read_excel(path, header=i)
            df.columns = [str(c).strip().upper() for c in df.columns]
            print(f"[DEBUG] Encabezado detectado en fila {i}")
            print(f"[DEBUG] Columnas: {list(df.columns)}")
            print(f"[DEBUG] Filas totales: {len(df)}")
            return df
    print(f"[DEBUG] ⚠️ No se detectó encabezado con 'POS' en primeras 10 filas, usando header=0")
    return pd.read_excel(path)

def find_col(df, keyword):
    """Busca una columna por palabra clave (busca en diferentes formatos)"""
    keyword_upper = keyword.upper()
    for c in df.columns:
        col_upper = str(c).strip().upper()
        if keyword_upper in col_upper:
            print(f"  [FOUND] '{keyword}' -> '{c}'")
            return c
    print(f"  [NOT FOUND] '{keyword}' en columnas: {list(df.columns)}")
    return None

def _clean_xls(v):
    """Convierte un valor de celda Excel a string limpio; devuelve '' si es nan/None."""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none", "nat", "null") else s


def obtener_firma_ok_path():
    for nombre in FIRMA_OK_CANDIDATOS:
        path = os.path.join(APP_DIR, nombre)
        if os.path.exists(path):
            return path
    return None

def upsert_piezas_desde_excel(db, df, col_pos, obra_col, cant_col, perfil_col, peso_col, desc_col):
    """Modo anterior opcional: precarga piezas en BD desde Excel."""
    saved_count = 0
    obras_detectadas = set()
    for idx, row in df.iterrows():
        pos = _clean_xls(row.get(col_pos, ""))
        obra = _clean_xls(row.get(obra_col, ""))
        perfil = _clean_xls(row.get(perfil_col, ""))
        descripcion = _clean_xls(row.get(desc_col, ""))
        cant_raw = row.get(cant_col, None)
        peso_raw = row.get(peso_col, None)

        try:
            cantidad = float(cant_raw) if cant_raw not in (None, "") and _clean_xls(cant_raw) else None
        except Exception:
            cantidad = None
        try:
            peso = float(peso_raw) if peso_raw not in (None, "") and _clean_xls(peso_raw) else None
        except Exception:
            peso = None

        print(f"[DEBUG] Fila {idx}: pos={pos}, obra={obra}, cant={cantidad}, perfil={perfil}")

        if pos and obra:
            obras_detectadas.add(obra)
            try:
                existing = db.execute(
                    "SELECT id FROM procesos WHERE posicion=? AND obra=?",
                    (pos, obra)
                ).fetchone()
                if not existing:
                    db.execute("""
                        INSERT INTO procesos (posicion, obra, cantidad, perfil, peso, descripcion)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (pos, obra, cantidad, perfil or None, peso, descripcion or None))
                else:
                    db.execute("""
                        UPDATE procesos
                        SET obra=?, cantidad=?, perfil=?, peso=?, descripcion=?
                        WHERE posicion=? AND obra=?
                    """, (obra, cantidad, perfil or None, peso, descripcion or None, pos, obra))
                saved_count += 1
            except Exception as e:
                print(f"  ❌ ERROR en {pos}: {str(e)}")
                pass

    db.commit()

    for obra in sorted(obras_detectadas):
        _asegurar_estructura_databook_si_valida(obra)

    return saved_count

def generar_etiquetas_qr(excel_file, logo_path, cargar_bd_excel=False):
    """Genera PDF con etiquetas A3 y QR codes"""
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
                db, df, col_pos, obra_col, cant_col, perfil_col, peso_col, desc_col
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
        prefijos_duplicar_igual = ["A", "T"]
        rows_expandidas = []
        
        # Expandir filas según cantidad
        for idx, row in df.iterrows():
            pos = str(row.get(col_pos, "")).strip()
            pos_upper = pos.upper()
            cant_str = str(row.get(cant_col, "0")).split(".")[0]
            
            try:
                cant = int(cant_str) if cant_str else 1
            except:
                cant = 1
            
            es_expandible = any(pos_upper.startswith(prefijo) for prefijo in prefijos_expandibles)
            es_duplicar_igual = any(pos_upper.startswith(prefijo) for prefijo in prefijos_duplicar_igual)
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
                        
                        qr_text = f"http://192.168.0.134:5000/pieza/{quote(pos)}"
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

# ======================
# ORDEN DE PROCESOS
# ======================
ORDEN_PROCESOS = ["ARMADO", "SOLDADURA", "PINTURA", "DESPACHO"]


def _extraer_ciclos_reinspeccion(reinspeccion_txt):
    ciclos = []
    if not reinspeccion_txt:
        return ciclos

    patron = re.compile(
        r"^(?:Ciclo:\s*(\d+)\s*\|\s*)?(?:Proceso:\s*([^|]+)\|\s*)?Fecha:\s*([^|]+)\|\s*(?:Operador|Operario):\s*([^|]+)\|\s*Estado:\s*([^|]+)(?:\|.*)?$",
        re.IGNORECASE,
    )

    lineas = [ln.strip() for ln in str(reinspeccion_txt).split("\n") if ln.strip()]
    for ln in lineas:
        m = patron.match(ln)
        if not m:
            continue
        motivo_m = re.search(r"\|\s*Motivo:\s*([^|]+)", ln, re.IGNORECASE)
        firma_m = re.search(r"\|\s*Firma:\s*([^|]+)", ln, re.IGNORECASE)
        responsable_m = re.search(r"\|\s*(?:Responsable|Inspector):\s*([^|]+)", ln, re.IGNORECASE)
        responsable_txt = (responsable_m.group(1).strip() if responsable_m else "")
        ciclos.append({
            "ciclo": int(m.group(1)) if m.group(1) else None,
            "proceso": (m.group(2) or "").strip().upper(),
            "fecha": (m.group(3) or "").strip(),
            "operario": (m.group(4) or "").strip(),
            "estado": (m.group(5) or "").strip().upper(),
            "motivo": (motivo_m.group(1).strip() if motivo_m else ""),
            "firma": (firma_m.group(1).strip() if firma_m else ""),
            "responsable": responsable_txt,
            "inspector": responsable_txt,
        })
    return ciclos


def _estado_control_aprueba(estado):
    estado_base = (estado or "").strip().upper()
    return estado_base in (
        "OK",
        "APROBADO",
        "OBS",
        "OBSERVACION",
        "OBSERVACIÓN",
        "OM",
        "OP MEJORA",
        "OPORTUNIDAD DE MEJORA",
    )


def _proceso_aprobado(estado, reinspeccion_txt):
    if _estado_control_aprueba(estado):
        return True
    ciclos = _extraer_ciclos_reinspeccion(reinspeccion_txt)
    if not ciclos:
        return False
    return _estado_control_aprueba(ciclos[-1].get("estado"))


def _estado_pieza_persistente(estado, reinspeccion_txt):
    estado_base = (estado or "").strip().upper()
    if _estado_control_aprueba(estado_base):
        return "APROBADA"
    if estado_base in ("NC", "NO CONFORME", "NO CONFORMIDAD"):
        ciclos = _extraer_ciclos_reinspeccion(reinspeccion_txt)
        if ciclos and _estado_control_aprueba(ciclos[-1].get("estado")):
            return "APROBADA"
        return "NO_APROBADA"
    return "PENDIENTE"


def _registrar_trazabilidad(db, proceso_id, posicion, obra, proceso, estado_control, estado_pieza, firma_digital, accion, re_inspeccion, tipo_evento):
    db.execute(
        """
        INSERT INTO trazabilidad_estados (
            proceso_id, posicion, obra, proceso, estado_control, estado_pieza, firma_digital, accion, re_inspeccion, tipo_evento
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            proceso_id,
            (posicion or "").strip(),
            (obra or "").strip() or None,
            (proceso or "").strip().upper(),
            (estado_control or "").strip().upper(),
            (estado_pieza or "").strip().upper(),
            (firma_digital or "").strip(),
            (accion or "").strip(),
            (re_inspeccion or "").strip(),
            (tipo_evento or "").strip().upper(),
        ),
    )


def _agregar_ciclo_reinspeccion(actual, proceso, fecha, operario, estado, motivo="", firma="", responsable=""):
    existentes = _extraer_ciclos_reinspeccion(actual)
    numero = len(existentes) + 1
    linea = (
        f"Ciclo: {numero} | Proceso: {(proceso or '').strip().upper()} | Fecha: {fecha} | "
        f"Operario: {operario} | Estado: {estado}"
    )
    if responsable:
        linea += f" | Responsable: {responsable}"
    if firma:
        linea += f" | Firma: {firma}"
    if motivo:
        linea += f" | Motivo: {motivo}"
    previo = (actual or "").strip()
    return f"{previo}\n{linea}" if previo else linea


def _obtener_timeline_pieza(db, pos, obra=None):
    if obra:
        rows = db.execute(
            """
            SELECT fecha_evento, proceso, estado_control, estado_pieza, firma_digital, accion, re_inspeccion, tipo_evento
            FROM trazabilidad_estados
            WHERE posicion=? AND COALESCE(obra, '') = COALESCE(?, '')
            ORDER BY datetime(fecha_evento) DESC, id DESC
            """,
            (pos, obra),
        ).fetchall()
    else:
        rows = db.execute(
            """
            SELECT fecha_evento, proceso, estado_control, estado_pieza, firma_digital, accion, re_inspeccion, tipo_evento
            FROM trazabilidad_estados
            WHERE posicion=?
            ORDER BY datetime(fecha_evento) DESC, id DESC
            """,
            (pos,),
        ).fetchall()
    return rows


def obtener_procesos_completados(pos, obra=None, ot_id=None):
    """Retorna lista de procesos aprobados (OK efectivo) en orden, sin saltos."""
    db = get_db()
    if ot_id is not None:
        rows = db.execute(
            "SELECT proceso, estado, re_inspeccion FROM procesos WHERE posicion=? AND ot_id=? ORDER BY id",
            (pos, ot_id)
        ).fetchall()
    elif obra:
        rows = db.execute(
            "SELECT proceso, estado, re_inspeccion FROM procesos WHERE posicion=? AND obra=? ORDER BY id",
            (pos, obra)
        ).fetchall()
    else:
        rows = db.execute("SELECT proceso, estado, re_inspeccion FROM procesos WHERE posicion=? ORDER BY id", (pos,)).fetchall()

    aprobados = set()
    for proceso, estado, reinspeccion in rows:
        proc = (proceso or "").strip().upper()
        if proc not in ORDEN_PROCESOS:
            continue
        if _proceso_aprobado(estado, reinspeccion):
            aprobados.add(proc)

    completados = []
    for proc in ORDEN_PROCESOS:
        if proc in aprobados:
            completados.append(proc)
        else:
            break
    return completados

def pieza_completada(pos, obra=None, ot_id=None):
    """Retorna True si DESPACHO está aprobado (OK efectivo)."""
    return "DESPACHO" in obtener_procesos_completados(pos, obra, ot_id)

def validar_siguiente_proceso(pos, nuevo_proceso, obra=None, ot_id=None):
    """Valida que el proceso siga el orden correcto"""
    procesos_hechos = obtener_procesos_completados(pos, obra, ot_id)
    
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

    # Bloqueo adicional: no permitir avanzar si alguna etapa previa tiene NC abierta.
    db = get_db()
    if ot_id is not None:
        rows_prev = db.execute(
            """
            SELECT UPPER(TRIM(COALESCE(proceso, ''))) AS proceso,
                   UPPER(TRIM(COALESCE(estado, ''))) AS estado,
                   COALESCE(re_inspeccion, ''),
                   id
            FROM procesos
            WHERE posicion=? AND ot_id=?
            ORDER BY id DESC
            """,
            (pos, ot_id),
        ).fetchall()
    elif obra:
        rows_prev = db.execute(
            """
            SELECT UPPER(TRIM(COALESCE(proceso, ''))) AS proceso,
                   UPPER(TRIM(COALESCE(estado, ''))) AS estado,
                   COALESCE(re_inspeccion, ''),
                   id
            FROM procesos
            WHERE posicion=? AND COALESCE(obra, '')=COALESCE(?, '')
            ORDER BY id DESC
            """,
            (pos, obra),
        ).fetchall()
    else:
        rows_prev = db.execute(
            """
            SELECT UPPER(TRIM(COALESCE(proceso, ''))) AS proceso,
                   UPPER(TRIM(COALESCE(estado, ''))) AS estado,
                   COALESCE(re_inspeccion, ''),
                   id
            FROM procesos
            WHERE posicion=?
            ORDER BY id DESC
            """,
            (pos,),
        ).fetchall()

    latest_prev = {}
    for proc, estado, reinsp, row_id in rows_prev:
        if proc in ORDEN_PROCESOS and proc not in latest_prev:
            latest_prev[proc] = (estado, reinsp)

    estados_nc = {"NC", "NO CONFORME", "NO CONFORMIDAD"}
    for proc_prev in ORDEN_PROCESOS[:idx_nuevo]:
        dato = latest_prev.get(proc_prev)
        if not dato:
            continue
        estado_prev, reinsp_prev = dato
        if estado_prev in estados_nc and not _proceso_aprobado(estado_prev, reinsp_prev):
            return False, f"❌ No podés avanzar a {nuevo_proceso}: {proc_prev} tiene NC abierta sin cierre de re-inspección"
    
    return True, "OK"

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
        max-width: 1200px;
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
        grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
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
        
        <div class="modules-grid">
            <a href="/modulo/ot" class="module-card ot">
                <span class="module-icon">📋</span>
                <h3>Órdenes de Trabajo</h3>
                <p>Crear y gestionar órdenes de trabajo, seguimiento de estado y entregas</p>
            </a>
            
            <a href="/modulo/produccion" class="module-card produccion">
                <span class="module-icon">🏭</span>
            </td>
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
                <h3>Parte Semanal - Empleados</h3>
                <p>Registro de empleados, horas de trabajo y actividades por operario</p>
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

            <a href="/home" class="module-card piezas">
                <span class="module-icon">📈</span>
                <h3>Estado de Piezas por Proceso</h3>
                <p>Seguimiento por pieza, filtros por obra y avance de procesos escaneados</p>
            </a>
            
            <a href="/modulo/generador" class="module-card generador">
                <span class="module-icon">🏷️</span>
                <h3>Generador de Etiquetas QR</h3>
                <p>Genera etiquetas A3 con códigos QR desde archivos Excel</p>
            </a>

            <a href="/modulo/gestion-calidad" class="module-card gestioncalidad">
                <span class="module-icon">✅</span>
                <h3>Gestión de Calidad</h3>
                <p>Dashboard de no conformidades, observaciones y oportunidades de mejora por proceso</p>
            </a>
            
            <a href="/modulo/historial" class="module-card historial">
                <span class="module-icon">📚</span>
                <h3>Historial de OTs</h3>
                <p>Órdenes de trabajo cerradas - Archivo de OTs finalizadas</p>
            </a>
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
    responsables_control = _obtener_responsables_control(db)
    responsable_por_firma = {
        str(data.get("firma", "")).strip().lower(): nombre
        for nombre, data in responsables_control.items()
        if str(data.get("firma", "")).strip()
    }
    
    # Obtener parámetros de búsqueda
    busqueda_obra = request.args.get('obra', '').strip()
    busqueda_pieza = request.args.get('pieza', '').strip()
    mensaje = request.args.get('mensaje', '').strip()
    
    # Obtener obras disponibles desde piezas escaneadas (mismo universo del listado)
    obras_disponibles = db.execute(
        """
                SELECT DISTINCT TRIM(p.obra) AS obra
                FROM procesos p
                WHERE COALESCE(p.escaneado_qr, 0) = 1
                    AND p.obra IS NOT NULL
                    AND TRIM(p.obra) <> ''
                    AND (
                                (p.ot_id IS NOT NULL AND EXISTS (
                                        SELECT 1 FROM ordenes_trabajo ot
                                        WHERE ot.id = p.ot_id AND ot.fecha_cierre IS NULL
                                ))
                                OR
                                (p.ot_id IS NULL AND EXISTS (
                                        SELECT 1 FROM ordenes_trabajo ot
                                        WHERE TRIM(COALESCE(ot.obra, '')) = TRIM(COALESCE(p.obra, ''))
                                            AND ot.fecha_cierre IS NULL
                                ))
                    )
        ORDER BY obra ASC
        """
    ).fetchall()
    obras_list = [o[0] for o in obras_disponibles]
    
    # Obtener piezas simples (sin JOIN, ya que ot_id podría no existir todavía)
    all_rows = db.execute("""
        SELECT p.*
        FROM procesos p
        WHERE COALESCE(p.escaneado_qr, 0) = 1
          AND (
                (p.ot_id IS NOT NULL AND EXISTS (
                    SELECT 1 FROM ordenes_trabajo ot
                    WHERE ot.id = p.ot_id AND ot.fecha_cierre IS NULL
                ))
                OR
                (p.ot_id IS NULL AND EXISTS (
                    SELECT 1 FROM ordenes_trabajo ot
                    WHERE TRIM(COALESCE(ot.obra, '')) = TRIM(COALESCE(p.obra, ''))
                      AND ot.fecha_cierre IS NULL
                ))
          )
        ORDER BY posicion ASC
    """).fetchall()
    
    # Agrupar por posición + obra para permitir códigos repetidos en distintas obras
    piezas = {}
    for r in all_rows:
        pos = str(r[1] or '').strip()  # posicion (índice 1)
        if not pos:
            continue
        obra = str(r[8] or '').strip() if len(r) > 8 else ''  # obra (índice 8)
        key = (pos, obra)
        if key not in piezas:
            piezas[key] = r  # Guardamos toda la fila
    
    piezas_unicas = sorted(piezas.keys(), key=lambda x: (x[0], x[1]))
    
    # Filtrar por obra (primer filtro)
    if busqueda_obra:
        obra_fil = busqueda_obra.strip().lower()
        piezas_unicas = [k for k in piezas_unicas if (k[1] or '').strip().lower() == obra_fil]
    
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

    # Generar opciones del dropdown con las obras
    obras_options = '<option value="">-- Seleccionar Obra --</option>'
    for obra in obras_list:
        selected = 'selected' if obra == busqueda_obra else ''
        obras_options += f'<option value="{obra}" {selected}>{obra}</option>'

    def obtener_resumen_panel_pieza(pos_sel, obra_sel):
        rows = db.execute(
            """
            SELECT UPPER(TRIM(proceso)), UPPER(TRIM(COALESCE(estado, ''))), COALESCE(re_inspeccion, ''), COALESCE(firma_digital, ''), COALESCE(fecha, ''), COALESCE(estado_pieza, '')
            FROM procesos
            WHERE posicion=?
              AND COALESCE(obra, '') = COALESCE(?, '')
              AND UPPER(TRIM(COALESCE(proceso, ''))) IN ('ARMADO','SOLDADURA','PINTURA','DESPACHO')
                            AND (
                                        (ot_id IS NOT NULL AND EXISTS (
                                                SELECT 1 FROM ordenes_trabajo ot
                                                WHERE ot.id = procesos.ot_id AND ot.fecha_cierre IS NULL
                                        ))
                                        OR
                                        (ot_id IS NULL AND EXISTS (
                                                SELECT 1 FROM ordenes_trabajo ot
                                                WHERE TRIM(COALESCE(ot.obra, '')) = TRIM(COALESCE(procesos.obra, ''))
                                                    AND ot.fecha_cierre IS NULL
                                        ))
                            )
            ORDER BY id DESC
            """,
            (pos_sel, obra_sel or '')
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
    for pos_key, obra_key in piezas_unicas:
        latest_proc, stats_proc = obtener_resumen_panel_pieza(pos_key, obra_key)
        panel_cache[(pos_key, obra_key)] = (latest_proc, stats_proc)
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
        <form method="get" action="/home">
            <div class="filtro-grupo">
                <select name="obra">
                    {obras_options}
                </select>
                <input type="text" name="pieza" placeholder="🔍 Buscar por Posición..." value="{busqueda_pieza}">
            </div>
            <button type="submit">🔎 Buscar</button>
            <button
                type="submit"
                class="btn-eliminar-obra"
                formaction="/home/eliminar-obra"
                formmethod="post"
                onclick="return confirm('¿Seguro que querés eliminar TODAS las piezas de la obra seleccionada? Esta acción no se puede deshacer.')"
            >🗑️ Eliminar piezas de la obra seleccionada</button>
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
        for pos, obra_key in piezas_pagina:
            pieza_data = piezas[(pos, obra_key)]
            latest_proc, stats_proc = panel_cache.get((pos, obra_key), ({}, {}))
            
            # Obtener la obra (índice 8 — obra fue agregada con ALTER TABLE al final)
            obra_raw = str(pieza_data[8]) if pieza_data[8] else ''
            
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
                    estado_pieza_proc = (dato.get('estado_pieza') or '').strip().upper() or '-'

                    # Obtener última fecha aprobada (considerando ciclos de re-inspección)
                    fecha_aprobada = '-'
                    if _estado_control_aprueba(estado_proc):
                        fecha_aprobada = fecha_proc
                    elif ciclos_proc:
                        for ciclo in reversed(ciclos_proc):
                            if _estado_control_aprueba(ciclo.get('estado')):
                                fecha_aprobada = (ciclo.get('fecha') or '-').strip() or '-'
                                break
                        if fecha_aprobada == '-':
                            fecha_aprobada = fecha_proc

                    if _estado_control_aprueba(estado_proc):
                        badge = '<span class="chip chip-ok">APROBADA</span>'
                        hallazgo = 'Conforme' if estado_proc in ('OK', 'APROBADO') else f'Hallazgo {estado_proc}'
                        detalle = fecha_aprobada
                    elif estado_proc in ('NC', 'NO CONFORME', 'NO CONFORMIDAD'):
                        if ciclos_proc and _estado_control_aprueba(ciclos_proc[-1].get('estado')):
                            badge = '<span class="chip chip-ok">APROBADA</span>'
                            hallazgo = 'NC cerrada'
                            detalle = fecha_aprobada
                        elif ciclos_proc:
                            badge = '<span class="chip chip-warn">RE-INSPECCION</span>'
                            hallazgo = 'En curso'
                            detalle = fecha_aprobada
                        else:
                            badge = '<span class="chip chip-nc">NO APROBADA</span>'
                            hallazgo = 'NC abierta'
                            detalle = fecha_proc
                    else:
                        badge = '<span class="chip chip-neutral">PENDIENTE</span>'
                        hallazgo = 'Sin cierre'
                        detalle = fecha_proc

                    firma_txt = 'OK' if firma_proc else 'Falta firma'
                    ultimo_ciclo_txt = '-'
                    if ciclos_proc:
                        ultimo = ciclos_proc[-1]
                        ultimo_ciclo_txt = f"{(ultimo.get('estado') or '-').upper()} | {(ultimo.get('fecha') or '-')}"
                    tooltip_partes = [
                        f"<b>Estado control:</b> {html_lib.escape(estado_proc or '-')}",
                        f"<b>Estado pieza:</b> {html_lib.escape(estado_pieza_proc)}",
                        f"<b>Fecha:</b> {html_lib.escape(fecha_proc)}",
                        f"<b>Responsable:</b> {html_lib.escape(responsable_proc)}",
                        f"<b>Re-inspecciones:</b> {len(ciclos_proc)}",
                        f"<b>Último ciclo:</b> {html_lib.escape(ultimo_ciclo_txt)}",
                    ]
                    tooltip_html = "<br>".join(tooltip_partes)
                    celdas.append(f'<td><div class="stage-box">{badge}<div class="stage-sub">{detalle}</div><div class="stage-tooltip">{tooltip_html}</div></div></td>')
            
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
                    <a class="btn-ver" href="/pieza/{quote(pos)}?obra={quote(obra_link)}">Ver Pieza</a>
                    <form method="post" action="/home/eliminar-pieza" style="display:inline;">
                        <input type="hidden" name="posicion" value="{pos}">
                        <input type="hidden" name="obra" value="{obra_link}">
                        <button
                            type="submit"
                            class="btn-eliminar-pieza"
                            onclick="return confirm('¿Seguro que querés eliminar esta pieza? Esta acción no se puede deshacer.')"
                        >Eliminar</button>
                    </form>
                </td>
            </tr>
            """
        html += "</table>"
        
        # Generar paginación
        html += "<div class='paginacion'>"
        
        # Botón anterior
        params = []
        if busqueda_obra:
            params.append(f"obra={busqueda_obra}")
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

@app.route("/home/eliminar-obra", methods=["POST"])
def eliminar_piezas_por_obra():
    obra = request.form.get("obra", "").strip()
    if not obra:
        return redirect("/home?mensaje=" + quote("⚠️ Seleccioná una obra antes de eliminar"))

    db = get_db()
    cursor = db.execute("DELETE FROM procesos WHERE obra = ?", (obra,))
    eliminadas = cursor.rowcount if cursor.rowcount is not None else 0
    db.commit()

    mensaje = f"✅ Se eliminaron {eliminadas} registro(s) de la obra: {obra}"
    return redirect("/home?mensaje=" + quote(mensaje))

@app.route("/home/eliminar-pieza", methods=["POST"])
def eliminar_pieza_individual():
    posicion = request.form.get("posicion", "").strip()
    obra = request.form.get("obra", "").strip()

    if not posicion:
        return redirect("/home?mensaje=" + quote("⚠️ Falta la posición de la pieza a eliminar"))

    db = get_db()
    if obra:
        cursor = db.execute(
            "DELETE FROM procesos WHERE posicion = ? AND obra = ?",
            (posicion, obra)
        )
    else:
        cursor = db.execute(
            "DELETE FROM procesos WHERE posicion = ? AND (obra IS NULL OR TRIM(obra) = '')",
            (posicion,)
        )

    eliminadas = cursor.rowcount if cursor.rowcount is not None else 0
    db.commit()

    if obra:
        mensaje = f"✅ Pieza eliminada: {posicion} ({obra}) - {eliminadas} registro(s)"
    else:
        mensaje = f"✅ Pieza eliminada: {posicion} - {eliminadas} registro(s)"
    return redirect("/home?mensaje=" + quote(mensaje))

# ======================
# VER PIEZA (MEJORADO)
# ======================
@app.route("/pieza/<pos>")
def pieza(pos):
    db = get_db()

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
                    WHERE posicion=? AND obra=?
                    LIMIT 1
                """, (pos, qr_obra)).fetchone()
                todas_filas = db.execute("""
                    SELECT * FROM procesos
                    WHERE posicion=? AND obra=?
                    ORDER BY id
                """, (pos, qr_obra)).fetchall()
            else:
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

    if len(todas_filas) == 0:
        html += "<div class='card'><b>⚠ SIN REGISTROS TODAVÍA</b></div>"
    else:
        # Mostrar los registros de procesos
        for r in todas_filas:
            # Solo mostrar si tiene proceso definido
            # índices reales: proceso=2, fecha=3, operario=4, estado=5, reproceso=6
            if r[2]:  # si tiene proceso
                extras = db.execute(
                    "SELECT reproceso, re_inspeccion, firma_digital FROM procesos WHERE id=?",
                    (r[0],)
                ).fetchone()
                accion_txt = str(extras[0]).strip() if extras and extras[0] else ""
                re_inspeccion_txt = str(extras[1]).strip() if extras and extras[1] else ""
                firma_txt = str(extras[2]).strip() if extras and extras[2] else ""
                estado_valor = str(r[5] or "").strip().upper()
                if estado_valor in ("OK", "APROBADO"):
                    estado_class = "estado-ok"
                elif estado_valor in ("NC", "NO CONFORME", "NO CONFORMIDAD"):
                    estado_class = "estado-nc"
                elif estado_valor in ("OBS", "OBSERVACION", "OBSERVACIÓN"):
                    estado_class = "estado-obs"
                elif estado_valor in ("OM", "OP MEJORA", "OPORTUNIDAD DE MEJORA"):
                    estado_class = "estado-om"
                else:
                    estado_class = ""

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
                            ciclo_badge = '<span class="flujo-badge flujo-liberado">APROBADA</span>'
                        elif estado_c_upper == "NC":
                            ciclo_badge = '<span class="flujo-badge flujo-bloqueado">NO APROBADA</span>'
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
                    flujo_estado_html = '<span class="flujo-badge flujo-bloqueado">NO APROBADA</span>'
                elif _estado_control_aprueba(estado_valor):
                    flujo_estado_html = '<span class="flujo-badge flujo-liberado">APROBADA</span>'
                elif ciclos_reinspeccion:
                    flujo_estado_html = '<span class="flujo-badge flujo-curso">RE-INSPECCION EN CURSO</span>'
                else:
                    flujo_estado_html = '<span class="flujo-badge flujo-curso">PENDIENTE DE RESOLUCION</span>'

                # Botón Re-inspeccion solo si es NC Y el último ciclo no está OK ya
                reinsp_aprobada = _estado_control_aprueba(ultimo_ciclo_estado)
                acciones = ""
                if not es_completada:
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
    btn_href = "#" if es_completada else f"/cargar/{quote(pos)}?obra={quote(obra_url)}{ot_qs}"
    historial_href = f"/pieza/{quote(pos)}/historial?obra={quote(obra_url)}" if obra_url else f"/pieza/{quote(pos)}/historial"
    export_href = f"/pieza/{quote(pos)}/historial/export.csv?obra={quote(obra_url)}" if obra_url else f"/pieza/{quote(pos)}/historial/export.csv"

    html += f"""
    <div class="footer-actions">
        <a class="btn {btn_agregar}" href="{btn_href}">{btn_texto}</a>
        <a class="btn" href="{historial_href}" style="background: #0ea5e9;">🕒 Historial ISO</a>
        <a class="btn" href="{export_href}" style="background: #2563eb;">⬇ Exportar CSV</a>
        <a class="btn" href="/home" style="background: #16a34a;">📊 Ver Reporte de Piezas</a>
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

    if request.method == "POST":
        obra_post = request.form.get("obra", "").strip() or obra_qs
        ot_id_actual = ot_id_resuelto
        if obra_post and ot_id_actual is None:
            ot_id_actual = _obtener_ot_id_pieza(db, pos, obra_post)
            if ot_id_actual is None:
                ot_id_actual = _resolver_ot_id_para_obra(db, obra_post)
        pieza_url = f"/pieza/{quote(pos)}?obra={quote(obra_post)}" if obra_post else f"/pieza/{quote(pos)}"
        nuevo_proceso = request.form["proceso"]
        estado_val = (request.form.get("estado") or "").strip().upper()
        ots_obra = _obtener_ots_para_obra(db, obra_post)
        if len(ots_obra) > 1 and ot_id_actual is None:
            return redirect(f"/qr/seleccionar-ot?pos={quote(pos)}&obra={quote(obra_post)}")

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
    
    ots_obra = _obtener_ots_para_obra(db, obra_qs)
    ot_id_existente = ot_id_resuelto or _obtener_ot_id_pieza(db, pos, obra_qs)
    procesos_hechos = obtener_procesos_completados(pos, obra_qs if obra_qs else None, ot_id_existente)
    operarios_disponibles = _obtener_operarios_disponibles(db)
    opciones_operarios = "".join(
        f'<option value="{html_lib.escape(nombre)}">{html_lib.escape(nombre)}</option>'
        for nombre in operarios_disponibles
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
    if obra_qs:
        if ot_id_existente:
            info_orden += f"<br>🧾 OT asignada: <b>{ot_id_existente}</b>"
        elif len(ots_obra) > 1:
            info_orden += f"<br>⚠️ Esta obra tiene <b>{len(ots_obra)}</b> OTs. Primero asigná la OT en el paso posterior al escaneo QR."
    info_orden += "</div>"
    ot_hidden = str(ot_id_existente or (ots_obra[0][0] if len(ots_obra) == 1 else ''))

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
    #estado_select {{ font-weight: bold; }}
    </style>
    </head>

    <body>
    <h2>🛠 Cargar control - {pos}</h2>
    {info_orden}

    <form method="post">
        <input type="hidden" name="obra" value="{obra_qs}">
        <input type="hidden" name="ot_id" value="{ot_hidden}">
        Proceso:
        <select name="proceso">
            {opciones}
        </select>

        Fecha:
        <input type="date" name="fecha" required>

        Operario:
        <select name="operario" required>
            <option value="">-- Seleccionar operario --</option>
            {opciones_operarios}
        </select>

        Estado:
        <select name="estado" id="estado_select">
            <option value="OK" style="color:#15803d;">OK (APROBADO)</option>
            <option value="NC" style="color:#dc2626;">NC (No conformidad)</option>
            <option value="OBS" style="color:#ea580c;">OBS (Observacion)</option>
            <option value="OM" style="color:#ca8a04;">OM (Oportunidad de mejora)</option>
        </select>

        Accion:
        <input type="text" name="accion" placeholder="Dejar en blanco si no aplica">

        Responsable:
        <select name="responsable" id="responsable_select" required>
            <option value="">-- Seleccionar responsable --</option>
            {opciones_responsables}
        </select>

        Firma (digital):
        <input type="text" id="firma_digital_input" name="firma_digital" placeholder="Se completa automaticamente al seleccionar responsable" readonly>
        <img id="firma_ok_preview" src="" alt="Firma" style="display:none; margin-top:8px; max-width:280px; border:1px solid #ddd; border-radius:6px; background:white; padding:6px;" onerror="this.style.display='none';">

        <div id="reinspeccion_block" style="margin-top:12px; background:#fff7ed; border:1px solid #fdba74; border-radius:6px; padding:10px;">
            <b>Re-inspeccion (solo desde botón Re-inspeccion)</b><br>
            <div class="form-group">
                <label>Fecha:</label>
                <input type="date" id="reinspeccion_fecha" name="reinspeccion_fecha">
            </div>
            <div class="form-group">
                <label>Operario:</label>
                <select id="reinspeccion_operador" name="reinspeccion_operador">
                    <option value="">-- Seleccionar operario --</option>
                    {opciones_operarios}
                </select>
            </div>
            <div class="form-group">
                <label>Responsable:</label>
                <select id="reinspeccion_responsable" name="reinspeccion_responsable">
                    <option value="">-- Seleccionar responsable --</option>
                    {opciones_responsables}
                </select>
            </div>
            <div class="form-group">
                <label>Firma re-inspeccion:</label>
                <input type="text" id="reinspeccion_firma" name="reinspeccion_firma" placeholder="Se completa automaticamente al seleccionar responsable" readonly>
                <img id="reinspeccion_firma_ok_preview" src="" alt="Firma Re-inspeccion" style="display:none; margin-top:8px; max-width:280px; border:1px solid #ddd; border-radius:6px; background:white; padding:6px;" onerror="this.style.display='none';">
            </div>
            <div class="form-group">
                <label>Estado:</label>
                <select id="reinspeccion_estado" name="reinspeccion_estado">
                    <option value="">-- Seleccionar --</option>
                    <option value="OK">OK (APROBADO)</option>
                    <option value="NC">NC (No conformidad)</option>
                    <option value="OBS">OBS (Observacion)</option>
                    <option value="OM">OM (Oportunidad de mejora)</option>
                </select>
            </div>
            <div class="form-group">
                <label>Motivo (si corresponde):</label>
                <input type="text" id="reinspeccion_motivo" name="reinspeccion_motivo" placeholder="Motivo del resultado de re-inspeccion">
            </div>
        </div>

        <button type="submit">💾 Guardar</button>
    </form>
    <script>
    (function() {{
        const sel = document.getElementById('estado_select');
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
        if (reinspResponsableSel) reinspResponsableSel.addEventListener('change', syncReinspeccionResponsable);
        sel.addEventListener('change', pintarEstado);
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
        ("NC", "NC (No conformidad)"),
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
    lock_style = "background:#f3f4f6; color:#6b7280;" if solo_reinspeccion else ""
    titulo_form = "🔁 Re-inspeccion" if solo_reinspeccion else "✏️ Editar"
    aviso_modo = ""
    if solo_reinspeccion:
        aviso_modo = """<div style="background:#fff7ed; border:1px solid #fdba74; border-radius:6px; padding:10px; margin-bottom:12px;">
            ⚠️ <b>Modo Re-inspeccion:</b> Solo podés completar los campos de re-inspeccion. El resto está bloqueado.</div>"""

    reinspeccion_section_html = ""
    if solo_reinspeccion:
        reinspeccion_section_html = f"""
        <div id="reinspeccion_block" style="margin-top:12px; background:#fff7ed; border:1px solid #fdba74; border-radius:6px; padding:10px;">
            <b>Re-inspeccion</b><br>
            <div class="form-group">
                <label>Fecha:</label>
                <input type="date" id="reinspeccion_fecha" name="reinspeccion_fecha" value="">
            </div>
            <div class="form-group">
                <label>Operario:</label>
                <select id="reinspeccion_operador" name="reinspeccion_operador">
                    <option value="" selected>-- Seleccionar operario --</option>
                    {opciones_operarios}
                </select>
            </div>
            <div class="form-group">
                <label>Responsable:</label>
                <select id="reinspeccion_responsable" name="reinspeccion_responsable">
                    <option value="" selected>-- Seleccionar responsable --</option>
                    {opciones_responsables}
                </select>
            </div>
            <div class="form-group">
                <label>Firma re-inspeccion:</label>
                <input type="text" id="reinspeccion_firma" name="reinspeccion_firma" value="" placeholder="Se completa automaticamente al seleccionar responsable" readonly>
                <img id="reinspeccion_firma_ok_preview" src="" alt="Firma Re-inspeccion" style="display:none; margin-top:8px; max-width:280px; border:1px solid #ddd; border-radius:6px; background:white; padding:6px;" onerror="this.style.display='none';">
            </div>
            <div class="form-group">
                <label>Estado:</label>
                <select id="reinspeccion_estado" name="reinspeccion_estado">
                    <option value="" selected>-- Seleccionar --</option>
                    <option value="OK">OK (APROBADO)</option>
                    <option value="NC">NC (No conformidad)</option>
                    <option value="OBS">OBS (Observacion)</option>
                    <option value="OM">OM (Oportunidad de mejora)</option>
                </select>
            </div>
            <div class="form-group">
                <label>Motivo (si corresponde):</label>
                <input type="text" id="reinspeccion_motivo" name="reinspeccion_motivo" placeholder="Motivo del resultado de re-inspeccion">
            </div>
        </div>
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
    input[readonly], select[disabled] {{
        background: #f3f4f6;
        color: #6b7280;
        border: 1px solid #d1d5db;
        cursor: not-allowed;
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
    .form-group label {{ display:block; font-weight:bold; margin-top:10px; }}
    .info {{ background: #e3f2fd; padding: 10px; border-radius: 5px; margin-bottom: 15px; }}
    #estado_select {{ font-weight: bold; }}
    </style>
    </head>

    <body>
    <h2>{titulo_form} - {row_det[0]}</h2>
    <div class="info">Pieza: <b>{pos}</b></div>
    {aviso_modo}

    <form method="post">
        <div class="form-group">
            <label>Fecha:</label>
            <input type="date" name="fecha" value="{row_det[1]}" required {lock} style="{lock_style}">
        </div>
        <div class="form-group">
            <label>Operario:</label>
            <input type="text" name="operario" value="{row_det[2]}" required {lock} style="{lock_style}">
        </div>
        <div class="form-group">
            <label>Estado:</label>
            <select name="estado" id="estado_select" {lock_sel} style="{lock_style}">
                {opciones_estado}
            </select>
            {'<input type="hidden" name="estado" value="' + (row_det[3] or '') + '">' if solo_reinspeccion else ''}
        </div>
        <div class="form-group">
            <label>Accion:</label>
            <input type="text" name="accion" value="{row_det[4] if row_det[4] else ''}" {lock} style="{lock_style}">
        </div>

        {reinspeccion_section_html}

        <div class="form-group">
            <label>Firma (digital):</label>
            <input type="text" id="firma_digital_input" name="firma_digital" value="{firma_val}" placeholder="Se completa automaticamente cuando el estado es OK" {lock} style="{lock_style}">
            <img id="firma_ok_preview" src="/firma-ok" alt="Firma OK" style="display:none; margin-top:8px; max-width:280px; border:1px solid #ddd; border-radius:6px; background:white; padding:6px;" onerror="this.style.display='none';">
        </div>

        <button type="submit">💾 {'Guardar Re-inspeccion' if solo_reinspeccion else 'Guardar cambios'}</button>
        <a href="{pieza_url}" style="display:block; margin-top:10px; text-align:center; padding:10px; border-radius:8px; text-decoration:none; background:#2563eb; color:white; font-weight:bold;">⬅️ Volver a estado de pieza</a>
    </form>
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
# MÓDULO 1 - ÓRDENES DE TRABAJO
# ======================
@app.route("/modulo/ot")
def ot_lista():
    db = get_db()
    ots = db.execute("SELECT * FROM ordenes_trabajo WHERE fecha_cierre IS NULL ORDER BY id DESC").fetchall()
    
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
        <details open style="margin-bottom:16px;">
        <summary style="cursor:pointer;font-weight:bold;font-size:15px;padding:8px 0;color:#667eea;">
            🏗️ Ver por Obra
        </summary>
        <div style="margin-top:10px;">
        """
        # Agrupar OTs por obra
        obras_dict = {}
        for ot in ots:
            obra_key = str(ot[2] or "Sin obra").strip()
            obras_dict.setdefault(obra_key, []).append(ot)
        for obra_key, ots_obra in sorted(obras_dict.items()):
            html += f"""
            <div style="background:#f0f4ff;border-left:4px solid #667eea;padding:8px 12px;margin-bottom:6px;border-radius:4px;">
                <b>📁 {html_lib.escape(obra_key)}</b>
                &nbsp;&nbsp;
                {'&nbsp;'.join(
                    f'<a href="/modulo/ot/editar/{o[0]}" style="background:#667eea;color:white;padding:3px 9px;border-radius:4px;font-size:12px;text-decoration:none;">OT-{o[0]}: {html_lib.escape(str(o[3] or ""))}</a>'
                    for o in ots_obra
                )}
            </div>
            """
        html += "</div></details>"
        html += """
        <table>
            <tr>
                <th>ID</th>
                <th>Cliente</th>
                <th>Obra</th>
                <th>Título</th>
                <th>Tipo</th>
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
                <td>{ot[9] or '---'}</td>
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
        obra = (request.form.get("obra") or "").strip()
        db = get_db()
        db.execute("""
        INSERT INTO ordenes_trabajo (cliente, obra, titulo, fecha_entrega, estado, estado_avance, hs_previstas, tipo_estructura)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            request.form["cliente"],
            obra,
            request.form["titulo"],
            request.form["fecha_entrega"],
            request.form["estado"],
            0,
            request.form.get("hs_previstas") or 0,
            request.form.get("tipo_estructura") or ""
        ))
        db.commit()
        _asegurar_estructura_databook_si_valida(obra)
        # Si la obra tiene exactamente 1 OT (la que acabamos de crear), asignársela a sus procesos
        nueva_ot_id = db.execute(
            "SELECT id FROM ordenes_trabajo WHERE TRIM(COALESCE(obra,'')) = ? ORDER BY id", (obra,)
        ).fetchall()
        if len(nueva_ot_id) == 1:
            db.execute(
                "UPDATE procesos SET ot_id = ? WHERE TRIM(COALESCE(obra,'')) = ? AND ot_id IS NULL",
                (nueva_ot_id[0][0], obra)
            )
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
        
        <label>Hs Previstas:</label>
        <input type="number" name="hs_previstas" min="0" step="0.5" placeholder="0">

        <label>Tipo de Estructura:</label>
        <select name="tipo_estructura" required>
            <option value="">Seleccionar tipo...</option>
            <option value="TIPO I">TIPO I</option>
            <option value="TIPO II">TIPO II</option>
            <option value="TIPO III">TIPO III</option>
        </select>

        <div style="margin-top:10px; background:#fff7ed; border:1px solid #fed7aa; border-radius:6px; padding:10px; color:#7c2d12; font-size:13px; line-height:1.35;">
            <b>TIPO I:</b> Trabajos de herreria menores. En este grupo entraran trabajos como tapas camaras, barandas individuales, portones, elementos auxiliares para montaje, plataformas, etc.<br><br>
            <b>TIPO II:</b> Son elementos metalicos, estructurales, de complejidad tal que lo conforman varias partes y requieren de una ingenieria de detalle completa.<br><br>
            <b>TIPO III:</b> Son elementos metalicos de fabricacion en serie en donde la ingenieria, fabricacion y controles de calidad no aplica en los tipos 1 y 2. Todo elemento se encuadra en tipo 3 cuando supera las diez unidades.
        </div>
        
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
        SET cliente=?, obra=?, titulo=?, fecha_entrega=?, estado=?, hs_previstas=?, tipo_estructura=?
        WHERE id=?
        """, (
            request.form["cliente"],
            request.form["obra"],
            request.form["titulo"],
            request.form["fecha_entrega"],
            request.form["estado"],
            request.form.get("hs_previstas") or 0,
            request.form.get("tipo_estructura") or "",
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
        
        <label>Hs Previstas:</label>
        <input type="number" name="hs_previstas" min="0" step="0.5" value="{ot[8] or 0}">

        <label>Tipo de Estructura:</label>
        <select name="tipo_estructura" required>
            <option value="TIPO I" {"selected" if (len(ot) > 9 and ot[9] == "TIPO I") else ""}>TIPO I</option>
            <option value="TIPO II" {"selected" if (len(ot) > 9 and ot[9] == "TIPO II") else ""}>TIPO II</option>
            <option value="TIPO III" {"selected" if (len(ot) > 9 and ot[9] == "TIPO III") else ""}>TIPO III</option>
        </select>

        <div style="margin-top:10px; background:#fff7ed; border:1px solid #fed7aa; border-radius:6px; padding:10px; color:#7c2d12; font-size:13px; line-height:1.35;">
            <b>TIPO I:</b> Trabajos de herreria menores. En este grupo entraran trabajos como tapas camaras, barandas individuales, portones, elementos auxiliares para montaje, plataformas, etc.<br><br>
            <b>TIPO II:</b> Son elementos metalicos, estructurales, de complejidad tal que lo conforman varias partes y requieren de una ingenieria de detalle completa.<br><br>
            <b>TIPO III:</b> Son elementos metalicos de fabricacion en serie en donde la ingenieria, fabricacion y controles de calidad no aplica en los tipos 1 y 2. Todo elemento se encuadra en tipo 3 cuando supera las diez unidades.
        </div>
        
        <div style="margin-top:15px; background:#f3e8e8; border:1px solid #e5a3a3; border-radius:6px; padding:10px;">
            <b>Estado de Cierre:</b>
            <p style="margin:8px 0; font-size:13px;">
            {"🔒 <b>CERRADA</b> el " + ot[10][:16] if (len(ot) > 10 and ot[10]) else "✅ ACTIVA"}
            </p>
            {"<button type='submit' formaction='/modulo/ot/reabrir/" + str(ot[0]) + "' formmethod='post' style='width:auto; background:#e5a3a3; padding:8px 12px; border:none; border-radius:4px; cursor:pointer; font-weight:bold; margin-top:0;'>🔓 Reabrir OT</button>" if (len(ot) > 10 and ot[10]) else "<button type='submit' formaction='/modulo/ot/cerrar/" + str(ot[0]) + "' formmethod='post' style='width:auto; background:#667eea; padding:8px 12px; border:none; border-radius:4px; cursor:pointer; font-weight:bold; margin-top:0;' onclick='return confirm(\"¿Cerrar esta OT? Se ocultarán todas sus piezas y procesos.\");'>🔒 Cerrar OT</button>"}
        </div>
        
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

@app.route("/modulo/ot/cerrar/<int:ot_id>", methods=["POST"])
def ot_cerrar(ot_id):
    from datetime import datetime
    db = get_db()
    db.execute(
        "UPDATE ordenes_trabajo SET fecha_cierre = ? WHERE id=?",
        (datetime.now().isoformat(), ot_id)
    )
    db.commit()
    return redirect(f"/modulo/ot/editar/{ot_id}")

@app.route("/modulo/ot/reabrir/<int:ot_id>", methods=["POST"])
def ot_reabrir(ot_id):
    db = get_db()
    db.execute(
        "UPDATE ordenes_trabajo SET fecha_cierre = NULL WHERE id=?",
        (ot_id,)
    )
    db.commit()
    return redirect(f"/modulo/ot/editar/{ot_id}")

@app.route("/modulo/historial")
def historial_ots():
    db = get_db()
    ots_cerradas = db.execute("""
        SELECT * FROM ordenes_trabajo 
        WHERE fecha_cierre IS NOT NULL 
        ORDER BY fecha_cierre DESC
    """).fetchall()
    
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
    table { width: 100%; border-collapse: collapse; background: white; margin-top: 20px; 
            box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
    th, td { padding: 12px; border-bottom: 1px solid #ddd; text-align: left; }
    th { background: #667eea; color: white; }
    tr:hover { background: #f5f5f5; }
    .sin-datos { text-align: center; padding: 30px; color: #999; }
    .header { display: flex; justify-content: space-between; align-items: center; }
    .btn-reabrir { background: #e5a3a3; }
    .btn-reabrir:hover { background: #d48e8e; }
    </style>
    </head>
    <body>
    <div class="header">
        <div>
            <h2>📋 Historial de OTs - Órdenes Cerradas</h2>
            <a href="/" class="btn">⬅️ Volver al Inicio</a>
        </div>
        <a href="/modulo/ot" class="btn">📌 OTs Activas</a>
    </div>
    """
    
    if len(ots_cerradas) == 0:
        html += "<div class='sin-datos'>⚠️ No hay órdenes de trabajo cerradas</div>"
    else:
        html += """
        <details open style="margin-bottom:16px;">
        <summary style="cursor:pointer;font-weight:bold;font-size:15px;padding:8px 0;color:#667eea;">
            📁 Ver por Obra
        </summary>
        <div style="margin-top:10px;">
        """
        obras_dict = {}
        for ot in ots_cerradas:
            obra_key = str(ot[2] or "Sin obra").strip()
            obras_dict.setdefault(obra_key, []).append(ot)
        for obra_key, ots_obra in sorted(obras_dict.items()):
            html += f"""
            <div style="background:#f0f4ff;border-left:4px solid #667eea;padding:8px 12px;margin-bottom:6px;border-radius:4px;">
                <b>📁 {html_lib.escape(obra_key)}</b>
                &nbsp;&nbsp;
                {'&nbsp;'.join(
                    f'<a href="/modulo/ot/editar/{o[0]}" style="background:#667eea;color:white;padding:3px 9px;border-radius:4px;font-size:12px;text-decoration:none;">OT-{o[0]}: {html_lib.escape(str(o[3] or ""))}</a>'
                    for o in ots_obra
                )}
            </div>
            """
        html += "</div></details>"
        html += """
        <table>
            <tr>
                <th>ID</th>
                <th>Cliente</th>
                <th>Obra</th>
                <th>Título</th>
                <th>Tipo</th>
                <th>Fecha Entrega</th>
                <th>Estado</th>
                <th>Cierre</th>
                <th>Acciones</th>
            </tr>
        """
        for ot in ots_cerradas:
            cierre_txt = (ot[10][:16] if (len(ot) > 10 and ot[10]) else "-")
            html += f"""
            <tr style="background:#f0f0f0;">
                <td><b>{ot[0]}</b></td>
                <td>{ot[1]}</td>
                <td>{ot[2]}</td>
                <td>{ot[3]}</td>
                <td>{ot[9] or '---'}</td>
                <td>{ot[4]}</td>
                <td>{ot[5]}</td>
                <td><b>🔒 {cierre_txt}</b></td>
                <td>
                    <a href="/modulo/ot/editar/{ot[0]}" class="btn" style="background: #4facfe;">Ver</a>
                    <form method="post" action="/modulo/ot/reabrir/{ot[0]}" style="display:inline;">
                        <button type="submit" class="btn btn-reabrir">🔓 Reabrir</button>
                    </form>
                </td>
            </tr>
            """
        html += "</table>"
    
    html += """
    </body>
    </html>
    """
    return html

# ======================
# MÓDULO 2 - CALIDAD (Escaneo QR por Pieza)
# ======================
CONTROL_DESPACHO_ITEMS = [
    "Dar aviso al cliente para inspeccionar el producto antes de su envío",
    "Se dio aviso y coordinó con obra el arribo del pedido?",
    "Se coordinó con obra forma de descarga (Pala con uñas, hidro, etc)",
    "Cuenta con etiqueta de identificación con buena legibilidad y en lugar correcto",
    "Confección de remitos para ingreso a planta (Por triplicado)",
    "Control de embalaje: protección de aristas y zonas comprometidas",
    "El conjunto está en buenas condiciones de terminación superficial (Sin golpes, marcas)",
    "El conjunto está en buenas condiciones de pintura",
    "Se enviaron los elementos de fijación necesarios para el montaje",
    "Se envió pintura necesaria para los retoques",
]

@app.route("/modulo/calidad")
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

@app.route("/modulo/gestion-calidad", methods=["GET", "POST"])
def gestion_calidad_dashboard():
    from datetime import date, timedelta

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
        <div class="kpi"><div class="t">No Conformidades</div><div class="v">{total_nc}</div></div>
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
                    <th>No Conformidades</th>
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
                        <option value="NC">NC (No conformidad)</option>
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

# ======================
# SUB-MÓDULO RECEPCIÓN DE MATERIALES
# ======================
CONTROL_RECEPCION_ITEMS = [
    {
        "n": 1,
        "tipo": "Ubicacion del material",
        "detalle": "Se cuenta con suficiente lugar de acopio?",
        "frecuencia": "Siempre",
        "criterio": "100%",
    },
    {
        "n": 2,
        "tipo": "Documentacion",
        "detalle": "Coincide la solicitud de compra con el remito que trae el proveedor?",
        "frecuencia": "Siempre",
        "criterio": "100%",
    },
    {
        "n": 3,
        "tipo": "Documentacion",
        "detalle": "Traen en fisico, o se recibio por mail el certificado de calidad?",
        "frecuencia": "Siempre",
        "criterio": "Si no llego en fisico, hacer el reclamo posterior",
    },
    {
        "n": 4,
        "tipo": "Control visual",
        "detalle": "Material o materia prima correctamente empaquetado, embalado e identificado?",
        "frecuencia": "Siempre",
        "criterio": "En caso de no aprobar, dar aviso al coord. de EEMM",
    },
    {
        "n": 5,
        "tipo": "Control visual cuantitativo de materia prima",
        "detalle": "Controlar cantidad de paquetes",
        "frecuencia": "Siempre",
        "criterio": "100%",
    },
    {
        "n": 6,
        "tipo": "Control visual cuantitativo de materia prima",
        "detalle": "Controlar cantidad de barras",
        "frecuencia": "1 cada 3 paquetes",
        "criterio": "100%",
    },
    {
        "n": 7,
        "tipo": "Control visual cualitativo de materia prima",
        "detalle": "Exentas de defectos superficiales: deformaciones, alabeos, golpes, pliegues, fisuras, cascara excesiva, escamas u otras discontinuidades",
        "frecuencia": "Siempre",
        "criterio": "100%",
    },
    {
        "n": 8,
        "tipo": "Control visual de pintura / consumibles",
        "detalle": "Verificar fecha de caducidad",
        "frecuencia": "Siempre",
        "criterio": "100%",
    },
    {
        "n": 9,
        "tipo": "Control visual de otros",
        "detalle": "Estado general. Consultar a coordinar EEMM por controles particulares",
        "frecuencia": "Siempre",
        "criterio": "100%",
    },
    {
        "n": 10,
        "tipo": "Producto tercerizado",
        "detalle": "Analizar el producto en el formulario 7-9.2 Inspeccion y ensayos en produccion",
        "frecuencia": "Siempre",
        "criterio": "100%",
    },
]

@app.route("/modulo/calidad/recepcion", methods=["GET", "POST"])
def calidad_recepcion():
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Image, Paragraph, Spacer
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import cm
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import cm
    from io import BytesIO
    import os

    db = get_db()
    responsables_control = _obtener_responsables_control(db)
    firmas_responsables = {k: v.get("firma", "") for k, v in responsables_control.items()}
    imagenes_responsables = {k: v.get("firma_url", "") for k, v in responsables_control.items()}
    opciones_responsables = "".join(
        f'<option value="{html_lib.escape(nombre)}">{html_lib.escape(nombre)}</option>'
        for nombre in sorted(responsables_control.keys(), key=lambda x: x.lower())
    )

    if request.method == "POST":
        obra = (request.form.get("obra") or "").strip()
        proveedor = (request.form.get("proveedor") or "").strip()
        remito_asociado = (request.form.get("remito_asociado") or "").strip()
        responsable = (request.form.get("responsable") or "").strip()
        firma_form = (request.form.get("firma_digital") or "").strip()
        fecha = (request.form.get("fecha") or "").strip()

        if not all([obra, proveedor, remito_asociado, responsable, fecha]):
            return "Faltan datos requeridos", 400

        if responsable not in firmas_responsables:
            return "Seleccioná un responsable válido", 400

        firma_digital = firmas_responsables.get(responsable, "")
        if not firma_digital or firma_form != firma_digital:
            return "La firma es obligatoria y se completa automáticamente al seleccionar responsable", 400

        firma_path_responsable = _ruta_firma_responsable(responsables_control, responsable)

        detalle_items = []
        for item in CONTROL_RECEPCION_ITEMS:
            idx = item["n"]
            estado = (request.form.get(f"estado_{idx}") or "").strip().upper()
            observacion = (request.form.get(f"observacion_{idx}") or "").strip()
            if estado not in ("CONFORME", "NO CONFORME", "NO APLICA"):
                return f"Falta completar el estado del item {idx}", 400
            detalle_items.append({
                "n": idx,
                "tipo": item["tipo"],
                "detalle": item["detalle"],
                "frecuencia": item["frecuencia"],
                "criterio": item["criterio"],
                "estado": estado,
                "observacion": observacion,
            })

        pdf_buffer = BytesIO()
        doc = SimpleDocTemplate(
            pdf_buffer,
            pagesize=letter,
            topMargin=0.3*cm,
            bottomMargin=0.6*cm,
            leftMargin=0.5*cm,
            rightMargin=0.5*cm
        )

        elements = []
        styles = getSampleStyleSheet()
        base_style = ParagraphStyle('RecepBase', parent=styles['Normal'], fontSize=7.5, leading=9, textColor=colors.HexColor('#333333'))
        head_style = ParagraphStyle('RecepHead', parent=styles['Normal'], fontSize=7.5, leading=9, alignment=1, fontName='Helvetica-Bold', textColor=colors.white)

        encabezado_path = None
        posibles_encabezados = [
            os.path.join(APP_DIR, "ENCABEZADO_RECEPCION.png"),
            os.path.join(APP_DIR, "ENCABEZADO_RECEPCION.jpg"),
            os.path.join(APP_DIR, "ENCABEZADO_RECEPCION.jpeg"),
            os.path.join(APP_DIR, "ENCABEZADO_RECEPCION", "ENCABEZADO_RECEPCION.png"),
            os.path.join(APP_DIR, "ENCABEZADO_RECEPCION", "encabezado_recepcion.png"),
            os.path.join(APP_DIR, "ENCABEZADO_RECEPCION", "ENCABEZADO_RECEPCION.jpg"),
        ]
        for candidato in posibles_encabezados:
            if os.path.exists(candidato):
                encabezado_path = candidato
                break

        if encabezado_path:
            encabezado_img = Image(encabezado_path)
            max_width = 19.8 * cm
            if encabezado_img.drawWidth > max_width:
                escala = max_width / float(encabezado_img.drawWidth)
                encabezado_img.drawWidth *= escala
                encabezado_img.drawHeight *= escala
            elements.append(encabezado_img)
        else:
            elements.append(Paragraph("<b>CONTROL DE RECEPCION</b>", ParagraphStyle('RH1', parent=styles['Heading2'], alignment=1)))

        elements.append(Spacer(1, 0.2*cm))

        data_info = Table([
            [Paragraph(f"<b>Obra:</b> {obra}", base_style), Paragraph(f"<b>Proveedor:</b> {proveedor}", base_style)],
            [Paragraph(f"<b>Remito asociado:</b> {remito_asociado}", base_style), Paragraph(f"<b>Responsable:</b> {responsable}", base_style)],
            [Paragraph(f"<b>Fecha:</b> {fecha}", base_style), Paragraph("", base_style)],
        ], colWidths=[9.9*cm, 9.9*cm])
        data_info.setStyle(TableStyle([
            ('BOX', (0, 0), (-1, -1), 0.6, colors.HexColor('#fed7aa')),
            ('INNERGRID', (0, 0), (-1, -1), 0.35, colors.HexColor('#fed7aa')),
            ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#fffaf5')),
            ('LEFTPADDING', (0, 0), (-1, -1), 5),
            ('RIGHTPADDING', (0, 0), (-1, -1), 5),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        elements.append(data_info)
        elements.append(Spacer(1, 0.25*cm))

        table_data = [[
            Paragraph("<b>Tipo de Control</b>", head_style),
            Paragraph("<b>Frecuencia</b>", head_style),
            Paragraph("<b>Criterio de Aceptacion</b>", head_style),
            Paragraph("<b>Aprueba?</b>", head_style),
            Paragraph("<b>Observacion</b>", head_style),
        ]]

        for item in detalle_items:
            tipo_text = f"<b>{item['n']}- {item['tipo']}:</b><br/>{item['detalle']}"
            table_data.append([
                Paragraph(tipo_text, base_style),
                Paragraph(item['frecuencia'], base_style),
                Paragraph(item['criterio'], base_style),
                Paragraph(f"<b>{item['estado']}</b>", ParagraphStyle('EstadoRecep', parent=base_style, alignment=1, fontName='Helvetica-Bold')),
                Paragraph(item['observacion'] or "", base_style),
            ])

        control_table = Table(table_data, colWidths=[8.9*cm, 2.0*cm, 3.9*cm, 2.9*cm, 2.1*cm], repeatRows=1)
        control_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f97316')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('GRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#cbd5e1')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fff7ed')]),
        ]))
        elements.append(control_table)

        elements.append(Spacer(1, 0.35*cm))
        firma_cell_content = []
        if firma_path_responsable:
            firma_img = Image(firma_path_responsable)
            max_w = 5.4 * cm
            max_h = 1.8 * cm
            escala = min(max_w / float(firma_img.drawWidth), max_h / float(firma_img.drawHeight), 1.0)
            firma_img.drawWidth = firma_img.drawWidth * escala
            firma_img.drawHeight = firma_img.drawHeight * escala
            firma_cell_content.append(firma_img)
        firma_cell_content.append(Paragraph(f"<b>{html_lib.escape(responsable)}</b>", ParagraphStyle('FirmaRespNombre', parent=styles['Normal'], alignment=1, fontSize=9, textColor=colors.HexColor('#111827'))))

        firma_table = Table([
            ["", firma_cell_content, ""],
            ["", Paragraph("<b>Firma Responsable</b>", ParagraphStyle('FirmaResp', parent=styles['Normal'], alignment=1, fontSize=9, textColor=colors.HexColor('#333333'))), ""],
        ], colWidths=[6.2*cm, 7.4*cm, 6.2*cm])
        firma_table.setStyle(TableStyle([
            ('LINEABOVE', (1, 1), (1, 1), 1, colors.HexColor('#333333')),
            ('ALIGN', (1, 0), (1, 0), 'CENTER'),
            ('VALIGN', (1, 0), (1, 0), 'BOTTOM'),
            ('TOPPADDING', (1, 1), (1, 1), 8),
            ('BOTTOMPADDING', (1, 1), (1, 1), 0),
            ('LEFTPADDING', (0, 0), (-1, -1), 0),
            ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ]))
        elements.append(firma_table)

        doc.build(elements)
        pdf_buffer.seek(0)

        filename = f"Recepcion_{obra}_{fecha}.pdf".replace(" ", "_").replace("/", "-")
        _guardar_pdf_databook(obra, "calidad_recepcion", filename, pdf_buffer.getvalue())
        pdf_buffer.seek(0)
        return send_file(
            pdf_buffer,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=filename
        )

    obras = db.execute("""
        SELECT DISTINCT obra
        FROM ordenes_trabajo
                WHERE fecha_cierre IS NULL
                    AND obra IS NOT NULL AND TRIM(obra) <> ''
        ORDER BY obra ASC
    """).fetchall()
    
    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body { font-family: Arial; padding: 15px; background: #f4f4f4; }
    h2 { color: #333; border-bottom: 3px solid #4facfe; padding-bottom: 10px; }
    .btn { background: #667eea; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; }
    form { background: white; padding: 20px; border-radius: 5px; max-width: 1200px; margin: 20px 0; }
    input, select, textarea { width: 100%; padding: 10px; margin: 8px 0; border: 1px solid #ddd; border-radius: 4px; box-sizing: border-box; }
    label { display: block; font-weight: bold; margin-top: 15px; }
    button { width: 100%; padding: 12px; background: #fb7185; color: white; border: none; border-radius: 4px; cursor: pointer; margin-top: 20px; font-weight: bold; }
    button:hover { background: #f43f5e; }
    .items-table { width: 100%; border-collapse: collapse; background: white; margin-top: 15px; box-shadow: 0 2px 5px rgba(0,0,0,0.08); }
    .items-table th, .items-table td { padding: 10px; border: 1px solid #ddd; text-align: left; vertical-align: top; }
    .items-table th { background: #e5e7eb; color: #111827; text-align: center; }
    .items-table td:nth-child(2), .items-table td:nth-child(3), .items-table td:nth-child(4) { text-align: center; }
    </style>
    </head>
    <body>
    <a href="/modulo/calidad" class="btn">⬅️ Volver</a>
    <h2>📋 Control Recepción de Materiales</h2>
    <p style="background:#e3f2fd; color:#0d47a1; padding:10px; border-radius:5px;"><b>Estados:</b> Conforme &nbsp; | &nbsp; No conforme &nbsp; | &nbsp; No aplica</p>
    
    <form method="post">
        <label>Obra:</label>
        <select name="obra" required>
            <option value="">Seleccionar obra...</option>
    """
    
    for obra in obras:
        html += f'<option value="{obra[0]}">{obra[0]}</option>'
    
    html += """
        </select>

        <label>Proveedor:</label>
        <input type="text" name="proveedor" placeholder="Nombre del proveedor" required>

        <label>Remito asociado:</label>
        <input type="text" name="remito_asociado" placeholder="Ej: REM-000123" required>

        <label>Responsable:</label>
        <select name="responsable" id="responsable_select" required>
            <option value="">-- Seleccionar responsable --</option>
            {opciones_responsables}
        </select>

        <label>Firma digital:</label>
        <input type="text" id="firma_digital_input" name="firma_digital" placeholder="Se completa automaticamente al seleccionar responsable" readonly required>
        <img id="firma_preview" src="" alt="Firma Responsable" style="display:none; margin-top:8px; max-width:280px; border:1px solid #ddd; border-radius:6px; background:white; padding:6px;" onerror="this.style.display='none';">

        <table class="items-table">
            <tr>
                <th style="width:45%;">Tipo de Control</th>
                <th style="width:12%;">Frecuencia</th>
                <th style="width:18%;">Criterio de Aceptación</th>
                <th style="width:12%;">Aprueba?</th>
                <th style="width:13%;">Observación</th>
            </tr>
    """

    for item in CONTROL_RECEPCION_ITEMS:
        html += f"""
        <tr>
            <td><b>{item['n']}- {item['tipo']}:</b><br>{item['detalle']}</td>
            <td>{item['frecuencia']}</td>
            <td>{item['criterio']}</td>
            <td>
                <select name="estado_{item['n']}" required>
                    <option value="">Seleccionar...</option>
                    <option value="CONFORME">Conforme</option>
                    <option value="NO CONFORME">No conforme</option>
                    <option value="NO APLICA">No aplica</option>
                </select>
            </td>
            <td><textarea name="observacion_{item['n']}" rows="2" placeholder="Observación..."></textarea></td>
        </tr>
        """

    html += """
        </table>

        <label>Fecha de Recepción:</label>
        <input type="date" name="fecha" required>

        <button type="submit">📄 Generar PDF Recepción</button>
    </form>
    <script>
    (function() {
        const responsableSel = document.getElementById('responsable_select');
        const firmaInput = document.getElementById('firma_digital_input');
        const firmaPreview = document.getElementById('firma_preview');
        const firmasResponsables = {json.dumps(firmas_responsables, ensure_ascii=False)};
        const imagenesResponsables = {json.dumps(imagenes_responsables, ensure_ascii=False)};
        if (!responsableSel || !firmaInput) return;

        function syncResponsable() {
            const responsable = responsableSel.value || '';
            const firma = firmasResponsables[responsable] || '';
            const firmaUrl = imagenesResponsables[responsable] || '';
            firmaInput.value = firma;
            firmaInput.readOnly = true;
            if (firmaPreview) {
                if (firmaUrl) {
                    firmaPreview.src = firmaUrl;
                    firmaPreview.style.display = 'block';
                } else {
                    firmaPreview.style.display = 'none';
                }
            }
        }

        responsableSel.addEventListener('change', syncResponsable);
        syncResponsable();
    })();
    </script>
    </body>
    </html>
    """
    html = html.replace("{opciones_responsables}", opciones_responsables)
    html = html.replace("{json.dumps(firmas_responsables, ensure_ascii=False)}", json.dumps(firmas_responsables, ensure_ascii=False))
    html = html.replace("{json.dumps(imagenes_responsables, ensure_ascii=False)}", json.dumps(imagenes_responsables, ensure_ascii=False))
    return html

# ======================
# SUB-MÓDULO CONTROL DE DESPACHO
# ======================
@app.route("/modulo/calidad/despacho", methods=["GET", "POST"])
def calidad_despacho():
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Image, Paragraph, PageBreak, Spacer
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import mm, cm
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from io import BytesIO
    import os
    
    db = get_db()
    responsables_control = _obtener_responsables_control(db)
    firmas_responsables = {k: v.get("firma", "") for k, v in responsables_control.items()}
    imagenes_responsables = {k: v.get("firma_url", "") for k, v in responsables_control.items()}
    opciones_responsables = "".join(
        f'<option value="{html_lib.escape(nombre)}">{html_lib.escape(nombre)}</option>'
        for nombre in sorted(responsables_control.keys(), key=lambda x: x.lower())
    )
    
    if request.method == "POST":
        obra = (request.form.get("obra") or "").strip()
        responsable = (request.form.get("responsable") or "").strip()
        firma_form = (request.form.get("firma_digital") or "").strip()
        remito_asociado = (request.form.get("remito_asociado") or "").strip()
        fecha = request.form.get("fecha")
        detalle_items = []
        conteo = {"CONFORME": 0, "NO CONFORME": 0, "NO APLICA": 0}
        
        if not all([obra, responsable, remito_asociado, fecha]):
            return "Faltan datos requeridos", 400

        if responsable not in firmas_responsables:
            return "Seleccioná un responsable válido", 400

        firma_digital = firmas_responsables.get(responsable, "")
        if not firma_digital or firma_form != firma_digital:
            return "La firma es obligatoria y se completa automáticamente al seleccionar responsable", 400

        firma_path_responsable = _ruta_firma_responsable(responsables_control, responsable)

        for index, item_label in enumerate(CONTROL_DESPACHO_ITEMS, start=1):
            estado = (request.form.get(f"estado_{index}") or "").strip().upper()
            observacion = (request.form.get(f"observacion_{index}") or "").strip()
            if estado not in ("CONFORME", "NO CONFORME", "NO APLICA"):
                return f"Falta completar el estado del {item_label}", 400
            conteo[estado] += 1
            detalle_items.append({
                "item": index,
                "label": item_label,
                "estado": estado,
                "observacion": observacion,
            })

        if conteo["NO CONFORME"] > 0:
            resultado_general = "NO CONFORME"
        elif conteo["CONFORME"] > 0:
            resultado_general = "CONFORME"
        else:
            resultado_general = "NO APLICA"

        # Crear PDF directamente
        pdf_buffer = BytesIO()
        doc = SimpleDocTemplate(
            pdf_buffer,
            pagesize=letter,
            topMargin=0.3*cm,
            bottomMargin=0.5*cm,
            leftMargin=0.5*cm,
            rightMargin=0.5*cm
        )
        
        elements = []
        styles = getSampleStyleSheet()
        
        # Estilos personalizados
        title_style = ParagraphStyle(
            'TitleStyle',
            parent=styles['Heading1'],
            fontSize=18,
            textColor=colors.HexColor('#000000'),
            alignment=1,
            spaceAfter=0,
            fontName='Helvetica-Bold'
        )
        
        cell_style = ParagraphStyle(
            'CellStyle',
            parent=styles['Normal'],
            fontSize=8.5,
            textColor=colors.HexColor('#333333'),
            alignment=0,
            leading=12
        )
        
        header_cell_style = ParagraphStyle(
            'HeaderCellStyle',
            parent=styles['Normal'],
            fontSize=7,
            textColor=colors.HexColor('#1f2937'),
            alignment=1,
            leading=9,
            fontName='Helvetica-Bold'
        )
        
        # ====== ENCABEZADO DESDE IMAGEN (sin modificar diseño) ======
        encabezado_path = None
        posibles_encabezados = [
            "encabezado_despacho.png",
            "ENCABEZADO_DESPACHO.png",
            "encabezado_despacho.jpg",
            "ENCABEZADO_DESPACHO.jpg",
            "encabezado_despacho.jpeg",
            "ENCABEZADO_DESPACHO.jpeg",
        ]
        for nombre_archivo in posibles_encabezados:
            candidato = os.path.join(APP_DIR, nombre_archivo)
            if os.path.exists(candidato):
                encabezado_path = candidato
                break

        if encabezado_path:
            encabezado_img = Image(encabezado_path)
            max_width = 16.5 * cm
            if encabezado_img.drawWidth > max_width:
                escala = max_width / float(encabezado_img.drawWidth)
                encabezado_img.drawWidth = encabezado_img.drawWidth * escala
                encabezado_img.drawHeight = encabezado_img.drawHeight * escala
            elements.append(encabezado_img)
        else:
            # Fallback: mantener encabezado armado en tabla si la imagen aún no existe en disco.
            logo_path = os.path.join(APP_DIR, "LOGO.png")
            logo_width = 2.5*cm
            logo_height = 2*cm

            logo_cell = ""
            if os.path.exists(logo_path):
                try:
                    logo_cell = Image(logo_path, width=logo_width, height=logo_height)
                except Exception:
                    logo_cell = Paragraph("A3", header_cell_style)
            else:
                logo_cell = Paragraph("A3", header_cell_style)

            title_cell = Paragraph("CONTROL FINAL DE DESPACHO", title_style)
            codigo_cell = Paragraph("<b>Código<br/>7-9.5</b>", header_cell_style)

            header_table_data = [[logo_cell, title_cell, codigo_cell]]
            header_table = Table(header_table_data, colWidths=[2.8*cm, 11*cm, 2.5*cm])
            header_table.setStyle(TableStyle([
                ('ALIGN', (0, 0), (0, 0), 'CENTER'),
                ('ALIGN', (1, 0), (1, 0), 'CENTER'),
                ('ALIGN', (2, 0), (2, 0), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('LEFTPADDING', (0, 0), (-1, -1), 3),
                ('RIGHTPADDING', (0, 0), (-1, -1), 3),
                ('TOPPADDING', (0, 0), (-1, -1), 2),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ]))
            elements.append(header_table)

            # Segunda fila: Revisó, Aprobó, Fecha, Revisión, Página
            info_data = [[
                Paragraph("<b>Revisó:<br/>MF</b>", header_cell_style),
                Paragraph("<b>Aprobó:<br/>GI</b>", header_cell_style),
                Paragraph("<b>Fecha:<br/>10/12/2025</b>", header_cell_style),
                Paragraph("<b>Revisión:<br/>01</b>", header_cell_style),
                Paragraph("<b>Página 1 de 1</b>", header_cell_style),
            ]]
            info_table = Table(info_data, colWidths=[2.3*cm, 2.3*cm, 2.6*cm, 2.3*cm, 2.9*cm])
            info_table.setStyle(TableStyle([
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('LEFTPADDING', (0, 0), (-1, -1), 2),
                ('RIGHTPADDING', (0, 0), (-1, -1), 2),
                ('TOPPADDING', (0, 0), (-1, -1), 2),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 0), (-1, -1), 7),
                ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
            ]))
            elements.append(info_table)
        elements.append(Spacer(1, 0.28*cm))

        # Datos básicos mejor distribuidos debajo del encabezado
        info_style = ParagraphStyle(
            'InfoDespacho',
            parent=styles['Normal'],
            fontSize=9,
            textColor=colors.HexColor('#1f2937'),
            leading=11
        )
        info_data = [
            [Paragraph(f"<b>OBRA:</b> {obra}", info_style), Paragraph(f"<b>Responsable:</b> {responsable}", info_style)],
            [Paragraph(f"<b>Remito asociado:</b> {remito_asociado}", info_style), Paragraph(f"<b>Fecha:</b> {fecha}", info_style)],
        ]
        info_table = Table(info_data, colWidths=[8.2*cm, 8.3*cm])
        info_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#fffaf5')),
            ('BOX', (0, 0), (-1, -1), 0.6, colors.HexColor('#fed7aa')),
            ('INNERGRID', (0, 0), (-1, -1), 0.35, colors.HexColor('#fed7aa')),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        elements.append(info_table)
        elements.append(Spacer(1, 0.35*cm))
        
        # Tabla de control con 10 items
        table_data = [
            [Paragraph("<b>N°</b>", header_cell_style), 
             Paragraph("<b>CONTROL DE DESPACHO</b>", header_cell_style), 
             Paragraph("<b>VERIFICA</b>", header_cell_style), 
             Paragraph("<b>OBSERVACIÓN</b>", header_cell_style)]
        ]
        
        for item in detalle_items:
            item_num = item.get("item", "")
            label = item.get("label", "")
            estado = item.get("estado", "")
            
            observacion = item.get("observacion", "")
            
            table_data.append([
                Paragraph(f"<b>{item_num}</b>", cell_style),
                Paragraph(label, cell_style),
                Paragraph(f"<b>{estado}</b>", ParagraphStyle('EstadoDespacho', parent=cell_style, alignment=1, fontName='Helvetica-Bold')),
                Paragraph(observacion or "", cell_style)
            ])
        
        # Crear tabla
        control_table = Table(table_data, colWidths=[0.7*cm, 8.8*cm, 2.9*cm, 4*cm])
        control_table.setStyle(TableStyle([
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f97316')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ('GRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#cbd5e1')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fff7ed')]),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
        ]))
        elements.append(control_table)
        elements.append(Spacer(1, 0.3*cm))
        
        # Firma fija al pie para liberar espacio útil en la hoja
        def draw_footer_signature(canvas, doc_obj):
            x_left = doc_obj.leftMargin
            x_right = doc_obj.pagesize[0] - doc_obj.rightMargin
            x_center = (x_left + x_right) / 2

            y_line = 2.2 * cm
            y_text = 1.65 * cm
            y_img = 2.55 * cm

            canvas.saveState()
            canvas.setStrokeColor(colors.HexColor('#333333'))
            canvas.setLineWidth(1)
            canvas.line(x_center - 42 * mm, y_line, x_center + 42 * mm, y_line)

            if firma_path_responsable and os.path.isfile(firma_path_responsable):
                try:
                    canvas.drawImage(
                        firma_path_responsable,
                        x_center - (52 * mm),
                        y_img,
                        width=104 * mm,
                        height=20 * mm,
                        preserveAspectRatio=True,
                        mask='auto'
                    )
                except Exception:
                    pass

            canvas.setFont('Helvetica-Bold', 9)
            canvas.setFillColor(colors.HexColor('#333333'))
            canvas.drawCentredString(x_center, y_text, f'Responsable: {responsable}')
            canvas.restoreState()
        
        # Construir PDF
        doc.build(elements, onFirstPage=draw_footer_signature, onLaterPages=draw_footer_signature)
        pdf_buffer.seek(0)
        
        # Generar nombre de archivo
        filename = f"Despacho_{obra}_{fecha}.pdf"
        filename = filename.replace(" ", "_").replace("/", "-")

        _guardar_pdf_databook(obra, "calidad_despacho", filename, pdf_buffer.getvalue())
        pdf_buffer.seek(0)
        
        return send_file(
            pdf_buffer,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=filename
        )
    
    obras = db.execute("""
        SELECT DISTINCT obra
        FROM ordenes_trabajo
                WHERE fecha_cierre IS NULL
                    AND obra IS NOT NULL AND TRIM(obra) <> ''
        ORDER BY obra ASC
    """).fetchall()
    
    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body { font-family: Arial; padding: 15px; background: #f4f4f4; }
    h2 { color: #333; border-bottom: 3px solid #fa709a; padding-bottom: 10px; }
    .btn { background: #667eea; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; }
    .top-actions { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
    .btn-remito { background: #f97316; }
    .btn-remito:hover { background: #ea580c; }
    form { background: white; padding: 20px; border-radius: 5px; max-width: 1100px; margin: 20px 0; }
    input, select, textarea { width: 100%; padding: 10px; margin: 8px 0; border: 1px solid #ddd; border-radius: 4px; box-sizing: border-box; }
    label { display: block; font-weight: bold; margin-top: 15px; }
    button { width: 100%; padding: 12px; background: #ff9800; color: white; border: none; border-radius: 4px; cursor: pointer; margin-top: 20px; font-weight: bold; font-size: 14px; }
    button:hover { background: #fb8c00; }
    .items-table { width: 100%; margin-top: 15px; box-shadow: 0 2px 5px rgba(0,0,0,0.08); }
    .items-table th { background: #fb7185; }
    .items-table td textarea { min-height: 44px; margin: 0; }
    </style>
    </head>
    <body>
    <div class="top-actions">
        <a href="/modulo/calidad" class="btn">⬅️ Volver</a>
        <a href="/modulo/remito" class="btn btn-remito">🚚 Ir a Remitos</a>
    </div>
    <h2>📦 Control Despacho</h2>
    
    <form method="post">
        <label>Obra:</label>
        <select name="obra" required>
            <option value="">Seleccionar obra...</option>
    """
    
    for obra in obras:
        html += f'<option value="{obra[0]}">{obra[0]}</option>'
    
    html += """
        </select>

        <label>Remito asociado:</label>
        <input type="text" name="remito_asociado" placeholder="Ej: R-000123" required>
        
        <label>Responsable:</label>
        <select name="responsable" id="responsable_select" required>
            <option value="">-- Seleccionar responsable --</option>
            {opciones_responsables}
        </select>

        <label>Firma digital:</label>
        <input type="text" id="firma_digital_input" name="firma_digital" placeholder="Se completa automaticamente al seleccionar responsable" readonly required>
        <img id="firma_preview" src="" alt="Firma Responsable" style="display:none; margin-top:8px; max-width:280px; border:1px solid #ddd; border-radius:6px; background:white; padding:6px;" onerror="this.style.display='none';">

        <table class="items-table">
            <tr>
                <th style="width: 80px;">N°</th>
                <th>Control</th>
                <th style="width: 220px;">Estado</th>
                <th>Observación</th>
            </tr>
    """

    for index, item_label in enumerate(CONTROL_DESPACHO_ITEMS, start=1):
        html += f"""
            <tr>
                <td><b>{index}</b></td>
                <td>{item_label}</td>
                <td>
                    <select name="estado_{index}" required>
                        <option value="">Seleccionar...</option>
                        <option value="CONFORME">Conforme</option>
                        <option value="NO CONFORME">No conforme</option>
                        <option value="NO APLICA">No aplica</option>
                    </select>
                </td>
                <td>
                    <textarea name="observacion_{index}" rows="2" placeholder="Observación del item {index}..."></textarea>
                </td>
            </tr>
        """

    html += """
        </table>
        
        <label>Fecha de Despacho:</label>
        <input type="date" name="fecha" required>
        
        <button type="submit">📄 Generar PDF Despacho</button>
    </form>
    <script>
    (function() {
        const responsableSel = document.getElementById('responsable_select');
        const firmaInput = document.getElementById('firma_digital_input');
        const firmaPreview = document.getElementById('firma_preview');
        const firmasResponsables = {json.dumps(firmas_responsables, ensure_ascii=False)};
        const imagenesResponsables = {json.dumps(imagenes_responsables, ensure_ascii=False)};
        if (!responsableSel || !firmaInput) return;

        function syncResponsable() {
            const responsable = responsableSel.value || '';
            const firma = firmasResponsables[responsable] || '';
            const firmaUrl = imagenesResponsables[responsable] || '';
            firmaInput.value = firma;
            firmaInput.readOnly = true;
            if (firmaPreview) {
                if (firmaUrl) {
                    firmaPreview.src = firmaUrl;
                    firmaPreview.style.display = 'block';
                } else {
                    firmaPreview.style.display = 'none';
                }
            }
        }

        responsableSel.addEventListener('change', syncResponsable);
        syncResponsable();
    })();
    </script>
    </body>
    </html>
    """
    html = html.replace("{opciones_responsables}", opciones_responsables)
    html = html.replace("{json.dumps(firmas_responsables, ensure_ascii=False)}", json.dumps(firmas_responsables, ensure_ascii=False))
    html = html.replace("{json.dumps(imagenes_responsables, ensure_ascii=False)}", json.dumps(imagenes_responsables, ensure_ascii=False))
    return html



# ======================
@app.route("/modulo/calidad/escaneo", methods=["GET"])
def calidad_escaneo():
    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * { box-sizing: border-box; }
    body { font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }
    h2 { color: #333; border-bottom: 3px solid #4facfe; padding-bottom: 10px; }
    .info { background: #e3f2fd; padding: 15px; border-radius: 5px; margin-bottom: 15px; color: #0d47a1; }
    .btn { display: inline-block; background: #4facfe; color: white; padding: 12px 20px;
           text-decoration: none; border-radius: 5px; margin-top: 10px; border: none; cursor: pointer; font-size: 16px; }
    .btn:hover { background: #2a7aad; }
    .submodulos-grid {
        margin-top: 22px;
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(170px, 1fr));
        gap: 12px;
        max-width: 620px;
    }
    .submodulo-btn {
        aspect-ratio: 1 / 1;
        border-radius: 10px;
        display: flex;
        flex-direction: column;
        justify-content: center;
        align-items: center;
        text-align: center;
        text-decoration: none;
        font-weight: bold;
        padding: 10px;
        box-shadow: 0 2px 6px rgba(0,0,0,0.12);
        border: 1px solid transparent;
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    .submodulo-btn:hover { transform: translateY(-2px); box-shadow: 0 6px 14px rgba(0,0,0,0.16); }
    .submodulo-btn .icono { font-size: 30px; line-height: 1; margin-bottom: 8px; }
    .submodulo-btn .texto { font-size: 14px; line-height: 1.25; }
    .submodulo-escaneo { background: #e0f2fe; color: #0c4a6e; border-color: #7dd3fc; }
    .submodulo-armado { background: #dcfce7; color: #14532d; border-color: #86efac; }
    .submodulo-pintura { background: #fef3c7; color: #78350f; border-color: #fcd34d; }
    </style>
    </head>
    <body>
    <h2>📱 Control Producción - Escaneo QR</h2>
    <div class="info"><strong>Seleccioná el sub módulo a utilizar:</strong></div>

    <div class="submodulos-grid">
        <a class="submodulo-btn submodulo-escaneo" href="/modulo/calidad/escaneo/qr">
            <span class="icono">📱</span>
            <span class="texto">ESCANEO QR</span>
        </a>
        <a class="submodulo-btn submodulo-armado" href="/modulo/calidad/escaneo/form-armado-soldadura">
            <span class="icono">🧩</span>
            <span class="texto">FORM ARMADO<br>Y SOLDADURA</span>
        </a>
        <a class="submodulo-btn submodulo-pintura" href="/modulo/calidad/escaneo/form-pintura">
            <span class="icono">🎨</span>
            <span class="texto">FORM<br>PINTURA</span>
        </a>
    </div>

    <div style="margin-top: 20px;">
        <a href="/modulo/calidad" class="btn">⬅️ Volver</a>
    </div>
    </body>
    </html>
    """
    return html


@app.route("/modulo/calidad/escaneo/qr", methods=["GET", "POST"])
def calidad_escaneo_qr():
    if request.method == "POST":
        qr_data = request.form.get("qr_code", "").strip()

        if not qr_data:
            return redirect("/modulo/calidad/escaneo/qr")

        redirect_url = construir_redirect_desde_qr(qr_data)
        if not redirect_url:
            return redirect("/modulo/calidad/escaneo/qr")

        return redirect(redirect_url)

    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <script src="https://unpkg.com/html5-qrcode"></script>
    <style>
    * { box-sizing: border-box; }
    body { font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }
    h2 { color: #333; border-bottom: 3px solid #4facfe; padding-bottom: 10px; }
    .info { background: #e3f2fd; padding: 15px; border-radius: 5px; margin-bottom: 15px; color: #0d47a1; }
    .btn { display: inline-block; background: #4facfe; color: white; padding: 12px 20px;
           text-decoration: none; border-radius: 5px; margin-top: 10px; border: none; cursor: pointer; font-size: 16px; }
    .btn:hover { background: #2a7aad; }
    .btn-secondary { background: #43e97b; }
    .btn-secondary:hover { background: #2cc96e; }
    .btn-danger { background: #f44336; }
    .btn-danger:hover { background: #da190b; }

    #qr-reader { width: 100%; max-width: 500px; margin: 20px 0; }
    #qr-reader > * { max-width: 100% !important; }

    .scanner-container { display: flex; flex-direction: column; max-width: 500px; }
    .button-group { display: flex; gap: 10px; flex-wrap: wrap; margin-top: 15px; }
    .button-group > * { flex: 1; min-width: 150px; }

    #manual-input-section {
        background: white; padding: 20px; border-radius: 5px; margin-top: 15px; display: none;
    }
    #manual-input-section.show { display: block; }

    input[type="text"] { width: 100%; padding: 15px; margin: 10px 0; border: 2px solid #4facfe;
            border-radius: 4px; font-size: 18px; }
    input[type="text"]:focus { outline: none; border-color: #2a7aad; box-shadow: 0 0 5px #4facfe; }

    .error-msg { background: #ffcccc; color: red; padding: 12px; border-radius: 5px; margin-bottom: 15px; display: none; }
    .error-msg.show { display: block; }
    .success-msg { background: #ccffcc; color: green; padding: 12px; border-radius: 5px; margin-bottom: 15px; display: none; }
    .success-msg.show { display: block; }
    </style>
    </head>
    <body>
    <h2>📱 ESCANEO QR</h2>

    <div class="info">
        <strong>📌 Instrucciones:</strong><br>
        1. Presiona "Iniciar Escaneo" para abrir la cámara<br>
        2. Apunta al código QR impreso en la pieza<br>
        3. El código se capturará automáticamente<br>
        4. Se abrirá la página de control de la pieza
    </div>

    <div class="error-msg" id="error-msg"></div>
    <div class="success-msg" id="success-msg"></div>

    <div class="scanner-container">
        <div id="qr-reader" style="display: none;"></div>

        <div class="button-group">
            <button class="btn btn-secondary" id="start-btn" onclick="startQRScan()">📷 Iniciar Escaneo</button>
            <button class="btn btn-danger" id="stop-btn" onclick="stopQRScan()" style="display: none;">⏹️ Detener</button>
            <button class="btn" onclick="toggleManualInput()">⌨️ Escaneo Manual</button>
        </div>
    </div>

    <div id="manual-input-section">
        <form method="post">
            <label><strong>Ingresa el QR manualmente:</strong></label>
            <input type="text" name="qr_code" placeholder="Escanea o pega el QR aquí..." autofocus autocomplete="off">
            <button type="submit" class="btn btn-secondary">✓ Procesar QR</button>
        </form>
    </div>

    <div style="margin-top: 20px;">
        <a href="/modulo/calidad/escaneo" class="btn">⬅️ Volver a Sub Módulos</a>
    </div>

    <script>
    let html5QrcodeScanner = null;
    let isScanning = false;

    function startQRScan() {
        if (isScanning) return;

        const qrReaderDiv = document.getElementById('qr-reader');
        const startBtn = document.getElementById('start-btn');
        const stopBtn = document.getElementById('stop-btn');
        const errorMsg = document.getElementById('error-msg');

        errorMsg.classList.remove('show');
        errorMsg.textContent = '';

        qrReaderDiv.style.display = 'block';
        startBtn.style.display = 'none';
        stopBtn.style.display = 'inline-block';
        isScanning = true;

        html5QrcodeScanner = new Html5Qrcode("qr-reader");

        Html5Qrcode.getCameras().then(devices => {
            if (devices && devices.length) {
                const cameraId = devices[0].id;
                html5QrcodeScanner.start(
                    cameraId,
                    { fps: 10, qrbox: 250 },
                    onQRCodeScanned,
                    onQRCodeError
                );
            }
        }).catch(err => {
            showError('Error al acceder a la cámara: ' + err);
            stopQRScan();
        });
    }

    function stopQRScan() {
        if (html5QrcodeScanner && isScanning) {
            html5QrcodeScanner.stop().then(() => {
                document.getElementById('qr-reader').style.display = 'none';
                document.getElementById('start-btn').style.display = 'inline-block';
                document.getElementById('stop-btn').style.display = 'none';
                isScanning = false;
            });
        }
    }

    function onQRCodeScanned(decodedText, decodedResult) {
        if (!isScanning) return;

        showSuccess('QR detectado: ' + decodedText);
        stopQRScan();
        processQR(decodedText);
    }

    function onQRCodeError(error) {
        // Silenciar errores de escaneo constantes
    }

    function processQR(qrData) {
        fetch('/procesar-qr', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ qr_code: qrData })
        })
        .then(response => response.json())
        .then(data => {
            if (data.redirect) {
                window.location.href = data.redirect;
            } else if (data.error) {
                showError(data.error);
            }
        })
        .catch(err => showError('Error al procesar QR: ' + err));
    }

    function toggleManualInput() {
        const section = document.getElementById('manual-input-section');
        section.classList.toggle('show');
        if (section.classList.contains('show')) {
            section.querySelector('input').focus();
        }
    }

    function showError(message) {
        const errorMsg = document.getElementById('error-msg');
        errorMsg.textContent = '❌ ' + message;
        errorMsg.classList.add('show');
    }

    function showSuccess(message) {
        const successMsg = document.getElementById('success-msg');
        successMsg.textContent = '✅ ' + message;
        successMsg.classList.add('show');
        setTimeout(() => successMsg.classList.remove('show'), 3000);
    }
    </script>
    </body>
    </html>
    """
    return html


def _render_form_produccion_manual(titulo, procesos_permitidos):
    obra_qs = request.args.get("obra", "").strip()
    db = get_db()
    responsables_control = _obtener_responsables_control(db)
    operarios_disponibles = _obtener_operarios_disponibles(db)
    firmas_responsables = {k: v.get("firma", "") for k, v in responsables_control.items()}
    imagenes_responsables = {k: v.get("firma_url", "") for k, v in responsables_control.items()}
    opciones_responsables = "".join(
        f'<option value="{html_lib.escape(nombre)}">{html_lib.escape(nombre)}</option>'
        for nombre in sorted(responsables_control.keys(), key=lambda x: x.lower())
    )
    opciones_operarios = "".join(
        f'<option value="{html_lib.escape(nombre)}">{html_lib.escape(nombre)}</option>'
        for nombre in operarios_disponibles
    )

    if request.method == "POST":
        pos = (request.form.get("posicion") or "").strip()
        obra = (request.form.get("obra") or "").strip()
        proceso = (request.form.get("proceso") or "").strip().upper()
        fecha = (request.form.get("fecha") or "").strip()
        operario = (request.form.get("operario") or "").strip()
        estado = (request.form.get("estado") or "").strip().upper()
        accion = (request.form.get("accion") or request.form.get("reproceso") or "").strip()
        responsable = (request.form.get("responsable") or "").strip()
        re_fecha = (request.form.get("reinspeccion_fecha") or "").strip()
        re_operador = (request.form.get("reinspeccion_operador") or "").strip()
        re_estado = (request.form.get("reinspeccion_estado") or "").strip().upper()
        re_motivo = (request.form.get("reinspeccion_motivo") or "").strip()
        re_responsable = (request.form.get("reinspeccion_responsable") or "").strip()
        re_firma_form = (request.form.get("reinspeccion_firma") or "").strip()
        firma_form = (request.form.get("firma_digital") or "").strip()
        firma_digital = firmas_responsables.get(responsable, "")

        if responsable not in firmas_responsables:
            return "Seleccioná un responsable válido", 400

        if not firma_digital or firma_form != firma_digital:
            return "La firma es obligatoria en cada escaneo", 400

        if any([re_fecha, re_operador, re_estado, re_motivo, re_responsable, re_firma_form]):
            return "La Re-inspeccion se registra solo desde el botón Re-inspeccion", 400

        re_inspeccion = ""
        estado_pieza = _estado_pieza_persistente(estado, re_inspeccion)
        firma_evento = firma_digital
        firma_reinspeccion = ""
        if all([re_fecha, re_operador, re_estado, re_responsable, re_firma_form]):
            if re_responsable not in firmas_responsables:
                return "Seleccioná un responsable válido para la Re-inspeccion", 400
            firma_reinspeccion = firmas_responsables.get(re_responsable, "")
            if not firma_reinspeccion or re_firma_form != firma_reinspeccion:
                return "La firma es obligatoria para registrar la Re-inspeccion", 400
            re_inspeccion = _agregar_ciclo_reinspeccion(
                "",
                proceso,
                re_fecha,
                re_operador,
                re_estado,
                re_motivo,
                firma_reinspeccion,
                re_responsable,
            )
            estado_pieza = _estado_pieza_persistente(estado, re_inspeccion)
            firma_evento = firma_reinspeccion or firma_digital

        if not all([pos, proceso, fecha, operario, estado]):
            return "Faltan datos requeridos", 400

        if proceso not in procesos_permitidos:
            return "Proceso no permitido para este formulario", 400

        if estado not in ("OK", "NC", "OBS", "OM"):
            return "Estado invalido", 400

        if pieza_completada(pos, obra if obra else None):
            return "La pieza ya esta completada y no admite nuevos procesos", 400

        es_valido, mensaje = validar_siguiente_proceso(pos, proceso, obra if obra else None)
        if not es_valido:
            return mensaje, 400

        existe_proceso = db.execute(
            """
            SELECT 1 FROM procesos
            WHERE posicion=?
              AND COALESCE(obra, '') = COALESCE(?, '')
              AND UPPER(TRIM(COALESCE(proceso, ''))) = ?
            LIMIT 1
            """,
            (pos, obra or "", proceso.upper())
        ).fetchone()
        if existe_proceso:
            return f"El proceso {proceso} ya está cargado para esta pieza", 400

        cursor = db.execute(
            """
            INSERT INTO procesos (posicion, obra, proceso, fecha, operario, estado, reproceso, re_inspeccion, firma_digital, estado_pieza, escaneado_qr)
            VALUES (?,?,?,?,?,?,?,?,?,?,1)
            """,
            (pos, obra or None, proceso, fecha, operario, estado, accion, re_inspeccion, firma_digital, estado_pieza)
        )
        _registrar_trazabilidad(
            db,
            cursor.lastrowid,
            pos,
            obra,
            proceso,
            estado,
            estado_pieza,
            firma_evento,
            accion,
            re_inspeccion,
            "ALTA_CONTROL_MANUAL",
        )
        db.commit()

        redirect_url = f"/pieza/{quote(pos)}"
        if obra:
            redirect_url += f"?obra={quote(obra)}"
        return redirect(redirect_url)

    opciones_proceso = ""
    for proc in procesos_permitidos:
        opciones_proceso += f'<option value="{proc}">{proc}</option>'

    html = f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {{ font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }}
    h2 {{ color: #333; border-bottom: 3px solid #4facfe; padding-bottom: 10px; }}
    form {{ background: white; padding: 18px; border-radius: 8px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); max-width: 700px; }}
    label {{ display:block; font-weight:bold; margin-top: 12px; }}
    input, select {{ width: 100%; padding: 10px; margin-top: 6px; border: 1px solid #d1d5db; border-radius: 6px; }}
    button {{ margin-top: 16px; width: 100%; padding: 12px; border: none; border-radius: 6px; font-weight: bold; color: white; background: #16a34a; cursor: pointer; }}
    .btn-volver {{ display:inline-block; margin-top: 12px; text-decoration:none; background:#667eea; color:white; padding:10px 15px; border-radius:6px; }}
    .info {{ background:#e3f2fd; color:#1e3a8a; padding:12px; border-radius:6px; margin-bottom:12px; }}
    </style>
    </head>
    <body>
    <h2>{titulo}</h2>
    <div class="info">Completá el formulario manual para registrar el proceso de producción.</div>
    <form method="post">
        <label>Posición de pieza</label>
        <input type="text" name="posicion" required>

        <label>Obra (opcional)</label>
        <input type="text" name="obra" value="{obra_qs}">

        <label>Proceso</label>
        <select name="proceso" required>
            {opciones_proceso}
        </select>

        <label>Fecha</label>
        <input type="date" name="fecha" required>

        <label>Operario</label>
        <select name="operario" required>
            <option value="">-- Seleccionar operario --</option>
            {opciones_operarios}
        </select>

        <label>Estado</label>
        <select name="estado" required>
            <option value="OK">OK (APROBADO)</option>
            <option value="NC">NC (No conformidad)</option>
            <option value="OBS">OBS (Observacion)</option>
            <option value="OM">OM (Oportunidad de mejora)</option>
        </select>

        <label>Accion</label>
        <input type="text" name="accion" placeholder="Detalle de accion (si aplica)">

        <label>Responsable</label>
        <select name="responsable" id="responsable_select" required>
            <option value="">-- Seleccionar responsable --</option>
            {opciones_responsables}
        </select>

        <label>Firma (digital)</label>
        <input type="text" id="firma_digital_input" name="firma_digital" placeholder="Se completa automaticamente al seleccionar responsable" readonly>
        <img id="firma_ok_preview" src="" alt="Firma" style="display:none; margin-top:8px; max-width:280px; border:1px solid #ddd; border-radius:6px; background:white; padding:6px;" onerror="this.style.display='none';">

        <div id="reinspeccion_block" style="margin-top:12px; background:#fff7ed; border:1px solid #fdba74; border-radius:6px; padding:10px;">
            <b>Re-inspeccion (solo desde botón Re-inspeccion)</b><br>
            <div class="form-group">
                <label>Fecha:</label>
                <input type="date" id="reinspeccion_fecha" name="reinspeccion_fecha">
            </div>
            <div class="form-group">
                <label>Operario:</label>
                <select id="reinspeccion_operador" name="reinspeccion_operador">
                    <option value="">-- Seleccionar operario --</option>
                    {opciones_operarios}
                </select>
            </div>
            <div class="form-group">
                <label>Responsable:</label>
                <select id="reinspeccion_responsable" name="reinspeccion_responsable">
                    <option value="">-- Seleccionar responsable --</option>
                    {opciones_responsables}
                </select>
            </div>
            <div class="form-group">
                <label>Firma re-inspeccion:</label>
                <input type="text" id="reinspeccion_firma" name="reinspeccion_firma" placeholder="Se completa automaticamente al seleccionar responsable" readonly>
                <img id="reinspeccion_firma_ok_preview" src="" alt="Firma Re-inspeccion" style="display:none; margin-top:8px; max-width:280px; border:1px solid #ddd; border-radius:6px; background:white; padding:6px;" onerror="this.style.display='none';">
            </div>
            <div class="form-group">
                <label>Estado:</label>
                <select id="reinspeccion_estado" name="reinspeccion_estado">
                    <option value="">-- Seleccionar --</option>
                    <option value="OK">OK (APROBADO)</option>
                    <option value="NC">NC (No conformidad)</option>
                    <option value="OBS">OBS (Observacion)</option>
                    <option value="OM">OM (Oportunidad de mejora)</option>
                </select>
            </div>
            <div class="form-group">
                <label>Motivo (si corresponde):</label>
                <input type="text" id="reinspeccion_motivo" name="reinspeccion_motivo" placeholder="Motivo del resultado de re-inspeccion">
            </div>
        </div>

        <button type="submit">💾 Guardar Proceso</button>
    </form>

    <a class="btn-volver" href="/modulo/calidad/escaneo">⬅️ Volver a Escaneo QR</a>
    <script>
    (function() {{
        const sel = document.querySelector('select[name="estado"]');
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
        if (!sel || !firmaInput || !responsableSel) return;

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

        function syncFormulario() {{
            setReinspeccionActiva(false);
            syncResponsable();
            syncReinspeccionResponsable();
        }}

        responsableSel.addEventListener('change', syncResponsable);
        if (reinspResponsableSel) reinspResponsableSel.addEventListener('change', syncReinspeccionResponsable);
        sel.addEventListener('change', syncFormulario);
        syncResponsable();
        syncReinspeccionResponsable();
        syncFormulario();
    }})();
    </script>
    </body>
    </html>
    """
    return html


@app.route("/modulo/calidad/escaneo/form-armado-soldadura", methods=["GET", "POST"])
def calidad_escaneo_form_armado_soldadura():
    from datetime import date
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

    db = get_db()
    responsables_control = _obtener_responsables_control(db)
    responsable_por_firma = {
        str(data.get("firma", "")).strip().lower(): nombre
        for nombre, data in responsables_control.items()
        if str(data.get("firma", "")).strip()
    }
    imagen_por_firma = {
        str(data.get("firma", "")).strip().lower(): str(data.get("firma_url", "")).strip()
        for nombre, data in responsables_control.items()
        if str(data.get("firma", "")).strip() and str(data.get("firma_url", "")).strip()
    }

    def _pdf_firma_cell_as(firma_txt):
        """Devuelve Image de ReportLab si existe archivo, sino Paragraph."""
        if not firma_txt:
            return Paragraph("-", base_style)
        url = imagen_por_firma.get(firma_txt.lower(), "")
        if url and "/firma-supervisor/" in url:
            from urllib.parse import unquote as _uq
            archivo = _uq(url.rsplit("/", 1)[-1])
            ruta = os.path.join(FIRMAS_EMPLEADOS_DIR, archivo)
            if os.path.isfile(ruta):
                try:
                    img = Image(ruta)
                    max_w, max_h = 2.0 * cm, 0.9 * cm
                    escala = min(max_w / float(img.drawWidth), max_h / float(img.drawHeight), 1.0)
                    img.drawWidth *= escala
                    img.drawHeight *= escala
                    return img
                except Exception:
                    pass
        return Paragraph(firma_txt or "-", base_style)

    def obtener_avance_obra(obra_sel):
        if not obra_sel:
            return []

        # Asegura metadatos completos para todas las piezas de la obra seleccionada.
        _completar_metadatos_por_obra_pos(db, obra_sel)

        # Mapa de metadatos por posicion: usa cualquier registro de la obra
        # (mismo criterio que en vistas de pieza donde los datos vienen del QR).
        meta_rows = db.execute(
            """
            SELECT posicion, cantidad, perfil
            FROM procesos
            WHERE obra = ?
                            AND EXISTS (
                                        SELECT 1 FROM ordenes_trabajo ot
                                        WHERE TRIM(COALESCE(ot.obra, '')) = TRIM(COALESCE(procesos.obra, ''))
                                            AND ot.fecha_cierre IS NULL
                            )
            ORDER BY id DESC
            """,
            (obra_sel,)
        ).fetchall()
        meta_por_pos = {}
        for pos_meta, cantidad_meta, perfil_meta in meta_rows:
            pos_meta_key = (pos_meta or "").strip()
            if not pos_meta_key:
                continue
            if pos_meta_key not in meta_por_pos:
                meta_por_pos[pos_meta_key] = {
                    "cantidad": "",
                    "perfil": "",
                }
            if not meta_por_pos[pos_meta_key]["cantidad"] and cantidad_meta not in (None, ""):
                meta_por_pos[pos_meta_key]["cantidad"] = str(cantidad_meta)
            if not meta_por_pos[pos_meta_key]["perfil"] and perfil_meta not in (None, ""):
                meta_por_pos[pos_meta_key]["perfil"] = str(perfil_meta)

        rows = db.execute(
            """
                                                                                                                                                                                                SELECT posicion, proceso, estado, fecha, firma_digital, re_inspeccion, id, cantidad, perfil
            FROM procesos
            WHERE obra = ?
              AND UPPER(TRIM(proceso)) IN ('ARMADO', 'SOLDADURA')
                            AND EXISTS (
                                        SELECT 1 FROM ordenes_trabajo ot
                                        WHERE TRIM(COALESCE(ot.obra, '')) = TRIM(COALESCE(procesos.obra, ''))
                                            AND ot.fecha_cierre IS NULL
                            )
            ORDER BY id DESC
            """,
            (obra_sel,)
        ).fetchall()

        avance = {}
        for pos, proceso, estado, fecha_reg, firma_digital, re_inspeccion, _, cantidad, perfil in rows:
            pos_key = (pos or "").strip()
            if not pos_key:
                continue

            proc_key = (proceso or "").strip().upper()
            if pos_key not in avance:
                avance[pos_key] = {
                    "posicion": pos_key,
                    "cantidad": "",
                    "perfil": "",
                    "armado": "",
                    "armado_fecha": "",
                    "armado_responsable": "",
                    "armado_firma_digital": "",
                    "armado_firma_url": "",
                    "armado_reinspeccion": "",
                    "soldadura": "",
                    "soldadura_fecha": "",
                    "soldadura_responsable": "",
                    "soldadura_firma_digital": "",
                    "soldadura_firma_url": "",
                    "soldadura_reinspeccion": "",
                }

            if not avance[pos_key]["cantidad"] and cantidad not in (None, ""):
                avance[pos_key]["cantidad"] = str(cantidad)
            if not avance[pos_key]["perfil"] and perfil not in (None, ""):
                avance[pos_key]["perfil"] = str(perfil)

            # Fallback: completar desde cualquier fila de la misma posicion/obra (QR/import).
            meta = meta_por_pos.get(pos_key, {})
            if not avance[pos_key]["cantidad"] and meta.get("cantidad"):
                avance[pos_key]["cantidad"] = meta.get("cantidad", "")
            if not avance[pos_key]["perfil"] and meta.get("perfil"):
                avance[pos_key]["perfil"] = meta.get("perfil", "")
            if proc_key == "ARMADO" and not avance[pos_key]["armado"]:
                avance[pos_key]["armado"] = (estado or "").strip().upper()
                avance[pos_key]["armado_fecha"] = (fecha_reg or "").strip()
                firma_txt = (firma_digital or "").strip()
                avance[pos_key]["armado_firma_digital"] = firma_txt
                avance[pos_key]["armado_responsable"] = responsable_por_firma.get(firma_txt.lower(), "-") if firma_txt else "-"
                avance[pos_key]["armado_firma_url"] = imagen_por_firma.get(firma_txt.lower(), "") if firma_txt else ""
                avance[pos_key]["armado_reinspeccion"] = (re_inspeccion or "").strip()
            elif proc_key == "SOLDADURA" and not avance[pos_key]["soldadura"]:
                avance[pos_key]["soldadura"] = (estado or "").strip().upper()
                avance[pos_key]["soldadura_fecha"] = (fecha_reg or "").strip()
                firma_txt = (firma_digital or "").strip()
                avance[pos_key]["soldadura_firma_digital"] = firma_txt
                avance[pos_key]["soldadura_responsable"] = responsable_por_firma.get(firma_txt.lower(), "-") if firma_txt else "-"
                avance[pos_key]["soldadura_firma_url"] = imagen_por_firma.get(firma_txt.lower(), "") if firma_txt else ""
                avance[pos_key]["soldadura_reinspeccion"] = (re_inspeccion or "").strip()

        return sorted(avance.values(), key=lambda x: x["posicion"])

    obras_db = db.execute(
        """
        SELECT DISTINCT TRIM(obra) AS obra
        FROM procesos
                WHERE obra IS NOT NULL
                    AND TRIM(obra) <> ''
                    AND EXISTS (
                                SELECT 1 FROM ordenes_trabajo ot
                                WHERE TRIM(COALESCE(ot.obra, '')) = TRIM(COALESCE(procesos.obra, ''))
                                    AND ot.fecha_cierre IS NULL
                    )
        ORDER BY obra ASC
        """
    ).fetchall()
    obras = [r[0] for r in obras_db]

    obra = (request.values.get("obra") or "").strip()
    rows_avance = obtener_avance_obra(obra)

    if request.method == "POST" and (request.form.get("accion") or "").strip().lower() == "pdf":
        if not obra:
            return redirect("/modulo/calidad/escaneo/form-armado-soldadura?mensaje=" + quote("⚠️ Seleccioná una obra"))

        pdf_buffer = BytesIO()
        doc = SimpleDocTemplate(
            pdf_buffer,
            pagesize=letter,
            topMargin=0.4*cm,
            bottomMargin=0.6*cm,
            leftMargin=0.5*cm,
            rightMargin=0.5*cm
        )

        elements = []
        styles = getSampleStyleSheet()
        base_style = ParagraphStyle('BaseAS', parent=styles['Normal'], fontSize=7.4, leading=8.6, textColor=colors.HexColor('#333333'))
        head_style = ParagraphStyle('HeadAS', parent=styles['Normal'], fontSize=7.2, leading=8.2, alignment=1, fontName='Helvetica-Bold', textColor=colors.white)

        encabezado_path = None
        candidatos = [
            os.path.join(APP_DIR, "ENCABEZADO_ARMADO Y SOLDADURA.png"),
            os.path.join(APP_DIR, "ENCABEZADO_ARMADO Y SOLDADURA.jpg"),
            os.path.join(APP_DIR, "ENCABEZADO_ARMADO Y SOLDADURA.jpeg"),
            os.path.join(APP_DIR, "ENCABEZADO_ARMADO_SOLDADURA.png"),
            os.path.join(APP_DIR, "ENCABEZADO_ARMADO_SOLDADURA.jpg"),
            os.path.join(APP_DIR, "ENCABEZADO_ARMADO_SOLDADURA.jpeg"),
            os.path.join(APP_DIR, "encabezado_armado_soldadura.png"),
        ]
        for c in candidatos:
            if os.path.exists(c):
                encabezado_path = c
                break

        if encabezado_path:
            head_img = Image(encabezado_path)
            max_width = 19.8 * cm
            if head_img.drawWidth > max_width:
                ratio = max_width / float(head_img.drawWidth)
                head_img.drawWidth *= ratio
                head_img.drawHeight *= ratio
            elements.append(head_img)
        else:
            logo_path = os.path.join(APP_DIR, "LOGO.png")
            logo_cell = Image(logo_path, width=2.8*cm, height=2.2*cm) if os.path.exists(logo_path) else Paragraph("A3", base_style)
            title_cell = Paragraph("<b>CONTROL DE ARMADO Y SOLDADURA</b>", ParagraphStyle('TAS', parent=styles['Heading2'], alignment=1, textColor=colors.HexColor('#111827')))
            code_cell = Paragraph("<b>Código<br/>7-9.3</b>", ParagraphStyle('CAS', parent=base_style, alignment=1, fontName='Helvetica-Bold'))
            header = Table([[logo_cell, title_cell, code_cell]], colWidths=[4.8*cm, 12.0*cm, 3.0*cm])
            header.setStyle(TableStyle([
                ('GRID', (0,0), (-1,-1), 0.8, colors.HexColor('#111827')),
                ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
                ('ALIGN', (0,0), (-1,-1), 'CENTER'),
                ('TOPPADDING', (0,0), (-1,-1), 4),
                ('BOTTOMPADDING', (0,0), (-1,-1), 4),
            ]))
            elements.append(header)

        elements.append(Spacer(1, 0.2*cm))

        info = Table([
            [Paragraph(f"<b>Obra:</b> {obra}", base_style), Paragraph(f"<b>Fecha:</b> {date.today().isoformat()}", base_style)],
        ], colWidths=[9.9*cm, 9.9*cm])
        info.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,-1), colors.HexColor('#fff7ed')),
            ('BOX', (0,0), (-1,-1), 0.6, colors.HexColor('#fdba74')),
            ('INNERGRID', (0,0), (-1,-1), 0.35, colors.HexColor('#fed7aa')),
            ('LEFTPADDING', (0,0), (-1,-1), 6),
            ('RIGHTPADDING', (0,0), (-1,-1), 6),
            ('TOPPADDING', (0,0), (-1,-1), 5),
            ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ]))
        elements.append(info)
        elements.append(Spacer(1, 0.25*cm))

        table_data = [[
            Paragraph("<b>Posición</b>", head_style),
            Paragraph("<b>Cantidad</b>", head_style),
            Paragraph("<b>Perfil</b>", head_style),
            Paragraph("<b>Estado Armado</b>", head_style),
            Paragraph("<b>Fecha Armado</b>", head_style),
            Paragraph("<b>Responsable Armado</b>", head_style),
            Paragraph("<b>Firma digital Armado</b>", head_style),
            Paragraph("<b>Estado Soldadura</b>", head_style),
            Paragraph("<b>Fecha Soldadura</b>", head_style),
            Paragraph("<b>Responsable Soldadura</b>", head_style),
            Paragraph("<b>Firma digital Soldadura</b>", head_style),
        ]]
        for r in rows_avance:
            table_data.append([
                Paragraph(r["posicion"], base_style),
                Paragraph(_format_cantidad_1_decimal(r["cantidad"]), base_style),
                Paragraph(r["perfil"] or "-", base_style),
                Paragraph(r["armado"] or "PENDIENTE", base_style),
                Paragraph(r["armado_fecha"] or "-", base_style),
                Paragraph(r["armado_responsable"] or "-", base_style),
                _pdf_firma_cell_as(r["armado_firma_digital"]),
                Paragraph(r["soldadura"] or "PENDIENTE", base_style),
                Paragraph(r["soldadura_fecha"] or "-", base_style),
                Paragraph(r["soldadura_responsable"] or "-", base_style),
                _pdf_firma_cell_as(r["soldadura_firma_digital"]),
            ])

        # Total ancho: 19.8cm (igual que área útil del PDF)
        t = Table(table_data, colWidths=[1.5*cm, 1.1*cm, 2.3*cm, 1.45*cm, 1.75*cm, 1.9*cm, 2.35*cm, 1.45*cm, 1.75*cm, 1.9*cm, 2.35*cm], repeatRows=1)
        t.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor('#f97316')),
            ('TEXTCOLOR', (0,0), (-1,0), colors.white),
            ('GRID', (0,0), (-1,-1), 0.4, colors.HexColor('#cbd5e1')),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [colors.white, colors.HexColor('#fff7ed')]),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('LEFTPADDING', (0,0), (-1,-1), 4),
            ('RIGHTPADDING', (0,0), (-1,-1), 4),
            ('TOPPADDING', (0,0), (-1,-1), 4),
            ('BOTTOMPADDING', (0,0), (-1,-1), 4),
        ]))
        elements.append(t)
        elements.append(Spacer(1, 0.3*cm))

        reinsp_head = ParagraphStyle('ReinspHeadAS', parent=styles['Normal'], fontSize=7.0, leading=8.0, alignment=1, fontName='Helvetica-Bold', textColor=colors.white)
        reinsp_sub_head = ParagraphStyle('ReinspSubHeadAS', parent=styles['Normal'], fontSize=6.7, leading=7.8, alignment=1, fontName='Helvetica-Bold', textColor=colors.HexColor('#7c2d12'))
        reinsp_rows = [[
            Paragraph("<b>Posición</b>", reinsp_head),
            Paragraph("<b>🔩 ARMADO</b>", reinsp_head), "", "", "", "",
            Paragraph("<b>⚡ SOLDADURA</b>", reinsp_head), "", "", "", "",
        ], [
            Paragraph("", reinsp_sub_head),
            Paragraph("<b>Estado</b>", reinsp_sub_head),
            Paragraph("<b>Fecha</b>", reinsp_sub_head),
            Paragraph("<b>Acción correctiva</b>", reinsp_sub_head),
            Paragraph("<b>Responsable</b>", reinsp_sub_head),
            Paragraph("<b>Firma</b>", reinsp_sub_head),
            Paragraph("<b>Estado</b>", reinsp_sub_head),
            Paragraph("<b>Fecha</b>", reinsp_sub_head),
            Paragraph("<b>Acción correctiva</b>", reinsp_sub_head),
            Paragraph("<b>Responsable</b>", reinsp_sub_head),
            Paragraph("<b>Firma</b>", reinsp_sub_head),
        ]]

        for r in rows_avance:
            estado_arm = (r.get("armado") or "").strip().upper()
            estado_sold = (r.get("soldadura") or "").strip().upper()
            es_nc_arm = estado_arm in ("NC", "NO CONFORME", "NO CONFORMIDAD")
            es_nc_sold = estado_sold in ("NC", "NO CONFORME", "NO CONFORMIDAD")
            if not (es_nc_arm or es_nc_sold):
                continue

            ciclos_arm = _extraer_ciclos_reinspeccion(r.get("armado_reinspeccion") or "") if es_nc_arm else []
            ciclos_sold = _extraer_ciclos_reinspeccion(r.get("soldadura_reinspeccion") or "") if es_nc_sold else []
            ult_arm = ciclos_arm[-1] if ciclos_arm else {}
            ult_sold = ciclos_sold[-1] if ciclos_sold else {}

            reinsp_rows.append([
                Paragraph(r.get("posicion") or "-", base_style),
                Paragraph((ult_arm.get("estado") or "-").strip().upper() if es_nc_arm else "-", base_style),
                Paragraph((ult_arm.get("fecha") or "-").strip() if es_nc_arm else "-", base_style),
                Paragraph((ult_arm.get("motivo") or "-").strip() if es_nc_arm else "-", base_style),
                Paragraph((ult_arm.get("responsable") or "-").strip() if es_nc_arm else "-", base_style),
                _pdf_firma_cell_as((ult_arm.get("firma") or "").strip()) if es_nc_arm else Paragraph("-", base_style),
                Paragraph((ult_sold.get("estado") or "-").strip().upper() if es_nc_sold else "-", base_style),
                Paragraph((ult_sold.get("fecha") or "-").strip() if es_nc_sold else "-", base_style),
                Paragraph((ult_sold.get("motivo") or "-").strip() if es_nc_sold else "-", base_style),
                Paragraph((ult_sold.get("responsable") or "-").strip() if es_nc_sold else "-", base_style),
                _pdf_firma_cell_as((ult_sold.get("firma") or "").strip()) if es_nc_sold else Paragraph("-", base_style),
            ])

        if len(reinsp_rows) > 2:
            elements.append(Paragraph("<b>Re-inspección (solo piezas NC)</b>", ParagraphStyle('ReinspTitleAS', parent=styles['Normal'], fontSize=9, textColor=colors.HexColor('#9a3412'))))
            elements.append(Spacer(1, 0.1*cm))
            # Total ancho: 19.8cm (igual a tabla principal)
            rt = Table(reinsp_rows, colWidths=[1.5*cm, 1.2*cm, 1.55*cm, 2.6*cm, 1.55*cm, 2.25*cm, 1.2*cm, 1.55*cm, 2.6*cm, 1.55*cm, 2.25*cm], repeatRows=2)
            rt.setStyle(TableStyle([
                ('SPAN', (1,0), (5,0)),
                ('SPAN', (6,0), (10,0)),
                ('SPAN', (0,0), (0,1)),
                ('BACKGROUND', (0,0), (10,0), colors.HexColor('#ea580c')),
                ('BACKGROUND', (1,1), (5,1), colors.HexColor('#ffedd5')),
                ('BACKGROUND', (6,1), (10,1), colors.HexColor('#fff7ed')),
                ('TEXTCOLOR', (0,0), (10,0), colors.white),
                ('GRID', (0,0), (10,-1), 0.35, colors.HexColor('#cbd5e1')),
                ('ROWBACKGROUNDS', (0,2), (10,-1), [colors.white, colors.HexColor('#fff7ed')]),
                ('VALIGN', (0,0), (10,-1), 'MIDDLE'),
                ('ALIGN', (0,0), (10,1), 'CENTER'),
                ('LEFTPADDING', (0,0), (10,-1), 3),
                ('RIGHTPADDING', (0,0), (10,-1), 3),
                ('TOPPADDING', (0,0), (10,-1), 3),
                ('BOTTOMPADDING', (0,0), (10,-1), 3),
            ]))
            elements.append(rt)

        doc.build(elements)
        pdf_buffer.seek(0)
        filename = f"Control_Armado_Soldadura_{obra}_{date.today().isoformat()}.pdf".replace(" ", "_")
        _guardar_pdf_databook(obra, "calidad_armado_soldadura", filename, pdf_buffer.getvalue())
        pdf_buffer.seek(0)
        return send_file(pdf_buffer, mimetype='application/pdf', as_attachment=True, download_name=filename)

    total = len(rows_avance)
    comp_armado = len([r for r in rows_avance if r["armado"]])
    comp_sold = len([r for r in rows_avance if r["soldadura"]])

    opciones_obras = '<option value="">-- Seleccionar obra --</option>'
    for o in obras:
        sel = 'selected' if o == obra else ''
        opciones_obras += f'<option value="{o}" {sel}>{o}</option>'

    rows_html = ""
    for r in rows_avance:
        armado_txt = r["armado"] if r["armado"] else "PENDIENTE"
        sold_txt = r["soldadura"] if r["soldadura"] else "PENDIENTE"
        armado_class = "ok" if r["armado"] else "pend"
        sold_class = "ok" if r["soldadura"] else "pend"
        rows_html += f"""
        <tr>
            <td><b>{r['posicion']}</b></td>
            <td class="td-meta">{_format_cantidad_1_decimal(r['cantidad'])}</td>
            <td class="td-meta">{r['perfil'] or '-'}</td>
            <td class="td-a">
                <div class="{armado_class}">{armado_txt}</div>
            </td>
            <td class="td-a">{r['armado_fecha'] or '-'}</td>
            <td class="td-a">{r['armado_responsable'] or '-'}</td>
            <td class="td-a">{'<img src="' + r['armado_firma_url'] + '" style="max-height:32px;max-width:95px;vertical-align:middle;border:1px solid #e5e7eb;border-radius:4px;background:#fff;padding:2px;">' if r.get('armado_firma_url') else (r['armado_firma_digital'] or '-')}</td>
            <td class="td-s">
                <div class="{sold_class}">{sold_txt}</div>
            </td>
            <td class="td-s">{r['soldadura_fecha'] or '-'}</td>
            <td class="td-s">{r['soldadura_responsable'] or '-'}</td>
            <td class="td-s">{'<img src="' + r['soldadura_firma_url'] + '" style="max-height:32px;max-width:95px;vertical-align:middle;border:1px solid #e5e7eb;border-radius:4px;background:#fff;padding:2px;">' if r.get('soldadura_firma_url') else (r['soldadura_firma_digital'] or '-')}</td>
        </tr>
        """

    def _firma_html_reinsp(firma_txt):
        firma_val = (firma_txt or "").strip()
        if not firma_val:
            return "-"
        firma_url = imagen_por_firma.get(firma_val.lower(), "")
        if firma_url:
            return f'<img src="{firma_url}" style="max-height:30px;max-width:90px;vertical-align:middle;border:1px solid #e5e7eb;border-radius:4px;background:#fff;padding:2px;">'
        return html_lib.escape(firma_val)

    reinspeccion_rows_html = ""
    for r in rows_avance:
        estado_arm = (r.get("armado") or "").strip().upper()
        estado_sold = (r.get("soldadura") or "").strip().upper()
        es_nc_arm = estado_arm in ("NC", "NO CONFORME", "NO CONFORMIDAD")
        es_nc_sold = estado_sold in ("NC", "NO CONFORME", "NO CONFORMIDAD")
        if not (es_nc_arm or es_nc_sold):
            continue

        ciclos_arm = _extraer_ciclos_reinspeccion(r.get("armado_reinspeccion") or "") if es_nc_arm else []
        ciclos_sold = _extraer_ciclos_reinspeccion(r.get("soldadura_reinspeccion") or "") if es_nc_sold else []
        ult_arm = ciclos_arm[-1] if ciclos_arm else {}
        ult_sold = ciclos_sold[-1] if ciclos_sold else {}

        arm_estado = html_lib.escape((ult_arm.get("estado") or "-").strip().upper()) if es_nc_arm else "-"
        arm_fecha = html_lib.escape((ult_arm.get("fecha") or "-").strip()) if es_nc_arm else "-"
        arm_accion = html_lib.escape((ult_arm.get("motivo") or "-").strip()) if es_nc_arm else "-"
        arm_resp = html_lib.escape((ult_arm.get("responsable") or "-").strip()) if es_nc_arm else "-"
        arm_firma = _firma_html_reinsp((ult_arm.get("firma") or "").strip()) if es_nc_arm else "-"

        sold_estado = html_lib.escape((ult_sold.get("estado") or "-").strip().upper()) if es_nc_sold else "-"
        sold_fecha = html_lib.escape((ult_sold.get("fecha") or "-").strip()) if es_nc_sold else "-"
        sold_accion = html_lib.escape((ult_sold.get("motivo") or "-").strip()) if es_nc_sold else "-"
        sold_resp = html_lib.escape((ult_sold.get("responsable") or "-").strip()) if es_nc_sold else "-"
        sold_firma = _firma_html_reinsp((ult_sold.get("firma") or "").strip()) if es_nc_sold else "-"

        reinspeccion_rows_html += f"""
        <tr>
            <td><b>{html_lib.escape(r.get('posicion') or '-')}</b></td>
            <td>{arm_estado}</td>
            <td>{arm_fecha}</td>
            <td>{arm_accion}</td>
            <td>{arm_resp}</td>
            <td>{arm_firma}</td>
            <td>{sold_estado}</td>
            <td>{sold_fecha}</td>
            <td>{sold_accion}</td>
            <td>{sold_resp}</td>
            <td>{sold_firma}</td>
        </tr>
        """

    if not reinspeccion_rows_html:
        reinspeccion_rows_html = "<tr><td colspan='11' style='text-align:center;color:#6b7280;'>No hay piezas NC con datos de re-inspección</td></tr>"

    if not rows_html:
        rows_html = "<tr><td colspan='11' style='text-align:center;color:#6b7280;'>No hay registros para la obra seleccionada</td></tr>"

    html = f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {{ font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }}
    h2 {{ color: #9a3412; border-bottom: 3px solid #f97316; padding-bottom: 10px; }}
    .box {{ background:white; border-radius:8px; padding:14px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); margin-bottom: 14px; }}
    .filtro {{ display:grid; grid-template-columns: 1fr auto; gap:10px; align-items:end; }}
    .filtro select, .filtro input {{ width:100%; padding:10px; border:1px solid #d1d5db; border-radius:6px; }}
    .btn {{ background:#f97316; color:white; border:none; padding:10px 14px; border-radius:6px; font-weight:bold; cursor:pointer; text-decoration:none; display:inline-block; }}
    .btn:hover {{ background:#ea580c; }}
    .btn-blue {{ background:#2563eb; }}
    .btn-blue:hover {{ background:#1d4ed8; }}
    .kpis {{ display:grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap:10px; margin-top: 10px; }}
    .kpi {{ background:#fff7ed; border:1px solid #fed7aa; border-radius:8px; padding:10px; }}
    .kpi .t {{ font-size:12px; color:#9a3412; }}
    .kpi .v {{ font-size:22px; color:#7c2d12; font-weight:bold; }}
    table {{ width:100%; border-collapse: collapse; background:white; table-layout: fixed; }}
    th, td {{ border-bottom:1px solid #e5e7eb; padding:9px; text-align:center; font-size:13px; }}
    th {{ background:#f97316; color:white; }}
    th.th-pos {{ background:#c2410c; }}
    th.th-armado {{ background:#ea580c; letter-spacing:0.5px; }}
    th.th-soldadura {{ background:#f97316; letter-spacing:0.5px; }}
    th.th-sub {{ font-size:11px; font-weight:600; }}
    th.th-meta {{ background:#c2410c; }}
    th.th-sub-a {{ background:#ffedd5; color:#7c2d12; }}
    th.th-sub-s {{ background:#fff7ed; color:#9a3412; }}
    td.td-meta {{ background:#fffaf5; }}
    td.td-a {{ background:#fff7ed; }}
    td.td-s {{ background:#ffedd5; }}
    td:first-child {{ text-align:left; }}
    .ok {{ color:#166534; font-weight:bold; }}
    .pend {{ color:#9ca3af; font-weight:bold; }}
    .actions {{ display:flex; gap:10px; flex-wrap:wrap; margin-top:12px; }}
    .reinsp-box {{ margin-top:16px; border:1px solid #fdba74; border-radius:8px; background:#fff7ed; padding:10px; }}
    .reinsp-title {{ margin:0 0 8px 0; color:#9a3412; font-size:15px; }}
    .reinsp-table th {{ background:#ea580c; color:white; font-size:12px; }}
    .reinsp-table td {{ font-size:12px; }}
    .reinsp-table .reinsp-group-arm {{ background:#ea580c; }}
    .reinsp-table .reinsp-group-sold {{ background:#f97316; }}
    .reinsp-table .reinsp-sub-arm {{ background:#ffedd5; color:#7c2d12; }}
    .reinsp-table .reinsp-sub-sold {{ background:#fff7ed; color:#9a3412; }}
    </style>
    </head>
    <body>
    <h2>🧩 Control de Armado y Soldadura</h2>

    <div class="box">
        <form method="get" action="/modulo/calidad/escaneo/form-armado-soldadura">
            <div class="filtro">
                <div>
                    <label><b>Filtrar por obra</b></label>
                    <select name="obra" required>
                        {opciones_obras}
                    </select>
                </div>
                <button type="submit" class="btn">Aplicar filtro</button>
            </div>
        </form>

        <div class="kpis">
            <div class="kpi"><div class="t">Piezas en formulario</div><div class="v">{total}</div></div>
            <div class="kpi"><div class="t">Armado cargado</div><div class="v">{comp_armado}</div></div>
            <div class="kpi"><div class="t">Soldadura cargada</div><div class="v">{comp_sold}</div></div>
        </div>
    </div>

    <div class="box">
        <table>
            <tr>
                <th class="th-pos" rowspan="2">Posición</th>
                <th class="th-meta" rowspan="2">Cantidad</th>
                <th class="th-meta" rowspan="2">Perfil</th>
                <th class="th-armado" colspan="4">🔩 ARMADO</th>
                <th class="th-soldadura" colspan="4">⚡ SOLDADURA</th>
            </tr>
            <tr>
                <th class="th-sub th-sub-a">Estado</th>
                <th class="th-sub th-sub-a">Fecha</th>
                <th class="th-sub th-sub-a">Responsable</th>
                <th class="th-sub th-sub-a">Firma</th>
                <th class="th-sub th-sub-s">Estado</th>
                <th class="th-sub th-sub-s">Fecha</th>
                <th class="th-sub th-sub-s">Responsable</th>
                <th class="th-sub th-sub-s">Firma</th>
            </tr>
            {rows_html}
        </table>

        <div class="reinsp-box">
            <h3 class="reinsp-title">Re-inspección (solo piezas NC)</h3>
            <table class="reinsp-table">
                <tr>
                    <th rowspan="2">Posición</th>
                    <th class="reinsp-group-arm" colspan="5">🔩 ARMADO</th>
                    <th class="reinsp-group-sold" colspan="5">⚡ SOLDADURA</th>
                </tr>
                <tr>
                    <th class="reinsp-sub-arm">Estado</th>
                    <th class="reinsp-sub-arm">Fecha</th>
                    <th class="reinsp-sub-arm">Acción correctiva</th>
                    <th class="reinsp-sub-arm">Responsable</th>
                    <th class="reinsp-sub-arm">Firma</th>
                    <th class="reinsp-sub-sold">Estado</th>
                    <th class="reinsp-sub-sold">Fecha</th>
                    <th class="reinsp-sub-sold">Acción correctiva</th>
                    <th class="reinsp-sub-sold">Responsable</th>
                    <th class="reinsp-sub-sold">Firma</th>
                </tr>
                {reinspeccion_rows_html}
            </table>
        </div>

        <form method="post" action="/modulo/calidad/escaneo/form-armado-soldadura">
            <input type="hidden" name="accion" value="pdf">
            <input type="hidden" name="obra" value="{obra}">
            <div class="actions">
                <button type="submit" class="btn">📄 Generar PDF</button>
                <a href="/modulo/calidad/escaneo" class="btn btn-blue">⬅️ Volver a Sub Módulos</a>
            </div>
        </form>
    </div>
    </body>
    </html>
    """
    return html


@app.route("/modulo/calidad/escaneo/form-pintura", methods=["GET", "POST"])
def calidad_escaneo_form_pintura():
    from datetime import date
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
    from reportlab.lib.pagesizes import landscape, letter
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle

    db = get_db()
    responsables_control = _obtener_responsables_control(db)
    firmas_responsables = {k: v.get("firma", "") for k, v in responsables_control.items()}

    def _to_float(val):
        txt = str(val or "").strip().replace(",", ".")
        if not txt:
            return 0.0
        try:
            return float(txt)
        except Exception:
            return 0.0

    def _obtener_piezas_obra(obra_sel):
        if not obra_sel:
            return []

        _completar_metadatos_por_obra_pos(db, obra_sel)

        rows = db.execute(
            """
            SELECT posicion, cantidad, perfil
            FROM procesos
            WHERE TRIM(COALESCE(obra, '')) = TRIM(?)
              AND TRIM(COALESCE(posicion, '')) <> ''
              AND EXISTS (
                    SELECT 1 FROM ordenes_trabajo ot
                    WHERE TRIM(COALESCE(ot.obra, '')) = TRIM(COALESCE(procesos.obra, ''))
                      AND ot.fecha_cierre IS NULL
              )
            ORDER BY id DESC
            """,
            (obra_sel,)
        ).fetchall()

        piezas_map = {}
        for pos, cantidad, perfil in rows:
            key = (pos or "").strip()
            if not key or key in piezas_map:
                continue
            piezas_map[key] = {
                "pieza": key,
                "cantidad": _format_cantidad_1_decimal(cantidad),
                "descripcion": str(perfil or "").strip(),
            }

        return sorted(piezas_map.values(), key=lambda x: x["pieza"])

    obras_db = db.execute(
        """
        SELECT DISTINCT TRIM(obra) AS obra
        FROM procesos
        WHERE obra IS NOT NULL AND TRIM(obra) <> ''
          AND EXISTS (
                SELECT 1 FROM ordenes_trabajo ot
                WHERE TRIM(COALESCE(ot.obra, '')) = TRIM(COALESCE(procesos.obra, ''))
                  AND ot.fecha_cierre IS NULL
          )
        ORDER BY obra ASC
        """
    ).fetchall()
    obras = [r[0] for r in obras_db]

    obra = (request.values.get("obra") or "").strip()
    piezas = _obtener_piezas_obra(obra)

    if request.method == "POST" and (request.form.get("accion") or "").strip().lower() == "pdf":
        obra = (request.form.get("obra") or "").strip()
        if not obra:
            return "Selecciona una obra", 400

        piezas_form = request.form.getlist("pieza[]")
        cantidades_form = request.form.getlist("cantidad[]")
        desc_form = request.form.getlist("descripcion[]")
        sup_estado_form = request.form.getlist("sup_estado[]")
        sup_resp_form = request.form.getlist("sup_responsable[]")
        sup_firma_form = request.form.getlist("sup_firma[]")
        mano1_form = request.form.getlist("mano1[]")
        mano2_form = request.form.getlist("mano2[]")
        mano3_form = request.form.getlist("mano3[]")
        mano4_form = request.form.getlist("mano4[]")
        espesor_form = request.form.getlist("espesor_solicitado[]")
        pint_resp_form = request.form.getlist("pintura_responsable[]")
        pint_firma_form = request.form.getlist("pintura_firma[]")

        filas_pintura = []
        total_filas = len(piezas_form)
        for i in range(total_filas):
            pieza = (piezas_form[i] if i < len(piezas_form) else "").strip()
            if not pieza:
                continue
            cantidad = (cantidades_form[i] if i < len(cantidades_form) else "").strip()
            descripcion = (desc_form[i] if i < len(desc_form) else "").strip()
            sup_estado = (sup_estado_form[i] if i < len(sup_estado_form) else "").strip().upper()
            sup_resp = (sup_resp_form[i] if i < len(sup_resp_form) else "").strip()
            sup_resp_nombre = (sup_firma_form[i] if i < len(sup_firma_form) else "").strip()  # Ahora es el nombre
            sup_firma = responsables_control.get(sup_resp_nombre, {}).get("firma", "") if sup_resp_nombre else ""
            mano1 = _to_float(mano1_form[i] if i < len(mano1_form) else "")
            mano2 = _to_float(mano2_form[i] if i < len(mano2_form) else "")
            mano3 = _to_float(mano3_form[i] if i < len(mano3_form) else "")
            mano4 = _to_float(mano4_form[i] if i < len(mano4_form) else "")
            espesor = _to_float(espesor_form[i] if i < len(espesor_form) else "")
            pint_resp = (pint_resp_form[i] if i < len(pint_resp_form) else "").strip()
            pint_resp_nombre = (pint_firma_form[i] if i < len(pint_firma_form) else "").strip()  # Ahora es el nombre
            pint_firma = responsables_control.get(pint_resp_nombre, {}).get("firma", "") if pint_resp_nombre else ""
            estado_final = "OK" if mano4 > espesor else "NO CONFORME"

            filas_pintura.append({
                "pieza": pieza,
                "cantidad": cantidad,
                "descripcion": descripcion,
                "sup_estado": sup_estado,
                "sup_resp": sup_resp,
                "sup_firma": sup_firma,
                "mano1": mano1,
                "mano2": mano2,
                "mano3": mano3,
                "mano4": mano4,
                "espesor": espesor,
                "estado_final": estado_final,
                "pint_resp": pint_resp,
                "pint_firma": pint_firma,
            })

        med_fechas = request.form.getlist("med_fecha[]")
        med_horas = request.form.getlist("med_hora[]")
        med_temps = request.form.getlist("med_temp[]")
        med_humedades = request.form.getlist("med_humedad[]")

        mediciones = []
        total_med = max(len(med_fechas), len(med_horas), len(med_temps), len(med_humedades), 4)
        for i in range(total_med):
            fecha_m = (med_fechas[i] if i < len(med_fechas) else "").strip()
            hora_m = (med_horas[i] if i < len(med_horas) else "").strip()
            temp_m = (med_temps[i] if i < len(med_temps) else "").strip()
            hum_m = (med_humedades[i] if i < len(med_humedades) else "").strip()
            if not (fecha_m or hora_m or temp_m or hum_m):
                continue
            mediciones.append({
                "mano": str(i + 1),
                "fecha": fecha_m,
                "hora": hora_m,
                "temp": temp_m,
                "humedad": hum_m,
            })

        pdf_buffer = BytesIO()
        doc = SimpleDocTemplate(
            pdf_buffer,
            pagesize=landscape(letter),
            topMargin=0.5 * cm,
            bottomMargin=0.6 * cm,
            leftMargin=0.6 * cm,
            rightMargin=0.6 * cm,
        )

        styles = getSampleStyleSheet()
        base_style = ParagraphStyle('BaseP', parent=styles['Normal'], fontSize=7.2, leading=8.4, textColor=colors.HexColor('#1f2937'))
        head_style = ParagraphStyle('HeadP', parent=styles['Normal'], fontSize=7.1, leading=8.2, alignment=1, fontName='Helvetica-Bold', textColor=colors.white)
        title_style = ParagraphStyle('TitleP', parent=styles['Heading2'], fontSize=14, textColor=colors.HexColor('#111827'), alignment=0)

        def _encabezado_pintura_path():
            candidatos = [
                os.path.join(APP_DIR, "ENCABEZAO_PINTURA.png"),
                os.path.join(APP_DIR, "ENCABEZAO_PINTURA.jpg"),
                os.path.join(APP_DIR, "ENCABEZAO_PINTURA.jpeg"),
                os.path.join(APP_DIR, "ENCABEZADO_PINTURA.png"),
                os.path.join(APP_DIR, "ENCABEZADO_PINTURA.jpg"),
                os.path.join(APP_DIR, "ENCABEZADO_PINTURA.jpeg"),
            ]
            for c in candidatos:
                if os.path.exists(c):
                    return c
            return None

        def _firma_pdf_flowable(responsable_nombre):
            ruta = _ruta_firma_responsable(responsables_control, responsable_nombre)
            if not ruta:
                return Paragraph("-", base_style)
            try:
                img = RLImage(ruta)
                img.drawWidth = 1.9 * cm
                img.drawHeight = 0.55 * cm
                return img
            except Exception:
                return Paragraph("-", base_style)

        elements = []
        encabezado_pintura = _encabezado_pintura_path()
        if encabezado_pintura:
            try:
                encabezado_img = RLImage(encabezado_pintura)
                max_width = 26.0 * cm
                if encabezado_img.drawWidth > max_width:
                    escala = max_width / float(encabezado_img.drawWidth)
                    encabezado_img.drawWidth *= escala
                    encabezado_img.drawHeight *= escala
                elements.append(encabezado_img)
            except Exception:
                elements.append(Paragraph("<b>CONTROL DE PINTURA</b>", title_style))
        else:
            elements.append(Paragraph("<b>CONTROL DE PINTURA</b>", title_style))
        elements.append(Spacer(1, 0.2 * cm))

        info = Table([
            [Paragraph(f"<b>Obra:</b> {obra}", base_style), Paragraph(f"<b>Fecha reporte:</b> {date.today().isoformat()}", base_style)],
        ], colWidths=[13.4 * cm, 13.4 * cm])
        info.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#fff7ed')),
            ('BOX', (0, 0), (-1, -1), 0.6, colors.HexColor('#fed7aa')),
            ('INNERGRID', (0, 0), (-1, -1), 0.35, colors.HexColor('#fed7aa')),
            ('LEFTPADDING', (0, 0), (-1, -1), 6),
            ('RIGHTPADDING', (0, 0), (-1, -1), 6),
            ('TOPPADDING', (0, 0), (-1, -1), 5),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ]))
        elements.append(info)
        elements.append(Spacer(1, 0.25 * cm))

        med_table_data = [[
            Paragraph("<b>MANO</b>", head_style),
            Paragraph("<b>FECHA</b>", head_style),
            Paragraph("<b>HORA</b>", head_style),
            Paragraph("<b>TEMPERATURA</b>", head_style),
            Paragraph("<b>HUMEDAD</b>", head_style),
        ]]
        if mediciones:
            for m in mediciones:
                med_table_data.append([
                    Paragraph(m["mano"], base_style),
                    Paragraph(m["fecha"] or "-", base_style),
                    Paragraph(m["hora"] or "-", base_style),
                    Paragraph(m["temp"] or "-", base_style),
                    Paragraph(m["humedad"] or "-", base_style),
                ])
        else:
            med_table_data.append([Paragraph("-", base_style), Paragraph("-", base_style), Paragraph("-", base_style), Paragraph("-", base_style), Paragraph("-", base_style)])

        med_table = Table(med_table_data, colWidths=[2.0 * cm, 4.4 * cm, 3.2 * cm, 4.2 * cm, 4.2 * cm], repeatRows=1)
        med_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0ea5e9')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('GRID', (0, 0), (-1, -1), 0.35, colors.HexColor('#cbd5e1')),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f0f9ff')]),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))

        elements.append(Paragraph("<b>1) Temperatura y Humedad</b>", ParagraphStyle('Sec1', parent=styles['Normal'], fontSize=9, textColor=colors.HexColor('#0c4a6e'))))
        elements.append(Spacer(1, 0.08 * cm))
        elements.append(med_table)
        elements.append(Spacer(1, 0.22 * cm))

        pie_table_data = [
            [
                Paragraph("<b>PIEZA</b>", head_style),
                Paragraph("<b>CANT.</b>", head_style),
                Paragraph("<b>DESCRIPCION</b>", head_style),
                Paragraph("<b>CONTROL SUPERFICIE</b>", head_style),
                "",
                "",
                Paragraph("<b>CONTROL PINTURA (ESPESOR PROM. PELICULA SECA)</b>", head_style),
                "",
                "",
                "",
                "",
                "",
                "",
                "",
            ],
            [
                "",
                "",
                "",
                Paragraph("<b>ESTADO</b>", head_style),
                Paragraph("<b>RESPONSABLE</b>", head_style),
                Paragraph("<b>FIRMA</b>", head_style),
                Paragraph("<b>MANO 1</b>", head_style),
                Paragraph("<b>MANO 2</b>", head_style),
                Paragraph("<b>MANO 3</b>", head_style),
                Paragraph("<b>MANO 4</b>", head_style),
                Paragraph("<b>ESPESOR SOLICITADO</b>", head_style),
                Paragraph("<b>ESTADO FINAL</b>", head_style),
                Paragraph("<b>RESPONSABLE</b>", head_style),
                Paragraph("<b>FIRMA</b>", head_style),
            ],
        ]

        if filas_pintura:
            for r in filas_pintura:
                pie_table_data.append([
                    Paragraph(r["pieza"], base_style),
                    Paragraph(r["cantidad"] or "-", base_style),
                    Paragraph(r["descripcion"] or "-", base_style),
                    Paragraph(r["sup_estado"] or "-", base_style),
                    Paragraph(r["sup_resp"] or "-", base_style),
                    _firma_pdf_flowable(r["sup_resp"]),
                    Paragraph(f"{r['mano1']:.0f}", base_style),
                    Paragraph(f"{r['mano2']:.0f}", base_style),
                    Paragraph(f"{r['mano3']:.0f}", base_style),
                    Paragraph(f"{r['mano4']:.0f}", base_style),
                    Paragraph(f"{r['espesor']:.0f}", base_style),
                    Paragraph(f"<b>{r['estado_final']}</b>", base_style),
                    Paragraph(r["pint_resp"] or "-", base_style),
                    _firma_pdf_flowable(r["pint_resp"]),
                ])
        else:
            pie_table_data.append([Paragraph("-", base_style)] + [Paragraph("-", base_style) for _ in range(13)])

        pie_table = Table(
            pie_table_data,
            colWidths=[1.8 * cm, 1.0 * cm, 2.9 * cm, 1.5 * cm, 1.9 * cm, 2.0 * cm, 1.0 * cm, 1.0 * cm, 1.0 * cm, 1.0 * cm, 1.4 * cm, 1.8 * cm, 1.9 * cm, 2.0 * cm],
            repeatRows=2,
        )
        pie_table.setStyle(TableStyle([
            ('SPAN', (0, 0), (0, 1)),
            ('SPAN', (1, 0), (1, 1)),
            ('SPAN', (2, 0), (2, 1)),
            ('SPAN', (3, 0), (5, 0)),
            ('SPAN', (6, 0), (13, 0)),
            ('BACKGROUND', (0, 0), (2, 1), colors.HexColor('#f97316')),
            ('BACKGROUND', (3, 0), (5, 1), colors.HexColor('#ea580c')),
            ('BACKGROUND', (6, 0), (13, 1), colors.HexColor('#f97316')),
            ('TEXTCOLOR', (0, 0), (-1, 1), colors.white),
            ('GRID', (0, 0), (-1, -1), 0.35, colors.HexColor('#cbd5e1')),
            ('ROWBACKGROUNDS', (0, 2), (-1, -1), [colors.white, colors.HexColor('#fff7ed')]),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('LEFTPADDING', (0, 0), (-1, -1), 3),
            ('RIGHTPADDING', (0, 0), (-1, -1), 3),
            ('TOPPADDING', (0, 0), (-1, -1), 3),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
        ]))

        elements.append(Paragraph("<b>2) Estado de Superficie y Manos de Pintura</b>", ParagraphStyle('Sec2', parent=styles['Normal'], fontSize=9, textColor=colors.HexColor('#9a3412'))))
        elements.append(Spacer(1, 0.08 * cm))
        elements.append(pie_table)

        doc.build(elements)
        pdf_buffer.seek(0)
        
        # Guardar registro en BD
        mediciones_json = json.dumps(mediciones)
        piezas_json = json.dumps(filas_pintura)
        cursor = db.execute(
            """INSERT INTO control_pintura 
               (obra, mediciones, piezas, estado, usuario_creacion, usuario_modificacion)
               VALUES (?, ?, ?, 'activo', 'usuario', 'usuario')""",
            (obra, mediciones_json, piezas_json)
        )
        db.commit()
        control_id = cursor.lastrowid
        
        filename = f"Control_Pintura_{obra}_{date.today().isoformat()}.pdf".replace(" ", "_")
        _guardar_pdf_databook(obra, "calidad_pintura", filename, pdf_buffer.getvalue())
        pdf_buffer.seek(0)
        return send_file(pdf_buffer, mimetype='application/pdf', as_attachment=True, download_name=filename)

    opciones_obras = '<option value="">-- Seleccionar obra --</option>'
    for o in obras:
        sel = 'selected' if o == obra else ''
        opciones_obras += f'<option value="{o}" {sel}>{o}</option>'

    opciones_responsables = '<option value="">Seleccionar...</option>' + "".join(
        f'<option value="{html_lib.escape(nombre)}">{html_lib.escape(nombre)}</option>'
        for nombre in sorted(responsables_control.keys(), key=lambda x: x.lower())
    )

    mediciones_html = ""
    for i in range(1, 5):
        mediciones_html += f"""
        <tr>
            <td><b>Mano {i}</b></td>
            <td><input type=\"date\" name=\"med_fecha[]\"></td>
            <td><input type=\"time\" name=\"med_hora[]\"></td>
            <td>
                <input type=\"number\" step=\"0.1\" name=\"med_temp[]\" placeholder=\"°C\">
            </td>
            <td><input type=\"number\" step=\"0.1\" name=\"med_humedad[]\" placeholder=\"%\"></td>
        </tr>
        """

    piezas_rows_html = ""
    for idx, p in enumerate(piezas, start=1):
        piezas_rows_html += f"""
        <tr class="pieza-row" data-idx="{idx}">
            <td><b>{html_lib.escape(p['pieza'])}</b><input type=\"hidden\" name=\"pieza[]\" value=\"{html_lib.escape(p['pieza'])}\"></td>
            <td>{int(float(p['cantidad']) if p['cantidad'] else 0)}<input type=\"hidden\" name=\"cantidad[]\" value=\"{html_lib.escape(p['cantidad'])}\"></td>
            <td>{html_lib.escape(p['descripcion']) if p['descripcion'] else '-'}<input type=\"hidden\" name=\"descripcion[]\" value=\"{html_lib.escape(p['descripcion'])}\"></td>

            <td>
                <select name=\"sup_estado[]\" class=\"sup-estado\" data-idx=\"{idx}\" required>
                    <option value=\"\">Seleccionar...</option>
                    <option value=\"CONFORME\">Conforme</option>
                    <option value=\"NO CONFORME\">No conforme</option>
                    <option value=\"NO APLICA\">No aplica</option>
                </select>
            </td>
            <td>
                <select name=\"sup_responsable[]\" class=\"sup-resp\" data-idx=\"{idx}\" required>
                    {opciones_responsables}
                </select>
            <td>
                <input type=\"text\" id=\"sup-firma-{idx}\" readonly placeholder=\"Automática\">
                <input type=\"hidden\" name=\"sup_firma[]\" id=\"sup-firma-path-{idx}\">
            </td>

            <td><input type=\"number\" step=\"10\" name=\"mano1[]\" class=\"mano1\" data-idx=\"{idx}\"></td>
            <td><input type=\"number\" step=\"10\" name=\"mano2[]\" class=\"mano2\" data-idx=\"{idx}\"></td>
            <td><input type=\"number\" step=\"10\" name=\"mano3[]\" class=\"mano3\" data-idx=\"{idx}\"></td>
            <td><input type=\"number\" step=\"10\" name=\"mano4[]\" class=\"mano4\" data-idx=\"{idx}\" required></td>
            <td><input type=\"number\" step=\"10\" name=\"espesor_solicitado[]\" class=\"espesor\" data-idx=\"{idx}\" required></td>
            <td><input type=\"text\" class=\"estado-final\" id=\"estado-final-{idx}\" readonly></td>

            <td>
                <select name=\"pintura_responsable[]\" class=\"pint-resp\" data-idx=\"{idx}\" required>
                    {opciones_responsables}
                </select>
            </td>
            <td>
                <input type=\"text\" id=\"pint-firma-{idx}\" readonly placeholder=\"Automática\">
                <input type=\"hidden\" name=\"pintura_firma[]\" id=\"pint-firma-path-{idx}\">
            </td>
        </tr>
        """

    if not piezas_rows_html:
        piezas_rows_html = "<tr><td colspan='14' style='text-align:center;color:#6b7280;'>Seleccioná una obra para cargar piezas</td></tr>"

    html = f"""
    <html>
    <head>
    <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
    <style>
    body {{ font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }}
    h2 {{ color: #9a3412; border-bottom: 3px solid #f97316; padding-bottom: 10px; margin-top: 0; }}
    .box {{ background:white; border-radius:8px; padding:14px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); margin-bottom: 14px; }}
    .filtro {{ display:grid; grid-template-columns: 1fr auto; gap:10px; align-items:end; }}
    .filtro select, .filtro input {{ width:100%; padding:10px; border:1px solid #d1d5db; border-radius:6px; }}
    .btn {{ background:#f97316; color:white; border:none; padding:10px 14px; border-radius:6px; font-weight:bold; cursor:pointer; text-decoration:none; display:inline-block; }}
    .btn:hover {{ background:#ea580c; }}
    .btn-blue {{ background:#2563eb; }}
    .btn-blue:hover {{ background:#1d4ed8; }}
    table {{ width:100%; border-collapse: collapse; background:white; table-layout: fixed; }}
    th, td {{ border-bottom:1px solid #e5e7eb; padding:8px; text-align:center; font-size:12px; }}
    th {{ background:#f97316; color:white; }}
    .th-med {{ background:#0ea5e9; }}
    .th-sup {{ background:#ea580c; }}
    .th-pint {{ background:#f97316; }}
    td:first-child {{ text-align:left; }}
    input, select {{ width:100%; padding:7px; border:1px solid #d1d5db; border-radius:6px; box-sizing:border-box; font-size:12px; }}
    .actions {{ display:flex; gap:10px; flex-wrap:wrap; margin-top:12px; }}
    .estado-ok {{ color:#166534; font-weight:bold; background:#dcfce7; }}
    .estado-nc {{ color:#991b1b; font-weight:bold; background:#fee2e2; }}
    .firma-img {{ display:block; margin:auto; }}
    .pieza-row.deshabilitada input, .pieza-row.deshabilitada select {{ opacity:0.5; cursor:not-allowed; }}
    </style>
    </head>
    <body>
    <h2>🎨 Control de Pintura</h2>

    <div class=\"box\">
        <form method=\"get\" action=\"/modulo/calidad/escaneo/form-pintura\">
            <div class=\"filtro\">
                <div>
                    <label><b>Filtrar por obra</b></label>
                    <select name=\"obra\" required>
                        {opciones_obras}
                    </select>
                </div>
                <button type=\"submit\" class=\"btn\">Aplicar filtro</button>
            </div>
        </form>
    </div>

    <form method=\"post\" action=\"/modulo/calidad/escaneo/form-pintura\">
        <input type=\"hidden\" name=\"accion\" value=\"pdf\">
        <input type=\"hidden\" name=\"obra\" value=\"{html_lib.escape(obra)}\">

        <div class=\"box\">
            <h3 style=\"margin-top:0;color:#0c4a6e;\">1) Temperatura y Humedad</h3>
            <table>
                <tr>
                    <th class=\"th-med\" style=\"width:14%;\">Mano</th>
                    <th class=\"th-med\">Fecha</th>
                    <th class=\"th-med\">Hora</th>
                    <th class=\"th-med\">Temperatura (°C)</th>
                    <th class=\"th-med\">Humedad (%)</th>
                </tr>
                {mediciones_html}
            </table>
        </div>

            <h3 style=\"margin-top:0;color:#9a3412;\">2) Estado de Superficie y Control Pintura (Espesor prom. película seca)</h3>
            <table>
                <tr>
                    <th rowspan=\"2\" style=\"width:8%;\">Pieza</th>
                    <th rowspan=\"2\" style=\"width:5%;\">Cant.</th>
                    <th rowspan=\"2\" style=\"width:10%;\">Descripción</th>
                    <th class=\"th-sup\" colspan=\"3\">Control Superficie</th>
                    <th class=\"th-pint\" colspan=\"8\">Control Pintura (Espesor prom. película seca)</th>
                </tr>
                <tr>
                    <th class=\"th-sup\">Estado</th>
                    <th class=\"th-sup\">Responsable</th>
                    <th class=\"th-sup\">Firma</th>
                    <th class=\"th-pint\">Mano 1</th>
                    <th class=\"th-pint\">Mano 2</th>
                    <th class=\"th-pint\">Mano 3</th>
                    <th class=\"th-pint\">Mano 4</th>
                    <th class=\"th-pint\">Espesor solicitado</th>
                    <th class=\"th-pint\">Estado final</th>
                    <th class=\"th-pint\">Responsable</th>
                    <th class=\"th-pint\">Firma</th>
                </tr>
                {piezas_rows_html}
            </table>

            <div class=\"actions\">
                <button type=\"submit\" class=\"btn\">📄 Generar PDF Pintura</button>
                <a href=\"/modulo/calidad/escaneo/controles-pintura\" class=\"btn btn-blue\">📋 Ver Controles Anteriores</a>
                <a href=\"/modulo/calidad/escaneo\" class=\"btn btn-blue\">⬅️ Volver a Sub Módulos</a>
            </div>
        </div>
    </form>

    <script>
    (function() {{
        const firmasResponsables = {json.dumps(firmas_responsables, ensure_ascii=False)};

        function updateFirma(selectEl, displayId, pathId) {{
            const responsable = selectEl.value || '';
            const firma = firmasResponsables[responsable] || '';
            const display = document.getElementById(displayId);
            const path = document.getElementById(pathId);
            if (display) display.value = firma || '';
            if (path) path.value = responsable;
        }}

        function toggleRowDisabled(idx, isDisabled) {{
            const row = document.querySelector('.pieza-row[data-idx="' + idx + '"]');
            if (!row) return;
            const inputs = row.querySelectorAll('input, select');
            inputs.forEach(inp => {{
                if (inp.classList.contains('sup-estado')) return; // no deshabilitar el selector
                if (isDisabled) {{
                    inp.disabled = true;
                    inp.style.opacity = '0.5';
                }} else {{
                    if (!inp.hasAttribute('disabled-when-noapl')) {{
                        inp.disabled = false;
                        inp.style.opacity = '1';
                    }}
                }}
            }});
            row.classList.toggle('deshabilitada', isDisabled);
        }}

        document.querySelectorAll('.sup-resp').forEach(sel => {{
            sel.addEventListener('change', () => updateFirma(sel, 'sup-firma-' + sel.dataset.idx, 'sup-firma-path-' + sel.dataset.idx));
        }});
        document.querySelectorAll('.pint-resp').forEach(sel => {{
            sel.addEventListener('change', () => updateFirma(sel, 'pint-firma-' + sel.dataset.idx, 'pint-firma-path-' + sel.dataset.idx));
        }});

        document.querySelectorAll('.sup-estado').forEach(sel => {{
            sel.addEventListener('change', () => {{
                const isNoAplica = sel.value === 'NO APLICA';
                toggleRowDisabled(sel.dataset.idx, isNoAplica);
            }});
        }});

        function toFloat(v) {{
            const n = parseFloat(String(v || '').replace(',', '.'));
            return isNaN(n) ? 0 : n;
        }}

        function updateEstado(idx) {{
            const mano4 = toFloat((document.querySelector('.mano4[data-idx="' + idx + '"]') || {{value:''}}).value);
            const esp = toFloat((document.querySelector('.espesor[data-idx="' + idx + '"]') || {{value:''}}).value);
            const out = document.getElementById('estado-final-' + idx);
            if (!out) return;
            const ok = mano4 > esp;
            out.value = ok ? 'OK' : 'NO CONFORME';
            out.classList.remove('estado-ok', 'estado-nc');
            out.classList.add(ok ? 'estado-ok' : 'estado-nc');
        }}

        document.querySelectorAll('.mano4, .espesor').forEach(inp => {{
            inp.addEventListener('input', () => updateEstado(inp.dataset.idx));
            updateEstado(inp.dataset.idx);
        }});
    }})();
    </script>
    </body>
    </html>
    """
    return html

# ======================
# RUTA LISTAR CONTROLES DE PINTURA
# ======================
@app.route("/modulo/calidad/escaneo/controles-pintura", methods=["GET"])
def listar_controles_pintura():
    db = get_db()
    
    obra_filtro = (request.args.get("obra") or "").strip()
    
    query = "SELECT id, obra, fecha_creacion, fecha_modificacion FROM control_pintura WHERE estado='activo'"
    params = []
    
    if obra_filtro:
        query += " AND TRIM(COALESCE(obra, '')) = TRIM(?)"
        params.append(obra_filtro)
    
    query += " ORDER BY fecha_creacion DESC LIMIT 100"
    
    controles = db.execute(query, params).fetchall()
    
    # Obtener lista de obras únicas
    obras_list = db.execute(
        "SELECT DISTINCT obra FROM control_pintura WHERE estado='activo' AND obra<>'' ORDER BY obra"
    ).fetchall()
    obras = [o[0] for o in obras_list]
    
    opciones_obras = '<option value="">-- Todas las obras --</option>'
    for o in obras:
        sel = 'selected' if o == obra_filtro else ''
        opciones_obras += f'<option value="{o}" {sel}>{o}</option>'
    
    filas_html = ""
    for ctrl in controles:
        ctrl_id, obra, fecha_creacion, fecha_mod = ctrl
        fmt_fecha = fecha_creacion.split(" ")[0] if fecha_creacion else "-"
        btn_editar = f'<a href="/modulo/calidad/escaneo/editar-control-pintura/{ctrl_id}" class="btn btn-edit">✏️ Editar</a>'
        btn_pdf = f'<a href="/modulo/calidad/escaneo/generar-pdf-control/{ctrl_id}" class="btn btn-pdf">📄 PDF</a>'
        filas_html += f"""
        <tr>
            <td>{ctrl_id}</td>
            <td>{html_lib.escape(obra)}</td>
            <td>{fmt_fecha}</td>
            <td style="text-align:center;">{btn_editar} {btn_pdf}</td>
        </tr>
        """
    
    if not filas_html:
        filas_html = "<tr><td colspan='4' style='text-align:center;color:#6b7280;'>No hay controles registrados</td></tr>"
    
    html = f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    body {{ font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }}
    h2 {{ color: #9a3412; border-bottom: 3px solid #f97316; padding-bottom: 10px; margin-top: 0; }}
    .box {{ background:white; border-radius:8px; padding:14px; box-shadow: 0 2px 6px rgba(0,0,0,0.08); margin-bottom: 14px; }}
    .filtro {{ display:grid; grid-template-columns: 1fr auto; gap:10px; align-items:end; }}
    .filtro select, .filtro input {{ width:100%; padding:10px; border:1px solid #d1d5db; border-radius:6px; }}
    .btn {{ background:#f97316; color:white; border:none; padding:8px 12px; border-radius:6px; font-weight:bold; cursor:pointer; text-decoration:none; display:inline-block; font-size:12px; }}
    .btn:hover {{ background:#ea580c; }}
    .btn-blue {{ background:#2563eb; }}
    .btn-blue:hover {{ background:#1d4ed8; }}
    .btn-edit {{ background:#059669; }}
    .btn-edit:hover {{ background:#047857; }}
    .btn-pdf {{ background:#7c3aed; }}
    .btn-pdf:hover {{ background:#6d28d9; }}
    table {{ width:100%; border-collapse: collapse; background:white; }}
    th, td {{ border-bottom:1px solid #e5e7eb; padding:10px; text-align:left; font-size:13px; }}
    th {{ background:#f97316; color:white; font-weight:bold; }}
    .actions {{ display:flex; gap:10px; flex-wrap:wrap; margin-top:12px; }}
    </style>
    </head>
    <body>
    <h2>📋 Controles de Pintura Anteriores</h2>

    <div class="box">
        <form method="get" action="/modulo/calidad/escaneo/controles-pintura">
            <div class="filtro">
                <div>
                    <label><b>Filtrar por obra</b></label>
                    <select name="obra">
                        {opciones_obras}
                    </select>
                </div>
                <button type="submit" class="btn">🔍 Filtrar</button>
            </div>
        </form>
    </div>

    <div class="box">
        <table>
            <thead>
                <tr>
                    <th style="width:10%;">ID</th>
                    <th style="width:40%;">Obra</th>
                    <th style="width:25%;">Fecha Creación</th>
                    <th style="width:25%;">Acciones</th>
                </tr>
            </thead>
            <tbody>
                {filas_html}
            </tbody>
        </table>
    </div>

    <div class="actions">
        <a href="/modulo/calidad/escaneo/form-pintura" class="btn btn-blue">➕ Nuevo Control</a>
        <a href="/modulo/calidad/escaneo" class="btn btn-blue">⬅️ Volver a Sub Módulos</a>
    </div>
    </body>
    </html>
    """
    return html

# ======================
# RUTA GENERAR PDF DESDE CONTROL GUARDADO
# ======================
@app.route("/modulo/calidad/escaneo/generar-pdf-control/<int:control_id>", methods=["GET"])
def generar_pdf_control(control_id):
    from datetime import date
    db = get_db()
    ctrl_row = db.execute("SELECT id, obra, mediciones, piezas FROM control_pintura WHERE id=? AND estado='activo'", (control_id,)).fetchone()
    if not ctrl_row: return "Control no encontrado", 404
    ctrl_id, obra, mediciones_json, piezas_json = ctrl_row
    mediciones = json.loads(mediciones_json) if mediciones_json else []
    filas_pintura = json.loads(piezas_json) if piezas_json else []
    responsables_control = _obtener_responsables_control(db)
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
    from reportlab.lib.pagesizes import landscape, letter
    pdf_buffer = BytesIO()
    doc = SimpleDocTemplate(pdf_buffer, pagesize=landscape(letter), topMargin=0.5*cm, bottomMargin=0.6*cm, leftMargin=0.6*cm, rightMargin=0.6*cm)
    styles = getSampleStyleSheet()
    base_style = ParagraphStyle('BaseP', parent=styles['Normal'], fontSize=7.2, leading=8.4, textColor=colors.HexColor('#1f2937'))
    head_style = ParagraphStyle('HeadP', parent=styles['Normal'], fontSize=7.1, leading=8.2, alignment=1, fontName='Helvetica-Bold', textColor=colors.white)
    title_style = ParagraphStyle('TitleP', parent=styles['Heading2'], fontSize=14, textColor=colors.HexColor('#111827'))

    def _encabezado_pintura_path():
        candidatos = [
            os.path.join(APP_DIR, "ENCABEZAO_PINTURA.png"),
            os.path.join(APP_DIR, "ENCABEZAO_PINTURA.jpg"),
            os.path.join(APP_DIR, "ENCABEZAO_PINTURA.jpeg"),
            os.path.join(APP_DIR, "ENCABEZADO_PINTURA.png"),
            os.path.join(APP_DIR, "ENCABEZADO_PINTURA.jpg"),
            os.path.join(APP_DIR, "ENCABEZADO_PINTURA.jpeg"),
        ]
        for c in candidatos:
            if os.path.exists(c):
                return c
        return None

    def _firma_pdf_flowable(responsable_nombre):
        ruta = _ruta_firma_responsable(responsables_control, responsable_nombre)
        if not ruta:
            return Paragraph("-", base_style)
        try:
            img = RLImage(ruta)
            img.drawWidth = 1.9 * cm
            img.drawHeight = 0.55 * cm
            return img
        except Exception:
            return Paragraph("-", base_style)

    elements = []
    encabezado_pintura = _encabezado_pintura_path()
    if encabezado_pintura:
        try:
            encabezado_img = RLImage(encabezado_pintura)
            max_width = 26.0 * cm
            if encabezado_img.drawWidth > max_width:
                escala = max_width / float(encabezado_img.drawWidth)
                encabezado_img.drawWidth *= escala
                encabezado_img.drawHeight *= escala
            elements.append(encabezado_img)
        except Exception:
            elements.append(Paragraph("<b>CONTROL DE PINTURA</b>", title_style))
    else:
        elements.append(Paragraph("<b>CONTROL DE PINTURA</b>", title_style))
    elements.append(Spacer(1, 0.2*cm))
    info = Table([[Paragraph(f"<b>Obra:</b> {obra}", base_style), Paragraph(f"<b>Fecha reporte:</b> {date.today().isoformat()}", base_style)]], colWidths=[13.4*cm, 13.4*cm])
    elements.append(info)
    elements.append(Spacer(1, 0.2*cm))
    elements.append(Paragraph("<b>1) Temperatura y Humedad</b>", ParagraphStyle('Sec1', parent=styles['Normal'], fontSize=9, textColor=colors.HexColor('#0c4a6e'))))
    elements.append(Spacer(1, 0.08*cm))
    med_table_data = [[Paragraph(f"<b>Mano</b>", head_style), Paragraph(f"<b>Fecha</b>", head_style), Paragraph(f"<b>Hora</b>", head_style), Paragraph(f"<b>Temperatura (°C)</b>", head_style), Paragraph(f"<b>Humedad (%)</b>", head_style)]]
    for m in mediciones: med_table_data.append([Paragraph(f"Mano {m['mano']}", base_style), Paragraph(m['fecha'] or "-", base_style), Paragraph(m['hora'] or "-", base_style), Paragraph(m['temp'] or "-", base_style), Paragraph(m['humedad'] or "-", base_style)])
    med_table = Table(med_table_data, colWidths=[2.6*cm, 2.8*cm, 2.8*cm, 3.5*cm, 2.5*cm])
    med_table.setStyle(TableStyle([('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0ea5e9')), ('TEXTCOLOR', (0, 0), (-1, 0), colors.white), ('ALIGN', (0, 0), (-1, -1), 'CENTER'), ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'), ('FONTSIZE', (0, 0), (-1, 0), 7.5), ('FONTSIZE', (0, 1), (-1, -1), 6.8), ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#d1d5db')), ('LEFTPADDING', (0, 0), (-1, -1), 3), ('RIGHTPADDING', (0, 0), (-1, -1), 3), ('TOPPADDING', (0, 0), (-1, -1), 3), ('BOTTOMPADDING', (0, 0), (-1, -1), 3)]))
    elements.append(med_table)
    elements.append(Spacer(1, 0.15*cm))
    elements.append(Paragraph("<b>2) Estado de Superficie y Control Pintura (Espesor prom. película seca)</b>", ParagraphStyle('Sec2', parent=styles['Normal'], fontSize=9, textColor=colors.HexColor('#9a3412'))))
    elements.append(Spacer(1, 0.08*cm))
    pie_table_data = [
        [Paragraph("<b>Pieza</b>", head_style), Paragraph("<b>Cant.</b>", head_style), Paragraph("<b>Descripción</b>", head_style), Paragraph("<b>Control Superficie</b>", head_style), "", "", Paragraph("<b>Control Pintura (Espesor prom. película seca)</b>", head_style), "", "", "", "", "", "", ""],
        ["", "", "", Paragraph("<b>Estado</b>", head_style), Paragraph("<b>Responsable</b>", head_style), Paragraph("<b>Firma</b>", head_style), Paragraph("<b>Mano 1</b>", head_style), Paragraph("<b>Mano 2</b>", head_style), Paragraph("<b>Mano 3</b>", head_style), Paragraph("<b>Mano 4</b>", head_style), Paragraph("<b>Espesor solicitado</b>", head_style), Paragraph("<b>Estado final</b>", head_style), Paragraph("<b>Responsable</b>", head_style), Paragraph("<b>Firma</b>", head_style)],
    ]
    if filas_pintura:
        for r in filas_pintura:
            m1 = float(r.get("mano1", 0) or 0)
            m2 = float(r.get("mano2", 0) or 0)
            m3 = float(r.get("mano3", 0) or 0)
            m4 = float(r.get("mano4", 0) or 0)
            esp = float(r.get("espesor", 0) or 0)
            pie_table_data.append([
                Paragraph(r.get("pieza", "-") or "-", base_style),
                Paragraph(str(r.get("cantidad", "-") or "-"), base_style),
                Paragraph(r.get("descripcion", "-") or "-", base_style),
                Paragraph(r.get("sup_estado", "-") or "-", base_style),
                Paragraph(r.get("sup_resp", "-") or "-", base_style),
                _firma_pdf_flowable(r.get("sup_resp", "") or ""),
                Paragraph(f"{m1:.0f}", base_style),
                Paragraph(f"{m2:.0f}", base_style),
                Paragraph(f"{m3:.0f}", base_style),
                Paragraph(f"{m4:.0f}", base_style),
                Paragraph(f"{esp:.0f}", base_style),
                Paragraph(r.get("estado_final", "-") or "-", base_style),
                Paragraph(r.get("pint_resp", "-") or "-", base_style),
                _firma_pdf_flowable(r.get("pint_resp", "") or ""),
            ])
    else:
        pie_table_data.append([Paragraph("-", base_style)] + [Paragraph("-", base_style) for _ in range(13)])
    pie_table = Table(pie_table_data, colWidths=[1.5*cm]*14)
    pie_table.setStyle(TableStyle([
        ('SPAN', (0, 0), (0, 1)),
        ('SPAN', (1, 0), (1, 1)),
        ('SPAN', (2, 0), (2, 1)),
        ('SPAN', (3, 0), (5, 0)),
        ('SPAN', (6, 0), (13, 0)),
        ('BACKGROUND', (0, 0), (2, 1), colors.HexColor('#f97316')),
        ('BACKGROUND', (3, 0), (5, 1), colors.HexColor('#ea580c')),
        ('BACKGROUND', (6, 0), (13, 1), colors.HexColor('#f97316')),
        ('TEXTCOLOR', (0, 0), (-1, 1), colors.white),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#d1d5db')),
        ('ROWBACKGROUNDS', (0, 2), (-1, -1), [colors.white, colors.HexColor('#fff7ed')]),
        ('LEFTPADDING', (0, 0), (-1, -1), 2),
        ('RIGHTPADDING', (0, 0), (-1, -1), 2),
        ('TOPPADDING', (0, 0), (-1, -1), 2),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
    ]))
    elements.append(pie_table)
    doc.build(elements)
    pdf_buffer.seek(0)
    filename = f"Control_Pintura_{obra}_ID{control_id}_{date.today().isoformat()}.pdf".replace(" ", "_")
    return send_file(pdf_buffer, mimetype='application/pdf', as_attachment=True, download_name=filename)

# ======================
# RUTA EDITAR CONTROL DE PINTURA
# ======================
@app.route("/modulo/calidad/escaneo/editar-control-pintura/<int:control_id>", methods=["GET", "POST"])
def editar_control_pintura(control_id):
    from datetime import date
    db = get_db()
    ctrl_row = db.execute("SELECT id, obra, mediciones, piezas FROM control_pintura WHERE id=? AND estado='activo'", (control_id,)).fetchone()
    if not ctrl_row: return "Control no encontrado", 404
    ctrl_id, obra, mediciones_json, piezas_json = ctrl_row
    mediciones = json.loads(mediciones_json) if mediciones_json else []
    filas_pintura = json.loads(piezas_json) if piezas_json else []
    responsables_control = _obtener_responsables_control(db)
    firmas_responsables = {k: v.get("firma", "") for k, v in responsables_control.items()}
    if request.method == "POST" and (request.form.get("accion") or "").strip().lower() == "pdf":
        def _to_float(val):
            txt = str(val or "").strip().replace(",", ".")
            if not txt: return 0.0
            try: return float(txt)
            except: return 0.0
        piezas_form, cantidades_form, desc_form, sup_estado_form, sup_resp_form, sup_firma_form = request.form.getlist("pieza[]"), request.form.getlist("cantidad[]"), request.form.getlist("descripcion[]"), request.form.getlist("sup_estado[]"), request.form.getlist("sup_responsable[]"), request.form.getlist("sup_firma[]")
        mano1_form, mano2_form, mano3_form, mano4_form, espesor_form, pint_resp_form, pint_firma_form = request.form.getlist("mano1[]"), request.form.getlist("mano2[]"), request.form.getlist("mano3[]"), request.form.getlist("mano4[]"), request.form.getlist("espesor_solicitado[]"), request.form.getlist("pintura_responsable[]"), request.form.getlist("pintura_firma[]")
        filas_pintura_nuevas = []
        for i in range(len(piezas_form)):
            pieza = (piezas_form[i] if i < len(piezas_form) else "").strip()
            if not pieza: continue
            m4_val = _to_float(mano4_form[i] if i < len(mano4_form) else "")
            esp_val = _to_float(espesor_form[i] if i < len(espesor_form) else "")
            sup_resp_nombre = (sup_firma_form[i] if i < len(sup_firma_form) else "").strip()
            sup_firma = responsables_control.get(sup_resp_nombre, {}).get("firma", "") if sup_resp_nombre else ""
            pint_resp_nombre = (pint_firma_form[i] if i < len(pint_firma_form) else "").strip()
            pint_firma = responsables_control.get(pint_resp_nombre, {}).get("firma", "") if pint_resp_nombre else ""
            filas_pintura_nuevas.append({"pieza": pieza, "cantidad": (cantidades_form[i] if i < len(cantidades_form) else "").strip(), "descripcion": (desc_form[i] if i < len(desc_form) else "").strip(), "sup_estado": (sup_estado_form[i] if i < len(sup_estado_form) else "").strip().upper(), "sup_resp": (sup_resp_form[i] if i < len(sup_resp_form) else "").strip(), "sup_firma": sup_firma, "mano1": _to_float(mano1_form[i] if i < len(mano1_form) else ""), "mano2": _to_float(mano2_form[i] if i < len(mano2_form) else ""), "mano3": _to_float(mano3_form[i] if i < len(mano3_form) else ""), "mano4": m4_val, "espesor": esp_val, "estado_final": "OK" if m4_val > esp_val else "NO CONFORME", "pint_resp": (pint_resp_form[i] if i < len(pint_resp_form) else "").strip(), "pint_firma": pint_firma})
        med_fechas, med_horas, med_temps, med_humedades = request.form.getlist("med_fecha[]"), request.form.getlist("med_hora[]"), request.form.getlist("med_temp[]"), request.form.getlist("med_humedad[]")
        mediciones_nuevas = []
        for i in range(max(len(med_fechas), len(med_horas), len(med_temps), len(med_humedades))):
            fecha_m = (med_fechas[i] if i < len(med_fechas) else "").strip()
            if not any([(med_fechas[i] if i < len(med_fechas) else "").strip(), (med_horas[i] if i < len(med_horas) else "").strip(), (med_temps[i] if i < len(med_temps) else "").strip(), (med_humedades[i] if i < len(med_humedades) else "").strip()]): continue
            mediciones_nuevas.append({"mano": str(i+1), "fecha": fecha_m, "hora": (med_horas[i] if i < len(med_horas) else "").strip(), "temp": (med_temps[i] if i < len(med_temps) else "").strip(), "humedad": (med_humedades[i] if i < len(med_humedades) else "").strip()})
        db.execute("UPDATE control_pintura SET mediciones=?, piezas=?, fecha_modificacion=CURRENT_TIMESTAMP WHERE id=?", (json.dumps(mediciones_nuevas), json.dumps(filas_pintura_nuevas), control_id))
        db.commit()
        from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image as RLImage
        from reportlab.lib.pagesizes import landscape, letter
        pdf_buffer = BytesIO()
        doc = SimpleDocTemplate(pdf_buffer, pagesize=landscape(letter), topMargin=0.5*cm, bottomMargin=0.6*cm, leftMargin=0.6*cm, rightMargin=0.6*cm)
        styles = getSampleStyleSheet()
        base_style = ParagraphStyle('BaseP', parent=styles['Normal'], fontSize=7.2, leading=8.4, textColor=colors.HexColor('#1f2937'))
        head_style = ParagraphStyle('HeadP', parent=styles['Normal'], fontSize=7.1, leading=8.2, alignment=1, fontName='Helvetica-Bold', textColor=colors.white)
        title_style = ParagraphStyle('TitleP', parent=styles['Heading2'], fontSize=14, textColor=colors.HexColor('#111827'))

        def _encabezado_pintura_path():
            candidatos = [
                os.path.join(APP_DIR, "ENCABEZAO_PINTURA.png"),
                os.path.join(APP_DIR, "ENCABEZAO_PINTURA.jpg"),
                os.path.join(APP_DIR, "ENCABEZAO_PINTURA.jpeg"),
                os.path.join(APP_DIR, "ENCABEZADO_PINTURA.png"),
                os.path.join(APP_DIR, "ENCABEZADO_PINTURA.jpg"),
                os.path.join(APP_DIR, "ENCABEZADO_PINTURA.jpeg"),
            ]
            for c in candidatos:
                if os.path.exists(c):
                    return c
            return None

        def _firma_pdf_flowable(responsable_nombre):
            ruta = _ruta_firma_responsable(responsables_control, responsable_nombre)
            if not ruta:
                return Paragraph("-", base_style)
            try:
                img = RLImage(ruta)
                img.drawWidth = 1.9 * cm
                img.drawHeight = 0.55 * cm
                return img
            except Exception:
                return Paragraph("-", base_style)

        elements = []
        encabezado_pintura = _encabezado_pintura_path()
        if encabezado_pintura:
            try:
                encabezado_img = RLImage(encabezado_pintura)
                max_width = 26.0 * cm
                if encabezado_img.drawWidth > max_width:
                    escala = max_width / float(encabezado_img.drawWidth)
                    encabezado_img.drawWidth *= escala
                    encabezado_img.drawHeight *= escala
                elements.append(encabezado_img)
            except Exception:
                elements.append(Paragraph("<b>CONTROL DE PINTURA (EDITADO)</b>", title_style))
        else:
            elements.append(Paragraph("<b>CONTROL DE PINTURA (EDITADO)</b>", title_style))
        elements.append(Spacer(1, 0.2*cm))
        info = Table([[Paragraph(f"<b>Obra:</b> {obra}", base_style), Paragraph(f"<b>Fecha:</b> {date.today().isoformat()}", base_style)]], colWidths=[13.4*cm, 13.4*cm])
        elements.append(info)
        elements.append(Spacer(1, 0.2*cm))
        elements.append(Paragraph("<b>1) Temperatura y Humedad</b>", ParagraphStyle('Sec1', parent=styles['Normal'], fontSize=9, textColor=colors.HexColor('#0c4a6e'))))
        elements.append(Spacer(1, 0.08*cm))
        med_table_data = [[Paragraph("<b>Mano</b>", head_style), Paragraph("<b>Fecha</b>", head_style), Paragraph("<b>Hora</b>", head_style), Paragraph("<b>Temp (°C)</b>", head_style), Paragraph("<b>Humedad (%)</b>", head_style)]]
        for m in mediciones_nuevas: med_table_data.append([Paragraph(f"Mano {m['mano']}", base_style), Paragraph(m['fecha'] or "-", base_style), Paragraph(m['hora'] or "-", base_style), Paragraph(m['temp'] or "-", base_style), Paragraph(m['humedad'] or "-", base_style)])
        med_table = Table(med_table_data, colWidths=[2.6*cm, 2.8*cm, 2.8*cm, 3.5*cm, 2.5*cm])
        med_table.setStyle(TableStyle([('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0ea5e9')), ('TEXTCOLOR', (0, 0), (-1, 0), colors.white), ('ALIGN', (0, 0), (-1, -1), 'CENTER'), ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'), ('FONTSIZE', (0, 0), (-1, 0), 7.5), ('FONTSIZE', (0, 1), (-1, -1), 6.8), ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#d1d5db')), ('LEFTPADDING', (0, 0), (-1, -1), 3), ('RIGHTPADDING', (0, 0), (-1, -1), 3), ('TOPPADDING', (0, 0), (-1, -1), 3), ('BOTTOMPADDING', (0, 0), (-1, -1), 3)]))
        elements.append(med_table)
        elements.append(Spacer(1, 0.15*cm))
        elements.append(Paragraph("<b>2) Estado de Superficie y Control Pintura (Espesor prom. película seca)</b>", ParagraphStyle('Sec2', parent=styles['Normal'], fontSize=9, textColor=colors.HexColor('#9a3412'))))
        elements.append(Spacer(1, 0.08*cm))
        pie_table_data = [
            [Paragraph("<b>Pieza</b>", head_style), Paragraph("<b>Cant.</b>", head_style), Paragraph("<b>Descripción</b>", head_style), Paragraph("<b>Control Superficie</b>", head_style), "", "", Paragraph("<b>Control Pintura (Espesor prom. película seca)</b>", head_style), "", "", "", "", "", "", ""],
            ["", "", "", Paragraph("<b>Estado</b>", head_style), Paragraph("<b>Responsable</b>", head_style), Paragraph("<b>Firma</b>", head_style), Paragraph("<b>Mano 1</b>", head_style), Paragraph("<b>Mano 2</b>", head_style), Paragraph("<b>Mano 3</b>", head_style), Paragraph("<b>Mano 4</b>", head_style), Paragraph("<b>Espesor solicitado</b>", head_style), Paragraph("<b>Estado final</b>", head_style), Paragraph("<b>Responsable</b>", head_style), Paragraph("<b>Firma</b>", head_style)],
        ]
        for r in filas_pintura_nuevas:
            pie_table_data.append([
                Paragraph(r["pieza"], base_style),
                Paragraph(r["cantidad"] or "-", base_style),
                Paragraph(r["descripcion"] or "-", base_style),
                Paragraph(r["sup_estado"] or "-", base_style),
                Paragraph(r["sup_resp"] or "-", base_style),
                _firma_pdf_flowable(r["sup_resp"]),
                Paragraph(f"{r['mano1']:.0f}", base_style),
                Paragraph(f"{r['mano2']:.0f}", base_style),
                Paragraph(f"{r['mano3']:.0f}", base_style),
                Paragraph(f"{r['mano4']:.0f}", base_style),
                Paragraph(f"{r['espesor']:.0f}", base_style),
                Paragraph(r["estado_final"], base_style),
                Paragraph(r["pint_resp"] or "-", base_style),
                _firma_pdf_flowable(r["pint_resp"]),
            ])
        pie_table = Table(pie_table_data, colWidths=[1.5*cm]*14)
        pie_table.setStyle(TableStyle([
            ('SPAN', (0, 0), (0, 1)),
            ('SPAN', (1, 0), (1, 1)),
            ('SPAN', (2, 0), (2, 1)),
            ('SPAN', (3, 0), (5, 0)),
            ('SPAN', (6, 0), (13, 0)),
            ('BACKGROUND', (0, 0), (2, 1), colors.HexColor('#f97316')),
            ('BACKGROUND', (3, 0), (5, 1), colors.HexColor('#ea580c')),
            ('BACKGROUND', (6, 0), (13, 1), colors.HexColor('#f97316')),
            ('TEXTCOLOR', (0, 0), (-1, 1), colors.white),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#d1d5db')),
            ('ROWBACKGROUNDS', (0, 2), (-1, -1), [colors.white, colors.HexColor('#fff7ed')]),
            ('LEFTPADDING', (0, 0), (-1, -1), 2),
            ('RIGHTPADDING', (0, 0), (-1, -1), 2),
            ('TOPPADDING', (0, 0), (-1, -1), 2),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 2),
        ]))
        elements.append(pie_table)
        doc.build(elements)
        pdf_buffer.seek(0)
        filename = f"Control_Pintura_{obra}_ID{control_id}_EDITADO_{date.today().isoformat()}.pdf".replace(" ", "_")
        return send_file(pdf_buffer, mimetype='application/pdf', as_attachment=True, download_name=filename)
    opciones_resp = '<option value="">Sel...</option>' + "".join(f'<option value="{html_lib.escape(k)}">{html_lib.escape(k)}</option>' for k in sorted(responsables_control.keys()))
    med_html = ""
    for i in range(1, 5):
        med = mediciones[i-1] if i-1 < len(mediciones) else {}
        med_html += f'<tr><td>M{i}</td><td><input type="date" name="med_fecha[]" value="{med.get("fecha", "")}"></td><td><input type="time" name="med_hora[]" value="{med.get("hora", "")}"></td><td><input type="number" step="0.1" name="med_temp[]" value="{med.get("temp", "")}" placeholder="°C"></td><td><input type="number" step="0.1" name="med_humedad[]" value="{med.get("humedad", "")}" placeholder="%"></td></tr>'
    piezas_html = ""
    for idx, p in enumerate(filas_pintura, 1):
        piezas_html += f'<tr><td>{html_lib.escape(p.get("pieza", ""))}<input type="hidden" name="pieza[]" value="{html_lib.escape(p.get("pieza", ""))}"></td><td>{html_lib.escape(p.get("cantidad", ""))}<input type="hidden" name="cantidad[]" value="{html_lib.escape(p.get("cantidad", ""))}"></td><td><input type="hidden" name="descripcion[]" value="{html_lib.escape(p.get("descripcion", ""))}">{html_lib.escape(p.get("descripcion", ""))}</td><td><select name="sup_estado[]"><option>Sel</option><option {"selected" if p.get("sup_estado") == "CONFORME" else ""}>OK</option><option {"selected" if p.get("sup_estado") == "NO CONFORME" else ""}>NO</option></select></td><td><select name="sup_responsable[]" class="sr" data-i="{idx}">{opciones_resp}</select></td><td><input type="text" name="sup_firma[]" id="sf{idx}" value="{html_lib.escape(p.get("sup_resp", ""))}" readonly></td><td><input type="number" step="0.01" name="mano1[]" value="{p.get("mano1", 0)}" class="m1" data-i="{idx}"></td><td><input type="number" step="0.01" name="mano2[]" value="{p.get("mano2", 0)}" class="m2" data-i="{idx}"></td><td><input type="number" step="0.01" name="mano3[]" value="{p.get("mano3", 0)}" class="m3" data-i="{idx}"></td><td><input type="number" step="0.01" name="mano4[]" value="{p.get("mano4", 0)}" class="m4" data-i="{idx}"></td><td><input type="number" step="0.01" name="espesor_solicitado[]" value="{p.get("espesor", 0)}" class="esp" data-i="{idx}"></td><td><input type="text" id="ef{idx}" value="{p.get("estado_final", "")}" readonly></td><td><select name="pintura_responsable[]" class="pr" data-i="{idx}">{opciones_resp}</select></td><td><input type="text" name="pintura_firma[]" id="pf{idx}" value="{html_lib.escape(p.get("pint_resp", ""))}" readonly></td></tr>'
    return f'<html><head><style>body{{font-family:Arial;padding:10px;}}table{{width:100%;border-collapse:collapse;}}th,td{{border:1px solid #ddd;padding:5px;font-size:10px;}}th{{background:#f97316;color:white;}}input,select{{width:100%;box-sizing:border-box;padding:4px;}}button{{background:#f97316;color:white;border:none;padding:6px 10px;border-radius:4px;cursor:pointer;}}</style></head><body><h2>✏️ Editar Control ID {control_id}</h2><form method="post"><input type="hidden" name="accion" value="pdf"><table><tr><th>M</th><th>Fecha</th><th>Hora</th><th>T°C</th><th>%H</th></tr>{med_html}</table><table><tr><th colspan="6">Pieza</th><th colspan="8">Pintura</th></tr><tr><th>Pieza</th><th>Cant</th><th>Desc</th><th>Est</th><th>Resp</th><th>Firma</th><th>M1</th><th>M2</th><th>M3</th><th>M4</th><th>Esp</th><th>EF</th><th>Resp</th><th>Firma</th></tr>{piezas_html}</table><br><button>Guardar PDF</button> <a href="/modulo/calidad/escaneo/controles-pintura" style="padding:6px 10px;background:#2563eb;color:white;text-decoration:none;border-radius:4px;">Volver</a></form><script>const f={json.dumps(firmas_responsables)};function uf(s,id){{document.getElementById(id).value=s.value||"";}}document.querySelectorAll(".sr").forEach(s=>s.addEventListener("change",()=>uf(s,"sf"+s.dataset.i)));document.querySelectorAll(".pr").forEach(s=>s.addEventListener("change",()=>uf(s,"pf"+s.dataset.i)));function ue(i){{const m4=parseFloat(document.querySelector(".m4[data-i=\'"+i+"\']").value)||0;const e=parseFloat(document.querySelector(".esp[data-i=\'"+i+"\']").value)||0;document.getElementById("ef"+i).value=m4>e?"OK":"NO";}}document.querySelectorAll(".m4,.esp").forEach(x=>x.addEventListener("input",()=>ue(x.dataset.i)));</script></body></html>'

# ======================
# ENDPOINT JSON PARA PROCESAR QR
# ======================
@app.route("/procesar-qr", methods=["POST"])
def procesar_qr():
    try:
        data = request.get_json()
        qr_data = data.get("qr_code", "").strip()
        
        if not qr_data:
            return jsonify({"error": "QR vacío"}), 400

        redirect_url = construir_redirect_desde_qr(qr_data)
        if not redirect_url:
            return jsonify({"error": "Formato de QR inválido"}), 400

        return jsonify({"redirect": redirect_url}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ======================
# MÓDULO 3 - PARTE SEMANAL (Placeholder)
# ======================
@app.route("/modulo/parte", methods=["GET", "POST"])
def parte_semanal():
    db = get_db()
    
    if request.method == "POST":
        accion = (request.form.get("accion") or "").strip()

        if accion == "guardar_empleado":
            nombre_emp = (request.form.get("empleado_nombre") or "").strip()
            puesto_emp = (request.form.get("empleado_puesto") or "").strip()
            firma_emp = (request.form.get("empleado_firma") or "").strip()

            if not nombre_emp or not puesto_emp or not firma_emp:
                return redirect("/modulo/parte?mensaje=" + quote("⚠️ Completá nombre, puesto y firma electrónica"))

            firma_imagen_rel = _resolver_imagen_firma_empleado(nombre_emp, firma_emp)

            existe = db.execute(
                "SELECT id FROM empleados_parte WHERE LOWER(TRIM(nombre)) = LOWER(TRIM(?))",
                (nombre_emp,)
            ).fetchone()

            if existe:
                db.execute(
                    """
                    UPDATE empleados_parte
                    SET nombre=?, puesto=?, firma_electronica=?, firma_imagen_path=?, fecha_actualizacion=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (nombre_emp, puesto_emp, firma_emp, firma_imagen_rel, existe[0])
                )
                mensaje = "✅ Empleado actualizado"
            else:
                try:
                    db.execute(
                        """
                        INSERT INTO empleados_parte (nombre, puesto, firma_electronica, firma_imagen_path)
                        VALUES (?, ?, ?, ?)
                        """,
                        (nombre_emp, puesto_emp, firma_emp, firma_imagen_rel)
                    )
                except sqlite3.IntegrityError:
                    return redirect("/modulo/parte?mensaje=" + quote("⚠️ Ya existe un empleado con ese nombre"))
                mensaje = "✅ Empleado cargado"

            db.commit()
            return redirect("/modulo/parte?mensaje=" + quote(mensaje))

        if accion == "editar_empleado":
            empleado_id = (request.form.get("empleado_id") or "").strip()
            nombre_emp = (request.form.get("empleado_nombre") or "").strip()
            puesto_emp = (request.form.get("empleado_puesto") or "").strip()
            firma_emp = (request.form.get("empleado_firma") or "").strip()

            if not empleado_id.isdigit():
                return redirect("/modulo/parte?mensaje=" + quote("⚠️ Empleado inválido"))
            if not nombre_emp or not puesto_emp or not firma_emp:
                return redirect("/modulo/parte?mensaje=" + quote("⚠️ Completá nombre, puesto y firma electrónica"))

            firma_imagen_rel = _resolver_imagen_firma_empleado(nombre_emp, firma_emp)

            try:
                db.execute(
                    """
                    UPDATE empleados_parte
                    SET nombre=?, puesto=?, firma_electronica=?, firma_imagen_path=?, fecha_actualizacion=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (nombre_emp, puesto_emp, firma_emp, firma_imagen_rel, int(empleado_id))
                )
            except sqlite3.IntegrityError:
                return redirect("/modulo/parte?mensaje=" + quote("⚠️ Ya existe un empleado con ese nombre"))

            db.commit()
            return redirect("/modulo/parte?mensaje=" + quote("✅ Empleado editado"))

        if accion == "eliminar_empleado":
            empleado_id = (request.form.get("empleado_id") or "").strip()
            if not empleado_id.isdigit():
                return redirect("/modulo/parte?mensaje=" + quote("⚠️ Empleado inválido"))

            db.execute("DELETE FROM empleados_parte WHERE id=?", (int(empleado_id),))
            db.commit()
            return redirect("/modulo/parte?mensaje=" + quote("✅ Empleado eliminado"))

        from datetime import datetime
        semana_inicio = request.form.get("semana_inicio")
        empleados_json = request.form.get("empleados_json", "[]")
        
        if not semana_inicio:
            return "Falta fecha de inicio", 400
        
        import json
        empleados = json.loads(empleados_json)

        empleados_map = {}
        for nombre, firma_digital, firma_imagen_path in db.execute(
            "SELECT nombre, firma_electronica, firma_imagen_path FROM empleados_parte"
        ).fetchall():
            clave = str(nombre or "").strip().lower()
            if clave:
                empleados_map[clave] = {
                    "firma_digital": str(firma_digital or "").strip(),
                    "firma_imagen_path": str(firma_imagen_path or "").strip(),
                }
        
        for emp in empleados:
            nombre_emp = str(emp.get('nombre') or '').strip()
            firma_data = empleados_map.get(nombre_emp.lower(), {})
            horas_total = sum([float(emp.get(dia, 0) or 0) for dia in ['lun', 'mar', 'mie', 'jue', 'vie', 'sab']])
            db.execute("""
                INSERT INTO partes_trabajo (fecha, operario, ot_id, horas, firma_digital, firma_imagen_path, actividad)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                semana_inicio,
                nombre_emp,
                emp.get('ot_id'),
                horas_total,
                firma_data.get("firma_digital", ""),
                firma_data.get("firma_imagen_path", ""),
                f"Semana del {semana_inicio}"
            ))
        
        db.commit()
        return redirect("/modulo/parte")
    
    # Obtener OT disponibles y empleados recientes
    ots = db.execute(
        "SELECT id, obra, titulo FROM ordenes_trabajo WHERE estado != 'Finalizada' AND fecha_cierre IS NULL ORDER BY id DESC"
    ).fetchall()

    empleados_catalogo = db.execute(
        "SELECT id, nombre, puesto, firma_electronica, firma_imagen_path FROM empleados_parte ORDER BY nombre"
    ).fetchall()

    operarios_catalogo = db.execute(
        """
        SELECT nombre
        FROM empleados_parte
        WHERE LOWER(TRIM(COALESCE(puesto, ''))) LIKE '%operario%'
        ORDER BY nombre
        """
    ).fetchall()

    mensaje = (request.args.get("mensaje") or "").strip()
    mensaje_html = ""
    if mensaje:
        clase = "flash-error" if ("⚠️" in mensaje or "❌" in mensaje) else "flash-ok"
        mensaje_html = f'<div class="flash {clase}">{html_lib.escape(mensaje)}</div>'

    operarios_options = ""
    for (nombre_operario,) in operarios_catalogo:
        nombre_txt = str(nombre_operario or "").strip()
        if nombre_txt:
            operarios_options += f'<option value="{html_lib.escape(nombre_txt)}">{html_lib.escape(nombre_txt)}</option>'

    empleados_listado = ""
    for empleado_id, nombre, puesto, firma, firma_imagen_path in empleados_catalogo:
        nombre_txt = html_lib.escape(str(nombre or "").strip())
        puesto_txt = html_lib.escape(str(puesto or "").strip())
        firma_txt = html_lib.escape(str(firma or "").strip())
        empleados_listado += f"""
            <tr>
                <td>
                    <input type="text" name="empleado_nombre" value="{nombre_txt}" form="edit-emp-{empleado_id}" required>
                </td>
                <td>
                    <input type="text" name="empleado_puesto" value="{puesto_txt}" form="edit-emp-{empleado_id}" required>
                </td>
                <td>
                    <input type="text" name="empleado_firma" value="{firma_txt}" form="edit-emp-{empleado_id}" required>
                </td>
                <td style="white-space: nowrap; min-width: 220px;">
                    <form id="edit-emp-{empleado_id}" method="post" style="display:inline; margin:0; padding:0; background:transparent;">
                        <input type="hidden" name="accion" value="editar_empleado">
                        <input type="hidden" name="empleado_id" value="{empleado_id}">
                        <button type="submit" class="btn-mini">💾 Editar</button>
                    </form>
                    <form method="post" style="display:inline; margin:0; padding:0; background:transparent;" onsubmit="return confirm('¿Eliminar empleado?');">
                        <input type="hidden" name="accion" value="eliminar_empleado">
                        <input type="hidden" name="empleado_id" value="{empleado_id}">
                        <button type="submit" class="btn-mini btn-mini-del">🗑 Eliminar</button>
                    </form>
                </td>
            </tr>
        """

    if not empleados_listado:
        empleados_listado = "<tr><td colspan='4' style='text-align:center;color:#6b7280;'>No hay empleados cargados</td></tr>"
    
    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * { box-sizing: border-box; }
    body { font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }
    h2 { color: #333; border-bottom: 3px solid #667eea; padding-bottom: 10px; }
    .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
    .header-actions { display: flex; gap: 10px; align-items: center; }
    .btn { background: #667eea; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; }
    .btn-reportes { background: #43a047; }
    .btn-reportes:hover { background: #2e7d32; }
    form { background: white; padding: 20px; border-radius: 5px; margin: 20px 0; }
    .form-group { margin-bottom: 20px; }
    label { display: block; font-weight: bold; margin-bottom: 5px; }
    input[type="date"], input[type="text"], select { width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 4px; }
    table { width: 100%; border-collapse: collapse; background: white; margin-top: 15px; }
    th, td { padding: 10px; border: 1px solid #ddd; text-align: center; }
    th { background: #667eea; color: white; font-weight: bold; }
    td { background: white; }
    input[type="number"] { width: 100%; padding: 5px; border: 1px solid #ccc; border-radius: 3px; }
    .btn-add { background: #43e97b; padding: 8px 12px; cursor: pointer; margin-top: 10px; }
    .btn-add:hover { background: #2cc96e; }
    button { width: 100%; padding: 12px; background: #43e97b; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; }
    button:hover { background: #2cc96e; }
    .btn-delete { background: #fa709a; color: white; padding: 5px 10px; border: none; cursor: pointer; border-radius: 3px; }
    .total { font-weight: bold; background: #e8f5e9; }
    .seccion-empleados { background: #ffffff; border: 1px solid #d8e0f0; border-radius: 8px; padding: 16px; margin-bottom: 16px; }
    .grid-3 { display: grid; grid-template-columns: repeat(3, 1fr); gap: 10px; }
    .tabla-empleados { margin-top: 12px; }
    .tabla-empleados th { background: #3949ab; }
    .btn-mini { width: auto; padding: 8px 10px; font-size: 12px; margin-right: 6px; margin-top: 4px; }
    .btn-mini-del { background: #ef5350; }
    .btn-mini-del:hover { background: #d84343; }
    .flash { padding: 10px 12px; border-radius: 6px; margin-bottom: 14px; font-weight: bold; }
    .flash-ok { background: #e8f5e9; color: #1b5e20; border: 1px solid #a5d6a7; }
    .flash-error { background: #fff3e0; color: #8a4b00; border: 1px solid #ffcc80; }
    @media (max-width: 900px) { .grid-3 { grid-template-columns: 1fr; } }
    </style>
    </head>
    <body>
    <div class="header">
        <h2>⏱ Parte Semanal - Empleados</h2>
        <div class="header-actions">
            <a href="/modulo/parte/reportes" class="btn btn-reportes">📊 Ver reportes</a>
            <a href="/" class="btn">⬅️ Volver</a>
        </div>
    </div>
    """ + mensaje_html + """
    
    <form method="post" id="parte-form">
        <input type="hidden" name="accion" value="guardar_parte">
        <div class="form-group">
            <label>Semana iniciando:</label>
            <input type="date" name="semana_inicio" id="semana_inicio" required>
        </div>
        
        <h3>📋 Planilla de Horas (Lunes a Sábado)</h3>
        <table id="planilla-table">
            <tr>
                <th>Empleado</th>
                <th>OT Asignada</th>
                <th>Lun</th>
                <th>Mar</th>
                <th>Mié</th>
                <th>Jue</th>
                <th>Vie</th>
                <th>Sáb</th>
                <th>Total</th>
                <th>Eliminar</th>
            </tr>
            <tr id="template-row" style="display: none;">
                <td>
                    <select class="empleado-input" required>
                        <option value="">Seleccionar operario...</option>
    """ + operarios_options + """
                    </select>
                </td>
                <td>
                    <select class="ot-input" required>
                        <option value="">Seleccionar...</option>
    """
    
    for ot in ots:
        obra_ot = str(ot[1] or '').strip()
        titulo_ot = str(ot[2] or '').strip()
        etiqueta_ot = f"{ot[0]} - {obra_ot} - {titulo_ot}" if titulo_ot else f"{ot[0]} - {obra_ot}"
        html += f'<option value="{ot[0]}">{etiqueta_ot}</option>'
    
    html += """
                    </select>
                </td>
                <td><input type="number" class="horas lun" min="0" max="24" step="0.5" value="0"></td>
                <td><input type="number" class="horas mar" min="0" max="24" step="0.5" value="0"></td>
                <td><input type="number" class="horas mie" min="0" max="24" step="0.5" value="0"></td>
                <td><input type="number" class="horas jue" min="0" max="24" step="0.5" value="0"></td>
                <td><input type="number" class="horas vie" min="0" max="24" step="0.5" value="0"></td>
                <td><input type="number" class="horas sab" min="0" max="24" step="0.5" value="0"></td>
                <td class="total">0</td>
                <td><button type="button" class="btn-delete" onclick="this.parentElement.parentElement.remove(); actualizarTotales();">Eliminar</button></td>
            </tr>
            <tr id="resumen-dias" style="background:#e8f5e9; font-weight:bold;">
                <td colspan="2"><b>Sumatoria HS por día</b></td>
                <td id="sum-lun">0.0</td>
                <td id="sum-mar">0.0</td>
                <td id="sum-mie">0.0</td>
                <td id="sum-jue">0.0</td>
                <td id="sum-vie">0.0</td>
                <td id="sum-sab">0.0</td>
                <td id="sum-total">0.0</td>
                <td>—</td>
            </tr>
        </table>
        
        <button type="button" class="btn-add" onclick="agregarEmpleado()">➕ Agregar Empleado</button>
        
        <input type="hidden" name="empleados_json" id="empleados_json">
        <button type="submit" onclick="guardarParte()">💾 Guardar Parte Semanal</button>
    </form>

    <div class="seccion-empleados">
        <h3>👥 Cargar Empleados</h3>
        <form method="post" id="empleados-form" style="margin: 0; padding: 0; background: transparent;">
            <input type="hidden" name="accion" value="guardar_empleado">
            <div class="grid-3">
                <div>
                    <label>Nombre</label>
                    <input type="text" name="empleado_nombre" placeholder="Nombre y apellido" required>
                </div>
                <div>
                    <label>Puesto</label>
                    <input type="text" name="empleado_puesto" placeholder="Ej: Soldador" required>
                </div>
                <div>
                    <label>Firma electrónica</label>
                    <input type="text" name="empleado_firma" placeholder="Código o nombre de firma" required>
                </div>
            </div>
            <button type="submit" style="margin-top: 10px;">💾 Guardar Empleado</button>
        </form>

        <table class="tabla-empleados">
            <tr>
                <th>Nombre</th>
                <th>Puesto</th>
                <th>Firma electrónica</th>
                <th>Acciones</th>
            </tr>
    """ + empleados_listado + """
        </table>
    </div>
    
    <script>
    function agregarEmpleado() {
        const template = document.getElementById('template-row');
        const newRow = template.cloneNode(true);
        newRow.id = '';
        newRow.style.display = '';

        const resumen = document.getElementById('resumen-dias');
        const parent = resumen ? resumen.parentNode : document.getElementById('planilla-table');
        parent.insertBefore(newRow, resumen);
        
        newRow.querySelectorAll('.horas').forEach(input => {
            input.addEventListener('change', actualizarTotales);
        });
        
        newRow.querySelector('.btn-delete').onclick = function() {
            this.parentElement.parentElement.remove();
            actualizarTotales();
        };
    }
    
    function actualizarTotales() {
        const rows = document.querySelectorAll('#planilla-table tr');
        const sum = { lun: 0, mar: 0, mie: 0, jue: 0, vie: 0, sab: 0 };

        function valorHora(row, clase) {
            const el = row.querySelector(clase);
            return el ? (parseFloat(el.value) || 0) : 0;
        }

        rows.forEach((row, idx) => {
            if (idx === 0 || row.id === 'template-row' || row.id === 'resumen-dias') return;

            const horas = row.querySelectorAll('.horas');
            let total = 0;
            horas.forEach(h => total += parseFloat(h.value) || 0);

            const elTotal = row.querySelector('.total');
            if (elTotal) {
                elTotal.textContent = total.toFixed(1);
            }

            sum.lun += valorHora(row, '.lun');
            sum.mar += valorHora(row, '.mar');
            sum.mie += valorHora(row, '.mie');
            sum.jue += valorHora(row, '.jue');
            sum.vie += valorHora(row, '.vie');
            sum.sab += valorHora(row, '.sab');
        });

        document.getElementById('sum-lun').textContent = sum.lun.toFixed(1);
        document.getElementById('sum-mar').textContent = sum.mar.toFixed(1);
        document.getElementById('sum-mie').textContent = sum.mie.toFixed(1);
        document.getElementById('sum-jue').textContent = sum.jue.toFixed(1);
        document.getElementById('sum-vie').textContent = sum.vie.toFixed(1);
        document.getElementById('sum-sab').textContent = sum.sab.toFixed(1);
        document.getElementById('sum-total').textContent = (sum.lun + sum.mar + sum.mie + sum.jue + sum.vie + sum.sab).toFixed(1);
    }
    
    function guardarParte() {
        const semana = document.getElementById('semana_inicio').value;
        if (!semana) {
            alert('❌ Selecciona la fecha de inicio');
            return;
        }
        
        const rows = document.querySelectorAll('#planilla-table tr');
        const empleados = [];
        
        rows.forEach((row, idx) => {
            if (idx === 0 || row.id === 'template-row' || row.id === 'resumen-dias') return;

            const empleadoSelect = row.querySelector('.empleado-input');
            if (!empleadoSelect || empleadoSelect.value.trim() === '') return;
            
            empleados.push({
                nombre: empleadoSelect.value,
                ot_id: row.querySelector('.ot-input').value,
                lun: row.querySelector('.lun').value,
                mar: row.querySelector('.mar').value,
                mie: row.querySelector('.mie').value,
                jue: row.querySelector('.jue').value,
                vie: row.querySelector('.vie').value,
                sab: row.querySelector('.sab').value
            });
        });
        
        if (empleados.length === 0) {
            alert('❌ Agrega al menos un empleado');
            return;
        }
        
        document.getElementById('empleados_json').value = JSON.stringify(empleados);
        document.getElementById('parte-form').submit();
    }
    
    function inicializarFilasIniciales() {
        const yaHayFilas = document.querySelectorAll('#planilla-table tr:not(#template-row):not(#resumen-dias)').length > 1;
        if (yaHayFilas) return;

        const filasIniciales = 6;
        for (let i = 0; i < filasIniciales; i++) {
            agregarEmpleado();
        }

        actualizarTotales();
    }

    // Inicializa en modo inmediato y también como respaldo cuando dispara DOMContentLoaded.
    inicializarFilasIniciales();
    document.addEventListener('DOMContentLoaded', inicializarFilasIniciales);
    </script>
    </body>
    </html>
    """
    return html

@app.route("/modulo/parte/reportes")
def parte_semanal_reportes():
    db = get_db()

    filtro_obra = request.args.get("obra", "").strip()
    filtro_empleado = request.args.get("empleado", "").strip()
    filtro_semana = request.args.get("semana", "").strip()
    filtro_mes = request.args.get("mes", "").strip()

    obras = db.execute("""
        SELECT DISTINCT TRIM(ot.obra) AS obra
        FROM partes_trabajo pt
        LEFT JOIN ordenes_trabajo ot ON ot.id = pt.ot_id
        WHERE ot.obra IS NOT NULL AND TRIM(ot.obra) <> ''
        ORDER BY obra ASC
    """).fetchall()
    empleados = db.execute("""
        SELECT DISTINCT TRIM(operario) AS operario
        FROM partes_trabajo
        WHERE operario IS NOT NULL AND TRIM(operario) <> ''
        ORDER BY operario ASC
    """).fetchall()

    condiciones = []
    params = []
    if filtro_obra:
        condiciones.append("TRIM(COALESCE(ot.obra, '')) = ?")
        params.append(filtro_obra)
    if filtro_empleado:
        condiciones.append("LOWER(TRIM(COALESCE(pt.operario, ''))) = ?")
        params.append(filtro_empleado.lower())
    if filtro_semana:
        condiciones.append("pt.fecha = ?")
        params.append(filtro_semana)
    if filtro_mes:
        condiciones.append("substr(pt.fecha, 1, 7) = ?")
        params.append(filtro_mes)

    mes_label = "-"
    if filtro_mes:
        try:
            anio, mes_num = filtro_mes.split("-")
            meses_nombres = [
                "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
                "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
            ]
            mes_label = f"{meses_nombres[int(mes_num) - 1]} {anio}"
        except Exception:
            mes_label = filtro_mes

    where_sql = f"WHERE {' AND '.join(condiciones)}" if condiciones else ""
    
    reportes = db.execute(f"""
        SELECT pt.id,
               pt.fecha,
               pt.operario,
               TRIM(COALESCE(ot.obra, '')) AS obra,
               pt.ot_id,
               TRIM(COALESCE(ot.titulo, '')) AS ot_titulo,
               TRIM(COALESCE(pt.actividad, '')) AS actividad,
               COALESCE(pt.horas, 0) AS horas
        FROM partes_trabajo pt
        LEFT JOIN ordenes_trabajo ot ON ot.id = pt.ot_id
        {where_sql}
        ORDER BY pt.fecha DESC, pt.operario ASC
    """, params).fetchall()

    total_horas = sum(float(r[7] or 0) for r in reportes)

    opciones_obras = '<option value="">Todas las obras</option>'
    for obra in obras:
        obra_val = str(obra[0] or '').strip()
        selected = 'selected' if obra_val == filtro_obra else ''
        opciones_obras += f'<option value="{obra_val}" {selected}>{obra_val}</option>'

    opciones_empleados = '<option value="">Todos los empleados</option>'
    for empleado in empleados:
        empleado_val = str(empleado[0] or '').strip()
        selected = 'selected' if empleado_val == filtro_empleado else ''
        opciones_empleados += f'<option value="{empleado_val}" {selected}>{empleado_val}</option>'

    semanas = db.execute("""
        SELECT DISTINCT fecha
        FROM partes_trabajo
        WHERE fecha IS NOT NULL AND TRIM(fecha) <> ''
        ORDER BY fecha DESC
    """).fetchall()
    opciones_semanas = '<option value="">Todas las semanas</option>'
    for semana in semanas:
        semana_val = str(semana[0] or '').strip()
        selected = 'selected' if semana_val == filtro_semana else ''
        opciones_semanas += f'<option value="{semana_val}" {selected}>{semana_val}</option>'

    meses = db.execute("""
        SELECT DISTINCT substr(fecha, 1, 7) AS mes
        FROM partes_trabajo
        WHERE fecha IS NOT NULL AND TRIM(fecha) <> ''
        ORDER BY mes DESC
    """).fetchall()
    opciones_meses = '<option value="">Todos los meses</option>'
    for mes in meses:
        mes_val = str(mes[0] or '').strip()
        if mes_val:
            año, mes_num = mes_val.split('-')
            mes_nombre = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'][int(mes_num) - 1]
            selected = 'selected' if mes_val == filtro_mes else ''
            opciones_meses += f'<option value="{mes_val}" {selected}>{mes_nombre} {año}</option>'

    # Agrupar reportes por semana y empleado
    reportes_por_semana = {}
    for rep in reportes:
        fecha = rep[1] or ''
        if fecha not in reportes_por_semana:
            reportes_por_semana[fecha] = {}
        
        operario = rep[2] or ''
        if operario not in reportes_por_semana[fecha]:
            reportes_por_semana[fecha][operario] = []
        
        reportes_por_semana[fecha][operario].append(rep)

    filas = ""
    for fecha in sorted(reportes_por_semana.keys(), reverse=True):
        reps_semana = reportes_por_semana[fecha]
        total_empleados = len(reps_semana)
        
        # Calcular totales de la semana
        horas_semana = sum(float(r[7] or 0) for row_list in reps_semana.values() for r in row_list)
        registros_semana = sum(len(row_list) for row_list in reps_semana.values())
        
        # Encabezado de semana
        filas += f"""
        <tr style="background: #eef8fd; font-weight: bold;">
            <td colspan="8" style="background: #a8d8ea; color: #1f4e5f; padding: 12px; font-size: 16px;">
                📅 Semana del {fecha} | {total_empleados} empleados | {registros_semana} registros | {horas_semana:.1f} HS
            </td>
        </tr>
        """
        
        # Filas de empleados en esa semana
        for operario in sorted(reps_semana.keys()):
            rows_emp = reps_semana[operario]
            horas_emp = sum(float(r[7] or 0) for r in rows_emp)
            
            for idx, rep in enumerate(rows_emp):
                parte_id = rep[0]
                ot_id = rep[4] or '-'
                ot_titulo = rep[5] or '---'
                actividad = rep[6] or '---'
                obra = rep[3] or '---'
                horas = float(rep[7] or 0)
                
                horas_cell = f"<b>{horas:.1f}</b>" if idx == len(rows_emp) - 1 else f"{horas:.1f}"
                
                filas += f"""
        <tr>
            <td><b>{operario}</b></td>
            <td>{obra}</td>
            <td><b>{ot_id}</b></td>
            <td>{ot_titulo}</td>
            <td>{actividad}</td>
            <td style="text-align: center;">1</td>
            <td style="text-align: right;">{horas_cell}</td>
            <td style="text-align: center;"><a href="/modulo/parte/reportes/eliminar/{parte_id}" style="color: #d32f2f; text-decoration: none; font-weight: bold; cursor: pointer;" onclick="return confirm('¿Estás seguro de que deseas eliminar este registro?');">✕</a></td>
        </tr>
        """

    if not filas:
        filas = "<tr><td colspan='8' style='text-align:center; color:#777;'>No hay partes guardados para ese filtro</td></tr>"

    html = f"""
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * {{ box-sizing: border-box; }}
    body {{ font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }}
    .header {{ display: flex; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 20px; }}
    .header-left {{ display: flex; align-items: center; gap: 12px; flex: 1; }}
    h2 {{ color: #333; border-bottom: 3px solid #43a047; padding-bottom: 10px; margin: 0; }}
    .header-btns {{ display: flex; gap: 8px; }}
    .btn {{ background: #667eea; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; }}
    .btn:hover {{ background: #5568d3; }}
    .btn-pdf {{ background: #ff9800; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; border: none; cursor: pointer; font-weight: bold; }}
    .btn-pdf:hover {{ background: #f57c00; }}
    .filters {{ background: white; padding: 18px; border-radius: 6px; margin-bottom: 16px; box-shadow: 0 2px 5px rgba(0,0,0,0.08); }}
    .filters form {{ display: grid; grid-template-columns: 1fr 1fr 1fr 1fr auto auto; gap: 10px; align-items: end; }}
    label {{ display: block; font-weight: bold; margin-bottom: 5px; }}
    select {{ width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 4px; }}
    button {{ padding: 10px 16px; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; background: #43a047; color: white; }}
    button:hover {{ background: #2e7d32; }}
    .btn-clear {{ background: #9e9e9e; }}
    .btn-clear:hover {{ background: #757575; }}
    .summary {{ background: #e8f5e9; border-left: 5px solid #43a047; padding: 14px; border-radius: 5px; margin-bottom: 16px; color: #1b5e20; font-size: 16px; }}
    table {{ width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 5px rgba(0,0,0,0.08); font-size: 13px; }}
    th, td {{ padding: 10px; border-bottom: 1px solid #ddd; text-align: left; }}
    th {{ background: #43a047; color: white; font-weight: bold; font-size: 12px; }}
    tr:nth-child(even) td {{ background: #fafafa; }}
    td {{ word-wrap: break-word; overflow-wrap: break-word; }}
    td:nth-child(4), td:nth-child(7) {{ text-align: center; }}
    td:nth-child(8) {{ text-align: right; font-weight: bold; }}
    .btn-delete-row {{
        background: #d32f2f;
        color: white;
        text-decoration: none;
        padding: 6px 10px;
        border-radius: 4px;
        font-size: 12px;
        font-weight: bold;
        display: inline-block;
    }}
    .btn-delete-row:hover {{
        background: #b71c1c;
    }}
    @media (max-width: 900px) {{
        .filters form {{ grid-template-columns: 1fr; }}
        .header {{ flex-direction: column; align-items: stretch; }}
    }}
    </style>
    </head>
    <body>
    <div class="header">
        <div class="header-left">
            <h2>📊 Reportes de Parte Semanal - Empleados</h2>
        </div>
        <div class="header-btns">
            <form method="POST" action="/modulo/parte/reportes/pdf" style="display: inline;">
                <input type="hidden" name="obra" value="{filtro_obra}">
                <input type="hidden" name="empleado" value="{filtro_empleado}">
                <input type="hidden" name="semana" value="{filtro_semana}">
                <input type="hidden" name="mes" value="{filtro_mes}">
                <button type="submit" class="btn-pdf">📄 Descargar PDF</button>
            </form>
            <a href="/modulo/parte" class="btn">⬅️ Volver</a>
        </div>
    </div>

    <div class="filters">
        <form method="get">
            <div>
                <label>Obra</label>
                <select name="obra">
                    {opciones_obras}
                </select>
            </div>
            <div>
                <label>Empleado</label>
                <select name="empleado">
                    {opciones_empleados}
                </select>
            </div>
            <div>
                <label>Semana</label>
                <select name="semana">
                    {opciones_semanas}
                </select>
            </div>
            <div>
                <label>Mes</label>
                <select name="mes">
                    {opciones_meses}
                </select>
            </div>
            <button type="submit">Filtrar</button>
            <a href="/modulo/parte/reportes" class="btn btn-clear">Limpiar</a>
        </form>
    </div>

    <div class="summary">
        Horas consumidas: <b>{total_horas:.1f}</b> | Registros encontrados: <b>{len(reportes)}</b>
    </div>

    <table>
        <tr>
            <th style="width: 15%;">Operario</th>
            <th style="width: 12%;">Obra</th>
            <th style="width: 6%;">OT</th>
            <th style="width: 30%;">Descripción OT</th>
            <th style="width: 20%;">Actividad</th>
            <th style="width: 6%;">Reg.</th>
            <th style="width: 8%;">HS</th>
            <th style="width: 3%;">Acción</th>
        </tr>
        {filas}
    </table>
    </body>
    </html>
    """
    return html

@app.route("/modulo/parte/reportes/eliminar/<int:parte_id>")
def parte_semanal_reporte_eliminar(parte_id):
    db = get_db()
    db.execute("DELETE FROM partes_trabajo WHERE id=?", (parte_id,))
    db.commit()
    return redirect("/modulo/parte/reportes")

@app.route("/modulo/parte/reportes/pdf", methods=["POST"])
def parte_semanal_reportes_pdf():
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
    from reportlab.lib.pagesizes import letter, landscape
    from reportlab.lib import colors
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from datetime import datetime

    db = get_db()

    filtro_obra = request.form.get("obra", "").strip()
    filtro_empleado = request.form.get("empleado", "").strip()
    filtro_semana = request.form.get("semana", "").strip()
    filtro_mes = request.form.get("mes", "").strip()

    condiciones = []
    params = []
    if filtro_obra:
        condiciones.append("TRIM(COALESCE(ot.obra, '')) = ?")
        params.append(filtro_obra)
    if filtro_empleado:
        condiciones.append("LOWER(TRIM(COALESCE(pt.operario, ''))) = ?")
        params.append(filtro_empleado.lower())
    if filtro_semana:
        condiciones.append("pt.fecha = ?")
        params.append(filtro_semana)
    if filtro_mes:
        condiciones.append("substr(pt.fecha, 1, 7) = ?")
        params.append(filtro_mes)

    mes_label = "-"
    if filtro_mes:
        try:
            anio, mes_num = filtro_mes.split("-")
            meses_nombres = [
                "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio",
                "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"
            ]
            mes_label = f"{meses_nombres[int(mes_num) - 1]} {anio}"
        except Exception:
            mes_label = filtro_mes

    where_sql = f"WHERE {' AND '.join(condiciones)}" if condiciones else ""

    reportes = db.execute(f"""
        SELECT pt.fecha,
               pt.operario,
               TRIM(COALESCE(ot.obra, '')) AS obra,
               pt.ot_id,
               TRIM(COALESCE(ot.titulo, '')) AS ot_titulo,
               TRIM(COALESCE(pt.actividad, '')) AS actividad,
               COALESCE(pt.horas, 0) AS horas
        FROM partes_trabajo pt
        LEFT JOIN ordenes_trabajo ot ON ot.id = pt.ot_id
        {where_sql}
        ORDER BY pt.fecha DESC, pt.operario ASC
    """, params).fetchall()

    reportes_por_semana = {}
    for rep in reportes:
        fecha = rep[0] or ''
        operario = rep[1] or ''
        reportes_por_semana.setdefault(fecha, {}).setdefault(operario, []).append(rep)

    total_horas = sum(float(r[6] or 0) for r in reportes)
    total_registros = len(reportes)

    pdf_buffer = BytesIO()
    doc = SimpleDocTemplate(
        pdf_buffer,
        pagesize=landscape(letter),
        leftMargin=18,
        rightMargin=18,
        topMargin=18,
        bottomMargin=18,
    )

    styles = getSampleStyleSheet()
    story = []

    title_style = ParagraphStyle(
        'ReporteTitle',
        parent=styles['Heading1'],
        fontSize=16,
        leading=20,
        textColor=colors.HexColor('#1f2937'),
        spaceAfter=4,
    )
    subtitle_style = ParagraphStyle(
        'ReporteSubTitle',
        parent=styles['Normal'],
        fontSize=10,
        leading=12,
        textColor=colors.HexColor('#4b5563'),
    )
    cell_style = ParagraphStyle(
        'CellWrap',
        parent=styles['Normal'],
        fontSize=8,
        leading=10,
    )

    logo_path = os.path.join(APP_DIR, "LOGO.png")
    logo_flow = ""
    if os.path.exists(logo_path):
        logo_flow = Image(logo_path, width=40 * mm, height=22 * mm)

    header_right = Paragraph(
        (
            "<b>REPORTE DE PARTE MENSUAL</b><br/><font size='10'>REGISTRO DE HORAS - EMPLEADOS</font>"
            if filtro_mes else
            "<b>REPORTE DE PARTE SEMANAL</b><br/><font size='10'>REGISTRO DE HORAS - EMPLEADOS</font>"
        ),
        title_style,
    )
    header_table = Table([[logo_flow, header_right]], colWidths=[50 * mm, 215 * mm])
    header_table.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('ALIGN', (0, 0), (0, 0), 'LEFT'),
        ('ALIGN', (1, 0), (1, 0), 'LEFT'),
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
    ]))
    story.append(header_table)
    story.append(Spacer(1, 8))

    info_row1 = [
        Paragraph(f"<b>Semana:</b> {filtro_semana or '-'}", subtitle_style),
        Paragraph(f"<b>Obra:</b> {filtro_obra or '-'}", subtitle_style),
        Paragraph(f"<b>Empleado:</b> {filtro_empleado or '-'}", subtitle_style),
    ]
    info_row2 = [
        Paragraph(f"<b>Mes:</b> {mes_label}", subtitle_style),
        Paragraph(f"<b>Total HS:</b> {total_horas:.1f}", subtitle_style),
        Paragraph(f"<b>Registros:</b> {total_registros}", subtitle_style),
    ]
    info_table = Table([info_row1, info_row2], colWidths=[75 * mm, 75 * mm, 75 * mm])
    info_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#f8fafc')),
        ('BOX', (0, 0), (-1, -1), 0.6, colors.HexColor('#cbd5e1')),
        ('INNERGRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#e2e8f0')),
        ('LEFTPADDING', (0, 0), (-1, -1), 8),
        ('RIGHTPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
    ]))
    story.append(info_table)
    story.append(Spacer(1, 10))

    table_data = [['OPERARIO', 'OBRA', 'OT', 'DESCRIPCION OT', 'ACTIVIDAD', 'REG.', 'HS']]
    filas_semana = []
    for fecha in sorted(reportes_por_semana.keys(), reverse=True):
        reps_semana = reportes_por_semana[fecha]

        total_empleados = len(reps_semana)
        horas_semana = sum(float(r[6] or 0) for rows in reps_semana.values() for r in rows)
        registros_semana = sum(len(rows) for rows in reps_semana.values())
        filas_semana.append(len(table_data))
        table_data.append([
            Paragraph(
                f"<b>SEMANA DEL {fecha} | {total_empleados} empleados | {registros_semana} registros | {horas_semana:.1f} HS</b>",
                cell_style,
            ),
            '', '', '', '', '', ''
        ])

        for operario in sorted(reps_semana.keys()):
            for rep in reps_semana[operario]:
                table_data.append([
                    Paragraph(str(operario or '---'), cell_style),
                    Paragraph(str(rep[2] or '---'), cell_style),
                    Paragraph(str(rep[3] or '-'), cell_style),
                    Paragraph(str(rep[4] or '---'), cell_style),
                    Paragraph(str(rep[5] or '---'), cell_style),
                    Paragraph('1', cell_style),
                    Paragraph(f"{float(rep[6] or 0):.1f}", cell_style),
                ])

    table = Table(
        table_data,
        colWidths=[32 * mm, 30 * mm, 14 * mm, 58 * mm, 82 * mm, 12 * mm, 12 * mm],
        repeatRows=1,
    )
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#ff9800')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
        ('ALIGN', (2, 0), (2, -1), 'CENTER'),
        ('ALIGN', (5, 0), (-1, -1), 'RIGHT'),
        ('BOX', (0, 0), (-1, -1), 0.5, colors.HexColor('#ffcc99')),
        ('INNERGRID', (0, 0), (-1, -1), 0.3, colors.HexColor('#ffe0b2')),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fff3e0')]),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ('LEFTPADDING', (0, 0), (-1, -1), 4),
        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
    ]))

    if filas_semana:
        week_style_cmds = []
        for fila in filas_semana:
            week_style_cmds.extend([
                ('SPAN', (0, fila), (6, fila)),
                ('BACKGROUND', (0, fila), (6, fila), colors.HexColor('#f7d7b4')),
                ('TEXTCOLOR', (0, fila), (6, fila), colors.HexColor('#7a4b12')),
                ('FONTNAME', (0, fila), (6, fila), 'Helvetica-Bold'),
                ('FONTSIZE', (0, fila), (6, fila), 9),
                ('ALIGN', (0, fila), (6, fila), 'LEFT'),
                ('TOPPADDING', (0, fila), (6, fila), 6),
                ('BOTTOMPADDING', (0, fila), (6, fila), 6),
            ])
        table.setStyle(TableStyle(week_style_cmds))
    story.append(table)
    story.append(Spacer(1, 12))

    footer_style = ParagraphStyle(
        'Footer',
        parent=styles['Normal'],
        fontSize=8,
        textColor=colors.HexColor('#9ca3af'),
        alignment=0,
    )
    story.append(Paragraph(f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}", footer_style))

    doc.build(story)
    pdf_buffer.seek(0)

    filename = f"Parte_Semanal_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    return send_file(
        pdf_buffer,
        mimetype='application/pdf',
        as_attachment=True,
        download_name=filename,
    )

# ======================
# MÓDULO 4 - REMITOS (Placeholder)
# ======================
@app.route("/modulo/remito", methods=["GET", "POST"])
def remitos():
    db = get_db()
    
    if request.method == "POST":
        ot_id = request.form.get("ot_id")
        fecha_remito = request.form.get("fecha_remito")
        transporte = request.form.get("transporte", "")
        piezas_ids = request.form.getlist("piezas")
        manual_articulos = request.form.getlist("manual_articulo[]")
        manual_cantidades_raw = request.form.getlist("manual_cantidad[]")
        manual_observaciones_list = request.form.getlist("manual_observaciones[]")

        manual_items = []
        total_manual_rows = max(len(manual_articulos), len(manual_cantidades_raw), len(manual_observaciones_list))
        for i in range(total_manual_rows):
            articulo = (manual_articulos[i] if i < len(manual_articulos) else "").strip()
            cantidad_raw = (manual_cantidades_raw[i] if i < len(manual_cantidades_raw) else "").strip()
            observaciones = (manual_observaciones_list[i] if i < len(manual_observaciones_list) else "").strip()

            if not articulo:
                continue

            try:
                cantidad = int(float(cantidad_raw)) if cantidad_raw else 1
            except Exception:
                cantidad = 1

            if cantidad < 1:
                cantidad = 1

            manual_items.append({
                "articulo": articulo,
                "cantidad": cantidad,
                "observaciones": observaciones
            })

        # Compatibilidad con carga manual anterior (campos simples)
        if not manual_items:
            manual_articulo_simple = (request.form.get("manual_articulo", "") or "").strip()
            manual_cantidad_simple_raw = (request.form.get("manual_cantidad", "") or "").strip()
            manual_observaciones_simple = (request.form.get("manual_observaciones", "") or "").strip()
            if manual_articulo_simple:
                try:
                    manual_cantidad_simple = int(float(manual_cantidad_simple_raw)) if manual_cantidad_simple_raw else 1
                except Exception:
                    manual_cantidad_simple = 1
                if manual_cantidad_simple < 1:
                    manual_cantidad_simple = 1
                manual_items.append({
                    "articulo": manual_articulo_simple,
                    "cantidad": manual_cantidad_simple,
                    "observaciones": manual_observaciones_simple
                })
        
        if not ot_id or not fecha_remito:
            return "Faltan datos requeridos", 400
        if not piezas_ids and not manual_items:
            return "Debe seleccionar al menos una pieza o cargar un articulo manual", 400
        
        # Obtener datos de OT
        ot = db.execute("SELECT cliente, obra FROM ordenes_trabajo WHERE id = ?", (ot_id,)).fetchone()
        
        if not ot:
            return "OT no encontrada", 404
        
        # Generar PDF
        try:
            from datetime import datetime

            # Numero consecutivo de remito (R-000001, R-000002, ...)
            next_remito = db.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM remitos").fetchone()[0]
            remito_code = f"R-{int(next_remito):06d}"

            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            pdf_base = f"remito_{remito_code}_{timestamp}.pdf"
            pdf_filename = os.path.join(REMITOS_DIR, pdf_base)

            doc = SimpleDocTemplate(
                pdf_filename,
                pagesize=landscape(letter),
                leftMargin=18,
                rightMargin=18,
                topMargin=18,
                bottomMargin=18
            )
            story = []
            styles = getSampleStyleSheet()

            title_style = ParagraphStyle(
                'RemitoTitle',
                parent=styles['Heading1'],
                fontSize=18,
                leading=22,
                textColor=colors.HexColor('#1f2937'),
                spaceAfter=4
            )
            subtitle_style = ParagraphStyle(
                'RemitoSubTitle',
                parent=styles['Normal'],
                fontSize=11,
                leading=14,
                textColor=colors.HexColor('#4b5563')
            )
            cell_style = ParagraphStyle(
                'CellWrap',
                parent=styles['Normal'],
                fontSize=8,
                leading=10
            )

            logo_path = os.path.join(APP_DIR, "LOGO.png")
            logo_flow = ""
            if os.path.exists(logo_path):
                logo_flow = Image(logo_path, width=40*mm, height=22*mm)

            header_right = Paragraph(
                f"<b>REMITO DE ENTREGA</b><br/><font size='11'>Remito N.&deg; {remito_code}</font>",
                title_style
            )
            header_table = Table([[logo_flow, header_right]], colWidths=[45*mm, 210*mm])
            header_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('ALIGN', (0, 0), (0, 0), 'LEFT'),
                ('ALIGN', (1, 0), (1, 0), 'LEFT'),
                ('LEFTPADDING', (0, 0), (-1, -1), 0),
                ('RIGHTPADDING', (0, 0), (-1, -1), 0),
                ('TOPPADDING', (0, 0), (-1, -1), 0),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ]))
            story.append(header_table)

            total_items = len(piezas_ids) + len(manual_items)
            info_rows = [
                [
                    Paragraph(f"<b>Cliente:</b> {ot[0]}", subtitle_style),
                    Paragraph(f"<b>OT:</b> {ot_id}", subtitle_style),
                    Paragraph(f"<b>Fecha:</b> {fecha_remito}", subtitle_style)
                ],
                [
                    Paragraph(f"<b>Obra:</b> {ot[1]}", subtitle_style),
                    Paragraph(f"<b>Transporte:</b> {transporte or '-'}", subtitle_style),
                    Paragraph(f"<b>Cant. de Items:</b> {total_items}", subtitle_style)
                ]
            ]
            info_table = Table(info_rows, colWidths=[95*mm, 85*mm, 75*mm])
            info_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#f8fafc')),
                ('BOX', (0, 0), (-1, -1), 0.6, colors.HexColor('#cbd5e1')),
                ('INNERGRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#e2e8f0')),
                ('LEFTPADDING', (0, 0), (-1, -1), 8),
                ('RIGHTPADDING', (0, 0), (-1, -1), 8),
                ('TOPPADDING', (0, 0), (-1, -1), 5),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ]))
            story.append(info_table)
            story.append(Spacer(1, 10))

            table_data = [['POS.', 'PERFIL', 'PESO', 'TOTAL', 'ENVIADO', 'DESCRIPCION', 'OBSERVACIONES']]
            total_enviado_sum = 0

            for pieza_id in piezas_ids:
                pieza = db.execute("""
                    SELECT p_despacho.id,
                           p_first.posicion,
                           p_first.obra,
                           COALESCE(p_first.cantidad, ''),
                           COALESCE(p_first.perfil, ''),
                           COALESCE(p_first.peso, ''),
                           COALESCE(p_first.descripcion, '')
                    FROM procesos p_despacho
                    LEFT JOIN procesos p_first ON p_despacho.posicion = p_first.posicion
                                               AND p_despacho.obra = p_first.obra
                                               AND p_first.id = (
                                                   SELECT MIN(id) FROM procesos
                                                   WHERE posicion = p_despacho.posicion
                                                     AND obra = p_despacho.obra
                                               )
                    WHERE p_despacho.id = ?
                """, (pieza_id,)).fetchone()

                if pieza:
                    posicion = str(pieza[1]) if pieza[1] else ''
                    cantidad_total = int(float(pieza[3]) if pieza[3] else 0)
                    perfil = str(pieza[4]) if pieza[4] else ''
                    peso = str(pieza[5]) if pieza[5] else ''
                    descripcion = str(pieza[6]) if pieza[6] else ''
                    observaciones = request.form.get(f"obs_{pieza_id}", "")

                    cantidad_enviada = request.form.get(f"cant_{pieza_id}", str(cantidad_total))
                    try:
                        cantidad_enviada = int(cantidad_enviada)
                    except Exception:
                        cantidad_enviada = cantidad_total

                    if cantidad_enviada < 0:
                        cantidad_enviada = 0
                    if cantidad_enviada > cantidad_total:
                        cantidad_enviada = cantidad_total

                    total_enviado_sum += cantidad_enviada
                    enviado_display = f"{cantidad_enviada} de {cantidad_total}"

                    table_data.append([
                        Paragraph(posicion, cell_style),
                        Paragraph(perfil, cell_style),
                        Paragraph(peso, cell_style),
                        Paragraph(str(cantidad_total), cell_style),
                        Paragraph(enviado_display, cell_style),
                        Paragraph(descripcion, cell_style),
                        Paragraph(observaciones or '-', cell_style)
                    ])

            for idx, item in enumerate(manual_items, start=1):
                manual_articulo = item["articulo"]
                manual_cantidad = item["cantidad"]
                manual_observaciones = item["observaciones"]
                total_enviado_sum += manual_cantidad
                table_data.append([
                    Paragraph(f"MANUAL {idx}", cell_style),
                    Paragraph("-", cell_style),
                    Paragraph("-", cell_style),
                    Paragraph(str(manual_cantidad), cell_style),
                    Paragraph(f"{manual_cantidad} de {manual_cantidad}", cell_style),
                    Paragraph(manual_articulo, cell_style),
                    Paragraph(manual_observaciones or '-', cell_style)
                ])

            table = Table(
                table_data,
                colWidths=[20*mm, 32*mm, 18*mm, 16*mm, 25*mm, 68*mm, 75*mm],
                repeatRows=1
            )
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f97316')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 9),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('ALIGN', (0, 0), (0, -1), 'CENTER'),
                ('ALIGN', (2, 0), (4, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('GRID', (0, 0), (-1, -1), 0.4, colors.HexColor('#cbd5e1')),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#fff7ed')]),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('LEFTPADDING', (0, 0), (-1, -1), 5),
                ('RIGHTPADDING', (0, 0), (-1, -1), 5),
            ]))
            story.append(table)
            story.append(Spacer(1, 10))

            resumen = Table(
                [[
                    Paragraph(f"<b>Total enviado:</b> {total_enviado_sum} unidades", subtitle_style),
                    Paragraph(f"<b>Remito:</b> {remito_code}", subtitle_style)
                ]],
                colWidths=[170*mm, 85*mm]
            )
            resumen.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#f8fafc')),
                ('BOX', (0, 0), (-1, -1), 0.5, colors.HexColor('#cbd5e1')),
                ('LEFTPADDING', (0, 0), (-1, -1), 8),
                ('RIGHTPADDING', (0, 0), (-1, -1), 8),
                ('TOPPADDING', (0, 0), (-1, -1), 5),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
            ]))
            story.append(resumen)
            story.append(Spacer(1, 18))

            firma_table = Table(
                [[
                    Paragraph("<b>Responsable de Entrega</b><br/><br/>____________________________", subtitle_style),
                    Paragraph("<b>Recibido Por</b><br/><br/>____________________________", subtitle_style)
                ]],
                colWidths=[125*mm, 125*mm]
            )
            firma_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('LEFTPADDING', (0, 0), (-1, -1), 0),
                ('RIGHTPADDING', (0, 0), (-1, -1), 0),
                ('TOPPADDING', (0, 0), (-1, -1), 0),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
            ]))
            story.append(firma_table)

            doc.build(story)

            try:
                with open(pdf_filename, "rb") as f_pdf:
                    _guardar_pdf_databook(ot[1], "remitos", pdf_base, f_pdf.read())
            except Exception:
                pass

            material_entregado_value = ','.join(piezas_ids)
            for item in manual_items:
                manual_tag = f"MANUAL:{item['articulo']}"
                material_entregado_value = f"{material_entregado_value},{manual_tag}" if material_entregado_value else manual_tag

            db.execute("""
                INSERT INTO remitos (cliente, ot_id, material_entregado, cantidad, fecha, pdf_path)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (ot[0], ot_id, material_entregado_value, total_enviado_sum, fecha_remito, pdf_base))
            db.commit()

            return redirect(f"/descargar-remito/{pdf_base}")
        except Exception as e:
            return f"Error generando PDF: {str(e)}", 500
    
    
    # Obtener OT disponibles
    ots = db.execute("SELECT id, cliente, obra FROM ordenes_trabajo WHERE estado != 'Finalizada' AND fecha_cierre IS NULL ORDER BY id DESC").fetchall()
    obras_disponibles = sorted({str(ot[2] or "").strip() for ot in ots if str(ot[2] or "").strip()})
    ots_por_obra = {}
    for ot in ots:
        obra_txt = str(ot[2] or "").strip()
        if not obra_txt:
            continue
        ots_por_obra.setdefault(obra_txt, []).append({
            "id": int(ot[0]),
            "cliente": str(ot[1] or ""),
            "obra": obra_txt,
        })
    ots_por_obra_json = json.dumps(ots_por_obra, ensure_ascii=False).replace("</", "<\\/")
    remitos_list = db.execute("SELECT * FROM remitos ORDER BY fecha_creacion DESC LIMIT 15").fetchall()
    
    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * { box-sizing: border-box; }
    body { font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }
    h2 { color: #333; border-bottom: 3px solid #f093fb; padding-bottom: 10px; }
    .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
    .header-actions { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
    .btn { background: #667eea; color: white; padding: 10px 15px; text-decoration: none; border-radius: 5px; }
    .btn-despacho { background: #f97316; }
    .btn-despacho:hover { background: #ea580c; }
    form { background: white; padding: 20px; border-radius: 5px; margin: 20px 0; max-width: 1200px; }
    .form-group { margin-bottom: 15px; }
    label { display: block; font-weight: bold; margin-bottom: 5px; }
    input[type="text"], input[type="date"], select { width: 100%; padding: 10px; border: 1px solid #ddd; border-radius: 4px; }
    button { width: 100%; padding: 12px; background: #43e97b; color: white; border: none; border-radius: 4px; cursor: pointer; font-weight: bold; font-size: 16px; }
    button:hover { background: #2cc96e; }
    
    /* Tabla de piezas mejorada */
    .piezas-table-wrapper { margin: 20px 0; overflow-x: auto; }
    .piezas-table { width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    .piezas-table th { background: #667eea; color: white; padding: 12px; text-align: left; font-weight: bold; border-bottom: 2px solid #556bd7; }
    .piezas-table td { padding: 12px; border-bottom: 1px solid #e0e0e0; }
    .piezas-table tr:hover { background: #f9f9f9; }
    .piezas-table input[type="checkbox"] { margin-right: 8px; cursor: pointer; width: 18px; height: 18px; }
    .pieza-row { background: white; }
    .pieza-row textarea { width: 100%; padding: 8px; border: 1px solid #ddd; border-radius: 3px; font-family: Arial; font-size: 12px; resize: vertical; min-height: 40px; }
    .cantidad-cell, .perfil-cell, .peso-cell { text-align: center; font-size: 13px; }
    .descripcion-cell { max-width: 150px; }
    .cantidad-input { width: 80px; padding: 6px; border: 1px solid #ddd; border-radius: 3px; text-align: center; font-size: 13px; }
    .cantidad-info { font-size: 12px; color: #666; font-weight: bold; }
    .manual-item-box { background: #fff7ed; border: 1px solid #fed7aa; border-radius: 6px; padding: 12px; margin-top: 8px; }
    .manual-item-grid { display: grid; grid-template-columns: 2fr 1fr; gap: 10px; }
    .manual-item-grid textarea { width: 100%; min-height: 70px; padding: 8px; border: 1px solid #ddd; border-radius: 4px; resize: vertical; }
    .manual-item-row { border: 1px dashed #fdba74; border-radius: 6px; padding: 10px; margin-bottom: 10px; background: #fffbeb; }
    .manual-item-actions { display: flex; justify-content: flex-end; gap: 8px; margin-top: 8px; }
    .manual-item-title { font-size: 13px; font-weight: bold; color: #9a3412; margin-bottom: 8px; }
    .btn-manual { border: none; border-radius: 4px; color: white; padding: 7px 10px; cursor: pointer; font-size: 12px; }
    .btn-add-manual { background: #16a34a; }
    .btn-remove-manual { background: #dc2626; }
    
    /* Tabla de remitos generados */
    table { width: 100%; border-collapse: collapse; background: white; margin-top: 20px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }
    th, td { padding: 10px; border-bottom: 1px solid #ddd; text-align: left; }
    th { background: #f093fb; color: white; font-weight: bold; }
    tr:hover { background: #f5f5f5; }
    .btn-small { display: inline-block; padding: 5px 10px; font-size: 12px; width: auto; }
    .btn-download { background: #4facfe; text-decoration: none; border-radius: 3px; color: white; }
    .btn-delete { background: #ef4444; text-decoration: none; border-radius: 3px; color: white; margin-left: 6px; }
    
    .error { color: #d32f2f; margin: 10px 0; padding: 10px; background: #ffebee; border-radius: 3px; }
    .success { color: #388e3c; margin: 10px 0; padding: 10px; background: #e8f5e9; border-radius: 3px; }
    </style>
    </head>
    <body>
    <div class="header">
        <h2>📦 Generador de Remitos</h2>
        <div class="header-actions">
            <a href="/modulo/calidad/despacho" class="btn btn-despacho">📦 Ir a Control Despacho (Formulario)</a>
            <a href="/" class="btn">⬅️ Volver</a>
        </div>
    </div>
    
    <form method="post" id="remito-form">
        <div class="form-group">
            <label>Obra:</label>
            <select id="obra-select" onchange="cargarOTs()" required>
                <option value="">Seleccionar obra...</option>
    """

    for obra in obras_disponibles:
        html += f'<option value="{html_lib.escape(obra)}">{html_lib.escape(obra)}</option>'
    
    html += """
            </select>
        </div>

        <div class="form-group">
            <label>Orden de Trabajo:</label>
            <select name="ot_id" id="ot-select" onchange="cargarPiezas()" required disabled>
                <option value="">Seleccionar OT...</option>
            </select>
        </div>
        
        <div class="form-group">
            <label>Fecha de Remito:</label>
            <input type="date" name="fecha_remito" required>
        </div>
        
        <div class="form-group">
            <label>Transporte:</label>
            <input type="text" name="transporte" placeholder="Ej: Empresa XYZ, Auto particular, etc.">
        </div>

        <div class="form-group">
            <label><b>✓ Piezas Aprobadas en Despacho:</b></label>
            <div class="piezas-table-wrapper" id="piezas-container">
                <p style="color: #999; padding: 20px;">Selecciona una OT primero...</p>
            </div>
        </div>

        <div class="form-group">
            <label><b>➕ Carga Manual (pieza u otro articulo):</b></label>
            <div class="manual-item-box">
                <div id="manual-items-container">
                    <div class="manual-item-row">
                        <div class="manual-item-title">Articulo manual #1</div>
                        <div class="manual-item-grid">
                            <div>
                                <label>Articulo / Descripcion:</label>
                                <input type="text" name="manual_articulo[]" placeholder="Ej: Buloneria, placa adicional, insumo, etc.">
                            </div>
                            <div>
                                <label>Cantidad:</label>
                                <input type="number" name="manual_cantidad[]" min="1" value="1" placeholder="1">
                            </div>
                        </div>
                        <div style="margin-top: 10px;">
                            <label>Observaciones del articulo manual:</label>
                            <textarea name="manual_observaciones[]" placeholder="Detalle adicional del articulo cargado manualmente..."></textarea>
                        </div>
                    </div>
                </div>
                <div class="manual-item-actions">
                    <button type="button" class="btn-manual btn-add-manual" onclick="agregarArticuloManual()">+ Agregar otro articulo</button>
                </div>
            </div>
        </div>
        
        <button type="submit">📄 Generar Remito PDF</button>
    </form>
    
    <h2>Remitos Generados</h2>
    <table>
        <tr>
            <th>N° Remito</th>
            <th>OT</th>
            <th>Cliente</th>
            <th>Cantidad</th>
            <th>Fecha</th>
            <th>Acciones</th>
        </tr>
    """
    
    for remito in remitos_list:
        pdf_name = os.path.basename(remito[6] or "")
        remito_code = f"R-{int(remito[0]):06d}"
        if pdf_name.startswith("remito_R-"):
            partes = pdf_name.split("_")
            if len(partes) >= 2:
                remito_code = partes[1]

        html += f"""
        <tr>
            <td><b>{remito_code}</b></td>
            <td><b>{remito[2]}</b></td>
            <td>{remito[1]}</td>
            <td>{int(remito[4])}</td>
            <td>{remito[5]}</td>
            <td>
                <a href="/descargar-remito/{quote(pdf_name)}" class="btn btn-small btn-download">📥 Descargar</a>
                <a href="/eliminar-remito/{remito[0]}" class="btn btn-small btn-delete" onclick="return confirm('¿Eliminar este remito? Esta acción no se puede deshacer.');">🗑 Eliminar</a>
            </td>
        </tr>
        """
    
    html += """
    </table>

    <script id="ots-por-obra-json" type="application/json">__OTS_POR_OBRA_JSON__</script>

    <script>
    const otsPorObra = JSON.parse(document.getElementById('ots-por-obra-json').textContent || '{{}}');

    function cargarOTs() {
        const obraSel = document.getElementById('obra-select').value;
        const otSelect = document.getElementById('ot-select');

        otSelect.innerHTML = '<option value="">Seleccionar OT...</option>';
        document.getElementById('piezas-container').innerHTML = '<p style="color: #999; padding: 20px;">Selecciona una OT...</p>';

        if (!obraSel || !otsPorObra[obraSel] || !Array.isArray(otsPorObra[obraSel]) || otsPorObra[obraSel].length === 0) {
            otSelect.disabled = true;
            return;
        }

        otsPorObra[obraSel].forEach(ot => {
            const opt = document.createElement('option');
            opt.value = String(ot.id);
            opt.textContent = `OT ${ot.id} - ${ot.cliente}`;
            otSelect.appendChild(opt);
        });
        otSelect.disabled = false;
    }

    function renumerarArticulosManual() {
        const rows = document.querySelectorAll('#manual-items-container .manual-item-row');
        rows.forEach((row, index) => {
            let title = row.querySelector('.manual-item-title');
            if (!title) {
                title = document.createElement('div');
                title.className = 'manual-item-title';
                row.insertBefore(title, row.firstChild);
            }
            title.textContent = `Articulo manual #${index + 1}`;
        });
    }

    function agregarArticuloManual() {
        const container = document.getElementById('manual-items-container');
        const row = document.createElement('div');
        row.className = 'manual-item-row';
        row.innerHTML = `
            <div class="manual-item-title"></div>
            <div class="manual-item-grid">
                <div>
                    <label>Articulo / Descripcion:</label>
                    <input type="text" name="manual_articulo[]" placeholder="Ej: Buloneria, placa adicional, insumo, etc.">
                </div>
                <div>
                    <label>Cantidad:</label>
                    <input type="number" name="manual_cantidad[]" min="1" value="1" placeholder="1">
                </div>
            </div>
            <div style="margin-top: 10px;">
                <label>Observaciones del articulo manual:</label>
                <textarea name="manual_observaciones[]" placeholder="Detalle adicional del articulo cargado manualmente..."></textarea>
            </div>
            <div class="manual-item-actions">
                <button type="button" class="btn-manual btn-remove-manual" onclick="this.closest('.manual-item-row').remove(); renumerarArticulosManual();">- Quitar</button>
            </div>
        `;
        container.appendChild(row);
        renumerarArticulosManual();
    }
    
    function cargarPiezas() {
        const otId = document.getElementById('ot-select').value;

        if (!otId) {
            document.getElementById('piezas-container').innerHTML = '<p style="color: #999; padding: 20px;">Selecciona una OT...</p>';
            return;
        }
        
        // Cargar piezas vía AJAX
        fetch(`/api/piezas-remito/${otId}`)
            .then(response => response.json())
            .then(data => {
                if (data.error) {
                    console.error('Error del servidor:', data.error);
                    document.getElementById('piezas-container').innerHTML = `<p class="error">Error: ${data.error}</p>`;
                } else if (data.piezas && data.piezas.length > 0) {
                    let html = '<table class="piezas-table"><thead><tr>';
                    html += '<th style="width: 40px;">✓</th>';
                    html += '<th style="width: 100px;">Posición</th>';
                    html += '<th style="width: 140px;">Total</th>';
                    html += '<th style="width: 100px;">A Enviar</th>';
                    html += '<th style="width: 120px;">Perfil</th>';
                    html += '<th style="width: 100px;">Peso</th>';
                    html += '<th style="width: 200px;">Descripción</th>';
                    html += '<th style="width: 250px;">Observaciones</th>';
                    html += '</tr></thead><tbody>';
                    
                    data.piezas.forEach(pieza => {
                        // Convertir cantidad a entero (sin decimales)
                        const cantidadTotal = parseInt(parseFloat(pieza.cantidad) || 0);
                        html += `<tr class="pieza-row">
                            <td><input type="checkbox" name="piezas" value="${pieza.id}" checked></td>
                            <td><strong>${pieza.posicion}</strong></td>
                            <td class="cantidad-info">${cantidadTotal} unidades</td>
                            <td><input type="number" name="cant_${pieza.id}" class="cantidad-input" value="${cantidadTotal}" min="0" max="${cantidadTotal}" placeholder="0"></td>
                            <td class="perfil-cell">${pieza.perfil}</td>
                            <td class="peso-cell">${pieza.peso}</td>
                            <td class="descripcion-cell">${pieza.descripcion}</td>
                            <td><textarea name="obs_${pieza.id}" placeholder="Observaciones..."></textarea></td>
                        </tr>`;
                    });
                    
                    html += '</tbody></table>';
                    document.getElementById('piezas-container').innerHTML = html;
                } else {
                    document.getElementById('piezas-container').innerHTML = '<p class="error">No hay piezas disponibles</p>';
                }
            })
            .catch(err => {
                console.error('Error cargando piezas:', err);
                document.getElementById('piezas-container').innerHTML = `<p class="error">Error: ${err.message}</p>`;
            });
    }

            renumerarArticulosManual();
    </script>
    </body>
    </html>
    """
    html = html.replace("__OTS_POR_OBRA_JSON__", ots_por_obra_json)
    return html

# ======================
# MÓDULO 5 - ESTADO DE PRODUCCIÓN (Dashboard interactivo)
# ======================
@app.route("/modulo/estado")
def estado_produccion():
    html = """<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Estado de Producción</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/html2canvas@1.4.1/dist/html2canvas.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/jspdf@2.5.1/dist/jspdf.umd.min.js"></script>
<style>
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    font-family: 'Segoe UI', Arial, sans-serif;
    background: linear-gradient(135deg, #fff4e6 0%, #ffe4c7 50%, #ffd0a8 100%);
    min-height: 100vh;
    padding: 20px;
}
.container { max-width: 1300px; margin: 0 auto; }
.top-bar {
    display: flex; justify-content: space-between; align-items: center;
    background: rgba(255,255,255,0.92); border-radius: 14px; padding: 16px 22px;
    border: 1px solid #fdba74; box-shadow: 0 6px 20px rgba(154,52,18,0.1);
    margin-bottom: 20px;
}
.top-title { display: flex; align-items: center; gap: 12px; }
.top-title img {
    width: 58px;
    height: 34px;
    object-fit: contain;
    border-radius: 6px;
    background: #fff;
    border: 1px solid #fed7aa;
    padding: 2px;
}
.top-bar h2 { color: #7c2d12; font-size: 1.45em; }
.btn {
    display: inline-block; background: #f97316; color: white;
    padding: 9px 18px; border-radius: 8px; text-decoration: none;
    font-weight: bold; font-size: 0.9em;
}
.btn:hover { background: #ea580c; }
.period-bar {
    display: flex; gap: 10px; margin-bottom: 20px; flex-wrap: wrap;
    background: rgba(255,255,255,0.88); border-radius: 12px;
    padding: 14px 18px; border: 1px solid #fdba74;
    box-shadow: 0 4px 12px rgba(154,52,18,0.08);
    align-items: center;
}
.period-bar > span { font-weight: bold; color: #9a3412; margin-right: 6px; }
.period-btn {
    padding: 9px 24px; border: 2px solid #f97316; border-radius: 22px;
    background: white; color: #f97316; font-weight: bold; cursor: pointer;
    font-size: 0.9em; transition: all 0.18s;
}
.period-btn.active, .period-btn:hover { background: #f97316; color: white; }
.filtro-tipo {
    padding: 8px 10px;
    border: 1px solid #fdba74;
    border-radius: 8px;
    color: #7c2d12;
    background: #fff;
    font-weight: 600;
}
.tipo-desc {
    width: 100%;
    margin-top: 8px;
    color: #9a3412;
    font-size: 0.8em;
}
.fecha-desde { margin-left: auto; color: #9a3412; font-size: 0.85em; font-style: italic; }
.kpi-row {
    display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
    gap: 16px; margin-bottom: 20px;
}
.kpi-card {
    background: white; border-radius: 12px; padding: 18px 14px;
    box-shadow: 0 4px 12px rgba(154,52,18,0.1);
    border-left: 5px solid #f97316; text-align: center;
}
.kpi-valor { font-size: 1.9em; font-weight: bold; color: #ea580c; }
.kpi-label { font-size: 0.82em; color: #9a3412; margin-top: 5px; }
.chart-full {
    background: white; border-radius: 14px; padding: 22px;
    box-shadow: 0 6px 18px rgba(154,52,18,0.1); border: 1px solid #ffedd5;
    margin-bottom: 20px;
}
.chart-full h3 {
    color: #7c2d12; margin-bottom: 16px; font-size: 1.1em;
    border-bottom: 2px solid #ffedd5; padding-bottom: 8px;
}
.chart-full canvas { max-height: 360px; }
.charts-row {
    display: grid; grid-template-columns: 1fr 1fr;
    gap: 20px; margin-bottom: 20px;
}
.chart-card {
    background: white; border-radius: 14px; padding: 22px;
    box-shadow: 0 6px 18px rgba(154,52,18,0.1); border: 1px solid #ffedd5;
}
.chart-card h3 {
    color: #7c2d12; margin-bottom: 16px; font-size: 1.1em;
    border-bottom: 2px solid #ffedd5; padding-bottom: 8px;
}
.chart-card canvas { max-height: 300px; }
.no-data-msg {
    text-align: center; padding: 30px; color: #9a3412;
    background: #fff7ed; border-radius: 8px; font-style: italic;
}
.btn-pdf {
    background: #7c2d12; display: inline-flex; align-items: center; gap: 6px;
}
.btn-pdf:hover { background: #9a3412; }
.pdf-export .top-title img {
    width: 96px;
    height: 56px;
}
.pdf-export .top-actions {
    display: none !important;
}
@media print {
    .top-actions {
        display: none !important;
    }
    .top-title img {
        width: 96px;
        height: 56px;
    }
}
@media (max-width: 768px) { .charts-row { grid-template-columns: 1fr; } }
</style>
</head>
<body>
<div class="container">

  <div class="top-bar">
        <div class="top-title">
            <img src="/logo-a3" alt="Logo empresa">
            <h2>📊 Estado de Producción</h2>
        </div>
        <div class="top-actions" style="display:flex;gap:10px;">
                        <a id="btn-pdf" href="#" onclick="exportarVistaPDF(); return false;" class="btn btn-pdf">📄 Generar reporte PDF</a>
      <a href="/" class="btn">⬅️ Volver</a>
    </div>
  </div>

  <div class="period-bar">
    <span>🗓 Período:</span>
    <button class="period-btn" onclick="cambiarPeriodo(this,'semana')">Semana</button>
    <button class="period-btn active" onclick="cambiarPeriodo(this,'mes')">Mes</button>
    <button class="period-btn" onclick="cambiarPeriodo(this,'trimestre')">Trimestre</button>
        <span style="margin-left: 10px; font-weight: bold; color: #9a3412;">Tipo Estructura:</span>
        <select id="filtro-tipo-obra" class="filtro-tipo" onchange="cambiarTipoObra()">
            <option value="">Todos</option>
            <option value="TIPO I">TIPO I</option>
            <option value="TIPO II">TIPO II</option>
            <option value="TIPO III">TIPO III</option>
        </select>
        <span style="margin-left: 10px; font-weight: bold; color: #9a3412;">Obra:</span>
        <select id="filtro-obra" class="filtro-tipo" onchange="cambiarObra()">
            <option value="">Todas</option>
        </select>
    <span class="fecha-desde" id="fecha-desde-txt"></span>
        <div class="tipo-desc" id="tipo-desc-text">Seleccione un tipo para ver la descripción.</div>
  </div>

  <div class="kpi-row">
    <div class="kpi-card">
      <div class="kpi-valor" id="kpi-hs-prev">—</div>
      <div class="kpi-label">HS Previstas (OTs activas)</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-valor" id="kpi-hs-carg">—</div>
            <div class="kpi-label">HS Consumidas (período)</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-valor" id="kpi-eficiencia">—</div>
      <div class="kpi-label">Eficiencia HS (%)</div>
    </div>
    <div class="kpi-card">
      <div class="kpi-valor" id="kpi-kg-total">—</div>
      <div class="kpi-label">KG producidos (período)</div>
    </div>
        <div class="kpi-card">
            <div class="kpi-valor" id="kpi-kg-hs">—</div>
            <div class="kpi-label">KG/HS</div>
        </div>
  </div>

        <div class="chart-full">
                                <h3>⏱ HS Consumidas vs HS Presupuestadas por Obra (OTs agrupadas)</h3>
        <div id="no-data-hs" class="no-data-msg" style="display:none">Sin datos de horas por obra para el período seleccionado.</div>
        <canvas id="chartHS"></canvas>
    </div>

  <div class="charts-row">
    <div class="chart-card">
      <h3>⚖️ KG procesados por Estación</h3>
      <div id="no-data-kg" class="no-data-msg" style="display:none">Sin datos de kg para el período.</div>
      <canvas id="chartKg"></canvas>
    </div>
    <div class="chart-card">
      <h3>📈 Distribución de KG en Planta</h3>
      <canvas id="chartKgDona"></canvas>
    </div>
  </div>

</div>

<script>
let chartHS = null, chartKg = null, chartKgDona = null;

let periodoActivo = 'mes';
let tipoObraActivo = '';
let obraActiva = '';
function cambiarPeriodo(btn, periodo) {
    document.querySelectorAll('.period-btn').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    periodoActivo = periodo;
    cargarDatos(periodo);
}

function cambiarTipoObra() {
    tipoObraActivo = document.getElementById('filtro-tipo-obra').value || '';
    actualizarDescripcionTipo(tipoObraActivo);
    cargarDatos(periodoActivo);
}

function cambiarObra() {
    obraActiva = document.getElementById('filtro-obra').value || '';
    cargarDatos(periodoActivo);
}

function actualizarDescripcionTipo(tipo) {
    const el = document.getElementById('tipo-desc-text');
    const descripciones = {
        'TIPO I': 'TIPO I: Trabajos de herreria menores.',
        'TIPO II': 'TIPO II: Estructuras metalicas pesadas.',
        'TIPO III': 'TIPO III: Elementos metalicos en serie.'
    };
    el.textContent = descripciones[tipo] || 'Seleccione un tipo para ver la descripcion.';
}

async function exportarVistaPDF() {
    const btn = document.getElementById('btn-pdf');
    const objetivo = document.querySelector('.container');
    const textoOriginal = btn.textContent;
    btn.textContent = '⏳ Generando...';
    objetivo.classList.add('pdf-export');

    try {
        const canvas = await html2canvas(objetivo, {
            scale: 2,
            useCORS: true,
            backgroundColor: '#fff4e6'
        });

        const { jsPDF } = window.jspdf;
        const pdf = new jsPDF({
            orientation: 'landscape',
            unit: 'mm',
            format: 'a3'
        });

        const pageW = pdf.internal.pageSize.getWidth();
        const pageH = pdf.internal.pageSize.getHeight();
        const margin = 8;
        const maxW = pageW - margin * 2;
        const maxH = pageH - margin * 2;

        const imgW = canvas.width;
        const imgH = canvas.height;
        const ratio = Math.min(maxW / imgW, maxH / imgH);
        const drawW = imgW * ratio;
        const drawH = imgH * ratio;
        const x = (pageW - drawW) / 2;
        const y = (pageH - drawH) / 2;

        const imgData = canvas.toDataURL('image/png', 1.0);
        pdf.addImage(imgData, 'PNG', x, y, drawW, drawH, undefined, 'FAST');

        const fecha = new Date();
        const yyyymmdd = fecha.getFullYear().toString() +
            String(fecha.getMonth() + 1).padStart(2, '0') +
            String(fecha.getDate()).padStart(2, '0');
        pdf.save('estado_produccion_pantalla_' + periodoActivo + '_' + yyyymmdd + '.pdf');
    } catch (err) {
        alert('No se pudo generar el PDF de pantalla.');
        console.error(err);
    } finally {
        objetivo.classList.remove('pdf-export');
        btn.textContent = textoOriginal;
    }
}

function cargarDatos(periodo) {
    const params = new URLSearchParams({ periodo: periodo, tipo_obra: tipoObraActivo, obra: obraActiva });
    fetch('/api/dashboard-estado?' + params.toString())
        .then(r => r.json())
        .then(data => renderDashboard(data))
        .catch(err => console.error('Error:', err));
}

function renderDashboard(data) {
    const fd = data.fecha_desde.split('-');
    document.getElementById('fecha-desde-txt').textContent =
        'Desde: ' + fd[2] + '/' + fd[1] + '/' + fd[0];

    const hs = data.hs_por_ot;
    const hsObra = data.hs_por_obra || [];
    const kg = data.kg_por_estacion;

    // Filtro por obra: mantener opciones sincronizadas con backend
    const filtroObra = document.getElementById('filtro-obra');
    if (filtroObra) {
        const obras = data.obras_disponibles || [];
        const valorActual = obraActiva || '';
        filtroObra.innerHTML = '<option value="">Todas</option>';
        obras.forEach(o => {
            const opt = document.createElement('option');
            opt.value = o;
            opt.textContent = o;
            if (o === valorActual) opt.selected = true;
            filtroObra.appendChild(opt);
        });
    }

    // KPIs
    const totalPrev = hs.reduce((s, o) => s + o.hs_previstas, 0);
    const totalCarg = hs.reduce((s, o) => s + o.hs_cargadas, 0);
    const totalKg   = Object.values(kg).reduce((s, v) => s + v, 0);
    const efic      = totalPrev > 0 ? ((totalCarg / totalPrev) * 100).toFixed(1) : '—';
    const kgHs      = totalCarg > 0 ? (totalKg / totalCarg).toFixed(1) : '—';

    document.getElementById('kpi-hs-prev').textContent  = totalPrev.toFixed(1) + ' hs';
    document.getElementById('kpi-hs-carg').textContent  = totalCarg.toFixed(1) + ' hs';
    document.getElementById('kpi-eficiencia').textContent = efic !== '—' ? efic + '%' : '—';
    document.getElementById('kpi-kg-total').textContent = totalKg.toFixed(1) + ' kg';
    document.getElementById('kpi-kg-hs').textContent = kgHs !== '—' ? kgHs + ' kg/hs' : '—';

    // === Chart HS por OBRA (OTs agrupadas) ===
    if (chartHS) chartHS.destroy();
    if (hsObra.length === 0) {
        document.getElementById('no-data-hs').style.display = 'block';
        document.getElementById('chartHS').style.display = 'none';
    } else {
        document.getElementById('no-data-hs').style.display = 'none';
        document.getElementById('chartHS').style.display = 'block';
        chartHS = new Chart(document.getElementById('chartHS'), {
            type: 'bar',
            data: {
                labels: hsObra.map(o => o.label),
                datasets: [
                    {
                        label: 'HS Presupuestadas',
                        data: hsObra.map(o => o.hs_previstas),
                        backgroundColor: 'rgba(253,186,116,0.85)',
                        borderColor: '#f97316',
                        borderWidth: 2,
                        borderRadius: 5
                    },
                    {
                        label: 'HS Consumidas',
                        data: hsObra.map(o => o.hs_cargadas),
                        backgroundColor: 'rgba(234,88,12,0.85)',
                        borderColor: '#c2410c',
                        borderWidth: 2,
                        borderRadius: 5
                    }
                ]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: { position: 'top' },
                    tooltip: { callbacks: { label: ctx => ctx.dataset.label + ': ' + ctx.parsed.y.toFixed(1) + ' hs' } }
                },
                scales: {
                    y: { beginAtZero: true, title: { display: true, text: 'Horas' } },
                    x: { ticks: { maxRotation: 35, minRotation: 10 } }
                }
            }
        });
    }

    // === Chart KG bar ===
    const estaciones = ['ARMADO', 'SOLDADURA', 'PINTURA', 'DESPACHO'];
    const colores = ['rgba(249,115,22,0.85)', 'rgba(234,88,12,0.85)', 'rgba(194,65,12,0.85)', 'rgba(124,45,18,0.85)'];
    const kgVals = estaciones.map(e => kg[e] || 0);
    const hayKg = kgVals.some(v => v > 0);

    if (chartKg) chartKg.destroy();
    if (!hayKg) {
        document.getElementById('no-data-kg').style.display = 'block';
        document.getElementById('chartKg').style.display = 'none';
    } else {
        document.getElementById('no-data-kg').style.display = 'none';
        document.getElementById('chartKg').style.display = 'block';
        chartKg = new Chart(document.getElementById('chartKg'), {
            type: 'bar',
            data: {
                labels: estaciones,
                datasets: [{
                    label: 'KG',
                    data: kgVals,
                    backgroundColor: colores,
                    borderWidth: 2,
                    borderRadius: 8
                }]
            },
            options: {
                responsive: true,
                plugins: {
                    legend: { display: false },
                    tooltip: { callbacks: { label: ctx => ctx.parsed.y.toFixed(1) + ' kg' } }
                },
                scales: { y: { beginAtZero: true, title: { display: true, text: 'KG' } } }
            }
        });
    }

    // === Chart KG dona ===
    if (chartKgDona) chartKgDona.destroy();
    chartKgDona = new Chart(document.getElementById('chartKgDona'), {
        type: 'doughnut',
        data: {
            labels: estaciones,
            datasets: [{
                data: kgVals,
                backgroundColor: ['#f97316', '#ea580c', '#c2410c', '#7c2d12'],
                borderWidth: 2,
                borderColor: '#fff'
            }]
        },
        options: {
            responsive: true,
            cutout: '60%',
            plugins: {
                legend: { position: 'bottom' },
                tooltip: { callbacks: { label: ctx => ctx.label + ': ' + ctx.parsed.toFixed(1) + ' kg' } }
            }
        }
    });
}

cargarDatos('mes');
actualizarDescripcionTipo(tipoObraActivo);
</script>
</body>
</html>
"""
    return html

# ======================
# API DASHBOARD ESTADO DE PRODUCCIÓN
# ======================
@app.route("/api/dashboard-estado")
def api_dashboard_estado():
    from datetime import date, timedelta
    periodo = request.args.get("periodo", "mes")
    tipo_obra = (request.args.get("tipo_obra") or "").strip().upper()
    obra = (request.args.get("obra") or "").strip()

    today = date.today()
    if periodo == "semana":
        fecha_desde = today - timedelta(days=7)
    elif periodo == "trimestre":
        fecha_desde = today - timedelta(days=90)
    else:
        fecha_desde = today.replace(day=1)

    fecha_desde_str = str(fecha_desde)
    db = get_db()
    tipo_params = ()
    tipo_filter_sql = ""
    if tipo_obra in ("TIPO I", "TIPO II", "TIPO III"):
        tipo_filter_sql = " AND UPPER(COALESCE(ot.tipo_estructura, '')) = ?"
        tipo_params = (tipo_obra,)

    obra_filter_sql = ""
    obra_params = ()
    if obra:
        obra_filter_sql = " AND TRIM(COALESCE(ot.obra, '')) = ?"
        obra_params = (obra,)

    # 1. HS cargadas (en el período) vs HS previstas (total OT) por cada OT
    ots = db.execute(f"""
        SELECT ot.id,
               COALESCE(NULLIF(TRIM(ot.obra),''), NULLIF(TRIM(ot.titulo),''), 'OT ' || ot.id) AS nombre,
               COALESCE(ot.hs_previstas, 0) AS hs_previstas,
               COALESCE(SUM(CASE WHEN pt.fecha >= ? THEN pt.horas ELSE 0 END), 0) AS hs_cargadas
        FROM ordenes_trabajo ot
        LEFT JOIN partes_trabajo pt ON pt.ot_id = ot.id
        WHERE ot.fecha_cierre IS NULL {tipo_filter_sql}{obra_filter_sql}
        GROUP BY ot.id
        HAVING COALESCE(ot.hs_previstas, 0) > 0 OR hs_cargadas > 0
        ORDER BY ot.id DESC
    """, (fecha_desde_str,) + tipo_params + obra_params).fetchall()

    hs_por_ot = []
    for row in ots:
        nombre = str(row[1] or '')[:22]
        hs_por_ot.append({
            "ot_id": row[0],
            "label": f"OT {row[0]} · {nombre}",
            "hs_previstas": round(float(row[2] or 0), 1),
            "hs_cargadas":  round(float(row[3] or 0), 1)
        })

    # 1b. HS previstas vs HS consumidas por OBRA
    obras = db.execute(f"""
        WITH hs_ot AS (
            SELECT ot.id,
                   COALESCE(NULLIF(TRIM(ot.obra),''), 'SIN OBRA') AS obra,
                   COALESCE(ot.hs_previstas, 0) AS hs_previstas,
                   COALESCE(SUM(CASE WHEN pt.fecha >= ? THEN pt.horas ELSE 0 END), 0) AS hs_cargadas
            FROM ordenes_trabajo ot
            LEFT JOIN partes_trabajo pt ON pt.ot_id = ot.id
            WHERE ot.fecha_cierre IS NULL {tipo_filter_sql}{obra_filter_sql}
            GROUP BY ot.id
        )
        SELECT obra,
               SUM(hs_previstas) AS hs_previstas,
               SUM(hs_cargadas) AS hs_cargadas
        FROM hs_ot
        GROUP BY obra
        HAVING SUM(hs_previstas) > 0 OR SUM(hs_cargadas) > 0
        ORDER BY SUM(hs_cargadas) DESC, obra ASC
    """, (fecha_desde_str,) + tipo_params + obra_params).fetchall()

    obras_disponibles_rows = db.execute(f"""
        SELECT DISTINCT TRIM(COALESCE(ot.obra, '')) AS obra
        FROM ordenes_trabajo ot
        WHERE ot.fecha_cierre IS NULL AND TRIM(COALESCE(ot.obra, '')) <> '' {tipo_filter_sql}
        ORDER BY obra ASC
    """, tipo_params).fetchall()
    obras_disponibles = [str(r[0]) for r in obras_disponibles_rows if str(r[0] or '').strip()]

    hs_por_obra = []
    for row in obras:
        hs_por_obra.append({
            "label": str(row[0] or 'SIN OBRA')[:24],
            "hs_previstas": round(float(row[1] or 0), 1),
            "hs_cargadas": round(float(row[2] or 0), 1)
        })

    # 2. Suma de kg por estación en el período.
    # Los registros de proceso (ARMADO/SOLDADURA/etc.) no tienen peso;
    # el peso está en la fila de escaneo inicial de cada pieza.
    # Se hace JOIN con la subquery que obtiene el peso real por (posicion, obra),
    # igual que lo hace el módulo "Estado de Piezas por Proceso".
    kg_rows = db.execute(f"""
        WITH obra_tipo AS (
            SELECT LOWER(TRIM(obra)) AS obra_key,
                   UPPER(COALESCE(tipo_estructura, '')) AS tipo_estructura
            FROM ordenes_trabajo
            WHERE COALESCE(TRIM(obra), '') <> ''
            GROUP BY LOWER(TRIM(obra)), UPPER(COALESCE(tipo_estructura, ''))
        )
        SELECT pr.proceso,
               SUM(COALESCE(CAST(pd.peso AS REAL), 0)) AS total_kg
        FROM procesos pr
        LEFT JOIN (
            SELECT posicion,
                   COALESCE(obra, '') AS obra,
                   MAX(COALESCE(CAST(peso AS REAL), 0)) AS peso
            FROM procesos
            WHERE COALESCE(escaneado_qr, 0) = 1
            GROUP BY posicion, COALESCE(obra, '')
        ) pd ON pr.posicion = pd.posicion
             AND COALESCE(pr.obra, '') = pd.obra
                LEFT JOIN obra_tipo otp ON LOWER(TRIM(COALESCE(pr.obra, ''))) = otp.obra_key
        WHERE pr.proceso IN ('ARMADO','SOLDADURA','PINTURA','DESPACHO')
          AND pr.fecha >= ?
          AND COALESCE(pr.escaneado_qr, 0) = 1
                    AND (? = '' OR otp.tipo_estructura = ?)
                    AND (? = '' OR LOWER(TRIM(COALESCE(pr.obra, ''))) = LOWER(?))
        GROUP BY pr.proceso
                """, (fecha_desde_str, tipo_obra, tipo_obra, obra, obra)).fetchall()

    kg_por_estacion = {"ARMADO": 0.0, "SOLDADURA": 0.0, "PINTURA": 0.0, "DESPACHO": 0.0}
    for row in kg_rows:
        if row[0] in kg_por_estacion:
            kg_por_estacion[row[0]] = round(float(row[1] or 0), 2)

    return jsonify({
        "periodo": periodo,
        "fecha_desde": fecha_desde_str,
        "tipo_obra": tipo_obra,
        "obra": obra,
        "obras_disponibles": obras_disponibles,
        "hs_por_ot": hs_por_ot,
        "hs_por_obra": hs_por_obra,
        "kg_por_estacion": kg_por_estacion
    })

# ======================
# PDF ESTADO DE PRODUCCIÓN
# ======================
@app.route("/api/dashboard-estado/pdf")
def dashboard_estado_pdf():
    from datetime import date, timedelta, datetime
    from reportlab.graphics.shapes import Drawing, String
    from reportlab.graphics.charts.barcharts import VerticalBarChart
    from reportlab.graphics.charts.piecharts import Pie

    periodo = request.args.get("periodo", "mes")
    today = date.today()
    if periodo == "semana":
        fecha_desde = today - timedelta(days=7)
        periodo_label = "Semana"
    elif periodo == "trimestre":
        fecha_desde = today - timedelta(days=90)
        periodo_label = "Trimestre"
    else:
        fecha_desde = today.replace(day=1)
        periodo_label = "Mes"

    fecha_desde_str = str(fecha_desde)
    db = get_db()

    ots = db.execute("""
        SELECT ot.id,
               COALESCE(NULLIF(TRIM(ot.obra),''), NULLIF(TRIM(ot.titulo),''), 'OT ' || ot.id) AS nombre,
               COALESCE(ot.hs_previstas, 0) AS hs_previstas,
               COALESCE(SUM(CASE WHEN pt.fecha >= ? THEN pt.horas ELSE 0 END), 0) AS hs_consumidas
        FROM ordenes_trabajo ot
        LEFT JOIN partes_trabajo pt ON pt.ot_id = ot.id
        GROUP BY ot.id
        HAVING COALESCE(ot.hs_previstas, 0) > 0 OR hs_consumidas > 0
        ORDER BY hs_consumidas DESC, ot.id DESC
    """, (fecha_desde_str,)).fetchall()

    hs_por_ot = []
    for row in ots:
        hs_por_ot.append({
            "label": f"OT {row[0]} · {str(row[1] or '')[:24]}",
            "hs_previstas": round(float(row[2] or 0), 1),
            "hs_consumidas": round(float(row[3] or 0), 1)
        })

    kg_rows = db.execute("""
        SELECT pr.proceso,
               SUM(COALESCE(CAST(pd.peso AS REAL), 0)) AS total_kg
        FROM procesos pr
        LEFT JOIN (
            SELECT posicion,
                   COALESCE(obra, '') AS obra,
                   MAX(COALESCE(CAST(peso AS REAL), 0)) AS peso
            FROM procesos
            WHERE COALESCE(escaneado_qr, 0) = 1
            GROUP BY posicion, COALESCE(obra, '')
        ) pd ON pr.posicion = pd.posicion
             AND COALESCE(pr.obra, '') = pd.obra
        WHERE pr.proceso IN ('ARMADO','SOLDADURA','PINTURA','DESPACHO')
          AND pr.fecha >= ?
          AND COALESCE(pr.escaneado_qr, 0) = 1
        GROUP BY pr.proceso
    """, (fecha_desde_str,)).fetchall()

    kg_por_estacion = {"ARMADO": 0.0, "SOLDADURA": 0.0, "PINTURA": 0.0, "DESPACHO": 0.0}
    for row in kg_rows:
        if row[0] in kg_por_estacion:
            kg_por_estacion[row[0]] = round(float(row[1] or 0), 2)

    total_prev = sum(o["hs_previstas"] for o in hs_por_ot)
    total_cons = sum(o["hs_consumidas"] for o in hs_por_ot)
    total_kg = sum(kg_por_estacion.values())
    efic_str = f"{(total_cons / total_prev * 100):.1f}%" if total_prev > 0 else "—"
    kg_hs_str = f"{(total_kg / total_cons):.1f}" if total_cons > 0 else "—"

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=landscape(A3),
        leftMargin=14,
        rightMargin=14,
        topMargin=12,
        bottomMargin=12
    )
    story = []
    styles = getSampleStyleSheet()

    title_style = ParagraphStyle(
        'DashTitleVisual',
        parent=styles['Heading1'],
        fontSize=24,
        leading=28,
        textColor=colors.HexColor('#7c2d12'),
        spaceAfter=2,
        fontName='Helvetica-Bold'
    )
    subtitle_style = ParagraphStyle(
        'DashSubVisual',
        parent=styles['Normal'],
        fontSize=11,
        leading=14,
        textColor=colors.HexColor('#7c2d12')
    )
    card_title_style = ParagraphStyle(
        'CardTitle',
        parent=styles['Normal'],
        fontSize=11,
        leading=13,
        textColor=colors.HexColor('#9a3412'),
        alignment=1,
        fontName='Helvetica-Bold'
    )
    card_value_style = ParagraphStyle(
        'CardValue',
        parent=styles['Normal'],
        fontSize=22,
        leading=24,
        textColor=colors.HexColor('#ea580c'),
        alignment=1,
        fontName='Helvetica-Bold'
    )

    logo_path = os.path.join(APP_DIR, "LOGO.png")
    logo_flow = Image(logo_path, width=78*mm, height=40*mm) if os.path.exists(logo_path) else Paragraph("<b>A3</b>", subtitle_style)

    logo_header = Table([[logo_flow]], colWidths=[390*mm])
    logo_header.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#fff7ed')),
        ('BOX', (0, 0), (-1, -1), 1.1, colors.HexColor('#f97316')),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('LEFTPADDING', (0, 0), (-1, -1), 6),
        ('RIGHTPADDING', (0, 0), (-1, -1), 6),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
    ]))
    story.append(logo_header)
    story.append(Spacer(1, 5))

    header_copy = Paragraph(
        f"<b>Estado de Producción</b><br/>"
        f"<font size='12'>Período: {periodo_label} &nbsp;&nbsp;&nbsp; Desde: {fecha_desde.strftime('%d/%m/%Y')} &nbsp;&nbsp;&nbsp; Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}</font>",
        title_style
    )
    header_text = Table([[header_copy]], colWidths=[390*mm])
    header_text.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#fff7ed')),
        ('BOX', (0, 0), (-1, -1), 1.1, colors.HexColor('#f97316')),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('LEFTPADDING', (0, 0), (-1, -1), 8),
        ('RIGHTPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 6),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
    ]))
    story.append(header_text)
    story.append(Spacer(1, 9))

    cards = [
        [Paragraph('HS PREVISTAS', card_title_style), Paragraph(f"{total_prev:.1f}<br/><font size='11'>hs</font>", card_value_style)],
        [Paragraph('HS CONSUMIDAS', card_title_style), Paragraph(f"{total_cons:.1f}<br/><font size='11'>hs</font>", card_value_style)],
        [Paragraph('EFICIENCIA HS', card_title_style), Paragraph(efic_str, card_value_style)],
        [Paragraph('KG PROCESADOS', card_title_style), Paragraph(f"{total_kg:.1f}<br/><font size='11'>kg</font>", card_value_style)],
        [Paragraph('KG / HS', card_title_style), Paragraph(kg_hs_str, card_value_style)],
    ]
    cards_table = Table([cards], colWidths=[78*mm, 78*mm, 78*mm, 78*mm, 78*mm])
    cards_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.white),
        ('BOX', (0, 0), (-1, -1), 0.9, colors.HexColor('#fdba74')),
        ('INNERGRID', (0, 0), (-1, -1), 0.6, colors.HexColor('#fed7aa')),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
    ]))
    story.append(cards_table)
    story.append(Spacer(1, 10))

    # Datos para graficos
    top_rows = hs_por_ot[:8]
    hs_prev = [o['hs_previstas'] for o in top_rows] or [0]
    hs_cons = [o['hs_consumidas'] for o in top_rows] or [0]
    hs_labels = [o['label'][:16] for o in top_rows] or ['Sin datos']

    estaciones = ['ARMADO', 'SOLDADURA', 'PINTURA', 'DESPACHO']
    kg_vals = [kg_por_estacion[e] for e in estaciones]

    # Grafico principal HS
    hs_box = Drawing(820, 250)
    hs_box.add(String(30, 226, 'HS Consumidas vs HS Previstas (Top 8 OTs)', fontSize=13, fillColor=colors.HexColor('#7c2d12')))
    hs_chart = VerticalBarChart()
    hs_chart.x = 48
    hs_chart.y = 36
    hs_chart.width = 730
    hs_chart.height = 165
    hs_chart.data = [hs_prev, hs_cons]
    hs_chart.categoryAxis.categoryNames = hs_labels
    hs_chart.categoryAxis.labels.angle = 30
    hs_chart.categoryAxis.labels.boxAnchor = 'ne'
    hs_chart.categoryAxis.labels.dx = 8
    hs_chart.categoryAxis.labels.dy = -2
    hs_chart.categoryAxis.labels.fontSize = 8
    hs_chart.valueAxis.valueMin = 0
    hs_chart.valueAxis.valueStep = max(1, int(max(hs_prev + hs_cons + [1]) / 6))
    hs_chart.barSpacing = 3
    hs_chart.groupSpacing = 8
    hs_chart.bars[0].fillColor = colors.HexColor('#fdba74')
    hs_chart.bars[1].fillColor = colors.HexColor('#ea580c')
    hs_box.add(hs_chart)

    hs_container = Table([[hs_box]], colWidths=[390*mm])
    hs_container.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor('#fffaf5')),
        ('BOX', (0, 0), (-1, -1), 0.9, colors.HexColor('#fdba74')),
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ]))
    story.append(hs_container)
    story.append(Spacer(1, 8))

    # Bloque inferior visual: barra kg + torta + estaciones
    kg_bar_draw = Drawing(390, 230)
    kg_bar_draw.add(String(42, 205, 'KG procesados por estación', fontSize=12, fillColor=colors.HexColor('#7c2d12')))
    kg_bar = VerticalBarChart()
    kg_bar.x = 40
    kg_bar.y = 34
    kg_bar.width = 320
    kg_bar.height = 150
    kg_bar.data = [kg_vals if sum(kg_vals) > 0 else [0, 0, 0, 0]]
    kg_bar.categoryAxis.categoryNames = estaciones
    kg_bar.categoryAxis.labels.fontSize = 8
    kg_bar.valueAxis.valueMin = 0
    kg_bar.valueAxis.valueStep = max(1, int(max(kg_vals + [1]) / 5))
    kg_bar.barWidth = 40
    kg_bar.barSpacing = 16
    kg_bar.groupSpacing = 14
    kg_bar.bars[0].fillColor = colors.HexColor('#f97316')
    kg_bar_draw.add(kg_bar)

    kg_pie_draw = Drawing(390, 230)
    kg_pie_draw.add(String(85, 205, 'Distribución de KG en planta', fontSize=12, fillColor=colors.HexColor('#7c2d12')))
    pie = Pie()
    pie.x = 125
    pie.y = 30
    pie.width = 150
    pie.height = 150
    if sum(kg_vals) > 0:
        pie.data = kg_vals
        pie.labels = [f"{estaciones[i]} {kg_vals[i]:.1f}" for i in range(len(estaciones))]
    else:
        pie.data = [1]
        pie.labels = ['Sin datos']
    pie_colors = [colors.HexColor('#f97316'), colors.HexColor('#ea580c'), colors.HexColor('#c2410c'), colors.HexColor('#7c2d12')]
    for i in range(len(pie.data)):
        pie.slices[i].fillColor = pie_colors[i % len(pie_colors)]
    kg_pie_draw.add(pie)

    estaciones_cards = []
    for idx, est in enumerate(estaciones):
        kg_v = kg_por_estacion[est]
        est_cell = Table([[Paragraph(f"<b>{est}</b><br/><font size='13'>{kg_v:.1f} kg</font>", subtitle_style)]], colWidths=[90*mm])
        est_colors = ['#fff7ed', '#ffedd5', '#fed7aa', '#fdba74']
        est_cell.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, -1), colors.HexColor(est_colors[idx % len(est_colors)])),
            ('BOX', (0, 0), (-1, -1), 0.7, colors.HexColor('#f97316')),
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('TOPPADDING', (0, 0), (-1, -1), 9),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 9),
        ]))
        estaciones_cards.append(est_cell)

    estaciones_table = Table([[estaciones_cards[0], estaciones_cards[1]], [estaciones_cards[2], estaciones_cards[3]]], colWidths=[90*mm, 90*mm])
    estaciones_table.setStyle(TableStyle([
        ('LEFTPADDING', (0, 0), (-1, -1), 4),
        ('RIGHTPADDING', (0, 0), (-1, -1), 4),
        ('TOPPADDING', (0, 0), (-1, -1), 4),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
    ]))

    lower_left = Table([[kg_bar_draw]], colWidths=[186*mm])
    lower_left.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, -1), colors.white),
        ('BOX', (0, 0), (-1, -1), 0.8, colors.HexColor('#fdba74')),
    ]))

    right_stack = Table([[kg_pie_draw], [Spacer(1, 3)], [estaciones_table]], colWidths=[186*mm])
    right_stack.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.white),
        ('BOX', (0, 0), (-1, 0), 0.8, colors.HexColor('#fdba74')),
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ]))

    lower = Table([[lower_left, right_stack]], colWidths=[186*mm, 186*mm])
    lower.setStyle(TableStyle([
        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
        ('LEFTPADDING', (0, 0), (-1, -1), 0),
        ('RIGHTPADDING', (0, 0), (-1, -1), 0),
        ('TOPPADDING', (0, 0), (-1, -1), 0),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 0),
    ]))
    story.append(lower)

    doc.build(story)
    buf.seek(0)

    fname = f"estado_produccion_visual_{periodo}_{today.strftime('%Y%m%d')}.pdf"
    _guardar_pdf_databook("GENERAL", "produccion", fname, buf.getvalue())
    buf.seek(0)
    return send_file(
        buf,
        mimetype="application/pdf",
        as_attachment=True,
        download_name=fname
    )

# ======================
# MÓDULO 6 - PRODUCCIÓN (Versión anterior)
# ======================
@app.route("/modulo/produccion")
def produccion():
    db = get_db()
    ots_en_curso = db.execute("""
        SELECT id, cliente, obra, titulo, fecha_entrega, estado, estado_avance, fecha_creacion
        FROM ordenes_trabajo 
        WHERE estado != 'Finalizada' AND fecha_cierre IS NULL
        ORDER BY fecha_entrega ASC
    """).fetchall()
    
    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * { box-sizing: border-box; }
    body { font-family: Arial; padding: 15px; background: #f4f4f4; margin: 0; }
    h2 { color: #333; border-bottom: 3px solid #f093fb; padding-bottom: 10px; }
    .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
    .btn { display: inline-block; background: #667eea; color: white; padding: 10px 15px; 
           text-decoration: none; border-radius: 5px; font-weight: bold; cursor: pointer; border: none; }
    .btn:hover { background: #5568d3; }
    .btn-update { background: #43e97b; }
    .btn-update:hover { background: #2cc96e; }
    table { width: 100%; border-collapse: collapse; background: white; box-shadow: 0 2px 5px rgba(0,0,0,0.1); }
    th, td { padding: 12px; border-bottom: 1px solid #ddd; text-align: left; }
    th { background: #f093fb; color: white; font-weight: bold; }
    tr:hover { background: #f5f5f5; }
    .progress { width: 100%; height: 20px; background: #e0e0e0; border-radius: 10px; overflow: hidden; }
    .progress-bar { height: 100%; background: linear-gradient(90deg, #43e97b, #38f9d7); text-align: center; 
                    color: white; font-size: 12px; line-height: 20px; }
    .sin-datos { text-align: center; padding: 30px; color: #999; }
    .modal { display: none; position: fixed; z-index: 1; left: 0; top: 0; width: 100%; height: 100%; 
             background-color: rgba(0,0,0,0.4); }
    .modal.show { display: block; }
    .modal-content { background-color: white; margin: 10% auto; padding: 20px; border: 1px solid #888; 
                     width: 80%; max-width: 500px; border-radius: 5px; }
    .close { color: #aaa; float: right; font-size: 28px; font-weight: bold; cursor: pointer; }
    .close:hover { color: #000; }
    input[type="number"], textarea { width: 100%; padding: 10px; margin: 10px 0; border: 1px solid #ddd; 
                                      border-radius: 4px; }
    label { display: block; font-weight: bold; margin-top: 10px; }
    button { width: 100%; padding: 10px; background: #43e97b; color: white; border: none; 
             border-radius: 4px; font-weight: bold; cursor: pointer; margin-top: 10px; }
    button:hover { background: #2cc96e; }
    </style>
    </head>
    <body>
    <div class="header">
        <h2>🏭 Control de Producción</h2>
        <a href="/" class="btn">⬅️ Volver</a>
    </div>
    
    """
    
    if len(ots_en_curso) == 0:
        html += "<div class='sin-datos'>✅ No hay órdenes en curso. ¡Todas finalizadas!</div>"
    else:
        html += """
        <table>
            <tr>
                <th>ID</th>
                <th>Cliente</th>
                <th>Obra</th>
                <th>Título</th>
                <th>% Avance</th>
                <th>Fecha Entrega</th>
                <th>Estado</th>
                <th>Acciones</th>
            </tr>
        """
        for ot in ots_en_curso:
            progreso = ot[6] if ot[6] is not None else 0
            html += f"""
            <tr>
                <td><b>{ot[0]}</b></td>
                <td>{ot[1]}</td>
                <td>{ot[2]}</td>
                <td>{ot[3]}</td>
                <td>
                    <div class="progress">
                        <div class="progress-bar" style="width: {progreso}%">{progreso}%</div>
                    </div>
                </td>
                <td>{ot[4]}</td>
                <td>{ot[5]}</td>
                <td>
                    <button class="btn btn-update" onclick="abrirFormulario({ot[0]}, '{ot[1]}', '{ot[2]}', '{ot[3]}')">
                        📊 Actualizar
                    </button>
                </td>
            </tr>
            """
        html += "</table>"
    
    html += """
    </body>
    </html>
    
    <div id="modal" class="modal">
        <div class="modal-content">
            <span class="close" onclick="cerrarFormulario()">&times;</span>
            <h3>Actualizar % Avance</h3>
            <form id="form-avance">
                <label>OT: <span id="ot-info"></span></label>
                <input type="hidden" id="ot-id" name="ot_id">
                
                <label for="porcentaje">% Completado (0-100):</label>
                <input type="number" id="porcentaje" name="porcentaje" min="0" max="100" value="0" required>
                
                <label for="observaciones">Observaciones:</label>
                <textarea id="observaciones" name="observaciones" rows="4"></textarea>
                
                <button type="button" onclick="guardarAvance()">💾 Guardar</button>
            </form>
        </div>
    </div>
    
    <script>
    function abrirFormulario(otId, cliente, obra, titulo) {
        document.getElementById('ot-id').value = otId;
        document.getElementById('ot-info').textContent = `${otId} - ${cliente} / ${obra} / ${titulo}`;
        document.getElementById('modal').classList.add('show');
    }
    
    function cerrarFormulario() {
        document.getElementById('modal').classList.remove('show');
    }
    
    function guardarAvance() {
        const otId = document.getElementById('ot-id').value;
        const porcentaje = document.getElementById('porcentaje').value;
        const observaciones = document.getElementById('observaciones').value;
        
        fetch('/guardar-avance', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ ot_id: otId, porcentaje: porcentaje, observaciones: observaciones })
        })
        .then(response => response.json())
        .then(data => {
            if (data.success) {
                alert('✅ Avance guardado correctamente');
                location.reload();
            } else {
                alert('❌ Error: ' + data.error);
            }
        })
        .catch(err => alert('❌ Error: ' + err));
    }
    
    window.onclick = function(event) {
        const modal = document.getElementById('modal');
        if (event.target == modal) {
            modal.classList.remove('show');
        }
    }
    </script>
    """
    return html

# ======================
# ENDPOINT GUARDAR AVANCE PRODUCCIÓN
# ======================
@app.route("/guardar-avance", methods=["POST"])
def guardar_avance():
    try:
        data = request.get_json()
        ot_id = data.get("ot_id")
        porcentaje = int(data.get("porcentaje", 0))
        observaciones = data.get("observaciones", "")
        
        if not ot_id or porcentaje < 0 or porcentaje > 100:
            return jsonify({"success": False, "error": "Datos inválidos"}), 400
        
        db = get_db()
        
        # Guardar en histórico
        from datetime import date
        db.execute("""
            INSERT INTO avance_produccion (ot_id, fecha, porcentaje, observaciones)
            VALUES (?, ?, ?, ?)
        """, (ot_id, str(date.today()), porcentaje, observaciones))
        
        # Actualizar estado_avance de OT
        db.execute("UPDATE ordenes_trabajo SET estado_avance = ? WHERE id = ?", (porcentaje, ot_id))
        db.commit()
        
        return jsonify({"success": True}), 200
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ======================
# ENDPOINT CARGAR PIEZAS PARA REMITO
# ======================
@app.route("/api/piezas-remito/<int:ot_id>", methods=["GET"])
def api_piezas_remito(ot_id):
    try:
        db = get_db()
        ot = db.execute(
            "SELECT TRIM(COALESCE(obra, '')) FROM ordenes_trabajo WHERE id = ?",
            (ot_id,)
        ).fetchone()

        if not ot:
            return jsonify({"error": "OT no encontrada", "piezas": []}), 404

        obra_ot = (ot[0] or "").strip()
        if not obra_ot:
            return jsonify({"piezas": []}), 200

        # Filtrar por ot_id si las piezas ya tienen asignado ese campo;
        # si no, usar obra como fallback (compatibilidad con registros anteriores)
        piezas_por_ot = db.execute("""
            SELECT p_despacho.id,
                   p_first.posicion,
                   p_first.obra,
                   COALESCE(p_first.cantidad, ''),
                   COALESCE(p_first.perfil, ''),
                   COALESCE(p_first.peso, ''),
                   COALESCE(p_first.descripcion, '')
            FROM procesos p_despacho
            LEFT JOIN procesos p_first ON p_despacho.posicion = p_first.posicion 
                                       AND p_despacho.obra = p_first.obra
                                       AND p_first.id = (
                                           SELECT MIN(id) FROM procesos 
                                           WHERE posicion = p_despacho.posicion 
                                           AND obra = p_despacho.obra
                                       )
            WHERE p_despacho.ot_id = ?
              AND p_despacho.proceso = 'DESPACHO'
              AND UPPER(TRIM(COALESCE(p_despacho.estado, ''))) = 'OK'
        """, (ot_id,)).fetchall()

        piezas_por_obra = db.execute("""
            SELECT p_despacho.id,
                   p_first.posicion,
                   p_first.obra,
                   COALESCE(p_first.cantidad, ''),
                   COALESCE(p_first.perfil, ''),
                   COALESCE(p_first.peso, ''),
                   COALESCE(p_first.descripcion, '')
            FROM procesos p_despacho
            LEFT JOIN procesos p_first ON p_despacho.posicion = p_first.posicion
                                       AND p_despacho.obra = p_first.obra
                                       AND p_first.id = (
                                           SELECT MIN(id) FROM procesos
                                           WHERE posicion = p_despacho.posicion
                                           AND obra = p_despacho.obra
                                       )
            WHERE TRIM(COALESCE(p_despacho.obra, '')) = ?
              AND (p_despacho.ot_id IS NULL OR p_despacho.ot_id = ?)
              AND p_despacho.proceso = 'DESPACHO'
              AND UPPER(TRIM(COALESCE(p_despacho.estado, ''))) = 'OK'
        """, (obra_ot, ot_id)).fetchall()

        # Combinar: preferir los de ot_id exacto; agregar los sin ot_id asignado por obra
        ids_vistos = {p[0] for p in piezas_por_ot}
        piezas = list(piezas_por_ot) + [p for p in piezas_por_obra if p[0] not in ids_vistos]

        def natural_key_posicion(valor):
            texto = str(valor or "").strip().upper()
            return [int(part) if part.isdigit() else part for part in re.split(r"(\d+)", texto)]

        piezas = sorted(piezas, key=lambda fila: natural_key_posicion(fila[1]))
        
        piezas_list = []
        for p in piezas:
            pieza_id = p[0]
            posicion = str(p[1]) if p[1] else ''
            obra = str(p[2]) if p[2] else ''
            cantidad = str(p[3]) if p[3] else ''
            perfil = str(p[4]) if p[4] else ''
            peso = str(p[5]) if p[5] else ''
            descripcion = str(p[6]) if p[6] else ''
            
            # Devolver todos los datos individualmente
            piezas_list.append({
                "id": pieza_id,
                "posicion": posicion,
                "obra": obra,
                "cantidad": cantidad,
                "perfil": perfil,
                "peso": peso,
                "descripcion": descripcion,
                "nombre": f"{posicion} - {descripcion}".strip()
            })
        
        return jsonify({
            "piezas": piezas_list
        }), 200
    except Exception as e:
        return jsonify({
            "error": str(e),
            "piezas": []
        }), 500

# ======================
# ENDPOINT ELIMINAR REMITO
# ======================
@app.route("/eliminar-remito/<int:remito_id>", methods=["GET"])
def eliminar_remito(remito_id):
    try:
        db = get_db()
        row = db.execute("SELECT pdf_path FROM remitos WHERE id = ?", (remito_id,)).fetchone()
        if not row:
            return "Remito no encontrado", 404

        pdf_stored = row[0] or ""
        pdf_name = os.path.basename(pdf_stored)
        pdf_full_path = os.path.join(REMITOS_DIR, pdf_name)

        db.execute("DELETE FROM remitos WHERE id = ?", (remito_id,))
        db.commit()

        if pdf_name and os.path.exists(pdf_full_path):
            os.remove(pdf_full_path)

        return redirect("/modulo/remito")
    except Exception as e:
        return f"Error eliminando remito: {str(e)}", 500

# ======================
# ENDPOINT DESCARGAR REMITO
# ======================
@app.route("/descargar-remito/<filename>")
def descargar_remito(filename):
    try:
        # Usar ruta absoluta
        filepath = os.path.join(REMITOS_DIR, filename)
        if not os.path.exists(filepath):
            return "Remito no encontrado", 404
        return send_file(filepath, as_attachment=True, download_name=filename)
    except Exception as e:
        return f"Error descargando remito: {str(e)}", 500

# ======================
# MÓDULO 7 - GENERADOR DE ETIQUETAS QR
# ======================
@app.route("/modulo/generador")
def generador_qr_main():
    html = """
    <html>
    <head>
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; padding: 20px; background: #f4f4f4; }
    .container { max-width: 600px; margin: 0 auto; }
    h1 { color: #333; margin-bottom: 10px; }
    .breadcrumb { color: #666; margin-bottom: 30px; }
    .breadcrumb a { color: #667eea; text-decoration: none; margin-right: 10px; }
    .info-box { background: #e3f2fd; border-left: 5px solid #667eea; padding: 15px; 
                border-radius: 5px; margin-bottom: 20px; }
    .info-box h3 { color: #1976d2; margin-bottom: 10px; }
    .info-box p { color: #555; line-height: 1.6; }
    .btn { display: inline-block; background: #43e97b; color: white; padding: 12px 20px; 
           text-decoration: none; border-radius: 5px; border: none; cursor: pointer; font-size: 16px; width: 100%; text-align: center; }
    .btn:hover { background: #2cc96e; }
    .btn-volver { background: #667eea; margin-top: 20px; }
    .btn-volver:hover { background: #5568d3; }
    form { background: white; padding: 20px; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
    .form-group { margin-bottom: 20px; }
    label { display: block; margin-bottom: 8px; font-weight: bold; color: #333; }
    input[type="file"] { display: block; padding: 10px; border: 2px solid #ddd; border-radius: 5px; 
                         width: 100%; box-sizing: border-box; }
    .required { color: red; }
    </style>
    </head>
    <body>
    <div class="container">
        <div class="breadcrumb">
            <a href="/">← Volver al Inicio</a>
        </div>
        
        <h1>🏷️ Generador de Etiquetas QR A3</h1>
        
        <div class="info-box">
            <h3>📌 Instrucciones</h3>
            <p>
                1. Descarga el archivo Excel de plantilla<br>
                2. Carga el archivo Excel con posiciones (ARMADOS)<br>
                3. Presiona "Generar PDF" para crear las etiquetas<br>
                4. El sistema expandirá automáticamente los códigos expandibles (V, C, PU, INS)
            </p>
        </div>
        
        <form method="post" enctype="multipart/form-data">
            <div class="form-group">
                <label for="excel1">
                    📄 Archivo Excel - ARMADOS <span class="required">*</span>
                </label>
                <input type="file" name="excel1" accept=".xlsx,.xls" required>
                <small style="color: #666;">Debe contener columnas: POS, PLANO, REV, OBRA, CANT, PERFIL, PESO, DESCRIP</small>
            </div>
            
            <button type="submit" class="btn">⚙️ Generar PDF</button>
        </form>
        
        <a href="/modulo/generador/plantilla" class="btn btn-volver" style="background: #4facfe; margin-top: 15px;">
            📥 Descargar Plantilla Excel
        </a>
        
        <a href="/" class="btn btn-volver">← Volver al Inicio</a>
    </div>
    </body>
    </html>
    """
    return html

@app.route("/modulo/generador", methods=["POST"])
def generador_qr_procesar():
    try:
        if 'excel1' not in request.files:
            return "❌ Error: Se requiere el archivo Excel", 400
        
        excel1_file = request.files['excel1']
        
        if excel1_file.filename == '':
            return "❌ Error: Se requiere el archivo Excel", 400
        
        # Logo path
        logo_path = r"C:\Users\usuar\OneDrive\Desktop\python\LOGO.png"
        if not os.path.exists(logo_path):
            return "❌ Error: Logo no encontrado en la ruta esperada", 400
        
        # Guardar archivo temporal
        with tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False) as tmp1:
            excel1_file.save(tmp1)
            excel1_path = tmp1.name
        
        # Generar PDF
        pdf_buffer = generar_etiquetas_qr(excel1_path, logo_path)
        
        # Limpiar archivo temporal
        try:
            os.unlink(excel1_path)
        except:
            pass
        
        # Retornar PDF para descargar
        return send_file(
            pdf_buffer,
            mimetype='application/pdf',
            as_attachment=True,
            download_name='ETIQUETAS_A3.pdf'
        )
    
    except Exception as e:
        return f"""
        <html>
        <head><style>
        body {{ font-family: Arial; padding: 20px; }}
        .error {{ background: #ffe5e5; color: #d32f2f; padding: 15px; border-radius: 5px; }}
        .btn {{ display: inline-block; background: #667eea; color: white; padding: 10px 15px; 
               text-decoration: none; border-radius: 5px; margin-top: 15px; }}
        </style></head><body>
        <div class="error">
            <h2>❌ Error al generar PDF</h2>
            <p>{str(e)}</p>
        </div>
        <a href="/modulo/generador" class="btn">← Intentar de nuevo</a>
        </body></html>
        """, 400

@app.route("/modulo/generador/plantilla")
def generador_plantilla():
    """Genera una plantilla Excel de ejemplo"""
    try:
        # Crear DataFrame con datos de ejemplo
        data = {
            'POSICION': ['V1', 'V2', 'C1', 'PU1'],
            'PLANO': ['PLANO-001', 'PLANO-002', 'PLANO-003', 'PLANO-004'],
            'REV': ['A', 'B', 'A', 'C'],
            'OBRA': ['OBRA-2026', 'OBRA-2026', 'OBRA-2026', 'OBRA-2026'],
            'CANTIDAD': [5, 3, 2, 1],
            'PERFIL': ['PERFIL-A', 'PERFIL-B', 'PERFIL-C', 'PERFIL-D'],
            'PESO': [2.5, 3.2, 1.8, 4.1],
            'DESCRIPCION': ['Descripción parte 1', 'Descripción parte 2', 'Descripción parte 3', 'Descripción parte 4']
        }
        
        df = pd.DataFrame(data)
        
        # Crear buffer para Excel
        excel_buffer = BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='ARMADOS', index=False)
        
        excel_buffer.seek(0)
        
        return send_file(
            excel_buffer,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name='PLANTILLA_ETIQUETAS.xlsx'
        )
    
    except Exception as e:
        return f"Error generando plantilla: {str(e)}", 400

# ======================
app.run(host="0.0.0.0", port=5000, debug=True)
