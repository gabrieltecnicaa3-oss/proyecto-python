import os
import re
import sqlite3
import unicodedata
from urllib.parse import quote, unquote


def get_db():
    return sqlite3.connect("database.db")


def _resolver_ot_id_para_obra(db, obra):
    obra_txt = str(obra or "").strip()
    if not obra_txt:
        return None
    rows = db.execute(
        "SELECT id FROM ordenes_trabajo WHERE TRIM(COALESCE(obra,'')) = ? AND (es_mantenimiento IS NULL OR es_mantenimiento = 0) ORDER BY id",
        (obra_txt,),
    ).fetchall()
    return rows[0][0] if len(rows) == 1 else None


def _obtener_ots_para_obra(db, obra):
    obra_txt = str(obra or "").strip()
    if not obra_txt:
        return []
    return db.execute(
        "SELECT id, titulo FROM ordenes_trabajo WHERE TRIM(COALESCE(obra,'')) = ? AND (es_mantenimiento IS NULL OR es_mantenimiento = 0) ORDER BY id",
        (obra_txt,),
    ).fetchall()


def _obtener_ot_id_pieza(db, pos, obra):
    obra_txt = str(obra or "").strip()
    if not obra_txt:
        return None
    rows = db.execute(
        "SELECT DISTINCT ot_id FROM procesos WHERE posicion = ? AND TRIM(COALESCE(obra,'')) = ? AND ot_id IS NOT NULL",
        (pos, obra_txt),
    ).fetchall()
    return rows[0][0] if len(rows) == 1 else None


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


# Secciones que subdividen por OT dentro de la carpeta de sección
_SECCIONES_CON_SUBCARPETA_OT = {"calidad_armado_soldadura", "calidad_pintura", "remitos"}


def _resolver_carpeta_ot(ot_id, obra):
    """Devuelve el nombre de subcarpeta 'OT xx-titulo' para las secciones que la requieren."""
    if ot_id is None:
        return "OT SIN DEFINIR"
    ot_id_txt = str(ot_id).strip()
    if not ot_id_txt.isdigit():
        return "OT SIN DEFINIR"
    titulo = ""
    try:
        db = get_db()
        row = db.execute(
            """
            SELECT TRIM(COALESCE(titulo, ''))
            FROM ordenes_trabajo
            WHERE id = ?
              AND TRIM(COALESCE(obra, '')) = TRIM(COALESCE(?, ''))
            LIMIT 1
            """,
            (int(ot_id_txt), str(obra or "").strip()),
        ).fetchone()
        if row and row[0]:
            titulo = str(row[0]).strip()
    except Exception:
        titulo = ""
    carpeta_ot = f"OT {int(ot_id_txt)}"
    if titulo:
        carpeta_ot = f"{carpeta_ot}-{titulo}"
    return _normalizar_nombre_carpeta(carpeta_ot)


def _asegurar_estructura_databook(obra, databooks_dir, databook_secciones, ot_id=None):
    obra_dir = os.path.join(databooks_dir, _normalizar_nombre_carpeta(obra))

    # Recopilar todas las OTs activas para la obra (más la del ot_id dado)
    ot_ids_obra = set()
    if ot_id is not None:
        ot_ids_obra.add(ot_id)
    try:
        db = get_db()
        rows = db.execute(
            """
            SELECT id FROM ordenes_trabajo
            WHERE TRIM(COALESCE(obra, '')) = TRIM(COALESCE(?, ''))
              AND fecha_cierre IS NULL
              AND (es_mantenimiento IS NULL OR es_mantenimiento = 0)
            """,
            (str(obra or "").strip(),)
        ).fetchall()
        for r in rows:
            ot_ids_obra.add(r[0])
    except Exception:
        pass

    for seccion_key, seccion_rel in databook_secciones.items():
        if seccion_key in _SECCIONES_CON_SUBCARPETA_OT and ot_ids_obra:
            for ot in ot_ids_obra:
                ot_subcarpeta = _resolver_carpeta_ot(ot, obra)
                os.makedirs(os.path.join(obra_dir, seccion_rel, ot_subcarpeta), exist_ok=True)
        else:
            os.makedirs(os.path.join(obra_dir, seccion_rel), exist_ok=True)

    return obra_dir


def _asegurar_estructura_databook_si_valida(obra, databooks_dir, databook_secciones, ot_id=None):
    obra_txt = str(obra or "").strip()
    if not obra_txt or obra_txt == "---":
        return ""
    return _asegurar_estructura_databook(obra_txt, databooks_dir, databook_secciones, ot_id=ot_id)


def _guardar_pdf_databook(obra, seccion_key, filename, pdf_bytes, databooks_dir, databook_secciones, ot_id=None):
    if not pdf_bytes:
        return ""

    obra_dir = os.path.join(databooks_dir, _normalizar_nombre_carpeta(obra))
    seccion_rel = databook_secciones.get(seccion_key, "")
    if seccion_key in _SECCIONES_CON_SUBCARPETA_OT and ot_id is not None:
        ot_subcarpeta = _resolver_carpeta_ot(ot_id, obra)
        destino_dir = os.path.join(obra_dir, seccion_rel, ot_subcarpeta) if seccion_rel else os.path.join(obra_dir, ot_subcarpeta)
    else:
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


def _completar_metadatos_por_obra_pos(db, obra=None, posicion=None):
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
            meta[key] = {"cantidad": None, "perfil": "", "peso": None}

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


def _resolver_imagen_firma_empleado(nombre, firma_electronica, firmas_empleados_dir):
    candidatos = []
    try:
        for nombre_archivo in os.listdir(firmas_empleados_dir):
            ruta_archivo = os.path.join(firmas_empleados_dir, nombre_archivo)
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


def _url_firma_desde_path(firma_imagen_path, firmas_empleados_dir):
    nombre_archivo = os.path.basename(str(firma_imagen_path or "").strip())
    if not nombre_archivo:
        return ""
    ruta_abs = os.path.join(firmas_empleados_dir, nombre_archivo)
    if not os.path.isfile(ruta_abs):
        return ""
    return f"/firma-supervisor/{quote(nombre_archivo)}"


def _obtener_responsables_control(db, firmas_empleados_dir, inspector_firmas):
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

        firma_path = str(firma_imagen_path or "").strip() or _resolver_imagen_firma_empleado(
            nombre_txt,
            firma_txt,
            firmas_empleados_dir,
        )
        responsables[nombre_txt] = {
            "firma": firma_txt,
            "firma_url": _url_firma_desde_path(firma_path, firmas_empleados_dir),
        }

    if responsables:
        return responsables

    for nombre_txt, firma_txt in inspector_firmas.items():
        firma_path = _resolver_imagen_firma_empleado(nombre_txt, firma_txt, firmas_empleados_dir)
        responsables[nombre_txt] = {
            "firma": firma_txt,
            "firma_url": _url_firma_desde_path(firma_path, firmas_empleados_dir),
        }
    return responsables


def _ruta_firma_responsable(responsables_control, responsable, firmas_empleados_dir):
    info = responsables_control.get(str(responsable or "").strip()) or {}
    firma_url = str(info.get("firma_url") or "").strip()
    archivo = ""
    if "/firma-supervisor/" in firma_url:
        archivo = unquote(firma_url.rsplit("/", 1)[-1])
    if not archivo:
        firma_rel = _resolver_imagen_firma_empleado(
            responsable,
            info.get("firma", ""),
            firmas_empleados_dir,
        )
        archivo = os.path.basename(str(firma_rel or "").strip())
    if not archivo:
        return ""
    ruta = os.path.join(firmas_empleados_dir, archivo)
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

    rows = db.execute(
        """
        SELECT DISTINCT TRIM(operario) AS operario
        FROM procesos
        WHERE TRIM(COALESCE(operario, '')) <> ''
        ORDER BY operario
        """
    ).fetchall()
    return [str(r[0]).strip() for r in rows if r and str(r[0]).strip()]
