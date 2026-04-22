"""
Utilidades para subir archivos a Google Drive usando la API con cuenta de servicio.

Configuración requerida (variables de entorno en Railway):
  GOOGLE_CREDENTIALS_JSON  - contenido completo del JSON de la cuenta de servicio
  GOOGLE_DRIVE_FOLDER_ID   - ID de la carpeta raíz en Drive (carpeta "08 Reportes Produccion")

Para obtener GOOGLE_DRIVE_FOLDER_ID:
  1. Abrí la carpeta "08 Reportes Produccion" en drive.google.com
  2. El ID es la parte de la URL después de /folders/
     Ej: https://drive.google.com/drive/folders/1ABC123... → ID = 1ABC123...

Para crear la cuenta de servicio:
  1. console.cloud.google.com → Crear proyecto
  2. Habilitar "Google Drive API"
  3. IAM y Admin → Cuentas de servicio → Crear cuenta de servicio
  4. Crear clave JSON → descargar
  5. Compartir la carpeta de Drive con el email de la cuenta de servicio
  6. Pegar el contenido del JSON como variable GOOGLE_CREDENTIALS_JSON en Railway
"""

import os
import json
import io

_drive_service = None
_drive_init_attempted = False


def _get_drive_service():
    global _drive_service, _drive_init_attempted
    if _drive_init_attempted:
        return _drive_service
    _drive_init_attempted = True

    credentials_json = os.environ.get("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not credentials_json:
        return None

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build

        info = json.loads(credentials_json)
        scopes = ["https://www.googleapis.com/auth/drive"]
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
        _drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
    except Exception as e:
        print(f"[Drive] No se pudo inicializar el servicio: {e}")
        _drive_service = None

    return _drive_service


def drive_disponible():
    """Retorna True si Drive API está configurada y disponible."""
    return _get_drive_service() is not None


def _buscar_o_crear_carpeta(service, nombre, parent_id):
    """Busca una carpeta por nombre dentro del parent. Si no existe, la crea."""
    nombre_safe = str(nombre or "").strip().replace("'", "\\'")
    query = (
        f"name='{nombre_safe}' "
        f"and '{parent_id}' in parents "
        f"and mimeType='application/vnd.google-apps.folder' "
        f"and trashed=false"
    )
    try:
        results = service.files().list(
            q=query,
            fields="files(id, name)",
            spaces="drive",
            pageSize=1,
        ).execute()
        files = results.get("files", [])
        if files:
            return files[0]["id"]
    except Exception as e:
        print(f"[Drive] Error buscando carpeta '{nombre}': {e}")

    # Crear la carpeta
    try:
        metadata = {
            "name": nombre,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_id],
        }
        folder = service.files().create(body=metadata, fields="id").execute()
        return folder["id"]
    except Exception as e:
        print(f"[Drive] Error creando carpeta '{nombre}': {e}")
        return None


def subir_pdf_a_drive(pdf_bytes, filename, obra, seccion_nombre, ot_subfolder=None):
    """
    Sube un PDF a Google Drive manteniendo la estructura de carpetas:
      {GOOGLE_DRIVE_FOLDER_ID}/{obra}/{seccion_nombre}/{ot_subfolder?}/{filename}

    Parámetros:
      pdf_bytes      - bytes del PDF
      filename       - nombre del archivo (ej: "control_armado_OT5.pdf")
      obra           - nombre de la obra (ej: "LDC-056")
      seccion_nombre - nombre de la sección (ej: "1.3-Armado y soldadura")
      ot_subfolder   - subcarpeta de OT opcional (ej: "OT-005")

    Retorna el link de Drive del archivo subido, o None si falla.
    """
    service = _get_drive_service()
    if service is None:
        return None

    root_folder_id = os.environ.get("GOOGLE_DRIVE_FOLDER_ID", "").strip()
    if not root_folder_id:
        print("[Drive] GOOGLE_DRIVE_FOLDER_ID no configurado")
        return None

    try:
        # Crear estructura de carpetas
        obra_folder_id = _buscar_o_crear_carpeta(service, obra, root_folder_id)
        if not obra_folder_id:
            return None

        seccion_folder_id = _buscar_o_crear_carpeta(service, seccion_nombre, obra_folder_id)
        if not seccion_folder_id:
            return None

        destino_folder_id = seccion_folder_id
        if ot_subfolder:
            ot_folder_id = _buscar_o_crear_carpeta(service, ot_subfolder, seccion_folder_id)
            if ot_folder_id:
                destino_folder_id = ot_folder_id

        # Subir el archivo
        file_metadata = {
            "name": filename,
            "parents": [destino_folder_id],
        }
        media = _MediaIoBaseUpload(io.BytesIO(pdf_bytes), mimetype="application/pdf")
        uploaded = service.files().create(
            body=file_metadata,
            media_body=media,
            fields="id, webViewLink",
        ).execute()

        link = uploaded.get("webViewLink", "")
        print(f"[Drive] Subido: {filename} → {link}")
        return link

    except Exception as e:
        print(f"[Drive] Error subiendo '{filename}': {e}")
        return None


# Importación lazy de MediaIoBaseUpload para evitar error si la librería no está instalada
def _MediaIoBaseUpload(fh, mimetype):
    from googleapiclient.http import MediaIoBaseUpload
    return MediaIoBaseUpload(fh, mimetype=mimetype, resumable=False)
