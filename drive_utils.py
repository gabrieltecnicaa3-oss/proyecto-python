"""
Utilidades para subir archivos a Google Drive usando la API.

Configuracion soportada (variables de entorno en Railway):

1) Cuenta de servicio (recomendado si se usa Unidad Compartida)
    GOOGLE_CREDENTIALS_JSON  - contenido completo del JSON de la cuenta de servicio
    GOOGLE_DRIVE_FOLDER_ID   - ID de la carpeta raiz en Drive

2) OAuth de usuario (alternativa si NO hay Unidad Compartida)
    GOOGLE_OAUTH_CLIENT_ID
    GOOGLE_OAUTH_CLIENT_SECRET
    GOOGLE_OAUTH_REFRESH_TOKEN
    GOOGLE_DRIVE_FOLDER_ID

Nota: las cuentas de servicio no tienen cuota en Mi unidad. Si no podes usar
Unidad Compartida, usa OAuth de usuario para subir con tu propia cuota.
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
    oauth_client_id = os.environ.get("GOOGLE_OAUTH_CLIENT_ID", "").strip()
    oauth_client_secret = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET", "").strip()
    oauth_refresh_token = os.environ.get("GOOGLE_OAUTH_REFRESH_TOKEN", "").strip()

    try:
        from google.oauth2 import service_account
        from google.oauth2.credentials import Credentials
        from googleapiclient.discovery import build

        scopes = ["https://www.googleapis.com/auth/drive"]

        # Si hay OAuth de usuario, priorizarlo para permitir subir a Mi unidad.
        if oauth_client_id and oauth_client_secret and oauth_refresh_token:
            creds = Credentials(
                token=None,
                refresh_token=oauth_refresh_token,
                token_uri="https://oauth2.googleapis.com/token",
                client_id=oauth_client_id,
                client_secret=oauth_client_secret,
                scopes=scopes,
            )
            _drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
            return _drive_service

        if not credentials_json:
            return None

        info = json.loads(credentials_json)
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
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
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
        folder = service.files().create(body=metadata, fields="id", supportsAllDrives=True).execute()
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
            supportsAllDrives=True,
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
