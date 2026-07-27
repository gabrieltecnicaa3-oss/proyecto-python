# MES TEMPLATE — Guía de personalización para un nuevo cliente

## ¿Qué es esta plantilla?

Esta es la plantilla maestra del sistema MES (Manufacturing Execution System).  
Contiene toda la arquitectura, módulos y base de datos del sistema, sin datos específicos de ningún cliente.

---

## Estructura de módulos incluidos

| Módulo | Archivo Blueprint | Ruta base |
|---|---|---|
| Órdenes de Trabajo | `ot_routes.py` | `/modulo/ot` |
| Calidad (controles) | `calidad_routes.py` | `/modulo/calidad` |
| Gestión de Calidad | `gestion_calidad_routes.py` | `/modulo/gestion-calidad` |
| Partes de Trabajo | `parte_routes.py` | `/modulo/parte` |
| Remitos | `remito_routes.py` | `/modulo/remitos` |
| Estado de Producción | `estado_routes.py` | `/modulo/estado` |
| Panel de Producción | `produccion_routes.py` | `/modulo/produccion` |
| Generador QR | `generador_routes.py` | `/modulo/generador` |
| Programación | `programacion_routes.py` | `/modulo/programacion` |
| Reportes | `reportes_routes.py` | `/modulo/reportes` |
| Tablero Ejecutivo | `tablero_ejecutivo_routes.py` | `/modulo/tablero-ejecutivo` |
| Análisis Estratégico | `analisis_estrategico_routes.py` | `/modulo/analisis-estrategico` |
| Auditoría de Obra | `auditoria_obra_routes.py` | `/modulo/auditoria-obra` |
| Económico | `economico_routes.py` | `/modulo/economico` |
| Suministros / Compras | `suministros_routes.py` | `/modulo/suministros` |

---

## Pasos para crear un nuevo proyecto desde esta plantilla

### 1. Copiar la carpeta

```
Copiar MES_TEMPLATE → MES_NombreEmpresa
```

### 2. Configurar usuarios iniciales

Editar `app2.py`, sección `USUARIOS_INICIALES`:

```python
USUARIOS_INICIALES = [
    ("admin", "contrasena_segura", "Nombre Admin", ROLE_ADMIN, 1),
    ("supervisor1", "temp1234", "Nombre Supervisor", ROLE_SUPERVISOR, 1),
    ("operario1", "temp1234", "Nombre Operario", ROLE_OBRA, 1),
    # ... agregar todos los usuarios del cliente
]
```

> **Importante:** Cambiar las contraseñas después del primer login desde el panel de administración.

### 3. Configurar el inspector de calidad (firma OK)

Editar `app2.py` y `calidad_routes.py`:

```python
FIRMA_OK_AUTOMATICA = "NOMBRE INSPECTOR"   # app2.py
_FIRMA_OK_AUTOMATICA = "NOMBRE INSPECTOR"  # calidad_routes.py

INSPECTOR_FIRMAS = {
    "Nombre Inspector 1": "NOMBRE INSPECTOR 1",
    "Nombre Inspector 2": "NOMBRE INSPECTOR 2",
}
```

### 4. Cargar firmas digitales

Colocar archivos de imagen `.png` o `.jpg` en la carpeta `Firmas empleados/`.

- Para la firma OK automática: `FIRMA_OK.png`
- Para firmas de supervisores en reportes: `firma_jefe_produccion.png`, `firma_responsable_tecnico.png`
- Para la firma del coordinador en auditorías: `firma_coordinador.png`

Luego editar en `reportes_routes.py` y `auditoria_obra_routes.py` los nombres de archivo y de persona.

### 5. Configurar supervisores del módulo Suministros

Editar `suministros_routes.py`:

```python
SUPERVISORES = ["Nombre Supervisor 1", "Nombre Supervisor 2"]
```

### 6. Cargar proveedores (opcional)

Editar `suministros_routes.py`, lista `PROVEEDORES_EXCEL`. Puede quedar vacía y cargarse manualmente desde la interfaz.

### 7. Configurar variables de entorno

```bash
cp .env.example .env
# Editar .env con los valores reales
```

Variables clave:
- `FLASK_SECRET_KEY` → generar con `python -c "import secrets; print(secrets.token_hex(32))"`
- `DB_ENGINE` → `sqlite` (local) o `mysql` (producción)
- Si MySQL: completar `MYSQL_HOST`, `MYSQL_DB`, `MYSQL_USER`, `MYSQL_PASSWORD`
- Si Google Drive: completar `GOOGLE_CREDENTIALS_JSON` y `GOOGLE_DRIVE_FOLDER_ID`

### 8. Crear entorno virtual e instalar dependencias

```bash
python -m venv .venv
.venv\Scripts\activate       # Windows
source .venv/bin/activate    # Linux/Mac
pip install -r requirements.txt
```

### 9. Ejecutar localmente

```bash
# Windows (doble clic o desde terminal):
run_local.bat

# Directamente:
python app2.py
```

### 10. Desplegar en Railway (producción)

El archivo `Procfile` ya está configurado:
```
web: gunicorn app2:app --bind 0.0.0.0:$PORT --workers 1 --timeout 120
```

Configurar las variables de entorno en Railway antes del deploy.

---

## Agregar o quitar módulos

### Para quitar un módulo:
1. Comentar o eliminar el `import` y el `app.register_blueprint(...)` correspondiente en `app2.py` (al final del archivo).
2. El resto de la aplicación sigue funcionando normalmente.

### Para agregar un módulo nuevo:
1. Crear el archivo `nuevo_modulo_routes.py` con un Blueprint Flask.
2. Importarlo y registrarlo en `app2.py`:
   ```python
   from nuevo_modulo_routes import nuevo_modulo_bp
   app.register_blueprint(nuevo_modulo_bp)
   ```

---

## Roles del sistema

| Rol | Acceso |
|---|---|
| `administrador` | Acceso completo + tablero ejecutivo + reportes |
| `supervisor` | Acceso a todos los módulos operativos |
| `obra` | Solo lectura y escaneo QR (no puede crear/eliminar datos críticos) |

---

## Base de datos

- **SQLite** (por defecto): archivo `database.db` creado automáticamente al iniciar.
- **MySQL** (producción): configurar variables de entorno `MYSQL_*`.
- **Migración SQLite → MySQL**: ejecutar `python migrate_sqlite_to_mysql.py`.

Las tablas se crean automáticamente al primer inicio (función `init_db()` en `app2.py`).

---

## Catálogo de artículos (Suministros)

El archivo `articulos_seed.py` contiene perfiles estructurales de acero como punto de partida.  
Se puede editar o reemplazar según los materiales del cliente.

---

## Notas de arquitectura

- `app2.py` → Aplicación principal: inicialización, autenticación, rutas core.
- `db_utils.py` → Capa de acceso a datos, compatible SQLite y MySQL.
- `proceso_utils.py` → Lógica de estados de piezas y trazabilidad.
- `qr_utils.py` → Generación y lectura de QRs, helpers de Excel.
- `drive_utils.py` → Integración con Google Drive.
