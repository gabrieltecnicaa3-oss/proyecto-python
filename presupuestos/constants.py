"""Catálogos y constantes de configuración del módulo Presupuestos.

Fuente: ESPECIFICACION_MODULO_PRESUPUESTOS_V1.md, sección 2.2.
Nada de esto se hardcodea en la lógica de cálculo/UI: estas son las listas
de validación; los valores por defecto de tarifas/porcentajes viven en la
tabla `config_presupuestos` (editable desde UI en un paso posterior).
"""

ESTADOS_PRESUPUESTO = ("borrador", "enviado", "adjudicado", "perdido")

TIPOS_SECCION = ("FABRICACION", "MONTAJE")

TIPOS_ITEM_MATERIALES = ("perfil", "porcentaje", "no_listado", "chapa", "tornillos", "grating", "fijaciones")

# Tareas cuyo rubro "materiales" (sección FABRICACION) usa el modelo de
# superficie (m2, tipo_item "chapa"/"tornillos") en vez del modelo de perfiles
# estructurales (kg, tipo_item "perfil"/"porcentaje"). También se les oculta
# ingeniería y bulones (no aplican a chapa/zinguería).
TAREAS_MODO_CHAPA = ("Chapeado",)

# Tareas cuyo rubro "materiales" usa el modelo de Grating (m2 + kg/m2
# automático desde el catálogo, tipo_item "grating"/"fijaciones"). Se les
# oculta bulones (no aplica a Grating), pero sí usan ingeniería.
TAREAS_MODO_GRATING = ("Grating",)

# Tareas estándar que se autocrean (Fabricación + Montaje, en 0) al crear un
# presupuesto nuevo: cubren el flujo habitual, y después se completan o se
# dejan sin usar según lo que requiera cada proyecto.
TAREAS_ESTANDAR = (
    "Insertos",
    "Estructura Metálica",
    "Correas",
    "Chapeado",
    "Zinguería",
    "Barandas",
    "Esc Marinera",
    "Grating",
)

# Opciones fijas que se ofrecen en el selector del botón "+" para agregar una
# tarea nueva a un presupuesto ya creado (mismo set que TAREAS_ESTANDAR, en el
# orden pedido para ese selector).
TAREAS_SELECCIONABLES = (
    "Insertos",
    "Estructura Metálica",
    "Correas",
    "Chapeado",
    "Zinguería",
    "Barandas",
    "Esc Marinera",
    "Grating",
)

# Plantilla de materiales (tipo_item "perfil") que se autocrean para el rubro
# "materiales" de FABRICACION la primera vez que se abre una tarea de ese
# nombre (si todavía no tiene ninguna línea de perfil cargada). `catalogo_descripcion`
# debe matchear exacto la descripcion de articulos_sum para resolver el perfil_id;
# cantidad/largo_mm quedan en 0 para completar por proyecto.
MATERIALES_TEMPLATE_POR_TAREA = {
    "Barandas": (
        {"descripcion": "Pasamano y parantes", "catalogo_descripcion": "TUBO C DIAM 50,80 x 2,00", "precio_unitario_kg": 1.35},
        {"descripcion": "Guardarrodilla", "catalogo_descripcion": "TUBO C DIAM 38,10 x 2,00", "precio_unitario_kg": 135},
        {"descripcion": "Guardapie", "catalogo_descripcion": "PL 4''x3/16''", "precio_unitario_kg": 1.4},
    ),
    "Esc Marinera": (
        {"descripcion": "Zancas", "catalogo_descripcion": "LPN 64x6,4", "precio_unitario_kg": 1.35, "cantidad": 2},
        {"descripcion": "Guardahombre", "catalogo_descripcion": "PL 2''x3/16''", "precio_unitario_kg": 1.4, "largo_mm": 1500},
        {"descripcion": "Guardahombre", "catalogo_descripcion": "PL 2''x3/16''", "precio_unitario_kg": 1.4, "cantidad": 5},
        {"descripcion": "Escalones", "catalogo_descripcion": "D 20,64", "precio_unitario_kg": 1.35},
    ),
}

# Rubros válidos por tipo de sección (sección 2.2).
RUBROS_POR_SECCION = {
    "FABRICACION": (
        "materiales",
        "ingenieria",
        "bulones",
        "pintura",
        "mano_obra",
        "consumibles",
        "subcontratos",
        "fletes",
    ),
    "MONTAJE": (
        "mano_obra",
        "consumibles",
        "equipo",
        "subcontratos",
        "ingeniero",
        "tecnico_hys",
    ),
}


def rubro_valido(tipo_seccion, rubro):
    """True si `rubro` está permitido para la sección FABRICACION/MONTAJE."""
    tipo_norm = str(tipo_seccion or "").strip().upper()
    rubro_norm = str(rubro or "").strip()
    return rubro_norm in RUBROS_POR_SECCION.get(tipo_norm, ())


# ─────────────────────────────────────────────────────────────────
# Reporte 3 — planilla de carga masiva de pedidos de compra "día 0" en Odoo
# ─────────────────────────────────────────────────────────────────

# Sin CUIT: Odoo no acepta el texto con el número.
ODOO_VENDOR = "A3 Servicios Constructivos S.R.L."

# Order Reference de Fabricación = prefijo + obra; el de Montaje es solo la obra.
ODOO_PREFIJO_FABRICACION = "TA-"

ODOO_PEDIDO_COLUMNAS = (
    "Order Reference",
    "Vendor*",
    "Order Deadline",
    "Expected Arrival",
    "Vendor Reference",
    "Order Lines/Products*",
    "Order Lines/Quantity",
    "Order Lines/Unit Price",
    "Order Lines/Taxes",
    "Payment Terms",
    "Order Lines/Analytic Distribution",
)

# Rubros que entran al pedido por sección (el resto — mano_obra, consumibles,
# fletes, ingeniero, tecnico_hys, GG, impuestos, beneficio — no se carga).
ODOO_RUBROS_POR_SECCION = {
    "FABRICACION": ("materiales", "bulones", "pintura", "ingenieria", "subcontratos"),
    "MONTAJE": ("equipo", "subcontratos"),
}

# Sugerencias para el campo producto_odoo de catalogo_equipos (texto libre en la
# pantalla de configuración; esto solo alimenta el autocompletado).
ODOO_PRODUCTOS_EQUIPOS_SUGERIDOS = (
    "Alquiler de HIDRO 42 TM",
    "Alquiler HAULOTTE BRAZO ARTICULADO 16M ALTURA",
    "Alquiler GRUA 30 TONELADAS",
)
