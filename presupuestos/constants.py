"""Catálogos y constantes de configuración del módulo Presupuestos.

Fuente: ESPECIFICACION_MODULO_PRESUPUESTOS_V1.md, sección 2.2.
Nada de esto se hardcodea en la lógica de cálculo/UI: estas son las listas
de validación; los valores por defecto de tarifas/porcentajes viven en la
tabla `config_presupuestos` (editable desde UI en un paso posterior).
"""

ESTADOS_PRESUPUESTO = ("borrador", "enviado", "adjudicado", "perdido")

TIPOS_SECCION = ("FABRICACION", "MONTAJE")

TIPOS_ITEM_MATERIALES = ("perfil", "porcentaje", "no_listado")

# Tareas estándar que se autocrean (Fabricación + Montaje, en 0) al crear un
# presupuesto nuevo: cubren el flujo habitual, y después se completan o se
# dejan sin usar según lo que requiera cada proyecto.
TAREAS_ESTANDAR = (
    "Estructura Metálica",
    "Correas",
    "Chapeado",
    "Zinguería",
    "Barandas",
    "Escalera Gato",
    "Grating",
)

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
