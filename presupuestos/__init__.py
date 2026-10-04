"""Módulo Presupuestos — aislado del resto del MES.

Ver ESPECIFICACION_MODULO_PRESUPUESTOS_V1.md. Principio de diseño clave:
este paquete no debe depender de otros módulos del MES; el único punto de
integración futuro es la acción "Adjudicar" (sección 5.4 de la especificación).

Este paquete todavía no expone rutas: por ahora solo contiene el modelo de
datos (models.py) y los catálogos/constantes de configuración (constants.py).
El blueprint se registra en app2.py recién cuando se agreguen las vistas
(paso 3 del plan de entrega, sección 7 de la especificación).
"""
from flask import Blueprint

presupuestos_bp = Blueprint(
    "presupuestos",
    __name__,
    url_prefix="/modulo/presupuestos",
)
