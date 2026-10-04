"""Migración del módulo Presupuestos: crea sus tablas si no existen.

Uso:
    python migrar_modulo_presupuestos.py

Es idempotente (usa CREATE TABLE IF NOT EXISTS), corre contra el motor
configurado por las variables de entorno de db_utils.py (SQLite o MySQL).
No toca ninguna tabla de otros módulos.
"""
from db_utils import get_db
from presupuestos.models import ensure_tablas_presupuestos


def main():
    db = get_db()
    ensure_tablas_presupuestos(db)
    print("[presupuestos] Tablas verificadas/creadas: presupuestos, tareas, "
          "tarea_secciones, items_costo, catalogo_equipos, "
          "catalogo_esquemas_pintura, config_presupuestos")


if __name__ == "__main__":
    main()
