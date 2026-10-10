# ESPECIFICACIÓN MÓDULO PRESUPUESTOS V1
### MES — Sistema de Gestión de Producción · Módulo nuevo

**Alcance de este documento:** convertir el Excel de presupuestación actual en un módulo del MES, manteniendo la lógica exacta que ya usás, pero estructurada para que Copilot la programe en pasos chicos sin perder el hilo. Este documento es la referencia única — cada prompt a Copilot cita la sección correspondiente, nunca se le pega el documento entero.

**Principios de diseño (no negociables, aplican a todo el módulo):**
1. Módulo aislado (blueprint propio), con sus propias tablas. Solo toca el resto del MES a través de un único punto: la acción "Adjudicar".
2. Nada hardcodeado — tarifas, %, catálogos de equipos y esquemas de pintura son configuración editable, no constantes en el código.
3. El motor de cálculo vive en funciones puras, sin Flask ni base de datos, para poder testearlo con los números reales del Excel como caso de prueba.
4. Reutiliza el catálogo de perfiles que ya existe en el módulo de Compras — no se crea un catálogo de materiales nuevo.

---

## 1. Excel actual — qué hace hoy

Estructura observada en `MKPRESUPUESTO.md`:

- Un presupuesto se divide en **Tareas** (ej: "Cantoneras y rejillas"). Un presupuesto puede tener varias tareas.
- Cada Tarea tiene **una o dos secciones independientes**: Fabricación y/o Montaje — no todas las tareas tienen ambas. Ejemplos reales: "Estructura metálica" (Fab + Mon), "Chapeado" (solo Mon), "Correas" (Fab + Mon), "Zinguería" (Fab + Mon). Se elige al crear la tarea, no es fijo.
- Cada sección tiene sus propios Costos Directos (por rubro), y su propia cascada de GG → Beneficio → Impuestos → Precio de Venta.
- **GG, Beneficio e Impuestos son porcentajes independientes por sección** (fabricación y montaje casi nunca tienen el mismo %, confirmado con tus números: Beneficio Fab 12% vs. Beneficio Montaje 25%).
- Hay un cómputo de materiales aparte (perfil, largo, kg/m del catálogo, m2/m del catálogo) que alimenta tanto el costo de Materiales como el de Pintura (por m2).
- Hay una hoja "Resumen" que cruza todas las tareas y muestra cada categoría de costo con su % sobre el total general.
- Indicador de gestión: costo en USD/kg de estructura (informativo, no interviene en ningún cálculo).

**Validado con tus números reales** (tarea "Cantoneras y rejillas"):
```
Costo Directo Fabricación:     $ 792.672
+ GG 7%:                       $  55.487   → subtotal $ 848.159
+ Beneficio 12%:                $ 101.779   → subtotal $ 949.938
+ Impuestos 5%:                 $  47.497   → PRECIO VENTA FAB = $ 997.434

Costo Directo Montaje:         $ 4.279.818
+ GG 7%:                       $ 299.587   → subtotal $ 4.579.405
+ Beneficio 25%:                $ 1.144.851 → subtotal $ 5.724.257
+ Impuestos 3%:                 $  171.728  → PRECIO VENTA MONTAJE = $ 5.895.985

TOTAL TAREA = 997.434 + 5.895.985 = $ 6.893.419  ✔ coincide con el Excel
```
Este ejemplo se usa como **test unitario real** en el paso 2 del plan de entrega (sección 7).

---

## 2. Modelo de datos

### 2.1 Tablas nuevas

**`presupuestos`** (encabezado)
| Campo | Tipo | Nota |
|---|---|---|
| id | PK | |
| cliente | texto | |
| obra_referencia | texto | antes de existir la OT |
| estado | enum | borrador / enviado / adjudicado / perdido |
| fecha_creacion | fecha | |
| fecha_adjudicacion | fecha nullable | |
| ot_id | FK nullable | se completa al adjudicar |
| tipo_cambio_referencia | decimal nullable | solo para el indicador USD/kg en reportes |

**`tareas`**
| Campo | Tipo | Nota |
|---|---|---|
| id | PK | |
| presupuesto_id | FK | |
| nombre | texto | ej. "Cantoneras y rejillas" |
| orden | entero | |

**`tarea_secciones`** — 1 o 2 filas por tarea (Fabricación y/o Montaje, según corresponda a esa tarea)
| Campo | Tipo | Nota |
|---|---|---|
| id | PK | |
| tarea_id | FK | |
| tipo | enum | FABRICACION / MONTAJE |
| gg_pct | decimal | default del presupuesto, editable |
| beneficio_pct | decimal | default del presupuesto, editable |
| imp_pct | decimal | default del presupuesto, editable |

Restricción: una tarea no puede tener dos secciones del mismo `tipo`, pero puede tener solo una de las dos (ej. "Chapeado" = solo MONTAJE). Al crear la tarea se eligen qué secciones tiene (checkbox Fabricación / checkbox Montaje, al menos una marcada); se puede agregar la sección faltante más adelante si hace falta.

**`items_costo`** — una fila por línea de cualquier rubro
| Campo | Tipo | Nota |
|---|---|---|
| id | PK | |
| tarea_seccion_id | FK | |
| rubro | enum | ver tabla de validación 2.2 |
| tipo_item | texto nullable | usado solo en `materiales` ('perfil' / 'porcentaje') |
| datos | JSON | campos específicos del rubro, ver sección 3 |
| subtotal | decimal | calculado por el motor, persistido para no recalcular todo siempre |

**`catalogo_equipos`** (nuevo, para Montaje)
| Campo | Tipo |
|---|---|
| id | PK |
| nombre | texto (ej. "Hidrogrúa", "Grúa 20tn", "Manlift") |
| tarifa_dia_default | decimal |

**`catalogo_esquemas_pintura`** (nuevo)
| Campo | Tipo |
|---|---|
| id | PK |
| nombre | texto (ej. "Epoxi + Poliuretano", "Antióxido + Sintético", "Directo al metal", "Epoxi") |
| precio_unitario_m2_default | decimal |

**`config_categorias_odoo`** (mapeo editable, usado solo por el Reporte 2 — Sección 5.3)
| Campo | Tipo | Nota |
|---|---|---|
| id | PK | |
| concepto | enum | mismos valores que `rubro` (Sección 2.2) + `GG`, `IMPUESTOS`, `BENEFICIO` (conceptos de la cascada, no son rubros de `items_costo`) |
| tipo_seccion | enum | FABRICACION / MONTAJE |
| categoria_odoo | texto nullable | null = excluido del Reporte 2 (es el caso de BENEFICIO) |

Valores iniciales (cargar como seed, no hardcodear en el reporte):

| concepto | tipo_seccion | categoria_odoo |
|---|---|---|
| mano_obra | FABRICACION | Gastos e Inversión Laboral |
| mano_obra | MONTAJE | Gastos e Inversión Laboral |
| consumibles | FABRICACION | Consumibles de Obra y/o Taller |
| consumibles | MONTAJE | Consumibles de Obra y/o Taller |
| materiales *(incluye perfil, chapa, zingueria, grating)* | FABRICACION | Materiales de Estructuras Metálicas y Herrería |
| bulones | FABRICACION | Materiales de Estructuras Metálicas y Herrería |
| pintura | FABRICACION | Pintura y Accesorios |
| fletes | FABRICACION | Fletes y Traslados de Cargas |
| subcontratos | FABRICACION | Subcontratos - Estructuras Metálicas y Herrería |
| subcontratos | MONTAJE | Subcontratos - Estructuras Metálicas y Herrería |
| equipo | MONTAJE | Alquiler de Maquinaria, Grúas y Vehículos |
| ingenieria | FABRICACION | Proyectos de Ingeniería y Arquitectura |
| ingeniero *(Director de obra)* | MONTAJE | Honorarios y Certificaciones de Obra |
| tecnico_hys | MONTAJE | Honorarios y Certificaciones de Obra |
| GG | FABRICACION | Gastos de Administración |
| GG | MONTAJE | Gastos de Administración |
| IMPUESTOS | FABRICACION | Impuestos |
| IMPUESTOS | MONTAJE | Impuestos |
| BENEFICIO | FABRICACION | *(null — excluido)* |
| BENEFICIO | MONTAJE | *(null — excluido)* |

**`config_presupuestos`** (valores iniciales para presupuestos nuevos, editables desde UI)
| Campo | Tipo |
|---|---|
| gg_pct_default_fab, beneficio_pct_default_fab, imp_pct_default_fab | decimal |
| gg_pct_default_mon, beneficio_pct_default_mon, imp_pct_default_mon | decimal |
| tarifa_dh_taller_default, tarifa_dh_obra_default | decimal |
| tarifa_consumible_dh_taller_default, tarifa_consumible_dh_obra_default | decimal |

Al crear un presupuesto, estos valores se copian a su propia configuración. Se
pueden editar en los datos generales del presupuesto; los cambios globales
posteriores solo sirven de base para presupuestos nuevos y no alteran los ya
creados. Las secciones nuevas toman los porcentajes por defecto del presupuesto.

### 2.2 Rubros válidos por tipo de sección

| Sección | Rubros permitidos |
|---|---|
| FABRICACION | `materiales`, `bulones`, `pintura`, `fletes`, `subcontratos`, `mano_obra`, `consumibles`, `ingenieria` |
| MONTAJE | `mano_obra`, `consumibles`, `equipo`, `subcontratos`, `ingeniero`, `tecnico_hys` |

### 2.3 Tabla existente que se reutiliza (no crear)

El catálogo compartido `articulos_sum` se sincroniza desde
`catalogo_local_importar.csv`, exportado de la base local: 1.155 descripciones
únicas (1.094 perfiles, 29 chapas y 32 grating). Los 1.156 registros locales
incluyen un duplicado idéntico de `TUBO 120x140x6,35`.

La sincronización se ejecuta al acceder a Presupuestos o Suministros, una vez
por versión del archivo. Conserva IDs, códigos y estado activo de los artículos
existentes; actualiza unidad, categoría, kg/m y m²/m por descripción (ignorando
mayúsculas y espacios exteriores), y agrega los faltantes. No elimina artículos
adicionales ni modifica referencias de pedidos, compras o presupuestos.
Los 24 nombres históricos de LPN en pulgadas se reemplazan por sus equivalentes
verificados en milímetros: se reasignan `items_op.articulo_id`,
`items_oc.articulo_id` e `items_costo.perfil_id` (incluido el JSON `datos`) antes
de eliminar las filas en pulgadas. Esta migración se ejecuta una sola vez y
revierte las reasignaciones si falla. Otros nombres distintos no se fusionan.
Los cambios manuales posteriores no se sobrescriben al recargar.

La categoría `Tubos` del archivo se importa como `Tubo circular` (180 artículos).
Una migración adicional elimina los artículos restantes de la categoría antigua
`TUBO CIRCULAR` y luego renombra `Tubos` a `Tubo circular`, conservando los IDs
de estos últimos. Antes de eliminar, deja en NULL sus vínculos en pedidos,
compras y presupuestos (incluido `datos.perfil_id`), sin borrar líneas ni cambiar
sus demás datos o subtotales guardados. Si se recalcula un presupuesto que usaba
un perfil eliminado, es necesario seleccionar un nuevo material del catálogo.
La limpieza distingue el nombre antiguo en mayúsculas del nuevo nombre,
es transaccional y se ejecuta una sola vez incluso con el catálogo ya importado.

**`catalogo_perfiles`** (módulo de Compras): `id`, `nombre`, `kg_m`, `m2_m`. El módulo de Presupuestos solo la referencia por FK desde `items_costo.datos.perfil_id`. Debe poder expandirse desde Compras sin que Presupuestos requiera cambios.

### 2.4 Generalización — familias de materiales

En la pestaña Grating, tanto el selector de familias como el selector de
materiales muestran exclusivamente Grating (`GRA ...` o `Grating ...`).
En Chapeado muestran exclusivamente chapas (`CH ...`, `Chapa ...` o
`Chapas ...`). Se reconoce el tipo de tarea y, para tareas antiguas, su nombre,
sin distinguir mayúsculas ni espacios exteriores. El modo se conserva si la
tarea tiene un nombre personalizado y un tipo reconocido.

El rubro `materiales` no se limita a Perfiles. Cada familia tiene su propia unidad base y su propio catálogo:

| Familia | `tipo_item` | Unidad base | Catálogo | Campos del catálogo |
|---|---|---|---|---|
| Perfil (ya existe) | `perfil` | kg (vía largo) | `catalogo_perfiles` | `kg_m`, `m2_m` |
| Chapa | `chapa` | m2 | `catalogo_chapas` (nuevo) | `precio_m2_default` |
| Zinguería — Plegados y Tubos | `zingueria_ml` | ml | `catalogo_zingueria` (nuevo, campo `categoria`: Plegado/Tubo/Accesorio) | `precio_default` |
| Zinguería — Accesorios | `zingueria_unidad` | unidad | mismo `catalogo_zingueria`, `categoria='Accesorio'` | `precio_default` |
| Zinguería — Fijaciones | `zingueria_fijaciones` | calculado (ver 3.1) | — (tornillo/remache genérico, no necesita catálogo propio) | — |
| Grating — paño | `grating_panel` | m2 | `catalogo_grating` (nuevo) | `precio_m2_default` |
| Grating — escalón | `grating_escalon` | unidad | `catalogo_grating` (mismo, o tabla separada) | `precio_unidad_default` |

**Confirmado con Gabriel — no cambia con esta generalización:**
- El m2 que usa `pintura` (Sección 3.1) sigue sumando **solo** líneas `tipo_item='perfil'`. Chapa y Grating no aportan a ese m2, aunque también tengan superficie.
- El indicador `$/kg` y `USD/kg` (Sección 3.5) sigue calculándose **solo** sobre el peso de líneas `tipo_item='perfil'`. Chapa, Zinguería y Grating quedan fuera del indicador.

Cada catálogo nuevo (`catalogo_chapas`, `catalogo_zingueria`, `catalogo_grating`) puede empezar como una tabla simple (`id`, `nombre`, precio default) — no necesitan la complejidad de `catalogo_perfiles`.

---

## 3. Motor de cálculo — fórmulas exactas

### 3.1 Fabricación — por rubro

Los m² de Placas (`materiales` / tipo `porcentaje`) se calculan automáticamente
como `0.05 × Σ m2` de todas las líneas de perfiles de la misma sección,
independientemente del orden de carga. Este 5% es fijo y no depende del
porcentaje editable usado para calcular su peso. Los m² de placas se muestran
en su fila y se incluyen en el total de m² de materiales; no modifican la base
del cálculo de pintura.

| Rubro | `datos` (JSON) | Fórmula de `subtotal` |
|---|---|---|
| `materiales` / tipo `perfil` | `{perfil_id, cantidad, largo_mm, cant_barras?, precio_unitario_kg}` | `largo_m = largo_mm/1000`<br>`peso = cantidad × largo_m × perfil.kg_m`<br>`m2 = cantidad × largo_m × perfil.m2_m`<br>`subtotal = peso × precio_unitario_kg` |
| `materiales` / tipo `porcentaje` | `{descripcion, porcentaje}` | `subtotal = porcentaje × subtotal_materiales_perfiles` (suma de las líneas tipo `perfil` de la misma sección, cargadas antes) |
| `materiales` / tipo `chapa` | `{chapa_id, cantidad, ancho_mm, largo_mm, precio_m2}` | `m2 = cantidad × (ancho_mm/1000) × (largo_mm/1000)`<br>`subtotal = m2 × precio_m2`<br>(este m2 NO se suma al m2_total_materiales de Pintura — ver Sección 2.4) |
| `materiales` / tipo `zingueria_ml` (Plegados: cumbrera, canaleta, babeta, esquinero / Tubos de desagüe) | `{zingueria_id, cantidad, largo_mm, precio_ml}` | `subtotal = cantidad × (largo_mm/1000) × precio_ml` |
| `materiales` / tipo `zingueria_unidad` (Accesorios: codos, bocas, curvas, ramal T) | `{zingueria_id, cantidad, precio_unidad}` | `subtotal = cantidad × precio_unidad` |
| `materiales` / tipo `zingueria_fijaciones` (tornillos/remaches) | `{tornillos_por_metro, precio_unitario}` — `tornillos_por_metro` prefijado en 4, editable | `ml_total_zingueria = Σ (cantidad × largo_mm/1000)` de todas las líneas `zingueria_ml` de la misma sección (Plegados + Tubos juntos — avisame si Tubos no debería contar acá)<br>`cantidad_tornillos = tornillos_por_metro × ml_total_zingueria`<br>`subtotal = cantidad_tornillos × precio_unitario` |
| `materiales` / tipo `grating_panel` | `{grating_id, cantidad, ancho_mm, largo_mm, precio_m2}` | `m2 = cantidad × (ancho_mm/1000) × (largo_mm/1000)`<br>`subtotal = m2 × precio_m2` |
| `materiales` / tipo `grating_escalon` | `{grating_id, cantidad, precio_unidad}` | `subtotal = cantidad × precio_unidad` |
| `bulones` | `{porcentaje}` | `subtotal = porcentaje × subtotal_materiales` (dato de entrada: %) |
| `pintura` | `{esquema_id, precio_unitario_m2}` | `subtotal = precio_unitario_m2 × m2_total_materiales` (m2 acumulado de las líneas `materiales`/`perfil` de la misma sección) |
| `fletes` | `{descripcion, cantidad, precio_unitario}` (múltiples líneas) | `subtotal = cantidad × precio_unitario` |
| `subcontratos` | `{descripcion, cantidad, precio_unitario}` (múltiples líneas) | `subtotal = cantidad × precio_unitario` |
| `mano_obra` (taller) | `{operarios, dias, tarifa_dh}` (tarifa_dh precarga `tarifa_dh_taller_default`) | `subtotal = operarios × dias × tarifa_dh` |
| `consumibles` (taller) | `{operarios, dias, tarifa_dh}` (mismos operarios/días que mano_obra; tarifa precarga `tarifa_consumible_dh_taller_default`) | `subtotal = operarios × dias × tarifa_dh` |
| `ingenieria` | `{monto}` (dato de entrada: $, manual) | `subtotal = monto`<br>`porcentaje_informativo = monto / subtotal_materiales` (solo se muestra, no se usa en ningún cálculo posterior) |

`COSTO_DIRECTO_FAB = Σ subtotal de todos los items de la sección Fabricación`

### 3.2 Montaje — por rubro

| Rubro | `datos` (JSON) | Fórmula de `subtotal` |
|---|---|---|
| `mano_obra` (obra) | `{operarios, dias, tarifa_dh}` (precarga `tarifa_dh_obra_default`) | `subtotal = operarios × dias × tarifa_dh` |
| `consumibles` (obra) | `{operarios, dias, tarifa_dh}` (precarga `tarifa_consumible_dh_obra_default`) | `subtotal = operarios × dias × tarifa_dh` |
| `equipo` | `{equipo_id, dias, tarifa_dia}` (múltiples líneas; tarifa_dia precarga desde `catalogo_equipos`) | `subtotal = dias × tarifa_dia` por línea |
| `subcontratos` | `{descripcion, cantidad, precio_unitario}` | `subtotal = cantidad × precio_unitario` |
| `ingeniero` (Director de obra) | `{tarifa_dia, dias}` | `subtotal = tarifa_dia × dias` |
| `tecnico_hys` | `{tarifa_dia, dias}` | `subtotal = tarifa_dia × dias` |

`COSTO_DIRECTO_MON = Σ subtotal de todos los items de la sección Montaje`

### 3.3 Cascada final — igual para ambas secciones, % propios de cada una

```
GG          = gg_pct × COSTO_DIRECTO
subtotal_1  = COSTO_DIRECTO + GG
BENEFICIO   = beneficio_pct × subtotal_1
subtotal_2  = subtotal_1 + BENEFICIO
IMPUESTOS   = imp_pct × subtotal_2
PRECIO_VENTA = subtotal_2 + IMPUESTOS
```
⚠️ Orden confirmado con datos reales: **GG → Beneficio → Impuestos** (no Impuestos antes que Beneficio).

### 3.4 Totales

```
PRECIO_VENTA_TAREA = PRECIO_VENTA_FAB + PRECIO_VENTA_MON
PRECIO_VENTA_PRESUPUESTO = Σ PRECIO_VENTA_TAREA (todas las tareas del presupuesto)
```

### 3.5 Indicador de reporte — $/kg y USD/kg (no interviene en el cálculo del precio)

```
peso_total_kg   = Σ peso de líneas materiales/perfil (todas las tareas)
costo_estructura = $materiales + $mano_obra_taller + $mano_obra_obra + $pintura
indicador_pesos_kg = costo_estructura / peso_total_kg
indicador_usd_kg   = indicador_pesos_kg / tipo_cambio_referencia
```

---

## 4. Pantallas

**Principio de interacción — vista tipo planilla, no wizard de pasos.** La pantalla de una Tarea tiene que verse y comportarse como la hoja de Excel: todos los rubros visibles a la vez, sin navegar entre pasos ni tocar un botón "Guardar" por rubro. Nada de "cargo materiales → guardo → cargo mano de obra → guardo...". Esto cambia el patrón de guardado (ver 4.4) más que el layout en sí.

1. **Listado de presupuestos** — cliente, obra, estado, precio de venta total, fecha.
2. **Presupuesto → Datos generales** — cliente, obra, estado.
3. **Presupuesto → por Tarea** (se pueden agregar tareas; cada una es un bloque, no un tab que oculta el resto):
   - Layout de una sola vista continua (scroll, no tabs entre rubros) para cada sección que la tarea tenga: si la tarea es Fab+Mon, ambos sub-bloques se ven uno debajo del otro; si es solo Montaje, no se muestra el bloque de Fabricación.
   - Cada sub-bloque (Fabricación/Montaje) muestra **todas sus tablas de rubros a la vez**, apiladas, tal como están agrupadas en el Excel — no una tabla por vez.
   - **Panel de resultado fijo/sticky** (visible sin scrollear, igual que mirar la esquina del Excel mientras cargás datos): Costo Directo, GG, Beneficio, Impuestos, Precio de Venta — de la sección, de la tarea, y del presupuesto completo.
   - **Panel de indicadores instantáneos**, en el mismo sticky: $/kg y USD/kg (Sección 3.5), para que se vea de un vistazo si el precio está en mercado mientras se sigue cargando.
4. **Resumen del presupuesto** — todas las tareas, total FAB + MON por tarea, gran total.
5. **Configuración** — valores iniciales para presupuestos nuevos (`config_presupuestos`), catálogo de equipos y catálogo de esquemas de pintura. Cada presupuesto conserva y permite editar sus propios porcentajes y tarifas por defecto.

### 4.4 Patrón de guardado — autosave, no botón por rubro

- Cada celda/campo editado dispara un **autosave con debounce** (ej. 500-800ms después de que el usuario deja de tipear, o al perder foco del campo) — no un botón "Guardar" explícito por línea ni por rubro.
- Después de cada autosave exitoso, el frontend vuelve a pedir el recálculo (el mismo endpoint de recálculo del paso 3) y **actualiza el panel de resultado e indicadores en el momento**, sin recargar la pantalla.
- **La fórmula de cálculo NO se duplica en JavaScript.** El frontend nunca calcula subtotales por su cuenta — siempre muestra lo que devuelve el endpoint de recálculo (única fuente de verdad: `calculo_presupuesto.py`). Esto evita que el motor de cálculo tenga dos versiones (Python y JS) que se puedan desincronizar.
- Mientras el autosave está en curso, mostrar un indicador chico de "guardando..." no bloqueante — el usuario tiene que poder seguir tipeando en otro campo sin esperar.

---

## 5. Reportes

1. **Resumen por categoría** (igual a la hoja "Resumen" del Excel): cruza todas las tareas y muestra cada categoría (Mano de obra taller, Consumibles taller, Mano de obra montaje, Consumibles montaje, Materiales, Pintura, Fletes, Equipos, Ingeniería, Director de obra, Técnico HyS, GG Fab, GG Mon, Impuestos Fab, Impuestos Mon, Beneficio Fab, Beneficio Mon) con $ y % sobre el total general.
2. **Indicador $/kg y USD/kg** — sección 3.5.
3. **Resumen de recursos a obra** (exportable) — mano de obra total, equipos necesarios, materiales a comprar. Es el resumen que hoy armás a mano; automatizarlo es el objetivo original de este módulo.
4. **Adjudicación → OT** — único punto de integración con el resto del MES: crea la OT y precarga el resumen de recursos y el detalle de materiales para Compras.

---

## 5.2 Reporte 1 — Explosión de insumos (Excel)

Librería recomendada: `openpyxl` (ya estándar para exportar Excel desde Flask).

**Estructura del workbook:**
- **Una pestaña por Tarea** (nombre de pestaña = nombre de la tarea). Cada pestaña muestra, para esa tarea, una tabla con columnas `Categoría | Fabricación ($) | Montaje ($)`:
  - Filas que existen en ambas secciones (Mano de obra, Consumibles, Subcontratos, GG, Impuestos, Beneficio): valor en ambas columnas.
  - Filas exclusivas de Fabricación (Materiales — incluye perfil+chapa+zingueria+grating, Bulones, Pintura, Fletes, Ingeniería): valor solo en columna Fabricación, `—` en Montaje.
  - Filas exclusivas de Montaje (Equipos, Director de obra, Técnico HyS): valor solo en columna Montaje, `—` en Fabricación.
  - Fila final: `TOTAL` = Precio de Venta Fabricación + Precio de Venta Montaje de esa tarea.
- **Una pestaña "Resumen"**: misma estructura de columnas, pero sumando todas las tareas del presupuesto (es el total general).

Esta pestaña "Resumen" usa exactamente los mismos números que la Sección 5 punto 1 (resumen por categoría) — es la versión exportable de esa pantalla.

## 5.3 Reporte 2 — Previsión de fondos para Odoo (Excel)

Una sola tabla (confirmado: sin pestañas por tarea), construida a partir de los totales de la pestaña "Resumen" del Reporte 1, pero reclasificando cada concepto a su `categoria_odoo` (tabla `config_categorias_odoo`, Sección 2.1) y agrupando por esa categoría.

**Columnas:**
```
CATEGORIA | TA-{obra_referencia} | {obra_referencia}
```
- `obra_referencia` sale de `presupuestos.obra_referencia`.
- Columna `TA-{obra}` = suma de los conceptos cuyo `tipo_seccion = FABRICACION` que mapean a esa categoría.
- Columna `{obra}` = suma de los conceptos cuyo `tipo_seccion = MONTAJE` que mapean a esa categoría.
- Si dos conceptos mapean a la misma categoría dentro de la misma sección (ej. Director de obra + Técnico HyS, ambos Montaje → "Honorarios y Certificaciones de Obra"), se suman en una sola fila.
- Si dos conceptos de distinta sección mapean a la misma categoría (ej. Mano de obra Fab + Mano de obra Mon → "Gastos e Inversión Laboral"), cada uno va a su propia columna, no se suman entre sí.
- Los conceptos con `categoria_odoo = null` (Beneficio Fab y Beneficio Mon) **no aparecen** en este reporte.

**Filas** (orden sugerido, agrupando categorías repetidas):
```
Materiales de Estructuras Metálicas y Herrería   | Σ materiales+bulones (todas tareas) | —
Pintura y Accesorios                              | Σ pintura                            | —
Fletes y Traslados de Cargas                      | Σ fletes                             | —
Proyectos de Ingeniería y Arquitectura            | Σ ingenieria                         | —
Gastos e Inversión Laboral                        | Σ mano_obra_taller                   | Σ mano_obra_montaje
Consumibles de Obra y/o Taller                    | Σ consumibles_taller                 | Σ consumibles_montaje
Subcontratos - Estructuras Metálicas y Herrería   | Σ subcontratos_fab                   | Σ subcontratos_mon
Alquiler de Maquinaria, Grúas y Vehículos         | —                                     | Σ equipos
Honorarios y Certificaciones de Obra              | —                                     | Σ ingeniero + Σ tecnico_hys
Gastos de Administración                          | Σ GG_fab                             | Σ GG_mon
Impuestos                                          | Σ impuestos_fab                      | Σ impuestos_mon
```

---

## 6. Elementos precargados (V0 — copia simple)

Para elementos que se repiten (ej. "Escalera marinera", "Baranda tipo estándar"): guardar un set de líneas de referencia y poder insertarlas de una vez en una tarea, en vez de tipear cada rubro de memoria cada vez.

**`catalogo_elementos_compuestos`**
| Campo | Tipo |
|---|---|
| id | PK |
| nombre | texto (ej. "Escalera marinera") |
| descripcion | texto |

**`catalogo_elementos_compuestos_lineas`** — una copia congelada de una línea de `items_costo`, sin `tarea_seccion_id`
| Campo | Tipo |
|---|---|
| id | PK |
| elemento_id | FK |
| tipo_seccion | FABRICACION / MONTAJE (a qué sección va esta línea al insertarla) |
| rubro | igual que `items_costo.rubro` |
| tipo_item | igual que `items_costo.tipo_item` |
| datos | JSON, mismo formato que `items_costo.datos` |

**Cómo se usa:**
1. El usuario arma una tarea normal, la carga como corresponde, y desde ahí elige "Guardar como elemento precargado" con un nombre — esto copia todas las líneas de esa tarea (de ambas secciones si tiene) a `catalogo_elementos_compuestos` + sus líneas.
2. En cualquier tarea futura, "Insertar elemento precargado" → elige el elemento → el sistema **duplica tal cual** todas sus líneas dentro de la tarea actual (en la sección correspondiente según `tipo_seccion`).
3. El usuario ajusta a mano las cantidades que cambien (largo real, cantidad de escalones, etc.) — no hay escalado automático en esta versión.

**V1 (futuro, no programar ahora):** si en el uso real se ve que siempre se ajusta el mismo campo de la misma forma predecible al cambiar el largo/cantidad total, se puede agregar escalado automático por regla de rubro (cantidad en materiales, días en mano de obra/equipos). No vale la pena construirlo antes de tener ese patrón confirmado con uso real.

---

## 7. Conexión futura (no programar todavía, solo dejar la puerta abierta)

- El campo `cant_barras` en materiales está pensado para conectarse más adelante con el optimizador de corte de barras (ya existe como proyecto aparte) — hoy se carga a mano, en una v2 podría calcularse automático.
- El motor de cálculo (sección 3) debe quedar en funciones puras sin dependencias del resto del MES, precisamente para poder extraerlo como producto independiente de IndustrialApps sin reescritura, si más adelante se decide venderlo aparte.

---

## 8. Plan de entrega a Copilot — un prompt por paso, nunca el documento completo

| Paso | Qué pedirle a Copilot | Qué citarle de este documento |
|---|---|---|
| 1 | Tablas y migraciones | Sección 2 completa |
| 2 | Motor de cálculo puro (`calculo_presupuesto.py`), sin Flask ni DB. **Usar el ejemplo de la sección 1 como test unitario real** — si no da exacto $997.434 y $5.895.985, algo está mal antes de seguir. | Sección 3 completa + el ejemplo numérico de la sección 1 |
| 3 | Endpoints CRUD (presupuesto, tarea, item) + endpoint de recálculo que llama al motor del paso 2 | Sección 2.1 (tablas) + firma de funciones del paso 2 |
| 4 (revisado) | UI de carga por tarea — vista única tipo planilla con autosave, no wizard por rubro | Sección 4 completa (puntos 1-3 y 4.4) |
| 5 | Resumen y reportes | Sección 4 punto 4, Sección 5 |
| 6 | Configuración (tarifas, catálogo equipos, catálogo pintura) | Sección 2.1 (`config_presupuestos`, `catalogo_equipos`, `catalogo_esquemas_pintura`) + Sección 4 punto 5 |
| 7 | Botón "Adjudicar" — único paso que toca el resto del MES | Sección 5 punto 4 |

Cada paso es una sesión de Copilot separada. No avances al paso N+1 sin haber validado el paso N (especialmente el paso 2 — si el motor de cálculo no reproduce tus números exactos, todo lo que se construya arriba hereda el error).
