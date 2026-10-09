"""Pantalla de mapeo Tarea -> OT de un presupuesto adjudicado (paso previo al
volcado del previsto al módulo económico, que NO se escribe acá).

Escribe solo en `tarea_ot_reparto`. Una tarea sin filas queda como
"Sin OT (nivel obra)".
"""
import json

from flask import request, jsonify

from . import presupuestos_bp
from .routes import _db, _calcular_resultado_tarea
from .models import (
    obtener_presupuesto,
    listar_tareas,
    listar_reparto_tareas,
    reemplazar_reparto_tareas,
)
from .reparto_ot import validar_reparto_tarea, sugerir_porcentajes_por_kg
from .views import _ESTILO_BASE, _obtener_obra_referencia

_MAX_OT_KG = 50


def _tareas_con_fabricacion(db, presupuesto_id):
    """Tareas con Fabricación de costo directo > 0, con el precio de venta de esa sección."""
    tareas = []
    for tarea in listar_tareas(db, presupuesto_id):
        fab = _calcular_resultado_tarea(db, tarea["id"])["fabricacion"]
        if fab["resumen"]["costo_directo"] > 0:
            tareas.append({
                "id": tarea["id"],
                "nombre": tarea["nombre"],
                "precio_venta": fab["cascada"]["precio_venta"],
            })
    return tareas


def _etiqueta_ot(ot_id, obra, titulo, cerrada):
    partes = [f"OT {ot_id}", obra, titulo]
    texto = " - ".join(p for p in partes if p)
    return f"{texto} (cerrada)" if cerrada else texto


def _ots_para_selector(db, obra_referencia, ot_ids_guardadas):
    """Devuelve (ots, aviso). Solo las OT de la obra; si no hay ninguna, todas + aviso.
    Las OT de mantenimiento no son destino de un presupuesto."""
    filas = db.execute(
        """
        SELECT id, TRIM(COALESCE(obra, '')), TRIM(COALESCE(titulo, '')), fecha_cierre
        FROM ordenes_trabajo
        WHERE es_mantenimiento IS NULL OR es_mantenimiento = 0
        ORDER BY id DESC
        """
    ).fetchall()
    todas = [
        {"id": int(r[0]), "obra": r[1], "etiqueta": _etiqueta_ot(r[0], r[1], r[2], bool(r[3]))}
        for r in filas
    ]

    obra_norm = (obra_referencia or "").strip().lower()
    de_la_obra = [o for o in todas if obra_norm and o["obra"].lower() == obra_norm]

    aviso = ""
    if de_la_obra:
        ots = de_la_obra
    else:
        ots = todas
        aviso = (
            f"No hay OT cargadas para la obra «{obra_referencia}». Se muestran todas las OT."
            if obra_norm else
            "El presupuesto no tiene una obra de referencia. Se muestran todas las OT."
        )

    # Una OT ya guardada que quedó fuera de la lista (otra obra) igual tiene que verse.
    presentes = {o["id"] for o in ots}
    ots += [o for o in todas if o["id"] in ot_ids_guardadas and o["id"] not in presentes]
    return [{"id": o["id"], "etiqueta": o["etiqueta"]} for o in ots], aviso


def _pagina_mensaje(presupuesto_id, mensaje):
    return f"""
    <html><head><meta name="viewport" content="width=device-width, initial-scale=1">
    <style>{_ESTILO_BASE}</style></head>
    <body><div class="wrap">
        <div class="card">
            <h3 style="margin-top:0;">Mapeo Tarea → OT</h3>
            <p>{mensaje}</p>
            <a href="/modulo/presupuestos/{presupuesto_id}/resumen" class="btn btn-secondary">⬅️ Volver al resumen</a>
        </div>
    </div></body></html>
    """


@presupuestos_bp.route("/<int:presupuesto_id>/reparto", methods=["GET"])
def vista_reparto_ot(presupuesto_id):
    db = _db()
    presupuesto = obtener_presupuesto(db, presupuesto_id)
    if not presupuesto:
        return "<h3>❌ Presupuesto no encontrado</h3>", 404
    if presupuesto.get("estado") != "adjudicado":
        return _pagina_mensaje(presupuesto_id, "El mapeo Tarea → OT solo está disponible para presupuestos adjudicados."), 400

    tareas = _tareas_con_fabricacion(db, presupuesto_id)
    reparto = listar_reparto_tareas(db, [t["id"] for t in tareas])
    for tarea in tareas:
        tarea["reparto"] = reparto.get(tarea["id"], [])

    guardadas = {fila["ot_id"] for filas in reparto.values() for fila in filas}
    obra_referencia = _obtener_obra_referencia(db, presupuesto, presupuesto_id)
    ots, aviso_ots = _ots_para_selector(db, obra_referencia, guardadas)

    datos = {"tareas": tareas, "ots": ots, "aviso_ots": aviso_ots}
    # "</" escapado para que ningún nombre de tarea/OT pueda cerrar el <script>.
    datos_js = json.dumps(datos, ensure_ascii=False).replace("</", "<\\/")
    return (
        _REPARTO_HTML
        .replace("__ESTILO__", _ESTILO_BASE)
        .replace("__PRESUPUESTO_ID__", str(presupuesto_id))
        .replace("__DATOS__", datos_js)
    )


@presupuestos_bp.route("/<int:presupuesto_id>/reparto/kg", methods=["GET"])
def vista_reparto_kg(presupuesto_id):
    """KG por OT como los calcula Control de Producción + porcentajes sugeridos
    (None si alguna OT no tiene kg cargados)."""
    from produccion_routes import _avance_y_desglose_ot  # import diferido: evita ciclos al registrar blueprints

    db = _db()
    ot_ids = []
    for texto in (request.args.get("ot_ids") or "").split(","):
        texto = texto.strip()
        if texto.isdigit() and int(texto) not in ot_ids:
            ot_ids.append(int(texto))
    ot_ids = ot_ids[:_MAX_OT_KG]

    kg_por_ot = {}
    for ot_id in ot_ids:
        try:
            kg_por_ot[ot_id] = float(_avance_y_desglose_ot(db, ot_id)[3] or 0.0)
        except Exception:
            kg_por_ot[ot_id] = 0.0

    sugerencia = sugerir_porcentajes_por_kg(kg_por_ot)
    return jsonify({
        "kg": {str(k): v for k, v in kg_por_ot.items()},
        "sugerencia": {str(k): v for k, v in sugerencia.items()} if sugerencia else None,
    })


@presupuestos_bp.route("/<int:presupuesto_id>/reparto", methods=["POST"])
def guardar_reparto_ot(presupuesto_id):
    db = _db()
    presupuesto = obtener_presupuesto(db, presupuesto_id)
    if not presupuesto:
        return jsonify({"error": "Presupuesto no encontrado"}), 404
    if presupuesto.get("estado") != "adjudicado":
        return jsonify({"error": "Solo se puede mapear un presupuesto adjudicado."}), 400

    enviado = (request.get_json(silent=True) or {}).get("tareas")
    if not isinstance(enviado, dict):
        return jsonify({"error": "Datos inválidos."}), 400

    tareas = {t["id"]: t for t in _tareas_con_fabricacion(db, presupuesto_id)}
    errores = []
    a_guardar = {}
    for clave, filas in enviado.items():
        tarea = tareas.get(int(clave)) if str(clave).isdigit() else None
        if tarea is None:
            errores.append({"tarea_id": None, "mensaje": f"La tarea {clave} no pertenece a este presupuesto o no tiene Fabricación."})
            continue

        limpias, error = validar_reparto_tarea(filas if isinstance(filas, list) else [])
        if not error:
            for fila in limpias:
                if not db.execute("SELECT 1 FROM ordenes_trabajo WHERE id = ?", (fila["ot_id"],)).fetchone():
                    error = f"La OT {fila['ot_id']} no existe."
                    break
        if error:
            errores.append({"tarea_id": tarea["id"], "mensaje": error})
        else:
            a_guardar[tarea["id"]] = limpias

    if errores:
        return jsonify({"errores": errores}), 400

    try:
        reemplazar_reparto_tareas(db, a_guardar)
    except Exception as e:
        return jsonify({"error": f"No se pudo guardar: {e}"}), 500
    con_ot = sum(1 for filas in a_guardar.values() if filas)
    return jsonify({"ok": True, "mensaje": f"Reparto guardado ({con_ot} tarea(s) con OT, {len(a_guardar) - con_ot} sin OT)."})


_REPARTO_HTML = """
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>__ESTILO__
.reparto-fila { display: grid; grid-template-columns: minmax(220px, 1fr) 120px auto; gap: 8px; align-items: center; margin-bottom: 6px; }
.reparto-fila select, .reparto-fila input { margin-bottom: 0; }
.reparto-pie { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; margin-top: 8px; }
.suma-ok { color: #166534; font-weight: 700; }
.suma-mal { color: #b91c1c; font-weight: 700; }
.error-tarea { color: #b91c1c; font-weight: 600; margin-top: 6px; }
.aviso-ots { background: #fffbeb; border: 1px solid #fde68a; color: #92400e; border-radius: 8px; padding: 10px; margin-bottom: 12px; font-size: 13px; }
.btn[disabled] { opacity: 0.5; cursor: not-allowed; }
.chip-sin-ot { background: #e2e8f0; color: #475569; border-color: #cbd5e1; }
</style>
</head>
<body>
<div class="wrap">
    <div class="top-bar">
        <h2>🧩 Mapeo Tarea → OT — Presupuesto #__PRESUPUESTO_ID__</h2>
        <a href="/modulo/presupuestos/__PRESUPUESTO_ID__/resumen" class="btn btn-secondary">⬅️ Volver al resumen</a>
    </div>
    <div class="card">
        <div class="muted">Elegí a qué OT va la Fabricación de cada tarea. Una tarea puede repartirse entre varias OT
        (los porcentajes deben sumar 100%). Sin filas, la tarea queda como «Sin OT (nivel obra)».</div>
    </div>
    <div id="aviso-ots" class="aviso-ots" style="display:none;"></div>
    <div id="lista-tareas"></div>
    <div id="msg-global" style="font-weight:700;margin:8px 0;"></div>
    <button type="button" class="btn" id="btn-guardar" onclick="guardar()">💾 Guardar reparto</button>
    <button type="button" class="btn" id="btn-volcar" style="background:#0f766e;" onclick="abrirVolcado()">📤 Volcar previsto a OT</button>
    <a href="/modulo/presupuestos/__PRESUPUESTO_ID__/reparto/historial" class="btn btn-secondary">📜 Historial de volcados</a>
    <div id="panel-volcado" class="card" style="display:none;margin-top:14px;"></div>
</div>

<script>
const DATOS = __DATOS__;
const URL_BASE = "/modulo/presupuestos/__PRESUPUESTO_ID__/reparto";
const estado = {};
let dirty = false;

function esc(t) {
    return String(t == null ? "" : t).replace(/[&<>"']/g, c => ({"&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;"}[c]));
}
function money(v) {
    return "$ " + (Number(v) || 0).toLocaleString("es-AR", {minimumFractionDigits: 2, maximumFractionDigits: 2});
}
function num(v) {
    const n = parseFloat(String(v).replace(",", "."));
    return isNaN(n) ? null : n;
}
function suma(id) {
    return estado[id].filas.reduce((a, f) => a + (num(f.porcentaje) || 0), 0);
}

function opcionesOt(seleccionada) {
    let html = '<option value="">-- Elegir OT --</option>';
    DATOS.ots.forEach(o => {
        html += `<option value="${o.id}" ${String(o.id) === String(seleccionada) ? "selected" : ""}>${esc(o.etiqueta)}</option>`;
    });
    return html;
}

// Estado del botón "Sugerir por kg": {habilitado, ayuda}
function estadoSugerir(id) {
    const e = estado[id];
    const ots = e.filas.map(f => f.ot_id);
    if (ots.some(o => !o)) return {habilitado: false, ayuda: "Elegí una OT en cada fila."};
    if (new Set(ots).size !== ots.length) return {habilitado: false, ayuda: "Hay una OT repetida."};
    if (e.cargandoKg) return {habilitado: false, ayuda: "Calculando kg..."};
    if (!e.sugerencia) return {habilitado: false, ayuda: "Alguna de las OT no tiene kg cargados."};
    return {habilitado: true, ayuda: "Reparte proporcional a los kg de cada OT (después podés editarlo)."};
}

function renderTarea(id) {
    const t = DATOS.tareas.find(x => x.id === id);
    const e = estado[id];
    const sinOt = e.filas.length === 0;
    const chip = sinOt
        ? '<span class="chip chip-sin-ot">Sin OT (nivel obra)</span>'
        : `<span class="chip chip-fab">${e.filas.length} OT</span>`;

    const filas = e.filas.map((f, i) => `
        <div class="reparto-fila">
            <select data-tarea="${id}" data-idx="${i}" data-campo="ot_id">${opcionesOt(f.ot_id)}</select>
            <input type="number" step="0.01" min="0" max="100" placeholder="%" value="${esc(f.porcentaje)}"
                   data-tarea="${id}" data-idx="${i}" data-campo="porcentaje">
            <button type="button" class="btn btn-danger btn-sm" data-accion="quitar" data-tarea="${id}" data-idx="${i}">✕</button>
        </div>`).join("");

    let pie = `<button type="button" class="btn btn-secondary btn-sm" data-accion="agregar" data-tarea="${id}">➕ Agregar OT</button>`;
    if (e.filas.length >= 2) {
        const s = estadoSugerir(id);
        pie += `<button type="button" class="btn btn-sm" data-accion="sugerir" data-tarea="${id}" title="${esc(s.ayuda)}" ${s.habilitado ? "" : "disabled"}>⚖️ Sugerir por kg</button>`;
    }
    pie += `<span id="suma-${id}"></span>`;

    document.getElementById("t-" + id).innerHTML = `
        <div style="display:flex;justify-content:space-between;gap:8px;flex-wrap:wrap;margin-bottom:8px;">
            <div><b>${esc(t.nombre)}</b> <span class="muted">· Fabricación: ${money(t.precio_venta)}</span></div>
            <div>${chip}</div>
        </div>
        ${filas}
        <div class="reparto-pie">${pie}</div>
        <div class="error-tarea" id="error-${id}">${esc(e.error || "")}</div>`;
    actualizarSuma(id);
}

function actualizarSuma(id) {
    const el = document.getElementById("suma-" + id);
    if (!el) return;
    if (estado[id].filas.length === 0) { el.innerHTML = ""; return; }
    const s = suma(id);
    const ok = Math.abs(s - 100) < 0.000001;
    el.innerHTML = `<span class="${ok ? "suma-ok" : "suma-mal"}">Suma: ${s.toLocaleString("es-AR", {maximumFractionDigits: 2})}% ${ok ? "✔" : "(debe ser 100%)"}</span>`;
}

function refrescarKg(id) {
    const e = estado[id];
    e.sugerencia = null;
    const ots = e.filas.map(f => f.ot_id);
    if (e.filas.length < 2 || ots.some(o => !o) || new Set(ots).size !== ots.length) { e.cargandoKg = false; return; }
    e.cargandoKg = true;
    const token = (e.token = (e.token || 0) + 1);
    fetch(`${URL_BASE}/kg?ot_ids=${ots.join(",")}`)
        .then(r => r.json())
        .then(d => { if (token === e.token) { e.sugerencia = d.sugerencia; e.cargandoKg = false; renderTarea(id); } })
        .catch(() => { if (token === e.token) { e.sugerencia = null; e.cargandoKg = false; renderTarea(id); } });
}

function inicializar() {
    if (DATOS.aviso_ots) {
        const av = document.getElementById("aviso-ots");
        av.textContent = DATOS.aviso_ots;
        av.style.display = "block";
    }
    const lista = document.getElementById("lista-tareas");
    if (!DATOS.tareas.length) {
        lista.innerHTML = '<div class="card sin-datos">Ninguna tarea de este presupuesto tiene Fabricación con costo.</div>';
        document.getElementById("btn-guardar").style.display = "none";
        return;
    }
    DATOS.tareas.forEach(t => {
        estado[t.id] = {filas: t.reparto.map(r => ({ot_id: String(r.ot_id), porcentaje: String(r.porcentaje)})), sugerencia: null, cargandoKg: false, error: ""};
        const div = document.createElement("div");
        div.className = "card";
        div.id = "t-" + t.id;
        lista.appendChild(div);
        renderTarea(t.id);
        refrescarKg(t.id);
    });
}

document.addEventListener("click", ev => {
    const b = ev.target.closest("button[data-accion]");
    if (!b) return;
    const id = Number(b.dataset.tarea);
    const e = estado[id];
    const accion = b.dataset.accion;
    e.error = "";
    dirty = true;
    if (accion === "agregar") {
        e.filas.push({ot_id: "", porcentaje: e.filas.length === 0 ? "100" : ""});
        refrescarKg(id);
        renderTarea(id);
    } else if (accion === "quitar") {
        e.filas.splice(Number(b.dataset.idx), 1);
        refrescarKg(id);
        renderTarea(id);
    } else if (accion === "sugerir" && e.sugerencia) {
        e.filas.forEach(f => { f.porcentaje = String(e.sugerencia[f.ot_id]); });
        renderTarea(id);
    }
});

document.addEventListener("input", ev => {
    const c = ev.target;
    if (c.dataset.campo !== "porcentaje") return;
    const id = Number(c.dataset.tarea);
    estado[id].filas[Number(c.dataset.idx)].porcentaje = c.value;
    estado[id].error = "";
    dirty = true;
    document.getElementById("error-" + id).textContent = "";
    actualizarSuma(id);
});

document.addEventListener("change", ev => {
    const c = ev.target;
    if (c.dataset.campo !== "ot_id") return;
    const id = Number(c.dataset.tarea);
    estado[id].filas[Number(c.dataset.idx)].ot_id = c.value;
    estado[id].error = "";
    dirty = true;
    refrescarKg(id);
    renderTarea(id);
});

function guardar() {
    const msg = document.getElementById("msg-global");
    msg.style.color = "#b91c1c";
    let hayError = false;
    const payload = {};
    DATOS.tareas.forEach(t => {
        const e = estado[t.id];
        e.error = "";
        if (e.filas.length && Math.abs(suma(t.id) - 100) >= 0.000001) {
            e.error = `Los porcentajes suman ${suma(t.id).toLocaleString("es-AR", {maximumFractionDigits: 2})}% y deben sumar 100%.`;
            hayError = true;
        }
        payload[t.id] = e.filas.map(f => ({ot_id: f.ot_id, porcentaje: f.porcentaje}));
        renderTarea(t.id);
    });
    if (hayError) { msg.textContent = "Corregí los porcentajes marcados antes de guardar."; return; }

    msg.style.color = "#64748b";
    msg.textContent = "Guardando...";
    fetch(URL_BASE, {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify({tareas: payload})})
        .then(r => r.json().then(d => ({ok: r.ok, d})))
        .then(({ok, d}) => {
            if (ok) { dirty = false; msg.style.color = "#166534"; msg.textContent = "✔ " + d.mensaje; return; }
            msg.style.color = "#b91c1c";
            msg.textContent = d.error || "Corregí los errores marcados antes de guardar.";
            (d.errores || []).forEach(x => {
                if (x.tarea_id && estado[x.tarea_id]) { estado[x.tarea_id].error = x.mensaje; renderTarea(x.tarea_id); }
                else msg.textContent += " " + x.mensaje;
            });
        })
        .catch(() => { msg.style.color = "#b91c1c"; msg.textContent = "No se pudo guardar (error de conexión)."; });
}

// ---- Volcado del previsto a las OT ----
let huellaVolcado = null;

function cerrarVolcado() {
    document.getElementById("panel-volcado").style.display = "none";
}

function abrirVolcado() {
    const msg = document.getElementById("msg-global");
    if (dirty) {
        msg.style.color = "#b91c1c";
        msg.textContent = "Hay cambios sin guardar: guardá el reparto antes de volcar.";
        return;
    }
    msg.textContent = "";
    const panel = document.getElementById("panel-volcado");
    panel.style.display = "block";
    panel.innerHTML = '<div class="muted">Calculando vista previa...</div>';
    panel.scrollIntoView({behavior: "smooth"});
    fetch(URL_BASE + "/volcado/preview")
        .then(r => r.json().then(d => ({ok: r.ok, d})))
        .then(({ok, d}) => {
            if (!ok) { panel.innerHTML = `<div class="error-tarea">${esc(d.error)}</div><button type="button" class="btn btn-secondary" onclick="cerrarVolcado()">Cerrar</button>`; return; }
            renderVolcado(d);
        })
        .catch(() => { panel.innerHTML = '<div class="error-tarea">No se pudo calcular la vista previa.</div>'; });
}

function renderVolcado(d) {
    huellaVolcado = d.huella;
    const c = d.cuadre;
    let h = '<h3 style="margin-top:0;">Vista previa del volcado (todavía no se escribió nada)</h3>';

    h += `<div class="resultado-box" style="${c.cuadra ? "" : "background:#fef2f2;border-color:#fecaca;"}">
        <div class="fila"><span>Asignado a OT</span><span>${money(c.total_asignado)}</span></div>
        <div class="fila"><span>Tareas sin OT</span><span>${money(c.total_sin_ot)}</span></div>
        <div class="fila"><span>Precio de venta Fabricación del presupuesto</span><span>${money(c.precio_venta_fabricacion)}</span></div>
        <div class="fila"><span><b>Control de cuadre</b> (diferencia ${money(c.diferencia)}, tolerancia de redondeo ${money(c.tolerancia)})</span>
            <span class="${c.cuadra ? "suma-ok" : "suma-mal"}">${c.cuadra ? "✔ Cuadra" : "✖ No cuadra"}</span></div>
    </div>`;
    h += `<div class="muted" style="margin-top:6px;">ℹ Montaje (${money(d.montaje_total)}) no se vuelca.</div>`;
    if (d.bloqueo) h += `<div class="error-tarea">${esc(d.bloqueo)}</div>`;
    if (d.primer_volcado) h += '<div class="muted" style="margin-top:6px;">Es el primer volcado de este presupuesto: no hay un volcado anterior para comparar.</div>';

    if (d.sin_ot.length) {
        h += '<div class="aviso-ots" style="margin-top:10px;"><b>Tareas sin OT</b> (este previsto queda a nivel obra, no se vuelca a ninguna OT):<ul style="margin:6px 0 0 18px;">'
            + d.sin_ot.map(t => `<li>${esc(t.nombre)} — ${money(t.total)}</li>`).join("") + "</ul></div>";
    }
    if (d.ediciones_manuales.length) {
        h += '<div class="aviso-ots" style="margin-top:10px;"><b>Posibles ediciones manuales:</b> estos valores de la OT no coinciden con lo que dejó el último volcado. Revisalos antes de pisarlos.<ul style="margin:6px 0 0 18px;">'
            + d.ediciones_manuales.map(o => `<li>${esc(o.etiqueta)}: ` + o.diferencias.map(x =>
                `${esc(x.rubro)} hoy ${money(x.actual)} (último volcado ${money(x.ultimo_volcado)})`).join("; ") + "</li>").join("") + "</ul></div>";
    }
    if (d.ots_sin_tareas_ahora.length) {
        h += '<div class="aviso-ots" style="margin-top:10px;"><b>OT que recibieron previsto en el volcado anterior y ya no tienen tareas asignadas</b> (no se modifican):<ul style="margin:6px 0 0 18px;">'
            + d.ots_sin_tareas_ahora.map(o => `<li>${esc(o.etiqueta)}</li>`).join("") + "</ul></div>";
    }

    d.ots.forEach(o => {
        const filas = o.filas.map(f => `<tr style="${f.cambia ? "background:#fef9c3;" : ""}">
            <td>${esc(f.rubro)}</td><td style="text-align:right;">${money(f.actual)}</td>
            <td style="text-align:right;${f.cambia ? "font-weight:800;" : ""}">${money(f.nuevo)}</td></tr>`).join("");
        h += `<h4 style="margin:14px 0 4px 0;">${esc(o.etiqueta)}</h4>
            <table><tr><th>Rubro</th><th style="text-align:right;">Valor actual en la OT</th><th style="text-align:right;">Valor nuevo</th></tr>
            ${filas}
            <tr style="font-weight:800;background:#eef2ff;"><td>Total</td><td style="text-align:right;">${money(o.total_actual)}</td><td style="text-align:right;">${money(o.total_nuevo)}</td></tr></table>`;
    });

    h += '<label style="margin-top:12px;">Nota (opcional)</label><input type="text" id="nota-volcado" maxlength="300" placeholder="Ej: reparto definitivo tras revisión">';
    if (d.ediciones_manuales.length) {
        h += '<label style="display:flex;gap:8px;align-items:center;font-weight:600;"><input type="checkbox" id="chk-ediciones" style="width:auto;margin:0;" onchange="actualizarBotonConfirmar()"> Revisé las posibles ediciones manuales y acepto pisarlas.</label>';
    }
    h += `<div id="msg-volcado" style="font-weight:700;margin:8px 0;"></div>
        <button type="button" class="btn" id="btn-confirmar-volcado" style="background:#0f766e;" onclick="confirmarVolcado()" disabled>✅ Confirmar volcado</button>
        <button type="button" class="btn btn-secondary" onclick="cerrarVolcado()">Cancelar</button>`;

    const panel = document.getElementById("panel-volcado");
    panel.innerHTML = h;
    panel.dataset.puedeConfirmar = d.puede_confirmar ? "1" : "0";
    actualizarBotonConfirmar();
}

function actualizarBotonConfirmar() {
    const panel = document.getElementById("panel-volcado");
    const chk = document.getElementById("chk-ediciones");
    const btn = document.getElementById("btn-confirmar-volcado");
    if (btn) btn.disabled = !(panel.dataset.puedeConfirmar === "1" && (!chk || chk.checked));
}

function confirmarVolcado() {
    const msg = document.getElementById("msg-volcado");
    const btn = document.getElementById("btn-confirmar-volcado");
    btn.disabled = true;
    msg.style.color = "#64748b";
    msg.textContent = "Volcando...";
    fetch(URL_BASE + "/volcado", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({huella: huellaVolcado, nota: document.getElementById("nota-volcado").value}),
    })
        .then(r => r.json().then(d => ({ok: r.ok, d})))
        .then(({ok, d}) => {
            if (ok) {
                msg.style.color = "#166534";
                msg.innerHTML = "✔ " + esc(d.mensaje) + ' <a href="' + URL_BASE + '/historial">Ver historial</a>';
                return;
            }
            msg.style.color = "#b91c1c";
            msg.textContent = d.error || "No se pudo volcar.";
        })
        .catch(() => { msg.style.color = "#b91c1c"; msg.textContent = "No se pudo volcar (error de conexión)."; btn.disabled = false; });
}

inicializar();
</script>
</body>
</html>
"""
