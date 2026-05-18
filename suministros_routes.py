from flask import Blueprint, request, redirect
from db_utils import get_db
import html


suministros_bp = Blueprint("suministros", __name__, url_prefix="/modulo/suministros")

ESTADOS_SOLICITUD = ["Pendiente", "En compra", "Parcial", "Recibida", "Cancelada"]
PRIORIDADES = ["Baja", "Media", "Alta", "Urgente"]


def _ensure_suministros_tables(db):
        db.execute(
                """
                CREATE TABLE IF NOT EXISTS solicitudes_compra (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        fecha_solicitud DATETIME DEFAULT CURRENT_TIMESTAMP,
                        solicitante TEXT NOT NULL,
                        obra TEXT,
                        sector TEXT,
                        prioridad TEXT DEFAULT 'Media',
                        estado TEXT DEFAULT 'Pendiente',
                        observaciones TEXT
                )
                """
        )
        db.execute(
                """
                CREATE TABLE IF NOT EXISTS items_solicitud (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        solicitud_id INTEGER NOT NULL,
                        descripcion TEXT NOT NULL,
                        cantidad REAL NOT NULL,
                        unidad TEXT,
                        proveedor_sugerido TEXT,
                        fecha_necesaria DATE,
                        estado_item TEXT DEFAULT 'Pendiente',
                        observaciones TEXT,
                        FOREIGN KEY (solicitud_id) REFERENCES solicitudes_compra(id)
                )
                """
        )
        try:
                db.execute("CREATE INDEX IF NOT EXISTS idx_items_solicitud_id ON items_solicitud(solicitud_id)")
        except Exception:
                pass
        db.commit()


def _fmt_num(value):
        try:
                num = float(value)
        except Exception:
                return "0"
        if num.is_integer():
                return str(int(num))
        return f"{num:.2f}".rstrip("0").rstrip(".")


@suministros_bp.route("/")
def dashboard_suministros():
        db = get_db()
        _ensure_suministros_tables(db)

        total = db.execute("SELECT COUNT(1) FROM solicitudes_compra").fetchone()
        pendientes = db.execute(
                "SELECT COUNT(1) FROM solicitudes_compra WHERE COALESCE(estado, 'Pendiente') IN ('Pendiente', 'En compra', 'Parcial')"
        ).fetchone()
        recibidas = db.execute("SELECT COUNT(1) FROM solicitudes_compra WHERE estado = 'Recibida'").fetchone()

        return f"""
        <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: Arial, sans-serif; background:#f8fafc; margin:0; padding:16px; color:#0f172a; }}
                .wrap {{ max-width:1000px; margin:0 auto; }}
                .top a {{ display:inline-block; margin-right:8px; margin-bottom:8px; text-decoration:none; padding:9px 12px; border-radius:8px; }}
                .btn {{ background:#0f766e; color:#fff; }}
                .btn2 {{ background:#2563eb; color:#fff; }}
                .btn3 {{ background:#334155; color:#fff; }}
                .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(180px,1fr)); gap:10px; margin:12px 0 16px; }}
                .card {{ background:#fff; border:1px solid #e2e8f0; border-radius:10px; padding:12px; }}
                .n {{ font-size:28px; font-weight:800; margin-top:4px; }}
                .box {{ background:#fff; border:1px solid #e2e8f0; border-radius:10px; padding:12px; }}
            </style>
        </head>
        <body>
            <div class="wrap">
                <h2>Suministros / Compras</h2>
                <div class="top">
                    <a class="btn" href="/modulo/suministros/solicitudes/nueva">+ Nueva solicitud</a>
                    <a class="btn2" href="/modulo/suministros/solicitudes">Ver solicitudes</a>
                    <a class="btn3" href="/">Volver al panel</a>
                </div>
                <div class="cards">
                    <div class="card"><div>Total solicitudes</div><div class="n">{int(total[0] or 0)}</div></div>
                    <div class="card"><div>Abiertas</div><div class="n">{int(pendientes[0] or 0)}</div></div>
                    <div class="card"><div>Recibidas</div><div class="n">{int(recibidas[0] or 0)}</div></div>
                </div>
                <div class="box">
                    <b>Estado del módulo:</b> operativo (v1).<br>
                    Ya podés crear solicitudes con ítems, listarlas y cambiar su estado.
                </div>
            </div>
        </body>
        </html>
        """


@suministros_bp.route("/solicitudes")
def solicitudes():
        db = get_db()
        _ensure_suministros_tables(db)

        rows = db.execute(
                """
                SELECT s.id,
                             COALESCE(s.fecha_solicitud, ''),
                             COALESCE(s.solicitante, ''),
                             COALESCE(s.obra, ''),
                             COALESCE(s.prioridad, 'Media'),
                             COALESCE(s.estado, 'Pendiente'),
                             COUNT(i.id) AS items
                FROM solicitudes_compra s
                LEFT JOIN items_solicitud i ON i.solicitud_id = s.id
                GROUP BY s.id, s.fecha_solicitud, s.solicitante, s.obra, s.prioridad, s.estado
                ORDER BY s.id DESC
                """
        ).fetchall()

        filas = ""
        for sid, fecha, solicitante, obra, prioridad, estado, items in rows:
                filas += f"""
                <tr>
                    <td>{int(sid)}</td>
                    <td>{html.escape(str(fecha or ''))}</td>
                    <td>{html.escape(str(solicitante or ''))}</td>
                    <td>{html.escape(str(obra or '-'))}</td>
                    <td>{html.escape(str(prioridad or 'Media'))}</td>
                    <td>{html.escape(str(estado or 'Pendiente'))}</td>
                    <td>{int(items or 0)}</td>
                    <td><a href="/modulo/suministros/solicitudes/{int(sid)}">Ver</a></td>
                </tr>
                """

        return f"""
        <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: Arial, sans-serif; background:#f8fafc; margin:0; padding:16px; color:#0f172a; }}
                .wrap {{ max-width:1100px; margin:0 auto; }}
                a.btn {{ display:inline-block; margin-right:8px; margin-bottom:10px; text-decoration:none; padding:9px 12px; border-radius:8px; color:#fff; background:#2563eb; }}
                table {{ width:100%; border-collapse:collapse; background:#fff; border:1px solid #e2e8f0; }}
                th, td {{ padding:10px; border-bottom:1px solid #e2e8f0; text-align:left; font-size:14px; }}
                th {{ background:#f1f5f9; }}
            </style>
        </head>
        <body>
            <div class="wrap">
                <h2>Solicitudes de compra</h2>
                <a class="btn" href="/modulo/suministros/solicitudes/nueva">+ Nueva solicitud</a>
                <a class="btn" style="background:#0f766e" href="/modulo/suministros">Dashboard</a>
                <a class="btn" style="background:#334155" href="/">Panel principal</a>
                <table>
                    <tr>
                        <th>ID</th><th>Fecha</th><th>Solicitante</th><th>Obra</th><th>Prioridad</th><th>Estado</th><th>Ítems</th><th>Detalle</th>
                    </tr>
                    {filas if filas else '<tr><td colspan="8">No hay solicitudes cargadas.</td></tr>'}
                </table>
            </div>
        </body>
        </html>
        """


@suministros_bp.route("/solicitudes/nueva", methods=["GET", "POST"])
def nueva_solicitud():
        db = get_db()
        _ensure_suministros_tables(db)
        error = ""

        if request.method == "POST":
                solicitante = (request.form.get("solicitante") or "").strip()
                obra = (request.form.get("obra") or "").strip()
                sector = (request.form.get("sector") or "").strip()
                prioridad = (request.form.get("prioridad") or "Media").strip()
                observaciones = (request.form.get("observaciones") or "").strip()

                descs = request.form.getlist("item_desc[]")
                cants = request.form.getlist("item_cant[]")
                units = request.form.getlist("item_unidad[]")
                provs = request.form.getlist("item_proveedor[]")
                fechas = request.form.getlist("item_fecha[]")
                obs_items = request.form.getlist("item_obs[]")

                items_ok = []
                for idx, d in enumerate(descs):
                        desc = (d or "").strip()
                        cant_raw = (cants[idx] if idx < len(cants) else "").strip()
                        if not desc:
                                continue
                        try:
                                cant = float(cant_raw.replace(",", ".")) if cant_raw else 0
                        except Exception:
                                cant = 0
                        if cant <= 0:
                                continue
                        items_ok.append(
                                (
                                        desc,
                                        cant,
                                        (units[idx] if idx < len(units) else "").strip(),
                                        (provs[idx] if idx < len(provs) else "").strip(),
                                        (fechas[idx] if idx < len(fechas) else "").strip(),
                                        (obs_items[idx] if idx < len(obs_items) else "").strip(),
                                )
                        )

                if not solicitante:
                        error = "El solicitante es obligatorio"
                elif prioridad not in PRIORIDADES:
                        error = "Prioridad inválida"
                elif not items_ok:
                        error = "Cargá al menos un ítem con descripción y cantidad mayor a 0"
                else:
                        cur = db.execute(
                                """
                                INSERT INTO solicitudes_compra (solicitante, obra, sector, prioridad, estado, observaciones)
                                VALUES (?, ?, ?, ?, 'Pendiente', ?)
                                """,
                                (solicitante, obra, sector, prioridad, observaciones),
                        )
                        solicitud_id = int(getattr(cur, "lastrowid", 0) or 0)
                        if solicitud_id <= 0:
                                row = db.execute("SELECT MAX(id) FROM solicitudes_compra").fetchone()
                                solicitud_id = int(row[0] or 0)

                        for desc, cant, unidad, proveedor, fecha_necesaria, obs_item in items_ok:
                                db.execute(
                                        """
                                        INSERT INTO items_solicitud
                                        (solicitud_id, descripcion, cantidad, unidad, proveedor_sugerido, fecha_necesaria, observaciones)
                                        VALUES (?, ?, ?, ?, ?, ?, ?)
                                        """,
                                        (solicitud_id, desc, cant, unidad, proveedor, fecha_necesaria, obs_item),
                                )

                        db.commit()
                        return redirect(f"/modulo/suministros/solicitudes/{solicitud_id}")

        opts_prio = "".join(f"<option value='{p}'>{p}</option>" for p in PRIORIDADES)
        return f"""
        <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: Arial, sans-serif; background:#f8fafc; margin:0; padding:16px; color:#0f172a; }}
                .wrap {{ max-width:1100px; margin:0 auto; }}
                .card {{ background:#fff; border:1px solid #e2e8f0; border-radius:10px; padding:12px; margin-bottom:12px; }}
                .grid {{ display:grid; grid-template-columns:repeat(4, minmax(140px, 1fr)); gap:8px; }}
                input, select, textarea, button {{ width:100%; box-sizing:border-box; padding:8px; border:1px solid #cbd5e1; border-radius:7px; }}
                table {{ width:100%; border-collapse:collapse; }}
                th, td {{ border-bottom:1px solid #e2e8f0; padding:6px; text-align:left; }}
                .err {{ background:#fee2e2; color:#991b1b; border:1px solid #fecaca; border-radius:8px; padding:8px; margin-bottom:8px; }}
                .top a {{ display:inline-block; margin-right:8px; text-decoration:none; padding:9px 12px; border-radius:8px; color:#fff; background:#334155; }}
            </style>
            <script>
                function agregarFila() {{
                    const tbody = document.getElementById('items-body');
                    const tr = document.createElement('tr');
                    tr.innerHTML = `
                        <td><input name="item_desc[]" required></td>
                        <td><input name="item_cant[]" type="number" step="0.01" min="0.01" required></td>
                        <td><input name="item_unidad[]" placeholder="u, kg, m"></td>
                        <td><input name="item_proveedor[]"></td>
                        <td><input name="item_fecha[]" type="date"></td>
                        <td><input name="item_obs[]"></td>
                        <td><button type="button" onclick="this.closest('tr').remove()">Quitar</button></td>
                    `;
                    tbody.appendChild(tr);
                }}
            </script>
        </head>
        <body>
            <div class="wrap">
                <h2>Nueva solicitud de compra</h2>
                <div class="top">
                    <a href="/modulo/suministros/solicitudes">Volver a solicitudes</a>
                    <a href="/modulo/suministros">Dashboard</a>
                </div>
                {f'<div class="err">{html.escape(error)}</div>' if error else ''}
                <form method="post">
                    <div class="card">
                        <div class="grid">
                            <div><label>Solicitante</label><input name="solicitante" required></div>
                            <div><label>Obra</label><input name="obra" placeholder="Ej: GGO-001"></div>
                            <div><label>Sector</label><input name="sector" placeholder="Ej: Taller"></div>
                            <div><label>Prioridad</label><select name="prioridad">{opts_prio}</select></div>
                        </div>
                        <div style="margin-top:8px;"><label>Observaciones</label><textarea name="observaciones" rows="2"></textarea></div>
                    </div>
                    <div class="card">
                        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
                            <b>Ítems</b>
                            <button type="button" onclick="agregarFila()" style="width:auto;">+ Agregar ítem</button>
                        </div>
                        <table>
                            <thead>
                                <tr><th>Descripción</th><th>Cantidad</th><th>Unidad</th><th>Proveedor sugerido</th><th>Fecha necesaria</th><th>Obs.</th><th>Acción</th></tr>
                            </thead>
                            <tbody id="items-body">
                                <tr>
                                    <td><input name="item_desc[]" required></td>
                                    <td><input name="item_cant[]" type="number" step="0.01" min="0.01" required></td>
                                    <td><input name="item_unidad[]" placeholder="u, kg, m"></td>
                                    <td><input name="item_proveedor[]"></td>
                                    <td><input name="item_fecha[]" type="date"></td>
                                    <td><input name="item_obs[]"></td>
                                    <td><button type="button" onclick="this.closest('tr').remove()">Quitar</button></td>
                                </tr>
                            </tbody>
                        </table>
                    </div>
                    <button type="submit" style="background:#0f766e; color:#fff; font-weight:700;">Guardar solicitud</button>
                </form>
            </div>
        </body>
        </html>
        """


@suministros_bp.route("/solicitudes/<int:solicitud_id>", methods=["GET", "POST"])
def ver_solicitud(solicitud_id):
        db = get_db()
        _ensure_suministros_tables(db)

        if request.method == "POST":
                nuevo_estado = (request.form.get("estado") or "").strip()
                if nuevo_estado in ESTADOS_SOLICITUD:
                        db.execute("UPDATE solicitudes_compra SET estado = ? WHERE id = ?", (nuevo_estado, solicitud_id))
                        db.commit()
                return redirect(f"/modulo/suministros/solicitudes/{solicitud_id}")

        row = db.execute(
                """
                SELECT id, COALESCE(fecha_solicitud, ''), COALESCE(solicitante, ''), COALESCE(obra, ''),
                             COALESCE(sector, ''), COALESCE(prioridad, 'Media'), COALESCE(estado, 'Pendiente'), COALESCE(observaciones, '')
                FROM solicitudes_compra
                WHERE id = ?
                """,
                (solicitud_id,),
        ).fetchone()

        if not row:
                return "<h3>Solicitud no encontrada</h3><a href='/modulo/suministros/solicitudes'>Volver</a>", 404

        items = db.execute(
                """
                SELECT id, COALESCE(descripcion, ''), COALESCE(cantidad, 0), COALESCE(unidad, ''),
                             COALESCE(proveedor_sugerido, ''), COALESCE(fecha_necesaria, ''), COALESCE(estado_item, 'Pendiente'), COALESCE(observaciones, '')
                FROM items_solicitud
                WHERE solicitud_id = ?
                ORDER BY id ASC
                """,
                (solicitud_id,),
        ).fetchall()

        estado_options = "".join(
                f"<option value='{e}' {'selected' if e == row[6] else ''}>{e}</option>" for e in ESTADOS_SOLICITUD
        )

        filas_items = ""
        for iid, desc, cant, unidad, prov, fecha_n, estado_i, obs_i in items:
                filas_items += f"""
                <tr>
                    <td>{int(iid)}</td>
                    <td>{html.escape(str(desc or ''))}</td>
                    <td>{html.escape(_fmt_num(cant))}</td>
                    <td>{html.escape(str(unidad or '-'))}</td>
                    <td>{html.escape(str(prov or '-'))}</td>
                    <td>{html.escape(str(fecha_n or '-'))}</td>
                    <td>{html.escape(str(estado_i or 'Pendiente'))}</td>
                    <td>{html.escape(str(obs_i or '-'))}</td>
                </tr>
                """

        return f"""
        <html>
        <head>
            <meta name="viewport" content="width=device-width, initial-scale=1">
            <style>
                body {{ font-family: Arial, sans-serif; background:#f8fafc; margin:0; padding:16px; color:#0f172a; }}
                .wrap {{ max-width:1100px; margin:0 auto; }}
                .card {{ background:#fff; border:1px solid #e2e8f0; border-radius:10px; padding:12px; margin-bottom:12px; }}
                .top a {{ display:inline-block; margin-right:8px; margin-bottom:8px; text-decoration:none; padding:9px 12px; border-radius:8px; color:#fff; background:#334155; }}
                table {{ width:100%; border-collapse:collapse; }}
                th, td {{ padding:8px; border-bottom:1px solid #e2e8f0; text-align:left; }}
                th {{ background:#f1f5f9; }}
                .meta {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(190px,1fr)); gap:8px; }}
                .pill {{ display:inline-block; background:#e2e8f0; border-radius:999px; padding:4px 10px; font-size:12px; }}
            </style>
        </head>
        <body>
            <div class="wrap">
                <h2>Solicitud #{int(row[0])}</h2>
                <div class="top">
                    <a href="/modulo/suministros/solicitudes">Volver a solicitudes</a>
                    <a href="/modulo/suministros">Dashboard</a>
                </div>

                <div class="card">
                    <div class="meta">
                        <div><b>Fecha:</b><br>{html.escape(str(row[1] or '-'))}</div>
                        <div><b>Solicitante:</b><br>{html.escape(str(row[2] or '-'))}</div>
                        <div><b>Obra:</b><br>{html.escape(str(row[3] or '-'))}</div>
                        <div><b>Sector:</b><br>{html.escape(str(row[4] or '-'))}</div>
                        <div><b>Prioridad:</b><br><span class="pill">{html.escape(str(row[5] or 'Media'))}</span></div>
                        <div><b>Estado actual:</b><br><span class="pill">{html.escape(str(row[6] or 'Pendiente'))}</span></div>
                    </div>
                    <div style="margin-top:8px;"><b>Observaciones:</b><br>{html.escape(str(row[7] or '-'))}</div>
                    <form method="post" style="margin-top:10px; display:flex; gap:8px; align-items:center;">
                        <label for="estado"><b>Cambiar estado:</b></label>
                        <select name="estado" id="estado" style="padding:7px; border:1px solid #cbd5e1; border-radius:7px;">{estado_options}</select>
                        <button type="submit" style="padding:8px 12px; border:0; border-radius:8px; background:#0f766e; color:#fff;">Guardar</button>
                    </form>
                </div>

                <div class="card">
                    <h3>Ítems</h3>
                    <table>
                        <tr><th>ID</th><th>Descripción</th><th>Cantidad</th><th>Unidad</th><th>Proveedor sugerido</th><th>Fecha necesaria</th><th>Estado item</th><th>Observaciones</th></tr>
                        {filas_items if filas_items else '<tr><td colspan="8">Sin ítems.</td></tr>'}
                    </table>
                </div>
            </div>
        </body>
        </html>
        """
