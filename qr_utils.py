import os


def load_clean_excel(path):
    """Carga Excel y detecta automaticamente el encabezado."""
    try:
        import pandas as pd
        raw = pd.read_excel(path, header=None)
        for i in range(10):
            row = raw.iloc[i].fillna("").astype(str).str.upper()
            if any("POS" in str(x) for x in row):
                df = pd.read_excel(path, header=i)
                df.columns = [str(c).strip().upper() for c in df.columns]
                print(f"[DEBUG] Encabezado detectado en fila {i}")
                print(f"[DEBUG] Columnas: {list(df.columns)}")
                print(f"[DEBUG] Filas totales: {len(df)}")
                return df
        print("[DEBUG] No se detecto encabezado con 'POS' en primeras 10 filas, usando header=0")
        return pd.read_excel(path)
    except Exception as e:
        print(f"[ERROR] Error loading Excel: {e}")
        return None


def find_col(df, keyword):
    """Busca una columna por palabra clave."""
    keyword_upper = keyword.upper()
    for c in df.columns:
        col_upper = str(c).strip().upper()
        if keyword_upper in col_upper:
            print(f"  [FOUND] '{keyword}' -> '{c}'")
            return c
    print(f"  [NOT FOUND] '{keyword}' en columnas: {list(df.columns)}")
    return None


def clean_xls(v):
    """Convierte un valor de celda Excel a string limpio; devuelve '' si es nan/None."""
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none", "nat", "null") else s


def obtener_firma_ok_path(app_dir, firma_ok_candidatos):
    for nombre in firma_ok_candidatos:
        path = os.path.join(app_dir, nombre)
        if os.path.exists(path):
            return path
    return None


def upsert_piezas_desde_excel(
    db,
    df,
    col_pos,
    obra_col,
    cant_col,
    perfil_col,
    peso_col,
    desc_col,
    asegurar_databook_si_valida=None,
):
    """Modo anterior opcional: precarga piezas en BD desde Excel."""
    saved_count = 0
    obras_detectadas = set()
    for idx, row in df.iterrows():
        pos = clean_xls(row.get(col_pos, ""))
        obra = clean_xls(row.get(obra_col, ""))
        perfil = clean_xls(row.get(perfil_col, ""))
        descripcion = clean_xls(row.get(desc_col, ""))
        cant_raw = row.get(cant_col, None)
        peso_raw = row.get(peso_col, None)

        try:
            cantidad = float(cant_raw) if cant_raw not in (None, "") and clean_xls(cant_raw) else None
        except Exception:
            cantidad = None
        try:
            peso = float(peso_raw) if peso_raw not in (None, "") and clean_xls(peso_raw) else None
        except Exception:
            peso = None

        print(f"[DEBUG] Fila {idx}: pos={pos}, obra={obra}, cant={cantidad}, perfil={perfil}")

        if pos and obra:
            obras_detectadas.add(obra)
            try:
                existing = db.execute(
                    "SELECT id FROM procesos WHERE posicion=? AND obra=?",
                    (pos, obra),
                ).fetchone()
                if not existing:
                    db.execute(
                        """
                        INSERT INTO procesos (posicion, obra, cantidad, perfil, peso, descripcion)
                        VALUES (?, ?, ?, ?, ?, ?)
                        """,
                        (pos, obra, cantidad, perfil or None, peso, descripcion or None),
                    )
                else:
                    db.execute(
                        """
                        UPDATE procesos
                        SET obra=?, cantidad=?, perfil=?, peso=?, descripcion=?
                        WHERE posicion=? AND obra=?
                        """,
                        (obra, cantidad, perfil or None, peso, descripcion or None, pos, obra),
                    )
                saved_count += 1
            except Exception as e:
                print(f"  ERROR en {pos}: {str(e)}")
                pass

    db.commit()

    if asegurar_databook_si_valida:
        for obra in sorted(obras_detectadas):
            asegurar_databook_si_valida(obra)

    return saved_count
