import os
import sqlite3
from typing import Iterable

import pymysql


SQLITE_PATH = os.getenv("SQLITE_PATH", "database.db")
MYSQL_HOST = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "appuser")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "App1234!")
MYSQL_DB = os.getenv("MYSQL_DB", "gestion_produccion")
CHUNK_SIZE = int(os.getenv("MIGRATION_CHUNK_SIZE", "500"))


def mysql_type(sqlite_decl: str) -> str:
    decl = (sqlite_decl or "").upper()
    if any(token in decl for token in ("DATE", "TIME")):
        return "DATETIME"
    if "INT" in decl:
        return "BIGINT"
    if "VARCHAR" in decl or decl.startswith("CHAR("):
        return decl
    if any(token in decl for token in ("CLOB", "TEXT", "CHAR")):
        return "LONGTEXT"
    if "BLOB" in decl:
        return "LONGBLOB"
    if any(token in decl for token in ("REAL", "FLOA", "DOUB")):
        return "DOUBLE"
    return "DECIMAL(20,6)"


def mysql_default_literal(default_value: object) -> str:
    raw = str(default_value).strip()

    # SQLite may expose SQL functions/keywords as text defaults.
    if raw.upper() in {"CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "NULL"}:
        return raw.upper()

    # SQLite wraps string defaults with single quotes in PRAGMA output.
    if raw.startswith("'") and raw.endswith("'"):
        inner = raw[1:-1].replace("'", "''")
        return f"'{inner}'"

    # Keep numeric values unquoted when possible.
    try:
        float(raw)
        return raw
    except ValueError:
        escaped = raw.replace("'", "''")
        return f"'{escaped}'"


def get_tables(sqlite_conn: sqlite3.Connection) -> list[str]:
    rows = sqlite_conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
    ).fetchall()
    return [row[0] for row in rows]


def build_create_table_sql(sqlite_conn: sqlite3.Connection, table: str) -> str:
    columns = sqlite_conn.execute(f"PRAGMA table_info({table})").fetchall()
    col_defs = []

    for _, name, col_type, notnull, default_value, pk in columns:
        if pk and (col_type or "").upper().find("INT") != -1:
            col_def = f"`{name}` BIGINT AUTO_INCREMENT"
        else:
            col_def = f"`{name}` {mysql_type(col_type)}"

        if notnull:
            col_def += " NOT NULL"

        mysql_col_type = col_def.split("`", 2)[-1].strip().split(" ", 1)[0].upper()
        if default_value is not None and mysql_col_type not in {"TEXT", "LONGTEXT", "MEDIUMTEXT", "TINYTEXT", "BLOB", "LONGBLOB", "MEDIUMBLOB", "TINYBLOB", "JSON", "GEOMETRY"}:
            col_def += f" DEFAULT {mysql_default_literal(default_value)}"

        if pk:
            col_def += " PRIMARY KEY"

        col_defs.append(col_def)

    cols_sql = ",\n  ".join(col_defs)
    return f"CREATE TABLE IF NOT EXISTS `{table}` (\n  {cols_sql}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"


def chunked(rows: list[tuple], size: int) -> Iterable[list[tuple]]:
    for i in range(0, len(rows), size):
        yield rows[i : i + size]


def migrate_table(sqlite_conn: sqlite3.Connection, mysql_conn: pymysql.Connection, table: str) -> tuple[int, int]:
    columns_info = sqlite_conn.execute(f"PRAGMA table_info({table})").fetchall()
    columns = [row[1] for row in columns_info]

    create_sql = build_create_table_sql(sqlite_conn, table)
    with mysql_conn.cursor() as cur:
        cur.execute(f"DROP TABLE IF EXISTS `{table}`")
        cur.execute(create_sql)

        quoted_cols = ", ".join(f"`{c}`" for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        insert_sql = f"INSERT INTO `{table}` ({quoted_cols}) VALUES ({placeholders})"

        rows = sqlite_conn.execute(f"SELECT {', '.join(columns)} FROM {table}").fetchall()
        inserted = 0
        for batch in chunked(rows, CHUNK_SIZE):
            cur.executemany(insert_sql, batch)
            inserted += len(batch)

    mysql_conn.commit()
    source_count = sqlite_conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    return source_count, inserted


def main() -> None:
    sqlite_conn = sqlite3.connect(SQLITE_PATH)
    mysql_conn = pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        charset="utf8mb4",
        autocommit=False,
    )

    try:
        tables = get_tables(sqlite_conn)
        with mysql_conn.cursor() as cur:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")

        print(f"Migrating {len(tables)} tables from {SQLITE_PATH} to {MYSQL_DB}...")
        for table in tables:
            source_count, inserted = migrate_table(sqlite_conn, mysql_conn, table)
            print(f"- {table}: sqlite={source_count}, mysql_inserted={inserted}")

        with mysql_conn.cursor() as cur:
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
        mysql_conn.commit()
        print("Migration completed.")
    finally:
        sqlite_conn.close()
        mysql_conn.close()


if __name__ == "__main__":
    main()
