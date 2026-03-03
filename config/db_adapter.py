import os
import sqlite3
from typing import Optional, Tuple
from pathlib import Path
from config.settings import get_settings

_settings = get_settings()

def _is_postgres() -> bool:
    return str(_settings.db_backend).lower() == "postgres" and bool(_settings.database_url)

def open_conn_and_cursor(read_only: bool = False):
    if not _is_postgres():
        db_path = _settings.database_path
        if read_only:
            resolved = Path(db_path).resolve()
            uri = resolved.as_uri() + "?mode=ro"
            conn = sqlite3.connect(uri, uri=True)
        else:
            conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        return conn, cur
    else:
        import psycopg
        from psycopg.rows import dict_row
        conn = psycopg.connect(_settings.database_url)
        cur = conn.cursor(row_factory=dict_row)
        return conn, cur

def close_conn(conn, cursor: Optional[object] = None):
    try:
        if cursor:
            cursor.close()
    except Exception:
        pass
    try:
        conn.close()
    except Exception:
        pass
