# src/core/db.py
from __future__ import annotations
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import sqlite3

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config


# ============================================================
#   EXCEPCIÓN CENTRAL DE BD
# ============================================================
class DatabaseError(Exception):
    pass


# ============================================================
#   BASE CLASS — MOTOR NEUTRO
# ============================================================
class BaseDB:
    """Clase base compartida para cualquier motor de BD."""
    
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = None

    # --------------------------------------------------------
    def connect(self):
        """Conexión segura a SQLite."""
        try:
            info(f"Conectando a SQLite → {self.db_path}")
            self.connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                isolation_level=None  # modo autocommit estable
            )
            ok("Conexión exitosa.")
            return self.connection
        except Exception as e:
            error(f"Error al conectar DB: {e}")
            raise DatabaseError(e)

    # --------------------------------------------------------
    def close(self):
        """Cierra la conexión limpia."""
        if self.connection:
            try:
                self.connection.close()
                ok("Conexión cerrada correctamente.")
            except Exception:
                pass

    # --------------------------------------------------------
    def execute(self, query: str, params: Optional[tuple] = None):
        """Ejecuta INSERT/UPDATE/DELETE con control de errores."""
        if self.connection is None:
            self.connect()

        cur = self.connection.cursor()
        try:
            if params:
                cur.execute(query, params)
            else:
                cur.execute(query)
            return cur
        except Exception as e:
            error(f"Error en execute(): {e}\nQuery: {query}")
            raise DatabaseError(e)

    # --------------------------------------------------------
    def read_query(self, query: str):
        """Ejecuta SELECT usando pandas."""
        import pandas as pd
        if self.connection is None:
            self.connect()

        try:
            df = pd.read_sql_query(query, self.connection)
            ok(f"Consulta realizada: {len(df)} filas.")
            return df
        except Exception as e:
            error(f"Error en read_query(): {e}")
            raise DatabaseError(e)

    # --------------------------------------------------------
    def fetch_all(self, table: str) -> List[Dict[str, Any]]:
        """SELECT * FROM ... parseado a dict."""
        if self.connection is None:
            self.connect()

        try:
            cur = self.connection.cursor()
            cur.execute(f"SELECT * FROM {table}")
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]
            return [dict(zip(cols, row)) for row in rows]
        except Exception as e:
            error(f"Error leyendo tabla {table}: {e}")
            raise DatabaseError(e)

    # --------------------------------------------------------
    def get_tables(self) -> List[str]:
        if self.connection is None:
            self.connect()

        try:
            cur = self.connection.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [r[0] for r in cur.fetchall()]
            ok(f"Tablas: {tables}")
            return tables
        except Exception as e:
            error(f"No se pudieron listar tablas: {e}")
            raise DatabaseError(e)


# ============================================================
#   BD ORIGEN → DATAPULSE
# ============================================================
class SourceDB(BaseDB):
    """Lee la base DataPulse (origen)."""

    def __init__(self):
        cfg = get_config()
        super().__init__(cfg.db_source_path)
        info(f"BD Origen: {cfg.db_source_path}")


# ============================================================
#   BD DESTINO → PULSEFORGE
# ============================================================
class PulseForgeDB(BaseDB):
    """Escribe y lee la base interna PulseForge."""

    def __init__(self):
        cfg = get_config()
        super().__init__(cfg.db_path)
        info(f"BD PulseForge: {cfg.db_path}")

    # Método extra para escritura masiva segura
    def insert(self, table: str, data: Dict[str, Any]):
        if not data:
            warn("Insert vacío, se omite.")
            return

        columns = ", ".join(data.keys())
        placeholders = ", ".join(["?"] * len(data))
        values = tuple(data.values())

        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        self.execute(query, values)

    def update(self, table: str, data: Dict[str, Any], where: str, params: tuple):
        sets = ", ".join([f"{k}=?" for k in data.keys()])
        values = tuple(data.values())
        query = f"UPDATE {table} SET {sets} WHERE {where}"
        self.execute(query, values + params)
