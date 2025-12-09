# src/core/db.py
from __future__ import annotations
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

# -------------------------
# Bootstrap interno
# -------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

import sqlite3
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config


# -------------------------
# Excepción de base de datos
# -------------------------
class DatabaseError(Exception):
    pass


# -------------------------
# Motor base universal
# -------------------------
class BaseDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = None

    # ---------------------
    # Conexión
    # ---------------------
    def connect(self):
        try:
            info(f"Conectando SQLite → {self.db_path}")
            self.connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                isolation_level=None
            )
            ok("Conexión establecida.")
            return self.connection
        except Exception as e:
            error(f"Error al conectar: {e}")
            raise DatabaseError(e)

    # ---------------------
    # Cierre
    # ---------------------
    def close(self):
        if self.connection:
            try:
                self.connection.close()
                ok("Conexión cerrada.")
            except Exception:
                pass

    # ---------------------
    # Ejecutar sentencia
    # ---------------------
    def execute(self, query: str, params: Optional[tuple] = None):
        if self.connection is None:
            self.connect()

        cur = self.connection.cursor()
        try:
            cur.execute(query, params or ())
            return cur
        except Exception as e:
            error(f"Error execute(): {e}\nQuery: {query}")
            raise DatabaseError(e)

    # ---------------------
    # SELECT en DataFrame
    # ---------------------
    def read_query(self, query: str):
        import pandas as pd
        if self.connection is None:
            self.connect()

        try:
            df = pd.read_sql_query(query, self.connection)
            ok(f"SELECT → {len(df)} filas.")
            return df
        except Exception as e:
            error(f"Error read_query(): {e}")
            raise DatabaseError(e)

    # ---------------------
    # SELECT * tabla
    # ---------------------
    def fetch_all(self, table: str) -> List[Dict[str, Any]]:
        if self.connection is None:
            self.connect()

        try:
            cur = self.connection.cursor()
            cur.execute(f"SELECT * FROM {table}")
            rows = cur.fetchall()
            cols = [c[0] for c in cur.description]
            return [dict(zip(cols, row)) for row in rows]
        except Exception as e:
            error(f"Error fetch_all({table}): {e}")
            raise DatabaseError(e)

    # ---------------------
    # Listado tablas
    # ---------------------
    def get_tables(self) -> List[str]:
        if self.connection is None:
            self.connect()

        try:
            cur = self.connection.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [r[0] for r in cur.fetchall()]
            ok(f"Tablas detectadas: {tables}")
            return tables
        except Exception as e:
            error(f"No se pudieron listar tablas: {e}")
            raise DatabaseError(e)


# -------------------------
# BD Origen → DataPulse
# -------------------------
class SourceDB(BaseDB):
    def __init__(self):
        cfg = get_config()
        super().__init__(cfg.db_source)
        info(f"BD Origen configurada: {cfg.db_source}")


# -------------------------
# BD PulseForge interna
# -------------------------
class PulseForgeDB(BaseDB):
    def __init__(self):
        cfg = get_config()
        super().__init__(cfg.db_pulseforge)
        info(f"BD PulseForge configurada: {cfg.db_pulseforge}")

    # ---------------------
    # Insert seguro
    # ---------------------
    def insert(self, table: str, data: Dict[str, Any]):
        if not data:
            warn("Insert omitido (data vacía).")
            return

        cols = ", ".join(data.keys())
        marks = ", ".join(["?"] * len(data))
        values = tuple(data.values())

        q = f"INSERT INTO {table} ({cols}) VALUES ({marks})"
        self.execute(q, values)

    # ---------------------
    # Update seguro
    # ---------------------
    def update(self, table: str, data: Dict[str, Any], where: str, params: tuple):
        sets = ", ".join([f"{k}=?" for k in data.keys()])
        values = tuple(data.values())

        q = f"UPDATE {table} SET {sets} WHERE {where}"
        self.execute(q, values + params)


# -------------------------
# BD de destino final → Nuevo PulseForge
# -------------------------
class NewDB(BaseDB):
    def __init__(self):
        cfg = get_config()
        super().__init__(cfg.db_new)
        info(f"BD Nueva configurada: {cfg.db_new}")
