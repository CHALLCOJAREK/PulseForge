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
from contextlib import contextmanager

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config


# =====================================================
# Excepción de base de datos
# =====================================================
class DatabaseError(Exception):
    pass


# =====================================================
# Context manager para conexiones seguras
# =====================================================
@contextmanager
def safe_cursor(conn):
    cur = None
    try:
        cur = conn.cursor()
        yield cur
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise DatabaseError(f"DB Error: {e}")
    finally:
        if cur:
            cur.close()


# =====================================================
# Motor universal
# =====================================================
class BaseDB:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection: Optional[sqlite3.Connection] = None

    # ---------------------
    # Conexión segura
    # ---------------------
    def connect(self):
        if self.connection:
            return self.connection

        try:
            info(f"Conectando SQLite → {self.db_path}")
            self.connection = sqlite3.connect(
                self.db_path,
                check_same_thread=False,
                isolation_level="DEFERRED",
                timeout=10
            )
            ok("Conexión establecida.")
            return self.connection
        except Exception as e:
            error(f"Error al conectar: {e}")
            raise DatabaseError(e)

    # ---------------------
    # Cierre seguro
    # ---------------------
    def close(self):
        if self.connection:
            try:
                self.connection.close()
                ok("Conexión cerrada.")
            except:
                pass

    # ---------------------
    # Ejecutar sentencia
    # ---------------------
    def execute(self, query: str, params: Optional[tuple] = None):
        conn = self.connect()
        with safe_cursor(conn) as cur:
            try:
                cur.execute(query, params or ())
                return cur
            except Exception as e:
                error(f"Error execute(): {e}\nQuery: {query}")
                raise

    # ---------------------
    # SELECT → DataFrame seguro
    # ---------------------
    def read_query(self, query: str):
        import pandas as pd

        conn = self.connect()
        try:
            df = pd.read_sql_query(query, conn)
            ok(f"SELECT → {len(df)} filas.")
            return df
        except Exception as e:
            error(f"Error read_query(): {e}")
            raise DatabaseError(e)

    # ---------------------
    # SELECT * tabla (con verificación)
    # ---------------------
    def fetch_all(self, table: str):
        conn = self.connect()

        # Validar tabla antes de ejecutar
        if table not in self.get_tables():
            raise DatabaseError(f"Tabla no encontrada: {table}")

        try:
            q = f"SELECT * FROM {table}"
            df = self.read_query(q)
            return df.to_dict(orient="records")
        except Exception as e:
            error(f"Error fetch_all({table}): {e}")
            raise

    # ---------------------
    # Listar tablas
    # ---------------------
    def get_tables(self) -> List[str]:
        conn = self.connect()
        try:
            with safe_cursor(conn) as cur:
                cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
                return [r[0] for r in cur.fetchall()]
        except Exception as e:
            error(f"No se pudieron listar tablas: {e}")
            raise DatabaseError(e)


# =====================================================
# BD Origen
# =====================================================
class SourceDB(BaseDB):
    def __init__(self):
        cfg = get_config()
        super().__init__(cfg.db_source)
        info(f"BD Origen configurada: {cfg.db_source}")


# =====================================================
# BD Interna PulseForge
# =====================================================
class PulseForgeDB(BaseDB):
    def __init__(self):
        cfg = get_config()

        # Corregido: antes decía cfg.db_pulseforge (NO existe)
        super().__init__(cfg.db_destino)

        info(f"BD PulseForge configurada: {cfg.db_destino}")

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


# =====================================================
# BD Nueva (Destino final / export)
# =====================================================
class NewDB(BaseDB):
    def __init__(self):
        cfg = get_config()
        super().__init__(cfg.db_new)
        info(f"BD Nueva configurada: {cfg.db_new}")
