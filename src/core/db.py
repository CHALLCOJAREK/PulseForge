# src/core/db.py
from __future__ import annotations

# --- BOOTSTRAP PARA RUTAS ---
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))
# -----------------------------

import sqlite3
from typing import Optional, List, Dict, Any

# Config
from src.core.env_loader import get_config

# -----------------------------
# LOGGING UNIFICADO
# -----------------------------
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


# ================================================================
#     MOTOR DE BASE DE DATOS UNIFICADO – PULSEFORGE DB ENGINE
# ================================================================
class DatabaseError(Exception):
    pass


class PulseForgeDB:
    def __init__(self):
        self.cfg = get_config()
        self.connection = None

    # ------------------------------------------------------------
    #   CONECTOR PRINCIPAL
    # ------------------------------------------------------------
    def connect(self):
        db_type = self.cfg.db_type

        info(f"Conectando a la base de datos ({db_type})…")

        try:
            if db_type == "sqlite":
                self.connection = sqlite3.connect(self.cfg.db_path)
                ok(f"Conectado a SQLite: {self.cfg.db_path}")

            elif db_type == "postgres":
                import psycopg2
                self.connection = psycopg2.connect(self.cfg.db_path)
                ok("Conectado a PostgreSQL")

            elif db_type == "mysql":
                import mysql.connector
                self.connection = mysql.connector.connect(self.cfg.db_path)
                ok("Conectado a MySQL")

            else:
                raise DatabaseError(f"Tipo de DB no soportado: {db_type}")

        except Exception as e:
            error(f"Error conectando a la DB: {e}")
            raise DatabaseError(str(e))

    # ------------------------------------------------------------
    #   LEER TABLAS
    # ------------------------------------------------------------
    def get_tables(self) -> List[str]:
        if self.connection is None:
            self.connect()

        info("Listando tablas de la base de datos…")

        cursor = self.connection.cursor()

        try:
            if self.cfg.db_type == "sqlite":
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cursor.fetchall()]

            elif self.cfg.db_type == "postgres":
                cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
                tables = [row[0] for row in cursor.fetchall()]

            elif self.cfg.db_type == "mysql":
                cursor.execute("SHOW TABLES")
                tables = [row[0] for row in cursor.fetchall()]

            ok(f"Tablas encontradas: {tables}")
            return tables

        except Exception as e:
            error(f"Error listando tablas: {e}")
            raise DatabaseError(str(e))

    # ------------------------------------------------------------
    #   LEER COLUMNAS DE UNA TABLA
    # ------------------------------------------------------------
    def get_columns(self, table: str) -> List[str]:
        if self.connection is None:
            self.connect()

        info(f"Obteniendo columnas de: {table}")

        cursor = self.connection.cursor()

        try:
            if self.cfg.db_type == "sqlite":
                cursor.execute(f"PRAGMA table_info({table})")
                columns = [row[1] for row in cursor.fetchall()]

            elif self.cfg.db_type == "postgres":
                cursor.execute(
                    f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{table}'
                    """
                )
                columns = [row[0] for row in cursor.fetchall()]

            elif self.cfg.db_type == "mysql":
                cursor.execute(f"DESCRIBE {table}")
                columns = [row[0] for row in cursor.fetchall()]

            ok(f"Columnas obtenidas correctamente.")
            return columns

        except Exception as e:
            error(f"Error listando columnas de {table}: {e}")
            raise DatabaseError(str(e))

    # ------------------------------------------------------------
    #   LEER TODA UNA TABLA COMO DICTS
    # ------------------------------------------------------------
    def fetch_all(self, table: str) -> List[Dict[str, Any]]:
        if self.connection is None:
            self.connect()

        info(f"Leyendo registros de la tabla: {table}")
        cursor = self.connection.cursor()

        try:
            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()

            col_names = [desc[0] for desc in cursor.description]
            data = [dict(zip(col_names, r)) for r in rows]

            ok(f"Registros obtenidos: {len(data)}")
            return data

        except Exception as e:
            error(f"Error leyendo tabla {table}: {e}")
            raise DatabaseError(str(e))


# =============================================================
#  TEST DIRECTO DEL MÓDULO — VISUAL ENTERPRISE PRO
# =============================================================
if __name__ == "__main__":
    print("\n" + "="*63)
    print("🔵  PULSEFORGE · DB ENGINE TEST")
    print("="*63 + "\n")

    try:
        # ----------------------------------------------------
        # CONFIGURACIÓN
        # ----------------------------------------------------
        cfg = get_config()
        ok("Configuración cargada correctamente.\n")

        print("📂 CONFIGURACIÓN")
        print("-" * 63)
        print(f"  • Tipo de DB        : {cfg.db_type}")
        print(f"  • Base origen       : {cfg.db_path}")
        print(f"  • IGV               : {cfg.igv}")
        print(f"  • Detracción        : {cfg.detraccion_porcentaje}")
        print(f"  • TC USD → PEN      : {cfg.tipo_cambio_usd_pen}\n")

        # ----------------------------------------------------
        # CONEXIÓN
        # ----------------------------------------------------
        print("🗄️  CONEXIÓN")
        print("-" * 63)
        db = PulseForgeDB()
        db.connect()
        ok("Conexión establecida.\n")

        # ----------------------------------------------------
        # TABLAS
        # ----------------------------------------------------
        tables = db.get_tables()
        print(f"📊 TABLAS ({len(tables)} encontradas)")
        print("-" * 63)
        for t in tables:
            print(f"  - {t}")
        print()

        # ----------------------------------------------------
        # COLUMNAS POR TABLA
        # ----------------------------------------------------
        print("📑 ESTRUCTURA DE COLUMNAS")
        print("-" * 63)

        for t in tables:
            cols = db.get_columns(t)

            print(f"\n📁 {t}  ({len(cols)} columnas)")
            print("    ----------------------------------------")

            for c in cols:
                print(f"    - {c}")

            print("    ----------------------------------------")

        print("\n" + "="*63)
        print("🟢  PRUEBA TERMINADA — SIN ERRORES")
        print("="*63 + "\n")

    except Exception as e:
        print("\n" + "="*63)
        error("ERROR CRÍTICO EN LA PRUEBA")
        error(str(e))
        print("="*63 + "\n")
