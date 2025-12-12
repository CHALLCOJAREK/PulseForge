# src/loaders/newdb_builder.py
from __future__ import annotations
import sqlite3
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env


class NewDBBuilder:

    def __init__(self):
        self.db_path = Path(str(get_env("PULSEFORGE_NEWDB_PATH")).strip())

        info("=== PulseForge · Creación / Verificación de BD destino ===")
        info(f"Ruta destino → {self.db_path}")

        self._ensure_folder()
        self._create_schema()

    # ---------------------------------------------------------
    def _ensure_folder(self):
        folder = self.db_path.parent
        if not folder.exists():
            folder.mkdir(parents=True, exist_ok=True)
            ok(f"Carpeta creada: {folder}")

    # ---------------------------------------------------------
    def _create_schema(self):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        info("Creando tablas base PulseForge…")

        # -----------------------------------------------------
        # BANCOS
        # -----------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS bancos_pf (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_hash TEXT UNIQUE,
            fecha TEXT,
            tipo_mov TEXT,
            descripcion TEXT,
            operacion TEXT,
            destinatario TEXT,
            tipo_documento TEXT,
            monto REAL,
            moneda TEXT,
            banco_codigo TEXT
        );
        """)

        # -----------------------------------------------------
        # CLIENTES
        # -----------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS clientes_pf (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_hash TEXT UNIQUE,
            ruc TEXT,
            razon_social TEXT
        );
        """)

        # -----------------------------------------------------
        # FACTURAS
        # -----------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS facturas_pf (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_hash TEXT UNIQUE,
            ruc TEXT,
            cliente_generador TEXT,
            serie TEXT,
            numero TEXT,
            combinada TEXT,
            fecha_emision TEXT,
            vencimiento TEXT,
            subtotal REAL,
            igv REAL,
            total REAL,
            estado_fs TEXT,
            estado_cont TEXT,
            fue_cobrado INTEGER,
            match_id TEXT
        );
        """)

        # -----------------------------------------------------
        # CALCULOS
        # -----------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS calculos_pf (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            factura_hash TEXT,
            subtotal REAL,
            igv REAL,
            total_con_igv REAL,
            detraccion REAL,
            total_sin_detraccion REAL,
            total_final REAL,
            dias_credito INTEGER,
            fecha_pago TEXT,
            variacion REAL
        );
        """)

        # -----------------------------------------------------
        # MATCH
        # -----------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS match_pf (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            factura_hash TEXT,
            banco_hash TEXT,
            cliente_hash TEXT,
            tipo_monto_match TEXT,
            monto_factura REAL,
            monto_banco REAL,
            diferencia REAL,
            porcentaje_match REAL,
            estado TEXT,
            fecha_match TEXT
        );
        """)

        # -----------------------------------------------------
        # AUDITORÍA
        # -----------------------------------------------------
        cur.execute("""
        CREATE TABLE IF NOT EXISTS auditoria_pf (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            evento TEXT,
            detalle TEXT,
            fecha TEXT
        );
        """)

        conn.commit()
        conn.close()
        ok("Todas las tablas creadas ✔")
