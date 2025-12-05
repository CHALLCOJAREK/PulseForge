# src/loaders/newdb_builder.py
from __future__ import annotations
import sys
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env


class NewDBBuilder:
    """
    Construye la base de datos interna de PulseForge.
    Diseño profesional y alineado a:
      - calculator.py
      - data_mapper.py
      - match_writer.py
      - matcher_engine.py
    """

    def __init__(self):
        info("Inicializando NewDBBuilder …")

        db_path = get_env("PULSEFORGE_NEWDB_PATH")
        if not db_path:
            error("PULSEFORGE_NEWDB_PATH no definido.")
            raise ValueError("Ruta destino de BD ausente.")

        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        ok(f"Base destino → {self.db_path}")

    # ============================================================
    def build(self, reset: bool = False):
        if reset and self.db_path.exists():
            warn("RESET activo → eliminando BD previa.")
            self.db_path.unlink()

        conn = sqlite3.connect(self.db_path)

        try:
            cur = conn.cursor()
            info("Creando estructura PulseForge …")

            # ============================================================
            # CLIENTES
            # ============================================================
            cur.execute("""
                CREATE TABLE IF NOT EXISTS clientes_pf (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    ruc             TEXT NOT NULL,
                    razon_social    TEXT,
                    source_hash     TEXT,
                    created_at      TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ============================================================
            # FACTURAS — DISEÑO DEFINITIVO
            # ============================================================
            cur.execute("""
                CREATE TABLE IF NOT EXISTS facturas_pf (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,

                    -- Datos base
                    ruc                 TEXT,
                    cliente_generador   TEXT,
                    serie               TEXT,
                    numero              TEXT,
                    combinada           TEXT,

                    -- Fechas
                    fecha_emision       TEXT,
                    vencimiento         TEXT,

                    -- Cálculos contables (calculator.py)
                    subtotal            REAL,
                    igv                 REAL,
                    total               REAL,
                    detraccion          REAL,
                    total_neto_cobrado  REAL,

                    -- Estado de cobranza
                    fue_cobrado         INTEGER DEFAULT 0,
                    fecha_cobro         TEXT,
                    match_id            INTEGER,

                    source_hash         TEXT UNIQUE,
                    created_at          TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ============================================================
            # MOVIMIENTOS BANCARIOS
            # ============================================================
            cur.execute("""
                CREATE TABLE IF NOT EXISTS movimientos_pf (
                    id                      INTEGER PRIMARY KEY AUTOINCREMENT,

                    banco_codigo            TEXT,
                    fecha                   TEXT,
                    tipo_mov                TEXT,
                    descripcion             TEXT,

                    -- Montos normalizados
                    monto_original          REAL,
                    moneda_original         TEXT,
                    monto_pen               REAL,

                    operacion               TEXT,
                    destinatario            TEXT,
                    tipo_documento          TEXT,

                    -- Clasificación
                    es_cuenta_empresa       INTEGER,
                    es_cuenta_detraccion    INTEGER,
                    es_cuenta_principal     INTEGER,

                    source_hash             TEXT,
                    created_at              TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ============================================================
            # MATCHES — RESUMEN OFICIAL
            # ============================================================
            cur.execute("""
                CREATE TABLE IF NOT EXISTS matches_pf (
                    id                  INTEGER PRIMARY KEY AUTOINCREMENT,

                    factura_id          INTEGER,
                    movimiento_id       INTEGER,

                    -- Montos comparados
                    monto_factura       REAL,
                    monto_banco         REAL,
                    diferencia          REAL,

                    -- Método
                    match_tipo          TEXT,
                    score               REAL,
                    razon_ia            TEXT,

                    source_hash         TEXT UNIQUE,
                    created_at          TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ============================================================
            # DETALLE DEL MATCH — TRAZABILIDAD COMPLETA
            # ============================================================
            cur.execute("""
                CREATE TABLE IF NOT EXISTS match_detalles_pf (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,

                    factura_id              INTEGER,
                    movimiento_id           INTEGER,

                    serie                   TEXT,
                    numero                  TEXT,
                    combinada               TEXT,
                    ruc                     TEXT,
                    cliente                 TEXT,

                    fecha_mov               TEXT,
                    banco_codigo            TEXT,
                    descripcion_banco       TEXT,

                    tipo_comparacion        TEXT,
                    monto_ref               REAL,
                    monto_banco             REAL,
                    diff_monto              REAL,

                    es_detraccion_bn        INTEGER,
                    coincide_fecha          INTEGER,
                    coincide_monto          INTEGER,
                    coincide_nombre         INTEGER,
                    dias_diff_fecha         INTEGER,

                    score_final             REAL,
                    resultado_final         TEXT,
                    created_at              TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ============================================================
            # LOGS (opcional)
            # ============================================================
            cur.execute("""
                CREATE TABLE IF NOT EXISTS logs_pf (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nivel TEXT,
                    mensaje TEXT,
                    timestamp TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ============================================================
            # ÍNDICES
            # ============================================================
            info("Creando índices …")
            cur.executescript("""
                CREATE INDEX IF NOT EXISTS idx_facturas_ruc
                    ON facturas_pf(ruc);

                CREATE INDEX IF NOT EXISTS idx_facturas_match
                    ON facturas_pf(match_id);

                CREATE INDEX IF NOT EXISTS idx_movimientos_banco_fecha
                    ON movimientos_pf(banco_codigo, fecha);

                CREATE INDEX IF NOT EXISTS idx_matches_factura
                    ON matches_pf(factura_id);

                CREATE INDEX IF NOT EXISTS idx_matches_movimiento
                    ON matches_pf(movimiento_id);
            """)

            conn.commit()
            ok("✔ BD PulseForge creada correctamente.")

        except Exception as e:
            error(f"Error creando BD PulseForge → {e}")
            raise

        finally:
            conn.close()
