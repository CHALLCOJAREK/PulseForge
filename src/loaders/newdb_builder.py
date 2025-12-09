# src/loaders/newdb_builder.py
from __future__ import annotations
import sys
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config


class NewDBBuilder:

    def __init__(self):
        info("Inicializando NewDBBuilder…")

        cfg = get_config()

        # 🔥 FIX REAL: atributo correcto del config
        db_path = cfg.pulseforge_newdb_path

        if not db_path:
            error("PULSEFORGE_NEWDB_PATH no está definido en settings.json/.env")
            raise ValueError("Ruta destino de BD no encontrada.")

        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        ok(f"Base destino → {self.db_path}")

    # ======================================================
    def build(self, reset: bool = False):
        if reset and self.db_path.exists():
            warn("RESET activo → eliminando BD previa.")
            self.db_path.unlink()

        conn = sqlite3.connect(self.db_path)

        try:
            cur = conn.cursor()
            info("Creando estructura PulseForge …")

            # ============================================
            # TABLA CLIENTES
            # ============================================
            cur.execute("""
                CREATE TABLE IF NOT EXISTS clientes_pf (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ruc TEXT NOT NULL,
                    razon_social TEXT,
                    source_hash TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ============================================
            # TABLA FACTURAS
            # ============================================
            cur.execute("""
                CREATE TABLE IF NOT EXISTS facturas_pf (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                    detraccion REAL,
                    total_neto_cobrado REAL,
                    fue_cobrado INTEGER DEFAULT 0,
                    fecha_cobro TEXT,
                    match_id INTEGER,
                    source_hash TEXT UNIQUE,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ============================================
            # TABLA MOVIMIENTOS
            # ============================================
            cur.execute("""
                CREATE TABLE IF NOT EXISTS movimientos_pf (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    banco_codigo TEXT,
                    fecha TEXT,
                    tipo_mov TEXT,
                    descripcion TEXT,
                    monto_original REAL,
                    moneda_original TEXT,
                    monto_pen REAL,
                    operacion TEXT,
                    destinatario TEXT,
                    tipo_documento TEXT,
                    es_cuenta_empresa INTEGER,
                    es_cuenta_detraccion INTEGER,
                    es_cuenta_principal INTEGER,
                    source_hash TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ============================================
            # TABLA MATCHES RESUMEN
            # ============================================
            cur.execute("""
                CREATE TABLE IF NOT EXISTS matches_pf (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    factura_id INTEGER,
                    movimiento_id INTEGER,
                    monto_factura REAL,
                    monto_banco REAL,
                    diferencia REAL,
                    match_tipo TEXT,
                    score REAL,
                    razon_ia TEXT,
                    source_hash TEXT UNIQUE,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ============================================
            # TABLA MATCH DETALLE
            # ============================================
            cur.execute("""
                CREATE TABLE IF NOT EXISTS match_detalles_pf (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    factura_id INTEGER,
                    movimiento_id INTEGER,
                    serie TEXT,
                    numero TEXT,
                    combinada TEXT,
                    ruc TEXT,
                    cliente TEXT,
                    fecha_mov TEXT,
                    banco_codigo TEXT,
                    descripcion_banco TEXT,
                    tipo_comparacion TEXT,
                    monto_ref REAL,
                    monto_banco REAL,
                    diff_monto REAL,
                    es_detraccion_bn INTEGER,
                    coincide_fecha INTEGER,
                    coincide_monto INTEGER,
                    coincide_nombre INTEGER,
                    dias_diff_fecha INTEGER,
                    score_final REAL,
                    resultado_final TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ============================================
            # ÍNDICES
            # ============================================
            cur.executescript("""
                CREATE INDEX IF NOT EXISTS idx_facturas_ruc ON facturas_pf(ruc);
                CREATE INDEX IF NOT EXISTS idx_facturas_match ON facturas_pf(match_id);
                CREATE INDEX IF NOT EXISTS idx_movimientos_banco_fecha ON movimientos_pf(banco_codigo, fecha);
                CREATE INDEX IF NOT EXISTS idx_matches_factura ON matches_pf(factura_id);
                CREATE INDEX IF NOT EXISTS idx_matches_movimiento ON matches_pf(movimiento_id);
            """)

            conn.commit()
            ok("✔ BD PulseForge creada correctamente.")

        except Exception as e:
            error(f"Error creando BD PulseForge → {e}")
            raise

        finally:
            conn.close()
