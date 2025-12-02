# src/loaders/newdb_builder.py
from __future__ import annotations

# ============================================================
#  PULSEFORGE · NEW DB BUILDER (VERSIÓN CORPORATIVA)
#  Crea la BD destino pulseforge.sqlite con todas sus tablas
# ============================================================
import sys
import sqlite3
from pathlib import Path

# Bootstrap rutas
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env


class NewDBBuilder:
    """
    Construye la base de datos destino de PulseForge.
    Contiene tablas corporativas para:
      - clientes_pf
      - facturas_pf
      - movimientos_pf
      - matches_pf
      - logs_pf
    """

    def __init__(self) -> None:
        info("Inicializando NewDBBuilder…")

        db_path = get_env("PULSEFORGE_NEWDB_PATH")
        if not db_path:
            error("PULSEFORGE_NEWDB_PATH no configurado en .env")
            raise ValueError("Falta PULSEFORGE_NEWDB_PATH")

        self.db_path = Path(db_path)

        # Crear carpeta si no existe
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        ok(f"Ruta destino BD PulseForge → {self.db_path}")

    # --------------------------------------------------------
    def build(self, reset: bool = False) -> None:
        """
        Crea la base de datos PulseForge con todas sus tablas.
        Si reset=True, elimina la base existente antes de crearla.
        """

        if reset and self.db_path.exists():
            warn("Reset activado: eliminando base de datos previa.")
            self.db_path.unlink()

        conn = sqlite3.connect(self.db_path)

        try:
            info("Creando estructura de tablas PulseForge…")
            cur = conn.cursor()

            # ------------------------------------------------------------
            # CLIENTES
            cur.execute("""
                CREATE TABLE IF NOT EXISTS clientes_pf (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ruc TEXT NOT NULL,
                    razon_social TEXT,
                    source_hash TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ------------------------------------------------------------
            # FACTURAS
            cur.execute("""
                CREATE TABLE IF NOT EXISTS facturas_pf (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ruc TEXT,
                    cliente_generador TEXT,
                    subtotal REAL,
                    serie TEXT,
                    numero TEXT,
                    combinada TEXT,
                    estado_fs TEXT,
                    estado_cont TEXT,
                    fecha_emision TEXT,
                    fecha_limite_pago TEXT,
                    fecha_inicio_ventana TEXT,
                    fecha_fin_ventana TEXT,
                    neto_recibido REAL,
                    total_con_igv REAL,
                    detraccion_monto REAL,
                    estado_pago TEXT,
                    monto_cobrado REAL DEFAULT 0,
                    source_hash TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ------------------------------------------------------------
            # MOVIMIENTOS BANCARIOS
            cur.execute("""
                CREATE TABLE IF NOT EXISTS movimientos_pf (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    banco TEXT,
                    fecha TEXT,
                    tipo_mov TEXT,
                    descripcion TEXT,
                    monto REAL,
                    moneda TEXT,
                    operacion TEXT,
                    es_dolares INTEGER,
                    destinatario TEXT,
                    tipo_documento TEXT,
                    source_hash TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ------------------------------------------------------------
            # MATCHES (RESULTADOS DEL MATCHER)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS matches_pf (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    factura_id INTEGER,
                    movimiento_id INTEGER,
                    monto_aplicado REAL,
                    monto_detraccion REAL,
                    variacion REAL,
                    fecha_pago TEXT,
                    banco TEXT,
                    score REAL,
                    razon_ia TEXT,
                    match_tipo TEXT,
                    source_hash TEXT UNIQUE,
                    created_at TEXT DEFAULT (datetime('now','localtime')),
                    FOREIGN KEY (factura_id) REFERENCES facturas_pf(id),
                    FOREIGN KEY (movimiento_id) REFERENCES movimientos_pf(id)
                )
            """)

            # ------------------------------------------------------------
            # LOGS
            cur.execute("""
                CREATE TABLE IF NOT EXISTS logs_pf (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    nivel TEXT,
                    mensaje TEXT,
                    timestamp TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ------------------------------------------------------------
            # ÍNDICES
            info("Creando índices…")

            cur.executescript("""
                CREATE INDEX IF NOT EXISTS idx_facturas_ruc
                    ON facturas_pf(ruc);

                CREATE INDEX IF NOT EXISTS idx_facturas_combinada
                    ON facturas_pf(combinada);

                CREATE INDEX IF NOT EXISTS idx_mov_banco_fecha
                    ON movimientos_pf(banco, fecha);

                CREATE INDEX IF NOT EXISTS idx_mov_operacion
                    ON movimientos_pf(operacion);

                CREATE INDEX IF NOT EXISTS idx_matches_factura
                    ON matches_pf(factura_id);

                CREATE INDEX IF NOT EXISTS idx_matches_movimiento
                    ON matches_pf(movimiento_id);
            """)

            conn.commit()
            ok("Base de datos PulseForge creada correctamente.")

        except Exception as e:
            error(f"Error creando la nueva base PulseForge: {e}")
            raise
        finally:
            conn.close()


# ============================================================
# TEST LOCAL
# ============================================================
if __name__ == "__main__":
    try:
        builder = NewDBBuilder()
        builder.build(reset=True)
        ok("Test de creación de base completado exitosamente.")
    except Exception as e:
        error(f"Fallo en test de NewDBBuilder: {e}")
