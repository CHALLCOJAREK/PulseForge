# src/loaders/newdb_builder.py

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from sqlalchemy import create_engine, text
from src.core.env_loader import get_env

# Prints estilo Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class NewDBBuilder:
    """
    Crea la base de datos destino pulseforge.sqlite
    con las tablas estándar del sistema.
    """

    def __init__(self):
        self.env = get_env()
        self.db_path = self.env.get("PULSEFORGE_NEWDB_PATH")

        if not self.db_path:
            error("No hay ruta a la BD destino en .env (PULSEFORGE_NEWDB_PATH).")
            raise ValueError("BD destino no definida")

        self.engine = create_engine(f"sqlite:///{self.db_path}")

        info("Iniciando constructor de nueva BD PulseForge...")

    # =========================================================
    #   Crear tablas base
    # =========================================================
    def crear_tablas(self):
        info("Creando tablas pulseforge...")

        tablas_sql = [

            # ------------------- CLIENTES -------------------
            """
            CREATE TABLE IF NOT EXISTS clientes_pf (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ruc TEXT,
                razon_social TEXT
            );
            """,

            # ------------------- FACTURAS -------------------
            """
            CREATE TABLE IF NOT EXISTS facturas_pf (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                combinada TEXT,
                ruc TEXT,
                cliente_generador TEXT,
                subtotal REAL,
                igv REAL,
                total_con_igv REAL,
                detraccion REAL,
                neto_recibido REAL,
                fecha_emision TEXT,
                forma_pago INTEGER,
                fecha_limite_pago TEXT,
                fecha_inicio_ventana TEXT,
                fecha_fin_ventana TEXT,
                estado_fs TEXT,
                estado_cont TEXT
            );
            """,

            # ------------------- BANCOS -------------------
            """
            CREATE TABLE IF NOT EXISTS bancos_pf (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                banco TEXT,
                fecha_mov TEXT,
                tipo_mov TEXT,
                descripcion TEXT,
                serie TEXT,
                numero TEXT,
                monto REAL,
                moneda TEXT,
                operacion TEXT,
                destinatario TEXT,
                tipo_documento TEXT
            );
            """,

            # ------------------- MATCHES -------------------
            """
            CREATE TABLE IF NOT EXISTS matches_pf (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                factura TEXT,
                cliente TEXT,
                fecha_emision TEXT,
                fecha_limite TEXT,
                fecha_mov TEXT,
                banco TEXT,
                operacion TEXT,
                monto_factura REAL,
                monto_banco REAL,
                diferencia_monto REAL,
                similitud REAL,
                resultado TEXT
            );
            """
        ]

        with self.engine.connect() as conn:
            for sql in tablas_sql:
                conn.execute(text(sql))

        ok("Tablas creadas con éxito en pulseforge.sqlite 🚀")



# ======================================================
#   TEST DIRECTO (opcional)
# ======================================================
if __name__ == "__main__":
    info("🚀 Construyendo pulseforge.sqlite...")
    builder = NewDBBuilder()
    builder.crear_tablas()
    ok("BD PulseForge estructurada correctamente.")
