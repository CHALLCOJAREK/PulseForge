# src/loaders/newdb_builder.py

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import sqlite3
from datetime import datetime
from src.core.env_loader import get_env

# Prints estilo Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class NewDBBuilder:
    """
    Construye la BD nueva de PulseForge.
    Crea tablas:
      - facturas_procesadas
      - match_results
      - logs (auditoría interna)
    """

    def __init__(self):
        self.env = get_env()
        self.db_path = self.env.PULSEFORGE_NEWDB_PATH

        info(f"Iniciando creación/verificación de nueva BD en:")
        info(f"{self.db_path}")

        # Crear carpeta si no existe
        db_dir = os.path.dirname(self.db_path)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
            ok(f"Carpeta creada: {db_dir}")

        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()

        ok("Conectado a la BD destino.")


    # =======================================================
    #     CREAR TABLA facturas_procesadas
    # =======================================================
    def create_table_facturas(self):
        info("Creando/verificando tabla: facturas_procesadas...")

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS facturas_procesadas (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Factura TEXT UNIQUE,
            RUC TEXT,
            Razon_Social TEXT,
            Subtotal REAL,
            IGV REAL,
            Total REAL,
            Detraccion REAL,
            Monto_Neto REAL,
            Fecha_Emision TEXT,
            Fecha_Vencimiento TEXT,
            Fecha_Registro TEXT
        )
        """)

        ok("Tabla facturas_procesadas OK ✓")


    # =======================================================
    #     CREAR TABLA match_results
    # =======================================================
    def create_table_match(self):
        info("Creando/verificando tabla: match_results...")

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS match_results (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Factura TEXT UNIQUE,
            RUC TEXT,
            Razon_Social TEXT,
            Fecha_Pago TEXT,
            Monto_Pagado REAL,
            Cuenta_Pago TEXT,
            Tipo_Pago TEXT,
            Estado TEXT,
            Fecha_Registro TEXT
        )
        """)

        ok("Tabla match_results OK ✓")


    # =======================================================
    #     CREAR TABLA logs
    # =======================================================
    def create_table_logs(self):
        info("Creando/verificando tabla: logs...")

        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            evento TEXT,
            detalle TEXT
        )
        """)

        ok("Tabla logs OK ✓")


    # =======================================================
    #     GUARDAR LOG
    # =======================================================
    def write_log(self, evento, detalle=""):
        """
        Guarda log en la BD Y en archivo .log externo
        """
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Guardar en SQLite
        self.cursor.execute(
            "INSERT INTO logs (timestamp, evento, detalle) VALUES (?, ?, ?)",
            (ts, evento, detalle)
        )
        self.conn.commit()

        # Log externo
        logs_dir = os.path.join(os.path.dirname(self.db_path), "logs")
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)

        log_file = os.path.join(logs_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log")

        with open(log_file, "a", encoding="utf-8") as f:
            f.write(f"[{ts}] {evento} — {detalle}\n")

        ok(f"Log escrito: {evento}")


    # =======================================================
    #     CONSTRUIR TODAS LAS TABLAS
    # =======================================================
    def build(self):
        info("Construyendo estructura completa de la BD nueva...")

        self.create_table_facturas()
        self.create_table_match()
        self.create_table_logs()

        self.write_log("BD Construida", "La estructura inicial fue creada correctamente.")

        ok("PulseForge DB LISTA PARA TRABAJAR 🚀")


# =======================================================
#     TEST DIRECTO
# =======================================================
if __name__ == "__main__":
    info("🚀 Testeando NewDBBuilder...")
    builder = NewDBBuilder()
    builder.build()
