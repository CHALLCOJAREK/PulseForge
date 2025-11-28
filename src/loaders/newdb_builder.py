# src/loaders/newdb_builder.py

import os
from sqlalchemy import create_engine, text
from src.core.env_loader import get_env

def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class NewDBBuilder:

    def __init__(self):
        self.env = get_env()

        # Nombre correcto: PULSEFORGE_NEWDB_PATH
        self.db_path = self.env.get("PULSEFORGE_NEWDB_PATH")

        if not self.db_path:
            error("No existe PULSEFORGE_NEWDB_PATH en .env.")
            raise ValueError("BD destino no definida en .env")

        info("Iniciando constructor de nueva BD PulseForge...")

        # Crear engine
        self.engine = create_engine(f"sqlite:///{self.db_path}", future=True)


    # ============================================================
    #  CREACIÓN DE TABLAS — VERSIÓN BLINDADA Y DEFINITIVA
    # ============================================================
    def crear_tablas(self):
        info("Creando tablas pulseforge...")

        tablas_sql = [

            # =============== CLIENTES ===============
            """
            CREATE TABLE IF NOT EXISTS clientes_pf (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ruc TEXT,
                razon_social TEXT
            );
            """,

            # =============== FACTURAS ===============
            """
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
                forma_pago INTEGER,
                Vencimiento TEXT,
                dias_pago INTEGER,
                igv REAL,
                total_con_igv REAL,
                detraccion_monto REAL,
                neto_recibido REAL,
                fecha_limite_pago TEXT,
                fecha_inicio_ventana TEXT,
                fecha_fin_ventana TEXT
            );
            """,

            # =============== BANCOS ===============
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

            # =============== MATCHES ===============
            """
            CREATE TABLE IF NOT EXISTS matches_pf (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                factura TEXT,
                cliente TEXT,
                ruc TEXT,
                fecha_emision TEXT,
                fecha_limite TEXT,
                fecha_mov TEXT,
                banco_pago TEXT,
                operacion TEXT,
                monto_banco REAL,
                monto_banco_equiv REAL,
                monto_ref_usado REAL,
                tipo_monto_ref TEXT,
                diferencia_monto REAL,
                sim_nombre REAL,
                tiene_terminos_flex INTEGER,
                resultado TEXT,
                banco_det TEXT,
                fecha_det TEXT,
                monto_det REAL,
                razon TEXT
            );
            """
        ]

        with self.engine.connect() as conn:
            for sql in tablas_sql:
                conn.execute(text(sql))

        ok("Tablas creadas con éxito en pulseforge.sqlite 🚀")
