# src/loaders/invoice_writer.py

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
from sqlalchemy import create_engine, text
from src.core.env_loader import get_env

# Prints estilo Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class InvoiceWriter:
    """
    Inserta facturas procesadas (mapper + calculator)
    en la tabla facturas_pf de pulseforge.sqlite.
    """

    def __init__(self):
        self.env = get_env()

        # ⚠️ CORRECCIÓN CRÍTICA:
        # Antes: self.env.get("PULSEFORGE_NEWDB_PATH") → devuelve None
        # Ahora: usamos el nombre REAL almacenado en EnvConfig: DB_PATH_NUEVA
        self.db_path = self.env.get("DB_PATH_NUEVA")

        if not self.db_path:
            error("No se encontró la ruta de BD destino (DB_PATH_NUEVA).")
            raise ValueError("Ruta de BD destino no definida en entorno.")

        self.engine = create_engine(f"sqlite:///{self.db_path}")
        ok("InvoiceWriter listo para escribir facturas procesadas.")


    # =======================================================
    #   LIMPIAR TABLA (para FULL RUN)
    # =======================================================
    def limpiar_tabla(self):
        info("Limpiando tabla facturas_pf...")

        sql = text("DELETE FROM facturas_pf;")

        with self.engine.connect() as conn:
            conn.execute(sql)

        ok("Tabla facturas_pf limpiada.")


    # =======================================================
    #   INSERTAR FACTURAS
    # =======================================================
    def escribir_facturas(self, df_facturas: pd.DataFrame):
        info(f"Insertando {len(df_facturas)} facturas en PulseForge...")

        try:
            df_facturas.to_sql(
                "facturas_pf",
                con=self.engine,
                if_exists="append",
                index=False
            )
        except Exception as e:
            error(f"Error insertando facturas: {e}")
            raise

        ok("Facturas insertadas correctamente en facturas_pf 🚀")


if __name__ == "__main__":
    warn("Test directo del InvoiceWriter. No usar en producción.")
