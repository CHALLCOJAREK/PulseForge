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

        self.db_path = self.env.get("PULSEFORGE_NEWDB_PATH")
        if not self.db_path:
            error("No se encontró PULSEFORGE_NEWDB_PATH en .env.")
            raise ValueError("Ruta de BD destino no definida.")

        self.engine = create_engine(f"sqlite:///{self.db_path}")

        ok("InvoiceWriter listo para escribir facturas procesadas.")


    # =======================================================
    #   LIMPIAR TABLA (opcional)
    # =======================================================
    def limpiar_tabla(self):
        """
        Borra todo el contenido de facturas_pf.
        Solo usar antes de una carga completa.
        """
        info("Limpiando tabla facturas_pf...")

        sql = text("DELETE FROM facturas_pf;")

        with self.engine.connect() as conn:
            conn.execute(sql)

        ok("Tabla facturas_pf limpiada.")


    # =======================================================
    #   INSERTAR FACTURAS
    # =======================================================
    def escribir_facturas(self, df_facturas: pd.DataFrame):
        """
        Inserta el DataFrame en facturas_pf.
        df_facturas debe contener ya los cálculos del Calculator.
        """

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



# =======================================================
#   TEST DIRECTO (opcional)
# =======================================================
if __name__ == "__main__":
    warn("Test directo del InvoiceWriter. No usar en producción.")
