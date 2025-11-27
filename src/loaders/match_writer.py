# src/loaders/match_writer.py

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


class MatchWriter:
    """
    Inserta los resultados del Matcher en la tabla matches_pf.
    """

    def __init__(self):
        self.env = get_env()

        self.db_path = self.env.get("PULSEFORGE_NEWDB_PATH")
        if not self.db_path:
            error("No se encontró PULSEFORGE_NEWDB_PATH en .env.")
            raise ValueError("Ruta de BD destino no definida.")

        self.engine = create_engine(f"sqlite:///{self.db_path}")

        ok("MatchWriter listo para operar.")


    # =======================================================
    #   LIMPIAR TABLA DE MATCHES
    # =======================================================
    def limpiar_tabla(self):
        """
        Limpia matches_pf.
        Usar solo antes de una corrida FULL.
        """
        info("Limpiando tabla matches_pf...")

        with self.engine.connect() as conn:
            conn.execute(text("DELETE FROM matches_pf;"))

        ok("Tabla matches_pf limpiada.")


    # =======================================================
    #   INSERTAR MATCHES
    # =======================================================
    def escribir_matches(self, df_matches: pd.DataFrame):
        """
        Inserta los matches del Matcher.
        df_matches debe contener:
            factura
            cliente
            fecha_emision
            fecha_limite
            fecha_mov
            banco
            operacion
            monto_factura
            monto_banco
            diferencia_monto
            similitud
            resultado
        """

        info(f"Insertando {len(df_matches)} matches en PulseForge...")

        try:
            df_matches.to_sql(
                "matches_pf",
                con=self.engine,
                if_exists="append",
                index=False
            )
        except Exception as e:
            error(f"Error insertando matches: {e}")
            raise

        ok("Matches insertados correctamente en matches_pf 🚀")



# =======================================================
#   TEST DIRECTO (opcional)
# =======================================================
if __name__ == "__main__":
    warn("Test directo del MatchWriter. Solo para debug.")
