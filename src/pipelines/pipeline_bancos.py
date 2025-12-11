#  src/pipelines/pipeline_bancos.py
from __future__ import annotations
import sys
import sqlite3
from pathlib import Path
import pandas as pd

# ------------------------------------------------------------
# Bootstrap rutas
# ------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ------------------------------------------------------------
# Imports corporativos
# ------------------------------------------------------------
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config
from src.transformers.calculator import Calculator


# ============================================================
#   PIPELINE BANCOS — NORMALIZACIÓN ÚNICA ESTÁNDAR 2025
# ============================================================
class PipelineBancos:

    def __init__(self):
        info("Inicializando PipelineBancos…")

        self.cfg = get_config()
        self.db_path = self.cfg.db_pulseforge
        self.calc = Calculator()

        ok(f"PipelineBancos listo. BD destino → {self.db_path}")

    # --------------------------------------------------------
    # LECTURA DIRECTA DESDE BD DESTINO
    # --------------------------------------------------------
    def load_bancos(self) -> pd.DataFrame:
        """Carga movimientos desde bancos_pf."""
        try:
            conn = sqlite3.connect(self.db_path)

            df = pd.read_sql_query("""
                SELECT * FROM bancos_pf;
            """, conn)

            conn.close()

            if df.empty:
                warn("No se encontraron movimientos en bancos_pf.")
            else:
                ok(f"Movimientos cargados: {len(df)}")

            return df

        except Exception as e:
            error(f"Error cargando bancos_pf: {e}")
            return pd.DataFrame()

    # --------------------------------------------------------
    # PROCESO PRINCIPAL
    # --------------------------------------------------------
    def process(self) -> pd.DataFrame:
        """Aplica proceso de normalización bancaria."""
        df = self.load_bancos()

        if df.empty:
            warn("PipelineBancos: No hay data para procesar.")
            return df

        info("Normalizando movimientos bancarios…")
        df_out = self.calc.process_bancos(df)

        ok("Movimientos bancarios procesados correctamente.")
        return df_out

    # --------------------------------------------------------
    # GUARDADO (opcional)
    # --------------------------------------------------------
    def save(self, df: pd.DataFrame, table_name: str = "bancos_pf_norm"):
        """Guarda la versión normalizada."""
        try:
            conn = sqlite3.connect(self.db_path)
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            conn.close()
            ok(f"Movimientos guardados en tabla: {table_name}")
        except Exception as e:
            error(f"Error guardando movimientos bancarios: {e}")


# ============================================================
# EJECUCIÓN DIRECTA
# ============================================================
if __name__ == "__main__":
    pb = PipelineBancos()
    df_out = pb.process()

    if df_out is not None and not df_out.empty:
        info("Vista previa del resultado:")
        print(df_out.head())
