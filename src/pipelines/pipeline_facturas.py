# src/pipelines/pipeline_facturas.py
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
#   PIPELINE FACTURAS — NORMALIZACIÓN + CÁLCULO CONTABLE
# ============================================================
class PipelineFacturas:

    def __init__(self):
        info("Inicializando PipelineFacturas…")
        self.cfg = get_config()
        self.db_path = self.cfg.db_pulseforge
        self.calc = Calculator()
        ok(f"PipelineFacturas listo. BD destino → {self.db_path}")

    # --------------------------------------------------------
    # LECTURA DIRECTA DESDE BD DESTINO
    # --------------------------------------------------------
    def load_facturas(self) -> pd.DataFrame:
        """Carga las facturas crudas desde facturas_pf."""
        try:
            conn = sqlite3.connect(self.db_path)
            df = pd.read_sql_query("SELECT * FROM facturas_pf;", conn)
            conn.close()

            if df.empty:
                warn("No se encontraron facturas en facturas_pf.")
            else:
                ok(f"Facturas cargadas: {len(df)}")

            return df

        except Exception as e:
            error(f"Error cargando facturas_pf: {e}")
            return pd.DataFrame()

    # --------------------------------------------------------
    # PROCESO PRINCIPAL (NORMALIZAR + CALCULAR)
    # --------------------------------------------------------
    def process(self) -> pd.DataFrame:
        """
        Aplica Calculator a todas las facturas ya cargadas
        y guarda inmediatamente los cálculos en la BD destino.
        """
        df = self.load_facturas()
        if df.empty:
            warn("PipelineFacturas: No hay data que procesar.")
            return df

        info("Aplicando Calculator sobre facturas…")
        df_calc = self.calc.process_facturas(df)

        # ====================================================
        # NUEVO: Guardar cálculos en calculos_pf
        # ====================================================
        try:
            self.calc.save_calculos(df_calc)
            ok("Cálculos financieros persistidos correctamente.")
        except Exception as e:
            error(f"Error guardando cálculos financieros: {e}")

        ok("Facturas procesadas correctamente.")
        return df_calc

    # --------------------------------------------------------
    # GUARDADO OPCIONAL EN TABLA AUXILIAR (no se usa en FULL)
    # --------------------------------------------------------
    def save(self, df: pd.DataFrame, table_name: str = "facturas_pf_calc"):
        """Guarda los cálculos en una tabla opcional."""
        try:
            conn = sqlite3.connect(self.db_path)
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            conn.close()
            ok(f"Cálculo de facturas guardado en tabla: {table_name}")
        except Exception as e:
            error(f"Error guardando resultados de facturas: {e}")


# ============================================================
# EJECUCIÓN DIRECTA
# ============================================================
if __name__ == "__main__":
    pf = PipelineFacturas()
    df_out = pf.process()

    if df_out is not None and not df_out.empty:
        info("Vista previa del resultado:")
        print(df_out.head())
