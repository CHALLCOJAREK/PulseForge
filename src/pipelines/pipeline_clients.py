#  src/pipelines/pipeline_clients.py
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
from src.transformers.data_mapper import DataMapper


# ============================================================
#   PIPELINE CLIENTES — NORMALIZACIÓN RAW
# ============================================================
class PipelineClientes:

    def __init__(self):
        info("Inicializando PipelineClientes…")

        self.cfg = get_config()
        self.db_path = self.cfg.db_pulseforge
        self.mapper = DataMapper()

        ok(f"PipelineClientes listo. BD destino → {self.db_path}")

    # --------------------------------------------------------
    # LECTURA DESDE BD DESTINO
    # --------------------------------------------------------
    def load_clientes(self) -> pd.DataFrame:
        """Lee clientes desde clientes_pf (tabla destino RAW)."""
        try:
            conn = sqlite3.connect(self.db_path)

            df = pd.read_sql_query("""
                SELECT * FROM clientes_pf;
            """, conn)

            conn.close()

            if df.empty:
                warn("No se encontraron registros en clientes_pf.")
            else:
                ok(f"Clientes cargados: {len(df)}")

            return df

        except Exception as e:
            error(f"Error cargando clientes_pf: {e}")
            return pd.DataFrame()

    # --------------------------------------------------------
    # PROCESO PRINCIPAL
    # --------------------------------------------------------
    def process(self) -> list[dict]:
        """
        Aplica DataMapper.map_clientes() a la tabla cargada.
        Retorna lista READY-TO-INSERT para writers o posteriores pipelines.
        """
        df = self.load_clientes()

        if df.empty:
            warn("PipelineClientes: No hay data que procesar.")
            return []

        info("Mapeando clientes…")
        clientes_list = self.mapper.map_clientes(df)

        ok("Clientes procesados correctamente.")
        return clientes_list

    # --------------------------------------------------------
    # GUARDADO (opcional)
    # --------------------------------------------------------
    def save(self, clientes_list: list[dict], table_name: str = "clientes_pf_norm"):
        """Persiste clientes normalizados (si se requiere)."""
        try:
            df = pd.DataFrame(clientes_list)

            conn = sqlite3.connect(self.db_path)
            df.to_sql(table_name, conn, if_exists="replace", index=False)
            conn.close()

            ok(f"Clientes guardados en tabla: {table_name}")
        except Exception as e:
            error(f"Error guardando clientes normalizados: {e}")
