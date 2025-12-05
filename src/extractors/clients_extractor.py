# src/extractors/clients_extractor.py
from __future__ import annotations

import sys
import sqlite3
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env
from src.transformers.data_mapper import DataMapper


class ClientsExtractor:
    """
    Extrae y normaliza clientes desde DataPulse usando la tabla definida
    en settings.json. Devuelve un DataFrame listo para insertar en
    clientes_pf.
    """

    def __init__(self):
        info("Inicializando ClientsExtractor…")

        self.mapper = DataMapper()
        tablas_cfg = self.mapper.settings.get("tablas", {})
        self.tabla_clientes = tablas_cfg.get("clientes")

        if not self.tabla_clientes:
            error("settings.json no tiene 'clientes' en 'tablas'")
            raise KeyError("Falta configuración de tabla de clientes")

        ok(f"Tabla de clientes detectada → {self.tabla_clientes}")

    # ==================================================================
    def _connect(self):
        """Conexión a BD origen DataPulse."""
        db_path = get_env("PULSEFORGE_SOURCE_DB", required=True)
        if not Path(db_path).exists():
            error(f"BD origen no encontrada: {db_path}")
            raise FileNotFoundError("No existe BD origen")
        return sqlite3.connect(db_path)

    # ==================================================================
    def _load_raw(self) -> pd.DataFrame:
        """Lee la tabla cruda tal cual existe en DataPulse."""
        try:
            conn = self._connect()
            query = f'SELECT * FROM "{self.tabla_clientes}"'
            df = pd.read_sql_query(query, conn)
            conn.close()

            if df.empty:
                warn("Tabla de clientes vacía en BD origen.")

            return df

        except Exception as e:
            error(f"Error leyendo clientes: {e}")
            raise

    # ==================================================================
    def extract(self) -> pd.DataFrame:
        """
        Extrae → normaliza → mapea → devuelve DataFrame.
        (ANTES devolvía list[dict], ahora corregido)
        """
        df_raw = self._load_raw()

        # DataMapper devuelve list[dict]
        mapped_list = self.mapper.map_clientes(df_raw)

        # Convertimos a DataFrame (lo que necesita el Writer)
        df = pd.DataFrame(mapped_list)

        ok(f"Clientes extraídos y mapeados: {len(df)}")
        return df


# ============================================================
# TEST LOCAL
# ============================================================
if __name__ == "__main__":
    try:
        info("⚙️ Test local ClientsExtractor…")
        ce = ClientsExtractor()
        df = ce.extract()
        print(df.head())
        ok(f"Test finalizado. Total: {len(df)}")
    except Exception as e:
        error(f"Fallo en test: {e}")
