# src/extractors/invoices_extractor.py
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


class InvoicesExtractor:
    """
    Extrae facturas desde la BD origen (DataPulse) usando el nombre de tabla
    definido en settings.json. Devuelve lista de diccionarios listos para
    insertarse en facturas_pf mediante DataMapper.
    """

    def __init__(self):
        info("Inicializando InvoicesExtractor…")

        self.mapper = DataMapper()
        tablas_cfg = self.mapper.settings.get("tablas", {})
        self.tabla_facturas = tablas_cfg.get("facturas")

        if not self.tabla_facturas:
            error("Falta 'facturas' en settings['tablas']")
            raise KeyError("No existe tabla facturas en configuración.")

        ok(f"Tabla de facturas → {self.tabla_facturas}")

    # --------------------------------------------------------
    def _connect(self):
        """Conexión limpia a DataPulse SQLite."""
        db_path = get_env("PULSEFORGE_SOURCE_DB", required=True)

        db_file = Path(db_path)
        if not db_file.exists():
            error(f"BD origen no encontrada: {db_file}")
            raise FileNotFoundError("No existe BD origen")

        return sqlite3.connect(db_file)

    # --------------------------------------------------------
    def _load_raw(self) -> pd.DataFrame:
        """Lee data cruda desde DataPulse."""
        conn = self._connect()

        try:
            query = f'SELECT * FROM "{self.tabla_facturas}"'
            df = pd.read_sql_query(query, conn)

            if df.empty:
                warn("Tabla de facturas vacía.")

            return df

        except Exception as e:
            error(f"Error leyendo facturas: {e}")
            raise
        finally:
            conn.close()

    # --------------------------------------------------------
    def _normalize_columns(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Solo copia las columnas que están en settings.json
        (columnas_facturas). NO hace cálculos ni interpretaciones.
        """
        cols_cfg = self.mapper.settings["columnas_facturas"]
        df_norm = pd.DataFrame()

        for col_std, col_real in cols_cfg.items():
            if col_real not in df_raw.columns:
                warn(f"[FACTURAS] Falta columna '{col_real}' → se coloca None.")
                df_norm[col_std] = None
            else:
                df_norm[col_std] = df_raw[col_real]

        return df_norm

    # --------------------------------------------------------
    def extract(self) -> list[dict]:
        """
        Extrae → normaliza → mapea → devuelve list[dict].
        """
        df_raw = self._load_raw()
        df_norm = self._normalize_columns(df_raw)

        # DataMapper produce una lista de diccionarios ya calculada
        mapped = self.mapper.map_facturas(df_norm)

        ok(f"Facturas extraídas y mapeadas → {len(mapped)} registros.")
        return mapped
