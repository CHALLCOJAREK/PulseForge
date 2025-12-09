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
from src.core.env_loader import get_config
from src.core.utils import parse_date, clean_amount
from src.transformers.data_mapper import DataMapper


class InvoicesExtractor:

    def __init__(self):
        info("Inicializando InvoicesExtractor…")

        # Config centralizada
        self.cfg = get_config()

        # DataMapper (solo para mapeos)
        self.mapper = DataMapper()

        # Tabla de facturas
        self.tabla_facturas = self.cfg.tablas.get("facturas")

        if not self.tabla_facturas:
            error("Falta 'facturas' en config.tablas")
            raise KeyError("No existe tabla facturas en configuración.")

        ok(f"Tabla de facturas configurada → {self.tabla_facturas}")

        # Columnas dinámicas desde settings.json
        self.cols_cfg = self.cfg.columnas_facturas


    # --------------------------------------------------------
    def _connect(self):
        db_path = self.cfg.db_source
        file = Path(db_path)

        if not file.exists():
            error(f"BD origen no encontrada: {file}")
            raise FileNotFoundError("No existe BD origen")

        return sqlite3.connect(file)

    # --------------------------------------------------------
    def _load_raw(self) -> pd.DataFrame:
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
    def _normalize_df(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """Selecciona columnas reales basadas en settings.columnas_facturas."""
        df = pd.DataFrame()

        for col_std, posibles in self.cols_cfg.items():

            if isinstance(posibles, list):
                col_real = next((c for c in posibles if c in df_raw.columns), None)
            else:
                col_real = posibles if posibles in df_raw.columns else None

            if not col_real:
                warn(f"[FACTURAS] Falta columna '{col_std}' → None")
                df[col_std] = None
            else:
                df[col_std] = df_raw[col_real]

        return df

    # --------------------------------------------------------
    def _fix_dates(self, df: pd.DataFrame, campos: list[str]):
        """Parsea fechas sin romper el proceso."""
        for campo in campos:
            if campo in df.columns:
                df[campo] = df[campo].apply(
                    lambda x: parse_date(x) if x not in (None, "", "nan", "0") else None
                )

        return df

    # --------------------------------------------------------
    def extract(self) -> list[dict]:

        df_raw = self._load_raw()
        df = self._normalize_df(df_raw)

        # Fechas
        df = self._fix_dates(df, ["fecha_emision", "vencimiento"])

        # Subtotal limpio
        if "subtotal" in df.columns:
            df["subtotal"] = df["subtotal"].apply(clean_amount)

        # Mapeo estandarizado de DataMapper
        mapped = self.mapper.map_facturas(df)

        ok(f"Facturas extraídas y mapeadas → {len(mapped)} registros.")
        return mapped
