# src/extractors/bank_extractor.py
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


class BankExtractor:
    """
    Extrae movimientos bancarios desde DataPulse en base a las tablas
    configuradas en settings.json → 'tablas_bancos'.

    Devuelve list[dict] listos para insertar en movimientos_pf.
    """

    def __init__(self):
        info("Inicializando BankExtractor…")
        self.mapper = DataMapper()

        self.tablas_bancos = self.mapper.settings.get("tablas_bancos", {})
        if not self.tablas_bancos:
            error("settings.json no contiene 'tablas_bancos'")
            raise KeyError("Falta tablas_bancos en configuración.")

        ok(f"Tablas detectadas: {self.tablas_bancos}")

    # ---------------------------------------------------------
    def _connect(self):
        """Conexión a la BD origen DataPulse."""
        db_path = get_env("PULSEFORGE_SOURCE_DB", required=True)

        if not Path(db_path).exists():
            error(f"No existe BD origen: {db_path}")
            raise FileNotFoundError("BD origen no encontrada.")

        return sqlite3.connect(db_path)

    # ---------------------------------------------------------
    def _load_raw(self, tabla_real: str, cod: str) -> pd.DataFrame:
        """Lee tabla cruda."""
        conn = self._connect()

        try:
            query = f'SELECT * FROM "{tabla_real}"'
            df = pd.read_sql_query(query, conn)

            if df.empty:
                warn(f"[{cod}] Tabla vacía.")
            return df

        except Exception as e:
            error(f"Error leyendo banco {cod}: {e}")
            raise

        finally:
            conn.close()

    # ---------------------------------------------------------
    def extract(self) -> list[dict]:
        """
        Extrae → normaliza → mapea → devuelve movimientos bancarios list[dict].
        """

        movimientos_finales = []

        for cod, tabla_real in self.tablas_bancos.items():

            df_raw = self._load_raw(tabla_real, cod)
            if df_raw.empty:
                continue

            # --------------------------------------------
            # Mapeo profesional usando DataMapper
            # --------------------------------------------
            mapped_rows = self.mapper.map_bancos(df_raw, tabla_real)

            # Agregar banco_codigo a cada fila
            for m in mapped_rows:
                m["banco_codigo"] = cod

            movimientos_finales.extend(mapped_rows)

            ok(f"{len(mapped_rows)} movimientos procesados para banco {cod}")

        if not movimientos_finales:
            warn("No se encontraron movimientos bancarios.")
            return []

        ok(f"TOTAL movimientos bancarios normalizados: {len(movimientos_finales)}")
        return movimientos_finales
