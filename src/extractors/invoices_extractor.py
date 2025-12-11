# src/extractors/invoices_extractor.py
from __future__ import annotations

# ------------------------------------------------------------
# Bootstrap rutas
# ------------------------------------------------------------
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ------------------------------------------------------------
# Imports core
# ------------------------------------------------------------
import pandas as pd

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config
from src.core.db import SourceDB
from src.core.utils import parse_date, normalize_text
from src.transformers.data_mapper import DataMapper


# ============================================================
#   EXTRACTOR DE FACTURAS (PULSEFORGE 2025)
# ============================================================
class InvoicesExtractor:

    def __init__(self):
        info("Inicializando InvoicesExtractor…")

        self.cfg = get_config()
        self._db = SourceDB()
        self._db.connect()

        # Tabla origen
        self._tabla_facturas = self.cfg.tablas.get("facturas")
        if not self._tabla_facturas:
            error("Falta 'facturas' en settings.tablas.")
            raise KeyError("Config inválida: no existe tabla de facturas.")

        ok(f"Tabla de facturas configurada → {self._tabla_facturas}")

        # Columnas dinámicas definidas en settings.json
        self.cols_cfg = self.cfg.columnas_facturas or {}
        if not self.cols_cfg:
            warn("columnas_facturas vacío. Mapper trabajará limitado.")

        # Mapper PulseForge
        self.mapper = DataMapper()

        # Para trazabilidad
        self._col_origen_map = {}

    # --------------------------------------------------------
    @staticmethod
    def _norm(name: str) -> str:
        if not isinstance(name, str):
            return ""
        return (
            name.strip()
                .lower()
                .replace(" ", "")
                .replace("_", "")
        )

    # --------------------------------------------------------
    def _pick(self, df: pd.DataFrame, posibles: list[str]) -> str | None:
        if df.empty or not posibles:
            return None

        posibles_norm = [self._norm(p) for p in posibles]
        cols_norm = {col: self._norm(col) for col in df.columns}

        # 1) Exacto
        for col, norm in cols_norm.items():
            if norm in posibles_norm:
                return col

        # 2) Parcial
        for col, norm in cols_norm.items():
            if any(alias in norm for alias in posibles_norm):
                return col

        # 3) Parcial invertido
        for col, norm in cols_norm.items():
            if any(norm in alias for alias in posibles_norm):
                return col

        return None

    # --------------------------------------------------------
    def _load_raw(self) -> pd.DataFrame:
        try:
            q = f'SELECT * FROM "{self._tabla_facturas}"'
            df = self._db.read_query(q)

            if df.empty:
                warn(f"Tabla '{self._tabla_facturas}' vacía en BD origen.")
                return df

            # FIX columnas con espacios invisibles
            df.columns = [c.strip() for c in df.columns]

            ok(f"Facturas cargadas desde origen → {len(df)} filas.")
            return df

        except Exception as e:
            error(f"Error leyendo facturas desde BD origen: {e}")
            return pd.DataFrame()

    # --------------------------------------------------------
    def _normalize_df(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        df = pd.DataFrame()
        self._col_origen_map.clear()

        if df_raw.empty:
            warn("DF crudo vacío en _normalize_df().")
            return df

        for campo_std, posibles_nombres in self.cols_cfg.items():

            # Asegurar que la lista de alias siempre sea una lista
            if not isinstance(posibles_nombres, list):
                posibles_nombres = [posibles_nombres]

            col_real = self._pick(df_raw, posibles_nombres)


            if not col_real:
                warn(f"[FACTURAS] No se halló columna para '{campo_std}'")
                df[campo_std] = None
                continue

            df[campo_std] = df_raw[col_real]
            self._col_origen_map[campo_std] = col_real

        # mostrar mapeo
        if self._col_origen_map:
            info("📑 Mapeo columnas facturas:")
            for std, real in self._col_origen_map.items():
                info(f"   - {std:<15} ⇐ {real}")

        return df

    # --------------------------------------------------------
    def _fix_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        for campo in ["fecha_emision", "vencimiento"]:
            if campo in df.columns:
                df[campo] = df[campo].apply(
                    lambda x: parse_date(x) if x not in (None, "", "nan", "0") else None
                )
        return df

    # --------------------------------------------------------
    def _post_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        for campo in ["cliente_generador", "estado_cont", "estado_fs"]:
            if campo in df.columns:
                df[campo] = df[campo].astype(str).apply(normalize_text)
        return df

    # --------------------------------------------------------
    # Pipeline principal → devuelve LISTA DE DICTS
    # --------------------------------------------------------
    def extract(self) -> list[dict]:
        df_raw = self._load_raw()

        if df_raw.empty:
            warn("InvoicesExtractor.extract() → SIN registros de facturas.")
            return []

        df = self._normalize_df(df_raw)
        if df.empty:
            warn("DF normalizado vacío.")
            return []

        df = self._fix_dates(df)
        df = self._post_clean(df)

        info("Inicializando DataMapper PulseForge (RAW → inteligente)…")
        mapped = self.mapper.map_facturas(df)

        ok(f"Facturas extraídas + mapeadas: {len(mapped)}")
        return mapped

    # ============================================================
    #          INTERFAZ ESTÁNDAR PARA PIPELINES → ie.run()
    # ============================================================
    def run(self) -> list[dict]:
        """
        Devuelve facturas listas para los writers.
        DataMapper YA devuelve lista de dicts → no transformamos nada.
        """
        registros = self.extract()

        if not registros:
            warn("[InvoicesExtractor] No retornó registros.")
            return []

        ok(f"[InvoicesExtractor] Registros preparados para carga: {len(registros)}")
        return registros
