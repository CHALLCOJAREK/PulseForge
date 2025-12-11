# src/extractors/clients_extractor.py
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
from typing import List, Optional
import hashlib

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config
from src.core.db import SourceDB
from src.core.utils import normalize_text, clean_ruc


# ============================================================
#   EXTRACTOR DE CLIENTES (LIGERO / SÓLO LECTURA)
# ============================================================
class ClientsExtractor:

    def __init__(self) -> None:
        info("Inicializando ClientsExtractor…")

        cfg = get_config()
        self._tabla_clientes: Optional[str] = cfg.tablas.get("clientes")

        if not self._tabla_clientes:
            warn("settings.json no define 'clientes' en 'tablas'. Extractor devolverá vacío.")
        else:
            ok(f"Tabla de clientes configurada → {self._tabla_clientes}")

        # Una sola conexión viva
        self._db = SourceDB()
        self._db.connect()

    # --------------------------------------------------------
    @staticmethod
    def _norm(name: str) -> str:
        if not isinstance(name, str):
            return ""
        return name.strip().lower().replace(" ", "").replace("_", "")

    # --------------------------------------------------------
    # Selección inteligente de columnas
    # --------------------------------------------------------
    def _pick_column(self, df: pd.DataFrame, posibles: List[str]) -> Optional[str]:
        if df.empty:
            return None

        posibles_norm = [self._norm(p) for p in posibles]
        cols_norm = {col: self._norm(col) for col in df.columns}

        # Match exacto
        for col, norm in cols_norm.items():
            if norm in posibles_norm:
                return col

        # Alias contenido
        for col, norm in cols_norm.items():
            if any(alias in norm for alias in posibles_norm):
                return col

        # Nombre contenido en alias
        for col, norm in cols_norm.items():
            if any(norm in alias for alias in posibles_norm):
                return col

        return None

    # --------------------------------------------------------
    # Lectura cruda desde BD origen
    # --------------------------------------------------------
    def _load_raw(self) -> pd.DataFrame:
        if not self._tabla_clientes:
            return pd.DataFrame()

        try:
            q = f'SELECT * FROM "{self._tabla_clientes}"'
            df = self._db.read_query(q)

            if df.empty:
                warn(f"Tabla '{self._tabla_clientes}' vacía.")
            else:
                ok(f"Clientes cargados: {len(df)}")

            return df

        except Exception as e:
            error(f"Error leyendo tabla clientes '{self._tabla_clientes}': {e}")
            return pd.DataFrame()

    # --------------------------------------------------------
    # Extractor principal → DataFrame
    # --------------------------------------------------------
    def extract(self) -> pd.DataFrame:
        df_raw = self._load_raw()
        if df_raw.empty:
            warn("ClientsExtractor.extract() → vacío")
            return pd.DataFrame(columns=["ruc", "razon_social"])

        # Detectar columnas
        col_ruc = self._pick_column(df_raw, ["ruc", "documento", "doc", "dni"])
        col_name = self._pick_column(df_raw, ["razon", "cliente", "nombre", "rs", "name"])

        if not col_ruc:
            error("No se detectó columna RUC → extractor aborta.")
            return pd.DataFrame(columns=["ruc", "razon_social"])

        if not col_name:
            error("No se detectó columna nombre/razón social → extractor aborta.")
            return pd.DataFrame(columns=["ruc", "razon_social"])

        ok(f"Columna RUC detectada → {col_ruc}")
        ok(f"Columna nombre detectada → {col_name}")

        # Normalización
        df = pd.DataFrame()
        df["ruc"] = df_raw[col_ruc].astype(str).apply(clean_ruc)
        df["razon_social"] = df_raw[col_name].astype(str).apply(normalize_text)

        # Limpieza
        antes = len(df)
        df = df[df["ruc"] != ""]
        df = df.drop_duplicates(subset=["ruc"])
        despues = len(df)

        ok(f"Clientes normalizados → {despues} (descartados {antes - despues})")

        return df

    # --------------------------------------------------------
    # HASH único por cliente
    # --------------------------------------------------------
    @staticmethod
    def _make_hash(cli: dict) -> str:
        try:
            base = "|".join(str(cli.get(k, "")) for k in sorted(cli.keys()))
            return hashlib.sha256(base.encode("utf-8")).hexdigest()
        except Exception:
            return ""

    # --------------------------------------------------------
    # Convertir DF → lista de dicts para Writers
    # --------------------------------------------------------
    def _df_to_records(self, df: pd.DataFrame) -> list[dict]:
        registros: list[dict] = []

        if df is None or df.empty:
            return []

        for _, row in df.iterrows():
            cli = {
                "ruc": row.get("ruc"),
                "razon_social": row.get("razon_social"),
            }
            cli["source_hash"] = self._make_hash(cli)
            registros.append(cli)

        return registros

    # --------------------------------------------------------
    # API estándar para pipelines → ce.run()
    # --------------------------------------------------------
    def run(self) -> list[dict]:
        """
        1) Extrae clientes como DataFrame.
        2) Convierte a registros list[dict] listos para ClientsWriter.
        """
        df = self.extract()

        if df.empty:
            warn("[ClientsExtractor] No hay clientes extraídos.")
            return []

        registros = self._df_to_records(df)
        ok(f"[ClientsExtractor] Registros listos para carga: {len(registros)}")

        return registros
