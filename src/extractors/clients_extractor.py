# src/extractors/clients_extractor.py
from __future__ import annotations

# --- BOOTSTRAP RUTAS ---
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# --- IMPORTS CORE ---
import pandas as pd
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config
from src.core.db import SourceDB
from src.core.utils import normalize_text, clean_ruc


# ============================================================
#   EXTRACTOR UNIVERSAL DE CLIENTES (SOLO LECTURA)
#   - Lee tabla de clientes desde BD origen (DataPulse)
#   - Aplica normalización mínima (RUC + razón social)
#   - NO mapea al modelo final (eso es trabajo de transformers)
# ============================================================
class ClientsExtractor:

    def __init__(self) -> None:
        info("Inicializando ClientsExtractor...")

        cfg = get_config()
        self._cfg = cfg
        self._tabla_clientes = cfg.tablas.get("clientes")

        if not self._tabla_clientes:
            warn("settings.json no tiene 'clientes' en 'tablas'. El extractor devolverá DF vacío.")
            self._tabla_clientes = None
        else:
            ok(f"Tabla de clientes configurada → {self._tabla_clientes}")

        self._db = SourceDB()

    # --------------------------------------------------------
    #   DETECCIÓN DINÁMICA DE COLUMNAS
    # --------------------------------------------------------
    @staticmethod
    def _pick_column(df: pd.DataFrame, candidates: list[str], fallback_index: int | None = None) -> str | None:
        """
        Elige una columna buscando por palabras clave en el nombre.
        Si no encuentra, usa fallback por índice (si se da).
        """
        cols_lower = {c.lower(): c for c in df.columns}

        for pattern in candidates:
            for low, real in cols_lower.items():
                if pattern in low:
                    return real

        if fallback_index is not None and 0 <= fallback_index < len(df.columns):
            return df.columns[fallback_index]

        return None

    # --------------------------------------------------------
    #   LECTURA CRUDA DESDE BD ORIGEN
    # --------------------------------------------------------
    def _load_raw(self) -> pd.DataFrame:
        """Lee la tabla de clientes tal cual existe en DataPulse."""
        if not self._tabla_clientes:
            return pd.DataFrame()

        try:
            self._db.connect()
            query = f'SELECT * FROM "{self._tabla_clientes}"'
            df = pd.read_sql_query(query, self._db.connection)

            if df.empty:
                warn(f"Tabla de clientes '{self._tabla_clientes}' está vacía en BD origen.")
            else:
                ok(f"Clientes crudos cargados desde '{self._tabla_clientes}': {len(df)} filas.")

            return df

        except Exception as e:
            error(f"Error leyendo clientes desde '{self._tabla_clientes}': {e}")
            return pd.DataFrame()

        finally:
            self._db.close()

    # --------------------------------------------------------
    #   EXTRACTOR PRINCIPAL
    # --------------------------------------------------------
    def extract(self) -> pd.DataFrame:
        """
        Extrae clientes desde la BD origen y devuelve un DataFrame
        normalizado mínimamente con:
            - ruc
            - razon_social

        El mapeo al modelo final (campos adicionales, hashes, etc.)
        se hará en el módulo transformers/DataMapper.
        """
        df_raw = self._load_raw()
        if df_raw.empty:
            warn("ClientsExtractor.extract() → DataFrame vacío.")
            return pd.DataFrame(columns=["ruc", "razon_social"])

        # Detectar columnas dinámicamente
        col_ruc = self._pick_column(
            df_raw,
            candidates=["ruc", "documento", "doc"],
            fallback_index=0
        )
        col_nombre = self._pick_column(
            df_raw,
            candidates=["razon", "cliente", "nombre", "name"],
            fallback_index=1 if len(df_raw.columns) > 1 else 0
        )

        if not col_ruc:
            warn("No se encontró columna de RUC en clientes. Se aborta extractor.")
            return pd.DataFrame(columns=["ruc", "razon_social"])

        if not col_nombre:
            warn("No se encontró columna de nombre/razón social en clientes. Se aborta extractor.")
            return pd.DataFrame(columns=["ruc", "razon_social"])

        ok(f"Columna RUC detectada → {col_ruc}")
        ok(f"Columna nombre detectada → {col_nombre}")

        # Normalización mínima
        clientes = pd.DataFrame()
        clientes["ruc"] = df_raw[col_ruc].astype(str).apply(clean_ruc)
        clientes["razon_social"] = df_raw[col_nombre].astype(str).apply(normalize_text)

        # Limpieza
        before = len(clientes)
        clientes = clientes[clientes["ruc"] != ""]
        clientes = clientes.drop_duplicates(subset=["ruc"])
        after = len(clientes)

        ok(f"Clientes normalizados: {after} registros (filtrados {before - after}).")

        return clientes