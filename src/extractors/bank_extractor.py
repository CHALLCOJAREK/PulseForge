# src/extractors/bank_extractor.py
from __future__ import annotations

# ============================================================
#  EXTRACTOR DE BANCOS · PULSEFORGE · SQLITE (VERSIÓN PRO)
# ============================================================
import sys
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

# ------------------------------------------------------------
#  BOOTSTRAP RUTAS
# ------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env
from src.transformers.data_mapper import DataMapper


# ============================================================
#  CONEXIÓN SQLITE
# ============================================================
def _get_sqlite_connection() -> sqlite3.Connection:
    db_type = str(get_env("PULSEFORGE_DB_TYPE", default="sqlite")).strip().lower()
    if db_type != "sqlite":
        error(f"PULSEFORGE_DB_TYPE='{db_type}' no soportado en BankExtractor.")
        raise ValueError("Solo se soporta SQLite por ahora en BankExtractor.")

    db_path = str(get_env("PULSEFORGE_DB_PATH")).strip()
    if not db_path:
        error("PULSEFORGE_DB_PATH no configurado en .env")
        raise ValueError("Falta PULSEFORGE_DB_PATH en .env")

    db_file = Path(db_path)
    if not db_file.exists():
        error(f"No existe la BD origen: {db_file}")
        raise FileNotFoundError(db_file)

    info(f"Conectando a BD origen SQLite → {db_file}")
    return sqlite3.connect(db_file)


# ============================================================
#  EXTRACTOR DE BANCOS
# ============================================================
class BankExtractor:

    def __init__(self) -> None:
        info("Inicializando BankExtractor…")
        self.mapper = DataMapper()

        tablas_cfg: Dict[str, str] = self.mapper.settings.get("tablas", {})
        if not tablas_cfg:
            error("No se encontraron 'tablas' en settings.json.")
            raise KeyError("settings.tablas")

        # Filtrar solo las tablas de bancos (las que comienzan con 'banco_')
        self._bank_tables: Dict[str, str] = {
            key: value
            for key, value in tablas_cfg.items()
            if key.startswith("banco_")
        }

        if not self._bank_tables:
            warn("No se definieron tablas de bancos en settings.json (prefijo 'banco_').")

        ok(f"BankExtractor listo. Tablas bancos detectadas: {list(self._bank_tables.keys())}")

        # Configuración de columnas de bancos desde settings.json
        self._col_cfg: Dict[str, str] = self.mapper.settings.get("columnas_bancos", {})

    # --------------------------------------------------------
    #  Mapeo nombre lógico → código de banco
    # --------------------------------------------------------
    @staticmethod
    def _banco_code_from_key(key: str) -> str:
        """
        Convierte la clave de settings (banco_nacion, banco_bcp_soles, etc.)
        en un código compacto que usará el matcher (BN, BCP, IBK, etc.).
        """
        k = key.lower()

        if "nacion" in k:
            return "BN"
        if "bbva" in k:
            return "BBVA"
        if "bcp" in k:
            return "BCP"
        if "interbank" in k or "interb" in k or "ibk" in k:
            return "IBK"
        if "arequipa" in k:
            return "AREQUIPA"
        if "finanzas" in k:
            return "FINANZAS"

        # Fallback genérico
        return key.upper()

    # --------------------------------------------------------
    #  Helper para buscar columna "parecida" cuando la de settings
    #  no existe en la tabla real (ej: n_operacion vs num_operacion)
    # --------------------------------------------------------
    @staticmethod
    def _fallback_column(logical_key: str,
                         hint: Optional[str],
                         cols: List[str]) -> Optional[str]:
        cols_lower = {c.lower(): c for c in cols}

        # Si el hint viene y existe exacto (case-insensitive)
        if hint:
            h = hint.lower()
            if h in cols_lower:
                return cols_lower[h]

        # Fallbacks específicos por tipo de campo
        if logical_key == "operacion":
            for cand in ["n_operacion", "num_operacion", "nro_operacion", "operacion", "referencia"]:
                if cand in cols_lower:
                    return cols_lower[cand]

        if logical_key == "descripcion":
            for cand in ["descripcion_actividad", "descripcion", "descripción", "glosa", "detalle", "concepto"]:
                if cand in cols_lower:
                    return cols_lower[cand]

        if logical_key == "tipo_mov":
            for cand in ["tipo_mov", "tipo_movimiento"]:
                if cand in cols_lower:
                    return cols_lower[cand]

        if logical_key == "destinatario":
            for cand in ["destinatario", "beneficiario", "cliente"]:
                if cand in cols_lower:
                    return cols_lower[cand]

        if logical_key == "tipo_documento":
            for cand in ["tipo_documento", "tdoc", "tipo_doc"]:
                if cand in cols_lower:
                    return cols_lower[cand]

        if logical_key == "monto":
            for cand in ["monto", "abono", "importe", "monto_total"]:
                if cand in cols_lower:
                    return cols_lower[cand]

        if logical_key == "moneda":
            for cand in ["moneda", "divisa"]:
                if cand in cols_lower:
                    return cols_lower[cand]

        # Si no se encontró nada
        return None

    # --------------------------------------------------------
    #  Normalización de una tabla de banco específica
    # --------------------------------------------------------
    def _normalize_single_bank(self, df_raw: pd.DataFrame, banco_code: str) -> pd.DataFrame:
        """
        A partir del DataFrame crudo de UNA tabla de banco:
          - Asigna código de banco en columna 'Banco'
          - Crea columnas estándar esperadas por DataMapper.map_bancos:
              Banco, Descripcion, Monto, Moneda,
              Operacion, Tipo_Mov, Destinatario, Tipo_Documento
          - Conserva 'fecha' y demás columnas crudas para que el mapper
            pueda detectar la columna de fecha automáticamente.
        """

        df = df_raw.copy()

        # Marcar banco
        df["Banco"] = banco_code

        # Config settings columnas_bancos (puede venir vacío o parcial)
        cfg = self._col_cfg or {}

        # Mapeo lógico → nombre interno que espera DataMapper.map_bancos
        target_names = {
            "descripcion": "Descripcion",
            "monto": "Monto",
            "moneda": "Moneda",
            "operacion": "Operacion",
            "tipo_mov": "Tipo_Mov",
            "destinatario": "Destinatario",
            "tipo_documento": "Tipo_Documento",
        }

        for logical_key, target_col in target_names.items():
            hint_col = cfg.get(logical_key)  # nombre de columna sugerido por settings.json
            chosen_col = None

            if hint_col and hint_col in df.columns:
                chosen_col = hint_col
            else:
                chosen_col = self._fallback_column(logical_key, hint_col, list(df.columns))

            if not chosen_col:
                warn(f"[BANCOS] No se encontró columna para '{logical_key}' en banco '{banco_code}'. "
                     f"Columna '{target_col}' quedará vacía.")
                df[target_col] = ""
                continue

            # Copiar valor a la columna estándar
            df[target_col] = df[chosen_col]

        return df

    # --------------------------------------------------------
    #  LECTURA Y NORMALIZACIÓN GLOBAL DE TODOS LOS BANCOS
    # --------------------------------------------------------
    def get_bancos_mapeados(self) -> pd.DataFrame:
        """
        Lee todas las tablas de bancos configuradas, las normaliza
        y luego aplica DataMapper.map_bancos() a cada una.
        Devuelve un único DataFrame estándar para todo PulseForge.
        """
        if not self._bank_tables:
            warn("No hay tablas de bancos configuradas. Retornando DF vacío.")
            return pd.DataFrame()

        conn = _get_sqlite_connection()
        df_all: List[pd.DataFrame] = []

        try:
            for key, table in self._bank_tables.items():
                banco_code = self._banco_code_from_key(key)
                info(f"Leyendo movimientos de banco '{banco_code}' desde tabla '{table}'…")

                try:
                    df_raw = pd.read_sql_query(f'SELECT * FROM "{table}"', conn)
                except Exception as e:
                    error(f"Error leyendo tabla '{table}': {e}")
                    continue

                if df_raw.empty:
                    warn(f"Tabla '{table}' vacía. Se omite.")
                    continue

                ok(f"Movimientos crudos leídos de '{table}': {len(df_raw)} filas.")

                # Normalizar crudo → columnas estándar internas
                df_norm = self._normalize_single_bank(df_raw, banco_code)

                # Pasar por DataMapper.map_bancos para esquema final
                try:
                    df_mapped = self.mapper.map_bancos(df_norm)
                except Exception as e:
                    error(f"Error en map_bancos para '{table}': {e}")
                    continue

                df_all.append(df_mapped)
                ok(f"Movimientos normalizados para banco '{banco_code}': {len(df_mapped)} filas.")

        finally:
            conn.close()

        if not df_all:
            warn("No se generaron movimientos bancarios normalizados. Retornando DF vacío.")
            return pd.DataFrame()

        df_final = pd.concat(df_all, ignore_index=True)
        ok(f"Total movimientos bancarios normalizados (todos los bancos): {len(df_final)} registros.")

        return df_final


# ============================================================
#  TEST LOCAL
# ============================================================
if __name__ == "__main__":
    try:
        be = BankExtractor()
        df_bancos = be.get_bancos_mapeados()

        print(df_bancos.head())
        print("\nResumen por banco:")
        if not df_bancos.empty:
            print(df_bancos.groupby("Banco")["Monto"].count())

        ok("Test rápido de BankExtractor completado.")
    except Exception as e:
        error(f"Fallo en test de BankExtractor: {e}")
