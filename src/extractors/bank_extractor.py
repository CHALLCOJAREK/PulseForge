# src/extractors/bank_extractor.py
from __future__ import annotations

# ------------------------------------------------------------
# Bootstrap rutas
# ------------------------------------------------------------
import sys
from pathlib import Path
import hashlib  # 👈 nuevo, para generar source_hash

ROOT = Path(__file__).resolve().parents[2]     # C:/Proyectos/PulseForge
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ------------------------------------------------------------
# Imports principales
# ------------------------------------------------------------
from typing import Optional, Dict, List
import pandas as pd

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config
from src.core.db import SourceDB
from src.core.utils import clean_amount, normalize_text


class BankExtractor:
    """
    Extractor de movimientos bancarios basado en configuración dinámica.

    - Soporta:
        • Una tabla única de movimientos (tabla_movimientos_unica)
        • Varias tablas por banco (tablas_bancos)
    - Columnas detectadas desde settings.json → columnas_bancos
    """

    def __init__(self) -> None:
        info("Inicializando BankExtractor…")

        cfg = get_config()

        # Conexión a BD origen (DataPulse)
        self._db = SourceDB()

        # Config bancaria desde settings.json
        self.tabla_unica: str = cfg.tabla_movimientos_unica or ""
        self.tablas_bancos: Dict[str, str] = cfg.tablas_bancos or {}
        self.cols_bank: Dict[str, List[str]] = cfg.columnas_bancos or {}

        if not self.tabla_unica and not self.tablas_bancos:
            error("No hay configuración de bancos en settings.json")
            raise KeyError("Config bancos no encontrada.")

        if not self.cols_bank:
            warn("columnas_bancos vacío en settings.json → se devolverán columnas sin mapear.")

        # Log de configuración para trazabilidad
        if self.tabla_unica:
            info(f"Tabla única de movimientos configurada → {self.tabla_unica}")
        else:
            info("Modo multi-tablas bancarias activado:")
            for codigo, tabla in self.tablas_bancos.items():
                info(f"   - {codigo}: {tabla}")

        ok("BankExtractor listo.")

    # --------------------------------------------------------
    # Helpers internos
    # --------------------------------------------------------
    @staticmethod
    def _normalize_name(name: str) -> str:
        if not isinstance(name, str):
            return ""
        return name.strip().lower().replace(" ", "").replace("_", "")

    def _pick_column(self, df: pd.DataFrame, posibles: List[str]) -> Optional[str]:
        if df.empty or not posibles:
            return None

        posibles_norm = [self._normalize_name(p) for p in posibles]
        cols_norm = {col: self._normalize_name(col) for col in df.columns}

        # 1) Match exacto normalizado
        for col, col_norm in cols_norm.items():
            if col_norm in posibles_norm:
                return col

        # 2) Alias contenido en nombre
        for col, col_norm in cols_norm.items():
            if any(alias in col_norm for alias in posibles_norm):
                return col

        # 3) Nombre de columna contenido en alias
        for col, col_norm in cols_norm.items():
            if any(col_norm in alias for alias in posibles_norm):
                return col

        return None

    # --------------------------------------------------------
    # LECTURA DE TABLA ORIGEN
    # --------------------------------------------------------
    def _read_table(self, table_name: str) -> pd.DataFrame:
        try:
            query = f'SELECT * FROM "{table_name}"'
            df = self._db.read_query(query)

            if df.empty:
                warn(f"Tabla de banco vacía → {table_name}")

            return df

        except Exception as e:
            warn(f"No se pudo leer tabla '{table_name}': {e}")
            return pd.DataFrame()

    # --------------------------------------------------------
    # PROCESAR Y NORMALIZAR UNA TABLA
    # --------------------------------------------------------
    def _process_table(self, df_raw: pd.DataFrame, codigo_banco: str) -> pd.DataFrame:
        if df_raw.empty:
            return pd.DataFrame()

        df_norm = pd.DataFrame()

        # Mapear columnas mediante configuración
        for campo, opciones in self.cols_bank.items():
            col = self._pick_column(df_raw, opciones)

            if not col:
                warn(f"[{codigo_banco}] Columna no encontrada → {campo} (opciones: {opciones})")
                df_norm[campo] = None
                continue

            df_norm[campo] = df_raw[col]

        # Normalizar monto
        if "monto" in df_norm.columns:
            df_norm["monto"] = df_norm["monto"].apply(clean_amount)

        # Normalizar textos
        for campo in ["descripcion", "tipo_mov", "destinatario", "tipo_documento"]:
            if campo in df_norm.columns:
                df_norm[campo] = df_norm[campo].astype(str).apply(normalize_text)

        df_norm["banco_codigo"] = codigo_banco
        ok(f"[{codigo_banco}] Movimientos normalizados: {len(df_norm)}")

        return df_norm

    # --------------------------------------------------------
    # MÉTODO PRINCIPAL DE EXTRACCIÓN → DEVUELVE DATAFRAME
    # --------------------------------------------------------
    def extract(self) -> pd.DataFrame:
        movimientos: List[pd.DataFrame] = []

        # --- Tabla única ---
        if self.tabla_unica:
            df_raw = self._read_table(self.tabla_unica)

            if not df_raw.empty and self.cols_bank:
                df_norm = self._process_table(df_raw, "GENERAL")
                movimientos.append(df_norm)
            elif not df_raw.empty:
                df_raw["banco_codigo"] = "GENERAL"
                movimientos.append(df_raw)

            if not movimientos:
                warn("No se encontraron movimientos bancarios en tabla única.")
                return pd.DataFrame()

            df_final = pd.concat(movimientos, ignore_index=True)
            ok(f"TOTAL movimientos extraídos (tabla única): {len(df_final)}")
            return df_final

        # --- Múltiples tablas por banco ---
        for codigo, tabla in self.tablas_bancos.items():
            df_raw = self._read_table(tabla)
            if df_raw.empty:
                continue

            df_norm = (
                self._process_table(df_raw, codigo)
                if self.cols_bank else df_raw.assign(banco_codigo=codigo)
            )

            if not df_norm.empty:
                movimientos.append(df_norm)

        if not movimientos:
            warn("No se encontraron movimientos bancarios en ninguna tabla configurada.")
            return pd.DataFrame()

        df_final = pd.concat(movimientos, ignore_index=True)
        ok(f"TOTAL movimientos extraídos (multi-banco): {len(df_final)}")
        return df_final

    # --------------------------------------------------------
    # HASH INTERNO PARA CADA MOVIMIENTO
    # --------------------------------------------------------
    @staticmethod
    def _make_hash(mov: dict) -> str:
        try:
            base = "|".join(str(mov.get(k, "")) for k in sorted(mov.keys()))
            return hashlib.sha256(base.encode("utf-8")).hexdigest()
        except Exception:
            return ""

    # --------------------------------------------------------
    # CONVERTIR DF → LISTA DE DICCS PARA LOS WRITERS
    # --------------------------------------------------------
    def _df_to_records(self, df: pd.DataFrame) -> list[dict]:
        registros: list[dict] = []

        if df is None or df.empty:
            return registros

        for _, row in df.iterrows():
            mov = {
                "fecha": row.get("fecha"),
                "tipo_mov": row.get("tipo_mov"),
                "descripcion": row.get("descripcion"),
                "operacion": row.get("operacion"),
                "destinatario": row.get("destinatario"),
                "tipo_documento": row.get("tipo_documento"),
                "monto": row.get("monto"),
                "moneda": row.get("moneda"),
                "banco_codigo": row.get("banco_codigo"),
            }

            # Hash único por movimiento (clave técnica)
            mov["source_hash"] = self._make_hash(mov)
            registros.append(mov)

        return registros

    # --------------------------------------------------------
    # INTERFAZ ESTÁNDAR PARA PIPELINES: be.run()
    # --------------------------------------------------------
    def run(self) -> list[dict]:
        """
        1) Extrae todos los movimientos en un DataFrame normalizado.
        2) Convierte a lista de diccionarios con source_hash.
        3) Devuelve listo para BankWriter.save_many().
        """
        df = self.extract()

        if df is None or df.empty:
            warn("[BankExtractor] No hay movimientos bancarios extraídos.")
            return []

        registros = self._df_to_records(df)
        ok(f"[BankExtractor] Registros preparados para carga: {len(registros)}")
        return registros
