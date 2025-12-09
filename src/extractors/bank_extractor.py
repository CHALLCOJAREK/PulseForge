# src/extractors/bank_extractor.py
from __future__ import annotations

# -------------------------
# Bootstrap interno
# -------------------------
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# -------------------------
# Imports principales
# -------------------------
import pandas as pd
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config
from src.core.db import SourceDB
from src.core.utils import clean_amount, normalize_text


# ============================================================
#   EXTRACTOR UNIVERSAL DE MOVIMIENTOS BANCARIOS
#   - Soporta 1 tabla única o múltiples tablas
#   - Columnas dinámicas detectadas desde settings.json
#   - No usa DataMapper (transformación mínima)
# ============================================================
class BankExtractor:

    def __init__(self):
        info("Inicializando BankExtractor…")

        cfg = get_config()

        # conexión a BD origen (DataPulse)
        self._db = SourceDB()

        # tabla única (opcional)
        self.tabla_unica = cfg.tabla_movimientos_unica

        # múltiples tablas de bancos
        self.tablas_bancos = cfg.bancos

        # columnas dinámicas desde settings.json
        self.cols_bank = cfg.columnas_bancos  

        if not self.tabla_unica and not self.tablas_bancos:
            error("No hay configuración bancaria en settings.json")
            raise KeyError("Config bancos no encontrada.")

        ok(f"Config bancos cargada → única={self.tabla_unica}, múltiples={self.tablas_bancos}")


    # --------------------------------------------------------
    #   DETECTOR UNIVERSAL DE COLUMNAS
    # --------------------------------------------------------
    @staticmethod
    def _pick(df: pd.DataFrame, posibles: list[str]) -> str | None:
        posibles = [p.lower() for p in posibles]

        for col in df.columns:
            col_low = col.lower()
            if any(tag in col_low for tag in posibles):
                return col
        return None


    # --------------------------------------------------------
    #   LECTURA DE TABLA ORIGEN
    # --------------------------------------------------------
    def _read_table(self, table_name: str) -> pd.DataFrame:
        try:
            self._db.connect()
            df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', self._db.connection)

            if df.empty:
                warn(f"Tabla de banco vacía → {table_name}")

            return df

        except Exception as e:
            warn(f"No se pudo leer tabla {table_name}: {e}")
            return pd.DataFrame()

        finally:
            self._db.close()


    # --------------------------------------------------------
    #   PROCESAR Y NORMALIZAR UNA TABLA
    # --------------------------------------------------------
    def _process_table(self, df_raw: pd.DataFrame, codigo_banco: str) -> pd.DataFrame:
        df = pd.DataFrame()

        # detectar cada columna requerida
        for campo, opciones in self.cols_bank.items():
            col = self._pick(df_raw, opciones)

            if not col:
                warn(f"[{codigo_banco}] Columna no encontrada → {campo}")
                df[campo] = None
                continue

            df[campo] = df_raw[col]

        # normalizar monto
        if "monto" in df.columns:
            df["monto"] = df["monto"].apply(clean_amount)

        # normalizar textos
        for campo in ["descripcion", "tipo_mov", "destinatario", "tipo_documento"]:
            if campo in df.columns:
                df[campo] = df[campo].astype(str).apply(normalize_text)

        df["banco_codigo"] = codigo_banco

        ok(f"[{codigo_banco}] Movimientos normalizados: {len(df)}")
        return df


    # --------------------------------------------------------
    #   MÉTODO PRINCIPAL DE EXTRACCIÓN
    # --------------------------------------------------------
    def extract(self) -> pd.DataFrame:
        movimientos = []

        # --- PRIORIDAD: TABLA ÚNICA ---
        if self.tabla_unica:
            df_raw = self._read_table(self.tabla_unica)

            if not df_raw.empty:
                df_norm = self._process_table(df_raw, "GENERAL")
                movimientos.append(df_norm)

            return pd.concat(movimientos, ignore_index=True) if movimientos else pd.DataFrame()

        # --- TABLAS MULTIPLES ---
        for codigo, tabla in self.tablas_bancos.items():
            df_raw = self._read_table(tabla)
            if df_raw.empty:
                continue

            df_norm = self._process_table(df_raw, codigo)
            movimientos.append(df_norm)

        if not movimientos:
            warn("No se encontraron movimientos bancarios en ninguna tabla.")
            return pd.DataFrame()

        df_final = pd.concat(movimientos, ignore_index=True)
        ok(f"TOTAL movimientos extraídos: {len(df_final)}")
        return df_final
    
