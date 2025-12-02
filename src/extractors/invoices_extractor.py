# src/extractors/invoices_extractor.py
from __future__ import annotations

# ============================================================
#  EXTRACTOR DE FACTURAS · PULSEFORGE · SQLITE (FINAL)
# ============================================================
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


# ------------------------------------------------------------
def _get_sqlite_connection() -> sqlite3.Connection:
    db_path = str(get_env("PULSEFORGE_DB_PATH")).strip()
    if not Path(db_path).exists():
        error(f"No existe BD origen: {db_path}")
        raise FileNotFoundError(db_path)

    info(f"Conectando a SQLite → {db_path}")
    return sqlite3.connect(db_path)


# ============================================================
#  EXTRACTOR
# ============================================================
class InvoicesExtractor:

    def __init__(self):
        info("Inicializando InvoicesExtractor…")
        self.mapper = DataMapper()

        tablas_cfg = self.mapper.settings.get("tablas", {})
        self._tabla = tablas_cfg.get("facturas")

        if not self._tabla:
            error("Falta configuración de tabla facturas en settings.json")
            raise KeyError("tabla facturas")

        ok(f"InvoicesExtractor listo. Tabla = '{self._tabla}'")

    # --------------------------------------------------------
    def _load_raw(self) -> pd.DataFrame:
        conn = _get_sqlite_connection()
        try:
            info(f"Leyendo facturas desde '{self._tabla}'…")
            df = pd.read_sql_query(f'SELECT * FROM "{self._tabla}"', conn)
            ok(f"Facturas crudas leídas: {len(df)} filas.")
            return df
        finally:
            conn.close()

    # --------------------------------------------------------
    def _normalize(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        settings_cols = self.mapper.settings.get("columnas_facturas", {})
        df_norm = pd.DataFrame()

        # COPIAR COLUMNAS REALES
        for key_std, col_real in settings_cols.items():
            if col_real not in df_raw.columns:
                warn(f"[FACTURAS] Columna '{col_real}' no existe → vacía.")
                df_norm[key_std] = None
            else:
                df_norm[key_std] = df_raw[col_real]

        # RENOMBRAR EXACTO PARA DATAMAPPER
        rename_map = {
            "ruc": "RUC",
            "cliente_generador": "Cliente_Generador",
            "subtotal": "Subtotal",
            "serie": "Serie",
            "numero": "Numero",
            "combinada": "Combinada",
            "estado_fs": "Estado_FS",
            "estado_cont": "Estado_Cont",
            "fecha_emision": "Fecha_Emision",
            "forma_pago": "Forma_Pago",
            "vencimiento": "Vencimiento",
        }

        df_norm = df_norm.rename(columns=rename_map)

        ok("Facturas normalizadas y renombradas correctamente.")
        return df_norm

    # --------------------------------------------------------
    def get_facturas_mapeadas(self) -> pd.DataFrame:
        df_raw = self._load_raw()
        df_norm = self._normalize(df_raw)
        df_mapped = self.mapper.map_facturas(df_norm)
        ok(f"Facturas mapeadas OK: {len(df_mapped)} filas.")
        return df_mapped


# ============================================================
if __name__ == "__main__":
    try:
        extractor = InvoicesExtractor()
        df = extractor.get_facturas_mapeadas()
        print(df.head())
        ok("Test de facturas completado correctamente.")
    except Exception as e:
        error(f"Fallo en test de InvoicesExtractor: {e}")
