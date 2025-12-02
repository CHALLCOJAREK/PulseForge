# src/extractors/clients_extractor.py
from __future__ import annotations

# ============================================================
#  EXTRACTOR DE CLIENTES · PULSEFORGE · SQLITE
# ============================================================
import sys
import sqlite3
from pathlib import Path
from typing import Optional

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
#  HELPERS DE CONEXIÓN
# ============================================================
def _get_sqlite_connection() -> sqlite3.Connection:
    """
    Abre conexión SQLite a la BD origen indicada en PULSEFORGE_DB_PATH.
    Solo soporta tipo 'sqlite' (PULSEFORGE_DB_TYPE).
    """
    db_type = str(get_env("PULSEFORGE_DB_TYPE", default="sqlite")).strip().lower()
    if db_type != "sqlite":
        error(f"PULSEFORGE_DB_TYPE='{db_type}' no soportado. Solo 'sqlite' por ahora.")
        raise ValueError("Tipo de base de datos no soportado. Use 'sqlite'.")

    db_path = str(get_env("PULSEFORGE_DB_PATH")).strip()
    if not db_path:
        error("PULSEFORGE_DB_PATH no configurado en .env")
        raise ValueError("Falta PULSEFORGE_DB_PATH en configuración.")

    db_file = Path(db_path)
    if not db_file.exists():
        error(f"Base de datos origen no encontrada: {db_file}")
        raise FileNotFoundError(f"No existe la BD origen: {db_file}")

    info(f"Conectando a BD origen SQLite → {db_file}")
    return sqlite3.connect(db_file)


# ============================================================
#  EXTRACTOR DE CLIENTES
# ============================================================
class ClientsExtractor:
    """
    Extrae clientes desde la BD origen (DataPulse) y devuelve un
    DataFrame normalizado con columnas estándar para PulseForge:

        ['RUC', 'Razon_Social']

    Luego DataMapper termina de pulir el formato.
    """

    def __init__(self) -> None:
        info("Inicializando ClientsExtractor…")
        self.mapper = DataMapper()
        self._tabla_clientes = self._resolve_tabla_clientes()
        ok(f"ClientsExtractor listo. Tabla clientes = '{self._tabla_clientes}'")

    # --------------------------------------------------------
    #  RESOLVER NOMBRE DE TABLA DESDE settings.json
    # --------------------------------------------------------
    def _resolve_tabla_clientes(self) -> str:
        tablas_cfg = self.mapper.settings.get("tablas", {})
        tabla = tablas_cfg.get("clientes")

        if not tabla:
            error("No se encontró 'clientes' dentro de settings['tablas'].")
            raise KeyError("Falta configuración de tabla 'clientes' en settings.json")

        return str(tabla)

    # --------------------------------------------------------
    #  LECTURA CRUDA DESDE SQLITE
    # --------------------------------------------------------
    def _load_raw_clientes(self) -> pd.DataFrame:
        """
        Lee la tabla de clientes cruda desde SQLite.
        """
        conn = _get_sqlite_connection()
        try:
            info(f"Leyendo clientes desde tabla SQLite '{self._tabla_clientes}'…")
            query = f'SELECT * FROM "{self._tabla_clientes}"'
            df = pd.read_sql_query(query, conn)

            if df.empty:
                warn("La tabla de clientes está vacía.")
            else:
                ok(f"Clientes crudos leídos: {len(df)} filas.")

            return df

        except Exception as e:
            error(f"Error leyendo tabla de clientes '{self._tabla_clientes}': {e}")
            raise
        finally:
            conn.close()

    # --------------------------------------------------------
    #  NORMALIZAR COLUMNAS (RUC / Razon_Social)
    # --------------------------------------------------------
    @staticmethod
    def _normalize_clientes_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Intenta detectar de forma robusta las columnas de RUC y Razón Social
        y las normaliza a:

            'RUC'         → texto, sin espacios
            'Razon_Social'→ texto, sin espacios

        Esta función NO usa IA, aquí todo es determinista.
        La IA se usa después en el matcher para similitudes.
        """
        if df.empty:
            warn("DataFrame de clientes vacío en normalización.")
            return pd.DataFrame(columns=["RUC", "Razon_Social"])

        df_norm = df.copy()
        cols_lower = {c.lower().strip(): c for c in df_norm.columns}

        # -------- Detectar columna de RUC --------
        candidatos_ruc = [
            "ruc", "ruc_cliente", "num_doc", "nro_doc", "numero_documento",
            "doc", "nrodocumento"
        ]
        col_ruc: Optional[str] = None

        for k in candidatos_ruc:
            if k in cols_lower:
                col_ruc = cols_lower[k]
                break

        if not col_ruc:
            for c in df_norm.columns:
                if "ruc" in c.lower():
                    col_ruc = c
                    break

        if not col_ruc:
            error("No se pudo identificar columna RUC en clientes.")
            raise KeyError("No se encontró columna de RUC en clientes.")

        # -------- Detectar columna de Razón Social --------
        candidatos_rs = [
            "razon_social", "razón_social", "razon social",
            "nombre_razon_social", "nombre / razón social",
            "nombre", "cliente", "proveedor"
        ]
        col_rs: Optional[str] = None

        for k in candidatos_rs:
            if k in cols_lower:
                col_rs = cols_lower[k]
                break

        if not col_rs:
            for c in df_norm.columns:
                cl = c.lower()
                if "razon" in cl or "razón" in cl or "nombre" in cl:
                    col_rs = c
                    break

        if not col_rs:
            error("No se pudo identificar columna Razón Social en clientes.")
            raise KeyError("No se encontró columna de Razón Social en clientes.")

        info(f"Columna RUC detectada → '{col_ruc}'")
        info(f"Columna Razón Social detectada → '{col_rs}'")

        out = pd.DataFrame()
        out["RUC"] = df_norm[col_ruc].astype(str).str.strip()
        out["Razon_Social"] = df_norm[col_rs].astype(str).str.strip()

        ok("Clientes normalizados a esquema estándar (RUC / Razon_Social).")
        return out

    # --------------------------------------------------------
    #  API PÚBLICA
    # --------------------------------------------------------
    def get_clientes_mapeados(self) -> pd.DataFrame:
        """
        Devuelve un DataFrame ya normalizado por DataMapper:

            ['RUC', 'Razon_Social']

        Listo para integrarse con el resto de PulseForge.
        """
        df_raw = self._load_raw_clientes()
        df_norm = self._normalize_clientes_columns(df_raw)
        df_mapped = self.mapper.map_clientes(df_norm)

        ok(f"Clientes mapeados OK: {len(df_mapped)} registros.")
        return df_mapped


# ============================================================
#  TEST LOCAL RÁPIDO
# ============================================================
if __name__ == "__main__":
    try:
        ce = ClientsExtractor()
        df_cli = ce.get_clientes_mapeados()
        print(df_cli.head())
        ok("Test rápido de ClientsExtractor completado.")
    except Exception as e:
        error(f"Fallo en test de ClientsExtractor: {e}")
