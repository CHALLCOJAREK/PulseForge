# src/extractors/bank_extractor.py

import os
import sys
import pandas as pd
from sqlalchemy import text

# Acceso al núcleo del sistema
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.core.db import get_db
from src.core.env_loader import get_env

# Prints Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class BankExtractor:

    def __init__(self):
        info("Inicializando extractor bancario…")

        self.env = get_env()
        self.db = get_db()

        # ========================
        # Load settings
        # ========================
        import json
        cfg_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../config"))

        settings_path = os.path.join(cfg_dir, "settings.json")
        constants_path = os.path.join(cfg_dir, "constants.json")

        if not os.path.exists(settings_path):
            error(f"settings.json no encontrado en {settings_path}")
            raise FileNotFoundError()

        with open(settings_path, "r", encoding="utf-8") as f:
            self.settings = json.load(f)

        # constants.json opcional
        if os.path.exists(constants_path):
            with open(constants_path, "r", encoding="utf-8") as f:
                self.constants = json.load(f)
        else:
            self.constants = {"considerar_montos_cero": False}

        self.col = self.settings["columnas_bancos"]

        ok("Extractor bancario listo.")


    # =======================================
    # Normalizador flexible de nombres
    # =======================================
    @staticmethod
    def _norm(s):
        return str(s).lower().replace(" ", "").replace("_", "").strip()

    def _find_col(self, df, configured_name):
        """
        Permite buscar columnas aunque vengan con:
        - mayúsculas/minúsculas
        - espacios
        - guiones bajos
        - variaciones leves
        """
        if not configured_name:
            return None

        target = self._norm(configured_name)
        mapa = {self._norm(c): c for c in df.columns}

        if target in mapa:
            return mapa[target]

        # búsqueda parcial
        for norm, real in mapa.items():
            if target in norm or norm in target:
                return real

        return None


    # =======================================
    # Cargar tabla y normalizarla
    # =======================================
    def _load_table(self, table_name, alias, moneda_fija):
        info(f"Cargando banco {alias} desde {table_name}")

        try:
            df_raw = pd.read_sql(text(f"SELECT rowid, * FROM {table_name}"), self.db.engine_origen)
        except Exception as e:
            error(f"Error leyendo {table_name}: {e}")
            return pd.DataFrame()

        if df_raw.empty:
            warn(f"Tabla {table_name} vacía.")
            return pd.DataFrame()

        # Leer columnas usando nombres configurados
        fecha        = self._find_col(df_raw, self.col.get("fecha"))
        tipo_mov     = self._find_col(df_raw, self.col.get("tipo_mov"))
        descripcion  = self._find_col(df_raw, self.col.get("descripcion"))
        monto        = self._find_col(df_raw, self.col.get("monto"))
        operacion    = self._find_col(df_raw, self.col.get("operacion"))
        destinatario = self._find_col(df_raw, self.col.get("destinatario"))
        tipo_doc     = self._find_col(df_raw, self.col.get("tipo_documento"))

        if not fecha or not monto:
            warn(f"{table_name}: faltan columnas mínimas.")
            return pd.DataFrame()

        df = pd.DataFrame()
        df["Banco"]         = alias
        df["source_table"]  = table_name
        df["origen_rowid"]  = df_raw["rowid"]

        # Fecha
        df["Fecha"] = pd.to_datetime(df_raw[fecha], errors="coerce")

        # Tipo de movimiento
        if tipo_mov:
            df["Tipo_Mov"] = df_raw[tipo_mov].astype(str).str.upper().str.strip()
        else:
            df["Tipo_Mov"] = ""

        # Descripción
        df["Descripcion"] = df_raw[descripcion].astype(str).str.strip() if descripcion else ""

        # Monto (parse robusto)
        df["Monto"] = (
            df_raw[monto]
            .astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
        ).astype(float)

        # Moneda fija por tabla
        df["Moneda"] = moneda_fija

        # N° operación
        df["Operacion"] = df_raw[operacion].astype(str).str.strip() if operacion else ""

        # Destinatario
        df["Destinatario"] = df_raw[destinatario].astype(str).str.strip() if destinatario else ""

        # Tipo de documento
        df["Tipo_Documento"] = (
            df_raw[tipo_doc].astype(str).str.upper().str.strip()
            if tipo_doc else ""
        )

        # Filtrar montos cero (según constants.json)
        if not self.constants.get("considerar_montos_cero", False):
            df = df[df["Monto"] != 0]

        ok(f"Movimientos validados: {len(df)}")
        return df


    # =======================================
    # Unificar TODOS los bancos
    # =======================================
    def get_all(self):
        info("Unificando movimientos bancarios…")

        t = self.settings["tablas"]

        bancos_cfg = [
            ("banco_nacion",          "BN",      "PEN"),
            ("banco_bbva_soles",      "BBVA-S",  "PEN"),
            ("banco_bcp_dolares",     "BCP-USD", "USD"),
            ("banco_bcp_soles",       "BCP-S",   "PEN"),
            ("banco_interbank_soles", "IBK-S",   "PEN"),
            ("banco_arequipa_soles",  "ARE-S",   "PEN"),
            ("banco_finanzas_soles",  "FIN-S",   "PEN"),
        ]

        dfs = []
        for key, alias, moneda in bancos_cfg:
            tbl = t.get(key)
            if not tbl:
                warn(f"{key} no está configurado en settings.json")
                continue

            df = self._load_table(tbl, alias, moneda)
            if not df.empty:
                dfs.append(df)

        if not dfs:
            warn("No se encontró ningún movimiento bancario.")
            return pd.DataFrame()

        df_final = pd.concat(dfs, ignore_index=True)
        df_final = df_final.sort_values(by=["Fecha", "Banco", "Monto"])

        ok(f"Total movimientos unificados: {len(df_final)}")
        return df_final


# =======================================
# Test manual rápido
# =======================================
if __name__ == "__main__":
    info("🔧 Test extractor bancario")
    ex = BankExtractor()
    df = ex.get_all()
    print(df.head())