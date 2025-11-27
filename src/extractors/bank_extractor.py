# src/extractors/bank_extractor.py

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
from sqlalchemy import text

from src.core.db import get_db
from src.core.env_loader import get_env

# Prints estilo Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class BankExtractor:
    """
    Extrae y unifica movimientos bancarios desde varias tablas:
      - Banco de la Nación
      - BBVA soles
      - BCP dólares
      - BCP soles
      - Interbank soles
      - Arequipa soles
      - Finanzas soles

    Hace lectura flexible de columnas usando settings.json, y devuelve
    un DataFrame unificado con columnas estándar:

      Banco
      Fecha
      Tipo_Mov
      Descripcion
      Serie
      Numero
      Monto
      Moneda
      Operacion
      Destinatario
      Tipo_Documento
    """

    def __init__(self):
        self.env = get_env()
        self.db = get_db()

        info("Inicializando extractor de bancos...")

        from json import load

        # settings.json
        settings_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../config/settings.json")
        )
        if not os.path.exists(settings_path):
            error(f"No se encontró settings.json en: {settings_path}")
            raise FileNotFoundError("settings.json no encontrado")

        with open(settings_path, "r", encoding="utf-8") as f:
            self.settings = load(f)

        # constants.json (para flags tipo considerar_montos_cero)
        constants_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../config/constants.json")
        )
        if os.path.exists(constants_path):
            with open(constants_path, "r", encoding="utf-8") as f:
                self.constants = load(f)
        else:
            warn("No se encontró constants.json. Usando valores por defecto.")
            self.constants = {
                "considerar_montos_cero": False
            }

        self.col_bancos = self.settings["columnas_bancos"]

        ok("Extractor de bancos listo para trabajar.")


    # =======================================================
    #   Normalizador de nombres de columna (flexible)
    # =======================================================
    @staticmethod
    def _norm_name(name: str) -> str:
        """
        Normaliza nombres:
        - a minúsculas
        - sin espacios
        - sin guiones bajos
        """
        return (
            str(name)
            .lower()
            .strip()
            .replace(" ", "")
            .replace("_", "")
        )

    def _find_column_flexible(self, df: pd.DataFrame, configured_name: str):
        """
        Busca la columna en df usando el nombre configurado,
        tolerando mayúsculas, espacios y cambios menores.
        """
        if not configured_name:
            return None

        target_norm = self._norm_name(configured_name)

        # Construimos mapa normalizado -> real
        norm_map = {self._norm_name(c): c for c in df.columns}

        # Match directo por nombre normalizado
        if target_norm in norm_map:
            return norm_map[target_norm]

        # Si no encontramos, intentamos por contiene (por si viene "descripcion actividad")
        for norm_col, real_col in norm_map.items():
            if target_norm in norm_col or norm_col in target_norm:
                return real_col

        return None


    # =======================================================
    #   Cargar una tabla bancaria y normalizarla
    # =======================================================
    def _load_bank_table(self, table_name: str, bank_label: str) -> pd.DataFrame:
        info(f"Cargando movimientos de banco: {bank_label} ({table_name})")

        query = text(f"SELECT * FROM {table_name}")

        try:
            df_raw = pd.read_sql(query, self.db.engine_origen)
        except Exception as e:
            error(f"Error leyendo tabla {table_name}: {e}")
            return pd.DataFrame()  # devolvemos vacío, no rompemos todo

        if df_raw.empty:
            warn(f"Tabla {table_name} está vacía.")
            return pd.DataFrame()

        # Buscamos columnas usando lectura flexible
        col_fecha        = self._find_column_flexible(df_raw, self.col_bancos.get("fecha"))
        col_tipo_mov     = self._find_column_flexible(df_raw, self.col_bancos.get("tipo_mov"))
        col_desc         = self._find_column_flexible(df_raw, self.col_bancos.get("descripcion"))
        col_serie        = self._find_column_flexible(df_raw, self.col_bancos.get("serie"))
        col_numero       = self._find_column_flexible(df_raw, self.col_bancos.get("numero"))
        col_monto        = self._find_column_flexible(df_raw, self.col_bancos.get("monto"))
        col_moneda       = self._find_column_flexible(df_raw, self.col_bancos.get("moneda"))
        col_operacion    = self._find_column_flexible(df_raw, self.col_bancos.get("operacion"))
        col_destinatario = self._find_column_flexible(df_raw, self.col_bancos.get("destinatario"))
        col_tipo_doc     = self._find_column_flexible(df_raw, self.col_bancos.get("tipo_documento"))

        # Validamos las mínimas necesarias
        required = [("fecha", col_fecha), ("tipo_mov", col_tipo_mov), ("monto", col_monto)]
        faltantes = [name for name, col in required if col is None]

        if faltantes:
            warn(f"En {table_name} faltan columnas mínimas: {faltantes}. Se ignora esta tabla.")
            return pd.DataFrame()

        df = pd.DataFrame()
        df["Banco"] = bank_label

        # Fecha
        df["Fecha"] = pd.to_datetime(df_raw[col_fecha], errors="coerce")

        # Tipo de movimiento
        df["Tipo_Mov"] = df_raw[col_tipo_mov].astype(str).str.strip().str.upper()

        # Filtrar solo INGRESO (ignoramos SALDO, EGRESO, etc.)
        df = df[df["Tipo_Mov"] == "INGRESO"]
        if df.empty:
            warn(f"No hay movimientos de tipo INGRESO en {table_name}.")
            return pd.DataFrame()

        # Descripción
        if col_desc:
            df["Descripcion"] = df_raw[col_desc].astype(str).str.strip()
        else:
            df["Descripcion"] = ""

        # Serie y Número (pueden venir vacíos)
        if col_serie:
            df["Serie"] = df_raw[col_serie].astype(str).str.strip()
        else:
            df["Serie"] = ""

        if col_numero:
            df["Numero"] = df_raw[col_numero].astype(str).str.strip()
        else:
            df["Numero"] = ""

        # Monto
        df["Monto"] = pd.to_numeric(df_raw[col_monto], errors="coerce").fillna(0)

        # Moneda
        if col_moneda:
            df["Moneda"] = df_raw[col_moneda].astype(str).str.strip().str.upper()
        else:
            # Asumimos PEN si no hay columna
            df["Moneda"] = "PEN"

        # N° operación
        if col_operacion:
            df["Operacion"] = df_raw[col_operacion].astype(str).str.strip()
        else:
            df["Operacion"] = ""

        # Destinatario
        if col_destinatario:
            df["Destinatario"] = df_raw[col_destinatario].astype(str).str.strip()
        else:
            df["Destinatario"] = ""

        # Tipo de documento
        if col_tipo_doc:
            tipo_doc_series = df_raw[col_tipo_doc].astype(str).str.strip().str.upper()
            df["Tipo_Documento"] = tipo_doc_series
            # Opcional: descartar NOTAS
            mask_notas = tipo_doc_series.str.contains("NOTA", na=False)
            if mask_notas.any():
                df = df[~mask_notas]
        else:
            df["Tipo_Documento"] = ""

        # Filtrar montos cero si así está en constants
        if not self.constants.get("considerar_montos_cero", False):
            before = len(df)
            df = df[df["Monto"] != 0]
            after = len(df)
            if before != after:
                info(f"Filtrados {before - after} movimientos con monto 0 en {bank_label}.")

        ok(f"Movimientos normalizados para {bank_label}: {len(df)}")
        return df


    # =======================================================
    #   PROCESO PRINCIPAL: UNIFICAR TODOS LOS BANCOS
    # =======================================================
    def get_todos_movimientos(self) -> pd.DataFrame:
        """
        Devuelve un DataFrame unificado con TODOS los movimientos de INGRESO
        de todos los bancos configurados.
        """
        info("Extrayendo y unificando movimientos bancarios de todos los bancos...")

        tablas = self.settings["tablas"]

        bancos_def = [
            ("banco_nacion", "BN"),
            ("banco_bbva_soles", "BBVA-S"),
            ("banco_bcp_dolares", "BCP-USD"),
            ("banco_bcp_soles", "BCP-S"),
            ("banco_interbank_soles", "IBK-S"),
            ("banco_arequipa_soles", "AREQUIPA-S"),
            ("banco_finanzas_soles", "FINANZAS-S"),
        ]

        dfs = []
        for key, label in bancos_def:
            table_name = tablas.get(key)
            if not table_name:
                warn(f"No hay tabla configurada para {key} en settings.json. Se omite.")
                continue

            df_bank = self._load_bank_table(table_name, label)
            if not df_bank.empty:
                dfs.append(df_bank)

        if not dfs:
            warn("No se encontraron movimientos en ningún banco.")
            return pd.DataFrame()

        df_all = pd.concat(dfs, ignore_index=True)

        # Ordenamos por fecha para mejor lectura
        df_all = df_all.sort_values(by=["Fecha", "Banco", "Monto"], ascending=[True, True, True])

        ok(f"Total de movimientos unificados: {len(df_all)}")
        return df_all



# =======================================================
#   TEST DIRECTO MANUAL (opcional)
# =======================================================
if __name__ == "__main__":
    info("🚀 Testeando extractor de bancos (unificación completa)...")
    extractor = BankExtractor()
    df = extractor.get_todos_movimientos()
    ok("Extracción de bancos completada.")
    print(df.head())
