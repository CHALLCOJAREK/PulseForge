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
    Extrae movimientos bancarios desde la BD origen (DataPulse)
    para:
      - Cuenta empresa  (IBK u otra, según .env)
      - Cuenta detracción (BN u otra, según .env)

    Normaliza a las columnas:
      - Fecha
      - Monto
      - Descripcion
      - Cuenta
    """

    def __init__(self):
        self.env = get_env()
        self.db = get_db()

        info("Inicializando extractor de movimientos bancarios...")

        # Cargar settings.json
        from json import load

        settings_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../config/settings.json")
        )

        if not os.path.exists(settings_path):
            error(f"No se encontró settings.json en: {settings_path}")
            raise FileNotFoundError("Archivo settings.json no encontrado.")

        with open(settings_path, "r", encoding="utf-8") as f:
            self.settings = load(f)

        self.tablas = self.settings["tablas"]
        self.col_bancos = self.settings["columnas_bancos"]

        self.col_fecha = self.col_bancos["fecha"]
        self.col_monto = self.col_bancos["monto"]
        self.col_desc  = self.col_bancos["descripcion"]

        ok("Extractor de bancos listo para trabajar.")


    # =======================================================
    #   CARGA GENERICA DE TABLA BANCARIA
    # =======================================================
    def _load_bank_table(self, table_name: str, cuenta_label: str) -> pd.DataFrame:
        """
        Carga una tabla bancaria desde SQL y la normaliza
        a las columnas:
          - Fecha
          - Monto
          - Descripcion
          - Cuenta (label fijo)
        """

        info(f"Cargando tabla bancaria: {table_name} (Cuenta: {cuenta_label})")

        query = text(f"SELECT * FROM {table_name}")

        try:
            df = pd.read_sql(query, self.db.engine_origen)
        except Exception as e:
            error(f"Error leyendo tabla bancaria {table_name}: {e}")
            raise

        ok(f"Registros cargados desde {table_name}: {len(df)}")

        # Validar columnas necesarias
        required = [self.col_fecha, self.col_monto, self.col_desc]
        missing = [c for c in required if c not in df.columns]

        if missing:
            error(f"Faltan columnas en tabla bancaria {table_name}: {missing}")
            raise KeyError(f"Columnas faltantes en {table_name}: {missing}")

        # Normalizar nombres de columnas a un estándar interno
        df_norm = df[[self.col_fecha, self.col_monto, self.col_desc]].copy()
        df_norm.columns = ["Fecha", "Monto", "Descripcion"]

        # Agregar etiqueta de cuenta
        df_norm["Cuenta"] = cuenta_label

        info(f"Vista previa de movimientos de {cuenta_label}:")
        print(df_norm.head())

        return df_norm


    # =======================================================
    #   CUENTA EMPRESA
    # =======================================================
    def get_movimientos_empresa(self) -> pd.DataFrame:
        """
        Retorna movimientos de la cuenta empresa
        (tabla configurada en settings['tablas']['banco_empresa'])
        """
        tabla_empresa = self.tablas["banco_empresa"]
        cuenta_label = self.env.CUENTA_EMPRESA  # Ej: "IBK"
        return self._load_bank_table(tabla_empresa, cuenta_label)


    # =======================================================
    #   CUENTA DETRACCIÓN
    # =======================================================
    def get_movimientos_detraccion(self) -> pd.DataFrame:
        """
        Retorna movimientos de la cuenta de detracciones
        (tabla configurada en settings['tablas']['banco_detraccion'])
        """
        tabla_det = self.tablas["banco_detraccion"]
        cuenta_label = self.env.CUENTA_DETRACCION  # Ej: "BN"
        return self._load_bank_table(tabla_det, cuenta_label)


    # =======================================================
    #   TODOS LOS MOVIMIENTOS UNIFICADOS
    # =======================================================
    def get_todos_movimientos(self) -> pd.DataFrame:
        """
        Retorna un DataFrame unificado con:
        - movimientos de cuenta empresa
        - movimientos de cuenta detracción
        """
        info("Cargando movimientos de empresa y detracción...")

        df_emp = self.get_movimientos_empresa()
        df_det = self.get_movimientos_detraccion()

        df_all = pd.concat([df_emp, df_det], ignore_index=True)

        ok(f"Total de movimientos unificados: {len(df_all)}")
        info("Vista previa de todos los movimientos:")
        print(df_all.head())

        return df_all



# =======================================================
#   EJECUCIÓN DIRECTA (TEST)
# =======================================================
if __name__ == "__main__":
    info("🚀 Testeando BankExtractor...")

    be = BankExtractor()

    # Estos tests funcionarán cuando ya completes bien settings.json
    try:
        df_emp = be.get_movimientos_empresa()
        ok("Movimientos de cuenta empresa cargados correctamente.")
    except Exception as e:
        warn(f"No se pudieron cargar aún los movimientos de empresa: {e}")

    try:
        df_det = be.get_movimientos_detraccion()
        ok("Movimientos de cuenta detracción cargados correctamente.")
    except Exception as e:
        warn(f"No se pudieron cargar aún los movimientos de detracción: {e}")
