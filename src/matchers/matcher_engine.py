#  src/matchers/matcher_engine.py
from __future__ import annotations
import sys
import time
from pathlib import Path
import pandas as pd

# ------------------------------------------------------
#  Bootstrap de rutas
# ------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ------------------------------------------------------
#  Importación corporativa
# ------------------------------------------------------
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env
from src.transformers.matcher import Matcher
from src.transformers.calculator import Calculator


# ------------------------------------------------------
#  Barra de progreso discreta
# ------------------------------------------------------
def _progress(i: int, total: int, start_time: float) -> str:
    if total <= 0:
        return "[--------------------]   0%  ETA: --s"

    pct = min(100, int((i / total) * 100))
    bars = pct // 5

    elapsed = time.time() - start_time
    speed = i / elapsed if elapsed > 0 else 1
    eta = int((total - i) / speed) if speed > 0 else 0

    return f"[{'#' * bars}{'-' * (20 - bars)}] {pct:3d}%  ETA: {eta}s"


# ======================================================
#  MatcherEngine · Motor Oficial de Matching PulseForge
# ======================================================
class MatcherEngine:
    """
    Motor principal de matching entre facturas y bancos.
    - Aplica cálculo contable previo (Calculator)
    - Ejecuta Matcher para cada factura
    - Genera tabla de matches y tabla de detalles
    - No escribe en BD (eso lo hace pipeline_matcher)
    """

    def __init__(self):
        info("Inicializando MatcherEngine…")

        self.matcher = Matcher()
        self.calc = Calculator()

        # Parámetros corporativos desde settings/env
        self.days_tol = int(get_env("DAYS_TOLERANCE_PAGO", default=14))
        self.monto_var = float(get_env("MONTO_VARIACION", default=0.50))

        ok("MatcherEngine cargado correctamente.")

    # --------------------------------------------------
    #  Preparación de facturas
    # --------------------------------------------------
    def _prepare_facturas(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            warn("df_facturas vacío en preparation.")
            return df

        info("Normalizando facturas…")
        df = self.calc.process_facturas(df)

        # combinada = serie-numero
        if "combinada" not in df.columns:
            df["combinada"] = df["serie"].astype(str) + "-" + df["numero"].astype(str)

        return df

    # --------------------------------------------------
    #  Preparación de movimientos bancarios
    # --------------------------------------------------
    def _prepare_bancos(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            warn("df_bancos vacío en preparation.")
            return df

        info("Normalizando movimientos bancarios…")
        df = self.calc.process_bancos(df)

        # Estándar corporativo de fecha
        if "fecha" not in df.columns:
            warn("Columna fecha no encontrada, usando fecha por defecto.")
            df["fecha"] = pd.to_datetime("1970-01-01")

        return df

    # --------------------------------------------------
    #  Ejecución completa del motor de match
    # --------------------------------------------------
    def run(self, df_facturas: pd.DataFrame, df_bancos: pd.DataFrame):
        info("Iniciando MatcherEngine.run()…")

        if df_facturas.empty:
            warn("No hay facturas para procesar.")
            return pd.DataFrame(), pd.DataFrame()

        if df_bancos.empty:
            warn("No hay movimientos bancarios para procesar.")
            return pd.DataFrame(), pd.DataFrame()

        # 1) Normalizar
        df_f = self._prepare_facturas(df_facturas)
        df_b = self._prepare_bancos(df_bancos)

        total = len(df_f)
        start = time.time()

        info(f"Procesando {total} facturas para matching…\n")

        match_rows = []
        detalles_rows = []

        # --------------------------------------------------
        #  Loop principal del matching
        # --------------------------------------------------
        for i, fac in df_f.iterrows():

            # barra de progreso sin logs internos del matcher
            bar = _progress(i + 1, total, start)
            sys.stdout.write(f"\r🔵 {bar}")
            sys.stdout.flush()

            # Llamada directa al Matcher
            df_match, df_detalle = self.matcher.match(
                df_f.loc[[i]],
                df_b
            )

            # Matches (posible multi-match)
            if not df_match.empty:
                match_rows.extend(df_match.to_dict("records"))

            # Detalles (todos los candidatos evaluados)
            if not df_detalle.empty:
                detalles_rows.extend(df_detalle.to_dict("records"))

        print("\n")
        ok("Matching finalizado correctamente.")

        return pd.DataFrame(match_rows), pd.DataFrame(detalles_rows)
