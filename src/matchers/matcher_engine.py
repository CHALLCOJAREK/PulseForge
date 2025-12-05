# src/matchers/matcher_engine.py
from __future__ import annotations
import sys
import time
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn
from src.core.env_loader import get_env
from src.transformers.matcher import Matcher
from src.transformers.calculator import Calculator


# ------------------------------------------------------
#  BARRA DE PROGRESO LIMPIA
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
class MatcherEngine:
    """
    Motor oficial de Matching PulseForge
    - Calcula facturas y bancos con Calculator()
    - Ejecuta Matcher() para cada factura
    - Guarda TODOS los matches y detalles
    - Muestra barra de progreso suave (sin logs internos)
    """

    def __init__(self):
        info("Inicializando MatcherEngine…")
        self.matcher = Matcher()
        self.calc = Calculator()
        self.days_tol = int(get_env("DAYS_TOLERANCE_PAGO", default=14))
        self.monto_var = float(get_env("MONTO_VARIACION", default=0.50))
        ok("MatcherEngine listo.")

    # ---------------------------------------------------------
    def _prepare_facturas(self, df: pd.DataFrame) -> pd.DataFrame:
        info("Aplicando cálculo contable → facturas")
        df = self.calc.process_facturas(df)

        # Crear columna combinada si falta
        if "combinada" not in df.columns:
            df["combinada"] = df["serie"].astype(str) + "-" + df["numero"].astype(str)

        return df

    # ---------------------------------------------------------
    def _prepare_bancos(self, df: pd.DataFrame) -> pd.DataFrame:
        info("Procesando movimientos bancarios…")
        df = self.calc.process_bancos(df)

        if "Fecha" not in df.columns:
            warn("No existe columna Fecha, usando 1970-01-01")
            df["Fecha"] = pd.to_datetime("1970-01-01")

        return df

    # ---------------------------------------------------------
    def run(self, df_facturas: pd.DataFrame, df_bancos: pd.DataFrame):
        info("Iniciando motor de matching…")

        if df_facturas.empty:
            warn("No hay facturas para procesar.")
            return pd.DataFrame(), pd.DataFrame()

        if df_bancos.empty:
            warn("No hay movimientos bancarios.")
            return pd.DataFrame(), pd.DataFrame()

        # 1) Normalizar
        df_f = self._prepare_facturas(df_facturas)
        df_b = self._prepare_bancos(df_bancos)

        total = len(df_f)
        start = time.time()

        info(f"🔄 Matching de {total} facturas…\n")

        match_rows = []
        detalles_rows = []

        # ---------------------------------------------------------
        # LOOP PRINCIPAL
        # ---------------------------------------------------------
        for i, fac in df_f.iterrows():

            # Barra de progreso limpia
            bar = _progress(i + 1, total, start)
            sys.stdout.write(f"\r🔵 {bar}")
            sys.stdout.flush()

            # Ejecutar matcher para ESTA factura
            df_match, df_det = self.matcher.match(
                df_f.loc[[i]],   # factura aislada
                df_b            # todos los bancos
            )

            # MULTI-MATCH → se guardan TODOS los candidatos
            if not df_match.empty:
                for _, row in df_match.iterrows():
                    match_rows.append(row.to_dict())

            # DETALLES → se acumulan todos
            if not df_det.empty:
                detalles_rows.extend(df_det.to_dict("records"))

        print("\n")
        ok("🧩 Matching completado.")

        return pd.DataFrame(match_rows), pd.DataFrame(detalles_rows)
