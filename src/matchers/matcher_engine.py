# src/matchers/matcher_engine.py
from __future__ import annotations
import sys
import time
from pathlib import Path
from typing import Tuple
import pandas as pd

# ------------------------------------------------------
# Bootstrap de rutas
# ------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ------------------------------------------------------
# Importación corporativa
# ------------------------------------------------------
from src.core.logger import info, ok, warn
from src.core.env_loader import get_env, get_config
from src.transformers.calculator import Calculator

# IA opcional
try:
    from src.transformers.ai_helpers import ai_classify
    _AI_AVAILABLE = True
except Exception:
    _AI_AVAILABLE = False
    ai_classify = None


# ------------------------------------------------------
# Barra de progreso
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
# MatcherEngine
# ======================================================
class MatcherEngine:

    def __init__(self):
        info("Inicializando MatcherEngine…")

        self.cfg = get_config()
        self.calc = Calculator(self.cfg)

        self.days_tol = int(
            getattr(self.cfg.parametros, "dias_tolerancia_pago", 14)
            or int(get_env("DAYS_TOLERANCE_PAGO", default=14))
        )
        self.monto_var = float(
            getattr(self.cfg.parametros, "monto_variacion", 0.50)
            or float(get_env("MONTO_VARIACION", default=0.50))
        )

        self.min_score_match = 0.55

        ok("MatcherEngine cargado correctamente.")

    # --------------------------------------------------
    # Normalizar facturas
    # --------------------------------------------------
    def _prepare_facturas(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.calc.process_facturas(df)

        if "id" not in df.columns:
            raise ValueError("facturas_pf debe tener columna id")

        if "source_hash" not in df.columns:
            df["source_hash"] = df["id"].astype(str)

        return df

    # --------------------------------------------------
    # Normalizar bancos
    # --------------------------------------------------
    def _prepare_bancos(self, df: pd.DataFrame) -> pd.DataFrame:
        df = self.calc.process_bancos(df)

        df["fecha"] = pd.to_datetime(df.get("fecha"), errors="coerce")

        if "Monto_PEN" not in df.columns:
            df["Monto_PEN"] = pd.to_numeric(df.get("monto"), errors="coerce").fillna(0)

        df["descripcion_banco"] = df.get("descripcion", "").astype(str)
        df["operacion_banco"] = df.get("operacion", "").astype(str)
        df["banco_codigo"] = df.get("banco_codigo")

        return df

    # --------------------------------------------------
    # Filtrar candidatos (DOBLE MATCH REAL)
    # --------------------------------------------------
    def _filtrar_candidatos(self, fac: pd.Series, df_bancos: pd.DataFrame) -> pd.DataFrame:
        candidatos = []

        for tipo, monto in [
            ("TOTAL_FINAL", fac.get("total_final")),
            ("DETRACCION", fac.get("detraccion")),
        ]:
            if monto is None or monto <= 0:
                continue

            df = df_bancos[
                (df_bancos["Monto_PEN"] >= monto - self.monto_var) &
                (df_bancos["Monto_PEN"] <= monto + self.monto_var)
            ].copy()

            if df.empty:
                continue

            df["tipo_monto_match"] = tipo
            df["monto_objetivo"] = monto
            candidatos.append(df)

        if not candidatos:
            return pd.DataFrame()

        return pd.concat(candidatos, ignore_index=True)

    # --------------------------------------------------
    # Scoring
    # --------------------------------------------------
    def _score_candidato(self, fac: pd.Series, mov: pd.Series) -> Tuple[float, float, str]:
        target = mov["monto_objetivo"]
        monto_banco = mov["Monto_PEN"]

        variacion = abs(target - monto_banco)

        score_monto = max(0.0, 1.0 - (variacion / target)) if target > 0 else 0.0

        fecha_pago = fac.get("fecha_pago") or fac.get("vencimiento") or fac.get("fecha_emision")
        fecha_pago = pd.to_datetime(fecha_pago, errors="coerce")
        fecha_mov = pd.to_datetime(mov.get("fecha"), errors="coerce")

        if pd.isna(fecha_pago) or pd.isna(fecha_mov):
            score_fecha = 0.0
        else:
            dias = abs((fecha_mov - fecha_pago).days)
            score_fecha = max(0.0, 1.0 - (dias / self.days_tol))

        score = 0.7 * score_monto + 0.3 * score_fecha
        razon = f"Reglas → monto={score_monto:.2f}, fecha={score_fecha:.2f}"

        return score, variacion, razon

    # --------------------------------------------------
    # Ejecución principal
    # --------------------------------------------------
    def run(self, df_facturas: pd.DataFrame, df_bancos: pd.DataFrame):

        df_f = self._prepare_facturas(df_facturas)
        df_b = self._prepare_bancos(df_bancos)

        total = len(df_f)
        start = time.time()

        match_rows = []
        detalles_rows = []

        for i, fac in df_f.iterrows():
            sys.stdout.write(f"\r🔵 {_progress(i + 1, total, start)}")
            sys.stdout.flush()

            candidatos = self._filtrar_candidatos(fac, df_b)

            if candidatos.empty:
                continue

            for _, mov in candidatos.iterrows():
                score, variacion, razon = self._score_candidato(fac, mov)

                detalles_rows.append({
                    "factura_id": fac["id"],
                    "movimiento_id": mov["id"],
                    "monto_factura": mov["monto_objetivo"],
                    "monto_banco": mov["Monto_PEN"],
                    "variacion_monto": variacion,
                    "fecha_mov": mov["fecha"],
                    "banco_pago": mov["banco_codigo"],
                    "operacion": mov["operacion_banco"],
                    "descripcion_banco": mov["descripcion_banco"],
                    "score_similitud": score,
                    "razon_ia": razon,
                    "tipo_monto_match": mov["tipo_monto_match"],
                })

                if score >= self.min_score_match:
                    match_rows.append({
                        "factura_id": fac["id"],
                        "movimiento_id": mov["id"],
                        "monto_factura": mov["monto_objetivo"],
                        "monto_banco": mov["Monto_PEN"],
                        "diferencia": variacion,
                        "score_similitud": score,
                        "match_tipo": "MATCH",
                        "tipo_monto_match": mov["tipo_monto_match"],
                    })

        print("\n")
        ok("Matching finalizado correctamente.")

        return pd.DataFrame(match_rows), pd.DataFrame(detalles_rows)
