# src/matchers/matcher_engine.py
from __future__ import annotations
import sys
import time
from pathlib import Path
from typing import Tuple, List, Dict, Any
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
from src.core.env_loader import get_env, get_config
from src.transformers.calculator import Calculator

# IA opcional (no rompe si falla)
try:
    from src.transformers.ai_helpers import ai_classify
    _AI_AVAILABLE = True
except Exception:
    _AI_AVAILABLE = False
    ai_classify = None


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

    def __init__(self):
        info("Inicializando MatcherEngine…")

        self.cfg = get_config()
        self.calc = Calculator(self.cfg)

        # Parámetros corporativos
        self.days_tol = int(
            getattr(self.cfg.parametros, "dias_tolerancia_pago", 14)
            or int(get_env("DAYS_TOLERANCE_PAGO", default=14))
        )
        self.monto_var = float(
            getattr(self.cfg.parametros, "monto_variacion", 0.50)
            or float(get_env("MONTO_VARIACION", default=0.50))
        )

        # Score mínimo
        self.min_score_match = 0.55

        if _AI_AVAILABLE:
            ok("MatcherEngine cargado con IA habilitada.")
        else:
            warn("MatcherEngine: IA no disponible, se usará solo motor de reglas.")

        ok("MatcherEngine cargado correctamente.")

    # --------------------------------------------------
    #  Normalizar facturas
    # --------------------------------------------------
    def _prepare_facturas(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            warn("df_facturas vacío en _prepare_facturas.")
        else:
            info("Normalizando facturas…")

        df = self.calc.process_facturas(df)

        if "id" not in df.columns:
            raise ValueError("La tabla facturas_pf debe tener columna 'id'.")

        if "combinada" not in df.columns:
            df["combinada"] = df.get("serie", "").astype(str) + "-" + df.get("numero", "").astype(str)

        if "source_hash" not in df.columns:
            df["source_hash"] = df["id"].astype(str)

        return df

    # --------------------------------------------------
    #  Normalizar movimientos bancarios
    # --------------------------------------------------
    def _prepare_bancos(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            warn("df_bancos vacío en _prepare_bancos.")
            return df

        info("Normalizando movimientos bancarios…")

        df = self.calc.process_bancos(df)

        # Fecha estándar
        if "fecha" in df.columns:
            df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        elif "Fecha" in df.columns:
            df["fecha"] = pd.to_datetime(df["Fecha"], errors="coerce")
        else:
            warn("No se encontró columna de fecha en bancos. Se asignará NaT.")
            df["fecha"] = pd.NaT

        # Monto estándar en PEN
        if "Monto_PEN" not in df.columns:
            if "monto" in df.columns:
                df["Monto_PEN"] = pd.to_numeric(df["monto"], errors="coerce").fillna(0)
            else:
                warn("No se encontró columna de monto. Se asignará 0.")
                df["Monto_PEN"] = 0

        # Descripciones
        df["descripcion_banco"] = df.get("descripcion", "").astype(str)
        df["operacion_banco"] = df.get("operacion", "").astype(str)

        if "banco_codigo" not in df.columns:
            df["banco_codigo"] = None

        return df

    # --------------------------------------------------
    #  Filtrar candidatos
    # --------------------------------------------------
    def _filtrar_candidatos(self, fac: pd.Series, df_bancos: pd.DataFrame) -> pd.DataFrame:

        target_amount = fac.get("total_con_igv") or fac.get("total") or fac.get("subtotal") or 0
        target_amount = float(target_amount or 0)

        if target_amount <= 0:
            return pd.DataFrame()

        # Ventana de fechas
        fecha_pago = fac.get("fecha_pago")
        if pd.isna(fecha_pago):
            fecha_pago = fac.get("vencimiento") or fac.get("fecha_emision")

        if pd.isna(fecha_pago):
            fecha_min = fecha_max = None
        else:
            fecha_pago = pd.to_datetime(fecha_pago, errors="coerce")

            v_ini = fac.get("ventana_inicio")
            v_fin = fac.get("ventana_fin")

            if pd.notna(v_ini) and pd.notna(v_fin):
                fecha_min = pd.to_datetime(v_ini, errors="coerce")
                fecha_max = pd.to_datetime(v_fin, errors="coerce")
            else:
                fecha_min = fecha_pago - pd.to_timedelta(self.days_tol, unit="D")
                fecha_max = fecha_pago + pd.to_timedelta(self.days_tol, unit="D")

        # Rango montos
        delta = self.monto_var
        monto_min = target_amount - delta
        monto_max = target_amount + delta

        df = df_bancos.copy()

        # Filtrar por fecha
        if fecha_min is not None and fecha_max is not None:
            df = df[(df["fecha"] >= fecha_min) & (df["fecha"] <= fecha_max)]

        # Filtrar por monto
        df = df[(df["Monto_PEN"] >= monto_min) & (df["Monto_PEN"] <= monto_max)]

        return df

    # --------------------------------------------------
    # Score híbrido reglas + IA
    # --------------------------------------------------
    def _score_candidato(self, fac: pd.Series, mov: pd.Series) -> Tuple[float, float, str]:

        # Regla por monto
        target_amount = fac.get("total_con_igv") or fac.get("total") or fac.get("subtotal") or 0
        target_amount = float(target_amount or 0)
        monto_banco = float(mov.get("Monto_PEN") or 0)

        variacion = abs(target_amount - monto_banco)

        if target_amount > 0:
            score_monto = max(0.0, 1.0 - (variacion / target_amount))
        else:
            score_monto = 0.0

        # Regla por fecha
        fecha_pago = fac.get("fecha_pago")
        if pd.isna(fecha_pago):
            fecha_pago = fac.get("vencimiento") or fac.get("fecha_emision")

        fecha_pago = pd.to_datetime(fecha_pago, errors="coerce")
        fecha_mov = pd.to_datetime(mov.get("fecha"), errors="coerce")

        if pd.isna(fecha_pago) or pd.isna(fecha_mov):
            score_fecha = 0.0
        else:
            dias_diff = abs((fecha_mov - fecha_pago).days)
            score_fecha = max(0.0, 1.0 - (dias_diff / self.days_tol))

        score_base = 0.7 * score_monto + 0.3 * score_fecha
        razon = f"Reglas → monto={score_monto:.2f}, fecha={score_fecha:.2f}."

        # ------------------------------------------------------
        # IA OPCIONAL (OPCIÓN A — Adaptado a tu ai_helpers)
        # ------------------------------------------------------
        if _AI_AVAILABLE and ai_classify is not None:
            try:
                desc_fac = str(fac.get("cliente_generador") or "") + " " + str(fac.get("combinada") or "")
                desc_mov = str(mov.get("descripcion_banco") or "")

                prompt = f"""
Factura:
- Cliente: {desc_fac.strip()}
- Monto con IGV: {target_amount}
- Fecha estimada: {fecha_pago}

Movimiento:
- Monto banco: {monto_banco}
- Fecha mov: {fecha_mov}
- Descripción: {desc_mov}

Decide si el movimiento corresponde al pago de la factura.
Devuelve SOLO JSON:
{{
 "tipo": "pago_factura" | "transferencia" | "detraccion" | "otro",
 "probabilidad": 0.0–1.0,
 "justificacion": "texto"
}}
"""

                result = ai_classify(prompt)

                label = str(result.get("tipo", "")).lower()
                prob = float(result.get("probabilidad", 0.0))

                if "pago" in label:
                    score_ai = max(prob, 0.7)
                    razon = result.get("justificacion", "IA: match fuerte.")
                elif prob >= 0.4:
                    score_ai = max(prob * 0.6, 0.3)
                    razon = result.get("justificacion", "IA: match dudoso.")
                else:
                    score_ai = 0.0
                    razon = result.get("justificacion", "IA: no match.")

                score_total = 0.6 * score_base + 0.4 * score_ai

            except Exception as e:
                warn(f"IA no disponible para scoring detallado: {e}")
                score_total = score_base
        else:
            score_total = score_base

        return float(score_total), float(variacion), razon

    # --------------------------------------------------
    #  Ejecución principal
    # --------------------------------------------------
    def run(self, df_facturas: pd.DataFrame, df_bancos: pd.DataFrame):
        info("Iniciando MatcherEngine.run()…")

        if df_facturas.empty or df_bancos.empty:
            return pd.DataFrame(), pd.DataFrame()

        df_f = self._prepare_facturas(df_facturas)
        df_b = self._prepare_bancos(df_bancos)

        total = len(df_f)
        start = time.time()

        info(f"Procesando {total} facturas para matching…\n")

        match_rows = []
        detalles_rows = []

        for i, fac in df_f.iterrows():

            # progreso
            bar = _progress(i + 1, total, start)
            sys.stdout.write(f"\r🔵 {bar}")
            sys.stdout.flush()

            # candidatos
            candidatos = self._filtrar_candidatos(fac, df_b)

            if candidatos.empty:
                match_rows.append({
                    "factura_id": fac.get("id"),
                    "movimiento_id": None,
                    "subtotal": fac.get("subtotal"),
                    "igv": fac.get("igv"),
                    "total_con_igv": fac.get("total_con_igv"),
                    "detraccion_monto": fac.get("detraccion_monto"),
                    "neto_recibido": fac.get("neto_recibido"),
                    "monto_banco": None,
                    "monto_banco_equivalente": None,
                    "variacion_monto": None,
                    "fecha_mov": None,
                    "banco_pago": None,
                    "operacion": None,
                    "descripcion_banco": None,
                    "moneda": None,
                    "score_similitud": 0.0,
                    "razon_ia": "Sin candidatos en ventana de fechas/montos.",
                    "match_tipo": "NO_MATCH",
                })
                continue

            # evaluar candidatos
            best_score = -1
            best_row = None
            best_variacion = None
            best_razon = ""

            for _, mov in candidatos.iterrows():
                score, variacion, razon = self._score_candidato(fac, mov)

                detalles_rows.append({
                    "factura_id": fac.get("id"),
                    "movimiento_id": mov.get("id"),
                    "monto_factura": fac.get("total_con_igv"),
                    "monto_banco": mov.get("Monto_PEN"),
                    "variacion_monto": variacion,
                    "fecha_mov": mov.get("fecha"),
                    "banco_pago": mov.get("banco_codigo"),
                    "operacion": mov.get("operacion_banco"),
                    "descripcion_banco": mov.get("descripcion_banco"),
                    "score_similitud": score,
                    "razon_ia": razon,
                })

                if score > best_score:
                    best_score = score
                    best_row = mov
                    best_variacion = variacion
                    best_razon = razon

            # decidir match
            if best_row is None or best_score < self.min_score_match:
                match_rows.append({
                    "factura_id": fac.get("id"),
                    "movimiento_id": None,
                    "subtotal": fac.get("subtotal"),
                    "igv": fac.get("igv"),
                    "total_con_igv": fac.get("total_con_igv"),
                    "detraccion_monto": fac.get("detraccion_monto"),
                    "neto_recibido": fac.get("neto_recibido"),
                    "monto_banco": None,
                    "monto_banco_equivalente": None,
                    "variacion_monto": best_variacion,
                    "fecha_mov": None,
                    "banco_pago": None,
                    "operacion": None,
                    "descripcion_banco": None,
                    "moneda": None,
                    "score_similitud": float(best_score if best_score > 0 else 0),
                    "razon_ia": best_razon or "Score insuficiente para confirmar match.",
                    "match_tipo": "NO_MATCH",
                })
            else:
                match_rows.append({
                    "factura_id": fac.get("id"),
                    "movimiento_id": best_row.get("id"),
                    "subtotal": fac.get("subtotal"),
                    "igv": fac.get("igv"),
                    "total_con_igv": fac.get("total_con_igv"),
                    "detraccion_monto": fac.get("detraccion_monto"),
                    "neto_recibido": fac.get("neto_recibido"),
                    "monto_banco": best_row.get("Monto_PEN"),
                    "monto_banco_equivalente": best_row.get("Monto_PEN"),
                    "variacion_monto": best_variacion,
                    "fecha_mov": best_row.get("fecha"),
                    "banco_pago": best_row.get("banco_codigo"),
                    "operacion": best_row.get("operacion_banco"),
                    "descripcion_banco": best_row.get("descripcion_banco"),
                    "moneda": best_row.get("moneda"),
                    "score_similitud": float(best_score),
                    "razon_ia": best_razon,
                    "match_tipo": "MATCH",
                })

        print("\n")
        ok("Matching finalizado correctamente.")

        df_match = pd.DataFrame(match_rows)
        df_detalles = pd.DataFrame(detalles_rows)

        return df_match, df_detalles


# ======================================================
# TEST LOCAL
# ======================================================
if __name__ == "__main__":
    from src.core.db import PulseForgeDB

    info("=== TEST LOCAL · MATCHER ENGINE HÍBRIDO ===")
    db = PulseForgeDB()
    conn = db.connect()

    df_fact = pd.read_sql_query("SELECT * FROM facturas_pf", conn)
    df_bank = pd.read_sql_query("SELECT * FROM bancos_pf", conn)

    engine = MatcherEngine()
    df_match, df_det = engine.run(df_fact, df_bank)

    print("\n=== MATCH HEAD ===")
    print(df_match.head())

    print("\n=== DETALLES HEAD ===")
    print(df_det.head())

    ok("=== FIN TEST LOCAL MATCHER ENGINE ===")
