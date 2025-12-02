# src/matchers/matcher_engine.py
from __future__ import annotations

import sys
from pathlib import Path

# === BOOTSTRAP PARA PODER IMPORTAR src/ ===
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ============================================================
#  PULSEFORGE · MATCHER ENGINE (VERSIÓN LÓGICA SIMPLE PRO)
# ============================================================

import pandas as pd
from src.core.logger import info, ok, warn, error



class MatcherEngine:
    """
    Motor lógico de matching:
      - Cruza facturas con movimientos bancarios
      - Reglas simples: RUC, montos, combinada, ventana de fechas
      - Devuelve DataFrame listo para MatchWriter
    """

    def __init__(self):
        info("Inicializando MatcherEngine…")
        ok("MatcherEngine listo.")

    # --------------------------------------------------------
    def _score_match(self, factura_row, mov_row) -> float:
        score = 0.0

        # RUC exacto = 40 puntos
        if str(factura_row.get("ruc")) == str(mov_row.get("destinatario")):
            score += 40

        # Monto dentro de ± 1% = 40 puntos
        try:
            f_total = float(factura_row.get("total_con_igv", 0) or 0)
            b_monto = float(mov_row.get("monto", 0) or 0)

            if f_total == 0:
                pass
            else:
                diff = abs(f_total - b_monto)
                if diff <= f_total * 0.01:
                    score += 40
                elif diff <= f_total * 0.05:
                    score += 20
        except:
            pass

        # Fecha exacta = 10 puntos
        f_fecha = factura_row.get("fecha_emision")
        b_fecha = mov_row.get("fecha")

        if f_fecha and b_fecha and str(f_fecha)[:10] == str(b_fecha)[:10]:
            score += 10

        # Coincidencia por combinada en descripción = 10 puntos
        comb = str(factura_row.get("combinada") or "").strip()
        desc = str(mov_row.get("descripcion") or "").lower()

        if comb and comb.lower() in desc:
            score += 10

        return score

    # --------------------------------------------------------
    def run(self, df_facturas: pd.DataFrame, df_movs: pd.DataFrame) -> pd.DataFrame:
        """
        Retorna DataFrame:
          factura_id, movimiento_id, monto_aplicado, monto_detraccion,
          variacion, fecha_pago, banco, score, razon_ia, match_tipo, source_hash
        """

        if df_facturas.empty or df_movs.empty:
            warn("No hay facturas o movimientos para ejecutar matching.")
            return pd.DataFrame()

        results = []

        info("Ejecutando reglas de coincidencia…")

        for _, frow in df_facturas.iterrows():
            mejor_score = 0
            mejor_mov = None

            for _, mrow in df_movs.iterrows():
                sc = self._score_match(frow, mrow)
                if sc > mejor_score:
                    mejor_score = sc
                    mejor_mov = mrow

            if mejor_mov is None:
                continue

            # Variación entre monto factura y movimiento
            try:
                f_total = float(frow.get("total_con_igv", 0) or 0)
                b_monto = float(mejor_mov.get("monto", 0) or 0)
                variacion = b_monto - f_total
            except:
                variacion = None

            results.append({
                "factura_id": frow.get("id"),
                "movimiento_id": mejor_mov.get("id"),
                "monto_aplicado": mejor_mov.get("monto"),
                "monto_detraccion": frow.get("detraccion_monto"),
                "variacion": variacion,
                "fecha_pago": mejor_mov.get("fecha"),
                "banco": mejor_mov.get("banco"),
                "score": mejor_score,
                "razon_ia": f"Match estimado por reglas. Score={mejor_score}",
                "match_tipo": "auto",
                "source_hash": f"{frow.get('id')}-{mejor_mov.get('id')}"
            })

        df_out = pd.DataFrame(results)

        ok(f"MatcherEngine generó {len(df_out)} matches.")

        return df_out
