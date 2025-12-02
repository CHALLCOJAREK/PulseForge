# src/transformers/matcher.py
from __future__ import annotations

# ============================================================
#  BOOTSTRAP RUTAS
# ============================================================
import sys
import json
import re
from pathlib import Path
from typing import Any, Dict, List, Optional
from datetime import timedelta
from difflib import SequenceMatcher

import pandas as pd

CURRENT_FILE = Path(__file__).resolve()
ROOT_DIR = CURRENT_FILE.parents[2]

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

# ============================================================
#  IMPORTS CORE
# ============================================================
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env
from src.transformers.ai_helpers import ai_similarity, ai_decide_match


# ============================================================
#  MATCHER · PULSEFORGE
# ============================================================
class Matcher:
    """
    Motor contable empresarial para conciliar FACTURAS ↔ BANCOS.
    Funcionalidades:
      • Matching por monto (neto / total / subtotal)
      • Ventana inteligente de fechas
      • Conversión USD→PEN si aplica
      • Matching de detracciones BN
      • Scoring: monto + similitud nombre + keywords
      • IA opcional para:
            - similitud de nombres
            - decisiones finales ambiguas
    """

    FLEX_TERMS: List[str] = [
        "interbancario", "interbank", "interbanco",
        "factoring", "factoraje", "cesion", "cesión",
        "masivo", "pago masivo", "pago facturas",
        "deposito", "depósito", "transferencia", "trf", "trx",
    ]

    # -----------------------------------------------------------
    def __init__(self):
        info("Inicializando Matcher PulseForge…")

        self.var_monto = float(get_env("MONTO_VARIACION", default=0.50))
        self.tc_usd_pen = float(get_env("TIPO_CAMBIO_USD_PEN", default=3.8))
        self.extra_days = int(get_env("MATCH_EXTRA_DAYS", default=3))

        self.sim_strong = float(get_env("MATCH_SIM_MIN", default=0.55))
        self.sim_dudoso = float(get_env("MATCH_SIM_DUDOSO_MIN", default=0.35))

        activar_ia = str(get_env("ACTIVAR_IA", default="true")).strip().lower()
        self.use_ai = activar_ia in ("1", "true", "yes", "on")

        if self.use_ai:
            ok("Matcher: IA ACTIVADA ✔️")
        else:
            warn("Matcher: IA desactivada (solo reglas deterministas).")

        ok("Matcher inicializado correctamente.")

    # -----------------------------------------------------------
    @staticmethod
    def _similarity_basic(a: str, b: str) -> float:
        """Similitud básica (fallback determinista)."""
        a = (a or "").lower().strip()
        b = (b or "").lower().strip()
        if not a or not b:
            return 0.0
        return SequenceMatcher(None, a, b).ratio()

    @staticmethod
    def _safe_float(v: Any) -> Optional[float]:
        try:
            return float(v)
        except:
            return None

    def _contains_flex_terms(self, text: str) -> bool:
        t = (text or "").lower()
        return any(term in t for term in self.FLEX_TERMS)

    # ============================================================
    #  DETRACCIÓN BN
    # ============================================================
    def _match_detraccion(self, fac: pd.Series, df_banks: pd.DataFrame):
        det = self._safe_float(fac.get("detraccion_monto"))
        if not det or det <= 0:
            return None

        fecha_emision = fac.get("fecha_emision")
        fecha_limite = fac.get("fecha_limite_pago")

        if pd.isna(fecha_emision) or pd.isna(fecha_limite):
            fecha_ini = fecha_fin = None
        else:
            fecha_ini = fecha_emision
            fecha_fin = fecha_limite + timedelta(days=self.extra_days)

        # Buscar columna Banco
        col_banco = next((c for c in df_banks.columns if c.lower() == "banco"), None)
        if not col_banco:
            warn("Matcher: df_bancos no tiene columna 'Banco' para detracciones.")
            return None

        candidates = df_banks[df_banks[col_banco].astype(str).str.upper() == "BN"].copy()
        if candidates.empty:
            return None

        candidates["diff_monto"] = (candidates["Monto"] - det).abs()
        candidates = candidates[candidates["diff_monto"] <= self.var_monto]
        if candidates.empty:
            return None

        if fecha_ini is not None:
            candidates = candidates[
                (candidates["Fecha"] >= fecha_ini)
                & (candidates["Fecha"] <= fecha_fin)
            ]

        if candidates.empty:
            return None

        candidates["diff_dias"] = (candidates["Fecha"] - fecha_emision).abs().dt.days
        best = candidates.sort_values(by=["diff_monto", "diff_dias"]).iloc[0]

        return {
            "banco_det": best[col_banco],
            "fecha_det": best["Fecha"],
            "monto_det": best["Monto"],
            "operacion_det": best.get("Operacion", ""),
            "descripcion_det": best.get("Descripcion", ""),
        }

    # ============================================================
    #  MEJOR BASE DE MONTO (neto / total / subtotal)
    # ============================================================
    def _compute_best_monto_diff(self, fac, mov):
        neto = self._safe_float(fac.get("neto_recibido"))
        total = self._safe_float(fac.get("total_con_igv"))
        subt = self._safe_float(fac.get("subtotal"))
        monto = self._safe_float(mov.get("Monto"))

        if monto is None:
            return None

        es_usd = bool(mov.get("es_dolares"))
        monto_eq = monto * self.tc_usd_pen if es_usd else monto

        cand = []
        if neto:
            cand.append(("neto", neto, abs(monto_eq - neto)))
        if total:
            cand.append(("total", total, abs(monto_eq - total)))
        if subt:
            cand.append(("subtotal", subt, abs(monto_eq - subt)))

        if not cand:
            return None

        tipo_base, monto_ref, diff = min(cand, key=lambda x: x[2])

        return {
            "tipo_base": tipo_base,
            "monto_ref": monto_ref,
            "monto_banco_equiv": monto_eq,
            "diff_monto": diff,
            "es_usd": es_usd,
        }

    # ============================================================
    #  MATCH PRINCIPAL
    # ============================================================
    def match(self, df_facturas: pd.DataFrame, df_bancos: pd.DataFrame) -> pd.DataFrame:
        info("🔥 Iniciando proceso de matching PulseForge…")

        if df_facturas.empty:
            warn("Matcher: No hay facturas.")
            return pd.DataFrame()

        if df_bancos.empty:
            warn("Matcher: No hay movimientos bancarios.")
            return pd.DataFrame()

        df_banks = df_bancos.copy()

        # BLINDAJE COLUMNA FECHA
        fecha_cols = [c for c in df_banks.columns if c.lower() == "fecha"]
        if not fecha_cols:
            raise KeyError("df_bancos no contiene columna 'Fecha'.")

        base = fecha_cols[0]
        df_banks.rename(columns={base: "Fecha"}, inplace=True)
        df_banks["Fecha"] = pd.to_datetime(df_banks["Fecha"], errors="coerce")

        if "descripcion" in df_banks.columns and "Descripcion" not in df_banks.columns:
            df_banks.rename(columns={"descripcion": "Descripcion"}, inplace=True)

        if "operacion" in df_banks.columns and "Operacion" not in df_banks.columns:
            df_banks.rename(columns={"operacion": "Operacion"}, inplace=True)

        resultados = []

        # ============================================================
        #  LOOP FACTURAS
        # ============================================================
        for _, fac in df_facturas.iterrows():

            fac_id = fac.get("combinada")
            cliente = fac.get("cliente_generador")
            ruc = fac.get("ruc")

            fac_fecha = fac.get("fecha_emision")
            fecha_lim = fac.get("fecha_limite_pago")
            win_ini = fac.get("fecha_inicio_ventana")
            win_fin = fac.get("fecha_fin_ventana")

            # Detracción BN
            det = self._match_detraccion(fac, df_banks)

            # FILTRO POR FECHAS
            candidatos = df_banks.copy()
            if pd.notna(win_ini) and pd.notna(win_fin):
                candidatos = candidatos[
                    (candidatos["Fecha"] >= win_ini - timedelta(days=self.extra_days))
                    & (candidatos["Fecha"] <= win_fin + timedelta(days=self.extra_days))
                ]

            # Si no hay candidatos de fecha → NO_MATCH
            if candidatos.empty:
                resultados.append({
                    "factura": fac_id,
                    "cliente": cliente,
                    "ruc": ruc,
                    "fecha_emision": fac_fecha,
                    "fecha_limite": fecha_lim,
                    "fecha_mov": None,
                    "banco_pago": None,
                    "operacion": None,
                    "monto_banco": None,
                    "monto_banco_equiv": None,
                    "monto_ref_usado": None,
                    "tipo_monto_ref": None,
                    "diferencia_monto": None,
                    "sim_nombre": None,
                    "es_dolares": None,
                    "tiene_terminos_flex": False,
                    "resultado": "NO_MATCH",
                    "banco_det": det["banco_det"] if det else None,
                    "fecha_det": det["fecha_det"] if det else None,
                    "monto_det": det["monto_det"] if det else None,
                    "razon": "Sin movimientos en rango.",
                    "justificacion_ia": None,
                })
                continue

            # ============================================================
            # EVALUAR MOVIMIENTOS (SCORING)
            # ============================================================
            match_list = []

            for _, mov in candidatos.iterrows():

                monto_info = self._compute_best_monto_diff(fac, mov)
                if not monto_info:
                    continue

                if monto_info["diff_monto"] > (self.var_monto * 2):
                    continue

                desc = str(mov.get("Descripcion", "") or "")
                cliente_txt = str(cliente or "")

                # Similitud ▬▬▬▬▬▬▬▬▬▬▬
                sim_regla = self._similarity_basic(cliente_txt, desc)

                sim_ai = 0.0
                if self.use_ai and cliente_txt and desc:
                    try:
                        sim_ai = ai_similarity(cliente_txt, desc)
                    except:
                        sim_ai = sim_regla

                sim_final = max(sim_regla, sim_ai)
                has_flex = self._contains_flex_terms(desc)

                # EXCELENTE SCORE
                score_monto = 1 - (monto_info["diff_monto"] / (self.var_monto * 2))
                score_monto = max(0, min(1, score_monto))

                score = (
                    0.5 * score_monto +
                    0.4 * sim_final +
                    (0.1 if has_flex else 0)
                )

                match_list.append((score, mov, monto_info, sim_final, has_flex))

            # ============================================================
            #  SI NO HAY MATCHES POSIBLES
            # ============================================================
            if not match_list:
                resultados.append({
                    "factura": fac_id,
                    "cliente": cliente,
                    "ruc": ruc,
                    "fecha_emision": fac_fecha,
                    "fecha_limite": fecha_lim,
                    "fecha_mov": None,
                    "banco_pago": None,
                    "operacion": None,
                    "monto_banco": None,
                    "monto_banco_equiv": None,
                    "monto_ref_usado": None,
                    "tipo_monto_ref": None,
                    "diferencia_monto": None,
                    "sim_nombre": None,
                    "es_dolares": None,
                    "tiene_terminos_flex": False,
                    "resultado": "NO_MATCH",
                    "banco_det": det["banco_det"] if det else None,
                    "fecha_det": det["fecha_det"] if det else None,
                    "monto_det": det["monto_det"] if det else None,
                    "razon": "No hay movimientos compatibles.",
                    "justificacion_ia": None,
                })
                continue

            # ============================================================
            #  MEJOR CANDIDATO
            # ============================================================
            score, mov, mi, sim_final, has_flex = max(match_list, key=lambda x: x[0])

            # RESULTADO BASE
            if sim_final >= self.sim_strong:
                res = "MATCH"
            elif sim_final >= self.sim_dudoso or has_flex:
                res = "MATCH_DUDOSO"
            else:
                res = "MATCH_MONTOS_OK_NOMBRE_BAJO"

            just_ia = None

            # ============================================================
            #  IA PARA DECISIÓN FINAL (DUDOSOS)
            # ============================================================
            if self.use_ai and res in ("MATCH_DUDOSO", "MATCH_MONTOS_OK_NOMBRE_BAJO"):

                payload = {
                    "factura": str(fac_id),
                    "cliente": str(cliente or ""),
                    "ruc": str(ruc or ""),
                    "descripcion_banco": str(mov.get("Descripcion", "") or ""),
                    "monto_banco_equiv": float(mi["monto_banco_equiv"]),
                    "monto_ref": float(mi["monto_ref"]),
                    "tipo_monto_ref": str(mi["tipo_base"]),
                    "diff_monto": float(mi["diff_monto"]),
                    "sim_regla": float(sim_final),
                    "sim_ai": float(sim_final),
                    "tiene_terminos_flex": bool(has_flex),
                }

                try:
                    dec = ai_decide_match(payload)
                    decision = dec.get("decision", res)
                    just_ia = dec.get("justificacion")
                    if decision in ("MATCH", "MATCH_DUDOSO", "NO_MATCH"):
                        res = decision
                except Exception as e:
                    warn(f"Matcher: error IA decide_match → {e}")

            # ============================================================
            #  AGREGAR RESULTADO
            # ============================================================
            resultados.append({
                "factura": fac_id,
                "cliente": cliente,
                "ruc": ruc,
                "fecha_emision": fac_fecha,
                "fecha_limite": fecha_lim,
                "fecha_mov": mov["Fecha"],
                "banco_pago": mov.get("Banco", ""),
                "operacion": mov.get("Operacion", ""),
                "monto_banco": mov["Monto"],
                "monto_banco_equiv": mi["monto_banco_equiv"],
                "monto_ref_usado": mi["monto_ref"],
                "tipo_monto_ref": mi["tipo_base"],
                "diferencia_monto": round(mi["diff_monto"], 2),
                "sim_nombre": round(sim_final, 4),
                "es_dolares": mi["es_usd"],
                "tiene_terminos_flex": has_flex,
                "resultado": res,
                "banco_det": det["banco_det"] if det else None,
                "fecha_det": det["fecha_det"] if det else None,
                "monto_det": det["monto_det"] if det else None,
                "razon": res,
                "justificacion_ia": just_ia,
            })

        ok("🧩 Matching PulseForge completado sin errores.")
        return pd.DataFrame(resultados)


# ============================================================
#  TEST BÁSICO DEBUG (NO PRODUCCIÓN)
# ============================================================
if __name__ == "__main__":
    warn("Test manual Matcher — solo para debug interno.")

    df_fac = pd.DataFrame([{
        "combinada": "F001-123",
        "cliente_generador": "DANPER TRUJILLO S.A.C.",
        "ruc": "20123456789",
        "subtotal": 1000.0,
        "fecha_emision": pd.to_datetime("2024-01-01"),
        "fecha_limite_pago": pd.to_datetime("2024-01-31"),
        "fecha_inicio_ventana": pd.to_datetime("2024-01-15"),
        "fecha_fin_ventana": pd.to_datetime("2024-02-15"),
        "neto_recibido": 1180 * 0.96,
        "total_con_igv": 1180,
        "detraccion_monto": 1180 * 0.04,
    }])

    df_bank = pd.DataFrame([
        {
            "Fecha": "2024-01-31",
            "Monto": 1180,
            "moneda": "PEN",
            "Descripcion": "Abono pago factura F001-123 DANPER TRUJILLO SAC",
            "Operacion": "123456",
            "Banco": "BCP",
            "es_dolares": False,
        },
        {
            "Fecha": "2024-02-05",
            "Monto": 47.2,
            "moneda": "PEN",
            "Descripcion": "Depósito detracción SUNAT Danper Trujillo",
            "Operacion": "789012",
            "Banco": "BN",
            "es_dolares": False,
        },
    ])

    m = Matcher()
    print(m.match(df_fac, df_bank))

    ok("TEST Matcher COMPLETADO.")
