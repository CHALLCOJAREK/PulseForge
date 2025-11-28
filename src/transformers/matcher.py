# src/transformers/matcher.py
# --------------------------------------------------------------
#  PULSEFORGE · MATCH ENGINE (ULTRA-BLINDADO + IA)
#  - Matching por monto, fechas y reglas flexibles
#  - Detracciones BN
#  - Soporte USD → PEN
#  - Integración con IA (Gemini Flash + Pro) vía AIHelpers:
#       * similitud de nombres
#       * decisión en casos ambiguos
# --------------------------------------------------------------

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
from difflib import SequenceMatcher
from datetime import timedelta

from src.core.env_loader import get_env
from src.transformers.ai_helpers import AIHelpers

def info(msg):  print(f"🔵 {msg}")
def ok(msg):    print(f"🟢 {msg}")
def warn(msg):  print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class Matcher:

    FLEX_TERMS = [
        "interbancario", "interbank", "interbanco",
        "factoring", "factoraje", "cesion", "cesión",
        "masivo", "pago masivo", "pago facturas",
        "deposito", "depósito", "transferencia", "trf", "trx"
    ]

    # ---------------------------------------------------------------
    def __init__(self):
        self.env = get_env()

        # Reglas "clásicas"
        self.var_monto   = float(self.env.get("MONTO_VARIACION", 0.50))
        self.tc_usd_pen  = float(self.env.get("TIPO_CAMBIO_USD_PEN", 3.8))
        self.extra_days  = int(self.env.get("MATCH_EXTRA_DAYS", 3))

        self.sim_strong  = float(self.env.get("MATCH_SIM_MIN", 0.40))
        self.sim_dudoso  = float(self.env.get("MATCH_SIM_DUDOSO_MIN", 0.25))

        # Flag general IA
        self.use_ai = str(self.env.get("ACTIVAR_IA", "false")).lower() == "true"
        self.ai = None

        if self.use_ai:
            try:
                self.ai = AIHelpers()
                if not getattr(self.ai, "enabled", False):
                    warn("Matcher: AIHelpers deshabilitado (sin API key válida).")
                    self.use_ai = False
            except Exception as e:
                warn(f"Matcher: error iniciando AIHelpers: {e}")
                self.use_ai = False

        ok("Matcher ultra-blindado inicializado ✔️")

    # ---------------------------------------------------------------
    def _similarity(self, a: str, b: str) -> float:
        a = (a or "").lower()
        b = (b or "").lower()
        if not a or not b:
            return 0.0
        return SequenceMatcher(None, a, b).ratio()

    # ---------------------------------------------------------------
    @staticmethod
    def _safe_float(v):
        try:
            return float(v)
        except:
            return None

    # ---------------------------------------------------------------
    def _contains_flex_terms(self, text: str) -> bool:
        t = (text or "").lower()
        return any(term in t for term in self.FLEX_TERMS)

    # ===================================================================
    #  RULE — DETRACCIÓN BN
    # ===================================================================
    def _match_detraccion(self, fac, df_banks):

        det = self._safe_float(fac.get("detraccion_monto"))
        if not det or det <= 0:
            return None

        fecha_emision = fac.get("fecha_emision")
        fecha_limite  = fac.get("fecha_limite_pago")

        if not pd.isna(fecha_emision) and not pd.isna(fecha_limite):
            fecha_ini = fecha_emision
            fecha_fin = fecha_limite + timedelta(days=self.extra_days)
        else:
            fecha_ini = fecha_fin = None

        # Buscar columna Banco
        col_banco = None
        for c in df_banks.columns:
            if c.lower() == "banco":
                col_banco = c
                break

        if col_banco is None:
            warn("No existe columna 'Banco' en df_banks para detracción.")
            return None

        candidates = df_banks[df_banks[col_banco].str.upper() == "BN"].copy()
        if candidates.empty:
            return None

        # Filtro por monto (detracción)
        candidates["diff_monto"] = (candidates["Monto"] - det).abs()
        candidates = candidates[candidates["diff_monto"] <= self.var_monto]
        if candidates.empty:
            return None

        # Filtro por fecha si aplica
        if fecha_ini is not None:
            candidates = candidates[
                (candidates["Fecha"] >= fecha_ini) &
                (candidates["Fecha"] <= fecha_fin)
            ]

        if candidates.empty:
            return None

        candidates["diff_dias"] = (candidates["Fecha"] - fecha_emision).abs().dt.days

        best = candidates.sort_values(by=["diff_monto", "diff_dias"]).iloc[0]

        return {
            "banco_det":       best[col_banco],
            "fecha_det":       best["Fecha"],
            "monto_det":       best["Monto"],
            "operacion_det":   best.get("Operacion", ""),
            "descripcion_det": best.get("Descripcion", ""),
        }

    # ===================================================================
    #  USD → PEN + mejor base de monto (neto / total / subtotal)
    # ===================================================================
    def _compute_best_monto_diff(self, fac, mov):

        neto  = self._safe_float(fac.get("neto_recibido"))
        total = self._safe_float(fac.get("total_con_igv"))
        subt  = self._safe_float(fac.get("subtotal"))
        monto = self._safe_float(mov.get("Monto"))

        if monto is None:
            return None

        es_usd   = bool(mov.get("es_dolares"))
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

        mejor = min(cand, key=lambda x: x[2])

        return {
            "tipo_base":          mejor[0],
            "monto_ref":          mejor[1],
            "monto_banco_equiv":  monto_eq,
            "diff_monto":         mejor[2],
            "es_usd":             es_usd
        }

    # ===================================================================
    #  MATCH PRINCIPAL
    # ===================================================================
    def match(self, df_facturas: pd.DataFrame, df_bancos: pd.DataFrame):

        info("🔥 Iniciando matching ultra-blindado...")

        if df_facturas.empty:
            warn("No hay facturas para procesar.")
            return pd.DataFrame()

        if df_bancos.empty:
            warn("No hay movimientos bancarios para procesar.")
            return pd.DataFrame()

        df_banks = df_bancos.copy()

        # ---------------------------------------------
        # BLINDAJE TOTAL COLUMNA FECHA
        # ---------------------------------------------
        fecha_cols = [c for c in df_banks.columns if c.lower() == "fecha"]

        if len(fecha_cols) == 0:
            error("df_banks NO TIENE ninguna columna 'Fecha'.")
            raise KeyError("df_banks no contiene columna Fecha")

        base = fecha_cols[0]
        for c in fecha_cols[1:]:
            if c in df_banks.columns:
                df_banks.drop(columns=[c], inplace=True)

        df_banks.rename(columns={base: "Fecha"}, inplace=True)
        df_banks["Fecha"] = pd.to_datetime(df_banks["Fecha"], errors="coerce")

        if "fecha_mov" in df_banks.columns:
            df_banks.drop(columns=["fecha_mov"], inplace=True)

        if "descripcion" in df_banks.columns:
            df_banks.rename(columns={"descripcion": "Descripcion"}, inplace=True)

        if "operacion" in df_banks.columns:
            df_banks.rename(columns={"operacion": "Operacion"}, inplace=True)

        resultados = []

        # =====================================================
        # LOOP FACTURAS
        # =====================================================
        for _, fac in df_facturas.iterrows():

            fac_id  = fac.get("combinada")
            cliente = fac.get("cliente_generador")
            ruc     = fac.get("ruc")

            fac_fecha = fac.get("fecha_emision")
            fecha_lim = fac.get("fecha_limite_pago")
            win_ini   = fac.get("fecha_inicio_ventana")
            win_fin   = fac.get("fecha_fin_ventana")

            # Detracción
            det = self._match_detraccion(fac, df_banks)

            # Rango de fechas para el pago principal
            candidatos = df_banks.copy()
            if not pd.isna(win_ini) and not pd.isna(win_fin):
                candidatos = candidatos[
                    (candidatos["Fecha"] >= win_ini - timedelta(days=self.extra_days)) &
                    (candidatos["Fecha"] <= win_fin + timedelta(days=self.extra_days))
                ]

            if candidatos.empty:
                resultados.append({
                    "factura":        fac_id,
                    "cliente":        cliente,
                    "ruc":            ruc,
                    "fecha_emision":  fac_fecha,
                    "fecha_limite":   fecha_lim,
                    "fecha_mov":      None,
                    "banco_pago":     None,
                    "operacion":      None,
                    "monto_banco":    None,
                    "monto_banco_equiv": None,
                    "monto_ref_usado":   None,
                    "tipo_monto_ref":    None,
                    "diferencia_monto":   None,
                    "sim_nombre":         None,
                    "es_dolares":         None,
                    "tiene_terminos_flex": False,
                    "resultado":          "NO_MATCH",
                    "banco_det":          det["banco_det"] if det else None,
                    "fecha_det":          det["fecha_det"] if det else None,
                    "monto_det":          det["monto_det"] if det else None,
                    "razon":             "Sin movimientos en rango."
                })
                continue

            # MATCHING FLEXIBLE
            match_list = []

            for _, mov in candidatos.iterrows():

                monto_info = self._compute_best_monto_diff(fac, mov)
                if not monto_info:
                    continue

                # Filtro duro por monto
                if monto_info["diff_monto"] > (self.var_monto * 2):
                    continue

                desc = str(mov.get("Descripcion", "") or "")

                # Similitud "clásica"
                sim_regla = self._similarity(desc, cliente or "")

                # Similitud IA (si está activa)
                sim_ai = 0.0
                if self.use_ai and cliente:
                    try:
                        sim_ai = self.ai.similitud_nombres(cliente, desc)
                    except Exception as e:
                        warn(f"Error IA similitud_nombres: {e}")
                        sim_ai = 0.0

                # Similitud final (usamos lo mejor de ambos mundos)
                sim = max(sim_regla, sim_ai)

                has_flex = self._contains_flex_terms(desc)

                # Scoring combinado: monto + nombre + palabras clave
                score = (
                    0.5 * (1 - (monto_info["diff_monto"] / (self.var_monto * 2))) +
                    0.4 * sim +
                    (0.15 if has_flex else 0)
                )

                match_list.append((score, mov, monto_info, sim, has_flex, sim_regla, sim_ai))

            # Si no hay candidatos compatibles
            if not match_list:
                resultados.append({
                    "factura":        fac_id,
                    "cliente":        cliente,
                    "ruc":            ruc,
                    "fecha_emision":  fac_fecha,
                    "fecha_limite":   fecha_lim,
                    "fecha_mov":      None,
                    "banco_pago":     None,
                    "operacion":      None,
                    "monto_banco":    None,
                    "monto_banco_equiv": None,
                    "monto_ref_usado":   None,
                    "tipo_monto_ref":    None,
                    "diferencia_monto":   None,
                    "sim_nombre":         None,
                    "es_dolares":         None,
                    "tiene_terminos_flex": False,
                    "resultado":          "NO_MATCH",
                    "banco_det":          det["banco_det"] if det else None,
                    "fecha_det":          det["fecha_det"] if det else None,
                    "monto_det":          det["monto_det"] if det else None,
                    "razon":             "No hay movimientos compatibles."
                })
                continue

            # Tomar el mejor score
            score, mov, mi, sim, has_flex, sim_regla, sim_ai = max(
                match_list, key=lambda x: x[0]
            )

            # Clasificación base
            if sim >= self.sim_strong:
                res = "MATCH"
            elif sim >= self.sim_dudoso or has_flex:
                res = "MATCH_DUDOSO"
            else:
                res = "MATCH_MONTOS_OK_NOMBRE_BAJO"

            # IA para afinar decisión en casos dudosos
            if self.use_ai and res in ("MATCH_DUDOSO", "MATCH_MONTOS_OK_NOMBRE_BAJO"):
                payload = {
                    "factura":         str(fac_id),
                    "cliente":         str(cliente),
                    "ruc":             str(ruc),
                    "descripcion_banco": str(mov.get("Descripcion", "") or ""),
                    "monto_banco_equiv": mi["monto_banco_equiv"],
                    "monto_ref":         mi["monto_ref"],
                    "tipo_monto_ref":    mi["tipo_base"],
                    "diff_monto":        mi["diff_monto"],
                    "sim_regla":         sim_regla,
                    "sim_ai":            sim_ai,
                    "tiene_terminos_flex": has_flex,
                }
                try:
                    decision_ai = self.ai.decidir_match_ambiguo(payload)
                    if decision_ai in ("MATCH", "MATCH_DUDOSO", "NO_MATCH"):
                        res = decision_ai
                except Exception as e:
                    warn(f"Error IA decidir_match_ambiguo: {e}")

            resultados.append({
                "factura":        fac_id,
                "cliente":        cliente,
                "ruc":            ruc,
                "fecha_emision":  fac_fecha,
                "fecha_limite":   fecha_lim,
                "fecha_mov":      mov["Fecha"],
                "banco_pago":     mov.get("Banco", ""),
                "operacion":      mov.get("Operacion", ""),
                "monto_banco":    mov["Monto"],
                "monto_banco_equiv": mi["monto_banco_equiv"],
                "monto_ref_usado":   mi["monto_ref"],
                "tipo_monto_ref":    mi["tipo_base"],
                "diferencia_monto":  round(mi["diff_monto"], 2),
                "sim_nombre":        round(sim, 4),
                "es_dolares":        mi["es_usd"],
                "tiene_terminos_flex": has_flex,
                "resultado":         res,
                "banco_det":         det["banco_det"] if det else None,
                "fecha_det":         det["fecha_det"] if det else None,
                "monto_det":         det["monto_det"] if det else None,
                "razon":             res if res != "NO_MATCH" else "No hay movimientos compatibles."
            })

        ok("🧩 Matching ULTRA-BLINDADO completado sin errores.")
        return pd.DataFrame(resultados)


if __name__ == "__main__":
    warn("Test directo. No usar en producción.")
