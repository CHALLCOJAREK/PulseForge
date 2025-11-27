# src/transformers/matcher.py

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
from difflib import SequenceMatcher
from datetime import timedelta

from src.core.env_loader import get_env

# Prints estilo Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class Matcher:
    """
    Aplica las 8 reglas de negocio para unir facturas contra movimientos bancarios
    con enfoque HIGH-FLEX:
      1) Fecha probable de pago (fecha_emision + forma_pago ± tolerancia)
      2) Monto con variación ± MONTO_VARIACION (neto, total, subtotal, detracción)
      3) Cuando se encuentra monto → se asigna banco
      4) Validar descripción vs nombre de cliente (IA opcional)
      5) Flexibilizar si hay términos como "interbancario", "factoring", etc.
      6) Manejar pagos en otra moneda (USD → PEN)
      7) Buscar primero detracción en BN
      8) Cuando coincide monto → marcar factura como cobrada (MATCH / MATCH_DUDOSO)
    """

    FLEX_TERMS = [
        "interbancario", "interbank", "interbanco",
        "factoring", "factoraje", "cesion", "cesión",
        "masivo", "pago masivo", "pago facturas", "pago facturas",
        "deposito", "depósito", "transferencia", "trf", "trx"
    ]

    def __init__(self):
        self.env = get_env()

        # Flags IA
        self.use_ai = str(self.env.get("ACTIVAR_IA", "false")).lower() == "true"

        # Parámetros
        self.var_monto = float(self.env.get("MONTO_VARIACION", 0.50))
        self.tc_usd_pen = float(self.env.get("TIPO_CAMBIO_USD_PEN", 3.8))
        self.extra_days = int(self.env.get("MATCH_EXTRA_DAYS", 3))

        # Similitudes mínimas
        self.sim_strong = float(self.env.get("MATCH_SIM_MIN", 0.40))          # MATCH firme
        self.sim_dudoso = float(self.env.get("MATCH_SIM_DUDOSO_MIN", 0.25))   # MATCH_DUDOSO

        ok("Matcher inicializado correctamente (modo HIGH-FLEX).")

    # =======================================================
    #   IA / Similitud de texto
    # =======================================================
    def _similarity_ai(self, a: str, b: str) -> float:
        """
        Regla 4: IA opcional para comparar nombres.
        Si no hay IA, usa SequenceMatcher.
        """
        a = (a or "").lower()
        b = (b or "").lower()

        if not a or not b:
            return 0.0

        if not self.use_ai:
            return SequenceMatcher(None, a, b).ratio()

        try:
            import google.generativeai as genai
            genai.configure(api_key=self.env.get("API_GEMINI_KEY"))

            prompt = f"""
            Compara estos dos textos (nombre de cliente vs descripción bancaria)
            y responde SOLO un número entre 0 y 1 indicando qué tan similares son.

            Texto A: {a}
            Texto B: {b}
            """

            response = genai.GenerativeModel("gemini-pro").generate_content(prompt)
            score = float(response.text.strip())
            return max(0.0, min(1.0, score))
        except Exception as e:
            warn(f"IA falló ({e}), usando similitud simple.")
            return SequenceMatcher(None, a, b).ratio()

    # =======================================================
    #   Helpers de flexibilidad
    # =======================================================
    @staticmethod
    def _safe_float(v):
        try:
            return float(v)
        except Exception:
            return None

    def _contains_flex_terms(self, text: str) -> bool:
        t = (text or "").lower()
        return any(term in t for term in self.FLEX_TERMS)

    # =======================================================
    #   Regla 7 — Buscar Detracción en Banco de la Nación
    # =======================================================
    def _match_detraccion(self, fac, df_bancos):
        """
        Busca la detracción en banco BN (alias de Banco Nación).
        Retorna dict con datos de la detracción o None.
        """

        det = self._safe_float(fac.get("detraccion_monto"))
        if not det or det <= 0:
            return None

        fecha_emision = fac.get("fecha_emision")
        fecha_limite  = fac.get("fecha_limite_pago")

        if pd.isna(fecha_emision) or pd.isna(fecha_limite):
            fecha_ini = None
            fecha_fin = None
        else:
            fecha_ini = fecha_emision
            fecha_fin = fecha_limite + timedelta(days=self.extra_days)

        candidatos = df_bancos[df_bancos["Banco"] == "BN"].copy()
        if candidatos.empty:
            return None

        # Filtro por monto (alta precisión)
        candidatos["diff_monto"] = (candidatos["Monto"] - det).abs()
        candidatos = candidatos[candidatos["diff_monto"] <= self.var_monto]

        if candidatos.empty:
            return None

        # Filtro por fecha (si tenemos ventana)
        if fecha_ini is not None and fecha_fin is not None:
            candidatos = candidatos[
                (candidatos["Fecha"] >= fecha_ini) &
                (candidatos["Fecha"] <= fecha_fin)
            ]

        if candidatos.empty:
            return None

        # Escogemos el más cercano en monto y fecha
        candidatos["diff_dias"] = (candidatos["Fecha"] - fecha_emision).abs().dt.days
        candidatos = candidatos.sort_values(by=["diff_monto", "diff_dias"])

        best = candidatos.iloc[0]

        return {
            "banco_det": best["Banco"],
            "fecha_det": best["Fecha"],
            "monto_det": best["Monto"],
            "operacion_det": best.get("Operacion", ""),
            "descripcion_det": best.get("Descripcion", ""),
            "source_table_det": best.get("source_table", ""),
            "rowid_det": best.get("origen_rowid", None),
        }

    # =======================================================
    #   Regla 6 — Manejar USD → PEN
    # =======================================================
    def _compute_best_monto_diff(self, fac, mov):
        """
        Devuelve:
          - mejor_monto_ref (neto / total / subtotal)
          - monto_banco_usado (PEN o USD convertido)
          - diff_monto
          - flag_usd
        Usa:
          neto_recibido, total_con_igv, subtotal
        """

        neto    = self._safe_float(fac.get("neto_recibido"))
        total   = self._safe_float(fac.get("total_con_igv"))
        subt    = self._safe_float(fac.get("subtotal"))
        monto   = self._safe_float(mov.get("Monto"))

        if monto is None:
            return None

        es_usd = bool(mov.get("es_dolares", False))

        # monto en PEN (si viene en USD, lo convertimos)
        if es_usd:
            monto_equiv = monto * self.tc_usd_pen
        else:
            monto_equiv = monto

        candidatos = []
        if neto is not None:
            candidatos.append(("neto", neto, abs(monto_equiv - neto)))
        if total is not None:
            candidatos.append(("total", total, abs(monto_equiv - total)))
        if subt is not None:
            candidatos.append(("subtotal", subt, abs(monto_equiv - subt)))

        if not candidatos:
            return None

        mejor = min(candidatos, key=lambda x: x[2])  # menor diferencia

        return {
            "tipo_base": mejor[0],
            "monto_ref": mejor[1],
            "monto_banco_equiv": monto_equiv,
            "diff_monto": mejor[2],
            "es_usd": es_usd,
        }

    # =======================================================
    #   MATCH PRINCIPAL (Reglas 1–8)
    # =======================================================
    def match(self, df_facturas: pd.DataFrame, df_bancos: pd.DataFrame):
        """
        Devuelve un DataFrame con columnas principales:

          factura
          cliente
          ruc
          fecha_emision
          fecha_limite
          banco_pago
          fecha_mov
          monto_banco
          monto_banco_equiv
          monto_ref_usado
          tipo_monto_ref
          diferencia_monto
          sim_nombre
          es_dolares
          tiene_terminos_flex
          resultado
          razon

        Además incluye info de detracción:

          banco_det
          fecha_det
          monto_det
        """

        info("🔥 Iniciando proceso de matching (modo HIGH-FLEX)...")

        if df_facturas.empty:
            warn("No hay facturas para procesar.")
            return pd.DataFrame()

        if df_bancos.empty:
            warn("No hay movimientos bancarios.")
            return pd.DataFrame()

        resultados = []

        # Aseguramos tipos clave
        df_banks = df_bancos.copy()
        df_banks["Fecha"] = pd.to_datetime(df_banks["Fecha"], errors="coerce")

        # =======================================================
        #   Iteramos factura x factura
        # =======================================================
        for _, fac in df_facturas.iterrows():
            fac_id    = fac.get("combinada")
            cliente   = fac.get("cliente_generador")
            ruc       = fac.get("ruc")
            fac_fecha = fac.get("fecha_emision")
            fecha_lim = fac.get("fecha_limite_pago")
            ventana_ini = fac.get("fecha_inicio_ventana")
            ventana_fin = fac.get("fecha_fin_ventana")

            # -----------------------------------------------
            # Regla 7 — Buscar detracción primero
            # -----------------------------------------------
            det_match = self._match_detraccion(fac, df_banks)

            # -----------------------------------------------
            # Regla 1 + 2 + 6 — candidatos por fecha y monto
            # -----------------------------------------------
            candidatos = df_banks.copy()

            # Fecha dentro de ventana ± extra_days
            if not pd.isna(ventana_ini) and not pd.isna(ventana_fin):
                candidatos = candidatos[
                    (candidatos["Fecha"] >= ventana_ini - timedelta(days=self.extra_days)) &
                    (candidatos["Fecha"] <= ventana_fin + timedelta(days=self.extra_days))
                ]

            if candidatos.empty:
                resultados.append({
                    "factura": fac_id,
                    "cliente": cliente,
                    "ruc": ruc,
                    "fecha_emision": fac_fecha,
                    "fecha_limite": fecha_lim,
                    "banco_pago": None,
                    "fecha_mov": None,
                    "monto_banco": None,
                    "monto_banco_equiv": None,
                    "monto_ref_usado": None,
                    "tipo_monto_ref": None,
                    "diferencia_monto": None,
                    "sim_nombre": 0.0,
                    "es_dolares": None,
                    "tiene_terminos_flex": False,
                    "resultado": "NO_MATCH",
                    "razon": "Sin movimientos en ventana de fechas.",
                    "banco_det": det_match["banco_det"] if det_match else None,
                    "fecha_det": det_match["fecha_det"] if det_match else None,
                    "monto_det": det_match["monto_det"] if det_match else None,
                })
                continue

            # Calculamos mejor diferencia de monto para cada candidato
            match_rows = []
            for _, mov in candidatos.iterrows():
                monto_info = self._compute_best_monto_diff(fac, mov)
                if not monto_info:
                    continue

                # Regla 2 — variación de monto (HIGH-FLEX → hasta 2x variación)
                if monto_info["diff_monto"] > (self.var_monto * 2):
                    continue

                desc = mov.get("Descripcion", "") or ""
                sim  = self._similarity_ai(desc, cliente or "")
                has_flex_terms = self._contains_flex_terms(desc)

                # Scoring compuesto
                # -------------------------------------
                # score_monto: cuanto más cerca, mejor
                # score_nombre: similitud IA/texto
                # score_bono_flex: si hay términos "factoring", etc.
                # -------------------------------------
                max_diff = self.var_monto * 2 or 1.0
                score_monto = max(0.0, 1.0 - (monto_info["diff_monto"] / max_diff))
                score_nombre = sim
                score_bono_flex = 0.15 if has_flex_terms else 0.0

                score_total = (0.5 * score_monto) + (0.4 * score_nombre) + score_bono_flex

                match_rows.append({
                    "mov": mov,
                    "monto_info": monto_info,
                    "sim": sim,
                    "has_flex_terms": has_flex_terms,
                    "score_total": score_total,
                })

            if not match_rows:
                resultados.append({
                    "factura": fac_id,
                    "cliente": cliente,
                    "ruc": ruc,
                    "fecha_emision": fac_fecha,
                    "fecha_limite": fecha_lim,
                    "banco_pago": None,
                    "fecha_mov": None,
                    "monto_banco": None,
                    "monto_banco_equiv": None,
                    "monto_ref_usado": None,
                    "tipo_monto_ref": None,
                    "diferencia_monto": None,
                    "sim_nombre": 0.0,
                    "es_dolares": None,
                    "tiene_terminos_flex": False,
                    "resultado": "NO_MATCH",
                    "razon": "No hay movimientos que cumplan rango de monto.",
                    "banco_det": det_match["banco_det"] if det_match else None,
                    "fecha_det": det_match["fecha_det"] if det_match else None,
                    "monto_det": det_match["monto_det"] if det_match else None,
                })
                continue

            # Elegimos el candidato con mejor score_total
            best = max(match_rows, key=lambda x: x["score_total"])
            mov_best = best["mov"]
            mi = best["monto_info"]
            sim = best["sim"]
            has_flex_terms = best["has_flex_terms"]

            # -----------------------------------------------
            # Regla 8 — Clasificación del resultado
            # -----------------------------------------------
            if sim >= self.sim_strong:
                resultado = "MATCH"
                razon = "Monto y nombre consistentes."
            elif sim >= self.sim_dudoso or has_flex_terms:
                resultado = "MATCH_DUDOSO"
                razon = "Monto y fecha OK, nombre débil o movimiento tipo factoring/interbancario."
            else:
                resultado = "MATCH_MONTOS_OK_NOMBRE_BAJO"
                razon = "Monto dentro de rango pero el nombre casi no coincide."

            resultados.append({
                "factura": fac_id,
                "cliente": cliente,
                "ruc": ruc,
                "fecha_emision": fac_fecha,
                "fecha_limite": fecha_lim,
                "fecha_mov": mov_best["Fecha"],
                "banco_pago": mov_best["Banco"],
                "operacion": mov_best.get("Operacion", ""),
                "monto_banco": mov_best["Monto"],
                "monto_banco_equiv": mi["monto_banco_equiv"],
                "monto_ref_usado": mi["monto_ref"],
                "tipo_monto_ref": mi["tipo_base"],  # neto / total / subtotal
                "diferencia_monto": round(mi["diff_monto"], 2),
                "sim_nombre": round(sim, 4),
                "es_dolares": mi["es_usd"],
                "tiene_terminos_flex": has_flex_terms,
                "resultado": resultado,
                "razon": razon,
                # Detracción asociada (si la hay)
                "banco_det": det_match["banco_det"] if det_match else None,
                "fecha_det": det_match["fecha_det"] if det_match else None,
                "monto_det": det_match["monto_det"] if det_match else None,
            })

        ok("Matching completado (modo HIGH-FLEX). 🧩")
        return pd.DataFrame(resultados)


# =======================================================
#   TEST DIRECTO (opcional)
# =======================================================
if __name__ == "__main__":
    warn("Test directo del Matcher. No usar en producción.")
