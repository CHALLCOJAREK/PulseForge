# src/transformers/matcher.py
from __future__ import annotations

import sys
from pathlib import Path
from datetime import timedelta
from difflib import SequenceMatcher
from typing import Any, Optional, Dict

import pandas as pd

# === BOOTSTRAP RUTAS ===
CURRENT_FILE = Path(__file__).resolve()
ROOT = CURRENT_FILE.parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# === IMPORTS CORE ===
from src.core.logger import warn
from src.core.env_loader import get_env
from src.transformers.ai_helpers import ai_similarity, ai_decide_match


class Matcher:
    """
    MATCHER ENTERPRISE V4 – alineado con:
        - Calculator V3
        - MatcherEngine V3
        - MatchWriter Enterprise
        - DB schema newdb_builder.py

    No logs internos. Todo optimizado para rendimiento.
    """

    FLEX_TERMS = {
        "interbancario", "interbank", "interbanco",
        "factoring", "factoraje", "cesion", "cesión",
        "masivo", "pago masivo", "pago facturas",
        "deposito", "depósito", "transferencia", "trf", "trx",
    }

    # ============================================================
    # INIT
    # ============================================================
    def __init__(self):
        self.var_monto = float(get_env("MONTO_VARIACION", default=0.50))
        self.tc_usd_pen = float(get_env("TIPO_CAMBIO_USD_PEN", default=3.8))
        self.extra_days = int(get_env("MATCH_EXTRA_DAYS", default=3))

        self.sim_strong = float(get_env("MATCH_SIM_MIN", default=0.55))
        self.sim_dudoso = float(get_env("MATCH_SIM_DUDOSO_MIN", default=0.35))

        activar_ia = str(get_env("ACTIVAR_IA", default="true")).lower()
        self.use_ai = activar_ia in ("1", "true", "yes", "on")

    # ============================================================
    # HELPERS
    # ============================================================
    @staticmethod
    def _similarity_basic(a: str, b: str) -> float:
        a = (a or "").strip().lower()
        b = (b or "").strip().lower()
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
        text = (text or "").lower()
        return any(term in text for term in self.FLEX_TERMS)

    # ============================================================
    # DETRACCIÓN BN
    # ============================================================
    def _match_detraccion(self, fac: pd.Series, banks: pd.DataFrame):
        det = self._safe_float(fac.get("detraccion_monto"))
        if not det:
            return None

        fecha_ini = fac.get("fecha_emision")
        fecha_fin = fac.get("fecha_limite_pago")

        if pd.isna(fecha_ini) or pd.isna(fecha_fin):
            return None

        fecha_fin = fecha_fin + timedelta(days=self.extra_days)

        bn = banks[banks["Banco"].astype(str).str.upper() == "BN"]
        if bn.empty:
            return None

        bn = bn.assign(diff_monto=(bn["Monto"] - det).abs())
        bn = bn[bn["diff_monto"] <= self.var_monto]

        if bn.empty:
            return None

        bn = bn[(bn["Fecha"] >= fecha_ini) & (bn["Fecha"] <= fecha_fin)]
        if bn.empty:
            return None

        best = bn.sort_values("diff_monto").iloc[0]

        return {
            "banco_det": best.get("Banco"),
            "fecha_det": best.get("Fecha"),
            "monto_det": best.get("Monto"),
            "operacion_det": best.get("Operacion", ""),
            "descripcion_det": best.get("Descripcion", ""),
        }

    # ============================================================
    # EVALUACIÓN DE MONTO
    # ============================================================
    def _compute_best_monto_diff(self, fac: pd.Series, mov: pd.Series):
        monto = self._safe_float(mov.get("Monto"))
        if monto is None:
            return None

        es_usd = bool(mov.get("es_dolares"))
        monto_eq = monto * self.tc_usd_pen if es_usd else monto

        refs = (
            ("neto", fac.get("neto_recibido")),
            ("total", fac.get("total_con_igv")),
            ("subtotal", fac.get("subtotal")),
        )

        candidatos = []
        for nombre, val in refs:
            val_f = self._safe_float(val)
            if val_f:
                diff = abs(monto_eq - val_f)
                candidatos.append((nombre, val_f, diff))

        if not candidatos:
            return None

        tipo, ref, diff = min(candidatos, key=lambda x: x[2])

        return {
            "tipo_base": tipo,
            "monto_ref": ref,
            "monto_banco_equivalente": monto_eq,
            "diff_monto": diff,
            "es_usd": es_usd,
        }

    # ============================================================
    # MATCH PRINCIPAL
    # ============================================================
    def match(self, df_facturas: pd.DataFrame, df_bancos: pd.DataFrame):

        banks = df_bancos.copy()

        # Normalizar columna Banco
        col_banco = next((c for c in banks.columns if c.lower() == "banco"), None)
        if col_banco and col_banco != "Banco":
            banks.rename(columns={col_banco: "Banco"}, inplace=True)
        elif not col_banco:
            warn("Columna 'Banco' no encontrada → asignando DESCONOCIDO")
            banks["Banco"] = "DESCONOCIDO"

        # Asegurar columna Fecha
        if "Fecha" not in banks:
            base = next((c for c in banks.columns if c.lower() == "fecha"), None)
            if base:
                banks["Fecha"] = pd.to_datetime(banks[base], errors="coerce")

        rows_match = []
        rows_detalles = []

        # ============================================================
        # LOOP FACTURA X FACTURA
        # ============================================================
        for _, fac in df_facturas.iterrows():

            factura_id = fac.get("id") or fac.get("factura_id") or fac.get("combinada")
            cliente = fac.get("cliente_generador") or ""
            ruc = fac.get("ruc")

            fac_fecha = fac.get("fecha_emision")
            fac_lim = fac.get("fecha_limite_pago")

            win_ini = fac.get("fecha_inicio_ventana")
            win_fin = fac.get("fecha_fin_ventana")

            det = self._match_detraccion(fac, banks)

            candidatos = banks
            if pd.notna(win_ini) and pd.notna(win_fin):
                extra = timedelta(days=self.extra_days)
                candidatos = candidatos[
                    (candidatos["Fecha"] >= win_ini - extra)
                    & (candidatos["Fecha"] <= win_fin + extra)
                ]

            # SIN CANDIDATOS → NO MATCH
            if candidatos.empty:
                rows_match.append({
                    "factura_id": factura_id,
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
                    "score_similitud": None,
                    "razon_ia": "Sin movimientos en ventana.",
                    "match_tipo": "NO_MATCH",
                })
                continue

            mejores = []

            # ============================================================
            # LOOP CANDIDATOS
            # ============================================================
            for _, mov in candidatos.iterrows():

                # Eval monto
                monto_info = self._compute_best_monto_diff(fac, mov)
                if not monto_info:
                    continue

                if monto_info["diff_monto"] > self.var_monto * 2:
                    continue

                desc = str(mov.get("Descripcion") or "")
                sim_regla = self._similarity_basic(cliente, desc)

                # IA opcional
                if self.use_ai and 0.20 < sim_regla < 0.85:
                    sim_ai = ai_similarity(cliente, desc)
                else:
                    sim_ai = sim_regla

                sim_final = max(sim_regla, sim_ai)
                flex_flag = self._contains_flex_terms(desc)

                score_monto = 1 - (monto_info["diff_monto"] / (self.var_monto * 2))
                score = 0.5 * score_monto + 0.4 * sim_final + (0.1 if flex_flag else 0)

                mejores.append((score, mov, monto_info, sim_final, flex_flag))

                rows_detalles.append({
                    "factura_id": factura_id,
                    "combinada": fac.get("combinada"),
                    "serie": fac.get("serie"),
                    "numero": fac.get("numero"),
                    "ruc": ruc,
                    "cliente": cliente,
                    "movimiento_id": mov.get("id") or mov.get("Movimiento_ID") or None,
                    "fecha_mov": mov.get("Fecha"),
                    "banco_codigo": mov.get("Banco"),
                    "operacion": mov.get("Operacion"),
                    "descripcion_banco": desc,
                    "moneda": mov.get("Moneda"),
                    "monto_banco": mov.get("Monto"),
                    "monto_banco_equivalente": monto_info["monto_banco_equivalente"],
                    "es_dolares": monto_info["es_usd"],
                    "tipo_monto_ref": monto_info["tipo_base"],
                    "monto_ref": monto_info["monto_ref"],
                    "diff_monto": monto_info["diff_monto"],
                    "match_por_monto": int(monto_info["diff_monto"] <= self.var_monto),
                    "sim_nombre_regla": sim_regla,
                    "sim_nombre_regla": sim_regla,
                    "sim_nombre_ia": sim_ai,
                    "sim_nombre_max": sim_final,
                    "tiene_terminos_flex": int(flex_flag),
                    "ventana_inicio": win_ini,
                    "ventana_fin": win_fin,
                    "fecha_emision": fac_fecha,
                    "fecha_limite_pago": fac_lim,
                    "es_detraccion_bn": int(mov.get("Banco") == "BN"),
                    "coincide_detraccion": int(det is not None),
                    "score_final": score,
                    "resultado_final": None
                })

            # SIN COINCIDENCIAS → NO_MATCH
            if not mejores:
                rows_match.append({
                    "factura_id": factura_id,
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
                    "score_similitud": None,
                    "razon_ia": "Movimiento incompatible.",
                    "match_tipo": "NO_MATCH",
                })
                continue

            # ============================================================
            # TOMAR EL MEJOR
            # ============================================================
            score_best, mov_best, mi, sim_final, flex_flag = max(mejores, key=lambda x: x[0])

            # Categoría inicial
            if sim_final >= self.sim_strong:
                categoria = "MATCH"
            elif sim_final >= self.sim_dudoso or flex_flag:
                categoria = "MATCH_DUDOSO"
            else:
                categoria = "MATCH_MONTOS_OK_NOMBRE_BAJO"

            # IA overrides
            just_ia = None
            if self.use_ai and categoria != "MATCH":
                try:
                    dec = ai_decide_match({
                        "factura": factura_id,
                        "cliente": cliente,
                        "ruc": ruc,
                        "descripcion_banco": mov_best.get("Descripcion", ""),
                        "monto_banco_equivalente": float(mi["monto_banco_equivalente"]),
                        "monto_ref": float(mi["monto_ref"]),
                        "tipo_monto_ref": str(mi["tipo_base"]),
                        "diff_monto": float(mi["diff_monto"]),
                        "sim_regla": float(sim_final),
                        "sim_ai": float(sim_final),
                        "tiene_terminos_flex": bool(flex_flag),
                    })
                    categoria = dec.get("decision", categoria)
                    just_ia = dec.get("justificacion", "")
                except:
                    just_ia = ""

            # ============================================================
            # MATCH ROW (RESUMEN)
            # ============================================================
            rows_match.append({
                "factura_id": factura_id,
                "movimiento_id": mov_best.get("id") or mov_best.get("Movimiento_ID") or None,
                "subtotal": fac.get("subtotal"),
                "igv": fac.get("igv"),
                "total_con_igv": fac.get("total_con_igv"),
                "detraccion_monto": fac.get("detraccion_monto"),
                "neto_recibido": fac.get("neto_recibido"),
                "monto_banco": mov_best.get("Monto"),
                "monto_banco_equivalente": mi["monto_banco_equivalente"],
                "monto_detraccion_banco": det["monto_det"] if det else None,
                "variacion_monto": round(mi["diff_monto"], 2),
                "fecha_mov": mov_best.get("Fecha"),
                "banco_pago": mov_best.get("Banco"),
                "operacion": mov_best.get("Operacion"),
                "descripcion_banco": mov_best.get("Descripcion"),
                "moneda": mov_best.get("Moneda"),
                "score_similitud": round(sim_final, 4),
                "razon_ia": just_ia,
                "match_tipo": categoria,
            })

        # ============================================================
        # SALIDA FINAL
        # ============================================================
        return pd.DataFrame(rows_match), pd.DataFrame(rows_detalles)
