# src/transformers/matcher.py
from __future__ import annotations

import sys
from pathlib import Path
from datetime import timedelta
from difflib import SequenceMatcher
from typing import Any, Optional, Dict, Tuple

import pandas as pd

CURRENT_FILE = Path(__file__).resolve()
ROOT = CURRENT_FILE.parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.core.logger import warn
from src.core.env_loader import get_config, get_env
from src.transformers.ai_helpers import ai_similarity, ai_decide_match


class Matcher:
    FLEX_TERMS = {
        "interbancario", "interbank", "interbanco",
        "factoring", "factoraje", "cesion", "cesión",
        "masivo", "pago masivo", "pago facturas",
        "deposito", "depósito", "transferencia", "trf", "trx",
    }

    def __init__(self):
        cfg = get_config()
        self.cfg = cfg

        # ------------------------------------------------------
        # Parámetros contables y de variación
        # ------------------------------------------------------
        # Desde settings.json → parametros_contables
        self.var_monto = float(cfg.parametros.monto_variacion)
        # Alias ya definido en env_loader: tipo_cambio = tipo_cambio_usd_pen
        self.tc_usd_pen = float(cfg.tipo_cambio)

        # Días extras que extienden la ventana de búsqueda
        self.extra_days = 3

        # Umbrales de similitud
        self.sim_strong = 0.55
        self.sim_dudoso = 0.35

        # Flag híbrido IA (si algún día se mapea en cfg; si no, False)
        self.use_ai = bool(getattr(cfg, "activar_ia", False))

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

    def _compute_best_monto_diff(self, fac: pd.Series, mov: pd.Series) -> Optional[Dict[str, Any]]:
        monto_pen = self._safe_float(mov.get("Monto_PEN"))
        if monto_pen is None:
            monto_pen = self._safe_float(mov.get("Monto"))

        if monto_pen is None:
            return None

        refs = (
            ("neto_recibido", fac.get("neto_recibido")),
            ("total_con_igv", fac.get("total_con_igv")),
            ("subtotal", fac.get("subtotal")),
        )

        candidatos = []
        for nombre, val in refs:
            val_f = self._safe_float(val)
            if val_f is not None:
                diff = abs(monto_pen - val_f)
                candidatos.append((nombre, val_f, diff))

        if not candidatos:
            return None

        tipo, ref, diff = min(candidatos, key=lambda x: x[2])

        return {
            "tipo_base": tipo,
            "monto_ref": ref,
            "monto_banco_equivalente": monto_pen,
            "diff_monto": diff,
        }

    def match(self, df_facturas: pd.DataFrame, df_bancos: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:

        banks = df_bancos.copy()

        if "banco_codigo" in banks.columns and "Banco" not in banks.columns:
            banks["Banco"] = banks["banco_codigo"].astype(str)
        elif "Banco" not in banks.columns:
            banks["Banco"] = "DESCONOCIDO"

        if "Fecha" not in banks.columns:
            base = next((c for c in banks.columns if c.lower() == "fecha"), None)
            if base:
                banks["Fecha"] = pd.to_datetime(banks[base], errors="coerce")
            else:
                warn("Matcher: DF bancos sin columna Fecha.")
                banks["Fecha"] = pd.NaT

        if "Descripcion" not in banks.columns:
            desc_col = next((c for c in banks.columns
                             if c.lower() in ("descripcion", "detalle", "glosa", "concepto")), None)
            banks["Descripcion"] = banks[desc_col].astype(str) if desc_col else ""

        if "moneda" not in banks.columns:
            mon_col = next((c for c in banks.columns if c.lower() == "moneda"), None)
            banks["moneda"] = banks[mon_col].astype(str).str.upper().str.strip() if mon_col else "PEN"

        if "Operacion" not in banks.columns:
            op_col = next((c for c in banks.columns
                           if c.lower() in ("operacion", "nro_operacion", "referencia")), None)
            banks["Operacion"] = banks[op_col].astype(str) if op_col else ""

        if "Monto" not in banks.columns:
            monto_col = next((c for c in banks.columns
                              if c.lower() in ("monto", "importe", "montototal")), None)
            banks["Monto"] = pd.to_numeric(banks[monto_col], errors="coerce").fillna(0) if monto_col else 0

        if "Monto_PEN" not in banks.columns:
            banks["Monto_PEN"] = banks["Monto"]

        rows_match = []
        rows_detalles = []

        # -----------------------------
        #     LOOP FACTURA X FACTURA
        # -----------------------------
        for _, fac in df_facturas.iterrows():
            factura_id = fac.get("factura_id") or fac.get("id") or fac.get("combinada")
            cliente = fac.get("cliente_generador") or ""
            ruc = fac.get("ruc")

            fac_fecha = fac.get("fecha_emision")
            fac_lim = fac.get("fecha_limite_pago")
            win_ini = fac.get("fecha_inicio_ventana")
            win_fin = fac.get("fecha_fin_ventana")

            # Ventana de búsqueda
            if pd.isna(win_ini) or pd.isna(win_fin):
                if not pd.isna(fac_fecha):
                    win_ini = fac_fecha - timedelta(days=self.extra_days)
                    win_fin = fac_fecha + timedelta(days=self.extra_days)
                    candidatos = banks[(banks["Fecha"] >= win_ini) & (banks["Fecha"] <= win_fin)]
                else:
                    candidatos = banks
            else:
                extra = timedelta(days=self.extra_days)
                candidatos = banks[
                    (banks["Fecha"] >= win_ini - extra) &
                    (banks["Fecha"] <= win_fin + extra)
                ]

            # Sin candidatos
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

            # -----------------------
            # LOOP MOVIMIENTOS
            # -----------------------
            for idx_mov, mov in candidatos.iterrows():

                monto_info = self._compute_best_monto_diff(fac, mov)
                if not monto_info:
                    continue

                if monto_info["diff_monto"] > self.var_monto * 2:
                    continue

                desc = str(mov.get("Descripcion") or "")
                sim_regla = self._similarity_basic(cliente, desc)

                # IA optimizada → solo si aporta valor
                if self.use_ai and 0.25 < sim_regla < 0.80:
                    sim_ai = ai_similarity(cliente, desc)
                else:
                    sim_ai = sim_regla  # no llamamos IA

                sim_final = max(sim_regla, sim_ai)
                flex_flag = self._contains_flex_terms(desc)

                score_monto = 1 - (monto_info["diff_monto"] / (self.var_monto * 2))
                score = 0.5 * score_monto + 0.4 * sim_final + (0.1 if flex_flag else 0)

                mejores.append((score, idx_mov, mov, monto_info, sim_final, flex_flag))

                rows_detalles.append({
                    "factura_id": factura_id,
                    "combinada": fac.get("combinada"),
                    "serie": fac.get("serie"),
                    "numero": fac.get("numero"),
                    "ruc": ruc,
                    "cliente": cliente,
                    "movimiento_index": idx_mov,
                    "fecha_mov": mov.get("Fecha"),
                    "banco_codigo": mov.get("Banco"),
                    "operacion": mov.get("Operacion"),
                    "descripcion_banco": desc,
                    "moneda": mov.get("moneda"),
                    "monto_banco": mov.get("Monto"),
                    "monto_banco_equivalente": monto_info["monto_banco_equivalente"],
                    "tipo_monto_ref": monto_info["tipo_base"],
                    "monto_ref": monto_info["monto_ref"],
                    "diff_monto": monto_info["diff_monto"],
                    "sim_nombre_regla": sim_regla,
                    "sim_nombre_ia": sim_ai,
                    "sim_nombre_max": sim_final,
                    "tiene_terminos_flex": int(flex_flag),
                    "ventana_inicio": win_ini,
                    "ventana_fin": win_fin,
                    "fecha_emision": fac_fecha,
                    "fecha_limite_pago": fac_lim,
                    "score_final": score,
                })

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

            # -------------------------------------
            # TOMAR EL MEJOR CANDIDATO
            # -------------------------------------
            score_best, idx_best, mov_best, mi, sim_final, flex_flag = max(mejores, key=lambda x: x[0])

            # REGLA BASE
            if sim_final >= self.sim_strong and mi["diff_monto"] <= self.var_monto:
                categoria = "MATCH"
            elif sim_final >= self.sim_dudoso or flex_flag:
                categoria = "MATCH_DUDOSO"
            else:
                categoria = "MATCH_MONTOS_OK_NOMBRE_BAJO"

            # IA FINAL SOLO SI ES NECESARIO
            just_ia = ""
            if self.use_ai and categoria != "MATCH":
                try:
                    dec = ai_decide_match({
                        "factura": str(factura_id),
                        "cliente": str(cliente),
                        "ruc": str(ruc),
                        "descripcion_banco": str(mov_best.get("Descripcion")),
                        "monto_banco_equivalente": float(mi["monto_banco_equivalente"]),
                        "monto_ref": float(mi["monto_ref"]),
                        "tipo_monto_ref": str(mi["tipo_base"]),
                        "diff_monto": float(mi["diff_monto"]),
                        "sim_regla": float(sim_final),
                        "sim_ai": float(sim_final),
                        "tiene_terminos_flex": bool(flex_flag),
                    })
                    categoria = dec.get("decision", categoria)
                    just_ia = dec.get("justificacion", ""
                    )
                except:
                    pass

            rows_match.append({
                "factura_id": factura_id,
                "movimiento_id": idx_best,
                "subtotal": fac.get("subtotal"),
                "igv": fac.get("igv"),
                "total_con_igv": fac.get("total_con_igv"),
                "detraccion_monto": fac.get("detraccion_monto"),
                "neto_recibido": fac.get("neto_recibido"),
                "monto_banco": mov_best.get("Monto"),
                "monto_banco_equivalente": mi["monto_banco_equivalente"],
                "variacion_monto": round(mi["diff_monto"], 2),
                "fecha_mov": mov_best.get("Fecha"),
                "banco_pago": mov_best.get("Banco"),
                "operacion": mov_best.get("Operacion"),
                "descripcion_banco": mov_best.get("Descripcion"),
                "moneda": mov_best.get("moneda"),
                "score_similitud": round(sim_final, 4),
                "razon_ia": just_ia,
                "match_tipo": categoria,
            })

        return pd.DataFrame(rows_match), pd.DataFrame(rows_detalles)
