# src/transformers/matcher.py

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
import numpy as np
from difflib import SequenceMatcher
from datetime import datetime

from src.core.env_loader import get_env

# Prints estilo Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class Matcher:
    """
    Aplica las 8 reglas de negocio para unir facturas contra movimientos bancarios.
    """

    def __init__(self):
        self.env = get_env()

        # Si ACTIVAR_IA = true → comparar descripciones con IA
        self.use_ai = str(self.env.get("ACTIVAR_IA", "false")).lower() == "true"

        ok("Matcher inicializado correctamente.")


    # =======================================================
    #   Regla 4 — IA para similitud (opcional)
    # =======================================================
    def _similarity_ai(self, a, b):
        """
        Usa Gemini si está activado.
        Sino usa similitud por texto normal.
        """

        if not self.use_ai:
            # Similitud simple sin IA
            return SequenceMatcher(None, a.lower(), b.lower()).ratio()

        # IA real — Gemini
        try:
            import google.generativeai as genai
            genai.configure(api_key=self.env.get("API_GEMINI_KEY"))

            prompt = f"""
            Compara dos textos y dame SOLO un número entre 0 y 1 indicando qué tan similares son.
            Texto A: {a}
            Texto B: {b}
            """

            response = genai.GenerativeModel("gemini-pro").generate_content(prompt)
            score = float(response.text.strip())

            return max(0.0, min(1.0, score))

        except Exception as e:
            warn(f"IA falló ({e}), usando similitud simple.")
            return SequenceMatcher(None, a.lower(), b.lower()).ratio()



    # =======================================================
    #   MATCH PRINCIPAL (las 8 reglas)
    # =======================================================
    def match(self, df_facturas: pd.DataFrame, df_bancos: pd.DataFrame):
        """
        Devuelve un DataFrame con:

            factura_combinada
            cliente
            fecha_emision
            fecha_limite
            banco
            fecha_abono
            monto_banco
            monto_neto_factura
            diferencia
            coincidencia_monto
            coincidencia_fecha
            coincidencia_nombre
            resultado
            operacion
        """

        info("🔥 Iniciando proceso de matching...")

        resultados = []

        # =======================================================
        #   Iteramos factura x factura
        # =======================================================
        for _, fac in df_facturas.iterrows():

            fac_id = fac["combinada"]
            cliente = fac["cliente_generador"]
            neto = fac["neto_recibido"]
            detraccion = fac["detraccion_monto"]

            fecha_ini = fac["fecha_inicio_ventana"]
            fecha_fin = fac["fecha_fin_ventana"]

            fac_ruc = fac["ruc"]
            fac_fecha = fac["fecha_emision"]

            # =======================================================
            #   Regla 2 — Filtrar por monto ± variación
            # =======================================================
            movs_candidatos = df_bancos[
                (df_bancos["Monto"] >= fac["neto_recibido"] - 0.50) &
                (df_bancos["Monto"] <= fac["neto_recibido"] + 0.50)
            ]

            # Si no hay nada por monto, no sirve seguir con las otras reglas
            if movs_candidatos.empty:
                resultados.append({
                    "factura": fac_id,
                    "cliente": cliente,
                    "estado": "NO_MATCH",
                    "razon": "No hay movimientos por monto (Regla 2)."
                })
                continue

            # =======================================================
            #   Regla 1 — Filtrar por fecha ± ventana
            # =======================================================
            movs_candidatos = movs_candidatos[
                (movs_candidatos["Fecha"] >= fecha_ini) &
                (movs_candidatos["Fecha"] <= fecha_fin)
            ]

            if movs_candidatos.empty:
                resultados.append({
                    "factura": fac_id,
                    "cliente": cliente,
                    "estado": "NO_MATCH",
                    "razon": "No cayó dentro de la ventana de fechas (Regla 1)."
                })
                continue

            # =======================================================
            #   Regla 4 — Similitud por nombre del cliente en descripción
            # =======================================================
            movs_candidatos["sim_nombre"] = movs_candidatos["Descripcion"].apply(
                lambda d: self._similarity_ai(d, cliente)
            )

            # Buscar el máximo de similitud
            movs_candidatos = movs_candidatos.sort_values(by="sim_nombre", ascending=False)

            mejor = movs_candidatos.iloc[0]

            # =======================================================
            #   Evaluación final
            # =======================================================
            sim = mejor["sim_nombre"]
            monto_banco = mejor["Monto"]
            fecha_mov = mejor["Fecha"]
            banco = mejor["Banco"]

            resultados.append({
                "factura": fac_id,
                "cliente": cliente,
                "fecha_emision": fac_fecha,
                "fecha_limite": fac["fecha_limite_pago"],
                "fecha_mov": fecha_mov,
                "banco": banco,
                "operacion": mejor["Operacion"],
                "monto_factura_neto": neto,
                "monto_banco": monto_banco,
                "sim_nombre": sim,
                "diferencia_monto": round(abs(neto - monto_banco), 2),
                "resultado": "MATCH" if sim >= 0.40 else "MATCH_DUDOSO"
            })

        ok("Matching completado. 🧩")
        return pd.DataFrame(resultados)



# =======================================================
#   TEST DIRECTO (opcional)
# =======================================================
if __name__ == "__main__":
    warn("Test directo del Matcher. No usar en producción.")
