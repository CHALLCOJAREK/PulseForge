# src/test.py
from __future__ import annotations

import os, sys
import pandas as pd

# ================================================
# BOOTSTRAP RUTAS
# ================================================
ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.abspath(os.path.join(ROOT, "..")))

from src.core.logger import info, ok, warn, error
from src.transformers.calculator import Calculator
from src.transformers.matcher import Matcher
from src.matchers.matcher_engine import MatcherEngine


# =======================================================
#   TEST COMPLETO — PULSEFORGE ENTERPRISE
# =======================================================
def run_test():

    info("=== 🚀 INICIANDO TEST UNITARIO DEL MATCHER ENTERPRISE ===")

    # =======================================================
    #   1) FACTURA REALISTA (solo subtotal)
    # =======================================================
    df_fact = pd.DataFrame([{
        "ruc": "20543312211",
        "cliente_generador": "GYTRES S.A.C.",
        "subtotal": 8500.00,   # SOLO ESTO - lo demás lo calcula el sistema
        "serie": "F001",
        "numero": "4567",
        "fecha_emision": "2024-11-01",
        "forma_pago": "Crédito 30 días"
    }])

    # =======================================================
    #   2) MOVIMIENTOS BANCARIOS REALES
    #      - monto sin detracción (empresa)
    #      - monto detracción (BN)
    # =======================================================
    df_bank = pd.DataFrame([
        {
            "id": 1500,
            "Fecha": "2024-12-01",
            "Monto": 9628.80,  # 🔥 monto sin detracción
            "moneda": "PEN",
            "Descripcion": "Pago factura F001-4567 GiTRES SAC",
            "Operacion": "OP987654",
            "Banco": "BCP",
            "es_dolares": False,
        },
        {
            "id": 32,
            "Fecha": "2024-11-29",
            "Monto": 401.20,   # 🔥 monto detracción BN
            "moneda": "PEN",
            "Descripcion": "Depósito detracción F001-4567",
            "Operacion": "BN001",
            "Banco": "BN",
            "es_dolares": False,
        }
    ])

    ok("Datos ficticios cargados.")

    # =======================================================
    #   NORMALIZAR FECHAS
    # =======================================================
    try:
        df_fact["fecha_emision"] = pd.to_datetime(df_fact["fecha_emision"])
        df_bank["Fecha"] = pd.to_datetime(df_bank["Fecha"])
        ok("Fechas normalizadas correctamente.")
    except Exception as e:
        error(f"Error normalizando fechas: {e}")
        return

    # =======================================================
    #   3) APLICAR CALCULATOR
    # =======================================================
    info("Ejecutando Calculator…")
    calc = Calculator()

    df_fact_proc = calc.process_facturas(df_fact)
    df_bank_proc = calc.process_bancos(df_bank)

    ok("Calculator ejecutado correctamente.")

    # =======================================================
    #   4) MATCHING ENTERPRISE
    # =======================================================
    info("Ejecutando Matcher Enterprise…")
    engine = MatcherEngine()

    # match(facturas, bancos)
    df_match, df_detalles = engine.run(df_fact_proc, df_bank_proc)

    ok("Matcher ejecutado correctamente.")

    # =======================================================
    #   MOSTRAR RESULTADOS
    # =======================================================
    print("\n=========== DF_MATCH ===========")
    print(df_match)

    print("\n=========== DF_DETALLES ===========")
    print(df_detalles)

    ok("=== TEST UNITARIO COMPLETADO SIN ERRORES ===")


# =======================================================
#   EJECUCIÓN DIRECTA
# =======================================================
if __name__ == "__main__":
    run_test()
