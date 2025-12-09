# src/transformers/test_transformers.py
from __future__ import annotations

import sys
from pathlib import Path
import pandas as pd

# ---------------------------------------------------------
# BOOTSTRAP
# ---------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config

from src.transformers.data_mapper import DataMapper
from src.transformers.calculator import Calculator
from src.transformers.matcher import Matcher


# =========================================================
#   TEST DATAMAPPER — CLIENTES / FACTURAS
# =========================================================
def test_mapper(df_clients, df_invoices):
    info("=== TEST → DataMapper ===")
    
    mapper = DataMapper()

    mapped_clients = mapper.map_clientes(df_clients)
    ok(f"Clientes mapeados → {len(mapped_clients)}")

    mapped_facturas = mapper.map_facturas(df_invoices)
    ok(f"Facturas mapeadas → {len(mapped_facturas)}")

    return mapped_clients, mapped_facturas


# =========================================================
#   TEST CALCULATOR — PROCESA FACTURAS
# =========================================================
def test_calculator(df_facturas):
    info("=== TEST → Calculator ===")

    calc = Calculator()
    df_calc = calc.process_facturas(df_facturas)

    ok(f"Facturas procesadas → {len(df_calc)}")
    print(df_calc.head(5))

    return df_calc


# =========================================================
#   TEST MATCHER — MATCHING COMPLETO
# =========================================================
def test_matcher(df_facturas, df_bancos):
    info("=== TEST → Matcher (IA + reglas) ===")

    matcher = Matcher()
    df_match, df_detalles = matcher.match(df_facturas, df_bancos)

    ok(f"Resultados de match → {len(df_match)}")
    ok(f"Detalles generados → {len(df_detalles)}")

    print(df_match.head(5))
    print("\nDETALLES:")
    print(df_detalles.head(5))

    return df_match, df_detalles


# =========================================================
#   RUN GLOBAL
# =========================================================
if __name__ == "__main__":
    info("===============================================")
    info("       TEST TRANSFORMERS · PulseForge")
    info("===============================================")

    cfg = get_config()
    ok("Config cargada correctamente.")

    # IMPORTAR DATA REAL DESDE EXTRACTORS
    from src.extractors.clients_extractor import ClientsExtractor
    from src.extractors.invoices_extractor import InvoicesExtractor
    from src.extractors.bank_extractor import BankExtractor

    df_clients = pd.DataFrame(ClientsExtractor().extract())
    df_invoices = pd.DataFrame(InvoicesExtractor().extract())
    df_banks = BankExtractor().extract()   # DataFrame directo (correcto)

    # 1) DATAMAPPER (solo clientes y facturas)
    mapped_clients, mapped_facturas = test_mapper(
        df_clients,
        df_invoices,
    )

    df_facturas_calc = pd.DataFrame(mapped_facturas)

    # 2) CALCULATOR
    df_facturas_ready = test_calculator(df_facturas_calc)

    # 3) MATCHER (Facturas procesadas + Bancos ya normalizados)
    df_match, df_detalles = test_matcher(
        df_facturas_ready,
        df_banks,
    )

    info("------------------------------------------------")
    ok("TEST TRANSFORMERS COMPLETADO CON ÉXITO")
    info("------------------------------------------------")
