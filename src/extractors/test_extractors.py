# src/extractors/test_extractors.py
from __future__ import annotations

# --- BOOTSTRAP ---
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# --- IMPORTS ---
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config
from src.extractors.clients_extractor import ClientsExtractor
from src.extractors.invoices_extractor import InvoicesExtractor
from src.extractors.bank_extractor import BankExtractor


# ============================================================
#   TEST: CLIENTS EXTRACTOR
# ============================================================
def test_clients():
    info("=== TEST → ClientsExtractor ===")
    ce = ClientsExtractor()
    df = ce.extract()

    if df.empty:
        warn("ClientsExtractor → DF vacío.")
    else:
        ok(f"Clientes extraídos: {len(df)}")
        print(df.head(5))

    return df


# ============================================================
#   TEST: INVOICES EXTRACTOR
# ============================================================
def test_invoices():
    info("=== TEST → InvoicesExtractor ===")
    ie = InvoicesExtractor()
    df = ie.extract()

    if len(df) == 0:
        warn("InvoicesExtractor → DF vacío.")
    else:
        ok(f"Facturas extraídas: {len(df)}")
        print(df[:5])

    return df



# ============================================================
#   TEST: BANK EXTRACTOR
# ============================================================
def test_banks():
    info("=== TEST → BankExtractor ===")
    be = BankExtractor()
    df = be.extract()

    if len(df) == 0:
        warn("BankExtractor → DF vacío.")
    else:
        ok(f"Movimientos bancarios extraídos: {len(df)}")
        print(df.head(5))

    return df


# ============================================================
#   RUNNER GENERAL
# ============================================================
if __name__ == "__main__":
    info("=============================================")
    info("     INICIO TEST COMPLETO DE EXTRACTORS")
    info("=============================================")

    cfg = get_config()
    ok(f"BD origen configurada → {cfg.db_source}")

    df_clients = test_clients()
    df_invoices = test_invoices()
    df_banks = test_banks()

    info("---------------------------------------------")
    ok("TEST MÓDULO EXTRACTORS FINALIZADO CON ÉXITO")
    info("---------------------------------------------")

    print("\nResumen final:")
    print(f" - Clientes  → {len(df_clients)} registros")
    print(f" - Facturas  → {len(df_invoices)} registros")
    print(f" - Bancos    → {len(df_banks)} registros")
