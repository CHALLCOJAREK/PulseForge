# src/pipelines/full_run.py

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from datetime import datetime

# Core
from src.core.env_loader import get_env
from src.loaders.newdb_builder import NewDBBuilder

# Extractors
from src.extractors.invoices_extractor import InvoicesExtractor
from src.extractors.clients_extractor import ClientsExtractor
from src.extractors.bank_extractor import BankExtractor

# Transformers
from src.transformers.calculator import Calculator

# Matcher
from src.matchers.matcher import Matcher

# Writers
from src.loaders.invoice_writer import InvoiceWriter
from src.loaders.match_writer import MatchWriter

# Prints Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


def full_run():
    info("🔥 Iniciando ejecución completa de PulseForge...")
    start_time = datetime.now()

    # ======================================================
    # 1) Cargar env
    # ======================================================
    env = get_env()
    ok("Variables de entorno cargadas ✓")

    # ======================================================
    # 2) Construir nueva BD
    # ======================================================
    builder = NewDBBuilder()
    builder.build()

    # ======================================================
    # 3) Extraer facturas
    # ======================================================
    inv = InvoicesExtractor()
    df_facturas = inv.load_invoices()
    ok(f"Facturas extraídas: {len(df_facturas)}")

    # ======================================================
    # 4) Extraer clientes (razón social)
    # ======================================================
    cli = ClientsExtractor()
    df_clientes = cli.get_client_data()
    ok(f"Clientes extraídos: {len(df_clientes)}")

    # ======================================================
    # 5) Unir facturas + clientes
    # ======================================================
    info("Uniendo facturas con razón social...")
    df_facturas = df_facturas.merge(df_clientes, on="RUC", how="left")
    ok("Unión completada ✓")

    # ======================================================
    # 6) Calcular subtotales, IGV, neto, detracción, vencimiento
    # ======================================================
    calc = Calculator()
    df_calc = calc.procesar_facturas(df_facturas)
    ok("Cálculos contables aplicados ✓")

    # ======================================================
    # 7) Guardar facturas procesadas
    # ======================================================
    writer_inv = InvoiceWriter()
    writer_inv.guardar_facturas(df_calc)
    writer_inv.close()

    # ======================================================
    # 8) Extraer movimientos bancarios
    # ======================================================
    bank = BankExtractor()
    df_bancos = bank.get_todos_movimientos()
    ok(f"Movimientos bancarios cargados: {len(df_bancos)}")

    # ======================================================
    # 9) Aplicar matcher
    # ======================================================
    matcher = Matcher()
    df_match = matcher.cruzar(df_calc, df_bancos)
    ok("Cruce bancario completado ✓")

    # ======================================================
    # 10) Guardar resultados del matcher
    # ======================================================
    writer_match = MatchWriter()
    writer_match.guardar_matches(df_match)
    writer_match.close()

    # ======================================================
    # 11) Log final del proceso
    # ======================================================
    builder.write_log("Ejecución FullRun", f"Procesadas {len(df_calc)} facturas.")

    # ======================================================
    # 12) Tiempo final
    # ======================================================
    end_time = datetime.now()
    duracion = (end_time - start_time).total_seconds()

    ok("🔥 PulseForge – FULL RUN COMPLETADO 🔥")
    info(f"🕒 Duración total: {duracion} segundos")
    info("✔ Sistema listo, datos actualizados en BD nueva.")


# ======================================================
#   EJECUCIÓN DIRECTA
# ======================================================
if __name__ == "__main__":
    full_run()
