# src/pipelines/full_run.py

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

# ========== EXTRACTORS ==========
from src.extractors.clients_extractor import ClientsExtractor
from src.extractors.invoices_extractor import InvoicesExtractor
from src.extractors.bank_extractor import BankExtractor

# ========== TRANSFORMERS ==========
from src.transformers.data_mapper import DataMapper
from src.transformers.calculator import Calculator
from src.transformers.matcher import Matcher

# ========== LOADERS ==========
from src.loaders.newdb_builder import NewDBBuilder
from src.loaders.invoice_writer import InvoiceWriter
from src.loaders.match_writer import MatchWriter

# ========== UTILS ==========
from src.core.env_loader import get_env

# Prints estilo Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


# =======================================================
#   FULL RUN: EJECUCIÓN COMPLETA DEL SISTEMA
# =======================================================
def full_run():
    info("🚀 Iniciando ejecución completa de PulseForge...")

    env = get_env()

    # 1) EXTRACTION
    info("📥 Extrayendo clientes...")
    clientes_df = ClientsExtractor().get_client_data()

    info("📥 Extrayendo facturas...")
    facturas_df = InvoicesExtractor().load_invoices()

    info("📥 Extrayendo movimientos bancarios...")
    bancos_df = BankExtractor().get_todos_movimientos()

    # 2) MAPPING
    mapper = DataMapper()

    info("🔄 Mapeando clientes...")
    clientes_m = mapper.map_clientes(clientes_df)

    info("🔄 Mapeando facturas...")
    facturas_m = mapper.map_facturas(facturas_df)

    info("🔄 Mapeando movimientos bancarios...")
    bancos_m = mapper.map_bancos(bancos_df)

    # 3) CALCULATOR
    info("🧮 Ejecutando cálculos financieros...")
    calc = Calculator()
    facturas_calc = calc.process_facturas(facturas_m)
    bancos_calc   = calc.process_bancos(bancos_m)

    # 4) MATCHER
    info("🧩 Iniciando matching completo...")
    matcher = Matcher()
    matches_df = matcher.match(facturas_calc, bancos_calc)

    # 5) NEW DB BUILDER
    info("🏗️ Construyendo BD destino (PulseForge)...")
    builder = NewDBBuilder()
    builder.crear_tablas()

    # 6) WRITERS
    invoice_writer = InvoiceWriter()
    match_writer   = MatchWriter()

    info("🧹 Limpiando tablas destino para carga FULL...")
    invoice_writer.limpiar_tabla()
    match_writer.limpiar_tabla()

    info("📤 Insertando facturas en pulseforge.sqlite...")
    invoice_writer.escribir_facturas(facturas_calc)

    info("📤 Insertando matches en pulseforge.sqlite...")
    match_writer.escribir_matches(matches_df)

    # 7) RESUMEN FINAL
    ok("🎯 Full Run completado con éxito.")
    print("\n================ RESULTADO FINAL ================\n")
    print(f"Facturas procesadas: {len(facturas_calc)}")
    print(f"Movimientos bancarios leídos: {len(bancos_calc)}")
    print(f"Matches generados: {len(matches_df)}")
    print("\n================================================\n")

    return {
        "facturas": facturas_calc,
        "bancos": bancos_calc,
        "matches": matches_df
    }



# =======================================================
#   TEST DIRECTO
# =======================================================
if __name__ == "__main__":
    full_run()
