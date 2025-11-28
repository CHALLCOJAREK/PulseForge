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


def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


# =======================================================
#   FULL RUN · PULSEFORGE · ENTERPRISE MODE
# =======================================================
def full_run():
    info("🚀 FULL RUN — PulseForge iniciando...")

    env = get_env()

    # =======================================================
    #   0) CONSTRUIR BD DESTINO
    # =======================================================
    info("🏗️ Construyendo estructura pulseforge.sqlite...")
    builder = NewDBBuilder()
    builder.crear_tablas()

    invoice_writer = InvoiceWriter()
    match_writer   = MatchWriter()

    info("🧹 Limpiando tablas destino (modo FULL)...")
    invoice_writer.limpiar_tabla()
    match_writer.limpiar_tabla()

    # =======================================================
    #   1) EXTRACTION
    # =======================================================
    info("📥 Extrayendo clientes...")
    clientes_df = ClientsExtractor().get_client_data()
    if clientes_df.empty:
        warn("Clientes vacío.")

    info("📥 Extrayendo facturas...")
    facturas_df = InvoicesExtractor().load_invoices()
    if facturas_df.empty:
        error("❌ No se encontraron facturas. FULL RUN abortado.")
        return None

    info("📥 Extrayendo movimientos bancarios...")
    bancos_df = BankExtractor().get_todos_movimientos()
    if bancos_df.empty:
        warn("⚠️ No hay movimientos bancarios.")

    # =======================================================
    #   LIMPIEZA GLOBAL PRE-MAPPING
    # =======================================================
    info("🧽 Normalizando nombres de columnas globales...")

    def clean_cols(df):
        df.columns = [str(c).strip().replace("\n", "").replace("\r", "") for c in df.columns]
        return df

    clientes_df = clean_cols(clientes_df)
    facturas_df = clean_cols(facturas_df)
    bancos_df   = clean_cols(bancos_df)

    # =======================================================
    #   2) MAPPING
    # =======================================================
    mapper = DataMapper()

    info("🔄 Mapeando clientes...")
    clientes_m = mapper.map_clientes(clientes_df)

    info("🔄 Mapeando facturas...")
    facturas_m = mapper.map_facturas(facturas_df)

    info("🔄 Mapeando movimientos bancarios (blindado)...")
    bancos_m = mapper.map_bancos(bancos_df)

    # =======================================================
    #   3) CALCULATOR
    # =======================================================
    info("🧮 Ejecutando cálculos financieros...")
    calc = Calculator()

    facturas_calc = calc.process_facturas(facturas_m)
    bancos_calc   = calc.process_bancos(bancos_m)

    # =======================================================
    #   VALIDACIONES ANTES DEL MATCH
    # =======================================================
    if "fecha_mov" not in bancos_calc.columns and "Fecha" not in bancos_calc.columns:
        warn("⚠️ WARNING: bancos_calc no trae columna Fecha. El matcher la reconstruirá.")

    if "Banco" not in bancos_calc.columns:
        warn("⚠️ WARNING: bancos_calc no trae Banco. Intentaremos detectar columnas equivalentes.")

    # =======================================================
    #   4) MATCHER
    # =======================================================
    info("🧩 Matching iniciado...")
    matcher = Matcher()
    matches_df = matcher.match(facturas_calc, bancos_calc)

    # =======================================================
    #   5) LOADERS
    # =======================================================
    info("📤 Guardando facturas en la BD destino...")
    invoice_writer.escribir_facturas(facturas_calc)

    info("📤 Guardando matches en la BD destino...")
    match_writer.escribir_matches(matches_df)

    # =======================================================
    #   6) RESUMEN FINAL
    # =======================================================
    ok("🎯 FULL RUN completado correctamente.")

    print("\n================= RESULTADO FINAL =================")
    print(f"Facturas procesadas:        {len(facturas_calc)}")
    print(f"Movimientos bancarios:      {len(bancos_calc)}")
    print(f"Matches generados:          {len(matches_df)}")
    print("===================================================\n")

    return {
        "clientes": clientes_m,
        "facturas": facturas_calc,
        "bancos": bancos_calc,
        "matches": matches_df
    }


if __name__ == "__main__":
    full_run()
