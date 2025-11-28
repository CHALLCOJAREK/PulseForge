# src/pipelines/incremental.py

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from datetime import datetime
import sqlite3
import pandas as pd

# Core
from src.core.env_loader import get_env

# Extractors
from src.extractors.invoices_extractor import InvoicesExtractor
from src.extractors.clients_extractor import ClientsExtractor
from src.extractors.bank_extractor import BankExtractor

# Transformers
from src.transformers.data_mapper import DataMapper
from src.transformers.calculator import Calculator
from src.transformers.matcher import Matcher

# Loaders
from src.loaders.newdb_builder import NewDBBuilder
from src.loaders.invoice_writer import InvoiceWriter
from src.loaders.match_writer import MatchWriter

# Prints Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


def incremental_run():
    info("⚡ Iniciando ejecución INCREMENTAL de PulseForge (por COMBINADA)...")
    start_time = datetime.now()

    env = get_env()
    db_path = env.get("PULSEFORGE_NEWDB_PATH")

    if not db_path:
        error("PULSEFORGE_NEWDB_PATH no definido en .env. Abortando incremental.")
        return

    # =====================================================
    # 0) Asegurar estructura de BD (idempotente)
    # =====================================================
    builder = NewDBBuilder()
    builder.crear_tablas()

    invoice_writer = InvoiceWriter()
    match_writer   = MatchWriter()

    # =====================================================
    # 1) Leer COMBINADAS ya procesadas en facturas_pf
    # =====================================================
    info("📂 Leyendo facturas ya existentes en facturas_pf...")

    try:
        conn = sqlite3.connect(db_path)
        cur = conn.cursor()
        cur.execute("SELECT combinada FROM facturas_pf;")
        existentes = {row[0] for row in cur.fetchall()}
        conn.close()
    except Exception as e:
        error(f"No se pudo leer facturas_pf: {e}")
        return

    info(f"Facturas ya registradas en PulseForge: {len(existentes)}")

    # =====================================================
    # 2) EXTRACTION: clientes, facturas, bancos
    # =====================================================
    info("📥 Extrayendo clientes desde DataPulse...")
    clientes_df = ClientsExtractor().get_client_data()

    info("📥 Extrayendo facturas desde DataPulse...")
    facturas_df = InvoicesExtractor().load_invoices()
    if facturas_df.empty:
        warn("No se encontraron facturas nuevas en DataPulse. Nada que procesar.")
        return

    info("📥 Extrayendo movimientos bancarios desde DataPulse...")
    bancos_df = BankExtractor().get_all()   # OJO: usar get_all() también en full_run
    if bancos_df.empty:
        warn("No se encontraron movimientos bancarios. Se cargarán solo facturas nuevas sin matching.")

    # =====================================================
    # 3) MAPPING
    # =====================================================
    mapper = DataMapper()

    info("🔄 Mapeando clientes...")
    clientes_m = mapper.map_clientes(clientes_df)

    info("🔄 Mapeando facturas...")
    facturas_m = mapper.map_facturas(facturas_df)

    info("🔄 Mapeando bancos...")
    bancos_m = mapper.map_bancos(bancos_df)

    if facturas_m.empty:
        warn("Tras el mapeo, no quedaron facturas válidas. Abortando incremental.")
        return

    # =====================================================
    # 4) CALCULATOR (sobre TODAS las facturas mapeadas)
    # =====================================================
    info("🧮 Ejecutando cálculos financieros sobre facturas...")
    calc = Calculator()
    facturas_calc_all = calc.process_facturas(facturas_m)

    info("🧮 Preparando movimientos bancarios para matching...")
    bancos_calc = calc.process_bancos(bancos_m) if not bancos_m.empty else pd.DataFrame()

    # =====================================================
    # 5) Filtrar SOLO facturas nuevas por COMBINADA
    # =====================================================
    info("📌 Filtrando facturas nuevas por COMBINADA...")

    if "combinada" not in facturas_calc_all.columns:
        error("La columna 'combinada' no existe en facturas_calc_all. Revisa el DataMapper.")
        return

    facturas_nuevas = facturas_calc_all[~facturas_calc_all["combinada"].isin(existentes)].copy()
    info(f"Facturas nuevas detectadas: {len(facturas_nuevas)}")

    if facturas_nuevas.empty:
        ok("No hay facturas nuevas. Incremental sin cambios.")
        end_time = datetime.now()
        dur = (end_time - start_time).total_seconds()
        info(f"🕒 Duración total: {dur} segundos")
        return

    # =====================================================
    # 6) MATCHER solo para facturas nuevas
    # =====================================================
    if bancos_calc.empty:
        warn("No hay movimientos bancarios. Se insertan facturas nuevas sin matches.")
        matches_nuevos = pd.DataFrame()
    else:
        info("🧩 Ejecutando matching SOLO para facturas nuevas...")
        matcher = Matcher()
        matches_nuevos = matcher.match(facturas_nuevas, bancos_calc)
        info(f"Matches generados para facturas nuevas: {len(matches_nuevos)}")

    # =====================================================
    # 7) WRITE → Insertar NUEVAS facturas y matches
    # =====================================================
    info("📤 Insertando facturas nuevas en facturas_pf...")
    invoice_writer.escribir_facturas(facturas_nuevas)

    if not matches_nuevos.empty:
        info("📤 Insertando matches nuevos en matches_pf...")
        match_writer.escribir_matches(matches_nuevos)
    else:
        warn("No se insertaron matches (DataFrame vacío).")

    # =====================================================
    # 8) Resumen final
    # =====================================================
    end_time = datetime.now()
    dur = (end_time - start_time).total_seconds()

    ok("⚡ PulseForge – INCREMENTAL COMPLETADO ⚡")
    print("\n============== RESUMEN INCREMENTAL ==============")
    print(f"Facturas nuevas procesadas:      {len(facturas_nuevas)}")
    print(f"Movimientos bancarios usados:    {len(bancos_calc)}")
    print(f"Matches nuevos generados:        {len(matches_nuevos)}")
    print(f"Duración total (segundos):       {dur}")
    print("=================================================\n")


if __name__ == "__main__":
    incremental_run()
