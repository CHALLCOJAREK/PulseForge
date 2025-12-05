# src/main.py
from __future__ import annotations

import sys
import traceback
from pathlib import Path

# ============================================================
#  BOOTSTRAP GLOBAL
# ============================================================
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env
from src.loaders.newdb_builder import NewDBBuilder

# EXTRACTORS
from src.extractors.invoices_extractor import InvoicesExtractor
from src.extractors.bank_extractor import BankExtractor
from src.extractors.clients_extractor import ClientsExtractor

# LOADERS
from src.loaders.invoice_writer import InvoiceWriter
from src.loaders.bank_writer import BankWriter
from src.loaders.clients_writer import ClientsWriter
from src.loaders.match_writer import MatchWriter

# MATCHER ENGINE
from src.matchers.matcher_engine import MatcherEngine

# DB WRAPPER
from src.core.db import PulseForgeDB


# ============================================================
#  PULSEFORGE MAIN ORCHESTRATOR
# ============================================================
def main(full_reset: bool = False):
    info("=== 🚀 INICIANDO PULSEFORGE ===")

    # ========================================================
    # 0. CREACIÓN / RESET DE BASE DE DATOS
    # ========================================================
    if full_reset:
        warn("Reseteo completo activado: recreando base de datos…")
        NewDBBuilder().build(reset=True)
    else:
        ok("La base existente será utilizada.")

    # Auto-crear si no existe
    db_path = Path(get_env("PULSEFORGE_NEWDB_PATH"))
    if not db_path.exists():
        warn(f"La BD destino no existe → generando nueva en {db_path}")
        NewDBBuilder().build(reset=True)
    else:
        ok("BD destino encontrada. Modo incremental activado.")

    db = PulseForgeDB()

    # ========================================================
    # 1. EXTRACTORS
    # ========================================================
    info("📥 Extrayendo CLIENTES…")
    df_clients = ClientsExtractor().extract()
    ok(f"Clientes extraídos: {len(df_clients)}")

    info("📥 Extrayendo FACTURAS…")
    df_facturas = InvoicesExtractor().extract()
    ok(f"Facturas extraídas: {len(df_facturas)}")

    info("📥 Extrayendo BANCOS…")
    df_bancos = BankExtractor().extract()
    ok(f"Movimientos extraídos: {len(df_bancos)}")

    # ========================================================
    # 2. LOADERS → BD
    # ========================================================
    info("💾 Guardando CLIENTES…")
    ClientsWriter().save(df_clients, reset=full_reset)

    info("💾 Guardando FACTURAS…")
    InvoiceWriter().save(df_facturas, reset=full_reset)

    info("💾 Guardando BANCOS…")
    BankWriter().save(df_bancos, reset=full_reset)

    ok("Datos cargados en BD correctamente.")

    # ========================================================
    # 3. CARGAR DESDE BD PARA MATCHING
    # ========================================================
    info("📤 Leyendo facturas y bancos desde BD…")

    df_fact = db.read("SELECT * FROM facturas_pf")
    df_mov = db.read("SELECT * FROM movimientos_pf")

    ok(f"Facturas cargadas: {len(df_fact)}")
    ok(f"Movimientos cargados: {len(df_mov)}")

    # ========================================================
    # 4. MATCHER ENGINE
    # ========================================================
    info("🔍 Ejecutando motor de Matching…")
    engine = MatcherEngine()

    df_match, df_detalles = engine.run(df_fact, df_mov)

    ok(f"Resultados de match: {len(df_match)}")
    ok(f"Detalles generados: {len(df_detalles)}")

    # ========================================================
    # 5. GUARDAR RESULTADOS
    # ========================================================
    info("💾 Guardando resultados del MATCH…")
    MatchWriter().save(df_match, df_detalles, reset=full_reset)

    ok("Matching guardado correctamente.")
    ok("✨ PulseForge finalizado sin errores.")


# ============================================================
# ENTRYPOINT
# ============================================================
if __name__ == "__main__":
    try:
        main(full_reset=True)
    except Exception as e:
        error("❌ Error crítico en PulseForge:")
        error(str(e))
        print(traceback.format_exc())
