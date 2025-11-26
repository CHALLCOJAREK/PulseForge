# src/pipelines/incremental.py

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

# DB direct read
import sqlite3

# Prints Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


def incremental_run():
    info("⚡ Iniciando ejecución INCREMENTAL de PulseForge...")
    start_time = datetime.now()

    env = get_env()

    # =============================================
    # 1. Asegurar que la BD nueva existe
    # =============================================
    builder = NewDBBuilder()
    builder.build()

    # =============================================
    # 2. Cargar facturas existentes en BD nueva
    # =============================================
    conn = sqlite3.connect(env.DB_PATH_NUEVA)
    cursor = conn.cursor()

    cursor.execute("SELECT Factura FROM facturas_procesadas")
    existentes = {row[0] for row in cursor.fetchall()}
    info(f"Facturas ya en BD nueva: {len(existentes)}")

    # =============================================
    # 3. Extraer facturas originales (DataPulse)
    # =============================================
    inv = InvoicesExtractor()
    df_facturas = inv.load_invoices()

    # Determinar facturas nuevas por combinación Serie-Numero
    df_facturas["Combinada"] = df_facturas["Serie"].astype(str) + "-" + df_facturas["Numero"].astype(str)

    df_nuevas = df_facturas[~df_facturas["Combinada"].isin(existentes)]
    info(f"Facturas nuevas encontradas: {len(df_nuevas)}")

    if df_nuevas.empty:
        warn("No hay nuevas facturas. Pasando al cruce bancario…")
    else:
        # =============================================
        # 4. Extraer clientes
        # =============================================
        cli = ClientsExtractor()
        df_cli = cli.get_client_data()

        info("Unificando facturas nuevas con razón social...")
        df_nuevas = df_nuevas.merge(df_cli, on="RUC", how="left")

        # =============================================
        # 5. Calcular importes
        # =============================================
        calc = Calculator()
        df_calc_nuevas = calc.procesar_facturas(df_nuevas)

        # =============================================
        # 6. Guardar nuevas facturas
        # =============================================
        writer_inv = InvoiceWriter()
        writer_inv.guardar_facturas(df_calc_nuevas)
        writer_inv.close()

    # =============================================
    # 7. Extraer movimientos bancarios
    # =============================================
    bank = BankExtractor()
    df_bancos = bank.get_todos_movimientos()

    # =============================================
    # 8. Cargar facturas pendientes para cruce
    # =============================================
    cursor.execute("SELECT Factura FROM match_results WHERE Estado = 'Pendiente'")
    pendientes = {row[0] for row in cursor.fetchall()}

    info(f"Facturas pendientes para cruce: {len(pendientes)}")

    if not pendientes:
        ok("No hay facturas pendientes. Nada más por hacer hoy.")
        return

    # Tomar SOLO esas facturas desde BD original
    df_pend = df_facturas[df_facturas["Combinada"].isin(pendientes)]

    # Unir nuevamente con clientes
    cli = ClientsExtractor()
    df_cli = cli.get_client_data()

    df_pend = df_pend.merge(df_cli, on="RUC", how="left")

    # Calcular totales
    calc = Calculator()
    df_calc_pend = calc.procesar_facturas(df_pend)

    # =============================================
    # 9. Ejecutar matcher SOLO para estas
    # =============================================
    matcher = Matcher()
    df_match = matcher.cruzar(df_calc_pend, df_bancos)

    # =============================================
    # 10. Guardar resultados del matcher
    # =============================================
    writer_match = MatchWriter()
    writer_match.guardar_matches(df_match)
    writer_match.close()

    # =============================================
    # 11. Guardar log final
    # =============================================
    builder.write_log(
        "IncrementalRun",
        f"Nuevas: {len(df_nuevas)}, Pendientes procesadas: {len(df_pend)}",
    )

    # =============================================
    # 12. Tiempo total
    # =============================================
    end_time = datetime.now()
    duracion = (end_time - start_time).total_seconds()

    ok("⚡ PulseForge – INCREMENTAL COMPLETADO ⚡")
    info(f"🕒 Duración total: {duracion} segundos")


# ======================================================
#   EJECUCIÓN DIRECTA
# ======================================================
if __name__ == "__main__":
    incremental_run()
