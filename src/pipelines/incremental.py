# src/pipelines/incremental.py

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from datetime import datetime
import sqlite3

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
from src.transformers.matcher import Matcher

# Writers
from src.loaders.invoice_writer import InvoiceWriter
from src.loaders.match_writer import MatchWriter

# Prints Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


def incremental_run():
    info("⚡ Iniciando ejecución INCREMENTAL de PulseForge...")
    start_time = datetime.now()

    # =====================================================================
    # 0. Cargar entorno y columnas reales desde settings.json
    # =====================================================================
    env = get_env()
    from json import load

    settings_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "../../config/settings.json")
    )
    with open(settings_path, "r", encoding="utf-8") as f:
        settings = load(f)

    col = settings["columnas_facturas"]   # columnas reales del JSON

    # =====================================================================
    # 1. Asegurar BD nueva lista
    # =====================================================================
    builder = NewDBBuilder()
    builder.build()

    # =====================================================================
    # 2. Leer facturas ya procesadas en PulseForge
    # =====================================================================
    conn = sqlite3.connect(env.DB_PATH_NUEVA)
    cursor = conn.cursor()

    cursor.execute("SELECT Factura FROM facturas_procesadas")
    existentes = {row[0] for row in cursor.fetchall()}
    info(f"Facturas ya guardadas en BD Nueva: {len(existentes)}")

    # =====================================================================
    # 3. Extraer facturas desde DataPulse
    # =====================================================================
    inv = InvoicesExtractor()
    df_facturas = inv.load_invoices()

    # Crear Combinada usando el nombre real de columnas
    df_facturas["Combinada"] = (
        df_facturas[col["serie"]].astype(str)
        + "-"
        + df_facturas[col["numero"]].astype(str)
    )

    # =====================================================================
    # 4. Filtrar solo facturas nuevas
    # =====================================================================
    df_nuevas = df_facturas[~df_facturas["Combinada"].isin(existentes)]

    info(f"Facturas nuevas encontradas: {len(df_nuevas)}")

    # =====================================================================
    # 5. Si hay nuevas facturas → procesarlas
    # =====================================================================
    if not df_nuevas.empty:
        # 5.1 Extraer clientes
        cli = ClientsExtractor()
        df_cli = cli.get_client_data()

        info("Unificando facturas nuevas con razón social...")
        df_nuevas = df_nuevas.merge(df_cli, on="RUC", how="left")

        # 5.2 Calcular importes
        calc = Calculator()
        df_calc_nuevas = calc.procesar_facturas(df_nuevas)

        # 5.3 Guardar nuevas facturas
        writer_inv = InvoiceWriter()
        writer_inv.guardar_facturas(df_calc_nuevas)
        writer_inv.close()

    else:
        warn("No hay nuevas facturas. Continuando con cruce bancario...")

    # =====================================================================
    # 6. Cargar bancos
    # =====================================================================
    bank = BankExtractor()
    df_bancos = bank.get_todos_movimientos()

    # =====================================================================
    # 7. Cargar facturas pendientes para cruce
    # =====================================================================
    cursor.execute("SELECT Factura FROM match_results WHERE Estado = 'Pendiente'")
    pendientes = {row[0] for row in cursor.fetchall()}

    info(f"Facturas pendientes para cruce: {len(pendientes)}")

    if not pendientes:
        ok("No hay facturas pendientes. Nada más por hacer.")
        return

    # Tomar SOLO esas facturas desde df_facturas
    df_pend = df_facturas[df_facturas["Combinada"].isin(pendientes)]

    # Unir con clientes
    cli = ClientsExtractor()
    df_cli = cli.get_client_data()
    df_pend = df_pend.merge(df_cli, on="RUC", how="left")

    # Recalcular importes
    calc = Calculator()
    df_calc_pend = calc.procesar_facturas(df_pend)

    # =====================================================================
    # 8. Ejecutar matcher
    # =====================================================================
    matcher = Matcher()
    df_match = matcher.cruzar(df_calc_pend, df_bancos)

    # =====================================================================
    # 9. Guardar matches
    # =====================================================================
    writer_match = MatchWriter()
    writer_match.guardar_matches(df_match)
    writer_match.close()

    # =====================================================================
    # 10. Log final
    # =====================================================================
    builder.write_log(
        "IncrementalRun",
        f"Nuevas={len(df_nuevas)} | Pendientes procesadas={len(df_pend)}"
    )

    # =====================================================================
    # 11. Tiempo total
    # =====================================================================
    end_time = datetime.now()
    duracion = (end_time - start_time).total_seconds()

    ok("⚡ PulseForge – INCREMENTAL COMPLETADO ⚡")
    info(f"🕒 Duración total: {duracion} segundos")


if __name__ == "__main__":
    incremental_run()
