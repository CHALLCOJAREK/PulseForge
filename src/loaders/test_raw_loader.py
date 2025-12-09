# src/loaders/test_raw_loader.py
from __future__ import annotations

import sys
from pathlib import Path
import sqlite3

# ---------------------------------------------------------
# BOOTSTRAP
# ---------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_config

from src.loaders.newdb_builder import NewDBBuilder
from src.loaders.raw_writer import RawWriter

from src.extractors.clients_extractor import ClientsExtractor
from src.extractors.invoices_extractor import InvoicesExtractor
from src.extractors.bank_extractor import BankExtractor


# =========================================================
# HELPER → contar filas rápido
# =========================================================
def count_rows(conn, table: str) -> int:
    try:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        return cur.fetchone()[0]
    except Exception:
        return -1


# =========================================================
# TEST PRINCIPAL
# =========================================================
if __name__ == "__main__":
    info("===============================================")
    info("      TEST RAW LOADER · PulseForge DESTINO")
    info("===============================================")

    # -----------------------------------------------------
    # 1) Crear BD destino
    # -----------------------------------------------------
    builder = NewDBBuilder()
    builder.build(reset=True)

    cfg = get_config()
    db_path = Path(cfg.newdb_path)

    if not db_path.exists():
        error("BD destino NO creada.")
        sys.exit(1)

    ok(f"BD destino lista → {db_path}")

    # -----------------------------------------------------
    # 2) Ejecutar extractores reales
    # -----------------------------------------------------
    info("Extrayendo clientes…")
    clientes = ClientsExtractor().extract()

    info("Extrayendo facturas…")
    facturas = InvoicesExtractor().extract()

    info("Extrayendo movimientos bancarios…")
    bancos = BankExtractor().extract()

    ok(f"Clientes: {len(clientes)}")
    ok(f"Facturas: {len(facturas)}")
    ok(f"Movimientos: {len(bancos)}")

    # -----------------------------------------------------
    # 3) Guardar en BD destino
    # -----------------------------------------------------
    writer = RawWriter()

    info("Insertando CLIENTES en BD destino…")
    writer.insert_clientes(clientes.to_dict(orient="records"))

    info("Insertando FACTURAS en BD destino…")
    writer.insert_facturas(facturas.to_dict(orient="records"))

    info("Insertando MOVIMIENTOS en BD destino…")
    writer.insert_movimientos(bancos.to_dict(orient="records"))

    ok("Inserciones completadas.")

    # -----------------------------------------------------
    # 4) Validar BD destino
    # -----------------------------------------------------
    info("Validando estructura y conteos…")

    conn = sqlite3.connect(db_path)

    tablas = {
        "clientes_pf",
        "facturas_pf",
        "movimientos_pf",
        "matches_pf",
        "match_detalles_pf",
        "logs_pf",
    }

    for t in tablas:
        n = count_rows(conn, t)
        if n >= 0:
            ok(f"Tabla {t} → {n} filas")
        else:
            warn(f"Tabla {t} NO encontrada")

    conn.close()

    info("----------------------------------------------")
    ok("TEST RAW LOADER FINALIZADO CON ÉXITO")
    info("----------------------------------------------")
