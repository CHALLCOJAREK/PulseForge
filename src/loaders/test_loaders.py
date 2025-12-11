# src/loaders/test_loaders.py
from __future__ import annotations
import sys
from pathlib import Path
import sqlite3

# ----------------------------------------------------------------------
# Bootstrap
# ----------------------------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# ----------------------------------------------------------------------
# Core
# ----------------------------------------------------------------------
from src.core.logger import info, ok, warn, error
from src.core.env_loader import get_env, get_config

# ----------------------------------------------------------------------
# Loaders
# ----------------------------------------------------------------------
from src.loaders.newdb_builder import NewDBBuilder
from src.loaders.invoice_writer import InvoiceWriter
from src.loaders.bank_writer import BankWriter
from src.loaders.clients_writer import ClientsWriter

# ----------------------------------------------------------------------
# Extractors
# ----------------------------------------------------------------------
from src.extractors.bank_extractor import BankExtractor
from src.extractors.clients_extractor import ClientsExtractor
from src.extractors.invoices_extractor import InvoicesExtractor


# ======================================================================
#              PIPELINE COMPLETO (EXTRACT → LOAD → VERIFY)
# ======================================================================
def main():

    info("=== TEST LOADERS · PIPELINE COMPLETO (E2E) ===")

    # ******************************************************************
    # 1. Cargar .env ANTES de cualquier get_env()
    # ******************************************************************
    cfg = get_config()   # Esto carga el .env y settings.json
    ok("Configuración cargada. Variables de entorno listas.")

    # ******************************************************************
    # 2. Construir/verificar BD destino
    # ******************************************************************
    info("Creando/verificando BD destino…")
    NewDBBuilder()   # Crea todas las tablas pf_*

    db_path_str = get_env("PULSEFORGE_NEWDB_PATH")
    if not db_path_str:
        error("❌ PULSEFORGE_NEWDB_PATH no configurado. Revisa el .env.")
        return

    db_path = Path(db_path_str)
    ok(f"BD destino: {db_path}")

    # ******************************************************************
    # 3. Ejecutar extractores reales desde DataPulse
    # ******************************************************************
    info("Ejecutando extractores…")

    be = BankExtractor()
    ce = ClientsExtractor()
    ie = InvoicesExtractor()

    bancos_df = be.extract()
    clientes_df = ce.extract()
    facturas_list = ie.extract()

    # InvoicesExtractor ya devuelve lista de dicts
    bancos_list = bancos_df.to_dict(orient="records")
    clientes_list = clientes_df.to_dict(orient="records")

    ok(f"Movimientos extraídos: {len(bancos_list)}")
    ok(f"Clientes extraídos: {len(clientes_list)}")
    ok(f"Facturas extraídas: {len(facturas_list)}")

    # ******************************************************************
    # 4. Insertar datos en BD destino
    # ******************************************************************
    info("Insertando datos en BD destino…")

    bw = BankWriter()
    cw = ClientsWriter()
    iw = InvoiceWriter()

    bw.save_many(bancos_list)
    cw.save_many(clientes_list)
    iw.save_many(facturas_list)

    ok("Datos insertados correctamente en la BD destino.")

    # ******************************************************************
    # 5. Leer BD destino para verificación rápida
    # ******************************************************************
    info("Leyendo datos de BD destino para verificación…")

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    # Movimientos bancarios
    cur.execute("SELECT id, fecha, descripcion, monto, banco_codigo FROM bancos_pf LIMIT 5;")
    rows_bank = cur.fetchall()
    info("📌 Vista previa movimientos bancarios:")
    for r in rows_bank:
        print("   ", r)

    # Clientes
    cur.execute("SELECT id, ruc, razon_social FROM clientes_pf LIMIT 5;")
    rows_clients = cur.fetchall()
    info("📌 Vista previa clientes:")
    for r in rows_clients:
        print("   ", r)

    # Facturas
    cur.execute("SELECT id, serie, numero, subtotal, total FROM facturas_pf LIMIT 5;")
    rows_inv = cur.fetchall()
    info("📌 Vista previa facturas:")
    for r in rows_inv:
        print("   ", r)

    conn.close()

    ok("=== TEST LOADERS COMPLETADO EXITOSAMENTE ===")


# ======================================================================
# Ejecución directa
# ======================================================================
if __name__ == "__main__":
    main()
