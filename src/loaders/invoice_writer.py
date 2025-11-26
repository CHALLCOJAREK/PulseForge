# src/loaders/invoice_writer.py

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import sqlite3
from datetime import datetime
import pandas as pd
from src.core.env_loader import get_env

# Prints estilo Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class InvoiceWriter:
    """
    Escribe facturas procesadas en la BD nueva (pulseforge.sqlite),
    de forma incremental (INSERT/UPDATE según corresponda).
    """

    def __init__(self):
        self.env = get_env()
        self.db_path = self.env.DB_PATH_NUEVA

        info(f"Conectando a BD nueva para escribir facturas: {self.db_path}")
        db_dir = os.path.dirname(self.db_path)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
            ok(f"Carpeta creada para BD nueva: {db_dir}")

        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        ok("Conexión a BD nueva lista.")

    # ============================
    # LOG
    # ============================
    def _write_log(self, evento, detalle=""):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # En tabla logs (si existe)
        try:
            self.cursor.execute(
                "INSERT INTO logs (timestamp, evento, detalle) VALUES (?, ?, ?)",
                (ts, evento, detalle),
            )
            self.conn.commit()
        except Exception as e:
            warn(f"No se pudo escribir log en tabla logs: {e}")

        # En archivo externo
        logs_dir = os.path.join(os.path.dirname(self.db_path), "logs")
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)

        log_file = os.path.join(logs_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log")
        try:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(f"[{ts}] {evento} — {detalle}\n")
        except Exception as e:
            warn(f"No se pudo escribir log en archivo: {e}")

    # ============================
    # UPSET (INSERT/UPDATE)
    # ============================
    def _factura_existe(self, factura_code: str) -> bool:
        self.cursor.execute(
            "SELECT 1 FROM facturas_procesadas WHERE Factura = ?",
            (factura_code,),
        )
        return self.cursor.fetchone() is not None

    def guardar_facturas(self, df: pd.DataFrame):
        """
        Espera un DataFrame que venga de Calculator,
        con al menos estas columnas:

        - Combinada
        - RUC
        - Razon_Social
        - Subtotal_calc
        - IGV_calc
        - Total_calc
        - Detraccion_calc
        - Monto_Neto_calc
        - Fecha_Emision_Parsed
        - Fecha_Vencimiento
        """
        info("Escribiendo facturas procesadas en BD nueva (incremental)...")

        requeridas = [
            "Combinada",
            "RUC",
            "Razon_Social",
            "Subtotal_calc",
            "IGV_calc",
            "Total_calc",
            "Detraccion_calc",
            "Monto_Neto_calc",
            "Fecha_Emision_Parsed",
            "Fecha_Vencimiento",
        ]

        faltantes = [c for c in requeridas if c not in df.columns]
        if faltantes:
            error(f"Faltan columnas en DataFrame para InvoiceWriter: {faltantes}")
            raise KeyError(f"Columnas faltantes para InvoiceWriter: {faltantes}")

        insertados = 0
        actualizados = 0
        fecha_registro = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for _, row in df.iterrows():
            factura_code = str(row["Combinada"])
            ruc = str(row["RUC"])
            razon = str(row["Razon_Social"]) if row["Razon_Social"] is not None else ""

            subtotal = row["Subtotal_calc"]
            igv = row["IGV_calc"]
            total = row["Total_calc"]
            detraccion = row["Detraccion_calc"]
            neto = row["Monto_Neto_calc"]

            fecha_emision = None
            if pd.notnull(row["Fecha_Emision_Parsed"]):
                fecha_emision = row["Fecha_Emision_Parsed"].strftime("%Y-%m-%d")

            fecha_venc = None
            if pd.notnull(row["Fecha_Vencimiento"]):
                fecha_venc = row["Fecha_Vencimiento"].strftime("%Y-%m-%d")

            if not self._factura_existe(factura_code):
                # INSERT
                self.cursor.execute(
                    """
                    INSERT INTO facturas_procesadas (
                        Factura, RUC, Razon_Social,
                        Subtotal, IGV, Total, Detraccion, Monto_Neto,
                        Fecha_Emision, Fecha_Vencimiento, Fecha_Registro
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        factura_code,
                        ruc,
                        razon,
                        subtotal,
                        igv,
                        total,
                        detraccion,
                        neto,
                        fecha_emision,
                        fecha_venc,
                        fecha_registro,
                    ),
                )
                insertados += 1
                ok(f"Nueva factura insertada: {factura_code}")
                self._write_log("Factura insertada", f"{factura_code}")
            else:
                # UPDATE
                self.cursor.execute(
                    """
                    UPDATE facturas_procesadas
                    SET RUC = ?,
                        Razon_Social = ?,
                        Subtotal = ?,
                        IGV = ?,
                        Total = ?,
                        Detraccion = ?,
                        Monto_Neto = ?,
                        Fecha_Emision = ?,
                        Fecha_Vencimiento = ?,
                        Fecha_Registro = ?
                    WHERE Factura = ?
                    """,
                    (
                        ruc,
                        razon,
                        subtotal,
                        igv,
                        total,
                        detraccion,
                        neto,
                        fecha_emision,
                        fecha_venc,
                        fecha_registro,
                        factura_code,
                    ),
                )
                actualizados += 1
                warn(f"Factura actualizada: {factura_code}")
                self._write_log("Factura actualizada", f"{factura_code}")

        self.conn.commit()
        ok(f"Facturas guardadas. Insertadas: {insertados}, Actualizadas: {actualizados}")

    def close(self):
        try:
            self.conn.close()
            ok("Conexión a BD nueva cerrada correctamente.")
        except Exception as e:
            warn(f"Error al cerrar conexión: {e}")


# =======================================================
#   TEST DIRECTO
# =======================================================
if __name__ == "__main__":
    info("🚀 Testeando InvoiceWriter...")

    from src.extractors.invoices_extractor import InvoicesExtractor
    from src.extractors.clients_extractor import ClientsExtractor
    from src.transformers.calculator import Calculator

    inv = InvoicesExtractor()
    cli = ClientsExtractor()
    calc = Calculator()

    df_fact = inv.load_invoices()
    df_cli = cli.get_client_data()

    # unir razon social
    df_fact = df_fact.merge(df_cli, on="RUC", how="left")

    df_calc = calc.procesar_facturas(df_fact)

    writer = InvoiceWriter()
    writer.guardar_facturas(df_calc)
    writer.close()
