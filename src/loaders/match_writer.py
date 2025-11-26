# src/loaders/match_writer.py

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


class MatchWriter:
    """
    Guarda los resultados del cruce bancario (matcher)
    en la BD nueva, de manera incremental.
    """

    def __init__(self):
        self.env = get_env()
        self.db_path = self.env.DB_PATH_NUEVA

        info(f"Conectando a BD nueva para escribir resultados de matcher: {self.db_path}")

        db_dir = os.path.dirname(self.db_path)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
            ok(f"Carpeta creada para BD nueva: {db_dir}")

        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        ok("Conexión lista para match_writer.")


    # =======================================================
    # LOG
    # =======================================================
    def _write_log(self, evento, detalle=""):
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Log en tabla SQLite
        try:
            self.cursor.execute(
                "INSERT INTO logs (timestamp, evento, detalle) VALUES (?, ?, ?)",
                (ts, evento, detalle),
            )
            self.conn.commit()
        except:
            warn("No se pudo escribir log en tabla log.")

        # Log en archivo externo
        logs_dir = os.path.join(os.path.dirname(self.db_path), "logs")
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)

        log_file = os.path.join(logs_dir, f"{datetime.now().strftime('%Y-%m-%d')}.log")

        try:
            with open(log_file, "a", encoding="utf-8") as f:
                f.write(f"[{ts}] {evento} — {detalle}\n")
        except:
            warn("No se pudo escribir log en archivo externo.")


    # =======================================================
    # VERIFICAR SI FACTURA YA EXISTE EN RESULTADOS
    # =======================================================
    def _existe(self, factura_code: str):
        self.cursor.execute(
            "SELECT Estado FROM match_results WHERE Factura = ?",
            (factura_code,),
        )
        return self.cursor.fetchone()


    # =======================================================
    # GUARDAR RESULTADOS DEL MATCHER
    # =======================================================
    def guardar_matches(self, df: pd.DataFrame):
        """
        Guarda los resultados del cruce:
        - Estado
        - Pagos
        - Fechas
        - Cuenta
        """

        info("Escribiendo resultados del Matcher (incremental)...")

        requeridas = [
            "Factura",
            "RUC",
            "Razon_Social",
            "Fecha_Pago",
            "Monto_Pagado",
            "Cuenta_Pago",
            "Tipo_Pago",
            "Estado",
        ]

        faltantes = [c for c in requeridas if c not in df.columns]
        if faltantes:
            error(f"Faltan columnas en match_writer: {faltantes}")
            raise KeyError(f"Columnas faltantes: {faltantes}")

        insertados = 0
        actualizados = 0
        fecha_registro = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        for _, row in df.iterrows():
            factura_code = row["Factura"]
            estado = row["Estado"]

            previo = self._existe(factura_code)

            # ===========================
            # INSERT NUEVO
            # ===========================
            if previo is None:
                self.cursor.execute(
                    """
                    INSERT INTO match_results (
                        Factura, RUC, Razon_Social, Fecha_Pago,
                        Monto_Pagado, Cuenta_Pago, Tipo_Pago,
                        Estado, Fecha_Registro
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        factura_code,
                        row["RUC"],
                        row["Razon_Social"],
                        row["Fecha_Pago"],
                        row["Monto_Pagado"],
                        row["Cuenta_Pagado"],
                        row["Tipo_Pago"],
                        estado,
                        fecha_registro,
                    )
                )
                insertados += 1
                ok(f"Nuevo resultado insertado: {factura_code} -> {estado}")
                self._write_log("Match insertado", f"{factura_code} -> {estado}")
                continue

            # ===================================
            # UPDATE CUANDO CAMBIA EL ESTADO
            # ===================================
            estado_prev = previo[0]

            # Caso 1: Antes Pendiente → ahora Pagada
            if estado_prev != estado:
                warn(f"Estado actualizado: {factura_code} | {estado_prev} → {estado}")

                self.cursor.execute(
                    """
                    UPDATE match_results
                    SET Fecha_Pago = ?,
                        Monto_Pagado = ?,
                        Cuenta_Pago = ?,
                        Tipo_Pago = ?,
                        Estado = ?,
                        Fecha_Registro = ?
                    WHERE Factura = ?
                    """,
                    (
                        row["Fecha_Pago"],
                        row["Monto_Pagado"],
                        row["Cuenta_Pagado"],
                        row["Tipo_Pago"],
                        estado,
                        fecha_registro,
                        factura_code
                    )
                )
                actualizados += 1
                self._write_log("Match actualizado", f"{factura_code}: {estado_prev} → {estado}")
                continue

            # Si no hay cambios
            info(f"Sin cambios: {factura_code} ({estado})")


        self.conn.commit()

        ok(f"Resultados guardados. Insertados: {insertados}, Actualizados: {actualizados}")


    # =======================================================
    # CERRAR
    # =======================================================
    def close(self):
        try:
            self.conn.close()
            ok("Conexión a BD nueva cerrada.")
        except:
            warn("Error cerrando la conexión.")



# =======================================================
# TEST DIRECTO
# =======================================================
if __name__ == "__main__":
    info("🚀 Testeando MatchWriter...")

    from src.extractors.invoices_extractor import InvoicesExtractor
    from src.extractors.bank_extractor import BankExtractor
    from src.extractors.clients_extractor import ClientsExtractor
    from src.transformers.calculator import Calculator
    from src.matchers.matcher import Matcher

    inv = InvoicesExtractor()
    cli = ClientsExtractor()
    bank = BankExtractor()
    calc = Calculator()
    matcher = Matcher()

    df_fact = inv.load_invoices().merge(cli.get_client_data(), on="RUC", how="left")
    df_calc = calc.procesar_facturas(df_fact)
    df_mov = bank.get_todos_movimientos()
    df_match = matcher.cruzar(df_calc, df_mov)

    writer = MatchWriter()
    writer.guardar_matches(df_match)
    writer.close()
