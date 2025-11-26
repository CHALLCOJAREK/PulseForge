# src/matchers/matcher.py

import sys, os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

import pandas as pd
from datetime import timedelta
from src.core.env_loader import get_env

# Prints estilo Fénix
def info(msg): print(f"🔵 {msg}")
def ok(msg): print(f"🟢 {msg}")
def warn(msg): print(f"🟡 {msg}")
def error(msg): print(f"🔴 {msg}")


class Matcher:
    """
    Cruza facturas con movimientos bancarios aplicando:
    1) Fecha emisión + forma de pago ± tolerancia
    2) Variación de monto ± 0.50
    3) Detracción primero, luego neto
    4) Validación por nombre empresa y/o IA
    5) Manejo de pagos múltiples
    """

    def __init__(self):
        self.env = get_env()
        self.tolerancia_dias = self.env.DAYS_TOLERANCE_PAGO
        self.variacion_monto = self.env.MONTO_VARIACION
        self.activar_ia = (str(self.env.ACTIVAR_IA).lower() == "true")

        info("Inicializando Matcher (cruce de facturas vs bancos)...")
        ok("Matcher listo. 🧠🔥")


    # =======================================================
    #     UTILIDAD: validar nombres de empresa
    # =======================================================
    def _empresa_match(self, razon_social, descripcion_banco):
        """
        Verifica si el nombre de la empresa aparece en la descripción bancaria.
        Si ACTIVAR_IA=true, usa IA (Gemini) para validar similitud.
        """

        razon = str(razon_social).lower()
        desc  = str(descripcion_banco).lower()

        # Coincidencia directa
        if razon in desc:
            return True

        # Palabras clave
        palabras = razon.split()
        coincidencias = sum(1 for p in palabras if p in desc)

        if coincidencias >= 2:
            return True

        # Si IA está activada
        if self.activar_ia:
            try:
                import google.generativeai as genai
                genai.configure(api_key=self.env.API_GEMINI_KEY)
                prompt = f"""
                ¿El siguiente texto bancario está relacionado con la empresa '{razon_social}'?
                Descripción: {descripcion_banco}
                Responde solo SI o NO.
                """
                model = genai.GenerativeModel("gemini-pro")
                r = model.generate_content(prompt).text.lower()

                if "si" in r:
                    return True

            except Exception as e:
                warn(f"IA no disponible: {e}")

        return False


    # =======================================================
    #     UTILIDAD: buscar coincidencias de monto
    # =======================================================
    def _match_monto(self, monto_esperado, monto_banco):
        return abs(monto_esperado - monto_banco) <= self.variacion_monto


    # =======================================================
    #     UTILIDAD: validar rango de fechas
    # =======================================================
    def _match_fechas(self, fecha_venc, fecha_banco):
        """
        La regla dice:
        Fecha emisión + forma de pago  ± 14 días  ≈  fecha de abono
        """
        rango_min = fecha_venc - timedelta(days=self.tolerancia_dias)
        rango_max = fecha_venc + timedelta(days=self.tolerancia_dias)
        return rango_min <= fecha_banco <= rango_max


    # =======================================================
    #     CRUCE PRINCIPAL
    # =======================================================
    def cruzar(self, df_facturas, df_bancos):
        """
        Devuelve un DataFrame con columnas:
        - Estado (Pendiente / Pagada)
        - Fecha_Pago
        - Monto_Pagado
        - Cuenta_Pago
        - Tipo_Pago (Detraccion / Neto)
        """

        info("Iniciando cruce inteligente de facturas...")

        resultados = []

        for idx, fac in df_facturas.iterrows():
            serie = fac["Serie"]
            numero = fac["Numero"]
            combinada = f"{serie}-{numero}"

            razon_social = fac["Razon_Social"]
            monto_neto = fac["Monto_Neto_calc"]
            monto_detra = fac["Detraccion_calc"]
            fecha_venc = fac["Fecha_Vencimiento"]

            info(f"📄 Procesando factura {combinada} de {razon_social}")

            # Buscar DETRACCIÓN primero en BN
            df_bn = df_bancos[df_bancos["Cuenta"] == self.env.CUENTA_DETRACCION]
            df_bn = df_bn.copy()

            df_bn["Fecha"] = pd.to_datetime(df_bn["Fecha"], errors="coerce")
            df_bn["Monto"] = df_bn["Monto"].astype(float)

            match_detraccion = df_bn[
                df_bn["Monto"].apply(lambda m: self._match_monto(monto_detra, m))
                &
                df_bn["Fecha"].apply(lambda f: self._match_fechas(fecha_venc, f))
            ]

            if not match_detraccion.empty:
                ok(f"💰 Detracción encontrada para {combinada}.")
                det = match_detraccion.iloc[0]

                # Ahora buscar NETO en IBK
                df_ibk = df_bancos[df_bancos["Cuenta"] == self.env.CUENTA_EMPRESA].copy()
                df_ibk["Fecha"] = pd.to_datetime(df_ibk["Fecha"], errors="coerce")
                df_ibk["Monto"] = df_ibk["Monto"].astype(float)

                match_neto = df_ibk[
                    df_ibk["Monto"].apply(lambda m: self._match_monto(monto_neto, m))
                    &
                    df_ibk["Fecha"].apply(lambda f: self._match_fechas(fecha_venc, f))
                    &
                    df_ibk["Descripcion"].apply(lambda d: self._empresa_match(razon_social, d))
                ]

                if not match_neto.empty:
                    ok(f"🟢 FACTURA COMPLETAMENTE PAGADA: {combinada}")
                    pago = match_neto.iloc[0]

                    resultados.append({
                        "Factura": combinada,
                        "RUC": fac["RUC"],
                        "Razon_Social": razon_social,
                        "Fecha_Pago": pago["Fecha"],
                        "Monto_Pagado": pago["Monto"],
                        "Cuenta_Pago": pago["Cuenta"],
                        "Tipo_Pago": "Completo",
                        "Estado": "Pagada"
                    })
                    continue

                warn(f"Detracción encontrada, pero NETO aún no aparece para {combinada}.")
                resultados.append({
                    "Factura": combinada,
                    "RUC": fac["RUC"],
                    "Razon_Social": razon_social,
                    "Fecha_Pago": det["Fecha"],
                    "Monto_Pagado": det["Monto"],
                    "Cuenta_Pago": det["Cuenta"],
                    "Tipo_Pago": "Detracción",
                    "Estado": "Pendiente Neto"
                })
                continue

            warn(f"No se halló detracción para {combinada}. Buscando solo NETO...")

            # Sin detracción: buscar pago único
            df_ibk = df_bancos[df_bancos["Cuenta"] == self.env.CUENTA_EMPRESA].copy()
            df_ibk["Fecha"] = pd.to_datetime(df_ibk["Fecha"], errors="coerce")
            df_ibk["Monto"] = df_ibk["Monto"].astype(float)

            match_single = df_ibk[
                df_ibk["Monto"].apply(lambda m: self._match_monto(monto_neto, m))
                &
                df_ibk["Fecha"].apply(lambda f: self._match_fechas(fecha_venc, f))
                &
                df_ibk["Descripcion"].apply(lambda d: self._empresa_match(razon_social, d))
            ]

            if not match_single.empty:
                ok(f"🟢 FACTURA PAGADA (sin detracción): {combinada}")
                pago = match_single.iloc[0]

                resultados.append({
                    "Factura": combinada,
                    "RUC": fac["RUC"],
                    "Razon_Social": razon_social,
                    "Fecha_Pago": pago["Fecha"],
                    "Monto_Pagado": pago["Monto"],
                    "Cuenta_Pago": pago["Cuenta"],
                    "Tipo_Pago": "Unico",
                    "Estado": "Pagada"
                })
                continue

            warn(f"Factura NO tiene coincidencias bancarias: {combinada}")
            resultados.append({
                "Factura": combinada,
                "RUC": fac["RUC"],
                "Razon_Social": razon_social,
                "Fecha_Pago": None,
                "Monto_Pagado": 0,
                "Cuenta_Pago": None,
                "Tipo_Pago": "Ninguno",
                "Estado": "Pendiente"
            })

        ok("Cruce finalizado ✓")

        return pd.DataFrame(resultados)



# =======================================================
#   TEST DIRECTO
# =======================================================
if __name__ == "__main__":
    info("🚀 Testeando Matcher...")

    from src.extractors.invoices_extractor import InvoicesExtractor
    from src.extractors.bank_extractor import BankExtractor
    from src.transformers.calculator import Calculator
    from src.extractors.clients_extractor import ClientsExtractor

    inv = InvoicesExtractor()
    cli = ClientsExtractor()
    calc = Calculator()
    bank = BankExtractor()

    df_fact = inv.load_invoices()
    df_clients = cli.get_client_data()

    # unir razon social
    df_fact = df_fact.merge(df_clients, on="RUC", how="left")

    df_fact = calc.procesar_facturas(df_fact)
    df_mov = bank.get_todos_movimientos()

    matcher = Matcher()
    df_result = matcher.cruzar(df_fact, df_mov)

    print("\n🔍 RESULTADO FINAL:")
    print(df_result.head())
