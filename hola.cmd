(forge_env) PS C:\Proyectos\PulseForge> python src/cli.py full
>> 
🔵 Ejecutando PulseForge en modo FULL RUN...
🔵 🚀 FULL RUN — PulseForge iniciando...
🔵 Inicializando configuración global PulseForge...
🔵 Cargando configuración desde archivo .env...
🔵 Variable cargada: PULSEFORGE_DB_TYPE = sqlite
🔵 Variable cargada: PULSEFORGE_DB_PATH = C:\Proyectos\DataPulse\db\datapulse.sqlite
🔵 Variable cargada: PULSEFORGE_NEWDB_PATH = C:\Proyectos\PulseForge\data\pulseforge.sqlite
🔵 Variable cargada: DETRACCION_PORCENTAJE = 0.04
🔵 Variable cargada: IGV = 0.18
🔵 Variable cargada: API_GEMINI_KEY = AIzaSyC6k7eQRqKzfSk7vIEZn07f4BhzFIyuvoM
🔵 Variable cargada: DAYS_TOLERANCE_PAGO = 14
🔵 Variable cargada: MONTO_VARIACION = 0.50
🔵 Variable cargada: CUENTA_EMPRESA = IBK
🔵 Variable cargada: CUENTA_DETRACCION = BN
🔵 Variable cargada: ACTIVAR_IA = true
🔵 Variable cargada: MODO_DEBUG = false
🟢 Variables de entorno cargadas correctamente. PulseForge listo. 🚀
🔵 🏗️ Construyendo estructura pulseforge.sqlite...
🔵 Iniciando constructor de nueva BD PulseForge...
🔵 Creando tablas pulseforge...
🟢 Tablas creadas con éxito en pulseforge.sqlite 🚀
🟢 InvoiceWriter listo para escribir facturas procesadas.
🟢 MatchWriter listo para operar.
🔵 🧹 Limpiando tablas destino (modo FULL)...
🔵 Limpiando tabla facturas_pf...
🟢 Tabla facturas_pf limpiada.
🔵 Limpiando tabla matches_pf...
🟢 Tabla matches_pf limpiada.
🔵 📥 Extrayendo clientes...
🔵 Cargando gestor global de BD...
🔵 Inicializando gestor de bases de datos...
🔵 Conectando a BD origen (DataPulse): C:\Proyectos\DataPulse\db\datapulse.sqlite
🟢 Conexión con BD origen exitosa.
🔵 Preparando conexión a BD destino (PulseForge): C:\Proyectos\PulseForge\data\pulseforge.sqlite
🟢 Conexión con BD destino exitosa.
🟢 Conexiones a bases de datos listas. PulseForge puede trabajar. 🚀
🔵 Inicializando extractor de clientes...
🟢 Extractor de clientes listo para trabajar.
🔵 Cargando tabla de clientes: excel_1_clientes_proveedores
🟢 Registros de clientes cargados: 1003
🟢 Columna RUC detectada como: Ruc / Dni
🟢 Columna Razón Social detectada como: Razon Social
🔵 Vista previa de clientes normalizados:
           RUC                   Razon_Social
0  10005208748  CONDEMAYTA LARICO JHONY SIXTO
1  10104340984     GARCIA ROJAS PABLO ORLANDO
2  10211014194   CAPARACHIN BALDEON TEODOMIRO
3  10238587935           QUISPE QUISPE AQUINO
4  10239605783              MAMANI ALATA RAUL
🔵 📥 Extrayendo facturas...
🔵 Inicializando extractor de facturas...
🟢 Extractor de facturas listo para trabajar.
🔵 Cargando tabla de facturas: excel_6_control_servicios
🟢 Facturas cargadas: 1479
🟢 Columnas críticas detectadas correctamente.
🟢 Facturas normalizadas correctamente.
🔵 📥 Extrayendo movimientos bancarios...
🔵 Inicializando extractor bancario…
🟢 Extractor bancario listo.
🔵 Unificando movimientos bancarios…
🔵 Cargando banco BN desde excel_1_b_banco_nacion
🟢 Movimientos validados: 748
🔵 Cargando banco BBVA-S desde excel_1_b_bbva_soles
🟢 Movimientos validados: 50
🔵 Cargando banco BCP-USD desde excel_1_b_bcp_dolares
🟢 Movimientos validados: 212
🔵 Cargando banco BCP-S desde excel_1_b_bcp_soles
🟢 Movimientos validados: 3083
🔵 Cargando banco IBK-S desde excel_1_b_interbank_soles
🟢 Movimientos validados: 150
🔵 Cargando banco ARE-S desde excel_1_c_arequipa_soles
🟢 Movimientos validados: 782
🔵 Cargando banco FIN-S desde excel_1_c_finanzas_soles
🟢 Movimientos validados: 883
🟢 Total movimientos unificados: 5908
🔵 🧽 Normalizando nombres de columnas globales...
🟢 DataMapper inicializado correctamente.
🔵 🔄 Mapeando clientes...
🔵 Normalizando clientes...
🟢 Clientes normalizados: 1003 registrados.
🔵 🔄 Mapeando facturas...
🔵 Normalizando facturas...
🟢 Facturas normalizadas: 1479 registros.
🔵 🔄 Mapeando movimientos bancarios (blindado)...
🔵 Normalizando movimientos bancarios...
🟢 Movimientos bancarios normalizados: 5908 registros.
🔵 🧮 Ejecutando cálculos financieros...
🟢 Calculator inicializado correctamente.
🔵 Aplicando cálculos financieros a facturas...
🟢 Cálculos financieros aplicados con éxito.
🔵 Preparando movimientos bancarios...
🟢 Movimientos bancarios preparados correctamente.
🔵 🧩 Matching iniciado...
🟢 Matcher ultra-blindado inicializado ✔️
🔵 🔥 Iniciando matching ultra-blindado...
🟢 🧩 Matching ULTRA-BLINDADO completado sin errores.
🔵 📤 Guardando facturas en la BD destino...
🔵 Insertando 1250 facturas en PulseForge...
🟢 Facturas insertadas correctamente en facturas_pf 🚀
🔵 📤 Guardando matches en la BD destino...
🔵 Insertando 1250 matches en PulseForge...
🟢 Matches insertados correctamente en matches_pf 🚀
🟢 🎯 FULL RUN completado correctamente.

================= RESULTADO FINAL =================
Facturas procesadas:        1250
Movimientos bancarios:      5908
Matches generados:          1250
===================================================

🟢 Proceso FULL terminado. ✔