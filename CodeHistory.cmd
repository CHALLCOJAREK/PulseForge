(forge_env) PS C:\Proyectos\PulseForge> & C:/Proyectos/PulseForge/forge_env/Scripts/python.exe c:/Proyectos/PulseForge/src/pipelines/incremental.py
🔵 INFO Inicializando PipelineIncremental…
🔵 INFO Inicializando PipelineClients…
🔵 INFO Inicializando ClientsExtractor…
🔵 INFO Inicializando DataMapper PulseForge…
🟢 OK DataMapper cargado correctamente.
🟢 OK ClientsExtractor listo. Tabla clientes = 'excel_1_clientes_proveedores'
🔵 INFO Inicializando ClientsWriter…
🟢 .env cargado correctamente desde: C:\Proyectos\PulseForge\.env
🟢 OK ClientsWriter listo. BD destino = C:\Proyectos\PulseForge\data\pulseforge.sqlite
🟢 OK PipelineClients inicializado correctamente.
🔵 INFO Inicializando PipelineFacturas…
🔵 INFO Inicializando InvoicesExtractor…
🔵 INFO Inicializando DataMapper PulseForge…
🟢 OK DataMapper cargado correctamente.
🟢 OK InvoicesExtractor listo. Tabla = 'excel_6_control_servicios'
🔵 INFO Inicializando InvoiceWriter…
🟢 OK InvoiceWriter listo. BD destino: C:\Proyectos\PulseForge\data\pulseforge.sqlite
🟢 OK PipelineFacturas inicializado correctamente.
🔵 INFO Inicializando PipelineBancos…
🔵 INFO Inicializando BankExtractor…
🔵 INFO Inicializando DataMapper PulseForge…
🟢 OK DataMapper cargado correctamente.
🟢 OK BankExtractor listo. Tablas bancos detectadas: ['banco_nacion', 'banco_bbva_soles', 'banco_bcp_dolares', 'banco_bcp_soles', 'banco_interbank_soles', 'banco_arequipa_soles', 'banco_finanzas_soles']
🔵 INFO Inicializando BankWriter…
🟢 OK BankWriter listo. BD destino: C:\Proyectos\PulseForge\data\pulseforge.sqlite
🟢 OK PipelineBancos inicializado correctamente.
🔵 INFO Inicializando PipelineMatcher…
🔵 INFO Conectando a BD PulseForge → C:\Proyectos\PulseForge\data\pulseforge.sqlite
🔵 INFO Inicializando MatchWriter…
🟢 OK MatchWriter listo. BD destino → C:\Proyectos\PulseForge\data\pulseforge.sqlite
🔵 INFO Inicializando MatcherEngine…
🟢 OK MatcherEngine listo.
🟢 OK PipelineMatcher listo.
🟢 OK PipelineIncremental inicializado correctamente.
🔵 INFO 🚀 Ejecutando PipelineIncremental…
🔵 INFO 📂 [1/4] Clientes – incremental
🔵 INFO Conectando a BD origen SQLite → C:\Proyectos\DataPulse\db\datapulse.sqlite
🔵 INFO Leyendo clientes desde tabla SQLite 'excel_1_clientes_proveedores'…
🟢 OK Clientes crudos leídos: 1003 filas.
🔵 INFO Columna RUC detectada → 'Ruc / Dni'
🔵 INFO Columna Razón Social detectada → 'Razon Social'
🟢 OK Clientes normalizados a esquema estándar (RUC / Razon_Social).
🔵 INFO Normalizando clientes…
🟢 OK Clientes normalizados: 1003 registros.
🟢 OK Clientes mapeados OK: 1003 registros.
🟢 OK source_hash generado para clientes.
🔵 INFO [INCREMENTAL] clientes_pf: total=1003, nuevos=0
🟡 WARN No hay nuevos clientes para insertar.
🔵 INFO 📄 [2/4] Facturas – incremental
🔵 INFO Conectando a SQLite → C:\Proyectos\DataPulse\db\datapulse.sqlite
🔵 INFO Leyendo facturas desde 'excel_6_control_servicios'…
🟢 OK Facturas crudas leídas: 1479 filas.
🟢 OK Facturas normalizadas y renombradas correctamente.
🔵 INFO Normalizando facturas…
🟢 OK Facturas normalizadas: 1479 registros.
🟢 OK Facturas mapeadas OK: 1479 filas.
🟢 OK source_hash generado para facturas.
🔵 INFO [INCREMENTAL] facturas_pf: total=1479, nuevos=1479
🔵 INFO Conectando a BD PulseForge → C:\Proyectos\PulseForge\data\pulseforge.sqlite
🔵 INFO Insertando facturas en facturas_pf…
🟢 OK Facturas insertadas en facturas_pf: 1479
🔵 INFO 🏦 [3/4] Bancos – incremental
🔵 INFO Conectando a BD origen SQLite → C:\Proyectos\DataPulse\db\datapulse.sqlite
🔵 INFO Leyendo movimientos de banco 'BN' desde tabla 'excel_1_b_banco_nacion'…
🟢 OK Movimientos crudos leídos de 'excel_1_b_banco_nacion': 829 filas.
🔵 INFO Normalizando movimientos bancarios…
🟢 OK Movimientos bancarios normalizados: 829 registros.
🟢 OK Movimientos normalizados para banco 'BN': 829 filas.
🔵 INFO Leyendo movimientos de banco 'BBVA' desde tabla 'excel_1_b_bbva_soles'…
🟢 OK Movimientos crudos leídos de 'excel_1_b_bbva_soles': 50 filas.
🔵 INFO Normalizando movimientos bancarios…
🟢 OK Movimientos bancarios normalizados: 50 registros.
🟢 OK Movimientos normalizados para banco 'BBVA': 50 filas.
🔵 INFO Leyendo movimientos de banco 'BCP' desde tabla 'excel_1_b_bcp_dolares'…
🟢 OK Movimientos crudos leídos de 'excel_1_b_bcp_dolares': 434 filas.
🔵 INFO Normalizando movimientos bancarios…
🟢 OK Movimientos bancarios normalizados: 434 registros.
🟢 OK Movimientos normalizados para banco 'BCP': 434 filas.
🔵 INFO Leyendo movimientos de banco 'BCP' desde tabla 'excel_1_b_bcp_soles'…
🟢 OK Movimientos crudos leídos de 'excel_1_b_bcp_soles': 4646 filas.
🔵 INFO Normalizando movimientos bancarios…
🟢 OK Movimientos bancarios normalizados: 4646 registros.
🟢 OK Movimientos normalizados para banco 'BCP': 4646 filas.
🔵 INFO Leyendo movimientos de banco 'IBK' desde tabla 'excel_1_b_interbank_soles'…
🟢 OK Movimientos crudos leídos de 'excel_1_b_interbank_soles': 189 filas.
🔵 INFO Normalizando movimientos bancarios…
🟢 OK Movimientos bancarios normalizados: 189 registros.
🟢 OK Movimientos normalizados para banco 'IBK': 189 filas.
🔵 INFO Leyendo movimientos de banco 'AREQUIPA' desde tabla 'excel_1_c_arequipa_soles'…
🟢 OK Movimientos crudos leídos de 'excel_1_c_arequipa_soles': 1110 filas.
🔵 INFO Normalizando movimientos bancarios…
🟢 OK Movimientos bancarios normalizados: 1110 registros.
🟢 OK Movimientos normalizados para banco 'AREQUIPA': 1110 filas.
🔵 INFO Leyendo movimientos de banco 'FINANZAS' desde tabla 'excel_1_c_finanzas_soles'…
🟢 OK Movimientos crudos leídos de 'excel_1_c_finanzas_soles': 985 filas.
🔵 INFO Normalizando movimientos bancarios…
🟢 OK Movimientos bancarios normalizados: 985 registros.
🟢 OK Movimientos normalizados para banco 'FINANZAS': 985 filas.
🟢 OK Total movimientos bancarios normalizados (todos los bancos): 8243 registros.
🟢 OK source_hash generado para movimientos bancarios.
🔵 INFO [INCREMENTAL] movimientos_pf: total=8243, nuevos=8243
🟡 WARN [BANK_WRITER] Columna faltante 'Tipo_Mov'. Se crea vacía.
🟡 WARN [BANK_WRITER] Columna faltante 'Destinatario'. Se crea vacía.
🟡 WARN [BANK_WRITER] Columna faltante 'Tipo_Documento'. Se crea vacía.
🔵 INFO Conectando a BD PulseForge → C:\Proyectos\PulseForge\data\pulseforge.sqlite
🔵 INFO Insertando movimientos bancarios en movimientos_pf…
🟢 OK Movimientos insertados en movimientos_pf: 8243
🔵 INFO 🤖 [4/4] Matcher – incremental (evita duplicados por hash)
🔵 INFO 🚀 Ejecutando PipelineMatcher…
🔵 INFO 📄 Cargando facturas desde facturas_pf…
🟢 OK Facturas cargadas: 5916
🔵 INFO 🏦 Cargando movimientos desde movimientos_pf…
🟢 OK Movimientos cargados: 16486
🔵 INFO 🤖 Ejecutando motor de Matching IA/Reglas…
🔵 INFO Ejecutando reglas de coincidencia…





















