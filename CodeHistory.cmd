(forge_env) PS C:\Proyectos\PulseForge> & C:/Proyectos/PulseForge/forge_env/Scripts/python.exe c:/Proyectos/PulseForge/src/core/test.py
🔵 INFO === INICIANDO TEST CORE PULSEFORGE ===
🔵 INFO Probando carga de configuración...
🔵 INFO Cargando configuración universal...
🟢 OK .env cargado.
🟢 OK settings.json cargado.
🟢 OK constants.json cargado.
🟢 OK Configuración universal cargada.
🟢 OK Config cargada → DB Origen: C:\Proyectos\DataPulse\db\datapulse.sqlite
🟢 OK Tablas dinámicas detectadas:
   - facturas → excel_6_control_servicios
   - clientes → excel_1_clientes_proveedores
🔵 INFO Probando conexiones reales a bases de datos...
🔵 INFO BD Origen configurada: C:\Proyectos\DataPulse\db\datapulse.sqlite
🔵 INFO Conectando SQLite → C:\Proyectos\DataPulse\db\datapulse.sqlite
🟢 OK Conexión establecida.
🟢 OK BD ORIGEN → Conexión abierta.
🟢 OK BD ORIGEN → Tablas detectadas:
   - excel_1_ruben_bustamante
   - excel_1_walter_a
   - excel_1_miguel_solis
   - excel_1_anthony_huamani
   - excel_2_presupuestos
   - excel_2_clientes_proveedores
   - excel_2_bd_pagos
   - excel_1_clientes_proveedores
   - excel_1_b_bcp_soles
   - excel_1_b_bcp_dolares
   - excel_1_b_interbank_soles
   - excel_1_b_bbva_soles
   - excel_1_b_banco_nacion
   - excel_1_c_arequipa_soles
   - excel_1_c_finanzas_soles
   - excel_1_caja_operativa
   - excel_1_luis_rivera
   - excel_1_walter_asillo
   - excel_1_juber_monteza
   - excel_1_willian_leon
   - excel_1_juan_tarifa
   - excel_1_brayan_hanco
   - excel_1_jorge_queque
   - excel_1_nelson_quispe
   - excel_1_ronald_guizado
   - excel_2_bd_detracciones
   - excel_3_presupuestos
   - excel_4_compras
   - excel_4_ventas
   - excel_5_pe
   - excel_6_control_servicios
🟢 OK BD ORIGEN → Tabla 'excel_1_ruben_bustamante' tiene 405 filas.
🟢 OK Conexión cerrada.
🔵 INFO BD PulseForge configurada: C:\Proyectos\PulseForge\data\pulseforge.sqlite
🔵 INFO Conectando SQLite → C:\Proyectos\PulseForge\data\pulseforge.sqlite
🟢 OK Conexión establecida.
🟢 OK BD PULSEFORGE → Conexión abierta.
🟢 OK BD PULSEFORGE → Tablas detectadas:
🟡 WARN BD PULSEFORGE no tiene tablas.
🟢 OK Conexión cerrada.
🔵 INFO BD Nueva configurada: C:\Proyectos\PulseForge\data\pulseforge.sqlite
🔵 INFO Conectando SQLite → C:\Proyectos\PulseForge\data\pulseforge.sqlite
🟢 OK Conexión establecida.
🟢 OK BD NUEVA → Conexión abierta.
🟢 OK BD NUEVA → Tablas detectadas:
🟡 WARN BD NUEVA no tiene tablas.
🟢 OK Conexión cerrada.
🔵 INFO Probando lectura real de datos desde BD Origen...
🔵 INFO BD Origen configurada: C:\Proyectos\DataPulse\db\datapulse.sqlite
🔵 INFO Conectando SQLite → C:\Proyectos\DataPulse\db\datapulse.sqlite
🟢 OK Conexión establecida.
🟢 OK Tabla 'excel_6_control_servicios' → 5 filas mostradas (vista previa):
   - {'ruc': '20100192064', 'cliente_generador': 'Moly-Cop Adesur S.A.', 'planta_proyecto': 'La Joya', 'codigo': 'ST.07', 'servicio': 'Recolección y Transporte de Residuos No Peligrosos.', 'fecha_fs': '2025-01-02 00:00:00', 'sede_fs': 'AQP', 'serie_fs': '1', 'ficha_servicio_fs': '3916', 'estado_fs': 'Emitida', 'detalle_servicio_fs': 'No Aprovechables.', 'medida_fs': 'Kg.', 'cantidad_fs': '570', 'unidad_vehicular_fs': 'FOTON BKL-739 ', 'FECHA DE VAL.': '2025-01-27 00:00:00', 'VALORIZACIÓN ': '3244', 'MONEDA': 'USD', 'C. DF': '126.1638', 'C.OPE.': '137', 'SUB TOTAL': '263.1638', 'IGV': '47.36948399999999', 'TOTAL': '310.53', 'estado_v': 'EMITIDA', 'observaciones': None, 'estado_cont': 'ENVIADO', 'FECHA DE EMISION': '2025-01-30 00:00:00', 'SERIE2': 'FE01', 'NUMERO': '534', 'CONDICION DE PAGO': '30', 'VENCIMIENTO': '2025-03-01 00:00:00', 'T.C.': None, 'estado_fact': 'FACTURADO', 'F1': '00:00:00', 'IMPORTE 1': '0', 'F2': '00:00:00', 'IMPORTE 2': '0', 'F3': '00:00:00', 'IMPORTE 3': '0', 'F4': '00:00:00', 'IMPORTE 4': '0', 'F5': '00:00:00', 'IMPORTE 5': '0', 'saldo_ps': '575.6238461538462', 'estado_pago_ps': 'NO CORRESPONDE', 'PERIODO': 'Enero', 'COMBINADA': 'FE01-534', 'SOLES': None, 'fecha_valorizacion_ttf': '2025-01-27 00:00:00', 'fecha_factura_ttf': '2025-01-30 00:00:00', 'fecha_pago_ttf': '00:00:00', 'fs_val_ttf': '25', 'val_fact_ttf': '3', 'fact_pago_ttf': '0', 'tiempo_tf': '28', 'fecha_disposicion_ttd': '2025-02-06 00:00:00', 'fecha_recepcion_documento_ttd': '2025-02-12 00:00:00', 'fecha_envío_digital_documentos_ttd': '2025-02-19 00:00:00', 'fecha_envío_físico_documentos _ttd': '2025-04-11 00:00:00', 'fs_fd_ttd': '35', 'fd_frd_ttd': '6', 'frd_fend_ttd': '7', 'fend_fenf_ttd': '51', 'tiempo_td': '99', 'ejercicio_ttd': '2025', 'estado': 'True', 'dentro_plazo': '0', 'observaciones.1': None}
   - {'ruc': '20100211115', 'cliente_generador': 'Fab. De Chocolates La Iberica S.A.', 'planta_proyecto': 'Arequipa', 'codigo': 'ST.07', 'servicio': 'Recolección y Transporte de Residuos No Peligrosos.', 'fecha_fs': '2024-12-30 00:00:00', 'sede_fs': 'AQP', 'serie_fs': '1', 'ficha_servicio_fs': '3917', 'estado_fs': 'Emitida', 'detalle_servicio_fs': 'No Aprovechables.', 'medida_fs': 'Kg.', 'cantidad_fs': '90', 'unidad_vehicular_fs': 'FOTON VDJ-872', 'FECHA DE VAL.': '2024-12-31 00:00:00', 'VALORIZACIÓN ': '3215', 'MONEDA': 'SOLES', 'C. DF': '4.37', 'C.OPE.': '0', 'SUB TOTAL': '393.3', 'IGV': '70.794', 'TOTAL': '464.09', 'estado_v': 'EMITIDA', 'observaciones': None, 'estado_cont': 'ENVIADO', 'FECHA DE EMISION': '2025-01-30 00:00:00', 'SERIE2': 'FE01', 'NUMERO': '536', 'CONDICION DE PAGO': '30', 'VENCIMIENTO': '2025-03-01 00:00:00', 'T.C.': '1', 'estado_fact': 'FACTURADO', 'F1': '00:00:00', 'IMPORTE 1': '0', 'F2': '00:00:00', 'IMPORTE 2': '0', 'F3': '00:00:00', 'IMPORTE 3': '0', 'F4': '00:00:00', 'IMPORTE 4': '0', 'F5': '00:00:00', 'IMPORTE 5': '0', 'saldo_ps': '464.0900000000001', 'estado_pago_ps': 'NO CORRESPONDE', 'PERIODO': 'Diciembre', 'COMBINADA': 'FE01-536', 'SOLES': '464.09', 'fecha_valorizacion_ttf': '2024-12-31 00:00:00', 'fecha_factura_ttf': '2025-01-30 00:00:00', 'fecha_pago_ttf': '00:00:00', 'fs_val_ttf': '1', 'val_fact_ttf': '30', 'fact_pago_ttf': '0', 'tiempo_tf': '31', 'fecha_disposicion_ttd': '2025-02-10 00:00:00', 'fecha_recepcion_documento_ttd': '2025-02-19 00:00:00', 'fecha_envío_digital_documentos_ttd': '2025-02-24 00:00:00', 'fecha_envío_físico_documentos _ttd': 'Pendiente', 'fs_fd_ttd': '42', 'fd_frd_ttd': '9', 'frd_fend_ttd': '5', 'fend_fenf_ttd': '0', 'tiempo_td': '56', 'ejercicio_ttd': '2024', 'estado': 'False', 'dentro_plazo': '0', 'observaciones.1': None}
   - {'ruc': '20100172543', 'cliente_generador': 'MOVITECNICA S.A.', 'planta_proyecto': 'Arequipa', 'codigo': 'ST.06', 'servicio': 'Recolección y Transporte de Residuos Peligrosos.', 'fecha_fs': '2024-12-27 00:00:00', 'sede_fs': 'AQP', 'serie_fs': '1', 'ficha_servicio_fs': '3918', 'estado_fs': 'Emitida', 'detalle_servicio_fs': 'Peligrosos.', 'medida_fs': 'Kg.', 'cantidad_fs': '110', 'unidad_vehicular_fs': ' MAXUS VBW-889', 'FECHA DE VAL.': '2024-12-30 00:00:00', 'VALORIZACIÓN ': '3209', 'MONEDA': 'SOLES', 'C. DF': '46.2', 'C.OPE.': '912.42', 'SUB TOTAL': '958.62', 'IGV': '172.5516', 'TOTAL': '1131.17', 'estado_v': 'EMITIDA', 'observaciones': None, 'estado_cont': 'ENVIADO', 'FECHA DE EMISION': '2025-01-07 00:00:00', 'SERIE2': 'FE01', 'NUMERO': '502', 'CONDICION DE PAGO': '15', 'VENCIMIENTO': '2025-01-22 00:00:00', 'T.C.': '1', 'estado_fact': 'FACTURADO', 'F1': '00:00:00', 'IMPORTE 1': '0', 'F2': '00:00:00', 'IMPORTE 2': '0', 'F3': '00:00:00', 'IMPORTE 3': '0', 'F4': '00:00:00', 'IMPORTE 4': '0', 'F5': '00:00:00', 'IMPORTE 5': '0', 'saldo_ps': '1131.17', 'estado_pago_ps': 'NO CORRESPONDE', 'PERIODO': 'Diciembre', 'COMBINADA': 'FE01-502', 'SOLES': '1131.17', 'fecha_valorizacion_ttf': '2024-12-30 00:00:00', 'fecha_factura_ttf': '2025-01-07 00:00:00', 'fecha_pago_ttf': '00:00:00', 'fs_val_ttf': '3', 'val_fact_ttf': '8', 'fact_pago_ttf': '0', 'tiempo_tf': '11', 'fecha_disposicion_ttd': '2024-12-30 00:00:00', 'fecha_recepcion_documento_ttd': '2024-12-27 00:00:00', 'fecha_envío_digital_documentos_ttd': '2025-01-17 00:00:00', 'fecha_envío_físico_documentos _ttd': '2025-02-12 00:00:00', 'fs_fd_ttd': '3', 'fd_frd_ttd': '0', 'frd_fend_ttd': '21', 'fend_fenf_ttd': '26', 'tiempo_td': '50', 'ejercicio_ttd': '2024', 'estado': 'True', 'dentro_plazo': '0', 'observaciones.1': None}
   - {'ruc': '20100172543', 'cliente_generador': 'MOVITECNICA S.A.', 'planta_proyecto': 'Arequipa', 'codigo': 'ST.07', 'servicio': 'Recolección y Transporte de Residuos No Peligrosos.', 'fecha_fs': '2024-12-27 00:00:00', 'sede_fs': 'AQP', 'serie_fs': '1', 'ficha_servicio_fs': '3919', 'estado_fs': 'Emitida', 'detalle_servicio_fs': 'No Aprovechables.', 'medida_fs': 'Kg.', 'cantidad_fs': '90', 'unidad_vehicular_fs': ' MAXUS VBW-889', 'FECHA DE VAL.': '2024-12-30 00:00:00', 'VALORIZACIÓN ': '3210', 'MONEDA': 'SOLES', 'C. DF': None, 'C.OPE.': None, 'SUB TOTAL': '192.5', 'IGV': '34.65', 'TOTAL': '227.15', 'estado_v': 'EMITIDA', 'observaciones': None, 'estado_cont': 'ENVIADO', 'FECHA DE EMISION': '2025-01-20 00:00:00', 'SERIE2': 'FE01', 'NUMERO': '510', 'CONDICION DE PAGO': '30', 'VENCIMIENTO': '2025-02-19 00:00:00', 'T.C.': '1', 'estado_fact': 'FACTURADO', 'F1': '00:00:00', 'IMPORTE 1': '0', 'F2': '00:00:00', 'IMPORTE 2': '0', 'F3': '00:00:00', 'IMPORTE 3': '0', 'F4': '00:00:00', 'IMPORTE 4': '0', 'F5': '00:00:00', 'IMPORTE 5': '0', 'saldo_ps': '227.15', 'estado_pago_ps': 'NO CORRESPONDE', 'PERIODO': 'Diciembre', 'COMBINADA': 'FE01-510', 'SOLES': '227.15', 'fecha_valorizacion_ttf': '2024-12-30 00:00:00', 'fecha_factura_ttf': '2025-01-20 00:00:00', 'fecha_pago_ttf': '00:00:00', 'fs_val_ttf': '3', 'val_fact_ttf': '21', 'fact_pago_ttf': '0', 'tiempo_tf': '24', 'fecha_disposicion_ttd': '2025-01-14 00:00:00', 'fecha_recepcion_documento_ttd': '2024-12-27 00:00:00', 'fecha_envío_digital_documentos_ttd': '2025-02-05 00:00:00', 'fecha_envío_físico_documentos _ttd': 'Pendiente', 'fs_fd_ttd': '18', 'fd_frd_ttd': '0', 'frd_fend_ttd': '40', 'fend_fenf_ttd': '0', 'tiempo_td': '58', 'ejercicio_ttd': '2024', 'estado': 'False', 'dentro_plazo': '0', 'observaciones.1': None}
   - {'ruc': '20516903113', 'cliente_generador': 'Grupo de Gestión C S.A.', 'planta_proyecto': 'Arequipa', 'codigo': 'ST.06', 'servicio': 'Recolección y Transporte de Residuos Peligrosos.', 'fecha_fs': '2024-12-27 00:00:00', 'sede_fs': 'AQP', 'serie_fs': '1', 'ficha_servicio_fs': '3920', 'estado_fs': 'Emitida', 'detalle_servicio_fs': 'Peligrosos.', 'medida_fs': 'Kg.', 'cantidad_fs': '120', 'unidad_vehicular_fs': ' MAXUS VBW-889', 'FECHA DE VAL.': '2024-12-30 00:00:00', 'VALORIZACIÓN ': '3211', 'MONEDA': 'SOLES', 'C. DF': '606.61', 'C.OPE.': '0', 'SUB TOTAL': '606.61', 'IGV': '109.1898', 'TOTAL': '715.8', 'estado_v': 'EMITIDA', 'observaciones': None, 'estado_cont': 'ENVIADO', 'FECHA DE EMISION': '2025-01-20 00:00:00', 'SERIE2': 'FE01', 'NUMERO': '509', 'CONDICION DE PAGO': '15', 'VENCIMIENTO': '2025-02-04 00:00:00', 'T.C.': '1', 'estado_fact': 'FACTURADO', 'F1': '00:00:00', 'IMPORTE 1': '0', 'F2': '00:00:00', 'IMPORTE 2': '0', 'F3': '00:00:00', 'IMPORTE 3': '0', 'F4': '00:00:00', 'IMPORTE 4': '0', 'F5': '00:00:00', 'IMPORTE 5': '0', 'saldo_ps': '715.8', 'estado_pago_ps': 'NO CORRESPONDE', 'PERIODO': 'Diciembre', 'COMBINADA': 'FE01-509', 'SOLES': '715.8', 'fecha_valorizacion_ttf': '2024-12-30 00:00:00', 'fecha_factura_ttf': '2025-01-20 00:00:00', 'fecha_pago_ttf': '00:00:00', 'fs_val_ttf': '3', 'val_fact_ttf': '21', 'fact_pago_ttf': '0', 'tiempo_tf': '24', 'fecha_disposicion_ttd': '2024-12-30 00:00:00', 'fecha_recepcion_documento_ttd': '2024-12-27 00:00:00', 'fecha_envío_digital_documentos_ttd': '2025-01-17 00:00:00', 'fecha_envío_físico_documentos _ttd': 'Pendiente', 'fs_fd_ttd': '3', 'fd_frd_ttd': '0', 'frd_fend_ttd': '21', 'fend_fenf_ttd': '0', 'tiempo_td': '24', 'ejercicio_ttd': '2024', 'estado': 'True', 'dentro_plazo': '0', 'observaciones.1': None}
🟢 OK Conexión cerrada.
🔵 INFO Probando utils...
🟢 OK normalize_text → abc def & co.
🟢 OK clean_amount → 1234.56
🟢 OK parse_date → 2024-01-20
🟢 OK format_date_yyyymmdd → 20240120
🟢 OK date_diff_days → 9
🟢 OK clean_ruc → 2012345678
🔵 INFO Probando validaciones...
🟢 OK IGV → 0.18
🟢 OK Detracción → 0.12
🟢 OK TC → 3.8
🟢 OK validate_required → True
🟢 OK validate_positive → True
🟢 OK validate_date → True
🟢 OK validate_ruc → True
🟢 OK === TEST CORE COMPLETADO ===


(forge_env) PS C:\Proyectos\PulseForge> & C:/Proyectos/PulseForge/forge_env/Scripts/python.exe c:/Proyectos/PulseForge/src/extractors/test_extractors.py
🔵 INFO =============================================
🔵 INFO      INICIO TEST COMPLETO DE EXTRACTORS
🔵 INFO =============================================
🔵 INFO Cargando configuración universal...
🟢 OK .env cargado.
🟢 OK settings.json cargado.
🟢 OK constants.json cargado.
🟢 OK Configuración universal cargada.
🟢 OK BD origen configurada → C:\Proyectos\DataPulse\db\datapulse.sqlite
🔵 INFO === TEST → ClientsExtractor ===
🔵 INFO Inicializando ClientsExtractor...
🟢 OK Tabla de clientes configurada → excel_1_clientes_proveedores
🔵 INFO BD Origen configurada: C:\Proyectos\DataPulse\db\datapulse.sqlite
🔵 INFO Conectando SQLite → C:\Proyectos\DataPulse\db\datapulse.sqlite
🟢 OK Conexión establecida.
🟢 OK Clientes crudos cargados desde 'excel_1_clientes_proveedores': 1003 filas.
🟢 OK Conexión cerrada.
🟢 OK Columna RUC detectada → Ruc / Dni
🟢 OK Columna nombre detectada → Razon Social
🟢 OK Clientes normalizados: 975 registros (filtrados 28).
🟢 OK Clientes extraídos: 975
           ruc                   razon_social
0  10005208748  condemayta larico jhony sixto
1  10104340984     garcia rojas pablo orlando
2  10211014194   caparachin baldeon teodomiro
3  10238587935           quispe quispe aquino
4  10239605783              mamani alata raul
🔵 INFO === TEST → InvoicesExtractor ===
🔵 INFO Inicializando InvoicesExtractor…
🔵 INFO Inicializando DataMapper PulseForge…
🟢 OK DataMapper cargado correctamente.
🟢 OK Tabla de facturas configurada → excel_6_control_servicios
🔵 INFO Mapeando facturas…
🟢 OK Facturas mapeadas: 1479
🟢 OK Facturas extraídas y mapeadas → 1479 registros.
🟢 OK Facturas extraídas: 1479
[{'subtotal': 0.0, 'igv': 0.0, 'total_con_igv': 0.0, 'detraccion_monto': 0.0, 'neto_recibido': 0.0, 'ruc': '', 'cliente_generador': '', 'serie': '', 'numero': '', 'combinada': '', 'fecha_emision': '', 'vencimiento': '', 'fue_cobrado': 0, 'match_id': None, 'source_hash': '3c7ba1921efc0b9d102b7f0d563d441b4788c1fed7c355afaf73ca3789386a94'}, {'subtotal': 0.0, 'igv': 0.0, 'total_con_igv': 0.0, 'detraccion_monto': 0.0, 'neto_recibido': 0.0, 'ruc': '', 'cliente_generador': '', 'serie': '', 'numero': '', 'combinada': '', 'fecha_emision': '', 'vencimiento': '', 'fue_cobrado': 0, 'match_id': None, 'source_hash': '3c7ba1921efc0b9d102b7f0d563d441b4788c1fed7c355afaf73ca3789386a94'}, {'subtotal': 0.0, 'igv': 0.0, 'total_con_igv': 0.0, 'detraccion_monto': 0.0, 'neto_recibido': 0.0, 'ruc': '', 'cliente_generador': '', 'serie': '', 'numero': '', 'combinada': '', 'fecha_emision': '', 'vencimiento': '', 'fue_cobrado': 0, 'match_id': None, 'source_hash': '3c7ba1921efc0b9d102b7f0d563d441b4788c1fed7c355afaf73ca3789386a94'}, {'subtotal': 0.0, 'igv': 0.0, 'total_con_igv': 0.0, 'detraccion_monto': 0.0, 'neto_recibido': 0.0, 'ruc': '', 'cliente_generador': '', 'serie': '', 'numero': '', 'combinada': '', 'fecha_emision': '', 'vencimiento': '', 'fue_cobrado': 0, 'match_id': None, 'source_hash': '3c7ba1921efc0b9d102b7f0d563d441b4788c1fed7c355afaf73ca3789386a94'}, {'subtotal': 0.0, 'igv': 0.0, 'total_con_igv': 0.0, 'detraccion_monto': 0.0, 'neto_recibido': 0.0, 'ruc': '', 'cliente_generador': '', 'serie': '', 'numero': '', 'combinada': '', 'fecha_emision': '', 'vencimiento': '', 'fue_cobrado': 0, 'match_id': None, 'source_hash': '3c7ba1921efc0b9d102b7f0d563d441b4788c1fed7c355afaf73ca3789386a94'}]
🔵 INFO === TEST → BankExtractor ===
🔵 INFO Inicializando BankExtractor…
🔵 INFO BD Origen configurada: C:\Proyectos\DataPulse\db\datapulse.sqlite
🟢 OK Config bancos cargada → única=, múltiples={'BN': 'excel_1_b_banco_nacion', 'BBVA': 'excel_1_b_bbva_soles', 'BCP_USD': 'excel_1_b_bcp_dolares', 'BCP': 'excel_1_b_bcp_soles', 'IBK': 'excel_1_b_interbank_soles', 'AREQUIPA': 'excel_1_c_arequipa_soles', 'FINANZAS': 'excel_1_c_finanzas_soles'}
🔵 INFO Conectando SQLite → C:\Proyectos\DataPulse\db\datapulse.sqlite
🟢 OK Conexión establecida.
🟢 OK Conexión cerrada.
🟢 OK [BN] Movimientos normalizados: 829
🔵 INFO Conectando SQLite → C:\Proyectos\DataPulse\db\datapulse.sqlite
🟢 OK Conexión establecida.
🟢 OK Conexión cerrada.
🟢 OK [BBVA] Movimientos normalizados: 50
🔵 INFO Conectando SQLite → C:\Proyectos\DataPulse\db\datapulse.sqlite
🟢 OK Conexión establecida.
🟢 OK Conexión cerrada.
🟢 OK [BCP_USD] Movimientos normalizados: 434
🔵 INFO Conectando SQLite → C:\Proyectos\DataPulse\db\datapulse.sqlite
🟢 OK Conexión establecida.
🟢 OK Conexión cerrada.
🟢 OK [BCP] Movimientos normalizados: 4646
🔵 INFO Conectando SQLite → C:\Proyectos\DataPulse\db\datapulse.sqlite
🟢 OK Conexión establecida.
🟢 OK Conexión cerrada.
🟢 OK [IBK] Movimientos normalizados: 189
🔵 INFO Conectando SQLite → C:\Proyectos\DataPulse\db\datapulse.sqlite
🟢 OK Conexión establecida.
🟢 OK Conexión cerrada.
🟢 OK [AREQUIPA] Movimientos normalizados: 1110
🔵 INFO Conectando SQLite → C:\Proyectos\DataPulse\db\datapulse.sqlite
🟢 OK Conexión establecida.
🟢 OK Conexión cerrada.
🟢 OK [FINANZAS] Movimientos normalizados: 985
🟢 OK TOTAL movimientos extraídos: 8243
🟢 OK Movimientos bancarios extraídos: 8243
                 fecha tipo_mov                     descripcion serie numero     monto moneda operacion   destinatario tipo_documento banco_codigo
0  2025-01-01 00:00:00        r  saldo inicial - ejercicio 2024  None   None  45894.01    PEN      None           none           none           BN
1  2025-01-02 00:00:00        r                        not.abon  FE02    165   3866.00    PEN  63467627  gytres s.a.c.        factura           BN
2  2025-01-02 00:00:00        r                        not.abon  FE01    462    405.00    PEN  63414889  gytres s.a.c.           none           BN
3  2025-01-02 00:00:00        r                        not.abon  FE01    415    315.00    PEN  63414837  gytres s.a.c.           none           BN
4  2025-01-02 00:00:00        r                        not.abon  FE02    167     78.00    PEN  63379785  gytres s.a.c.           none           BN
🔵 INFO ---------------------------------------------
🟢 OK TEST MÓDULO EXTRACTORS FINALIZADO CON ÉXITO
🔵 INFO ---------------------------------------------

Resumen final:
 - Clientes  → 975 registros
 - Facturas  → 1479 registros
 - Bancos    → 8243 registros