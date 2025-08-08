from pyrfc import Connection

# Parámetros de conexión a SAP
sap_conn_params = {
    'ashost': '192.168.0.9',
    'sysnr': '00',
    'client': '300',
    'user': 'ETL_INVERTEC',
    'passwd': 'Invertec.24$'
}

try:
    # Conexión
    conn = Connection(**sap_conn_params)
    print('✅ Conexión exitosa')

    # Nombres de campos (cabecera de columnas)
    metadata = conn.call('DDIF_FIELDINFO_GET', TABNAME='AUFK')
    fields = [field['FIELDNAME'] for field in metadata['DFIES_TAB']]
    print(f"📋 Campos en AUFK ({len(fields)}):\n", fields)

    # Leer datos con RFC_READ_TABLE
    result = conn.call('RFC_READ_TABLE',
                       QUERY_TABLE='AUFK',
                       DELIMITER='|',
                       ROWCOUNT=10)  # puedes cambiar ROWCOUNT por más registros

    # Procesar resultados
    rows = result['DATA']
    print("\n📦 Datos obtenidos:")
    for row in rows:
        print(row['WA'])

except Exception as e:
    print('❌ Error:', e)

finally:
    conn.close()
