from pyrfc import Connection

# Conexión a SAP
sap_conn_params = {
    'ashost': '192.168.0.6',
    'sysnr': '00',
    'client': '300',
    'user': 'ETL_SAP',
    'passwd': 'Etl_2020'
}

try:
    conn = Connection(**sap_conn_params)
    print('Conexión exitosa')

    # Obtener la información de los campos de la tabla RSEG
    result = conn.call('DDIF_FIELDINFO_GET', TABNAME='EKPO')

    # Imprimir la lista de campos válidos
    fields = [field['FIELDNAME'] for field in result['DFIES_TAB']]
    print("Campos válidos en EKPO:")
    print(fields)

except Exception as e:
    print('Error de conexión', e)

finally:
    conn.close()
