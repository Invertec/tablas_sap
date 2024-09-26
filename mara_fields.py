from pyrfc import Connection

# Conexión a SAP
sap_conn_params = {
    'ashost': '192.168.0.9',
    'sysnr': '00',
    'client': '300',
    'user': 'ETL_SAP',
    'passwd': 'Etl_2020'
}

try:
    conn = Connection(**sap_conn_params)
    print('Conexión exitosa')

    # Lista de campos que vamos a probar uno por uno o en grupos pequeños
    fields_to_test = [
        ['MATNR'], 
        ['ERSDA'], 
        ['ERNAM'], 
        ['LAEDA'], 
        ['AENAM'], 
        ['VPSTA'], 
        ['PSTAT'], 
        ['MTART'], 
        ['MBRSH'], 
        ['MATKL'], 
        ['BISMT'], 
        ['MEINS'], 
        ['BRGEW'], 
        ['NTGEW'], 
        ['GEWEI'],
        ['MAKTX']
    ]

    for field_group in fields_to_test:
        try:
            # Probar extracción con un solo campo o un grupo pequeño
            fields = [{'FIELDNAME': field} for field in field_group]
            result = conn.call('RFC_READ_TABLE', QUERY_TABLE='MARA', DELIMITER='|', ROWCOUNT=1, FIELDS=fields)
            print(f"Campos válidos: {field_group}")
        except Exception as e:
            print(f"Error con el campo o grupo de campos: {field_group}, Error: {e}")

except Exception as e:
    print('Error de conexión', e)
finally:
    conn.close()
