import json
from pyrfc import Connection
import pandas as pd
from db_operations import save_to_database

def fetch_RSEG_data():
    """Extrae datos de SAP de la tabla RSEG y los guarda en la base de datos."""
    # Cargar la configuración desde el archivo JSON
    with open('config.json') as config_file:
        config = json.load(config_file)

    # Configuración de la conexión a SAP
    sap_conn = Connection(
        user=config['sap']['user'],
        passwd=config['sap']['password'],
        ashost=config['sap']['ashost'],
        sysnr=config['sap']['sysnr'],
        client=config['sap']['client']
    )

    # Leer los campos para la tabla RSEG
    fields = config['fields']['RSEG']

    # Asegurarse de pasar los campos como diccionarios con 'FIELDNAME'
    field_dicts = [{'FIELDNAME': field} for field in fields]

    # Llamada a la función RFC para la tabla RSEG
    result = sap_conn.call('RFC_READ_TABLE',
                           QUERY_TABLE='RSEG',
                           DELIMITER='|',
                           FIELDS=field_dicts)

    # Procesar los resultados
    data = result['DATA']
    rows = [row['WA'].split('|') for row in data]  # Dividir cada fila por el delimitador

    # Filtrar filas que tengan exactamente el mismo número de columnas que en 'fields'
    rows = [row for row in rows if len(row) == len(fields)]

    # Debug: imprimir si hay diferencias de columnas
    for row in rows:
        if len(row) != len(fields):
            print(f"Fila con longitud incorrecta: {row} (esperado: {len(fields)} columnas)")

    # Crear un DataFrame con las filas válidas
    df = pd.DataFrame(rows, columns=fields)

    # Cerrar la conexión de SAP
    sap_conn.close()

    # Guardar el DataFrame en la base de datos
    save_to_database(df, 'RSEG_Data')

    return df
