import json
from pyrfc import Connection
import pandas as pd
from db_operations import save_to_database

def fetch_MAKT_data():
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

    # Leer los campos para la tabla MAKT
    fields = config['fields']['MAKT']

    # Llamada a la función RFC para la tabla MAKT
    result = sap_conn.call('RFC_READ_TABLE',
                            QUERY_TABLE='MAKT',
                            DELIMITER='|',
                            FIELDS=[{'FIELDNAME': field} for field in fields])

    # Procesar los resultados
    data = result['DATA']
    rows = [row['WA'].split('|') for row in data]  # Dividir cada fila por el delimitador

    # Asegurarse de que cada fila tenga el número correcto de columnas
    rows = [row for row in rows if len(row) == len(fields)]

    # Crear un DataFrame con los resultados
    df = pd.DataFrame(rows, columns=fields)

    # Cerrar la conexión de SAP
    sap_conn.close()

    # Guardar el DataFrame en la base de datos
    save_to_database(df, 'MAKT_Data')

    return df
