import json
from pyrfc import Connection
import pandas as pd
from db_operations import save_to_database

def fetch_MSEG_data():
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

    # Leer los campos para la tabla MSEG
    fields = config['fields']['MSEG']

    # Llamada a la función RFC para la tabla MSEG
    result = sap_conn.call('RFC_READ_TABLE',
                            QUERY_TABLE='MSEG',
                            DELIMITER='|',
                            FIELDS=[{'FIELDNAME': field} for field in fields])

    # Procesar los resultados
    data = result['DATA']

    rows = []
    for row in data:
        wa = row['WA']

        # Sustituir el valor conflictivo exacto por 0 (solo si está dentro de campos)
        wa_clean = wa.replace('|000004677', '|0')

        # Separar en columnas
        values = wa_clean.split('|')

        # Verificar que coincida con el número esperado de columnas
        if len(values) == len(fields):
            rows.append(values)
        else:
            print(f"[WARN] Fila ignorada por columna desalineada: {wa}")
            continue

    # Crear un DataFrame con los resultados
    df = pd.DataFrame(rows, columns=fields)

    # Conversiones de tipo
    numeric_fields = ['MENGE']
    int_fields = ['MJAHR', 'ZEILE']
    date_fields = ['BUDAT_MKPF']

    for field in numeric_fields:
        if field in df.columns:
            df[field] = df[field].str.replace(',', '.').astype(float)

    for field in int_fields:
        if field in df.columns:
            df[field] = pd.to_numeric(df[field], errors='coerce').fillna(0).astype(int)

    for field in date_fields:
        if field in df.columns:
            df[field] = pd.to_datetime(df[field], format='%Y%m%d', errors='coerce')

    # Cerrar la conexión de SAP
    sap_conn.close()

    # Guardar el DataFrame en la base de datos
    save_to_database(df, 'MSEG_Data')

    return df
