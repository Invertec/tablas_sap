from pyrfc import Connection
from sqlalchemy import create_engine
import pandas as pd

def extract_data_from_sap(query_table, fields, db_table_name):
    # Configuración de la conexión a SAP
    sap_conn = Connection(user='testti', passwd='Invertec.2024', ashost='192.168.0.6', sysnr='00', client='300')

    # Llamada a la función RFC
    result = sap_conn.call('RFC_READ_TABLE',
                            QUERY_TABLE=query_table,
                            DELIMITER='|',
                            FIELDS=[{'FIELDNAME': field} for field in fields])

    # Procesar los resultados
    data = result['DATA']
    rows = [row['WA'].split('|') for row in data]  # Dividir cada fila por el delimitador

    # Crear un DataFrame con los resultados
    df = pd.DataFrame(rows, columns=fields)

    # Configuración de la conexión a SQL Server
    database_config = {
        'user': 'sa',
        'password': 'Invertek23',
        'server': '192.168.1.12',
        'database': 'tablas_sap'
    }

    conn_str = (f"mssql+pyodbc://{database_config['user']}:{database_config['password']}@"
                f"{database_config['server']}/{database_config['database']}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")

    # Crear la conexión a la base de datos
    engine = create_engine(conn_str)

    # Guardar el DataFrame en la base de datos
    df.to_sql(db_table_name, con=engine, if_exists='replace', index=False)

    # Cerrar la conexión de SAP
    sap_conn.close()

if __name__ == '__main__':
    # Campos que deseas leer
    fields = ['EBELN', 'BUKRS', 'BSTYP', 'BSART', 'STATU', 'AEDAT', 'ERNAM', 
              'LASTCHANGEDATETIME', 'PINCR', 'LPONR', 'LIFNR', 'SPRAS', 'ZTERM', 
              'ZBD1T', 'ZBD2T', 'ZBD3T', 'ZBD1P', 'ZBD2P', 'EKORG', 'EKGRP', 
              'WAERS', 'WKURS', 'BEDAT', 'RLWRT']
    
    extract_data_from_sap('EKKO', fields, 'EKKO_Data')
