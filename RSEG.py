import os
from pyrfc import Connection
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

# Cargar las variables de entorno desde un archivo .env
load_dotenv()

# Función para conectar a SAP
def sap_connect():
    return Connection(
        user=os.getenv('SAP_USER'),
        passwd=os.getenv('SAP_PASSWD'),
        ashost=os.getenv('SAP_ASHOST'),
        sysnr=os.getenv('SAP_SYSNR'),
        client=os.getenv('SAP_CLIENT')
    )

# Función para leer datos de una tabla SAP
def read_sap_table(sap_conn, table_name, fields, delimiter='|'):
    result = sap_conn.call('RFC_READ_TABLE',
                           QUERY_TABLE=table_name,
                           DELIMITER=delimiter,
                           FIELDS=[{'FIELDNAME': field} for field in fields])
    data = result['DATA']
    
    # Procesar los resultados
    rows = [row['WA'].split(delimiter) for row in data]
    
    # Asegurarse de que cada fila tenga el número correcto de columnas
    rows = [row for row in rows if len(row) == len(fields)]
    
    # Crear un DataFrame con los resultados
    return pd.DataFrame(rows, columns=fields)

# Función para conectar a SQL Server
def sql_server_connect():
    return create_engine(
        f"mssql+pyodbc://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
        f"{os.getenv('DB_SERVER')}/{os.getenv('DB_NAME')}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    )

# Función para guardar un DataFrame en la base de datos SQL Server
def save_to_sql(df, table_name, engine):
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

def main():
    # Conectar a SAP
    sap_conn = sap_connect()

    # Campos a leer de la tabla RSEG
    fields = ['BELNR', 'GJAHR', 'BUZEI', 'EBELN', 'EBELP', 'MATNR', 'BWKEY', 'BUKRS', 
              'WERKS', 'WRBTR', 'SHKZG', 'MWSKZ', 'MENGE', 'BSTME', 'BPMNG', 'BPRME', 
              'LBKUM', 'MEINS', 'VRKUM', 'PSTYP', 'KNTTP', 'BKLAS', 'EXKBE', 'BUSTW', 
              'XBLNR', 'SALK3', 'LFBNR', 'LFPOS', 'MATBF', 'SGTXT']

    # Leer datos de la tabla SAP RSEG
    df = read_sap_table(sap_conn, 'RSEG', fields)

    # Conectar a SQL Server
    engine = sql_server_connect()

    # Guardar los datos en SQL Server
    save_to_sql(df, 'RSEG_Data', engine)

    # Cerrar la conexión de SAP
    sap_conn.close()

if __name__ == '__main__':
    main()
