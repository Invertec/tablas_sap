import os
from pyrfc import Connection
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

def create_sap_connection():
    """Crea una conexión a SAP utilizando credenciales desde variables de entorno."""
    try:
        return Connection(
            user=os.getenv('SAP_USER'),
            passwd=os.getenv('SAP_PASS'),
            ashost=os.getenv('SAP_HOST'),
            sysnr=os.getenv('SAP_SYSNR'),
            client=os.getenv('SAP_CLIENT')
        )
    except Exception as e:
        print(f"Error al conectar a SAP: {e}")
        raise

def create_sql_connection():
    """Crea una conexión a SQL Server utilizando credenciales desde variables de entorno."""
    try:
        database_config = {
            'user': os.getenv('SQL_USER'),
            'password': os.getenv('SQL_PASS'),
            'server': os.getenv('SQL_SERVER'),
            'database': os.getenv('SQL_DATABASE')
        }
        conn_str = (f"mssql+pyodbc://{database_config['user']}:{database_config['password']}@"
                    f"{database_config['server']}/{database_config['database']}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")
        return create_engine(conn_str)
    except Exception as e:
        print(f"Error al conectar a SQL Server: {e}")
        raise

def extract_data_from_sap(query_table, fields, db_table_name):
    """Extrae datos de SAP y los guarda en SQL Server."""
    sap_conn = create_sap_connection()
    
    # Llamada a la función RFC
    result = sap_conn.call('RFC_READ_TABLE',
                            QUERY_TABLE=query_table,
                            DELIMITER='|',
                            FIELDS=[{'FIELDNAME': field} for field in fields])

    # Procesar los resultados
    data = result['DATA']
    rows = [row['WA'].split('|') for row in data]  # Dividir cada fila por el delimitador

    # Asegurarse de que cada fila tenga el número correcto de columnas
    rows = [row for row in rows if len(row) == len(fields)]

    # Crear un DataFrame con los resultados
    df = pd.DataFrame(rows, columns=fields)

    # Guardar el DataFrame en la base de datos
    engine = create_sql_connection()
    df.to_sql(db_table_name, con=engine, if_exists='replace', index=False)

    # Cerrar la conexión de SAP
    sap_conn.close()

if __name__ == '__main__':
    # Campos que deseas leer de la tabla LFA1
    fields = ['LIFNR', 'LAND1', 'NAME1', 'NAME2', 'NAME3', 'NAME4', 'ORT01', 
              'ORT02', 'PFACH', 'PSTLZ', 'REGIO', 'STCD1', 'STRAS', 'ADRNR', 
              'MCOD1', 'MCOD2', 'MCOD3', 'ANRED', 'ERDAT', 'ERNAM', 'SORTL']
    
    extract_data_from_sap('LFA1', fields, 'LFA1_Data')
