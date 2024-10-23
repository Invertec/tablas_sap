import os
from pyrfc import Connection
from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv

# Cargar variables de entorno desde un archivo .env
load_dotenv()

# Función para conectar a SAP
def sap_connect():
    try:
        conn = Connection(
            user=os.getenv('SAP_USER'),
            passwd=os.getenv('SAP_PASSWORD'),
            ashost=os.getenv('SAP_HOST'),
            sysnr=os.getenv('SAP_SYSNR'),
            client=os.getenv('SAP_CLIENT')
        )
        return conn
    except Exception as e:
        print(f"Error conectando a SAP: {e}")
        return None

# Función para leer datos de SAP
def read_sap_table(sap_conn, table_name, fields, delimiter='|'):
    try:
        result = sap_conn.call('RFC_READ_TABLE',
                               QUERY_TABLE=table_name,
                               DELIMITER=delimiter,
                               FIELDS=[{'FIELDNAME': field} for field in fields])
        data = result['DATA']
        rows = [row['WA'].split(delimiter) for row in data]
        rows = [row for row in rows if len(row) == len(fields)]
        return pd.DataFrame(rows, columns=fields)
    except Exception as e:
        print(f"Error leyendo la tabla {table_name} en SAP: {e}")
        return None

# Función para conectar a SQL Server
def sql_server_connect():
    try:
        conn_str = (f"mssql+pyodbc://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@"
                    f"{os.getenv('DB_SERVER')}/{os.getenv('DB_NAME')}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")
        engine = create_engine(conn_str)
        return engine
    except Exception as e:
        print(f"Error conectando a SQL Server: {e}")
        return None

# Función para guardar el DataFrame en SQL Server
def save_to_sql(df, table_name, engine):
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print(f"Datos guardados exitosamente en la tabla {table_name}")
    except Exception as e:
        print(f"Error guardando datos en la tabla {table_name}: {e}")

def main():
    # Conectar a SAP
    sap_conn = sap_connect()
    if not sap_conn:
        return

    # Campos a leer de la tabla T003T
    fields = ['BLART', 'LTEXT']

    # Leer datos de la tabla SAP T003T
    df = read_sap_table(sap_conn, 'T003T', fields)
    if df is None:
        return

    # Conectar a SQL Server
    engine = sql_server_connect()
    if not engine:
        return

    # Guardar los datos en SQL Server
    save_to_sql(df, 'T003T_Data', engine)

    # Cerrar la conexión a SAP
    sap_conn.close()

if __name__ == '__main__':
    main()
