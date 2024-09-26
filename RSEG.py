from pyrfc import Connection
from sqlalchemy import create_engine
import pandas as pd

# Configuración de la conexión a SAP
sap_conn = Connection(user='wcorrea', passwd='Invertec.22$', ashost='192.168.0.6', sysnr='00', client='300')

# Campos que deseas leer de la tabla RSEG
fields = ['BELNR', 'GJAHR', 'BUZEI', 'EBELN', 'EBELP', 'MATNR', 'BWKEY', 'BUKRS', 
          'WERKS', 'WRBTR', 'SHKZG', 'MWSKZ', 'MENGE', 'BSTME', 'BPMNG', 'BPRME', 
          'LBKUM', 'MEINS', 'VRKUM', 'PSTYP', 'KNTTP', 'BKLAS', 'EXKBE', 'BUSTW', 
          'XBLNR', 'SALK3', 'LFBNR', 'LFPOS', 'MATBF', 'SGTXT']

# Llamada a la función RFC para la tabla RSEG
result = sap_conn.call('RFC_READ_TABLE',
                        QUERY_TABLE='RSEG',
                        DELIMITER='|',
                        FIELDS=[{'FIELDNAME': field} for field in fields])

# Procesar los resultados
data = result['DATA']

# Dividir cada fila por el delimitador
rows = [row['WA'].split('|') for row in data]

# Asegurarse de que cada fila tenga el número correcto de columnas
rows = [row for row in rows if len(row) == len(fields)]

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
df.to_sql('RSEG_Data', con=engine, if_exists='replace', index=False)

# Cerrar la conexión de SAP
sap_conn.close()
