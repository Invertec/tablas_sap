from pyrfc import Connection
from sqlalchemy import create_engine
import pandas as pd

# Configuración de la conexión a SAP
sap_conn = Connection(user='wcorrea', passwd='Invertec.22$', ashost='192.168.0.6', sysnr='00', client='300')

# Campos que deseas leer de la tabla MARA y MAKT
fields_mara = ['MATNR', 'ERSDA', 'ERNAM', 'LAEDA', 'AENAM', 'VPSTA', 'PSTAT', 'MTART', 'MBRSH', 'MATKL', 'BISMT', 'MEINS', 'BRGEW', 'NTGEW', 'GEWEI']
fields_makt = ['MATNR', 'MAKTX']

# Llamada a la función RFC para la tabla MARA
result_mara = sap_conn.call('RFC_READ_TABLE',
                            QUERY_TABLE='MARA',
                            DELIMITER='|',
                            FIELDS=[{'FIELDNAME': field} for field in fields_mara])

# Procesar los resultados de MARA
data_mara = result_mara['DATA']
rows_mara = [row['WA'].split('|') for row in data_mara]
rows_mara = [row for row in rows_mara if len(row) == len(fields_mara)]

# Crear un DataFrame con los resultados de MARA
df_mara = pd.DataFrame(rows_mara, columns=fields_mara)

# Llamada a la función RFC para la tabla MAKT (texto asociado al material)
result_makt = sap_conn.call('RFC_READ_TABLE',
                            QUERY_TABLE='MAKT',
                            DELIMITER='|',
                            FIELDS=[{'FIELDNAME': field} for field in fields_makt])

# Procesar los resultados de MAKT
data_makt = result_makt['DATA']
rows_makt = [row['WA'].split('|') for row in data_makt]
rows_makt = [row for row in rows_makt if len(row) == len(fields_makt)]

# Crear un DataFrame con los resultados de MAKT
df_makt = pd.DataFrame(rows_makt, columns=fields_makt)

# Realizar la unión entre los DataFrames de MARA y MAKT usando el campo MATNR
df_final = pd.merge(df_mara, df_makt, on='MATNR', how='left')

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
df_final.to_sql('MARA_Data', con=engine, if_exists='replace', index=False)

# Cerrar la conexión de SAP
sap_conn.close()
