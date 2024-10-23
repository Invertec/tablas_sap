import json
from sqlalchemy import create_engine

def save_to_database(df, table_name):
    # Cargar la configuración desde el archivo JSON
    with open('config.json') as config_file:
        config = json.load(config_file)

    # Configuración de la conexión a SQL Server
    database_config = config['database']
    conn_str = (f"mssql+pyodbc://{database_config['user']}:{database_config['password']}@"
                f"{database_config['server']}/{database_config['database']}?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes")

    # Crear la conexión a la base de datos
    engine = create_engine(conn_str)

    # Guardar el DataFrame en la base de datos
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

    # Cerrar la conexión (si es necesario, puedes manejar la conexión como un contexto)
    engine.dispose()
