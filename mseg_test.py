import json
from pyrfc import Connection, ABAPApplicationError, ABAPRuntimeError, LogonError, CommunicationError
import pandas as pd
import uuid  # Para generar identificadores únicos
from sqlalchemy import create_engine

def save_to_database(df, table_name):
    # Conectar con la base de datos (ajustar los parámetros a tu configuración)
    engine = create_engine('mssql+pyodbc://usuario:contraseña@servidor/nombre_bd?driver=ODBC+Driver+17+for+SQL+Server')

    # Eliminar columnas duplicadas
    df = df.loc[:, ~df.columns.duplicated()]

    # Verificar si la tabla ya existe
    if not engine.dialect.has_table(engine, table_name):
        print(f"Creando la tabla '{table_name}'...")
        df.to_sql(table_name, engine, if_exists='fail', index=False)
    else:
        print(f"Insertando datos en '{table_name}'...")
        df.to_sql(table_name, engine, if_exists='append', index=False)

def fetch_MSEG_data():
    try:
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

        # Agregar campos únicos para identificar cada registro
        unique_fields = ['MBLNR', 'MJAHR', 'ZEILE']
        for field in unique_fields:
            if field not in fields:
                fields.append(field)

        # Paginación
        batch_size = 10000
        rows_skipped = 0
        all_data = []

        while True:
            # Llamada a la función RFC para la tabla MSEG con paginación
            result = sap_conn.call('RFC_READ_TABLE',
                                   QUERY_TABLE='MSEG',
                                   DELIMITER='|',
                                   FIELDS=[{'FIELDNAME': field} for field in fields],
                                   ROWSKIPS=rows_skipped,
                                   ROWCOUNT=batch_size)

            # Procesar los resultados
            data = result['DATA']
            if not data:
                break

            # Dividir cada fila por el delimitador
            rows = [row['WA'].split('|') for row in data]

            # Validar que cada fila tenga el número correcto de columnas
            valid_rows = [row for row in rows if len(row) == len(fields)]

            # Agregar un identificador único a cada fila
            for row in valid_rows:
                row.insert(0, str(uuid.uuid4()))

            all_data.extend(valid_rows)
            rows_skipped += batch_size

            print(f"Lote procesado: {len(valid_rows)} registros. Total acumulado: {len(all_data)} registros.")

        # Crear un DataFrame con los datos
        df = pd.DataFrame(all_data, columns=['ID'] + fields)

        # Cerrar la conexión de SAP
        sap_conn.close()

        # Guardar en la base de datos
        save_to_database(df, 'MSEG_Data')

        print("Proceso finalizado con éxito.")

    except (ABAPApplicationError, ABAPRuntimeError, LogonError, CommunicationError) as e:
        print(f"Error de SAP: {e}")
    except Exception as e:
        print(f"Error inesperado: {e}")
    finally:
        if 'sap_conn' in locals() and sap_conn.alive:
            sap_conn.close()

# Ejecutar la función
fetch_MSEG_data()
