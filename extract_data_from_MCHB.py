import json
from pyrfc import Connection
import pandas as pd
from db_operations import save_to_database

def fetch_MCHB_data():
    try:
        print("Cargando configuración...")
        # Cargar configuración
        with open('config.json') as config_file:
            config = json.load(config_file)

        print("Conectando a SAP...")
        sap_conn = Connection(
            user=config['sap']['user'],
            passwd=config['sap']['password'],
            ashost=config['sap']['ashost'],
            sysnr=config['sap']['sysnr'],
            client=config['sap']['client']
        )
        print("Conexión a SAP exitosa.")

        # Campos y rangos
        fields = ["MANDT", "MATNR", "WERKS", "LGORT", "CHARG", "ERSDA"]
        date_ranges = [
            ("20240101", "20240115"),
            ("20240116", "20240131")
        ]
        rows_per_request = 1000
        all_data = []

        # Consulta por rangos de fechas
        for start_date, end_date in date_ranges:
            offset = 0
            while True:
                result = sap_conn.call(
                    'RFC_READ_TABLE',
                    QUERY_TABLE='MCHB',
                    DELIMITER='|',
                    FIELDS=[{'FIELDNAME': field} for field in fields],
                    OPTIONS=[{'TEXT': f"ERSDA >= '{start_date}' AND ERSDA <= '{end_date}'"}],
                    ROWSKIPS=offset,
                    ROWCOUNT=rows_per_request
                )

                data = result['DATA']
                rows = [row['WA'].split('|') for row in data]

                if not rows:
                    print(f"Finalizado rango {start_date} - {end_date}.")
                    break

                all_data.extend(rows)
                print(f"Se procesaron {len(rows)} filas para rango {start_date} - {end_date}, offset {offset}.")
                offset += rows_per_request

        # Crear DataFrame y guardar en la base de datos
        df = pd.DataFrame(all_data, columns=fields)
        if not df.empty:
            save_to_database(df, 'MCHB_Data')
            print("Datos guardados en la base de datos.")
        else:
            print("No se encontraron datos para guardar.")

        sap_conn.close()
        return df

    except Exception as e:
        print(f"Error en fetch_MCHB_data: {e}")
