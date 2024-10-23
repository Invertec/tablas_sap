from extract_data_from_EKKO  import fetch_EKKO_data
from extract_data_from_EKPO  import fetch_EKPO_data
from extract_data_from_LFA1  import fetch_LFA1_data
from extract_data_from_MAKT  import fetch_MAKT_data
from extract_data_from_RBKP  import fetch_RBKP_data
from extract_data_from_RSEG  import fetch_RSEG_data
from extract_data_from_T003T import fetch_T003T_data
from extract_data_from_T161T import fetch_T161T_data



def main():
    ekko_data = fetch_EKKO_data()
    ekpo_data = fetch_EKPO_data()
    lfa1_data = fetch_LFA1_data()
    makt_data = fetch_MAKT_data()
    rbkp_data = fetch_RBKP_data()
    rseg_data = fetch_RSEG_data()
    t003t_data = fetch_T003T_data()
    t161t= fetch_T161T_data()


    
    # Aquí puedes hacer algo con los datos, como imprimirlos o analizarlos
    print("Datos de EKKO guardados en la base de datos.")
    print("Datos de EKPO guardados en la base de datos.")
    print("Datos de LFA1 guardados en la base de datos.")
    print("Datos de MAKT guardados en la base de datos.")
    print("Datos de RBKP guardados en la base de datos.")
    print("Datos de RSEG guardados en la base de datos.")
    print("Datos de T003T guardados en la base de datos.")
    print("Datos de T161T guardados en la base de datos.")

if __name__ == "__main__":
    main()