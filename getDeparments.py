import pandas as pd

def readCSV(file_path):
    # Leer el archivo CSV
    try:
        data = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error al leer el archivo CSV: {e}")
        return

    # Imprimir las primeras 5 filas del archivo CSV
    print("Las primeras 5 filas del archivo CSV son:")
    print(data.head())

print("reading file ...")
file_path = "C:\\Globant_Test\\data_challenge_files\\departments.csv"

readCSV(file_path)
