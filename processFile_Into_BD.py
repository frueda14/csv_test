import pandas as pd
import psycopg2
import os

# PostgreSQL database config
db_config = 
{
    'dbname': 'bd_name_here',
    'user': 'user_name',
    'password': 'password',
    'host': 'localhost',
    'port': '5432' # default port
}

def process_csv_files_in_folder(directory_path, separator):
    # Folder exists?
    if not os.path.isdir(directory_path):
        print(f"File path {directory_path} doesn't exists.")
        return

    # Navigates the folder looking for CSV files
    for each_file in os.listdir(directory_path):

        if each_file.endswith(".csv"):
            file_path = os.path.join(directory_path, each_file)
            readCSV(file_path, db_config, separator)


def readCSV(file_path, db_config, separator):

    insert_query = ''

    # Leer el archivo CSV
    try:
        data = pd.read_csv(file_path, sep=separator)
    except Exception as e:
        print(f"Error while reading CSV file: {e}")
        return

    # validating file name to build the proper insert_query string
    if "departments" in file_path:
        insert_query = """ INSERT INTO departments (columna1, columna2) VALUES (%s, %s) """
    elif "hired_employees" in file_path:
        insert_query = """ INSERT INTO hired_employees (columna1, columna2, columna3, columna4, columna5) VALUES (%s, %s, %s, %s, %s) """
    elif "jobs" in file_path:
        insert_query = """ INSERT INTO jobs (columna1, columna2) VALUES (%s, %s) """
    else:
        insert_query = ''

    # Trying to connect to the postgres database
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error while connecting to the PostgreSQL database: {e}")
        return

    # Inserting file data into table
    try:
        count = 0
        for each_file_line, row in data.iterrows():
            cursor.execute(insert_query, (row[0], row[1])) # find a way to adjust parameters depending on the table
            count += 1
            if count % 1000 == 0:
                conn.commit()
                print(f"Rows processed {count}.")
        
        # Final commit to avoid missing any rows after finishing the loop
        conn.commit()  
    except Exception as e:
        conn.rollback()
        print(f"Error al insertar datos en la tabla: {e}")
    finally:
        cursor.close()
        conn.close()

    print("Datos insertados correctamente.")


# Looks for the directory which contains all of the csv files to import
directory_path = "\\csv_files"

# File separator
file_separator = ','

process_csv_files_in_folder(directory_path, file_separator)