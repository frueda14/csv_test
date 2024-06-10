import pandas as pd
import psycopg2

def readCSV(file_path, db_config):
    # Leer el archivo CSV
    try:
        data = pd.read_csv(file_path)
    except Exception as e:
        print(f"Error while reading CSV file: {e}")
        return


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
            cursor.execute(insert_query, (row[0], row[1]))
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


# List of the files to be processed
file_paths = ["C:\\Globant_Test\\data_challenge_files\\departments.csv", "C:\\Globant_Test\\data_challenge_files\\hired_employees.csv", "C:\\Globant_Test\\data_challenge_files\\jobs.csv"]

# File separator
file_separator = ','

# PostgreSQL database config
db_config = 
{
    'dbname': 'bd_name_here',
    'user': 'user_name',
    'password': 'password',
    'host': 'host',
    'port': '5432' # default port
}

# Reading collection of file paths
for each_file_path in file_paths:
    print("Reading files ...")
    readCSV(each_file_path, db_config)

print("Finished ...")