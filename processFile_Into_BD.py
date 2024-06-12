import pandas as pd
import psycopg2
import os

# PostgreSQL database config
db_config = {
    'dbname': 'csv_test_db',
    'user': 'postgres',
    'password': 'postgres2024#',
    'host': 'localhost',
    'port': '5432'  # default port
}

def delete_data_tables(db_config):
    delete_query = 'delete from csv_files.departments; delete from csv_files.hired_employees; delete from csv_files.jobs;'
    # Trying to connect to the postgres database
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
    except Exception as e:
        print(f"Error while connecting to the PostgreSQL database: {e}")
        return

    # deleting data before new insertion
    try:
        cursor.execute(delete_query)
        conn.commit()
        print("Tables deleted ...")
    except Exception as e:
        conn.rollback()
        cursor.close()
        conn.close()
        print(f"Error while deleting data from tables: {e}")
        return;

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
        data = pd.read_csv(file_path, sep=separator, header=None)
        data.fillna(value={0: 0}, inplace=True)
        data.fillna(value={1: 'NO NAME'}, inplace=True)
        data.fillna(value={2: '1990-01-01T23:27:38Z'}, inplace=True)
        data.fillna(value={3: 0}, inplace=True)
        data.fillna(value={4: 0}, inplace=True)
    except Exception as e:
        print(f"Error while reading CSV file: {e}")
        return

    # validating file name to build the proper insert_query string
    if "departments" in file_path:
        insert_query = """ INSERT INTO csv_files.departments (id, department) VALUES (%s, %s) """
    elif "hired_employees" in file_path:
        insert_query = """ INSERT INTO csv_files.hired_employees (id, name, hire_datetime, department_id, job_id) VALUES (%s, %s, %s, %s, %s) """
    elif "jobs" in file_path:
        insert_query = """ INSERT INTO csv_files.jobs (id, job) VALUES (%s, %s) """
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

            if "departments" in file_path:
                cursor.executemany(insert_query, [(row.iloc[0], row.iloc[1])])
            elif "hired_employees" in file_path:
                cursor.executemany(insert_query, [(row.iloc[0], row.iloc[1], row.iloc[2], row.iloc[3], row.iloc[4])])
            elif "jobs" in file_path:
                cursor.executemany(insert_query, [(row.iloc[0], row.iloc[1])])
            else:
                print(f"Nothing to insert")
            count += 1
            if count % 1000 == 0:
                conn.commit()
                print(f"Rows processed {count}.") 
        # Final commit to avoid missing any rows after finishing the loop
        conn.commit()
        print("Data inserted correctly.")
    except Exception as e:
        conn.rollback()
        print(f"Error while inserting data into tables: {e}")
    finally:
        cursor.close()
        conn.close()


# Looks for the directory which contains all of the csv files to import
directory_path = "C:\\Users\\fabia\\OneDrive\\Documentos\\GitHub\\csv_test\\csv_files"
# File separator
file_separator = ','
print("Starting ...")
delete_data_tables(db_config)
process_csv_files_in_folder(directory_path, file_separator)
print("Finish ...")