# CSV Test with Pandas and PostgreSQL

This repository demonstrates how to process CSV files using Pandas and insert the data into a PostgreSQL database.

## Overview

The provided script reads CSV files from a specified directory and inserts the data into appropriate tables in a PostgreSQL database.

## Project Structure

- `csv_files/`: Directory containing the CSV files to be processed.
- `processFile_Into_BD.py`: Python script to read CSV files and insert data into PostgreSQL.
- `queries_to_use/`: Directory containing SQL queries used in the script.
- `README.md`: Project documentation.

## Setup

1. **Clone the repository:**
    ```bash
    git clone https://github.com/frueda14/csv_test.git
    cd csv_test
    ```

2. **Install the required Python packages:**
    ```bash
    pip install pandas psycopg2
    ```

3. **Configure PostgreSQL database connection:**
   Update the `db_config` dictionary in the `processFile_Into_BD.py` script with your PostgreSQL database credentials.

## Usage

1. **Place your CSV files in the `csv_files/` directory.**
2. **Run the script:**
    ```bash
    python processFile_Into_BD.py
    ```

## Script Details

The script performs the following steps:
- Reads CSV files from the specified directory.
- Determines the appropriate PostgreSQL table based on the file name.
- Inserts data into the corresponding table using `executemany`.

## Contact

For any questions or suggestions, please contact [frueda14](https://github.com/frueda14).
