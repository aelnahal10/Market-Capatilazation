import requests
import pandas as pd
from bs4 import BeautifulSoup
import sqlite3
import logging
from datetime import datetime
from io import StringIO

# Centralized logging setup
def setup_logging():
    log_filename = datetime.now().strftime('etl_log_%Y-%m-%d.txt')
    logging.basicConfig(filename=log_filename, filemode='a', format='%(asctime)s - %(levelname)s - %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)

def log_progress(message):
    logging.info(message)

# Extract data from a webpage
def extract_data(url, table_attributes):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('table', attrs=table_attributes)
            if table:
                df = pd.read_html(StringIO(str(table)))[0]
                log_progress(f"Data extracted successfully from {url}")
                return df
            else:
                log_progress("No tables found")
                return pd.DataFrame()
        else:
            log_progress("Failed to fetch data")
            return pd.DataFrame()
    except Exception as e:
        log_progress(f"Error during extraction: {e}")
        return pd.DataFrame()

# Transform the extracted data
def transform_data(df, exchange_rates_csv):
    try:
        exchange_rates = pd.read_csv(exchange_rates_csv)
        rates = exchange_rates.set_index('Currency')['Rate'].to_dict()
        for currency in ['GBP', 'EUR', 'INR']:
            df[f'Market Cap ({currency})'] = (df['Market cap (US$ billion)'] * rates[currency]).round(2)
        log_progress('Data transformed successfully')
        return df
    except Exception as e:
        log_progress(f"Error during transformation: {e}")
        return pd.DataFrame()

# Load data to a CSV file
def load_to_csv(df, output_path):
    try:
        df.to_csv(output_path, index=False)
        log_progress(f"Data saved to CSV at {output_path}.")
    except Exception as e:
        log_progress(f"Error saving to CSV: {e}")

# Load data to a database
def load_to_database(df, database_name, table_name):
    try:
        with sqlite3.connect(database_name) as conn:
            df.to_sql(table_name, conn, if_exists='replace', index=False, method='multi')
            log_progress(f"Data saved to database table {table_name} in {database_name}.")
    except Exception as e:
        log_progress(f"Error saving to database: {e}")

# Main ETL function
def run_etl(url, table_attributes, exchange_rates_csv, output_csv_path, database_name, table_name):
    setup_logging()
    log_progress('Starting ETL process')
    df = extract_data(url, table_attributes)
    if not df.empty:
        df_transformed = transform_data(df, exchange_rates_csv)
        load_to_csv(df_transformed, output_csv_path)
        load_to_database(df_transformed, database_name, table_name)
        log_progress('ETL process completed successfully')
    else:
        log_progress('ETL process did not complete due to data extraction issues')

# Example usage
if __name__ == "__main__":
    url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
    table_attributes = {'class': 'wikitable'}
    exchange_rates_csv = 'exchange_rates.csv'
    output_csv_path = 'largest_banks.csv'
    database_name = 'Banks.db'
    table_name = 'Largest_banks'

    run_etl(url, table_attributes, exchange_rates_csv, output_csv_path, database_name, table_name)
