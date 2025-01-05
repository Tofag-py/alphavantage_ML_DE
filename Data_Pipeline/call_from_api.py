import logging
import csv
import os
import requests
import yaml
from prefect import task, flow
from prefect.logging import get_run_logger
import boto3
import json
import pandas as pd
import io

# Custom CSV Logging Handler
class CSVHandler(logging.Handler):
    def __init__(self, filename):
        super().__init__()
        self.filename = filename

        # Ensure the directory for the CSV exists
        os.makedirs(os.path.dirname(self.filename), exist_ok=True)

        # Open the file in append mode and write the header if it's a new file
        self.file = open(self.filename, "a", newline="")
        self.writer = csv.writer(self.file)

        # Write the header if the file is empty
        if os.stat(self.filename).st_size == 0:
            self.writer.writerow(["Timestamp", "Logger", "Level", "Message"])

    def emit(self, record):
        # Write a new log entry
        self.writer.writerow([
            self.formatTime(record),  # Timestamp
            record.name,             # Logger name
            record.levelname,        # Log level
            record.msg               # Log message
        ])

    def close(self):
        self.file.close()
        super().close()


# Configure Logging
LOG_FILE = "./logs/flow_logs.csv"  # CSV log location
csv_handler = CSVHandler(LOG_FILE)

logging.basicConfig(
    level=logging.INFO,
    handlers=[
        csv_handler,          # Log to CSV
        logging.StreamHandler()  
    ]
)


# Prefect Tasks and Flow
@task(name="log-task")
def logger_task(message):
    logger = get_run_logger()
    logger.info(message)


@task(name="retrieve_key", retries=3)
def retrieve_key(config_path):
    # Load the YAML file
    with open(config_path, "r") as file:
        config = yaml.safe_load(file)
        api_key = config['AlphaAdvantage']['key']
    return api_key


@task(name="make_api_call", retries=3)
def make_api_call(symbol, interval, api_key):
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol={symbol}&interval={interval}&apikey={api_key}'
    response = requests.get(url)
    data = response.json()
    
    # Extract the time series data and convert it to a DataFrame
    time_series = data.get("Time Series (60min)", {})
    if not time_series:
        raise ValueError("Time series data is missing in the API response.")
    
    # Convert to DataFrame
    df = pd.DataFrame.from_dict(time_series, orient='index')
    df.columns = ["Open", "High", "Low", "Close", "Volume"]
    df.index = pd.to_datetime(df.index)
    
    # Preprocess the data to fix alignment issues
    df = preprocess_data(df)
    
    return df


def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    
    if df.isnull().any(axis=1).sum() > 0:
        # Reset index and attempt to parse the data correctly
        df_reset = df.reset_index()
        
        # Ensure the first column is the timestamp and the remaining are data columns
        df_reset.columns = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']

        # Now re-set the timestamp as index
        df_reset['Timestamp'] = pd.to_datetime(df_reset['Timestamp'], errors='coerce')
        df_reset.set_index('Timestamp', inplace=True)

        # Drop any rows where timestamp parsing failed (invalid data)
        df_reset = df_reset.dropna(subset=["Open", "High", "Low", "Close", "Volume"])

        return df_reset

    return df


@task(name="store_to_s3", retries=3)
def store_to_s3(data, bucket_name, s3_key):
    s3_client = boto3.client('s3')
    
    # Convert DataFrame to CSV format in memory
    csv_buffer = io.StringIO()
    data.to_csv(csv_buffer)
    
    # Upload CSV data to S3
    s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
    return f"Data successfully stored in S3: {bucket_name}/{s3_key}"


@task(name="append_to_s3", retries=3)
def append_data_to_s3(new_data, bucket_name, s3_key):
    s3_client = boto3.client('s3')
    
    try:
        # Retrieve existing data from S3 (if any)
        existing_data = None
        try:
            response = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
            existing_data = pd.read_csv(response['Body'])
        except s3_client.exceptions.NoSuchKey:
            # If no data exists in S3, initialize an empty DataFrame
            existing_data = pd.DataFrame()
        
        # Concatenate the new data with the existing data
        updated_data = pd.concat([existing_data, new_data]).drop_duplicates()

        # Write the updated data back to S3 in CSV format
        csv_buffer = io.StringIO()
        updated_data.to_csv(csv_buffer)
        s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=csv_buffer.getvalue())
        
        return f"Data successfully updated and stored in S3: {bucket_name}/{s3_key}"

    except Exception as e:
        raise RuntimeError(f"Error appending data to S3: {e}")


@flow(name="Get Data to S3")
def get_data_to_s3():
    config_path = "../Config/Api_key"
    symbol = 'IBM'
    interval = '60min'
    bucket_name = "alpha-vantage-data-bucket"
    s3_key = f"alpha_vantage/{symbol}_{interval}.csv"

    logger_task("Starting the data retrieval flow.")

    # Retrieve API key and make API call
    api_key = retrieve_key(config_path)
    data = make_api_call(symbol, interval, api_key)

    # Store the data in S3 and append with new data
    store_to_s3(data, bucket_name, s3_key)
    result_message = append_data_to_s3(data, bucket_name, s3_key)

    logger_task(result_message)


# Run the flow
if __name__ == "__main__":
    get_data_to_s3()
