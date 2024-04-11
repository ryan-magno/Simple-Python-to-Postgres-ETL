# Import necessary packages
import pandas as pd
import logging
from sqlalchemy import create_engine

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Function to extract data from csv and load it into a pandas DataFrame
def extract_data(file_path):
    try:
        return pd.read_csv(file_path)
    except Exception as e:
        logging.error("Error loading data", exc_info=e)
        return None

# Function to fill missing values in the DataFrame
def fill_missing_values(data):
    try:
        columns_to_fill = ['date_added', 'job_type', 'organization', 'salary', 'sector']
        for column in columns_to_fill:
            data[column].fillna('unknown', inplace=True)
        return data
    except Exception as e:
        logging.error("Error filling missing values", exc_info=e)
        return None

# Function to save the cleaned DataFrame to a new csv file
def save_data_to_csv(data, file_path):
    try:
        data.to_csv(file_path, index=False)
    except Exception as e:
        logging.error("Error saving data", exc_info=e)
        return False
    return True

# Function to save the cleaned DataFrame to a PostgreSQL database
def save_data_to_postgres(data, table_name, database, user, password, host, port):
    try:
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        data.to_sql(table_name, engine, if_exists='replace', index=False)
        return True
    except Exception as e:
        logging.error("Error saving data to PostgreSQL", exc_info=e)
        return False

# Main execution flow
if __name__ == "__main__":
    # Specify the path to your dataset
    dataset_path = 'dataset.csv'
    cleaned_dataset_path = 'cleaned_dataset.csv'
    table_name = 'job_postings'  # Name of the table in the database

    # Load the data
    data = extract_data(dataset_path)
    if data is not None:
        logging.info("Data loaded successfully.")

        # Fill missing values
        data = fill_missing_values(data)
        if data is not None:
            logging.info("Missing values filled.")

            # Save the cleaned data to csv
            if save_data_to_csv(data, cleaned_dataset_path):
                logging.info(f"Cleaned data saved to {cleaned_dataset_path}.")

                # Save the cleaned data to PostgreSQL
                if save_data_to_postgres(data, table_name, database="job_postings",
                                        user="postgres", password="password",
                                        host="localhost", port="5432"):
                    logging.info("Cleaned data saved to PostgreSQL.")
                else:
                    logging.error("Failed to save cleaned data to PostgreSQL.")
            else:
                logging.error("Failed to save cleaned data to csv.")
        else:
            logging.error("Failed to fill missing values.")
    else:
        logging.error("Failed to load data.")
