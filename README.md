# Simple ETL Project

## Project Overview

This project demonstrates a simple ETL (Extract, Transform, Load) process using Python. It is designed to extract data from a CSV file, transform the data by filling in missing values, and then load the cleaned data into both a new CSV file and a PostgreSQL database.

The ETL process consists of the following steps:
1. **Extract**: Load data from a specified CSV file into a pandas DataFrame.
2. **Transform**: Fill missing values in the DataFrame for specified columns with 'unknown'.
3. **Load**: Save the cleaned DataFrame to a new CSV file and a PostgreSQL database.

## Installation

To run this project, you need Python installed on your machine along with the following Python packages:
- pandas
- SQLAlchemy
- PostgreSQL database (for loading the data into a database)

## Usage 

Before running the script, ensure you have a PostgreSQL database setup and accessible. You will need to modify the database connection settings within the script to match your PostgreSQL setup.

To run the ETL process, execute the script `main.py`:

```
python main.py
```

Ensure you have the CSV file named `dataset.csv` in the same directory as your script. 
