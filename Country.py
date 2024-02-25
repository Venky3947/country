import pandas as pd
from sqlalchemy import create_engine

# Extract: Read CSV data
def read_csv_file(file_path):
    try:
        df = pd.read_csv(file_path)
        print("CSV file successfully loaded.")
        return df
    except Exception as e:
        print(f"Error reading the CSV file: {e}")
        return None

# Transform: Clean data and count countries per region
def clean_and_transform_data(df):
    try:
        # Remove rows with missing values in critical columns
        df_cleaned = df.dropna(subset=['name', 'region', 'sub-region'])
        
        # Count the number of countries per region
        df_aggregated = df_cleaned.groupby(['region']).size().reset_index(name='CountryCount')
        
        print("Data transformation successful.")
        return df_aggregated
    except Exception as e:
        print(f"Error during data transformation: {e}")
        return None

# Load: Save processed data to SQL database
def load_data_to_db(df, db_connection_string):
    try:
        engine = create_engine(db_connection_string)
        df.to_sql('countries_per_region', con=engine, index=False, if_exists='replace')
        print("Data successfully loaded into the database.")
    except Exception as e:
        print(f"Error loading data into the database: {e}")

# Example usage
file_path = '/mnt/data/country_full.csv'
db_connection_string = 'sqlite:///example.db'  # Adjust for your target SQL server or cloud DB

df = read_csv_file(file_path)
if df is not None:
    df_transformed = clean_and_transform_data(df)
    if df_transformed is not None:
        load_data_to_db(df_transformed, db_connection_string)
