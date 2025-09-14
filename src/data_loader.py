import os
import csv
import pandas as pd
from sqlalchemy import create_engine, inspect

def load_database(data_path=None, db_uri=None):
    if db_uri:
        # Connect to database using SQLAlchemy and read all tables (example: first table)
        engine = create_engine(db_uri)
        inspector = inspect(engine)
        table_names = inspector.get_table_names()
        if not table_names:
            raise ValueError("No tables found in database.")
        # Read the first table as a list of dicts
        with engine.connect() as conn:
            result = conn.execute(f"SELECT * FROM {table_names[0]}")
            columns = result.keys()
            data = [dict(zip(columns, row)) for row in result.fetchall()]
        return data

    elif data_path:
        if not os.path.exists(data_path):
            raise FileNotFoundError(f"Database file {data_path} not found.")
        # Load CSV as list of dicts
        with open(data_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            data = [row for row in reader]
        return data

    else:
        raise ValueError("Provide either data_path or db_uri")

def load_databases(data_paths):
    """
    Load and combine multiple CSV files into a single DataFrame.
    data_paths: list of file paths to CSV files.
    Returns: combined pandas DataFrame.
    """
    dfs = []
    for path in data_paths:
        try:
            df = pd.read_csv(path)
            dfs.append(df)
        except Exception as e:
            print(f"Error loading {path}: {e}")
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df
    else:
        return pd.DataFrame()  # Return empty DataFrame if nothing loaded