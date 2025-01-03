import pandas as pd
from sqlalchemy import create_engine
import urllib.parse

class load():
    def load(self):
        # Database Configuration
        db_user = "postgres"
        db_password = "Jeet@6291"
        db_host = "localhost"
        db_port = "5432"
        db_name = "UCL"

        # URL-encode the password
        encoded_password = urllib.parse.quote_plus(db_password)

        # Create Engine
        engine = create_engine(
            f"postgresql://{db_user}:{encoded_password}@{db_host}:{db_port}/{db_name}"
        )

        # File Path
        csv_file_path = "Data/NORMALIZE/API_output_NORM.csv"

        # Read CSV
        try:
            df = pd.read_csv(csv_file_path)
            print("CSV file successfully loaded into a DataFrame.")
        except Exception as e:
            print(f"Error reading CSV file: {e}")
            exit()

        # Load DataFrame into Database
        try:
            df.to_sql(name="ucl_2425", con=engine, if_exists="replace", index=False)
            print("Data successfully loaded into the 'ucl_2425' table.")
        except Exception as e:
            print(f"Error loading data into the database: {e}")
