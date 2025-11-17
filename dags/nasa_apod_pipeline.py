import pendulum
import requests
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from airflow.models import Variable

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator

api_key = Variable.get("nasa_api_key") # Import NASA API key from Airflow Variables
NASA_APOD_API = f"https://api.nasa.gov/planetary/apod?api_key={api_key}"
POSTGRES_CONN_ID = "astro_default" # Airflow Postgres connection ID
POSTGRES_TABLE = "nasa_apod" # Target Postgres table name
CSV_FILE_PATH = Path("/usr/local/airflow/data/apod_data.csv") # Local path to save CSV file

# Define the DAG
@dag(
    dag_id="nasa_apod_etl_pipeline",
    start_date=pendulum.datetime(2025, 11, 16, tz="UTC"), # Set a future start date to prevent immediate execution
    schedule="@daily", # Run the DAG daily
    catchup=False,
    doc_md="DAG for NASA APOD ETL Pipeline",
)
def nasa_apod_etl():
    @task
    def task_extract_and_transform() -> str: # Task to extract and transform data from NASA APOD API
        print(f"Extracting Data from {NASA_APOD_API}")
        response = requests.get(NASA_APOD_API) # Make the API request
        response.raise_for_status() # Raise an error for bad responses
        data = response.json()

        fields_of_interest = [
            "date",
            "title",
            "url",
            "explanation",
            "media_type",
            "service_version",
            "hdurl",
        ]

        df = pd.json_normalize(data) # Normalize the JSON data into a flat table

        # Ensure all fields of interest are present
        for col in fields_of_interest:
            if col not in df.columns:
                df[col] = None

        df_cleaned = df[fields_of_interest] # Select only the fields of interest

        print("Data transformed successfully:")
        print(df_cleaned.head())

        return df_cleaned.to_json(orient="split") # Passing the data to the next Airflow task 

    @task
    def task_load_to_postgres_and_csv(json_data: str): # Task to load data into Postgres and save as CSV
        df = pd.read_json(json_data, orient="split") # Read the data from the previous task

        print(f"Saving Data to Local CSV at {CSV_FILE_PATH}...")
        CSV_FILE_PATH.parent.mkdir(parents=True, exist_ok=True) # Ensure the directory exists
        df.to_csv(CSV_FILE_PATH, index=False) # Save DataFrame to CSV
        print("Successfully saved to CSV.")

        print(f"Loading Data into Postgres Table '{POSTGRES_TABLE}'...")
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID) # Create Postgres hook
        engine = hook.get_sqlalchemy_engine() # Get SQLAlchemy engine from the hook

        # Load DataFrame into Postgres table
        df.to_sql(
            name=POSTGRES_TABLE,
            con=engine,
            if_exists="append", # Append to existing table
            index=False,
            chunksize=500,
        )
        print("Successfully Loaded Data to Postgres.")

    task_version_with_dvc = BashOperator( # Task to version the CSV file using DVC
        task_id="task_version_with_dvc",
        doc="Run DVC to version the new CSV file",
        cwd="/usr/local/airflow", # Set working directory to Airflow home
        # Commands to add the CSV file to DVC and commit it to the DVC cache
        bash_command=f"""
        echo "Running dvc add for {CSV_FILE_PATH.relative_to('/usr/local/airflow')}"
        
        # This command stages the CSV file, creating/updating the .dvc metadata file
        dvc add {CSV_FILE_PATH.relative_to('/usr/local/airflow')}

        echo "Committing data to DVC cache..."
        
        # This command saves the file's contents to the .dvc/cache
        dvc commit {CSV_FILE_PATH.relative_to('/usr/local/airflow')}.dvc
        
        echo "DVC versioning complete."
        """
    )

    task_commit_git_metadata = BashOperator( # Task to commit the DVC metadata file to Git
        task_id="task_commit_git_metadata",
        doc="Commit the updated DVC metadata file to Git",
        cwd="/usr/local/airflow", # Set working directory to Airflow home
        # Commands to add and commit the .dvc file to Git
        bash_command=f"""
        echo "Committing DVC metadata file to Git..."
        
        # Ensure the Airflow directory is marked as a safe directory for Git
        git config --global --add safe.directory /usr/local/airflow
        
        # Configure Git user
        git config --global user.email "baseersoomro2013@gmail.com"
        git config --global user.name "Noor Ul Baseer"
        
        # Add the .dvc file that was changed in the previous step
        git add {CSV_FILE_PATH.relative_to('/usr/local/airflow')}.dvc
        
        # We add '[skip ci]' to prevent CI/CD loops if this repo is on GitHub
        git commit -m "Update NASA APOD data via Airflow [skip ci]"
        
        echo "Git commit complete."
        """
    )

    data_json = task_extract_and_transform() #Call the first task and store its output

    load_task = task_load_to_postgres_and_csv(data_json) # Call the second task with the output of the first task

    load_task >> task_version_with_dvc >> task_commit_git_metadata # Define task dependencies

# Instantiate the DAG
nasa_apod_etl()
