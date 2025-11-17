import pendulum
import requests
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from io import StringIO
import subprocess
from airflow.models import Variable

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator

api_key = Variable.get("nasa_api_key") # Import NASA API key from Airflow Variables
test_date = "2025-11-16"
NASA_APOD_API = f"https://api.nasa.gov/planetary/apod?api_key={api_key}&date={test_date}"
POSTGRES_CONN_ID = "postgres_default" # Airflow Postgres connection ID
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
        dvc commit {CSV_FILE_PATH.relative_to('/usr/local/airflow')}
        
        echo "DVC versioning complete."
        """
    )

    @task
    def task_push_git_metadata(csv_file_path_str: str) -> str: # Task to commit DVC metadata to Git and push
        # Retrieve GitHub Personal Access Token from Airflow Variables
        try:
            gh_pat = Variable.get("GH_PAT")
        except KeyError:
            raise Exception("GH_PAT Variable not found in Airflow. Please add it.")
            
        # GitHub repository details
        github_username = "NoorUlBaseer"
        repo_name = "NASA-APOD-ETL-Pipeline"
        remote_url = f"https://{gh_pat}@github.com/{github_username}/{repo_name}.git" # Remote URL with PAT
        
        csv_file_path = Path(csv_file_path_str) # Convert string path back to Path object
        dvc_file_path = csv_file_path.with_suffix(csv_file_path.suffix + '.dvc') # Corresponding .dvc file path
        
        project_root = "/usr/local/airflow" # Airflow home directory as project root

        print("Running Git commands...")
        
        def run_cmd(cmd: list): # Helper function to run shell commands
            print(f"Running command: {' '.join(cmd)}")
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=project_root
            )
            if result.returncode != 0:
                print(f"Error Output: {result.stderr}")
                raise Exception(f"Command failed: {result.stderr}")
            print(f"Command Output: {result.stdout}")
            return result.stdout

        print("Configuring Git...")
        run_cmd(["git", "config", "--global", "--add", "safe.directory", project_root]) # Mark directory as safe
        run_cmd(["git", "config", "--global", "user.email", "baseersoomro2013@gmail.com"]) # Set Git user email
        run_cmd(["git", "config", "--global", "user.name", "Noor Ul Baseer (Airflow)"]) # Set Git user name

        print("Setting Git pull strategy to rebase...")
        run_cmd(["git", "config", "--global", "pull.rebase", "true"]) # Set pull strategy to rebase

        print("Adding DVC file...")
        run_cmd(["git", "add", str(dvc_file_path.relative_to(project_root))]) # Stage the .dvc file for commit

        print("Checking for changes...")
        status_result = run_cmd(["git", "status", "--porcelain", str(dvc_file_path.relative_to(project_root))]) # Check Git status
        # If there are no changes, exit early
        if not status_result:
            print("No changes to commit. Exiting clean.")
            return "No new data; no commit pushed."
        
        print("Committing DVC file...")
        run_cmd(["git", "commit", "-m", "feat(data): Update NASA APOD data via Airflow [skip ci]"]) # Commit the .dvc file

        print("Setting remote URL and pulling latest changes...")
        run_cmd(["git", "remote", "set-url", "origin", remote_url]) # Set remote URL with PAT
        run_cmd(["git", "pull", "origin", "master"]) # Pull latest changes from GitHub
        
        print("Pushing to GitHub...")
        run_cmd(["git", "push", "origin", "HEAD:master"]) # Push changes to GitHub

        return "Successfully pushed DVC metadata to GitHub."

    data_json = task_extract_and_transform() #Call the first task and store its output

    load_task = task_load_to_postgres_and_csv(data_json) # Call the second task with the output of the first task

    push_task = task_push_git_metadata(str(CSV_FILE_PATH)) # Call the third task with the CSV file path

    load_task >> task_version_with_dvc >> push_task # Define task dependencies

# Instantiate the DAG
nasa_apod_etl()
