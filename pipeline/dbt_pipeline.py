from prefect import task, flow
from dotenv import load_dotenv
import os
import subprocess

# Load .env file
load_dotenv()

@task
def set_env_vars():
    os.environ["SNOWFLAKE_ACCOUNT"] = os.getenv("snowflake_account")
    os.environ["SNOWFLAKE_USER"] = os.getenv("snowflake_user")
    os.environ["SNOWFLAKE_PASSWORD"] = os.getenv("snowflake_password")
    os.environ["SNOWFLAKE_DATABASE"] = os.getenv("snowflake_database")
    os.environ["SNOWFLAKE_WAREHOUSE"] = os.getenv("snowflake_datawarehouse")
    os.environ["SNOWFLAKE_SCHEMA"] = os.getenv("snowflake_schema")
    return True

@task
def run_dbt_models():
    # Change directory to your dbt project if needed
    dbt_cmd = ["dbt", "run"]
    result = subprocess.run(dbt_cmd, capture_output=True, text=True)
    print(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt run failed: {result.stderr}")
    return True

@flow(name="dbt_snowflake_pipeline")
def dbt_pipeline():
    set_env_vars()
    run_dbt_models()

if __name__ == "__main__":
    dbt_pipeline()
