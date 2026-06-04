"""
nyc_tlc_snowflake_dbt — orchestrates the Project 2 pipeline.

Flow:  load_raw  >>  dbt_transform (Cosmos renders seeds, snapshot, models and
       tests as individual Airflow tasks, in dependency order).

dbt runs in LOCAL execution mode using the dbt in .venv-airflow (installed via
astronomer-cosmos[dbt-snowflake]). dbt packages live in dbt/dbt_packages
(project-local), so they resolve regardless of which venv's dbt is invoked.
Snowflake creds come from ~/.dbt/profiles.yml via env vars inherited from the
Airflow process (which was started after `source .env`).
"""
import os
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.constants import ExecutionMode

HOME = os.path.expanduser("~")
REPO = f"{HOME}/project2-snowflake-dbt"
DBT_PROJECT_DIR = f"{REPO}/dbt"
DBT_EXECUTABLE = f"{REPO}/.venv-airflow/bin/dbt"
PROFILES_YML = f"{HOME}/.dbt/profiles.yml"
LOADER_PY = f"{REPO}/ingestion/load_raw.py"
DBT_VENV_PYTHON = f"{REPO}/.venv-dbt/bin/python"

profile_config = ProfileConfig(
    profile_name="project2",
    target_name="dev",
    profiles_yml_filepath=PROFILES_YML,
)

execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
    dbt_executable_path=DBT_EXECUTABLE,
)

with DAG(
    dag_id="nyc_tlc_snowflake_dbt",
    description="NYC TLC pipeline: raw load + dbt transform on Snowflake",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["project2", "snowflake", "dbt"],
) as dag:

    load_raw = BashOperator(
        task_id="load_raw",
        bash_command=f"{DBT_VENV_PYTHON} {LOADER_PY}",
        cwd=REPO,
    )

    transform = DbtTaskGroup(
        group_id="dbt_transform",
        project_config=ProjectConfig(DBT_PROJECT_DIR),
        profile_config=profile_config,
        execution_config=execution_config,
    )

    load_raw >> transform
