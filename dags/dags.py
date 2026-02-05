from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import os


DAG_FOLDER = os.path.dirname(os.path.realpath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(DAG_FOLDER, ".."))
VENV_PYTHON = f"{PROJECT_ROOT}/.venv/bin/python"
VENV_DBT = f"{PROJECT_ROOT}/.venv/bin/dbt"
DBT_PROJECT_DIR = f"{PROJECT_ROOT}/dbt_project"


# Set default parameters for "Retry Logic" compliance
default_args = {
    'owner': 'martin_chen',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2026, 1, 1),
}

# DAG structure
with DAG(
    dag_id='dbt_project_orchestration_v1',
    default_args=default_args,
    description='A DAG to handle dbt ingestion, validation and transformation',
    schedule='@daily',
    catchup=False,  # dempotency Guarantee: No retroactive execution of historical tasks
    tags=['dbt', 'sql_project'],
) as dag:

    # Ingestion
    start_ingestion = BashOperator(
        task_id='data_ingestion',
        bash_command=f'cd {PROJECT_ROOT} && {VENV_PYTHON} {PROJECT_ROOT}/data_ingestion/ingest.py'
    )

    # dbt Transformation & Validation
    dbt_build = BashOperator(
        task_id='dbt_build_all_models',
        bash_command=(
            f'cd {DBT_PROJECT_DIR} && '
            'export DBT_SEND_ANONYMOUS_USAGE_STATS=False && '
            f'{VENV_DBT} build --profiles-dir .'
        )
    )

    # Done
    finish_workflow = EmptyOperator(task_id='workflow_complete')

    # 3. Set dependencies for "Handles dependencies" compliance.
    start_ingestion >> dbt_build >> finish_workflow
