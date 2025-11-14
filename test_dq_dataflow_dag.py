"""
Simple test DAG to run Dataflow job only.
Hardcoded parameters - no config file needed.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator

# ===== HARDCODED CONFIGURATION =====
# Update these values to match your GCP setup
PROJECT_ID = "your-gcp-project-id"
REGION = "us-central1"
ARTIFACT_BUCKET = "your-artifact-bucket"
DATA_BUCKET = "your-data-bucket"
TABLE_NAME = "itemCurrent"

# Input/Output paths
INPUT_PATTERN = f"gs://{DATA_BUCKET}/data/{TABLE_NAME}/*.parquet"
RULES_YAML = f"gs://{ARTIFACT_BUCKET}/rules/rules_{TABLE_NAME}.yml"
OUTPUT_STATS = f"gs://{DATA_BUCKET}/dq_results/{TABLE_NAME}/test_run/stats.json"

# DAG arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
with DAG(
    'test_dq_dataflow_job',
    default_args=default_args,
    description='Simple test DAG to run Dataflow DQ check job',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['test', 'dataflow', 'dq'],
) as dag:

    # Start task
    start = EmptyOperator(
        task_id='start',
        dag=dag
    )

    # Run Dataflow job
    run_dataflow_job = DataflowCreatePythonJobOperator(
        task_id='run_dq_check_dataflow',
        py_file=f"gs://{ARTIFACT_BUCKET}/scripts/dq_check_dataflow.py",
        job_name=f"dq-check-{TABLE_NAME}-{{{{ ts_nodash }}}}",  # Uses execution timestamp
        location=REGION,
        options={
            'input_pattern': INPUT_PATTERN,
            'rules_yaml': RULES_YAML,
            'output_stats': OUTPUT_STATS,
            'table_name': TABLE_NAME,
            'dq_run_id': 'test_run_{{ ts_nodash }}',  # Uses execution timestamp
        },
        gcp_conn_id='google_cloud_default',
        wait_until_finished=True,  # Wait for job to complete
    )

    # End task
    end = EmptyOperator(
        task_id='end',
        dag=dag
    )

    # Define workflow: start -> run_dataflow_job -> end
    start >> run_dataflow_job >> end

