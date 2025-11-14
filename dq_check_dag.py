import json
import logging
import uuid
import yaml
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.utils.dates import days_ago
from airflow.utils.state import State

# --- Load Configuration ---
with open('config.yaml', 'r') as f:
    config = yaml.safe_load(f)

# --- Extract Configuration Values ---
PROJECT_ID = config['project_id']
REGION = config['region']
DATA_BUCKET = config['data_bucket']
ARTIFACT_BUCKET = config['artifact_bucket']
DQ_AUDIT_TABLE = f"{PROJECT_ID}.{config['audit_dataset']}.{config['dq_audit_table']}"

# --- Table Configurations ---
TABLE_CONFIGS = {}
for table_name, table_config in config['table_configs'].items():
    TABLE_CONFIGS[table_name] = {
        'input_pattern': table_config['input_pattern'].format(
            data_bucket=DATA_BUCKET
        ),
        'rules_yaml': table_config['rules_yaml'].format(
            artifact_bucket=ARTIFACT_BUCKET
        ),
        'output_path': table_config['output_path'].format(
            data_bucket=DATA_BUCKET,
            dq_run_id='{dq_run_id}'
        ),
        'table_name': table_config.get('table_name', table_name)
    }

# --- Audit Logger Setup ---
from audit_logger import BigQueryAuditLogger
audit_logger = BigQueryAuditLogger(
    gcp_conn_id=config['audit_logger']['gcp_conn_id'],
    project_id=PROJECT_ID,
    dataset_id=config['audit_dataset'],
    dag_summary_table=config['dag_summary_table'],
    task_audit_table=config['task_audit_table'],
    run_env=config['audit_logger']['run_env']
)

# --- Standard Task-Level Callbacks ---
def on_task_failure(context):
    """
    Generic callback for ANY task failure.
    """
    task_instance = context['task_instance']
    log.error(f"CRITICAL: Task {task_instance.task_id} failed in DAG {task_instance.dag_id}")
    audit_logger.log_task_run(
        context=context,
        component_id=config['audit_logger']['component_id'],
        source_stream="data_quality",
        task_status="FAILED",
        start_time=task_instance.start_date,
        end_time=datetime.now(),
        task_log=f"Task {task_instance.task_id} failed",
        task_exception=str(context.get('exception', 'Unknown error'))
    )

def on_task_success(context):
    """Optional: standard logging for task success."""
    task_instance = context['task_instance']
    log.info(f"Task {context['task_instance'].task_id} succeeded.")
    audit_logger.log_task_run(
        context=context,
        component_id=config['audit_logger']['component_id'],
        source_stream="data_quality",
        task_status="SUCCESS",
        start_time=task_instance.start_date,
        end_time=datetime.now(),
        task_log=f"Task {task_instance.task_id} completed successfully",
        task_exception=None
    )

default_args = {
    'owner': 'data-engineering', # egsop
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 0,
    'on_failure_callback': on_task_failure,
    # 'on_success_callback': on_task_success, # Uncomment if noisy logging is desired
}

log = logging.getLogger(__name__)

with DAG(
    'dq_check_orchestration',
    default_args=default_args,
    description='Orchestrates Dataflow DQ Check with auditing',
    schedule_interval='@daily', # none
    start_date=days_ago(1),
    catchup=False,
    tags=['data-quality', 'production'],
) as dag:

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    dq_run_id = f"dq_run_{uuid.uuid4().hex[:8]}"

    dataflow_tasks = {}
    output_paths = []
    for table_name, table_config in TABLE_CONFIGS.items():
        output_path = table_config['output_path'].format(dq_run_id=dq_run_id)
        output_paths.append(output_path)
        
        run_dq_dataflow = DataflowCreatePythonJobOperator(
            task_id=f"run_dq_check_dataflow_{table_name}",
            py_file=f"gs://{ARTIFACT_BUCKET}/scripts/dq_check_dataflow.py",
            job_name=f"dq-check-{table_name}-{datetime.now().strftime('%Y%m%d-%H%M%S')}",
            location=REGION,
            options={
                'input_pattern': table_config['input_pattern'],
                'rules_yaml': table_config['rules_yaml'],
                'output_stats': output_path,
                'table_name': table_config['table_name'],
                'dq_run_id': dq_run_id 
            },
            gcp_conn_id='google_cloud_default',
            wait_until_finished=True,
        )
        dataflow_tasks[table_name] = run_dq_dataflow

    @task(trigger_rule="all_done")
    def perform_complex_audit(dq_run_id, output_paths, **context):
        """
        Reads Dataflow outputs for all tables, formats to the strict DQ Audit Schema, and inserts to BQ.
        """
        dag_run = context['dag_run']
        ti = context['task_instance']
        
        dag_audit_id = str(uuid.uuid4())
        src_details = []

        for output_path in output_paths:
            try:
                gcs_hook = GCSHook()
                bucket, blob = output_path.replace("gs://", "").split("/", 1)
                
                if gcs_hook.exists(bucket_name=bucket, object_name=blob):
                    file_content = gcs_hook.download(bucket_name=bucket, object_name=blob)
                    dq_results = json.loads(file_content.decode('utf-8'))

                    details_list = dq_results.get('details', [])
                    
                    if details_list:
                        for detail in details_list:
                            src_details.append({
                                "file_name": detail.get('file_name', 'UNKNOWN'),
                                "column_name": detail.get('column', 'N/A'),
                                "total_count": detail.get('element_count', 0),
                                "rule_type": detail.get('expectation', 'UNKNOWN'),
                                "threshold": str(detail.get('sample_unexpected_values', '')),
                                "failed": not detail.get('success', False),
                                "failed_count": detail.get('unexpected_count', 0),
                                "status": "FAIL" if not detail.get('success', False) else "PASS",
                                "message": str(detail.get('sample_unexpected_values', ''))
                            })
                    else:
                        src_details.append({
                            "file_name": dq_results.get('table_name', 'UNKNOWN'),
                            "column_name": "N/A",
                            "total_count": dq_results.get('total_records', 0),
                            "rule_type": "AGGREGATE",
                            "threshold": "",
                            "failed": dq_results.get('dq_status', 'UNKNOWN') == 'FAILED',
                            "failed_count": dq_results.get('total_unexpected_count', 0),
                            "status": dq_results.get('dq_status', 'UNKNOWN'),
                            "message": f"Pass rate: {dq_results.get('pass_rate_pct', 0):.2f}%"
                        })
                else:
                    log.error(f"Dataflow output not found for {output_path}. DQ job likely failed.")
                    src_details.append({
                        "file_name": output_path, 
                        "status": "CRITICAL_FAILURE", 
                        "message": "Dataflow failed to produce output statistics."
                    })

            except Exception as e:
                log.error(f"Failed to parse DQ results for {output_path}: {e}")
                src_details.append({
                    "file_name": output_path, 
                    "status": "AUDIT_PARSING_ERROR", 
                    "message": str(e)
                })

        audit_logger.log_dq_audit(context, dq_run_id, "multiple_tables", src_details, dag_audit_id)

    audit_task = perform_complex_audit(dq_run_id, output_paths)

    def decide_next_steps(output_paths, **context):
        """Reads all Dataflow outputs to decide operational next steps."""
        all_passed = True
        for output_path in output_paths:
            gcs_hook = GCSHook()
            bucket, blob = output_path.replace("gs://", "").split("/", 1)
            if not gcs_hook.exists(bucket, blob):
                return "handler_critical_failure"
                 
            stats = json.loads(gcs_hook.download(bucket, blob).decode('utf-8'))
            pass_rate_pct = stats.get('pass_rate_pct', 0)
            if pass_rate_pct < 95.0:
                all_passed = False
                break
        
        if all_passed:
            return "proceed_to_curated"
        return "handler_quarantine"

    branch_decision = BranchPythonOperator(
        task_id="dq_threshold_check",
        python_callable=decide_next_steps,
        op_kwargs={'output_paths': output_paths},
        provide_context=True
    )

    proceed = EmptyOperator(task_id="proceed_to_curated")
    quarantine = EmptyOperator(task_id="handler_quarantine")
    critical_fail = EmptyOperator(task_id="handler_critical_failure")

    start >> list(dataflow_tasks.values())
    list(dataflow_tasks.values()) >> audit_task
    list(dataflow_tasks.values()) >> branch_decision
    branch_decision >> [proceed, quarantine, critical_fail]
    [audit_task, proceed, quarantine, critical_fail] >> end
