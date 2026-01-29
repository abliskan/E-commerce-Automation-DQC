from __future__ import annotations

import os
import subprocess
import logging
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowFailException

# ENV
load_dotenv()

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/include/dbt")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/include/dbt")
SODA_CONFIG = os.getenv("SODA_CONFIG", "/opt/airflow/include/soda/soda_config.yml")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# DAG definition
DEFAULT_ARGS = {
    "owner": "DE-warehouse",
    "retries": 0,
}

# DAG
with DAG(
    dag_id="dag_modeling_transform_dq_dwh",
    start_date=datetime(2024, 1, 1),
    schedule="15 * * * *", # Hourly at minute 15
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["dwh", "dbt", "soda"],
) as dag:

    start = EmptyOperator(task_id="start")

    # DIMENSIONS
    @task
    def dbt_run_dimensions():
        subprocess.run(
            [
              "dbt",
              "run",
              "--project-dir", "/opt/airflow/include/dbt",
              "--profiles-dir", "/opt/airflow/include/dbt",
              "--select", "tag:dimension",
            ],
             check=True,
        )

    # FACTS
    @task
    def dbt_run_facts():
        subprocess.run(
            [
              "dbt",
              "run",
              "--project-dir", "/opt/airflow/include/dbt",
              "--profiles-dir", "/opt/airflow/include/dbt",
              "--select", "tag:fact"
            ],
            check=True,
        )

    # TEST
    @task
    def dbt_test_dim():
        subprocess.run(
            [
              "dbt",
              "test",
              "--project-dir", "/opt/airflow/include/dbt",
              "--profiles-dir", "/opt/airflow/include/dbt",
              "--select", "tag:dimension"
            ],
            check=True,
        )

    @task
    def dbt_test_fact():
        subprocess.run(
            [
              "dbt",
              "test",
              "--project-dir", "/opt/airflow/include/dbt",
              "--profiles-dir", "/opt/airflow/include/dbt",
              "--select", "tag:fact"
            ],
            check=True,
        )

    # SODA: DWH CHECKS
    @task(trigger_rule="all_done")
    def soda_scan_dwh():
        subprocess.run(
            [
                "soda", "scan",
                "-d", "dwh",
                "-c", SODA_CONFIG,
                "/opt/airflow/include/soda/dwh/dwh_checks.yml"
            ],
            check=True
        )
    
    # BRANCHING (TaskFlow-native)
    @task.branch(trigger_rule="all_done")
    def branch_on_dwh_dq(exit_code: int) -> str:
        if exit_code == 0:
            return "dwh_success"
        else:
            return "run_dwh_quarantine"
    
    # QUARANTINE PATH (FAIL)
    @task
    def run_dwh_quarantine():
        cmd = ["dbt", "run", "--select", "tag:quarantine_dwh"]
        result = subprocess.run(
            cmd, cwd=DBT_PROJECT_DIR, capture_output=True, text=True
        )
        logging.info(result.stdout)
        if result.returncode != 0:
            logging.error(result.stderr)
            raise Exception("dbt quarantine failed")
    
    # SLACK ALERT
    @task.branch(trigger_rule="all_done")
    def notify_slack():
        logging.info("❌ DWH DATA QUALITY FAILED")
        if not SLACK_WEBHOOK_URL:
            print("Slack webhook not configured")
            return

        payload = {
            "text": (
                ":warning: *DWH DATA QUALITY FAILURE*\n"
                "• Soda checks failed\n"
                "• Data moved to *dwh quarantine*\n"
                "• DAG: dwh_dimensional_modeling"
            )
        }

        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        response.raise_for_status()

    @task(trigger_rule="all_done")
    def fail_pipeline() -> None:
        raise AirflowFailException("DWH data quality violations detected")

    # DWH OK PATH (SUCCESS)
    @task
    def dwh_success() -> None:
        logging.info("DWH pipeline completed successfully")

    # DAG wiring
    soda_exit = soda_scan_dwh()
    branch = branch_on_dwh_dq(soda_exit)
    end = EmptyOperator(task_id="end")
    
    # Flow
    (
        start
        >> dbt_run_dimensions()
        >> dbt_test_dim()
        >> dbt_run_facts()
        >> dbt_test_fact()
        >> soda_exit
        >> branch
    )

    branch >> dwh_success() >> end
    
    (
    branch 
    >> run_dwh_quarantine()
    >> notify_slack()
    >> fail_pipeline()
    )
