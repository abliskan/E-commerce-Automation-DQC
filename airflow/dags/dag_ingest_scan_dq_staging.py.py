from __future__ import annotations

import os
import subprocess
import logging
import requests
from dotenv import load_dotenv
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.operators.empty import EmptyOperator
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

with DAG(
    dag_id="dag_ingest_scan_dq_staging",
    start_date=datetime(2024, 1, 1),
    schedule="0 * * * *", # Hourly
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["staging", "dbt", "soda"],
) as dag:

    start = EmptyOperator(task_id="start")

    # DBT
    @task
    def dbt_run_staging():
        subprocess.run(
            ["dbt", "run", "--select", "tag:staging"],
            cwd=DBT_PROJECT_DIR,
            env={**os.environ, "DBT_PROFILES_DIR": DBT_PROFILES_DIR},
            check=True,
        )

    @task
    def dbt_test_staging():
        subprocess.run(
            ["dbt", "test", "--select", "tag:staging"],
            cwd=DBT_PROJECT_DIR,
            env={**os.environ, "DBT_PROFILES_DIR": DBT_PROFILES_DIR},
            check=True,
        )
        
    # SODA CHECKS
    @task(trigger_rule="all_done")
    def soda_scan_staging():
        subprocess.run(
            [
                "soda",
                "scan",
                "-d",
                "bronze_staging",
                "-c",
                SODA_CONFIG,
                "/opt/airflow/include/soda/checks/staging_checks.yml",
            ],
            check=True,
        )
        
    # Branch based on Soda result (TaskFlow-native)
    @task.branch(trigger_rule="all_done")
    def branch_on_staging_dq(exit_code: int) -> str:
        if exit_code == 0:
            return "staging_success"
        else:
            return "run_staging_quarantine"

    # QUARANTINE PATH (FAIL)
    @task
    def run_staging_quarantine():
        subprocess.run(
            [
                "dbt",
                "run",
                "--select",
                "models/quarantine/staging"
            ],
        cwd=DBT_PROJECT_DIR,
        env={**os.environ, "DBT_PROFILES_DIR": DBT_PROFILES_DIR},
        check=True,
    )
        
    # SLACK ALERT
    @task.branch(trigger_rule="all_done")
    def notify_slack():
        logging.info("❌ STAGING DATA QUALITY FAILED")
        if not SLACK_WEBHOOK_URL:
            print("Slack webhook not configured")
            return

        payload = {
            "text": (
                ":warning: *STAGING DATA QUALITY FAILURE*\n"
                "• Soda checks failed\n"
                "• Data moved to *staging quarantine*\n"
                "• DAG: extract_oltp_to_staging_with_quarantine"
            )
        }

        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        response.raise_for_status()
        
    @task(trigger_rule="all_done")
    def fail_pipeline() -> None:
        raise AirflowFailException(
            "Staging data quality violations detected"
        )
        
    # Staging OK PATH (SUCCESS)
    @task
    def staging_success() -> None:
        logging.info("Staging pipeline completed successfully")
    
    # DAG wiring
    soda_exit = soda_scan_staging()
    branch = branch_on_staging_dq(soda_exit)
    end = EmptyOperator(task_id="end")

    # FLOW
    (
        start
        >> dbt_run_staging()
        >> dbt_test_staging()
        >> soda_exit
        >> branch
    )

    branch >> staging_success() >> end
    
    (
    branch 
    >> run_staging_quarantine()
    >> notify_slack()
    >> fail_pipeline()
    )
