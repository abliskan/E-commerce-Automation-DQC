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
from airflow.utils.trigger_rule import TriggerRules

# ENV
load_dotenv()

DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR", "/opt/airflow/include/dbt")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", "/opt/airflow/include/dbt")
SODA_CONFIG = os.getenv("SODA_CONFIG", "/opt/airflow/include/soda/soda_config.yml")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

DEFAULT_ARGS = {
    "owner": "DE-warehouse",
    "retries": 0,
}

# DAG
with DAG(
    dag_id="dag_analytics_scan_dq_mart",
    start_date=datetime(2024, 1, 1),
    schedule="30 * * * *", # Hourly at minute 30
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["mart", "dbt", "soda"],
) as dag:

    start = EmptyOperator(task_id="start")

    # Build mart models only
    @task
    def dbt_run_mart():
        subprocess.run(
            [
              "dbt",
              "run",
              "--project-dir", "/opt/airflow/include/dbt",
              "--profiles-dir", "/opt/airflow/include/dbt",
              "--select", "tag:mart"
            ],
            check=True,
        )

    # Run dbt tests on mart
    @task
    def dbt_test_mart():
        subprocess.run(
            [
              "dbt",
              "test",
              "--project-dir", "/opt/airflow/include/dbt",
              "--profiles-dir", "/opt/airflow/include/dbt",
              "--select", "tag:mart"
            ],
            check=True,
        )
    
    # Run soda scan on mart | SODA SCANS (PER MART DOMAIN)
    @task
    def soda_scan_mart_cs_sp():
        subprocess.run(
            [
                "soda", "scan",
                "-d", "mart_customer_support",
                "-c", SODA_CONFIG,
                "/opt/airflow/include/soda/mart/mart_customer_metrics.yml"
            ],
            check=True
        )
    
    @task
    def soda_scan_mart_dl_rv():
        subprocess.run(
            [
                "soda", "scan",
                "-d", "mart_finance",
                "-c", SODA_CONFIG,
                "/opt/airflow/include/soda/mart/mart_daily_revenue.yml"
            ],
            check=True
        )

    @task
    def soda_scan_mart_ods():
        subprocess.run(
            [
                "soda", "scan",
                "-d", "mart_sales",
                "-c", SODA_CONFIG,
                "/opt/airflow/include/soda/mart/mart_orders.yml"
            ],
            check=True
        )

    @task
    def soda_scan_mart_shp():
        subprocess.run(
            [
                "soda", "scan",
                "-d", "mart_logistic",
                "-c", SODA_CONFIG,
                "/opt/airflow/include/soda/mart/mart_shipments.yml"
            ],
            check=True
        )

    @task
    def soda_scan_mart_pd_pf():
        subprocess.run(
            [
                "soda", "scan",
                "-d", "mart_product",
                "-c", SODA_CONFIG,
                "/opt/airflow/include/soda/mart/mart_product_performance.yml"
            ],
            check=True
        )
    
    # SLACK ALERT | PATH (FAIL)
    @task(trigger_rule=TriggerRule.ONE_FAILED)
    def notify_slack():
        logging.info("❌ MART PIPELINE FAILED")
        if not SLACK_WEBHOOK_URL:
            print("Slack webhook not configured")
            return

        payload = {
            "text": (
                ":warning: *MART PIPELINE FAILED*\n"
                "• Soda checks failed\n"
                "• Check Soda / dbt test logs.*\n"
                "• DAG: dag_mart_analytics"
            )
        }

        response = requests.post(SLACK_WEBHOOK_URL, json=payload)
        response.raise_for_status()
    
    # MART OK PATH (SUCCESS)
    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def mart_success():
        logging.info("🎉 MART PIPELINE SUCCESS")
    
    # DAG wiring
    mart_checks = (
       soda_scan_mart_cs_sp()
       >> soda_scan_mart_dl_rv()
       >> soda_scan_mart_ods()
       >> soda_scan_mart_shp()
       >> soda_scan_mart_pd_pf()
       >> [mart_success(), notify_slack()]
    )
    end = EmptyOperator(task_id="end")

    # Flow
    (
       start
       >> dbt_run_mart()
       >> dbt_test_mart()
       >> mart_checks
       >> end
    )
