from __future__ import annotations

import os
import psycopg2
from datetime import datetime, timedelta

from dotenv import load_dotenv
from faker import Faker
from faker_commerce import Provider

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.log.logging_mixin import LoggingMixin

# Config
DEFAULT_ROWS_PER_TABLE = 20_000
DEFAULT_CHUNK_SIZE = 5_000

# DAG definition
DEFAULT_ARGS = {
    "owner": "DE-operationals",
    "retries": 0,
}

# Bronze Seeder using psycopg2 COPY FROM STDIN
class BronzeSeeder(LoggingMixin):
    def __init__(self, rows_per_table: int = DEFAULT_ROWS_PER_TABLE, chunk_size: int = DEFAULT_CHUNK_SIZE) -> None:
        super().__init__()
        load_dotenv()
        self.rows_per_table = rows_per_table
        self.chunk_size = chunk_size

        self.fake = Faker()
        self.fake.add_provider(Provider)
        self.conn = None
        self.cur = None

    # ------------------ DB Connection ------------------
    def _conn_params(self) -> dict:
        return dict(
            host=os.getenv("PGHOST", "postgres-dbt"),
            port=int(os.getenv("PGPORT", "5432")),
            dbname=os.getenv("PGDATABASE", "analytics"),
            user=os.getenv("PGUSER", "dbt_user"),
            password=os.getenv("PGPASSWORD", "dbt_password"),
        )

    def _open_copy_connection(self) -> None:
        params = self._conn_params()
        self.log.info(
            "Opening COPY connection to Postgres at %(host)s:%(port)d db=%(dbname)s user=%(user)s",
            params,
        )
        self.conn = psycopg2.connect(**params)
        self.conn.autocommit = False
        self.cur = self.conn.cursor()
        self.log.info("COPY connection established")

    def _close_copy_connection(self) -> None:
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        self.log.info("Postgres connection closed")

    # ------------------ DDL in its own autocommit connection ------------------
    def create_tables_autocommit(self) -> None:
        params = self._conn_params()
        self.log.info(
            "Running DDL in autocommit connection: %(host)s:%(port)d db=%(dbname)s user=%(user)s",
            params,
        )
        ddl = """
        CREATE SCHEMA IF NOT EXISTS bronze;

        CREATE TABLE IF NOT EXISTS bronze.customers_raw (
          customer_id BIGINT,
          email TEXT,
          name TEXT,
          country TEXT,
          created_at TIMESTAMPTZ,
          status TEXT,
          _ingested_at TIMESTAMPTZ,
          _source TEXT,
          _batch_id UUID,
          _op TEXT,
          _ts TIMESTAMPTZ,
          _deleted BOOLEAN DEFAULT FALSE
        );

        CREATE TABLE IF NOT EXISTS bronze.products_raw (
          product_id BIGINT,
          title TEXT,
          category TEXT,
          brand TEXT,
          active BOOLEAN,
          created_at TIMESTAMPTZ,
          _ingested_at TIMESTAMPTZ,
          _source TEXT,
          _batch_id UUID,
          _op TEXT,
          _ts TIMESTAMPTZ,
          _deleted BOOLEAN DEFAULT FALSE
        );

        CREATE TABLE IF NOT EXISTS bronze.product_variants_raw (
          variant_id BIGINT,
          product_id BIGINT,
          sku TEXT,
          barcode TEXT,
          price NUMERIC(12,2),
          cost NUMERIC(12,2),
          active BOOLEAN,
          _ingested_at TIMESTAMPTZ,
          _source TEXT,
          _batch_id UUID,
          _op TEXT,
          _ts TIMESTAMPTZ,
          _deleted BOOLEAN DEFAULT FALSE
        );

        CREATE TABLE IF NOT EXISTS bronze.orders_raw (
          order_id BIGINT,
          channel_id INT,
          customer_id INT,
          order_ts TIMESTAMPTZ,
          status TEXT,
          currency TEXT,
          total_amount NUMERIC(12,2),
          _ingested_at TIMESTAMPTZ,
          _source TEXT,
          _batch_id UUID,
          _op TEXT,
          _ts TIMESTAMPTZ,
          _deleted BOOLEAN DEFAULT FALSE
        );

        CREATE TABLE IF NOT EXISTS bronze.order_items_raw (
          order_item_id BIGINT,
          order_id BIGINT,
          product_id BIGINT,
          variant_id BIGINT,
          qty INT,
          unit_price NUMERIC(12,2),
          discount NUMERIC(12,2),
          tax NUMERIC(12,2),
          line_amount NUMERIC(12,2),
          created_at TIMESTAMPTZ,
          _ingested_at TIMESTAMPTZ,
          _source TEXT,
          _batch_id UUID,
          _op TEXT,
          _ts TIMESTAMPTZ,
          _deleted BOOLEAN DEFAULT FALSE
        );

        CREATE TABLE IF NOT EXISTS bronze.payments_raw (
          payment_id BIGINT,
          order_id BIGINT,
          method TEXT,
          amount NUMERIC(12,2),
          status TEXT,
          paid_ts TIMESTAMPTZ,
          _ingested_at TIMESTAMPTZ,
          _source TEXT,
          _batch_id UUID,
          _op TEXT,
          _ts TIMESTAMPTZ,
          _deleted BOOLEAN DEFAULT FALSE
        );

        CREATE TABLE IF NOT EXISTS bronze.shipments_raw (
          shipment_id BIGINT,
          order_id BIGINT,
          carrier TEXT,
          service TEXT,
          tracking_no TEXT,
          shipped_ts TIMESTAMPTZ,
          status TEXT,
          _ingested_at TIMESTAMPTZ,
          _source TEXT,
          _batch_id UUID,
          _op TEXT,
          _ts TIMESTAMPTZ,
          _deleted BOOLEAN DEFAULT FALSE
        );
        """

        conn = psycopg2.connect(**params)
        try:
            conn.autocommit = True
            cur = conn.cursor()
            self.log.info("Executing DDL for bronze.* tables...")
            cur.execute(ddl)
            cur.execute("SELECT to_regclass('bronze.customers_raw');")
            exists = cur.fetchone()[0]
            self.log.info("Post-DDL check – bronze.customers_raw = %s", exists)
            cur.close()
        
        finally:
                conn.close()
                self.log.info("DDL autocommit connection closed")

    # ------------------ Orchestration ------------------
    def run(self) -> None:
        try:
             self.create_tables_autocommit()
             self.log.info("Bronze seeding completed successfully.")

        except Exception as e:
             self.log.exception("Error seeding bronze data, rolling back.")
             self.conn.rollback()
             raise
        finally:
             self._close_copy_connection()

# Airflow DAG definition
with DAG(
    dag_id="dag_create_table_oltp",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "seed", "copy_from_stdin"],
) as dag:

    start = EmptyOperator(task_id="start")

    def create_bronze_callable(**kwargs):
        seeder = BronzeSeeder(rows_per_table=DEFAULT_ROWS_PER_TABLE)
        seeder.run()

    create_bronze = PythonOperator(
        task_id="create_bronze_tables",
        python_callable=create_bronze_callable,
    )

    end = EmptyOperator(task_id="end")

    start >> create_bronze >> end