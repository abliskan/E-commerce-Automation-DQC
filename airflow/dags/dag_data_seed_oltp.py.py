from __future__ import annotations

import csv
import io
import os
import random
import uuid
import pandas as pd
import psycopg2
from datetime import datetime, timedelta as td
from datetime import datetime as dt

from dotenv import load_dotenv
from faker import Faker
from faker_commerce import Provider

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.log.logging_mixin import LoggingMixin

# Global constants
BRONZE_TABLES = [
    "bronze.customers_raw",
    "bronze.products_raw",
    "bronze.product_variants_raw",
    "bronze.orders_raw",
    "bronze.order_items_raw",
    "bronze.payments_raw",
    "bronze.shipments_raw",
]

DEFAULT_ROWS_PER_TABLE = 20_000
DEFAULT_CHUNK_SIZE = 5_000

# DAG definition
DEFAULT_ARGS = {
    "owner": "DE-operationals",
    "depends_on_past": False,
    "retries": 0,
}

# BronzeSeeder using pandas + SQLAlchemy
class BronzeSeeder(LoggingMixin):
    def __init__(
        self,
        rows_per_table: int = DEFAULT_ROWS_PER_TABLE,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        super().__init__()
        load_dotenv()

        self.rows_per_table = rows_per_table
        self.chunk_size = chunk_size

        self.fake = Faker()
        self.fake.add_provider(Provider)

        self.conn: psycopg2.extensions.connection | None = None
        self.cur: psycopg2.extensions.cursor | None = None
        self._open_connection()

    def _conn_params(self) -> dict:
        return dict(
            host=os.getenv("PGHOST"),
            port=int(os.getenv("PGPORT")),
            dbname=os.getenv("PGDATABASE"),
            user=os.getenv("PGUSER"),
            password=os.getenv("PGPASSWORD"),
        )

    def _open_connection(self) -> None:
        params = self._conn_params()
        self.log.info("Opening seed connection → %s", params)
        self.conn = psycopg2.connect(**params)
        self.conn.autocommit = False
        self.cur = self.conn.cursor()

        self.cur.execute("SELECT current_database(), current_schema();")
        db, schema = self.cur.fetchone()
        self.log.info("Seed connection using database=%s schema=%s", db, schema)

    def _close_connection(self) -> None:
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        self.log.info("Seed connection closed")

    # Helpers
    def _rand_ts(self) -> datetime:
        base = datetime(2024, 1, 1)
        return base + td(
            days=random.randint(0, 90),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
        )

    def _audit(self) -> dict:
        now = datetime.utcnow()
        return {
            "_ingested_at": now,
            "_source": "faker_seed_copy_csv",
            "_batch_id": uuid.uuid4(),
            "_op": "I",
            "_ts": now,
            "_deleted": False,
        }

    # Schema checks
    def check_tables_exist(self) -> None:
        missing: list[str] = []
        for tbl in BRONZE_TABLES:
            self.cur.execute("SELECT to_regclass(%s);", (tbl,))
            exists = self.cur.fetchone()[0]
            self.log.info("Table check %s → %s", tbl, exists)
            if exists is None:
                missing.append(tbl)

        if missing:
            raise RuntimeError(
                f"Missing bronze tables: {', '.join(missing)}. "
                "Run create_bronze_tables DAG first."
            )

    # COPY CSV loader
    def _copy_table(self, table: str, columns: list[str], row_generator) -> None:
        self.cur.execute("SELECT to_regclass(%s);", (table,))
        if self.cur.fetchone()[0] is None:
            raise RuntimeError(f"Table {table} does not exist in this connection")

        total = self.rows_per_table
        chunk = self.chunk_size
        self.log.info("Starting COPY CSV into %s (rows=%d, chunk=%d)", table, total, chunk)

        for start in range(0, total, chunk):
            count = min(chunk, total - start)

            rows = [row_generator() for _ in range(count)]
            df = pd.DataFrame(rows, columns=columns)

            buf = io.StringIO()
            df.to_csv(
                buf,
                index=False,
                header=False,
                quoting=csv.QUOTE_MINIMAL,
                na_rep="",  # empty field -> NULL
            )
            buf.seek(0)

            col_list = ", ".join(columns)
            sql = f"COPY {table} ({col_list}) FROM STDIN WITH (FORMAT csv, NULL '')"
            self.cur.copy_expert(sql, buf)

            self.log.info("%s: inserted %d/%d rows", table, start + count, total)
    
    # Row generators – all INTEGER-safe
    def gen_customer_row(self) -> list:
        audit = self._audit()
        return [
            random.randint(1, 50_000),                # customer_id
            self.fake.email(),                        # email
            self.fake.name(),                         # name
            random.choice(["SG", "MY", "ID", "TH"]),  # country
            self._rand_ts(),                          # created_at
            random.choice(["active", "inactive"]),    # status
            audit["_ingested_at"],
            audit["_source"],
            audit["_batch_id"],
            audit["_op"],
            audit["_ts"],
            audit["_deleted"],
        ]

    def gen_product_row(self) -> list:
        audit = self._audit()
        return [
            random.randint(1_000, 30_000),                                     # product_id
            self.fake.ecommerce_name(),                                        # title
            random.choice(["Electronics", "Accessories", "Audio", "Storage"]), # category
            self.fake.company(),                                               # brand
            True,                                                              # active
            self._rand_ts(),                                                   # created_at
            audit["_ingested_at"],
            audit["_source"],
            audit["_batch_id"],
            audit["_op"],
            audit["_ts"],
            audit["_deleted"],
        ]

    def gen_variant_row(self) -> list:
        """
        Variant rows with INTEGER price and cost
        """
        audit = self._audit()
        price = random.randint(5, 500)       # int
        cost = random.randint(1, price)      # int <= price

        return [
            random.randint(2_000, 80_000),         # variant_id
            random.randint(1_000, 30_000),         # product_id
            f"SKU-{uuid.uuid4().hex[:8].upper()}", # sku
            self.fake.ean13(),                     # barcode
            price,                                 # price (int)
            cost,                                  # cost (int)
            True,                                  # active
            audit["_ingested_at"],
            audit["_source"],
            audit["_batch_id"],
            audit["_op"],
            audit["_ts"],
            audit["_deleted"],
        ]

    def gen_order_row(self) -> list:
        """
        Orders with INTEGER total_amount to match INTEGER/BIGINT DDL.
        """
        audit = self._audit()
        total_amount = random.randint(10, 100_000)            # int amount

        return [
            random.randint(1, 50_000),                        # order_id
            random.randint(1, 8),                             # channel_id
            random.randint(1, 50_000),                        # customer_id
            self._rand_ts(),                                  # order_ts
            random.choice(["paid", "pending", "cancelled"]),  # status
            random.choice(["SGD", "MYR", "IDR", "THB"]),      # currency
            total_amount,                                     # total_amount (INT)
            audit["_ingested_at"],
            audit["_source"],
            audit["_batch_id"],
            audit["_op"],
            audit["_ts"],
            audit["_deleted"],
        ]

    def gen_order_item_row(self) -> list:
        audit = self._audit()
        qty = random.randint(1, 5)
        unit_price = random.randint(5, 500)
        discount = random.randint(0, 5)
        tax = int(unit_price * 0.07)  # integer-ish tax
        line_amount = qty * (unit_price - discount) + tax
        product_id = random.randint(1, 30_000)
        variant_id = (product_id * 10) + random.randint(1, 3)
        
        return [
            random.randint(1, 1_000_000),              # order_item_id
            random.randint(1, 50_000),                 # order_id
            product_id,                                # product_id
            variant_id,                                # variant_id
            qty,                                       # qty
            unit_price,                                # unit_price
            discount,                                  # discount
            tax,                                       # tax
            line_amount,                               # line_amount
            self._rand_ts(),                           # create_at
            audit["_ingested_at"],
            audit["_source"],
            audit["_batch_id"],
            audit["_op"],
            audit["_ts"],
            audit["_deleted"],
        ]

    def gen_payment_row(self) -> list:
        audit = self._audit()
        amount = random.randint(10, 100_000)  # int amount

        return [
            random.randint(1, 1_000_000),                                   # payment_id
            random.randint(1, 50_000),                                      # order_id
            random.choice(["CreditCard", "EWallet", "PayNow", "PayLater"]), # method
            amount,                                                         # amount
            random.choice(["paid", "pending", "failed", "refunded"]),       # status
            self._rand_ts(),                                                # paid_ts
            audit["_ingested_at"],
            audit["_source"],
            audit["_batch_id"],
            audit["_op"],
            audit["_ts"],
            audit["_deleted"],
        ]

    def gen_shipment_row(self) -> list:
        audit = self._audit()
        return [
            random.randint(1, 1_000_000),              # shipment_id
            random.randint(1, 50_000),                 # order_id
            self.fake.company(),                       # carrier
            random.choice(["Standard", "Express"]),    # service
            f"TRK-{uuid.uuid4().hex[:10].upper()}",    # tracking_no
            self._rand_ts(),                           # shipped_ts
            random.choice(["processing", "shipped"]),  # status
            audit["_ingested_at"],
            audit["_source"],
            audit["_batch_id"],
            audit["_op"],
            audit["_ts"],
            audit["_deleted"],
        ]

    # Main orchestrator
    def run(self) -> None:
        try:
            # 1. Ensure tables exist
            self.check_tables_exist()

            # Extra explicit check
            self.cur.execute("SELECT to_regclass('bronze.customers_raw');")
            exists = self.cur.fetchone()[0]
            self.log.info("Pre-COPY to_regclass('bronze.customers_raw') = %s", exists)

            # 2. Seed all tables
            self._copy_table(
                "bronze.customers_raw",
                [
                    "customer_id",
                    "email",
                    "name",
                    "country",
                    "created_at",
                    "status",
                    "_ingested_at",
                    "_source",
                    "_batch_id",
                    "_op",
                    "_ts",
                    "_deleted",
                ],
                self.gen_customer_row,
            )

            self._copy_table(
                "bronze.products_raw",
                [
                    "product_id",
                    "title",
                    "category",
                    "brand",
                    "active",
                    "created_at",
                    "_ingested_at",
                    "_source",
                    "_batch_id",
                    "_op",
                    "_ts",
                    "_deleted",
                ],
                self.gen_product_row,
            )
            
            self._copy_table(
                "bronze.product_variants_raw",
                [
                    "variant_id",
                    "product_id",
                    "sku",
                    "barcode",
                    "price",
                    "cost",
                    "active",
                    "_ingested_at",
                    "_source",
                    "_batch_id",
                    "_op",
                    "_ts",
                    "_deleted",
                ],
                self.gen_variant_row,
            )

            self._copy_table(
                "bronze.orders_raw",
                [
                    "order_id",
                    "channel_id",
                    "customer_id",
                    "order_ts",
                    "status",
                    "currency",
                    "total_amount",
                    "_ingested_at",
                    "_source",
                    "_batch_id",
                    "_op",
                    "_ts",
                    "_deleted",
                ],
                self.gen_order_row,
            )

            self._copy_table(
                "bronze.order_items_raw",
                [
                    "order_item_id",
                    "order_id",
                    "product_id",
                    "variant_id",
                    "qty",
                    "unit_price",
                    "discount",
                    "tax",
                    "line_amount",
                    "created_at",
                    "_ingested_at",
                    "_source",
                    "_batch_id",
                    "_op",
                    "_ts",
                    "_deleted",
                ],
                self.gen_order_item_row,
            )

            self._copy_table(
                "bronze.payments_raw",
                [
                    "payment_id",
                    "order_id",
                    "method",
                    "amount",
                    "status",
                    "paid_ts",
                    "_ingested_at",
                    "_source",
                    "_batch_id",
                    "_op",
                    "_ts",
                    "_deleted",
                ],
                self.gen_payment_row,
            )

            self._copy_table(
                "bronze.shipments_raw",
                [
                    "shipment_id",
                    "order_id",
                    "carrier",
                    "service",
                    "tracking_no",
                    "shipped_ts",
                    "status",
                    "_ingested_at",
                    "_source",
                    "_batch_id",
                    "_op",
                    "_ts",
                    "_deleted",
                ],
                self.gen_shipment_row,
            )

            self.conn.commit()
            self.log.info("✅ Bronze seeding completed successfully (integer-safe).")

        except Exception:
            self.log.exception("❌ Error while seeding bronze data, rolling back.")
            if self.conn:
                self.conn.rollback()
            raise
        finally:
            self._close_connection()


# Airflow DAG definition
def check_bronze_tables_callable(**context):
    seeder = BronzeSeeder(rows_per_table=1, chunk_size=1)  # tiny, just for check
    try:
        seeder.check_tables_exist()
    finally:
        seeder._close_connection()


def seed_bronze_callable(**context):
    seeder = BronzeSeeder()
    seeder.run()


with DAG(
    dag_id="seed_bronze_data",
    start_date=dt(2023, 1, 1),
    schedule=None,
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "seed", "ingestion"],
) as dag:
    start = EmptyOperator(task_id="start")

    check_bronze = PythonOperator(
        task_id="check_bronze_tables_exist",
        python_callable=check_bronze_tables_callable,
    )

    seed_bronze = PythonOperator(
        task_id="seed_bronze_copy_from_stdin",  # name kept to match your UI
        python_callable=seed_bronze_callable,
    )

    end = EmptyOperator(task_id="end")

    start >> check_bronze >> seed_bronze >> end