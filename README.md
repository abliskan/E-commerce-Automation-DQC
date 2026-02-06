# E-commerce Automation Data Quality Check
## Introduction
E-commerce companies struggle with inconsistent reporting where revenue, order counts, and performance metrics differ across teams because each department pulls data from different systems or applies its own transformation logic. 
This results in contradicting figures at executive meetings, delays in decision-making, and a lack of faith in analytics results.
Furthermore, operational reports frequently contain missing, duplicated, or incomplete order records as a result of system faults, integration issues, or manual data entry problems, which can directly affect financial and operational performance assessments.
<br>
Logistics performance is also challenging to track precisely because shipping data is typically scattered among systems, making it harder to discover delays, failed deliveries, or bottlenecks in the fulfillment process.
Without a governed data platform that standardizes, validates, and models data consistently, the organization operates reactively, spending more time reconciling numbers than generating insights. 
This project tackles these issues by establishing a dependable data pipeline with built-in data quality controls and structured analytics layers, guaranteeing that business choices are based on accurate, consistent, and trustworthy information.

# Business Problem
Organizations often suffer from:
- 15–25% discrepancy in revenue numbers across teams
- 5–10% data errors caused by missing or duplicate records
- Delayed shipment visibility leading to customer dissatisfaction
- 30–40% analyst time spent cleaning data instead of analyzing

# Data Undestanding
The data source consists of 7 tables from operational data store (ODS).
![Data Dictionary](asset/erd-database-design.png)

# Project Structure
```text
E-commerce-Automation-DQC/
├─ airflow/
│  ├─ dags/
│  │  ├─ dag_create_table_oltp.py
│  │  ├─ dag_data_seed_oltp.py
│  │  ├─ dag_ingest_scan_dq_staging.py
│  │  ├─ dag_modeling_transform_dq_dwh.py
│  │  └─ dag_analytics_scan_dq_mart.py
│  │  │
├─ include/
│  ├─ soda/
│  │  ├─ soda_config.yml
│  │  ├─ staging/
│  │  │  └─ staging_checks.yml
│  │  ├─ dwh/
│  │  │  └─ dwh_checks.yml
│  │  ├─ mart/
│  │  │  ├─ mart_customer_metrics.yml
│  │  │  ├─ mart_daily_revenue.yml
│  │  │  ├─ mart_orders.yml
│  │  │  ├─ mart_product_performance.yml
│  │  │  └─ mart_shipments.yml
│  │  │
│  ├─ dbt/
│  │  ├─ models/ 
│  │  │  │  ├─ staging/
│  │  │  │  │  ├─ staging_schema.yml
│  │  │  │  │  ├─ stg_customers.sql
│  │  │  │  │  ├─ stg_order_items.sql
│  │  │  │  │  ├─ stg_orders.sql
│  │  │  │  │  ├─ stg_payments.sql
│  │  │  │  │  ├─ stg_product_variants.sql
│  │  │  │  │  ├─ stg_products.sql
│  │  │  │  │  └─ stg_shipments.sql
│  │  │  │  ├─ core/
│  │  │  │  │  ├─ dimension
│  │  │  │  │  │  ├─ dim_customers.sql
│  │  │  │  │  │  ├─ dim_dates.sql
│  │  │  │  │  │  ├─ dim_product_variants.sql
│  │  │  │  │  │  └─ dim_products.sql
│  │  │  │  │  └─ fact
│  │  │  │  │  │  ├─ fct_order_items.sql
│  │  │  │  │  │  ├─ fct_orders.sql
│  │  │  │  │  │  ├─ fct_payments.sql
│  │  │  │  │  │  └─ fct_shipments.sql
│  │  │  │  ├─ marts/
│  │  │  │  │  ├─ customer_support
│  │  │  │  │  │  └─ mart_customer_metrics.sql
│  │  │  │  │  ├─ finance
│  │  │  │  │  │  └─ mart_daily_revenue.sql
│  │  │  │  │  ├─ logistic
│  │  │  │  │  │  └─ mart_shipments.sql
│  │  │  │  │  ├─ product
│  │  │  │  │  │  └─ mart_product_performance.sql
│  │  │  │  │  └─ sales
│  │  │  │  │  │  └─ mart_orders.sql
│  │  │  │  └─ quarantine/
│  │  │  │  │  ├─ dwh/
│  │  │  │  │  │  ├─ quarantine_dim_customers.sql
│  │  │  │  │  │  ├─ quarantine_dim_dates.sql
│  │  │  │  │  │  ├─ quarantine_dim_product_variants.sql
│  │  │  │  │  │  ├─ quarantine_dim_products.sql
│  │  │  │  │  │  ├─ quarantine_fct_order_items.sql
│  │  │  │  │  │  ├─ quarantine_fct_orders.sql
│  │  │  │  │  │  ├─ quarantine_fct_payments.sql
│  │  │  │  │  │  └─ quarantine_fct_shipments.sql
│  │  │  │  │  ├─ staging/
│  │  │  │  │  │  ├─ quarantine_stg_customers.sql
│  │  │  │  │  │  ├─ quarantine_stg_order_items.sql
│  │  │  │  │  │  ├─ quarantine_stg_orders.sql
│  │  │  │  │  │  ├─ quarantine_stg_payments.sql
│  │  │  │  │  │  ├─ quarantine_stg_product_variants.sql
│  │  │  │  │  │  ├─ quarantine_stg_products.sql
│  │  │  │  │  │  └─ quarantine_stg_shipments.sql
│  │  ├─ profiles/
│  │  │  └─ profiles.yml 
│  │  ├─ seeds/
│  │  ├─ macros/
│  │  ├─ snapshots/
│  │  ├─ dbt_project.yml
│  │  ├─ packages.yml 
│  │  └─ profiles.yaml
│  │
├─ dashboards/
│  └─ metabase_export.json
│  │
├─ .gitignore
├─ .dockerignore
├─ .env-example
├─ .pre-commit-config.yaml
├─ .python-version
├─ docker-compose.yml
├─ Dockerfile
├─ requirements.txt
├─ pyproject.toml
├─ uv.lock
└─ README.md
```

# Technologies Used
- Python
- SQL
- Dbt
- Apache airflow
- PostgreSQL + DBeaver
- Soda core
- Docker & docker compose
- uv
- Slack webhooks

# Dataflow
```text
Here’s what happens operationally:
RAW ─────────► STAGING 
                  ↓
            DBT STAGING Run
                  ↓
            DBT Test STAGING
                  ↓
            Soda Staging Scan
                  ↓                                    
           ┌──── PASS ───────────────────────────────► DWH Core (dimensions & facts)
           │                                                      ↓
           │                                               DBT DWH (Dim + Fact) Run
           │                                                      ↓
           │                                               DBT Test DWH (Dim + Fact) 
           │                                                      ↓
           └──── QUARANTINE → Slack Alert → FAIL DAG       Soda DWH Core Scan
                                                                  ↓
                                                            ┌──── PASS ────────────────────────────────────► Mart
						                                    │                                                 ↓
						                                    │                                                DBT MART Run
                                                            │                                                 ↓
                                                            │                                                DBT Test MART
                                                            │                                                 ↓
                                                            └──── QUARANTINE → Slack Alert → FAIL DAG        Soda MART Core Scan
                                                                                                              ↓
                                                                                                       ┌──── PASS ────────────────────────────────────► BI / Dashboard Layer
                                                                                                       │  
                                                                                                       └──── FAIL ────►  ──────────────┐
														                                                                │ Soda Checks  │
																							                            │  (parallel)  │
														                                    						    └──────────────┘
                                                                                                                               │
                                                                                                                               └───→ Slack-Alert → FAIL DAG
```

# Architecture
## Alternative - Local
![Data pipeline](asset/Data-architecture-E-commerce-pipeline.png)

## Alternative - Cloud
(coming-soon)

# Pipeline Schedule
| Layer	       |  Schedule                 | Purpose               |
|--------------|---------------------------|-----------------------|
| **Staging**  | Every hour    	           | Operational freshness |
| **DWH**	   | Hourly after staging      | Trusted reporting     |
| **Mart**	   | Hourly after DWH	       | KPI refresh           |

# Data Quality Strategy

| Layer	       | Check Type	          | Failure Action                 |
|--------------|----------------------|--------------------------------|
| **Staging**  | Structural rules	  | Quarantine + Fail pipeline     |
| **DWH**	     | Business integrity | Quarantine + Fail pipeline     |
| **Mart**	   | KPI sanity checks	  | Fail pipeline                  |

# Key DQ KPIs
- DQ Pass Rate: Overall percentage of all Soda checks that pass across layers, serves as the single summary metric of data trustworthiness. [level: all layers]
- Data Freshness Lag (Hours): Measures the time difference between the current timestamp and the most recent record ingested, ensures data is updated within SLA (e.g., < 24 hours). [level: staging & mart]
- Completeness Ratio: Percentage of required fields populated across key tables (e.g., customer_id, order_id, payment_id); detects missing or null values. [level: staging & core/dwh]
- Uniqueness Ratio: Ratio of duplicate primary keys or rows found in critical tables to total records; indicates data duplication or faulty ingestion logic.  [level: staging]
- Business Rule Consistency: Proportion of transactions passing business logic checks (e.g., sum(payment.amount) ≈ sum(order_items.revenue)); ensures financial accuracy. [level: all layers]
- Schema Compliance: Percentage of datasets matching the expected schema (column names, data types, constraints); helps catch unexpected schema drifts. [level: staging]

# Business Impact
| Improvement	             | Expected Gain                    |
|--------------------------|----------------------------------|
| **Faster reporting**     | 50% reduction in reporting time  |
| **Data errors reduced**  | 70% fewer incidents              |
| **Decision speed**	     | 30% faster KPI availability      |
| **Analyst productivity** | +40% time on analysis            |

# How to Run This Project
## Alternative - Local
1. Clone this repo
``` bash
git clone <repo-url>
cd project
```

2. Create a virtual environment
```bash
python -m env e-commerce-env
```

3. Active the virtual environment
```bash
e-commerce-env\Scripts\activate
```

4. Install uv
```bash
pip install uv
```

5. Install dependencies
```bash
uv sync
```

6. Go to .env file and change all the content depending on your preference 

7. Prevent bad code before commit to repo
```bash
pre-commit install
pre-commit run --all-files
```

8. Go to subfolder to run the docker-compose <br>
8.1. (Optional) Before doing that, you first need to set up the permissions of two directories if you're use linux OS
```bash
cd docker-setup
sudo chmod a+rwx <project_directory>
```

8.2. Run the container
```bash
docker-compose up -d
```

8.3. Stop the container
```bash
docker-compose down -v
```

9. Accessing the database from DBeaver
<p align="center">
  <img src="asset\dbeaver-ss-connection.PNG">
</p>
Connection:
- Host=localhost <br>
- Database=<check_PGDATABASE_in_your_.env_file> <br>
- Port=<check_PGPORT_in_your_.env_file> <br>
- Click "Show all databases" checkbox <br>
- Username=<check_PGUSER_in_your_.env_file> <br>
- Password=<check_PGPASSWORD_in_your_.env_file> <br>
- (Optional) Click "Save password" checkbox <br>
- Click "Test Connection ..." button to verify db connection <br>
- Click "Finish" button <br>

10. Orchestrating with Airflow <br>
10.1. Access Airflow UI at -> http://localhost:8080
<p align="center">
  <img src="asset\airflow-main-dag.PNG">
</p>
10.2. Create table for raw data -> task_id: dag_create_table_oltp
<p align="center">
  <img src="asset\create_bronze_tables-graph-1.PNG">
</p>
10.3. Insert raw data -> task_id: seed_bronze_data
<p align="center">
  <img src="asset\seed_bronze_data-graph-1.PNG">
</p>
10.4. [Staging_layer] task_id: dag_ingest_scan_dq_staging, list of flows inside the dag: <br>
- use dbt to materialize staging models and extraction data from raw into staging <br>
- use dbt test to check and validate structure (include: not null, unique, relationships) <br>
- use soda to check and validates data behavior (include: volume, duplicates, invalid values, data freshness) <br>
- use dbt with quarantine to materialize quarantine models and isolate/moves bad rows into quarantine <br>
<p align="center">
  <img src="asset\extract_oltp_to_staging_with_quarantine-graph-1.PNG">
</p>
<br>
10.5. [core/dwh_layer] task_id: dag_modeling_transform_dq_dwh, list of flows inside the dag: <br>
- use dbt to materialize dimensional models and transform data from staging into dwh <br>
- use dbt test to check and validate structure (include: not null, unique, relationships) on dimension & fact <br>
- use soda to check and validates data behavior (include: volume, duplicates, invalid values, data freshness) on dimension & fact <br>
- use dbt with quarantine to materialize quarantine models and isolate/moves bad rows into quarantine <br>
<p align="center">
  <img src="asset\dwh_dimensional_modeling-graph-1.png">
</p>
<br>
10.6. [Mart_layer] task_id: dag_analytics_scan_dq_mart <br>
- use dbt to materialize mart models and load result of aggregate data from dim & fact table <br>
- use dbt test to check and validate structure (include: not null, unique, relationships) <br>
- use soda to check and validates data behavior (include: volume, duplicates, invalid values, data freshness) <br>
<p align="center">
  <img src="asset\mart_analytics-graph-1.png">
</p>

### Run dbt manually
Before run the the pipeline on Airflow UI, you can check the script work properly on docker container: <br>
11. Initializing the dbt project
```bash
dbt init <dbt_project_name>
```

12. Verify that your configuration has been set up correctly
```bash
dbt debug
```

13. Create staging schema directly from container CLI
```bash
cd <dbt_project_name>\models\staging
dbt run --select staging
```

13.1. Validates data models, sources, snapshots, and seeds
```bash
dbt test --select staging
```

14. Create dwh/core schema directly from container CLI
```bash
cd <dbt_project_name>\models\core
dbt run --select dwh
```

14.1. Validates data models, sources, snapshots, and seeds
```bash
dbt test --select dwh
```

15. Create mart schema directly from container CLI
```bash 
cd <dbt_project_name>\models\mart
dbt run --select mart
```

15.1. Validates data models, sources, snapshots, and seeds
```bash
dbt test --select mart
```

### Run soda manually
Before run the the pipeline on Airflow UI, you can check the script work properly on docker container: <br>
13.2. Scan data on staging phase
```bash
cd soda\staging
soda scan staging -d bronze_staging -c soda_config.yml checks/staging_checks.yml
```

14.2. Scan data on dwh/core phase
```bash
cd soda\dwh
soda scan staging -d dwh -c soda_config.yml dwh/dwh_checks.yml
```

15.2. Validates data models, sources, snapshots, and seeds
```bash <data mart on finance division>
cd soda\mart
soda scan staging -d mart_finance -c soda_config.yml mart/mart_daily_revenue.yml
```

## Alternative - Cloud
(Comming Soon)

# Production Deployment Checklist

# Data Visualization
1. Executive KPI Dashboard
<p align="center">
  <img src="asset\Executive-KPI-Dashboard.png">
</p>

2. Sales Performance Dashboard
<p align="center">
  <img src="asset\Sales Performance Analysis.png">
</p>

# Future enhancements
- Real-time streaming ingestion
- Data contracts per domain team
- Data catalog integration
- Separate Slack channels per layer
- ML feature store layer
- Implement DQ metrics dashboard using promotheus and grafana
