# E-commerce Automation Data Quality Check
## Introduction
E-commerce companies lose millions due to overselling (selling out-of-stock items), underselling (showing items as out-of-stock when available), and order fulfillment errors. When inventory systems fail to accurately track stock levels across multiple warehouses and sales channels in real-time, customers experience order cancellations, delayed shipments, and unfulfilled promises that damage brand reputation and customer loyalty. Without robust automated data quality checks monitoring inventory transactions, stock movements, and order fulfillment processes, e-commerce businesses cannot maintain the operational excellence required to compete where customer expectations for accuracy and speed continue to rise.

## Project Structure
```
ecommerce-dq-platform/
├─ airflow/
│  ├─ dags/
│  │  ├─ dag_seed_oltp.py
│  │  ├─ dag_extract_staging.py
│  │  ├─ dag_transform_core.py
│  │  ├─ dag_build_marts.py
│  │  └─ dag_refresh_metabase.py
├─ include/
│  ├─ soda/
│  │  ├─ warehouse.yml
│  │  ├─ models/
│  │  │  ├─ checks_staging.yml
│  │  │  ├─ checks_core.yml
│  │  │  └─ checks_marts.yml
│  │  │
│  ├─ dbt/
│  │  ├─ models/
│  │  │  ├─ staging/
│  │  │  ├─ core/
│  │  │  └─ marts/
│  │  ├─ seeds/
│  │  ├─ macros/
│  │  ├─ snapshots/
│  │  ├─ dbt_project.yml
│  │  └─ profiles.yaml
├─ scripts/
│  └─ seed_ecommerce_data.py
├─ dashboards/
│  └─ metabase_export.json
├─ .gitignore
├─ .dockerignore
├─ .env
├─ requirements.txt
├─ Dockerfile
├─ docker-compose.yml
└─ README.md
```

## Data pipeline architecture (On-premise)
!['Data pipeline'](/asset/DE-DQC-Architecture.ppg)
