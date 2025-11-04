# E-commerce Automation Data Quality Check
## Introduction
E-commerce companies lose millions due to overselling (selling out-of-stock items), underselling (showing items as out-of-stock when available), and order fulfillment errors. When inventory systems fail to accurately track stock levels across multiple warehouses and sales channels in real-time, customers experience order cancellations, delayed shipments, and unfulfilled promises that damage brand reputation and customer loyalty. Without robust automated data quality checks monitoring inventory transactions, stock movements, and order fulfillment processes, e-commerce businesses cannot maintain the operational excellence required to compete where customer expectations for accuracy and speed continue to rise.

## Project Structure
```
|-> E-commerce-Automation-DQC/
|--> dags/
|---> dag_etl_fakestore.py
|
|--> include/
|---> dbt
|----> models/
|-----> staging/
|-----> core/
|-----> marts/
|----> seeds/
|----> macros/
|----> profiles/
|-----> profiles.yml
|---> soda/
|----> warehouse.yml
|----> checks/
|-----> staging_orders.yml
|-----> core_fact_orders.yml
|-----> marts_finance.yml
|
|--> logs/
|--> plugins/
|
|--> asset/
|---> DE-DQC-Architecture.png
|
|--> requirements.txt
|--> .env
|--> Dockerfile
|--> docker-compose.yml
|--> .gitignore
|--> .dockerignore
```

## Data pipeline architecture (On-premise)
!['Data pipeline'](/asset/DE-DQC-Architecture.ppg)
