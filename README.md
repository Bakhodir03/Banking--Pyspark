# Banking Fraud Detection PySpark Project

This project processes a financial fraud detection dataset using PySpark and Medallion Architecture.

The main goal of this project is to build a data engineering pipeline for banking transaction data and prepare analytical outputs for fraud detection reporting.

## Architecture

```text
Raw CSV → Bronze Layer → Silver Layer → Gold Layer → CSV Export
```

## Project Structure

```text
pbanking-pyspark/
│
├── Data/
│   ├── financial_fraud_detection_dataset.csv
│   ├── bronze/
│   │   └── transactions/
│   ├── silver/
│   │   └── transactions/
│   ├── gold/
│   │   ├── transaction_type_summary/
│   │   ├── fraud_summary/
│   │   ├── monthly_summary/
│   │   ├── location_fraud_summary/
│   │   ├── payment_channel_fraud_summary/
│   │   ├── device_fraud_summary/
│   │   ├── merchant_category_fraud_summary/
│   │   └── high_risk_transactions/
│   └── exports/
│       ├── transaction_type_summary/
│       ├── fraud_summary/
│       ├── monthly_summary/
│       ├── location_fraud_summary/
│       ├── payment_channel_fraud_summary/
│       ├── device_fraud_summary/
│       ├── merchant_category_fraud_summary/
│       └── high_risk_transactions/
│
├── Jobs/
│   ├── raw_to_bronze.py
│   ├── check_bronze.py
│   ├── bronze_to_silver.py
│   ├── check_silver.py
│   ├── silver_to_gold.py
│   ├── check_gold.py
│   ├── advanced_fraud_analysis.py
│   └── export_gold_to_csv.py
│
└── README.md
```

## Dataset

The project uses the following source dataset:

```text
Data/financial_fraud_detection_dataset.csv
```

The dataset contains banking transaction records with fields such as:

- transaction_id
- timestamp
- sender_account
- receiver_account
- amount
- transaction_type
- merchant_category
- location
- device_used
- is_fraud
- fraud_type
- payment_channel
- ip_address
- device_hash

The dataset contains:

```text
Rows count: 5,000,000
Columns count: 20
```

## Bronze Layer

The Bronze layer reads the raw CSV file and saves it as Parquet.

Input:

```text
Data/financial_fraud_detection_dataset.csv
```

Output:

```text
Data/bronze/transactions
```

Additional columns added in Bronze layer:

- ingestion_time
- source_file

Purpose of Bronze layer:

- keep the raw data in a structured format
- convert CSV to Parquet
- add metadata about data ingestion
- preserve source file information

## Silver Layer

The Silver layer cleans and prepares the data for analysis.

Input:

```text
Data/bronze/transactions
```

Output:

```text
Data/silver/transactions
```

Main transformations:

- removes duplicate transaction_id
- filters null transaction_id
- filters null timestamp
- filters null amount
- keeps only amount greater than 0
- fills null fraud_type values with not_fraud
- adds transaction_date
- adds transaction_year
- adds transaction_month
- adds transaction_day

Silver layer result:

```text
Rows count: 5,000,000
Columns count: 24
```

## Gold Layer

The Gold layer creates analytical summary tables from the cleaned Silver data.

Input:

```text
Data/silver/transactions
```

Output:

```text
Data/gold
```

Generated Gold tables:

- transaction_type_summary
- fraud_summary
- monthly_summary
- location_fraud_summary
- payment_channel_fraud_summary
- device_fraud_summary
- merchant_category_fraud_summary
- high_risk_transactions

## Gold Analysis Results

### Transaction Type Summary

This table shows transaction count, total amount, and average amount by transaction type.

Generated table:

```text
Data/gold/transaction_type_summary
```

Example result:

```text
payment     → 1,250,438 transactions
transfer    → 1,250,334 transactions
deposit     → 1,250,593 transactions
withdrawal  → 1,248,635 transactions
```

### Fraud Summary

This table shows fraud and non-fraud transaction statistics.

Generated table:

```text
Data/gold/fraud_summary
```

Example result:

```text
Fraud transactions     → 179,553
Non-fraud transactions → 4,820,447
```

### Monthly Summary

This table shows monthly transaction count, total amount, and average amount.

Generated table:

```text
Data/gold/monthly_summary
```

Example result:

```text
2023-01 → 418,302 transactions
2023-02 → 382,551 transactions
2023-03 → 425,454 transactions
```

### Location Fraud Summary

This table shows fraud transaction count and fraud amount by location.

Generated table:

```text
Data/gold/location_fraud_summary
```

Example result:

```text
Toronto   → 22,501 fraud transactions
London    → 22,478 fraud transactions
Singapore → 22,461 fraud transactions
New York  → 22,460 fraud transactions
```

## Advanced Fraud Analysis

The project also includes additional fraud risk analysis.

Generated advanced analysis tables:

- payment_channel_fraud_summary
- device_fraud_summary
- merchant_category_fraud_summary
- high_risk_transactions

### Payment Channel Fraud Summary

This table calculates fraud rate by payment channel.

Example result:

```text
wire_transfer → 3.60%
card          → 3.59%
UPI           → 3.59%
ACH           → 3.58%
```

### Device Fraud Summary

This table calculates fraud rate by device type.

Example result:

```text
atm    → 3.62%
pos    → 3.59%
web    → 3.58%
mobile → 3.57%
```

### Merchant Category Fraud Summary

This table calculates fraud rate by merchant category.

Example result:

```text
entertainment → 3.61%
other         → 3.61%
travel        → 3.60%
grocery       → 3.60%
```

### High Risk Transactions

This table contains transactions selected using stronger fraud risk rules.

High risk conditions:

- confirmed fraud transaction
- high velocity score with geo anomaly
- high spending deviation with large amount
- high velocity score with large amount

Final high risk transaction count:

```text
391,520
```

Generated table:

```text
Data/gold/high_risk_transactions
```

## CSV Export

Final Gold tables are exported to CSV format.

Output folder:

```text
Data/exports
```

Exported CSV folders:

- transaction_type_summary
- fraud_summary
- monthly_summary
- location_fraud_summary
- payment_channel_fraud_summary
- device_fraud_summary
- merchant_category_fraud_summary
- high_risk_transactions

Each folder contains a CSV file in Spark output format:

```text
part-00000-....csv
```

## Technologies Used

- Python
- PySpark
- Spark SQL
- Parquet
- CSV
- Windows Hadoop winutils
- PyCharm

## How to Run

Run the scripts in this order:

````markdown
## How to Run

To run the full pipeline, execute this command from the project root folder:

```text
python run_pipeline.py


## Pipeline Steps

### 1. Raw to Bronze
````markdown
## How to Run

To run the full pipeline, execute this command from the project root folder:

```text
python run_pipeline.py
Script:

```text
Jobs/raw_to_bronze.py
```

This script reads the raw CSV dataset and writes it to Bronze layer as Parquet.

### 2. Check Bronze

Script:

```text
Jobs/check_bronze.py
```

This script validates the Bronze layer data.

### 3. Bronze to Silver

Script:

```text
Jobs/bronze_to_silver.py
```

This script cleans the Bronze data and writes the result to Silver layer.

### 4. Check Silver

Script:

```text
Jobs/check_silver.py
```

This script validates the Silver layer data.

### 5. Silver to Gold

Script:

```text
Jobs/silver_to_gold.py
```

This script creates main analytical summary tables.

### 6. Check Gold

Script:

```text
Jobs/check_gold.py
```

This script validates the Gold layer tables.

### 7. Advanced Fraud Analysis

Script:

```text
Jobs/advanced_fraud_analysis.py
```

This script creates additional fraud risk analysis tables.

### 8. Export Gold to CSV

Script:

```text
Jobs/export_gold_to_csv.py
```

This script exports Gold tables to CSV format.

## Final Result

The project successfully processes:

```text
5,000,000 transaction records
```

Final outputs include:

- Bronze Parquet dataset
- Silver cleaned Parquet dataset
- Gold analytical Parquet tables
- CSV exports for reporting
- advanced fraud risk analysis tables

## Project Purpose

This project demonstrates a real-world data engineering workflow using PySpark.

It covers:

- large-scale data processing
- medallion architecture
- data cleaning
- data transformation
- fraud analytics
- risk-based transaction filtering
- analytical dataset preparation
- export for reporting tools such as Excel or Power BI