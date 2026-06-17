from pathlib import Path
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parents[1]

gold_path = BASE_DIR / "Data" / "gold"
export_path = BASE_DIR / "Data" / "exports"

spark = SparkSession.builder \
    .appName("Export Gold to CSV") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

tables = [
    "transaction_type_summary",
    "fraud_summary",
    "monthly_summary",
    "location_fraud_summary",
    "payment_channel_fraud_summary",
    "device_fraud_summary",
    "merchant_category_fraud_summary",
    "high_risk_transactions"
]

for table in tables:
    df = spark.read.parquet(str(gold_path / table))

    output_path = export_path / table

    df.coalesce(1) \
        .write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(str(output_path))

    print(f"Exported: {output_path}")

spark.stop()
