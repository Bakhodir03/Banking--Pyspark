from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, round

BASE_DIR = Path(__file__).resolve().parents[1]

silver_path = BASE_DIR / "Data" / "silver" / "transactions"
gold_path = BASE_DIR / "Data" / "gold"

spark = SparkSession.builder \
    .appName("Silver to Gold Transactions") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(str(silver_path))

# 1. Transaction type summary
transaction_type_summary = df.groupBy("transaction_type") \
    .agg(
        count("*").alias("transactions_count"),
        round(sum("amount"), 2).alias("total_amount"),
        round(avg("amount"), 2).alias("avg_amount")
    )

# 2. Fraud summary
fraud_summary = df.groupBy("is_fraud") \
    .agg(
        count("*").alias("transactions_count"),
        round(sum("amount"), 2).alias("total_amount"),
        round(avg("amount"), 2).alias("avg_amount")
    )

# 3. Monthly summary
monthly_summary = df.groupBy("transaction_year", "transaction_month") \
    .agg(
        count("*").alias("transactions_count"),
        round(sum("amount"), 2).alias("total_amount"),
        round(avg("amount"), 2).alias("avg_amount")
    ) \
    .orderBy("transaction_year", "transaction_month")

# 4. Location fraud summary
location_fraud_summary = df.filter(col("is_fraud") == True) \
    .groupBy("location") \
    .agg(
        count("*").alias("fraud_count"),
        round(sum("amount"), 2).alias("fraud_amount")
    ) \
    .orderBy(col("fraud_count").desc())

transaction_type_summary.coalesce(1).write.mode("overwrite").parquet(str(gold_path / "transaction_type_summary"))
fraud_summary.coalesce(1).write.mode("overwrite").parquet(str(gold_path / "fraud_summary"))
monthly_summary.coalesce(1).write.mode("overwrite").parquet(str(gold_path / "monthly_summary"))
location_fraud_summary.coalesce(1).write.mode("overwrite").parquet(str(gold_path / "location_fraud_summary"))

print("Gold layer yozildi:")
print(gold_path)

print("Transaction type summary:")
transaction_type_summary.show(truncate=False)

print("Fraud summary:")
fraud_summary.show(truncate=False)

print("Monthly summary:")
monthly_summary.show(12, truncate=False)

print("Location fraud summary:")
location_fraud_summary.show(10, truncate=False)

spark.stop()
