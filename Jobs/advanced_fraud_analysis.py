from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum, avg, round

BASE_DIR = Path(__file__).resolve().parents[1]

silver_path = BASE_DIR / "Data" / "silver" / "transactions"
gold_path = BASE_DIR / "Data" / "gold"

spark = SparkSession.builder \
    .appName("Advanced Fraud Analysis") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(str(silver_path))

# 1. Payment channel bo‘yicha fraud analysis
payment_channel_fraud_summary = df.groupBy("payment_channel") \
    .agg(
        count("*").alias("transactions_count"),
        sum(col("is_fraud").cast("int")).alias("fraud_count"),
        round(sum("amount"), 2).alias("total_amount"),
        round(avg("amount"), 2).alias("avg_amount"),
        round((sum(col("is_fraud").cast("int")) / count("*")) * 100, 2).alias("fraud_rate_percent")
    ) \
    .orderBy(col("fraud_rate_percent").desc())

# 2. Device bo‘yicha fraud analysis
device_fraud_summary = df.groupBy("device_used") \
    .agg(
        count("*").alias("transactions_count"),
        sum(col("is_fraud").cast("int")).alias("fraud_count"),
        round(sum("amount"), 2).alias("total_amount"),
        round(avg("amount"), 2).alias("avg_amount"),
        round((sum(col("is_fraud").cast("int")) / count("*")) * 100, 2).alias("fraud_rate_percent")
    ) \
    .orderBy(col("fraud_rate_percent").desc())

# 3. Merchant category bo‘yicha fraud analysis
merchant_category_fraud_summary = df.groupBy("merchant_category") \
    .agg(
        count("*").alias("transactions_count"),
        sum(col("is_fraud").cast("int")).alias("fraud_count"),
        round(sum("amount"), 2).alias("total_amount"),
        round(avg("amount"), 2).alias("avg_amount"),
        round((sum(col("is_fraud").cast("int")) / count("*")) * 100, 2).alias("fraud_rate_percent")
    ) \
    .orderBy(col("fraud_rate_percent").desc())

# 4. Eng xavfli transactionlar
high_risk_transactions = df.filter(
    (col("is_fraud") == True) |
    (
        (col("velocity_score") >= 18) &
        (col("geo_anomaly_score") >= 0.8)
    ) |
    (
        (col("spending_deviation_score") >= 2) &
        (col("amount") >= 1000)
    ) |
    (
        (col("velocity_score") >= 15) &
        (col("amount") >= 1500)
    )
).select(
    "transaction_id",
    "timestamp",
    "sender_account",
    "receiver_account",
    "amount",
    "transaction_type",
    "merchant_category",
    "location",
    "device_used",
    "payment_channel",
    "is_fraud",
    "fraud_type",
    "velocity_score",
    "geo_anomaly_score",
    "spending_deviation_score"
).orderBy(col("amount").desc())

payment_channel_fraud_summary.coalesce(1).write.mode("overwrite").parquet(
    str(gold_path / "payment_channel_fraud_summary")
)

device_fraud_summary.coalesce(1).write.mode("overwrite").parquet(
    str(gold_path / "device_fraud_summary")
)

merchant_category_fraud_summary.coalesce(1).write.mode("overwrite").parquet(
    str(gold_path / "merchant_category_fraud_summary")
)

high_risk_transactions.coalesce(1).write.mode("overwrite").parquet(
    str(gold_path / "high_risk_transactions")
)

print("Advanced fraud analysis yozildi:")
print(gold_path)

print("\nPayment channel fraud summary:")
payment_channel_fraud_summary.show(truncate=False)

print("\nDevice fraud summary:")
device_fraud_summary.show(truncate=False)

print("\nMerchant category fraud summary:")
merchant_category_fraud_summary.show(truncate=False)

print("\nHigh risk transactions:")
high_risk_transactions.show(20, truncate=False)

print("High risk transactions count:", high_risk_transactions.count())

spark.stop()