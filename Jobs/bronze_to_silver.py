from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, year, month, dayofmonth

BASE_DIR = Path(__file__).resolve().parents[1]

bronze_path = BASE_DIR / "Data" / "bronze" / "transactions"
silver_path = BASE_DIR / "Data" / "silver" / "transactions"

spark = SparkSession.builder \
    .appName("Bronze to Silver Transactions") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(str(bronze_path))

silver_df = df \
    .dropDuplicates(["transaction_id"]) \
    .filter(col("transaction_id").isNotNull()) \
    .filter(col("timestamp").isNotNull()) \
    .filter(col("amount").isNotNull()) \
    .filter(col("amount") > 0) \
    .withColumn(
        "fraud_type",
        when(col("fraud_type").isNull(), "not_fraud").otherwise(col("fraud_type"))
    ) \
    .withColumn("transaction_date", to_date(col("timestamp"))) \
    .withColumn("transaction_year", year(col("timestamp"))) \
    .withColumn("transaction_month", month(col("timestamp"))) \
    .withColumn("transaction_day", dayofmonth(col("timestamp")))

silver_df.coalesce(4) \
    .write \
    .mode("overwrite") \
    .parquet(str(silver_path))

print("Silver layer yozildi:")
print(silver_path)
print("Rows count:", silver_df.count())
print("Columns count:", len(silver_df.columns))

spark.stop()