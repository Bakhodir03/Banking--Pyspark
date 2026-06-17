import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

os.environ["HADOOP_HOME"] = r"D:\hadoop"
os.environ["PATH"] = r"D:\hadoop\bin;" + os.environ["PATH"]

BASE_DIR = Path(__file__).resolve().parents[1]

raw_path = BASE_DIR / "Data" / "financial_fraud_detection_dataset.csv"
bronze_path = BASE_DIR / "Data" / "bronze" / "transactions"

spark = SparkSession.builder \
    .appName("Raw to Bronze Transactions") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(str(raw_path))

bronze_df = df \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("source_file", input_file_name())

bronze_df.write \
    .mode("overwrite") \
    .parquet(str(bronze_path))

print("Bronze layer yozildi:")
print(bronze_path)

print("Rows count:", bronze_df.count())
print("Columns count:", len(bronze_df.columns))
 
spark.stop()
