from pathlib import Path
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parents[1]

bronze_path = BASE_DIR / "Data" / "bronze" / "transactions"

spark = SparkSession.builder \
    .appName("Check Bronze Transactions") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(str(bronze_path))

print("Bronze path:")
print(bronze_path)

print("Schema:")
df.printSchema()

print("First 5 rows:")
df.show(5, truncate=False)

print("Columns count:", len(df.columns))
print("Columns:", df.columns)

print("Rows count:", df.count())

spark.stop()