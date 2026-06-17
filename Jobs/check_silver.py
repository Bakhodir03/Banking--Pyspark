from pathlib import Path
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parents[1]

silver_path = BASE_DIR / "Data" / "silver" / "transactions"

spark = SparkSession.builder \
    .appName("Check Silver Transactions") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

df = spark.read.parquet(str(silver_path))

print("Silver path:")
print(silver_path)

print("Schema:")
df.printSchema()

print("First 5 rows:")
df.show(5, truncate=False)

print("Columns count:", len(df.columns))
print("Columns:", df.columns)

print("Rows count:", df.count())

spark.stop()
