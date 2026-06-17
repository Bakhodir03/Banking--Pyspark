from pathlib import Path
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parents[1]

file_path = BASE_DIR / "Data" / "financial_fraud_detection_dataset.csv"

spark = SparkSession.builder \
    .appName("Banking PySpark Check Data") \
    .master("local[*]") \
    .getOrCreate()

df = spark.read \
    .option("header", True) \
    .option("inferSchema", True) \
    .csv(str(file_path))

print("Schema:")
df.printSchema()

print("First 5 rows:")
df.show(5, truncate=False)

print("Columns count:", len(df.columns))
print("Columns:", df.columns)

print("Rows count:", df.count())

spark.stop()