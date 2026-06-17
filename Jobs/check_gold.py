from pathlib import Path
from pyspark.sql import SparkSession

BASE_DIR = Path(__file__).resolve().parents[1]

gold_path = BASE_DIR / "Data" / "gold"

spark = SparkSession.builder \
    .appName("Check Gold Data") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

tables = [
    "transaction_type_summary",
    "fraud_summary",
    "monthly_summary",
    "location_fraud_summary"
]

for table in tables:
    print("\n==============================")
    print("TABLE:", table)
    print("==============================")

    df = spark.read.parquet(str(gold_path / table))

    df.printSchema()
    df.show(20, truncate=False)
    print("Rows count:", df.count())

spark.stop()