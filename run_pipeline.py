import subprocess
import sys
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
JOBS_DIR = BASE_DIR / "Jobs"

scripts = [
    "raw_to_bronze.py",
    "bronze_to_silver.py",
    "silver_to_gold.py",
    "advanced_fraud_analysis.py",
    "export_gold_to_csv.py",
]

print("Pipeline boshlandi...")

for script in scripts:
    script_path = JOBS_DIR / script

    print("\n==============================")
    print(f"Running: {script}")
    print("==============================")

    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(BASE_DIR)
    )

    if result.returncode != 0:
        print(f"Xatolik chiqdi: {script}")
        sys.exit(result.returncode)

    print(f"Tugadi: {script}")

print("\nPipeline muvaffaqiyatli tugadi!")
print("Natijalar Data/exports papkasida.")