import os
import time
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, to_date, avg, when

load_dotenv()

# 1. Start Spark with Azure Packages
spark = SparkSession.builder \
    .appName("GSOD_Task2_AvgAnnualTemp") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.6,com.microsoft.azure:azure-storage:8.6.6") \
    .getOrCreate()

# 2. Configure Azure Blob Access
storage_account_name = "groupdata479storage"
storage_account_key = os.environ["AZURE_STORAGE_KEY"]
container_name = "gsod-data"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net",
    storage_account_key
)

start_time = time.time()

# 3. Load the Dataset from Azure Blob
file_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/*/01*.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# 4. Extract station identifier and year from DATE field
df = df.withColumn("Year", year(to_date(col("DATE"), "yyyy-MM-dd")))

# 5. Handle missing or invalid TEMP values
# GSOD uses 9999.9 as a sentinel for missing temperature; also filter nulls
INVALID_TEMP = 9999.9

df_clean = df.withColumn(
    "TEMP_clean",
    when((col("TEMP").isNull()) | (col("TEMP") == INVALID_TEMP), None)
    .otherwise(col("TEMP"))
)

# 6. Distributed aggregation: compute average annual temperature per station per year
result = df_clean.groupBy("STATION", "Year") \
    .agg(avg("TEMP_clean").alias("average_temperature")) \
    .filter(col("average_temperature").isNotNull()) \
    .orderBy("STATION", "Year")

# 7. Show output
print("--- Average Annual Temperature per Station per Year ---")
result.show(truncate=False)

# 8. Save output as CSV
output_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/task2_output"
result.write.mode("overwrite").option("header", True).csv(output_path)

print("Output written to Azure Blob Storage at:", output_path)
print(f"\nTotal execution time: {time.time() - start_time:.2f} seconds")
