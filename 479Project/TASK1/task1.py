from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, year, to_date, when

# 1. Start Spark with Azure Packages
spark = SparkSession.builder \
    .appName("GSOD_Task1_Exploration") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.6,com.microsoft.azure:azure-storage:8.6.6") \
    .getOrCreate()

# 2. Configure Access
storage_account_name = "groupdata479storage"
storage_account_key = "0/+rC2RZGQpeAB1ZwXd+9Flw0uaFibsUqHBC6YhhlrAE/AFLIbORx/vMVxpA6LvB4KtyT9aXeWz7+AStFSutyw=="
container_name = "gsod-data"

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.blob.core.windows.net", 
    storage_account_key
)

# 3. Load the Dataset
# This reads all CSVs labeled "01" in the container. Did this because the full dataset is massive and takes forever Even with just 3 years.
file_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/*/01*.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

print("--- Schema and Data Types ---")
df.printSchema()

# Clean up the DATE column and extract for counting.
df = df.withColumn("Parsed_Date", to_date(col("DATE"), "yyyy-MM-dd"))
df = df.withColumn("Year", year(col("Parsed_Date")))

# Requirement: Counts
total_records = df.count()
unique_stations = df.select("STATION").distinct().count()
years_covered = df.select("Year").distinct().count()

print(f"Number of records: {total_records}")
print(f"Number of unique weather stations: {unique_stations}")
print(f"Number of years covered: {years_covered}")

# GSOD uses 9999.9 for missing temps. We check TEMP, MAX, and MIN.
invalid_temp_indicator = 9999.9

missing_temps = df.select(
    count(when(col("TEMP") == invalid_temp_indicator, True)).alias("Missing_Mean_TEMP"),
    count(when(col("MAX") == invalid_temp_indicator, True)).alias("Missing_MAX_TEMP"),
    count(when(col("MIN") == invalid_temp_indicator, True)).alias("Missing_MIN_TEMP")
)

print("--- Missing or Invalid Temperature Values (9999.9) ---")
missing_temps.show()