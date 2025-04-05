# qa_checks.py
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws, sum as _sum

# Connect to Postgres and get row counts
conn = psycopg2.connect(
    host="localhost",  # adjust if needed; within docker networks you might use the service name "postgres"
    port=5432,
    user="postgres",
    password="postgres",
    database="postgres"
)
cur = conn.cursor()

tables = ["robot", "teleop"]
pg_counts = {}
for table in tables:
    cur.execute(f"SELECT COUNT(*) FROM {table}")
    pg_counts[table] = cur.fetchone()[0]

cur.close()
conn.close()

# Start Spark session for Delta table access
spark = SparkSession.builder.appName("DeltaQA").getOrCreate()

delta_counts = {}
for table in tables:
    df = spark.read.format("delta").load(f"s3a://delta-tables/{table}")
    delta_counts[table] = df.count()

print("Row Count Comparison:")
for table in tables:
    print(f"{table}: Postgres = {pg_counts[table]} vs Delta = {delta_counts[table]}")

# Partial checksum: here we simply compute a hash sum over the 'UUID' column
def compute_checksum(df):
    # Create a new column with a SHA2 hash of the UUID
    df_hashed = df.withColumn("hash", sha2(concat_ws("", df["UUID"]), 256))
    checksum = df_hashed.agg(_sum("hash")).collect()[0][0]
    return checksum

for table in tables:
    delta_df = spark.read.format("delta").load(f"s3a://delta-tables/{table}")
    delta_checksum = compute_checksum(delta_df)
    # In a full implementation, you would compute a similar checksum in Postgres.
    pg_checksum = "Not computed"  # Placeholder
    print(f"{table}: Postgres checksum = {pg_checksum} vs Delta checksum = {delta_checksum}")

spark.stop()
