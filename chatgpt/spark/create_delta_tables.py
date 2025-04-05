# spark/create_delta_tables.py
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark = SparkSession.builder \
    .appName("CreateDeltaTables") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:1.0.0") \
    .getOrCreate()

# Configure Spark to access Minio (S3 API compatible)
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.access.key", "minioadmin")
hadoop_conf.set("fs.s3a.secret.key", "minioadmin")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")

# Schema definitions
robot_schema = "UUID string, Robot_ID string, Timestamp timestamp, Event_ID string"
teleop_schema = robot_schema + ", Teleoperator_ID string"
metrics_schema = "cpu string, memory string, disk_io string, timestamp timestamp"

# Create empty Delta tables with partitioning
for table, schema, partition in [("robot", robot_schema, "Robot_ID"),
                                  ("teleop", teleop_schema, "Robot_ID")]:
    df = spark.createDataFrame([], schema)
    df.write.format("delta").mode("overwrite").partitionBy(partition).save(f"s3a://delta-tables/{table}")

# Create delta table for system metrics
df_metrics = spark.createDataFrame([], metrics_schema)
df_metrics.write.format("delta").mode("overwrite").partitionBy("cpu").save("s3a://delta-tables/metrics")

spark.stop()
