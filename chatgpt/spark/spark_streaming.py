# spark/spark_streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StringType, StructType, StructField, TimestampType

spark = SparkSession.builder \
    .appName("KafkaToDelta") \
    .getOrCreate()

# Define a schema matching the payload from Postgres trigger
payload_schema = StructType([
    StructField("action", StringType(), True),
    StructField("data", StructType([
        StructField("UUID", StringType(), True),
        StructField("Robot_ID", StringType(), True),
        StructField("Timestamp", TimestampType(), True),
        StructField("Event_ID", StringType(), True),
        StructField("Teleoperator_ID", StringType(), True)
    ]), True)
])

# Read streaming data from Kafka
raw_df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka:9092") \
  .option("subscribe", "cdc_topic") \
  .load()

json_df = raw_df.selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), payload_schema).alias("payload")).select("payload.*")

# Function to process each micro-batch
def write_to_delta(batch_df, batch_id):
    # Separate rows based on the presence of Teleoperator_ID
    robot_df = batch_df.filter(col("data.Teleoperator_ID").isNull())
    if not robot_df.rdd.isEmpty():
        robot_df.select("data.*")\
                .write.format("delta")\
                .mode("append")\
                .save("s3a://delta-tables/robot")
    teleop_df = batch_df.filter(col("data.Teleoperator_ID").isNotNull())
    if not teleop_df.rdd.isEmpty():
        teleop_df.select("data.*")\
                 .write.format("delta")\
                 .mode("append")\
                 .save("s3a://delta-tables/teleop")

# Start the streaming query
query = json_df.writeStream.foreachBatch(write_to_delta).start()
query.awaitTermination()
