from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import to_timestamp, hour, round

# Initialize Spark Session
spark: SparkSession = SparkSession.builder.appName("IOTAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load DataFrames
df = spark.read.option("header", True).csv("/opt/bitnami/spark/IOT/input/sensor_data.csv")


def explore(df: DataFrame) -> DataFrame:
    # Extract Hour
    df = df.withColumn("timestamp_f", to_timestamp("timestamp"))
    df = df.withColumn("hour", hour(df.timestamp_f))

    df = df.withColumn("temperature_d", df.temperature.cast("double"))
    
    # Create Pivot Table
    pivot = df.groupBy("location").pivot("hour").avg("temperature_d")
    pivot.show()

    return pivot

# Save result
explore(df).coalesce(1).write.mode("overwrite").csv("/opt/bitnami/spark/IOT/output/task5", header=True)
