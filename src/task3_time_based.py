from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import to_timestamp, hour

# Initialize Spark Session
spark: SparkSession = SparkSession.builder.appName("IOTAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load DataFrames
df = spark.read.option("header", True).csv("/opt/bitnami/spark/IOT/input/sensor_data.csv")


def explore(df: DataFrame) -> DataFrame:
    # Extract Hour
    df = df.withColumn("timestamp_f", to_timestamp("timestamp"))
    df = df.withColumn("hour", hour(df.timestamp_f))
    
    # Create Temporary View
    df.createOrReplaceTempView("sensor_readings")
    
    # Group by hour and order by temperature
    hottest = spark.sql(
        "select hour, round(avg(temperature), 2) as avg_temp " + 
        "from sensor_readings " +
        "group by hour " +
        "order by hour"
    )
    hottest.show()

    return hottest

# Save result
explore(df).coalesce(1).write.mode("overwrite").csv("/opt/bitnami/spark/IOT/output/task3", header=True)
