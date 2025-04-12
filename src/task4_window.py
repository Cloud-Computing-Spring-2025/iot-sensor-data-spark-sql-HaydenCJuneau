from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import to_timestamp, hour

# Initialize Spark Session
spark: SparkSession = SparkSession.builder.appName("IOTAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load DataFrames
df = spark.read.option("header", True).csv("/opt/bitnami/spark/IOT/input/sensor_data.csv")


def explore(df: DataFrame) -> DataFrame:
    # Create Temporary View
    df.createOrReplaceTempView("sensor_readings")
    
    # Group by sensor and order by temperature
    # Add a window function to rank
    hottest = spark.sql(
    '''
    WITH avg_temps AS (
        SELECT sensor_id, ROUND(AVG(temperature), 2) AS avg_temp
        FROM sensor_readings
        GROUP BY sensor_id
    )
    SELECT sensor_id, avg_temp,
           RANK() OVER (ORDER BY avg_temp DESC) AS rank_temp
    FROM avg_temps
    ORDER BY rank_temp
    LIMIT 5
    '''
    )
    hottest.show()

    return hottest

# Save result
explore(df).coalesce(1).write.mode("overwrite").csv("/opt/bitnami/spark/IOT/output/task4", header=True)
