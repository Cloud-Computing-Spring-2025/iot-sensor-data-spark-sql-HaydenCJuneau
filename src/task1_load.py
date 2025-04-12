from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import desc, count, rank

# Initialize Spark Session
spark: SparkSession = SparkSession.builder.appName("IOTAnalysis").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Load DataFrames
df = spark.read.option("header", True).csv("/opt/bitnami/spark/IOT/input/sensor_data.csv")


def explore(df: DataFrame) -> DataFrame:
    # Create Temporary View
    df.createOrReplaceTempView("sensor_readings")
    
    # Show the first 5 rows.
    spark.sql("select * from sensor_readings limit 5").show()

    # Count the total number of records.
    spark.sql("select count(*) from sensor_readings").show()

    # Retrieve the distinct set of locations (or sensor types).
    grouped = spark.sql("select location from sensor_readings group by location")
    grouped.show()

    # Finally, write your DataFrame (or one key query result) to task1_output.csv.
    return grouped
    

# Save result
explore(df).coalesce(1).write.mode("overwrite").csv("/opt/bitnami/spark/IOT/output/task1", header=True)
