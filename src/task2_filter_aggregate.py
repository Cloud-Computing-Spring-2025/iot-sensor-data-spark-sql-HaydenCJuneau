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
    
    # Filter out rows where temperature is below 18 or above 30
    # Count rows are in and out of this range
    between = spark.sql("select * from sensor_readings where temperature between 18 and 30")
    print(f"{between.count()} readings are in the 18-30 range. {df.count() - between.count()} rows are outside this range.")

    # Group by location and compute average temp and humidity
    # Order by temperature
    # Retrieve the distinct set of locations (or sensor types).
    hottest = spark.sql(
        "select location, round(avg(temperature), 2) as avg_temp, round(avg(humidity), 2) as avg_humid " + 
        "from sensor_readings " + 
        "group by location " +
        "order by avg_temp desc"
    )
    hottest.show()

    return hottest

# Save result
explore(df).coalesce(1).write.mode("overwrite").csv("/opt/bitnami/spark/IOT/output/task2", header=True)
