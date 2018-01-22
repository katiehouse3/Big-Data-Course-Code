from pyspark.sql import SparkSession

# Connect to spark session and to Ulysses doc
logFile = "file:///home/katiehouse3/Documents/ulysses10.txt"
spark = SparkSession.builder.master("local").appName("PS6").getOrCreate()

# Read Ulysses doc
logData = spark.read.text(logFile).cache()
print(logData.count()) # Print count

spark.stop()
