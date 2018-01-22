from pyspark import SparkConf, SparkContext

# Connect to spark session and to Ulysses doc
conf = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf = conf)

# Read Ulysses doc
lines = sc.textFile("ulysses10.txt")
print lines.count()
