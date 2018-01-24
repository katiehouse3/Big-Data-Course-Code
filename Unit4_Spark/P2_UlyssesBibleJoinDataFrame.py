from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *

# Initiate Spark Session
spark = SparkSession.builder.master("local").appName("PS6").getOrCreate()
sc = spark.sparkContext

# Read Text files
u = spark.read.text("file:///home/katiehouse3/Documents/4300-2.txt")
b = spark.read.text("file:///home/katiehouse3/Documents/bible.txt")

# Clean data of punctuation
u = u.withColumn('value' , lower(regexp_replace('value', '[^0-9A-Za-z:\s]', '')))
b = b.withColumn('value' , lower(regexp_replace('value', '[^0-9A-Za-z:\s]', '')))

# Split lines and explode
uDF = u.select(split(u.value, " ").alias('word'))
bDF = b.select(split(b.value, " ").alias('word'))

uDF = uDF.select(explode('word')).withColumnRenamed("col", "word")
bDF = bDF.select(explode('word')).withColumnRenamed("col", "word")

# Remove null values
uDF = uDF.drop()
bDF = bDF.drop()

# Eliminate all verse numbers    
bDF = bDF.filter("word NOT LIKE '__:___:___'")

# Eliminate Stop Words
stopwords = list(sc.textFile('/home/katiehouse3/Documents/stopwordlist.txt').collect())
uDF = uDF.filter(~uDF.word.isin(stopwords))
bDF = bDF.filter(~bDF.word.isin(stopwords))

# Output results
bDF = bDF.groupBy("word").count().withColumnRenamed("count", "bcount")
uDF = uDF.groupBy("word").count().withColumnRenamed("count", "ucount")

bDF.orderBy(bDF.bcount.desc()).show(30)
uDF.orderBy(uDF.ucount.desc()).show(30)
bDF.join(uDF, 'word').orderBy(bDF.bcount.desc()).show(20)
