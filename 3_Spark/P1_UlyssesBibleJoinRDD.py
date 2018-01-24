from pyspark import SparkConf, SparkContext

# Connect to spark session and to Ulysses doc
conf = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf = conf)

# Upload docs
u_rdd = sc.textFile("file:///home/katiehouse3/Documents/4300-2.txt")
b_rdd = sc.textFile("file:///home/katiehouse3/Documents/bible.txt")

# Eliminate all verse numbers
import re
def haslines(line):
    return not re.match(r'\d\d\:\d\d\d\:\d\d\d', line)  
b_rdd = b_rdd.filter(haslines)

# Only contain unique words
u_dist = u_rdd.distinct()
b_dist = b_rdd.distinct()

# Transform and reduce
b_words = b_dist.flatMap(lambda x:
    x.lower()
    .replace(".", "").replace("?", "").replace("!", "")
    .replace("(", "").replace("-", "").replace(":", "")
    .replace(",", "").replace(")", "").split(" "))

u_words = u_dist.flatMap(lambda x:
    x.lower()
    .replace(".", "").replace("?", "").replace("!", "").replace("(", "")
    .replace("-", "").replace(":", "").replace(",", "").replace(")", "")
     .split(" ")
  )

# Remove all Stop Words
stopwords = set(sc.textFile('/home/katiehouse3/Documents/stopwordlist.txt').collect())
b_nonstop = b_words.filter(lambda x: x not in stopwords)
u_nonstop = u_words.filter(lambda x: x not in stopwords)

# Filter nulls
u_nonull = u_nonstop.filter(lambda x: x not in '')
b_nonull = b_nonstop.filter(lambda x: x not in '')  

# Map and output Result
u_result = u_nonull.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
b_result = b_nonull.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)

output = u_result.join(b_result)
output = output.map(lambda x: (x[0], ) + x[1]) #combine tuples
output2 = output.takeOrdered(20,lambda x: -x[2]) #sort by Bible

for x in output2:
    print x[0], x[1], x[2]

