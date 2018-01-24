# Assignment 04 part 3
# By: Katie House

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import *


# Part A: Import docs as Structured Data Frames
spark = SparkSession.builder.master("local").appName("PS6").getOrCreate()
sc = spark.sparkContext

t = sc.textFile("file:///home/katiehouse3/Documents/transactions.txt").cache()
p = sc.textFile("file:///home/katiehouse3/Documents/products.txt").cache()
t_fields = t.map(lambda e: e.split("#"))
p_fields = p.map(lambda e: e.split("#"))

tmap = t_fields.map(lambda e: Row(date = e[0], time = e[1], custid = int(e[2]), \
                   prodid = int(e[3]), qtyp = e[4], \
                    price = float(e[5]))).collect()
pmap = p_fields.map(lambda e: Row(prodid = int(e[0]), prodname = e[1], \
                   uprice = float(e[2]), qtya = int(e[3]))).collect()
tDF = spark.createDataFrame(tmap)
pDF = spark.createDataFrame(pmap)
jDF = tDF.join(pDF,"prodid")

# Part B: Top 5 Customers by largest spent
print "Part B: The top 5 customers by amount spent"
aDF = jDF.groupBy("custid").agg(sum("price").alias("MoneySpent"))
aDF = aDF.orderBy(aDF.MoneySpent.desc())
aDF.select(aDF.custid, bround('MoneySpent', 2).alias('MoneySpent')).show(5)

# Part C: Product Names of Top 5 Customers bought
print "Part C: The products the top 5 customers bought"
bDF = jDF.groupBy('custid').agg(sum("price").alias("MoneySpent"))
bDF = bDF.orderBy(bDF.MoneySpent.desc()).limit(5)
oDF = bDF.join(jDF, 'custid').select(jDF.prodname, bDF.custid)
oDF.groupBy('prodname').count().show(oDF.count(),False)


# Part D: Total number of Ten most popular products
print "Part D: The 10 most populer books by order qty:"
cDF = jDF.groupBy('prodid').agg(sum("qtyp").alias("NumPurch"))
cDF = cDF.orderBy(cDF.NumPurch.desc()).limit(10)
oDF = cDF.join(jDF, 'prodid').select(jDF.prodname, jDF.prodid, cDF.NumPurch)
oDF = oDF.groupby(["prodname","prodid"]).agg(max("NumPurch").alias("NumPurch"))
oDF.orderBy(oDF.NumPurch.desc()).show(oDF.count(),False)

# Part E: Order products by number sold then total value
oDF2 = oDF.join(pDF, 'prodid', 'left_outer')
oDF2 = oDF2.select(oDF2.prodid, oDF.prodname, round(oDF2.NumPurch,1).alias("NumPurch"), oDF2.uprice, round(oDF2.NumPurch * oDF2.uprice).alias("TotalValue"))
print "Part E: The 10 most populer books by total value:"
oDF2.sort("TotalValue", ascending=False).show(oDF.count(),False)
