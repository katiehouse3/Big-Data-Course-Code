# Assignment 04 part 4
# By: Katie House

from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import expr

# Connect to spark session and to Ulysses doc
conf = SparkConf().setMaster("local").setAppName("MyApp")
sc = SparkContext(conf = conf)

# Part A: Import docs as RDDs
t = sc.textFile("file:///home/katiehouse3/Documents/transactions.txt").cache()
p = sc.textFile("file:///home/katiehouse3/Documents/products.txt").cache()
t_rdd = t.map(lambda e: e.encode('ascii', 'ignore').split("#"))
p_rdd = p.map(lambda e: e.encode('ascii', 'ignore').split("#"))

# Part B: Top 5 Customers by largest spent
t_rdd_b = t_rdd.map(lambda x:(int(x[2]), float(x[5]))) #select customer ID and price paid
t_rdd_b = t_rdd_b.reduceByKey(lambda x, y: x + y)
a_output = t_rdd_b.takeOrdered(5,lambda x: -x[1]) 
print "Part B: The top 5 customers by amount spent"
print "Customer ID | Amount Spent"
for x in a_output:
    print x[0], x[1]
print '\n'

# Part C: Product Names of Top 5 Customers bought
t_rdd_c = t_rdd.map(lambda x:(int(x[2]), int(x[3]))) # select customer ID and product ID
a_output = sc.parallelize(a_output)         # convert top 5 customers to rdd
t_rdd_c = a_output.leftOuterJoin(t_rdd_c)       # add product IDs
t_rdd_c = t_rdd_c.map(lambda x: (x[0], ) + x[1])    # unnest tuple
t_rdd_c = t_rdd_c.map(lambda x:(x[2], x[0]))        # make product ID the key
t_rdd_c = t_rdd_c.reduceByKey(lambda a, b: a)       # remove duplicate product IDs
p_rdd_c = p_rdd.map(lambda x:(int(x[0]), x[1]))     # select product ID and product name
t_rdd_c = t_rdd_c.leftOuterJoin(p_rdd_c)        # join transaction and product tables
t_rdd_c = t_rdd_c.map(lambda x: (x[0], ) + x[1])    # unnest tuple
t_rdd_c = t_rdd_c.map(lambda x:(x[1], x[2]))        # select customer id and product name
b_output = t_rdd_c.sortBy(lambda x: x[0],ascending=False).collect() 
print "Part C: The products the top 5 customers bought"
print "Customer ID | Product Name"
for x in b_output:
    print "   ", x[0], "    ", x[1]
print '\n'

# Part D: Total number of Ten most popular products
t_rdd_d = t_rdd.map(lambda x:(int(x[3]), int(x[4])))    # Select product ID and qty bought
t_rdd_d = t_rdd_d.reduceByKey(lambda a, b: a + b)   # Take sum of qty bought
p_rdd_d = p_rdd.map(lambda x:(int(x[0]), x[1]))     # select product ID and product name
j_rdd_d = t_rdd_d.leftOuterJoin(p_rdd_d)        # join transaction and product tables
j_rdd_d = j_rdd_d.map(lambda x: (x[0], ) + x[1])    # unnest tuple
d_output = j_rdd_d.takeOrdered(10,lambda x: -x[1]) 
print "Part D: The 10 most populer books by order qty:"
print "Product ID | Qty Purch | Product Name"
for x in d_output:
    print "   ",x[0],"      ", x[1],"       ", x[2]
print '\n'

# Part E: Order products by number sold then total value
d_output = sc.parallelize(d_output)         # convert top 10 products to rdd
d_output = d_output.map(lambda x: (x[0], tuple(x[1:])))
p_rdd_e = p_rdd.map(lambda x:(int(x[0]), float(x[2])))  # Select product ID and product price
j_rdd_e = d_output.leftOuterJoin(p_rdd_e)       # join transaction and product tables

j_rdd_e = j_rdd_e.map(lambda x: (x[0], ) + x[1])    # unnest tuple
j_rdd_e = j_rdd_e.map(lambda (x1,(x2,x3),x4): (x1,x2,x3,x4))
j_rdd_e = j_rdd_e.map(lambda x: (x[0], x[1], x[3], x[1]*x[3], x[2]))
e_output = j_rdd_e.takeOrdered(10,lambda x: -x[3]) 
print "Part E: The 10 most populer books by total value:"
print "Product ID | Qty Purch | Unit Price |    Total Value    | Product Name"
for x in e_output:
    print "   ",x[0],"      ", x[1],"       ", x[2],"       ", x[3],"       ", x[4]
print '\n'

