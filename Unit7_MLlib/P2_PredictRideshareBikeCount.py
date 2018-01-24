# Adapted from "Regression Analysis with Spark MLlib.py"
# By: Katie House

from pyspark.sql import SparkSession
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql.functions import count, when, isnan, col
import numpy as np

spark = SparkSession.builder.master("local").appName("PS6").getOrCreate()
sc = spark.sparkContext

path = "file:///home/katiehouse3/Desktop/hour_noheader.csv"
raw_data = sc.textFile(path)
num_data = raw_data.count()
records = raw_data.map(lambda x: x.split(","))
records.cache()

# PART B: 
# Convert to Dataframe and Exclude Nulls
rDF = records.toDF()
print "\nPART B: Convert to Dataframe and find/exclude Nulls"
print "\nConverted Dataframe (1st row):"
rDF.show(1)
print "Count number of nulls per column:"
rDF.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in rDF.columns]).show()
print "No nulls to replace"

# PART A: 
# catagorical feature mapping
# function to get the categorical feature mapping for a given variable column
def get_mapping(rdd, idx):
    return rdd.map(lambda fields: fields[idx]).distinct().zipWithIndex().collectAsMap()

# extract all the catgorical mappings
mappings = [get_mapping(records, i) for i in range(2,10)] 
cat_len = sum(map(len, mappings))       #Feature vector length for categorical features
num_len = len(records.first()[11:15])       #Feature vector length for numerical features
total_len = num_len + cat_len

# function to use the feature mappings to extract binary & numerical feature vectors
def extract_features(record):
    cat_vec = np.zeros(cat_len)
    i = 0
    step = 0
    for field in record[2:9]: # exclude first two variables
        m = mappings[i]
        idx = m[field]
        cat_vec[idx + step] = 1
        i = i + 1
        step = step + len(m)
    
    num_vec = np.array([float(field) for field in record[10:14]]) # exclude casual and registered
    return np.concatenate((cat_vec, num_vec))

# function to extract the label from the last column
def extract_label(record):
    return float(record[-1])

# create feature vector
data = records.map(lambda r: LabeledPoint(extract_label(r), extract_features(r)))
first_point = data.first()
print "\nPART A: catagorical feature mapping (excl variables instant,dteday,casual,registered)\n"
print "Original data:\n" + str(records.first())
print "\nFeature vector:\nLabel: " + str(first_point.label)
print "Linear Model feature vector:\n" + str(first_point.features)
print "Linear Model feature vector length: " + str(len(first_point.features)) +"\n\n"


# PART C: 
# Training a Regression Model
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.tree import DecisionTree

# create training and testing sets for linear model
data_with_idx = data.zipWithIndex().map(lambda (k, v): (v, k))
test = data_with_idx.sample(False, 0.15, 42)    #15% of total sample
train = data_with_idx.subtractByKey(test)

train_data = train.map(lambda (idx, p): p)
test_data = test.map(lambda (idx, p) : p)

train_size = train_data.count()
test_size = test_data.count()
print "PART C: Create training and testing random samples" 
print "Training data size: %d" % train_size
print "Test data size: %d" % test_size
print "Total data size: %d " % num_data
print "Train + Test size : %d\n" % (train_size + test_size)

# Train your model using LinearRegressionSGD method
linear_model = LinearRegressionWithSGD.train(train_data, iterations=10, step=0.1, intercept=False)
true_vs_predicted = test_data.map(lambda p: (p.label, linear_model.predict(p.features)))
print "\nLinear Model predictions: " + str(true_vs_predicted.take(5))


# PART D: 
# set up performance metrics functions 
def squared_error(actual, pred):
    return (pred - actual)**2
def abs_error(actual, pred):
    return np.abs(pred - actual)

# compute performance metrics for linear model
mse = true_vs_predicted.map(lambda (t, p): squared_error(t, p)).mean()
mae = true_vs_predicted.map(lambda (t, p): abs_error(t, p)).mean()
print "\n\nPART D: Accuracy Metrics" 
print "Linear Model - Mean Squared Error: %2.4f" % mse
print "Linear Model - Mean Absolute Error: %2.4f" % mae
