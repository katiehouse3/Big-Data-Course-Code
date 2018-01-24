# Assignment 07 part 2 
# By: Katie House 
from pyspark.sql import SparkSession
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
import numpy as np
'''PROBLEM 1'''
# Part A: Import docs as Structured Data Frames
spark = SparkSession.builder.master("local").appName("PS6").getOrCreate()
sc = spark.sparkContext
rDF = spark.read.option("nullValue", "NA").csv("file:///home/katiehouse3/Desktop/auto_mpg_original-1.csv")
DF = rDF.toDF("mpg", "cylin","displcmnt","hp","wgt","accel","mdlyr","orig","carname")
 
# Part C: Find Nulls and Replace with Col Averages
DF_nona = DF.dropna()           #Drop nulls to calculate average
mpg = DF_nona.select(DF_nona.mpg.cast("float").alias('mpg'))
hp  = DF_nona.select(DF_nona.hp.cast("float").alias('hp'))
mpg_avg = mpg.groupBy().avg('mpg').collect()
hp_avg = hp.groupBy().avg('hp').collect()
mpg_avg = mpg_avg[0][0]
hp_avg = hp_avg[0][0]
DF = DF.na.fill({'mpg': mpg_avg, 'hp': hp_avg})

# Part B: Randomly select 15% of data for testing, and 85% for training
splits = DF.randomSplit([0.85, 0.25], 24)
trainDF = splits[0]
testDF = splits[1]


'''~~~~~~~~~~PROBLEM 2~~~~~~~~~~~~~~'''
# PART A: Select mpg and hp columns from Test and Train data
columns_num = [3, 0]
trainDF= trainDF.select(*(trainDF.columns[i] for i in columns_num))
testDF = testDF.select(*(testDF.columns[i] for i in columns_num))

trainrdd = trainDF.rdd.cache()
testrdd = testDF.rdd.cache()

trainrdd = trainrdd.map(tuple)
testrdd = testrdd.map(tuple)

print "\nPART A: Select hp and mpg"
print "\nTrain Data (hp,mpg): \n%s" % trainrdd.take(5)
print "Test Data (hp,mpg): \n%s" % testrdd.take(5)

# PART B: Assign mpg as a feature and horsepower as the target
def parsePoint(line):
    values = [float(x) for x in line]
    return LabeledPoint(values[0], values[1:])

trainData = trainrdd.map(parsePoint)
testData = testrdd.map(parsePoint)

print "\n\nPART B: mpg as a feature and horsepower as the target"
print "\ntrain Parsed Data top 5 rows (hp, mpg) :\n %s\n" % trainData.take(5)

# PART C: Build the linear regression model
model = LinearRegressionWithSGD.train(trainData, iterations=50, step=0.01, intercept=False)

print "\n\nPART C: Identify linear model: hp(mpg) = weights(mpg) + intercept"
print "\n model: %s" % model

# PART D: Evaluate the model on test data
valuesAndPreds = testData.map(lambda p: (p.label, model.predict(p.features)))

print "\n\nPART D: Evaluate model with test data"
print "\n First 10 rows (actual, prediction):\n %s \n" % valuesAndPreds.take(10)

# PART E: Calculate 2 standard measures of model accuracy
def squared_error(actual, pred):
    return (pred - actual)**2
def abs_error(actual, pred):
    return np.abs(pred - actual)

mse = valuesAndPreds.map(lambda (t, p): squared_error(t, p)).mean()
mae = valuesAndPreds.map(lambda (t, p): abs_error(t, p)).mean()
print "\n\nPART E: Calculate 2 standard measures of model accuracy\n"
print "Linear Model - Mean Squared Error: %2.4f" % mse
print "Linear Model - Mean Absolute Error: %2.4f\n" % mae


# PART F: Output to CSV
def toCSVLine(data):
  return ','.join(str(d) for d in data)

lines = testrdd.map(toCSVLine)
lines.saveAsTextFile('file:///home/katiehouse3/Desktop/output')
