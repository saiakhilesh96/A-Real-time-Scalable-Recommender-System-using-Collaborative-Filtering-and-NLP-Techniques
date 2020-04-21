#Aum Sri Sai Ram

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
import time

#create spark session
#spark = SparkSession.builder.appName("ALS factorization").config("spark.driver.maxResultSize", "96g").config("spark.driver.memory", "96g").config("spark.executor.memory", "10g").getOrCreate()
spark = SparkSession.builder.appName("ALS factorization").config("spark.driver.maxResultSize", "96g").config("spark.driver.memory", "96g").getOrCreate()
#spark = SparkSession.builder.appName("ALS factorization").getOrCreate()
#sc= spark.sparkContext

#read the input file 
#csv file format (usedid,itemid,rating) 
df = spark.read.csv('hdfs://master:9000/user/hadoop/akhilesh/mapped_ratings.csv',header= True,inferSchema=True)

#drop the null values if they exist in any of the three columns.
df= df.na.drop(subset=["user","item","rating"])


#create the checkpoint interval
spark.sparkContext.setCheckpointDir("hdfs://master:9000/user/hadoop/akhilesh/check_point_directory/als")

#create checkpoint interval
#ALS.checkpointInterval= 20

#set of hyper parameters
ranks= [10,30,50,100,150,200]
reg_params=[0.1,0.01,0.001,0.25,0.025,0.005,0.002,0.05,0.16,0.18,0.0002]
max_iter= [100,200,300,400,500]

evaluator = RegressionEvaluator(metricName='rmse', labelCol='rating', predictionCol='prediction')
#training and validation data splitting
(trainingRatings, validationRatings) = df.randomSplit([80.0, 20.0])

trainingRatings= trainingRatings.repartition(96)
_= trainingRatings.cache()

min_error = float('inf')
best_rank = -1
best_regularization = 0
best_model = None
best_iter = 10

#Hyper-Parameter tuning
"""
for r in ranks:
    for reg in reg_params:
        for iterations in max_iter:
             als= ALS(rank= r,maxIter=10,regParam= reg)
             model= als.fit(trainingRatings)
             predictions = model.transform(validationRatings)
             error= evaluator.evaluate(predictions.na.drop())
             print('{} latent factors and regularization = {}: validation RMSE is {}'.format(r, reg, error))
             if error < min_error:
               min_error = error
               best_rank = r
               best_regularization = reg
               best_model = model
	       best_iter = iterations
"""
#Hyper-Parameter tuning
for r in ranks:
    for reg in reg_params:
        for iterations in max_iter:
            als= ALS(rank= r,maxIter=10,regParam= reg)
            model= als.fit(trainingRatings)
            predictions = model.transform(validationRatings)
            error= evaluator.evaluate(predictions.na.drop())
            print('{} latent factors and regularization = {}: validation RMSE is {}'.format(r, reg, error))
            if error < min_error:
                min_error = error
                best_rank = r
                best_regularization = reg
                best_model = model
                best_iter = iterations

print('\nThe best model has {} latent factors and regularization = {} with RMSE = {} for iterations = {}'.format(best_rank, best_regularization, min_error,best_iter))

#Instantiate ALS
als = ALS(rank=best_rank, maxIter=1000, regParam= best_regularization, seed=99)

#repartition the df
df= df.repartition(96)

#cache the df
_= df.cache()

start_time= time.time()
model= als.fit(df) #Perform ALS training
print("Training time : ",time.time() - start_time)

start_time= time.time()
recDF= model.recommendForAllUsers(10) #recommend top 10 products to all users and store it in a dataframe
print("Recommending time : ",time.time() - start_time)

#Now we have to keep only the products.
def filtertuples(l):
    res = [sub[0] for sub in l] 
    return res

filtertuples_function_udf = F.udf(filtertuples, ArrayType(IntegerType()))

recDF= recDF.withColumn("recommended",filtertuples_function_udf(recDF['recommendations'])).drop('recommendations') 

recDF= recDF.withColumn('recommended',recDF['recommended'].cast(StringType()))

#write the resulting dataframe into a csv file for future.
start_time= time.time()
#recDF.repartition(1).write.option("delimiter","\t").format('com.databricks.spark.csv').save("hdfs://master:9000/user/hadoop/akhilesh/ALS_result",header= True)
recDF.repartition(1).write.option("sep","\t").option("encoding","UTF-8").save("hdfs://master:9000/user/hadoop/akhilesh/ALS_result",header= True)
print("writing time : ",time.time() - start_time)
