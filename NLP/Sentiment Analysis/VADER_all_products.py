#!/usr/bin/env python
# coding: utf-8

# # Aum Sri Sai Ram

# In[1]:


from pyspark import SparkContext
from pyspark.sql import SQLContext
#from pyspark.sql.types import StringType
from pyspark.sql.functions import monotonically_increasing_id
#from pyspark.sql.types import StructType
from pyspark.sql.types import *
#from pyspark.sql import Row
from pyspark.sql.functions import grouping
from pyspark.sql import functions as F
import string
from pyspark import sql
import time


# ## Data Acquisition

# In[2]:


#data_rdd= sc.textFile('file:///home/akhilesh/Project/Data/movies.txt')


# In[ ]:


start_time= time.time()


# In[ ]:


sc= SparkContext()


# In[ ]:


sqlc= sql.SQLContext(sc)


# In[2]:


data_rdd= sc.textFile('hdfs://master:9000/user/hadoop/akhilesh/movies.txt')


# ## Data Preprocessing

# In[3]:


products= data_rdd.filter(lambda line: "productId: " in line).map(lambda line: [line.replace("product/productId: ","")])


# In[5]:


reviews= data_rdd.filter(lambda line: "review/text: " in line).map(lambda line: [line.replace("review/text: ","")])


# ### convert the reviews and products rdds into dataframes and combine them as a single dataframe

# In[7]:


productsDF= products.toDF(['products'])
reviewsDF= reviews.toDF(['review'])


productsDF= productsDF.withColumn('id',monotonically_increasing_id())
reviewsDF= reviewsDF.withColumn('id',monotonically_increasing_id())

#DF= productsDF.join(reviewsDF,'id','outer').drop('id')
pro_rev_DF= productsDF.join(reviewsDF,'id','outer')

#DF.show()


# ### we'll group all the reviews for all products.

# In[8]:


#This is working. :)
prod_reviews_DF= pro_rev_DF.groupBy('products').agg(F.concat_ws(".",F.collect_list('review')))


# # VADER Sentiment Analysis on all the grouped reviews for all products

# In[28]:


def sentiments(r):
    from nltk.sentiment.vader import SentimentIntensityAnalyzer
    s= SentimentIntensityAnalyzer()
    #print("______________SAIRAM THE REVIEW IS _______________",r[1])
    return s.polarity_scores(r[1])['compound']


# In[29]:


prod_reviews_rdd= prod_reviews_DF.rdd

c_sc= prod_reviews_rdd.map(sentiments)

csDF= c_sc.map(lambda x: (x, )).toDF(['compoundscore'])

csDF= csDF.withColumn('id',monotonically_increasing_id())

prod_reviews_DF= prod_reviews_DF.withColumn('id',monotonically_increasing_id())

vaderDF= prod_reviews_DF.join(csDF,'id','outer').drop('id','concat_ws(., collect_list(review))')
print('\n\n\t Total time to perform : ',time.time() - start_time)
vaderDF.repartition(1).write.option("delimiter",",").format('com.databricks.spark.csv').save("hdfs://master:9000/user/hadoop/akhilesh/compoundscores",header= True)


# In[ ]:


print('\n\n\t Total time to perform and write: ',time.time() - start_time)


# In[36]:


vaderDF.show()

