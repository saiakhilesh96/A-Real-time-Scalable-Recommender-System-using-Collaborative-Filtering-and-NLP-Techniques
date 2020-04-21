#!/usr/bin/env python
# coding: utf-8

# # Aum Sri Sai Ram

# This script extracts the aspects for all the products from all the reviews and ranks them and saves the top 5 aspects for each product

# ## Required imports

# In[ ]:

from pyspark import sql
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import SQLContext

import time


# In[5]:


from pyspark.sql.types import StringType
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StructType
from pyspark.sql.types import *
from pyspark.sql import Row
from pyspark.sql.functions import grouping
from pyspark.sql import functions as F
import string


# In[ ]:


sc= SparkContext()

sqlc= sql.SQLContext(sc)

# ## Data Acquisition

# In[6]:


#data_rdd= sc.textFile('file:///home/akhilesh/Project/Data/movies.txt')
data_rdd= sc.textFile('hdfs://master:9000/user/hadoop/akhilesh/movies.txt')


# ## Data Preprocessing

# In[7]:


products= data_rdd.filter(lambda line: "productId: " in line).map(lambda line: [line.replace("product/productId: ","")])

#products= sc.parallelize(products.take(1000))


# In[9]:


reviews= data_rdd.filter(lambda line: "review/text: " in line).map(lambda line: [line.replace("review/text: ","")])

#reviews= sc.parallelize(reviews.take(1000))

# ### convert the reviews and products rdds into dataframes and combine them as a single dataframe

# In[11]:


productsDF= products.toDF(['products'])
reviewsDF= reviews.toDF(['review'])

products_unique= productsDF.distinct()

productsDF= productsDF.withColumn('id',monotonically_increasing_id())
reviewsDF= reviewsDF.withColumn('id',monotonically_increasing_id())

#DF= productsDF.join(reviewsDF,'id','outer').drop('id')
pro_rev_DF= productsDF.join(reviewsDF,'id','outer')

#DF.show()


# ### Each review is divided into tokens (words)

# In[12]:


#word tokenizer
def word_tokenize1(x):
    import nltk
    #print("sairam",x)
    lowerW = x[0].lower()
    return nltk.word_tokenize(x[0])

words = reviews.map(word_tokenize1)

print("Words Extracted and all tokens done")

# ### Remove stop words

# In[13]:


def remove_stopwords(x):
    from nltk.corpus import stopwords
    stop_words=set(stopwords.words('english'))
    for i in x:
        x = [w for w in x if w not in stop_words]
    return x

stopwordsremovedRDD= words.map(remove_stopwords)

print("Stop words removed")

# ### Remove punctuations from these list of words.

# In[14]:


def remove_punctuations(x):
    import string
    list_punct=list(string.punctuation)
    xtra_list= ['<','>','br']
    list_punct= list_punct+xtra_list
    for i in x:
        x = [w for w in x if w not in list_punct]
    return x

no_punc= stopwordsremovedRDD.map(remove_punctuations) #In this RDD contains all the words which do not have any punctuations and excessive words.

print("punctuations removed")


# ### POS Tagging

# In[15]:


def pos_tag(x):
    import nltk
    return nltk.pos_tag(x)

pos_word = no_punc.map(pos_tag)

print("POS Tagging over")

# ## Aspect Extraction

# ### Extract the Nouns

# In[16]:


def extract_nouns(x):
    nouns= ['NN', 'NNS', 'NNP', 'NNPS']
    #nouns= ['NNP', 'NNPS']
    for i in range(len(x)):
        for tup in x:
            x= [tup for tup in x if tup[1] in nouns]
    return x

extracted_nouns= pos_word.map(extract_nouns)

print("nouns are filtered")

# ### Extract only the words from the above resulting rdd of (word,noun)

# In[17]:


#This method will extract only those nouns excluding the POS tags from the words
def extract_words_from_nouns(x):
    for i in range(len(x)):
        x[i]= x[i][0]
    return x

only_nouns= extracted_nouns.map(extract_words_from_nouns)

print("only nouns are filtered")

# ### create a dataframe of list of words(only_nouns)

# In[18]:


nounsDF= only_nouns.map(lambda x: Row(list(x))).toDF(['nouns']) # Thanks Swami 

print("nouns Data frame is created")

# In[19]:


nounsDF= nounsDF.withColumn('id',monotonically_increasing_id())

print("id are mapped for nouns Data frame is created")

# In[20]:


pro_rev_nn_DF= pro_rev_DF.join(nounsDF,'id','outer')

print("Data frame is created with products reviews and nouns")

# ### we group for each product, all its reviews and all the nouns extracted from all the reviews

# In[21]:


nn_DF= pro_rev_nn_DF.drop('id','review')

print("products reviews and nouns")

#some code I am adding to check for optimizations. Lets see. pls help swami...

nn_DF= nn_DF.groupBy('products').agg(F.collect_list('nouns'))
print("products nouns list of list of nouns grouping over")
#user defined function
def flatlist(s):
    fl= [item for sublist in s for item in sublist]
    import pandas as pd
    res= pd.Series(fl).value_counts().sort_values(ascending=False).index.tolist()[0:5]
    return res

flatlist_function_udf = F.udf(flatlist, ArrayType(StringType()))

prod_nouns_DF= nn_DF.withColumn("noun",flatlist_function_udf(nn_DF['collect_list(nouns)'])).drop('collect_list(nouns)') 


#end of the extra optimization


# In[24]:


print("Aspect Extraction DONE")
#print(prod_nouns_DF.first())
print("Aspect Ranking also done")
from pyspark.sql.types import StringType
prod_nouns_DF= prod_nouns_DF.withColumn('noun',prod_nouns_DF['noun'].cast(StringType()))
print("casting over")
print("lets write this into a file")
start= time.time()
prod_nouns_DF.repartition(1).write.option("sep", "\t").option("encoding", "UTF-8").csv('hdfs://master:9000/user/hadoop/akhilesh/aspects')
print("writing is over",start-time.time())

#aspects_df.repartition(1).write.option("delimiter","\t").format('com.databricks.spark.csv').save("hdfs://master:9000/user/hadoop/akhilesh/aspects",header= True)

print("writing done")
