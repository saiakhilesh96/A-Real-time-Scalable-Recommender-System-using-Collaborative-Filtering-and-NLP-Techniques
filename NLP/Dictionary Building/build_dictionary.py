import gzip
import gensim #this is not there but now imported
import logging
import pandas
import numpy as np
import random
import pandas as pd
from operator import itemgetter

f1= open('vader_lexicon.txt',"a")
f1.write("\n")
f1.close()

def convert2floats(l):
    j= 0
    while j < len(l):
        l[j][2] = float(l[j][2])
        j = j + 1
    return l

def add_to_vader_package(add_word):
    f=open('vader_lexicon.txt')
    lines=f.readlines()
    l1= lines[440:] #this creates a list of lines from the text file
    #This will create a list of lists the fields in the lines are made into a list of lists
    l2= []
    for i in l1:
        words = i.split("\t")
        l2.append(words)
    converted_list = convert2floats(l2)
    #converted_list
    #calculate euclidean diatance for each token and append it to the list
    for i in converted_list:
        euclid_dist = 0.44957 - i[2]
        if(euclid_dist < 0):
            euclid_dist *= -1
        i.append(euclid_dist)
    sorted_list = sorted(converted_list,key = itemgetter(4))
    prsorted_list[0][3]
    line_to_add_to_vader = add_word+"\t"+str(sorted_list[0][1])+"\t"+str(sorted_list[0][2])+"\t"+sorted_list[0][3]
    f.close()
    f1 = open('vader_lexicon.txt','a')
    f1.write(line_to_add_to_vader)
    f1.close()

def word_present(word2add):
    with open('vader_lexicon.txt') as file:
        contents = file.read()
        if word2add in contents:
            return True
        return False

data_file = "food_reviews.txt.gz"
with gzip.open(data_file,'rb') as f:
    for i,line in enumerate (f):
        #print(line)
        break

def read_input(input_file):
    """This method reads only gzip format files"""
    #logging.info("reading file {0} ... this may take a while".format(input_file))
    with gzip.open(input_file,'rb') as f:
        for i, line in enumerate(f):
            #if(i % 10000 == 0):
                #logging.info("read {0} reviews".format(i))
            #do some pre-processing stuff
            yield gensim.utils.simple_preprocess(line)

#read tokenized reviews into a list
#each review becomes a series of words
#this becomes a list of lists
documents= list(read_input(data_file))

#build vocabulary and train model
model= gensim.models.Word2Vec(
        documents,
        size=150,
        window=10,
        min_count=2,
        workers=10,
        )
model.train(documents,total_examples=len(documents),epochs=10)

wl= "wide"
a = model.wv.most_similar(positive=wl)

k = 0
while (k < 5):
    word_to_add = a[k][0]
    if(word_present(word_to_add) == False):
        print(word_to_add)
        add_to_vader_package(word_to_add)
    k+= 1
