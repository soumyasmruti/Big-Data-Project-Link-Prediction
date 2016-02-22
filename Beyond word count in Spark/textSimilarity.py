import pyspark
from pyspark import SparkContext,SparkConf
import sys
import collections
from operator import add
import numpy as np
from numpy import array
from numpy.linalg import norm 


#Function definitions

def jaccardSimilarity(tup):
    
    set1 = set(tup[0][1]).intersection(set(tup[1][1]))
    set2 = set(tup[0][1]).union(set(tup[1][1]))
    return float(len(set1))/float(len(set2))


def cosineSimilarity(a,b, top1000):
    "Build word to vec"
    a_w = [a.get(str(word), 0) for word in top1000]
    b_w = [b.get(str(word), 0) for word in top1000]
    
    "compute cosine similarity of a to b: (a_vect dot b_vect)/{||a_vect||*||b_vect||}"
    num = np.dot(a_w,b_w)
    denom = norm(a_w)*norm(b_w)
    return float(num/denom)

def wordCount(wordListRDD):
    """Creates a pair RDD with word counts from an RDD of words."""
    return wordListRDD.map(lambda x: (x,1)).reduceByKey(lambda x, y: x + y)



#Create Spark Context

sc=pyspark.SparkContext()


#Read all the files from the directory and create RDD using wholeTextFiles() method

books=sc.wholeTextFiles("1880sfemalecorpus/*.txt");
bookRDD = books.mapValues(lambda x: x.replace('\n', '').split(" "))
bookWordsRDD = bookRDD.mapValues(lambda x: [x[i] for i in range(0, len(x)-1) if len(x[i]) > 3 ])


bookPairsRDD = bookWordsRDD.cartesian(bookWordsRDD)


bookJaccardRDD = bookPairsRDD.map(lambda x: (x[0][0],jaccardSimilarity(x)))


bookJaccardMatRDD = bookJaccardRDD.map(lambda (x,y): (x, [y])).reduceByKey(lambda p,q: p+q)


# In[80]:

bookJaccardMatrixRDD = bookJaccardMatRDD.map(lambda x: x[1])

#print bookJaccardMatrixRDD.take(5)



bookTopWordsRDD = bookRDD.mapValues(lambda x: [x[i] for i in range(0, len(x)-1) if len(x[i]) > 4 ])                         .flatMap(lambda x: x[1])

top1000WordsAndCounts =  wordCount(bookTopWordsRDD).takeOrdered(1000, lambda x: -x[1])
top1000WordsAndCounts[:10]
top1000Words = [top1000WordsAndCounts[i][0] for i in range(0, len(top1000WordsAndCounts) - 1)]

top1000Words[:10]

bookCosWordCountRDD = bookWordsRDD.mapValues(lambda x: collections.Counter(x))
bookCosPairsRDD = bookCosWordCountRDD.cartesian(bookCosWordCountRDD)
bookCosineRDD = bookCosPairsRDD.map(lambda x: (x[0][0],cosineSimilarity(x[0][1],x[1][1], top1000Words)))
bookCosineMatRDD = bookCosineRDD.map(lambda (x,y): (x, [y])).reduceByKey(lambda p,q: p+q)
bookCosineMatrixRDD = bookCosineMatRDD.map(lambda x: x[1])

#bookCosineRDD.take(4)

dataJaccard = np.array(bookJaccardMatrixRDD.take(451), dtype=float)
dataCosine = np.array(bookCosineMatrixRDD.take(451), dtype=float)

