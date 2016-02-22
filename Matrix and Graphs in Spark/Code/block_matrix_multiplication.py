
import pyspark
from pyspark import SparkContext, SparkConf
from operator import add
import numpy as np


#User defined functions

def replaceText(line):
    line=line.replace("array([ ","");
    line=line.replace("])","");
    return float(line);



#Creating Spark Context

sc=pyspark.SparkContext();


# #### Block Matrix-Matrix Multiplication 

# In[244]:

s = 10
t = 20 
v = 5



mat = sc.textFile("a2data/a_10Kx2K.txt")


# In[133]:

matFilter = mat.map(lambda x: [float(i) for i in x.split(" ")])



# In[231]:

mat2 = sc.textFile("a2data/b_2Kx10K.txt")
matFilter2 = mat2.map(lambda x: [float(i) for i in x.split(" ")])
matgroupp = matFilter.map(lambda x: (x[0]/s, [x[2]])).reduceByKey(lambda p,q: p+q)
matgroup2 = matFilter2.map(lambda x: (x[1]/v, [x[2]])).reduceByKey(lambda p,q: p+q)
matInter = matgroupp.cartesian(matgroup2)
matmul = matInter.map(lambda x: ((x[0][0]*s, x[1][0]*v), np.dot(x[0][1], x[1][1]))).sortByKey(True)


# In[232]:

matmul.saveAsTextFile("results/MatrixMatrixMultiplicationBBB10k2k_2k10k.txt")

