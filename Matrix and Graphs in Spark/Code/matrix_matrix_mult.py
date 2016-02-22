import pyspark
from pyspark import SparkContext, SparkConf
from operator import add
import numpy as np


#User defined functions

#To obtain only the data value from the vector input file.
def replaceText(line):
    line=line.replace("array([ ","");
    line=line.replace("])","");
    return float(line);


#Creating Spark Context

sc=pyspark.SparkContext();

#Loading the input matrix file
mat = sc.textFile("../a2data/a_100x200.txt",4)

#Converting string to floats and splitting based on space
matFilter = mat.map(lambda x: [float(i) for i in x.split(" ")])

#Mapping key value pair in the form (index, value) or (i, Mij)
matgroup = matFilter.map(lambda x: (x[0], [x[2]])).reduceByKey(lambda p,q: p+q)

#Loading the other matrix file  and mapping key value pair in the 
#form (index,value) or (i, Mij)

mat2 = sc.textFile("../a2data/b_200x100.txt",4)
matFilter2 = mat2.map(lambda x: [float(i) for i in x.split(" ")])
matgroup2 = matFilter2.map(lambda x: (x[1], [x[2]])).reduceByKey(lambda p,q: p+q)
matInter = matgroup.cartesian(matgroup2)
matmul = matInter.map(lambda x: ((int(x[0][0]),int(x[1][0])) ,(x[0][0], x[1][0], np.dot(x[0][1], x[1][1])))).sortByKey(True)


# In[65]:

matmul.map(lambda x: x[1]).saveAsTextFile("MatrixMultiplication100x200_200x100.txt")
