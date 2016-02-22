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

#Loading the input matrix file
mat = sc.textFile("../a2data/a_10Kx2K.txt",4)

#Converting string to floats and splitting based on space
matFilter = mat.map(lambda x: [float(i) for i in x.split(" ")])

#Mapping key value pair in the form (index, value) or (i, Mij)
matgroup = matFilter.map(lambda x: (x[0], [x[2]])).reduceByKey(lambda p,q: p+q)

#Loading the input vector file and creating a RDD and key value pair 
#of the form (1, value)
vect = sc.textFile("../a2data/x_2K.txt",4).map(lambda x: (1, [replaceText(x)])).reduceByKey(lambda p,q: p+q)

#Performing matrix vector multiplication by forming the cartesian and 
#performing the multiplication
matvec = matgroup.cartesian(vect)
matvecmul = matvec.map(lambda x: (int(x[0][0]), np.dot(x[0][1], x[1][1]))).sortByKey(True)

print matvecmul.take(2)

#Saving the result to a text file in the format (index, value) since this
#would be a scalar

matvecmul.saveAsTextFile("MatrixVectorMultiplication10kx2k_2k.txt")







