import pyspark
from pyspark import SparkContext, SparkConf
from operator import add
import numpy as np

sc=pyspark.SparkContext()

#Input file names

file1="../a2data/b_200x100.txt"
file2="../a2data/a_100x200.txt";

# In[3]:

#Reading in the input data files and creating RDDs in appropriate format

mat = sc.textFile(file1)
matFilter = mat.map(lambda x: [float(i) for i in x.split(" ")])
mat2 = sc.textFile(file2)
matFilter2 = mat2.map(lambda x: [float(i) for i in x.split(" ")])


# In[4]:

#Finding the k value to be used for the one pass matrix multiplication approach
x=file2.replace(".txt","")
kval=x.split("x")[1]
isK=kval.find('K') 
#print isK

if(isK> 0):
    inter=kval.replace("K","")
    kvalue=int(inter)*1000
else:
    kvalue=int(kval)

print kvalue


# In[5]:

#Map and Reduce to obtain the necessary row and columns to be multiplied
matOnePass = matFilter.flatMap(lambda x : [((int(x[0]), k), (x)) for k in range(0, kvalue)])
matOnePasslist = matOnePass.map(lambda x: (x[0], [x[1][2]])).reduceByKey(lambda p,q: p+q).sortByKey(True)


# In[6]:

#Map and Reduce to obtain the necessary row and columns to be multiplied
matOnePass2 = matFilter2.flatMap(lambda x : [((k, int(x[1])), (x)) for k in range(0, kvalue)])
matOnePasslist2 = matOnePass2.map(lambda x: (x[0], [x[1][2]])).reduceByKey(lambda p,q: p+q).sortByKey(True)


# In[7]:

#Combining the two RDDs
matCartOP = matOnePasslist.join(matOnePasslist2).sortByKey(True)


# In[8]:

#Actual matrix multiplication
import numpy as np
matmul = matCartOP.map(lambda x: (x[0], np.dot(x[1][0], x[1][1]))).sortByKey(True)


# In[9]:

#Saving the results to text file
matmul.saveAsTextFile("MatrixMultOnePass200x200.txt")


# In[ ]:



