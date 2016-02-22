
import pyspark
from pyspark import SparkContext, SparkConf
from operator import add
import numpy as np

sc=pyspark.SparkContext()


sgMat = sc.textFile("../a2_graphs/Assign2_200.txt")
sgMatFilter = sgMat.map(lambda x: [int(i) for i in x.split(" ")])


# In[216]:

max_inter=sgMatFilter.max()
kvalue=max(max_inter) + 1
print kvalue


# In[217]:

#Creating a list/matrix with all zeroes
liste = [range(0,kvalue), range(0, kvalue)]
finalList = []
import itertools
for element in itertools.product(*liste):
    finalList.append((element, 0))


# In[218]:

#Creating a RDD out of the list
wholeRDD = sc.parallelize(finalList)
#wholeRDD.count()


# In[219]:

# RDD for the input data file
sgMatTupFilter = sgMatFilter.map(lambda x: ((x[0], x[1]), x[2]))
#sgMatTupFilter.take(10)


# In[220]:

#Union Operation to obtain all the rows or the full-adjacency matrix for the given graph
AMatrix=sgMatTupFilter.union(wholeRDD)


# In[221]:

MatrixA=AMatrix.map(lambda x:(x[0],x[1])).reduceByKey(lambda p,q:p+q).sortByKey(True)


# In[222]:

# Creating an identity matrix of the same size as the given input. 

liste = [range(0,kvalue), range(0, kvalue)]
finalList = []
import itertools
for element in itertools.product(*liste):
    if element[0]==element[1]:
        finalList.append((element, 1))
    else:   
        finalList.append((element, 0))
    
#finalList[:10]


# In[223]:

#Creating an RDD for the Identity Matrix
Identity=sc.parallelize(finalList)


# In[224]:

#Performing union to bring it to a single RDD to perform the addition operation
APlusI=MatrixA.union(Identity)


# In[225]:

# Performing the actual addtion by reducingByKey

APlusIValue=APlusI.map(lambda x: (x[0], x[1])).reduceByKey(lambda p,q:p+q).sortByKey(True)
APlusIValue.take(10)


# In[226]:

APlusIRDD=APlusIValue.map(lambda x: (x[0][0],[x[1]])).reduceByKey(lambda p,q:p+q).sortByKey(True)
ARDD=MatrixA.map(lambda x: (x[0][1],[x[1]])).reduceByKey(lambda p,q:p+q).sortByKey(True)
MatMult=ARDD.cartesian(APlusIRDD)


# In[ ]:

MatMultValue=MatMult.map(lambda x: ((x[0][0],x[1][0]),np.dot(x[0][1],x[1][1]))).sortByKey(True)


# In[210]:

MatMultValue.take(10)


# In[211]:

def isShallow(list):
    for i in list:
        if list[i]==0:
            flag=0
            break;
        else:
            flag=1
            
    return flag


# In[213]:


ShallowGraphCheck=MatMultValue.map(lambda x: (x[0][0],[x[1]])).reduceByKey(lambda p,q:p+q).sortByKey(True)
ShallowGraphCheckValues=ShallowGraphCheck.values().map(isShallow)
#ShallowGraphCheckValues.collect()


# In[214]:

for i in ShallowGraphCheckValues.collect():
    if i==0:
        flag=0
        break;         
    else:
        flag=1
if flag==0:
    print 'Given graph is not a shallow graph'
else:
    print 'Given graph is a shallow graph'


# In[ ]:



