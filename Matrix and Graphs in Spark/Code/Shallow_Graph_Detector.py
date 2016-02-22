import pyspark
from pyspark import SparkContext, SparkConf
from operator import add
import numpy as np

sc=pyspark.SparkContext()
sgMat = sc.textFile("../a2_graphs/Assign2_1000.txt")
sgMatFilter = sgMat.map(lambda x: [int(i) for i in x.split(" ")])


# In[130]:

max_inter=sgMatFilter.max()
kvalue=max(max_inter) + 1
print kvalue


# In[131]:

sgFinal2 = sgMatFilter.map(lambda x: (x[1], [{x[0]: x[2]}]))                      .reduceByKey(lambda p,q: p+q)                      .sortByKey(True) 
#sgFinal2.take(10)            


# In[132]:

sgFinal = sgMatFilter.map(lambda x: (x[0], [{x[1]:x[2]}]))                     .reduceByKey(lambda p,q: p+q)                     .sortByKey(True)
#sgFinal.take(10) 


# In[133]:

sgJoined = sgFinal.cartesian(sgFinal2)


# In[134]:

def multiply(list1,list2):
    prod=0
    for i in list1:
        if i in list2:
                prod=prod+1
    
    return prod

ASquare= sgJoined.map(lambda x: ((x[0][0],x[1][0]),multiply(x[0][1],x[1][1]))).cache()


# In[135]:

#ASquare.take(10)


# In[136]:

A = sgMatFilter.map(lambda x: ((x[0], x[1]), x[2]))


# In[137]:

ASquarePlusA=ASquare.union(A)
ASquarePlusAValue=ASquarePlusA.map(lambda x: (x[0],x[1])).reduceByKey(lambda p,q:p+q).sortByKey(True).cache()
ASquarePlusAValue.count()


# In[138]:

#ASquarePlusAValue.take(101)


# In[139]:

ShallowGraphCheck=ASquarePlusAValue.map(lambda x: (x[0][0],[x[1]])).reduceByKey(lambda p,q:p+q).sortByKey(True)
#ShallowGraphCheck.take(100)
#ShallowGraphCheck.count()


# In[140]:

if ASquarePlusAValue.count() != (kvalue*kvalue):
    print 'Given graph is not a shallow graph'
else:
    print 'Given graph is a shallow graph'


# In[ ]:




# In[ ]:



