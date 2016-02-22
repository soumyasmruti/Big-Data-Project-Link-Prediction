import pyspark
from pyspark import SparkContext, SparkConf
from operator import add
import numpy as np

sc=pyspark.SparkContext()
sgMat = sc.textFile("../a2_graphs/Assign2_100.txt")
sgMatFilter = sgMat.map(lambda x: [int(i) for i in x.split(" ")])


# In[113]:

#Finding k value
max_inter=sgMatFilter.max()
kvalue=max(max_inter) + 1
print kvalue


# In[114]:

liste = [range(0,kvalue), range(0, kvalue)]
finalList = []
import itertools
for element in itertools.product(*liste):
    finalList.append((element, 0))
#finalList[:10]


# In[115]:

wholeRDD = sc.parallelize(finalList)


# In[116]:

sgMatTupFilter = sgMatFilter.map(lambda x: ((x[0], x[1]), x[2]))


# In[117]:

AMatrix=sgMatTupFilter.union(wholeRDD)


# In[118]:

MatrixA=AMatrix.map(lambda x:(x[0],x[1])).reduceByKey(lambda p,q:p+q).sortByKey(True)


# In[119]:

MatrixAPrime=MatrixA.map(lambda x:(x[0][0],[x[1]])).reduceByKey(lambda p,q:p+q).sortByKey(True)


# In[120]:

MatrixAPrimePlus=MatrixA.map(lambda x:(x[0][1],[x[1]])).reduceByKey(lambda p,q:p+q).sortByKey(True)


# In[121]:

MatMult=MatrixAPrime.cartesian(MatrixAPrimePlus)


# In[122]:

import numpy as np

def mult(list1,list2):
    prod=0
    for i in range(0, len(list1)):
        for j in range(0,len(list2)):
            if(i==j):
                prod = prod + (list1[i] * list2[j])
                #print prod
                
    return prod           
ASquare= MatMult.map(lambda x: ((x[0][0],x[1][0]),np.dot(x[0][1],x[1][1]))).sortByKey(True)
#ASquare= MatMult.map(lambda x: ((x[0][0],x[1][0]),mult(x[0][1],x[1][1]))).sortByKey(True)


# In[123]:

AFinal=MatrixA.map(lambda x:((x[0][0],x[0][1]),x[1]))
#AFinal.take(105)


# In[124]:

ASquarePlusA=ASquare.union(AFinal)
#ASquarePlusA.take(200)


# In[125]:

FinalResult=ASquarePlusA.map(lambda x:(x[0],x[1])).reduceByKey(lambda p,q:p+q).sortByKey(True).cache()



# In[129]:

ShallowGraphCheck=FinalResult.map(lambda x: (x[0][0],[x[1]])).reduceByKey(lambda p,q:p+q).sortByKey(True)
#ShallowGraphCheck.take(10)


# In[131]:

def isShallow(list):
    for i in list:
        if list[i]== 0:
            flag=0
            break;
        else:
            flag=1
    return flag       


# In[132]:

ShallowGraphCheckValues=ShallowGraphCheck.values().map(isShallow)


# In[133]:

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





