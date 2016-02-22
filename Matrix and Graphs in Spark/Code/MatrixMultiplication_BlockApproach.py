
# coding: utf-8

# In[3]:

#Specify the input file names here
file1 = "a_100x200.txt"
file2 = "b_200x100.txt"


# In[4]:


#Parsing the input files to determine m,n and p values. The two matrices are of size mxn and nxp
x=file2.replace(".txt","")
y=file1.replace(".txt","")
m=int(y.split("x")[0].split("_")[1])
n=int(x.split("x")[0].split("_")[1])
kval=x.split("x")[1]

isK=kval.find('K') 
#print isK

if(isK> 0):
   inter=kval.replace("K","")
   p=int(inter)*1000
else:
   p=int(kval)


# In[3]:

#Creating RDDs for the input matrices
mat = sc.textFile(file1)
mat2 = sc.textFile(file2)
matFilter = mat.map(lambda x: [float(i) for i in x.split(" ")])
matFilter2 = mat2.map(lambda x: [float(i) for i in x.split(" ")])


# In[4]:

#Defining the block sizes by parameters s,t and v. Again the blocks are sxt and txv

s = 10
t = 20
v = 5
matBlocks = matFilter.flatMap(lambda x : [((int(x[0]/s), int(x[1]/t), k), (int(x[0]%s), int(x[1]%t), x[2])) for k in range(0, p/v)])


# In[5]:

matBlocks2 = matFilter2.flatMap(lambda x : [((k, int(x[0]/t), int(x[1]/v)), (int(x[0]%t), int(x[1]%v), x[2])) for k in range(0, m/s)])


# In[6]:

matOnePassBlock = matBlocks.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda p,q: p+q)
matOnePassBlock2 =  matBlocks2.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda p,q: p+q)


# In[7]:

matBlockCartOP = matOnePassBlock.join(matOnePassBlock2).sortByKey(True)


# In[8]:

def multiply(x):
    listA = x[1][0]
    listB = x[1][1]
    fDict = {}
    for a in listA:
        for b in listB:
            print a, b
            if a[1] == b[0]:
                fDict[(a[0], b[1])] = fDict.get((a[0], b[1]),0)+a[2]*b[2]
    print fDict
    finalList = []
    for key, value in fDict.iteritems():
        finalList.append(((x[0][0]*s + key[0], x[0][2]*v + key[1]), value))
    return finalList


# In[9]:

matBlockMul = matBlockCartOP.flatMap(multiply).sortByKey(True)


## In[10]:
#
#def secondPass(values):
#    result = 0
#    for value in values:
#        result += value
#    return result  
#
#
## In[11]:
#
#matSecPass = matBlockMul.map(lambda x: (x[0], [x[1]])).reduceByKey(lambda p,q: p+q)
#matFinalRes = matSecPass.map(lambda x: (x[0], secondPass(x[1])))


# In[1]:

matBlockMul.saveAsTextFile("MatrixMatrixMultiplicationBlock100_100.txt")

