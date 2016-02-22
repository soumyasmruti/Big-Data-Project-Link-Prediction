import pyspark
from pyspark import SparkContext, SparkConf
from operator import add
import numpy as np

sc=pyspark.SparkContext()

dataRDD = sc.wholeTextFiles("/user/wikidata/p00/*")

from xml.dom import minidom
import re

def xml_parsing(fileName):
    
    text_content = fileName.encode('utf-8')

    # Regular expressions to find all the links and text from XML files and storing them in two lists
    TITLE_RE = re.compile(r'<title>\s*(.+)\s*<\/title>')
    LINK_RE = re.compile(r'\[\[([^\]]+)\]')
    TEXT_RE = re.compile(r'<text.+>\s*(.+)\s*<\/text>')

    #print 'Links:' , LINK_RE.findall(text_content)
    #print 'Text: ', TEXT_RE.findall(text_content)
    title = TITLE_RE.findall(text_content)
    #Creating a tuple of links, text where each of them is a list
    tup = (LINK_RE.findall(text_content), TEXT_RE.findall(text_content))

    return " ".join(title), tup


# In[3]:

contents = dataRDD.values().map(xml_parsing)
linkRDD = contents.flatMap(lambda x: [x[0]])


# In[4]:

linkList = sorted(linkRDD.collect())
link = {}
for i, j in enumerate(linkList):
	link[j]=i
import json
with open('PageTitles.json', 'w') as fp:
    json.dump(link, fp)


# In[5]:

import numpy as np
import operator

nVector = [(0.15/len(link))]*len(link)
def documentTermVector(i, fileLinks):
    vecList = [0.0]*len(link)
    m = 0
    print len(fileLinks)
    for each in fileLinks:
        if "|" in each:
            x = each.split("|")[1]
            try:
                if(link[x]):
                    m += 1
            except KeyError:
                continue
        else:
            try:
                print each
                if(link[each]+1):
                    print m
                    m += 1
            except KeyError:
                continue
    print m
    for each in fileLinks:
        if "|" in each:
            x = each.split("|")[1]
            try:
                vecList[link[x]] += 0.85/m
            except KeyError:
                continue
        else:
            try:
                vecList[link[each]] += 0.85/m
            except KeyError:
                continue
    tempList = map(operator.add, vecList,nVector)
    finallist = [[link[i], s, r] for s, r in enumerate(tempList)]
    return finallist


# In[6]:

tdmMatrix = contents.flatMap(lambda x: documentTermVector(x[0], x[1][0]))
colMat = tdmMatrix.map(lambda x: (x[1], [x[2]])).reduceByKey(lambda p,q:p+q)


# In[7]:

randomSurfer = sc.parallelize([(1.0/len(link))]*len(link)).map(lambda x: (0, [x])).reduceByKey(lambda p,q:p+q)
# tdmMatrix.take(2)
# documentTermVector(1, ['Amuck',
#                         'Amuck',                                           
# 'Amuck','Amuck',
#  'Symphony No. 9 (Beethoven)',
#  'Running amok',
#  'BiblicalInterpretation',
#  'Abiword',
#  'BirthofaNation',
#  'RUR-5 ASROC',
#  'Alexandria Troas',
#  'Benjamin Lee Whorf',
#  'Amnon'])


# In[8]:

cartRDD = colMat.cartesian(randomSurfer)                .map(lambda x: np.dot(x[0][1], x[1][1]))


# In[9]:

cart2RDD = colMat.cartesian(cartRDD.map(lambda x: (0, [x])).reduceByKey(lambda p,q:p+q))                .map(lambda x: np.dot(x[0][1], x[1][1]))


# In[12]:

def pageRank(vprime, vnewprime, c):
    c+=1
    norm = []
    norm = vprime.map(lambda x: (0, [x]))                 .reduceByKey(lambda p,q:p+q)                 .cartesian(vnewprime.map(lambda x: (0, [x])).reduceByKey(lambda p,q:p+q))                 .map(lambda x: np.linalg.norm((np.array(x[0][1]) - np.array(x[1][1])), ord = 2)).collect()
    n = float(norm[0])
    print "Norm is: ", c, n, type(n)
    if n <= 0.000001:
        pageRankList = vnewprime.collect()
        pageRankDict = {}
        for i, j in enumerate(pageRankList):
        	pageRankDict[j]=i
        with open('PageRankResults.json', 'w' ) as fp:
            json.dump(pageRankDict, fp) 
        return 0
    else:
        vNew = colMat.cartesian(vnewprime.map(lambda x: (0, [x]))                     .reduceByKey(lambda p,q:p+q))                     .map(lambda x: np.dot(x[0][1], x[1][1]))
        pageRank(vnewprime, vNew, c)


# In[13]:

pg = pageRank(cartRDD, cart2RDD, 0)


# In[14]:

#link["Brazil"]


# In[16]:

#print link.get(843) #843,877,916


# In[41]:

#link.keys()[link.values().index(915)]


# In[ ]:



