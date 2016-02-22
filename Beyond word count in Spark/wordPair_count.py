'''Ten frequently occurring word pairs program implemented in PySpark
Input file: Sherlock.txt
Filter Conditions: Each of the word in the word pair is of length > 4
Output : 10 most commonly occurring word pairs, sorted based on count in descending order'''


import os
import sys
import pyspark
from pyspark import SparkConf, SparkContext
from operator import add 
 

'''Removes punctuation, changes to lower case, and strips leading and trailing spaces.'''
import re
def removePunctuation(text):
	
	text = re.sub(r'[^\w\s]','',text)
	return text.lower().strip()


# Creating a Spark Context

sc=pyspark.SparkContext();

'''Read the input file "Sherlock.txt", remove punctuation and perform the word pair count
using map,flatMap,reduce and filter functions'''

lines = (sc.textFile("sherlock.txt",8).map(removePunctuation))
wordsRDD= lines.map(lambda l :l.split(" "));
biGramsRDD=wordsRDD.flatMap(lambda word : [(word[i],word[i+1]) for i in range(0, len(word)-1)])
filteredbiGrams=biGramsRDD.filter(lambda w: len(w[0]) > 4  and len(w[1]) > 4)
requiredbiGrams=filteredbiGrams.map(lambda w : (w,1)).reduceByKey(lambda x,y :x+y)
#print requiredbiGrams.take(10)
result=requiredbiGrams.takeOrdered(10, lambda x: -x[1])


#print the result 

for (word, count) in result:
	print("%s: %i" % (word, count))
	
	
	
# Results 	
#Without removing the punctuation
	
''' (u'Prince', u'Andrew'): 631
(u'United', u'States'): 229
(u'Prince', u'Andrew,'): 163
(u'Prince', u'Vasili'): 140
(u'Prince', u'Andrew.'): 97
(u'Project', u'Gutenberg-tm'): 86
(u'Prince', u"Andrew's"): 76
(u'United', u'States,'): 68
(u'takes', u'place'): 67
(u'Project', u'Gutenberg'): 66 '''


# Removing the punctuation

'''(u'prince', u'andrew'): 907
(u'united', u'states'): 392
(u'prince', u'vasili'): 178
(u'project', u'gutenbergtm'): 104
(u'project', u'gutenberg'): 99
(u'sherlock', u'holmes'): 99
(u'mademoiselle', u'bourienne'): 91
(u'takes', u'place'): 90
(u'prince', u'andrews'): 79
(u'marya', u'dmitrievna'): 76'''