import os
import sys
from operator import add

# Path for spark source folder
os.environ['SPARK_HOME']="/Users/Anirudh/Desktop/Software/spark-1.2.1"

# Append pyspark  to Python Path
sys.path.append("/Users/Anirudh/Desktop/Software/spark-1.2.1/python")

try:
    from pyspark import SparkContext
    from pyspark import SparkConf

    print ("Successfully imported Spark Modules")

except ImportError as e:
    print ("Can not import Spark Modules", e)
    sys.exit(1)
    
    
sc = SparkContext('local')
# words = sc.parallelize(["scala","java","hadoop","spark","akka"])
# print words.count()  

lines = sc.textFile("Sherlock.txt", 1)
counts = lines.flatMap(lambda x: x.split(' ')) \
			  .map(lambda x: (x, 1)) \
			  .reduceByKey(add)
output = counts.collect()
for (word, count) in output:
	print("%s: %i" % (word, count))  