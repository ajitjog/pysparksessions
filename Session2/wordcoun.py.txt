#read a text file and count how many words are there...
#
#

#setMaster: determines where the spark program will run.
# local system (simulation of  1 node spark cluster)
# kubernetes cluster
# hadoop cluster
# Multi Node Spark Cluster
#
import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[2]").setAppName("wordcountapp")


def retrieveWords(line):
    return re.compile(r'\W+',re.UNICODE).split(line.lower())


sc= SparkContext(conf=conf)

linesrdd = sc.textFile("file:///c:/pysparkdemos/*.txt")

wordslines = linesrdd.flatMap(retrieveWords)

wordMaps = wordslines.map(lambda line: (line,1))

wordMaps =wordMaps.reduceByKey(lambda l1,l2: l1+l2)

wordMapsFlipped = wordMaps.map(lambda wt: (wt[1],wt[0])).sortByKey(False)

results = wordMapsFlipped.take(5)

for result in results:
    print(result[1]," is occurring ",result[0]," times")








