from pyspark import SparkConf, SparkContext

from collections import namedtuple

accounts=namedtuple("account",["accno","accbal"])

conf=SparkConf().setMaster("local").setAppName("accumapp")

sc = SparkContext(conf=conf)

accrdd= sc.parallelize([accounts("a1",5000),accounts("a2",51000)])

accuSum=sc.accumulator(0)
def sumbals(a):
    global accuSum
    accuSum+=a.accbal
accrdd.foreach(sumbals)

print(accuSum.value)
