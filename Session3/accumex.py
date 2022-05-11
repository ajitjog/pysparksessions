from pyspark import SparkConf, SparkContext


conf=SparkConf().setMaster("local").setAppName("accumapp")

sc = SparkContext(conf=conf)

rdd= sc.range(1,10)

accuSum=sc.accumulator(0)
def countFun(x):
    print("adding",x)
    global accuSum
    accuSum+=x
rdd.foreach(countFun)

print(accuSum.value)
