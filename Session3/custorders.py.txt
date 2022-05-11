from pyspark import SparkConf, SparkContext


conf=SparkConf().setMaster("local").setAppName("custordersapp")

sc = SparkContext(conf=conf)

def loadcustomermaster():
        cmfile = open("C:/pysparkdemos/Session3/customers.txt","r")
        lines = cmfile.readlines()
        custs={}
        cmfile.close()
        for l in lines:
            fields = l.split(",")
            custs[fields[0]] = fields[1]
        return custs

customernames = sc.broadcast(loadcustomermaster())

custordersfile = sc.textFile("file:///C:/pysparkdemos/Session3/custorders1.txt")

custorders = custordersfile.map( lambda line: ( line.split(",")[1] ,  int(line.split(",")[3]) ) )

custorderamounts= custorders.reduceByKey(lambda amt1,amt2: amt1 + amt2  )

custorderamounts = custorderamounts.map(lambda c: (customernames.value[c[0]],c[1])   )

result = custorderamounts.collect()

for res in result:
    print(res)

