import sys
from pyspark.sql import SparkSession

from pyspark.sql.types import StructType,StructField,StringType,DateType,IntegerType

from pyspark.sql.functions import col,expr

spark = SparkSession.builder.master("local").appName("temptabledemo").enableHiveSupport().getOrCreate()

#print(spark.sparkContext.getConf().getAll())
spark.catalog.setCurrentDatabase("mydb")
tblinv = spark.sql("select * from tblinvoices")


tblinv.show()

#spark.catalog.listTables("mydb")