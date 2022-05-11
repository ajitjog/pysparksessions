import sys
from pyspark.sql import SparkSession

from pyspark.sql.types import StructType,StructField,StringType,DateType,IntegerType

from pyspark.sql.functions import col,expr

spark = SparkSession.builder.master("local").appName("temptabledemo").enableHiveSupport().getOrCreate()

#spark.SparkContext.getConf().set()
custinv_schema = StructType([StructField("invno",IntegerType()),  \
                            StructField("invdate",DateType()),  \
                            StructField("invamt",IntegerType()),  \
                            StructField("custid",StringType()),  \
                            StructField("city",StringType()) ])


custinv_df = spark.read \
                .option("header",True) \
                .schema(custinv_schema) \
                .csv(sys.argv[1])

spark.sql("create database if not exists mydb")

spark.catalog.setCurrentDatabase("mydb")

custinv_df.write \
        .mode("overwrite") \
        .saveAsTable("tblinvoices")

