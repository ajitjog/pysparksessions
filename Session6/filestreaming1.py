import sys
from pyspark.sql import SparkSession

from pyspark.sql.types import StructType,StructField,StringType,DateType,IntegerType

from pyspark.sql.functions import col,expr

spark = SparkSession.builder.master("local").appName("schemademo"). \
        config("spark.streaming.stopGracefullyOnShutdown", "true").getOrCreate()

#spark.SparkContext.getConf().set()
custinv_schema = StructType([StructField("invno",IntegerType()),  \
                            StructField("invdate",DateType()),  \
                            StructField("invamt",IntegerType()),  \
                            StructField("custid",StringType()),  \
                            StructField("city",StringType()) ])

cust_schema = StructType([StructField("custid",StringType()),  \
                          StructField("custname",StringType()),  \
                          StructField("creditbal",IntegerType())  ])


custinv_df = spark.readStream. \
             format("csv"). \
             option("header",True). \
             option("path","input"). \
             schema(custinv_schema). \
             option("dateFormat","dd/MM/yyyy"). \
             load()


cust_df = spark.read \
            .format("csv") \
            .option("header",True) \
            .schema(cust_schema) \
            .load("customers.txt")

#print(custinv_df.schema.simpleString())


custinv_df2 = custinv_df.withColumn("InvPaymentBy",col("invdate") + 15). \
                         withColumn("discount",custinv_df.invamt * 0.1 ). \
                         withColumn("invcat",expr("case when invamt >= 50000 then 'A' else 'B' end"))

custinv_df2 = custinv_df2.join(cust_df, cust_df.custid == custinv_df2.custid).drop(cust_df.custid)

#fileQuery = custinv_df2.writeStream \
#            .format("json") \
#            .outputMode("append") \
#            .partitionBy("city") \
#            .option("checkpointlocation","chkdir") \
#            .trigger(processingTime = "15 seconds") \
#            .option("path","output/") \
#            .start() 

consoleQuery = custinv_df2.writeStream \
                .format("console") \
                .outputMode("append") \
                .partitionBy("city") \
                .option("checkpointlocation","chkdir") \
                .trigger(processingTime = "15 seconds") \
                .start() 
print("Waiting for invoices to arrive........")
#fileQuery.awaitTermination()
consoleQuery.awaitTermination()

