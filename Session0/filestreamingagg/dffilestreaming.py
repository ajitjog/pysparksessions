from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType,StructField,StringType,DateType,IntegerType


custord_schema = StructType([StructField("orderno",IntegerType()),  \
                            StructField("custno",StringType()),  \
                            StructField("city",StringType()),  \
                            StructField("orderamt",IntegerType()) ])
#if this script is executed as a program
# and not imported as a module
#

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("GroupByDemo") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .master("local[2]") \
        .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions",1)

    
    cust_df = spark.readStream \
        .format("csv") \
        .option("path","input") \
        .option("header", "true") \
        .schema(custord_schema) \
        .option("maxFilesPerTrigger", 1) \
        .load()
    

    cust_grp_df = cust_df.groupBy("custno") \
                    .agg(f.sum("orderamt").alias("totalbiz"),f.count("orderno").alias("#orders"))


    file_query = cust_grp_df.writeStream \
        .format("console") \
        .outputMode("update") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="15 seconds") \
        .start()

    print("Waiting for orders......")
    file_query.awaitTermination()