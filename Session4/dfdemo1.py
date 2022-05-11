from pyspark.sql import SparkSession
from pyspark.sql import functions as f

#if this script is executed as a program
# and not imported as a module
#

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("GroupByDemo") \
        .master("local[2]") \
        .getOrCreate()

    spark.conf.set("spark.sql.shuffle.partitions",1)

    cust_df = spark.read \
        .format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load("custorders*.txt")
    

    cust_grp_df = cust_df.groupBy("custno") \
                    .agg(f.sum("orderamt").alias("totalbiz"),f.count("orderno").alias("#orders"))
    cust_grp_df.show()

    cust_grp_df.write \
        .format("csv") \
        .mode("append") \
        .option("path", "grpdata/") \
        .save()