from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType,StructField,StringType,DateType,IntegerType

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("File Streaming Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()

    custinv_schema = StructType([StructField("invno",IntegerType()),  \
                            StructField("invdate",DateType()),  \
                            StructField("invamt",IntegerType()),  \
                            StructField("custid",StringType()),  \
                            StructField("city",StringType()) ])

    inv_df = spark.readStream \
        .format("csv") \
        .option("path", "input/*.txt") \
        .schema(custinv_schema) \
        .option("maxFilesPerTrigger", 1) \
        .load()

   
    invoiceWriterQuery = inv_df.writeStream \
        .format("json") \
        .queryName("Invoice Writer") \
        .outputMode("append") \
        .option("path", "output") \
        .option("checkpointLocation", "chk-point-dir") \
        .trigger(processingTime="15 seconds") \
        .start()
    print("Looking for invoices.......")
    invoiceWriterQuery.awaitTermination()
