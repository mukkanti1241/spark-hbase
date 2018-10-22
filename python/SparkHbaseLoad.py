#
# The source file is open source and access to public
# Generar information about the file
# This file mainly contains 3 layers Extract,Transformation and Loading phases
# Extract is pulling data from local files or HDFS(parquet) files
# Transformation is doing some logic related transformations
# Loading layer is writing data to HDFS/Hbase.
# Finally results will be displayed on console
# Source file is available in my GitHub repository on https://github.com/mukkanti1241/spark-hbase
#
"""
You can run with the following command
Run with:
  ./bin/spark-submit  --jars <SHC connector and other dependent jars> --files <habse-site.xml file with location> <python source file> < run time arguements>
"""
# Required packages
import sys
from os import environ
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# function defination for extracting data from local files or HDFS parquet files
def extract_from_source(spark,srctype,src_filename):
    if srctype == "local":
        return spark.read.load("file://"+local_file_path+src_filename+".csv",format="csv", sep=delimmter, inferSchema="true")
    else:
        return spark.read.parquet("hdfs://"+hostname+":"+port+src_hdfs_path+src_filename+".parquet")

# function defination for performing certain business logic
def transformation_logic(df1,df2,df3):
    df4=df1.select(col("_c0").alias("First_NM"), col("_c1").alias("Last_NM"), col("_c2").alias("Act_ID") \
    ,col("_c3").alias("Income"), col("_c4").alias("Addr_ID")) \
    .where(col("Addr_ID").isNotNull() & col("Act_ID").isNotNull())
    df5=df2.select(col("_c0").alias("City"), col("_C1").alias("Pst_CD"), col("_c2").alias("Addr_ID")) \
    .where(col("Addr_ID").isNotNull())
    df6=df3.select(col("_c0").alias("Act_ID"), col("_c1").alias("Addr_ID"), col("_c2").alias("Tran_Amt") \
    ,col("_c3").alias("Tran_DT")).where(col("Addr_ID").isNotNull() & col("Act_ID").isNotNull()) \
    df4_jn_df5_jn_df6=df4.join(df5,(df4.Addr_ID==df5.Addr_ID), "inner").drop(df5["Addr_ID"]) \
    .join(df6,(df4.Act_ID==df6.Act_ID) &(df4.Addr_ID==df6.Addr_ID),inner") \
    .drop(df6["Act_ID"]).drop(df6["Addr_ID"])
    return df4_jn_df5_jn_df6

# function defination for writing data to hdfs
def write_to_hdfs (spark, df_target):
    df_target.write.parquet("hdfs://"+hostname+":"+port+target_hdfs_path+target_filename)

# function defination for writing data to hbase
def write_to_hbase(spark,df_target,catalog):
    df_target.write.options (catalog=catalog).format ('org.apache.spark.sql.execution.datasources.hbase').save ()

# function defination for reading data from hbase
def read_from_hbase(spark,catalog):
    return spark \
    .read \
    .options (catalog=catalog) \
    .format ('org.apache.spark.sql.execution.datasources.hbase') \
     load ()

# function defination for final output
def final_result(spark,df_hbase):
    df7=df_hbase.select("First_NM","Last_NM","Income","Addr_ID","City","Pst_CD","Tran_Amt",col("Tran_DT").cast("timestamp"), month("Tran_dT").alias("Month"))
    df7_filter=df7.filter(col("Income").between("100","150"))

    window=Window. \
        partitionBy('City', 'Month'). \
        orderBy(df7['First_NM'].asc())

    df8=df7.withColumn("Monthly_Spent", sum("Tran_Amt").over(window))
    df9=df.filter("Monthly_Spent=1000").drop("Month")
    return df9

if __name__ == "__main__":
    if len(sys.argv) != 3:
        exit(-1)
        print("Usage: CustomerTranLoad.py <working directory> <file type> <environment>")
    if sys.argv[1] == "dev":
        #Getting execution specific parameters for DEV environment
        hbase_namespace=environ.get("dev_hbase_namespace")
        hbase_table=environ.get("dev_hbase_table")
        hbase_cf_cust=environ.get("dev_hbase_cf_cust")
        hbase_cf_addr=environ.get("dev_hbase_cf_addr")
        hbase_cf_tran=environ.get("dev_hbase_cf_tran")
        hbase_cf_cust_col1=environ.get("dev_hbase_cust_cf_col1")
        hbase_cf_cust_col2=environ.get("dev_hbase_cf_cust_col2")
        hbase_cf_cust_col3=environ.get("dev_hbase_cf_cust_col3")
        hbase_cf_addr_col1=environ.get("dev_hbase_cf_addr_col1")
        hbase_cf_addr_col2=environ.get("dev_hbase_cf_addr_col2")
        hbase_cf_addr_col3=environ.get("dev_hbase_cf_addr_col3")
        hbase_cf_tran_col1=environ.get("dev_hbase_cf_tran_col1")
        hbase_cf_tran_col2=environ.get("dev_hbase_cf_tran_col2")
        delimmter=environ.get("dev_delimmter")
        hostname=environ.get("dev_hostname")
        port=environ.get("dev_port")
        local_file_path=environ.get("dev_local_file_path")
        src_hdfs_path=environ.get("dev_src_hdfs_path")
        target_hdfs_path=environ.get("dev_target_hdfs_path")
        filename_customer=environ.get("dev_filename_customer")
        filename_addr=environ.get("dev_filename_addr")
        filename_tran=environ.get("dev_filename_tran")

    else:
        # Getting execution specific parameters for DEV environment
       hbase_namespace=environ.get("prod_hbase_namespace")
       hbase_table=environ.get("prod_hbase_table")
       hbase_cf_cust=environ.get("prod_hbase_cf_cust")
       hbase_cf_addr=environ.get("prod_hbase_cf_addr")
       hbase_cf_tran=environ.get("prod_hbase_cf_tran")
       hbase_cf_cust_col1=environ.get("prod_hbase_cust_cf_col1")
       hbase_cf_cust_col2=environ.get("prod_hbase_cf_cust_col2")
       hbase_cf_cust_col3=environ.get("prod_hbase_cf_cust_col3")
       hbase_cf_addr_col1=environ.get("prod_hbase_cf_addr_col1")
       hbase_cf_addr_col2=environ.get("prod_hbase_cf_addr_col2")
       hbase_cf_addr_col3=environ.get("prod_hbase_cf_addr_col3")
       hbase_cf_tran_col1=environ.get("prod_hbase_cf_tran_col1")
       hbase_cf_tran_col2=environ.get("prod_hbase_cf_tran_col2")
       delimmter=environ.get("prod_delimmter")
       hostname=environ.get("prod_hostname")
       port=environ.get("prod_port")
       local_file_path=environ.get("prod_local_file_path")
       src_hdfs_path=environ.get("prod_src_hdfs_path")
       target_hdfs_path=environ.get("prod_target_hdfs_path")
       filename_customer=environ.get("prod_filename_customer")
       filename_addr=environ.get("prod_filename_addr")
       filename_tran=environ.get("prod_filename_tran")

# Initializing spark session
    spark = SparkSession \
    .builder \
    .appName ("Customer Transaction Load") \
    .config ("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate ()
# Initializing Logger object
    log4jLogger=spark._jvm.org.apache.log4j
    LOGGER=log4jLogger.LogManager.getLogger('MYLOGGER')
    LOGGER.setLevel (log4jLogger.Level.WARN)

    LOGGER.info("=============pyspark script logger initialized")
    LOGGER.info ("=============Extraction is initiated for Customer sample file")
    df_cust=extract_from_source(spark,sys.argv[1],filename_customer)#Extracts data from local or HDFS.Extacting data from customer samaple data

    LOGGER.info ("=============Extraction is finished for customer data and initiating extraction for Address sample file")
    df_addr=extract_from_source(spark,sys.argv[1],filename_addr)#Extracts data from local or HDFS.Extacting data from address samaple data

    LOGGER.info ("=============Extraction is finished for Address source and initiated for transaction sample file")
    df_tran=extract_from_source(spark,sys.argv[1],filename_tran)#Extracts data from local or HDFS.Extacting data from transaction samaple data
    LOGGER.info ("=============Extraction is finished for all 3 files")

    LOGGER.info ("=============Transformation logic is initiated")
    df_target=transformation_logic(df_cust,df_addr,df_tran)#Does required transformation from all 3 files
    LOGGER.info ("=============Finished Transformation execution")

    LOGGER.info ("=============Writing Data to HDFS is in progress")
    write_to_hdfs(spark,df_target)#Loads data to HDFS
    LOGGER.info ("=============Writing data to HDFS is completed")

    #Defifning mapping schema from Hbase
    catalog = ''.join ("""{
    "table":{"namespace":hbase_namespace, "name":hbase_table, "tableCoder":"PrimitiveType"},
        "rowkey":"key",
        "columns":{
             "key":{"cf":"rowkey", "col":"key", "type":"string"},
             hbase_cf_cust_col1:{"cf":hbase_cf_cust, "col":hbase_cf_cust_col1, "type":"string"},
             hbase_cf_cust_col2:{"cf":hbase_cf_cust, "col":hbase_cf_cust_col2, "type":"string"},
             hbase_cf_cust_col3:{"cf":hbase_cf_cust, "col":hbase_cf_cust_col3, "type":"long"},
             hbase_cf_addr_col1:{"cf":hbase_cf_addr, "col":hbase_cf_addr_col1, "type":"int"},
             hbase_cf_addr_col2:{"cf":hbase_cf_addr, "col":hbase_cf_addr_col2, "type":"string"},
             hbase_cf_addr_col3:{"cf":hbase_cf_addr, "col":hbase_cf_addr_col3, "type":"float"},
             hbase_cf_tran_col1:{"cf":hbase_cf_tran, "col":hbase_cf_tran_col1, "type":"string"},
             hbase_cf_tran_col2:{"cf":hbase_cf_tran, "col":hbase_cf_tran_col2, "type":"string"}
            }
        }""".split ())

    LOGGER.info ("=============Writing data to Hbase is in progress")
    write_to_hbase(spark,df_target,catalog)# Writing data to Hbase table
    LOGGER.info ("=============Writing ddata to Hbase is completed")

    df_hbase=read_from_hbase(spark,catalog)# Reading data from Hbase for required customers

    df=final_result(spark,df_hbase) # This is the final result with with required aggregated values
    df.show()# Displaying results on console
    spark.stop()# closing spark session
