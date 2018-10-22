# spark-hbase

This Package mainely describes Integrating Spark with Hbase and HDFS
Job deals with Extraction,Transformation and Loading data
Extraction: Data can be loaded from local or HDFS parquet files.
Transformation: All transformation logic performed
Loading: Finally data will be loaded into HDFS or Hbase

Depedencies:
Spark(Python/Scala)
Hadoop cluster
Hbase
SHC(spark hadoop connector)(I took hortonworks SHC jars for my integration)

Follow the below execution steps:
Package contains diffrent folders "params","python","sampledata","shell".

params: contains environment files( please refer folders)
python: contains python files
sampledata: contains sample source files for execution
shell: contains unix shell scripts to trigger our jobs

If you submit shell script job which automatically triggers spark-submit job and updates you.

#./spark_hbase_ld.sh <working directory> <Environment> <Python file>
 ./spark_hbase_ld.sh  /spark-hbase/ "dev" SparkHbaseLd.py
  
  in the above SparkHbaseLd.py contains original logic. Please refer the samae.
  
  # End 
