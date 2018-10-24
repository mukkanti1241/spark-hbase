# Script will invoke setting up all environment variables and invoking spark-submit job
# Usage 
# spark_hbasse_ld.sh <working directory> <Environment> <Python file>
#
#!/bin/bash
# Validating number of arguments are valid or not
if [ "$#" -ne 3 ]
then
    echo "=============Usage: spark_hbase_ld.sh <working directory> <Environment> <Python File>   =================="
    exit 3
fi
# Variable initilization
start_time=`date '+%Y-%m-%d %H:%M:%S'`
echo " Job execution started at "$start_time
#current working directory and environment variables initializing
cwd=$1  
env=$2
python_file=$3
param_file=$cwd"/param/"$env"_params.txt"
# Exporting application specific variables
    source $param_file
# End of exporting application specific variables
# Subitting spark-submit job 
echo " Spark Job is submitting as spark-submit -master $master_url --jars $shc_jars  --files $hbase_site_path $python_file $cwd $env"
 exec spark-submit --master $master_url --jars $shc_jars  --files $hbase_site_path $python_file $cwd $env
   
# Validating job is success or failure and sending email to support team
if [ "$?" -ne 0 ]
then
    echo "Job Failed" | mail -s "Job Failed" someone@somewhere.com
        exit 3
fi
end_time=`date '+%Y-%m-%d %H:%M:%S'`
echo "======Job is successfuly completed at "$end_time
echo "======End of Job========"
