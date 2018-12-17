#!/bin/bash
# process_date:  For file uploads and S3 operations
# table_date: For database (Hive table) operations.  This is usually set to 2 days before process_date
# Usage: sh ./main_etl.sh -d 20181217 -l -205 -f bigquery_orders.csv

echo "Adhoc Processing starts...."
echo date

while getopts d:l:f option 
do 
 case "${option}" 
 in 
 d) RUNDATE=${OPTARG};; 
 l) LOOKBACK=${OPTARG};; 
 f) FILENAME=${OPTARG};; 
 esac 
done 
 
echo "Date:"$RUNDATE 
echo "Lookback:"$LOOKBACK
echo "Filename:"$FILENAME

table_date=$(date -d "$RUNDATE -2 days" +'%Y%m%d')
process_date=$RUNDATE

lookback=$LOOKBACK
filename=$FILENAME

echo "Process Date: $process_date"
echo "Table Date: $table_date"
echo "File name: $filename"
echo "Lookback Days: $lookback"

spark-submit --deploy-mode client --executor-memory 4G --num-executors 20 --py-files /usr/bin/pyspark /home/$USER/scripts/adhocs/ingest_data.py -d=$process_date  -t=$table_date -f=$filename

hive -hiveconf day=$table_date -hiveconf lookback=$lookback -f /app/$USER/scripts/adhocs/adhoc_main_etl.hql

spark-submit --deploy-mode client --executor-memory 4G --num-executors 20 --py-files /usr/bin/pyspark /home/$USER/scripts/adhocs/finalise.py -d=$process_date -t=$table_date -f=$filename
