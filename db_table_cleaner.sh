
#!/bin/bash
#ADHOC Queries:  We don't need these tables once reports are verified OK by analysts
# Default:  Cleans upto 30 days worth of process tables
# lookback:  Go back this number of days to cleanup report tables.
# Usage: sh ./run_db_cleanup.sh -l 30

echo "Adhoc Table Cleanup starts...."
echo $(date --utc)

while getopts l: option
do
 case "${option}"
 in
 l) lookback=${OPTARG};;
 esac
done

process_date=$(date --utc +'%Y%m%d')

echo -e "Process Date: $process_date \n"
echo -e "Lookback Days: $lookback \n"

#while loop using the lookback as the limit
COUNTER=0
         while [  $COUNTER -le $lookback ]; do
             echo The counter is $COUNTER
             cleanup_date=$(date --utc -d "$process_date - $COUNTER days" +'%Y%m%d')
             echo "Cleanup Date: $cleanup_date"
             hive -hiveconf cleanup_date=$cleanup_date -f /app/$USER/scripts/adhocs/db_cleanup.hql
             let COUNTER=COUNTER+1
         done
echo -e "================================================================\n"
echo -e "End of Cleanup of Reports Redundant tables. $(date --utc) \n"
echo -e "================================================================\n"
