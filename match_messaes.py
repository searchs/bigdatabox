#!/usr/bin/env python3
from __future__ import print_function

# Important info:  Apache Spark MUST be available on machine (Jenkins Slave)

import os
import sys
from stat import *
from datetime import datetime 

os.system("pip3 install -r requirements.txt --user")

import findspark
findspark.init()

import pyspark
from pyspark import SparkContext, SparkConf

run_date = datetime.today().strftime('%Y%m%d')

primary = sys.argv[1]
secondary = sys.argv[2]

'''
Compares 2 extract files contents
'''
def main():
    conf = (SparkConf()
                .setMaster("local")
                .setAppName("compare_engine"))
                
    sc = SparkContext(conf = conf)
    # sc = SparkContext(appName='compare_engine')
    sc.setLogLevel('INFO')

    raw_primary = sc.textFile(primary)                                                  
    raw_secondary = sc.textFile(secondary)

    # debug info
    print(raw_primary.getNumPartitions())
    print(raw_secondary.getNumPartitions())

    primary_count = raw_primary.count()
    print(primary_count)

    secondary_count = raw_secondary.count()
    print(secondary_count)

    # Returns the number of records in PRIMARY not in found SECONDARY
    notCompRecords  = raw_primary.subtract(raw_secondary)
    notCompRecords.count()

    # notCompRecords.collect()
    rev_diff_records = raw_secondary.subtract(raw_primary)

    # Returns number of records in SECONDARY file not found in PRIMARY
    print(rev_diff_records.count())
    os.system('rm -Rf collects_*.csv')
    notCompRecords.repartition(1).saveAsTextFile('collects_{}.csv'.format(run_date))

    os.system('cat collects_{}.csv/part-00000 > collect_{}_report.csv'.format(run_date, run_date))
    os.system('wc -l collect_{}_report.csv'.format(run_date))

    sc.stop()

    # Manifest content
    os.system('touch manifest_file.csv')
    # print("Details of file: {}".format(primary))
    primary_stats = os.stat(primary)
    print(os.system(''))

    # print(primary_stats)
    # print("Details of file: {}".format(secondary))

    secondary_stats = os.stat(secondary)
    # print(secondary_stats)


if __name__ == "__main__":
    main()
