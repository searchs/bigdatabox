#!/usr/bin/env python
import sys
import os
import re
import pandas as pd
# import modin.pandas as pd #replcaing basic Pandas with faster modin

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import udf

import glob


sc = SparkContext()
sqlContext = SQLContext(sc)
spark = SparkSession(sc)

raw_data =  glob.glob("data/*.log")

df = spark.read.text(raw_data)
df.printSchema()


df.show(5, truncate=False)

sample_logs = [item['value'] for item in df.take(15)]

# EXTRACT HOSTS
host_pattern = r'(^\S+\.[\S+\.]+\S+)\s'
hosts = [re.search(host_pattern, item).group(1)
           if re.search(host_pattern, item)
           else 'no match'
           for item in sample_logs]

# EXTRACT TIMESTAMPS
ts_pattern = r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
timestamps = [re.search(ts_pattern, item).group(1) for item in sample_logs]

# EXTRACT HTTP METHODS/PROTOCOLS
method_uri_protocol_pattern = r'\"(\S+)\s(\S+)\s*(\S*)\"'
method_uri_protocol = [re.search(method_uri_protocol_pattern, item).groups()
               if re.search(method_uri_protocol_pattern, item)
               else 'no match'
              for item in sample_logs]

# EXTRACT STATUS CODES
status_pattern = r'\s(\d{3})\s'
status = [re.search(status_pattern, item).group(1) for item in sample_logs]

# EXTRACT HTTP RESPONSE CONTENT SIZE
content_size_pattern = r'\s(\d+)$'
content_size = [re.search(content_size_pattern, item).group(1) for item in sample_logs]


# COMBINED ALGO
from pyspark.sql.functions import regexp_extract

logs_df = df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                         regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                         regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                         regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                         regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                         regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                         regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))
logs_df.show(10, truncate=True)
print((logs_df.count(), len(logs_df.columns)))


# CHECK NULL COLUMN COUNT
(df.filter(df['value'].isNull()).count())

bad_rows_df = logs_df.filter(logs_df['host'].isNull()|
                             logs_df['timestamp'].isNull() |
                             logs_df['method'].isNull() |
                             logs_df['endpoint'].isNull() |
                             logs_df['status'].isNull() |
                             logs_df['content_size'].isNull()|
                             logs_df['protocol'].isNull())
bad_rows_df.count()

# GET COLUMNS WITH NULLS
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as spark_sum

def count_null(col_name):
    return spark_sum(col(col_name).isNull().cast('integer')).alias(col_name)

# Build up a list of column expressions, one per column.
exprs = [count_null(col_name) for col_name in logs_df.columns]

# Run the aggregation. The *exprs converts the list of expressions into
# variable function arguments.
logs_df.agg(*exprs).show()


# HANDLE NULLS IN COLUMN - HTTP STATUS
regexp_extract('value', r'\s(\d{3})\s', 1).cast('integer').alias( 'status')

null_status_df = df.filter(~df['value'].rlike(r'\s(\d{3})\s'))
null_status_df.count()

null_status_df.show(truncate=False)

bad_status_df = null_status_df.select(regexp_extract('value', host_pattern, 1).alias('host'),
                                      regexp_extract('value', ts_pattern, 1).alias('timestamp'),
                                      regexp_extract('value', method_uri_protocol_pattern, 1).alias('method'),
                                      regexp_extract('value', method_uri_protocol_pattern, 2).alias('endpoint'),
                                      regexp_extract('value', method_uri_protocol_pattern, 3).alias('protocol'),
                                      regexp_extract('value', status_pattern, 1).cast('integer').alias('status'),
                                      regexp_extract('value', content_size_pattern, 1).cast('integer').alias('content_size'))
bad_status_df.show(truncate=False)



logs_df = logs_df[logs_df['status'].isNotNull()]
exprs = [count_null(col_name) for col_name in logs_df.columns]
logs_df.agg(*exprs).show()

regexp_extract('value', r'\s(\d+)$', 1).cast('integer').alias('content_size')


null_content_size_df = df.filter(~df['value'].rlike(r'\s\d+$'))
null_content_size_df.count()

null_content_size_df.take(10)

logs_df = logs_df.na.fill({'content_size': 0})
exprs = [count_null(col_name) for col_name in logs_df.columns]
logs_df.agg(*exprs).show()

month_map = {
  'Jan': 1, 'Feb': 2, 'Mar':3, 'Apr':4, 'May':5, 'Jun':6, 'Jul':7,
  'Aug':8,  'Sep': 9, 'Oct':10, 'Nov': 11, 'Dec': 12
}

def parse_clf_time(text):
    """ Convert Common Log time format into a Python datetime object
    Args:
        text (str): date and time in Apache time format [dd/mmm/yyyy:hh:mm:ss (+/-)zzzz]
    Returns:
        a string suitable for passing to CAST('timestamp')
    """
    # NOTE: We're ignoring the time zones here, might need to be handled depending on the problem you are solving
    return "{0:04d}-{1:02d}-{2:02d} {3:02d}:{4:02d}:{5:02d}".format(
      int(text[7:11]),
      month_map[text[3:6]],
      int(text[0:2]),
      int(text[12:14]),
      int(text[15:17]),
      int(text[18:20])
    )

udf_parse_time = udf(parse_clf_time)

logs_df = (logs_df\
    .select('*', udf_parse_time(logs_df['timestamp'])\
    .cast('timestamp')\
    .alias('time'))\
    .drop('timestamp'))

logs_df.show(10, truncate=True)

logs_df.printSchema()

logs_df.cache()



