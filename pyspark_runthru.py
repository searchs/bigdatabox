#Pyspark Transformations

from pyspark.sql import SparkSession

import pyspark.sql.types as typ
import pyspark.sql.functions as f

spark = SparkSession \
    .builder \
    .appName("Python Spark Refresher") \
    .master("local[*]") \
    .getOrCreate()

rdd_valid_struc = spark.sparkContext.parallelize([
    ('Tom', 0)
    ,('Sparky', 1)
    ,('Sarah', 2)
    ,('Oreilly',3)

])

names_only = rdd_valid_struc.map(lambda elem: elem[0])


def split_on_comma(input_string):
    """Split string using comma"""
    return input_string.split(',')

for el in rdd_valid_struc.map(split_on_comma).take(2):
    print(el)

# shorter version with comprehension
rdd_from_file = spark.read.text('../raw_data.txt')
for el in rdd_valid_struc.map(lambda ele: ele.split(',')).take(2):
    print(el)


def convert_to_float(input_str):
    """Convert entry into float or return -665 code

    Keyword arguments:
    argument -- description
    Return: return_description
    """

    try:
        return float(input_str)
    except:
        return -665

to_filter = rdd_from_file \
    .map(lambda e: e.split(',')) \
    .map(lambda raw:
        [r for r in raw[:4]] +
        [convert_to_float(r) for r in raw[4:]])

to_filter.take(2)

# FlatMap

def map_sample(input_list):
    output_list = []

    for item in input_list:
        temp = item.copy()
        temp[1] *= 10

        output_list.append(temp)
    return output_list


def flatmap_sample(input_list_2):
    output_list_2 = []

    for item in input_list_2:
        temp = item.copy()
        temp[1] *= 10

        output_list_2 += temp
    return output_list_2



sample_list = [
    [1,3]
    ,[2,4]
    ,[3,5]
    ,[4,6]
    ,[5,7]
]



import datetime as dt

def parse_csv_row(input_row):
    try:
        row_split = input_row.split(",")
        row_split[0] = dt.datetime.strptime(row_split[0], '%m/%d/%y')
        row_split[4] = int(row_split[4])

        for i in [5,6]:
            row_split[i] = float(row_split[i])
        return [row_split]
    except:
        return []


rdd_from_file.map(parse_csv_row).filter(lambda elem: len(elem) > 0).take(3)
rdd_from_file_clean = rdd_from_file.flatMap(parse_csv_row)
rdd_from_file_clean.take(3)


# Distincts

samp_list = [1,1,1,4,3,4,5,6,4,8,6,9]
distinct = []
seen = {}

for el in samp_list:
    if el in seen:
        continue
    else:
        distinct.append(el)
        seen[el] = 1

print(distinct)

# Using PySpark
items = rdd_from_file_clean.map(lambda m: m[3]).distinct()
items.collect()


rdd_from_file_clean.map(lambda m: m[2]).distinct().count()


# Sampling

rdd_from_file_clean.sample(False, 0.2).count()


cities = spark.sparkContext.parallelize([
    ('East', 'Boston'),
    ('Central', 'Chicago'),
    ('West', 'Seattle'),
    ('Isale', 'Eko')
])

rdd_from_file_clean.map(lambda m: (m[1], m)) \
    .join(cities) \
    .map(lambda c: c[1][0] + [c[1][1]]) \
    .take(2)

rdd_from_file_clean.getNumPartitions()

rdd_from_file_clean_repartitioned = rdd_from_file_clean.repartition(2)
rdd_from_file_clean_repartitioned.getNumPartitions()


# Sort RDD content
rdd_from_file_clean_repartitioned_sorted = rdd_from_file_clean \
    .map(lambda elem: (int(elem[6]), elem)) \
    .repartitionAndSortWithinPartitions(2, lambda x:x) \
    .map(lambda elem: tuple(elem[1]))


rdd_from_file_clean_repartitioned_sorted.glom().collect()


# TODO: Actions
from operator import add
total_value = rdd_from_file_clean.map(lambda a: a[-1]).reduce(add)
total_value
# another implementation
total_value_2 = rdd_from_file_clean.map(lambda e: e[-1]).reduce(lambda x,y: x + y)
total_value_2


# TODO: ReduceByKey
sales_by_region = rdd_from_file_clean.map(lambda rr: (rr[1], rr[-1])) \
    .reduceByKey(lambda x,y: x +y)


for elem in sales_by_region.collect():
    print(elem)


# TODO: Count
rdd_from_file_clean.count()
rdd_from_file.countApprox(10, confidence=0.9)

sales = rdd_from_file_clean.map(lambda e: e[2])

sales.countApproxDistinct()

sales.distinct().count()


# TODO: Foreach
distinct_sales = sales.distinct()
distinct_sales.foreach(print)

# TODO: Aggregate Action

seqOp = (lambda x,y: (x[0] + y, x[1] + 1) )
combOp = (lambda x,y: (x[0] + y[0], x[1] + y[1]) )

rdd_from_file_clean.map(lambda el: el[-1]) \
    .aggregate((0.0,0), seqOp, combOp)

# TODO: AggregateByKey
seqOp = (lambda x,y: (x[0] + y, x[1] + 1) )
combOp = (lambda x,y: (x[0] + y[0], x[1] + y[1]) )

for elem in rdd_from_file_clean \
    .map(lambda el: (el[2], (el[-1],1))) \
    .aggregateByKey((0.0,0), seqOp, combOp) \
    .map(lambda el: (el[0], el[1][0], el[1][1], el[1][0]/el[1][1])) \
    .collect():
        print(elem)


# TODO:  CombineByKey
def combiner(elem):
    return [elem]


def value_merger(elem1, elem2):
    elem1.append(elem2)
    return elem1

def combiner_merger(elem_1, elem_2):
    el1 = dict(elem_1)

    for e in elem_2:
        if e[0] not in el1:
            el1[e[0]] = 0

        el1[e[0]] += e[1]
    return list(el1.items())

for element in rdd_from_file_clean \
    .map(lambda el: (el[2], el[3], el[-1]))) \
    .combineByKey(combiner, value_merger, combiner_merger) \
    .collect():
        print(element)


# TODO: Histogram


hist = rdd_from_file_clean \
    .map(lambda el: el[-1]) \
    .histogram(10)

for bucket in [(round(b,0), v) for b,v in zip(hist[0], hist[1])]:
    print(bucket)



# TODO:  SortBy - Action
# Example: top sales
for elem in rdd_from_file_clean \
    .map(lambda el: (el[2], el[0].strftime('%Y-%m'), el[1],el[-1])) \
    .take(5):
        print(elem)



# TODO: SortByKey
for elem in rdd_from_file_clean \
    .map(lambda el: (el[-1], (el[2],el[0].strftime('%Y-%m'), el[1]))) \
    .sortByKey(ascending=False) \
    .take(5):
        print(elem)



# TODO: Maths Ops

rdd_from_file_clean.agg(
    {
        'Total' : 'avg'
    }
).show()

# TODO: Multiple aggs
rdd_from_file_clean.agg(
    {
        'Total': 'avg'
        ,'Units': 'sum'
    }
)


# TODO: Multiple Aggregations
aggregations = [
    f.min('Total').alias('Total_min')
    ,f.max('Total').alias('Total_max')
    ,f.avg('Total').alias('Total_avg')
    ,f.stddev('Total').alias('Total_stddeve')
]

(
    rdd_from_file_clean.agg(*aggregations).show()
)


# TODO: Using SparkSQL

rdd_from_file_clean.createOrReplaceTempView('sales_view')
(
    spark.sql('''
              SELECT
                MIN(Total) AS Total_min
                ,MAX(Total) AS Total_max
                ,AVG(Total) AS Total_avg
                ,STDDEV(Total) AS Total_std
                FROm sales_view
              ''')
    .show()
)

# TODO: Use SelectExpr
rdd_from_file_clean.selectExpr(
    'MIN(Total) AS Total_min'
    ,'MAX(Total) AS Total_max'
    ).show()


regions_df = spark.createDataFrame()

rdd_from_file_clean.join(
    regions_df, on=['Regions'], how='left_outer').orderBy('OrderDate').show(4)



# TODO:  Describe only numeric fields

numeric_columns = [e[0]
                   for e in rdd_from_file_clean.dtypes
                   if e[1] in ('int', 'double')
                   ]

rdd_from_file_clean.select(numeric_columns).describe().show()


# TODO: Skewness - Kurtosis
rdd_from_file_clean.agg(*[
    f.kurtosis(f.col('Total')).alias('kurtosis_Total')
    ,f.skewness(f.col('Total')).alias('skewness_Total')
]).show()



#!/usr/bin/env python3

import numpy as np

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pyspark.sql.types as typ





if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .appName("intense sales data") \
        .master("local[*]") \
        .getOrCreate()

    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")


    raw_df = spark.read.csv(
        "sample_data.csv",
        inferSchema=True,
        header=True)


    numeric_columns = [e[0]
                   for e in raw_df.dtypes
                   if e[1] in ('int', 'double')
                   ]

    raw_df.select(numeric_columns).describe().show()


    (
        raw_df.select('Region', 'Rep')
        .distinct()
        .orderBy('Region', 'Rep')
        .show()
    )

    raw_df = (
        raw_df
        .withColumn('OrderDate',
                    f.to_date('OrderDate','MM/dd/yy'))
    )

    (raw_df
     .withColumnRenamed('OrderDate', 'Date')
     .withColumnRenamed('Region', 'Location')
     .show(5)
    )

    sample_df_broken_rdd = (
        raw_df.rdd.map( lambda row:
            row[:5] +
            ((None,None) if np.random.rand() < 0.2 else tuple(row[5:]))
            )
    )

    sample_df_broken = (
        spark
        .createDataFrame(
            sample_df_broken_rdd
            , raw_df.columns
        )
    )

    sample_df_broken = sample_df_broken.dropna(subset=['OrderDate'])

    # Filling Missing Values
    avg_unit_cost = (
        sample_df_broken
        .select('UnitCost')
        .agg(
            f.mean(f.col('UnitCost')).alias('UnitCost')
        ).toPandas()
        .to_dict('records')
        )

    print(avg_unit_cost)

    sample_df_fixed = (
        sample_df_broken
        .fillna(*avg_unit_cost)
        .withColumn('Total', f.col('Units') * f.col('UnitCost'))
    )

    sample_df_fixed.show(4)

    # TODO: Filter Data
    raw_df.where('Item = "Pencil"').show(4)

    raw_df.filter('Item IN ("Pencil", "Binder") AND Rep = "Jones"').show(4)


    # TODO: Aggregating Date in DataFrames
    (
        raw_df
         .groupBy('Rep', 'Region')
         .count()
         .orderBy('count', ascending=False)
         .show()
    )


    (
        raw_df
        .groupBy('Rep', 'Item')
        .agg(
            f.sum('Units').alias('UnitsTotal')
            ,f.round(f.sum('Total'), 2).alias('GrandTotal')
            ,f.round(f.avg('Total'), 2).alias('AvgPerTransaction')
        )
        .orderBy('GrandTotal', ascending=False)
        .show()

    )

    # Saving Data
    # CSV
    (
        sample_df_fixed
        .write
        .mode('overwrite')
        .csv('./outputs/sample_data_fixed.csv')
    )

    # PARQUET
    (
        sample_df_fixed
        .write
        .parquet(
            './outputs/sample_data_fixed.parquet'
            , mode='overwrite'
            , partitionBy='Rep'
            , compression='gzip'
        )
    )

    (
      sample_df_fixed
      .write
      .json(
          './outputs/sample_data_fixed.json'
          , mode='overwrite'
          , dateFormat='yyy-mm-dd'
          , compression='gzip'
      )
    )


