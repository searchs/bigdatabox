#Using SparkContext
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[2]").setAppName("RDD Example")
sc = SparkContext(conf=conf)

# different way of setting configurations 
#conf.setMaster('some url')
#conf.set('spark.executor.memory', '2g')
#conf.set('spark.executor.cores', '4')
#conf.set('spark.cores.max', '40')
#conf.set('spark.logConf', True)

# sparkContext.parallelize materializes data into RDD 
# documentation: https://spark.apache.org/docs/2.1.1/programming-guide.html#parallelized-collections
rdd = sc.parallelize([('Richard', 22), ('Alfred', 23), ('Loki',4), ('Albert', 12), ('Alfred', 9)])

rdd.collect() # [('Richard', 22), ('Alfred', 23), ('Loki', 4), ('Albert', 12), ('Alfred', 9)]

# create two different RDDs
left = sc.parallelize([("Richard", 1), ("Alfred", 4)])
right = sc.parallelize([("Richard", 2), ("Alfred", 5)])

joined_rdd = left.join(right)
collected = joined_rdd.collect()

collected #[('Alfred', (4, 5)), ('Richard', (1, 2))]


# Using SparkSession
# Notice we’re using pyspark.sql library here
from pyspark.sql import SparkSession

spark = SparkSession.builder \
        .master("local") \
        .appName("CSV file loader") \
        .getOrCreate()

# couple ways of setting configurations
#spark.conf.set("spark.executor.memory", '8g')
#spark.conf.set('spark.executor.cores', '3')
#spark.conf.set('spark.cores.max', '3')
#spark.conf.set("spark.driver.memory", '8g')

file_path = "./AB_NYC_2019.csv"
# Always load csv files with header=True
df = spark.read.csv(file_path, header=True)

df.printSchema()

df.select('neighbourhood').distinct().show(10, False)



def repartition(
    df: DataFrame, 
    min_partitions: int, 
    max_partitions: int
) -> DataFrame:
        """Apply repartition or coalesce to DataFrame based on project defaults"""
    num_partitions = df.rdd.getNumPartitions()
    if(num_partitions < min_partitions):
        df = df.repartition(min_partitions)
    elif(num_partitions > max_partitions):
        df = df.coalesce(max_partitions)
    return df

