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
